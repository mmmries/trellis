//! Postgres-backed catalog for transform definitions (issue #23).
//!
//! **Schema choice**: a definition is stored as its original source text
//! (`transform_definitions.definition_text`) rather than a serialized AST.
//! Reading it back re-parses via [`super::parse`], reusing the grammar's
//! own parser instead of standing up a second, independently-drifting
//! serialization format for the same information. The tradeoff is a
//! re-parse on every read; given definitions are small and read
//! infrequently relative to the source-row volume they govern, that's the
//! right side to take the cost on. Flagged in the issue #23 report as the
//! open question to confirm.
//!
//! Every source table has exactly one monotonically increasing version, in
//! `source_table_versions`, that definition creation bumps in the same
//! transaction as the insert. That version lives in a real, lockable row
//! (not a computed value) so stage 05's version fence can `FOR SHARE`/
//! `FOR UPDATE` it directly.
//!
//! v1 definitions are immutable: this module only exposes creation and
//! read, no update/delete.

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::pool::Pool;

use super::ast::{KeySpace, RelationshipDef, TransformDef, ValueType};
use super::backfill::{self, BackfillError};
use super::ddl::{self, DdlError};
use super::error::ParseError;
use super::model::{
    Definition, EdgeKind, NodeKind, RelationshipCardinality, RelationshipDefinition, SchemaEdge,
    SchemaNode,
};
use super::parser::{parse, parse_relationship};
use super::validate::{RelationshipWarning, ResolvedRelationship, ValidationError, validate};

/// Why creating or reading a definition failed.
#[derive(Debug)]
pub enum CatalogError {
    /// The source text failed to parse (issue #22's grammar).
    Parse(ParseError),
    /// The parsed definition failed validation (issue #23).
    Validate(ValidationError),
    /// Acquiring a connection or running a query against Postgres failed.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// A definition's persisted `source_columns` jsonb held a value other
    /// than `"numeric"`/`"text"`/`"boolean"`/`"uuid"` for some column —
    /// meaning the row was written by something other than
    /// [`create_definition`], since that's the only writer and it only ever
    /// encodes [`ValueType`]'s variants.
    UnknownValueType { column: String, text: String },
    /// The definition's initial backfill (issue #23) failed to enumerate its
    /// source table.
    Backfill(crate::intake::IntakeError),
    /// `def.source` doesn't resolve to any schema on this connection's
    /// search path (see [`resolve_source_schema_in_txn`]) — the table was
    /// dropped, renamed, or never existed under that bare name.
    SourceTableNotFound(String),
    /// An aggregate (`GROUP BY`) definition (issue #47) was rejected because
    /// its source table's replica identity doesn't guarantee the old row
    /// image the delta-maintenance path (`apply_aggregate.rs`) needs on
    /// delete/update/re-parent. Wraps [`crate::intake::IntakeError`] — the
    /// same [`crate::intake::require_replica_identity_full`] check
    /// [`assert_replica_identity_supports_to_many`] mirrors for relationships
    /// (#41) — rather than [`CatalogError::Backfill`]'s blanket
    /// `From<IntakeError>`, since that variant's message ("failed to
    /// backfill...") would misdescribe a definition-time rejection as a
    /// backfill failure.
    ReplicaIdentityRequired(crate::intake::IntakeError),
    /// [`install_definition`]'s target-table DDL (run before either backfill
    /// path) failed.
    Ddl(DdlError),
    /// [`install_definition`]'s direct backfill attempt
    /// ([`backfill::backfill_definition`]) failed with something other than
    /// [`BackfillError::Unsupported`] — an `Unsupported` shape instead falls
    /// back to the ring ([`create_definition`]) rather than surfacing here.
    DirectBackfill(BackfillError),
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::Parse(err) => write!(f, "failed to parse transform definition: {err}"),
            CatalogError::Validate(err) => {
                write!(f, "definition failed validation: {err}")
            }
            CatalogError::Db(err) => {
                write!(f, "transform catalog database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            CatalogError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            CatalogError::UnknownValueType { column, text } => write!(
                f,
                "column '{column}' has an unrecognized persisted value type '{text}'"
            ),
            CatalogError::Backfill(err) => {
                write!(f, "failed to backfill the definition's source table: {err}")
            }
            CatalogError::SourceTableNotFound(table) => {
                write!(f, "source table \"{table}\" not found on the search path")
            }
            CatalogError::ReplicaIdentityRequired(err) => write!(f, "{err}"),
            CatalogError::Ddl(err) => write!(f, "failed to create target table: {err}"),
            CatalogError::DirectBackfill(err) => write!(f, "direct backfill failed: {err}"),
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CatalogError::Parse(err) => Some(err),
            CatalogError::Validate(err) => Some(err),
            CatalogError::Db(err) => Some(err),
            CatalogError::Pool(err) => Some(err),
            CatalogError::UnknownValueType { .. } => None,
            CatalogError::Backfill(err) => Some(err),
            CatalogError::SourceTableNotFound(_) => None,
            CatalogError::ReplicaIdentityRequired(err) => Some(err),
            CatalogError::Ddl(err) => Some(err),
            CatalogError::DirectBackfill(err) => Some(err),
        }
    }
}

impl From<ParseError> for CatalogError {
    fn from(err: ParseError) -> Self {
        CatalogError::Parse(err)
    }
}

impl From<ValidationError> for CatalogError {
    fn from(err: ValidationError) -> Self {
        CatalogError::Validate(err)
    }
}

impl From<tokio_postgres::Error> for CatalogError {
    fn from(err: tokio_postgres::Error) -> Self {
        CatalogError::Db(err)
    }
}

impl From<crate::error::Error> for CatalogError {
    fn from(err: crate::error::Error) -> Self {
        CatalogError::Pool(err)
    }
}

impl From<crate::intake::IntakeError> for CatalogError {
    fn from(err: crate::intake::IntakeError) -> Self {
        CatalogError::Backfill(err)
    }
}

/// Parses, validates, and stores a new transform definition, bumping its
/// source table's version in the same transaction. `source_columns` maps the
/// definition's source table's known columns to their [`ValueType`] (see
/// [`super::validate::validate`] — introspecting a live Postgres schema for
/// this is intake's concern, out of scope here).
pub async fn create_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    create_definition_inner(pool, source_text, source_columns, true).await
}

/// Like [`create_definition`], but stages *no* ring-enumeration backfill: the
/// definition and its version bump are persisted, but the source table is not
/// enumerated into the ring. Callers that build the target directly
/// (`defs::backfill::backfill_definition` — issue #63 M3's set-based,
/// key-range-chunked source→target build) use this so the from-scratch build
/// doesn't *also* flood the ring with one `Recompute` marker per source row;
/// the ring is then left to handle only live CDC deltas after the direct
/// build's fence. Every other caller wants the ring-enumeration backfill and
/// keeps using [`create_definition`].
pub async fn create_definition_without_backfill(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    create_definition_inner(pool, source_text, source_columns, false).await
}

/// The front door real callers use to stand up a new definition (issue #63
/// C1): creates the target table, then backfills it with the fast,
/// set-based [`backfill::backfill_definition`] when `def`'s shape supports
/// it, falling back to the ring-based [`create_definition`] only on
/// [`BackfillError::Unsupported`].
///
/// Target-table creation always runs first, unconditionally — before either
/// backfill path is attempted — because neither `backfill_definition` nor
/// `create_definition` creates it themselves; both assume it already exists
/// (see their own doc comments). Doing it once here, ahead of both branches,
/// preserves the fast path's build/CDC fence: the table exists and is fully
/// built by the direct backfill *before* the definition is ever persisted to
/// the catalog via [`create_definition_without_backfill`], so nothing can
/// fold a CDC delta onto this target before the build has run. The ring
/// fallback branch also benefits from the table already existing, though it
/// has no comparable fence requirement of its own.
pub async fn install_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    target_schema: &str,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;

    match &def.key_space {
        KeySpace::OneToOne => {
            let pk = ddl::source_primary_key(pool, &def.source)
                .await
                .map_err(CatalogError::Ddl)?;
            ddl::create_target_table(pool, &def, target_schema, &pk, source_columns)
                .await
                .map_err(CatalogError::Ddl)?;
        }
        KeySpace::Aggregate { .. } => {
            ddl::create_aggregate_target_table(pool, &def, target_schema, source_columns)
                .await
                .map_err(CatalogError::Ddl)?;
        }
    }

    match backfill::backfill_definition(pool, &def, target_schema, source_columns).await {
        Ok(()) => {
            // Issue #79 (bug B): the direct build just folded the current
            // contents of `def.source` and every to-side relationship table it
            // reads into the target. Record that coverage *before* persisting
            // this definition, so the redundant publication-join catch-up
            // enumeration of those tables can be skipped (see
            // `record_direct_backfill_coverage`).
            record_direct_backfill_coverage(pool, &def).await?;
            create_definition_without_backfill(pool, source_text, source_columns).await
        }
        Err(BackfillError::Unsupported(_)) => {
            create_definition(pool, source_text, source_columns).await
        }
        Err(err) => Err(CatalogError::DirectBackfill(err)),
    }
}

/// Records direct-backfill coverage (issue #79, bug B) for a definition that
/// was just built through the fast path: the definition's own source table
/// plus every to-side relationship table its fields read. Coverage lets
/// [`crate::intake::publication::run_pending_backfills`] skip the redundant
/// full-table enumeration those tables would otherwise trigger when they join
/// the CDC publication.
///
/// Runs *before* the new definition is persisted, so [`table_has_other_reader`]
/// sees only the *pre-existing* readers of each table. Coverage is recorded
/// only for a table this build is the sole reader of; if any other definition
/// already reads it, we instead *clear* coverage, because a second reader was
/// (or will be) built at a different fence and only a full enumeration can be
/// trusted to catch every reader up — the conservative default the issue's
/// safety valve calls for. Recording all tables in one transaction keeps the
/// read-then-write of `table_has_other_reader`/record/clear consistent.
async fn record_direct_backfill_coverage(
    pool: &Pool,
    def: &TransformDef,
) -> Result<(), CatalogError> {
    // Distinct to-side tables this definition reads through a relationship.
    let resolved = resolve_relationships(pool, def).await?;
    let mut tables: HashSet<String> = resolved.values().map(|r| r.to_table.clone()).collect();
    tables.insert(def.source.clone());

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for bare_table in tables {
        let schema = resolve_source_schema_in_txn(&txn, &bare_table).await?;
        let qualified = crate::intake::publication::qualify(&schema, &bare_table)?;
        if table_has_other_reader(&txn, &bare_table).await? {
            crate::intake::publication::clear_backfill_coverage(&*txn, &qualified).await?;
        } else {
            crate::intake::publication::record_backfill_coverage(&*txn, &qualified).await?;
        }
    }
    txn.commit().await?;
    Ok(())
}

/// Whether any *already-persisted* transform definition reads `table` — either
/// as its own `FROM` source, or as the to-side of a relationship anchored on a
/// table some definition transforms. Conservative on the relationship side: it
/// does not confirm the anchoring definition's text actually references that
/// relationship, so it may report a reader where none truly exists. That only
/// ever suppresses a coverage record (falling back to full enumeration), which
/// is always safe — the direction the issue's safety valve demands.
async fn table_has_other_reader(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
) -> Result<bool, CatalogError> {
    let exists: bool = txn
        .query_one(
            "select \
               exists(select 1 from transform_definitions where source_table = $1) \
               or exists( \
                 select 1 from relationship_definitions r \
                 join transform_definitions d on d.source_table = r.from_table \
                 where r.to_table = $1 \
               )",
            &[&table],
        )
        .await?
        .get(0);
    Ok(exists)
}

async fn create_definition_inner(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    backfill: bool,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;
    // Issue #40: enrichment fields (`<rel>.<col>`) are validated against
    // catalog-resolved relationship metadata — cardinality (ADR-0006's
    // to-one/to-many rules) and each referenced to-side column's type — which
    // the sync, DB-less validator can't fetch itself, so resolve it here (same
    // caller-supplies-context split as `source_columns`).
    let relationships = resolve_relationships(pool, &def).await?;
    validate(&def, source_columns, &relationships)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Issue #47: an aggregate (`GROUP BY`) definition's delta-maintenance
    // path needs the source row's *old* image on delete/update/re-parent
    // (`apply_aggregate.rs`'s `accumulate_changes`) to find which group to
    // decrement — reject up front, before any of this transaction's other
    // side effects, if the source table's replica identity can't guarantee
    // one. Checked first (ahead of node/edge/backfill work below) so a
    // doomed-to-fail aggregate definition never enumerates its source table
    // or touches the schema graph.
    if let KeySpace::Aggregate { .. } = &def.key_space {
        assert_replica_identity_supports_aggregate(&txn, &def).await?;
    }

    // Issue #20: every definition's source and target resolve to a
    // first-class `SchemaNode`, created on first reference (a source node
    // the moment something first transforms it; a target node the moment
    // its owning definition is created) — a side effect alongside the
    // catalog writes below rather than a change to `TransformDef`'s shape,
    // per the issue's "prefer the smaller change" guidance.
    let source_node = resolve_node_in_txn(&txn, &def.source, NodeKind::Source).await?;
    let target_node = resolve_node_in_txn(&txn, &def.target, NodeKind::Target).await?;

    // Issue #22 (generalized): reject this definition if the `Source` edge
    // it's about to add — def.source -> def.target — would close a cycle in
    // the table-level dependency graph, transitively through any edges
    // already persisted. Checked against the transaction's own view of
    // `schema_edges` so it sees the graph exactly as it will look right up
    // to (but not including) the edge this definition is about to add.
    // Known v1 limitation: under Postgres's default read-committed
    // isolation, two concurrent `create_definition` calls (e.g. one adding
    // a->b, another adding b->a) can each pass this check before either
    // commits — nothing here serializes them (no advisory lock/
    // SERIALIZABLE) — so a cycle could theoretically still persist. Same
    // class of race as any check-then-insert pattern; accepted for now,
    // out of scope for this issue.
    reject_if_table_cycle(&txn, &def.source, &def.target).await?;

    // Issue #21: a transform's `FROM` is a `Source` dependency edge from its
    // source node to its target node — persisted alongside the node
    // resolutions above so `dependents_of` can walk the graph instead of
    // matching on `transform_definitions.source_table` string equality.
    persist_edge_in_txn(&txn, source_node.id, target_node.id, EdgeKind::Source).await?;

    // Issue #23: a definition's initial backfill is one enumeration of its
    // source table, staged as `Recompute` triggers into the active ring
    // segment via the same append path CDC/reverse-propagation use — one
    // call here regardless of how many calculated fields the definition
    // declares, not one per field, preserving the "N columns, one backfill"
    // property as the definition model becomes first-class. Today the
    // grammar's `FROM`/`TARGET` have no schema-qualification syntax, so
    // `def.source` must be resolved live, exactly as Postgres itself would
    // resolve the bare name: via `resolve_source_schema_in_txn`, which walks
    // `search_path` (`pool::session_bootstrap` pins it to the Trellis
    // schema, then the target schema, then `public`, in that order). This
    // covers both a raw/CDC source (typically `public`) and a chained
    // definition's source being a *previous* definition's target table
    // (whatever schema `config.target_schema()` actually resolved to,
    // which may not be the `DEFAULT_TARGET_SCHEMA` constant if overridden)
    // without needing to special-case on `source_node.is_target`.
    if backfill {
        let source_schema = resolve_source_schema_in_txn(&txn, &def.source).await?;
        let qualified_source = crate::intake::publication::qualify(&source_schema, &def.source)?;
        crate::intake::publication::enumerate_and_append(&txn, &qualified_source).await?;
    }

    let version: i64 = txn
        .query_one(
            "insert into source_table_versions (source_table, version)
             values ($1, 1)
             on conflict (source_table)
             do update set version = source_table_versions.version + 1
             returning version",
            &[&def.source],
        )
        .await?
        .get(0);

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let id: i64 = txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text, source_columns)
             values ($1, $2, $3, $4, jsonb_object($5::text[], $6::text[]))
             returning id",
            &[
                &def.target,
                &def.source,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
            ],
        )
        .await?
        .get(0);

    txn.commit().await?;

    Ok(Definition {
        id,
        source_version: version,
        def,
        source_columns: source_columns.clone(),
    })
}

/// Parses, validates, and stores a new relationship declaration (issue #26
/// storage, issue #27 validation, ADR-0006): resolves/creates `schema_nodes`
/// for both endpoints, persists a `schema_edges` row from `from_table` to
/// `to_table` tagged [`EdgeKind::Relationship`], and inserts the immutable
/// `relationship_definitions` row — all in one transaction, mirroring
/// [`create_definition`]'s pattern.
///
/// Validates, in order: both endpoints' `table.column` exist and resolve to
/// comparable Postgres types ([`column_type_in_txn`] /
/// [`assert_comparable_types`], ADR-0006's "type-check the join"); the
/// relationship's name is not already declared on `from_table`
/// ([`ValidationError::DuplicateRelationshipName`], a friendlier
/// definition-time surfacing of the same rule
/// `relationship_definitions_from_table_name_key` backstops at the DB
/// level); and the new `Relationship` edge would not close a cycle
/// ([`reject_if_table_cycle`], generalized unchanged from
/// [`create_definition`]'s `Source`-edge use). Cardinality
/// ([`RelationshipCardinality`]) is determined via
/// [`to_col_cardinality_in_txn`] and persisted, not rejected on — ADR-0006's
/// "a to-many reference must be aggregate-wrapped" rule is a *reference*-time
/// check (validating how a relationship is *used* in a calculated field),
/// deferred past this issue since no such reference resolves yet (see
/// [`super::ast::Expr::RelationshipPath`]).
///
/// Node-kind resolution: a relationship's endpoints may each be "a source
/// table or a transform target, in any combination" (ADR-0006), and nothing
/// here can tell which without cross-referencing `transform_definitions`.
/// Both endpoints resolve as [`NodeKind::Source`] — directionally accurate
/// either way, since computing the join reads both tables' columns
/// regardless of whether one side later turns out to also be a transform
/// target — and [`resolve_node_in_txn`]'s flags are additive (OR'd in, never
/// cleared), so a later `create_definition` call that resolves the same
/// table as [`NodeKind::Target`] merges into the same node rather than
/// conflicting with this choice.
pub async fn create_relationship(
    pool: &Pool,
    source_text: &str,
) -> Result<RelationshipDefinition, CatalogError> {
    let def: RelationshipDef = parse_relationship(source_text)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    let from_type = column_type_in_txn(&txn, &def.from_table, &def.from_col).await?;
    let to_type = column_type_in_txn(&txn, &def.to_table, &def.to_col).await?;
    assert_comparable_types(&def, &from_type, &to_type)?;
    assert_join_key_type_supported(&def, &from_type, &to_type)?;

    let already_declared: bool = txn
        .query_one(
            "select exists (
                select 1 from relationship_definitions where from_table = $1 and name = $2
             )",
            &[&def.from_table, &def.name],
        )
        .await?
        .get(0);
    if already_declared {
        return Err(ValidationError::DuplicateRelationshipName {
            from_table: def.from_table.clone(),
            name: def.name.clone(),
        }
        .into());
    }

    let from_node = resolve_node_in_txn(&txn, &def.from_table, NodeKind::Source).await?;
    // `to_table` is marked `is_source` here too, even though a relationship's
    // to-side is often really a transform target: the flag is additive/OR'd
    // (a later `create_definition` call can still set `is_target` on the
    // same node), and no consumer reads `schema_nodes.is_source` directly
    // today — [`all_source_tables`] (the publication feeder) reads
    // `transform_definitions`/`relationship_definitions` directly, not this
    // flag (issue #65: it now also follows relationship edges transitively,
    // but still via those tables, not `schema_nodes`). If a future
    // `is_source` consumer reads `schema_nodes` directly, re-check this call.
    let to_node = resolve_node_in_txn(&txn, &def.to_table, NodeKind::Source).await?;

    // The `Relationship` edge is persisted `to_table -> from_table` (parent
    // -> child), matching `Source`'s "to_node depends on from_node"
    // convention (see `SchemaEdge`'s doc comment): the FK-holding
    // `from_table` is the dependent side — a bare-path reference like
    // `product.x` in a calculated field over `from_table` pulls from
    // `to_table`, so `from_table` depends on `to_table`, not the reverse.
    // Persisting it `from_table -> to_table` instead (the naive reading of
    // "FROM ... TO ...") would invert that: it'd wrongly reject a
    // target-table-references-its-own-source relationship as a false
    // 2-cycle (both edges actually mean "target depends on source"), and it
    // would make future dependents-of-a-changed-table traversals (#28+)
    // miss relationship dependents, since `edges_from(to_table)` wouldn't
    // reach `from_table` at all.
    reject_if_table_cycle(&txn, &def.to_table, &def.from_table).await?;

    persist_edge_in_txn(&txn, to_node.id, from_node.id, EdgeKind::Relationship).await?;

    let cardinality = to_col_cardinality_in_txn(&txn, &def.to_table, &def.to_col).await?;

    // To-many's join key is a non-PK column on the to-side; reverse recompute
    // reads it from delete/re-parent pre-images, which the default (PK)
    // replica identity omits — reject unless the to-side carries it (#41).
    if cardinality == RelationshipCardinality::ToMany {
        assert_replica_identity_supports_to_many(&txn, &def).await?;
    }

    let mut warnings = Vec::new();
    if !has_usable_fk_index_in_txn(&txn, &def.from_table, &def.from_col).await? {
        warnings.push(RelationshipWarning::MissingFkIndex {
            from_table: def.from_table.clone(),
            from_col: def.from_col.clone(),
        });
    }

    let id: i64 = txn
        .query_one(
            "insert into relationship_definitions
                (name, from_table, from_col, to_table, to_col, definition_text, cardinality)
             values ($1, $2, $3, $4, $5, $6, $7)
             returning id",
            &[
                &def.name,
                &def.from_table,
                &def.from_col,
                &def.to_table,
                &def.to_col,
                &source_text,
                &cardinality.as_str(),
            ],
        )
        .await?
        .get(0);

    txn.commit().await?;

    Ok(RelationshipDefinition {
        id,
        def,
        cardinality,
        warnings,
    })
}

/// Reads back the relationship named `name` declared on `from_table` — the
/// pair a [`super::ast::Expr::RelationshipPath`]'s `rel` head resolves
/// against (ADR-0006: a relationship name is unique per from-table, not
/// global, so both are needed to identify one row). Re-parses the persisted
/// `definition_text` rather than reconstructing [`RelationshipDef`] from the
/// denormalized columns, matching [`dependents_of`]'s "reuse the grammar's
/// own parser" convention; `cardinality` is read back from its own column
/// instead, since it isn't part of the source text (issue #27: it's derived,
/// not declared).
pub async fn relationship_by_name(
    pool: &Pool,
    from_table: &str,
    name: &str,
) -> Result<Option<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, definition_text, cardinality
             from relationship_definitions
             where from_table = $1 and name = $2",
            &[&from_table, &name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    let id: i64 = row.get(0);
    let text: String = row.get(1);
    let cardinality_text: String = row.get(2);
    let def = parse_relationship(&text)?;
    let cardinality =
        RelationshipCardinality::from_persisted(&cardinality_text).unwrap_or_else(|| {
            panic!(
                "relationship_definitions.cardinality held unrecognized value '{cardinality_text}'"
            )
        });
    Ok(Some(RelationshipDefinition {
        id,
        def,
        cardinality,
        // Creation-time guidance, not a fact about the persisted row — see
        // the field's doc comment on [`RelationshipDefinition`].
        warnings: Vec::new(),
    }))
}

/// Every relationship whose `to_table` is `to_table` — the reverse of
/// [`relationship_by_name`]'s `from_table` lookup. The staging reverse
/// recompute (issue #30) uses this to answer "a row in this table just
/// changed; which relationships point *at* it, so which from-side targets must
/// re-derive?". Re-parses each `definition_text` and reads `cardinality` from
/// its own column, exactly like [`relationship_by_name`].
pub async fn relationships_to_table(
    pool: &Pool,
    to_table: &str,
) -> Result<Vec<RelationshipDefinition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select id, definition_text, cardinality
             from relationship_definitions
             where to_table = $1
             order by id",
            &[&to_table],
        )
        .await?;

    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.get(0);
        let text: String = row.get(1);
        let cardinality_text: String = row.get(2);
        let def = parse_relationship(&text)?;
        let cardinality = RelationshipCardinality::from_persisted(&cardinality_text)
            .unwrap_or_else(|| {
                panic!(
                    "relationship_definitions.cardinality held unrecognized value '{cardinality_text}'"
                )
            });
        result.push(RelationshipDefinition {
            id,
            def,
            cardinality,
            // Creation-time guidance, not a fact about the persisted row — see
            // the field's doc comment on [`RelationshipDefinition`].
            warnings: Vec::new(),
        });
    }
    Ok(result)
}

/// Resolves every relationship a definition's calculated fields reference
/// (issue #40) into the [`ResolvedRelationship`] map [`super::validate`] needs
/// to enforce ADR-0006's reference-time cardinality and type rules. The
/// validator is sync and DB-less, so — exactly like `source_columns` — the
/// caller does the catalog + `pg_catalog` lookups here and passes the result
/// in.
///
/// For each distinct relationship name used in `def` (via
/// [`super::eval::relationship_references`]), looks it up on `def.source` and
/// resolves the type of every to-side column those paths read. A referenced
/// column that doesn't exist on the to-side is a hard
/// [`ValidationError::UnknownRelationshipColumn`] here (ADR-0005: check, don't
/// assume). An unknown relationship *name* is left absent from the map so the
/// validator reports it as [`ValidationError::UnknownRelationship`] against
/// the specific field, rather than this resolver guessing which field to
/// blame.
pub(crate) async fn resolve_relationships(
    pool: &Pool,
    def: &TransformDef,
) -> Result<HashMap<String, ResolvedRelationship>, CatalogError> {
    let mut cols_by_rel: HashMap<String, Vec<String>> = HashMap::new();
    for (rel, column) in super::eval::relationship_references(def) {
        cols_by_rel.entry(rel).or_default().push(column);
    }

    let mut resolved = HashMap::with_capacity(cols_by_rel.len());
    for (rel, columns) in cols_by_rel {
        let Some(reldef) = relationship_by_name(pool, &def.source, &rel).await? else {
            // Unknown name: leave it out; the validator names the offending
            // field in `ValidationError::UnknownRelationship`.
            continue;
        };
        let to_table = reldef.def.to_table.clone();
        let mut column_types = HashMap::with_capacity(columns.len());
        for column in columns {
            let pg_type = column_type(pool, &to_table, &column).await?;
            column_types.insert(column, value_type_from_pg(&pg_type));
        }
        resolved.insert(
            rel,
            ResolvedRelationship {
                cardinality: reldef.cardinality,
                to_table,
                to_col: reldef.def.to_col.clone(),
                column_types,
            },
        );
    }
    Ok(resolved)
}

/// Resolves `source_table`'s actual schema the same way Postgres itself
/// would resolve the bare, unqualified name: the first schema on this
/// connection's `search_path` (`current_schemas(false)`, in `search_path`
/// order) that actually has a table by that name. Mirrors `defctl`'s own
/// `source_columns`/`qualified_source_tables` introspection — see
/// [`create_definition`]'s call site for why this replaced an
/// `is_target`-based guess.
async fn resolve_source_schema_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    source_table: &str,
) -> Result<String, CatalogError> {
    let row = txn
        .query_opt(
            "select table_schema from information_schema.tables \
             where table_name = $1 and table_schema = any(current_schemas(false)) \
             order by array_position(current_schemas(false), table_schema) \
             limit 1",
            &[&source_table],
        )
        .await?;
    row.map(|row| row.get(0))
        .ok_or_else(|| CatalogError::SourceTableNotFound(source_table.to_string()))
}

/// Pooled (non-transaction) counterpart to [`column_type_in_txn`], for
/// resolvers that run before `create_definition` opens its transaction (issue
/// #40's [`resolve_relationships`]). Same query, same
/// [`ValidationError::UnknownRelationshipColumn`] on a missing column.
async fn column_type(pool: &Pool, table: &str, column: &str) -> Result<String, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&table, &column],
        )
        .await?;
    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(ValidationError::UnknownRelationshipColumn {
            table: table.to_string(),
            column: column.to_string(),
        }
        .into()),
    }
}

/// Maps a Postgres `format_type` rendering to the evaluator's [`ValueType`].
/// A copy of `staging::apply`'s same-named helper (the `defs` layer is
/// upstream of `staging`, so it can't reuse it without a backward
/// dependency); keep the two in sync. Anything not clearly numeric, boolean,
/// or uuid is treated as text, the safe verbatim-passthrough default for a
/// to-side enrichment column.
fn value_type_from_pg(pg_type: &str) -> ValueType {
    let base = pg_type.split('(').next().unwrap_or(pg_type).trim();
    match base {
        "uuid" => ValueType::Uuid,
        "boolean" => ValueType::Boolean,
        "smallint" | "integer" | "bigint" | "numeric" | "real" | "double precision" => {
            ValueType::Numeric
        }
        _ => ValueType::Text,
    }
}

/// The Postgres type of `table.column`, as rendered by `format_type`, via a
/// bound `::regclass` cast (matching [`super::ddl::source_primary_key`]'s
/// convention) rather than string-interpolating either name into the query.
/// Distinguishes "the table itself doesn't resolve" from "the table exists
/// but has no such column" only in that both are reported the same way
/// (issue #27 doesn't need the distinction: either one means the endpoint
/// isn't real) — see [`ValidationError::UnknownRelationshipColumn`].
async fn column_type_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
    column: &str,
) -> Result<String, CatalogError> {
    let row = txn
        .query_opt(
            "select pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attname = $2
               and a.attnum > 0
               and not a.attisdropped",
            &[&table, &column],
        )
        .await?;
    match row {
        Some(row) => Ok(row.get(0)),
        None => Err(ValidationError::UnknownRelationshipColumn {
            table: table.to_string(),
            column: column.to_string(),
        }
        .into()),
    }
}

/// Postgres type names that are freely joinable despite not being textually
/// identical — the common case of an identity primary key (`bigint`) and a
/// foreign key column declared as a plain `integer`, or a `text`/`character
/// varying` split between two independently-authored tables. Anything not
/// named here must match `from_type`/`to_type` exactly to be considered
/// comparable; see [`assert_comparable_types`].
///
/// `pg_type` is [`column_type_in_txn`]'s `format_type(atttypid, atttypmod)`
/// rendering, which includes any length/precision modifier (`character
/// varying(255)`, `numeric(10,2)`). The modifier is stripped before bucket
/// matching — otherwise `varchar(255)` and `varchar(100)`, or `text` and
/// `varchar(n)`, would fall into the `other` catch-all as two distinct
/// strings and be wrongly rejected as a type mismatch, even though they're
/// exactly the kind of join this function exists to allow.
fn type_family(pg_type: &str) -> &str {
    let base = pg_type.split('(').next().unwrap_or(pg_type).trim();
    match base {
        "smallint" | "integer" | "bigint" => "integer",
        "numeric" | "real" | "double precision" => "numeric",
        "text" | "character varying" | "character" => "text",
        other => other,
    }
}

/// Postgres type base names (modifier already stripped, as in
/// [`type_family`]) whose equality is *text-stable* — `a::text = b::text`
/// agrees with the type's native typed `=` for every value. This is a
/// positive allowlist, not [`type_family`]'s equivalence-class bucketing:
/// [`type_family`] groups `character`/`character varying`/`text` together
/// (correctly, for comparability) even though `character`'s native `=` is
/// blank-padding-insensitive while its `::text` rendering is blank-padded,
/// so a family-based check would wrongly wave it through here.
///
/// Only the join key's *own* type matters for this list, not what it's
/// compared against, so allowed/rejected status is a per-type fact.
/// Anything not named here — `numeric`/`real`/`double precision`
/// (fractional/arbitrary-precision: `1.0::text` != `1.00::text` though
/// numerically equal), `character`/`citext` (blank-padding or
/// case-insensitivity native to the type but not its `::text` form),
/// `timestamp`/`timestamptz`/`date`/`time` (`::text` is session-TimeZone- or
/// style-dependent), `boolean`, `bytea`, `json`/`jsonb`, or any unknown
/// type — is rejected as a join key.
const TEXT_STABLE_JOIN_KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    "uuid",
    "text",
    "character varying",
];

/// Rejects `def` if either endpoint's join key type isn't in
/// [`TEXT_STABLE_JOIN_KEY_TYPES`] — issue #28 review, hardened per review of
/// #27/#28 (a numeric-only blocklist missed `character(n)`, `citext`, and
/// `timestamptz`, which also diverge under the engine's `::text`-equality
/// join vs. the Postgres oracle's native typed `=`). Checks both sides
/// rather than relying on [`assert_comparable_types`]'s family match to
/// stand in for the other: `character` and `character varying` share a
/// family but only one is on this allowlist, so a from/to pair could
/// straddle the line.
fn assert_join_key_type_supported(
    def: &RelationshipDef,
    from_type: &str,
    to_type: &str,
) -> Result<(), CatalogError> {
    for (table, column, pg_type) in [
        (&def.from_table, &def.from_col, from_type),
        (&def.to_table, &def.to_col, to_type),
    ] {
        let base = pg_type.split('(').next().unwrap_or(pg_type).trim();
        if !TEXT_STABLE_JOIN_KEY_TYPES.contains(&base) {
            return Err(ValidationError::RelationshipUnsupportedJoinKeyType {
                name: def.name.clone(),
                table: table.clone(),
                column: column.clone(),
                pg_type: pg_type.to_string(),
            }
            .into());
        }
    }
    Ok(())
}

/// Rejects `def` if `from_type`/`to_type` (both already resolved by
/// [`column_type_in_txn`]) aren't in the same [`type_family`] — ADR-0006's
/// "type-check the join" requirement.
fn assert_comparable_types(
    def: &RelationshipDef,
    from_type: &str,
    to_type: &str,
) -> Result<(), CatalogError> {
    if type_family(from_type) == type_family(to_type) {
        return Ok(());
    }
    Err(ValidationError::RelationshipTypeMismatch {
        name: def.name.clone(),
        from_table: def.from_table.clone(),
        from_col: def.from_col.clone(),
        from_type: from_type.to_string(),
        to_table: def.to_table.clone(),
        to_col: def.to_col.clone(),
        to_type: to_type.to_string(),
    }
    .into())
}

/// [`RelationshipCardinality::ToOne`] iff `to_col` is the sole column of a
/// `PRIMARY KEY` or `UNIQUE` index on `to_table`, introspected live against
/// `pg_catalog` (`pg_index.indisunique` covers both index kinds; `indkey`'s
/// length excludes any multi-column index `to_col` merely participates in,
/// since that doesn't make `to_col` alone unique) — ADR-0006's cardinality
/// rule. `indisvalid`/`indpred is null` exclude indexes that don't actually
/// guarantee global uniqueness of `to_col`: a not-yet-validated index (e.g.
/// left behind by a failed `CREATE UNIQUE INDEX CONCURRENTLY`) or a partial
/// unique index (`... where active`), which only constrains the rows it
/// covers. Assumes `to_table`/`to_col` already resolved (callers run this
/// after [`column_type_in_txn`] has confirmed both exist).
async fn to_col_cardinality_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    to_table: &str,
    to_col: &str,
) -> Result<RelationshipCardinality, CatalogError> {
    let is_unique: bool = txn
        .query_one(
            "select exists (
                select 1
                from pg_index i
                join pg_attribute a
                  on a.attrelid = i.indrelid and a.attname = $2
                where i.indrelid = pg_catalog.to_regclass($1)
                  and i.indisunique
                  and i.indisvalid
                  and i.indpred is null
                  and array_length(i.indkey::int2[], 1) = 1
                  and i.indkey[0] = a.attnum
             )",
            &[&to_table, &to_col],
        )
        .await?
        .get(0);

    Ok(if is_unique {
        RelationshipCardinality::ToOne
    } else {
        RelationshipCardinality::ToMany
    })
}

/// Rejects a *to-many* relationship (issue #41) whose to-side lacks a replica
/// identity that carries the join column (`to_col`) in row pre-images. For
/// to-many the join key is a *non-PK* column, and the staging reverse-recompute
/// resolver reads it from a DELETE/UPDATE pre-image to find which from-side
/// rows to re-derive. Under the default replica identity (`d`, the primary
/// key) — or none (`n`) — that non-PK column is absent from the pre-image, so
/// a delete or re-parent would silently under-recompute and diverge from the
/// Postgres oracle. Correct only when the to-side has:
/// * `REPLICA IDENTITY FULL` (`relreplident = 'f'`) — every column is in the
///   pre-image; or
/// * `REPLICA IDENTITY USING INDEX` (`relreplident = 'i'`) whose index — the
///   one flagged `pg_index.indisreplident` — includes `to_col` among its
///   columns (`indkey` maps to the attnum of `to_col`).
///
/// Callers invoke this only for [`RelationshipCardinality::ToMany`]; to-one
/// carries the FK in the from-side's own row image and needs no extra replica
/// identity (see ADR-0006). Assumes `to_table`/`to_col` already resolved.
async fn assert_replica_identity_supports_to_many(
    txn: &tokio_postgres::Transaction<'_>,
    def: &RelationshipDef,
) -> Result<(), CatalogError> {
    let adequate: bool = txn
        .query_one(
            "select
                c.relreplident = 'f'
                or (
                    c.relreplident = 'i'
                    and exists (
                        select 1
                        from pg_index i
                        join pg_attribute a
                          on a.attrelid = i.indrelid and a.attname = $2
                        where i.indrelid = c.oid
                          and i.indisreplident
                          and a.attnum = any(i.indkey::int2[])
                    )
                )
             from pg_class c
             where c.oid = pg_catalog.to_regclass($1)",
            &[&def.to_table, &def.to_col],
        )
        .await?
        .get(0);

    if adequate {
        Ok(())
    } else {
        Err(ValidationError::RelationshipToManyRequiresReplicaIdentity {
            name: def.name.clone(),
            to_table: def.to_table.clone(),
            to_col: def.to_col.clone(),
        }
        .into())
    }
}

/// Rejects an aggregate (`GROUP BY`) definition (issue #47) whose source
/// table's replica identity doesn't guarantee an old row image on
/// delete/update. Unlike [`assert_replica_identity_supports_to_many`]'s
/// to-many relationship check — which only needs one non-PK join column
/// (`to_col`) present in the pre-image, and so accepts a covering
/// `REPLICA IDENTITY USING INDEX` — an aggregate's delta maintenance
/// (`apply_aggregate.rs`'s `accumulate_changes`) needs the *entire* old row:
/// every `GROUP BY` column (to find which group a deleted/re-parented row
/// was decrementing) and every column any `SUM`/`AVG`/`MIN`/`MAX` field
/// reads (to subtract its old contribution). Only `REPLICA IDENTITY FULL`
/// (`relreplident = 'f'`) guarantees that for an arbitrary set of columns, so
/// this doesn't attempt the narrower per-column index check the to-many path
/// does.
///
/// Delegates the actual rejection to
/// [`crate::intake::require_replica_identity_full`] (issue #7's scaffolding,
/// previously unwired — see its module doc) so the error text — including
/// the exact `ALTER TABLE ... REPLICA IDENTITY FULL;` statement — comes from
/// one place rather than being duplicated here. That function's own
/// `needs_old_image` parameter is unconditional (it rejects whenever passed
/// `true`, regardless of the table's actual replica identity), so it is not
/// enough on its own — [`crate::intake::needs_old_image`] would always
/// return `true` for [`KeySpace::Aggregate`], rejecting every aggregate
/// definition forever, even after an operator runs the suggested `ALTER
/// TABLE`. This function closes that gap by querying `pg_class.relreplident`
/// itself first and only passing `true` through when the source table is
/// actually inadequate today.
async fn assert_replica_identity_supports_aggregate(
    txn: &tokio_postgres::Transaction<'_>,
    def: &TransformDef,
) -> Result<(), CatalogError> {
    let is_full: bool = txn
        .query_one(
            "select relreplident = 'f' from pg_class where oid = pg_catalog.to_regclass($1)",
            &[&def.source],
        )
        .await?
        .get(0);

    crate::intake::require_replica_identity_full(&def.source, !is_full)
        .map_err(CatalogError::ReplicaIdentityRequired)
}

/// Whether `from_table` has a usable index for looking up rows by
/// `from_col` (issue #31) — the query reverse propagation runs when a
/// related `to_table` row changes (ADR-0006). "Usable" means a `btree`
/// index whose *leading* column is `from_col`: a plain `where from_col =
/// $1` lookup can use such an index regardless of what other columns
/// follow it, so — unlike [`to_col_cardinality_in_txn`]'s uniqueness check —
/// this doesn't require `from_col` to be the index's only column.
///
/// Excludes indexes that can't be trusted for this lookup:
/// * `indisvalid` — a not-yet-validated index (e.g. left behind by a failed
///   `CREATE INDEX CONCURRENTLY`) isn't usable yet.
/// * `am.amname = 'btree'` — other access methods (`gin`, `brin`, `hash`)
///   either don't support this leading-column equality lookup the way
///   btree does, or aren't worth special-casing for what's only a
///   performance hint.
/// * `indexprs is null` — an expression index's leading "column" isn't a
///   plain column reference, so `indkey[0]` is `0` and never matches a real
///   `attnum`; this is already excluded by the `indkey[0] = a.attnum` join
///   condition, called out here since it's not obvious from the SQL alone.
/// * `indpred is null` — a partial index only covers the rows satisfying
///   its predicate, so the planner won't use it for an unqualified
///   `from_col = $1` lookup across all rows; same exclusion
///   [`to_col_cardinality_in_txn`] applies for uniqueness, for the same
///   reason.
///
/// Never issues DDL — this only informs the caller's decision to emit
/// [`RelationshipWarning::MissingFkIndex`] (ADR-0005: Trellis never modifies
/// the source schema).
async fn has_usable_fk_index_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    from_table: &str,
    from_col: &str,
) -> Result<bool, CatalogError> {
    let has_index: bool = txn
        .query_one(
            "select exists (
                select 1
                from pg_index i
                join pg_attribute a
                  on a.attrelid = i.indrelid and a.attname = $2
                join pg_class ic on ic.oid = i.indexrelid
                join pg_am am on am.oid = ic.relam
                where i.indrelid = pg_catalog.to_regclass($1)
                  and i.indisvalid
                  and i.indpred is null
                  and am.amname = 'btree'
                  and i.indkey[0] = a.attnum
             )",
            &[&from_table, &from_col],
        )
        .await?
        .get(0);
    Ok(has_index)
}

/// Resolves `table_name` to its [`SchemaNode`], creating one if this is the
/// first time Trellis has seen the table and setting its `kind` role flag
/// (`is_source`/`is_target`) to `true`. Idempotent, and additive across
/// roles: resolving the same table under both [`NodeKind::Source`] and
/// [`NodeKind::Target`] over separate calls (chained/multi-hop transforms —
/// see [`super::model::NodeKind`]'s doc comment) merges into one node with
/// both flags set, rather than erroring.
///
/// Runs in its own transaction; [`create_definition`] instead calls
/// [`resolve_node_in_txn`] directly so both of a definition's node
/// resolutions land in the same transaction as the definition write.
pub async fn resolve_node(
    pool: &Pool,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let node = resolve_node_in_txn(&txn, table_name, kind).await?;
    txn.commit().await?;
    Ok(node)
}

/// The transactional core of [`resolve_node`] — see its doc comment.
/// Upserts `table_name`, OR-ing `kind`'s role flag into whatever the row
/// already has (or defaulting the other flag `false` if the row is new).
async fn resolve_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let (is_source, is_target) = match kind {
        NodeKind::Source => (true, false),
        NodeKind::Target => (false, true),
    };

    let row = txn
        .query_one(
            "insert into schema_nodes (table_name, is_source, is_target)
             values ($1, $2, $3)
             on conflict (table_name) do update
                set is_source = schema_nodes.is_source or excluded.is_source,
                    is_target = schema_nodes.is_target or excluded.is_target
             returning id, is_source, is_target",
            &[&table_name, &is_source, &is_target],
        )
        .await?;

    Ok(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    })
}

/// Pool-level wrapper over [`persist_edge_in_txn`], mirroring
/// [`resolve_node`]'s relationship to [`resolve_node_in_txn`]. Exists so the
/// `on conflict do nothing` dedup path on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint has direct test
/// coverage — [`create_definition`] can never hit it itself, since
/// `transform_definitions.target_table` is unique and so no two definitions
/// can ever resolve to the same `(from_node_id, to_node_id)` pair.
pub async fn persist_edge(
    pool: &Pool,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    persist_edge_in_txn(&txn, from_node_id, to_node_id, kind).await?;
    txn.commit().await?;
    Ok(())
}

/// Records that `to_node_id` depends on `from_node_id` via `kind` — the
/// transactional core [`create_definition`] calls for a transform's `FROM`
/// edge. `on conflict do nothing` on `schema_edges`'s
/// `(from_node_id, to_node_id, kind)` uniqueness constraint makes
/// re-declaring the same transform's edge idempotent (definitions are
/// immutable, but nothing stops the same source/target pair from being
/// resolved through this path more than once as the graph grows).
async fn persist_edge_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    from_node_id: i64,
    to_node_id: i64,
    kind: EdgeKind,
) -> Result<(), CatalogError> {
    txn.execute(
        "insert into schema_edges (from_node_id, to_node_id, kind)
         values ($1, $2, $3)
         on conflict (from_node_id, to_node_id, kind) do nothing",
        &[&from_node_id, &to_node_id, &kind.as_str()],
    )
    .await?;
    Ok(())
}

/// Rejects a definition whose `Source` edge — `source_table -> target_table`
/// — would close a cycle in the table-level dependency graph: this holds
/// iff `target_table` can already reach `source_table` through some path of
/// existing [`super::model::SchemaEdge`]s (of any kind — see
/// [`create_definition`]'s call site for why this stays kind-generic).
/// Walks the transaction's current `schema_edges`/`schema_nodes` state as a
/// small in-memory adjacency map, hand-rolled DFS, matching
/// [`super::validate::detect_cycle`]'s column-level convention rather than
/// pulling in a graph crate for a problem this small.
async fn reject_if_table_cycle(
    txn: &tokio_postgres::Transaction<'_>,
    source_table: &str,
    target_table: &str,
) -> Result<(), CatalogError> {
    let rows = txn
        .query(
            "select from_node.table_name, to_node.table_name
             from schema_edges se
             join schema_nodes from_node on from_node.id = se.from_node_id
             join schema_nodes to_node on to_node.id = se.to_node_id",
            &[],
        )
        .await?;

    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let from: String = row.get(0);
        let to: String = row.get(1);
        adjacency.entry(from).or_default().push(to);
    }

    if let Some(mut path) = find_table_path(&adjacency, target_table, source_table) {
        // `path` is target_table -> ... -> source_table; appending
        // target_table closes the loop the new source_table -> target_table
        // edge would create, for a message naming the whole cycle.
        path.push(target_table.to_string());
        return Err(ValidationError::TableCycle { cycle: path }.into());
    }
    Ok(())
}

/// DFS from `from` to `to` over `adjacency`, returning the path (inclusive
/// of both ends) if one exists.
fn find_table_path(
    adjacency: &HashMap<String, Vec<String>>,
    from: &str,
    to: &str,
) -> Option<Vec<String>> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut path: Vec<String> = Vec::new();
    if find_table_path_from(adjacency, from, to, &mut visited, &mut path) {
        Some(path)
    } else {
        None
    }
}

fn find_table_path_from(
    adjacency: &HashMap<String, Vec<String>>,
    node: &str,
    to: &str,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
) -> bool {
    path.push(node.to_string());
    if node == to {
        return true;
    }
    visited.insert(node.to_string());
    if let Some(neighbors) = adjacency.get(node) {
        for neighbor in neighbors {
            if !visited.contains(neighbor)
                && find_table_path_from(adjacency, neighbor, to, visited, path)
            {
                return true;
            }
        }
    }
    path.pop();
    false
}

/// The [`SchemaNode`] already resolved for `table_name`, or `None` if
/// nothing has ever referenced it as a source or a target.
pub async fn node_for_table(
    pool: &Pool,
    table_name: &str,
) -> Result<Option<SchemaNode>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, is_source, is_target from schema_nodes where table_name = $1",
            &[&table_name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    Ok(Some(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    }))
}

/// The [`SchemaEdge`]s of `kind` directed away from `node_table` — a
/// generalized, `transform_definitions`-agnostic sibling of
/// [`dependents_of`] for edge kinds whose dependents don't join back into
/// `transform_definitions` (e.g. [`EdgeKind::Relationship`], whose
/// dependents are `relationship_definitions` rows, read separately via
/// [`relationship_by_name`]). Returns raw edges rather than joining onto any
/// definition table, so it works for any [`EdgeKind`] without needing a
/// kind-specific query.
pub async fn edges_from(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<SchemaEdge>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select se.id, se.from_node_id, se.to_node_id
             from schema_edges se
             join schema_nodes from_node on from_node.id = se.from_node_id
             where from_node.table_name = $1 and se.kind = $2
             order by se.id",
            &[&node_table, &kind.as_str()],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| SchemaEdge {
            id: row.get(0),
            from_node_id: row.get(1),
            to_node_id: row.get(2),
            kind,
        })
        .collect())
}

/// Splits `source_columns` into the parallel key/value text arrays
/// `jsonb_object`'s two-array form wants (see `create_definition`'s insert
/// and [`transforms_for_source`]'s matching read side).
fn encode_type_map(source_columns: &HashMap<String, ValueType>) -> (Vec<&str>, Vec<&'static str>) {
    let mut keys = Vec::with_capacity(source_columns.len());
    let mut vals = Vec::with_capacity(source_columns.len());
    for (name, value_type) in source_columns {
        keys.push(name.as_str());
        vals.push(match value_type {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        });
    }
    (keys, vals)
}

/// One definition row's non-`source_columns` fields, accumulated while
/// [`transforms_for_source`] walks its single, lateral-joined query — see
/// that function's doc comment.
struct PendingDefinition {
    source_version: i64,
    text: String,
    source_columns: HashMap<String, ValueType>,
}

/// The transform definitions that depend on `node_table` via a `kind` edge
/// in the persisted dependency graph (issue #21) — e.g. `EdgeKind::Source`
/// answers "what reads from `node_table` as its `FROM`". Walking
/// `schema_edges` (rather than matching on
/// `transform_definitions.source_table` string equality) is what makes this
/// a real graph lookup: multi-hop chains resolve by calling this again with
/// a dependent's target table, not by any special-casing here.
///
/// One query, not one-per-definition-row (issue #69): a `left join lateral
/// jsonb_each_text(...)` unnests every dependent definition's persisted
/// `source_columns` map inline, so this is still "decode JSON via SQL, no
/// serde_json dependency" — matching `staging::apply::decode_image`'s
/// convention — just decoded for every row in one round trip instead of one
/// per definition. The `left join` (rather than an inner join/`cross join
/// lateral`) matters: a definition whose `source_columns` is `{}` must still
/// come back with zero entries, not disappear from the result entirely.
pub async fn dependents_of(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, e.key, e.value
             from schema_nodes from_node
             join schema_edges se on se.from_node_id = from_node.id and se.kind = $2
             join schema_nodes to_node on to_node.id = se.to_node_id
             join transform_definitions t on t.target_table = to_node.table_name
             left join lateral jsonb_each_text(t.source_columns) e on true
             where from_node.table_name = $1
             order by t.id",
            &[&node_table, &kind.as_str()],
        )
        .await?;

    // `order` preserves the query's `order by t.id` across the group-by
    // done in Rust below (a plain `HashMap` has no ordering of its own).
    let mut order: Vec<i64> = Vec::new();
    let mut by_id: HashMap<i64, PendingDefinition> = HashMap::new();

    for row in rows {
        let id: i64 = row.get(0);
        let key: Option<String> = row.get(3);
        let value: Option<String> = row.get(4);

        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            PendingDefinition {
                source_version: row.get(1),
                text: row.get(2),
                source_columns: HashMap::new(),
            }
        });

        if let (Some(key), Some(value)) = (key, value) {
            let value_type = match value.as_str() {
                "numeric" => ValueType::Numeric,
                "text" => ValueType::Text,
                "boolean" => ValueType::Boolean,
                "uuid" => ValueType::Uuid,
                other => {
                    return Err(CatalogError::UnknownValueType {
                        column: key,
                        text: other.to_string(),
                    });
                }
            };
            pending.source_columns.insert(key, value_type);
        }
    }

    let mut result = Vec::with_capacity(order.len());
    for id in order {
        let pending = by_id.remove(&id).expect("id was just pushed to order");
        let def = parse(&pending.text)?;
        result.push(Definition {
            id,
            source_version: pending.source_version,
            def,
            source_columns: pending.source_columns,
        });
    }
    Ok(result)
}

/// The transform definitions currently subscribed to `source_table` — the
/// mapping intake (#7/#8) will use to decide what to subscribe to. A thin
/// wrapper over [`dependents_of`] filtered to [`EdgeKind::Source`], the only
/// edge kind persisted today.
pub async fn transforms_for_source(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<Definition>, CatalogError> {
    dependents_of(pool, source_table, EdgeKind::Source).await
}

/// Every table that needs CDC capture for at least one registered transform
/// (issue #65): every distinct anchor `source_table` (as stored — see
/// [`create_definition`]'s `def.source`), plus every table transitively
/// reachable from one of those anchors by following
/// `relationship_definitions.from_table -> to_table` edges. A calculated
/// field on a transform anchored at `from_table` can read a relationship
/// path into `to_table` (and, through a chained relationship declared with
/// `to_table` as its own `from_table`, into a table beyond that), so
/// `to_table` must be in the CDC publication too, even though no transform
/// is anchored there directly — see the issue for the silently-dropped-write
/// bug this closes.
///
/// The recursive CTE below seeds the set with the same anchor tables the
/// pre-#65 query returned, then unions in each edge's `to_table` reached
/// from a table already in the set, transitively. `union` (not `union all`)
/// is required, not just tidy: Postgres's recursive-query dedup compares
/// each new candidate row against every row already in the accumulated
/// result and drops it if already present, so a relationship cycle (`to_table`
/// eventually looping back to an ancestor `from_table`) can only ever
/// propose table names already in the set — the recursion adds nothing new
/// on that iteration and terminates, rather than looping forever. A
/// relationship declared on a table that never anchors a registered
/// transform never seeds the recursion, so its `to_table` correctly never
/// appears (issue #65's test case 4).
///
/// Issue #14: a running [`crate::Client`]'s maintenance loop polls this to
/// notice a transform (or now, a relationship reachable from one) registered
/// against a table it hasn't seen before, so it can add that table to the
/// publication and discharge its backfill without waiting for a restart.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "with recursive reachable(table_name) as (
                select distinct source_table from transform_definitions
                union
                select rd.to_table
                from relationship_definitions rd
                join reachable r on r.table_name = rd.from_table
             )
             select table_name from reachable",
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// The current version of `source_table`, or `None` if no definition has
/// ever been created against it.
pub async fn source_table_version(
    pool: &Pool,
    source_table: &str,
) -> Result<Option<i64>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select version from source_table_versions where source_table = $1",
            &[&source_table],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}
