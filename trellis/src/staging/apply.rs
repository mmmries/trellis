//! Apply ∪ mark-drained (issue #11, stage 05) — the 1-1/scalar subset only.
//! See docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md.
//!
//! **Scope**: only [`crate::defs::ast::KeySpace::OneToOne`] definitions.
//! The aggregate delta model (groups, invertibility, min/max recompute,
//! composite partials, grain migration) is out of scope — it's blocked on
//! aggregate transform-defs, which don't exist yet.
//!
//! The design's three phases map onto three functions:
//!
//! - Phase 1 (claim + fold, one short transaction) is [`drain_once`]'s own
//!   opening block, reusing [`super::claim::claim`],
//!   [`super::claim::owned_bucket_filter`], and [`super::fold::fold`]
//!   directly — there is nothing 1-1-specific about claiming or folding, so
//!   this module adds no wrapper around them.
//! - Phase 2 (compute: evaluate `f()` against every folded change, no
//!   transaction, no locks) is [`compute`].
//! - Phase 3 (apply ∪ mark-drained, one transaction) is
//!   [`apply_and_mark_drained`].
//!
//! [`drain_once`] is the orchestrator tying the three together, including
//! the version-fence/serialization retry loop the design calls for.
//! [`next_claimable_segment`] is the "which batch should a free worker pick
//! up next" query a real drain loop (not assembled here — see doc 04's
//! [`super::liveness::claim_unless_paused`]) would call before it.

use std::collections::HashMap;
use std::fmt;

use tokio_postgres::types::ToSql;
use tokio_postgres::{GenericClient, Transaction};

use crate::defs::ast::{KeySpace, TransformDef, ValueType};
use crate::defs::catalog::{self, CatalogError};
use crate::defs::ddl::{self, DdlError, PrimaryKeyColumn};
use crate::defs::eval::{
    self, EvalError, RelationshipContext, Row, ToManyRelationship, ToOneRelationship,
};
use crate::defs::model::RelationshipCardinality;
use crate::defs::validate::{self, ValidationError};
use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};

use super::append::{self, StagedChange};
use super::apply_aggregate::{self, AggregateTargetPlan};
use super::claim;
use super::error::StagingError;
use super::fold::{self, FoldedChange};
use super::liveness::FenceMissBackoff;
use super::quarantine;

/// The absolute ceiling on [`FoldedChange::hop_gen`] propagation, a backstop
/// over and above the schema-derived hop bound doc 05 describes ("one past
/// its deepest trigger"): even if the catalog's own graph analysis is wrong
/// or a definition cycle somehow reaches this stage, propagation cannot
/// wind up more than this many hops before [`ApplyError::HopBoundExceeded`]
/// stops it. `defs::validate` already rejects definition cycles at
/// creation time (`detect_cycle`), so this is defense-in-depth, not the
/// primary guard.
pub const MAX_HOP_GEN: i32 = 32;

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Why a drain attempt (fold, compute, or apply ∪ mark-drained) failed. A
/// module-local enum, plain `Display` + `std::error::Error`, composing with
/// the crate's other error types via `From` — matching
/// [`StagingError`]/[`CatalogError`]/[`DdlError`]/[`EvalError`]'s own
/// convention.
#[derive(Debug)]
pub enum ApplyError {
    /// A failure from the staging ring (claim, fold, append).
    Staging(StagingError),
    /// A failure reading the transform catalog.
    Catalog(CatalogError),
    /// A failure introspecting a source table's primary key.
    Ddl(DdlError),
    /// A failure evaluating a definition's calculated fields.
    Eval(EvalError),
    /// A definition's calculated fields failed to type-infer against its own
    /// persisted `source_columns` — meaning a definition that already passed
    /// [`crate::defs::validate::validate`] at creation time no longer
    /// type-checks against the map it was created with, which should not be
    /// reachable; kept as a typed error rather than a panic per this
    /// module's own convention of not trusting invariants it cannot enforce
    /// itself.
    Validate(ValidationError),
    /// Substituting a `GROUP BY` definition's cross-field-alias references
    /// (`defs::backfill::substituted_field_exprs`) failed — a cyclic alias
    /// chain or a pathologically large expansion. Both are rejected by
    /// [`crate::defs::validate::validate`] (a real cycle) or bounded at
    /// definition-creation time (the direct backfill's node budget) before a
    /// definition can ever reach live apply, so this should not be reachable
    /// for a definition that already passed backfill at creation time — kept
    /// as a typed error rather than a panic per this module's convention of
    /// not trusting invariants it cannot enforce itself.
    Backfill(crate::defs::backfill::BackfillError),
    /// A direct Postgres protocol/query error, for statements this module
    /// runs itself (the version fence, the per-target apply statement, the
    /// completion statement) rather than through another module's helper.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// The completion statement's `DELETE FROM seg_claims ... RETURNING
    /// bucket` matched no rows: this worker's claim was gone by the time
    /// Phase 3 tried to complete it (reclaimed on TTL, or raced by another
    /// worker). Nothing was applied twice — Phase 3 is one transaction and
    /// this is checked before it commits — but the caller must not treat
    /// its work as done; the buckets it thought it owned need reclaiming by
    /// whoever holds them now.
    ClaimLost,
    /// Phase 3's version fence found `source_table_versions.version` had
    /// moved since Phase 2 loaded it: a definition change landed on
    /// `src_table` mid-drain. Routine and immediately retryable — see
    /// [`super::liveness::release`]'s doc comment on why a fence miss is
    /// not parked behind the reclaim TTL.
    VersionFenceMiss { src_table: String },
    /// Downstream propagation would have staged a `Recompute` row past
    /// [`MAX_HOP_GEN`]. Named rather than silently truncated: an operator
    /// needs to know a wave ran away, and which target tables it ran away
    /// through, rather than have the tail of it quietly disappear.
    HopBoundExceeded { hop_gen: i32, tables: Vec<String> },
    /// A folded record names a source table Postgres no longer has
    /// (`42P01` from a live query against it) — issue #16's "one sanctioned
    /// exception to immutability": no retry or per-key quarantine can
    /// resolve this, since the table itself is gone, not any one row.
    /// [`drain_once`] routes this to [`quarantine::purge_dropped_table`]
    /// rather than the ordinary isolate/evict path.
    SourceTableDropped { source_table: String },
    /// [`super::quarantine::resume_column`] (or, indirectly,
    /// [`crate::app::Trellis::resume_column`]) was asked to resume a
    /// `(transform, column)` pair with no currently-paused `column_status`
    /// row — resuming a column that isn't paused is caller error, not a
    /// silent no-op. Also reused for "no such column on this definition at
    /// all," so an address naming a real transform but the wrong field name
    /// gets a specific error rather than silently doing nothing.
    ColumnNotPaused { transform: String, column: String },
    /// [`super::quarantine::resume_column`] was asked to resume a column
    /// whose owning definition is not currently [`crate::defs::model::TransformStatus::Live`]
    /// — most concretely, a definition still `Backfilling` behind an
    /// in-flight `backfill_chunks` queue nothing is draining. `resume_column`
    /// takes one snapshot of the *source* table and only clears
    /// `column_status` after writing it back, so any row a still-running
    /// backfill chunk inserts into the target *during* that window is never
    /// in the snapshot and never revisited once the column is unpaused —
    /// permanently stranding that row's column at NULL/default while
    /// `resume_column` reports success. This branch's cascade pause
    /// (`defs::catalog::column_dependents`, unlike the `status = 'live'`
    /// filtered paths CDC apply uses) can reach a downstream definition in
    /// exactly this state, so the gate is not just theoretical. Refusing to
    /// resume until the definition reaches `Live` closes the window instead
    /// of racing it.
    DefinitionNotLive { transform: String },
}

impl ApplyError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here, so the mapping composes rather than re-deriving a
    /// category this crate already has one for.
    /// [`ApplyError::SourceTableDropped`] names a source table that no
    /// longer exists -> [`ErrorCode::NotFound`]; [`ApplyError::ClaimLost`],
    /// [`ApplyError::VersionFenceMiss`], and [`ApplyError::HopBoundExceeded`]
    /// are all internal drain-mechanics conditions the caller can't act on
    /// beyond "retry" -> [`ErrorCode::Internal`].
    pub fn code(&self) -> ErrorCode {
        match self {
            ApplyError::Staging(err) => err.code(),
            ApplyError::Catalog(err) => err.code(),
            ApplyError::Ddl(err) => err.code(),
            ApplyError::Eval(err) => err.code(),
            ApplyError::Validate(err) => err.code(),
            ApplyError::Backfill(err) => err.code(),
            ApplyError::Db(err) => error_code::classify_pg_error(err),
            ApplyError::Pool(err) => err.code(),
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::HopBoundExceeded { .. } => ErrorCode::Internal,
            ApplyError::SourceTableDropped { .. } => ErrorCode::NotFound,
            ApplyError::ColumnNotPaused { .. } => ErrorCode::NotFound,
            // The definition's persisted status conflicts with what
            // `resume_column` was asked to do, the same category
            // `ValidationError::DuplicateRelationshipName` and
            // `StagingError::ProducerAlreadyRunning` use for "existing state
            // blocks this request" rather than "the request itself is
            // malformed" (-> Validation) or "nothing by that name exists"
            // (-> NotFound).
            ApplyError::DefinitionNotLive { .. } => ErrorCode::Conflict,
        }
    }
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplyError::Staging(err) => write!(f, "staging ring error: {err}"),
            ApplyError::Catalog(err) => write!(f, "transform catalog error: {err}"),
            ApplyError::Ddl(err) => write!(f, "target-table DDL error: {err}"),
            ApplyError::Eval(err) => write!(f, "calculated-field evaluation error: {err}"),
            ApplyError::Validate(err) => {
                write!(f, "calculated-field type inference error: {err}")
            }
            ApplyError::Backfill(err) => {
                write!(f, "calculated-field alias substitution error: {err}")
            }
            ApplyError::Db(err) => {
                write!(f, "apply database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            ApplyError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            ApplyError::ClaimLost => write!(
                f,
                "this worker's claim was gone by completion time; nothing was applied twice, \
                 but the buckets it thought it owned must be reclaimed by whoever holds them now"
            ),
            ApplyError::VersionFenceMiss { src_table } => write!(
                f,
                "source table '{src_table}' changed definitions mid-drain; retry against the \
                 current catalog"
            ),
            ApplyError::HopBoundExceeded { hop_gen, tables } => write!(
                f,
                "downstream propagation exceeded the hop bound (hop_gen {hop_gen} > \
                 {MAX_HOP_GEN}) through: {tables:?}"
            ),
            ApplyError::SourceTableDropped { source_table } => write!(
                f,
                "source table '{source_table}' no longer exists; purging its staged rows"
            ),
            ApplyError::ColumnNotPaused { transform, column } => write!(
                f,
                "'{transform}.{column}' is not currently paused (or is not a column of that \
                 definition)"
            ),
            ApplyError::DefinitionNotLive { transform } => write!(
                f,
                "'{transform}' is not currently live (it may still be backfilling); resuming a \
                 paused column requires its definition to be live first"
            ),
        }
    }
}

impl std::error::Error for ApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApplyError::Staging(err) => Some(err),
            ApplyError::Catalog(err) => Some(err),
            ApplyError::Ddl(err) => Some(err),
            ApplyError::Eval(err) => Some(err),
            ApplyError::Validate(err) => Some(err),
            ApplyError::Backfill(err) => Some(err),
            ApplyError::Db(err) => Some(err),
            ApplyError::Pool(err) => Some(err),
            ApplyError::ClaimLost
            | ApplyError::VersionFenceMiss { .. }
            | ApplyError::HopBoundExceeded { .. }
            | ApplyError::SourceTableDropped { .. }
            | ApplyError::ColumnNotPaused { .. }
            | ApplyError::DefinitionNotLive { .. } => None,
        }
    }
}

impl From<StagingError> for ApplyError {
    fn from(err: StagingError) -> Self {
        ApplyError::Staging(err)
    }
}

impl From<CatalogError> for ApplyError {
    fn from(err: CatalogError) -> Self {
        ApplyError::Catalog(err)
    }
}

impl From<DdlError> for ApplyError {
    fn from(err: DdlError) -> Self {
        ApplyError::Ddl(err)
    }
}

impl From<EvalError> for ApplyError {
    fn from(err: EvalError) -> Self {
        ApplyError::Eval(err)
    }
}

impl From<ValidationError> for ApplyError {
    fn from(err: ValidationError) -> Self {
        ApplyError::Validate(err)
    }
}

impl From<crate::defs::backfill::BackfillError> for ApplyError {
    fn from(err: crate::defs::backfill::BackfillError) -> Self {
        ApplyError::Backfill(err)
    }
}

impl From<tokio_postgres::Error> for ApplyError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApplyError::Db(err)
    }
}

impl From<crate::error::Error> for ApplyError {
    fn from(err: crate::error::Error) -> Self {
        ApplyError::Pool(err)
    }
}

// ---------------------------------------------------------------------
// src_table qualification
// ---------------------------------------------------------------------

/// The catalog's lookup key for a folded record's `src_table`: everything
/// after the last `.`, if any.
///
/// A definition's `def.source` is always a *bare* table name — even once
/// issue #76 taught the grammar's `TRANSFORM ... FROM <table>` clause an
/// explicit `schema.table` spelling, `def.source` itself still only ever
/// holds the bare table part (see `defs::ast::TransformDef`'s own doc
/// comment for why; `defs::parser`'s grammar and `defs/mod.rs`'s own tests
/// cover both the bare and explicitly-qualified parses). CDC intake's
/// own producer, though, always stages changes under the qualified
/// `"schema.table"` shape `intake::publication::qualify` builds, which
/// [`FoldedChange::src_table`] inherits directly from the ring. This is the
/// one seam that reconciles the two conventions: strip a schema prefix
/// before ever asking the catalog about a folded record's source.
///
/// Note this produces a *bare* key even though, as of issue #72,
/// `transform_definitions.source_table`/`source_table_versions.source_table`
/// themselves now persist the fully-qualified form — those columns' own
/// read sites (e.g. [`crate::defs::source_table_version`]) match against
/// their bare table-name suffix precisely so this function's output, and
/// every internal key this whole apply path builds from it (`by_source`,
/// `ApplyPlan::versions`, etc.), can stay unchanged rather than needing this
/// hot path to thread real schema identity through. See
/// [`crate::defs::source_table_version`]'s doc comment for the full
/// bare-vs-qualified rationale — including why issue #73 (which also
/// persists `transform_definitions.target_table` qualified) does *not*
/// retire this stripping: a target table's own downstream `src_table` (the
/// `Recompute` rows this module stages) is still unqualified —
/// [`crate::defs::ddl::neighbor_table_name`] deliberately never adds a
/// schema, issue #73 or not — so stripping remains a no-op for that case,
/// exactly as before #72, rather than becoming a stable identity function
/// this call site could now skip outright. Retiring the split entirely (by
/// qualifying every emitted `src_table`, `Recompute` rows included) is issue
/// #75's emission-audit territory.
///
/// This function's output stays purely a *lookup key* (issue #76's own
/// reviewer follow-up): every catalog read below it (`source_table_version`,
/// `transforms_for_source`, `relationships_to_table`) keeps using this bare
/// form, matching the bare-suffix indexes those tables are keyed on. The
/// *physical* SQL builders that actually read a live source row
/// (`ddl::source_primary_key`, [`read_live_rows_batch`], the source string
/// embedded in an [`AggregateTargetPlan`]) use the qualified
/// `change.src_table` each bucket's own changes already carry instead — see
/// `compute`'s `by_source` loop — never this bare key, so a same-named table
/// in a different schema can't make one of those builders read the wrong
/// physical relation.
fn catalog_source_key(src_table: &str) -> &str {
    match src_table.rsplit_once('.') {
        Some((_, table)) => table,
        None => src_table,
    }
}

/// Resolves `src_table` to the fully-qualified identity
/// [`catalog::transforms_for_source`]/[`catalog::dependents_of`] now require
/// (issue #74, ADR-0007: `schema_nodes` keys on qualified identity, so a
/// bare lookup there silently finds nothing rather than erroring).
///
/// A no-op for the common case — `src_table` already contains a `.` — which
/// covers every real CDC-staged or backfill-enumerated change (issue #76
/// qualifies `change.src_table` unconditionally at the point it's staged).
/// Two different shapes of bare `src_table` reach this function, needing
/// two different resolutions — both handled by delegating to
/// [`catalog::resolve_graph_identity`] rather than this function choosing
/// between them itself:
///
/// 1. A downstream `Recompute` trigger *this apply path itself* staged for
///    a chained definition's target (`compute`'s "Downstream propagation"
///    step, `apply_and_mark_drained`), carrying the plain, bare
///    `def.def.target` as its `src_table` (qualifying every such row at the
///    point it's staged is issue #75's emission-audit territory, not this
///    one's — see `catalog_source_key`'s own doc comment on the same
///    deliberate-bare convention). This is `resolve_graph_identity`'s
///    bare-target-suffix fallback: the name can only be some other live
///    definition's own target.
/// 2. A reverse-recompute trigger for a relationship's from-side
///    (`from_side_keys_for_join`/`from_side_keys_with_non_null_join`'s
///    callers below, staging `rel.def.from_table` as `src_table`) —
///    `relationship_definitions.from_table` is always bare (ADR-0007's
///    "Scope" section leaves relationship endpoints unqualified) and is a
///    genuine *source* table, never anyone's target, so the bare-target-
///    suffix fallback above would never find it. This is
///    `resolve_graph_identity`'s *first* step instead: a plain physical
///    `search_path` lookup, exactly like resolving a fresh definition's own
///    bare `FROM`.
async fn qualified_schema_node_key(pool: &Pool, src_table: &str) -> Result<String, ApplyError> {
    if src_table.contains('.') {
        return Ok(src_table.to_string());
    }
    Ok(catalog::resolve_graph_identity(pool, src_table).await?)
}

/// Decodes a staged jsonb image (bound as text — this crate has no
/// `serde_json` dependency, matching `append.rs`/`fold.rs`'s convention)
/// into a [`Row`] via `jsonb_each_text`, so the evaluator never has to
/// parse JSON itself. A JSON `null` value decodes to `None`, matching
/// `Row`'s "absent column" vs. "present but NULL" distinction the evaluator
/// depends on (`eval.rs`'s `MissingColumn` vs. plain `None` propagation).
/// The intermediate `::text` cast matters, same as `append.rs`: `$1::jsonb`
/// alone makes Postgres describe the placeholder as `jsonb`, which
/// `&str`'s `ToSql` rejects before the value is ever sent.
async fn decode_image(pool: &Pool, image_text: &str) -> Result<Row, ApplyError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select key, value from jsonb_each_text($1::text::jsonb)",
            &[&image_text],
        )
        .await?;
    let mut row = Row::with_capacity(rows.len());
    for r in rows {
        let key: String = r.get(0);
        let value: Option<String> = r.get(1);
        row.insert(key, value);
    }
    Ok(row)
}

/// Re-reads every one of `keys`' current rows from `source_table` live, in
/// one round trip, for folded changes that carried no image at all (see
/// [`compute`]'s doc comment on the three shapes) — the batched replacement
/// for what used to be one `read_live_row` round trip per key (issue #13: a
/// backfill's initial enumeration stages every pre-existing row as exactly
/// this shape, so a naive per-key refetch made backfill throughput scale
/// with source table size in network round trips, not rows).
///
/// The `jsonb_each_text` unnest happens in the same query as the `any($1)`
/// row lookup — a `cross join lateral`, one column per matched row — so
/// decoding costs no extra round trip either; [`decode_image`]'s per-image
/// query is only paid for images that arrive already staged (`new_image`),
/// never for a live refetch. A key absent from the returned map means its
/// row is gone (already deleted, or never existed), which [`compute`]
/// treats as a delete, matching `read_live_row`'s old `None` case exactly.
async fn read_live_rows_batch(
    pool: &Pool,
    source_table: &str,
    pk: &PrimaryKeyColumn,
    keys: &[&str],
) -> Result<HashMap<String, Row>, ApplyError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let pk_ident = quote_ident(&pk.name);
    let sql = format!(
        "select m.k, e.key, e.value \
         from (select {pk_ident}::text as k, to_jsonb(t.*) as doc from {} t \
               where {pk_ident} = any($1::text[]::{}[])) m \
         cross join lateral jsonb_each_text(m.doc) e",
        ddl::qualified_source_table(source_table),
        pk.data_type,
    );
    let db_rows = client.query(&sql, &[&keys]).await?;
    let mut rows: HashMap<String, Row> = HashMap::new();
    for db_row in db_rows {
        let key: String = db_row.get(0);
        let field: String = db_row.get(1);
        let value: Option<String> = db_row.get(2);
        rows.entry(key).or_default().insert(field, value);
    }
    Ok(rows)
}

/// The from-side keys whose `from_col` matches any of `join_keys` (compared as
/// `::text`, the relationship join-key convention shared with the evaluator —
/// exact for the integer/uuid/text keys relationships allow, numeric keys
/// being rejected at definition time). Returns `(from_pk_text, from_col_text)`
/// so the reverse-recompute caller can map each matched from-side row back to
/// the join value — hence the triggering related-row change's `hop_gen` — that
/// pulled it in. A `NULL` `from_col` never matches (SQL `NULL`), so such rows
/// are absent, exactly like the evaluator's LEFT JOIN no-match.
async fn from_side_keys_for_join(
    pool: &Pool,
    from_table: &str,
    from_pk: &PrimaryKeyColumn,
    from_col: &str,
    join_keys: &[String],
) -> Result<Vec<(String, String)>, ApplyError> {
    if join_keys.is_empty() {
        return Ok(Vec::new());
    }
    let client = pool.get().await?;
    let sql = format!(
        "select {pk}::text, {col}::text \
         from {tbl} \
         where {col}::text = any($1::text[])",
        pk = quote_ident(&from_pk.name),
        col = quote_ident(from_col),
        tbl = quote_ident(from_table),
    );
    let rows = client.query(&sql, &[&join_keys]).await?;
    Ok(rows
        .into_iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect())
}

/// Every from-side key whose `from_col` is currently non-`NULL` (issue #98).
/// A `TRUNCATE` stages one key-less sentinel (`append::TRUNCATE_SENTINEL_KEY`),
/// not per-row images, so unlike [`from_side_keys_for_join`] there is no
/// specific set of join-key *values* to match against — the to-side table is
/// now completely empty. Every from-side row that still points at
/// *something* must therefore re-derive to `NULL`: there's no way to tell,
/// after the fact, which of those rows previously matched a real to-side row
/// (and so must newly go stale) versus already pointed at nothing (and so
/// were already `NULL`) — both converge to the same `NULL` result once the
/// to-side is empty, so both are recomputed rather than trying to
/// distinguish them.
async fn from_side_keys_with_non_null_join(
    pool: &Pool,
    from_table: &str,
    from_pk: &PrimaryKeyColumn,
    from_col: &str,
) -> Result<Vec<String>, ApplyError> {
    let client = pool.get().await?;
    let sql = format!(
        "select {pk}::text from {tbl} where {col} is not null",
        pk = quote_ident(&from_pk.name),
        col = quote_ident(from_col),
        tbl = quote_ident(from_table),
    );
    let rows = client.query(&sql, &[]).await?;
    Ok(rows.into_iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Builds the [`RelationshipContext`] a relationship-enriched from-side target
/// needs to re-evaluate (issue #30 wiring of the #28/#29 evaluator): for each
/// relationship the definition references, the related to-side rows keyed by
/// their `to_col` text, plus the referenced to-side columns' types. Join keys
/// are the distinct `from_col` values of the from-side rows this batch will
/// evaluate — so only the related rows those rows actually need are fetched.
/// `from_table` is the definition's own source table (a relationship's
/// `from_table`).
pub(crate) async fn build_relationship_context(
    pool: &Pool,
    from_table: &str,
    def: &TransformDef,
    rows: &[Option<Row>],
) -> Result<RelationshipContext, ApplyError> {
    // Group the referenced columns by relationship name (a relationship may be
    // read for more than one column across the definition's fields).
    let mut cols_by_rel: HashMap<String, Vec<String>> = HashMap::new();
    for (rel, column) in eval::relationship_references(def) {
        let cols = cols_by_rel.entry(rel).or_default();
        if !cols.contains(&column) {
            cols.push(column);
        }
    }

    let mut by_name: HashMap<String, ToOneRelationship> = HashMap::new();
    let mut to_many_by_name: HashMap<String, ToManyRelationship> = HashMap::new();

    for (rel_name, columns) in cols_by_rel {
        let Some(reldef) = catalog::relationship_by_name(pool, from_table, &rel_name).await? else {
            // Unknown relationship: leave it out and let the evaluator surface
            // `EvalError::UnknownRelationship`, the same as the pure path.
            continue;
        };
        let from_col = reldef.def.from_col.clone();
        let to_col = reldef.def.to_col.clone();
        let to_table = reldef.def.to_table.clone();

        // The join keys we need on the to-side: the distinct non-NULL
        // `from_col` values of the from-side rows this batch evaluates.
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut join_keys: Vec<String> = Vec::new();
        for row in rows.iter().flatten() {
            if let Some(Some(text)) = row.get(&from_col)
                && seen.insert(text.as_str())
            {
                join_keys.push(text.clone());
            }
        }

        let to_columns = to_column_types(pool, &to_table, &columns).await?;
        let grouped = fetch_to_side_rows(pool, &to_table, &to_col, &join_keys).await?;

        match reldef.cardinality {
            RelationshipCardinality::ToOne => {
                // `to_col` is UNIQUE, so each key has exactly one related row.
                let to_rows_by_key = grouped
                    .into_iter()
                    .filter_map(|(k, mut v)| v.pop().map(|row| (k, row)))
                    .collect();
                by_name.insert(
                    rel_name,
                    ToOneRelationship {
                        from_col,
                        cardinality: RelationshipCardinality::ToOne,
                        to_columns,
                        to_rows_by_key,
                    },
                );
            }
            RelationshipCardinality::ToMany => {
                to_many_by_name.insert(
                    rel_name,
                    ToManyRelationship {
                        from_col,
                        to_columns,
                        to_rows_by_key: grouped,
                    },
                );
            }
        }
    }

    Ok(RelationshipContext::new(by_name).with_to_many(to_many_by_name))
}

/// The to-side rows whose `to_col` matches any of `join_keys`, grouped by that
/// key's `::text` (the evaluator's key convention, shared with
/// [`from_side_keys_for_join`]). A `NULL` `to_col` is absent (SQL `NULL` never
/// joins) — matching the evaluator's requirement that such a to-side row carry
/// no key. To-one relationships get exactly one row per key (`to_col` is
/// UNIQUE); to-many get the full related set. Decodes each row's columns via
/// the same in-SQL `jsonb_each_text` unnest [`read_live_rows_batch`] uses.
async fn fetch_to_side_rows(
    pool: &Pool,
    to_table: &str,
    to_col: &str,
    join_keys: &[String],
) -> Result<HashMap<String, Vec<Row>>, ApplyError> {
    if join_keys.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let sql = format!(
        "select m.jk, m.rn, e.key, e.value \
         from (select {col}::text as jk, \
                      row_number() over () as rn, \
                      to_jsonb(t.*) as doc \
               from {tbl} t \
               where {col}::text = any($1::text[])) m \
         cross join lateral jsonb_each_text(m.doc) e",
        col = quote_ident(to_col),
        tbl = quote_ident(to_table),
    );
    let db_rows = client.query(&sql, &[&join_keys]).await?;
    // Assemble each row by its stable `rn`, carrying its join key, then group.
    let mut assembled: HashMap<i64, (String, Row)> = HashMap::new();
    for db_row in db_rows {
        let jk: String = db_row.get(0);
        let rn: i64 = db_row.get(1);
        let field: String = db_row.get(2);
        let value: Option<String> = db_row.get(3);
        let entry = assembled.entry(rn).or_insert_with(|| (jk, Row::new()));
        entry.1.insert(field, value);
    }
    let mut grouped: HashMap<String, Vec<Row>> = HashMap::new();
    for (_, (jk, row)) in assembled {
        grouped.entry(jk).or_default().push(row);
    }
    Ok(grouped)
}

/// The [`ValueType`] of each named column on `table`, introspected live from
/// `pg_catalog` via `format_type` (matching `catalog::column_type_in_txn`), so
/// a to-side relationship column's text is typed the same way the from-side
/// source columns are. A column not found is simply absent — the evaluator
/// defaults an absent to-side column to `Numeric`.
pub(crate) async fn to_column_types(
    pool: &Pool,
    table: &str,
    columns: &[String],
) -> Result<HashMap<String, ValueType>, ApplyError> {
    if columns.is_empty() {
        return Ok(HashMap::new());
    }
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod) \
             from pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attname = any($2::text[]) \
               and a.attnum > 0 \
               and not a.attisdropped",
            &[&table, &columns],
        )
        .await?;
    let mut types = HashMap::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let pg_type: String = row.get(1);
        types.insert(name, value_type_from_pg(&pg_type));
    }
    Ok(types)
}

/// Maps a Postgres `format_type` rendering to the evaluator's [`ValueType`],
/// mirroring `catalog::type_family`'s buckets. Anything not clearly numeric,
/// boolean, or uuid is treated as text — a verbatim passthrough that can't
/// misparse, the safe default for a to-side enrichment column.
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

// ---------------------------------------------------------------------
// Phase 2: compute
// ---------------------------------------------------------------------

/// Issue #51/ADR-0009 decision 5: records one applied change's per-transform
/// hop latency and throughput, from data [`compute`]'s by-source grouping
/// already has in hand — no new I/O, no new join. `transform` is the
/// consuming definition's target table (this crate's one "transform name,"
/// per `ApplyError::ColumnNotPaused`/`DefinitionNotLive`'s own `transform`
/// fields). `src_changed` is [`FoldedChange::src_changed`]: `Some` for a
/// change that traces back to a real source commit (the histogram's
/// `.observe()` value is `now - src_changed`), `None` for a bare recompute
/// trigger with no origin timestamp to measure against — such a change
/// still counts toward throughput, just not latency.
///
/// Called once per applied change per consuming definition — both the 1-1
/// write/delete dispatch and the aggregate accumulate path below call this
/// at the point each of their per-change loops already visits every folded
/// change, so this reuses grouping/iteration `compute` performs regardless
/// of whether metrics are recorded, per the ADR's "effectively free"
/// framing.
///
/// Issue #52: every `Some(src_changed)` this function sees is also buffered
/// into `end_to_end_origins`, keyed by `transform` — one origin timestamp
/// per applied change, same as the per-transform histogram observes. This
/// is *not* itself gated on terminal-ness: at the point every call site
/// below runs, `compute` hasn't yet determined which targets in this batch
/// are terminal (that's [`ApplyPlan::downstream_readers`], computed once,
/// after every source's changes have been evaluated — see the end of
/// [`compute`]). Buffering here and flushing only the terminal targets'
/// entries there reuses that one dedup'd downstream-reader lookup instead of
/// adding a second one per change.
fn record_transform_apply_metrics(
    transform: &str,
    src_changed: Option<std::time::SystemTime>,
    end_to_end_origins: &mut HashMap<String, Vec<std::time::SystemTime>>,
) {
    if let Some(src_changed) = src_changed {
        let latency = std::time::SystemTime::now()
            .duration_since(src_changed)
            .unwrap_or(std::time::Duration::ZERO);
        crate::metrics::record_transform_latency(transform, latency);
        end_to_end_origins
            .entry(transform.to_string())
            .or_default()
            .push(src_changed);
    }
    crate::metrics::increment_changes_applied(transform);
}

/// One key's write into a target table: the evaluated calculated-field
/// values, rendered to their canonical text form (aligned with the owning
/// [`TargetPlan::field_names`]/[`TargetPlan::field_types`]) plus the
/// `hop_gen` it carries forward if this write propagates downstream.
///
/// Kept as text rather than [`eval::Value`] so [`apply_target`] can bind it
/// straight into a parameterized query — every one of [`ValueType`]'s three
/// variants renders to a plain text form Postgres's own `::text::<type>`
/// cast round-trips exactly (numeric's decimal text, `Display for bool`'s
/// `true`/`false`, text values verbatim) — matching this module's existing
/// "text in, typed cast in SQL" convention for every other value it writes.
///
/// `src_changed` (issues #51/#52's multi-hop gap) is the triggering
/// [`FoldedChange::src_changed`], carried forward the same way `hop_gen` is
/// — so a downstream `Recompute` row this write's own propagation stages
/// (see [`apply_and_mark_drained_many`]'s step 4) keeps a real origin
/// instead of losing it at this hop.
#[derive(Debug, Clone)]
struct TargetWrite {
    pk_text: String,
    values: Vec<Option<String>>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
}

/// One key's deletion from a target table (the folded change had no
/// `new_image`). `src_changed` plays the same forward-carrying role as
/// [`TargetWrite::src_changed`].
#[derive(Debug, Clone)]
struct TargetDelete {
    pk_text: String,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
}

/// Everything Phase 3 needs to write one target table: its primary key
/// shape (for the pre-lock/upsert/delete SQL), the calculated-field column
/// names and their inferred [`ValueType`]s (both aligned with every
/// [`TargetWrite::values`], so [`apply_target`] knows which Postgres type
/// each column casts to), and the writes and deletes this batch computed
/// for it.
#[derive(Debug, Clone)]
struct TargetPlan {
    pk: PrimaryKeyColumn,
    field_names: Vec<String>,
    field_types: Vec<ValueType>,
    writes: Vec<TargetWrite>,
    deletes: Vec<TargetDelete>,
    /// The persisted, fully-qualified `"schema.table"` identity of this
    /// target (issue #73's `Definition::target_table`, ADR-0007) —
    /// carried alongside the bare `def.def.target` this plan is keyed by
    /// (see [`ApplyPlan::targets`]'s doc comment on why the map key itself
    /// stays bare) so [`apply_target`] can bind the *right* physical table
    /// into its `INSERT`/`UPDATE`/`DELETE` SQL, rather than leaving a
    /// target explicitly qualified into a non-default schema (issue #76) to
    /// resolve against whatever `search_path` the executing session
    /// happens to carry. Mirrors [`AggregateTargetPlan::source`]/this same
    /// struct's own eventual reuse of `qualified_source`'s established
    /// pattern from #76.
    qualified_target: String,
}

/// One target table this batch must clear in full before its own keyed
/// writes/deletes apply (issue #60: a truncate on `src_table` clears every
/// row a 1-1 transform ever derived from it). `pk` is the target's primary
/// key shape — reused, per [`compute`]'s existing convention, from the
/// truncated source's own PK introspection (a 1-1 target's key column
/// mirrors its source's) — needed so Phase 3's `DELETE ... RETURNING` can
/// name the right column. `hop_gen` is the triggering truncate sentinel's
/// own `hop_gen`, carried forward so keys the clear physically removes
/// propagate downstream at `hop_gen + 1`, exactly like any other
/// physically-changed key.
///
/// `src_changed` is the triggering truncate sentinel's own `src_changed`
/// (issues #51/#52's multi-hop gap), fan-in tie-broken by `min` across
/// however many truncated sources resolve to this same target — see
/// [`earliest_src_changed`]'s doc comment for why `min`, not `max`, is the
/// right merge here.
#[derive(Debug, Clone)]
struct ClearPlan {
    pk: PrimaryKeyColumn,
    hop_gen: i32,
    /// Same role as [`TargetPlan::qualified_target`]: the persisted,
    /// fully-qualified target identity this clear's `DELETE FROM` must bind,
    /// rather than the bare map key it's stored under.
    qualified_target: String,
    src_changed: Option<std::time::SystemTime>,
}

/// The aggregate-target counterpart to [`ClearPlan`] — see
/// [`ApplyPlan::aggregate_clears`]'s doc comment for why this carries no
/// [`PrimaryKeyColumn`] of its own (a plain full-table `DELETE`, no
/// `RETURNING`-projected key shape needed). `qualified_target` plays the
/// same role [`ClearPlan::qualified_target`]/[`TargetPlan::qualified_target`]
/// do: the persisted, fully-qualified identity the `DELETE FROM` must bind,
/// not the bare map key this is stored under.
#[derive(Debug, Clone)]
struct AggregateClearPlan {
    hop_gen: i32,
    qualified_target: String,
}

/// The fan-in tie-break for [`StagedChange::Recompute::src_changed`]
/// (issues #51/#52's multi-hop gap): when more than one to-side change in a
/// batch feeds the same propagated key (forward propagation's `changed` map,
/// or reverse recompute's `(from_table, from_key)` accumulator), the
/// **earliest** (`min`) of their origins wins — the oldest/earliest
/// source-commit timestamp captures the slowest straggler in the group,
/// matching the p99/stall-visibility intent these latency histograms exist
/// for. This is deliberately the opposite of `hop_gen`'s own fan-in
/// tie-break (`max`, propagation depth: the deepest contributor sets the
/// bound) — same shape of merge, different direction, because the two
/// numbers answer different questions ("how stale is the staler input" vs.
/// "how deep is the deepest input").
///
/// `None` never wins over a real `Some`: an origin-less contributor (a
/// backfill-enumerated recompute, or any other change with no traceable
/// source commit) doesn't get to blank out a known origin its fan-in sibling
/// carried — it simply contributes nothing to the merge. Only when *every*
/// contributor is origin-less does the result stay `None`.
fn earliest_src_changed(
    a: Option<std::time::SystemTime>,
    b: Option<std::time::SystemTime>,
) -> Option<std::time::SystemTime> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(t), None) | (None, Some(t)) => Some(t),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::earliest_src_changed;
    use std::time::{Duration, SystemTime};

    #[test]
    fn earliest_src_changed_picks_the_lesser_of_two_known_origins() {
        let earlier = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let later = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        assert_eq!(
            earliest_src_changed(Some(later), Some(earlier)),
            Some(earlier),
            "the earlier of two known origins must win, regardless of argument order"
        );
        assert_eq!(
            earliest_src_changed(Some(earlier), Some(later)),
            Some(earlier)
        );
    }

    #[test]
    fn earliest_src_changed_never_lets_a_none_beat_a_known_origin() {
        let known = SystemTime::UNIX_EPOCH + Duration::from_secs(5);
        assert_eq!(
            earliest_src_changed(Some(known), None),
            Some(known),
            "an origin-less fan-in sibling must not blank out a known origin"
        );
        assert_eq!(earliest_src_changed(None, Some(known)), Some(known));
    }

    #[test]
    fn earliest_src_changed_of_two_unknowns_stays_unknown() {
        assert_eq!(earliest_src_changed(None, None), None);
    }
}

/// Phase 2's output: an in-memory plan Phase 3 applies inside one
/// transaction, with no further catalog reads of its own beyond the version
/// fence.
///
/// `versions` is the version-fence's read set: every source table this
/// batch evaluated against, and the `source_table_versions.version` Phase 2
/// loaded for it (`None` for a source with no definitions at all — still
/// fenced, so a definition created against it mid-drain is caught too).
/// `downstream_readers` records, per target table this batch wrote to,
/// whether any definition currently reads it — decided here (a catalog
/// read, like the evaluation lookups above it) rather than in Phase 3,
/// which holds no pool connection of its own and must stay a pure,
/// txn-scoped function. See [`apply_and_mark_drained`]'s doc comment on the
/// staleness this implies and why it's an accepted tradeoff.
#[derive(Debug, Clone, Default)]
pub struct ApplyPlan {
    versions: HashMap<String, Option<i64>>,
    targets: HashMap<String, TargetPlan>,
    /// [`KeySpace::Aggregate`] targets' per-group deltas (issue #11's
    /// aggregate extension) — the same role [`ApplyPlan::targets`] plays for
    /// [`KeySpace::OneToOne`], kept as a separate map since the two key
    /// spaces' Phase 3 write shapes (`apply_target`'s ordered pre-lock CTE
    /// vs. `apply_aggregate::apply_aggregate_target`'s sequential per-group
    /// upserts) are different enough not to share one plan type.
    aggregate_targets: HashMap<String, AggregateTargetPlan>,
    downstream_readers: HashMap<String, bool>,
    /// Targets to clear in full at Phase 3, keyed by target table name —
    /// issue #60's truncate propagation. See [`ClearPlan`].
    clears: HashMap<String, ClearPlan>,
    /// The aggregate-target counterpart to [`ApplyPlan::clears`]: a
    /// truncate on an aggregate definition's source clears every group, but
    /// (unlike a 1-1 target's single-column primary key) there is no single
    /// column shape to `RETURNING`-project a physically-changed group key
    /// out of generically, and no downstream reader can consume an
    /// aggregate target's composite key as a 1-1 source today regardless —
    /// so this is applied as a plain `DELETE FROM <target>` (every group
    /// atomically gone), counted toward [`ApplyOutcome::keys_deleted`], but
    /// *not* staged for downstream propagation. A documented gap, not an
    /// oversight: closing it needs composite-key downstream propagation,
    /// out of scope for this issue (see `staging::apply_aggregate`'s module
    /// doc comment for the rest of what this issue does cover).
    ///
    /// Investigated (issue #11 review): could a definition actually be
    /// *created* reading from an aggregate target today, making this skip a
    /// live correctness gap rather than a moot one? Yes — `defs::validate`/
    /// `create_definition` impose no primary-key-shape check at
    /// definition-creation time, so nothing stops such a definition from
    /// being saved. But `compute()`'s Phase 2 unconditionally calls
    /// `ddl::source_primary_key` for every distinct source table a batch's
    /// folded changes touch, *before* any per-definition dispatch — so the
    /// very first drain attempt against that source fails loudly with
    /// `DdlError::CompositePrimaryKeyUnsupported` (surfaced as
    /// [`ApplyError::Ddl`]), before the encoded composite group-key text
    /// could ever be misread as a single-column key. The ordinary
    /// aggregate-write path below (the "3b" step) stages downstream
    /// Recompute rows keyed the same encoded way for exactly the same
    /// reason: both paths are consistent in outcome (fail loud, never
    /// silently misuse the key) regardless of which one a batch takes, so
    /// this skip is not a live gap today.
    aggregate_clears: HashMap<String, AggregateClearPlan>,
    /// Issue #16: the (non-truncate) folded records whose `(src_table,
    /// key)` is already in the `poison` marker table — excluded from every
    /// map above (the fold excludes a poisoned key *globally*, not just
    /// from evaluation), and instead parked into `poison_held` by
    /// [`apply_and_mark_drained`], in the same Phase-3 transaction, before
    /// the drained mark. See `quarantine::park_batch_contribution`'s doc
    /// comment for why this must happen even when this batch didn't cause
    /// the key's eviction.
    poisoned_park: Vec<FoldedChange>,
    /// Issue #16: every non-truncate, non-poisoned `(src_table, key)` this
    /// batch computed against — cleared from `key_deaths` by
    /// [`apply_and_mark_drained`] on a successful commit, per doc 06's "a
    /// clean drain clears the counters for the keys it just applied."
    applied_keys: Vec<(String, String)>,
    /// Issue #30's reverse recompute: when a *to-side* (related) row changed,
    /// each `(from_table, from_key_text, hop_gen)` here is a from-side row
    /// whose relationship enrichment depends on that changed related row and
    /// so must be re-derived. Resolved in Phase 2 (a live join-key lookup on
    /// `from_table` — see [`from_side_keys_for_join`]) and emitted as ordinary
    /// image-less [`StagedChange::Recompute`]s by [`apply_and_mark_drained`],
    /// reusing the same async staging/apply/fence pipeline forward propagation
    /// uses rather than any bespoke persisted reverse index. `hop_gen` is the
    /// triggering related-row change's own `hop_gen + 1`, hop-bounded at emit.
    /// The trailing `Option<SystemTime>` is the triggering change's
    /// `src_changed`, fan-in tie-broken by [`earliest_src_changed`] when more
    /// than one to-side change resolves to the same `(from_table,
    /// from_key)` (issues #51/#52's multi-hop gap).
    reverse_recomputes: Vec<(String, String, i32, Option<std::time::SystemTime>)>,
}

/// Phase 2 (design doc: "no transaction, no locks"): evaluates every
/// folded change's `f()` against the transform currently reading its
/// source table, grouped by (unqualified) `src_table` so each source's
/// catalog version is loaded — and fenced against — exactly once.
///
/// Reloads the catalog fresh on every call, including retries: this is
/// what makes [`drain_once`]'s retry-on-fence-miss loop "reload, recompute"
/// rather than needing any separate invalidation path.
pub async fn compute(pool: &Pool, folded: &[FoldedChange]) -> Result<ApplyPlan, ApplyError> {
    // Issue #16: exclude already-poisoned keys before anything else touches
    // them — the fold excludes a poisoned key globally, not just from this
    // one batch's evaluation. Truncate sentinels are never candidates: a
    // truncate is whole-keyspace, not a key quarantine can attribute
    // anything to.
    let candidates: Vec<(&str, &str)> = folded
        .iter()
        .filter(|c| !c.is_truncate)
        .map(|c| (c.src_table.as_str(), c.key.as_str()))
        .collect();
    let poisoned = quarantine::poisoned_keys_among(pool, &candidates).await?;

    let mut by_source: HashMap<&str, Vec<&FoldedChange>> = HashMap::new();
    // Truncate sentinels (issue #60) never enter the keyed by-source
    // evaluation loop below — they carry no key of their own (see
    // `append::TRUNCATE_SENTINEL_KEY`) and produce no write/delete;
    // they're handled separately, right after that loop.
    let mut truncated: Vec<&FoldedChange> = Vec::new();
    let mut poisoned_park: Vec<FoldedChange> = Vec::new();
    let mut applied_keys: Vec<(String, String)> = Vec::new();
    for change in folded {
        if change.is_truncate {
            truncated.push(change);
            continue;
        }
        if poisoned.contains(&(change.src_table.clone(), change.key.clone())) {
            poisoned_park.push(change.clone());
            continue;
        }
        applied_keys.push((change.src_table.clone(), change.key.clone()));
        by_source
            .entry(catalog_source_key(&change.src_table))
            .or_default()
            .push(change);
    }

    let mut versions: HashMap<String, Option<i64>> = HashMap::new();
    let mut targets: HashMap<String, TargetPlan> = HashMap::new();
    let mut aggregate_targets: HashMap<String, AggregateTargetPlan> = HashMap::new();
    // Issue #52: every `Some(src_changed)` origin timestamp
    // `record_transform_apply_metrics` sees below, buffered per consuming
    // target — flushed into `metrics::record_end_to_end_latency` only for
    // targets the `downstream_readers` computation at the end of this
    // function finds terminal (see that call site's comment).
    let mut end_to_end_origins: HashMap<String, Vec<std::time::SystemTime>> = HashMap::new();
    // Issue #79: deduped across *every* relationship (and every source_key)
    // this whole `compute` call processes, not just within one relationship's
    // `key_hops` — two distinct inbound relationships sharing the same
    // `from_table` (e.g. `posts` and `comments` both pointing at `authors`)
    // otherwise each independently queue a full reverse-recompute pass over
    // every touched from-side key, doubling (or worse, with N relationships)
    // the backlog for no benefit: only one recompute per from-side row is
    // ever needed, at the highest hop_gen any contributing relationship
    // required. Keyed by `(from_table, from_key)`; drained into the
    // `Vec` shape `ApplyPlan` expects right before it's constructed below.
    // The `Option<SystemTime>` half is `src_changed` (issues #51/#52's
    // multi-hop gap), fan-in tie-broken by `earliest_src_changed` (min) —
    // deliberately the opposite merge direction from `hop_gen`'s `max`, see
    // that function's doc comment.
    let mut reverse_recomputes: HashMap<(String, String), (i32, Option<std::time::SystemTime>)> =
        HashMap::new();

    for (source_key, changes) in by_source {
        let version = catalog::source_table_version(pool, source_key).await?;
        versions.insert(source_key.to_string(), version);

        // The fully-qualified source identity this batch's own CDC producer
        // staged (issue #76, ADR-0007) — `change.src_table`, not `source_key`
        // (that stays bare purely as the catalog lookup key, per
        // `catalog_source_key`'s own doc comment). Every change in this
        // bucket shares the same bare suffix by construction (`by_source`
        // grouped on it); they're expected to also share this qualified form
        // (the same physical table), so any one of them gives the right
        // answer for the physical reads below — used in place of a bare
        // `source_key` so `source_primary_key`/`read_live_rows_batch` don't
        // leave the schema to resolve against whatever `search_path` the
        // executing session happens to carry.
        let qualified_source = changes[0].src_table.as_str();

        // `source_key` alone determines the source table's primary key, not
        // the individual definition (issue #69) — introspected once per
        // source here and reused both below (every definition subscribed to
        // this source) and by the row decode below (every change, whichever
        // definition it's evaluated against). A live `42P01` here means
        // `source_key` no longer exists (issue #16's dropped-table purge,
        // not an ordinary DDL error) — see [`ApplyError::SourceTableDropped`].
        let pk = match ddl::source_primary_key(pool, qualified_source).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: source_key.to_string(),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped { source_table });
            }
            Err(err) => return Err(err.into()),
        };

        // Decoded/re-read once per change here — not once per (definition,
        // change) — since every definition subscribed to this source
        // evaluates the exact same row (issue #69): the image a change
        // carries, or the live re-read for an image-less recompute trigger,
        // doesn't depend on which definition is reading it. The `(None,
        // None)` shape — a bare recompute trigger with no image, the shape
        // every backfill enumeration produces — is collected instead of
        // re-read immediately, so every such key in this batch is fetched
        // in one [`read_live_rows_batch`] round trip rather than one
        // round trip per key (issue #13).
        let mut rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        let mut live_refetch_indices: Vec<usize> = Vec::new();
        for (i, change) in changes.iter().enumerate() {
            let row = match (&change.new_image, &change.old_image) {
                (Some(image_text), _) => Some(decode_image(pool, image_text).await?),
                (None, Some(_)) => None,
                (None, None) => {
                    live_refetch_indices.push(i);
                    None
                }
            };
            rows.push(row);
        }
        if !live_refetch_indices.is_empty() {
            let live_keys: Vec<&str> = live_refetch_indices
                .iter()
                .map(|&i| changes[i].key.as_str())
                .collect();
            let mut live_rows =
                read_live_rows_batch(pool, qualified_source, &pk, &live_keys).await?;
            for &i in &live_refetch_indices {
                rows[i] = live_rows.remove(changes[i].key.as_str());
            }
        }

        // `qualified_source` (via `qualified_schema_node_key`), not
        // `source_key`: `schema_nodes`/`schema_edges` now key on qualified
        // identity (issue #74, ADR-0007), so `transforms_for_source` (a
        // thin `dependents_of` wrapper) needs an exact qualified match
        // here, not the bare catalog-lookup key `catalog_source_key`'s own
        // doc comment already explains stays bare for
        // `source_table_version`/`relationships_to_table` below (both still
        // bare-suffix-keyed, unaffected by #74). `qualified_source` is
        // usually already fully-qualified (real CDC/backfill), but a
        // downstream-propagation hop's `src_table` is a bare target name
        // this same apply path staged — `qualified_schema_node_key` resolves
        // that case too; see its own doc comment.
        let defs = catalog::transforms_for_source(
            pool,
            &qualified_schema_node_key(pool, qualified_source).await?,
        )
        .await?;

        // Aggregate definitions need each change's *old*-side row too (to
        // derive a grain-migrating change's old group key and its old
        // contribution — see `apply_aggregate`'s doc comment), decoded once
        // here and shared across every aggregate definition on this source,
        // same as `rows` above. Only decoded when this source actually has
        // an aggregate reader, to avoid the extra round trips for the
        // (overwhelmingly common) 1-1-only source.
        let needs_old_rows = defs
            .iter()
            .any(|def| matches!(def.def.key_space, KeySpace::Aggregate { .. }));
        let mut old_rows: Vec<Option<Row>> = Vec::with_capacity(changes.len());
        if needs_old_rows {
            for change in &changes {
                let old_row = match &change.old_image {
                    Some(image_text) => Some(decode_image(pool, image_text).await?),
                    None => None,
                };
                old_rows.push(old_row);
            }
        } else {
            old_rows.resize_with(changes.len(), || None);
        }

        // Reverse recompute (issue #30): this source is some relationship's
        // *to-side*. A change to a related row must re-derive every from-side
        // row whose enrichment reads it. For each relationship pointing at
        // this table, collect the join-key text of every to-side row this
        // batch touched — the related row's `to_col`. For a to-one this is a
        // PRIMARY KEY/UNIQUE column, so it rides in the default replica
        // identity of every image, including a delete's pre-image. For a
        // to-many, `to_col` is the *foreign* side (non-key), so it only rides
        // in the pre-image when the to-side carries an adequate replica
        // identity — which is exactly why issue #41 gates that at
        // `create_relationship` (define) time: `REPLICA IDENTITY FULL` or a
        // covering replica-identity index. That gate is creation-time only and
        // not re-validated per batch, so an operator who later relaxes the
        // to-side's replica identity would silently degrade reverse recompute
        // here (the `.unwrap_or(&None)` below cannot tell an absent column from
        // a genuine NULL — hence the guard must live at define time, not here).
        // Then resolve, with one live lookup, the from-side keys whose
        // `from_col` matches, and stage each as an image-less recompute at the
        // triggering change's `hop_gen + 1`.
        let inbound_rels = catalog::relationships_to_table(pool, source_key).await?;
        // Decode each change's pre-image once, reused across every inbound
        // relationship below (the join key lives in the pre-image for a
        // delete/re-parent). Skipped entirely when this table is nobody's
        // to-side, so the common no-relationship source pays nothing.
        let reverse_old_rows: Vec<Option<Row>> = if inbound_rels.is_empty() {
            Vec::new()
        } else {
            let mut decoded = Vec::with_capacity(changes.len());
            for change in &changes {
                decoded.push(match &change.old_image {
                    Some(image_text) => Some(decode_image(pool, image_text).await?),
                    None => None,
                });
            }
            decoded
        };
        for rel in &inbound_rels {
            // Join-key text -> the max `hop_gen` of the to-side changes that
            // touched it (a re-parent update touches both its old and new
            // key; a delete carries only its pre-image), and (issues
            // #51/#52's multi-hop gap) the *earliest* (`min`) `src_changed`
            // among those same changes — see `earliest_src_changed`'s doc
            // comment for why the two use opposite merge directions.
            let mut key_hops: HashMap<String, i32> = HashMap::new();
            let mut key_src_changed: HashMap<String, Option<std::time::SystemTime>> =
                HashMap::new();
            for (i, change) in changes.iter().enumerate() {
                let mut note = |value: &Option<String>, hop: i32| {
                    if let Some(text) = value {
                        key_hops
                            .entry(text.clone())
                            .and_modify(|h| *h = (*h).max(hop))
                            .or_insert(hop);
                        key_src_changed
                            .entry(text.clone())
                            .and_modify(|sc| *sc = earliest_src_changed(*sc, change.src_changed))
                            .or_insert(change.src_changed);
                    }
                };
                if let Some(row) = &rows[i] {
                    note(row.get(&rel.def.to_col).unwrap_or(&None), change.hop_gen);
                }
                if let Some(old) = &reverse_old_rows[i] {
                    note(old.get(&rel.def.to_col).unwrap_or(&None), change.hop_gen);
                }
            }
            if key_hops.is_empty() {
                continue;
            }
            let join_keys: Vec<String> = key_hops.keys().cloned().collect();
            let from_pk = ddl::source_primary_key(pool, &rel.def.from_table).await?;
            let matches = from_side_keys_for_join(
                pool,
                &rel.def.from_table,
                &from_pk,
                &rel.def.from_col,
                &join_keys,
            )
            .await?;
            for (from_key, join_text) in matches {
                let hop = key_hops.get(&join_text).copied().unwrap_or(0) + 1;
                let src_changed = key_src_changed.get(&join_text).copied().flatten();
                reverse_recomputes
                    .entry((rel.def.from_table.clone(), from_key))
                    .and_modify(|(h, sc)| {
                        *h = (*h).max(hop);
                        *sc = earliest_src_changed(*sc, src_changed);
                    })
                    .or_insert((hop, src_changed));
            }
        }

        for def in &defs {
            let KeySpace::Aggregate { group_by } = &def.def.key_space else {
                let field_names: Vec<String> =
                    def.def.fields.iter().map(|f| f.name.clone()).collect();
                // A relationship-enriched field's type can't come from
                // `infer_field_types` (it rejects a `<rel>.<column>` path,
                // whose type is a *to-side* column's, unknown to the from-side
                // type map). The field's inferred type *is* its target column's
                // declared type, though (see `infer_field_types`' doc), and
                // that column already exists — introspect it. Non-relationship
                // definitions keep the pure inference, behavior-identical.
                let field_types: Vec<ValueType> =
                    if eval::relationship_references(&def.def).is_empty() {
                        // This branch runs only for relationship-free definitions
                        // (guarded above), so type inference needs no relationship
                        // metadata: an empty map (issue #40).
                        let inferred_types = validate::infer_field_types(
                            &def.def,
                            &def.source_columns,
                            &std::collections::HashMap::new(),
                        )?;
                        field_names
                            .iter()
                            .map(|name| {
                                inferred_types
                                    .get(name)
                                    .copied()
                                    .unwrap_or(ValueType::Numeric)
                            })
                            .collect()
                    } else {
                        // Broader sweep, reviewer follow-up to issue #74 (epic
                        // #78's own whole-branch review): `def.def.target` is
                        // always bare, even for a definition whose `TRANSFORM`
                        // clause explicitly spelled `schema.target` (issue #76;
                        // see `TransformDef`'s own doc comment), so binding it
                        // straight into `to_column_types`'s `to_regclass` lookup
                        // relied on the connection's pinned `search_path`
                        // (`Config::schema`/`Config::target_schema`/`"public"`)
                        // finding it — silently wrong (or simply absent) for a
                        // target explicitly qualified into a schema outside that
                        // pin. `def.target_table` is right here on the same
                        // struct, already the fully-qualified identity issue #73
                        // persisted at acceptance time — use it instead of
                        // re-deriving (or mis-deriving) the physical location
                        // from the bare AST field.
                        let target_types =
                            to_column_types(pool, &def.target_table, &field_names).await?;
                        field_names
                            .iter()
                            .map(|name| {
                                target_types
                                    .get(name)
                                    .copied()
                                    .unwrap_or(ValueType::Numeric)
                            })
                            .collect()
                    };

                // ADR-0003's amendment (column-level quarantine): a column
                // the fuse has paused is excluded from both this plan's
                // column list (so `apply_target`'s generated SQL never
                // mentions it at all — decision: freeze at the last
                // successfully computed value, don't null it out or keep
                // reattempting a formula that's already fused off) and from
                // evaluation itself below (so a still-broken paused formula
                // doesn't keep reproducing the same failure on every batch).
                // Empty for every definition with nothing currently paused —
                // the overwhelmingly common case — so this is a cheap,
                // indexed no-op read then, behavior-identical to before this
                // amendment.
                let paused = quarantine::paused_columns_for(pool, &def.def.target).await?;
                let (field_names, field_types): (Vec<String>, Vec<ValueType>) = if paused.is_empty()
                {
                    (field_names, field_types)
                } else {
                    field_names
                        .into_iter()
                        .zip(field_types)
                        .filter(|(name, _)| !paused.contains(name))
                        .unzip()
                };

                let plan = targets
                    .entry(def.def.target.clone())
                    .or_insert_with(|| TargetPlan {
                        pk: pk.clone(),
                        field_names: field_names.clone(),
                        field_types: field_types.clone(),
                        writes: Vec::new(),
                        deletes: Vec::new(),
                        // The persisted, fully-qualified identity (issue #73)
                        // — not re-derived, since `def` (this source's own
                        // catalog `Definition`) already carries it. See
                        // `TargetPlan::qualified_target`'s doc comment.
                        qualified_target: def.target_table.clone(),
                    });

                // Reused across every change below (issue #68): `regexp_count`'s
                // pattern is a validated string literal, so its compiled `Regex`
                // is the same for every row this definition evaluates, and
                // recompiling it per row would be wasted work at realistic row
                // volumes.
                let mut regex_cache = eval::RegexCache::new();

                // Relationship enrichment (issues #28/#29 eval, wired here by
                // #30): a target reading a `<rel>.<column>` path (to-one) or an
                // aggregate over one (to-many) needs the related to-side rows
                // built into a `RelationshipContext`. Built once per definition
                // over this source's from-side rows — the join keys are their
                // `from_col` values — then threaded into every row eval below.
                // A definition with no relationship references stays on the
                // plain `eval::evaluate` path, behavior-identical to before.
                let rel_ctx = if eval::relationship_references(&def.def).is_empty() {
                    None
                } else {
                    Some(build_relationship_context(pool, source_key, &def.def, &rows).await?)
                };

                // Three shapes, per fold.rs's rules: `Some(new_image)` is an
                // insert/update, evaluated straight from the staged post-image.
                // `(None, Some(old_image))` is a genuine CDC delete — the fold's
                // first image-bearing row's pre-image survived, only the last
                // one's post-image didn't. `(None, None)` is everything else
                // image-less: a bare recompute trigger (reverse propagation,
                // definition re-derive, backfill), or — coincidentally the same
                // shape — a key inserted and deleted within one batch. Neither
                // carries an image to evaluate, so both re-read the *current*
                // row from `source_key` live: present means write with the live
                // image; absent (already gone, or never existed) means delete.
                // This is why `eval.rs`'s "no database access here" is scoped to
                // the evaluator itself, not this module. Decoded/re-read once
                // per change above, shared across every definition on this
                // source (issue #69) — not redone here per definition.
                for (change, row) in changes.iter().zip(rows.iter()) {
                    match row {
                        Some(row) => {
                            // `def.source_columns` is the same type map this
                            // definition was validated against at creation time
                            // (#63's write-path gap: persisted alongside the
                            // definition — see `catalog::create_definition` —
                            // rather than defaulting every column to Numeric).
                            let mut evaluated = match &rel_ctx {
                                Some(ctx) => eval::evaluate_with_relationships_excluding(
                                    &def.def,
                                    row,
                                    &def.source_columns,
                                    ctx,
                                    &mut regex_cache,
                                    &paused,
                                )?,
                                None => eval::evaluate_excluding(
                                    &def.def,
                                    row,
                                    &def.source_columns,
                                    &mut regex_cache,
                                    &paused,
                                )?,
                            };
                            let values: Vec<Option<String>> = field_names
                                .iter()
                                .map(|name| match evaluated.remove(name) {
                                    Some(Some(value)) => Some(value.to_string()),
                                    Some(None) | None => None,
                                })
                                .collect();
                            plan.writes.push(TargetWrite {
                                pk_text: change.key.clone(),
                                values,
                                hop_gen: change.hop_gen,
                                src_changed: change.src_changed,
                            });
                        }
                        None => {
                            plan.deletes.push(TargetDelete {
                                pk_text: change.key.clone(),
                                hop_gen: change.hop_gen,
                                src_changed: change.src_changed,
                            });
                        }
                    }
                    record_transform_apply_metrics(
                        &def.def.target,
                        change.src_changed,
                        &mut end_to_end_origins,
                    );
                }
                continue;
            };

            // Aggregate dispatch (issue #11): fold this source's changes
            // into per-group deltas on `def.def.target`'s aggregate plan,
            // via `apply_aggregate` rather than duplicating its logic here.
            let group_by_types: Vec<ValueType> = group_by
                .iter()
                .map(|c| {
                    def.source_columns
                        .get(c)
                        .copied()
                        .unwrap_or(ValueType::Numeric)
                })
                .collect();
            // Substitute cross-field-alias references (e.g. `double_total =
            // total + total` where `total` is itself a field) once up front,
            // so classification and the plan's rendered `field_exprs` share
            // one substitution pass — see
            // `defs::backfill::substituted_field_exprs`'s doc comment for why
            // the raw, un-substituted `Expr` can't be rendered as SQL.
            let substituted_exprs = crate::defs::backfill::substituted_field_exprs(&def.def)?;
            // Issue #94: a to-one relationship path an aggregate field folds
            // (`SUM(post.word_count)`) needs its relationship's endpoints (to
            // build the recompute's LEFT JOIN) and its to-side column's type
            // (to type the target column). Both come from the same catalog
            // resolution `defs::catalog` validates against; a relationship-free
            // aggregate resolves to an empty map and costs one cheap no-op.
            let relationships = catalog::resolve_relationships(pool, &def.def).await?;
            let field_plans = apply_aggregate::classify_fields(
                &def.def,
                group_by,
                &def.source_columns,
                &substituted_exprs,
                &relationships,
            )?;
            let mut rel_joins: Vec<apply_aggregate::RelJoin> = Vec::new();
            for rel_name in relationships.keys() {
                // Endpoints (`from_col` especially) come from the stored
                // relationship row; `ResolvedRelationship` carries only the
                // to-side, since that's all the validator needs.
                if let Some(reldef) =
                    catalog::relationship_by_name(pool, &def.def.source, rel_name).await?
                {
                    // Mirrors `defs::backfill::resolve_to_one_joins`'s guard:
                    // the validator makes a to-many path in an aggregate
                    // unreachable today, but this loop has no other cardinality
                    // check of its own, and a silent to-many LEFT JOIN here
                    // would fan out source rows and inflate every SUM instead
                    // of failing loudly like the direct-build path does.
                    if reldef.cardinality != RelationshipCardinality::ToOne {
                        return Err(crate::defs::backfill::BackfillError::Unsupported(
                            "an aggregate over a to-many relationship".to_string(),
                        )
                        .into());
                    }
                    rel_joins.push(apply_aggregate::RelJoin {
                        name: rel_name.clone(),
                        to_table: reldef.def.to_table,
                        to_col: reldef.def.to_col,
                        from_col: reldef.def.from_col,
                    });
                }
            }
            rel_joins.sort_by(|a, b| a.name.cmp(&b.name));
            let field_exprs: HashMap<String, crate::defs::ast::Expr> = substituted_exprs
                .into_iter()
                .filter(|(name, _)| !group_by.contains(name))
                .collect();
            let target_plan = aggregate_targets
                .entry(def.def.target.clone())
                .or_insert_with(|| {
                    AggregateTargetPlan::new(
                        group_by.clone(),
                        group_by_types,
                        field_plans,
                        qualified_source.to_string(),
                        def.target_table.clone(),
                        field_exprs,
                        rel_joins,
                    )
                });

            let mut regex_cache = eval::RegexCache::new();
            apply_aggregate::accumulate_changes(
                target_plan,
                &def.def,
                &changes,
                &rows,
                &old_rows,
                &def.source_columns,
                &mut regex_cache,
            )?;
            for change in &changes {
                record_transform_apply_metrics(
                    &def.def.target,
                    change.src_changed,
                    &mut end_to_end_origins,
                );
            }
        }
    }

    // Truncate clears (issue #60): for each truncated src_table, resolve its
    // targets via the catalog and record a full clear for each — the same
    // "resolve targets from the catalog" step the by-source loop above runs
    // per key, just once per truncated source instead of once per key.
    let mut clears: HashMap<String, ClearPlan> = HashMap::new();
    let mut aggregate_clears: HashMap<String, AggregateClearPlan> = HashMap::new();
    for change in &truncated {
        let source_key = catalog_source_key(&change.src_table);
        // Fence this source too, even though nothing evaluated against it —
        // a definition change against a truncated source, landing mid-drain,
        // must trip Phase 3's version fence exactly like it would for a
        // source this batch actually evaluated `f()` against.
        let version = catalog::source_table_version(pool, source_key).await?;
        versions.entry(source_key.to_string()).or_insert(version);

        let pk = match ddl::source_primary_key(pool, &change.src_table).await {
            Ok(pk) => pk,
            Err(DdlError::Db(db_err)) if quarantine::is_undefined_table(&db_err) => {
                return Err(ApplyError::SourceTableDropped {
                    source_table: source_key.to_string(),
                });
            }
            Err(DdlError::NoPrimaryKey { source_table })
                if quarantine::source_table_missing(pool, &source_table).await? =>
            {
                return Err(ApplyError::SourceTableDropped { source_table });
            }
            Err(err) => return Err(err.into()),
        };
        // `&change.src_table` (qualified), not `source_key` (bare) — see
        // the by-source loop above's identical comment on its own
        // `transforms_for_source` call. A `TRUNCATE` is always a real
        // physical CDC event (never a bare, internally-synthesized
        // `Recompute` row), so `qualified_schema_node_key` is a no-op here
        // in practice — routed through it anyway for the same safety the
        // by-source loop gets, at effectively no cost.
        let defs = catalog::transforms_for_source(
            pool,
            &qualified_schema_node_key(pool, &change.src_table).await?,
        )
        .await?;
        for def in &defs {
            match &def.def.key_space {
                KeySpace::Aggregate { .. } => {
                    aggregate_clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen)
                        })
                        .or_insert(AggregateClearPlan {
                            hop_gen: change.hop_gen,
                            qualified_target: def.target_table.clone(),
                        });
                }
                KeySpace::OneToOne => {
                    clears
                        .entry(def.def.target.clone())
                        .and_modify(|existing| {
                            existing.hop_gen = existing.hop_gen.max(change.hop_gen);
                            existing.src_changed =
                                earliest_src_changed(existing.src_changed, change.src_changed);
                        })
                        .or_insert(ClearPlan {
                            pk: pk.clone(),
                            hop_gen: change.hop_gen,
                            qualified_target: def.target_table.clone(),
                            src_changed: change.src_changed,
                        });
                }
            }
            // A TRUNCATE is a genuine applied change to every direct
            // downstream target, same as a row-driven change — recorded
            // once per def per truncated source, mirroring the row-driven
            // by_source loop above (issue #51/ADR-0009 decision 5).
            record_transform_apply_metrics(
                &def.def.target,
                change.src_changed,
                &mut end_to_end_origins,
            );
        }

        // Issue #98: a TRUNCATE clears definitions reading this table
        // directly (above), but definitions that read it only *through* a
        // relationship — this table is some relationship's to-side — need
        // clearing too, and the "truncate clears" mechanism above only
        // resolves direct source readers via `transforms_for_source`. Reuse
        // the reverse-recompute mechanism (issue #30) that the row-driven
        // `by_source` loop above feeds for exactly this situation, staging
        // every from-side row currently pointing at this (now-empty) table
        // as an image-less recompute — see
        // `from_side_keys_with_non_null_join`'s doc comment for why "every
        // non-NULL join column", not a specific value list, is the right
        // query for a TRUNCATE. Pushed into the same `reverse_recomputes`
        // accumulator the row-driven path uses, so it's deduped the same way
        // (issue #79) and drained through the same image-less `Recompute`
        // pipeline below — no separate emission path needed.
        let inbound_rels = catalog::relationships_to_table(pool, source_key).await?;
        for rel in &inbound_rels {
            let from_pk = ddl::source_primary_key(pool, &rel.def.from_table).await?;
            let from_keys = from_side_keys_with_non_null_join(
                pool,
                &rel.def.from_table,
                &from_pk,
                &rel.def.from_col,
            )
            .await?;
            let hop = change.hop_gen + 1;
            for from_key in from_keys {
                reverse_recomputes
                    .entry((rel.def.from_table.clone(), from_key))
                    .and_modify(|(h, sc)| {
                        *h = (*h).max(hop);
                        *sc = earliest_src_changed(*sc, change.src_changed);
                    })
                    .or_insert((hop, change.src_changed));
            }
        }
    }

    let mut downstream_readers = HashMap::new();
    let mut all_targets: std::collections::HashSet<&String> = targets.keys().collect();
    all_targets.extend(clears.keys());
    all_targets.extend(aggregate_targets.keys());
    all_targets.extend(aggregate_clears.keys());
    for target in all_targets {
        // `target` is bare (`def.def.target`) — `schema_nodes` now keys on
        // qualified identity (issue #74, ADR-0007), so a bare lookup here
        // would silently find nothing and permanently disable downstream
        // propagation for every chained transform.
        // `qualified_schema_node_key` resolves it the same way it resolves
        // a bare `Recompute`-staged `src_table` above (this *is* exactly
        // that case, one step earlier: `target` is about to become such a
        // row's `src_table` the moment this loop's caller stages it).
        let has_downstream =
            !catalog::transforms_for_source(pool, &qualified_schema_node_key(pool, target).await?)
                .await?
                .is_empty();
        downstream_readers.insert(target.clone(), has_downstream);
        // Issue #52/ADR-0009 decision 2: end-to-end latency is only ever
        // recorded for a *terminal* transform — one with no downstream
        // reader of its own — reusing this exact "does anything read
        // `target`" lookup rather than a second one. An intermediate hop
        // (`has_downstream` true) still gets its per-transform latency from
        // `record_transform_apply_metrics` above; it simply never flushes
        // here, so its origins in `end_to_end_origins` are dropped once
        // this function returns.
        if !has_downstream && let Some(origins) = end_to_end_origins.get(target) {
            for origin in origins {
                let latency = std::time::SystemTime::now()
                    .duration_since(*origin)
                    .unwrap_or(std::time::Duration::ZERO);
                crate::metrics::record_end_to_end_latency(target, latency);
            }
        }
    }

    let reverse_recomputes: Vec<(String, String, i32, Option<std::time::SystemTime>)> =
        reverse_recomputes
            .into_iter()
            .map(|((from_table, from_key), (hop, src_changed))| {
                (from_table, from_key, hop, src_changed)
            })
            .collect();

    Ok(ApplyPlan {
        versions,
        targets,
        aggregate_targets,
        downstream_readers,
        clears,
        aggregate_clears,
        poisoned_park,
        applied_keys,
        reverse_recomputes,
    })
}

// ---------------------------------------------------------------------
// Phase 3: apply ∪ mark-drained
// ---------------------------------------------------------------------

/// Postgres's wire protocol caps one statement's total bound parameters at
/// `i16::MAX` (65535) — the same limit `append::append`'s `MAX_ROWS_PER_STATEMENT`
/// exists to respect. A write row's parameter count scales with its target's
/// field count, so unlike `append::append` (whose row shape is fixed) this
/// is a parameter budget, not a row count: [`apply_target`] divides it by
/// `cols_per_row` to get the actual chunk size. 60000 leaves headroom below
/// 65535 regardless of column count.
const MAX_WRITE_PARAMS_PER_STATEMENT: usize = 60_000;

/// One physically-touched target key, as [`apply_and_mark_drained_many`]'s
/// `changed` accumulator and downstream-propagation step track it: the key
/// text, the `hop_gen` it carries forward, and (issues #51/#52's multi-hop
/// gap) the `src_changed` origin it carries forward — `None` for an
/// aggregate target's group key (see the 3b step's doc comment) or any
/// other touched key with no traceable origin.
type ChangedKey = (String, i32, Option<std::time::SystemTime>);

/// Runs one target table's ordered pre-lock, then its no-op-suppressed
/// upsert and delete, returning the keys Postgres actually wrote to vs.
/// deleted (as opposed to every key this batch merely *proposed* — the
/// no-op-suppression `WHERE ... IS DISTINCT FROM ...` guard can mean a
/// proposed write physically changes nothing).
///
/// The pre-lock takes every key this call touches (write or delete) `FOR
/// UPDATE`, ordered ascending, in one round trip — the deadlock-avoidance
/// convention doc 05 calls for between concurrent workers writing
/// overlapping target rows. It binds the whole key set as a single `text[]`
/// parameter, so — unlike the upsert below — its size never approaches the
/// bind-parameter cap regardless of batch size. Because this transaction
/// already holds every lock it needs before the upsert/delete run, chunking
/// those into multiple statements below doesn't reopen the ordering gap the
/// pre-lock exists to close: two transactions racing on overlapping keys
/// still each take every lock, in the same ascending order, before either
/// writes anything.
async fn apply_target(
    txn: &Transaction<'_>,
    plan: &TargetPlan,
) -> Result<(Vec<String>, Vec<String>), ApplyError> {
    if plan.writes.is_empty() && plan.deletes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let pk_ident = quote_ident(&plan.pk.name);
    let pk_cast = plan.pk.data_type.as_str();
    // `plan.qualified_target` (issue #73's persisted identity), not a bare
    // `quote_ident(target)` — a target explicitly qualified into a
    // non-default schema (issue #76) isn't necessarily on this connection's
    // pinned `search_path`. See `TargetPlan::qualified_target`'s doc comment
    // and `ddl::qualified_target_table_ident`'s.
    let target_ident = ddl::qualified_target_table_ident(&plan.qualified_target);
    let field_idents: Vec<String> = plan.field_names.iter().map(|n| quote_ident(n)).collect();

    let mut lock_keys: Vec<&str> = plan
        .writes
        .iter()
        .map(|w| w.pk_text.as_str())
        .chain(plan.deletes.iter().map(|d| d.pk_text.as_str()))
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();

    txn.query(
        &format!(
            "select {pk_ident} from {target_ident} \
             where {pk_ident} = any($1::text[]::{pk_cast}[]) \
             order by {pk_ident} for update"
        ),
        &[&lock_keys],
    )
    .await?;

    let field_pg_types: Vec<&str> = plan
        .field_types
        .iter()
        .map(|t| match t {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        })
        .collect();

    let col_list = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    let mut written = Vec::new();
    if !plan.writes.is_empty() {
        let cols_per_row = 1 + plan.field_names.len();
        let rows_per_chunk = (MAX_WRITE_PARAMS_PER_STATEMENT / cols_per_row).max(1);

        // Every one of this target's calculated columns can be paused at
        // once (ADR-0003's amendment) — a single-field definition whose lone
        // column's fuse has tripped is the simplest such case. There is then
        // nothing for a conflicting key to update at all: `do update set`
        // with an empty set list is invalid SQL, and an empty-tuple `is
        // distinct from` comparison is too. `do nothing` is also the
        // semantically right behavior, not just the SQL-valid one — an
        // existing row with every column frozen genuinely has no physical
        // change to make; a brand-new key still gets its bare row inserted
        // (frozen at the column defaults) via the same statement's `insert`
        // half.
        let on_conflict = if field_idents.is_empty() {
            format!("on conflict ({pk_ident}) do nothing")
        } else {
            let set_list = field_idents
                .iter()
                .map(|f| format!("{f} = excluded.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            let target_cols = field_idents
                .iter()
                .map(|f| format!("{target_ident}.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            let excluded_cols = field_idents
                .iter()
                .map(|f| format!("excluded.{f}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "on conflict ({pk_ident}) do update set {set_list} \
                 where ({target_cols}) is distinct from ({excluded_cols})"
            )
        };

        for chunk in plan.writes.chunks(rows_per_chunk) {
            let mut rows_sql = Vec::with_capacity(chunk.len());
            let mut params: Vec<&(dyn ToSql + Sync)> =
                Vec::with_capacity(chunk.len() * cols_per_row);
            for (i, write) in chunk.iter().enumerate() {
                let base = i * cols_per_row;
                let mut row_parts = vec![format!("${}::text::{pk_cast}", base + 1)];
                params.push(&write.pk_text);
                for (j, pg_type) in field_pg_types.iter().enumerate() {
                    row_parts.push(format!("${}::text::{pg_type}", base + 2 + j));
                    params.push(&write.values[j]);
                }
                rows_sql.push(format!("({})", row_parts.join(", ")));
            }

            let sql = format!(
                "insert into {target_ident} ({col_list}) \
                 select * from (values {}) as v({col_list}) \
                 {on_conflict} \
                 returning {pk_ident}::text as pk",
                rows_sql.join(", "),
            );
            let rows = txn.query(&sql, &params).await?;
            written.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
        }
    }

    // A key can appear in both `plan.writes` and `plan.deletes` (e.g. two
    // differently-qualified `src_table` spellings folding to the same
    // catalog source and disagreeing on whether the row is still live —
    // see `compute`'s per-change write/delete dispatch). The old
    // single-CTE-statement form of this function got "the write wins"
    // for free from Postgres's rule that every data-modifying CTE in one
    // WITH sees the same pre-statement snapshot, so a delete could never
    // remove a row its sibling CTE had just inserted. Splitting the write
    // and delete into separate sequential statements (above/below) loses
    // that guarantee — the delete would now run against a snapshot that
    // already includes the write — so it's restored explicitly here
    // instead: never delete a key this same call just wrote.
    let write_keys: std::collections::HashSet<&str> =
        plan.writes.iter().map(|w| w.pk_text.as_str()).collect();

    let mut deleted = Vec::new();
    let delete_keys: Vec<&str> = plan
        .deletes
        .iter()
        .map(|d| d.pk_text.as_str())
        .filter(|k| !write_keys.contains(k))
        .collect();
    if !delete_keys.is_empty() {
        let rows = txn
            .query(
                &format!(
                    "delete from {target_ident} \
                     where {pk_ident} = any($1::text[]::{pk_cast}[]) \
                     returning {pk_ident}::text as pk"
                ),
                &[&delete_keys],
            )
            .await?;
        deleted.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
    }

    Ok((written, deleted))
}

/// Phase 3 (design doc: "apply ∪ mark-drained are one transaction"). Given
/// `plan` (Phase 2's output) and the claim it belongs to, runs, inside
/// `txn`:
///
/// 1. **The version fence**: `FOR SHARE`s every source table `plan`
///    evaluated against and compares its current version to the one loaded
///    at compute time. A mismatch means a definition changed mid-drain;
///    this returns [`ApplyError::VersionFenceMiss`] without writing
///    anything, for [`drain_once`] to retry against the reloaded catalog.
/// 2. **Truncate clears** (issue #60), one plain `DELETE FROM <target>` per
///    [`ApplyPlan::clears`] entry, run *before* that target's own ordered
///    pre-lock + upsert/delete below: a truncate-bearing batch always seals
///    with `bucket_count = 1` (`seal::seal_phase1`) and is drained under
///    `next_claimable_segment`'s barrier (no predecessor or successor
///    segment concurrently draining the same target), so within this one
///    transaction "clear, then write" is exactly what makes a same-batch
///    post-truncate insert (already computed into `plan.targets` by
///    [`compute`]) survive, while anything the truncate is meant to erase
///    does not.
/// 3. **The ordered pre-lock + upsert/delete**, per target table, via
///    [`apply_target`].
/// 4. **Downstream propagation**: for every physically-changed key (write
///    or delete — no-op-suppressed writes don't count) in a target table at
///    least one definition currently reads, stages a `Recompute` row at
///    `hop_gen + 1`, enforcing [`MAX_HOP_GEN`] first. Whether a target has
///    downstream readers was decided back in Phase 2
///    ([`ApplyPlan::downstream_readers`]), not re-checked here: Phase 3
///    holds no pool connection, only `txn`, and re-deriving "does anything
///    read this table" is a catalog read like the ones Phase 2 already did
///    for evaluation. A definition created between Phase 2 and this commit
///    that starts reading a target for the first time is not missed
///    forever — definition creation is responsible for backfilling its own
///    new consumer against current target state, a separate concern from
///    this batch's propagation.
/// 5. **The completion statement**: deletes this claim's `seg_claims` rows
///    and ORs their buckets into `segments.drained_mask`, flipping
///    `state` to `'drained'` once every bucket has drained — one statement,
///    so "this claim released" and "its buckets marked drained" can never
///    observably happen one without the other. Empty `DELETE ... RETURNING`
///    means the claim was already gone — [`ApplyError::ClaimLost`].
/// 6. A `pg_notify` on `wake_channel`, for anything awaiting convergence.
///
/// A thin single-segment wrapper over [`apply_and_mark_drained_many`] (issue
/// #63 Milestone 2) — every step below is shared verbatim with the
/// multi-segment path; this function exists only to keep the pre-#63 public
/// signature (and the every-`drain_once`-drains-exactly-one-segment
/// contract every existing caller and test relies on) unchanged.
pub async fn apply_and_mark_drained(
    txn: &Transaction<'_>,
    seg_seq: i64,
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ApplyOutcome, ApplyError> {
    let outcome =
        apply_and_mark_drained_many(txn, &[seg_seq], claimed_by, plan, wake_channel).await?;
    Ok(ApplyOutcome {
        keys_written: outcome.keys_written,
        keys_deleted: outcome.keys_deleted,
        batch_drained: outcome.segments_drained[0].1,
    })
}

/// The [`apply_and_mark_drained`] steps generalized over `seg_seqs` — issue
/// #63 Milestone 2's segment-coalescing seam. `plan` (Phase 2's output) was
/// computed once over every coalesced segment's *merged* folded changes
/// ([`super::fold::merge_folded_changes`]), so steps 1-4 below (version
/// fence, truncate clears, ordered writes, downstream propagation) already
/// run exactly once for the whole batch — that sharing *is* the milestone's
/// win, collapsing what used to be one such pass per sealed segment into
/// one pass for however many sealed segments this call coalesces. Only step
/// 5 (completion) is inherently per-segment: each `seg_seq` in `seg_seqs`
/// has its own `seg_claims` rows and its own `drained_mask`, so "this
/// claim's buckets are drained" must still be recorded once per segment,
/// all in this same transaction — the one place this function's cost still
/// scales with segment count, and it is O(1) per segment (no source-table
/// work), unlike the passes above it.
///
/// `seg_seqs` must be the segments this call actually holds at least one
/// claimed bucket on (never a segment this worker claimed nothing from —
/// see [`drain_many`]'s `owned` filtering), and, since [`ApplyPlan::versions`]
/// etc. are shared across all of them, must never mix a truncate-bearing
/// segment with any other (see [`next_claimable_segments`]'s barrier).
pub async fn apply_and_mark_drained_many(
    txn: &Transaction<'_>,
    seg_seqs: &[i64],
    claimed_by: &str,
    plan: &ApplyPlan,
    wake_channel: &str,
) -> Result<ManyApplyOutcome, ApplyError> {
    // 1. Version fence. `source_key` is bare (see `catalog_source_key`'s doc
    // comment); `source_table_versions.source_table` is qualified as of
    // issue #72, so this matches against its bare table-name suffix, same
    // as `defs::source_table_version`'s own read — see that function's doc
    // comment for why issue #73 doesn't retire this (short version:
    // `source_key` still traces back to `ddl::neighbor_table_name`, which
    // stays bare regardless; only issue #75's emission audit would let this
    // go back to an exact match).
    for (source_key, loaded_version) in &plan.versions {
        let row = txn
            .query_opt(
                "select version from source_table_versions \
                 where split_part(source_table, '.', 2) = $1 for share",
                &[source_key],
            )
            .await?;
        let current: Option<i64> = row.map(|r| r.get(0));
        if current != *loaded_version {
            return Err(ApplyError::VersionFenceMiss {
                src_table: source_key.clone(),
            });
        }
    }

    // 1b. Issue #16: park this batch's own folded contribution for every
    // already-poisoned key it's excluding, before the drained mark below —
    // "parked work is the source of truth" means every excluding batch must
    // do this itself, in the same transaction, not just the batch that
    // caused the eviction. See `quarantine::park_batch_contribution`'s doc
    // comment for why this runs unconditionally rather than only on the
    // batch that tripped the threshold. Attributed to the lowest (earliest)
    // of the coalesced segments — `poison_held`'s `seg_seq` is audit
    // bookkeeping ("which batch's contribution is this"), not something
    // later correctness depends on picking exactly right among several
    // equally-valid coalesced segments.
    quarantine::park_batch_contribution(txn, seg_seqs[0], &plan.poisoned_park).await?;

    let mut keys_written = 0usize;
    let mut keys_deleted = 0usize;
    // `changed` accumulates rather than overwrites per target (`extend`,
    // not `insert`): a target can appear in both `plan.clears` and
    // `plan.targets` in the same batch — a truncate clear followed by a
    // same-batch post-truncate write to the same target — and both halves'
    // physically-touched keys must propagate downstream. The third tuple
    // element (see [`ChangedKey`]) is `src_changed` (issues #51/#52's
    // multi-hop gap), carried into the `Recompute` row step 4 stages for
    // this key, so a downstream hop reached purely through automatic
    // propagation still traces back to a real origin.
    let mut changed: HashMap<&str, Vec<ChangedKey>> = HashMap::new();

    // 2. Truncate clears, before this target's own upsert/delete below —
    // see this function's doc comment on why "clear, then write" is safe
    // here specifically (single-bucket batch, barrier-drained).
    for (target, clear) in &plan.clears {
        let pk_ident = quote_ident(&clear.pk.name);
        let target_ident = ddl::qualified_target_table_ident(&clear.qualified_target);
        let cleared: Vec<String> = txn
            .query(
                &format!("delete from {target_ident} returning {pk_ident}::text as pk"),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        keys_deleted += cleared.len();
        if cleared.is_empty() {
            continue;
        }
        let touched: Vec<ChangedKey> = cleared
            .into_iter()
            .map(|k| (k, clear.hop_gen, clear.src_changed))
            .collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 2b. Aggregate truncate clears — see [`ApplyPlan::aggregate_clears`]'s
    // doc comment on why these are a plain full-table delete with no
    // downstream propagation, unlike every other clear/write/delete this
    // function tracks via `changed`.
    for clear in plan.aggregate_clears.values() {
        let target_ident = ddl::qualified_target_table_ident(&clear.qualified_target);
        let cleared = txn
            .execute(&format!("delete from {target_ident}"), &[])
            .await?;
        keys_deleted += cleared as usize;
    }

    // 3. Ordered pre-lock + upsert/delete, per target table.
    for (target, target_plan) in &plan.targets {
        let (written, deleted) = apply_target(txn, target_plan).await?;
        keys_written += written.len();
        keys_deleted += deleted.len();

        if written.is_empty() && deleted.is_empty() {
            continue;
        }

        let mut hop_gen_of: HashMap<&str, i32> = HashMap::new();
        let mut src_changed_of: HashMap<&str, Option<std::time::SystemTime>> = HashMap::new();
        for w in &target_plan.writes {
            hop_gen_of.insert(w.pk_text.as_str(), w.hop_gen);
            src_changed_of.insert(w.pk_text.as_str(), w.src_changed);
        }
        for d in &target_plan.deletes {
            hop_gen_of.insert(d.pk_text.as_str(), d.hop_gen);
            src_changed_of.insert(d.pk_text.as_str(), d.src_changed);
        }

        let touched: Vec<ChangedKey> = written
            .into_iter()
            .chain(deleted)
            .map(|key| {
                let hop_gen = hop_gen_of.get(key.as_str()).copied().unwrap_or(0);
                let src_changed = src_changed_of.get(key.as_str()).copied().flatten();
                (key, hop_gen, src_changed)
            })
            .collect();
        changed.entry(target.as_str()).or_default().extend(touched);
    }

    // 3b. Aggregate targets: same ordered-write step as 3, above, for
    // [`KeySpace::Aggregate`] definitions — see `apply_aggregate`'s doc
    // comment for the per-group delta/probe logic itself. Written/deleted
    // groups fold into the same `changed` accounting as the 1-1 case, so
    // downstream propagation below needs no branching of its own. This
    // stages Recompute rows keyed by the encoded composite group key, same
    // as any 1-1 target — see [`ApplyPlan::aggregate_clears`]'s doc comment
    // for why that is not a live misuse risk today: no definition reading
    // from an aggregate target can actually survive its first drain attempt.
    // `src_changed` is always `None` here (rather than threaded from
    // `apply_aggregate::AggregateTargetPlan`, out of scope for issues
    // #51/#52's fix — see this module's `apply_aggregate` submodule, whose
    // group written/deleted shape carries no origin today): a moot gap, not
    // a live one, for the exact same reason `aggregate_clears` already
    // documents — no definition can actually survive its first drain attempt
    // reading from an aggregate target's composite key, so the `Recompute`
    // rows staged from this branch never reach a real evaluator anyway.
    for (target, agg_plan) in &plan.aggregate_targets {
        // `&agg_plan.target` (issue #73's persisted identity), not the bare
        // `target` map key — see `AggregateTargetPlan::target`'s doc
        // comment. `target` itself stays bare here purely as the
        // `changed`/`downstream_readers` bookkeeping key below.
        let result =
            apply_aggregate::apply_aggregate_target(txn, &agg_plan.target, agg_plan).await?;
        keys_written += result.written.len();
        keys_deleted += result.deleted.len();

        if result.written.is_empty() && result.deleted.is_empty() {
            continue;
        }
        changed.entry(target.as_str()).or_default().extend(
            result
                .written
                .into_iter()
                .chain(result.deleted)
                .map(|(key, hop_gen)| (key, hop_gen, None)),
        );
    }

    // 4. Downstream propagation, with the hop bound checked before staging
    // anything.
    let mut recompute_changes = Vec::new();
    let mut hop_bound_tables = Vec::new();
    let mut worst_hop_gen = 0;
    for (target, touched) in &changed {
        if !plan
            .downstream_readers
            .get(*target)
            .copied()
            .unwrap_or(false)
        {
            continue;
        }
        for (key, hop_gen, src_changed) in touched {
            let next_hop = hop_gen + 1;
            if next_hop > MAX_HOP_GEN {
                hop_bound_tables.push(target.to_string());
                worst_hop_gen = worst_hop_gen.max(next_hop);
                continue;
            }
            recompute_changes.push(StagedChange::Recompute {
                src_table: target.to_string(),
                key: key.clone(),
                hop_gen: next_hop,
                group_key: None,
                src_changed: *src_changed,
            });
        }
    }

    // Reverse recompute (issue #30): from-side rows a changed related row must
    // re-derive, resolved in Phase 2 and staged here as ordinary image-less
    // recomputes — the same shape and same hop bound forward propagation uses,
    // just keyed by the from-side table/PK rather than a touched target key.
    for (from_table, key, hop_gen, src_changed) in &plan.reverse_recomputes {
        if *hop_gen > MAX_HOP_GEN {
            hop_bound_tables.push(from_table.clone());
            worst_hop_gen = worst_hop_gen.max(*hop_gen);
            continue;
        }
        recompute_changes.push(StagedChange::Recompute {
            src_table: from_table.clone(),
            key: key.clone(),
            hop_gen: *hop_gen,
            group_key: None,
            src_changed: *src_changed,
        });
    }

    if !hop_bound_tables.is_empty() {
        hop_bound_tables.sort();
        hop_bound_tables.dedup();
        return Err(ApplyError::HopBoundExceeded {
            hop_gen: worst_hop_gen,
            tables: hop_bound_tables,
        });
    }

    append::append(txn, &recompute_changes).await?;

    // 4b. Issue #16: a clean drain clears the death counters for every key
    // it just applied (not the poisoned ones it parked above) — doc 06's
    // "clean drain clears counters for keys it applied."
    quarantine::clear_key_deaths(txn, &plan.applied_keys).await?;

    // 5. Completion: release each coalesced segment's claim and mark its
    // buckets drained, in one statement per segment — inherently per-segment
    // (each has its own `seg_claims` rows and `drained_mask`), unlike steps
    // 1-4 above, which already ran once for the whole coalesced batch.
    let mut segments_drained = Vec::with_capacity(seg_seqs.len());
    for &seg_seq in seg_seqs {
        let bucket_count: i16 = txn
            .query_one(
                "select bucket_count from segments where seg_seq = $1",
                &[&seg_seq],
            )
            .await?
            .get(0);

        let claimed_buckets: Vec<i16> = txn
            .query(
                "delete from seg_claims where seg_seq = $1 and claimed_by = $2 returning bucket",
                &[&seg_seq, &claimed_by],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();

        if claimed_buckets.is_empty() {
            return Err(ApplyError::ClaimLost);
        }

        let mut mask: i64 = 0;
        for bucket in &claimed_buckets {
            mask |= 1i64 << bucket;
        }
        let full_mask: i64 = (1i64 << bucket_count) - 1;

        let completed = txn
            .query_opt(
                "update segments \
                 set drained_mask = drained_mask | $2::bigint, \
                     state = case when (drained_mask | $2::bigint) = $3::bigint \
                                  then 'drained' else state end \
                 where seg_seq = $1 and state = 'draining' \
                 returning state",
                &[&seg_seq, &mask, &full_mask],
            )
            .await?;
        let batch_drained = matches!(
            completed.map(|row| row.get::<_, String>(0)),
            Some(state) if state == "drained"
        );
        segments_drained.push((seg_seq, batch_drained));
    }

    // 6. Wake anything awaiting convergence.
    txn.execute("select pg_notify($1, '')", &[&wake_channel])
        .await?;

    Ok(ManyApplyOutcome {
        keys_written,
        keys_deleted,
        segments_drained,
    })
}

/// What one successful [`apply_and_mark_drained`] call did: how many target
/// rows it physically wrote/deleted (no-op-suppressed writes excluded), and
/// whether this call's completion flipped the segment to `'drained'`
/// (`false` if other buckets are still outstanding).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub batch_drained: bool,
}

/// What one successful [`apply_and_mark_drained_many`] call did — the
/// coalesced-segment counterpart to [`ApplyOutcome`] (issue #63 Milestone
/// 2): the same physically-written/deleted key counts, now totalled across
/// every segment this call drained from, plus each individual segment's own
/// `(seg_seq, batch_drained)` completion result — a coalesced call can flip
/// some of its segments to `'drained'` while leaving others still short a
/// peer's bucket, exactly as any one of them would on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManyApplyOutcome {
    pub keys_written: usize,
    pub keys_deleted: usize,
    pub segments_drained: Vec<(i64, bool)>,
}

// ---------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------

/// The most drain attempts [`drain_once`] retries before giving up and
/// surfacing the last error — a bound on a fence-miss/serialization-failure
/// loop that never resolves (e.g. a source under constant, colliding
/// definition churn), rather than retrying forever.
const MAX_APPLY_ATTEMPTS: u32 = 5;

/// Runs one full drain attempt against `seg_seq`: Phase 1 (claim + fold, in
/// one short transaction), Phase 2 (compute), and Phase 3 (apply ∪
/// mark-drained), retrying Phase 2+3 on a version-fence miss or a
/// serialization failure/deadlock — the design doc's "reload, recompute,
/// retry" loop — using [`FenceMissBackoff`] between attempts.
///
/// Returns `Ok(None)` if this call's claim won (and already owned) nothing
/// — the buckets were all already claimed by someone else — without
/// folding or computing anything. Otherwise returns the winning attempt's
/// [`ApplyOutcome`].
pub async fn drain_once(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
) -> Result<Option<ApplyOutcome>, ApplyError> {
    let mut folded = {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        claim::claim(&*txn, seg_seq, claimed_by, live_workers).await?;
        let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by).await?;
        if filter.is_empty() {
            txn.commit().await?;
            return Ok(None);
        }
        let folded = fold::fold(&txn, seg_seq, filter).await?;
        txn.commit().await?;
        folded
    };

    let mut backoff = FenceMissBackoff::new();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let plan = match compute(pool, &folded).await {
            Ok(plan) => plan,
            Err(ApplyError::SourceTableDropped { source_table }) => {
                // Issue #16's one sanctioned exception to immutability: the
                // table this batch's folded rows name is gone, not any one
                // row's fault, so no retry or per-key quarantine resolves
                // it. Purge every ring/quarantine row naming it and retry
                // with it excluded. Not counted against
                // `MAX_APPLY_ATTEMPTS` — this corrects `folded` itself
                // rather than retrying the same input.
                quarantine::purge_dropped_table(pool, &source_table).await?;
                folded.retain(|c| c.src_table != source_table);
                attempt -= 1;
                continue;
            }
            // Every other Phase 2 failure (an evaluator error against a
            // malformed staged image is the common case) goes through the
            // exact same classification the Phase 3 branch below uses — a
            // bad key's image fails `compute()` for the whole batch just as
            // surely as it would fail Phase 3, and isolation must attribute
            // it the same way regardless of which phase first tripped over
            // it.
            Err(err) => {
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        match apply_and_mark_drained(&txn, seg_seq, claimed_by, &plan, wake_channel).await {
            Ok(outcome) => {
                txn.commit().await?;
                backoff.reset();
                return Ok(Some(outcome));
            }
            Err(err) => {
                let _ = txn.rollback().await;
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
            }
        }
    }
}

/// The most sealed segments [`drain_many`] will coalesce into a single
/// compute-and-apply pass. A burst of incremental writes seals a new
/// segment roughly every 300ms (`ClientOptions::maintenance_interval`'s
/// default); this bounds how much of that backlog one drain call takes on
/// at once, so a very long burst still drains in several coalesced calls
/// rather than one unbounded one holding a single transaction (and its
/// locks) open over an ever-growing plan.
pub const MAX_COALESCE_SEGMENTS: usize = 32;

/// [`drain_once`] generalized over more than one sealed segment (issue #63
/// Milestone 2): claims and folds every segment in `seg_seqs` in one short
/// transaction (Phase 1), merges their folded changes into one
/// [`fold::merge_folded_changes`] list, then runs Phase 2 (compute) and
/// Phase 3 (apply ∪ mark-drained, via [`apply_and_mark_drained_many`])
/// exactly *once* over the merged list — collapsing what would have been
/// one full compute-and-apply pass per segment (each with its own version
/// fence read, its own ordered pre-lock/upsert, its own forced-group
/// bulk-recompute for any [`crate::defs::ast::KeySpace::Aggregate`] target,
/// and its own downstream-propagation staging) into one such pass for the
/// whole batch.
///
/// `seg_seqs` should come from [`next_claimable_segments`], which already
/// enforces the invariant this function relies on but does not itself
/// re-check: never mix a truncate-bearing segment with any other (a
/// truncate is drained alone — see that function's own doc comment on the
/// barrier). `seg_seqs` need not be claimable in full — a segment every one
/// of whose buckets a peer already holds simply contributes nothing and is
/// dropped before Phase 2 runs (mirroring [`drain_once`]'s `filter.is_empty()`
/// short-circuit, just per-segment instead of for the one segment it has).
///
/// Returns `Ok(None)` if this call's claims won nothing at all across every
/// segment in `seg_seqs` (every bucket of every one of them was already
/// claimed by a peer). Otherwise returns the winning attempt's
/// [`ManyApplyOutcome`], covering only the segments this call actually
/// claimed at least one bucket from — never a segment it claimed nothing
/// on, which [`apply_and_mark_drained_many`]'s completion step would
/// otherwise misreport as [`ApplyError::ClaimLost`].
pub async fn drain_many(
    pool: &Pool,
    seg_seqs: &[i64],
    claimed_by: &str,
    live_workers: i64,
    wake_channel: &str,
) -> Result<Option<ManyApplyOutcome>, ApplyError> {
    if seg_seqs.is_empty() {
        return Ok(None);
    }

    let (mut folded, owned_segments) = {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let mut per_segment: Vec<Vec<FoldedChange>> = Vec::with_capacity(seg_seqs.len());
        let mut owned: Vec<i64> = Vec::with_capacity(seg_seqs.len());
        for &seg_seq in seg_seqs {
            claim::claim(&*txn, seg_seq, claimed_by, live_workers).await?;
            let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by).await?;
            if filter.is_empty() {
                continue;
            }
            owned.push(seg_seq);
            per_segment.push(fold::fold(&txn, seg_seq, filter).await?);
        }
        txn.commit().await?;
        if owned.is_empty() {
            return Ok(None);
        }
        (fold::merge_folded_changes(per_segment), owned)
    };

    // Every retry-classification helper below (`classify_and_retry`,
    // `isolate_and_evict`) takes one representative `seg_seq` purely as
    // audit/probe bookkeeping (which batch's contribution a parked poison
    // row names; which real claim a rollback-only probe transaction's
    // completion step exercises) — never as something correctness depends
    // on picking exactly right among several equally-valid coalesced
    // segments. The lowest of this call's owned segments is as good a
    // representative as any; see `apply_and_mark_drained_many`'s doc
    // comment on the same choice for `poisoned_park`.
    let representative_seg_seq = owned_segments[0];

    let mut backoff = FenceMissBackoff::new();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let plan = match compute(pool, &folded).await {
            Ok(plan) => plan,
            Err(ApplyError::SourceTableDropped { source_table }) => {
                quarantine::purge_dropped_table(pool, &source_table).await?;
                folded.retain(|c| c.src_table != source_table);
                attempt -= 1;
                continue;
            }
            Err(err) => {
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    representative_seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
                continue;
            }
        };

        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        match apply_and_mark_drained_many(&txn, &owned_segments, claimed_by, &plan, wake_channel)
            .await
        {
            Ok(outcome) => {
                txn.commit().await?;
                backoff.reset();
                return Ok(Some(outcome));
            }
            Err(err) => {
                let _ = txn.rollback().await;
                if let Some(retry_folded) = classify_and_retry(
                    pool,
                    representative_seg_seq,
                    claimed_by,
                    wake_channel,
                    &folded,
                    attempt,
                    &mut backoff,
                    err,
                )
                .await?
                {
                    folded = retry_folded;
                }
            }
        }
    }
}

/// Classifies `err` (per [`quarantine::classify`]) and either retries or
/// propagates, shared by both [`drain_once`] and [`drain_many`]'s Phase 2
/// and Phase 3 failure arms so a bad key is attributed identically
/// regardless of which phase — or which of the two orchestrators —
/// first surfaced it.
///
/// Returns `Ok(Some(retry_folded))` if isolation evicted at least one key —
/// the caller must retry with `folded` replaced by `retry_folded`.
/// `Ok(None)` means "retry with `folded` unchanged" (a version fence
/// miss, a transient failure, or an isolate attempt that evicted nothing).
/// `Err(_)` propagates `err` (or a probe's own halting error) unmodified,
/// once retries are exhausted or the failure must never be retried at all.
#[allow(clippy::too_many_arguments)]
async fn classify_and_retry(
    pool: &Pool,
    seg_seq: i64,
    claimed_by: &str,
    wake_channel: &str,
    folded: &[FoldedChange],
    attempt: u32,
    backoff: &mut FenceMissBackoff,
    err: ApplyError,
) -> Result<Option<Vec<FoldedChange>>, ApplyError> {
    match quarantine::classify(&err) {
        // Version fence miss: reload schema and retry, backing off only on
        // consecutive misses — `FenceMissBackoff` is exactly that state
        // machine, reused as-is.
        quarantine::FailureClass::VersionFenceMiss => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                return Err(err);
            }
            let delay = backoff.next_delay();
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(None)
        }
        // Transient (lock contention, serialization failure, dropped
        // connection, statement timeout): retry, charge nothing, no backoff
        // — `FenceMissBackoff`'s escalating schedule is reserved for
        // consecutive fence misses specifically (doc 06).
        quarantine::FailureClass::Transient => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                return Err(err);
            }
            Ok(None)
        }
        // Halting schema diagnosis: never quarantine, propagate loudly
        // after recording the stop metric.
        quarantine::FailureClass::Halting => {
            quarantine::record_halting_stop(pool, &err.to_string()).await?;
            Err(err)
        }
        // Everything else: isolate each folded record alone to attribute
        // the failure to specific key(s), evicting any past the death
        // threshold and retrying without them. If nothing reproduces alone,
        // the error is surfaced, not blamed.
        quarantine::FailureClass::Isolate => {
            if attempt >= MAX_APPLY_ATTEMPTS {
                return Err(err);
            }
            match quarantine::isolate_and_evict(
                pool,
                seg_seq,
                claimed_by,
                wake_channel,
                folded,
                quarantine::DEFAULT_DEATH_THRESHOLD,
            )
            .await?
            {
                Some(retry_folded) => Ok(Some(retry_folded)),
                None => Err(err),
            }
        }
    }
}

/// The next batch a free worker should pick up: the lowest-`seg_seq`
/// segment that is `'sealed'` or `'draining'` and not yet fully drained
/// (`drained_mask` short of `(1 << bucket_count) - 1`). Ordered ascending
/// so batches drain roughly in creation order, though nothing here enforces
/// that strictly — a worker could still be mid-drain on an earlier segment
/// while this returns a later one.
///
/// The one exception is the truncate barrier (issue #60): a truncate is
/// whole-keyspace, but drains are per-bucket, parallel, and — per the
/// paragraph above — explicitly *not* ordered, so a truncate is a
/// two-directional drain barrier. Predecessors must drain first (else an
/// earlier batch's insert would apply after the truncate and wrongly
/// survive); successors must not drain first (else a later batch's
/// post-truncate insert would be wiped when the truncate's clear runs). Let
/// `B` be the lowest `seg_seq` among undrained truncate-bearing segments
/// (`segments.has_truncate`, set at seal time — see `seal::seal_phase1`);
/// this query never returns a segment past `B`. Because this query always
/// returns the *lowest* eligible `seg_seq`, `B` itself is only ever handed
/// out once every segment below it has drained — one clause gives both
/// directions of the barrier.
pub async fn next_claimable_segment(
    client: &impl GenericClient,
) -> Result<Option<i64>, ApplyError> {
    Ok(next_claimable_segments(client, 1).await?.into_iter().next())
}

/// [`next_claimable_segment`] generalized to return up to `max_batch`
/// claimable segments at once (issue #63 Milestone 2), for [`drain_many`] to
/// coalesce — the batch a burst of quickly-sealing segments needs so each
/// one doesn't pay its own full compute-and-apply pass.
///
/// Runs the exact same barrier-respecting query [`next_claimable_segment`]
/// does (see its doc comment for the truncate barrier `B`), just without
/// `next_claimable_segment`'s `limit 1`. The only additional rule this adds
/// is the one [`drain_many`]'s doc comment calls out as its caller-side
/// invariant: **a truncate-bearing segment is never coalesced with another
/// segment.** Because `B` is by definition the *lowest* seg_seq among
/// undrained truncate-bearing segments and this query never returns
/// anything past `B`, the only truncate-bearing segment that can ever
/// appear in the result set is `B` itself, and — being the barrier's own
/// upper bound — it is always the *last* (highest-`seg_seq`) row, never the
/// first. So: walk the ascending rows, taking ordinary (non-truncate)
/// segments into the batch; the moment a truncate-bearing row is reached,
/// stop — returning it alone if the batch collected so far is otherwise
/// empty (it's the lowest claimable segment, so it must be handed out on
/// its own), or returning what's already been collected without it
/// otherwise (it'll be handed out alone on some future call, once nothing
/// ordinary remains ahead of it).
pub async fn next_claimable_segments(
    client: &impl GenericClient,
    max_batch: usize,
) -> Result<Vec<i64>, ApplyError> {
    if max_batch == 0 {
        return Ok(Vec::new());
    }
    let limit = max_batch as i64;
    let rows = client
        .query(
            "select seg_seq, has_truncate from segments \
             where state in ('sealed', 'draining') \
               and drained_mask <> ((1::bigint << bucket_count) - 1) \
               and seg_seq <= coalesce( \
                   (select min(seg_seq) from segments \
                    where has_truncate \
                      and drained_mask <> ((1::bigint << bucket_count) - 1)), \
                   seg_seq \
               ) \
             order by seg_seq asc \
             limit $1",
            &[&limit],
        )
        .await?;

    let mut batch = Vec::with_capacity(rows.len());
    for row in rows {
        let seg_seq: i64 = row.get(0);
        let has_truncate: bool = row.get(1);
        if has_truncate {
            if batch.is_empty() {
                batch.push(seg_seq);
            }
            break;
        }
        batch.push(seg_seq);
    }
    Ok(batch)
}
