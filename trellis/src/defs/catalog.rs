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
//!
//! **Source and target identity are both fully-qualified (issues #72/#73,
//! ADR-0007).** `transform_definitions.source_table`/`target_table` and
//! `source_table_versions.source_table` hold `def.source`/`def.target`
//! resolved to their `schema.table` identity exactly once, at
//! definition-acceptance time (`create_definition_inner`'s
//! [`resolve_source_schema_in_txn`] call for the source side,
//! `Config::target_schema`/`intake::publication::qualify` for the target
//! side), never the bare spelling the grammar parsed. Every read of any of
//! these columns downstream must treat the value as already-qualified and
//! must not re-resolve it — see [`resolve_source_schema_in_txn`]'s own doc
//! comment for exactly why re-resolving a qualified value fails outright
//! rather than merely being redundant. A handful of read sites deliberately
//! stay bare regardless — `column_dependents`, `definition_by_target`, and
//! every `app.rs` read reachable through `docs/decisions/0003`'s
//! `transform.column` addressing scheme — because their callers only ever
//! have the bare name the grammar accepts back (issue #76 hasn't landed
//! qualified-target syntax) or because re-exposing the qualified spelling
//! through that addressing scheme would misparse a real transform address as
//! a column one (see each function's own doc comment); these match
//! `target_table`'s bare table-name suffix via `split_part` rather than the
//! qualified column directly. `schema_nodes`/`schema_edges` and
//! `relationship_definitions`' endpoints are the one place this module still
//! keys on the bare name across the board — that migration is issue #74's,
//! not this one's (see the `TODO(#74)`s at `create_definition_inner`'s
//! node/edge resolution, which now also explains why qualifying only
//! *this* function's two `resolve_node_in_txn` calls wouldn't actually be
//! safe ahead of #74).
//!
//! **Bare target-table suffixes are still enforced globally unique, just no
//! longer by `target_table`'s own `unique` constraint.** Qualifying
//! `target_table` (#73) narrowed that constraint to the qualified spelling
//! only, which would otherwise let e.g. `public.foo` and `custom.foo`
//! coexist as two live definitions — exactly the ambiguity every
//! `split_part`-based bare-suffix read site above assumes can't happen.
//! `create_definition_inner` re-closes that gap itself, at write time,
//! rejecting a new definition with [`CatalogError::TargetTableSuffixCollision`]
//! if its qualified target would collide with another live definition's
//! bare suffix under a different schema — see that check's own comment for
//! why this is provisional pending issue #76. This is now double-enforced,
//! not merely application-level: `transform_definitions_target_suffix_idx`
//! (`V23__transform_definitions_target_suffix_idx.sql`) is a real Postgres
//! expression unique index on `split_part(target_table, '.', 2)`, the same
//! DB-level backstop `target_table`'s own `unique` constraint already is for
//! exact-qualified-name collisions — it closes the race two concurrent
//! `create_definition` calls could otherwise win against each other's
//! same-transaction-invisible, still-uncommitted inserts. The app-level
//! check stays the primary path (a typed, name-carrying error beats a raw
//! constraint violation for the common, non-racing case); the rare
//! insert-time failure that check can't see is caught and translated back
//! into the same [`CatalogError::TargetTableSuffixCollision`] rather than
//! surfacing as an opaque [`CatalogError::Db`].

use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::error_code::{self, ErrorCode};
use crate::pool::Pool;

use super::ast::{Expr, KeySpace, RelationshipDef, TransformDef, ValueType};
use super::backfill::{self, BackfillError};
use super::chunk_queue;
use super::ddl::{self, DdlError};
use super::error::ParseError;
use super::model::{
    Definition, EdgeKind, NodeKind, RelationshipCardinality, RelationshipDefinition, SchemaEdge,
    SchemaNode, TransformStatus,
};
use super::parser::{parse, parse_relationship};
use super::validate::{
    RelationshipTypeMismatch, RelationshipWarning, ResolvedRelationship, ValidationError, validate,
};

/// Why creating or reading a definition failed. [`CatalogError::code`]
/// reports a stable, coarse [`ErrorCode`] category for this error alongside
/// its `Display` message — see `docs/decisions/0008-public-api-design.md`, decision 3.
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
    /// This definition's resolved, qualified target (`{target_schema}.{def.target}`)
    /// shares a bare table-name suffix with a *different* qualified target
    /// some other still-persisted definition already uses — e.g.
    /// `public.foo` alongside `custom.foo`, most plausibly from
    /// `Config::target_schema` changing between deploys and a `TRANSFORM
    /// ... TARGET foo` being redeclared under it. Issue #73 made
    /// `transform_definitions.target_table` store the qualified spelling, so
    /// `target_table text not null unique` (`V2__transform_catalog.sql`)
    /// only enforces uniqueness of *that* spelling now, not the bare suffix
    /// it used to store outright — checked and rejected here, in
    /// `create_definition_inner`, rather than at any individual read site,
    /// because every `split_part(target_table, '.', 2)`-keyed reader
    /// (`definition_by_target`, `dependents_of`, `app.rs`'s status/
    /// quarantine polls, `generative`'s `unsettled_definitions`) assumes
    /// that suffix is globally unique and has no way to safely cope with two
    /// definitions colliding under it — see this variant's `Display` message
    /// for the operator-facing explanation.
    ///
    /// Double-enforced as of the reviewer follow-up to issue #73: this
    /// application-level pre-check is still the primary path (it reports a
    /// clean, typed error naming both spellings — see
    /// `docs/decisions/0008-public-api-design.md` on why that beats a raw
    /// Postgres unique-violation for callers), but it only ever sees its own
    /// transaction's snapshot, so two concurrent `create_definition` calls
    /// resolving different-schema targets for the same bare suffix could
    /// each pass it and both commit. `transform_definitions_target_suffix_idx`
    /// (`V23__transform_definitions_target_suffix_idx.sql`) is the real
    /// DB-level guard that closes that race; `create_definition_inner`
    /// catches the rare insert-time unique-violation against it and
    /// translates it into this same variant (with `existing: None` — see the
    /// field's own doc comment) rather than letting it surface as a raw
    /// [`CatalogError::Db`].
    ///
    /// Provisional: issue #76 will teach the grammar an explicit
    /// `schema.table` spelling for `FROM`/`TARGET`, at which point an
    /// operator will be able to unambiguously address `custom.foo` as
    /// distinct from `public.foo` and this restriction may need to relax (or
    /// a different addressing scheme adopted) — until then, every bare-
    /// suffix reader above still only ever has the bare name to key off of,
    /// so disallowing the collision outright is the only choice that
    /// doesn't quietly corrupt one of them.
    ///
    /// Raised from two different places, both folding into this one variant
    /// since callers only need one type to match on: `create_definition_inner`'s
    /// pre-check (`existing: Some(_)`, the common case — the colliding row is
    /// still visible in this transaction's own snapshot, so its qualified
    /// spelling can be reported) and a genuine insert-time race against
    /// `transform_definitions_target_suffix_idx`
    /// (`V23__transform_definitions_target_suffix_idx.sql`, `existing: None`
    /// — a concurrent transaction's insert that the pre-check's snapshot
    /// couldn't see committed first, so by the time this transaction's own
    /// insert fails on the index, it has no further query available inside
    /// its now-aborted transaction to learn what it lost to).
    TargetTableSuffixCollision {
        /// The bare table-name suffix both spellings share.
        target: String,
        /// The qualified spelling this rejected definition resolved to.
        requested: String,
        /// The qualified spelling already persisted by another live
        /// definition under the same bare suffix, when known. `None` only
        /// for the insert-time race path above, where the aborted
        /// transaction has no way left to look it up.
        existing: Option<String>,
    },
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

impl CatalogError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` wherever
    /// one nests here ([`CatalogError::Parse`], [`CatalogError::Validate`],
    /// [`CatalogError::Pool`], [`CatalogError::Backfill`],
    /// [`CatalogError::ReplicaIdentityRequired`], [`CatalogError::Ddl`],
    /// [`CatalogError::DirectBackfill`]) rather than hardcoding one category
    /// for a whole variant — so, for instance, a
    /// [`CatalogError::Validate`]`(`[`ValidationError::DuplicateRelationshipName`]`)`
    /// still reports [`ErrorCode::Conflict`], not [`ErrorCode::Validation`].
    pub fn code(&self) -> ErrorCode {
        match self {
            CatalogError::Parse(err) => err.code(),
            CatalogError::Validate(err) => err.code(),
            CatalogError::Db(err) => error_code::classify_pg_error(err),
            CatalogError::Pool(err) => err.code(),
            // Persisted data corruption — written by something other than
            // this module's own writer.
            CatalogError::UnknownValueType { .. } => ErrorCode::Internal,
            CatalogError::Backfill(err) => err.code(),
            CatalogError::SourceTableNotFound(_) => ErrorCode::NotFound,
            // Collides with existing state (another live definition's
            // persisted target), not a structural/semantic rejection of this
            // definition's own text — the same category
            // `ValidationError::DuplicateRelationshipName` reports.
            CatalogError::TargetTableSuffixCollision { .. } => ErrorCode::Conflict,
            CatalogError::ReplicaIdentityRequired(err) => err.code(),
            CatalogError::Ddl(err) => err.code(),
            CatalogError::DirectBackfill(err) => err.code(),
        }
    }
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
            CatalogError::TargetTableSuffixCollision {
                target,
                requested,
                existing: Some(existing),
            } => write!(
                f,
                "target table \"{target}\" is ambiguous: this definition would persist \
                 \"{requested}\", but \"{existing}\" already exists under the same bare \
                 table name — two definitions cannot share a bare target-table name under \
                 different schemas"
            ),
            CatalogError::TargetTableSuffixCollision {
                target,
                requested,
                existing: None,
            } => write!(
                f,
                "target table \"{target}\" is ambiguous: this definition would persist \
                 \"{requested}\", but another definition was concurrently created under the \
                 same bare table name — two definitions cannot share a bare target-table \
                 name under different schemas"
            ),
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
            CatalogError::TargetTableSuffixCollision { .. } => None,
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
    // `Live`, not `Backfilling`: by the time this call returns, the source
    // table's pre-existing rows are already enumerated into the ring in the
    // very same transaction the row is inserted in (see `create_definition_inner`),
    // so there's no separate, awaited step for a caller to observe this row
    // sitting through first. Only `install_definition`'s direct-build path
    // has such a step — see its own `TransformStatus::Backfilling` use.
    //
    // `pool.target_schema()`, not a parameter of this function's own (issue
    // #73): this ring-path entry point never runs target-table DDL itself
    // (its caller is assumed to have already created the physical table —
    // see this module's doc comment), so there is no sibling DDL call for a
    // separately-threaded `target_schema` argument to ever drift from. See
    // [`crate::pool::Pool::target_schema`]'s own doc comment for why reading
    // it off `pool` here is exactly as safe as `install_definition` passing
    // its own explicit argument.
    create_definition_inner(
        pool,
        source_text,
        source_columns,
        true,
        TransformStatus::Live,
        pool.target_schema(),
    )
    .await
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
    // `pool.target_schema()` — see [`create_definition`]'s own call site for
    // why this ring-path entry point reads it off `pool` rather than taking
    // its own `target_schema` parameter.
    create_definition_inner(
        pool,
        source_text,
        source_columns,
        false,
        TransformStatus::Live,
        pool.target_schema(),
    )
    .await
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
/// (see their own doc comments).
///
/// **Status lifecycle (issue #55).** Once the target exists and the coverage
/// plan is captured, the definition row is persisted *speculatively* with
/// [`TransformStatus::Backfilling`] — before [`backfill::backfill_definition`]
/// runs, not after — so a status-polling caller (the pattern issue #82's
/// public API design settles on) can observe the row the moment it exists
/// rather than only once its backfill has already finished. Three things can
/// happen next:
///
/// * The direct build succeeds: the coverage plan is committed and the row
///   is flipped to [`TransformStatus::Live`] in place (same id, same
///   `target_table`).
/// * The direct build reports [`BackfillError::Unsupported`] (this shape
///   can't be rendered directly): the speculative row is deleted and
///   [`create_definition`] runs exactly as it did before this row existed,
///   inserting its own — now `Live` — row via the ring path.  Deleting first
///   frees `target_table`'s uniqueness constraint back up; re-running
///   [`resolve_node_in_txn`]/[`persist_edge_in_txn`]/the `source_table_versions`
///   bump for the same source/target pair is harmless — nodes upsert, edges
///   dedupe on conflict, and an extra version bump only costs a downstream
///   drain worker a routine, self-healing version-fence retry (see
///   `staging::apply::ApplyError::VersionFenceMiss`).
/// * The direct build fails for a real reason: the speculative row is
///   deleted and the error propagates, matching this function's existing
///   discipline of not rolling back the target-table DDL on failure either —
///   a failed install leaves no catalog row and an unbuilt (or partially
///   built), uncatalogued target table behind either way.
///
/// **Backgrounding (docs/decisions/0007's amendment).** A plain
/// (non-relationship) `KeySpace::OneToOne` definition's backfill is no longer
/// run in-call at all: once the speculative `Backfilling` row exists, its
/// PK-range chunk boundaries are enumerated and persisted as durable
/// `backfill_chunks` work items (`chunk_queue::enqueue_one_to_one`), and this
/// function returns *before a single row of the target is built* — a running
/// drain worker (`trellis::client`'s `app_worker_loop`) claims and executes
/// those chunks independently, flipping the definition to
/// [`TransformStatus::Live`] once every one is done
/// (`chunk_queue::finish_chunk` / [`complete_direct_backfill`]). A
/// relationship-enriched 1-1 definition or an aggregate definition still runs
/// its (still fully synchronous) direct build in-call exactly as before —
/// see [`super::backfill`]'s module docs for why those two shapes aren't
/// chunked into the durable queue yet.
///
/// **The CDC race this closes.** Before this change, persisting the row (and
/// its `schema_nodes`/`schema_edges`) before the direct build completed meant
/// a running drain worker's [`transforms_for_source`] could, in principle,
/// observe this transform and attempt to apply a live CDC delta against the
/// target while the build was still writing it — corrupting a field an
/// incremental accumulator (e.g. `AVG`) folds against an existing baseline,
/// not just racing harmlessly. [`dependents_of`]/[`transforms_for_source`]
/// now filter to `status = 'live'`, so no build path (backgrounded or still
/// synchronous) can have a delta folded into it while non-`live` — see
/// [`complete_direct_backfill`] for how a delta skipped during that window is
/// recovered rather than lost once the definition does go live.
pub async fn install_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    target_schema: &str,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;

    // Validate *before* any DDL is generated or executed, for every key-space
    // (issue #94). DDL generation type-infers each target column from the same
    // expressions the validator checks, so an invalid definition reaching DDL
    // first surfaces whatever incidental error inference happens to hit —
    // masking the real validation error — and, worse, can leave a target table
    // behind for a definition that is then rejected. `create_definition`/
    // `create_definition_without_backfill` validate again below; that repeat is
    // cheap and keeps those entry points safe when called directly.
    let relationships = resolve_relationships(pool, &def).await?;
    validate(&def, source_columns, &relationships)?;

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

    if let KeySpace::OneToOne = &def.key_space
        && !backfill::uses_relationships(&def)
    {
        return install_plain_one_to_one(pool, source_text, source_columns, &def, target_schema)
            .await;
    }

    // Issue #79 (bug B): capture each table's coverage fence *before* the
    // build reads it. The fence must precede every build read — a fence taken
    // after the build could vouch for a row the build never folded (see
    // `plan_direct_backfill_coverage` / `capture_backfill_coverage_fence`).
    let coverage_plan = plan_direct_backfill_coverage(pool, &def, &relationships).await?;

    // Issue #55: persist *before* running the backfill, not after — see this
    // function's doc comment for the full status-lifecycle rationale and the
    // cleanup story for each of the three outcomes below. No ring
    // enumeration (`backfill: false`): the direct build below is what's about
    // to fold the source's pre-existing rows in.
    // `target_schema` — this function's own parameter, the exact value the
    // DDL step above just created the physical target table under — is
    // threaded straight through rather than re-derived from `pool` (contrast
    // [`create_definition`]'s call site): issue #73's persisted qualification
    // must never be able to drift from what the DDL actually built, and a
    // parameter passed through unchanged can't drift from itself the way two
    // independently-sourced values merely expected to agree theoretically
    // could.
    let mut definition = create_definition_inner(
        pool,
        source_text,
        source_columns,
        false,
        TransformStatus::Backfilling,
        target_schema,
    )
    .await?;

    match backfill::backfill_definition(pool, &def, target_schema, source_columns).await {
        Ok(()) => {
            // The build folded each planned table's pre-build contents into the
            // target. Persist that coverage *before* the definition is marked
            // live, so the redundant publication-join catch-up enumeration of
            // those tables can be skipped.
            commit_direct_backfill_coverage(pool, &coverage_plan).await?;
            mark_definition_status(pool, definition.id, TransformStatus::Live).await?;
            definition.status = TransformStatus::Live;
            Ok(definition)
        }
        Err(BackfillError::Unsupported(_)) => {
            // This shape can't be built directly after all — discard the
            // speculative row (see doc comment: safe, since the ring path
            // below recreates every one of its side effects idempotently)
            // and fall back exactly as if the speculative row never existed.
            delete_definition_row(pool, definition.id).await?;
            create_definition(pool, source_text, source_columns).await
        }
        Err(err) => {
            delete_definition_row(pool, definition.id).await?;
            Err(CatalogError::DirectBackfill(err))
        }
    }
}

/// The plain (non-relationship) `KeySpace::OneToOne` half of
/// [`install_definition`]'s dispatch (see its doc comment): persists the
/// speculative `Backfilling` row exactly as the still-synchronous shapes do,
/// then either enumerates its chunk work into the durable queue
/// (`chunk_queue::enqueue_one_to_one`) or — a plain 1-1 definition can still
/// be `Unsupported` (a cyclic cross-field-alias chain, or a substitution
/// output past [`backfill::MAX_SUBSTITUTED_NODES`]) — falls back to the ring
/// exactly like the synchronous path does. `def.target`'s table already
/// exists (the caller's DDL step); no coverage-fence bookkeeping runs here —
/// see [`chunk_queue::enqueue_one_to_one`]'s doc comment for why this path
/// doesn't bother recording `backfill_coverage` for its own source table (a
/// pure performance optimization elsewhere, never a correctness requirement).
async fn install_plain_one_to_one(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
    def: &TransformDef,
    target_schema: &str,
) -> Result<Definition, CatalogError> {
    // `target_schema` — threaded straight from `install_definition`'s own
    // parameter, the same value its DDL step already created the physical
    // target table under — same no-drift-by-construction reasoning as
    // `install_definition`'s own `create_definition_inner` call (issue #73).
    let mut definition = create_definition_inner(
        pool,
        source_text,
        source_columns,
        false,
        TransformStatus::Backfilling,
        target_schema,
    )
    .await?;

    match chunk_queue::enqueue_one_to_one(pool, definition.id, def).await {
        Ok(status) => {
            definition.status = status;
            Ok(definition)
        }
        Err(BackfillError::Unsupported(_)) => {
            delete_definition_row(pool, definition.id).await?;
            create_definition(pool, source_text, source_columns).await
        }
        Err(err) => {
            delete_definition_row(pool, definition.id).await?;
            Err(CatalogError::DirectBackfill(err))
        }
    }
}

/// Flips an already-persisted definition row to `status` in place (issue
/// #55) — used by [`install_definition`] once its direct build finishes.
async fn mark_definition_status(
    pool: &Pool,
    id: i64,
    status: TransformStatus,
) -> Result<(), CatalogError> {
    let client = pool.get().await?;
    client
        .execute(
            "update transform_definitions set status = $1 where id = $2",
            &[&status.as_str(), &id],
        )
        .await?;
    Ok(())
}

/// Flips `definition_id` from [`TransformStatus::Backfilling`] to
/// [`TransformStatus::Live`] and parks a catch-up marker for its source table
/// — the "every chunk done" completion event
/// `chunk_queue::finish_chunk` calls once every `backfill_chunks` row for
/// `definition_id` is done (docs/decisions/0007's amendment). Runs inside the
/// caller's transaction, which must already hold a `for update` lock on
/// `definition_id`'s `transform_definitions` row (see `finish_chunk`) — that
/// lock is what makes two workers finishing different chunks of the same
/// definition near-simultaneously unable to race this completion in either
/// direction (both flipping it, or neither).
///
/// The parked marker (reusing the exact `pending_backfill` mechanism the
/// ring-fallback path already relies on — see
/// [`crate::intake::publication::park_backfill_catchup`]) is what makes
/// excluding a non-`live` definition from [`dependents_of`]/[`transforms_for_source`]
/// safe rather than lossy: any CDC delta for this source table that arrived
/// while this definition sat `backfilling` was never folded into its target
/// (the exclusion), but this marker's later discharge re-derives the target
/// from current source state, folding that delta in after all.
///
/// Only ever called for the plain (non-relationship) 1-1 chunk-queue path
/// today — a relationship-enriched 1-1 or aggregate definition still flips
/// `Backfilling` -> `Live` synchronously inside [`install_definition`] itself
/// via [`mark_definition_status`], since neither is chunked into
/// `backfill_chunks` (see this crate's `defs::backfill` module docs on why).
pub(crate) async fn complete_direct_backfill(
    txn: &tokio_postgres::Transaction<'_>,
    definition_id: i64,
) -> Result<(), CatalogError> {
    txn.execute(
        "update transform_definitions set status = $1 where id = $2",
        &[&TransformStatus::Live.as_str(), &definition_id],
    )
    .await?;

    // Issue #72 / ADR-0007: `transform_definitions.source_table` is already
    // the fully-qualified `schema.table` identity persisted at
    // definition-acceptance time (`create_definition_inner`) — read it back
    // and use it as-is. It must *not* be re-resolved through
    // [`resolve_source_schema_in_txn`] a second time here: that function
    // matches `information_schema.tables.table_name` (a bare name) exactly,
    // so handing it an already-qualified `"schema.table"` string would never
    // match anything and this would fail every time with
    // [`CatalogError::SourceTableNotFound`].
    let qualified: String = txn
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&definition_id],
        )
        .await?
        .get(0);
    crate::intake::publication::park_backfill_catchup(txn, &qualified).await?;
    Ok(())
}

/// Deletes a definition row by id (issue #55) — used by [`install_definition`]
/// to discard the speculative `Backfilling` row it persists ahead of its
/// direct build when that build doesn't pan out (falls back to the ring, or
/// fails outright). Only ever targets a row this same call just inserted, so
/// there's nothing else in the catalog yet that could reference it.
async fn delete_definition_row(pool: &Pool, id: i64) -> Result<(), CatalogError> {
    let client = pool.get().await?;
    client
        .execute("delete from transform_definitions where id = $1", &[&id])
        .await?;
    Ok(())
}

/// What to do with one table's coverage once a direct build succeeds: either
/// persist a fence captured before the build, or clear any stale record.
enum CoveragePlan {
    /// This build is the table's sole reader — record the pre-build fence.
    Record {
        qualified: String,
        fence: crate::intake::publication::CoverageFence,
    },
    /// Another definition already reads this table (built at a different
    /// fence), so only a full enumeration can be trusted to catch every reader
    /// up — clear any coverage to force that.
    Clear { qualified: String },
}

/// Plans direct-backfill coverage (issue #79, bug B) for a definition about to
/// be built through the fast path: its own source table plus every to-side
/// relationship table its fields read. For each, either captures a coverage
/// fence (row count + snapshot) or, when another definition already reads the
/// table, marks it for clearing.
///
/// **Runs before the build.** The captured fence must predate every read the
/// build makes of the table: the build reads to-side tables early (into staging
/// tables) and the source in per-chunk statements, none under a single
/// snapshot, so a fence taken *after* the build could be newer than a write the
/// build never saw and wrongly certify it as covered. A pre-build fence instead
/// leaves any build-window write invisible in the fence, so
/// [`crate::intake::publication::coverage_covers`] falls back to enumeration.
///
/// Also runs before the new definition is persisted, so [`table_has_other_reader`]
/// sees only the *pre-existing* readers of each table.
/// `resolved` is the caller's already-resolved relationship map (the same one
/// it validated against), passed in rather than re-resolved here: `install_definition`
/// needs it up front anyway to validate ahead of DDL.
async fn plan_direct_backfill_coverage(
    pool: &Pool,
    def: &TransformDef,
    resolved: &HashMap<String, ResolvedRelationship>,
) -> Result<Vec<CoveragePlan>, CatalogError> {
    // Distinct to-side tables this definition reads through a relationship.
    let mut tables: HashSet<String> = resolved.values().map(|r| r.to_table.clone()).collect();
    tables.insert(def.source.clone());

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let mut plans = Vec::with_capacity(tables.len());
    for bare_table in tables {
        let schema = resolve_source_schema_in_txn(&txn, &bare_table).await?;
        let qualified = crate::intake::publication::qualify(&schema, &bare_table)?;
        if table_has_other_reader(&txn, &bare_table, &qualified).await? {
            plans.push(CoveragePlan::Clear { qualified });
        } else {
            let fence =
                crate::intake::publication::capture_backfill_coverage_fence(&*txn, &qualified)
                    .await?;
            plans.push(CoveragePlan::Record { qualified, fence });
        }
    }
    txn.commit().await?;
    Ok(plans)
}

/// Persists a [`plan_direct_backfill_coverage`] result once the direct build
/// has succeeded (issue #79, bug B), in one transaction so the whole plan lands
/// atomically.
async fn commit_direct_backfill_coverage(
    pool: &Pool,
    plans: &[CoveragePlan],
) -> Result<(), CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    for plan in plans {
        match plan {
            CoveragePlan::Record { qualified, fence } => {
                crate::intake::publication::write_backfill_coverage(&*txn, qualified, fence)
                    .await?;
            }
            CoveragePlan::Clear { qualified } => {
                crate::intake::publication::clear_backfill_coverage(&*txn, qualified).await?;
            }
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
///
/// Takes both `table`'s bare and fully-qualified (`qualified`) spellings
/// (issue #72), because the two clauses below need different ones and
/// neither can be derived from the other inside this query:
///
/// * The first clause compares against `transform_definitions.source_table`,
///   which — since issue #72 — holds the *qualified* identity, so it needs
///   `qualified` to ever match.
/// * The second clause's join compares against `relationship_definitions.from_table`,
///   which still holds a *bare* name (relationship endpoints aren't
///   qualified yet — a later issue's job), so it's matched against
///   `split_part(d.source_table, '.', 2)` (d.source_table's bare table-name
///   suffix) rather than `d.source_table` itself, and `r.to_table = $2` needs
///   the bare `table`. This bare/qualified split is exactly the same
///   conservative-is-fine tradeoff the doc comment above already accepts for
///   this whole function: `split_part` can only ever *widen* a match (two
///   same-named tables in different schemas both count as "has a reader"),
///   never narrow one, so it can't turn a real "no other reader" into a
///   false positive strong enough to under-cover — it can only ever push
///   toward the always-safe `Clear` side.
async fn table_has_other_reader(
    txn: &tokio_postgres::Transaction<'_>,
    table: &str,
    qualified: &str,
) -> Result<bool, CatalogError> {
    let exists: bool = txn
        .query_one(
            "select \
               exists(select 1 from transform_definitions where source_table = $1) \
               or exists( \
                 select 1 from relationship_definitions r \
                 join transform_definitions d on split_part(d.source_table, '.', 2) = r.from_table \
                 where r.to_table = $2 \
               )",
            &[&qualified, &table],
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
    status: TransformStatus,
    target_schema: &str,
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
    //
    // Keyed on the *bare* `def.source`/`def.target`, not
    // `qualified_source`/`qualified_target` (both resolved below): as of
    // issue #73, both sides of *this* call now have a qualified form
    // available, but `schema_nodes` itself still keys on bare names pending
    // issue #74's migration of the whole graph to qualified identity, and
    // switching just this function's two `resolve_node_in_txn` calls to
    // qualified would not actually close that gap — it would reopen the
    // exact node-splitting bug the pre-#73 version of this comment warned
    // about, one level up: [`create_relationship`] resolves a relationship's
    // `from_table`/`to_table` endpoints (its own `resolve_node_in_txn`
    // calls, a few functions below) bare too, and relationship endpoints
    // aren't in this issue's scope — ADR-0007's own "Scope" section defers
    // them explicitly. A table that is both a transform source/target *and*
    // a relationship endpoint (the common case ADR-0006's examples all
    // chain off) would then resolve to two different `schema_nodes` rows for
    // the same physical table depending on which grammar last referenced
    // it — `TRANSFORM` (qualified, if this were switched) vs. `RELATIONSHIP`
    // (still bare) — which is strictly worse than today's "always bare, so
    // at least self-consistent" graph. Bare-for-now, consistent across
    // *every* caller of `resolve_node_in_txn` (not just this one), is the
    // safer half-step; issue #74 migrates every one of them — this
    // function's two calls and [`create_relationship`]'s two — to qualified
    // identity together, in one pass, rather than piecemeal. TODO(#74): pass
    // `qualified_source`/`qualified_target` here once that lands.
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
    //
    // Bare `def.source`/`def.target`, matching the node resolution above —
    // `schema_edges` is keyed by `schema_nodes.id`, so this walks the exact
    // same bare-keyed graph those nodes were just resolved into (TODO(#74)
    // applies here identically, for the same relationship-endpoint reason).
    reject_if_table_cycle(&txn, &def.source, &def.target).await?;

    // Issue #21: a transform's `FROM` is a `Source` dependency edge from its
    // source node to its target node — persisted alongside the node
    // resolutions above so `dependents_of` can walk the graph instead of
    // matching on `transform_definitions.source_table` string equality.
    persist_edge_in_txn(&txn, source_node.id, target_node.id, EdgeKind::Source).await?;

    // Issue #72 / ADR-0007: resolve `def.source` — the bare name the
    // grammar hands us (issue #76 will teach it an explicit `schema.table`
    // spelling; it doesn't accept one yet) — to its fully-qualified
    // `schema.table` identity exactly once, here, at definition-acceptance
    // time, via the same search-path walk [`resolve_source_schema_in_txn`]
    // always used. From this point on, `qualified_source` — never
    // `def.source` — is what gets persisted
    // (`source_table_versions`/`transform_definitions.source_table` below)
    // and threaded into every side effect that must agree with the
    // persisted row (`enumerate_and_append`'s ring entries below,
    // `complete_direct_backfill`'s catch-up marker elsewhere).
    //
    // Deliberately placed *after* [`reject_if_table_cycle`], not before:
    // unlike the bare-keyed node/edge resolution above, this requires
    // `def.source` to name a table that actually, physically exists yet
    // (`resolve_source_schema_in_txn` queries `information_schema.tables`) —
    // a real chained definition's source (a previous definition's target)
    // always does by the time it's created, but a cycle-rejected definition
    // in this same call may not (its `FROM` names a table only ever
    // registered as a `schema_nodes`/target row, never backfilled). Resolving
    // before the cycle check would surface a confusing
    // [`CatalogError::SourceTableNotFound`] for what's really a cycle,
    // pre-empting the more specific [`ValidationError::TableCycle`] this
    // definition should actually fail with.
    //
    // Resolving unconditionally (not just when `backfill` is set) matters:
    // both [`create_definition`] and [`create_definition_without_backfill`]
    // write the same `source_table` column, so both must qualify it the same
    // way regardless of which one skips ring enumeration.
    let source_schema = resolve_source_schema_in_txn(&txn, &def.source).await?;
    let qualified_source = crate::intake::publication::qualify(&source_schema, &def.source)?;

    // Issue #73 / ADR-0007: resolve `def.target` — likewise bare, the
    // grammar's `TARGET`/transform-name clause has no qualification syntax
    // either (issue #76) — to its fully-qualified identity exactly once,
    // here, mirroring `qualified_source` immediately above. Unlike the
    // source side, `def.target`'s schema is never search-path-resolved: a
    // source table's schema is *discovered* (it already exists somewhere on
    // the path), but a target table's schema is a config-time *decision*,
    // `target_schema` — the exact value this function's own caller
    // (`install_definition`, or `pool.target_schema()` for the ring-path
    // entry points — see their own call sites) already used, or is about to
    // use, for the physical `CREATE TABLE` (`ddl::qualified_target_table`).
    // Built via the same `intake::publication::qualify` helper as
    // `qualified_source`, not `ddl::qualified_target_table` directly: the two
    // produce different shapes for different jobs — `qualify` returns the
    // plain, unquoted `"schema.table"` this whole module's qualified-identity
    // convention already uses (what a downstream chained definition's own
    // `resolve_source_schema_in_txn` + `qualify` on its bare `def.source`
    // will independently reproduce once it names this target), while
    // `qualified_target_table` returns a separately-quoted
    // `"schema"."table"` string built for direct interpolation into DDL
    // text — never meant to be compared as a persisted identity string, and
    // never equal to `qualify`'s output byte-for-byte.
    let qualified_target = crate::intake::publication::qualify(target_schema, &def.target)?;

    // Reviewer follow-up to issue #73 / ADR-0007: reject this definition if
    // `qualified_target` shares a bare table-name suffix with a *different*
    // qualified spelling some other still-persisted definition already
    // uses. Before #73, `target_table` stored the bare name and its own
    // `unique` constraint (`V2__transform_catalog.sql`) enforced this for
    // free; now that the column stores the qualified spelling, that
    // constraint only guarantees the qualified string is unique, and
    // nothing else stopped `public.foo` and `custom.foo` from coexisting
    // (most plausibly: `Config::target_schema` changed between deploys and
    // an operator redeclared a same-named `TRANSFORM ... TARGET foo`).
    // Every `split_part(target_table, '.', 2)`-keyed read site downstream
    // ([`definition_by_target`], [`dependents_of`], `app.rs`'s
    // `status`/`quarantine_status`, `generative`'s `unsettled_definitions`)
    // was written assuming that suffix is globally unique — some
    // (`definition_by_target`) would silently splice two colliding
    // definitions' rows into one corrupted [`super::ast::TransformDef`]
    // rather than error — so this closes the gap once, here, at
    // definition-acceptance time, rather than teaching every one of those
    // call sites to defend against an ambiguity that shouldn't be able to
    // exist. Checked within this same transaction, against the same
    // `transaction`'s view of `transform_definitions` every other check in
    // this function already reads.
    //
    // Provisional, not a permanent rule: issue #76 will teach the grammar an
    // explicit `schema.table` spelling for `FROM`/`TARGET`, and once an
    // operator can write e.g. `TRANSFORM ... FROM custom.foo` to disambiguate
    // from `public.foo`, this restriction may need to relax (or a different
    // addressing scheme adopted) — but until #76 lands, every read site
    // above still only ever has the bare name to key off of, so allowing the
    // collision to be created at all would just move the silent-corruption
    // risk somewhere else.
    if let Some(row) = txn
        .query_opt(
            "select target_table from transform_definitions \
             where split_part(target_table, '.', 2) = $1 and target_table <> $2 \
             limit 1",
            &[&def.target, &qualified_target],
        )
        .await?
    {
        let existing: String = row.get(0);
        return Err(CatalogError::TargetTableSuffixCollision {
            target: def.target.clone(),
            requested: qualified_target,
            existing: Some(existing),
        });
    }

    // Issue #23: a definition's initial backfill is one enumeration of its
    // source table, staged as `Recompute` triggers into the active ring
    // segment via the same append path CDC/reverse-propagation use — one
    // call here regardless of how many calculated fields the definition
    // declares, not one per field, preserving the "N columns, one backfill"
    // property as the definition model becomes first-class. `qualified_source`
    // (resolved above, once) covers both a raw/CDC source (typically
    // `public`) and a chained definition's source being a *previous*
    // definition's target table (whatever schema `config.target_schema()`
    // actually resolved to, which may not be the `DEFAULT_TARGET_SCHEMA`
    // constant if overridden) without needing to special-case on
    // `source_node.is_target` — `resolve_source_schema_in_txn` walks
    // `search_path` (`pool::session_bootstrap` pins it to the Trellis
    // schema, then the target schema, then `public`, in that order)
    // identically either way.
    if backfill {
        crate::intake::publication::enumerate_and_append(&txn, &qualified_source).await?;
    }

    let version: i64 = txn
        .query_one(
            "insert into source_table_versions (source_table, version)
             values ($1, 1)
             on conflict (source_table)
             do update set version = source_table_versions.version + 1
             returning version",
            &[&qualified_source],
        )
        .await?
        .get(0);

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let status_text = status.as_str();

    // Reviewer follow-up to issue #73: `transform_definitions_target_suffix_idx`
    // (`V23__transform_definitions_target_suffix_idx.sql`) is the DB-level
    // backstop for the exact same invariant the pre-check above enforces
    // optimistically — it's what actually closes the race between two
    // concurrent `create_definition` calls each resolving a different-schema
    // target for the same bare suffix, since the pre-check's `select` only
    // ever sees its own transaction's snapshot and can't see the other
    // transaction's still-uncommitted insert. This insert is normally
    // expected to succeed (the pre-check already ruled out every collision
    // its own snapshot could see); a unique-violation against that specific
    // index here means the check passed but a concurrent transaction won the
    // race and committed first — translated into the same
    // [`CatalogError::TargetTableSuffixCollision`] the pre-check raises,
    // rather than left as an opaque [`CatalogError::Db`], so a caller sees
    // one typed error for this invariant regardless of which of the two
    // paths caught it. `existing` is `None` here (contrast the pre-check's
    // `Some`): the insert failure has already aborted this transaction, so
    // there's no further query available in it to look up what was won
    // against.
    let id: i64 = match txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text, source_columns, status)
             values ($1, $2, $3, $4, jsonb_object($5::text[], $6::text[]), $7)
             returning id",
            &[
                &qualified_target,
                &qualified_source,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
                &status_text,
            ],
        )
        .await
    {
        Ok(row) => row.get(0),
        Err(err) if is_target_suffix_index_violation(&err) => {
            return Err(CatalogError::TargetTableSuffixCollision {
                target: def.target.clone(),
                requested: qualified_target,
                existing: None,
            });
        }
        Err(err) => return Err(err.into()),
    };

    txn.commit().await?;

    Ok(Definition {
        id,
        source_version: version,
        def,
        source_columns: source_columns.clone(),
        status,
    })
}

/// Whether `err` is a unique-violation against
/// `transform_definitions_target_suffix_idx`
/// (`V23__transform_definitions_target_suffix_idx.sql`) specifically — the
/// signal [`create_definition_inner`]'s final insert uses to tell "a
/// concurrent transaction won the bare-target-suffix race the pre-check
/// above couldn't see" apart from any other constraint violation the same
/// insert could raise (most notably `transform_definitions_target_table_key`,
/// an exact-qualified-name duplicate, which stays a raw [`CatalogError::Db`]
/// — see `a_duplicate_target_table_surfaces_the_underlying_postgres_detail`).
/// Matched by constraint/index name, not just [`tokio_postgres::error::SqlState::UNIQUE_VIOLATION`]
/// alone, since that SQLSTATE alone can't distinguish the two — mirrors
/// `staging::quarantine::is_undefined_table`'s style of a small, named
/// `&tokio_postgres::Error -> bool` predicate rather than inlining the check
/// at its one call site.
fn is_target_suffix_index_violation(err: &tokio_postgres::Error) -> bool {
    let Some(db_err) = err.as_db_error() else {
        return false;
    };
    *db_err.code() == tokio_postgres::error::SqlState::UNIQUE_VIOLATION
        && db_err.constraint() == Some("transform_definitions_target_suffix_idx")
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
///
/// **Only ever call this on a *bare* name freshly parsed from a
/// definition's own source text** (ADR-0007) — `def.source`, never a value
/// read back from `transform_definitions.source_table`/
/// `source_table_versions.source_table`. Those columns hold the *qualified*
/// result this function already produced once, at the definition's own
/// acceptance time (issue #72); feeding a qualified `"schema.table"` string
/// back in here wouldn't just be redundant, it would always fail — this
/// query filters `information_schema.tables` by bare `table_name`, which a
/// qualified string never matches, so every call would return
/// [`CatalogError::SourceTableNotFound`].
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
    Err(
        ValidationError::RelationshipTypeMismatch(Box::new(RelationshipTypeMismatch {
            name: def.name.clone(),
            from_table: def.from_table.clone(),
            from_col: def.from_col.clone(),
            from_type: from_type.to_string(),
            to_table: def.to_table.clone(),
            to_col: def.to_col.clone(),
            to_type: to_type.to_string(),
        }))
        .into(),
    )
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
    status: TransformStatus,
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
///
/// **`status = 'live'` only** (the public API design's ADR-0007 amendment,
/// closing the CDC race commit 1fa8570 reopened): a `waiting_to_backfill`/
/// `backfilling`/`quarantined` definition's target may not yet reflect every
/// pre-existing source row (the direct-build chunk queue, or a
/// still-in-flight ring enumeration, hasn't necessarily finished), so a live
/// CDC delta folded into it now — via [`transforms_for_source`], the apply
/// path's read of this function — could permanently corrupt a value an
/// incremental accumulator (e.g. `AVG`) computes against a baseline. Excluding
/// non-`live` rows here means the apply path simply never attempts them; the
/// delta is not lost, though — [`crate::intake::publication::run_pending_backfills`]'s
/// discharge (parked via the same `pending_backfill` marker the ring-fallback
/// path already relies on, inserted when a definition flips to
/// [`TransformStatus::Live`] — see `chunk_queue::complete_direct_backfill`)
/// re-derives the definition's target from current source state once it goes
/// live, folding in anything skipped while it wasn't.
///
/// `node_table` ($1) is matched bare against `schema_nodes.table_name`,
/// unaffected by issue #73 — `schema_nodes` stays bare pending issue #74 (see
/// [`create_definition_inner`]'s doc comment), and every caller here
/// ([`transforms_for_source`], `staging::apply`'s `catalog_source_key`
/// results) already hands this a bare table name. The join from `to_node`
/// onto `transform_definitions`, though, *is* affected: `to_node.table_name`
/// is that same still-bare `schema_nodes` identity, but
/// `transform_definitions.target_table` has been fully-qualified since issue
/// #73, so a plain `t.target_table = to_node.table_name` would never match
/// again — matched instead against `target_table`'s bare table-name suffix
/// (`split_part`), the same convention [`table_has_other_reader`] already
/// established for its own bare/qualified `source_table` join.
pub async fn dependents_of(
    pool: &Pool,
    node_table: &str,
    kind: EdgeKind,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, t.status, e.key, e.value
             from schema_nodes from_node
             join schema_edges se on se.from_node_id = from_node.id and se.kind = $2
             join schema_nodes to_node on to_node.id = se.to_node_id
             join transform_definitions t on split_part(t.target_table, '.', 2) = to_node.table_name
             left join lateral jsonb_each_text(t.source_columns) e on true
             where from_node.table_name = $1 and t.status = 'live'
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
        let key: Option<String> = row.get(4);
        let value: Option<String> = row.get(5);

        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            let status_text: String = row.get(3);
            let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            });
            PendingDefinition {
                source_version: row.get(1),
                text: row.get(2),
                status,
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
            status: pending.status,
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
///
/// Returns **bare** table names, deliberately — a pre-issue-#72 contract this
/// keeps unchanged even though `transform_definitions.source_table` itself is
/// now qualified (issue #72). This recursive CTE mixes anchors (from
/// `transform_definitions.source_table`) with relationship-reachable tables
/// (from `relationship_definitions.to_table`/`from_table`, still bare —
/// relationship endpoints aren't qualified yet), so seeding it with anything
/// but a bare name would break the `rd.from_table` join for every anchor and
/// silently truncate the reachable set. The `split_part` below strips
/// `source_table` back to its bare table-name suffix at the seed, matching
/// what this function has always returned; both of this function's callers
/// ([`crate::client::reconcile_source_tables`] via `intake::publication::qualify`,
/// and [`crate::app::qualified_source_tables`] via its own `information_schema`
/// resolution) already re-qualify each bare result themselves and would
/// double-qualify (or, for `qualify`, hard-error on the embedded `.`) a
/// qualified name passed straight through.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "with recursive reachable(table_name) as (
                select distinct split_part(source_table, '.', 2) from transform_definitions
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
///
/// `source_table` is matched against `source_table_versions.source_table`'s
/// bare table-name suffix (`split_part(..., '.', 2)`), not the qualified
/// column directly, because this function's one caller
/// (`staging::apply::apply_and_mark_drained_many`'s `plan.versions` loop, fed
/// by `apply::catalog_source_key`) still hands it a bare name — see that
/// function's own doc comment for why: a CDC-staged `FoldedChange::src_table`
/// is qualified and gets stripped to bare before reaching here, and a
/// downstream (target-table-as-source) one was already bare. Since
/// `source_table_versions.source_table` is qualified as of issue #72,
/// matching it exactly against that already-bare key would never succeed;
/// the `split_part` match restores the pre-#72 bare-vs-bare comparison this
/// call site depends on.
///
/// **Not resolved by issue #73.** An earlier draft of this comment predicted
/// #73 (persisting `transform_definitions.target_table` qualified) would let
/// `apply.rs` pass a qualified key straight through here once it landed. It
/// doesn't: a chained definition's downstream `Recompute` trigger — what
/// actually stages a "target-table-as-source" change into this apply path —
/// gets its `src_table` from [`super::ddl::neighbor_table_name`], which
/// issue #73 deliberately leaves bare (see that function's own doc comment:
/// it's read live, over a connection whose `search_path` already resolves
/// it, not compared as a persisted identity string). Catalog persistence and
/// emitted-statement qualification are two different jobs — ADR-0007 splits
/// them into separate decision points (1) and (3) — and only the first is
/// this issue's. Making every emitted `src_table`/trigger row qualified, so
/// this and `staging::apply::catalog_source_key` could drop their
/// `split_part`/bare-suffix matching entirely, is issue #75's emission
/// audit, not #72's or #73's.
pub async fn source_table_version(
    pool: &Pool,
    source_table: &str,
) -> Result<Option<i64>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select version from source_table_versions \
             where split_part(source_table, '.', 2) = $1",
            &[&source_table],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}

/// Reads back one definition by its catalog id, re-parsing `definition_text`
/// exactly like every other read path in this module — used by
/// `chunk_queue`'s claim loop to reconstruct the [`super::ast::TransformDef`]
/// a claimed `backfill_chunks` row's `definition_id` names, so it can render
/// that chunk's write SQL. Returns `None` for an id nothing has ever
/// inserted (or already deleted, e.g. a definition dropped mid-backfill —
/// not exposed by any API yet, but `backfill_chunks`' `on delete cascade`
/// means this can legitimately come back empty for a stale claim).
pub(crate) async fn definition_by_id(
    pool: &Pool,
    id: i64,
) -> Result<Option<Definition>, CatalogError> {
    let client = pool.get().await?;
    // `left join lateral jsonb_each_text(...)` — same "decode JSON via SQL, no
    // serde_json dependency" convention `dependents_of` uses, just for one
    // row instead of a batch.
    let rows = client
        .query(
            "select t.source_version, t.definition_text, t.status, e.key, e.value \
             from transform_definitions t \
             left join lateral jsonb_each_text(t.source_columns) e on true \
             where t.id = $1",
            &[&id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let source_version: i64 = rows[0].get(0);
    let text: String = rows[0].get(1);
    let status_text: String = rows[0].get(2);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    let def = parse(&text)?;

    let mut source_columns = HashMap::new();
    for row in &rows {
        let key: Option<String> = row.get(3);
        let value: Option<String> = row.get(4);
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
            source_columns.insert(key, value_type);
        }
    }

    Ok(Some(Definition {
        id,
        source_version,
        def,
        source_columns,
        status,
    }))
}

/// Reads back one definition by its target table name — [`definition_by_id`]
/// keyed the other way, for callers that only have the address a `Trellis`
/// caller would use (`docs/decisions/0003-quarantine-storage-and-api.md`'s
/// amendment: a quarantine target is `transform` or `transform.column`,
/// where `transform` is this crate's `target_table`). Used by
/// `staging::quarantine`'s column-resume path to reconstruct the
/// [`super::ast::TransformDef`] whose column it's re-deriving.
///
/// `target_table` is — and, per this doc comment, must stay — the *bare*
/// name every caller here actually has: a `Trellis` API consumer only ever
/// knows the bare name their `TRANSFORM <name> FROM ...` text declared (the
/// grammar has no qualified-target syntax yet — issue #76), and
/// `docs/decisions/0003`'s `transform.column` addressing scheme parses on the
/// first `.` (`app::QuarantineTarget::parse`) — a qualified address here
/// would misparse as a column reference the moment a target table lived
/// outside the default schema. Matched against `target_table`'s bare
/// table-name suffix (`split_part`), not the qualified column directly,
/// since it's been fully-qualified since issue #73.
pub async fn definition_by_target(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, t.status, e.key, e.value \
             from transform_definitions t \
             left join lateral jsonb_each_text(t.source_columns) e on true \
             where split_part(t.target_table, '.', 2) = $1",
            &[&target_table],
        )
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }

    let id: i64 = rows[0].get(0);
    let source_version: i64 = rows[0].get(1);
    let text: String = rows[0].get(2);
    let status_text: String = rows[0].get(3);
    let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
        panic!("transform_definitions.status held unrecognized value '{status_text}'")
    });
    let def = parse(&text)?;

    let mut source_columns = HashMap::new();
    for row in &rows {
        let key: Option<String> = row.get(4);
        let value: Option<String> = row.get(5);
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
            source_columns.insert(key, value_type);
        }
    }

    Ok(Some(Definition {
        id,
        source_version,
        def,
        source_columns,
        status,
    }))
}

/// Every `(downstream_target_table, downstream_field_name)` pair whose
/// calculated-field expression reads `(upstream_table, upstream_column)` —
/// either directly (a chained 1-1 transform whose `FROM` *is*
/// `upstream_table`, referencing the column by its bare name) or through a
/// declared relationship whose `to_table` is `upstream_table` (a
/// relationship-enriched field's `<rel>.<column>` path). This is column-level
/// lineage, deliberately *not* [`dependents_of`]'s table-level
/// `schema_edges` walk: that graph answers "does transform X read table Y at
/// all," which is too coarse for ADR-0003's amendment — cascading a paused
/// *column* must not pause a downstream transform's *other* fields that don't
/// actually reference it. Scanning every definition's own field expressions
/// (rather than a persisted edge) is what makes this precise.
///
/// Direct (one-hop) dependents only; `staging::quarantine`'s cascade walks
/// this transitively itself, relying on the same cycle-freedom
/// `docs/transforms.md#chaining-and-cycle-detection` guarantees for the
/// table-level graph (a column-level reference can only exist where a
/// table-level dependency edge already does, so the same DAG property
/// applies).
///
/// Filtered to [`KeySpace::OneToOne`] downstream definitions only — column-
/// level pause/cascade/resume is explicitly scoped to the 1-1 tier (see this
/// module's callers in `staging::quarantine` and that module's "Column-level
/// fuse" section doc comment: `staging::apply_aggregate`'s incremental-delta
/// path has no notion of `column_status` at all). Without this filter, a
/// downstream [`KeySpace::Aggregate`] transform whose field happens to read a
/// just-paused upstream column would get a `column_status` row cascaded onto
/// it that nothing in the aggregate write path ever consults or clears, and
/// that `resume_column`'s cascade walk would later try (and fail) to
/// recompute via `staging::quarantine::recompute_column`'s single-row 1-1
/// recompute path.
pub(crate) async fn column_dependents(
    pool: &Pool,
    upstream_table: &str,
    upstream_column: &str,
) -> Result<Vec<(String, String)>, CatalogError> {
    let client = pool.get().await?;
    let def_rows = client
        .query(
            // `split_part(target_table, '.', 2)`, not the qualified column
            // directly (issue #73): the `String` this returns for each row
            // is pushed straight into `deps` below as a *downstream
            // transform* identifier, which flows into
            // `staging::quarantine`'s `column_status`/`column_pause_cascades`
            // bookkeeping — an entirely bare-keyed subsystem seeded from the
            // bare `transform` a `Trellis` caller passes to `pause_column`/
            // `resume_column`. Returning the newly-qualified spelling here
            // instead would split that bookkeeping across two spellings of
            // the same transform depending on whether a row was reached
            // directly or via cascade.
            "select split_part(target_table, '.', 2), definition_text from transform_definitions",
            &[],
        )
        .await?;
    let rel_rows = client
        .query(
            "select from_table, name, to_table from relationship_definitions",
            &[],
        )
        .await?;

    let mut rel_to_table: HashMap<(String, String), String> = HashMap::new();
    for row in rel_rows {
        let from_table: String = row.get(0);
        let name: String = row.get(1);
        let to_table: String = row.get(2);
        rel_to_table.insert((from_table, name), to_table);
    }

    let mut deps = Vec::new();
    for row in def_rows {
        let target: String = row.get(0);
        let text: String = row.get(1);
        // A definition already persisted here is expected to always re-parse
        // (the same assumption every other read path in this module makes);
        // skip rather than fail this best-effort lineage scan on the
        // unexpected chance it doesn't, rather than let one bad row prevent
        // cascading a pause to every other, healthy dependent.
        let Ok(def) = parse(&text) else { continue };
        if !matches!(def.key_space, KeySpace::OneToOne) {
            continue;
        }
        // `def.source` (freshly re-parsed from `definition_text`), not the
        // persisted `transform_definitions.source_table` column — issue #72
        // made that column fully-qualified, but `upstream_table` here is
        // always a bare *target* table name (a downstream transform's
        // `def.source` naming an upstream one's `def.target`, or a paused
        // column's own bare transform — see `staging::quarantine`'s
        // callers), so comparing against it needs the same bare spelling
        // `def.source` already gives for free, matching the `split_part`
        // read of `target_table` above.
        for field in &def.fields {
            if expr_references_column(
                &field.expr,
                &def.source,
                upstream_table,
                upstream_column,
                &rel_to_table,
            ) {
                deps.push((target.clone(), field.name.clone()));
            }
        }
    }
    Ok(deps)
}

/// Whether `expr` (one calculated field's expression, belonging to a
/// definition whose `FROM` is `def_source`) reads `(upstream_table,
/// upstream_column)` — see [`column_dependents`].
fn expr_references_column(
    expr: &Expr,
    def_source: &str,
    upstream_table: &str,
    upstream_column: &str,
    rel_to_table: &HashMap<(String, String), String>,
) -> bool {
    match expr {
        Expr::Column(name) => def_source == upstream_table && name == upstream_column,
        Expr::RelationshipPath { rel, column } => {
            column == upstream_column
                && rel_to_table
                    .get(&(def_source.to_string(), rel.clone()))
                    .is_some_and(|to_table| to_table == upstream_table)
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_references_column(
                lhs,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            ) || expr_references_column(
                rhs,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            )
        }
        Expr::FunctionCall { args, .. } => args.iter().any(|arg| {
            expr_references_column(
                arg,
                def_source,
                upstream_table,
                upstream_column,
                rel_to_table,
            )
        }),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn source_table_not_found_is_not_found() {
        assert_eq!(
            CatalogError::SourceTableNotFound("widgets".to_string()).code(),
            ErrorCode::NotFound
        );
    }

    #[test]
    fn unknown_value_type_is_internal() {
        assert_eq!(
            CatalogError::UnknownValueType {
                column: "price".to_string(),
                text: "money".to_string(),
            }
            .code(),
            ErrorCode::Internal
        );
    }

    /// [`CatalogError::Parse`] must delegate to [`ParseError::code`] rather
    /// than hardcoding a category.
    #[test]
    fn parse_delegates_to_the_wrapped_parse_error() {
        let inner = ParseError::UnterminatedString;
        let expected = inner.code();
        let wrapped = CatalogError::Parse(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Parse);
    }

    /// [`CatalogError::Validate`] must delegate to [`ValidationError::code`]
    /// rather than hardcoding a category — [`ValidationError::DuplicateRelationshipName`]
    /// is the one variant that isn't plain [`ErrorCode::Validation`], so this
    /// also exercises that [`ValidationError`]'s own special case survives
    /// the extra layer of nesting.
    #[test]
    fn validate_delegates_to_the_wrapped_validation_error() {
        let inner = ValidationError::DuplicateRelationshipName {
            from_table: "orders".to_string(),
            name: "customer".to_string(),
        };
        let expected = inner.code();
        let wrapped = CatalogError::Validate(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }
}
