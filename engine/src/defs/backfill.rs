//! Direct, set-based, key-range-chunked backfill of a definition's target
//! straight from its source (issue #63 milestone 3).
//!
//! # Why this exists
//!
//! A from-scratch backfill used to run entirely through the staging ring:
//! [`super::catalog::create_definition`] enumerates every source row as a
//! `Recompute` marker (`intake::publication::enumerate_and_append`), which the
//! ring then folds and applies. That stages one marker per source row, folds
//! the whole table in a single ring segment, and applies it as one giant
//! transaction — the cost M0's benchmark measured and M1/M2 chipped at.
//!
//! This module bypasses the ring for the initial build: it computes the target
//! directly with server-side `INSERT … SELECT` statements, chunked by key
//! range so each statement is one bounded transaction. The ring is then left
//! to carry only live CDC deltas that land *after* the build (see the fence
//! discussion below). Callers pair this with
//! [`super::catalog::create_definition_without_backfill`] so the ring
//! enumeration doesn't *also* run.
//!
//! # Correctness — the five concerns issue #63 M3 calls out
//!
//! 1. **Exhaustive, disjoint chunking.** The 1-1 build walks the source
//!    primary key in half-open ranges `(lo, hi]` discovered by
//!    `max()`-over-`LIMIT` (see [`backfill_one_to_one`]): every source row's PK
//!    falls in exactly one range, with no gap or overlap regardless of gaps in
//!    the key values. The aggregate build chunks by *group key* range instead
//!    (see [`backfill_aggregate`]): every group's key is a single point in the
//!    group-key space, so a group lands wholly in exactly one range and is
//!    therefore computed in exactly one chunk.
//!
//! 2. **The build/CDC fence.** Both builds write with `ON CONFLICT DO UPDATE
//!    SET col = excluded.col` — an *overwrite* that recomputes each target row
//!    (or whole group) from the current source, identical in effect to the
//!    ring's own image-less `Recompute` path. Overwrite is idempotent and
//!    order-independent, so the handoff to the ring is the same one the ring
//!    already relies on: the caller runs this build before live CDC
//!    application begins for the definition (target table created, build run,
//!    *then* the client starts), and any genuine post-build delta the ring
//!    later applies lands on a fully-built row. This is deliberately *not* the
//!    additive (`col = target.col + excluded.col`) merge the issue sketches as
//!    the aggregate default: additive merge is not idempotent (a re-run or an
//!    overlapping CDC delta double-counts) and cannot express a
//!    `RecomputeOnly` field (`MIN`/`MAX`/composed) that spans chunks at all.
//!    Chunking by group key lets every field kind use the safe overwrite form.
//!
//! 3. **Per-field-kind aggregates.** Each field is built with the same SQL the
//!    incremental bulk path (`staging::apply_aggregate::apply_forced_groups_bulk`)
//!    emits — `SUM` keeps its hidden `__{f}_count` partial, `AVG` its
//!    `__{f}_sum`/`__{f}_count` partials with the visible column derived as
//!    `sum/count`, `COUNT(*)` a bare `count(*)`, and everything else
//!    (`MIN`/`MAX`/composed) its rendered expression — so a target built here
//!    is byte-identical to one the ring would have produced, and a later CDC
//!    delta folds onto consistent partials.
//!
//! 4. **Idempotent retry.** Every chunk is its own transaction and every write
//!    is an overwrite, so a crash or error partway through is recovered by
//!    simply re-running the whole build: already-built rows/groups are
//!    recomputed to the same value, not doubled.
//!
//! 5. **1-1 bind-param safety.** The 1-1 build uses `INSERT … SELECT` over the
//!    source (server-side), not a `VALUES` list of client-bound rows, so it
//!    carries a fixed handful of bound parameters (the range bounds) regardless
//!    of chunk size — it never approaches the `i16::MAX` bind-parameter cap the
//!    ring's row-at-a-time apply path (`staging::apply::apply_target`) must
//!    chunk around.

use std::collections::{HashMap, HashSet};

use crate::pool::{Client, Pool, quote_ident};

use super::ast::{Expr, KeySpace, RelationshipDef, TransformDef, ValueType};
use super::ddl::{
    self, PrimaryKeyColumn, avg_sum_column, qualified_target_table, source_primary_key,
};
use super::invertibility::{AggregateArg, CountArg, classify};
use super::model::RelationshipCardinality;
use super::oracle::{render_expr_sql, render_rel_expr_sql};
use super::registry::lookup_aggregate_function;

/// Rows per chunk for the 1-1 primary-key-range build. Each chunk is one
/// bounded transaction; 50k keeps a chunk's write set well within a
/// comfortable transaction size while keeping the number of round trips low
/// for a million-row source.
const BACKFILL_CHUNK_ROWS: i64 = 50_000;

/// Distinct groups per chunk for the aggregate group-key-range build. Bounds
/// the number of target rows one chunk's `ON CONFLICT` transaction touches
/// (and, with it, the group-key ranges' hash-aggregate working set) regardless
/// of how many groups the source has in total.
const BACKFILL_CHUNK_GROUPS: i64 = 10_000;

/// Connection-scoped staging table the aggregate build materializes its
/// single-pass `GROUP BY` into before chunk-writing to the target. A fixed name
/// is safe: the build holds one pooled connection for its whole duration (so no
/// two aggregate builds share this name concurrently — concurrent builds get
/// distinct connections/sessions), and it is dropped both before creation
/// (crash-leftover on a reused pooled connection) and after the writes.
const STAGE_TABLE: &str = "_trellis_backfill_agg_staging";

/// Why a direct backfill could not run.
#[derive(Debug)]
pub enum BackfillError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Introspecting the source primary key (1-1 build) failed.
    Ddl(ddl::DdlError),
    /// The definition's shape isn't supported by the direct build yet — e.g. a
    /// relationship-enriched 1-1 definition, whose target the direct build
    /// can't render without the LEFT JOIN/correlated-subquery machinery the
    /// ring path uses. Such definitions must keep going through
    /// [`super::catalog::create_definition`]'s ring enumeration.
    Unsupported(String),
}

impl std::fmt::Display for BackfillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillError::Db(err) => {
                write!(f, "direct backfill database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            BackfillError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            BackfillError::Ddl(err) => write!(f, "direct backfill schema error: {err}"),
            BackfillError::Unsupported(what) => {
                write!(f, "direct backfill does not support {what}")
            }
        }
    }
}

impl std::error::Error for BackfillError {}

impl From<tokio_postgres::Error> for BackfillError {
    fn from(err: tokio_postgres::Error) -> Self {
        BackfillError::Db(err)
    }
}

impl From<crate::error::Error> for BackfillError {
    fn from(err: crate::error::Error) -> Self {
        BackfillError::Pool(err)
    }
}

impl From<ddl::DdlError> for BackfillError {
    fn from(err: ddl::DdlError) -> Self {
        BackfillError::Ddl(err)
    }
}

/// Builds `def`'s target directly from its source, in bounded key-range
/// chunks, dispatching on `def`'s key space (see the module docs). `def`'s
/// target table must already exist (created by
/// [`super::ddl::create_target_table`] /
/// [`super::ddl::create_aggregate_target_table`]) — this only writes rows, it
/// does not create the table. `target_schema` and `source_columns` are the
/// same values those DDL calls were given.
pub async fn backfill_definition(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), BackfillError> {
    match &def.key_space {
        KeySpace::OneToOne => {
            let pk = source_primary_key(pool, &def.source).await?;
            if uses_relationships(def) {
                backfill_relationship_one_to_one(pool, def, target_schema, &pk).await
            } else {
                backfill_one_to_one(pool, def, target_schema, &pk).await
            }
        }
        KeySpace::Aggregate { group_by } => {
            backfill_aggregate(pool, def, target_schema, group_by, source_columns).await
        }
    }
}

/// Whether any of `def`'s field expressions reads a relationship path — the
/// shape the 1-1 direct build can't render (see [`BackfillError::Unsupported`]).
fn uses_relationships(def: &TransformDef) -> bool {
    fn walk(expr: &Expr) -> bool {
        match expr {
            Expr::RelationshipPath { .. } => true,
            Expr::BinaryOp { lhs, rhs, .. } => walk(lhs) || walk(rhs),
            Expr::FunctionCall { args, .. } => args.iter().any(walk),
            Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
        }
    }
    def.fields.iter().any(|f| walk(&f.expr))
}

/// Whether `expr` — belonging to the field named `field_name` — contains a
/// `Column(name)` that refers to *another* calculated field's alias (i.e.
/// `name` names a different entry in `field_names`, the full set of `def`'s
/// field names). A field reading a source column of its own name
/// (`price AS price`) is the legitimate "self-passthrough" pattern the
/// validator's `infer_expr` (see `validate.rs`) explicitly carves out as a
/// real source-column reference, not a self-dependency — so `name ==
/// field_name` is never flagged here, only a *different* field's name.
///
/// Neither direct-build renderer (`render_expr_sql` / `render_rel_expr_sql`)
/// resolves an alias reference to the field that computes it — each renders
/// `Column(name)` as a bare reference to a same-named *source* column
/// (`quote_ident(name)` / `{source}.{name}`), which is exactly what Postgres
/// rejects when `name` is actually another calculated field's SELECT-list
/// alias (`select a+b as x, x+c as y` errors `column "x" does not exist`).
/// Both direct-build paths call this before rendering, so such a field falls
/// back to the ring instead of crashing (issue #83 follow-up).
fn expr_references_other_field_alias(
    expr: &Expr,
    field_name: &str,
    field_names: &HashSet<&str>,
) -> bool {
    match expr {
        Expr::Column(name) => name != field_name && field_names.contains(name.as_str()),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
        Expr::RelationshipPath { .. } => false,
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_references_other_field_alias(lhs, field_name, field_names)
                || expr_references_other_field_alias(rhs, field_name, field_names)
        }
        Expr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| expr_references_other_field_alias(arg, field_name, field_names)),
    }
}

/// Whether any field of `def` references another field's alias anywhere in
/// its expression tree — see [`expr_references_other_field_alias`]. Builds
/// the field-name set once, mirroring `oracle::referenced_source_columns`'s
/// pattern.
fn uses_cross_field_alias(def: &TransformDef) -> bool {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();
    def.fields
        .iter()
        .any(|f| expr_references_other_field_alias(&f.expr, &f.name, &field_names))
}

/// The 1-1 build: walk the source primary key in half-open `(lo, hi]` ranges,
/// each `INSERT … SELECT … ON CONFLICT DO UPDATE`-ing one bounded chunk.
///
/// Range discovery reads the max PK of the next `BACKFILL_CHUNK_ROWS` source
/// rows above `lo` (`select max(pk) from (select pk … where pk > lo order by
/// pk limit N)`); that max becomes `hi`, the chunk covers `pk > lo and pk <=
/// hi`, and the next `lo` is this `hi`. When the discovery query returns `NULL`
/// (no rows left above `lo`) the walk stops. Every source row's PK is `> lo`
/// for exactly one range and `<= hi` for that same range, so the ranges
/// partition the source exactly once with no gap or overlap — the off-by-one
/// this structure guards against is exactly what
/// `defs_backfill_direct`'s boundary test exercises.
async fn backfill_one_to_one(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    pk: &PrimaryKeyColumn,
) -> Result<(), BackfillError> {
    // A field referencing another calculated field's alias (e.g. `total =
    // double_price + tax` where `double_price` is itself a field) can't be
    // rendered by `render_expr_sql`, which renders every `Column(name)` as a
    // bare source-column reference — Postgres rejects one SELECT-list alias
    // referencing another in the same SELECT list. Bail before building any
    // SQL so this shape falls back to the ring instead of surfacing a raw
    // Postgres error as a hard `CatalogError::DirectBackfill`.
    if uses_cross_field_alias(def) {
        return Err(BackfillError::Unsupported(
            "a field that references another calculated field's alias".to_string(),
        ));
    }

    let source = quote_ident(&def.source);
    let target = qualified_target_table(target_schema, def);
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();

    let field_idents: Vec<String> = def.fields.iter().map(|f| quote_ident(&f.name)).collect();
    let field_exprs: Vec<String> = def
        .fields
        .iter()
        .map(|f| render_expr_sql(&f.expr))
        .collect();

    let insert_cols = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let select_exprs = std::iter::once(pk_ident.clone())
        .chain(field_exprs.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let update_sets = field_idents
        .iter()
        .map(|f| format!("{f} = excluded.{f}"))
        .collect::<Vec<_>>()
        .join(", ");
    // A field-less 1-1 definition can't exist (the grammar requires at least
    // one SELECT field), so `update_sets` is always non-empty.
    debug_assert!(!update_sets.is_empty());

    let client = pool.get().await?;
    for (lo, hi) in discover_pk_ranges(&client, &source, pk).await? {
        let where_clause = pk_range_where(&pk_ident, pk_cast, &lo);
        let insert_sql = format!(
            "insert into {target} ({insert_cols}) \
             select {select_exprs} from {source} where {where_clause} \
             on conflict ({pk_ident}) do update set {update_sets}"
        );
        match &lo {
            None => {
                client.execute(&insert_sql, &[&hi]).await?;
            }
            Some(lo) => {
                client.execute(&insert_sql, &[lo, &hi]).await?;
            }
        }
    }

    Ok(())
}

/// Walks the source primary key in half-open `(lo, hi]` ranges, returning them
/// in order (the first range's `lo` is `None`, meaning "`pk <= hi`"). Each `hi`
/// is the max PK of the next `BACKFILL_CHUNK_ROWS` rows above the previous `hi`
/// (`max()`-over-`LIMIT`); the walk stops when no rows remain above the last
/// boundary. The ranges partition the source exactly once with no gap or
/// overlap regardless of gaps in the key values. Shared by both 1-1 builds so
/// the plain and relationship-enriched paths chunk identically — see
/// [`backfill_one_to_one`]'s doc comment for the off-by-one this guards against.
/// `source` is the already-quoted source table identifier.
async fn discover_pk_ranges(
    client: &Client,
    source: &str,
    pk: &PrimaryKeyColumn,
) -> Result<Vec<(Option<String>, String)>, BackfillError> {
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();
    let mut ranges = Vec::new();
    let mut lo: Option<String> = None;
    loop {
        let hi: Option<String> = match &lo {
            None => {
                let row = client
                    .query_one(
                        &format!(
                            "select max({pk_ident})::text from \
                             (select {pk_ident} from {source} \
                              order by {pk_ident} limit {BACKFILL_CHUNK_ROWS}) s"
                        ),
                        &[],
                    )
                    .await?;
                row.get(0)
            }
            Some(lo) => {
                let row = client
                    .query_one(
                        &format!(
                            "select max({pk_ident})::text from \
                             (select {pk_ident} from {source} \
                              where {pk_ident} > $1::text::{pk_cast} \
                              order by {pk_ident} limit {BACKFILL_CHUNK_ROWS}) s"
                        ),
                        &[lo],
                    )
                    .await?;
                row.get(0)
            }
        };
        let Some(hi) = hi else {
            break;
        };
        ranges.push((lo.clone(), hi.clone()));
        lo = Some(hi);
    }
    Ok(ranges)
}

/// The `where` predicate restricting a PK-range chunk to `(lo, hi]`, binding the
/// bounds as `$1` (and `$2` when `lo` is present). Paired with
/// [`discover_pk_ranges`]; the caller binds `hi` (first chunk) or `lo, hi`.
fn pk_range_where(pk_ident: &str, pk_cast: &str, lo: &Option<String>) -> String {
    match lo {
        None => format!("{pk_ident} <= $1::text::{pk_cast}"),
        Some(_) => {
            format!("{pk_ident} > $1::text::{pk_cast} and {pk_ident} <= $2::text::{pk_cast}")
        }
    }
}

/// One aggregate field's build strategy — the direct-build counterpart to
/// `staging::apply_aggregate::AggFieldKind`, kept in lockstep with
/// `classify_fields` there (both route through [`super::invertibility::classify`]
/// so a field lands on the same strategy either way).
enum FieldKind {
    Sum,
    Avg,
    Count,
    RecomputeOnly,
}

fn classify_field(expr: &Expr) -> FieldKind {
    match expr {
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            match classify("COUNT", AggregateArg::Count(CountArg::Star)) {
                Some(v) if v.is_invertible() => FieldKind::Count,
                _ => FieldKind::RecomputeOnly,
            }
        }
        Expr::FunctionCall { name, args } if args.len() == 1 => {
            match classify(name, AggregateArg::Column(ValueType::Numeric)) {
                Some(v) if v.is_invertible() && name == "SUM" => FieldKind::Sum,
                Some(v) if v.is_invertible() && name == "AVG" => FieldKind::Avg,
                _ => FieldKind::RecomputeOnly,
            }
        }
        _ => FieldKind::RecomputeOnly,
    }
}

/// The rendered SQL for a one-argument aggregate call's argument — mirrors
/// `staging::apply_aggregate::agg_arg_sql` so `SUM`/`AVG` compute the same
/// `sum(arg)`/`count(arg)` the incremental path's probes do.
fn agg_arg_sql(expr: &Expr) -> String {
    let Expr::FunctionCall { args, .. } = expr else {
        panic!("agg_arg_sql called on a non-function-call field");
    };
    render_expr_sql(&args[0])
}

/// The aggregate build: aggregate the whole source in a **single** full-table
/// scan into a temporary staging table, then chunk the *writes* from that small
/// (group-count-sized) staging table into the target by group-key range. See
/// the module docs for why overwrite-by-group-key rather than additive-by-PK.
///
/// # Why single-pass-then-chunked-write (issue #63 M3 review)
///
/// An earlier shape chunked by group-key range directly over the *source*: each
/// chunk ran `INSERT … SELECT … FROM source WHERE (<group_cols>) > lo AND
/// (<group_cols>) <= hi GROUP BY …`. The source has no index on the group-key
/// columns (only its PK — an index on the GROUP BY columns was tried and
/// abandoned as ineffective, ADR 0005 / commit e001d8a), so every chunk did a
/// full sequential scan of the entire source filtered to one key range. With
/// `C` chunks that is `O(C × source_size)` total scan work — the exact
/// "rescan-the-whole-table-per-chunk" pathology M1/M2 fixed elsewhere in #63,
/// reappearing here. At 1M distinct groups (100 chunks) it projected to ~12.5s,
/// ~200x the ~60ms single-pass `GROUP BY` floor.
///
/// This design instead scans the source exactly **once** to materialize the
/// aggregate into a staging table (the `CREATE TEMP TABLE … AS SELECT … GROUP
/// BY` below — one seq scan, the ~60ms floor), and every subsequent read is of
/// that staging table, which is *group-count*-sized, not *source*-sized. A
/// primary key on the staging table's group columns turns each chunk's
/// range-write into a cheap index range scan rather than a staging seq scan, so
/// total scan work is `O(source_size)` for the one aggregation pass plus
/// `O(group_count)` for the writes — never `O(C × source_size)`.
///
/// Non-`NULL` group keys are partitioned into `(prev, hi]` ranges over the
/// ordered distinct group tuples in staging, which cover every non-`NULL` group
/// exactly once. A group whose key has a `NULL` component is deliberately never
/// built: the target's GROUP BY columns are its primary key, so no such row can
/// exist (the ring can't store one either). Such groups are filtered out when
/// staging is built, so they never reach the target.
async fn backfill_aggregate(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    group_by: &[String],
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), BackfillError> {
    let source = quote_ident(&def.source);
    let target = qualified_target_table(target_schema, def);
    let group_idents: Vec<String> = group_by.iter().map(|c| quote_ident(c)).collect();
    let group_casts: Vec<&'static str> = group_by
        .iter()
        .map(|c| ddl::pg_type_name(source_columns.get(c).copied().unwrap_or(ValueType::Numeric)))
        .collect();

    // Build the INSERT column list and, for each, the aggregate SELECT
    // expression that computes it from the source — mirroring
    // `apply_forced_groups_bulk`'s per-field-kind construction (issue #63 M3
    // concern #3) so a directly-built target is byte-identical to a ring-built
    // one. Group-key columns come first; every column is a stable target column
    // name, so it doubles as the staging table's column name.
    let count_cols = ddl::count_column_names(&def.fields);
    let mut insert_cols: Vec<String> = group_idents.clone();
    let mut stage_exprs: Vec<String> = group_idents.clone();
    // Issue #48: two fields (e.g. `SUM(amount)`/`AVG(amount)`) can share one
    // hidden count column (`count_cols`) — track which shared names have
    // already been emitted into this staging table's column list, so a
    // second field sharing a column never emits a duplicate, which both
    // `CREATE TEMP TABLE ... AS SELECT` and the later `ON CONFLICT DO
    // UPDATE` reject.
    let mut emitted_count_cols: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for field in &def.fields {
        if group_by.contains(&field.name) {
            continue;
        }
        let col = quote_ident(&field.name);
        match classify_field(&field.expr) {
            FieldKind::Sum => {
                let arg = agg_arg_sql(&field.expr);
                insert_cols.push(col);
                stage_exprs.push(format!("sum({arg})"));
                let count_col_name = count_cols[&field.name].clone();
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(quote_ident(&count_col_name));
                    stage_exprs.push(format!("count({arg})"));
                }
            }
            FieldKind::Avg => {
                let arg = agg_arg_sql(&field.expr);
                let sum_col = avg_sum_column(&field.name);
                let count_col_name = count_cols[&field.name].clone();
                insert_cols.push(quote_ident(&sum_col));
                stage_exprs.push(format!("sum({arg})"));
                if emitted_count_cols.insert(count_col_name.clone()) {
                    insert_cols.push(quote_ident(&count_col_name));
                    stage_exprs.push(format!("count({arg})"));
                }
                insert_cols.push(col);
                stage_exprs.push(format!(
                    "case when count({arg}) = 0 then null \
                     else sum({arg}) / count({arg})::numeric end"
                ));
            }
            FieldKind::Count => {
                insert_cols.push(col);
                stage_exprs.push("count(*)::numeric".to_string());
            }
            FieldKind::RecomputeOnly => {
                insert_cols.push(col);
                stage_exprs.push(format!("({})", render_expr_sql(&field.expr)));
            }
        }
    }

    let arity = group_idents.len();
    let update_sets: Vec<String> = insert_cols
        .iter()
        .skip(arity)
        .map(|c| format!("{c} = excluded.{c}"))
        .collect();
    debug_assert!(!update_sets.is_empty());

    let insert_cols_sql = insert_cols.join(", ");
    let group_by_sql = group_idents.join(", ");
    let group_tuple = group_idents.join(", ");
    let conflict_sql = group_idents.join(", ");
    let update_sets_sql = update_sets.join(", ");
    let not_null_pred = group_idents
        .iter()
        .map(|c| format!("{c} is not null"))
        .collect::<Vec<_>>()
        .join(" and ");
    // Each staging column aliased to its target column name, so the staging
    // table's columns line up with `insert_cols` for a plain SELECT on write.
    let stage_select_sql = insert_cols
        .iter()
        .zip(&stage_exprs)
        .map(|(col, expr)| format!("{expr} as {col}"))
        .collect::<Vec<_>>()
        .join(", ");

    let client = pool.get().await?;

    // Single full-table scan: aggregate the whole (non-NULL-key) source into a
    // connection-scoped staging table. This is the ~60ms `GROUP BY` floor and
    // the *only* pass over the source. The staging table has one row per group.
    // Drop first in case a crashed prior backfill on this pooled connection left
    // one behind; drop again at the end so it doesn't leak back into the pool.
    client
        .batch_execute(&format!("drop table if exists {STAGE_TABLE}"))
        .await?;
    client
        .execute(
            &format!(
                "create temp table {STAGE_TABLE} as \
                 select {stage_select_sql} from {source} \
                 where {not_null_pred} group by {group_by_sql}"
            ),
            &[],
        )
        .await?;
    // A primary key on the group columns makes each chunk's range-write below an
    // index range scan of the staging table rather than a full staging scan —
    // the group tuple is unique in the aggregated result, so it is a valid PK.
    client
        .batch_execute(&format!(
            "alter table {STAGE_TABLE} add primary key ({group_tuple})"
        ))
        .await?;

    let insert_for = |where_clause: &str| {
        format!(
            "insert into {target} ({insert_cols_sql}) \
             select {insert_cols_sql} from {STAGE_TABLE}{where_clause} \
             on conflict ({conflict_sql}) do update set {update_sets_sql}"
        )
    };

    // Discover group-key range boundaries from the small staging table: every
    // BACKFILL_CHUNK_GROUPS-th group tuple in ascending order.
    let boundary_select_text = group_idents
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    let boundary_sql = format!(
        "select {boundary_select_text} from ( \
             select {group_tuple}, row_number() over (order by {group_tuple}) as rn \
             from {STAGE_TABLE} \
         ) x where x.rn % {BACKFILL_CHUNK_GROUPS} = 0 order by {group_tuple}"
    );
    let boundary_rows = client.query(&boundary_sql, &[]).await?;
    let boundaries: Vec<Vec<Option<String>>> = boundary_rows
        .iter()
        .map(|row| {
            (0..arity)
                .map(|i| row.get::<_, Option<String>>(i))
                .collect()
        })
        .collect();

    // A row-value comparison `(g1, g2, …) <op> (b1::t1, b2::t2, …)` against a
    // bound tuple, binding the bound components as $start.. text and casting each
    // to its group column's type. Boundaries come from staging (whose keys are
    // all non-NULL), so every component is `Some`, but `Option<String>` is what
    // the row getter yields, so unwrap defensively.
    let tuple_cmp = |op: &str, start: usize| -> String {
        let lhs = group_tuple.clone();
        let rhs = (0..arity)
            .map(|i| format!("${}::text::{}", start + i, group_casts[i]))
            .collect::<Vec<_>>()
            .join(", ");
        format!("({lhs}) {op} ({rhs})")
    };

    let mut prev: Option<Vec<Option<String>>> = None;
    for hi in &boundaries {
        let where_clause = match &prev {
            None => format!(" where {}", tuple_cmp("<=", 1)),
            Some(_) => format!(
                " where {} and {}",
                tuple_cmp(">", 1),
                tuple_cmp("<=", arity + 1),
            ),
        };
        let sql = insert_for(&where_clause);
        let mut params: Vec<String> = Vec::new();
        if let Some(prev) = &prev {
            params.extend(prev.iter().map(|v| v.clone().unwrap_or_default()));
        }
        params.extend(hi.iter().map(|v| v.clone().unwrap_or_default()));
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            params.iter().map(|p| p as _).collect();
        client.execute(&sql, &param_refs).await?;
        prev = Some(hi.clone());
    }

    // Final open-ended range above the last boundary — or, when there were no
    // boundaries at all (group count <= BACKFILL_CHUNK_GROUPS), the single range
    // covering every group in staging.
    match &prev {
        None => {
            client.execute(&insert_for(""), &[]).await?;
        }
        Some(prev) => {
            let clause = format!(" where {}", tuple_cmp(">", 1));
            let params: Vec<String> = prev.iter().map(|v| v.clone().unwrap_or_default()).collect();
            let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                params.iter().map(|p| p as _).collect();
            client.execute(&insert_for(&clause), &param_refs).await?;
        }
    }

    // Return the staging table to a clean slate before the connection goes back
    // to the pool. NULL-key groups were filtered out when staging was built, so
    // they never reach the target: the target's GROUP BY columns are its primary
    // key and Postgres forbids a NULL there (the ring can't store one either).
    client
        .batch_execute(&format!("drop table if exists {STAGE_TABLE}"))
        .await?;

    Ok(())
}

/// Prefix for the connection-scoped staging tables the relationship build
/// materializes one per referenced to-many relationship. Safe as a fixed name
/// for the same reason [`STAGE_TABLE`] is: the build holds one pooled
/// connection for its whole duration and drops each table before creating it
/// (crash leftover) and after the writes.
const REL_STAGE_TABLE_PREFIX: &str = "_trellis_backfill_rel_staging_";

/// How one field of a relationship-enriched 1-1 definition is built.
enum RelFieldPlan {
    /// A field with no relationship reference — rendered as source SQL exactly
    /// as the plain 1-1 build (and the oracle) render it.
    Source(String),
    /// A field that is exactly a top-level aggregate over a to-many
    /// relationship path (`SUM(rel.col)` / `COUNT(rel.col)` / …). Its value is
    /// read from `rel`'s staging table; `agg` is the uppercased aggregate name
    /// (only `COUNT` needs the empty-set `coalesce(_, 0)`, matching Postgres's
    /// correlated `count` over zero rows — every other aggregate's empty-set
    /// result is `NULL`, which the `LEFT JOIN` already yields).
    ToManyAgg {
        rel: String,
        agg: String,
        column: String,
    },
}

/// Whether `expr` reads any relationship path anywhere in its tree.
fn expr_uses_relationship(expr: &Expr) -> bool {
    match expr {
        Expr::RelationshipPath { .. } => true,
        Expr::BinaryOp { lhs, rhs, .. } => {
            expr_uses_relationship(lhs) || expr_uses_relationship(rhs)
        }
        Expr::FunctionCall { args, .. } => args.iter().any(expr_uses_relationship),
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => false,
    }
}

/// Classifies one field (named `field_name`) of a relationship-enriched 1-1
/// definition into a [`RelFieldPlan`], or `None` if the direct build can't
/// render it correctly (so the caller falls back to the ring). The only
/// relationship shape the direct build supports is a *top-level* aggregate
/// whose sole argument is a relationship path — the to-many aggregate the
/// evaluator and oracle both match structurally. A bare to-one path
/// (`category.name`), a relationship reference nested inside a larger
/// expression (`SUM(rel.x) + 1`, `count(rel.a) > 0`), or a non-relationship
/// expression that references another field's alias (issue #83 follow-up —
/// see [`expr_references_other_field_alias`]; `render_rel_expr_sql` renders
/// `Column(name)` as a bare `{source}.{name}` reference, which Postgres
/// rejects for a same-SELECT-list alias) is left `None` and handled by the
/// ring. `field_names` is the full set of `def`'s field names, built once by
/// the caller.
fn plan_rel_field(
    expr: &Expr,
    field_name: &str,
    source: &str,
    rel_defs: &HashMap<String, RelationshipDef>,
    field_names: &HashSet<&str>,
) -> Option<RelFieldPlan> {
    if let Expr::FunctionCall { name, args } = expr
        && lookup_aggregate_function(name).is_some()
        && let [Expr::RelationshipPath { rel, column }] = args.as_slice()
    {
        return Some(RelFieldPlan::ToManyAgg {
            rel: rel.clone(),
            agg: name.clone(),
            column: column.clone(),
        });
    }
    if expr_uses_relationship(expr) {
        return None;
    }
    if expr_references_other_field_alias(expr, field_name, field_names) {
        return None;
    }
    Some(RelFieldPlan::Source(render_rel_expr_sql(
        expr, source, rel_defs,
    )))
}

/// Maps a relationship-lookup [`super::catalog::CatalogError`] into a
/// [`BackfillError`]. Only the DB/pool variants arise in the real install flow
/// (a referenced relationship's metadata was already resolved and validated by
/// `create_target_table` just before this build runs); a stored-definition
/// re-parse failure (corruption/parser drift) falls back to the ring, which
/// re-parses and surfaces the real error itself.
fn map_rel_lookup_err(err: super::catalog::CatalogError) -> BackfillError {
    use super::catalog::CatalogError;
    match err {
        CatalogError::Db(e) => BackfillError::Db(e),
        CatalogError::Pool(e) => BackfillError::Pool(e),
        _ => BackfillError::Unsupported(
            "a relationship whose stored definition failed to resolve".to_string(),
        ),
    }
}

/// The relationship-enriched 1-1 build: computes a target whose fields
/// aggregate over to-many relationship paths (`SUM(posts.x)`, `COUNT(comments)`)
/// directly with set-based SQL, instead of staging every source row as a
/// `Recompute` marker for per-row Rust evaluation.
///
/// Each referenced to-many relationship is aggregated over its whole to-side
/// table **once** into a connection-scoped staging table grouped by the join
/// key (one row per distinct key, primary-keyed for cheap index probes) — the
/// same single-pass-then-chunked-write shape [`backfill_aggregate`] uses, so
/// the to-side is never re-scanned per chunk. The target is then written in
/// source-primary-key range chunks (reusing [`discover_pk_ranges`], identical
/// to [`backfill_one_to_one`]): each chunk `INSERT … SELECT`s a bounded PK
/// range of the source `LEFT JOIN`ed to every relationship's staging table on
/// its join key.
///
/// The result is byte-identical to the correlated-subquery oracle
/// (`oracle::render_relationship_select_sql`) and the per-row evaluator
/// (`eval::eval_to_many_aggregate`): a grouped aggregate over the matching
/// to-side rows equals the correlated aggregate over the same rows, and the
/// `LEFT JOIN`'s no-match `NULL` reproduces Postgres's empty-correlated-set
/// semantics (`SUM`/`MIN`/`MAX`/`AVG` → `NULL`), with `COUNT` `coalesce`d to `0`
/// to match `count` over the empty set. Unlike the aggregate build, this target
/// carries no hidden partial columns: a relationship-enriched 1-1 target has
/// none (see `ddl::create_target_table`) — its live CDC deltas are applied by a
/// full per-parent recompute in the ring, not an incremental partial fold, so
/// there is nothing here to keep in lockstep.
///
/// Falls back with [`BackfillError::Unsupported`] (routing the whole definition
/// to the ring) if any field is a shape this build can't render exactly — a
/// bare to-one lookup, a relationship reference nested inside a larger
/// expression, or an aggregate over a relationship that resolves to a to-one.
async fn backfill_relationship_one_to_one(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    pk: &PrimaryKeyColumn,
) -> Result<(), BackfillError> {
    // Resolve every referenced relationship to its endpoints + cardinality the
    // same way the rest of the catalog does (`relationship_by_name`), so this
    // build reads the identical join metadata the ring/oracle do.
    let mut rel_defs: HashMap<String, RelationshipDef> = HashMap::new();
    let mut rel_cardinality: HashMap<String, RelationshipCardinality> = HashMap::new();
    for (rel, _column) in super::eval::relationship_references(def) {
        if rel_defs.contains_key(&rel) {
            continue;
        }
        let Some(reldef) = super::catalog::relationship_by_name(pool, &def.source, &rel)
            .await
            .map_err(map_rel_lookup_err)?
        else {
            // Unknown relationship name: the direct build can't render it — let
            // the ring path surface the same `UnknownRelationship` the
            // evaluator/validator would.
            return Err(BackfillError::Unsupported(
                "a definition referencing an unknown relationship".to_string(),
            ));
        };
        rel_cardinality.insert(rel.clone(), reldef.cardinality);
        rel_defs.insert(rel, reldef.def);
    }

    // Classify every field; bail to the ring on the first unsupported shape.
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();
    let plans: Vec<RelFieldPlan> = def
        .fields
        .iter()
        .map(|f| {
            plan_rel_field(&f.expr, &f.name, &def.source, &rel_defs, &field_names).ok_or_else(
                || {
                    BackfillError::Unsupported(
                        "a relationship-enriched 1-1 field that isn't a top-level to-many \
                         aggregate, or that references another calculated field's alias"
                            .to_string(),
                    )
                },
            )
        })
        .collect::<Result<_, _>>()?;

    // Every to-many aggregate must resolve to a to-many relationship; an
    // aggregate over a to-one is a shape the validator rejects — fall back
    // rather than emit wrong SQL for it.
    for plan in &plans {
        if let RelFieldPlan::ToManyAgg { rel, .. } = plan
            && rel_cardinality.get(rel) != Some(&RelationshipCardinality::ToMany)
        {
            return Err(BackfillError::Unsupported(
                "an aggregate over a to-one relationship".to_string(),
            ));
        }
    }

    let source = quote_ident(&def.source);
    let target = qualified_target_table(target_schema, def);
    let pk_ident = quote_ident(&pk.name);
    let pk_cast = pk.data_type.as_str();
    let pk_qualified = format!("{source}.{pk_ident}");

    let client = pool.get().await?;

    // Materialize one staging table per referenced to-many relationship: its
    // to-side table aggregated by the join key, one aliased column per field
    // reading that relationship. `rel_stage` maps a relationship name to its
    // staging table's identifier so the chunked write below can `LEFT JOIN` it.
    // Ordered by relationship name for deterministic table indices.
    let mut rel_names: Vec<&String> = rel_defs.keys().collect();
    rel_names.sort();
    let mut rel_stage: HashMap<String, String> = HashMap::new();
    for (i, rel) in rel_names.iter().enumerate() {
        let stage = format!("{REL_STAGE_TABLE_PREFIX}{i}");
        let reldef = &rel_defs[*rel];
        // The aggregate columns this relationship needs — one per field that
        // reads it, aliased to that field's name so the write can reference it.
        let mut agg_exprs: Vec<String> = Vec::new();
        for field in &def.fields {
            if let RelFieldPlan::ToManyAgg {
                rel: field_rel,
                agg,
                column,
            } = plan_rel_field(
                &field.expr,
                &field.name,
                &def.source,
                &rel_defs,
                &field_names,
            )
            .expect("fields already classified as supported")
                && &field_rel == *rel
            {
                agg_exprs.push(format!(
                    "{}({}) as {}",
                    agg.to_lowercase(),
                    quote_ident(&column),
                    quote_ident(&field.name),
                ));
            }
        }
        let to_table = quote_ident(&reldef.to_table);
        let to_col = quote_ident(&reldef.to_col);
        client
            .batch_execute(&format!("drop table if exists {stage}"))
            .await?;
        // Group by the join key; a NULL key never joins (SQL `NULL != NULL`), so
        // filtering it out is harmless and lets the key be a primary key.
        client
            .execute(
                &format!(
                    "create temp table {stage} as \
                     select {to_col} as _k, {aggs} from {to_table} \
                     where {to_col} is not null group by {to_col}",
                    aggs = agg_exprs.join(", "),
                ),
                &[],
            )
            .await?;
        client
            .batch_execute(&format!("alter table {stage} add primary key (_k)"))
            .await?;
        rel_stage.insert((*rel).clone(), stage);
    }

    // Build the INSERT's column list, its per-field SELECT expression, and the
    // ON CONFLICT update set. The primary key comes first, then one column per
    // field in definition order (mirroring `backfill_one_to_one`).
    let field_idents: Vec<String> = def.fields.iter().map(|f| quote_ident(&f.name)).collect();
    let select_field_exprs: Vec<String> = def
        .fields
        .iter()
        .zip(&plans)
        .map(|(field, plan)| match plan {
            RelFieldPlan::Source(sql) => sql.clone(),
            RelFieldPlan::ToManyAgg { rel, agg, .. } => {
                let stage_ref = format!("{}.{}", rel_stage[rel], quote_ident(&field.name));
                if agg == "COUNT" {
                    format!("coalesce({stage_ref}, 0)")
                } else {
                    stage_ref
                }
            }
        })
        .collect();

    let insert_cols = std::iter::once(pk_ident.clone())
        .chain(field_idents.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let select_exprs = std::iter::once(pk_qualified.clone())
        .chain(select_field_exprs.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let update_sets = field_idents
        .iter()
        .map(|f| format!("{f} = excluded.{f}"))
        .collect::<Vec<_>>()
        .join(", ");
    debug_assert!(!update_sets.is_empty());

    // Each relationship's staging table LEFT JOINed to the source on its join
    // key, so a source row with no matching to-side rows survives with NULL
    // aggregates (Postgres's empty-correlated-set semantics).
    let joins = rel_names
        .iter()
        .map(|rel| {
            let stage = &rel_stage[*rel];
            let from_col = quote_ident(&rel_defs[*rel].from_col);
            format!("left join {stage} on {stage}._k = {source}.{from_col}")
        })
        .collect::<Vec<_>>()
        .join(" ");

    for (lo, hi) in discover_pk_ranges(&client, &source, pk).await? {
        let where_clause = pk_range_where(&pk_qualified, pk_cast, &lo);
        let insert_sql = format!(
            "insert into {target} ({insert_cols}) \
             select {select_exprs} from {source} {joins} where {where_clause} \
             on conflict ({pk_ident}) do update set {update_sets}"
        );
        match &lo {
            None => {
                client.execute(&insert_sql, &[&hi]).await?;
            }
            Some(lo) => {
                client.execute(&insert_sql, &[lo, &hi]).await?;
            }
        }
    }

    // Drop the staging tables before the connection returns to the pool.
    for stage in rel_stage.values() {
        client
            .batch_execute(&format!("drop table if exists {stage}"))
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::ast::{FieldDef, Operator, Predicate};
    use super::*;

    /// `total = double_price + tax`, where `double_price` is itself a
    /// calculated field (`price + price`) — the shape that used to crash both
    /// direct-build renderers with a raw Postgres "column does not exist"
    /// error (issue #83 follow-up).
    fn cross_alias_def() -> TransformDef {
        TransformDef {
            target: "order_totals".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![
                FieldDef {
                    name: "double_price".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("price".to_string())),
                        rhs: Box::new(Expr::Column("price".to_string())),
                    },
                },
                FieldDef {
                    name: "total".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("double_price".to_string())),
                        rhs: Box::new(Expr::Column("tax".to_string())),
                    },
                },
            ],
            predicate: Predicate::True,
        }
    }

    /// A field referencing a source column that happens to share its own
    /// name (`price AS price`) — the legitimate self-passthrough pattern
    /// `validate::infer_expr` explicitly carves out, which is *not* an alias
    /// reference and must still take the direct-build path.
    fn self_passthrough_def() -> TransformDef {
        TransformDef {
            target: "order_view".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "price".to_string(),
                expr: Expr::Column("price".to_string()),
            }],
            predicate: Predicate::True,
        }
    }

    #[test]
    fn uses_cross_field_alias_detects_a_field_referencing_another_fields_alias() {
        assert!(uses_cross_field_alias(&cross_alias_def()));
    }

    #[test]
    fn uses_cross_field_alias_allows_self_passthrough() {
        assert!(!uses_cross_field_alias(&self_passthrough_def()));
    }

    #[test]
    fn uses_cross_field_alias_allows_plain_definitions_with_no_field_name_collisions() {
        let def = TransformDef {
            target: "t".to_string(),
            source: "s".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "x".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("a".to_string())),
                    rhs: Box::new(Expr::Column("a".to_string())),
                },
            }],
            predicate: Predicate::True,
        };
        assert!(!uses_cross_field_alias(&def));
    }

    /// `total = post_count + comment_count`, where both are to-many-aggregate
    /// fields on a relationship-enriched 1-1 definition — the relationship
    /// path's counterpart of `cross_alias_def`'s bug (issue #83 follow-up).
    fn rel_field_names() -> HashSet<&'static str> {
        HashSet::from(["post_count", "comment_count", "total"])
    }

    #[test]
    fn plan_rel_field_falls_back_for_a_field_referencing_a_to_many_aggregate_fields_alias() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("post_count".to_string())),
            rhs: Box::new(Expr::Column("comment_count".to_string())),
        };
        let plan = plan_rel_field(
            &expr,
            "total",
            "authors",
            &HashMap::new(),
            &rel_field_names(),
        );
        assert!(
            plan.is_none(),
            "a field referencing another to-many-aggregate field's alias must not be classified \
             as a plain Source expression"
        );
    }

    #[test]
    fn plan_rel_field_still_classifies_a_plain_source_field() {
        let expr = Expr::Column("name".to_string());
        let field_names: HashSet<&str> = HashSet::from(["name"]);
        let plan = plan_rel_field(&expr, "name", "authors", &HashMap::new(), &field_names);
        assert!(
            matches!(plan, Some(RelFieldPlan::Source(_))),
            "a plain source column reference (no alias collision) must still direct-build"
        );
    }
}
