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

use std::collections::HashMap;

use crate::pool::{Pool, quote_ident};

use super::ast::{Expr, KeySpace, TransformDef, ValueType};
use super::ddl::{
    self, PrimaryKeyColumn, avg_partial_columns, count_partial_column, qualified_target_table,
    source_primary_key,
};
use super::invertibility::{AggregateArg, CountArg, classify};
use super::oracle::render_expr_sql;

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
            backfill_one_to_one(pool, def, target_schema, &pk).await
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
    if uses_relationships(def) {
        return Err(BackfillError::Unsupported(
            "relationship-enriched 1-1 definitions".to_string(),
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

        let where_clause = match &lo {
            None => format!("{pk_ident} <= $1::text::{pk_cast}"),
            Some(_) => {
                format!("{pk_ident} > $1::text::{pk_cast} and {pk_ident} <= $2::text::{pk_cast}")
            }
        };
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

        lo = Some(hi);
    }

    Ok(())
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
    let mut insert_cols: Vec<String> = group_idents.clone();
    let mut stage_exprs: Vec<String> = group_idents.clone();
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
                insert_cols.push(quote_ident(&count_partial_column(&field.name)));
                stage_exprs.push(format!("count({arg})"));
            }
            FieldKind::Avg => {
                let arg = agg_arg_sql(&field.expr);
                let (sum_col, count_col) = avg_partial_columns(&field.name);
                insert_cols.push(quote_ident(&sum_col));
                stage_exprs.push(format!("sum({arg})"));
                insert_cols.push(quote_ident(&count_col));
                stage_exprs.push(format!("count({arg})"));
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
