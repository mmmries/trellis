//! Aggregate targets on the group basis (ADR-0002 "Aggregates: a
//! basis per group, kept by deltas").
//!
//! # Which definitions take it
//!
//! The group basis keeps every aggregate definition: define refuses an
//! aggregate outside the admitted set by family
//! (`defs::validate::validate_aggregate_admitted`), so [`route`] takes every
//! aggregate define accepts. A definition [`route`] takes:
//!
//! - is built, fresh or on a resume, by the plan job's one absolute write of
//!   every group ([`rebuild`]), over a partitioned source one transaction per
//!   leaf of its recorded tree and one for the groups that route to no leaf
//!   (an unrecorded tree only builds over an empty source); a resume drops
//!   and recreates the hidden table and keeps the visible target;
//! - is rebuilt in place once `live` ([`rebuild`]: `request_backfill`, a
//!   capture re-install, an automatic rebuild, or an upstream's rebuild),
//!   over a partitioned source one transaction per leaf of its recorded tree
//!   and one for the groups that route to no leaf (`super::build`'s
//!   `run_group_rebuild`).
//!
//! Over a partitioned source, its partition maintenance is corrected by
//! recomputing the groups that route to the node
//! ([`recompute_routed`], `super::partition_correction`'s "On the group
//! basis").
//!
//! # A source that is another definition's target
//!
//! A target is never captured. Its writers stage its changes through the
//! target-mutation seam, and a target with a group-basis reader is on the
//! seam's image feed (`super::target_mutations`, "The image feed"): each
//! changed key arrives as a CDC-shaped ring row with its exact old and new
//! images, ordered by the writer's write token, with the writer's
//! transaction as its `row_txid`. A page consumes those rows exactly as it
//! consumes captured ones. An upstream group with no visible row is absent
//! from the images, so a rollup's sums telescope as the group empties and
//! refills.
//!
//! An upstream build or rebuild that writes the target outside the seam
//! parks a catch-up for its readers, and a group-basis reader is rebuilt
//! instead (`crate::intake::catch_up::park_target_catchup_if_read`): an
//! image-less catch-up names keys, not the deltas a group needs. A group-basis
//! build that is a fresh build or a resume parks it too, though it writes
//! through the seam: a resume empties the partials and keeps the target, so
//! the prior images of the seam rows it and its pages stage match no row the
//! readers hold, and the readers' rebuilds overwrite what they did.
//!
//! # Which definitions
//!
//! A plain aggregate (every `GROUP BY` key a source column) whose every value field
//! composes the admitted aggregates (`COUNT(*)`, `COUNT(x)`, `SUM`/`AVG` over
//! an integer or `numeric` argument, and `MIN`/`MAX` over a plain source
//! column of an ordered type with no collation, "Extremes" below) with
//! literals, operators, functions and its `GROUP BY` keys. A `SUM`/`AVG`
//! argument may be an expression over the row (`SUM(a + b)`). Define refuses
//! any other aggregate by family.
//!
//! # Where its state lives
//!
//! The visible target is a plain table holding exactly the declared columns
//! and the `UNIQUE NULLS NOT DISTINCT` group identity. The
//! group's partials, its basis and its stamp live in a hidden table in the
//! instance's catalog schema, named by definition id ([`hidden_table_name`],
//! `agg_<id>`, beside a `JOIN`'s `join_<id>_*` tables): the row count
//! `__n`, and per distinct aggregate argument `i` (1-based, in
//! [`distinct_arguments`]' order) its non-null count `__count_<i>` and,
//! when a `SUM` or `AVG` reads it, its sum `__sum_<i>` as `numeric`, with a
//! `numeric` argument's scale and special-value counts beside it ("Numeric
//! sums" below). A group row is visible in the target only while `__n > 0`;
//! a zero group stays in the hidden table until the GC collects it (below).
//!
//! # Numeric sums
//!
//! Postgres shows `sum(numeric)` at the largest display scale among the
//! group's current finite values (`sum({1.5, 2.000})` is `3.500`, and `1.5`
//! once `2.000` leaves), which a running sum can't follow: it keeps every
//! scale it ever held. `avg(numeric)` takes its result scale from the sum's,
//! so a leftover scale changes its value too. And no running sum can take a
//! special value back out: `NaN − NaN` is `NaN`, `Infinity − Infinity` is
//! `NaN`, and so is `NaN × 0` in a `JOIN` product. So a `numeric` argument
//! keeps [`NUMERIC_PARTS`] ([`SumPart`]): `__sum_<i>`, the sum of its finite
//! values only; `__scales_<i>`, a `jsonb` object from each display scale
//! (`scale(x)`, 0 to 16383) to its count of finite values, with no zero
//! entry; and `__nan_<i>`, `__pinf_<i>` and `__ninf_<i>`, the counts of
//! `NaN`, `Infinity` and `-Infinity`. Every one is a sum or a count, so
//! every partial operation keeps it as it keeps a sum: a page's sides carry
//! each value's scale and class ([`SumPart::of_rows`]), an absolute write
//! counts them from the source, a `JOIN` product multiplies each by the
//! match count ([`SumPart::times`]), and a group is zero only when they are
//! all zero. The visible `SUM` ([`sum_value`]) is `NaN` while it counts a
//! `NaN` or both infinities, the infinity while it counts one, and otherwise
//! the finite sum rounded to the largest scale it counts a value of, which
//! is exactly Postgres's; the `AVG` divides that by the count. A
//! `numeric(p,s)` argument keeps the same partials (its map has the one
//! scale `s`); an integer argument keeps its sum alone ([`INTEGER_PARTS`]).
//! The maps are merged in SQL with no function of Trellis's own, so the
//! catalog needs none.
//!
//! # Zero groups
//!
//! A group whose partials are all zero (`__zero`, a stored generated
//! column) has no visible row, but its hidden row can still matter (ADR-0002
//! R4): an absolute write (a build's or a recompute's) sets
//! its basis, and a change that basis saw but a page hasn't drained yet must
//! still be refused. A fresh row has no basis and would apply it twice. So
//! the absolute write also sets the stamp, the newest segment holding a
//! change its snapshot saw (R5), and a zero group with a basis stays until
//! the contiguous drained prefix reaches its stamp: then every change the
//! basis saw has been applied or refused. A zero group with no basis
//! protects nothing, and a group with any non-zero partial is never
//! collected, whatever its `__n`. A basis with no stamp keeps its group.
//!
//! The maintenance loop's tombstone pass collects them
//! (`super::retire::collect_tombstones`), in batches that claim rows `for
//! update skip locked` in key order through the partial `where __zero`
//! index ([`collect_zero_groups_sql`]). It never waits on a page: a page
//! holds the locks of every group it applies to from its lock statement to
//! its commit. A page whose lock statement meets a group the GC is deleting
//! waits for the GC's commit and inserts the group afresh. Pages only make
//! `__zero` change when a group empties or refills, so an update that keeps
//! a group non-zero changes no indexed column and stays HOT.
//!
//! A `JOIN`'s `P` and `M` cells are groups of this kind, written by the
//! same writer, and their zero cells are collected by the same rule and
//! statement (`crate::defs::join`'s hidden tables). The pass takes no `J`
//! lock: a zero cell adds nothing to the product, so a page that reads the
//! key's cells under `J` gets the same `T` delta with it or without it. Its
//! `J` rows are never collected (ADR-0019).
//!
//! Its `T` groups are collected too, whatever the drained prefix: `T` has no
//! basis, since it changes only by the product's deltas, which no basis
//! filters, so a zero `T` group protects nothing, and adding a delta to it or
//! to a fresh group gives the same row. Every writer of `T` (a page, a
//! release, a partition correction, each build step) creates and locks the
//! groups it writes in one sorted `insert … on conflict` before it adds to
//! them, so a group the pass removed is inserted afresh and no delta lands on
//! a group the writer hasn't locked. A build's enumeration of `T`'s groups
//! reads without a lock, and its lock statement re-inserts a group collected
//! since, at zero. Nothing reads `T` to compute a delta, and `self_check`
//! counts a group `T` lacks as zeros.
//!
//! Zero groups left in place aren't free: a JOIN model measured 16x slower
//! with thousands of them on a few hot keys, and a definition whose groups
//! churn (a `GROUP BY` over a session id, say) would grow its hidden table
//! without bound.
//!
//! # One page, one target ([`apply_group_target`])
//!
//! 1. **Stage** (in [`lock_groups`]): one statement splits each ring row
//!    into sides (its old image negated and its new image, or its
//!    transition images), evaluates the group and each argument on
//!    each, and sums them per (group, `row_txid`, statement) into a session
//!    temporary table. It reads no basis, so this work, most of a page's,
//!    happens before any group is locked.
//! 2. **Lock** ([`lock_groups`], R1, R7): one statement inserts every group
//!    the staged sides name, sorted, and locks the ones that exist
//!    (`on conflict … do update … where false` locks without writing).
//! 3. A caller may do its own work here, holding the groups' locks: a
//!    `JOIN` (`super::join`) calls the steps itself, on its `P`, `M` and
//!    `T` ([`Writer`]), with its `J` locks between them.
//! 4. **Apply** ([`apply_locked_groups`], R2, F2): one later statement, so
//!    under `READ COMMITTED` its snapshot sees every committed basis. It
//!    sums the staged sides per (group, `row_txid`), keeps a sum only when
//!    its transaction isn't visible in the group's basis (a `NULL` basis
//!    sees nothing) and its `lsn` is above the target's truncate floor, adds
//!    what is kept to the hidden row, and writes the visible row when a
//!    visible value changed: an upsert while `__n > 0`, a delete when it
//!    falls to 0. It returns, per group, the change it applied. It runs with
//!    nested loops off, since its joins are between the page's own rows,
//!    whose sizes the planner can't see, and sequential scans off: it reaches
//!    the hidden table and the target only through their group identity
//!    index, one probe per group, and the hidden rows it updates by `ctid`,
//!    so its cost under the groups' locks is the page's groups, not the
//!    definition's.
//!
//! # Extremes (`MIN`/`MAX`)
//!
//! `MIN` and `MAX` have no inverse: removing a group's current extreme
//! leaves a next one no partial records. So the group keeps each extreme
//! as a value (`__min_<i>`/`__max_<i>`, at the argument's type, one per
//! distinct call) with a basis of its own, `__xbasis`, and its stamp,
//! `__xstamp`, and re-reads the extremes from the source when a retraction
//! reaches one (#1117's rules Rx1–Rx8). A definition with no `MIN`/`MAX`
//! has none of these columns, and its statements are as they were.
//!
//! - **The filter (Rx2).** A (group, transaction) sum applies to the
//!   extremes only when its transaction isn't visible in `__xbasis` (a
//!   `NULL` basis sees nothing), and to the partials only when it isn't
//!   visible in `__basis`; both above the truncate floor. The two bases
//!   differ: a probe stamps only `__xbasis`, so a change it saw still
//!   applies to the partials. Stamping `__basis` instead (one basis per
//!   group) would refuse that change's counts.
//! - **Insertions (Rx3).** Staging keeps the extreme of a (group,
//!   transaction, statement)'s insertion sides apart from its retraction
//!   sides' ([`staged_columns`]), and the apply moves each extreme by
//!   `least`/`greatest` to its insertions'. An insertion must be filtered:
//!   `least` is idempotent, but an insertion of a row since deleted, applied
//!   after the deletion's probe, would bring the deleted value back.
//! - **The trigger (Rx4).** After that, a group whose retraction sides
//!   reach an extreme (at or below a minimum, at or above a maximum), or
//!   whose extreme is `NULL` while it has a non-`NULL` retraction, is probed.
//!   A retraction below the stored minimum can arrive before its insertion,
//!   and one onto a `NULL` extreme before the insertion that would set it.
//! - **The probe (Rx5, [`probe_locked_groups`]).** In the page's own
//!   transaction, after its lock statement and its apply: one statement
//!   takes the snapshot and the newest segment, re-reads every extreme of
//!   each probed group from the source (one index endpoint each, with an
//!   index on the `GROUP BY` columns then the argument), and writes them
//!   with the snapshot as `__xbasis` and the segment as `__xstamp`. A probe
//!   read before the lock could miss a change another page applied in
//!   between. It takes the source's `ACCESS SHARE` lock while the page holds
//!   its groups, under the session's `lock_timeout` (I7): a timeout rolls the
//!   page back and the drain retries it.
//! - **Absolute writes (Rx6).** A build, rebuild, recompute, release and
//!   partition correction write the extremes, `__xbasis` and `__xstamp` from
//!   the snapshot they write the partials from.
//! - **The GC (Rx7).** A zero group is collected only when both its stamps
//!   are at or below the drained prefix (or the basis is missing): a probe's
//!   basis can have seen an insertion whose page hasn't drained.
//! - **Truncate (Rx8).** The truncate's wipe of the hidden table removes the
//!   extremes with the partials.
//!
//! The visible row is written from the partials and the extremes. The apply
//! and the probe each read an extreme's prior value with the group's
//! hidden row, for the visible row's prior image, since an extreme has no
//! change to subtract.
//!
//! # Image-less changes, releases and truncates
//!
//! An image-less change (a `Recompute`: a release of a key, or a marker's
//! re-read) has no sides. The
//! page recomputes the groups it names instead: its key's current row's
//! group and its prior-image hint's. The page's lock statement locks those
//! groups with its sides' groups, and after the page's sides apply,
//! [`recompute_locked_groups`] rewrites each of them from a `GROUP BY` of its
//! source rows, the statement's snapshot as its basis (R1, R5, R6).
//!
//! A held key's changes are skipped and parked: its held row keeps the
//! groups of every parked image (`poison_held.agg_groups`, the evicting
//! change's among them), not the images. A release or the automatic retry
//! recomputes those groups and the key's current row's ([`release`]), so
//! every group the key could owe is rewritten (R8) whatever absolute writes
//! reached them during the hold.
//!
//! A source `TRUNCATE` empties the hidden table and raises the target's
//! truncate floor on the truncate's own page ([`truncate_groups`]); the
//! drain barrier orders that page after every page of earlier segments and
//! before every page of later ones (R9).
//!
//! # A build or rebuild ([`rebuild`])
//!
//! A build or rebuild recomputes every group in place, as one absolute write
//! per source (a fresh build starts from an empty hidden table, and its
//! enumeration finds the groups in the source and the ring): lock the groups of the
//! enumeration union (the target's, the source's, and those the ring's old
//! images name; R8) in key order, then one statement runs the source's
//! `GROUP BY` from its snapshot and replaces each group's partials, basis,
//! stamp and visible row. Pages keep applying, against the new bases once it
//! commits; readers see the old values until then. It runs as the rebuild's
//! one job (`super::build`'s sweep job, which a group-basis definition's
//! rebuild is alone), and a fresh build's is its plan job, after the plan's
//! fence for a source that is another definition's target.
//!
//! Sides are never folded per key: the fold's first old and last
//! new image are wrong when a group's basis falls between two of a key's
//! changes. So each folded change carries its key's ring rows
//! (`super::fold::FoldedChange::group_sides`).
//!
//! A composed field (`SUM(a) + COUNT(b)`) is computed from the partials
//! whenever the visible row is written. Each changed visible row reaches the
//! target-mutation seam with its prior image, computed from the partials
//! before the page's change.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, KeySpace, TransformDef, ValueType, group_by_contains};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::oracle::{render_expr_sql, render_qualified_expr_sql};
use crate::integer::IntWidth;
use crate::pool::{jsonb_build_object_sql, quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::target_mutations::TargetMutations;

/// Planted bug `gb_fold_sides_per_key` (`crate::plant`): a key's ring rows
/// (`FoldedChange::group_sides`) folded into one, as the per-key fold folds
/// them: the first row's old image and the last row's new image, under the
/// last row's transaction and `lsn`. `None` when there is nothing to fold
/// (one row), or a row is a `Recompute` or carries transition images.
#[cfg(any(test, feature = "test-util"))]
fn planted_fold(sides: &[String]) -> Option<String> {
    if sides.len() < 2 {
        return None;
    }
    let rows: Vec<serde_json::Value> = sides
        .iter()
        .map(|side| serde_json::from_str(side).ok())
        .collect::<Option<_>>()?;
    if rows.iter().any(|row| {
        !matches!(row["op"].as_str(), Some("insert" | "update" | "delete")) || row["x"].is_object()
    }) {
        return None;
    }
    let (first, last) = (&rows[0], &rows[rows.len() - 1]);
    Some(
        serde_json::json!({
            "t": last["t"],
            "l": last["l"],
            "op": "update",
            "o": first["o"],
            "n": last["n"],
            "x": null,
        })
        .to_string(),
    )
}

/// One aggregate call of a definition's fields, over the group's partials.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum Agg {
    /// `COUNT(*)`: `__n`.
    CountStar,
    /// `COUNT(x)`: argument `x`'s non-null count.
    Count(usize),
    /// `SUM(x)`: its sum, `NULL` while its count is 0, cast to `cast`
    /// (`bigint` over `int2`/`int4`, `numeric` otherwise, as Postgres's
    /// `sum()` returns).
    Sum(usize, &'static str),
    /// `AVG(x)`: its sum over its count, as `numeric` (`numeric_div`, which
    /// is what Postgres's `avg()` over an exact argument computes).
    Avg(usize),
    /// `MIN(x)` (`max` false) or `MAX(x)` (`max` true) over the plain
    /// source column argument `arg`, whose type is `pg_type`: the group's
    /// hidden extreme ([`Extreme`]).
    Extreme {
        max: bool,
        arg: usize,
        pg_type: String,
    },
}

/// One hidden extreme of a group (see "Extremes" in the module doc): `MIN`
/// or `MAX` of the plain source column argument `arg` (0-based in
/// [`GroupShape::args`]), at the argument's type.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Extreme {
    pub(super) max: bool,
    pub(super) arg: usize,
    pub(super) pg_type: String,
}

impl Extreme {
    fn stem(&self) -> &'static str {
        if self.max { "max" } else { "min" }
    }

    /// The hidden table's column: `__min_<i>` or `__max_<i>`.
    pub(super) fn column(&self) -> String {
        format!("__{}_{}", self.stem(), self.arg + 1)
    }

    /// A working column named `<prefix><stem><i>` (`__ominN`, `__nmaxN`, …).
    fn named(&self, prefix: &str) -> String {
        format!("{prefix}{}{}", self.stem(), self.arg + 1)
    }

    /// The staged extreme of the page's insertion sides (`+`) of one
    /// (group, transaction, statement).
    fn plus(&self) -> String {
        self.named("__p")
    }

    /// The staged extreme of its retraction sides (`-`).
    fn minus(&self) -> String {
        self.named("__m")
    }

    /// The aggregate that combines values toward the extreme.
    fn aggregate(&self) -> &'static str {
        self.stem()
    }

    /// The function that moves the extreme by an insertion (Rx3).
    fn combine(&self) -> &'static str {
        if self.max { "greatest" } else { "least" }
    }

    /// `value` reaches `extreme`: at or below a minimum, at or above a
    /// maximum (Rx4).
    fn reaches(&self, value: &str, extreme: &str) -> String {
        let op = if self.max { ">=" } else { "<=" };
        format!("{value} {op} {extreme}")
    }

    /// The order a probe reads the argument in, the extreme first.
    fn order(&self) -> &'static str {
        if self.max { " desc" } else { "" }
    }
}

/// One distinct aggregate argument.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Arg {
    /// The argument over the image populated as the source's row type `s`.
    pub(super) sql: String,
    /// The argument, for the audit to render on its own
    /// (`super::self_check_groups`).
    pub(super) expr: Expr,
    /// Whether a `SUM` or `AVG` reads it, so it has a `__sum_<i>`.
    pub(super) summed: bool,
    /// Whether it is a `numeric` argument a `SUM` or `AVG` reads, so it
    /// keeps [`NUMERIC_PARTS`] (see "Numeric sums" in the module doc).
    pub(super) numeric: bool,
    /// The scale a `numeric` sum's column declares (`numeric(p,s)`, a
    /// negative `s` as 0), when the plan read one ([`declare_scales`]): every
    /// finite value it holds has that scale, so the statements count it
    /// without folding scale maps. Only a hint: each statement stays exact
    /// for a map that counts another scale, which a change of the column's
    /// type can leave behind. `None` for any other argument, and in every
    /// plan that reads no declared scale.
    pub(super) scale: Option<i32>,
}

/// What a group-basis definition's target looks like ([`route`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupShape {
    /// The `GROUP BY` columns: the source's, the target's and the hidden
    /// table's column of that name.
    pub(super) group_cols: Vec<String>,
    /// Each `GROUP BY` column's type in the target and hidden table. Unread
    /// by a `JOIN`'s writers, whose working tables copy the hidden table's
    /// columns ([`Writer`]).
    pub(super) group_types: Vec<String>,
    pub(super) args: Vec<Arg>,
    pub(super) aggs: Vec<Agg>,
    /// Each value field: its name, its type in the target, and its
    /// expression with each aggregate call replaced by its placeholder
    /// column ([`placeholder`]).
    pub(super) fields: Vec<(String, String, Expr)>,
}

impl GroupShape {
    /// The `GROUP BY` columns: the source's, the target's and the hidden
    /// table's column of that name.
    pub(crate) fn group_columns(&self) -> &[String] {
        &self.group_cols
    }

    /// The distinct aggregate arguments, each with its partials.
    pub(crate) fn arguments(&self) -> &[Arg] {
        &self.args
    }

    /// Each `GROUP BY` column's type in the target and hidden table, in
    /// [`Self::group_columns`]' order.
    pub(crate) fn group_types(&self) -> &[String] {
        &self.group_types
    }

    /// The hidden extremes, one per distinct `MIN`/`MAX` call, in
    /// [`Self::aggs`]' order. Empty for every definition with no `MIN` or
    /// `MAX`, whose hidden table and statements are as they were.
    pub(super) fn extremes(&self) -> Vec<Extreme> {
        self.aggs
            .iter()
            .filter_map(|agg| match agg {
                Agg::Extreme { max, arg, pg_type } => Some(Extreme {
                    max: *max,
                    arg: *arg,
                    pg_type: pg_type.clone(),
                }),
                _ => None,
            })
            .collect()
    }

    /// The source column each `MIN`/`MAX` reads, once each, in
    /// [`Self::extremes`]' order: an extreme's probe reads one endpoint of a
    /// source index on the `GROUP BY` columns then this column, and scans
    /// the group without one. Empty for a definition with no `MIN` or `MAX`.
    pub(crate) fn extreme_arguments(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for x in self.extremes() {
            if let Some(column) = extreme_column(&self.args[x.arg].expr)
                && !out.iter().any(|c| c == column)
            {
                out.push(column.to_string());
            }
        }
        out
    }
}

/// The plain source column `MIN`/`MAX` reads, if `arg` is one.
fn extreme_column(arg: &Expr) -> Option<&str> {
    match arg {
        Expr::Column(column) | Expr::SourceColumn { column, .. } => Some(column),
        _ => None,
    }
}

/// The column an aggregate call is read from inside a field's expression.
pub(super) fn placeholder(k: usize) -> String {
    format!("__trellis_a{k}")
}

/// Whether the group basis takes `def` (see the module doc), and its shape
/// if so.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<GroupShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    let mut group_cols = Vec::with_capacity(group_by.len());
    let mut group_types = Vec::with_capacity(group_by.len());
    for key in group_by {
        let value_type = source_columns.get(key.as_str())?;
        group_cols.push(key.clone());
        group_types.push(ddl::pg_type_name(*value_type).into_owned());
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let field_types = crate::defs::validate::infer_field_types(def, source_columns).ok()?;
    let distinct = distinct_arguments(&def.fields, group_by, &substituted);
    let mut shape = GroupShape {
        group_cols,
        group_types,
        args: distinct
            .iter()
            .map(|arg| Arg {
                sql: render_qualified_expr_sql(arg, "s"),
                expr: arg.clone(),
                summed: false,
                numeric: false,
                scale: None,
            })
            .collect(),
        aggs: Vec::new(),
        fields: Vec::new(),
    };
    let arg_of = |arg: &Expr| distinct.iter().position(|d| d == arg);
    for field in &def.fields {
        if group_by_contains(group_by, &field.name) {
            continue;
        }
        let type_of = |arg: &Expr| crate::defs::validate::infer_expr_type(arg, source_columns).ok();
        let expr = rewrite_with(
            &substituted[&field.name],
            &type_of,
            &arg_of,
            &mut shape,
            true,
        )?;
        let pg_type = ddl::pg_type_name(*field_types.get(&field.name)?).into_owned();
        shape.fields.push((field.name.clone(), pg_type, expr));
    }
    Some(shape)
}

/// `expr` with each aggregate call replaced by its placeholder column, the
/// call added to `shape.aggs`; `None` when some call isn't admitted.
/// `type_of` types an aggregate's argument, and `arg_of` finds its index in
/// `shape.args`.
pub(super) fn rewrite(
    expr: &Expr,
    type_of: &dyn Fn(&Expr) -> Option<ValueType>,
    arg_of: &dyn Fn(&Expr) -> Option<usize>,
    shape: &mut GroupShape,
) -> Option<Expr> {
    rewrite_with(expr, type_of, arg_of, shape, false)
}

/// [`rewrite`], admitting `MIN`/`MAX` over a plain source column when
/// `extremes` is set: a plain aggregate's ([`route`]). A `JOIN`'s `T` keeps
/// none.
fn rewrite_with(
    expr: &Expr,
    type_of: &dyn Fn(&Expr) -> Option<ValueType>,
    arg_of: &dyn Fn(&Expr) -> Option<usize>,
    shape: &mut GroupShape,
    extremes: bool,
) -> Option<Expr> {
    let rewrite =
        |e: &Expr, shape: &mut GroupShape| rewrite_with(e, type_of, arg_of, shape, extremes);
    match expr {
        Expr::FunctionCall { name, args } if is_aggregate(name) => {
            let agg = match (name.to_ascii_uppercase().as_str(), args.as_slice()) {
                (function @ ("MIN" | "MAX"), [arg]) if extremes => {
                    extreme_column(arg)?;
                    let value_type = type_of(arg)?;
                    if !crate::defs::registry::extreme_type_admitted(value_type) {
                        return None;
                    }
                    Agg::Extreme {
                        max: function == "MAX",
                        arg: arg_of(arg)?,
                        pg_type: ddl::pg_type_name(value_type).into_owned(),
                    }
                }
                ("COUNT", []) => Agg::CountStar,
                ("COUNT", [arg]) => Agg::Count(arg_of(arg)?),
                (function @ ("SUM" | "AVG"), [arg]) => {
                    let i = arg_of(arg)?;
                    let value_type = type_of(arg)?;
                    let cast = match value_type {
                        ValueType::Integer(IntWidth::Int2 | IntWidth::Int4) => "bigint",
                        ValueType::Integer(IntWidth::Int8) => "numeric",
                        ValueType::Numeric => "numeric",
                        _ => return None,
                    };
                    shape.args[i].summed = true;
                    shape.args[i].numeric |= value_type == ValueType::Numeric;
                    if function == "SUM" {
                        Agg::Sum(i, cast)
                    } else {
                        Agg::Avg(i)
                    }
                }
                _ => return None,
            };
            let k = match shape.aggs.iter().position(|a| *a == agg) {
                Some(k) => k,
                None => {
                    shape.aggs.push(agg);
                    shape.aggs.len() - 1
                }
            };
            Some(Expr::Column(placeholder(k)))
        }
        Expr::FunctionCall { name, args } => Some(Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite(a, shape))
                .collect::<Option<Vec<_>>>()?,
        }),
        Expr::BinaryOp { op, lhs, rhs } => Some(Expr::BinaryOp {
            op: *op,
            lhs: Box::new(rewrite(lhs, shape)?),
            rhs: Box::new(rewrite(rhs, shape)?),
        }),
        Expr::Negate(operand) => Some(Expr::Negate(Box::new(rewrite(operand, shape)?))),
        Expr::Cast {
            expr: operand,
            target,
        } => Some(Expr::Cast {
            expr: Box::new(rewrite(operand, shape)?),
            target: *target,
        }),
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => expr
            .map_logic_children(|child| rewrite(child, &mut *shape).ok_or(()))
            .expect("a logic node")
            .ok(),
        other => Some(other.clone()),
    }
}

/// The distinct arguments of the aggregate calls in the non-`GROUP BY`
/// fields, in field declaration order, then left to right within a field:
/// one per distinct argument, so `SUM(x)` and `AVG(x)` share one. `COUNT(*)`
/// has none. `substituted` is every field's cross-field-alias-resolved
/// expression ([`crate::defs::backfill::substituted_field_exprs`]), so an
/// aliased aggregate (`t2 = t` where `t = SUM(x)`) resolves to its source's
/// argument.
fn distinct_arguments(
    fields: &[crate::defs::ast::FieldDef],
    group_by: &[String],
    substituted: &HashMap<String, Expr>,
) -> Vec<Expr> {
    fn walk(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::FunctionCall { name, args } if args.len() == 1 && is_aggregate(name) => {
                if !out.contains(&args[0]) {
                    out.push(args[0].clone());
                }
            }
            Expr::FunctionCall { args, .. } => args.iter().for_each(|a| walk(a, out)),
            Expr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, out);
                walk(rhs, out);
            }
            Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => walk(operand, out),
            Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                for child in expr.logic_children() {
                    walk(child, out);
                }
            }
            Expr::Column(_)
            | Expr::SourceColumn { .. }
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::BooleanLiteral(_)
            | Expr::Null
            | Expr::TypedLiteral { .. } => {}
        }
    }
    let mut out = Vec::new();
    for field in fields {
        if !group_by_contains(group_by, &field.name) {
            walk(&substituted[&field.name], &mut out);
        }
    }
    out
}

/// Whether `name` is an aggregate function, admitted or not.
fn is_aggregate(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    crate::defs::registry::AGGREGATE_FUNCTIONS.contains(&upper.as_str())
        || crate::defs::validate::is_deferred_aggregate_name(&upper)
}

/// The hidden table's bare name in the catalog schema, for definition `id`
/// (see the module doc).
pub(crate) fn hidden_table_name(id: i64) -> String {
    format!("agg_{id}")
}

/// [`hidden_table_name`] as SQL, over the definition id expression `id`:
/// for a statement that asks which definitions are on the group basis by
/// whether their hidden table exists (`super::target_mutations`' image
/// feed).
pub(crate) fn hidden_table_name_sql(id: &str) -> String {
    format!("('agg_' || {id})")
}

/// The hidden table of definition `id`, quoted and qualified by the catalog
/// schema `schema`.
pub(crate) fn hidden_table_ident(schema: &str, id: i64) -> String {
    format!(
        "{}.{}",
        quote_ident(schema),
        quote_ident(&hidden_table_name(id))
    )
}

/// One partial a summed argument keeps beside its non-null count (see
/// "Numeric sums" in the module doc). An integer argument keeps its sum
/// alone ([`INTEGER_PARTS`]); a `numeric` one keeps all five
/// ([`NUMERIC_PARTS`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SumPart {
    /// The sum of the finite values, as `numeric`.
    Sum,
    /// The count of finite values per display scale (`scale(x)`), as a
    /// `jsonb` object from the scale's text to its count, with no zero
    /// entry: `{"2": 3, "5": 1}`.
    Scales,
    /// The count of `NaN`s.
    NaN,
    /// The count of `Infinity`s.
    PosInf,
    /// The count of `-Infinity`s.
    NegInf,
}

/// The partials of a summed integer argument: its sum.
pub(crate) const INTEGER_PARTS: &[SumPart] = &[SumPart::Sum];

/// The partials of a summed `numeric` argument, in the order every
/// statement and table lists them.
pub(crate) const NUMERIC_PARTS: &[SumPart] = &[
    SumPart::Sum,
    SumPart::Scales,
    SumPart::NaN,
    SumPart::PosInf,
    SumPart::NegInf,
];

/// The empty scale map.
pub(super) const NO_SCALES: &str = "'{}'::jsonb";

impl SumPart {
    /// The stem of its working columns: `<prefix><stem><i>`, so `__ds1`,
    /// `__dk1`, `__dnan1`, ….
    fn stem(self) -> &'static str {
        match self {
            SumPart::Sum => "s",
            SumPart::Scales => "k",
            SumPart::NaN => "nan",
            SumPart::PosInf => "pinf",
            SumPart::NegInf => "ninf",
        }
    }

    /// Its working column for argument `i` (1-based) under `prefix`.
    pub(crate) fn name(self, prefix: &str, i: usize) -> String {
        format!("{prefix}{}{i}", self.stem())
    }

    /// Its hidden-table column for argument `i` (1-based).
    pub(crate) fn hidden(self, i: usize) -> String {
        match self {
            SumPart::Sum => format!("__sum_{i}"),
            SumPart::Scales => format!("__scales_{i}"),
            SumPart::NaN => format!("__nan_{i}"),
            SumPart::PosInf => format!("__pinf_{i}"),
            SumPart::NegInf => format!("__ninf_{i}"),
        }
    }

    /// Its SQL type.
    pub(crate) fn sql_type(self) -> &'static str {
        match self {
            SumPart::Sum => "numeric",
            SumPart::Scales => "jsonb",
            SumPart::NaN | SumPart::PosInf | SumPart::NegInf => "bigint",
        }
    }

    /// Its zero, typed.
    pub(crate) fn zero(self) -> &'static str {
        match self {
            SumPart::Sum => "0::numeric",
            SumPart::Scales => NO_SCALES,
            SumPart::NaN | SumPart::PosInf | SumPart::NegInf => "0::bigint",
        }
    }

    /// Its hidden-table column definition for argument `i`.
    pub(crate) fn ddl(self, i: usize) -> String {
        let default = match self {
            SumPart::Scales => "'{}'",
            _ => "0",
        };
        format!(
            "{} {} not null default {default}",
            self.hidden(i),
            self.sql_type()
        )
    }

    /// Whether `value` is this partial's zero.
    pub(crate) fn is_zero(self, value: &str) -> String {
        match self {
            SumPart::Scales => format!("{value} = {NO_SCALES}"),
            _ => format!("{value} = 0"),
        }
    }

    /// Whether `value` is not this partial's zero.
    pub(crate) fn nonzero(self, value: &str) -> String {
        match self {
            SumPart::Scales => format!("{value} <> {NO_SCALES}"),
            _ => format!("{value} <> 0"),
        }
    }

    /// What one row's argument value `x` (`numeric` when `numeric`)
    /// contributes to this partial, times the integer weight `w`, as the
    /// argument of a `sum()`; `NULL` (which `sum()` skips) when it
    /// contributes nothing. Not for [`SumPart::Scales`], which no `sum()`
    /// totals ([`scales_of`]).
    fn weighted(self, x: &str, numeric: bool, w: &str) -> String {
        let w = if w == "1" {
            String::new()
        } else {
            format!("{w} * ")
        };
        match self {
            SumPart::Sum if numeric => {
                format!("{w}(case when scale(({x})) is not null then ({x}) end)")
            }
            SumPart::Sum => format!("{w}({x})::numeric"),
            SumPart::NaN => format!("{w}(({x}) = 'NaN'::numeric)::int"),
            SumPart::PosInf => format!("{w}(({x}) = 'Infinity'::numeric)::int"),
            SumPart::NegInf => format!("{w}(({x}) = '-Infinity'::numeric)::int"),
            SumPart::Scales => unreachable!("a scale map is no sum"),
        }
    }

    /// The total of this partial over the rows of an aggregate, each row's
    /// argument value `x` weighted by `w` (`1` for an absolute write's rows,
    /// a side's sign on a page): its zero over no rows. `scale` is the
    /// argument's declared scale, when it has one ([`Arg::scale`]): a scale
    /// map then counts the rows at that scale without folding a map, and
    /// folds only the rows at any other scale, which no row of a column of
    /// that type has.
    pub(crate) fn of_rows(self, x: &str, numeric: bool, w: &str, scale: Option<i32>) -> String {
        match (self, scale) {
            (SumPart::Scales, Some(s)) => {
                let other = format!("scale(({x})) <> {s}");
                let at = if w == "1" {
                    format!("count(*) filter (where scale(({x})) = {s})")
                } else {
                    format!("coalesce(sum({w}) filter (where scale(({x})) = {s}), 0)")
                };
                let others = scales_of(
                    &format!("array_agg(scale(({x}))) filter (where {other})"),
                    (w != "1")
                        .then(|| format!("array_agg({w}) filter (where {other})"))
                        .as_deref(),
                );
                format!(
                    "(case when bool_or({other}) is not true then {single} \
                           else {merged} end)",
                    single = single_scale(s, &at),
                    merged = scales_add(&single_scale(s, &at), &others, "1"),
                )
            }
            (SumPart::Scales, None) => scales_of(
                &format!("array_agg(scale(({x})))"),
                (w != "1").then(|| format!("array_agg({w})")).as_deref(),
            ),
            (SumPart::Sum, _) => {
                format!("coalesce(sum({}), 0)", self.weighted(x, numeric, w))
            }
            _ => format!("coalesce(sum({}), 0)::bigint", self.weighted(x, numeric, w)),
        }
    }

    /// The total of this partial's values `value` over the rows of an
    /// aggregate (those meeting `condition`, if any): its zero over no rows.
    /// A scale map folds only the maps that aren't empty, so one change's
    /// map, the common case, is taken as it is. Under a declared `scale`
    /// ([`Arg::scale`]) it sums each map's count at that scale, and folds
    /// only the maps that have another.
    pub(crate) fn total(self, value: &str, condition: Option<&str>, scale: Option<i32>) -> String {
        let filter = condition
            .map(|c| format!(" filter (where {c})"))
            .unwrap_or_default();
        match (self, scale) {
            (SumPart::Scales, Some(s)) => {
                let both = |c: String| match condition {
                    Some(cond) => format!("({cond}) and {c}"),
                    None => c,
                };
                let at = format!("coalesce(sum({}){filter}, 0)", scale_count(value, s));
                let others = scales_sum(&format!(
                    "array_agg(({value}) - '{s}'::text) filter (where {})",
                    both(format!("not {}", only_scale(value, s)))
                ));
                format!(
                    "(case when bool_and({only}){filter} is not false then {single} \
                           else {merged} end)",
                    only = only_scale(value, s),
                    single = single_scale(s, &at),
                    merged = scales_add(&single_scale(s, &at), &others, "1"),
                )
            }
            (SumPart::Scales, None) => {
                let only = match condition {
                    Some(c) => format!("{value} <> {NO_SCALES} and ({c})"),
                    None => format!("{value} <> {NO_SCALES}"),
                };
                scales_sum(&format!("array_agg({value}) filter (where {only})"))
            }
            (SumPart::Sum, _) => format!("coalesce(sum({value}){filter}, 0)"),
            _ => format!("coalesce(sum({value}){filter}, 0)::bigint"),
        }
    }

    /// `a + b`, under the argument's declared `scale` ([`Arg::scale`]).
    pub(crate) fn add(self, a: &str, b: &str, scale: Option<i32>) -> String {
        match self {
            SumPart::Scales => scales_add_at(a, b, "1", scale),
            _ => format!("({a} + {b})"),
        }
    }

    /// `a - b`, under the argument's declared `scale` ([`Arg::scale`]).
    pub(crate) fn sub(self, a: &str, b: &str, scale: Option<i32>) -> String {
        match self {
            SumPart::Scales => scales_add_at(a, b, "-1", scale),
            _ => format!("({a} - {b})"),
        }
    }

    /// `a × n`, for the integer `n`, under the argument's declared `scale`
    /// ([`Arg::scale`]).
    pub(crate) fn times(self, a: &str, n: &str, scale: Option<i32>) -> String {
        match (self, scale) {
            (SumPart::Scales, Some(s)) => format!(
                "(case when {only} then {single} else {general} end)",
                only = only_scale(a, s),
                single = single_scale(s, &format!("{} * ({n})::bigint", scale_count(a, s))),
                general = scales_times(a, n),
            ),
            (SumPart::Scales, None) => scales_times(a, n),
            _ => format!("({a} * {n})"),
        }
    }
}

/// The text of the error a strict statement raises on reading a scale map
/// that counts a scale other than the declared one ([`strict_count`]): the
/// caller rolls the statement back and runs it the general way
/// ([`is_off_scale`]).
pub(super) const OFF_SCALE: &str = "trellis: a scale map off its declared scale";

/// The scale map `map`'s count at its declared scale `s`, for a strict
/// statement, which carries a map counting only `s` as that count: an error
/// carrying [`OFF_SCALE`] when `map` counts another scale. The error's
/// expression reads `map`, so Postgres can't fold it while planning.
pub(super) fn strict_count(map: &str, s: i32) -> String {
    format!(
        "(case when {only} then {count} \
               else ('{OFF_SCALE} ' || ({map})::text)::bigint end)",
        only = only_scale(map, s),
        count = scale_count(map, s),
    )
}

/// Whether `error` is a strict statement's refusal of a map at another
/// scale ([`strict_count`]).
pub(super) fn is_off_scale(error: &tokio_postgres::Error) -> bool {
    error
        .as_db_error()
        .is_some_and(|e| e.message().contains(OFF_SCALE))
}

/// Runs `strict`, a strict statement ([`strict_count`]), under a savepoint,
/// and on its refusal of a map at another scale rolls it back and runs
/// `general` instead. Returns whether the strict statement held.
pub(super) async fn strictly_or_generally(
    txn: &Transaction<'_>,
    strict: &str,
    general: &str,
) -> Result<bool, ApplyError> {
    txn.batch_execute("savepoint trellis_strict").await?;
    match txn.batch_execute(strict).await {
        Ok(()) => {
            txn.batch_execute("release savepoint trellis_strict")
                .await?;
            Ok(true)
        }
        Err(e) if is_off_scale(&e) => {
            txn.batch_execute(
                "rollback to savepoint trellis_strict; release savepoint trellis_strict",
            )
            .await?;
            txn.batch_execute(general).await?;
            Ok(false)
        }
        Err(e) => Err(e.into()),
    }
}

/// A strict stage's scale map of the rows' values `x` (an aggregate over
/// them, [`GroupTargetPlan::stage_sql`]): empty, the apply deriving it from
/// the counts, when every value is `NULL` or finite at the declared scale
/// `s`; `NULL`, marking the page for the general stage, otherwise.
fn strict_stage_scales(x: &str, s: i32) -> String {
    format!(
        "(case when bool_and(coalesce(scale(({x})) = {s}, ({x}) is null)) then {NO_SCALES} end)"
    )
}

/// A strict build's scale map of the rows' values `x` (an aggregate over
/// them, [`GroupTargetPlan::fresh_select`]): their count at the declared
/// scale `s`, failing with [`OFF_SCALE`] when a finite value has another.
fn strict_build_scales(x: &str, s: i32) -> String {
    let scale = format!("scale(({x}))");
    format!(
        "(case when bool_and({scale} = {s}) is not false then {single} \
               else to_jsonb(('{OFF_SCALE} ' || count(*)::text)::bigint) end)",
        single = single_scale(s, &format!("count(*) filter (where {scale} = {s})")),
    )
}

/// The scale map `map` plus `change` values at its declared scale `s`
/// (a `bigint`), for an apply that derives a page's change from the counts
/// ([`GroupTargetPlan::derived_map`]): `map` itself when `change` is 0, the
/// single count rebuilt while `map` counts no other scale, and the general
/// sum otherwise.
fn derived_add(map: &str, change: &str, s: i32) -> String {
    format!(
        "(case when {change} = 0 then {map} \
               when {only} then {rebuilt} \
               else {general} end)",
        only = only_scale(map, s),
        rebuilt = single_scale(s, &format!("{} + {change}", scale_count(map, s))),
        general = scales_add(map, &single_scale(s, change), "1"),
    )
}

/// The display scale of the scale map `new` less `change` values at its
/// declared scale `s`, the prior image's map of a derived apply
/// ([`derived_add`]), without building that map while `new` counts no other
/// scale. It is `s` for an empty prior map too, which shows no finite sum.
fn derived_prior_scale(new: &str, change: &str, s: i32) -> String {
    format!(
        "(case when {} then {s} else {} end)",
        only_scale(new, s),
        max_scale(&scales_add(new, &single_scale(s, change), "-1"))
    )
}

/// The scale map `{"<s>": <count>}`, or the empty map when `count` is 0.
/// `count` is read twice: a `CASE` costs less than `jsonb_strip_nulls`,
/// which rebuilds the object, and an aggregate repeated in one select list is
/// computed once.
fn single_scale(s: i32, count: &str) -> String {
    format!(
        "(case when coalesce(({count}), 0) = 0 then {NO_SCALES} \
               else jsonb_build_object('{s}', {count}) end)"
    )
}

/// Whether the scale map `map` counts no scale but `s`.
fn only_scale(map: &str, s: i32) -> String {
    format!("((({map}) - '{s}'::text) = {NO_SCALES})")
}

/// The scale map `map`'s count at scale `s`, 0 for none.
fn scale_count(map: &str, s: i32) -> String {
    format!("coalesce(((({map}) ->> '{s}'))::bigint, 0)")
}

/// The scale map `a + sign × b`, zeros dropped. Under a declared `scale`,
/// two maps that count no other scale add as two counts.
fn scales_add_at(a: &str, b: &str, sign: &str, scale: Option<i32>) -> String {
    match scale {
        Some(s) => format!(
            "(case when coalesce({b}, {NO_SCALES}) = {NO_SCALES} then coalesce({a}, {NO_SCALES}) \
                   when {only_a} and {only_b} then {single} else {general} end)",
            only_a = only_scale(a, s),
            only_b = only_scale(b, s),
            single = single_scale(
                s,
                &format!("{} + {sign} * {}", scale_count(a, s), scale_count(b, s))
            ),
            general = scales_add(a, b, sign),
        ),
        None => scales_add(a, b, sign),
    }
}

/// The partials of an argument: none unsummed, [`NUMERIC_PARTS`] for a
/// `numeric` sum, [`INTEGER_PARTS`] otherwise.
pub(crate) fn sum_parts(summed: bool, numeric: bool) -> &'static [SumPart] {
    match (summed, numeric) {
        (false, _) => &[],
        (true, true) => NUMERIC_PARTS,
        (true, false) => INTEGER_PARTS,
    }
}

impl Arg {
    /// Its partials beside its count ([`sum_parts`]).
    pub(crate) fn parts(&self) -> &'static [SumPart] {
        sum_parts(self.summed, self.numeric)
    }
}

/// The scale map of the scales `keys` (an `int[]`, `NULL` elements
/// skipped), each weighted by its element of `weights` (a same-length
/// integer array), or by 1 with none: each scale's weights summed, zeros
/// dropped. Both are aggregates of the enclosing query (`array_agg(…)`),
/// read once in the select list: an aggregate can't be a function's argument
/// in `FROM`, and one over no column of that query would be the subquery's
/// own.
pub(crate) fn scales_of(keys: &str, weights: Option<&str>) -> String {
    let (weights, weight) = match weights {
        Some(weights) => (weights.to_string(), "__u.__sw"),
        None => ("null::bigint[]".to_string(), "1"),
    };
    format!(
        "(select coalesce(jsonb_object_agg(__z.__sk, __z.__sc), {NO_SCALES}) from ( \
             select __u.__sk::text as __sk, sum({weight})::bigint as __sc \
             from (select {keys} as __ka, {weights} as __wa) as __v \
             cross join lateral unnest(__v.__ka, __v.__wa) as __u(__sk, __sw) \
             where __u.__sk is not null group by __u.__sk having sum({weight}) <> 0) as __z)"
    )
}

/// The sum of the scale maps `maps` (a `jsonb[]`, `NULL` elements
/// skipped), zeros dropped. A single map is itself.
pub(crate) fn scales_sum(maps: &str) -> String {
    format!(
        "(select case when __m is null then {NO_SCALES} \
                      when cardinality(__m) = 1 then coalesce(__m[1], {NO_SCALES}) else ( \
             select coalesce(jsonb_object_agg(__sk, __sc), {NO_SCALES}) from ( \
                 select __e.key as __sk, sum(__e.value::bigint)::bigint as __sc \
                 from unnest(__m) as __u(__j) cross join lateral jsonb_each_text(__u.__j) as __e \
                 group by __e.key having sum(__e.value::bigint) <> 0) as __z) end \
          from (select {maps} as __m) as __v)"
    )
}

/// The scale map `a + sign × b`, zeros dropped.
fn scales_add(a: &str, b: &str, sign: &str) -> String {
    format!(
        "(select case when __b = {NO_SCALES} then __a \
                      when __a = {NO_SCALES} and {sign} = 1 then __b else ( \
             select coalesce(jsonb_object_agg(__sk, __sc), {NO_SCALES}) from ( \
                 select __sk, sum(__sc)::bigint as __sc from ( \
                     select key as __sk, value::bigint as __sc from jsonb_each_text(__a) \
                     union all \
                     select key, {sign} * value::bigint from jsonb_each_text(__b)) as __e \
                 group by __sk having sum(__sc) <> 0) as __z) end \
          from (select coalesce({a}, {NO_SCALES}) as __a, coalesce({b}, {NO_SCALES}) as __b) as __v)"
    )
}

/// The scale map `a × n`, for the integer `n`: empty when `n` is 0.
fn scales_times(a: &str, n: &str) -> String {
    format!(
        "(select case when __n = 0 or __a = {NO_SCALES} then {NO_SCALES} \
                      when __n = 1 then __a else ( \
             select jsonb_object_agg(key, value::bigint * __n) from jsonb_each_text(__a)) end \
          from (select coalesce({a}, {NO_SCALES}) as __a, ({n})::bigint as __n) as __v)"
    )
}

/// The largest scale with a count above 0 in the scale map `map`, `0` for
/// none.
fn max_scale(map: &str) -> String {
    format!(
        "coalesce((select max(key::int) from jsonb_each_text({map}) \
                   where value::bigint > 0), 0)"
    )
}

/// The value of a summed argument's `SUM` from its partials, `part` naming
/// each ([`SumPart`]); `NULL` handling (a count of 0) is the caller's. An
/// integer argument's is its sum. A `numeric` one's is Postgres's
/// `sum(numeric)`: `NaN` while it counts a `NaN` or both infinities, the
/// infinity while it counts one, else the finite sum at the largest scale
/// it counts a value of.
///
/// Under a declared `scale` ([`Arg::scale`]) the largest scale is that one
/// whenever the map counts no other, without reading the map's keys.
/// `display`, when given, is the display scale itself, which a caller that
/// knows it without the map passes ([`GroupTargetPlan::visible_tail_sql`]).
pub(crate) fn sum_value(
    numeric: bool,
    scale: Option<i32>,
    display: Option<&str>,
    part: &dyn Fn(SumPart) -> String,
) -> String {
    if !numeric {
        return part(SumPart::Sum);
    }
    let (nan, pinf, ninf) = (
        part(SumPart::NaN),
        part(SumPart::PosInf),
        part(SumPart::NegInf),
    );
    format!(
        "(case when {nan} > 0 or ({pinf} > 0 and {ninf} > 0) then 'NaN'::numeric \
               when {pinf} > 0 then 'Infinity'::numeric \
               when {ninf} > 0 then '-Infinity'::numeric \
               else round({sum}, {scale}) end)",
        sum = part(SumPart::Sum),
        scale = match (display, scale) {
            (Some(display), _) => display.to_string(),
            (None, Some(s)) => {
                let map = part(SumPart::Scales);
                format!(
                    "(case when {} and {} > 0 then {s} else {} end)",
                    only_scale(&map, s),
                    scale_count(&map, s),
                    max_scale(&map)
                )
            }
            (None, None) => max_scale(&part(SumPart::Scales)),
        },
    )
}

/// The columns of the hidden table `shape` gives a definition, in order:
/// the `GROUP BY` columns, the row count, a non-null count per aggregate
/// argument and a sum for each a `SUM` or `AVG` reads, an extreme per
/// distinct `MIN`/`MAX`, then the basis and stamp, the extremes' basis and
/// stamp when there are extremes, and the zero-group flag. A definition with
/// no `MIN`/`MAX` has no extreme column and no extremes' basis.
/// [`hidden_table_ddl`] creates exactly these (and the
/// group identity), and [`hidden_table_width`] counts them, so a limit on the
/// table's width is the width of the table.
fn hidden_table_columns(shape: &GroupShape) -> Vec<String> {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    columns.push("__n bigint not null default 0".to_string());
    let mut zero = vec!["__n = 0".to_string()];
    for (i, arg) in shape.args.iter().enumerate() {
        columns.push(format!("__count_{} bigint not null default 0", i + 1));
        zero.push(format!("__count_{} = 0", i + 1));
        for part in arg.parts() {
            columns.push(part.ddl(i + 1));
            zero.push(part.is_zero(&part.hidden(i + 1)));
        }
    }
    let extremes = shape.extremes();
    for extreme in &extremes {
        columns.push(format!("{} {}", extreme.column(), extreme.pg_type));
    }
    columns.push("__basis pg_snapshot".to_string());
    columns.push("__stamp bigint".to_string());
    if !extremes.is_empty() {
        columns.push("__xbasis pg_snapshot".to_string());
        columns.push("__xstamp bigint".to_string());
    }
    columns.push(format!(
        "__zero boolean not null generated always as ({}) stored",
        zero.join(" and ")
    ));
    columns
}

/// The zero flag's column definition of the hidden table of `shape`, over
/// every partial (see "Zero groups" in the module doc).
pub(crate) fn hidden_zero_column(shape: &GroupShape) -> String {
    hidden_table_columns(shape)
        .pop()
        .expect("the zero flag is the last column")
}

/// How many columns the hidden table of `shape` has.
pub(crate) fn hidden_table_width(shape: &GroupShape) -> usize {
    hidden_table_columns(shape).len()
}

/// How many columns the visible target of `shape` has.
pub(crate) fn target_table_width(shape: &GroupShape) -> usize {
    target_table_columns(shape).len()
}

/// The statements creating definition `id`'s hidden table in the catalog
/// schema `schema` (see the module doc) and its zero-group index (see "Zero
/// groups" there). Created empty with the definition's row, and dropped with
/// it ([`drop_hidden_table_sql`]).
pub(crate) fn hidden_table_ddl(schema: &str, id: i64, shape: &GroupShape) -> String {
    let mut columns = hidden_table_columns(shape);
    let group_cols = quoted(&shape.group_cols).join(", ");
    columns.push(format!("unique nulls not distinct ({group_cols})"));
    let table = hidden_table_ident(schema, id);
    format!(
        "create table {table} ({}); create index on {table} ({group_cols}) where __zero",
        columns.join(", ")
    )
}

/// One zero-group GC batch's statement on the hidden table `hidden`
/// (schema-qualified and quoted), whose zero-group index keys on
/// `group_cols` (quoted, joined): deletes up to `$1` zero groups, claimed
/// `for update skip locked` in key order. With `basis` (the table has a
/// basis column), only those that have no basis or whose stamp is at or
/// below `$2`, the contiguous drained prefix; without (a `JOIN`'s `T`),
/// every zero group. With `xbasis` too (the table keeps extremes), only
/// those whose extremes' basis is missing or whose `__xstamp` is at or below
/// `$2` as well (Rx7). See "Zero groups" in the module doc;
/// `super::retire::collect_tombstones` runs it.
pub(crate) fn collect_zero_groups_sql(
    hidden: &str,
    group_cols: &str,
    basis: bool,
    xbasis: bool,
) -> String {
    // Planted bug: the GC ignores the extremes' stamp. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let xbasis =
        xbasis && !crate::plant::fires(crate::plant::Plant::GbxGcIgnoresXstamp, basis && xbasis);
    let rule = match (basis, xbasis) {
        (true, true) => {
            " and (__basis is null or __stamp <= $2) and (__xbasis is null or __xstamp <= $2)"
        }
        (true, false) => " and (__basis is null or __stamp <= $2)",
        (false, _) => "",
    };
    format!(
        "delete from {hidden} where ctid = any(array( \
             select ctid from {hidden} \
             where __zero{rule} \
             order by {group_cols} limit $1 for update skip locked))"
    )
}

/// The statement dropping definition `id`'s hidden table from the catalog
/// schema `schema`, if it has one.
pub(crate) fn drop_hidden_table_sql(schema: &str, id: i64) -> String {
    format!("drop table if exists {}", hidden_table_ident(schema, id))
}

/// The columns of the visible target of `shape`: the `GROUP BY` columns and
/// each value field.
fn target_table_columns(shape: &GroupShape) -> Vec<String> {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    for (name, pg_type, _) in &shape.fields {
        columns.push(format!("{} {pg_type}", quote_ident(name)));
    }
    columns
}

/// The visible target's `create table` statement for `shape`: the `GROUP BY`
/// columns, each value field, and the group identity, nothing else (see the
/// module doc). `target` is the target, quoted and qualified.
pub(crate) fn target_table_ddl(target: &str, shape: &GroupShape) -> String {
    let mut columns = target_table_columns(shape);
    columns.push(format!(
        "unique nulls not distinct ({})",
        quoted(&shape.group_cols).join(", ")
    ));
    format!("create table {target} ({})", columns.join(", "))
}

fn quoted(columns: &[String]) -> Vec<String> {
    columns.iter().map(|c| quote_ident(c)).collect()
}

/// What a [`GroupTargetPlan`]'s statements write.
#[derive(Debug, Clone)]
pub(super) enum Writer {
    /// A plain aggregate's target: the hidden table `agg_<id>` and the
    /// visible target (see the module doc).
    Aggregate,
    /// One side's partials of a `JOIN` (`P` or `M`, `super::join`): the
    /// hidden table only, with no visible row. The apply adds the change it
    /// applied to each cell to `deltas`, a session temporary table, for the
    /// `JOIN`'s product step.
    JoinPartials {
        /// Quoted, `pg_temp.`-qualified.
        deltas: String,
    },
    /// A `JOIN`'s target groups (`T`, `super::join`): its hidden partials
    /// and the visible target. Its staged rows are the page's product
    /// deltas, already exact, so no basis or truncate floor filters them.
    JoinTarget,
}

/// Where the values of a group's columns live. A `JOIN`'s group and key
/// columns exist in three populations, each with the collation its column had
/// when the table was created: the source table's (live: a column can be
/// re-collated at any time), the hidden table's and its session tables', and
/// the visible target's. A change between deterministic collations pauses
/// nothing, so after one the populations disagree, and a statement that
/// compares, deduplicates or unions two of them would find no collation
/// Postgres can choose. See [`GroupCollations`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Population {
    Source,
    Hidden,
    Target,
}

/// A table's collatable columns' collations, by column: each a quoted,
/// qualified name ready to interpolate after `collate`
/// (`"pg_catalog"."POSIX"`). A column with no collation has no entry.
pub(crate) type ColumnCollations = HashMap<String, String>;

/// The collation of every collatable column of `table` (a name `to_regclass`
/// resolves), at the time of the call.
pub(crate) async fn column_collations(
    client: &impl tokio_postgres::GenericClient,
    table: &str,
) -> Result<ColumnCollations, tokio_postgres::Error> {
    Ok(client
        .query(
            "select a.attname::text, \
                    pg_catalog.format('%I.%I', n.nspname, co.collname) \
             from pg_catalog.pg_attribute a \
             join pg_catalog.pg_collation co on co.oid = a.attcollation \
             join pg_catalog.pg_namespace n on n.oid = co.collnamespace \
             where a.attrelid = pg_catalog.to_regclass($1) \
               and a.attnum > 0 and not a.attisdropped",
            &[&ddl::regclass_arg(table)],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The scale each of `args` declares as a column of `table` (a name
/// `to_regclass` resolves), at the time of the call ([`Arg::scale`]): for a
/// `numeric` sum over a plain column typed `numeric(p,s)`, `s` (0 for a
/// negative `s`, whose values have scale 0), and `None` for every other
/// argument. Each statement of a plan stays exact whatever scale its rows
/// hold, so a type changed since makes a slower statement, never a wrong one.
pub(super) async fn declared_scales(
    client: &impl tokio_postgres::GenericClient,
    table: &str,
    args: &[Arg],
) -> Result<Vec<Option<i32>>, tokio_postgres::Error> {
    fn column(arg: &Arg) -> Option<&str> {
        extreme_column(&arg.expr).filter(|_| arg.summed && arg.numeric)
    }
    let columns: Vec<&str> = args.iter().filter_map(column).collect();
    if columns.is_empty() {
        return Ok(vec![None; args.len()]);
    }
    let typmods: HashMap<String, i32> = client
        .query(
            "select a.attname::text, a.atttypmod from pg_catalog.pg_attribute a \
             where a.attrelid = pg_catalog.to_regclass($1) and a.attname = any($2) \
               and a.attnum > 0 and not a.attisdropped \
               and a.atttypid = 'pg_catalog.numeric'::pg_catalog.regtype \
               and a.atttypmod >= 4",
            &[&ddl::regclass_arg(table), &columns],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    Ok(args
        .iter()
        .map(|arg| {
            let typmod = typmods.get(column(arg)?)?;
            Some(numeric_scale(*typmod).max(0))
        })
        .collect())
}

/// The scale a `numeric` type modifier declares (Postgres's
/// `NUMERIC_TYPMOD_SCALE`: 11 bits, signed).
fn numeric_scale(typmod: i32) -> i32 {
    (((typmod - 4) & 0x7ff) ^ 1024) - 1024
}

impl GroupShape {
    /// Sets each argument's declared scale as a column of `table`
    /// ([`declared_scales`]).
    pub(crate) async fn declare_scales(
        &mut self,
        client: &impl tokio_postgres::GenericClient,
        table: &str,
    ) -> Result<(), tokio_postgres::Error> {
        let scales = declared_scales(client, table, &self.args).await?;
        self.set_scales(&scales);
        Ok(())
    }

    /// Sets each argument's declared scale to its element of `scales`.
    pub(super) fn set_scales(&mut self, scales: &[Option<i32>]) {
        for (arg, scale) in self.args.iter_mut().zip(scales) {
            arg.scale = *scale;
        }
    }
}

/// The collations of a `JOIN` structure's group columns, in each population
/// ([`Population`]), aligned with its [`GroupShape::group_cols`]. A column
/// with no collation (an integer), and a plan with none recorded (every
/// plain aggregate's: its hidden table, target and session tables are the
/// database's default collation, which any other collation overrides), has
/// `None`.
///
/// Equality under any deterministic collation is byte equality, so which
/// collation a comparison runs under changes no result; it decides only
/// whether Postgres can choose one, and which index a probe can use. The
/// hidden table's collation is the working one: every session table is
/// created from it, so values read from the source or the target are moved
/// to it ([`Self::coerce`]) where they enter, and a probe of the source's or
/// the target's index moves the working value to the indexed table's. Moved
/// only where the collations differ, so a definition no column of which was
/// re-collated runs the statements it always has.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupCollations {
    source: Vec<Option<String>>,
    hidden: Vec<Option<String>>,
    target: Vec<Option<String>>,
}

impl GroupCollations {
    /// The collations of `columns`, from each population's table (`source`
    /// and `target` are absent for a structure with none).
    pub(crate) fn of(
        columns: &[String],
        source: Option<&ColumnCollations>,
        hidden: &ColumnCollations,
        target: Option<&ColumnCollations>,
    ) -> Self {
        let pick = |from: Option<&ColumnCollations>| -> Vec<Option<String>> {
            columns
                .iter()
                .map(|c| from.and_then(|m| m.get(c)).cloned())
                .collect()
        };
        Self {
            source: pick(source),
            hidden: pick(Some(hidden)),
            target: pick(target),
        }
    }

    fn of_population(&self, population: Population, column: usize) -> Option<&str> {
        let collations = match population {
            Population::Source => &self.source,
            Population::Hidden => &self.hidden,
            Population::Target => &self.target,
        };
        collations.get(column)?.as_deref()
    }

    /// `expr`, the value of group column `column` in population `from`, as
    /// a value of population `to`'s collation: `expr collate <to's>` when
    /// the two differ, `expr` as is when they are the same or either is
    /// unknown.
    fn coerce(&self, expr: &str, column: usize, from: Population, to: Population) -> String {
        match (
            self.of_population(from, column),
            self.of_population(to, column),
        ) {
            (Some(from), Some(to)) if from != to => format!("{expr} collate {to}"),
            _ => expr.to_string(),
        }
    }
}

/// One page's work on one group-basis target ([`apply_group_target`]), or
/// on one of a `JOIN`'s structures ([`Writer`]).
#[derive(Debug, Clone)]
pub(crate) struct GroupTargetPlan {
    /// The target, qualified and unquoted (`transform_definitions.target_table`).
    pub(crate) target: String,
    target_ident: String,
    hidden_ident: String,
    /// The session's temporary table the page's sides are summed into
    /// before any group is locked ([`stage_sides`]), quoted.
    staged: String,
    /// The session's temporary table of the groups a page or a release
    /// recomputes ([`recompute_locked_groups`]), quoted.
    recomputed: String,
    /// The session's temporary table of the groups whose extremes a page
    /// probes ([`probe_locked_groups`]), quoted; empty for a `JOIN`'s
    /// structures, which keep no extremes.
    probed: String,
    /// The source, quoted and qualified: the row type images populate.
    source_row: String,
    /// The source as the ring names it (`src_table`), unquoted.
    source: String,
    /// The ring's segment tables, quoted and qualified, whose old images a
    /// rebuild's enumeration reads ([`rebuild`]).
    ring: Vec<String>,
    /// The source's row identity, which an image-less change's key names.
    source_pk: Vec<PrimaryKeyColumn>,
    /// The target's identity, its `GROUP BY` columns.
    identity: Vec<PrimaryKeyColumn>,
    pub(super) shape: GroupShape,
    /// Every ring row of every change the page has for this target.
    pub(super) sides: Vec<String>,
    /// The keys of the page's image-less changes, whose current rows'
    /// groups the page recomputes.
    recompute_keys: Vec<String>,
    pub(super) hop_gen: i32,
    pub(super) src_changed: Option<SystemTime>,
    pub(super) origin_lsn: Option<PgLsn>,
    pushed: bool,
    writer: Writer,
    /// The `ledger_truncate_floor` row the staged sides' `lsn` must be
    /// above, or `None` for none ([`Writer::JoinTarget`]).
    floor_key: Option<String>,
    /// More conditions, over the populated image `s`, a side must meet to
    /// be staged: ` and …` (with its leading space), or empty.
    stage_filter: String,
    /// The collations of the group columns in each population; empty for a
    /// plain aggregate ([`GroupCollations`]).
    collations: GroupCollations,
    /// What the page's staged sides hold of each numeric sum's partials, as
    /// [`stage_sides`] found it ([`StagedParts`]).
    staged_parts: StagedSummary,
}

/// What a page's staged sides hold of each `numeric` sum's partials, read
/// as it stages them ([`stage_sides`]), so its apply sums only what it
/// staged ([`GroupTargetPlan::staged_total`]).
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct StagedParts {
    /// The (1-based argument, partial) pairs every staged side holds 0 of:
    /// the special-value counts of a page with no `NaN` or infinity.
    zero: std::collections::HashSet<(usize, SumPart)>,
    /// The arguments with a declared scale ([`Arg::scale`]) whose staged
    /// scale maps count no other scale: their map's total is their count of
    /// finite values at that scale.
    uniform: std::collections::HashSet<usize>,
}

impl StagedParts {
    /// What a strict `JOIN` product stages into `T` (`super::join`): every
    /// `numeric` sum with a declared scale counts only it.
    pub(super) fn declared(shape: &GroupShape) -> Self {
        Self {
            zero: Default::default(),
            uniform: (1..=shape.args.len())
                .filter(|&i| {
                    let arg = &shape.args[i - 1];
                    arg.scale.is_some() && arg.parts() == NUMERIC_PARTS
                })
                .collect(),
        }
    }
}

/// A plan's [`StagedParts`], `None` until a [`stage_sides`] reads them:
/// a staging table filled any other way (a `JOIN`'s product into `T`) is
/// summed in full.
#[derive(Debug, Default)]
struct StagedSummary(std::sync::Mutex<Option<StagedParts>>);

impl Clone for StagedSummary {
    fn clone(&self) -> Self {
        Self(std::sync::Mutex::new(self.get()))
    }
}

impl StagedSummary {
    fn get(&self) -> Option<StagedParts> {
        self.0.lock().expect("the staged parts").clone()
    }

    fn set(&self, parts: Option<StagedParts>) {
        *self.0.lock().expect("the staged parts") = parts;
    }
}

/// The columns of a plan's staging table, as DDL.
fn staged_columns(shape: &GroupShape) -> Vec<String> {
    let mut columns: Vec<String> = shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect();
    columns.push("__txid xid8".to_string());
    columns.push("__lsn pg_lsn".to_string());
    columns.push("__n bigint".to_string());
    for (i, arg) in shape.args.iter().enumerate() {
        columns.push(format!("__c{} bigint", i + 1));
        for part in arg.parts() {
            columns.push(format!("{} {}", part.name("__", i + 1), part.sql_type()));
        }
    }
    // Per extreme, the extreme of the insertion sides and of the
    // retraction sides, kept apart (Rx4: a sum can't say which side reached
    // the extreme).
    for extreme in shape.extremes() {
        columns.push(format!("{} {}", extreme.plus(), extreme.pg_type));
        columns.push(format!("{} {}", extreme.minus(), extreme.pg_type));
    }
    columns
}

/// The columns of the recompute's session table: the group columns, typed
/// as the hidden table types them.
fn recompute_columns(shape: &GroupShape) -> Vec<String> {
    shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(c, t)| format!("{} {t}", quote_ident(c)))
        .collect()
}

/// A short stable hash of a session temp table's columns. The tables are
/// session temp tables, and the pool keeps its connections, so a connection
/// keeps the table it first created under a name. Putting the shape in the
/// name means a definition whose group column was re-typed (a resume re-reads
/// the types under the same id) gets a new table; the old one is never
/// touched again and goes with its session. 64 bits keep two shapes of one
/// definition from sharing a name, and the name within Postgres's 63 bytes
/// for any `i64` id. A `JOIN`'s working tables use it too (`super::join`).
/// FNV-1a: fixed by definition, unlike `DefaultHasher`.
pub(super) fn shape_hash(columns: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in columns.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl GroupTargetPlan {
    pub(crate) fn new(
        catalog_schema: &str,
        definition_id: i64,
        target: &str,
        source: &str,
        source_pk: Vec<PrimaryKeyColumn>,
        identity: Vec<PrimaryKeyColumn>,
        shape: GroupShape,
    ) -> Self {
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            hidden_ident: hidden_table_ident(catalog_schema, definition_id),
            staged: format!(
                "pg_temp.{}",
                quote_ident(&format!(
                    "trellis_group_sides_{definition_id}_{:016x}",
                    shape_hash(&staged_columns(&shape).join(","))
                ))
            ),
            recomputed: format!(
                "pg_temp.{}",
                quote_ident(&format!(
                    "trellis_group_recompute_{definition_id}_{:016x}",
                    shape_hash(&recompute_columns(&shape).join(","))
                ))
            ),
            probed: format!(
                "pg_temp.{}",
                quote_ident(&format!(
                    "trellis_group_probe_{definition_id}_{:016x}",
                    shape_hash(&recompute_columns(&shape).join(","))
                ))
            ),
            source_row: ddl::qualified_source_table(source),
            source: source.to_string(),
            ring: (0..super::append::RING_SIZE)
                .map(|slot| format!("{}.seg_{slot}", quote_ident(catalog_schema)))
                .collect(),
            source_pk,
            identity,
            shape,
            sides: Vec::new(),
            recompute_keys: Vec::new(),
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
            pushed: false,
            writer: Writer::Aggregate,
            floor_key: Some(target.to_string()),
            stage_filter: String::new(),
            collations: GroupCollations::default(),
            staged_parts: StagedSummary::default(),
        }
    }

    /// The plan of one `JOIN` structure (`super::join`): `hidden` is its
    /// hidden table (quoted, qualified), `staged` its session staging table
    /// (a bare name, made `pg_temp.`), and `target` the `JOIN`'s target,
    /// which errors and the seam name. `source` is the side's table, for a
    /// [`Writer::JoinPartials`]; a [`Writer::JoinTarget`] stages nothing
    /// from images. `floor_key` names the side's truncate floor.
    /// `recomputed` is a [`Writer::JoinPartials`]' table of the cells it
    /// recomputes (a bare name, made `pg_temp.`), and `source_pk` its
    /// table's row identity, which an image-less change's key names; a
    /// [`Writer::JoinTarget`] recomputes nothing and has neither.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn for_join(
        target: &str,
        hidden: String,
        staged: &str,
        source: Option<(&str, &str, Vec<PrimaryKeyColumn>)>,
        identity: Vec<PrimaryKeyColumn>,
        shape: GroupShape,
        writer: Writer,
        floor_key: Option<String>,
        stage_filter: String,
    ) -> Self {
        let (source_name, source_row, recomputed, source_pk) = match source {
            Some((source, recomputed, pk)) => (
                source,
                ddl::qualified_source_table(source),
                format!("pg_temp.{}", quote_ident(recomputed)),
                pk,
            ),
            None => ("", String::new(), String::new(), Vec::new()),
        };
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            hidden_ident: hidden,
            staged: format!("pg_temp.{}", quote_ident(staged)),
            recomputed,
            probed: String::new(),
            source_row,
            source: source_name.to_string(),
            ring: Vec::new(),
            source_pk,
            identity,
            shape,
            sides: Vec::new(),
            recompute_keys: Vec::new(),
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
            pushed: false,
            writer,
            floor_key,
            stage_filter,
            collations: GroupCollations::default(),
            staged_parts: StagedSummary::default(),
        }
    }

    /// This plan with the collations of its group columns
    /// ([`GroupCollations`]).
    /// Sets what its staging table holds ([`StagedParts`]), for a writer
    /// other than [`stage_sides`]: `None` for maps of any scales.
    pub(super) fn set_staged_parts(&self, parts: Option<StagedParts>) {
        self.staged_parts.set(parts);
    }

    pub(super) fn with_collations(mut self, collations: GroupCollations) -> Self {
        self.collations = collations;
        self
    }

    /// This plan with the ring's segment tables, whose old images a build's
    /// enumeration reads ([`enumerate_cells`]).
    pub(super) fn with_ring(mut self, catalog_schema: &str) -> Self {
        self.ring = (0..super::append::RING_SIZE)
            .map(|slot| format!("{}.seg_{slot}", quote_ident(catalog_schema)))
            .collect();
        self
    }

    /// Adds one folded change's ring rows. An image-less change (a
    /// `Recompute`) has no sides: the page recomputes the groups it names
    /// instead (see the module doc); a `JOIN` side recomputes the cells it
    /// names (`super::join`).
    pub(crate) fn push(&mut self, change: &FoldedChange) -> Result<(), ApplyError> {
        if change.has_recompute {
            if matches!(self.writer, Writer::JoinTarget) {
                return Err(self.unsupported("an image-less change pushed to a JOIN's T"));
            }
            self.recompute_keys.push(change.key.clone());
        }
        if change.group_sides.is_empty() {
            return Err(
                self.unsupported("a page folded without the unfolded ring rows its change needs")
            );
        }
        // Planted bug: fold the key's ring rows into one side. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        if let Some(folded) = planted_fold(&change.group_sides)
            && crate::plant::fires(crate::plant::Plant::GbFoldSidesPerKey, true)
        {
            self.sides.push(folded);
            self.note(change);
            return Ok(());
        }
        self.sides.extend(change.group_sides.iter().cloned());
        self.note(change);
        Ok(())
    }

    /// Merges `change`'s provenance into what the seam records for this
    /// plan's rows.
    pub(super) fn note(&mut self, change: &FoldedChange) {
        if self.pushed {
            self.hop_gen = self.hop_gen.max(change.hop_gen);
            self.src_changed =
                super::apply::earliest_src_changed(self.src_changed, change.src_changed);
            self.origin_lsn = earliest_origin(self.origin_lsn, change.origin_lsn);
        } else {
            self.hop_gen = change.hop_gen;
            self.src_changed = change.src_changed;
            self.origin_lsn = change.origin_lsn;
            self.pushed = true;
        }
    }

    pub(super) fn unsupported(&self, what: &'static str) -> ApplyError {
        ApplyError::GroupBasisUnsupported {
            target: self.target.clone(),
            what,
        }
    }

    /// The CTEs every statement starts with: `raw`, one parsed ring row per
    /// element of `$1`, and `sides`, its old and new sides with their
    /// transaction, `lsn` and sign. A `Recompute` has no sides: its `o` is
    /// a prior-image hint, not a change's old image.
    fn sides_sql() -> &'static str {
        "raw as (select x::jsonb as r from unnest($1::text[]) as u(x)), \
         sides as ( \
             select (r ->> 't')::xid8 as __txid, (r ->> 'l')::pg_lsn as __lsn, -1 as __sgn, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'old' \
                         else r -> 'o' end as __i \
             from raw where r ->> 'op' <> 'recompute' \
             union all \
             select (r ->> 't')::xid8, (r ->> 'l')::pg_lsn, 1, \
                    case when jsonb_typeof(r -> 'x') = 'object' then r -> 'x' -> 'new' \
                         else r -> 'n' end \
             from raw where r ->> 'op' <> 'recompute' \
         )"
    }

    /// `s.<group col>::<type> as <group col>`, for each `GROUP BY` column.
    /// A `JOIN` side's key columns are the image's own columns, at the
    /// hidden table's types, so they need no cast; at the hidden table's
    /// collation too ([`GroupCollations`]), so a source column re-collated
    /// after define still unions, deduplicates and matches with the cells
    /// the hidden table holds.
    fn group_values(&self) -> Vec<String> {
        self.shape
            .group_cols
            .iter()
            .zip(&self.shape.group_types)
            .enumerate()
            .map(|(i, (c, t))| {
                let c = quote_ident(c);
                match self.writer {
                    Writer::Aggregate => format!("s.{c}::{t} as {c}"),
                    Writer::JoinPartials { .. } | Writer::JoinTarget => format!(
                        "{} as {c}",
                        self.collations.coerce(
                            &format!("s.{c}"),
                            i,
                            Population::Source,
                            Population::Hidden
                        )
                    ),
                }
            })
            .collect()
    }

    /// The working columns a staged row or an applied delta carries after
    /// the group's: `<prefix>n`, then per argument `<prefix>c<i>` and each
    /// of its partials' ([`SumPart::name`]), each `null::<type> as <name>`.
    fn working_columns(&self, prefix: &str) -> Vec<String> {
        let mut columns = vec![format!("null::bigint as {prefix}n")];
        for (i, arg) in self.shape.args.iter().enumerate() {
            columns.push(format!("null::bigint as {prefix}c{}", i + 1));
            for part in arg.parts() {
                columns.push(format!(
                    "null::{} as {}",
                    part.sql_type(),
                    part.name(prefix, i + 1)
                ));
            }
        }
        columns
    }

    /// The visible target's group columns, `<col> as <col>` each, at the
    /// hidden table's collation ([`GroupCollations`]), for a select that
    /// unions them with the hidden table's own.
    fn target_group_values(&self) -> String {
        quoted(&self.shape.group_cols)
            .iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "{} as {c}",
                    self.collations
                        .coerce(c, i, Population::Target, Population::Hidden)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The `create` of a `JOIN` working table `table` (quoted,
    /// `pg_temp.`-qualified): the hidden table's group columns, at their
    /// exact types and collations, then `extra`. Once a session; its rows go
    /// with each page's transaction.
    fn join_working_ddl(&self, table: &str, extra: &[String]) -> String {
        let mut columns = quoted(&self.shape.group_cols);
        columns.extend(extra.iter().cloned());
        format!(
            "create temp table if not exists {} on commit delete rows as \
             select {} from {} with no data",
            table.trim_start_matches("pg_temp."),
            columns.join(", "),
            self.hidden_ident
        )
    }

    /// The `create` of [`Writer::JoinPartials`]' delta table.
    pub(super) fn deltas_ddl(&self) -> Option<String> {
        match &self.writer {
            Writer::JoinPartials { deltas } => {
                Some(self.join_working_ddl(deltas, &self.working_columns("__d")))
            }
            _ => None,
        }
    }

    /// The plan's source table, quoted and qualified: what a `FROM` or a
    /// whole-table `GROUP BY` of it names.
    pub(super) fn source_row(&self) -> &str {
        &self.source_row
    }

    /// The hidden table, quoted and qualified.
    pub(super) fn hidden(&self) -> &str {
        &self.hidden_ident
    }

    /// The staging table, quoted and `pg_temp.`-qualified.
    pub(super) fn staged(&self) -> &str {
        &self.staged
    }

    /// The table of the groups (cells) this plan recomputes, quoted and
    /// `pg_temp.`-qualified ([`recompute_locked_groups`]).
    pub(super) fn recomputed(&self) -> &str {
        &self.recomputed
    }

    /// The keys of the page's image-less changes, whose current rows'
    /// groups (cells) it recomputes.
    pub(super) fn recompute_keys(&self) -> &[String] {
        &self.recompute_keys
    }

    /// The join condition matching the group of `left`'s row to `right`'s,
    /// `NULL` matching `NULL`: per column, `array[left.c] = array[right.c]`.
    /// Array equality compares elements by the column type's default
    /// equality under the column's collation and counts two `NULL` elements
    /// equal, so it matches exactly the groups `GROUP BY` forms, and,
    /// unlike `is not distinct from`, it can be hashed or merged: a join of
    /// two group sets on it is a hash or merge join, not a nested loop
    /// rescanning one set per row of the other. Both sides must hold
    /// each column at the same type (the hidden table's, as
    /// [`Self::group_values`] reads a source): unlike `is not distinct
    /// from`, array equality coerces no element type.
    pub(super) fn group_match(&self, left: &str, right: &str) -> String {
        quoted(&self.shape.group_cols)
            .iter()
            .map(|c| format!("array[{left}.{c}] = array[{right}.{c}]"))
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// The lookup of the row of `table` (quoted) whose group is `alias`'s,
    /// `NULL` matching `NULL`, returning `columns` (over `x`), as one index
    /// probe on the group identity's index: plain equality, and, for each
    /// pattern of `NULL` columns, the same equality with `x.<col> is null`
    /// for those columns, behind a one-time test that `alias`'s group has
    /// exactly those `NULL`s. `is not distinct from` would match too, but no
    /// index serves it: each page naming a group with a `NULL` would scan
    /// the whole table. Past [`MAX_NULL_PATTERN_COLUMNS`] group columns the
    /// patterns get too many, and a group with a `NULL` takes that scan.
    fn group_lookup(
        &self,
        table: &str,
        population: Population,
        alias: &str,
        columns: &str,
    ) -> String {
        let cols = quoted(&self.shape.group_cols);
        // `alias`' group is the working collation's; the probe runs under
        // `table`'s, the collation its index is on.
        let probe = |i: usize, c: &str| {
            self.collations
                .coerce(&format!("{alias}.{c}"), i, Population::Hidden, population)
        };
        let equal: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("x.{c} = {}", probe(i, c)))
            .collect();
        let mut branches = vec![format!(
            "select {columns} from {table} x where {}",
            equal.join(" and ")
        )];
        if cols.len() <= MAX_NULL_PATTERN_COLUMNS {
            for pattern in 1..(1_u32 << cols.len()) {
                let is_null = |i: usize| pattern & (1 << i) != 0;
                let guard: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let not = if is_null(i) { "" } else { "not " };
                        format!("{alias}.{c} is {not}null")
                    })
                    .collect();
                let matches: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        if is_null(i) {
                            format!("x.{c} is null")
                        } else {
                            format!("x.{c} = {}", probe(i, c))
                        }
                    })
                    .collect();
                branches.push(format!(
                    "select {columns} from {table} x where ({}) and {}",
                    guard.join(" and "),
                    matches.join(" and ")
                ));
            }
        } else {
            let has_null: Vec<String> = cols
                .iter()
                .map(|c| format!("{alias}.{c} is null"))
                .collect();
            let same: Vec<String> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("x.{c} is not distinct from {}", probe(i, c)))
                .collect();
            branches.push(format!(
                "select {columns} from {table} x where ({}) and {}",
                has_null.join(" or "),
                same.join(" and ")
            ));
        }
        format!("({} limit 1)", branches.join(" union all "))
    }

    /// The `create` of [`Self::staged`], one row per (group, transaction,
    /// statement) of the page with each partial's signed sum. Created once a
    /// session; its rows go with each page's transaction.
    pub(super) fn staged_ddl(&self) -> String {
        if !matches!(self.writer, Writer::Aggregate) {
            let mut extra = vec![
                "null::xid8 as __txid".to_string(),
                "null::pg_lsn as __lsn".to_string(),
            ];
            extra.extend(self.working_columns("__"));
            return self.join_working_ddl(&self.staged, &extra);
        }
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.staged.trim_start_matches("pg_temp."),
            staged_columns(&self.shape).join(", ")
        )
    }

    /// The `create` of [`Self::recomputed`], one row per group to recompute
    /// (repeats allowed). Created once a session; its rows go with each
    /// transaction. A `JOIN` side's copies its hidden table's cell columns.
    fn recomputed_ddl(&self) -> String {
        if !matches!(self.writer, Writer::Aggregate) {
            return self.join_working_ddl(&self.recomputed, &[]);
        }
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.recomputed.trim_start_matches("pg_temp."),
            recompute_columns(&self.shape).join(", ")
        )
    }

    /// The `create`s of every session table this plan's page stages into:
    /// [`Self::staged`], and but for a `JOIN`'s `T` [`Self::recomputed`].
    pub(super) fn working_ddl(&self) -> String {
        match self.writer {
            Writer::Aggregate if !self.shape.extremes().is_empty() => format!(
                "{}; {}; {}",
                self.staged_ddl(),
                self.recomputed_ddl(),
                self.probed_ddl()
            ),
            Writer::Aggregate | Writer::JoinPartials { .. } => {
                format!("{}; {}", self.staged_ddl(), self.recomputed_ddl())
            }
            Writer::JoinTarget => self.staged_ddl(),
        }
    }

    /// The `create` of [`Self::probed`]'s table, one row per group whose
    /// extremes the page probes. Created once a session; its rows go with
    /// each page's transaction.
    fn probed_ddl(&self) -> String {
        format!(
            "create temp table if not exists {} ({}) on commit delete rows",
            self.probed.trim_start_matches("pg_temp."),
            recompute_columns(&self.shape).join(", ")
        )
    }

    /// The statement adding groups to [`Self::recomputed`] ([`name_groups`]):
    /// the group of each image of `$1` (a `text[]` of source-row images,
    /// populated as the source's row type, so an image holding only the
    /// group columns names its group), and the group of the current source
    /// row of each key, read by its typed key parts: `$2..`, one `text[]`
    /// per source key column. Each key reads its row by its own index
    /// probe. A nullable key column (another aggregate's grouping column,
    /// when the source is its target) also matches a `NULL` part.
    fn name_groups_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut on = Vec::with_capacity(self.source_pk.len());
        let mut arrays = Vec::with_capacity(self.source_pk.len());
        let mut parts = Vec::with_capacity(self.source_pk.len());
        for (i, column) in self.source_pk.iter().enumerate() {
            let p = format!("__p{i}");
            let c = quote_ident(&column.name);
            on.push(if column.nullable {
                format!("(s.{c} = u.{p} or (s.{c} is null and u.{p} is null))")
            } else {
                format!("s.{c} = u.{p}")
            });
            arrays.push(format!("${}::text[]::{}[]", i + 2, column.data_type));
            parts.push(p);
        }
        format!(
            "insert into {rc} ({cols}) \
             select {values} from ( \
                 select s.* from unnest($1::text[]) as i(x) \
                 cross join lateral jsonb_populate_record(null::{src}, x::jsonb) as s \
                 union all \
                 select s.* from unnest({arrays}) as u({parts}) \
                 cross join lateral (select * from {src} s where {on}) as s \
             ) as s where true{filter}",
            rc = self.recomputed,
            values = self.group_values().join(", "),
            src = self.source_row,
            arrays = arrays.join(", "),
            parts = parts.join(", "),
            on = on.join(" and "),
            filter = self.stage_filter,
        )
    }

    /// Step 0's statement ([`stage_sides`]): parses `$1`'s ring rows, splits
    /// them into sides, evaluates the group and each argument on each, and
    /// sums them per (group, transaction, statement) into [`Self::staged`].
    /// A `Recompute`'s prior-image hint names a group to recompute
    /// ([`Self::recomputed`]). Returns the number of ring rows it can't
    /// apply as deltas; when there are any it stages nothing.
    ///
    /// `strict` stages each `numeric` sum with a declared scale
    /// ([`Arg::scale`]) as if every value had it: no scale map, no
    /// special-value counts, and a `NULL` map marking a staged row with a
    /// value that is special or at another scale. [`stage_sides`] then stages
    /// the page again the general way. A strict statement has no array
    /// aggregate, so Postgres can hash its groups.
    fn stage_sql(&self, strict: bool) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut values = self.group_values();
        values.push("sd.__txid".to_string());
        values.push("sd.__lsn".to_string());
        values.push("sd.__sgn".to_string());
        let mut sums = vec!["sum(__sgn)::bigint".to_string()];
        // A `numeric` sum's scale is a grouping key too, so each staged row
        // holds one scale, and its map is that scale's signed count: no
        // per-row map to fold. One with a declared scale counts that scale
        // instead ([`SumPart::of_rows`]), with no key of its own.
        let mut scale_keys = String::new();
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            values.push(format!("{} as __a{i}", arg.sql));
            let count = format!("sum(__sgn * (__a{i} is not null)::int)::bigint");
            sums.push(count.clone());
            for part in arg.parts() {
                sums.push(match (part, arg.scale) {
                    (SumPart::Scales, Some(s)) if strict => {
                        strict_stage_scales(&format!("__a{i}"), s)
                    }
                    (SumPart::Sum, Some(_)) if strict => {
                        format!("coalesce(sum(__sgn * __a{i}), 0)")
                    }
                    (_, Some(_)) if strict => part.zero().to_string(),
                    (SumPart::Scales, None) => {
                        scale_keys.push_str(&format!(", scale(__a{i})"));
                        format!(
                            "(case when scale(__a{i}) is null or {count} = 0 then {NO_SCALES} \
                                   else jsonb_build_object(scale(__a{i})::text, {count}) end)"
                        )
                    }
                    _ => part.of_rows(&format!("__a{i}"), arg.numeric, "__sgn", arg.scale),
                });
            }
        }
        for extreme in self.shape.extremes() {
            let a = format!("__a{}", extreme.arg + 1);
            let agg = extreme.aggregate();
            sums.push(format!("{agg}({a}) filter (where __sgn = 1)"));
            sums.push(format!("{agg}({a}) filter (where __sgn = -1)"));
        }
        // What the staged sides hold of each numeric sum's partials
        // ([`Self::staged_flags`]).
        let mut returned = vec!["__n".to_string()];
        let mut flags = Vec::new();
        for (i, part) in self.staged_flags() {
            let arg = &self.shape.args[i - 1];
            let column = part.unwrap_or(SumPart::Scales).name("__", i);
            returned.push(column.clone());
            flags.push(match (part, arg.scale) {
                (Some(_), Some(_)) if strict => "true".to_string(),
                (Some(_), _) => format!("coalesce(bool_and({column} = 0), true)"),
                (None, Some(_)) if strict => {
                    format!("coalesce(bool_and({column} is not null), true)")
                }
                (None, Some(s)) => format!("coalesce(bool_and({}), true)", only_scale(&column, s)),
                (None, None) => unreachable!("a scale flag without a declared scale"),
            });
        }
        let returned = returned.join(", ");
        // A `JOIN`'s `T` has no `Recompute` rows ([`Self::push`]) and no
        // recompute table. Where the populated source row `s` is in scope a
        // column is read through its relation's alias (`raw.r`): the row's
        // columns are the source's own, and a source column named `r` would
        // make a bare `r` ambiguous.
        let hinted = if !matches!(self.writer, Writer::JoinTarget) {
            format!(
                "hinted as ( \
                     insert into {recomputed} ({cols}) \
                     select {group_values} from raw \
                     cross join lateral jsonb_populate_record(null::{src}, raw.r -> 'o') as s \
                     where raw.r ->> 'op' = 'recompute' and jsonb_typeof(raw.r -> 'o') = 'object' \
                       and (select __bad from bad) = 0{filter} \
                 ),",
                filter = self.stage_filter,
                recomputed = self.recomputed,
                group_values = self.group_values().join(", "),
                src = self.source_row,
            )
        } else {
            String::new()
        };
        format!(
            "with {sides}, \
             bad as ( \
                 select count(*) as __bad from raw \
                 where r ->> 'op' <> 'recompute' \
                   and jsonb_typeof(r -> 'x') is distinct from 'object' \
                   and ((r ->> 'op' in ('update', 'delete') \
                         and jsonb_typeof(r -> 'o') is distinct from 'object') \
                     or (r ->> 'op' in ('insert', 'update') \
                         and jsonb_typeof(r -> 'n') is distinct from 'object')) \
             ), \
             {hinted} \
             ev as ( \
                 select {values} \
                 from sides sd cross join lateral jsonb_populate_record(null::{src}, sd.__i) as s \
                 where jsonb_typeof(sd.__i) = 'object'{filter} \
             ), \
             staged as ( \
                 insert into {staged} \
                 select {cols}, __txid, __lsn, {sums} from ev \
                 where (select __bad from bad) = 0 \
                 group by {cols}, __txid, __lsn{scale_keys} \
                 returning {returned} \
             ) \
             select __bad, {flags} from bad",
            sides = Self::sides_sql(),
            values = values.join(", "),
            src = self.source_row,
            staged = self.staged,
            sums = sums.join(", "),
            hinted = hinted,
            filter = self.stage_filter,
            returned = returned,
            flags = if flags.is_empty() {
                "null::boolean[]".to_string()
            } else {
                format!(
                    "(select array[{}]::boolean[] from staged)",
                    flags.join(", ")
                )
            },
        )
    }

    /// What [`Self::stage_sql`]'s flags say ([`StagedParts`]), in the order
    /// it lists them: per `numeric` sum, whether every staged side holds 0
    /// `NaN`s, `Infinity`s and `-Infinity`s, then, under a declared scale,
    /// whether every staged map counts only that scale.
    fn staged_flags(&self) -> Vec<(usize, Option<SumPart>)> {
        let mut flags = Vec::new();
        for (i, arg) in self.shape.args.iter().enumerate() {
            if arg.parts() != NUMERIC_PARTS {
                continue;
            }
            for part in [SumPart::NaN, SumPart::PosInf, SumPart::NegInf] {
                flags.push((i + 1, Some(part)));
            }
            if arg.scale.is_some() {
                flags.push((i + 1, None));
            }
        }
        flags
    }

    /// Step 1's statement ([`lock_groups`]): inserts every group the staged
    /// sides or [`Self::recomputed`] name that has no hidden row, and locks
    /// the rest, sorted, in one statement (R1, R7). A `JOIN`'s `T` has only
    /// its staged rows.
    pub(super) fn lock_sql(&self) -> String {
        self.lock_sql_where("")
    }

    /// [`Self::lock_sql`] over only the groups that meet `filter` (` where …`
    /// over the group columns, with its leading space, or empty for all).
    pub(super) fn lock_sql_where(&self, filter: &str) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let recomputed = if !matches!(self.writer, Writer::JoinTarget) {
            format!(" union all select {cols} from {}", self.recomputed)
        } else {
            String::new()
        };
        format!(
            "insert into {hidden} ({cols}) \
             select distinct {cols} from ( \
                 select {cols} from {staged}{recomputed} \
             ) as g{filter} \
             order by {cols} \
             on conflict ({cols}) do update set __n = excluded.__n where false",
            hidden = self.hidden_ident,
            staged = self.staged,
        )
    }

    /// Planted bugs `gb_skip_group_lock` and `join_t_skip_lock`
    /// (`crate::plant`): [`Self::lock_sql`]'s groups that have no hidden row
    /// yet, inserted, and the rest left unlocked.
    #[cfg(any(test, feature = "test-util"))]
    pub(super) fn planted_insert_missing_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let recomputed = if !matches!(self.writer, Writer::JoinTarget) {
            format!(" union all select {cols} from {}", self.recomputed)
        } else {
            String::new()
        };
        format!(
            "insert into {hidden} ({cols}) \
             select distinct {cols} from ( \
                 select {cols} from {staged}{recomputed} \
             ) as g \
             where not exists (select 1 from {hidden} x where {matched}) \
             order by {cols} \
             on conflict do nothing",
            hidden = self.hidden_ident,
            staged = self.staged,
            matched = self.group_match("x", "g"),
        )
    }

    /// The SQL of aggregate `agg` over partials named by `n`, `count` (a
    /// function of the argument's 1-based index), `sum` (of the index and
    /// the partial, [`SumPart`]) and `extreme` (of the extreme). `args` are
    /// the shape's arguments.
    fn agg_sql(
        agg: &Agg,
        args: &[Arg],
        n: &str,
        count: &dyn Fn(usize) -> String,
        sum: &dyn Fn(usize, SumPart) -> String,
        display: &dyn Fn(usize) -> Option<String>,
        extreme: &dyn Fn(&Extreme) -> String,
    ) -> String {
        let value = |i: usize| {
            sum_value(
                args[i].numeric,
                args[i].scale,
                display(i + 1).as_deref(),
                &|part| sum(i + 1, part),
            )
        };
        match agg {
            Agg::Extreme { max, arg, pg_type } => extreme(&Extreme {
                max: *max,
                arg: *arg,
                pg_type: pg_type.clone(),
            }),
            Agg::CountStar => n.to_string(),
            Agg::Count(i) => count(i + 1),
            Agg::Sum(i, cast) => format!(
                "case when {c} = 0 then null else ({s})::{cast} end",
                c = count(i + 1),
                s = value(*i)
            ),
            Agg::Avg(i) => format!(
                "case when {c} = 0 then null else ({s}) / ({c})::numeric end",
                c = count(i + 1),
                s = value(*i)
            ),
        }
    }

    /// A lateral subquery computing every field over partials named by
    /// `n`, `count`, `sum` and `extreme`, as columns `<prefix>0`,
    /// `<prefix>1`, …. `display` gives a `numeric` sum's display scale where
    /// the caller knows it without its map ([`sum_value`]).
    fn fields_lateral(
        &self,
        prefix: &str,
        n: &str,
        count: &dyn Fn(usize) -> String,
        sum: &dyn Fn(usize, SumPart) -> String,
        display: &dyn Fn(usize) -> Option<String>,
        extreme: &dyn Fn(&Extreme) -> String,
    ) -> String {
        let aggs: Vec<String> = self
            .shape
            .aggs
            .iter()
            .enumerate()
            .map(|(k, agg)| {
                format!(
                    "{} as {}",
                    Self::agg_sql(agg, &self.shape.args, n, count, sum, display, extreme),
                    quote_ident(&placeholder(k))
                )
            })
            .collect();
        let fields: Vec<String> = self
            .shape
            .fields
            .iter()
            .enumerate()
            .map(|(j, (_, pg_type, expr))| {
                format!("({})::{pg_type} as {prefix}{j}", render_expr_sql(expr))
            })
            .collect();
        let aggs = if aggs.is_empty() {
            "select".to_string()
        } else {
            format!("select {}", aggs.join(", "))
        };
        format!(
            "lateral (select {} from ({aggs}) as a) as {prefix}",
            fields.join(", ")
        )
    }

    /// Step 3's statement ([`apply_locked_groups`]), over [`Self::staged`].
    /// `$1` is the target (for its truncate floor). `image_columns` are the
    /// target's columns when a reader needs prior images.
    fn apply_sql(&self, image_columns: Option<&[String]>) -> String {
        let tail = match &self.writer {
            Writer::JoinPartials { deltas } => self.deltas_tail_sql(deltas),
            Writer::Aggregate => self.visible_tail_sql(image_columns),
            // A `T` write that staged a group with no hidden row (see
            // `Self::lost_cte_sql`) ends in one more row, which is NULL in
            // its `changed` column, with the count in its `__dn` column.
            Writer::JoinTarget => {
                let mut sentinel = vec![
                    "null::text".to_string(),
                    "null::boolean".to_string(),
                    "null::boolean".to_string(),
                    "null::boolean".to_string(),
                    "null::text".to_string(),
                    "lost.n".to_string(),
                ];
                for arg in &self.shape.args {
                    sentinel.push("null::bigint".to_string());
                    if arg.summed {
                        sentinel.push("null::text".to_string());
                    }
                }
                format!(
                    "{} union all select {} from lost where lost.n > 0",
                    self.visible_tail_sql(image_columns),
                    sentinel.join(", ")
                )
            }
        };
        self.apply_sql_with(&tail)
    }

    /// [`Self::apply_sql`] of `T` in a JOIN build ([`Self::compare_held_sql`]):
    /// adds the staged deltas to the hidden partials and writes no visible
    /// row. Returns how many groups it wrote, and how many staged groups had
    /// no hidden row ([`Self::lost_cte_sql`]).
    fn apply_hidden_sql(&self) -> String {
        self.apply_sql_with("n as (select 1) select count(*), (select n from lost) from up")
    }

    /// The error for `groups` staged groups with no hidden row.
    fn lock_gap(&self, groups: i64) -> ApplyError {
        ApplyError::GroupLockGap {
            target: self.target.clone(),
            groups,
        }
    }

    /// ADR-0019's strict `T` write, as [`Self::apply_sql_with`]'s `lost` CTE:
    /// how many staged groups [`Self::basis_ctes_sql`]'s lookup found no
    /// hidden row for. Every writer of `T` runs [`Self::lock_sql`] first,
    /// which inserts a row for each staged group, so a group missing here is
    /// a gap in the lock set, and the statement would drop its delta. Empty
    /// off a `JOIN`'s `T`: the aggregate path locks the groups it recomputes
    /// too and keeps its statement as it was.
    fn lost_cte_sql(&self) -> &'static str {
        if matches!(self.writer, Writer::JoinTarget) {
            ", lost as (select (select count(*) from pg) - (select count(*) from b) as n)"
        } else {
            ""
        }
    }

    /// The total of partial `part` of argument `i` (1-based) over the staged
    /// sides (those meeting `condition`, if any), each named as the staging
    /// table names it. A partial every staged side holds 0 of is its zero,
    /// and the scale map of an argument whose staged maps count only its
    /// declared scale ([`StagedParts::uniform`]) is its count of finite
    /// values at that scale: no map is read.
    fn staged_total(
        &self,
        staged_parts: &Option<StagedParts>,
        i: usize,
        part: SumPart,
        condition: Option<&str>,
    ) -> String {
        let arg = &self.shape.args[i - 1];
        let zero = |part: SumPart| {
            staged_parts
                .as_ref()
                .is_some_and(|p| p.zero.contains(&(i, part)))
        };
        if zero(part) {
            return part.zero().to_string();
        }
        if let (SumPart::Scales, Some(s), Some(parts)) = (part, arg.scale, staged_parts)
            && parts.uniform.contains(&i)
        {
            let filter = condition
                .map(|c| format!(" filter (where {c})"))
                .unwrap_or_default();
            let mut finite = format!("coalesce(sum(__c{i}){filter}, 0)");
            for special in [SumPart::NaN, SumPart::PosInf, SumPart::NegInf] {
                if !zero(special) {
                    finite.push_str(&format!(
                        " - coalesce(sum({}){filter}, 0)",
                        special.name("__", i)
                    ));
                }
            }
            return single_scale(s, &format!("({finite})::bigint"));
        }
        part.total(&part.name("__", i), condition, arg.scale)
    }

    /// The declared scale of argument `i` (1-based) when every staged map
    /// counts only it ([`StagedParts::uniform`]): its apply derives the map's
    /// change from the counts ([`Self::finite_change`]) and reads no staged
    /// map.
    fn derived_map(&self, staged_parts: &Option<StagedParts>, i: usize) -> Option<i32> {
        let arg = &self.shape.args[i - 1];
        arg.scale.filter(|_| {
            arg.parts() == NUMERIC_PARTS
                && staged_parts
                    .as_ref()
                    .is_some_and(|p| p.uniform.contains(&i))
        })
    }

    /// The change in argument `i`'s (1-based) count of finite values, from
    /// its count's and its special-value counts' changes named under
    /// `prefix` (`t.__` for `t.__c1`, `t.__nan1`, …), less the specials no
    /// staged side holds.
    fn finite_change(&self, staged_parts: &Option<StagedParts>, prefix: &str, i: usize) -> String {
        let mut finite = format!("{prefix}c{i}");
        for special in [SumPart::NaN, SumPart::PosInf, SumPart::NegInf] {
            if !staged_parts
                .as_ref()
                .is_some_and(|p| p.zero.contains(&(i, special)))
            {
                finite.push_str(&format!(" - {}", special.name(prefix, i)));
            }
        }
        format!("({finite})")
    }

    fn apply_sql_with(&self, tail: &str) -> String {
        let cols = quoted(&self.shape.group_cols);
        let args = &self.shape.args;
        let staged_parts = self.staged_parts.get();
        // The per-(group, transaction) sums of each partial.
        let mut sums = vec!["sum(__n)::bigint as __n".to_string()];
        let mut totals = vec!["sum(__n)::bigint as __n".to_string()];
        let mut sets = vec!["__n = u.__n + t.__n".to_string()];
        let mut nonzero = vec!["t.__n <> 0".to_string()];
        let mut returning = vec!["t.__n as __dn".to_string(), "u.__n as __nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            sums.push(format!("sum(__c{i})::bigint as __c{i}"));
            totals.push(format!("sum(__c{i})::bigint as __c{i}"));
            sets.push(format!("__count_{i} = u.__count_{i} + t.__c{i}"));
            nonzero.push(format!("t.__c{i} <> 0"));
            returning.push(format!("t.__c{i} as __dc{i}, u.__count_{i} as __nc{i}"));
            for part in arg.parts() {
                let (staged, hidden) = (part.name("__", i), part.hidden(i));
                let derived = match part {
                    SumPart::Scales => self.derived_map(&staged_parts, i),
                    _ => None,
                };
                if let Some(s) = derived {
                    // A map derived from the counts ([`Self::derived_map`]):
                    // no staged map is read or summed, and the hidden map
                    // is rebuilt from its count at `s` while it counts no
                    // other scale, and kept while the finite count doesn't
                    // change.
                    let change = self.finite_change(&staged_parts, "t.__", i);
                    sums.push(format!("{NO_SCALES} as {staged}"));
                    totals.push(format!("{NO_SCALES} as {staged}"));
                    sets.push(format!(
                        "{hidden} = {}",
                        derived_add(&format!("u.{hidden}"), &change, s)
                    ));
                    // A `JOIN` side's deltas table takes the change's map.
                    let delta = if matches!(self.writer, Writer::JoinPartials { .. }) {
                        single_scale(s, &change)
                    } else {
                        NO_SCALES.to_string()
                    };
                    returning.push(format!(
                        "{delta} as {}, u.{hidden} as {}",
                        part.name("__d", i),
                        part.name("__n", i)
                    ));
                    continue;
                }
                let total = self.staged_total(&staged_parts, i, *part, None);
                sums.push(format!("{total} as {staged}"));
                totals.push(format!("{total} as {staged}"));
                if staged_parts
                    .as_ref()
                    .is_some_and(|p| p.zero.contains(&(i, *part)))
                {
                    // Every staged side holds 0 of it: the row keeps its value.
                } else {
                    sets.push(format!(
                        "{hidden} = {}",
                        part.add(&format!("u.{hidden}"), &format!("t.{staged}"), arg.scale)
                    ));
                    nonzero.push(part.nonzero(&format!("t.{staged}")));
                }
                returning.push(format!(
                    "t.{staged} as {}, u.{hidden} as {}",
                    part.name("__d", i),
                    part.name("__n", i)
                ));
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let matched = self.group_match("sg", "b");
        #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_mut))]
        let mut unseen = "not pg_visible_in_snapshot(sg.__txid, b.__basis)";
        // Planted bug: order the change against the basis instead of
        // asking whether the basis saw it. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        if crate::plant::armed() == Some(crate::plant::Plant::GbLsnNotVisibility) {
            unseen = "sg.__txid >= pg_snapshot_xmax(b.__basis)";
        }
        let extremes = self.shape.extremes();
        if !extremes.is_empty() {
            return self.apply_extremes_sql(
                tail, &extremes, &sums, &sets, &nonzero, &up_groups, &returning, unseen,
            );
        }
        format!(
            "{basis_ctes}{lost}, \
             f as ( \
                 select sg.*, b.__hc from sg join b on {matched} \
                 where b.__basis is null or {unseen} \
             ), \
             t as (select __hc, {totals} from f group by __hc), \
             up as ( \
                 update {hidden} u set {sets} from t \
                 where u.ctid = any(array(select t.__hc from t where {nonzero})) \
                   and u.ctid = t.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            basis_ctes = self.basis_ctes_sql(&sums),
            lost = self.lost_cte_sql(),
            totals = totals.join(", "),
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            nonzero = nonzero.join(" or "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
        )
    }

    /// [`Self::apply_sql_with`] for a definition with extremes (see
    /// "Extremes" in the module doc). Each (group, transaction) sum is
    /// checked against both bases: the partials' (`__ok`, R2) and the
    /// extremes' (`__xok`, Rx2). The partials add what `__ok` keeps, as
    /// without extremes; each extreme moves by `least`/`greatest` to the
    /// extreme of the insertion sides `__xok` keeps (Rx3), and the group is
    /// marked to probe (`__probe`) when a retraction `__xok` keeps reaches
    /// the extreme so moved, or the extreme is `NULL` (Rx4). The groups to
    /// probe go to [`Self::probed`]. `up` returns each extreme's prior value
    /// (`__o…`, read with the basis) and its new one (`__n…`).
    #[allow(clippy::too_many_arguments)]
    fn apply_extremes_sql(
        &self,
        tail: &str,
        extremes: &[Extreme],
        sums: &[String],
        sets: &[String],
        nonzero: &[String],
        up_groups: &[String],
        returning: &[String],
        unseen: &str,
    ) -> String {
        let staged_parts = self.staged_parts.get();
        let mut sums = sums.to_vec();
        let mut totals =
            vec!["coalesce(sum(__n) filter (where __ok), 0)::bigint as __n".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            totals.push(format!(
                "coalesce(sum(__c{i}) filter (where __ok), 0)::bigint as __c{i}"
            ));
            for part in arg.parts() {
                let staged = part.name("__", i);
                // A derived map ([`Self::derived_map`]) is read from the counts.
                let total =
                    if *part == SumPart::Scales && self.derived_map(&staged_parts, i).is_some() {
                        NO_SCALES.to_string()
                    } else {
                        self.staged_total(&staged_parts, i, *part, Some("__ok"))
                    };
                totals.push(format!("{total} as {staged}"));
            }
        }
        let mut sets = sets.to_vec();
        let mut touched = nonzero.to_vec();
        let mut returning = returning.to_vec();
        let mut probe = Vec::with_capacity(extremes.len());
        let mut prior = Vec::with_capacity(extremes.len());
        let xok = "(b.__xbasis is null or not pg_visible_in_snapshot(sg.__txid, b.__xbasis))";
        // Planted bug: the extremes are filtered by the partials' basis. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let xok = if crate::plant::armed() == Some(crate::plant::Plant::GbxSharedBasis) {
            "(b.__basis is null or not pg_visible_in_snapshot(sg.__txid, b.__basis))"
        } else {
            xok
        };
        let null_probes = true;
        // Planted bug: a `NULL` extreme is never probed. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let null_probes =
            null_probes && !crate::plant::fires(crate::plant::Plant::GbxNullExtremeNoProbe, true);
        for x in extremes {
            let (agg, plus, minus, old) = (x.aggregate(), x.plus(), x.minus(), x.named("__o"));
            let column = x.column();
            sums.push(format!("{agg}({plus}) as {plus}"));
            sums.push(format!("{agg}({minus}) as {minus}"));
            totals.push(format!("{agg}({plus}) filter (where __xok) as {plus}"));
            totals.push(format!("{agg}({minus}) filter (where __xok) as {minus}"));
            totals.push(format!("{agg}({old}) as {old}"));
            prior.push(format!("b.{old}"));
            sets.push(format!("{column} = {}(u.{column}, t.{plus})", x.combine()));
            touched.push(format!("t.{plus} is not null or t.{minus} is not null"));
            returning.push(format!(
                "t.{old} as {old}, u.{column} as {}",
                x.named("__n")
            ));
            let reached = x.reaches(&format!("t.{minus}"), &format!("u.{column}"));
            probe.push(if null_probes {
                format!("(t.{minus} is not null and (u.{column} is null or {reached}))")
            } else {
                format!("(t.{minus} is not null and coalesce({reached}, false))")
            });
        }
        let cols = quoted(&self.shape.group_cols).join(", ");
        format!(
            "{basis_ctes}, \
             f as ( \
                 select sg.*, b.__hc, {prior}, \
                        (b.__basis is null or {unseen}) as __ok, \
                        {xok} as __xok \
                 from sg join b on {matched} \
             ), \
             t as ( \
                 select __hc, {totals} from f where __ok or __xok group by __hc \
             ), \
             up as ( \
                 update {hidden} u set {sets} from t \
                 where u.ctid = any(array(select t.__hc from t where {touched})) \
                   and u.ctid = t.__hc \
                 returning {up_groups}, {returning}, {probe} as __probe \
             ), \
             probed as (insert into {probed} ({cols}) select {cols} from up where __probe), \
             {tail}",
            basis_ctes = self.basis_ctes_sql(&sums),
            prior = prior.join(", "),
            matched = self.group_match("sg", "b"),
            totals = totals.join(", "),
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            touched = touched.join(" or "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            probe = probe.join(" or "),
            probed = self.probed,
        )
    }

    /// [`Self::apply_sql_with`]'s first CTEs: `sg`, the staged sides above
    /// the truncate floor (`$1`'s) summed per (group, transaction) into
    /// `sums`; `pg`, their groups; and `b`, each group's hidden row and basis
    /// (and, with extremes, the extremes' basis `__xbasis` and each
    /// extreme's value `__o…`).
    fn basis_ctes_sql(&self, sums: &[String]) -> String {
        let col_list = quoted(&self.shape.group_cols).join(", ");
        let extremes = self.shape.extremes();
        let (mut held, mut names) = (
            if matches!(self.writer, Writer::JoinTarget) {
                vec!["x.ctid".to_string(), "null::pg_snapshot".to_string()]
            } else {
                vec!["x.ctid".to_string(), "x.__basis".to_string()]
            },
            vec!["__hc".to_string(), "__basis".to_string()],
        );
        if !extremes.is_empty() {
            held.push("x.__xbasis".to_string());
            names.push("__xbasis".to_string());
            for x in &extremes {
                held.push(format!("x.{}", x.column()));
                names.push(x.named("__o"));
            }
        }
        let b_columns: Vec<String> = names.iter().map(|n| format!("h.{n}")).collect();
        format!(
            "with sg as ( \
                 select {col_list}, __txid, {sums} from {staged} \
                 {floor} \
                 group by {col_list}, __txid \
             ), \
             pg as (select distinct {col_list} from sg), \
             b as ( \
                 select pg.*, {b_columns} from pg \
                 cross join lateral {lookup_hidden} as h({names}) \
             )",
            staged = self.staged,
            sums = sums.join(", "),
            b_columns = b_columns.join(", "),
            names = names.join(", "),
            floor = if self.floor_key.is_some() {
                "where __lsn > coalesce( \
                     (select floor from ledger_truncate_floor where target_table = $1), \
                     '0/0'::pg_lsn)"
            } else {
                ""
            },
            lookup_hidden = self.group_lookup(
                &self.hidden_ident,
                Population::Hidden,
                "pg",
                &held.join(", ")
            ),
        )
    }

    /// Planted bug `gb_lsn_not_visibility` (`crate::plant`): how many of
    /// the staged (group, transaction) sums the plant refuses that the
    /// visibility test would apply: those whose transaction was in flight
    /// when the group's basis was taken.
    #[cfg(any(test, feature = "test-util"))]
    fn planted_in_flight_sql(&self) -> String {
        format!(
            "{}, \
             x as (select 1 from sg join b on {} \
                   where b.__basis is not null \
                     and not pg_visible_in_snapshot(sg.__txid, b.__basis) \
                     and sg.__txid < pg_snapshot_xmax(b.__basis)) \
             select count(*) from x",
            self.basis_ctes_sql(&["sum(__n)::bigint as __n".to_string()]),
            self.group_match("sg", "b"),
        )
    }

    /// The end of [`Self::apply_sql`] for [`Writer::JoinPartials`], after its
    /// `up` CTE: adds each written cell's change to `deltas` and returns how
    /// many cells it wrote.
    fn deltas_tail_sql(&self, deltas: &str) -> String {
        let mut columns = quoted(&self.shape.group_cols);
        columns.push("__dn".to_string());
        for (i, arg) in self.shape.args.iter().enumerate() {
            columns.push(format!("__dc{}", i + 1));
            for part in arg.parts() {
                columns.push(part.name("__d", i + 1));
            }
        }
        let columns = columns.join(", ");
        format!(
            "d as (insert into {deltas} ({columns}) select {columns} from up) \
             select count(*) from up"
        )
    }

    /// The source rows of the group `rg` (a lateral reference), read through
    /// the root: an index probe of the leaf its key routes to. A group with a
    /// `NULL` part matches each column by `=`, or by `is null` on both sides,
    /// which prunes to the leaves that can hold it and can use an index;
    /// `is not distinct from` does neither, and would scan every leaf of the
    /// source once per such group.
    fn probe_rows_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols);
        // `rg`'s group is the working collation's; the probe runs under the
        // source column's, the collation its index is on.
        let rg = |i: usize, c: &str| {
            self.collations.coerce(
                &format!("rg.{c}"),
                i,
                Population::Hidden,
                Population::Source,
            )
        };
        let equal: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("s.{c} = {}", rg(i, c)))
            .collect();
        let has_null: Vec<String> = cols.iter().map(|c| format!("rg.{c} is null")).collect();
        let same: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "(s.{c} = {} or (rg.{c} is null and s.{c} is null))",
                    rg(i, c)
                )
            })
            .collect();
        format!(
            "select s.* from {src} s where {equal} \
             union all \
             select s.* from {src} s where ({has_null}) and {same}",
            src = self.source_row,
            equal = equal.join(" and "),
            has_null = has_null.join(" or "),
            same = same.join(" and "),
        )
    }

    /// The extreme `x` of the source rows of the group `rg` (a lateral
    /// reference), as one index probe: the first non-`NULL` argument in the
    /// extreme's order, read through the root. A group with a `NULL` part
    /// matches each such column by `is null`, one branch per pattern of
    /// `NULL` columns behind a one-time test of `rg`'s, as
    /// [`Self::group_lookup`] does: `is not distinct from` would make each
    /// probe of such a group scan the source (past
    /// [`MAX_NULL_PATTERN_COLUMNS`] group columns it does, for those
    /// groups). An index on the `GROUP BY` columns then the argument serves
    /// every branch with one endpoint read, `NULL` groups' included; without
    /// one the probe scans the group.
    fn extreme_probe_sql(&self, x: &Extreme) -> String {
        let cols = quoted(&self.shape.group_cols);
        let arg = quote_ident(
            extreme_column(&self.shape.args[x.arg].expr).expect("an extreme reads a plain column"),
        );
        // `rg`'s group is the working collation's; the probe runs under the
        // source column's, the collation its index is on.
        let rg = |i: usize, c: &str| {
            self.collations.coerce(
                &format!("rg.{c}"),
                i,
                Population::Hidden,
                Population::Source,
            )
        };
        // Each branch orders by the `GROUP BY` columns too, then the
        // argument, all one way: the index's own order. The planner drops
        // a column an `=` fixes from the order, but not one `is null`
        // fixes, so ordered by the argument alone a `NULL` group's branch
        // sorts every row of the group instead of reading one endpoint.
        let order: Vec<String> = cols
            .iter()
            .chain(std::iter::once(&arg))
            .map(|c| format!("s.{c}{}", x.order()))
            .collect();
        let branch = |condition: String| {
            format!(
                "(select s.{arg} as __v from {src} s where {condition} and s.{arg} is not null \
                  order by {order} limit 1)",
                src = self.source_row,
                order = order.join(", "),
            )
        };
        let equal: Vec<String> = cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("s.{c} = {}", rg(i, c)))
            .collect();
        let mut branches = vec![branch(equal.join(" and "))];
        if cols.len() <= MAX_NULL_PATTERN_COLUMNS {
            for pattern in 1..(1_u32 << cols.len()) {
                let is_null = |i: usize| pattern & (1 << i) != 0;
                let condition: Vec<String> = cols
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        if is_null(i) {
                            format!("rg.{c} is null and s.{c} is null")
                        } else {
                            format!("rg.{c} is not null and s.{c} = {}", rg(i, c))
                        }
                    })
                    .collect();
                branches.push(branch(condition.join(" and ")));
            }
        } else {
            let has_null: Vec<String> = cols.iter().map(|c| format!("rg.{c} is null")).collect();
            let same: Vec<String> = cols
                .iter()
                .enumerate()
                .map(|(i, c)| format!("s.{c} is not distinct from {}", rg(i, c)))
                .collect();
            branches.push(branch(format!(
                "({}) and {}",
                has_null.join(" or "),
                same.join(" and ")
            )));
        }
        // Planted bug: a `NULL` group key is matched by `=` alone. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        if crate::plant::fires(crate::plant::Plant::GbxNullKeyByEquality, true) {
            branches.truncate(1);
        }
        format!(
            "(select {agg}(__v) from ({branches}) as __p)",
            agg = x.aggregate(),
            branches = branches.join(" union all "),
        )
    }

    /// The probe's statement ([`probe_locked_groups`], Rx5): over the groups
    /// of [`Self::probed`], locked by the page's lock statement, takes the
    /// snapshot and reads the newest segment's `seg_seq` in this one
    /// statement, re-reads every extreme of each group from the source
    /// ([`Self::extreme_probe_sql`]), and writes them with the snapshot as
    /// the extremes' basis and the segment as their stamp. It reads and
    /// writes no partial: the partials keep their own basis. `up` returns
    /// each partial's value with a change of zero, and each extreme's prior
    /// and new value, for [`Self::visible_tail_sql`].
    fn probe_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let extremes = self.shape.extremes();
        let mut held = vec!["x.ctid".to_string()];
        let mut held_names = vec!["__hc".to_string()];
        let mut probes = Vec::with_capacity(extremes.len());
        let mut sets = Vec::with_capacity(extremes.len());
        let mut returning = vec!["0::bigint as __dn".to_string(), "u.__n as __nn".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            returning.push(format!("0::bigint as __dc{i}, u.__count_{i} as __nc{i}"));
            for part in arg.parts() {
                returning.push(format!(
                    "{} as {}, u.{} as {}",
                    part.zero(),
                    part.name("__d", i),
                    part.hidden(i),
                    part.name("__n", i)
                ));
            }
        }
        for x in &extremes {
            let column = x.column();
            held.push(format!("x.{column}"));
            held_names.push(x.named("__o"));
            probes.push(format!(
                "{} as {}",
                self.extreme_probe_sql(x),
                x.named("__r")
            ));
            sets.push(format!("{column} = r.{}", x.named("__r")));
            returning.push(format!(
                "r.{} as {}, u.{column} as {}",
                x.named("__o"),
                x.named("__o"),
                x.named("__n")
            ));
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let stamp = "__xbasis = snap.__snap, __xstamp = snap.__seg";
        // Planted bugs: the probe writes no extremes' basis, or stamps the
        // partials' basis instead. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let stamp = if crate::plant::fires(crate::plant::Plant::GbxProbeNoXbasis, true) {
            "__xstamp = snap.__seg"
        } else if crate::plant::fires(crate::plant::Plant::GbxSharedBasis, true) {
            "__basis = snap.__snap, __stamp = snap.__seg"
        } else {
            stamp
        };
        format!(
            "with snap as materialized ({SNAPSHOT_AND_STAMP}), \
             rg as (select distinct {col_list} from {probed}), \
             r as ( \
                 select h.*, p.* from rg \
                 cross join lateral {lookup_hidden} as h({held_names}) \
                 cross join lateral (select {probes}) as p \
             ), \
             up as ( \
                 update {hidden} u set {sets}, {stamp} \
                 from r, snap \
                 where u.ctid = any(array(select r.__hc from r)) and u.ctid = r.__hc \
                 returning {up_groups}, {returning}, false as __probe \
             ), \
             {tail}",
            probed = self.probed,
            lookup_hidden = self.group_lookup(
                &self.hidden_ident,
                Population::Hidden,
                "rg",
                &held.join(", ")
            ),
            held_names = held_names.join(", "),
            probes = probes.join(", "),
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            tail = self.visible_tail_sql(image_columns),
        )
    }

    /// The recompute's statement ([`recompute_locked_groups`], R1, R5): over
    /// the groups of [`Self::recomputed`], already locked, takes the
    /// snapshot and reads the newest segment's `seg_seq` in this one
    /// statement, `GROUP BY`s each group's source rows into its partials,
    /// and writes them with the snapshot as the group's basis and the
    /// segment as its stamp. A group with no source row keeps its hidden
    /// row at zero with the basis (R4). `image_columns` as
    /// [`Self::apply_sql`]'s.
    ///
    /// `node` (quoted and qualified) reads the groups' rows from one
    /// partition of the source instead of through the root, in one
    /// `GROUP BY` of the whole node rather than an index probe per group: a
    /// partition correction's ([`recompute_routed`]), whose caller has
    /// established that the node holds every row of every group in the set
    /// and no row of a group outside it.
    fn recompute_sql(&self, image_columns: Option<&[String]>, node: Option<&str>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut old = vec!["x.__n".to_string()];
        let mut old_names = vec!["__on".to_string()];
        // The scan form's partials, `NULL` for a group the node has no row of.
        let mut zeroed = vec!["coalesce(n.__rn, 0) as __rn".to_string()];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut returning = vec![
            "u.__n - r.__on as __dn".to_string(),
            "u.__n as __nn".to_string(),
        ];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            old.push(format!("x.__count_{i}"));
            old_names.push(format!("__oc{i}"));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            zeroed.push(format!("coalesce(n.__rc{i}, 0) as __rc{i}"));
            returning.push(format!(
                "u.__count_{i} - r.__oc{i} as __dc{i}, u.__count_{i} as __nc{i}"
            ));
            for part in arg.parts() {
                let (hidden, held, read) =
                    (part.hidden(i), part.name("__o", i), part.name("__r", i));
                old.push(format!("x.{hidden}"));
                old_names.push(held.clone());
                sets.push(format!("{hidden} = r.{read}"));
                zeroed.push(format!("coalesce(n.{read}, {}) as {read}", part.zero()));
                returning.push(format!(
                    "{} as {}, u.{hidden} as {}",
                    part.sub(&format!("u.{hidden}"), &format!("r.{held}"), arg.scale),
                    part.name("__d", i),
                    part.name("__n", i)
                ));
            }
        }
        // Rx6: the extremes from the same snapshot, which becomes their
        // basis too.
        let extremes = self.shape.extremes();
        let mut xsets = String::new();
        let keep = false;
        // Planted bug: the absolute write leaves the extremes, their basis
        // and their stamp as they were. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let keep = keep
            || crate::plant::fires(
                crate::plant::Plant::GbxAbsoluteWriteKeepsExtremes,
                !extremes.is_empty(),
            );
        for x in &extremes {
            let (column, fresh_name) = (x.column(), x.named("__r"));
            old.push(format!("x.{column}"));
            old_names.push(x.named("__o"));
            zeroed.push(format!("n.{fresh_name} as {fresh_name}"));
            if !keep {
                sets.push(format!("{column} = r.{fresh_name}"));
            }
            returning.push(format!(
                "r.{o} as {o}, u.{column} as {}",
                x.named("__n"),
                o = x.named("__o")
            ));
        }
        if !extremes.is_empty() {
            returning.push("false as __probe".to_string());
            if !keep {
                xsets = ", __xbasis = snap.__snap, __xstamp = snap.__seg".to_string();
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let lookup_hidden = self.group_lookup(
            &self.hidden_ident,
            Population::Hidden,
            "rg",
            &format!("x.ctid, {}", old.join(", ")),
        );
        let r = match node {
            None => format!(
                "r as ( \
                     select h.*, a.* from rg \
                     cross join lateral {lookup_hidden} as h(__hc, {old_names}) \
                     cross join lateral ({fresh}) as a \
                 )",
                old_names = old_names.join(", "),
                fresh = self.fresh_select(&[], &format!("({})", self.probe_rows_sql()), false),
            ),
            Some(node) => {
                // The node's group columns at the hidden table's types, as
                // `rg`'s: array equality needs both sides' element types
                // the same (a `varchar` source column against a `text`
                // group column has no `=`).
                let matched = self.group_match("n", "rg");
                format!(
                    "n as materialized ({fresh}), \
                     r as ( \
                         select h.*, {zeroed} from rg \
                         cross join lateral {lookup_hidden} as h(__hc, {old_names}) \
                         left join n on {matched} \
                     )",
                    fresh = self.fresh_select(&self.group_values(), node, false),
                    zeroed = zeroed.join(", "),
                    old_names = old_names.join(", "),
                )
            }
        };
        format!(
            "with snap as materialized ( \
                 select pg_catalog.pg_current_snapshot() as __snap, \
                        coalesce((select max(seg_seq) from segments), 0) as __seg \
             ), \
             rg as (select distinct {col_list} from {recomputed}), \
             {r}, \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg{xsets} \
                 from r, snap \
                 where u.ctid = any(array(select r.__hc from r)) and u.ctid = r.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            recomputed = self.recomputed,
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            tail = match &self.writer {
                Writer::JoinPartials { deltas } => self.deltas_tail_sql(deltas),
                Writer::Aggregate | Writer::JoinTarget => self.visible_tail_sql(image_columns),
            },
        )
    }

    /// A `JOIN` build's snapshot statement for one side ([`write_join_build_cells`]):
    /// after every lock statement of the build, one `GROUP
    /// BY` of `node` (the side's table or leaf, quoted and qualified) into
    /// `grouped` (a session temp table, quoted and `pg_temp.`-qualified,
    /// dropped at commit), with one more row (`__meta`) holding the
    /// statement's snapshot and the newest segment's `seg_seq`. A `create …
    /// as select` reads in parallel where Postgres can; the data-modifying
    /// write that follows can't.
    fn join_group_sql(&self, node: &str, grouped: &str, strict: bool) -> String {
        let cols = quoted(&self.shape.group_cols);
        let mut nulls = vec!["null".to_string(); cols.len() + 1];
        for arg in &self.shape.args {
            nulls.push("null".to_string());
            for part in arg.parts() {
                nulls.push(format!("null::{}", part.sql_type()));
            }
        }
        format!(
            "create temp table {table} on commit drop as \
             select false as __meta, null::pg_snapshot as __snap, null::bigint as __seg, f.* \
             from ({fresh}) as f \
             union all \
             select true, pg_catalog.pg_current_snapshot(), \
                    coalesce((select max(seg_seq) from segments), 0), {nulls}",
            table = grouped.trim_start_matches("pg_temp."),
            fresh = self.fresh_select(&self.group_values(), node, strict),
            nulls = nulls.join(", "),
        )
    }

    /// A `JOIN` build's write of one side's cells ([`write_join_build_cells`]),
    /// after [`Self::join_group_sql`] filled
    /// `grouped`: over the cells of [`Self::recomputed`], whose keys the
    /// build holds under `J` and whose `NULL`-key cells it has locked, writes
    /// each cell's partials once from `grouped`: an update of a cell the
    /// hidden table holds, an insert of one it doesn't (zero for a cell the
    /// `GROUP BY` doesn't have, R4), with `grouped`'s snapshot as its basis
    /// and its segment as its stamp. Each cell's change (new partials less the
    /// held ones, which the locks keep as they were at the snapshot) goes to
    /// `deltas`. A cell the snapshot sees in the table but the enumeration
    /// didn't name is left to its pages.
    ///
    /// Two statements: the first fills `cells` (a temp table, quoted and
    /// `pg_temp.`-qualified) with each cell, the `ctid` of its hidden row
    /// (`NULL` if none), its held partials and its new ones; the caller
    /// analyzes it; the second writes from it. The cells are matched to the
    /// hidden rows and to the `GROUP BY` by hash joins on
    /// [`Self::group_match`], not by a lookup per cell: a build's cells are
    /// every cell of the side. Postgres has no statistics for that match's
    /// arrays, so it guesses its rows at orders of magnitude too many; the
    /// write is planned from `cells`' own statistics instead. Planned from
    /// the guess, its anti-join's hash table split into as many batches as
    /// the guessed rows called for, and a build over a hidden table that
    /// already holds its cells (a rebuild, a resume) ran for minutes, or
    /// hours at 100M rows, on the batches alone.
    ///
    /// A keyed cell is held by `J`, not by a row lock, so the zero-cell
    /// collection (`super::retire`), which takes no `J` lock, may delete one
    /// the first statement read: the update then finds no row, and the cell
    /// is inserted instead. The caller holds a snapshot from before the
    /// first statement until the second is done, so that `ctid` still holds
    /// the dead row, never another key's cell in a freed line pointer.
    /// Its held partials were zero, as the collection
    /// deletes only zero cells, so its change is the same either way. The
    /// cells to insert are those the update didn't write, an anti-join of the
    /// cells against the update's rows (a cell with no hidden row has a
    /// `NULL` `__hc`, which matches none). It is the bare `not exists`: an
    /// `or` beside it keeps it a subplan, which rescans the update's rows
    /// once per cell when they outgrow memory.
    fn join_build_sql(&self, grouped: &str, cells: &str, deltas: &str) -> [String; 2] {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut held = vec!["x.__n as __on".to_string()];
        let mut cell = vec![
            "coalesce(h.__on, 0) as __on".to_string(),
            "coalesce(n.__rn, 0) as __rn".to_string(),
        ];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut insert_cols = cols.clone();
        insert_cols.push("__n".to_string());
        let mut insert_values: Vec<String> = cols.iter().map(|c| format!("r.{c}")).collect();
        insert_values.push("r.__rn".to_string());
        let mut delta_cols = cols.clone();
        delta_cols.push("__dn".to_string());
        let mut delta_values: Vec<String> = cols.iter().map(|c| format!("r.{c}")).collect();
        delta_values.push("r.__rn - r.__on".to_string());
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            held.push(format!("x.__count_{i} as __oc{i}"));
            cell.push(format!("coalesce(h.__oc{i}, 0) as __oc{i}"));
            cell.push(format!("coalesce(n.__rc{i}, 0) as __rc{i}"));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            insert_cols.push(format!("__count_{i}"));
            insert_values.push(format!("r.__rc{i}"));
            delta_cols.push(format!("__dc{i}"));
            delta_values.push(format!("r.__rc{i} - r.__oc{i}"));
            for part in arg.parts() {
                let (hidden, old, read) =
                    (part.hidden(i), part.name("__o", i), part.name("__r", i));
                held.push(format!("x.{hidden} as {old}"));
                cell.push(format!("coalesce(h.{old}, {}) as {old}", part.zero()));
                cell.push(format!("coalesce(n.{read}, {}) as {read}", part.zero()));
                sets.push(format!("{hidden} = r.{read}"));
                insert_cols.push(hidden);
                insert_values.push(format!("r.{read}"));
                delta_cols.push(part.name("__d", i));
                delta_values.push(part.sub(&format!("r.{read}"), &format!("r.{old}"), arg.scale));
            }
        }
        insert_cols.extend(["__basis", "__stamp"].map(String::from));
        insert_values.extend(["snap.__snap", "snap.__seg"].map(String::from));
        let rg_cols: Vec<String> = cols.iter().map(|c| format!("rg.{c}")).collect();
        let x_cols: Vec<String> = cols.iter().map(|c| format!("x.{c}")).collect();
        let fill = format!(
            "create temp table {table} on commit drop as \
             select {rg_cols}, h.__hc, {cell} \
             from (select distinct {col_list} from {recomputed}) rg \
             left join (select x.ctid as __hc, {x_cols}, {held} from {hidden} x) h \
                 on {h_match} \
             left join (select * from {grouped} where not __meta) n on {n_match}",
            table = cells.trim_start_matches("pg_temp."),
            recomputed = self.recomputed,
            hidden = self.hidden_ident,
            x_cols = x_cols.join(", "),
            held = held.join(", "),
            rg_cols = rg_cols.join(", "),
            cell = cell.join(", "),
            h_match = self.group_match("h", "rg"),
            n_match = self.group_match("n", "rg"),
        );
        let write = format!(
            "with snap as materialized (select __snap, __seg from {grouped} where __meta), \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg \
                 from {cells} r, snap where u.ctid = r.__hc \
                 returning r.__hc \
             ), \
             ins as ( \
                 insert into {hidden} ({insert_cols}) \
                 select {insert_values} from {cells} r, snap \
                 where not exists (select from up where up.__hc = r.__hc) \
             ), \
             d as (insert into {deltas} ({delta_cols}) select {delta_values} from {cells} r) \
             select count(*) from {cells}",
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            insert_cols = insert_cols.join(", "),
            insert_values = insert_values.join(", "),
            delta_cols = delta_cols.join(", "),
            delta_values = delta_values.join(", "),
        );
        [fill, write]
    }

    /// A rebuild's enumeration ([`rebuild`], R8): adds to
    /// [`Self::recomputed`] every group of the target's own (its hidden
    /// rows, zero groups included, and its visible rows), every group of the
    /// source, and the group of every old image in the ring for the source
    /// (`$1`): an old image or a statement's transition `old` row. The ring
    /// read covers every undrained change, and may name the groups of
    /// drained changes whose segment is not yet retired, which costs only
    /// their recompute.
    ///
    /// With a `scope` (one structure of a partitioned source's rebuild) it
    /// adds only the groups the scope's predicates route to the structure,
    /// and, for a leaf, the groups of the leaf's own rows instead of the
    /// source's.
    fn rebuild_groups_sql(&self, scope: Option<&RebuildScope>) -> String {
        format!(
            "insert into {} ({}) {}",
            self.recomputed,
            quoted(&self.shape.group_cols).join(", "),
            self.rebuild_groups_select(scope, "$1")
        )
    }

    /// The `select` of [`Self::rebuild_groups_sql`]: `source` is the SQL of
    /// the ring's name for the source (`src_table`).
    fn rebuild_groups_select(&self, scope: Option<&RebuildScope>, source: &str) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let (hidden_filter, target_filter, image_filter) = match scope {
            Some(scope) => (
                format!(" where {}", scope.hidden),
                format!(" where {}", scope.target),
                format!(" where {}", scope.images),
            ),
            None => Default::default(),
        };
        let images: Vec<String> = self
            .ring
            .iter()
            .flat_map(|seg| {
                [
                    format!(
                        "select old_image as __i from {seg} \
                         where src_table = {source} and jsonb_typeof(old_image) = 'object'"
                    ),
                    format!(
                        "select transition_images -> 'old' from {seg} \
                         where src_table = {source} \
                           and jsonb_typeof(transition_images -> 'old') = 'object'"
                    ),
                ]
            })
            .collect();
        // The stage filter (` and …`, a `JOIN`'s joined side's: a row with a
        // `NULL` key part is no `M` cell, as on a page) added to `filter` (a
        // ` where …` or nothing). Empty for every other writer, so the
        // statement is unchanged for them.
        let staged = |filter: &str| -> String {
            match (filter.is_empty(), self.stage_filter.is_empty()) {
                (_, true) => filter.to_string(),
                (true, false) => format!(" where true{}", self.stage_filter),
                (false, false) => format!("{filter}{}", self.stage_filter),
            }
        };
        // A leaf's own rows; the whole source for an unpartitioned rebuild;
        // none for the remainder, whose groups have no row of their own.
        let own = match scope {
            None => format!(
                " union all select distinct {values} from {src} s{filter}",
                values = self.group_values().join(", "),
                src = self.source_row,
                filter = staged(""),
            ),
            Some(RebuildScope {
                node: Some(node), ..
            }) => format!(
                " union all select distinct {values} from {node} s{filter}",
                values = self.group_values().join(", "),
                filter = staged(""),
            ),
            Some(_) => String::new(),
        };
        let image_filter = staged(&image_filter);
        // A `JOIN` side's cells have no visible target of their own.
        let target_branch = match self.writer {
            Writer::JoinPartials { .. } => String::new(),
            _ => format!(
                " union all select {} from {} t{target_filter}",
                self.target_group_values(),
                self.target_ident
            ),
        };
        format!(
            "select {cols} from {hidden} h{hidden_filter}{target_branch}{own} \
             union all select {values} from ({images}) as r \
                 cross join lateral jsonb_populate_record(null::{src}, r.__i) as s{image_filter}",
            hidden = self.hidden_ident,
            values = self.group_values().join(", "),
            src = self.source_row,
            images = images.join(" union all "),
        )
    }

    /// A rebuild's write ([`rebuild`], R1, R5): over the groups of
    /// [`Self::recomputed`], already locked, takes the snapshot and reads
    /// the newest segment's `seg_seq` in this one statement, runs the
    /// `GROUP BY` of the whole source, and replaces each group's partials
    /// with its own (zero for a group the `GROUP BY` doesn't have, R4), with
    /// the snapshot as its basis and the segment as its stamp. A group the
    /// snapshot sees in the source but that wasn't locked (one whose first
    /// change committed after the enumeration) is left to its pages.
    ///
    /// The visible row is replaced too: compared with the row the target
    /// holds, not with what the old partials say, so a rebuild repairs a
    /// visible row that disagrees with them. It is upserted where its new
    /// values differ, deleted where the group's count is 0, and each prior
    /// image is the row the target held. Returns rows in
    /// [`Self::visible_tail_sql`]'s layout, with each partial's new value in
    /// place of its change.
    fn rebuild_sql(&self, image_columns: Option<&[String]>, read: &RebuildRead) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut kept = vec!["coalesce(a.__rn, 0) as __rn".to_string()];
        let mut sets = vec!["__n = r.__rn".to_string()];
        let mut returning = vec!["u.__n as __nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            kept.push(format!("coalesce(a.__rc{i}, 0) as __rc{i}"));
            sets.push(format!("__count_{i} = r.__rc{i}"));
            returning.push(format!("u.__count_{i} as __nc{i}"));
            for part in arg.parts() {
                let (hidden, read) = (part.hidden(i), part.name("__r", i));
                kept.push(format!("coalesce(a.{read}, {}) as {read}", part.zero()));
                sets.push(format!("{hidden} = r.{read}"));
                returning.push(format!("u.{hidden} as {}", part.name("__n", i)));
            }
        }
        // Rx6: the extremes from the same `GROUP BY` and snapshot, which
        // becomes their basis too.
        let extremes = self.shape.extremes();
        let mut xsets = String::new();
        let keep = false;
        // Planted bug: the absolute write leaves the extremes, their basis
        // and their stamp as they were. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let keep = keep
            || crate::plant::fires(
                crate::plant::Plant::GbxAbsoluteWriteKeepsExtremes,
                !extremes.is_empty(),
            );
        for x in &extremes {
            let (column, fresh_name) = (x.column(), x.named("__r"));
            kept.push(format!("a.{fresh_name} as {fresh_name}"));
            if !keep {
                sets.push(format!("{column} = r.{fresh_name}"));
            }
            returning.push(format!("u.{column} as {}", x.named("__n")));
        }
        if !extremes.is_empty() && !keep {
            xsets = ", __xbasis = snap.__snap, __xstamp = snap.__seg".to_string();
        }
        let matched = self.group_match("a", "rg");
        let up_groups: Vec<String> = cols.iter().map(|c| format!("u.{c}")).collect();
        let lookup_hidden =
            self.group_lookup(&self.hidden_ident, Population::Hidden, "rg", "x.ctid");
        let r = match read {
            #[cfg(any(test, feature = "test-util"))]
            RebuildRead::Early(early) => format!(
                "a as (select * from {early} where __rn is not null), \
                 r as ( \
                     select h.__hc, {kept} from rg \
                     cross join lateral {lookup_hidden} as h(__hc) \
                     left join a on {matched} \
                 )",
                kept = kept.join(", "),
            ),
            // One `GROUP BY` of the source or of a leaf, matched to the
            // locked groups.
            RebuildRead::Source | RebuildRead::Node(_) => {
                let from = match read {
                    RebuildRead::Node(node) => node.as_str(),
                    _ => self.source_row.as_str(),
                };
                format!(
                    "a as ({fresh}), \
                     r as ( \
                         select h.__hc, {kept} from rg \
                         cross join lateral {lookup_hidden} as h(__hc) \
                         left join a on {matched} \
                     )",
                    fresh = self.fresh_select(&self.group_values(), from, false),
                    kept = kept.join(", "),
                )
            }
            // One index probe of the root per group, for a set that is not
            // a leaf's (the remainder's) or a leaf of a tree that is not as
            // recorded.
            RebuildRead::Probe => format!(
                "r as ( \
                     select h.__hc, {kept} from rg \
                     cross join lateral {lookup_hidden} as h(__hc) \
                     cross join lateral ({fresh}) as a \
                 )",
                kept = kept.join(", "),
                fresh = self.fresh_select(&[], &format!("({})", self.probe_rows_sql()), false),
            ),
        };
        #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_mut))]
        let mut snap = SNAPSHOT_AND_STAMP.to_string();
        #[cfg(any(test, feature = "test-util"))]
        if let RebuildRead::Early(early) = read {
            snap = format!("select __snap, __seg from {early} limit 1");
        }
        format!(
            "with snap as materialized ({snap}), \
             rg as (select distinct {col_list} from {recomputed}), \
             {r}, \
             up as ( \
                 update {hidden} u set {sets}, __basis = snap.__snap, __stamp = snap.__seg{xsets} \
                 from r, snap where u.ctid = r.__hc \
                 returning {up_groups}, {returning} \
             ), \
             {tail}",
            recomputed = self.recomputed,
            r = r,
            hidden = self.hidden_ident,
            sets = sets.join(", "),
            up_groups = up_groups.join(", "),
            returning = returning.join(", "),
            tail = self.held_tail_sql(image_columns),
        )
    }

    /// Planted bug `gb_build_snapshot_before_locks` (`crate::plant`):
    /// creates `early`, a temporary table holding [`Self::rebuild_sql`]'s
    /// `GROUP BY` of the whole source and its snapshot and stamp, all from
    /// this one statement's snapshot (one row of `NULL`s past the snapshot
    /// when the source is empty).
    #[cfg(any(test, feature = "test-util"))]
    fn planted_early_read_sql(&self, early: &str) -> String {
        format!(
            "create temporary table {early} on commit drop as \
             with snap as materialized ({SNAPSHOT_AND_STAMP}) \
             select snap.*, a.* from snap left join ({fresh}) as a on true",
            fresh = self.fresh_select(&self.group_values(), &self.source_row, false),
        )
    }

    /// An absolute write's read of the rows of `from` (a `FROM` item, read
    /// as `s`): `select <values>, <partials>`, one row per group of
    /// `values` (each `… as <group column>`), or one row over every row when
    /// `values` is empty. The partials are named `__rn`, then per argument
    /// `__rc<i>` and its sum's partials (`__r<stem><i>`), then per extreme
    /// its `__r<stem><i>`.
    ///
    /// With a `numeric` sum, the rows are first grouped by the groups and
    /// each such argument's `scale(x)` as well, then by the groups alone:
    /// each scale's count comes from a group of its own, so the second step
    /// folds one row per scale into the map, never one per source row.
    ///
    /// `strict` counts a `numeric` sum with a declared scale ([`Arg::scale`])
    /// as if every finite value had it, with no array aggregate (which
    /// would keep Postgres from hashing the groups), and fails with
    /// [`OFF_SCALE`] on a group with a finite value at another: its caller
    /// runs the statement again without it ([`strictly_or_generally`]).
    fn fresh_select(&self, values: &[String], from: &str, strict: bool) -> String {
        let args = &self.shape.args;
        let scaled: Vec<usize> = (0..args.len())
            .filter(|&i| args[i].scale.is_none() && args[i].parts().contains(&SumPart::Scales))
            .collect();
        // Each partial: its name, its aggregate over the rows, and, when the
        // rows are grouped by scale first, how the second step folds it.
        let mut partials: Vec<(String, String, String)> = vec![(
            "__rn".to_string(),
            "count(*)::bigint".to_string(),
            "coalesce(sum(__rn), 0)::bigint".to_string(),
        )];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            let count = format!("__rc{i}");
            partials.push((
                count.clone(),
                format!(
                    "(count(*) filter (where ({}) is not null))::bigint",
                    arg.sql
                ),
                format!("coalesce(sum({count}), 0)::bigint"),
            ));
            for part in arg.parts() {
                let name = part.name("__r", i);
                let fold = match part {
                    SumPart::Scales if arg.scale.is_none() => scales_of(
                        &format!("array_agg(__g{i})"),
                        Some(&format!("array_agg({count})")),
                    ),
                    _ => part.total(&name, None, arg.scale),
                };
                let agg = match (part, arg.scale) {
                    (SumPart::Scales, Some(s)) if strict => strict_build_scales(&arg.sql, s),
                    _ => part.of_rows(&arg.sql, arg.numeric, "1", arg.scale),
                };
                partials.push((name, agg, fold));
            }
        }
        for x in self.shape.extremes() {
            let name = x.named("__r");
            let fold = format!("{}({name})", x.aggregate());
            partials.push((
                name,
                format!("{}({})", x.aggregate(), args[x.arg].sql),
                fold,
            ));
        }
        let positions = |n: usize| -> String {
            if n == 0 {
                String::new()
            } else {
                let p: Vec<String> = (1..=n).map(|p| p.to_string()).collect();
                format!(" group by {}", p.join(", "))
            }
        };
        let lead =
            |columns: &[String]| -> String { columns.iter().map(|c| format!("{c}, ")).collect() };
        if scaled.is_empty() {
            let list: Vec<String> = partials
                .iter()
                .map(|(name, agg, _)| format!("{agg} as {name}"))
                .collect();
            return format!(
                "select {}{} from {from} s{}",
                lead(values),
                list.join(", "),
                positions(values.len())
            );
        }
        let mut inner: Vec<String> = values.to_vec();
        for &i in &scaled {
            inner.push(format!("scale(({})) as __g{}", args[i].sql, i + 1));
        }
        let keys = inner.len();
        // The scale maps the scale keys fold are computed by the outer step
        // alone.
        let keyed: Vec<String> = scaled
            .iter()
            .map(|&i| SumPart::Scales.name("__r", i + 1))
            .collect();
        for (name, agg, _) in &partials {
            if !keyed.contains(name) {
                inner.push(format!("{agg} as {name}"));
            }
        }
        let groups = quoted(&self.shape.group_cols);
        let groups: &[String] = if values.is_empty() { &[] } else { &groups };
        let outer: Vec<String> = partials
            .iter()
            .map(|(name, _, fold)| format!("{fold} as {name}"))
            .collect();
        format!(
            "select {}{} from (select {} from {from} s{}) as s{}",
            lead(groups),
            outer.join(", "),
            inner.join(", "),
            positions(keys),
            if groups.is_empty() {
                String::new()
            } else {
                format!(" group by {}", groups.join(", "))
            }
        )
    }

    /// The end of [`Self::rebuild_sql`] and [`Self::compare_held_sql`], after
    /// their `up` CTE, which returns each group's columns and its partials'
    /// values (`__nn`, `__nc<i>`, `__ns<i>`): the visible row each group's
    /// partials give is compared with the row the target **holds**, upserted
    /// where its values differ, deleted where the count is 0, and each prior
    /// image is the held row. Returns rows in [`Self::visible_tail_sql`]'s
    /// layout, with each partial's value in place of its change.
    fn held_tail_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        let mut applied = vec!["cur.__nn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            let i = i + 1;
            applied.push(format!("cur.__nc{i}"));
            if arg.summed {
                applied.push(format!("cur.__ns{i}::text"));
            }
        }
        // An absolute write leaves no extreme to probe.
        if !self.shape.extremes().is_empty() {
            applied.push("false".to_string());
        }
        let field_count = self.shape.fields.len();
        let field_names: Vec<String> = self
            .shape
            .fields
            .iter()
            .map(|(name, _, _)| quote_ident(name))
            .collect();
        let new_fields = self.fields_lateral(
            "__nf",
            "up.__nn",
            &|i| format!("up.__nc{i}"),
            &|i, part| part.name("up.__n", i),
            &|_| None,
            &|x| x.named("up.__n"),
        );
        let nf: Vec<String> = (0..field_count).map(|j| format!("nv.__nf{j}")).collect();
        let held: Vec<String> = (0..field_count).map(|j| format!("t.__vf{j}")).collect();
        let held_names: Vec<String> = (0..field_count).map(|j| format!("__vf{j}")).collect();
        let mut lookup_columns = vec!["x.ctid".to_string()];
        lookup_columns.extend(field_names.iter().map(|f| format!("x.{f}")));
        let mut lookup_names = vec!["__tc".to_string()];
        lookup_names.extend(held_names.iter().cloned());
        let same_values = if field_count == 0 {
            "true".to_string()
        } else {
            format!(
                "({}) is not distinct from ({})",
                nf.join(", "),
                held.join(", ")
            )
        };
        let field_updates: Vec<String> = field_names
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect();
        let upsert_action = if field_updates.is_empty() {
            "do nothing".to_string()
        } else {
            format!("do update set {}", field_updates.join(", "))
        };
        let prior_image = match image_columns {
            Some(columns) => {
                let pairs: Vec<String> = columns
                    .iter()
                    .map(|column| {
                        let value = if self.shape.group_cols.contains(column) {
                            format!("cur.{}", quote_ident(column))
                        } else if let Some(j) =
                            self.shape.fields.iter().position(|(n, _, _)| n == column)
                        {
                            format!("cur.__vf{j}")
                        } else {
                            "null".to_string()
                        };
                        format!("{}, ({value})::text", quote_literal(column))
                    })
                    .collect();
                format!(
                    "case when cur.__tc is not null then ({})::text end",
                    jsonb_build_object_sql(&pairs)
                )
            }
            None => "null::text".to_string(),
        };
        format!(
            "nv as (select up.*, {nf_all} from up cross join {new_fields}), \
             cur as ( \
                 select nv.*, t.* , \
                        (nv.__nn > 0) is distinct from (t.__tc is not null) \
                            or (nv.__nn > 0 and not ({same_values})) as __changed \
                 from nv left join lateral {lookup_target} as t({lookup_names}) on true \
             ), \
             written as ( \
                 insert into {target} ({col_list}{field_list}) \
                 select {col_list}{nf_list} from cur where __changed and __nn > 0 \
                 order by {col_list} \
                 on conflict ({col_list}) {upsert_action} \
             ), \
             deleted as ( \
                 delete from {target} where ctid = any(array( \
                     select __tc from cur where __changed and __nn <= 0 and __tc is not null)) \
             ) \
             select {key}, cur.__changed, cur.__nn > 0, cur.__tc is not null, {prior_image}, \
                    {applied} \
             from cur",
            nf_all = if field_count == 0 {
                "true as __trellis_none".to_string()
            } else {
                (0..field_count)
                    .map(|j| format!("__nf.__nf{j}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            },
            lookup_target = self.group_lookup(
                &self.target_ident,
                Population::Target,
                "nv",
                &lookup_columns.join(", ")
            ),
            lookup_names = lookup_names.join(", "),
            target = self.target_ident,
            field_list = field_names
                .iter()
                .map(|f| format!(", {f}"))
                .collect::<String>(),
            nf_list = (0..field_count)
                .map(|j| format!(", __nf{j}"))
                .collect::<String>(),
            key = ddl::pk_key_sql_expr(&self.identity, Some("cur")),
            applied = applied.join(", "),
        )
    }

    /// A JOIN build's visible compare for `T` ([`apply_join_build_groups`]),
    /// over every group of the staging table (the product's groups and
    /// [`Self::target_groups_sql`]'s zero rows), already locked and with the
    /// staged deltas applied to the hidden partials: each group's visible
    /// row, computed from the partials it now holds, is compared with the row
    /// the target holds ([`Self::held_tail_sql`]). That repairs a visible row
    /// that disagrees with the partials, and removes one whose group has no
    /// partial left, which a delta alone never reaches.
    fn compare_held_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let mut names = vec!["__nn".to_string()];
        let mut columns = vec!["x.__n".to_string()];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            names.push(format!("__nc{i}"));
            columns.push(format!("x.__count_{i}"));
            for part in arg.parts() {
                names.push(part.name("__n", i));
                columns.push(format!("x.{}", part.hidden(i)));
            }
        }
        let up_groups: Vec<String> = cols.iter().map(|c| format!("rg.{c}")).collect();
        let up_values: Vec<String> = names.iter().map(|n| format!("h.{n}")).collect();
        format!(
            "with rg as (select distinct {col_list} from {staged}), \
             up as ( \
                 select {up_groups}, {up_values} from rg \
                 cross join lateral {lookup_hidden} as h({names}) \
             ), \
             {tail}",
            staged = self.staged,
            up_groups = up_groups.join(", "),
            up_values = up_values.join(", "),
            lookup_hidden = self.group_lookup(
                &self.hidden_ident,
                Population::Hidden,
                "rg",
                &columns.join(", ")
            ),
            names = names.join(", "),
            tail = self.held_tail_sql(image_columns),
        )
    }

    /// A JOIN build's enumeration of `T`'s groups (ADR-0002's R8, one level
    /// up): a zero row in `T`'s staging table for every group the visible
    /// target holds and every group `T`'s hidden table holds, so the lock of
    /// [`Self::lock_sql`] covers them with the product's groups, and the
    /// compare reaches a visible row whose group has no source row left. A
    /// zero row changes no partial.
    fn target_groups_sql(&self) -> String {
        let cols = quoted(&self.shape.group_cols).join(", ");
        let mut insert = vec!["__txid", "__lsn", "__n"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let mut zeros = vec![
            "pg_current_xact_id()".to_string(),
            "'0/0'::pg_lsn".to_string(),
            "0".to_string(),
        ];
        for (i, arg) in self.shape.args.iter().enumerate() {
            let i = i + 1;
            insert.push(format!("__c{i}"));
            zeros.push("0".to_string());
            for part in arg.parts() {
                insert.push(part.name("__", i));
                zeros.push(part.zero().to_string());
            }
        }
        format!(
            "insert into {staged} ({cols}, {insert}) \
             select {cols}, {zeros} from ( \
                 select {cols} from {hidden} union select {target_cols} from {target} \
             ) as g",
            staged = self.staged,
            insert = insert.join(", "),
            zeros = zeros.join(", "),
            hidden = self.hidden_ident,
            target_cols = self.target_group_values(),
            target = self.target_ident,
        )
    }

    /// The end of [`Self::apply_sql`] and [`Self::recompute_sql`], after
    /// their `up` CTE, which returns each written group's columns, and per
    /// partial its change (`__dn`, `__dc<i>`, `__ds<i>`) and its new value
    /// (`__nn`, `__nc<i>`, `__ns<i>`): writes the visible row when a visible
    /// value changed (an upsert while `__n > 0`, a delete when it falls to
    /// 0), and returns per group its key, whether its visible row changed,
    /// whether it is visible now and was before, its prior visible image,
    /// and its change per partial.
    fn visible_tail_sql(&self, image_columns: Option<&[String]>) -> String {
        let cols = quoted(&self.shape.group_cols);
        let col_list = cols.join(", ");
        let args = &self.shape.args;
        // An extreme has no change to subtract: `up` carries its prior
        // value (`__o…`) beside its new one (`__n…`).
        let new_fields = self.fields_lateral(
            "__nf",
            "up.__nn",
            &|i| format!("up.__nc{i}"),
            &|i, part| part.name("up.__n", i),
            &|_| None,
            &|x| x.named("up.__n"),
        );
        // The prior image's partials are the new ones less the change. A
        // map the apply derived from the counts ([`Self::derived_map`]) has
        // no change map in `up`: its change counts only the declared scale,
        // so the prior map shows that scale wherever the new one counts no
        // other, and is rebuilt only where it does.
        let staged_parts = self.staged_parts.get();
        let prior_fields = self.fields_lateral(
            "__pf",
            "(up.__nn - up.__dn)",
            &|i| format!("(up.__nc{i} - up.__dc{i})"),
            &|i, part| match (part, self.derived_map(&staged_parts, i)) {
                (SumPart::Scales, Some(s)) => SumPart::Scales.sub(
                    &part.name("up.__n", i),
                    &single_scale(s, &self.finite_change(&staged_parts, "up.__d", i)),
                    Some(s),
                ),
                _ => part.sub(
                    &part.name("up.__n", i),
                    &part.name("up.__d", i),
                    self.shape.args[i - 1].scale,
                ),
            },
            &|i| {
                self.derived_map(&staged_parts, i).map(|s| {
                    derived_prior_scale(
                        &SumPart::Scales.name("up.__n", i),
                        &self.finite_change(&staged_parts, "up.__d", i),
                        s,
                    )
                })
            },
            &|x| x.named("up.__o"),
        );
        let field_count = self.shape.fields.len();
        let nf: Vec<String> = (0..field_count).map(|j| format!("__nf.__nf{j}")).collect();
        let pf: Vec<String> = (0..field_count).map(|j| format!("__pf.__pf{j}")).collect();
        let field_names: Vec<String> = self
            .shape
            .fields
            .iter()
            .map(|(name, _, _)| quote_ident(name))
            .collect();
        let vis_new: Vec<String> = (0..field_count).map(|j| format!("__nf{j}")).collect();
        let field_updates: Vec<String> = field_names
            .iter()
            .map(|f| format!("{f} = excluded.{f}"))
            .collect();
        let upsert_action = if field_updates.is_empty() {
            "do nothing".to_string()
        } else {
            format!("do update set {}", field_updates.join(", "))
        };
        // The prior visible row's image, for the seam, from the partials
        // before this statement's change.
        let prior_image = match image_columns {
            Some(columns) => {
                let pairs: Vec<String> = columns
                    .iter()
                    .map(|column| {
                        let value = if self.shape.group_cols.contains(column) {
                            format!("vis.{}", quote_ident(column))
                        } else if let Some(j) =
                            self.shape.fields.iter().position(|(n, _, _)| n == column)
                        {
                            format!("vis.__pf{j}")
                        } else {
                            "null".to_string()
                        };
                        format!("{}, ({value})::text", quote_literal(column))
                    })
                    .collect();
                format!(
                    "case when vis.__nn - vis.__dn > 0 then ({})::text end",
                    jsonb_build_object_sql(&pairs)
                )
            }
            None => "null::text".to_string(),
        };
        let mut applied: Vec<String> = vec!["vis.__dn".to_string()];
        for (i, arg) in args.iter().enumerate() {
            applied.push(format!("vis.__dc{}", i + 1));
            if arg.summed {
                applied.push(format!("vis.__ds{}::text", i + 1));
            }
        }
        // Whether the group's extremes are to be probed (Rx4).
        if !self.shape.extremes().is_empty() {
            applied.push("vis.__probe".to_string());
        }
        format!(
            "vis as ( \
                 select up.*, {nf_pf}, \
                        (up.__nn > 0, {nf_cols}) is distinct from \
                            (up.__nn - up.__dn > 0, {pf_cols}) as __changed \
                 from up cross join {new_fields} cross join {prior_fields} \
             ), \
             written as ( \
                 insert into {target} ({col_list}{field_list}) \
                 select {col_list}{vis_new} from vis where __changed and __nn > 0 \
                 order by {col_list} \
                 on conflict ({col_list}) {upsert_action} \
             ), \
             deleted as ( \
                 delete from {target} where ctid = any(array( \
                     select h.__c from vis cross join lateral {lookup_target} as h(__c) \
                     where vis.__changed and vis.__nn <= 0 and vis.__nn - vis.__dn > 0)) \
             ) \
             select {key}, vis.__changed, vis.__nn > 0, vis.__nn - vis.__dn > 0, {prior_image}, \
                    {applied} \
             from vis",
            nf_pf = if field_count == 0 {
                "true as __trellis_none".to_string()
            } else {
                nf.iter().chain(&pf).cloned().collect::<Vec<_>>().join(", ")
            },
            nf_cols = if nf.is_empty() {
                "true".to_string()
            } else {
                nf.join(", ")
            },
            pf_cols = if pf.is_empty() {
                "true".to_string()
            } else {
                pf.join(", ")
            },
            target = self.target_ident,
            field_list = field_names
                .iter()
                .map(|f| format!(", {f}"))
                .collect::<String>(),
            vis_new = vis_new.iter().map(|f| format!(", {f}")).collect::<String>(),
            lookup_target =
                self.group_lookup(&self.target_ident, Population::Target, "vis", "x.ctid"),
            key = ddl::pk_key_sql_expr(&self.identity, Some("vis")),
            applied = applied.join(", "),
        )
    }
}

/// The settings [`apply_locked_groups`]'s statement runs under, and their
/// reset for the rest of the page's transaction. An error leaves them on,
/// but it also aborts the transaction, where a `set local` ends.
const PLAN_SETTINGS: &str = "set local enable_nestloop = off; set local enable_seqscan = off";
const PLAN_RESET: &str =
    "set local enable_nestloop to default; set local enable_seqscan to default";

/// A `JOIN` build's settings for its statements over every cell
/// ([`write_join_build_cells`], `super::join`'s product): they join cell sets
/// on [`GroupTargetPlan::group_match`]'s arrays, which a merge join must sort,
/// and sorting arrays costs several times hashing them.
pub(super) const BULK_PLAN_SETTINGS: &str = "set local enable_mergejoin = off";
pub(super) const BULK_PLAN_RESET: &str = "set local enable_mergejoin to default";

/// The most `GROUP BY` columns a group lookup writes one index probe per
/// pattern of `NULL`s for (`GroupTargetPlan::group_lookup`): 15 probes at 4.
const MAX_NULL_PATTERN_COLUMNS: usize = 4;

/// What one page applied to one group ([`apply_locked_groups`]): the
/// group's identity key text (`ddl::pk_key_sql_expr`), and the deltas it
/// added to `__n`, to each argument's count and, for a summed argument, to
/// its sum (as text), in argument order; and whether its extremes are to
/// be probed (Rx4; always false for a definition with no `MIN`/`MAX`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppliedGroup {
    pub key: String,
    pub n: i64,
    pub counts: Vec<i64>,
    pub sums: Vec<Option<String>>,
    pub probe: bool,
}

/// Step 1 of a page on a group-basis target (see the module doc): locks, in
/// one sorted statement, every group a side of the page names and every
/// group its image-less changes name, creating the hidden rows of those
/// that have none.
pub(crate) async fn lock_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    stage_sides(txn, plan).await?;
    name_groups(txn, plan, &[], &plan.recompute_keys).await?;
    // Planted bug: the page probes every group it stages before its lock
    // statement, and not after its apply. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if planted_probe_before_lock(plan) {
        let cols = quoted(&plan.shape.group_cols).join(", ");
        txn.execute(
            &format!(
                "insert into {} ({cols}) select distinct {cols} from {}",
                plan.probed, plan.staged
            ),
            &[],
        )
        .await?;
        probe_locked_groups(txn, plan, &mut TargetMutations::new()).await?;
    }
    // Test-only pause point: the page's sides are staged, and none of its
    // groups is locked. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if matches!(plan.writer, Writer::Aggregate) {
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::BeforeGroupLock,
            &plan.target,
        )
        .await?;
    }
    // Planted bug: lock none of the groups that exist. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::fires(
        crate::plant::Plant::GbSkipGroupLock,
        matches!(plan.writer, Writer::Aggregate),
    ) {
        txn.execute(&plan.planted_insert_missing_sql(), &[]).await?;
        return Ok(());
    }
    txn.execute(&plan.lock_sql(), &[]).await?;
    Ok(())
}

/// Whether planted bug `gbx_probe_before_lock` (`crate::plant`) fires on
/// `plan`'s page: an aggregate's with extremes.
#[cfg(any(test, feature = "test-util"))]
fn planted_probe_before_lock(plan: &GroupTargetPlan) -> bool {
    crate::plant::fires(
        crate::plant::Plant::GbxProbeBeforeLock,
        matches!(plan.writer, Writer::Aggregate) && !plan.shape.extremes().is_empty(),
    )
}

/// Step 0 of a page on a group-basis target: sums the page's sides per
/// (group, transaction, statement) into the session's staging table, before
/// any group is locked. Parsing and evaluating the images is most of a
/// page's work, and none of it reads a basis, so it stays out of the time
/// the page holds its groups.
pub(super) async fn stage_sides(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    txn.batch_execute(&plan.working_ddl()).await?;
    // A page whose `numeric` sums with a declared scale hold only values at
    // it, the common case, stages them strictly; one that holds any other
    // is staged again the general way ([`GroupTargetPlan::stage_sql`]).
    let flagged = plan.staged_flags();
    let strict = flagged.iter().any(|(_, part)| part.is_none());
    let mut row = None;
    if strict {
        txn.batch_execute("savepoint trellis_strict_stage").await?;
        let strict_row = txn.query_one(&plan.stage_sql(true), &[&plan.sides]).await?;
        let flags: Vec<bool> = strict_row
            .get::<_, Option<Vec<bool>>>(1)
            .unwrap_or_default();
        let held = flagged
            .iter()
            .zip(&flags)
            .all(|((_, part), set)| part.is_some() || *set);
        if held {
            txn.batch_execute("release savepoint trellis_strict_stage")
                .await?;
            row = Some(strict_row);
        } else {
            txn.batch_execute(
                "rollback to savepoint trellis_strict_stage; \
                 release savepoint trellis_strict_stage",
            )
            .await?;
        }
    }
    let row = match row {
        Some(row) => row,
        None => {
            txn.query_one(&plan.stage_sql(false), &[&plan.sides])
                .await?
        }
    };
    let bad: i64 = row.get(0);
    let flags: Vec<bool> = row.get::<_, Option<Vec<bool>>>(1).unwrap_or_default();
    let mut parts = StagedParts::default();
    for ((i, part), set) in plan.staged_flags().into_iter().zip(flags) {
        match (part, set) {
            (Some(part), true) => {
                parts.zero.insert((i, part));
            }
            (None, true) => {
                parts.uniform.insert(i);
            }
            (_, false) => {}
        }
    }
    plan.staged_parts.set(Some(parts));
    if bad > 0 {
        return Err(plan.unsupported(
            "a ring row without the images its change needs (an image-less delete or \
             update); the group basis applies captured images only",
        ));
    }
    Ok(())
}

/// Adds to the groups `plan` recomputes ([`GroupTargetPlan::recomputed`])
/// the group of each of `images` (source-row images, or objects of the
/// group columns alone) and the group of the current source row of each of
/// `keys`. Reads the source before any group is locked: which groups to
/// recompute is decided here, and each is counted only by the recompute's
/// own statement, after its lock.
pub(super) async fn name_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    images: &[String],
    keys: &[String],
) -> Result<(), ApplyError> {
    if images.is_empty() && keys.is_empty() {
        return Ok(());
    }
    let mut parts: Vec<Vec<Option<String>>> =
        vec![Vec::with_capacity(keys.len()); plan.source_pk.len()];
    for key in keys {
        let split = ddl::split_pk_key(&plan.source_pk, &plan.source_row, key)?;
        for (column, part) in parts.iter_mut().zip(split) {
            column.push(part.map(|p| p.into_owned()));
        }
    }
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&images];
    for column in &parts {
        params.push(column);
    }
    txn.execute(&plan.name_groups_sql(), &params).await?;
    Ok(())
}

/// Step 3 of a page on a group-basis target (see the module doc): applies
/// the page's sides to the groups [`lock_groups`] locked, in a later
/// statement, and writes their visible rows. Each changed visible row is
/// recorded into `mutations`. Returns what it applied per group, and how
/// many visible rows it wrote and deleted.
pub(crate) async fn apply_locked_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(Vec<AppliedGroup>, usize, usize), ApplyError> {
    // Planted bug: count the pages whose basis test the plant
    // changes. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::armed() == Some(crate::plant::Plant::GbLsnNotVisibility) {
        let sql = plan.planted_in_flight_sql();
        let row = match &plan.floor_key {
            Some(key) => txn.query_one(&sql, &[key]).await?,
            None => txn.query_one(&sql, &[]).await?,
        };
        crate::plant::fires(
            crate::plant::Plant::GbLsnNotVisibility,
            row.get::<_, i64>(0) > 0,
        );
    }
    if let Writer::JoinPartials { .. } = plan.writer {
        let sql = plan.apply_sql(None);
        txn.batch_execute(PLAN_SETTINGS).await?;
        match &plan.floor_key {
            Some(key) => txn.query_one(&sql, &[key]).await?,
            None => txn.query_one(&sql, &[]).await?,
        };
        txn.batch_execute(PLAN_RESET).await?;
        return Ok((Vec::new(), 0, 0));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let sql = plan.apply_sql(image_columns.as_deref());
    // One index lookup per group (`b`), not per (group, transaction): a
    // page of 500 transactions over 4,000 groups has up to 100k of those.
    // Every other join is between the page's own CTEs, whose sizes the
    // planner can't see (`unnest` of a parameter): as a nested loop it is
    // quadratic in the page, so it's planned without one. A lateral lookup
    // is a nested loop whatever the setting. Without one, the hidden rows'
    // update can't probe each row by its `ctid` from the join, so it fetches
    // them by a TID scan over the array of the page's `ctid`s instead: as a
    // join on `ctid` alone it hashed a sequential scan of the whole hidden
    // table, every group the definition has, while the page held its groups'
    // locks (twice the statement's time at 1M groups and 4,000 a page).
    // Sequential scans are off too: the statement reads the hidden table and
    // the target only by `ctid` or by their group identity index, and a
    // table whose statistics say it is small (a new definition's, before
    // `analyze` reaches it) would otherwise be scanned whole as it grows.
    // The staging table has no index, so its scans stay sequential.
    txn.batch_execute(PLAN_SETTINGS).await?;
    let rows = match &plan.floor_key {
        Some(key) => txn.query(&sql, &[key]).await?,
        None => txn.query(&sql, &[]).await?,
    };
    txn.batch_execute(PLAN_RESET).await?;
    if let Some(sentinel) = rows
        .iter()
        .find(|row| row.get::<_, Option<bool>>(1).is_none())
    {
        return Err(plan.lock_gap(sentinel.get(5)));
    }
    Ok(written_groups(plan, rows, mutations))
}

/// Pins the settings a recorded constraint text's meaning depends on
/// (`crate::capture::partitions::pinned_settings`) for the rest of the
/// transaction, or until [`restore_settings`], and returns what to restore.
/// Only a routed enumeration runs under them, and it waits for a lock as the
/// session does: under its `lock_timeout` cap, which a `set local ... to
/// default` would drop.
pub(super) async fn pin_settings(txn: &Transaction<'_>) -> Result<[String; 4], ApplyError> {
    let row = txn
        .query_one(
            "select current_setting('search_path'), current_setting('lock_timeout'), \
                    current_setting('datestyle'), current_setting('timezone')",
            &[],
        )
        .await?;
    let saved: [String; 4] = [row.get(0), row.get(1), row.get(2), row.get(3)];
    txn.batch_execute(&crate::capture::partitions::pinned_settings())
        .await?;
    txn.execute("select set_config('lock_timeout', $1, true)", &[&saved[1]])
        .await?;
    Ok(saved)
}

/// Undoes [`pin_settings`].
pub(super) async fn restore_settings(
    txn: &Transaction<'_>,
    saved: &[String; 4],
) -> Result<(), ApplyError> {
    txn.execute(
        "select set_config('search_path', $1, true), set_config('lock_timeout', $2, true), \
                set_config('datestyle', $3, true), set_config('timezone', $4, true)",
        &[&saved[0], &saved[1], &saved[2], &saved[3]],
    )
    .await?;
    Ok(())
}

/// A JOIN build's enumeration of one side's cells (`super::join::build`,
/// ADR-0002's R8): every cell the side's hidden table holds, every cell of
/// its table's rows, and the cell of every old image of the ring's changes
/// to the table ([`GroupTargetPlan::rebuild_groups_select`]), into `cells`
/// (a session temp table, quoted and `pg_temp.`-qualified, dropped at
/// commit), which becomes the plan's [`GroupTargetPlan::recomputed`]. A
/// `create … as select`, so the read of the table runs in parallel where
/// Postgres can. With a `scope` (one leaf of a partitioned side, or its
/// remainder) only the cells the scope routes to the structure.
/// Reads without a lock.
pub(super) async fn enumerate_cells(
    txn: &Transaction<'_>,
    plan: &mut GroupTargetPlan,
    scope: Option<&RebuildScope>,
    cells: &str,
) -> Result<(), ApplyError> {
    let saved = match scope {
        Some(_) => Some(pin_settings(txn).await?),
        None => None,
    };
    txn.batch_execute(&format!(
        "create temp table {} on commit drop as {}",
        cells.trim_start_matches("pg_temp."),
        plan.rebuild_groups_select(scope, &crate::pool::quote_literal(&plan.source))
    ))
    .await?;
    if let Some(saved) = saved {
        restore_settings(txn, &saved).await?;
    }
    plan.recomputed = cells.to_string();
    analyze(txn, &[cells]).await
}

/// A JOIN build's enumeration of `T`'s groups: one zero row per group of the
/// visible target and of `T`'s hidden table, into `T`'s staging table
/// ([`GroupTargetPlan::target_groups_sql`]).
pub(super) async fn enumerate_target_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
) -> Result<(), ApplyError> {
    txn.execute(&plan.target_groups_sql(), &[]).await?;
    Ok(())
}

/// `T`'s write in a JOIN build, over the groups [`GroupTargetPlan::lock_sql`]
/// locked: adds the product's deltas to the hidden partials, then replaces
/// every locked group's visible row by comparing what its partials give with
/// the row the target holds ([`GroupTargetPlan::compare_held_sql`]). Each
/// changed visible row is recorded into `mutations`, with the held row as its
/// prior image. Returns the rows it wrote and deleted.
pub(super) async fn apply_join_build_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(PLAN_SETTINGS).await?;
    let applied = txn.query_one(&plan.apply_hidden_sql(), &[]).await?;
    txn.batch_execute(PLAN_RESET).await?;
    let lost: i64 = applied.get(1);
    if lost > 0 {
        return Err(plan.lock_gap(lost));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let rows = txn
        .query(&plan.compare_held_sql(image_columns.as_deref()), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// The recompute of the groups in [`GroupTargetPlan::recomputed`], which
/// the caller has locked (R1: [`lock_groups`] for a page, [`recompute_groups`]
/// for a release): an absolute write of each group's partials from a
/// `GROUP BY` of its source rows, with the statement's snapshot as its
/// basis (see [`GroupTargetPlan::recompute_sql`]). Each changed visible row
/// is recorded into `mutations`. Returns how many visible rows it wrote and
/// deleted. A page's own sides, applied before it in the same transaction,
/// are counted again here and overwritten: their transactions are committed,
/// so the recompute's snapshot sees them, and its basis refuses them from
/// then on.
///
/// A group-basis definition has no build chunks (its build is one
/// absolute write), so the recompute counts every source row of its groups.
pub(crate) async fn recompute_locked_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    recompute_locked_groups_from(txn, plan, None, mutations).await
}

/// [`recompute_locked_groups`], reading the groups' rows from `node` when
/// it is given ([`GroupTargetPlan::recompute_sql`]).
pub(super) async fn recompute_locked_groups_from(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    node: Option<&str>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    // A `JOIN` side writes no visible row: it adds each cell's change to
    // its deltas table, as its apply does.
    if let Writer::JoinPartials { .. } = plan.writer {
        txn.query_one(&plan.recompute_sql(None, node), &[]).await?;
        return Ok((0, 0));
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let rows = txn
        .query(&plan.recompute_sql(image_columns.as_deref(), node), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// The probe of the extremes of the groups in [`GroupTargetPlan::probed`]
/// (Rx5), which the page's lock statement locked and its apply named: one
/// statement takes the snapshot, re-reads each group's extremes from the
/// source with an index probe, and writes them with the snapshot as the
/// extremes' basis ([`GroupTargetPlan::probe_sql`]). It reads the source, so
/// it takes the source's `ACCESS SHARE` lock, under the session's
/// `lock_timeout` (I7): a timeout rolls the page back, and the drain retries
/// it. Each changed visible row is recorded into `mutations`. Returns how
/// many visible rows it wrote and deleted.
pub(crate) async fn probe_locked_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let rows = txn
        .query(&plan.probe_sql(image_columns.as_deref()), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// A `JOIN` build's write of one side's cells, `plan` a
/// [`Writer::JoinPartials`] (`super::join::build`): the snapshot statement,
/// one `GROUP BY` of `node` into `grouped` ([`GroupTargetPlan::join_group_sql`]),
/// then every cell of [`GroupTargetPlan::recomputed`] matched to its hidden
/// row and to `grouped` into `cells`, and written once from there
/// ([`GroupTargetPlan::join_build_sql`]). The caller holds `J` on every key
/// the cells name and has locked the cells with a `NULL` key part, so the
/// snapshot follows every lock statement.
pub(super) async fn write_join_build_cells(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    node: &str,
    grouped: &str,
    cells: &str,
) -> Result<(), ApplyError> {
    let Writer::JoinPartials { deltas } = &plan.writer else {
        return Err(plan.unsupported("a JOIN build's cell write on a structure with no cells"));
    };
    // Strictly where a sum declares its scale, then, should a group hold a
    // value at another, the general way ([`GroupTargetPlan::fresh_select`]).
    let general = plan.join_group_sql(node, grouped, false);
    if plan
        .shape
        .args
        .iter()
        .any(|a| a.scale.is_some() && a.parts() == NUMERIC_PARTS)
    {
        strictly_or_generally(txn, &plan.join_group_sql(node, grouped, true), &general).await?;
    } else {
        txn.batch_execute(&general).await?;
    }
    analyze(txn, &[grouped]).await?;
    let [fill, write] = plan.join_build_sql(grouped, cells, deltas);
    txn.batch_execute(BULK_PLAN_SETTINGS).await?;
    // The write updates each held cell by the `ctid` the match read. A
    // cell the zero-cell collection deletes in between leaves a dead row
    // there, and once no snapshot needs that row a vacuum or a prune may
    // free its line pointer for another key's cell, which the update would
    // then overwrite. The cursor's snapshot, taken before the match and
    // kept until the write is done, keeps every row the match saw from
    // being removed, as one statement's snapshot did.
    txn.batch_execute(&format!(
        "declare {MATCH_HOLD} no scroll cursor for select; {fill}"
    ))
    .await?;
    analyze(txn, &[cells]).await?;
    // Test-only pause point between the match and the write, holding no
    // snapshot of its own, as production holds none but the cursor's
    // between the two statements. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_between_transactions(
        txn,
        super::interleave::PausePoint::AfterJoinCellMatch,
        &plan.target,
    )
    .await?;
    txn.query_one(&write, &[]).await?;
    txn.batch_execute(&format!("close {MATCH_HOLD}; {BULK_PLAN_RESET}"))
        .await?;
    Ok(())
}

/// The cursor [`write_join_build_cells`] holds open from before its match
/// statement until its write is done, for its snapshot alone.
const MATCH_HOLD: &str = "trellis_join_match_hold";

/// Refreshes the planner's statistics of `tables` (session temp tables,
/// quoted and qualified) after a bulk statement filled them. Postgres never
/// analyzes a temp table on its own, so without this a build's statements
/// are planned for a few hundred rows where there are millions: nested
/// loops and sorts where a hash join is right.
pub(super) async fn analyze(txn: &Transaction<'_>, tables: &[&str]) -> Result<(), ApplyError> {
    if tables.is_empty() {
        return Ok(());
    }
    txn.batch_execute(&format!("analyze {}", tables.join(", ")))
        .await?;
    Ok(())
}

/// A release's recompute on a group-basis target: every
/// group the held key could owe (R8), each named by an object of its group
/// columns in `groups` (the held row's `agg_groups`: the groups of the old
/// and new images of every change skipped while the key was held, the
/// evicting change's among them), plus the group of the key's current row.
/// Locks them in one sorted statement (R1, R6), then recomputes them in
/// the next. The caller holds the version fence of the key's source, so no
/// page of it runs meanwhile. Returns how many visible rows it wrote and
/// deleted.
pub(crate) async fn recompute_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    groups: &[String],
    key: &str,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&plan.working_ddl()).await?;
    name_groups(txn, plan, groups, &[key.to_string()]).await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    recompute_locked_groups(txn, plan, mutations).await
}

/// [`recompute_groups`] over `groups` alone, reading their rows from `node`
/// when it is given.
async fn recompute_named_groups(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    groups: &[String],
    node: Option<&str>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
        .await?;
    name_groups(txn, plan, groups, &[]).await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    recompute_locked_groups_from(txn, plan, node, mutations).await
}

/// The plan of `definition`'s group-basis target, read in `txn`, if the
/// group basis takes it.
async fn plan_of(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
) -> Result<Option<GroupTargetPlan>, ApplyError> {
    let Some(mut shape) = route(&definition.def, &definition.source_columns) else {
        return Ok(None);
    };
    shape.declare_scales(txn, &definition.source_table).await?;
    let source_pk = ddl::source_primary_key_in_txn(txn, &definition.source_table).await?;
    let identity = ddl::identity_key_columns(txn, &definition.target_table).await?;
    Ok(Some(GroupTargetPlan::new(
        catalog_schema,
        definition.id,
        &definition.target_table,
        &definition.source_table,
        source_pk,
        identity,
        shape,
    )))
}

/// A partition correction's recompute on a group-basis target
/// (`super::partition_correction`): every group of `groups` (objects of the
/// group columns: the groups that route to the node, `super::partition_correction`
/// enumerates them) locked in one sorted statement and recomputed in the
/// next from a `GROUP BY` of its source rows, with the statement's snapshot
/// as its basis, then the seam's rows for every visible row that changed. A
/// change the node's operation removed that is still in the ring has a
/// transaction the basis sees, so it is refused when its page applies.
/// The rows are read through the root, one index probe per group, or, when
/// `node` (quoted and qualified) is given, by one `GROUP BY` of that
/// partition ([`GroupTargetPlan::recompute_sql`]'s condition is the
/// caller's). Returns whether the definition is on the group basis and,
/// when it is, the visible rows written and deleted.
pub(crate) async fn recompute_routed(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    groups: &[String],
    node: Option<&str>,
) -> Result<Option<(usize, usize)>, ApplyError> {
    let Some(plan) = plan_of(txn, catalog_schema, definition).await? else {
        return Ok(None);
    };
    let mut mutations = TargetMutations::new();
    let counts = recompute_named_groups(txn, &plan, groups, node, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(Some(counts))
}

/// A release's work on `definition` ([`super::quarantine::release_key`]),
/// when the group basis takes it: [`recompute_groups`] over the held row's
/// `agg_groups` and `key`'s current row, then the seam's rows for every
/// visible row that changed. Returns whether the definition is on the group
/// basis; when it isn't, does nothing.
pub(crate) async fn release(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    groups: &[String],
    key: &str,
) -> Result<bool, ApplyError> {
    let Some(plan) = plan_of(txn, catalog_schema, definition).await? else {
        return Ok(false);
    };
    let mut mutations = TargetMutations::new();
    recompute_groups(txn, &plan, groups, key, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(true)
}

/// One structure of a partitioned source's rebuild: a leaf of the
/// recorded tree, or the remainder, the groups that route to no leaf. Every
/// group key holds every partition-key column (ADR-0017), so a group routes
/// to exactly one leaf and the structures cover disjoint sets of groups. The
/// predicates are SQL booleans over a row aliased `h` (a hidden row), `t` (a
/// visible row) or `s` (a source-row image), each true for the groups the
/// structure covers; they must run under the pinned settings
/// (`crate::capture::partitions::pinned_settings`).
#[derive(Debug, Clone)]
pub(crate) struct RebuildScope {
    pub hidden: String,
    pub target: String,
    pub images: String,
    /// The leaf (quoted and qualified), if the structure is one, whose own
    /// rows' groups the enumeration adds. The caller holds it `ACCESS SHARE`.
    pub node: Option<String>,
    /// Whether the replace reads the leaf by one `GROUP BY`: the caller has
    /// established that the tree is as recorded, so the leaf holds every row
    /// of every group the scope covers and no row of any other. When it
    /// isn't, the replace reads each group's rows through the root.
    pub read_node: bool,
}

/// An absolute write's snapshot, its basis, and the newest segment there is,
/// its stamp (R5), read in one statement.
const SNAPSHOT_AND_STAMP: &str = "select pg_catalog.pg_current_snapshot() as __snap, \
     coalesce((select max(seg_seq) from segments), 0) as __seg";

/// Where a rebuild's replace reads the groups' rows from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RebuildRead {
    /// One `GROUP BY` of the whole source.
    Source,
    /// One `GROUP BY` of a partition (quoted and qualified).
    Node(String),
    /// One index probe of the source per group.
    Probe,
    /// Planted bug `gb_build_snapshot_before_locks` (`crate::plant`): the
    /// `GROUP BY` of the whole source and its snapshot, read into this
    /// temporary table before the groups were locked
    /// ([`GroupTargetPlan::planted_early_read_sql`]).
    #[cfg(any(test, feature = "test-util"))]
    Early(String),
}

/// A build or rebuild of a group-basis target (ADR-0002, "An aggregate's
/// build is one absolute write per structure"), in the
/// caller's `READ COMMITTED` transaction, which
/// commits it: one absolute write of every group, in three statements.
///
/// 1. **Enumerate** ([`GroupTargetPlan::rebuild_groups_sql`], R8): the
///    target's own groups, the source's, and those named by the old images
///    of the ring's changes to the source.
/// 2. **Lock** them in key order, in a statement of their own (R6), creating
///    the hidden rows of those that have none.
/// 3. **Replace** ([`GroupTargetPlan::rebuild_sql`]): one statement takes the
///    snapshot, runs the source's `GROUP BY`, and replaces each locked
///    group's partials and visible row, with the snapshot as its basis and
///    the newest segment as its stamp (R5). A group with no source row goes
///    to zero, and its visible row is removed (R4).
///
/// Pages keep applying. One that touches a locked group waits for the
/// commit, then reads the new basis, which refuses each change the snapshot
/// saw and lets a later one apply as a delta. A group the enumeration
/// didn't see (its first change committed after it) isn't locked, so the
/// replace leaves it to its pages, which apply each of its changes once.
/// Readers see each group's old values until the commit, then the new ones.
///
/// With a `scope`, the write covers one structure of a partitioned source's
/// rebuild (a leaf, or the groups that route to no leaf): the enumeration
/// names only the groups that route to it, and the replace reads the leaf's
/// rows by one `GROUP BY` of it, or each group's through the root.
///
/// Each changed visible row is recorded into `mutations`. Returns how many
/// visible rows it wrote and deleted.
pub(crate) async fn rebuild(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    scope: Option<&RebuildScope>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
        .await?;
    let session = if scope.is_some() {
        Some(pin_settings(txn).await?)
    } else {
        None
    };
    // Planted bug: read the source and take the basis before the
    // groups are locked. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let early = if crate::plant::fires(
        crate::plant::Plant::GbBuildSnapshotBeforeLocks,
        scope.is_none() && matches!(plan.writer, Writer::Aggregate),
    ) {
        let early = "pg_temp.trellis_planted_early_read".to_string();
        txn.batch_execute(&format!(
            "drop table if exists {early}; {}",
            plan.planted_early_read_sql(&early)
        ))
        .await?;
        Some(early)
    } else {
        None
    };
    txn.execute(&plan.rebuild_groups_sql(scope), &[&plan.source])
        .await?;
    if let Some(saved) = session {
        restore_settings(txn, &saved).await?;
    }
    // Test-only pause point: the groups are enumerated, and none is locked.
    // See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRebuildEnumerate,
        &plan.target,
    )
    .await?;
    txn.execute(&plan.lock_sql(), &[]).await?;
    // Test-only pause point: the groups are locked, and the replace's
    // snapshot is not yet taken. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterRebuildLock,
        &plan.target,
    )
    .await?;
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    let read = match scope {
        None => RebuildRead::Source,
        Some(RebuildScope {
            node: Some(node),
            read_node: true,
            ..
        }) => RebuildRead::Node(node.clone()),
        Some(_) => RebuildRead::Probe,
    };
    #[cfg(any(test, feature = "test-util"))]
    let read = early.map_or(read, RebuildRead::Early);
    let rows = txn
        .query(&plan.rebuild_sql(image_columns.as_deref(), &read), &[])
        .await?;
    let (_, written, deleted) = written_groups(plan, rows, mutations);
    Ok((written, deleted))
}

/// [`rebuild`] of `definition`'s target in `txn`, then the seam's rows for
/// every visible row that changed. Returns `None` when the group basis
/// doesn't take `definition`, and does nothing then; otherwise the visible
/// rows written and deleted.
pub(crate) async fn rebuild_definition(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &crate::defs::model::Definition,
    scope: Option<&RebuildScope>,
) -> Result<Option<(usize, usize)>, ApplyError> {
    let Some(mut shape) = route(&definition.def, &definition.source_columns) else {
        return Ok(None);
    };
    shape.declare_scales(txn, &definition.source_table).await?;
    let source_pk = ddl::source_primary_key_in_txn(txn, &definition.source_table).await?;
    let identity = ddl::identity_key_columns(txn, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        catalog_schema,
        definition.id,
        &definition.target_table,
        &definition.source_table,
        source_pk,
        identity,
        shape,
    );
    let mut mutations = TargetMutations::new();
    let counts = rebuild(txn, &plan, scope, &mut mutations).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(Some(counts))
}

/// What one statement wrote per group, read from the rows
/// [`GroupTargetPlan::visible_tail_sql`] returns, with each changed visible
/// row recorded into `mutations`; and how many visible rows it wrote and
/// deleted.
fn written_groups(
    plan: &GroupTargetPlan,
    rows: Vec<tokio_postgres::Row>,
    mutations: &mut TargetMutations,
) -> (Vec<AppliedGroup>, usize, usize) {
    let mut applied = Vec::with_capacity(rows.len());
    let (mut written, mut deleted) = (0, 0);
    for row in rows {
        let key: String = row.get(0);
        let changed: bool = row.get(1);
        let visible: bool = row.get(2);
        if changed {
            if visible {
                written += 1;
            } else {
                deleted += 1;
            }
            mutations.record(
                &plan.target,
                key.clone(),
                row.get(4),
                plan.hop_gen,
                plan.src_changed,
                plan.origin_lsn,
            );
        }
        let mut column = 5;
        let n: i64 = row.get(column);
        column += 1;
        let mut counts = Vec::with_capacity(plan.shape.args.len());
        let mut sums = Vec::with_capacity(plan.shape.args.len());
        for arg in &plan.shape.args {
            counts.push(row.get(column));
            column += 1;
            if arg.summed {
                sums.push(row.get(column));
                column += 1;
            } else {
                sums.push(None);
            }
        }
        let probe = !plan.shape.extremes().is_empty() && row.get::<_, bool>(column);
        applied.push(AppliedGroup {
            key,
            n,
            counts,
            sums,
            probe,
        });
    }
    (applied, written, deleted)
}

/// One page's work on one group-basis target: [`lock_groups`], the
/// interleaving tests' pause point, [`apply_locked_groups`], then
/// [`recompute_locked_groups`] when an image-less change named groups.
/// Returns the visible rows written and deleted.
#[tracing::instrument(
    name = "staging.apply_group_target",
    skip_all,
    fields(target = %plan.target, sides = plan.sides.len())
)]
pub(crate) async fn apply_group_target(
    txn: &Transaction<'_>,
    plan: &GroupTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    lock_groups(txn, plan).await?;
    // Test-only pause point: between the lock and the read of the
    // bases. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterEntryLock,
        &plan.target,
    )
    .await?;
    let (applied, mut written, mut deleted) = apply_locked_groups(txn, plan, mutations).await?;
    let probes = applied.iter().any(|group| group.probe);
    // Planted bug `gbx_probe_before_lock`: probed before the lock instead.
    #[cfg(any(test, feature = "test-util"))]
    let probes = probes && crate::plant::armed() != Some(crate::plant::Plant::GbxProbeBeforeLock);
    // A retraction reached a group's extreme (Rx4): re-read it.
    if probes {
        // Test-only pause point: the page's sides are applied, and its
        // groups' extremes are not probed yet. See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::BeforeExtremeProbe,
            &plan.target,
        )
        .await?;
        let (w, d) = probe_locked_groups(txn, plan, mutations).await?;
        written += w;
        deleted += d;
    }
    // Only an image-less change names a group to recompute: its key, and
    // its prior-image hint, which rides on the same ring row.
    if !plan.recompute_keys.is_empty() {
        let (w, d) = recompute_locked_groups(txn, plan, mutations).await?;
        written += w;
        deleted += d;
    }
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &plan.target,
    )
    .await?;
    Ok((written, deleted))
}

/// A source `TRUNCATE`'s clear of a group-basis target's hidden table
/// `hidden` ([`hidden_table_ident`]; ADR-0002 "Truncate, DDL, drop"):
/// empties every group's partials, basis
/// and stamp, zero groups included, and raises the target's truncate floor
/// to `lsn`, so a change from before the truncate that still reaches a page
/// is refused. The caller clears the visible target. It runs on the
/// truncate's own page, which the drain barrier orders after every page
/// of earlier segments and before every page of later ones
/// (`super::apply::next_claimable_segments`), so no page of the source
/// holds a group meanwhile, and which passes the same claim check as any
/// page.
pub(crate) async fn truncate_groups(
    txn: &Transaction<'_>,
    hidden: &str,
    target: &str,
    lsn: PgLsn,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!("truncate {hidden}")).await?;
    super::ledger::raise_truncate_floor(txn, target, lsn).await?;
    Ok(())
}

/// The plan of [`apply_locked_groups`]' statement on the group-basis target
/// `target` (its bare name), as `explain`'s text, under the settings a page
/// runs it with and over an empty staging table. For tests of the plan's
/// shape. It locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_apply(pool: &crate::pool::Pool, target: &str) -> Result<String, ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(mut shape) = route(&definition.def, &definition.source_columns) else {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a definition the group basis doesn't take",
        });
    };
    let mut client = pool.get().await?;
    shape
        .declare_scales(&**client, &definition.source_table)
        .await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        pool.schema(),
        definition.id,
        &definition.target_table,
        &definition.source_table,
        ddl::source_primary_key_in_txn(&**client, &definition.source_table).await?,
        identity,
        shape,
    );
    let txn = client.transaction().await?;
    txn.batch_execute(&plan.staged_ddl()).await?;
    txn.batch_execute(PLAN_SETTINGS).await?;
    let rows = txn
        .query(
            &format!("explain {}", plan.apply_sql(None)),
            &[&plan.target],
        )
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// The plans of the two statements that read a whole source (or node) by
/// one `GROUP BY` and match its groups to the locked ones: the node form of
/// [`GroupTargetPlan::recompute_sql`] (a `JOIN` side's build, `RESUME` and
/// rebuild) and [`GroupTargetPlan::rebuild_sql`]'s source read (a plain
/// aggregate's), for `target`'s group-basis definition, each read from the
/// whole source. Planned with nested loops disabled, so a nested loop that
/// matches the groups shows only where the match can't be hashed or merged;
/// the lateral lookups of the hidden rows stay nested loops. It
/// locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_whole_source_reads(
    pool: &crate::pool::Pool,
    target: &str,
) -> Result<[String; 2], ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a definition the group basis doesn't take",
        });
    };
    let mut client = pool.get().await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        pool.schema(),
        definition.id,
        &definition.target_table,
        &definition.source_table,
        ddl::source_primary_key_in_txn(&**client, &definition.source_table).await?,
        identity,
        shape,
    );
    let txn = client.transaction().await?;
    txn.batch_execute(&format!(
        "{}; {}; set local enable_nestloop = off",
        plan.staged_ddl(),
        plan.recomputed_ddl()
    ))
    .await?;
    let statements = [
        plan.recompute_sql(None, Some(plan.source_row())),
        plan.rebuild_sql(None, &RebuildRead::Source),
    ];
    let mut plans = [String::new(), String::new()];
    for (out, sql) in plans.iter_mut().zip(statements) {
        let rows = txn.query(&format!("explain {sql}"), &[]).await?;
        *out = rows
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n");
    }
    txn.rollback().await?;
    Ok(plans)
}

/// The plan of [`probe_locked_groups`]' statement on the group-basis target
/// `target` (its bare name), as `explain`'s text, over an empty table of
/// probed groups and with sequential scans disabled, so a small test source
/// plans the way a large one does: which index (if any) serves each
/// extreme's read, and whether it reads an endpoint or sorts the group. For
/// tests of the probe's path. It locks and writes nothing.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_probe(pool: &crate::pool::Pool, target: &str) -> Result<String, ApplyError> {
    let Some(definition) = crate::defs::catalog::definition_by_target(pool, target).await? else {
        return Err(ApplyError::TransformNotFound {
            transform: target.to_string(),
        });
    };
    let Some(shape) = route(&definition.def, &definition.source_columns) else {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a definition the group basis doesn't take",
        });
    };
    if shape.extremes().is_empty() {
        return Err(ApplyError::GroupBasisUnsupported {
            target: definition.target_table,
            what: "a probe of a definition with no MIN or MAX",
        });
    }
    let mut client = pool.get().await?;
    let identity = ddl::identity_key_columns(&**client, &definition.target_table).await?;
    let plan = GroupTargetPlan::new(
        pool.schema(),
        definition.id,
        &definition.target_table,
        &definition.source_table,
        ddl::source_primary_key_in_txn(&**client, &definition.source_table).await?,
        identity,
        shape,
    );
    let txn = client.transaction().await?;
    txn.batch_execute(&format!(
        "{}; {}; set local enable_seqscan = off",
        plan.staged_ddl(),
        plan.probed_ddl()
    ))
    .await?;
    let rows = txn
        .query(&format!("explain {}", plan.probe_sql(None)), &[])
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> HashMap<String, ValueType> {
        [
            ("g", ValueType::Integer(IntWidth::Int4)),
            ("h", ValueType::Text),
            ("a", ValueType::Integer(IntWidth::Int4)),
            ("b", ValueType::Integer(IntWidth::Int8)),
            ("m", ValueType::Numeric),
            ("f", ValueType::Float(crate::float::FloatWidth::Float8)),
        ]
        .into_iter()
        .map(|(name, ty)| (name.to_string(), ty))
        .collect()
    }

    fn shape(text: &str) -> Option<GroupShape> {
        let def = crate::defs::parse(text).expect("parses");
        route(&def, &columns())
    }

    /// A `JOIN` side whose group column was re-collated after define: a value
    /// read from the source enters under the hidden table's collation, the
    /// source's index is probed under the source's, and the visible target's
    /// groups meet the hidden table's under the hidden table's. Where the
    /// collations agree, or a column has none, the statements are untouched.
    #[test]
    fn a_join_columns_move_to_another_collation_only_where_they_differ() {
        let icu = "\"pg_catalog\".\"und-x-icu\"".to_string();
        let posix = "\"pg_catalog\".\"POSIX\"".to_string();
        let shape = shape("TRANSFORM t FROM s GROUP BY g, h SELECT g AS g, h AS h, COUNT(*) AS n")
            .expect("admitted");
        let side = |source: &[Option<&String>],
                    hidden: &[Option<&String>],
                    target: &[Option<&String>]| {
            let owned = |c: &[Option<&String>]| c.iter().map(|c| c.cloned()).collect::<Vec<_>>();
            GroupTargetPlan::for_join(
                "public.t",
                hidden_table_ident("trellis", 7),
                "staged",
                Some(("public.s", "recomputed", Vec::new())),
                Vec::new(),
                shape.clone(),
                Writer::JoinPartials {
                    deltas: "pg_temp.\"d\"".to_string(),
                },
                None,
                String::new(),
            )
            .with_collations(GroupCollations {
                source: owned(source),
                hidden: owned(hidden),
                target: owned(target),
            })
        };
        let plain = side(&[None, None], &[None, None], &[None, None]);
        let agree = side(
            &[None, Some(&icu)],
            &[None, Some(&icu)],
            &[None, Some(&icu)],
        );
        for same in [&plain, &agree] {
            assert!(!same.group_values().join(", ").contains("collate"));
            assert!(!same.probe_rows_sql().contains("collate"));
            assert!(!same.target_group_values().contains("collate"));
            assert!(
                !same
                    .group_lookup(&same.target_ident, Population::Target, "nv", "x.ctid")
                    .contains("collate")
            );
        }
        // The integer column never takes a collation; the text one does.
        let moved = side(
            &[None, Some(&posix)],
            &[None, Some(&icu)],
            &[None, Some(&posix)],
        );
        assert_eq!(
            moved.group_values(),
            [
                "s.\"g\" as \"g\"".to_string(),
                format!("s.\"h\" collate {icu} as \"h\""),
            ]
        );
        let probe = moved.probe_rows_sql();
        assert!(
            probe.contains(&format!("s.\"h\" = rg.\"h\" collate {posix}")),
            "{probe}"
        );
        assert!(probe.contains("s.\"g\" = rg.\"g\" "), "{probe}");
        assert_eq!(
            moved.target_group_values(),
            format!("\"g\" as \"g\", \"h\" collate {icu} as \"h\"")
        );
        let lookup = moved.group_lookup(&moved.target_ident, Population::Target, "nv", "x.ctid");
        assert!(
            lookup.contains(&format!("x.\"h\" = nv.\"h\" collate {posix}")),
            "{lookup}"
        );
        // The hidden table's own lookups are under its own collation.
        let hidden = moved.group_lookup(&moved.hidden_ident, Population::Hidden, "rg", "x.ctid");
        assert!(!hidden.contains("collate"), "{hidden}");
    }

    /// The widths a definition is checked against are the widths of the
    /// tables its DDL creates, whatever mix of arguments it has: a count-only
    /// argument has no sum column, a repeated argument no second pair.
    #[test]
    fn the_checked_widths_are_the_widths_of_the_created_tables() {
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY g, h SELECT g AS g, COUNT(h) AS n, SUM(a) AS s, AVG(b) AS v",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s, AVG(a) AS v, COUNT(a) AS c",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a + 1) AS s, COUNT(b) AS c, SUM(m) AS d",
        ] {
            let shape = shape(text).expect("routes");
            let hidden =
                crate::defs::ddl::declared_column_counts(&hidden_table_ddl("trellis", 3, &shape));
            assert_eq!(
                hidden.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
                [hidden_table_width(&shape)],
                "{text}"
            );
            let target = crate::defs::ddl::declared_column_counts(&target_table_ddl("t", &shape));
            assert_eq!(
                target.iter().map(|(_, n)| *n).collect::<Vec<_>>(),
                [target_table_width(&shape)],
                "{text}"
            );
        }
        // Spelled out once: 2 group columns, the count, 2 columns for `SUM(a)`
        // and 1 for `COUNT(h)`, then basis, stamp and zero flag.
        let shape = shape("TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(h) AS n, SUM(a) AS s")
            .expect("routes");
        assert_eq!(hidden_table_width(&shape), 2 + 1 + 1 + 2 + 3);
        assert_eq!(target_table_width(&shape), 2 + 2);
    }

    #[test]
    fn takes_the_admitted_aggregates_and_their_compositions() {
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(h) AS n, SUM(a) AS s, AVG(b) AS v",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(m) AS s, AVG(m) AS v",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a + b) AS s, COUNT(f) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) + COUNT(b) AS x, \
             COALESCE(SUM(a), 0) AS y, SUM(a) > COUNT(*) AS z",
            // An aggregate whose argument is read only under `NOT`,
            // `IS NULL` or a `CASE` still gets its partial.
            "TRANSFORM t FROM s GROUP BY g SELECT NOT (SUM(a) > 0) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(b) IS NULL AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT CASE WHEN COUNT(*) > 1 THEN SUM(a) END AS x, \
             CASE WHEN COUNT(h) > 0 THEN 1 ELSE AVG(b) END AS y",
        ] {
            assert!(shape(text).is_some(), "{text}");
        }
    }

    #[test]
    fn takes_no_refused_aggregate_and_no_one_to_one() {
        for text in [
            // `MIN`/`MAX` over an expression, or a collatable column.
            "TRANSFORM t FROM s GROUP BY g SELECT MIN(a + 1) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT MAX(h) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(f) AS n",
            "TRANSFORM t FROM s GROUP BY g SELECT AVG(f) AS n",
            "TRANSFORM t FROM s SELECT a AS a",
        ] {
            assert!(shape(text).is_none(), "{text}");
        }
        // A `numeric` expression keeps a `numeric` argument's partials.
        let shape = shape("TRANSFORM t FROM s GROUP BY g SELECT SUM(m + a) AS n").expect("taken");
        assert!(shape.args[0].numeric);
    }

    /// `MIN`/`MAX` over a plain column of an ordered, uncollated type is one
    /// hidden extreme per distinct call, typed as its argument, composed with
    /// the other aggregates; the hidden table gains the extremes and their
    /// own basis and stamp, and a definition with none keeps its columns.
    #[test]
    fn takes_min_and_max_over_a_plain_column() {
        let shape = shape(
            "TRANSFORM t FROM s GROUP BY g \
             SELECT MIN(a) AS lo, MAX(a) + COUNT(*) AS hi, MIN(a) AS lo2, MAX(f) AS fx, \
                    MIN(m) AS mm, SUM(a) AS total",
        )
        .expect("admitted");
        let extremes: Vec<(bool, usize, String)> = shape
            .extremes()
            .into_iter()
            .map(|x| (x.max, x.arg, x.pg_type))
            .collect();
        assert_eq!(
            extremes,
            [
                (false, 0, "integer".to_string()),
                (true, 0, "integer".to_string()),
                (true, 1, "double precision".to_string()),
                (false, 2, "numeric".to_string()),
            ]
        );
        let columns = hidden_table_columns(&shape);
        assert_eq!(
            columns[columns.len() - 9..],
            [
                "__min_1 integer",
                "__max_1 integer",
                "__max_2 double precision",
                "__min_3 numeric",
                "__basis pg_snapshot",
                "__stamp bigint",
                "__xbasis pg_snapshot",
                "__xstamp bigint",
                "__zero boolean not null generated always as \
                 (__n = 0 and __count_1 = 0 and __sum_1 = 0 and __count_2 = 0 and __count_3 = 0) \
                 stored",
            ]
        );
        let plain = shape_of("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS total");
        assert!(
            !hidden_table_columns(&plain)
                .iter()
                .any(|c| c.starts_with("__xbasis") || c.starts_with("__min")),
            "a definition with no MIN/MAX keeps its hidden table"
        );
    }

    fn shape_of(text: &str) -> GroupShape {
        shape(text).expect("admitted")
    }

    /// Every branch of an extreme's probe, the `NULL` groups' included,
    /// orders by the `GROUP BY` columns then the argument, all in the
    /// extreme's direction: the index's order, which Postgres reads as one
    /// endpoint. Ordered by the argument alone, a branch fixing a column by
    /// `is null` sorts the whole group (Postgres drops only an `=`-fixed
    /// column from the order).
    #[test]
    fn an_extreme_probe_reads_the_index_in_its_own_order() {
        let shape = shape_of("TRANSFORM t FROM s GROUP BY g, b SELECT MIN(a) AS lo, MAX(a) AS hi");
        let plan = GroupTargetPlan::new(
            "trellis",
            7,
            "public.t",
            "public.s",
            Vec::new(),
            Vec::new(),
            shape.clone(),
        );
        for (x, order) in shape.extremes().iter().zip([
            r#"order by s."g", s."b", s."a" limit 1"#,
            r#"order by s."g" desc, s."b" desc, s."a" desc limit 1"#,
        ]) {
            let sql = plan.extreme_probe_sql(x);
            // The all-`=` branch and one per pattern of `NULL` columns.
            assert_eq!(sql.matches("order by").count(), 4, "{sql}");
            assert_eq!(sql.matches(order).count(), 4, "{sql}");
        }
    }

    /// A session keeps the temp table it created under a name, and a resume
    /// keeps the definition id while re-reading the column types: the name
    /// must change with the shape, or a connection reuses the old table.
    #[test]
    fn the_staging_table_name_follows_its_shape() {
        let text = "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s";
        let name = |cols: &HashMap<String, ValueType>| {
            let def = crate::defs::parse(text).expect("parses");
            let shape = route(&def, cols).expect("admitted");
            GroupTargetPlan::new(
                "trellis",
                7,
                "public.t",
                "public.s",
                Vec::new(),
                Vec::new(),
                shape,
            )
            .staged
        };
        let int4 = columns();
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(name(&int4), name(&columns()));
        assert_ne!(name(&int4), name(&int8));
        assert!(name(&int4).starts_with("pg_temp.\"trellis_group_sides_7_"));
        let def = crate::defs::parse(text).expect("parses");
        let shape = route(&def, &int4).expect("admitted");
        let longest = GroupTargetPlan::new(
            "trellis",
            i64::MIN,
            "public.t",
            "public.s",
            Vec::new(),
            Vec::new(),
            shape,
        )
        .staged;
        let bare = longest
            .trim_start_matches("pg_temp.\"")
            .trim_end_matches('"');
        assert!(bare.len() <= 63, "{bare} fits a Postgres identifier");
    }

    /// The recompute table is a session table too: its name follows its
    /// group columns' types, and fits an identifier for any id.
    #[test]
    fn the_recompute_table_name_follows_its_shape() {
        let text = "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s";
        let name = |id: i64, cols: &HashMap<String, ValueType>| {
            let def = crate::defs::parse(text).expect("parses");
            let shape = route(&def, cols).expect("admitted");
            GroupTargetPlan::new(
                "trellis",
                id,
                "public.t",
                "public.s",
                Vec::new(),
                Vec::new(),
                shape,
            )
            .recomputed
        };
        let int4 = columns();
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(name(7, &int4), name(7, &columns()));
        assert_ne!(name(7, &int4), name(7, &int8));
        assert!(name(7, &int4).starts_with("pg_temp.\"trellis_group_recompute_7_"));
        let longest = name(i64::MIN, &int4);
        let bare = longest
            .trim_start_matches("pg_temp.\"")
            .trim_end_matches('"');
        assert!(bare.len() <= 63, "{bare} fits a Postgres identifier");
    }

    /// One page staged and locked on `client` for definition 7 over the
    /// source columns `cols`, after recreating its hidden table for that
    /// shape: one insert of group `g`. Returns the hidden table's groups.
    async fn stage_one_page(
        client: &mut tokio_postgres::Client,
        cols: &HashMap<String, ValueType>,
        g: i64,
    ) -> Vec<i64> {
        let schema = crate::config::DEFAULT_SCHEMA;
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, cols).expect("admitted");
        let mut plan = GroupTargetPlan::new(
            schema,
            7,
            "public.t",
            "public.s",
            Vec::new(),
            Vec::new(),
            shape.clone(),
        );
        plan.sides = vec![format!(
            r#"{{"op":"insert","t":"100","l":"0/10","n":{{"id":1,"g":{g},"a":5}}}}"#
        )];
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!(
            "{}; {}",
            drop_hidden_table_sql(schema, 7),
            hidden_table_ddl(schema, 7, &shape)
        ))
        .await
        .expect("hidden table");
        if let Err(error) = lock_groups(&txn, &plan).await {
            panic!("staging group {g}: {error:?}");
        }
        let groups = txn
            .query(&format!("select g::bigint from {}", plan.hidden_ident), &[])
            .await
            .expect("hidden rows")
            .iter()
            .map(|row| row.get(0))
            .collect();
        txn.commit().await.expect("commit");
        groups
    }

    /// The same connection stages a page for one definition id under two
    /// shapes, as after a resume that re-typed the group column (the hidden
    /// table and the plan are rebuilt here by hand): the second page's group, out of the
    /// first shape's range, stages into a table of its own type.
    #[tokio::test]
    async fn a_session_stages_a_retyped_group_into_a_table_of_its_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("create table public.s (id integer primary key, g integer, a integer)")
            .await
            .expect("source");
        assert_eq!(stage_one_page(&mut client, &columns(), 1).await, [1]);
        client
            .batch_execute("alter table public.s alter column g type bigint")
            .await
            .expect("re-type the group column");
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        assert_eq!(
            stage_one_page(&mut client, &int8, 5_000_000_000).await,
            [5_000_000_000]
        );
    }

    /// Names group `g` into the recompute table on `client`, as a recompute
    /// does, for definition 7 over the source columns `cols`.
    async fn name_one_group(
        client: &mut tokio_postgres::Client,
        cols: &HashMap<String, ValueType>,
        g: i64,
    ) {
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, cols).expect("admitted");
        let plan = GroupTargetPlan::new(
            crate::config::DEFAULT_SCHEMA,
            7,
            "public.t",
            "public.s",
            vec![PrimaryKeyColumn {
                name: "id".to_string(),
                data_type: "integer".to_string(),
                nullable: false,
                collation: None,
            }],
            Vec::new(),
            shape,
        );
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!("{}; {}", plan.staged_ddl(), plan.recomputed_ddl()))
            .await
            .expect("session tables");
        if let Err(error) = name_groups(&txn, &plan, &[format!(r#"{{"g":{g}}}"#)], &[]).await {
            panic!("naming group {g}: {error:?}");
        }
        txn.commit().await.expect("commit");
    }

    /// The recompute's table follows a re-typed group column the same way:
    /// the second recompute, of a group out of the first shape's range,
    /// names it into a table of its own type.
    #[tokio::test]
    async fn a_session_recomputes_a_retyped_group_into_a_table_of_its_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("create table public.s (id integer primary key, g integer, a integer)")
            .await
            .expect("source");
        name_one_group(&mut client, &columns(), 1).await;
        client
            .batch_execute("alter table public.s alter column g type bigint")
            .await
            .expect("re-type the group column");
        let mut int8 = columns();
        int8.insert("g".to_string(), ValueType::Integer(IntWidth::Int8));
        name_one_group(&mut client, &int8, 5_000_000_000).await;
    }

    /// The `explain` text of `sql` on `txn`.
    async fn explain(txn: &Transaction<'_>, sql: &str) -> String {
        txn.query(&format!("explain {sql}"), &[])
            .await
            .expect("explain")
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// A `JOIN` build's cell write over a hidden table that already holds
    /// its cells (a rebuild, a resume) matches every set by hash joins: the
    /// cells to the hidden rows and to the `GROUP BY`, then the cells to
    /// insert by an anti-join against the update's rows, planned for the
    /// cells there are. A correlated subplan there rescans the update's rows
    /// once per cell, and a hash table sized for the match's guessed rows
    /// (Postgres has no statistics for its arrays) loops over its batches:
    /// either way a 100M-row rebuild ran for hours.
    #[tokio::test]
    async fn a_join_build_write_over_held_cells_hashes_every_match() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        let hidden = hidden_table_ident("trellis", 7);
        let deltas = "pg_temp.\"trellis_test_deltas\"".to_string();
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden.clone(),
            "trellis_test_staged",
            Some(("public.s", "trellis_test_cells", Vec::new())),
            Vec::new(),
            shape.clone(),
            Writer::JoinPartials {
                deltas: deltas.clone(),
            },
            None,
            String::new(),
        );
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.segments (seg_seq bigint); \
                 create table public.s (id integer primary key, g integer, b bigint, a integer); \
                 insert into public.s select i, i, i % 100, i from generate_series(1, 20000) i; \
                 insert into {hidden} (g, __n, __count_1, __sum_1) \
                     select g, 1, 1, 1 from public.s; \
                 analyze public.s, {hidden}",
                hidden_table_ddl("trellis", 7, &shape)
            ))
            .await
            .expect("source and held cells");
        let grouped = "pg_temp.\"trellis_test_grouped\"";
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&format!(
            "{}; {}; insert into {} (g) select g from {hidden}",
            plan.working_ddl(),
            plan.deltas_ddl().expect("a side's deltas"),
            plan.recomputed,
        ))
        .await
        .expect("working tables");
        txn.batch_execute(&plan.join_group_sql("public.s", grouped, false))
            .await
            .expect("group");
        analyze(&txn, &[&plan.recomputed, grouped])
            .await
            .expect("analyze");
        txn.batch_execute(BULK_PLAN_SETTINGS)
            .await
            .expect("settings");
        let matched = "pg_temp.\"trellis_test_matched\"";
        let [fill, write] = plan.join_build_sql(grouped, matched, &deltas);
        let fill_plan = explain(&txn, &fill).await;
        assert!(!fill_plan.contains("Nested Loop"), "{fill_plan}");
        txn.batch_execute(&fill).await.expect("fill");
        analyze(&txn, &[matched]).await.expect("analyze");
        let write_plan = explain(&txn, &write).await;
        assert!(!write_plan.contains("SubPlan"), "{write_plan}");
        let anti = write_plan
            .lines()
            .find(|line| line.contains("Hash") && line.contains("Anti Join"))
            .unwrap_or_else(|| panic!("no hash anti-join: {write_plan}"));
        let rows: f64 = anti
            .split("rows=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|rows| rows.parse().ok())
            .expect("estimated rows");
        assert!(rows <= 40_000.0, "planned for 20000 cells: {write_plan}");
    }

    /// ADR-0019's strict `T` write: a `JOIN`'s `T` page or build step whose
    /// staged group has no hidden row (a writer that skipped
    /// [`GroupTargetPlan::lock_sql`], which inserts the missing rows) fails
    /// with [`ApplyError::GroupLockGap`], and rolls nothing in; the same
    /// writer after the lock applies.
    #[tokio::test]
    async fn a_join_t_write_of_a_group_with_no_hidden_row_is_an_error() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(crate::defs::recorded_keys::TEST_TABLE_DDL)
            .await
            .expect("definition_keys");
        let def =
            crate::defs::parse("TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s").expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        let hidden = hidden_table_ident("trellis", 7);
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.t (g integer primary key, s bigint)",
                hidden_table_ddl("trellis", 7, &shape)
            ))
            .await
            .expect("tables");
        let identity = ddl::identity_key_columns(&client, "public.t")
            .await
            .expect("identity");
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden.clone(),
            "trellis_test_t_staged",
            None,
            identity,
            shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        // Group 1 gains a row; group 2 nets to zero, and is staged too.
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5), \
                 (2, pg_current_xact_id(), '0/10', 0, 0, 0)",
            plan.staged
        ))
        .await
        .expect("stage");
        let is_gap = |err: ApplyError, groups: i64| {
            assert!(
                matches!(&err, ApplyError::GroupLockGap { target, groups: n }
                    if target == "public.t" && *n == groups),
                "{err:?}"
            );
        };
        // No lock: both groups are missing.
        let mut mutations = TargetMutations::assuming_read();
        let err = apply_locked_groups(&txn, &plan, &mut mutations)
            .await
            .expect_err("a page over groups with no hidden row");
        is_gap(err, 2);
        txn.rollback().await.expect("rollback");

        // The build step's statement reads the same.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5)",
            plan.staged
        ))
        .await
        .expect("stage");
        let err = apply_join_build_groups(&txn, &plan, &mut TargetMutations::assuming_read())
            .await
            .expect_err("a build step over a group with no hidden row");
        is_gap(err, 1);
        txn.rollback().await.expect("rollback");

        // Locked first, as every writer is: applies.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&format!(
            "insert into {} (g, __txid, __lsn, __n, __c1, __s1) values \
                 (1, pg_current_xact_id(), '0/10', 1, 1, 5), \
                 (2, pg_current_xact_id(), '0/10', 0, 0, 0)",
            plan.staged
        ))
        .await
        .expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        let (_, written, deleted) =
            apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
                .await
                .expect("a locked page applies");
        assert_eq!((written, deleted), (1, 0));
        let visible: Vec<(i32, i64)> = txn
            .query("select g, s from public.t order by g", &[])
            .await
            .expect("target")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(visible, vec![(1, 5)]);
        txn.rollback().await.expect("rollback");
    }

    /// [`a_join_t_write_of_a_group_with_no_hidden_row_is_an_error`] over a
    /// composite text-keyed group and a `COUNT(*)`, `COUNT(h)`, `AVG(b)`
    /// shape (a counted-only argument next to a summed one), with one staged
    /// group locked and one not: the page's statement returns a written
    /// group's row and the sentinel together, so their columns must agree,
    /// and the page still fails, counting only the unlocked group.
    #[tokio::test]
    async fn a_join_t_write_with_one_unlocked_group_fails_for_any_shape() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(crate::defs::recorded_keys::TEST_TABLE_DDL)
            .await
            .expect("definition_keys");
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(*) AS n, COUNT(h) AS c, AVG(b) AS v",
        )
        .expect("parses");
        let shape = route(&def, &columns()).expect("admitted");
        assert!(
            shape.args.iter().any(|arg| arg.summed) && shape.args.iter().any(|arg| !arg.summed),
            "a counted-only and a summed argument"
        );
        let hidden = hidden_table_ident("trellis", 8);
        client
            .batch_execute(&format!(
                "create schema if not exists trellis; {}; \
                 create table public.t (g integer, h text, n bigint, c bigint, v numeric, \
                                        primary key (g, h))",
                hidden_table_ddl("trellis", 8, &shape)
            ))
            .await
            .expect("tables");
        let identity = ddl::identity_key_columns(&client, "public.t")
            .await
            .expect("identity");
        let plan = GroupTargetPlan::for_join(
            "public.t",
            hidden,
            "trellis_test_t_staged_shape",
            None,
            identity,
            shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        // One row per group: `__n` 1, and per argument a count of 1 and a
        // sum of 4 where summed.
        let mut partials = vec!["__n".to_string()];
        let mut ones = vec!["1".to_string()];
        for (i, arg) in plan.shape.args.iter().enumerate() {
            partials.push(format!("__c{}", i + 1));
            ones.push("1".to_string());
            if arg.summed {
                partials.push(format!("__s{}", i + 1));
                ones.push("4".to_string());
            }
        }
        let stage = |g: i32, h: &str| {
            format!(
                "insert into {} (g, h, __txid, __lsn, {}) values \
                     ({g}, '{h}', pg_current_xact_id(), '0/10', {})",
                plan.staged,
                partials.join(", "),
                ones.join(", ")
            )
        };
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&stage(1, "x")).await.expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        // Staged after the lock: no hidden row.
        txn.batch_execute(&stage(2, "y")).await.expect("stage");
        let err = apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
            .await
            .expect_err("a page with an unlocked group");
        assert!(
            matches!(&err, ApplyError::GroupLockGap { target, groups: 1 } if target == "public.t"),
            "{err:?}"
        );
        txn.rollback().await.expect("rollback");

        // Both locked: the page applies, and writes both groups.
        let txn = client.transaction().await.expect("begin");
        txn.batch_execute(&plan.staged_ddl()).await.expect("staged");
        txn.batch_execute(&stage(1, "x")).await.expect("stage");
        txn.batch_execute(&stage(2, "y")).await.expect("stage");
        txn.batch_execute(&plan.lock_sql()).await.expect("lock");
        let (applied, written, deleted) =
            apply_locked_groups(&txn, &plan, &mut TargetMutations::assuming_read())
                .await
                .expect("a locked page applies");
        assert_eq!((applied.len(), written, deleted), (2, 2, 0));
        txn.rollback().await.expect("rollback");
    }

    #[test]
    fn shares_partials_between_calls_on_one_argument() {
        let shape = shape(
            "TRANSFORM t FROM s GROUP BY g \
             SELECT SUM(a) AS s, AVG(a) AS v, COUNT(a) AS c, COUNT(b) AS d, COUNT(*) AS n",
        )
        .expect("taken");
        assert_eq!(
            shape.args,
            vec![
                Arg {
                    sql: "s.\"a\"".to_string(),
                    expr: Expr::Column("a".to_string()),
                    summed: true,
                    numeric: false,
                    scale: None,
                },
                Arg {
                    sql: "s.\"b\"".to_string(),
                    expr: Expr::Column("b".to_string()),
                    summed: false,
                    numeric: false,
                    scale: None,
                },
            ]
        );
        assert_eq!(
            shape.aggs,
            vec![
                Agg::Sum(0, "bigint"),
                Agg::Avg(0),
                Agg::Count(0),
                Agg::Count(1),
                Agg::CountStar
            ]
        );
    }

    #[test]
    fn hidden_and_visible_tables_hold_what_the_module_doc_says() {
        let shape = shape("TRANSFORM t FROM s GROUP BY g SELECT SUM(m) AS s, COUNT(h) AS c")
            .expect("taken");
        assert!(shape.args[0].numeric && !shape.args[1].numeric);
        assert_eq!(
            hidden_table_ddl("trellis", 7, &shape),
            "create table \"trellis\".\"agg_7\" (\"g\" integer, __n bigint not null default 0, \
             __count_1 bigint not null default 0, __sum_1 numeric not null default 0, \
             __scales_1 jsonb not null default '{}', __nan_1 bigint not null default 0, \
             __pinf_1 bigint not null default 0, __ninf_1 bigint not null default 0, \
             __count_2 bigint not null default 0, __basis pg_snapshot, __stamp bigint, \
             __zero boolean not null generated always as \
             (__n = 0 and __count_1 = 0 and __sum_1 = 0 and __scales_1 = '{}'::jsonb \
             and __nan_1 = 0 and __pinf_1 = 0 and __ninf_1 = 0 and __count_2 = 0) stored, \
             unique nulls not distinct (\"g\")); \
             create index on \"trellis\".\"agg_7\" (\"g\") where __zero"
        );
        assert_eq!(
            target_table_ddl("\"public\".\"t\"", &shape),
            "create table \"public\".\"t\" (\"g\" integer, \"s\" numeric, \"c\" bigint, \
             unique nulls not distinct (\"g\"))"
        );
        assert_eq!(
            drop_hidden_table_sql("trellis", 7),
            "drop table if exists \"trellis\".\"agg_7\""
        );
    }

    /// Postgres's `NUMERIC_TYPMOD_SCALE`, a negative scale included.
    #[test]
    fn a_numeric_type_modifier_declares_its_scale() {
        // `numeric(10,2)`, `numeric(5,0)`, `numeric(5,-2)`, `numeric(1000,1000)`.
        for (typmod, scale) in [
            ((10 << 16) + 2 + 4, 2),
            ((5 << 16) + 4, 0),
            ((5 << 16) + (-2_i32 & 0x7ff) + 4, -2),
            ((1000 << 16) + 1000 + 4, 1000),
        ] {
            assert_eq!(numeric_scale(typmod), scale, "{typmod}");
        }
    }

    /// A declared scale is only a hint ([`Arg::scale`]): every statement it
    /// specialises stays exact for maps and rows at another scale, which a
    /// change of the column's type can leave behind. Each specialised
    /// expression is evaluated beside the general one, over maps and rows
    /// that count only the declared scale, none, or others too; and
    /// [`declared_scales`] reads the scale a column declares.
    #[tokio::test]
    async fn a_declared_scale_is_exact_for_any_scales() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let text = |sql: String| {
            let client = &client;
            async move {
                client
                    .query_one(&format!("select ({sql})::text"), &[])
                    .await
                    .unwrap_or_else(|e| panic!("{sql}: {e}"))
                    .get::<_, Option<String>>(0)
            }
        };
        let maps = [
            "'{}'::jsonb",
            "'{\"2\": 3}'::jsonb",
            "'{\"2\": -3}'::jsonb",
            "'{\"5\": 1}'::jsonb",
            "'{\"2\": 1, \"5\": 2}'::jsonb",
        ];
        let part = SumPart::Scales;
        for a in maps {
            for b in maps {
                for (special, general) in [
                    (part.add(a, b, Some(2)), part.add(a, b, None)),
                    (part.sub(a, b, Some(2)), part.sub(a, b, None)),
                ] {
                    assert_eq!(text(special).await, text(general).await, "{a}, {b}");
                }
            }
            for n in ["0", "1", "-2"] {
                assert_eq!(
                    text(part.times(a, n, Some(2))).await,
                    text(part.times(a, n, None)).await,
                    "{a} * {n}"
                );
            }
            let value = |scale| {
                sum_value(true, scale, None, &|p| match p {
                    SumPart::Sum => "1.123456".to_string(),
                    SumPart::Scales => a.to_string(),
                    _ => "0".to_string(),
                })
            };
            assert_eq!(text(value(Some(2))).await, text(value(None)).await, "{a}");
        }
        // Totals over maps, with and without a condition.
        for rows in [
            "values ('{}'::jsonb, true)",
            "values ('{\"2\": 1}'::jsonb, true), ('{\"2\": 2}', false), ('{\"2\": -1}', true)",
            "values ('{\"2\": 1}'::jsonb, true), ('{\"5\": 2}', true), ('{\"2\": 1, \"3\": 1}', false)",
        ] {
            for condition in [None, Some("c")] {
                let total = |scale| {
                    format!(
                        "select {} from ({rows}) as v(m, c)",
                        part.total("m", condition, scale)
                    )
                };
                assert_eq!(
                    text(total(Some(2))).await,
                    text(total(None)).await,
                    "{rows} {condition:?}"
                );
            }
        }
        // Counts over rows, weighted and not.
        for rows in [
            "values (1.25::numeric, 1), (2.50, -1), (3.75, 1)",
            "values (1.25::numeric, 1), (2.5, 1), (1.12345, -1), ('NaN', 1), (null, 1)",
            "values (null::numeric, 1)",
        ] {
            for w in ["1", "w"] {
                let count = |scale| {
                    format!(
                        "select {} from ({rows}) as v(x, w)",
                        part.of_rows("x", true, w, scale)
                    )
                };
                assert_eq!(
                    text(count(Some(2))).await,
                    text(count(None)).await,
                    "{rows} {w}"
                );
            }
        }
        // A derived apply's map and prior display scale, against the general
        // map arithmetic, for every map and change.
        for a in maps {
            for change in ["0", "1", "-1", "3"] {
                let general = part.add(a, &single_scale(2, change), None);
                assert_eq!(
                    text(derived_add(a, change, 2)).await,
                    text(general.clone()).await,
                    "{a} + {change}"
                );
                // The prior map is `a` less `change`; its display scale
                // matters only while it counts a finite value, and only a
                // map of positive counts is a group's (a transient negative
                // count leaves no visible row).
                let prior = part.sub(a, &single_scale(2, change), None);
                let shown = format!(
                    "{prior} <> {NO_SCALES} and \
                     (select bool_and(value::bigint > 0) from jsonb_each_text({prior}))"
                );
                if text(shown).await.as_deref() == Some("true") {
                    assert_eq!(
                        text(derived_prior_scale(a, change, 2)).await,
                        text(max_scale(&prior)).await,
                        "{a} - {change}"
                    );
                }
            }
        }
        // Mutation check: these inputs tell a wrong specialisation apart. A
        // derived add that rebuilt the declared scale's count without asking
        // whether the map counts another would drop the other scale.
        let mutated = |a: &str| single_scale(2, &format!("{} + 1", scale_count(a, 2)));
        let mut caught = false;
        for a in maps {
            caught |= text(mutated(a)).await != text(derived_add(a, "1", 2)).await;
        }
        assert!(caught, "no map tells the mutated derived add apart");
        // A strict read counts a map's declared scale, and refuses a map
        // that counts another with an error the caller recognises. The map
        // is a column, as in the statements, so the refusal is no constant
        // Postgres would fold while planning.
        for a in maps {
            let read = client
                .query_one(
                    &format!(
                        "select ({})::text from (select {a} as m) as v",
                        strict_count("v.m", 2)
                    ),
                    &[],
                )
                .await;
            let only = text(only_scale(a, 2)).await.as_deref() == Some("true");
            match read {
                Ok(row) => {
                    assert!(only, "{a} read strictly");
                    assert_eq!(
                        row.get::<_, Option<String>>(0),
                        text(scale_count(a, 2)).await,
                        "{a}"
                    );
                }
                Err(e) => assert!(!only && is_off_scale(&e), "{a}: {e}"),
            }
        }
        // A strict stage marks the rows it can't stage strictly, and a
        // strict build refuses them; over rows it can, each counts what the
        // general count does.
        for rows in [
            "values (1.25::numeric, 1), (2.50, -1), (null, 1)",
            "values (1.25::numeric, 1), (2.5, 1)",
            "values (1.25::numeric, 1), ('NaN', 1)",
            "values (null::numeric, 1)",
        ] {
            let staged = text(format!(
                "select {} from ({rows}) as v(x, w)",
                strict_stage_scales("x", 2)
            ))
            .await;
            let general = text(format!(
                "select {} from ({rows}) as v(x, w)",
                part.of_rows("x", true, "1", None)
            ))
            .await;
            let finite = text(format!(
                "select {} from ({rows}) as v(x, w)",
                single_scale(2, "count(*) filter (where scale(x) is not null)")
            ))
            .await;
            match staged.as_deref() {
                Some("{}") => assert_eq!(general, finite, "{rows}: staged strictly"),
                _ => assert_eq!(
                    text(format!(
                        "select bool_or(x is not null and scale(x) is distinct from 2) \
                         from ({rows}) as v(x, w)"
                    ))
                    .await
                    .as_deref(),
                    Some("true"),
                    "{rows}: staged the general way"
                ),
            }
            let built = client
                .query_one(
                    &format!(
                        "select ({})::text from ({rows}) as v(x, w)",
                        strict_build_scales("x", 2)
                    ),
                    &[],
                )
                .await;
            match built {
                Ok(row) => assert_eq!(row.get::<_, Option<String>>(0), general, "{rows}"),
                Err(e) => assert!(is_off_scale(&e), "{rows}: {e}"),
            }
        }
        client
            .batch_execute(
                "create table public.s (a numeric(10,2), b numeric, c numeric(5,-2), d integer)",
            )
            .await
            .expect("source");
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY d SELECT SUM(a) AS a, SUM(b) AS b, SUM(c) AS c, \
             SUM(a + 1) AS e, COUNT(a) AS n",
        )
        .expect("parses");
        let mut cols = HashMap::new();
        for c in ["a", "b", "c"] {
            cols.insert(c.to_string(), ValueType::Numeric);
        }
        cols.insert("d".to_string(), ValueType::Integer(IntWidth::Int4));
        let mut shape = route(&def, &cols).expect("admitted");
        shape
            .declare_scales(&client, "public.s")
            .await
            .expect("declared scales");
        let scales: Vec<Option<i32>> = shape.args.iter().map(|arg| arg.scale).collect();
        assert_eq!(scales, [Some(2), None, Some(0), None]);
    }
}
