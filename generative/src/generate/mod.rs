//! Program generation (design doc §1/§3): generators producing only valid
//! [`crate::model::Program`]s. Makes no engine calls — the module boundary
//! this crate enforces is that only [`crate::backend`] drives the engine's
//! pipeline.
//!
//! Today's scope is the *trivial* generator (issue #6): every source table is
//! shaped `(numeric pk, numeric c1, numeric c2)` and every definition is a
//! 1-1 numeric-`+`, each with a seed-before-mutate op stream. Improvement-plan
//! task **B3** (`local_docs/generative-suite-improvement-plan.md`) widened
//! this from *exactly* one table/one definition to **1-3 tables and 1-3
//! definitions**, each definition independently drawing one of the tables as
//! its source — see [`build_program_multi`] and [`TableSpec`]. The proptest
//! [`Strategy`]s that draw these live behind the `proptest` feature (see
//! `Cargo.toml`); the pure builders they map onto ([`build_program`],
//! [`build_program_multi`]) are always available so a hand-built pin can
//! reuse the exact same shape without pulling in proptest.
//!
//! `build_program` is kept as a single-table/single-def convenience
//! wrapper around [`build_program_multi`] (rather than changing its
//! signature) because every existing hand-built pin/test across
//! `generative/tests/*.rs` and this module already calls it with the old
//! two-argument shape; delegating keeps every one of those call sites
//! unchanged while sharing the exact same op-construction and pk-liveness
//! logic multi-table programs use.
//!
//! Improvement-plan task **B1** ("Multi-type schemas and the remaining
//! awkward values") widens each table further: every table now also gets
//! exactly one column each of [`ValueType::Text`], [`ValueType::Boolean`],
//! and [`ValueType::Uuid`] — unconditionally, not a probabilistically-drawn
//! dimension stacked on top of B3's table/def-count widening (mirroring
//! `crate::model::Table::new`'s existing "every table gets its pk column
//! unconditionally" philosophy) — and every definition sourced from that
//! table gets a matching identity-passthrough field for each (`SELECT <col>
//! AS <col>`, the same bare-`Expr::Column` shape
//! `engine/tests/defs_backfill_direct.rs` already exercises by hand, and the
//! same narrowing `engine::defs::ddl::passthrough_source_column` already
//! special-cases). See [`TableSpec`]'s `text_values`/`bool_values`/
//! `uuid_values` fields and the `text_value`/`bool_value`/`uuid_value`
//! strategies below.
//!
//! Improvement-plan task **B4** ("Aggregates") widens the generator past
//! `KeySpace::OneToOne` for the first time: every table now *also*
//! gets a "grain" column — one more [`ValueType::Numeric`] column,
//! unconditionally, the same "every table gets it" philosophy B1 used for
//! `Text`/`Boolean`/`Uuid` (see [`TableSpec`]'s `grain_values` field and the
//! `grain_value` strategy below) — and each definition independently draws
//! either the existing `total = c1 + c2` [`KeySpace::OneToOne`] shape, or a
//! new [`KeySpace::Aggregate`] shape grouped by that table's grain column,
//! with 2–5 fields drawn from `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over `c1`/`c2`
//! (see [`DefShape`], [`AggregateFn`], and the `aggregate_functions`/
//! `def_shape` strategies below). The grain column is deliberately **not**
//! `c1`/`c2` (which keep their existing wider `0..=VALUE_MAX` range and
//! remain the *values being aggregated*) — it's a tiny three-value domain
//! (`0`, `1`, or `2`, see [`GRAIN_MAX`]) specifically so a handful of seed
//! rows collide onto the same group, and a handful of `Delete` mutates has
//! real odds of emptying one out entirely — the single hardest edge in
//! aggregate maintenance (`docs/generative-test-suite.md` §4 calls the
//! aggregate delta path "the hardest guarantee": the only non-idempotent
//! maintenance in the engine), per `local_docs/generative-suite-improvement-plan.md`'s
//! task B4.
//!
//! **The domain is *not* "0, 1, 2, plus `NULL`"**, despite the improvement
//! plan's own text asking for exactly that. A `NULL` grouping value was
//! implemented and drawn during this task's development, and immediately
//! found a real engine bug: `engine::defs::ddl::create_aggregate_target_table`
//! declares the `GROUP BY` columns as the target's Postgres `PRIMARY KEY`,
//! which is unconditionally `NOT NULL` — so a source row with a `NULL`
//! grouping value makes every attempted write to that group fail with a
//! genuine `null value in column ... violates not-null constraint` error,
//! confirmed with a from-scratch, `ManualBackend`-free repro directly against
//! `engine::staging::apply`. Worse, `engine::client`'s `app_worker_loop`
//! treats that as "one bad batch" and retries the *same* segment forever
//! rather than surfacing it as fatal, so the source table's watermark never
//! advances and `staging::await_converged` never returns — observed as this
//! suite's own `run_convergence` hanging past its 30-second
//! `QUIESCE_TIMEOUT` on a freshly-provisioned, uncontended cluster with a
//! single seeded row. See [`grain_value`]'s doc comment for the full
//! writeup. That's a real, reproducible liveness bug worth fixing
//! (`create_aggregate_target_table` needs a NULL-tolerant unique constraint
//! instead of a bare composite `PRIMARY KEY`), but it's an engine schema
//! change outside this generative-suite-widening task's scope — so this
//! generator draws only the three non-`NULL` grain values, not the `NULL`
//! group the plan asked for, until that engine fix lands.
//!
//! **Design choice: every table gets a grain column, not just tables an
//! `Aggregate` def happens to source from.** The alternative (only tables
//! selected as an aggregate def's source get one) would need the table shape
//! itself to depend on how the *definitions* built on top of it turn out to
//! be drawn — but `table_spec` draws a `TableSpec` independently of, and
//! before, any def that might reference it (`trivial_program_with` draws all
//! tables first, then draws each def's source table index and shape). Making
//! every table's shape identical regardless of how it ends up used keeps
//! `TableSpec`/`Table::new` exactly as uniform as B1 already left them, and
//! costs nothing: an `Aggregate` def can then source from *any* drawn table
//! without a second table-shape variant to thread through, and a `OneToOne`
//! def sourced from a table just leaves that table's grain column
//! undrawn-from (present in the schema, never referenced by a field) —
//! exactly how a `OneToOne` def already leaves the pk column's *identity* as
//! a pk unused as a value.
//!
//! **B4 scope cuts:** the grain column is seeded once at `INSERT` time and
//! never touched by `Update`/`Delete`/`DuplicateInsert` (same B1 precedent —
//! a group changing membership via delete/insert of whole rows is already
//! the sharpest edge; *migrating* a live row from one group to another via
//! `Update` is real, separate coverage `engine/tests/apply_aggregate.rs`
//! already exercises by hand, not drawn here). `KeySpace::Aggregate.group_by`
//! is always exactly one column (never a composite/multi-column group-by,
//! which the engine grammar supports but this generator does not draw).
//! `OneToOne` defs never get a grain passthrough field (unlike the B1
//! `Text`/`Boolean`/`Uuid` columns) — the grain column's role here is
//! structural (an aggregate grouping key), not a type-coverage dimension, so
//! giving it a bare passthrough field would add surface without adding
//! coverage of anything the derivation-type coverage meta-test
//! (`tests/coverage.rs`'s `every_column_scalar_type_appears_via_a_derivation`)
//! doesn't already assert via `c1`/`c2`.
//!
//! **B1 scope cuts, stated plainly (design doc §3's own standard for honest
//! narrowing):**
//! - The new columns are seeded once at `INSERT` time and never touched by
//!   [`Mutate::Update`]/[`Mutate::Delete`]/[`Mutate::DuplicateInsert`], which
//!   continue to read/write only `c1`/`c2` exactly as before. This fully
//!   covers "every column scalar type appears via a derivation"
//!   (`generative/tests/coverage.rs`'s
//!   `every_column_scalar_type_appears_via_a_derivation`) without also
//!   having to model heterogeneous per-type `Update`/`DuplicateInsert`
//!   payloads — a separate future widening, not this task.
//! - No new operators, functions, or comparisons over the new types are
//!   drawn (that's B2, gated on a precedence-table prerequisite this session
//!   already decided to defer).
//! - Only syntactically-valid UUID text is ever drawn, or `NULL` — never a
//!   malformed UUID string. A malformed one would fail the `INSERT`/
//!   `UPDATE` statement's own `$n::text::uuid` cast, which is real, separate
//!   future coverage (a new `OpOutcome::Fails` case), not this task.
//! - Awkward text values (empty string, the literal text `"NULL"`, a
//!   U+001F-containing string, a comma/quote/backslash string) are drawn
//!   here, but this does **not** close the improvement plan's "attacks the
//!   key encoding" framing for B1: `engine::intake::extract_key`'s
//!   composite-key delimiter and `defs::oracle::group_key`'s `Aggregate`
//!   grouping-key encoding only matter for a *multi-column* primary key or
//!   an `Aggregate` key-space's `GROUP BY` columns — neither of which this
//!   generator draws (the pk stays single-column `Numeric`; there is no
//!   `Aggregate` key space yet, that's B4). The awkward text values drawn
//!   here are still real, valuable coverage of plain text round-tripping
//!   (SQL binding, `::text` casts, this harness's own snapshot diffing) —
//!   just not of that specific key-encoding risk, which remains open.
//!
//! # Awkward values (issue #7, design doc §3)
//!
//! Of the four awkward-value classes the design doc calls out:
//!
//! - **NULL in a nullable column** — implemented. `c1`/`c2` are already
//!   nullable at the schema level (`ManualBackend::create_source_table` never
//!   emits `NOT NULL` for a non-primary-key column), and the engine's `+`
//!   evaluator already treats a `NULL` operand as short-circuiting to `NULL`
//!   (`engine::defs::eval`'s `Operator::Add` arm), matching Postgres's own
//!   `numeric + NULL = NULL`. So drawing `None` for `c1`/`c2` needed no
//!   engine or model change — see [`Mutate`] and the `value` strategy below.
//! - **Empty string, the literal text `"NULL"`, delimiter-containing
//!   strings** — implemented by task B1 above, on the new `Text` column: see
//!   the `awkward_text_literal`/`text_value` strategies below. As noted
//!   above, this is real coverage of plain-text round-tripping, but it does
//!   *not* yet reach the delimiter-sensitive key-encoding paths (those need
//!   a multi-column pk or an `Aggregate` key space, both still future work).
//!
//! [`Strategy`]: proptest::strategy::Strategy

use std::collections::HashSet;

use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};

use crate::model::{NamePool, Op, OpOutcome, Program, Table};

/// The inclusive upper bound of the calculated-field value domain.
///
/// **Numeric-path pairing (design doc §3, a load-bearing invariant).** The
/// generator only ever emits small non-negative *integers* here, and the sole
/// derivation is `c1 + c2`. Both the engine (`engine::numeric::Numeric`) and
/// the SQL oracle (Postgres `numeric`) are arbitrary-precision base-10: an
/// integer sum has one exact representation on both sides, with no rounding
/// and no float path to fall off of, so the two can never disagree by
/// construction. The bound is therefore purely for legibility — a shrunk
/// counterexample reads at a glance — and for staying deliberately far from
/// any future column-type narrowing (e.g. an `int4` source column) that could
/// introduce a value one side represents differently. Widen this, or add
/// fractional/negative values, only together with the comparison-semantics
/// review the widening implies.
pub const VALUE_MAX: i64 = 99;

/// The most rows the trivial generator seeds before mutating. Kept tiny: each
/// op costs a full apply → quiesce → snapshot → compare round trip, and a
/// legible counterexample beats a large one (design doc §1, §4).
pub const MAX_SEED_ROWS: usize = 4;

/// The most mutate ops appended after the seed phase. Kept small because
/// every op is a full apply → quiesce → snapshot → compare round trip against
/// a real cluster, so the per-case cost scales with the op count.
pub const MAX_MUTATES: usize = 4;

/// The most source tables a drawn program has (improvement-plan task B3).
/// `1..=MAX_TABLES` via `prop_flat_map`, matching the `MAX_SEED_ROWS`/
/// `MAX_MUTATES` idiom above, so proptest's default numeric-range shrinking
/// reduces the table *count* first — a 1-table counterexample is the
/// readable one. Kept at 3 (not e.g. 2) so "two defs sharing one source" and
/// "defs over different sources" are both reachable without a def count that
/// forces one of them.
pub const MAX_TABLES: usize = 3;

/// The most definitions a drawn program has (improvement-plan task B3). Each
/// definition independently draws one of the program's tables as its
/// source — see [`build_program_multi`] — so this is deliberately not tied
/// to `MAX_TABLES`: two definitions can and do land on the same table.
pub const MAX_DEFS: usize = 3;

/// The inclusive upper bound of the grain column's value domain
/// (improvement-plan task B4): `0..=GRAIN_MAX` is the grain column's *entire*
/// value space — kept at `2` (three possible values total; see
/// [`grain_value`]'s doc comment for why this is three, not the plan's
/// originally-requested four including `NULL`) so it stays "tiny,
/// heavily-repeating" (the plan's own words): with `MAX_SEED_ROWS` (4) rows
/// drawn from a 3-value domain, a real collision (two seed rows landing in
/// the same group) is the common case, not a rare one, and a handful of
/// `Delete` mutates has real odds of emptying a group entirely — see the
/// module doc comment's B4 section.
pub const GRAIN_MAX: i64 = 2;

/// Renders an `Option<i64>` value to the rendered-text form
/// [`crate::model::Op`] wants: `None` (SQL `NULL`) stays `None`.
fn render(value: Option<i64>) -> Option<String> {
    value.map(|v| v.to_string())
}

/// One post-seed mutation, parameterized only by primary-key integers and
/// (for updates/duplicate-inserts) new field values — the plain-data draw a
/// proptest strategy shrinks, kept separate from the [`Op`] it renders into so
/// the builder stays proptest-free.
///
/// `c1`/`c2` are `Option<i64>`, `None` meaning SQL `NULL`: `c1`/`c2` are
/// nullable columns at the schema level (`ManualBackend::create_source_table`
/// emits no `NOT NULL` for any non-primary-key column), so a null value here
/// is exercising real, already-representable schema surface — no widening of
/// [`crate::model::Column`]/[`Table`] was needed for this (see the
/// module-level doc comment's "awkward values" note).
#[derive(Debug, Clone, PartialEq)]
pub enum Mutate {
    /// Set `c1`/`c2` on row `pk`. When `pk` names no seeded row this is a
    /// no-op at the source (Postgres updates zero rows, no error), which is
    /// exactly the "operation errors are checked, not swallowed" case the
    /// convergence property must still converge on (design doc §4).
    Update {
        pk: i64,
        c1: Option<i64>,
        c2: Option<i64>,
    },
    /// Delete row `pk`. A `pk` naming no seeded row is likewise a no-op.
    Delete { pk: i64 },
    /// Insert a *second* row at an already-seeded `pk`. When that pk is
    /// still live (the common case) this is a genuine `apply()` failure, not
    /// a source no-op: the source table's primary key is a real Postgres
    /// `primary key` constraint, so this statement is rejected with a
    /// unique-violation error and the source is left unchanged (a single
    /// `INSERT` is atomic — it cannot partially apply). Closes the issue #6
    /// gap where every generated op used to succeed, so "an op that errors
    /// changed nothing" (design doc §4) was never exercised against a *real*
    /// rejection (a missing-pk update/delete comes back as `Ok(0 rows)`, not
    /// an `Err`).
    ///
    /// If an earlier mutate in the same stream already deleted this pk, it
    /// is no longer live: this "duplicate" insert is then an ordinary
    /// successful insert that revives it — [`build_program`] simulates pk
    /// liveness across the whole mutate stream to expect the right outcome
    /// either way (see its `expect: OpOutcome` derivation).
    DuplicateInsert {
        pk: i64,
        c1: Option<i64>,
        c2: Option<i64>,
    },
}

/// Which of a source table's two aggregated columns (`c1`/`c2`) a generated
/// `SUM`/`AVG`/`MIN`/`MAX` field aggregates over (improvement-plan task B4).
/// `COUNT(*)` has no column argument at all (see [`AggregateFn::Count`]), so
/// this only shows up nested inside the other four variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateColumn {
    C1,
    C2,
}

/// One calculated field of a generated [`KeySpace::Aggregate`] definition
/// (task B4): one of the five functions
/// `engine::defs::registry::AGGREGATE_FUNCTIONS` accepts, carrying which
/// column it aggregates (all but `Count`, which is `COUNT(*)` row-counting
/// and takes no argument at all). Kept as a small enum — rather than a bare
/// `(name, column)` pair — so [`build_program_multi_with_shapes`]'s match
/// stays exhaustive against the registry's actual function set: adding a
/// sixth aggregate function to the engine would need a new variant here
/// before it could compile, not just a new string someone forgot to draw.
///
/// Two of `SUM`/`COUNT`/`AVG` are [`engine::defs::invertibility::Invertibility::Invertible`]
/// (delta-maintained); `MIN`/`MAX` are always
/// [`engine::defs::invertibility::Invertibility::RecomputeOnly`] — see that
/// module's doc comment. Drawing a real mix of both classes in the same
/// aggregate def (not just across different defs) is exactly what
/// `tests/coverage.rs`'s B4 floor tests assert actually happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Sum(AggregateColumn),
    Count,
    Avg(AggregateColumn),
    Min(AggregateColumn),
    Max(AggregateColumn),
}

impl AggregateFn {
    /// This function's canonical uppercased name, exactly as
    /// `engine::defs::registry::AGGREGATE_FUNCTIONS` spells it.
    fn name(self) -> &'static str {
        match self {
            AggregateFn::Sum(_) => "SUM",
            AggregateFn::Count => "COUNT",
            AggregateFn::Avg(_) => "AVG",
            AggregateFn::Min(_) => "MIN",
            AggregateFn::Max(_) => "MAX",
        }
    }

    /// The aggregated column, for every variant but `Count` (which has none).
    fn column(self) -> Option<AggregateColumn> {
        match self {
            AggregateFn::Sum(c)
            | AggregateFn::Avg(c)
            | AggregateFn::Min(c)
            | AggregateFn::Max(c) => Some(c),
            AggregateFn::Count => None,
        }
    }
}

/// Which shape a generated [`TransformDef`] takes (improvement-plan task B4
/// widens this past the previous always-`OneToOne` assumption): the existing
/// `total = c1 + c2` [`KeySpace::OneToOne`] shape, or a `GROUP BY`
/// [`KeySpace::Aggregate`] shape over one of its source table's grain column,
/// with `functions` giving its calculated fields beyond the grain-column
/// passthrough (see [`build_program_multi_with_shapes`]).
#[derive(Debug, Clone, PartialEq)]
pub enum DefShape {
    OneToOne,
    Aggregate { functions: Vec<AggregateFn> },
}

/// One drawn table's seed rows and post-seed mutate stream, for
/// [`build_program_multi`].
///
/// Every [`Op`] already names the table it targets (`crate::model::Op`), but
/// [`Mutate`] deliberately does not — it has no notion of "which table" at
/// all, only a pk within *some* table's own pk space. Rather than teach
/// `Mutate` a table field (which would let a single mutate stream reference a
/// table it wasn't drawn against, a whole class of invalid-program bug this
/// type is built to make unrepresentable), a multi-table program instead
/// groups each table's seed values with the mutates meant for *that* table.
/// [`build_program_multi`] then runs the same per-table pk-liveness
/// simulation [`build_program`] always ran, once per `TableSpec`, so table A's
/// deletes can never be mistaken for table B's — see that function's doc
/// comment.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableSpec {
    /// `seed_values[i]` is the `(c1, c2)` pair for seeded primary key
    /// `i + 1`, exactly as [`build_program`]'s `seed_values` parameter.
    pub seed_values: Vec<(Option<i64>, Option<i64>)>,
    /// `text_values[i]` is the rendered value (already in [`Op`]'s
    /// `Option<String>` convention — `None` is SQL `NULL`) for seeded
    /// primary key `i + 1`'s `Text` column (improvement-plan task B1). Must
    /// have exactly `seed_values.len()` entries — [`build_program_multi`]
    /// panics otherwise, a generator-bug-style check matching this module's
    /// other `assert!`s.
    pub text_values: Vec<Option<String>>,
    /// `bool_values[i]` is the rendered value (`"true"`/`"false"`, or `None`
    /// for SQL `NULL`) for seeded primary key `i + 1`'s `Boolean` column
    /// (task B1). Same length contract as `text_values`.
    pub bool_values: Vec<Option<String>>,
    /// `uuid_values[i]` is the rendered value (a syntactically-valid UUID
    /// string, or `None` for SQL `NULL` — never a malformed UUID string, see
    /// the module doc comment's B1 scope cuts) for seeded primary key
    /// `i + 1`'s `Uuid` column (task B1). Same length contract as
    /// `text_values`.
    pub uuid_values: Vec<Option<String>>,
    /// `grain_values[i]` is the rendered value (`"0"`/`"1"`/`"2"`, see
    /// [`GRAIN_MAX`]) for seeded primary key `i + 1`'s grain column
    /// (improvement-plan task B4). Stays `Option<String>` — the same shape
    /// every other seeded column here uses — so a hand-built pin can still
    /// construct a `None` (SQL `NULL`) grain value directly if one is ever
    /// needed (e.g. a regression pin for the `NULL`-grouping-key engine bug
    /// [`grain_value`]'s doc comment describes), even though the `grain_value`
    /// proptest strategy itself never draws one. Same length contract as
    /// `text_values`. Never touched by [`Mutate`] — see the module doc
    /// comment's B4 scope cuts.
    pub grain_values: Vec<Option<String>>,
    /// Mutates appended after this table's seed inserts (seed-before-mutate,
    /// design doc §3), targeting only this table's own pks. Never touches
    /// the `Text`/`Boolean`/`Uuid`/grain columns above — see the module doc
    /// comment's B1/B4 scope cuts.
    pub mutates: Vec<Mutate>,
}

impl TableSpec {
    /// Builds a `TableSpec` whose `Text`/`Boolean`/`Uuid` columns (task B1)
    /// and grain column (task B4) are all `NULL` for every seed row — a
    /// convenience for callers that only care about the numeric seed/mutate
    /// shape (e.g. the pk-liveness unit tests below, and [`build_program`]'s
    /// two-argument convenience wrapper), sparing them from hand-counting a
    /// matching-length `NULL` vector for each of the four new columns.
    pub fn numeric_only(
        seed_values: Vec<(Option<i64>, Option<i64>)>,
        mutates: Vec<Mutate>,
    ) -> Self {
        let row_count = seed_values.len();
        TableSpec {
            seed_values,
            text_values: vec![None; row_count],
            bool_values: vec![None; row_count],
            uuid_values: vec![None; row_count],
            grain_values: vec![None; row_count],
            mutates,
        }
    }
}

/// Renders one [`Mutate`] into the [`Op`] it becomes against `table` (columns
/// `pk_col`/`c1`/`c2`), threading `live` — that table's own pk-liveness set —
/// through so the emitted op's `expect` tracks the pk's *current* state, not
/// just whether it was originally seeded (see [`Mutate::DuplicateInsert`]).
///
/// Shared by [`build_program`] and [`build_program_multi`] so single- and
/// multi-table programs run the exact same liveness logic — this is the one
/// place that logic lives, specifically so a per-table bug (liveness leaking
/// across tables, or one table's pks silently continuing another's
/// numbering) has nowhere to hide a second, diverging copy.
fn render_mutate(
    mutate: &Mutate,
    table: &str,
    pk_col: &str,
    c1: &str,
    c2: &str,
    live: &mut HashSet<i64>,
) -> Op {
    match mutate {
        Mutate::Update { pk, c1: a, c2: b } => Op::Update {
            table: table.to_string(),
            pk: pk.to_string(),
            changes: vec![(c1.to_string(), render(*a)), (c2.to_string(), render(*b))],
            expect: if live.contains(pk) {
                OpOutcome::Succeeds
            } else {
                OpOutcome::AffectsNoRows
            },
        },
        Mutate::Delete { pk } => Op::Delete {
            table: table.to_string(),
            pk: pk.to_string(),
            // `remove` reports whether `pk` was live, and (whether or
            // not it was) leaves it dead afterward — exactly the delete
            // semantics we're simulating.
            expect: if live.remove(pk) {
                OpOutcome::Succeeds
            } else {
                OpOutcome::AffectsNoRows
            },
        },
        Mutate::DuplicateInsert { pk, c1: a, c2: b } => {
            // Only a genuine primary-key violation while `pk` is still
            // live. If an earlier mutate already deleted it, this isn't
            // a duplicate anymore — it's an ordinary successful insert
            // that revives the pk.
            let expect = if live.contains(pk) {
                OpOutcome::Fails
            } else {
                live.insert(*pk);
                OpOutcome::Succeeds
            };
            Op::Insert {
                table: table.to_string(),
                row: vec![
                    (pk_col.to_string(), Some(pk.to_string())),
                    (c1.to_string(), render(*a)),
                    (c2.to_string(), render(*b)),
                ],
                expect,
            }
        }
    }
}

/// Builds the trivial single-table/single-def program from already-drawn
/// data: `seed_values[i]` is the `(c1, c2)` pair for seeded primary key
/// `i + 1`, and `mutates` are appended after every seed insert
/// (seed-before-mutate, design doc §3, so every update and delete has real
/// rows to hit).
///
/// The schema and definition are fixed — one source table `t0` with a numeric
/// primary key `c0` and two numeric columns `c1`/`c2`, one 1-1 target `t1`
/// computing `c1 + c2` — so the only thing that varies (and shrinks) between
/// cases is the data and the mutate stream.
///
/// A thin convenience wrapper around [`build_program_multi`] (one
/// `TableSpec`, one def sourced from it) kept at its original two-argument
/// signature rather than folded away: every existing hand-built pin across
/// `generative/tests/*.rs` and this module's own tests already calls it this
/// shape, and there was no reason to touch two dozen call sites for a change
/// scoped to table/def *count* (improvement-plan task B3).
pub fn build_program(seed_values: &[(Option<i64>, Option<i64>)], mutates: &[Mutate]) -> Program {
    build_program_multi(
        &[TableSpec::numeric_only(
            seed_values.to_vec(),
            mutates.to_vec(),
        )],
        &[0],
    )
}

/// Builds a program over 1 or more tables and 1 or more definitions
/// (improvement-plan task B3): `tables[i]` describes source table `i`'s seed
/// rows and mutate stream (see [`TableSpec`]), and `def_sources[j]` is the
/// index into `tables` that definition `j` reads from — independently drawn,
/// so two definitions landing on the same table (fan-out) and definitions
/// spread across different tables are both representable, including every
/// mix of the two in one program.
///
/// Every table gets its own numeric pk/`c1`/`c2` schema, plus (task B1) one
/// `Text`/`Boolean`/`Uuid` column each, and its own independent pk-liveness
/// simulation (a fresh `HashSet` per `TableSpec`, via [`render_mutate`]):
/// table A's deletes and inserts can never be mistaken for table B's, and
/// each table's seeded pks start at `1` regardless of how many rows an
/// earlier table seeded — table identity, not draw order, is what a pk is
/// scoped to. Every definition sourced from a table gets the same field
/// list: `total = c1 + c2`, plus an identity-passthrough field for each of
/// that table's `Text`/`Boolean`/`Uuid` columns (task B1) — this function
/// widens *how many* tables/defs a program has and, since B1, *how many
/// scalar types* each table/def touches; it does not draw operators,
/// functions, or comparisons over those types (that's B2, out of scope
/// here).
///
/// Ops are emitted one table at a time, in `tables` order: all of table 0's
/// seeds and mutates, then all of table 1's, and so on. This keeps a
/// counterexample's op stream legible (every op naming table N groups
/// together) and is not a claim that real traffic interleaves tables that
/// way — nothing about the model or the backend assumes any particular
/// interleaving.
///
/// Panics if `tables` or `def_sources` is empty, or if a `def_sources` entry
/// is out of range for `tables` — both are generator bugs (every strategy
/// below draws `1..=MAX_TABLES`/`1..=MAX_DEFS` and indexes accordingly), not
/// conditions a caller should need to handle.
///
/// A thin wrapper around [`build_program_multi_with_shapes`] (every def
/// `OneToOne`) kept at its original `&[usize]` signature rather than folded
/// away — the same "don't touch two dozen call sites for an orthogonal
/// widening" reasoning [`build_program`]'s own doc comment gives for staying
/// at its two-argument shape (improvement-plan task B4 widens *which shapes*
/// a def can take, not how many tables/defs a program has, which is what
/// this signature already expresses).
pub fn build_program_multi(tables: &[TableSpec], def_sources: &[usize]) -> Program {
    let defs: Vec<(usize, DefShape)> = def_sources
        .iter()
        .map(|&idx| (idx, DefShape::OneToOne))
        .collect();
    build_program_multi_with_shapes(tables, &defs)
}

/// [`build_program_multi`]'s general form (improvement-plan task B4): `defs[j]`
/// is `(source table index, shape)` for definition `j`, so each definition
/// independently draws not just *which* table it sources from but *which
/// shape* it takes — the existing `total = c1 + c2` [`KeySpace::OneToOne`], or
/// a `GROUP BY` [`KeySpace::Aggregate`] over that table's grain column with
/// [`DefShape::Aggregate`]'s `functions` as its non-grouping fields.
///
/// Every table gets its own numeric pk/`c1`/`c2` schema, plus (task B1) one
/// `Text`/`Boolean`/`Uuid` column each, plus (task B4) one further Numeric
/// "grain" column (see the module doc comment), and its own independent
/// pk-liveness simulation (a fresh `HashSet` per `TableSpec`, via
/// [`render_mutate`]): table A's deletes and inserts can never be mistaken
/// for table B's, and each table's seeded pks start at `1` regardless of how
/// many rows an earlier table seeded — table identity, not draw order, is
/// what a pk is scoped to.
///
/// Ops are emitted one table at a time, in `tables` order: all of table 0's
/// seeds and mutates, then all of table 1's, and so on. This keeps a
/// counterexample's op stream legible (every op naming table N groups
/// together) and is not a claim that real traffic interleaves tables that
/// way — nothing about the model or the backend assumes any particular
/// interleaving.
///
/// Panics if `tables` or `defs` is empty, or if a `defs` entry's table index
/// is out of range for `tables` — both are generator bugs (every strategy
/// below draws `1..=MAX_TABLES`/`1..=MAX_DEFS` and indexes accordingly), not
/// conditions a caller should need to handle.
pub fn build_program_multi_with_shapes(
    tables: &[TableSpec],
    defs: &[(usize, DefShape)],
) -> Program {
    assert!(
        !tables.is_empty(),
        "build_program_multi_with_shapes: a program must draw at least one table"
    );
    assert!(
        !defs.is_empty(),
        "build_program_multi_with_shapes: a program must draw at least one definition"
    );

    let mut pool = NamePool::new();
    let mut built_tables = Vec::with_capacity(tables.len());
    let mut ops = Vec::new();

    for spec in tables {
        let row_count = spec.seed_values.len();
        assert_eq!(
            spec.text_values.len(),
            row_count,
            "build_program_multi_with_shapes: text_values must have one entry per seed row \
             ({row_count} seed rows, {} text values) — a generator bug",
            spec.text_values.len()
        );
        assert_eq!(
            spec.bool_values.len(),
            row_count,
            "build_program_multi_with_shapes: bool_values must have one entry per seed row \
             ({row_count} seed rows, {} bool values) — a generator bug",
            spec.bool_values.len()
        );
        assert_eq!(
            spec.uuid_values.len(),
            row_count,
            "build_program_multi_with_shapes: uuid_values must have one entry per seed row \
             ({row_count} seed rows, {} uuid values) — a generator bug",
            spec.uuid_values.len()
        );
        assert_eq!(
            spec.grain_values.len(),
            row_count,
            "build_program_multi_with_shapes: grain_values must have one entry per seed row \
             ({row_count} seed rows, {} grain values) — a generator bug",
            spec.grain_values.len()
        );

        // Tasks B1/B4: every table gets a Text/Boolean/Uuid column and a
        // grain column, always — see the module doc comment. `Table::new`
        // builds columns in the order given, so `columns[3..=5]` are
        // Text/Boolean/Uuid and `columns[6]` is the grain column, after the
        // pk (`columns[0]`) and `c1`/`c2` (`columns[1..=2]`). The grain
        // column is appended last (rather than interleaved) so every
        // existing `columns[1..=5]` index above is untouched by this
        // widening.
        let source = Table::new(
            &mut pool,
            &[
                ValueType::Numeric,
                ValueType::Numeric,
                ValueType::Text,
                ValueType::Boolean,
                ValueType::Uuid,
                ValueType::Numeric,
            ],
        );
        let c1 = source.columns[1].name.clone();
        let c2 = source.columns[2].name.clone();
        let text_col = source.columns[3].name.clone();
        let bool_col = source.columns[4].name.clone();
        let uuid_col = source.columns[5].name.clone();
        let grain_col = source.columns[6].name.clone();

        // This table's own pk-liveness simulation, independent of every
        // other table's — see [`render_mutate`] and the function doc
        // comment above. Every seeded pk starts live; every seed insert
        // always succeeds (pks are freshly minted, `1..=seed_count`, never
        // colliding *within this table*).
        let mut live: HashSet<i64> = (1..=spec.seed_values.len() as i64).collect();

        for (i, (a, b)) in spec.seed_values.iter().enumerate() {
            let pk = (i + 1) as i64;
            ops.push(Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some(pk.to_string())),
                    (c1.clone(), render(*a)),
                    (c2.clone(), render(*b)),
                    // Task B1: seeded once, here, and never touched again —
                    // see [`Mutate`]/the module doc comment's scope cut.
                    (text_col.clone(), spec.text_values[i].clone()),
                    (bool_col.clone(), spec.bool_values[i].clone()),
                    (uuid_col.clone(), spec.uuid_values[i].clone()),
                    // Task B4: likewise seeded once and never mutated — see
                    // the module doc comment's B4 scope cuts.
                    (grain_col.clone(), spec.grain_values[i].clone()),
                ],
                expect: OpOutcome::Succeeds,
            });
        }
        // A mutate's outcome depends on whether its pk is *currently* live
        // within this table, not just whether it started out seeded: the
        // `mutate` strategy below can draw several mutates against the same
        // pk (e.g. a `Delete` followed by an `Update`/`Delete`/
        // `DuplicateInsert` on that same now-gone pk), and each one's real
        // Postgres outcome tracks the row's live/dead state at the moment it
        // runs, not the original seed.
        for mutate in &spec.mutates {
            ops.push(render_mutate(
                mutate,
                &source.name,
                &source.pk_col,
                &c1,
                &c2,
                &mut live,
            ));
        }

        built_tables.push(source);
    }

    let defs = defs
        .iter()
        .map(|(idx, shape)| {
            let source = built_tables.get(*idx).unwrap_or_else(|| {
                panic!(
                    "build_program_multi_with_shapes: defs index {idx} out of range for {} \
                     tables — a generator bug",
                    built_tables.len()
                )
            });
            let c1 = source.columns[1].name.clone();
            let c2 = source.columns[2].name.clone();
            let text_col = source.columns[3].name.clone();
            let bool_col = source.columns[4].name.clone();
            let uuid_col = source.columns[5].name.clone();
            let grain_col = source.columns[6].name.clone();

            let (key_space, fields) = match shape {
                DefShape::OneToOne => (
                    KeySpace::OneToOne,
                    vec![
                        FieldDef {
                            name: "total".to_string(),
                            expr: Expr::BinaryOp {
                                op: Operator::Add,
                                lhs: Box::new(Expr::Column(c1)),
                                rhs: Box::new(Expr::Column(c2)),
                            },
                        },
                        // Task B1: one identity-passthrough field per new
                        // column, reusing the column's own name as the field
                        // name (`SELECT <col> AS <col>`) — the exact shape
                        // `engine/tests/defs_backfill_direct.rs` already
                        // exercises by hand, and every def sourced from this
                        // table gets the same three, determined by the
                        // table's shape rather than drawn independently per
                        // def (matching how `total` already works). The
                        // grain column (task B4) deliberately gets no
                        // matching passthrough field here — see the module
                        // doc comment's B4 scope cuts.
                        FieldDef {
                            name: text_col.clone(),
                            expr: Expr::Column(text_col),
                        },
                        FieldDef {
                            name: bool_col.clone(),
                            expr: Expr::Column(bool_col),
                        },
                        FieldDef {
                            name: uuid_col.clone(),
                            expr: Expr::Column(uuid_col),
                        },
                    ],
                ),
                DefShape::Aggregate { functions } => {
                    // The grouping-column passthrough field
                    // (`SELECT <grain> AS <grain>`) is required —
                    // `engine::defs::validate::validate` rejects any other
                    // expression under a grouping column's own name — and
                    // every other field is one of `functions`, drawn from
                    // `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over `c1`/`c2` (see
                    // [`AggregateFn`]/`aggregate_field_def`).
                    let mut fields = vec![FieldDef {
                        name: grain_col.clone(),
                        expr: Expr::Column(grain_col.clone()),
                    }];
                    fields.extend(
                        functions
                            .iter()
                            .map(|func| aggregate_field_def(*func, &c1, &c2)),
                    );
                    (
                        KeySpace::Aggregate {
                            group_by: vec![grain_col],
                        },
                        fields,
                    )
                }
            };

            TransformDef {
                target: pool.next_table_name(),
                source: source.name.clone(),
                key_space,
                fields,
                predicate: Predicate::True,
            }
        })
        .collect();

    Program {
        tables: built_tables,
        defs,
        ops,
    }
}

/// Builds one [`FieldDef`] for a drawn [`AggregateFn`] (improvement-plan task
/// B4): the field name encodes both the function and its aggregated column
/// (`sum_c1`, `avg_c2`, ...) so distinct `(function, column)` draws — even two
/// functions sharing the same column, e.g. `SUM(c1)` and `AVG(c1)`, which
/// deliberately exercises `engine::defs::ddl::count_column_names`'s shared
/// hidden-count-column path — never collide; `COUNT` has no column and always
/// takes the fixed name `cnt`. `c1`/`c2` are the source table's own rendered
/// column names (as everywhere else in this module).
fn aggregate_field_def(func: AggregateFn, c1: &str, c2: &str) -> FieldDef {
    let column_name = |column: AggregateColumn| match column {
        AggregateColumn::C1 => c1.to_string(),
        AggregateColumn::C2 => c2.to_string(),
    };
    match func.column() {
        Some(column) => {
            let column_name = column_name(column);
            FieldDef {
                name: format!("{}_{column_name}", func.name().to_lowercase()),
                expr: Expr::FunctionCall {
                    name: func.name().to_string(),
                    args: vec![Expr::Column(column_name)],
                },
            }
        }
        None => FieldDef {
            name: "cnt".to_string(),
            expr: Expr::FunctionCall {
                name: func.name().to_string(),
                args: Vec::new(),
            },
        },
    }
}

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use proptest::prelude::*;

    /// One in this many awkward-value draws comes back `None` (SQL `NULL`)
    /// when awkward values are enabled — frequent enough that a handful of
    /// seed rows/mutates reliably hits one, rare enough that most cases still
    /// exercise the plain numeric path.
    const NULL_WEIGHT: u32 = 1;
    const VALUE_WEIGHT: u32 = 4;

    /// A single calculated-field / column value: a small non-negative integer
    /// (see [`VALUE_MAX`]'s numeric-path pairing), or — when `awkward_values`
    /// is set — occasionally `None` (SQL `NULL`, see the module doc comment's
    /// "awkward values" note).
    ///
    /// When `awkward_values` is unset this is *exactly* the strategy the
    /// generator used before issue #7 (a bare `0..=VALUE_MAX` draw, just
    /// wrapped in `Some` outside the strategy), so a coverage meta-test can
    /// assert the flag-off path draws unchanged (design doc §3 "coverage
    /// meta-tests").
    fn value(awkward_values: bool) -> BoxedStrategy<Option<i64>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => (0..=VALUE_MAX).prop_map(Some),
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            (0..=VALUE_MAX).prop_map(Some).boxed()
        }
    }

    /// Weight of one specific awkward text literal, relative to a plain
    /// short string, when awkward values are enabled (task B1). Kept small
    /// like `NULL_WEIGHT` — rare per draw, reliably hit across a handful of
    /// seed rows.
    const AWKWARD_TEXT_WEIGHT: u32 = 1;

    /// A short, plain ASCII string — the "ordinary" `Text` column value both
    /// with and without awkward values enabled.
    fn plain_text() -> BoxedStrategy<String> {
        "[a-zA-Z0-9 ]{0,8}".boxed()
    }

    /// One specific awkward *text* value (design doc §3 / improvement-plan
    /// task B1): the empty string; the literal four-character text `"NULL"`
    /// (distinct from SQL `NULL`, i.e. `None` — that's [`value`]'s job, this
    /// is the string that spells the word); a string containing the U+001F
    /// unit-separator (`engine::intake::extract_key`'s composite-key
    /// delimiter — see the module doc comment's scope-cut note on why this
    /// doesn't yet reach that risk); and a string containing a
    /// comma/quote/backslash (SQL-binding/escaping awkwardness, independent
    /// of any delimiter).
    fn awkward_text_literal() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            Just("NULL".to_string()),
            Just("has\u{1f}unit-separator".to_string()),
            Just("has,comma'quote\"and\\backslash".to_string()),
        ]
        .boxed()
    }

    /// A single `Text` column value (task B1): a plain short string, or —
    /// when `awkward_values` is set — occasionally `None` (SQL `NULL`) or
    /// one of [`awkward_text_literal`]'s specific awkward strings.
    fn text_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain_text().prop_map(Some),
                NULL_WEIGHT => Just(None),
                AWKWARD_TEXT_WEIGHT => awkward_text_literal().prop_map(Some),
            ]
            .boxed()
        } else {
            plain_text().prop_map(Some).boxed()
        }
    }

    /// A single `Boolean` column value (task B1): the rendered text form
    /// [`Op`] wants (`"true"`/`"false"`), or — when `awkward_values` is set —
    /// occasionally `None` (SQL `NULL`).
    fn bool_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        let plain = prop_oneof![
            Just(Some("true".to_string())),
            Just(Some("false".to_string())),
        ];
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain,
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            plain.boxed()
        }
    }

    /// Formats 16 random bytes as a syntactically-valid UUID string
    /// (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`) — hand-rolled rather than
    /// pulling in the `uuid` crate: `uuid` is not a workspace dependency
    /// anywhere reachable from `generative` (checked `Cargo.lock` and every
    /// crate's `Cargo.toml` in the workspace before writing this), and
    /// proptest's own random bytes are all a validly-*shaped* UUID string
    /// needs — Postgres's `uuid` type only checks the 32-hex-digit/dash-
    /// grouping shape, not the RFC 4122 version/variant bits. This function
    /// sets them anyway (version 4, the common "random UUID" form) purely so
    /// a shrunk counterexample looks like a real application-generated UUID
    /// rather than an obviously-synthetic one.
    fn uuid_string(mut bytes: [u8; 16]) -> String {
        bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
        bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
             {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15],
        )
    }

    fn plain_uuid() -> BoxedStrategy<String> {
        prop::array::uniform16(any::<u8>())
            .prop_map(uuid_string)
            .boxed()
    }

    /// A single `Uuid` column value (task B1): a freshly-generated,
    /// syntactically-valid UUID string, or — when `awkward_values` is set —
    /// occasionally `None` (SQL `NULL`). Deliberately **never** a malformed
    /// UUID string (see the module doc comment's B1 scope cuts): a bad UUID
    /// would fail the `INSERT`/`UPDATE` statement's own `$n::text::uuid`
    /// cast — real, separate future coverage (a new `OpOutcome::Fails`
    /// case), not this task.
    fn uuid_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain_uuid().prop_map(Some),
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            plain_uuid().prop_map(Some).boxed()
        }
    }

    /// A single grain column value (improvement-plan task B4): `0..=GRAIN_MAX`,
    /// always `Some` — **never `NULL`**, despite the improvement-plan task's
    /// original text asking for a "0, 1, 2, plus NULL" domain. This is a
    /// deliberate scope cut discovered *while building this task*, not an
    /// oversight: a `NULL` grouping value is real, standard SQL (Postgres's
    /// `GROUP BY` puts every `NULL` into one group, like any other value),
    /// but this engine's `KeySpace::Aggregate` target-table DDL
    /// (`engine::defs::ddl::create_aggregate_target_table`) declares the
    /// `group_by` columns as the target's `PRIMARY KEY` — and Postgres
    /// primary-key columns are `NOT NULL` unconditionally, by definition, with
    /// no opt-out. A source row whose grouping column is `NULL` therefore
    /// makes every write to that group's target row fail with a real
    /// Postgres `null value in column ... violates not-null constraint`
    /// error — confirmed directly against a from-scratch, ManualBackend-free
    /// `engine::staging::apply` repro during this task's own testing, one
    /// `COUNT(*)`-only field, one seeded row with `NULL` grain, nothing else.
    /// Worse than a rejected write: the constraint violation surfaces deep
    /// inside `staging::apply`'s CDC drain path
    /// (`engine::client`'s `app_worker_loop`, whose own doc comment says "one
    /// bad batch never crashes the worker" — it releases the claim and
    /// *keeps retrying the same segment*), so the batch is retried forever,
    /// the watermark for that source table never advances past it, and
    /// `staging::await_converged` never returns — observed directly as this
    /// suite's `run_convergence` timing out at its 30s `QUIESCE_TIMEOUT`
    /// on a *fresh, uncontended* cluster, not just under load. This is a
    /// real, reproducible liveness bug (a NULL grouping value can wedge an
    /// aggregate source table's ingestion permanently), not merely an
    /// unsupported edge case — flagged here rather than fixed, since a real
    /// fix needs a schema change to `create_aggregate_target_table` (a
    /// NULL-tolerant unique constraint instead of a bare `PRIMARY KEY`, plus
    /// whatever upsert-conflict-target changes that implies throughout
    /// `staging::apply_aggregate`), which is engine work well outside this
    /// generative-suite-widening task's scope. Until that lands, this
    /// generator must not draw a value that's *known* to wedge the very
    /// pipeline it's exercising — see also `tests/coverage.rs`'s
    /// `awkward_values_off_never_draws_null`, which this keeps satisfying
    /// unconditionally (there's no `awkward_values`-gated branch to keep in
    /// sync here at all now) rather than incidentally.
    fn grain_value() -> impl Strategy<Value = Option<String>> {
        prop_oneof![
            Just(Some("0".to_string())),
            Just(Some("1".to_string())),
            Just(Some("2".to_string())),
        ]
    }

    /// Which of a source table's two aggregated columns (`c1`/`c2`) a drawn
    /// `SUM`/`AVG`/`MIN`/`MAX` field aggregates over (task B4).
    fn aggregate_column() -> impl Strategy<Value = AggregateColumn> {
        prop_oneof![Just(AggregateColumn::C1), Just(AggregateColumn::C2),]
    }

    /// The five function *kinds* [`aggregate_functions`] can draw, without
    /// their column argument — kept as its own tiny enum so
    /// `proptest::sample::subsequence` (which needs a concrete, `Clone`
    /// element type to draw an order-preserving, duplicate-free subset from)
    /// has something to draw over; [`aggregate_functions`] then pairs each
    /// drawn kind with an independently-drawn column.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FnKind {
        Sum,
        Count,
        Avg,
        Min,
        Max,
    }

    const ALL_FN_KINDS: [FnKind; 5] = [
        FnKind::Sum,
        FnKind::Count,
        FnKind::Avg,
        FnKind::Min,
        FnKind::Max,
    ];

    /// A drawn `Aggregate` def's non-grouping fields (task B4): 2 to 5 of the
    /// five aggregate functions (`engine::defs::registry::AGGREGATE_FUNCTIONS`),
    /// each drawn at most once (`subsequence` over [`ALL_FN_KINDS`] never
    /// repeats an element), so multiple functions genuinely co-occur on the
    /// same def without ever needing two fields of the same name. Every
    /// column-taking function's column is drawn independently per *function*
    /// (not per occurrence, since each function occurs at most once anyway),
    /// so two different functions landing on the *same* column — e.g.
    /// `SUM(c1)` and `AVG(c1)` — is a real, reachable draw: that's precisely
    /// the shape that exercises `engine::defs::ddl::count_column_names`'s
    /// shared hidden-count-column path (two fields aggregating the exact same
    /// argument share one partial column) from the generative side, not just
    /// `engine/tests/apply_aggregate.rs`'s hand-built fixture.
    ///
    /// The lower bound of 2 (not 1) guarantees at least two functions always
    /// co-occur, per the improvement-plan task's explicit ask ("draw at least
    /// 2-3 of the five per aggregate def so multiple functions actually
    /// co-occur"); the upper bound of 5 lets every function appear in the
    /// same def when proptest happens to draw it.
    fn aggregate_functions() -> impl Strategy<Value = Vec<AggregateFn>> {
        (
            proptest::sample::subsequence(ALL_FN_KINDS.to_vec(), 2..=5),
            aggregate_column(),
            aggregate_column(),
            aggregate_column(),
            aggregate_column(),
        )
            .prop_map(|(kinds, sum_col, avg_col, min_col, max_col)| {
                kinds
                    .into_iter()
                    .map(|kind| match kind {
                        FnKind::Sum => AggregateFn::Sum(sum_col),
                        FnKind::Count => AggregateFn::Count,
                        FnKind::Avg => AggregateFn::Avg(avg_col),
                        FnKind::Min => AggregateFn::Min(min_col),
                        FnKind::Max => AggregateFn::Max(max_col),
                    })
                    .collect()
            })
    }

    /// Which shape a drawn definition takes (task B4): the existing
    /// `OneToOne` `total = c1 + c2` shape, or a new `Aggregate` shape.
    /// Weighted evenly so both shapes get substantial, comparable coverage
    /// across a run — this is the dimension `tests/coverage.rs`'s B4 floor
    /// tests sample against, so it must not be so lopsided that 500 samples
    /// has a real chance of missing one of the five aggregate functions.
    fn def_shape() -> impl Strategy<Value = DefShape> {
        prop_oneof![
            1 => Just(DefShape::OneToOne),
            1 => aggregate_functions().prop_map(|functions| DefShape::Aggregate { functions }),
        ]
    }

    /// One mutate targeting `seed_count` seeded rows. The primary key for
    /// `Update`/`Delete` is drawn from `1..=seed_count + 1`: values
    /// `1..=seed_count` hit a seeded (or otherwise still-live) row, and
    /// `seed_count + 1` deliberately misses (a source no-op) so the property
    /// exercises the "op that errors changed nothing" path (design doc §4).
    /// `DuplicateInsert`'s pk is drawn from `1..=seed_count` only — a pk
    /// that started out seeded. Whether it actually collides at apply time
    /// depends on whether an earlier mutate in the same draw already deleted
    /// it ([`build_program`] simulates this to expect the right outcome
    /// either way, see [`Mutate::DuplicateInsert`]); most of the time it is
    /// still live, so this is the generator's main source of genuine
    /// primary-key-violation `apply()` errors (issue #6's gap).
    fn mutate(seed_count: usize, awkward_values: bool) -> impl Strategy<Value = Mutate> {
        let pk = 1..=(seed_count as i64 + 1);
        let dup_pk = 1..=(seed_count as i64);
        prop_oneof![
            (pk.clone(), value(awkward_values), value(awkward_values))
                .prop_map(|(pk, c1, c2)| Mutate::Update { pk, c1, c2 }),
            pk.prop_map(|pk| Mutate::Delete { pk }),
            (dup_pk, value(awkward_values), value(awkward_values))
                .prop_map(|(pk, c1, c2)| Mutate::DuplicateInsert { pk, c1, c2 }),
        ]
    }

    /// One drawn table's seed rows and mutate stream (see [`TableSpec`]):
    /// seed `1..=MAX_SEED_ROWS` rows with random values (now including one
    /// `Text`/`Boolean`/`Uuid` value and one grain value per row, tasks
    /// B1/B4), then append `0..=MAX_MUTATES` mutates over the numeric columns
    /// only (the new columns are never mutated — see the module doc
    /// comment's scope cuts) — the per-table shape [`trivial_program_with`]
    /// always drew, now reusable once per table in a multi-table program
    /// (improvement-plan task B3).
    fn table_spec(awkward_values: bool) -> impl Strategy<Value = TableSpec> {
        (1..=MAX_SEED_ROWS)
            .prop_flat_map(move |seed_count| {
                let seeds = prop::collection::vec(
                    (value(awkward_values), value(awkward_values)),
                    seed_count,
                );
                let texts = prop::collection::vec(text_value(awkward_values), seed_count);
                let bools = prop::collection::vec(bool_value(awkward_values), seed_count);
                let uuids = prop::collection::vec(uuid_value(awkward_values), seed_count);
                let grains = prop::collection::vec(grain_value(), seed_count);
                let mutates =
                    prop::collection::vec(mutate(seed_count, awkward_values), 0..=MAX_MUTATES);
                (seeds, texts, bools, uuids, grains, mutates)
            })
            .prop_map(
                |(seed_values, text_values, bool_values, uuid_values, grain_values, mutates)| {
                    TableSpec {
                        seed_values,
                        text_values,
                        bool_values,
                        uuid_values,
                        grain_values,
                        mutates,
                    }
                },
            )
    }

    /// Draws a [`Program`] over `1..=MAX_TABLES` tables and `1..=MAX_DEFS`
    /// definitions (improvement-plan task B3): each table independently
    /// draws its own seed/mutate stream ([`table_spec`]), and each
    /// definition independently draws both *which* table it reads from —
    /// uniformly over `0..table_count`, with no bias toward distinct
    /// sources — and (task B4) *which shape* it takes ([`def_shape`]), so
    /// "two definitions sharing one source table" and "definitions spread
    /// across different tables" are both reachable in the same program,
    /// including a mix of `OneToOne` and `Aggregate` defs. Everything maps
    /// through [`build_program_multi_with_shapes`], so proptest's integrated
    /// shrinking reduces the def count, then the table count (both ahead of
    /// any table's row count, values, or mutate stream — the same
    /// `1..=N`-via-`prop_flat_map` idiom [`MAX_SEED_ROWS`]/[`MAX_MUTATES`]
    /// already use), toward the smallest reproducing program — a 1-table/
    /// 1-def counterexample is the readable one.
    ///
    /// `awkward_values` gates every other column's `NULL` draw (see
    /// [`value`], [`text_value`], [`bool_value`], [`uuid_value`]); it does not
    /// gate [`grain_value`] (which never draws `NULL` at all — see its own
    /// doc comment for the engine bug that finding forced this scope cut
    /// over), nor does it affect [`Mutate::DuplicateInsert`] or [`def_shape`]
    /// (which of a def's fields are `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over — a
    /// distinct, structural widening, always available regardless of the
    /// flag).
    pub fn trivial_program_with(awkward_values: bool) -> impl Strategy<Value = Program> {
        prop::collection::vec(table_spec(awkward_values), 1..=MAX_TABLES)
            .prop_flat_map(|tables| {
                let table_count = tables.len();
                let defs = prop::collection::vec((0..table_count, def_shape()), 1..=MAX_DEFS);
                (Just(tables), defs)
            })
            .prop_map(|(tables, defs)| build_program_multi_with_shapes(&tables, &defs))
    }

    /// The generator's default strategy: awkward values (NULLs) on, so real
    /// runs exercise them (design doc §3).
    pub fn trivial_program() -> impl Strategy<Value = Program> {
        trivial_program_with(true)
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{trivial_program, trivial_program_with};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_program_seeds_before_mutating() {
        let program = build_program(
            &[(Some(1), Some(2)), (Some(3), Some(4))],
            &[
                Mutate::Update {
                    pk: 1,
                    c1: Some(5),
                    c2: Some(6),
                },
                Mutate::Delete { pk: 2 },
            ],
        );
        assert_eq!(program.tables.len(), 1);
        assert_eq!(program.defs.len(), 1);
        // Two inserts (the seeds) come first, then the two mutates.
        assert_eq!(program.ops.len(), 4);
        assert!(matches!(program.ops[0], Op::Insert { .. }));
        assert!(matches!(program.ops[1], Op::Insert { .. }));
        assert!(matches!(program.ops[2], Op::Update { .. }));
        assert!(matches!(program.ops[3], Op::Delete { .. }));
    }

    #[test]
    fn seeded_rows_get_consecutive_primary_keys_from_one() {
        let program = build_program(
            &[(Some(0), Some(0)), (Some(0), Some(0)), (Some(0), Some(0))],
            &[],
        );
        let pks: Vec<&str> = program
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Insert { row, .. } => row.first().and_then(|(_, v)| v.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(pks, vec!["1", "2", "3"]);
    }

    #[test]
    fn the_single_def_is_a_one_to_one_numeric_add() {
        let program = build_program(&[(Some(1), Some(1))], &[]);
        let def = &program.defs[0];
        assert_eq!(def.key_space, KeySpace::OneToOne);
        assert_eq!(def.predicate, Predicate::True);
        // `total` plus one identity-passthrough field per new Text/Boolean/
        // Uuid column (task B1) — see `build_program_multi`'s doc comment.
        assert_eq!(def.fields.len(), 4);
        assert_eq!(def.fields[0].name, "total");
        assert!(matches!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                ..
            }
        ));
        for field in &def.fields[1..] {
            assert!(
                matches!(&field.expr, Expr::Column(name) if name == &field.name),
                "passthrough field {:?} must be a bare `Expr::Column` referencing its own name: \
                 {field:?}",
                field.name
            );
        }
    }

    /// A `None` seed value must render as SQL `NULL` (no bound text), not the
    /// literal string `"None"` or `"NULL"` — the awkward-value machinery
    /// (issue #7) reuses the same `Option<String>` = `NULL` convention
    /// [`crate::model::Op`] already documents.
    #[test]
    fn a_null_seed_value_renders_as_sql_null_not_a_string() {
        let program = build_program(&[(None, Some(4))], &[]);
        let Op::Insert { row, .. } = &program.ops[0] else {
            panic!("expected an insert");
        };
        let c1 = row.iter().find(|(name, _)| name == "c1").unwrap();
        assert_eq!(c1.1, None);
    }

    /// [`Mutate::DuplicateInsert`] renders to a second `Op::Insert` at a
    /// pk that already exists in the seed — the shape that gives `apply()` a
    /// real primary-key violation to fail on (issue #6's gap).
    #[test]
    fn duplicate_insert_renders_a_second_insert_at_the_same_pk() {
        let program = build_program(
            &[(Some(1), Some(2))],
            &[Mutate::DuplicateInsert {
                pk: 1,
                c1: Some(9),
                c2: Some(9),
            }],
        );
        assert_eq!(program.ops.len(), 2);
        let Op::Insert { row, .. } = &program.ops[1] else {
            panic!("expected the duplicate insert to render as Op::Insert");
        };
        let pk = row.first().unwrap();
        assert_eq!(pk.1.as_deref(), Some("1"));
    }

    /// `build_program`'s pk-liveness simulation must track a pk across
    /// *multiple* mutates in the same stream, not just whether it was
    /// originally seeded — these are fast, DB-free pins for sequences the
    /// slow DB-backed proptest property only exercises when it happens to
    /// draw them (issue #4/A1: an op's expected outcome must always match
    /// what real Postgres would do).
    mod pk_liveness {
        use super::*;

        #[test]
        fn update_after_delete_on_the_same_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::Update {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                ],
            );
            // ops: [seed insert, delete, update]
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }

        #[test]
        fn delete_after_delete_on_the_same_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[Mutate::Delete { pk: 1 }, Mutate::Delete { pk: 1 }],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }

        #[test]
        fn duplicate_insert_after_delete_revives_the_pk_and_succeeds() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                ],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::Succeeds);
        }

        #[test]
        fn a_second_duplicate_insert_against_a_revived_pk_fails_again() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(7),
                        c2: Some(7),
                    },
                ],
            );
            assert_eq!(program.ops[2].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[3].expect(), &OpOutcome::Fails);
        }

        #[test]
        fn duplicate_insert_against_a_still_live_seeded_pk_fails() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[Mutate::DuplicateInsert {
                    pk: 1,
                    c1: Some(9),
                    c2: Some(9),
                }],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Fails);
        }

        #[test]
        fn update_or_delete_on_an_unseeded_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Update {
                        pk: 2,
                        c1: Some(9),
                        c2: Some(9),
                    },
                    Mutate::Delete { pk: 2 },
                ],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::AffectsNoRows);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }
    }

    /// [`build_program_multi`]'s pk-liveness simulation must be *per table*,
    /// not shared across tables (improvement-plan task B3) — fast, DB-free
    /// pins for exactly the class of bug the task's own validation section
    /// calls out: multi-table is new surface where a liveness-tracking bug
    /// (or a pk-numbering bug) could slip in undetected without a test
    /// aimed at it specifically.
    mod pk_liveness_multi_table {
        use super::*;

        /// A `Delete` on table A's pk 1 must not affect table B's pk 1: an
        /// `Update` on table B's still-live pk 1, issued right after table
        /// A's delete, must still predict `Succeeds` — never `AffectsNoRows`
        /// — proving the two tables' `live` sets don't leak into each other.
        #[test]
        fn a_delete_on_one_table_does_not_affect_the_same_pk_on_another_table() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), Some(2))],
                        vec![Mutate::Delete { pk: 1 }],
                    ),
                    TableSpec::numeric_only(
                        vec![(Some(9), Some(9))],
                        vec![Mutate::Update {
                            pk: 1,
                            c1: Some(5),
                            c2: Some(5),
                        }],
                    ),
                ],
                &[0],
            );
            // ops: [seed A pk1, delete A pk1, seed B pk1, update B pk1]
            assert_eq!(program.tables.len(), 2);
            assert_eq!(program.ops.len(), 4);
            assert!(matches!(program.ops[0], Op::Insert { .. }));
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds); // delete A pk1
            assert!(matches!(program.ops[2], Op::Insert { .. }));
            assert_eq!(
                program.ops[3].expect(),
                &OpOutcome::Succeeds,
                "table B's pk 1 must still be live even though table A's pk 1 was deleted: {program:#?}"
            );
        }

        /// The mirror case: a `DuplicateInsert` on table A's pk 1 (a real
        /// primary-key violation there) must not make table B's pk 1 look
        /// non-live — an `Update` on table B's pk 1 right after must still
        /// predict `Succeeds`.
        #[test]
        fn a_duplicate_insert_on_one_table_does_not_affect_the_same_pk_on_another_table() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), Some(2))],
                        vec![Mutate::DuplicateInsert {
                            pk: 1,
                            c1: Some(9),
                            c2: Some(9),
                        }],
                    ),
                    TableSpec::numeric_only(
                        vec![(Some(3), Some(4))],
                        vec![Mutate::Update {
                            pk: 1,
                            c1: Some(5),
                            c2: Some(5),
                        }],
                    ),
                ],
                &[1],
            );
            // ops: [seed A pk1, dup-insert A pk1 (fails), seed B pk1, update B pk1]
            assert_eq!(program.ops[1].expect(), &OpOutcome::Fails);
            assert_eq!(
                program.ops[3].expect(),
                &OpOutcome::Succeeds,
                "table A's still-live pk 1 must not affect table B's own pk 1: {program:#?}"
            );
        }

        /// Each table's seeded pks start at 1 independently — table B's
        /// first seeded pk must render `"1"`, not `"3"` (i.e. not continue
        /// numbering after table A's two seeded rows).
        #[test]
        fn each_tables_seeded_pks_start_at_one_independent_of_earlier_tables() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(vec![(Some(1), Some(1)), (Some(2), Some(2))], vec![]),
                    TableSpec::numeric_only(vec![(Some(3), Some(3))], vec![]),
                ],
                &[0, 1],
            );
            let table_b = &program.tables[1];
            let pks_for_table_b: Vec<&str> = program
                .ops
                .iter()
                .filter_map(|op| match op {
                    Op::Insert { table, row, .. } if table == &table_b.name => {
                        row.first().and_then(|(_, v)| v.as_deref())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(
                pks_for_table_b,
                vec!["1"],
                "table B's own pk numbering must start at 1, independent of table A's row count: {program:#?}"
            );
        }

        /// Sanity check on the multi-def side of B3: two definitions can
        /// independently draw the *same* source table (fan-out), and each
        /// still gets its own target and its own `total = c1 + c2` shape.
        #[test]
        fn two_definitions_can_share_one_source_table() {
            let program = build_program_multi(
                &[TableSpec::numeric_only(vec![(Some(1), Some(2))], vec![])],
                &[0, 0],
            );
            assert_eq!(program.tables.len(), 1);
            assert_eq!(program.defs.len(), 2);
            assert_eq!(program.defs[0].source, program.tables[0].name);
            assert_eq!(program.defs[1].source, program.tables[0].name);
            assert_ne!(
                program.defs[0].target, program.defs[1].target,
                "two defs over the same source must still get distinct targets"
            );
        }
    }
}
