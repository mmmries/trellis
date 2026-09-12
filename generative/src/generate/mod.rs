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
//! - At the time B1 landed, no operators/functions/comparisons over the new
//!   types were drawn yet (that widening was gated on a precedence-table
//!   prerequisite, deferred to a later session). Improvement-plan task
//!   **B2** ("Operators, functions, and literals") is that later session: it
//!   adds `Operator::GreaterThan`, `Expr::NumberLiteral`/`Expr::StringLiteral`,
//!   and the five scalar functions (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/
//!   `REGEXP_COUNT`/`COALESCE`) as one additional, nested "derived" field per
//!   definition — see [`DerivedShape`] and [`build_program_multi_with_derived`].
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
    /// Mutates appended after this table's seed inserts (seed-before-mutate,
    /// design doc §3), targeting only this table's own pks. Never touches
    /// the `Text`/`Boolean`/`Uuid` columns above — see the module doc
    /// comment's B1 scope cut.
    pub mutates: Vec<Mutate>,
}

impl TableSpec {
    /// Builds a `TableSpec` whose `Text`/`Boolean`/`Uuid` columns (task B1)
    /// are all `NULL` for every seed row — a convenience for callers that
    /// only care about the numeric seed/mutate shape (e.g. the pk-liveness
    /// unit tests below, and [`build_program`]'s two-argument convenience
    /// wrapper), sparing them from hand-counting a matching-length `NULL`
    /// vector for each of the three new columns.
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
pub fn build_program_multi(tables: &[TableSpec], def_sources: &[usize]) -> Program {
    assert!(
        !tables.is_empty(),
        "build_program_multi: a program must draw at least one table"
    );
    assert!(
        !def_sources.is_empty(),
        "build_program_multi: a program must draw at least one definition"
    );

    let mut pool = NamePool::new();
    let mut built_tables = Vec::with_capacity(tables.len());
    let mut ops = Vec::new();

    for spec in tables {
        let row_count = spec.seed_values.len();
        assert_eq!(
            spec.text_values.len(),
            row_count,
            "build_program_multi: text_values must have one entry per seed row \
             ({row_count} seed rows, {} text values) — a generator bug",
            spec.text_values.len()
        );
        assert_eq!(
            spec.bool_values.len(),
            row_count,
            "build_program_multi: bool_values must have one entry per seed row \
             ({row_count} seed rows, {} bool values) — a generator bug",
            spec.bool_values.len()
        );
        assert_eq!(
            spec.uuid_values.len(),
            row_count,
            "build_program_multi: uuid_values must have one entry per seed row \
             ({row_count} seed rows, {} uuid values) — a generator bug",
            spec.uuid_values.len()
        );

        // Task B1: every table gets a Text/Boolean/Uuid column, always —
        // see the module doc comment. `Table::new` builds columns in the
        // order given, so `columns[3..=5]` are these three, after the pk
        // (`columns[0]`) and `c1`/`c2` (`columns[1..=2]`).
        let source = Table::new(
            &mut pool,
            &[
                ValueType::Numeric,
                ValueType::Numeric,
                ValueType::Text,
                ValueType::Boolean,
                ValueType::Uuid,
            ],
        );
        let c1 = source.columns[1].name.clone();
        let c2 = source.columns[2].name.clone();
        let text_col = source.columns[3].name.clone();
        let bool_col = source.columns[4].name.clone();
        let uuid_col = source.columns[5].name.clone();

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

    let defs = def_sources
        .iter()
        .map(|&idx| {
            let source = built_tables.get(idx).unwrap_or_else(|| {
                panic!(
                    "build_program_multi: def_sources index {idx} out of range for {} tables — \
                     a generator bug",
                    built_tables.len()
                )
            });
            let c1 = source.columns[1].name.clone();
            let c2 = source.columns[2].name.clone();
            let text_col = source.columns[3].name.clone();
            let bool_col = source.columns[4].name.clone();
            let uuid_col = source.columns[5].name.clone();
            TransformDef {
                target: pool.next_table_name(),
                source: source.name.clone(),
                key_space: KeySpace::OneToOne,
                fields: vec![
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
                    // table gets the same three, determined by the table's
                    // shape rather than drawn independently per def
                    // (matching how `total` already works).
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

/// Improvement-plan task B2's D0 investigation ("with `>` and the five new
/// functions in play, is there now a genuine, row-data-dependent way for a
/// *valid* (post-`validate()`) definition to still error at eval time?").
///
/// **Finding: no.** Every shape [`DerivedShape`] draws stays eval-time-
/// infallible for the *values* this generator actually produces, the same
/// way AVG's hidden count-partial already keeps `+`/`AVG` divide-by-zero-free
/// (see this module's numeric-path pairing note above). Walked one shape at a
/// time (`engine/src/defs/eval.rs`'s `apply_operator`/`apply_function`):
///
/// - **`Operator::GreaterThan`**: both operands are `engine::numeric::Numeric`
///   (arbitrary-precision decimal, like `+`), and `Numeric::compare` never
///   errors — there is no overflow, division, or precision loss to trigger
///   one. `Postgres::numeric >` cannot error either.
/// - **`STRPOS`**: `haystack.find(needle)` is a total function over any two
///   `&str`s (`""`, no match, unicode — all handled, see
///   `engine/tests/defs_text_functions.rs`'s `STRPOS_CASES`); it always
///   returns a `usize`, never fails. Postgres's `strpos` is equally total.
/// - **`OCTET_LENGTH`/`CHAR_LENGTH`**: `str::len`/`str::chars().count()` never
///   fail for any valid Rust `String` (which every `Text` value already is,
///   having come from a Postgres `text` column — always valid UTF-8).
/// - **`REGEXP_COUNT`**: the *only* eval-time failure mode `eval::regexp_count`
///   has is `Regex::new(pattern)` failing to compile — and `validate.rs`'s
///   `validate_regexp_pattern` already compiles the pattern with the exact
///   same `regex` crate at *validate* time and rejects the definition before
///   it ever reaches eval, for every pattern this generator draws (a string
///   literal, never a column reference, so it's always the specific literal
///   `validate` checked). A validated definition can therefore never hit a
///   pattern-compile failure at eval time. The one caveat worth naming for a
///   future widening: Rust's `regex` crate and Postgres's own ARE dialect are
///   different engines, so a pattern the Rust crate compiles is not
///   guaranteed to be one Postgres's `regexp_count` also accepts (or accepts
///   with the same semantics) — a *dialect* mismatch, not a data-dependent
///   one. This generator sidesteps that risk entirely by only ever drawing
///   `REGEXP_COUNT` patterns from `strategy::REGEXP_COUNT_PATTERN_POOL`, the
///   same dialect-common subset (literal text, `.`, `*`, `+`, `?`, `[...]`,
///   `|`, `^`, `$`) `engine/tests/defs_text_functions.rs` already vets against
///   real Postgres — never an arbitrary pattern that might expose that gap.
///   A future widening that draws *arbitrary* regex syntax would need to
///   cross exactly this boundary (and would be the first place a genuine
///   SQL-oracle-vs-engine divergence, not a row-data-dependent eval error,
///   could show up).
/// - **`COALESCE`**: pure control flow (`eval_expr`'s `COALESCE` arm
///   short-circuits on the first non-`None` argument) — there is no
///   computation of its own to fail.
///
/// Since no shape here has a real, generator-reachable, row-data-dependent
/// error path, D0's "error-to-NULL oracle rendering" design (the plan's
/// option 2) has nothing to quarantine yet and is **not built** in this
/// session — building it now would be unused machinery with nothing to
/// exercise it, the same call this module's original numeric-`+`-only D0
/// pass made, just re-verified against the wider operator/function set this
/// task adds. The day a future widening draws something genuinely
/// data-dependent-fallible (an arbitrary regex pattern against arbitrary
/// text, a numeric cast that can overflow a *narrower* column type, division
/// by a column that can be zero, ...), this is the comment to update and the
/// decision to revisit.
#[derive(Debug, Clone, PartialEq)]
pub enum DerivedShape {
    /// `STRPOS(<text_col>, '<needle>')` — Numeric.
    Strpos { needle: String },
    /// `OCTET_LENGTH(<text_col>)` — Numeric.
    OctetLength,
    /// `CHAR_LENGTH(<text_col>)` — Numeric.
    CharLength,
    /// `REGEXP_COUNT(<text_col>, '<pattern>')` — Numeric. `pattern` is always
    /// one of `strategy::REGEXP_COUNT_PATTERN_POOL` — see this enum's own
    /// doc comment (the D0 finding) for why that pool, specifically, is what
    /// keeps this shape eval-time-infallible.
    RegexpCount { pattern: String },
    /// `COALESCE(<c1>, <fallback>)` — Numeric.
    CoalesceNumeric { fallback: i64 },
    /// `<c1> > <c2>` — Boolean; the plain, unnested `Operator::GreaterThan`
    /// case.
    PlainGreaterThan,
    /// `(<c1> + <c2>) > <c1>` — Boolean, depth 3: a `BinaryOp` nested inside
    /// another `BinaryOp` (the `c1 + c2 > c1` shape improvement-plan task B2
    /// names explicitly).
    ArithmeticGreaterThan,
    /// `STRPOS(<text_col>, '<needle>') > <threshold>` — Boolean, depth 3: a
    /// `FunctionCall` nested inside a `BinaryOp` (the `STRPOS(...) > 0` shape
    /// improvement-plan task B2 names explicitly) — the mixed
    /// operator-and-function nesting `tests/convergence.rs`'s hand-built pin
    /// exercises end-to-end.
    StrposGreaterThan { needle: String, threshold: i64 },
}

impl DerivedShape {
    /// A `STRPOS(<text_col>, '<needle>')` call, shared by [`Self::Strpos`]
    /// and [`Self::StrposGreaterThan`] so the two variants can't drift.
    fn strpos_call(text_col: &str, needle: &str) -> Expr {
        Expr::FunctionCall {
            name: "STRPOS".to_string(),
            args: vec![
                Expr::Column(text_col.to_string()),
                Expr::StringLiteral(needle.to_string()),
            ],
        }
    }

    /// Builds this shape's [`Expr`] tree against a table's own `c1`/`c2`
    /// (numeric) and `text_col` (text) column names — the same three columns
    /// [`build_program_multi_with_derived`] already threads through for the
    /// `total`/passthrough fields.
    pub fn build_expr(&self, c1: &str, c2: &str, text_col: &str) -> Expr {
        match self {
            DerivedShape::Strpos { needle } => Self::strpos_call(text_col, needle),
            DerivedShape::OctetLength => Expr::FunctionCall {
                name: "OCTET_LENGTH".to_string(),
                args: vec![Expr::Column(text_col.to_string())],
            },
            DerivedShape::CharLength => Expr::FunctionCall {
                name: "CHAR_LENGTH".to_string(),
                args: vec![Expr::Column(text_col.to_string())],
            },
            DerivedShape::RegexpCount { pattern } => Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![
                    Expr::Column(text_col.to_string()),
                    Expr::StringLiteral(pattern.clone()),
                ],
            },
            DerivedShape::CoalesceNumeric { fallback } => Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![
                    Expr::Column(c1.to_string()),
                    Expr::NumberLiteral(fallback.to_string()),
                ],
            },
            DerivedShape::PlainGreaterThan => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column(c1.to_string())),
                rhs: Box::new(Expr::Column(c2.to_string())),
            },
            DerivedShape::ArithmeticGreaterThan => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column(c1.to_string())),
                    rhs: Box::new(Expr::Column(c2.to_string())),
                }),
                rhs: Box::new(Expr::Column(c1.to_string())),
            },
            DerivedShape::StrposGreaterThan { needle, threshold } => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Self::strpos_call(text_col, needle)),
                rhs: Box::new(Expr::NumberLiteral(threshold.to_string())),
            },
        }
    }
}

/// Improvement-plan task B2: builds on [`build_program_multi`] by appending
/// one more calculated field (named `"derived"`) to every definition, drawn
/// from [`DerivedShape`] — the widening that exercises `Operator::GreaterThan`,
/// `Expr::NumberLiteral`/`Expr::StringLiteral`, the five scalar functions
/// (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/`REGEXP_COUNT`/`COALESCE`), and a
/// nested (depth >= 2) expression tree — none of which [`build_program_multi`]
/// itself draws.
///
/// Deliberately layered *on top of* [`build_program_multi`] (calling it, then
/// pushing one field per def) rather than folded into it: every existing
/// caller of `build_program_multi` — every hand-built pin across
/// `generative/tests/*.rs`, this module's own unit tests, `tests/coverage.rs`'s
/// exact-field-count assertions — keeps its exact prior behavior (task B3's
/// fixed `total`/passthrough shape, untouched), and only the (new) default
/// proptest strategy (see `strategy::trivial_program_with`) actually draws a
/// `derived` field.
///
/// Panics if `derived.len() != def_sources.len()` — a generator bug, matching
/// this module's other length-contract checks ([`TableSpec`]'s
/// `text_values`/`bool_values`/`uuid_values`).
pub fn build_program_multi_with_derived(
    tables: &[TableSpec],
    def_sources: &[usize],
    derived: &[DerivedShape],
) -> Program {
    assert_eq!(
        derived.len(),
        def_sources.len(),
        "build_program_multi_with_derived: derived must have one entry per definition \
         ({} definitions, {} derived shapes) — a generator bug",
        def_sources.len(),
        derived.len()
    );

    let mut program = build_program_multi(tables, def_sources);

    // Looked up by table name up front, before the mutable loop over
    // `program.defs` below, so the two loops don't need to borrow
    // `program.tables` and `program.defs` simultaneously.
    let columns_by_table: std::collections::HashMap<String, (String, String, String)> = program
        .tables
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                (
                    table.columns[1].name.clone(),
                    table.columns[2].name.clone(),
                    table.columns[3].name.clone(),
                ),
            )
        })
        .collect();

    for (def, shape) in program.defs.iter_mut().zip(derived) {
        let (c1, c2, text_col) = columns_by_table.get(&def.source).unwrap_or_else(|| {
            panic!(
                "build_program_multi_with_derived: def.source {:?} names no table in the \
                 program — a generator bug",
                def.source
            )
        });
        def.fields.push(FieldDef {
            name: "derived".to_string(),
            expr: shape.build_expr(c1, c2, text_col),
        });
    }

    program
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

    /// A short ASCII string, drawn from the same alphabet as [`plain_text`]
    /// (improvement-plan task B2), used as `STRPOS`'s needle argument. Kept
    /// in that alphabet (rather than fully arbitrary text) so a meaningful
    /// fraction of draws actually land a hit against a table's `Text` column
    /// (also drawn from [`plain_text`]'s alphabet) — a `STRPOS` that only
    /// ever returns `0` would still be correct, but a needle that sometimes
    /// hits is better coverage of the function's non-zero branch. Can be
    /// empty (Postgres's own `strpos(x, '') = 1` convention).
    fn strpos_needle() -> BoxedStrategy<String> {
        "[a-zA-Z0-9 ]{0,3}".boxed()
    }

    /// Regex patterns [`DerivedShape::RegexpCount`] draws from — restricted
    /// to syntax common to Rust's `regex` crate and Postgres's default ARE
    /// dialect (literal text, `.`, `*`, `+`, `?`, `[...]`, `|`, `^`, `$`),
    /// the exact same restriction `engine/tests/defs_text_functions.rs`'s
    /// `REGEXP_COUNT_METACHARACTER_CASES` already vets against real
    /// Postgres. See [`DerivedShape`]'s doc comment (the D0 finding) for why
    /// this specific restriction is what keeps `REGEXP_COUNT` eval-time-
    /// infallible for what this generator draws — a pattern outside this
    /// pool could in principle compile under the `regex` crate (so pass
    /// `validate()`) yet mean something different, or nothing, under
    /// Postgres's own ARE dialect, which is a dialect-mismatch risk this
    /// pool exists specifically to avoid.
    const REGEXP_COUNT_PATTERN_POOL: &[&str] = &[
        "a", "o", "e", "a.c", "colou?r", "cat|dog", "[a-z]+", "^a", "a$",
    ];

    fn regexp_pattern() -> BoxedStrategy<String> {
        proptest::sample::select(REGEXP_COUNT_PATTERN_POOL)
            .prop_map(str::to_string)
            .boxed()
    }

    /// Draws one [`DerivedShape`] (improvement-plan task B2), equally
    /// weighted across every variant — each of the five scalar functions,
    /// the plain and nested `Operator::GreaterThan` shapes, all reachable
    /// with the same probability, since the coverage floor
    /// (`tests/coverage.rs`) needs every one of them to show up across a
    /// bounded number of samples, not just the common case.
    fn derived_shape() -> impl Strategy<Value = DerivedShape> {
        prop_oneof![
            strpos_needle().prop_map(|needle| DerivedShape::Strpos { needle }),
            Just(DerivedShape::OctetLength),
            Just(DerivedShape::CharLength),
            regexp_pattern().prop_map(|pattern| DerivedShape::RegexpCount { pattern }),
            (0..=VALUE_MAX).prop_map(|fallback| DerivedShape::CoalesceNumeric { fallback }),
            Just(DerivedShape::PlainGreaterThan),
            Just(DerivedShape::ArithmeticGreaterThan),
            (strpos_needle(), 0..=VALUE_MAX).prop_map(|(needle, threshold)| {
                DerivedShape::StrposGreaterThan { needle, threshold }
            }),
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
    /// `Text`/`Boolean`/`Uuid` value per row, task B1), then append
    /// `0..=MAX_MUTATES` mutates over the numeric columns only (the new
    /// columns are never mutated — see the module doc comment's scope cut) —
    /// the per-table shape [`trivial_program_with`] always drew, now
    /// reusable once per table in a multi-table program (improvement-plan
    /// task B3).
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
                let mutates =
                    prop::collection::vec(mutate(seed_count, awkward_values), 0..=MAX_MUTATES);
                (seeds, texts, bools, uuids, mutates)
            })
            .prop_map(
                |(seed_values, text_values, bool_values, uuid_values, mutates)| TableSpec {
                    seed_values,
                    text_values,
                    bool_values,
                    uuid_values,
                    mutates,
                },
            )
    }

    /// Draws a [`Program`] over `1..=MAX_TABLES` tables and `1..=MAX_DEFS`
    /// definitions (improvement-plan task B3): each table independently
    /// draws its own seed/mutate stream ([`table_spec`]), and each
    /// definition independently draws *which* table it reads from —
    /// uniformly over `0..table_count`, with no bias toward distinct
    /// sources — so "two definitions sharing one source table" and
    /// "definitions spread across different tables" are both reachable in
    /// the same run, including a mix of both in one program. Everything maps
    /// through [`build_program_multi`], so proptest's integrated shrinking
    /// reduces the def count, then the table count (both ahead of any
    /// table's row count, values, or mutate stream — the same
    /// `1..=N`-via-`prop_flat_map` idiom [`MAX_SEED_ROWS`]/[`MAX_MUTATES`]
    /// already use), toward the smallest reproducing program — a 1-table/
    /// 1-def counterexample is the readable one.
    ///
    /// `awkward_values` gates only the *value* draws (see [`value`]); it does
    /// not affect [`Mutate::DuplicateInsert`], which is a distinct widening
    /// (a real `apply()` failure, not an awkward value) always available. It
    /// likewise does not gate [`DerivedShape`] (improvement-plan task B2):
    /// every definition always gets one `derived` field, the same
    /// "unconditional, not a probabilistically-drawn dimension" choice task
    /// B1 made for the `Text`/`Boolean`/`Uuid` columns above — see
    /// [`build_program_multi_with_derived`].
    pub fn trivial_program_with(awkward_values: bool) -> impl Strategy<Value = Program> {
        prop::collection::vec(table_spec(awkward_values), 1..=MAX_TABLES)
            .prop_flat_map(|tables| {
                let def_sources = prop::collection::vec(0..tables.len(), 1..=MAX_DEFS);
                (Just(tables), def_sources)
            })
            .prop_flat_map(|(tables, def_sources)| {
                let derived = prop::collection::vec(derived_shape(), def_sources.len());
                (Just(tables), Just(def_sources), derived)
            })
            .prop_map(|(tables, def_sources, derived)| {
                build_program_multi_with_derived(&tables, &def_sources, &derived)
            })
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
