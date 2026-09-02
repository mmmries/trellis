//! Program generation (design doc §1/§3): generators producing only valid
//! [`crate::model::Program`]s. Makes no engine calls — the module boundary
//! this crate enforces is that only [`crate::backend`] drives the engine's
//! pipeline.
//!
//! Today's scope is the *trivial* generator (issue #6): one source table, one
//! 1-1 numeric-`+` definition, and a seed-before-mutate op stream. The
//! proptest [`Strategy`]s that draw these live behind the `proptest` feature
//! (see `Cargo.toml`); the pure [`build_program`] builder they map onto is
//! always available so a hand-built pin can reuse the exact same shape without
//! pulling in proptest.
//!
//! [`Strategy`]: proptest::strategy::Strategy

use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};

use crate::model::{NamePool, Op, Program, Table};

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

/// One post-seed mutation, parameterized only by primary-key integers and
/// (for updates) new field values — the plain-data draw a proptest strategy
/// shrinks, kept separate from the [`Op`] it renders into so the builder stays
/// proptest-free.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutate {
    /// Set `c1`/`c2` on row `pk`. When `pk` names no seeded row this is a
    /// no-op at the source (Postgres updates zero rows, no error), which is
    /// exactly the "operation errors are checked, not swallowed" case the
    /// convergence property must still converge on (design doc §4).
    Update { pk: i64, c1: i64, c2: i64 },
    /// Delete row `pk`. A `pk` naming no seeded row is likewise a no-op.
    Delete { pk: i64 },
}

/// Builds the trivial program from already-drawn data: `seed_values[i]` is the
/// `(c1, c2)` pair for seeded primary key `i + 1`, and `mutates` are appended
/// after every seed insert (seed-before-mutate, design doc §3, so every update
/// and delete has real rows to hit).
///
/// The schema and definition are fixed — one source table `t0` with a numeric
/// primary key `c0` and two numeric columns `c1`/`c2`, one 1-1 target `t1`
/// computing `c1 + c2` — so the only thing that varies (and shrinks) between
/// cases is the data and the mutate stream.
pub fn build_program(seed_values: &[(i64, i64)], mutates: &[Mutate]) -> Program {
    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let c1 = source.columns[1].name.clone();
    let c2 = source.columns[2].name.clone();
    let target = pool.next_table_name();

    let def = TransformDef {
        target,
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(c1.clone())),
                rhs: Box::new(Expr::Column(c2.clone())),
            },
        }],
        predicate: Predicate::True,
    };

    let mut ops = Vec::with_capacity(seed_values.len() + mutates.len());
    for (i, (a, b)) in seed_values.iter().enumerate() {
        let pk = (i + 1) as i64;
        ops.push(Op::Insert {
            table: source.name.clone(),
            row: vec![
                (source.pk_col.clone(), Some(pk.to_string())),
                (c1.clone(), Some(a.to_string())),
                (c2.clone(), Some(b.to_string())),
            ],
        });
    }
    for mutate in mutates {
        ops.push(match mutate {
            Mutate::Update { pk, c1: a, c2: b } => Op::Update {
                table: source.name.clone(),
                pk: pk.to_string(),
                changes: vec![
                    (c1.clone(), Some(a.to_string())),
                    (c2.clone(), Some(b.to_string())),
                ],
            },
            Mutate::Delete { pk } => Op::Delete {
                table: source.name.clone(),
                pk: pk.to_string(),
            },
        });
    }

    Program {
        tables: vec![source],
        defs: vec![def],
        ops,
    }
}

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use proptest::prelude::*;

    /// A single calculated-field / column value: a small non-negative integer
    /// (see [`VALUE_MAX`]'s numeric-path pairing).
    fn value() -> impl Strategy<Value = i64> {
        0..=VALUE_MAX
    }

    /// One mutate targeting `seed_count` seeded rows. The primary key is drawn
    /// from `1..=seed_count + 1`: values `1..=seed_count` hit a seeded row, and
    /// `seed_count + 1` deliberately misses (a source no-op) so the property
    /// exercises the "op that errors changed nothing" path (design doc §4).
    fn mutate(seed_count: usize) -> impl Strategy<Value = Mutate> {
        let pk = 1..=(seed_count as i64 + 1);
        prop_oneof![
            (pk.clone(), value(), value()).prop_map(|(pk, c1, c2)| Mutate::Update { pk, c1, c2 }),
            pk.prop_map(|pk| Mutate::Delete { pk }),
        ]
    }

    /// Draws a trivial [`Program`]: seed `1..=MAX_SEED_ROWS` rows with random
    /// values, then append `0..=MAX_MUTATES` mutates over them. Everything maps
    /// through [`build_program`], so proptest's integrated shrinking minimizes
    /// the row count, the values, and the mutate stream toward the smallest
    /// reproducing program.
    pub fn trivial_program() -> impl Strategy<Value = Program> {
        (1..=MAX_SEED_ROWS)
            .prop_flat_map(|seed_count| {
                let seeds = prop::collection::vec((value(), value()), seed_count);
                let mutates = prop::collection::vec(mutate(seed_count), 0..=MAX_MUTATES);
                (seeds, mutates)
            })
            .prop_map(|(seeds, mutates)| build_program(&seeds, &mutates))
    }
}

#[cfg(feature = "proptest")]
pub use strategy::trivial_program;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_program_seeds_before_mutating() {
        let program = build_program(
            &[(1, 2), (3, 4)],
            &[
                Mutate::Update {
                    pk: 1,
                    c1: 5,
                    c2: 6,
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
        let program = build_program(&[(0, 0), (0, 0), (0, 0)], &[]);
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
        let program = build_program(&[(1, 1)], &[]);
        let def = &program.defs[0];
        assert_eq!(def.key_space, KeySpace::OneToOne);
        assert_eq!(def.predicate, Predicate::True);
        assert_eq!(def.fields.len(), 1);
        assert!(matches!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                ..
            }
        ));
    }
}
