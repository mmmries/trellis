//! Erroring rows, drawn on purpose: the held-key tier's programs
//! (`docs/generative-test-suite.md` §10, "The oracle and erroring rows").
//!
//! A raising definition's derived field divides by `<slot> - K`, with no
//! `NULLIF`, so Postgres raises `division by zero` for exactly the rows whose
//! `slot` column is `K`. The engine holds those keys in quarantine, and the
//! held-key run ([`crate::run::run_convergence_with_held_keys`]) models them.
//! Which rows hit `K` is drawn, not left to chance: [`aim_rows`] sets the
//! column to `K` where a draw says so and moves every other value off it, so
//! the rate is the draw's, whatever the expression. Every other derived field
//! keeps the raise-free guard ([`crate::expr_gen::Profile::END_TO_END`]).

use trellis::dev::defs::ast::{Expr, Operator};

use super::{DerivedShape, Mutate, TableSpec, VALUE_MAX};

/// Which numeric column a raising field divides by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivisorSlot {
    C1,
    C2,
}

impl DivisorSlot {
    /// The [`super::DERIVED_SLOTS`] name of the column.
    fn slot(self) -> &'static str {
        match self {
            DivisorSlot::C1 => super::DERIVED_SLOTS[0].0,
            DivisorSlot::C2 => super::DERIVED_SLOTS[1].0,
        }
    }
}

/// `dividend / (<divisor> - trigger)`: raises `division by zero` for a row
/// whose `divisor` column is `trigger` and whose `dividend` is a number (a
/// `NULL` or `NaN` dividend gives `NULL` or `NaN`), and computes for every
/// other row.
pub fn raising_shape(dividend: Expr, divisor: DivisorSlot, trigger: i64) -> DerivedShape {
    DerivedShape::from_expr(Expr::BinaryOp {
        op: Operator::Divide,
        lhs: Box::new(dividend),
        rhs: Box::new(Expr::BinaryOp {
            op: Operator::Subtract,
            lhs: Box::new(Expr::Column(divisor.slot().to_string())),
            rhs: Box::new(Expr::NumberLiteral(trigger.to_string())),
        }),
    })
}

/// A value next to `trigger` in the `0..=VALUE_MAX` range, for a value that
/// must not hit it.
fn off(trigger: i64) -> i64 {
    (trigger + 1) % (VALUE_MAX + 1)
}

/// Aims `spec`'s writes of `divisor`'s column at `trigger`: the `i`th write
/// of that column (seeds first, then mutates, in order) is set to `trigger`
/// when `hits[i]` is true, and moved off it otherwise. A write past the end
/// of `hits` is a miss. Returns how many writes hit.
pub fn aim_rows(spec: &mut TableSpec, divisor: DivisorSlot, trigger: i64, hits: &[bool]) -> usize {
    let mut index = 0;
    let mut hit_count = 0;
    let mut aim = |value: &mut Option<i64>| {
        let hit = hits.get(index).copied().unwrap_or(false);
        index += 1;
        if hit {
            *value = Some(trigger);
            hit_count += 1;
        } else if *value == Some(trigger) {
            *value = Some(off(trigger));
        }
    };
    for (c1, c2) in &mut spec.seed_values {
        aim(match divisor {
            DivisorSlot::C1 => c1,
            DivisorSlot::C2 => c2,
        });
    }
    for mutate in &mut spec.mutates {
        match (mutate, divisor) {
            (
                Mutate::Update { c1: col, .. } | Mutate::DuplicateInsert { c1: col, .. },
                DivisorSlot::C1,
            )
            | (
                Mutate::Update { c2: col, .. } | Mutate::DuplicateInsert { c2: col, .. },
                DivisorSlot::C2,
            ) => aim(col),
            (Mutate::ToggleNull { value, .. }, DivisorSlot::C1) => {
                let mut v = Some(*value);
                aim(&mut v);
                *value = v.expect("aim keeps a value a value");
            }
            _ => {}
        }
    }
    hit_count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> TableSpec {
        TableSpec {
            seed_values: vec![(Some(5), Some(5)), (Some(1), Some(2)), (None, Some(5))],
            text_values: vec![None; 3],
            bool_values: vec![None; 3],
            uuid_values: vec![None; 3],
            grain_values: vec![None; 3],
            mutates: vec![
                Mutate::Update {
                    pk: 2,
                    c1: Some(5),
                    c2: Some(5),
                },
                Mutate::ToggleNull { pk: 1, value: 5 },
                Mutate::Delete { pk: 1 },
            ],
        }
    }

    #[test]
    fn aiming_sets_the_hits_and_moves_every_other_value_off_the_trigger() {
        let mut spec = spec();
        let hits = aim_rows(&mut spec, DivisorSlot::C2, 5, &[false, true, false, true]);
        assert_eq!(hits, 2);
        assert_eq!(
            spec.seed_values,
            vec![(Some(5), Some(6)), (Some(1), Some(5)), (None, Some(6))]
        );
        assert_eq!(
            spec.mutates[0],
            Mutate::Update {
                pk: 2,
                c1: Some(5),
                c2: Some(5),
            }
        );
        // `c1` is not the divisor here: its values stay.
        assert_eq!(spec.mutates[1], Mutate::ToggleNull { pk: 1, value: 5 });
    }

    #[test]
    fn aiming_at_c1_reaches_a_toggle_null_value_and_wraps_off_the_top() {
        let mut spec = spec();
        spec.mutates[1] = Mutate::ToggleNull {
            pk: 1,
            value: VALUE_MAX,
        };
        let hits = aim_rows(&mut spec, DivisorSlot::C1, VALUE_MAX, &[]);
        assert_eq!(hits, 0);
        assert_eq!(spec.mutates[1], Mutate::ToggleNull { pk: 1, value: 0 });
    }

    #[test]
    fn a_raising_shape_divides_by_its_slot_less_the_trigger() {
        let shape = raising_shape(Expr::NumberLiteral("1".to_string()), DivisorSlot::C2, 7);
        let expr = shape.build_expr("a", "b", "t", "f");
        assert_eq!(
            crate::expr_gen::render_text(&expr, crate::expr_gen::TextStyle::default()),
            "1 / (b - 7)"
        );
    }
}
