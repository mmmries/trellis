//! A green run is currently a single bit: nobody can see that a `Delete` op
//! stopped being drawn, or that a run drew zero mutates, or that the
//! op-outcome paths added in A1 (`OpOutcome::Fails`/`AffectsNoRows`) stopped
//! occurring at all (`local_docs/generative-suite-improvement-plan.md` §A2).
//! [`Coverage`] is the accumulator that makes a run report what it actually
//! exercised: it walks a [`crate::model::Program`]'s plain-data structure —
//! no database access — so the exact same accounting works both against
//! programs actually driven through a backend (`tests/convergence.rs`'s
//! `Harness`) and against raw strategy draws sampled with no cluster at all
//! (`tests/coverage.rs`'s floor test).

use std::collections::{HashMap, HashSet};
use std::fmt;

use engine::defs::ast::{Expr, KeySpace, Operator, ValueType};

use crate::model::{Op, OpOutcome, Program};

/// Per-run coverage tallies over any number of [`Program`]s. Every field is
/// keyed on the `&'static str` name of the variant it tallies, not the
/// engine/model enum itself — [`ValueType`] in particular has no `Hash` impl
/// (it's an engine type, not owned by this crate), so this follows the same
/// name-mapping convention `tests/coverage.rs`'s `sorted_value_types` helper
/// already uses.
#[derive(Debug, Default)]
pub struct Coverage {
    /// Number of `record_program` calls, i.e. cases accounted for.
    pub cases: usize,
    /// `"Insert"` / `"Update"` / `"Delete"`, tallied from [`crate::model::Op`].
    pub ops_by_kind: HashMap<&'static str, usize>,
    /// `"Succeeds"` / `"Fails"` / `"AffectsNoRows"` / `"AnyOf"`, tallied from
    /// each op's [`Op::expect`] (added in A1).
    pub ops_by_outcome: HashMap<&'static str, usize>,
    /// Which [`Expr`] variant names appear across every `FieldDef.expr` on
    /// every def, walked recursively.
    pub expr_shapes: HashSet<&'static str>,
    /// Which [`Operator`] names appear on any `Expr::BinaryOp` encountered
    /// while walking `expr_shapes`.
    pub operators: HashSet<&'static str>,
    /// Which [`ValueType`] names appear on any column of any table.
    pub types_exercised: HashSet<&'static str>,
    /// `"OneToOne"` / `"Aggregate"`, tallied from each def's [`KeySpace`].
    pub key_spaces: HashMap<&'static str, usize>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tallies one program into this accumulator: one case, its ops (by kind
    /// and by expected outcome), every calculated field's expression shape
    /// and operator across `program.defs`, every column's scalar type across
    /// `program.tables`, and every def's key-space.
    pub fn record_program(&mut self, program: &Program) {
        self.cases += 1;

        for op in &program.ops {
            *self.ops_by_kind.entry(op_kind(op)).or_insert(0) += 1;
            *self
                .ops_by_outcome
                .entry(op_outcome_name(op.expect()))
                .or_insert(0) += 1;
        }

        for def in &program.defs {
            *self
                .key_spaces
                .entry(key_space_name(&def.key_space))
                .or_insert(0) += 1;
            for field in &def.fields {
                self.record_expr(&field.expr);
            }
        }

        for table in &program.tables {
            for column in &table.columns {
                self.types_exercised
                    .insert(value_type_name(column.value_type));
            }
        }
    }

    /// Recursively tallies `expr`'s own shape and, for `BinaryOp`, its
    /// operator, then descends into its subexpressions.
    fn record_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::Column(_) => {
                self.expr_shapes.insert("Column");
            }
            Expr::NumberLiteral(_) => {
                self.expr_shapes.insert("NumberLiteral");
            }
            Expr::StringLiteral(_) => {
                self.expr_shapes.insert("StringLiteral");
            }
            Expr::RelationshipPath { .. } => {
                self.expr_shapes.insert("RelationshipPath");
            }
            Expr::BinaryOp { op, lhs, rhs } => {
                self.expr_shapes.insert("BinaryOp");
                self.operators.insert(operator_name(*op));
                self.record_expr(lhs);
                self.record_expr(rhs);
            }
            Expr::FunctionCall { args, .. } => {
                self.expr_shapes.insert("FunctionCall");
                for arg in args {
                    self.record_expr(arg);
                }
            }
        }
    }
}

fn op_kind(op: &Op) -> &'static str {
    match op {
        Op::Insert { .. } => "Insert",
        Op::Update { .. } => "Update",
        Op::Delete { .. } => "Delete",
    }
}

fn op_outcome_name(outcome: &OpOutcome) -> &'static str {
    match outcome {
        OpOutcome::Succeeds => "Succeeds",
        OpOutcome::Fails => "Fails",
        OpOutcome::AffectsNoRows => "AffectsNoRows",
        OpOutcome::AnyOf(_) => "AnyOf",
    }
}

fn key_space_name(key_space: &KeySpace) -> &'static str {
    match key_space {
        KeySpace::OneToOne => "OneToOne",
        KeySpace::Aggregate { .. } => "Aggregate",
    }
}

fn value_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
    }
}

fn operator_name(op: Operator) -> &'static str {
    match op {
        Operator::Add => "Add",
        Operator::GreaterThan => "GreaterThan",
    }
}

/// Sorted `(name, count)` pairs, since `HashMap` iteration order isn't
/// stable across runs and this is meant to be read/diffed by a human.
fn sorted_counts(map: &HashMap<&'static str, usize>) -> Vec<(&'static str, usize)> {
    let mut entries: Vec<(&'static str, usize)> = map.iter().map(|(k, v)| (*k, *v)).collect();
    entries.sort_unstable_by_key(|(name, _)| *name);
    entries
}

/// Sorted names, for the same reason as [`sorted_counts`].
fn sorted_names(set: &HashSet<&'static str>) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = set.iter().copied().collect();
    names.sort_unstable();
    names
}

impl fmt::Display for Coverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "cases: {}", self.cases)?;

        write!(f, "ops_by_kind:")?;
        for (name, count) in sorted_counts(&self.ops_by_kind) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;

        write!(f, "ops_by_outcome:")?;
        for (name, count) in sorted_counts(&self.ops_by_outcome) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;

        writeln!(
            f,
            "expr_shapes: {}",
            sorted_names(&self.expr_shapes).join(", ")
        )?;
        writeln!(f, "operators: {}", sorted_names(&self.operators).join(", "))?;
        writeln!(
            f,
            "types_exercised: {}",
            sorted_names(&self.types_exercised).join(", ")
        )?;

        write!(f, "key_spaces:")?;
        for (name, count) in sorted_counts(&self.key_spaces) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::build_program;

    #[test]
    fn record_program_tallies_a_trivial_convergent_program() {
        let mut coverage = Coverage::new();
        let program = build_program(&[(Some(1), Some(2))], &[]);
        coverage.record_program(&program);

        assert_eq!(coverage.cases, 1);
        assert_eq!(coverage.ops_by_kind.get("Insert"), Some(&1));
        assert_eq!(coverage.ops_by_outcome.get("Succeeds"), Some(&1));
        assert!(coverage.expr_shapes.contains("Column"));
        assert!(coverage.expr_shapes.contains("BinaryOp"));
        assert!(coverage.operators.contains("Add"));
        assert!(coverage.types_exercised.contains("numeric"));
        assert_eq!(coverage.key_spaces.get("OneToOne"), Some(&1));
    }

    #[test]
    fn record_program_accumulates_across_calls() {
        let mut coverage = Coverage::new();
        coverage.record_program(&build_program(&[(Some(1), Some(2))], &[]));
        coverage.record_program(&build_program(&[(Some(3), Some(4))], &[]));

        assert_eq!(coverage.cases, 2);
        assert_eq!(coverage.ops_by_kind.get("Insert"), Some(&2));
    }

    #[test]
    fn display_output_is_stable_and_sorted() {
        let mut coverage = Coverage::new();
        coverage.record_program(&build_program(&[(Some(1), Some(2))], &[]));
        let printed = format!("{coverage}");
        assert!(printed.contains("cases: 1"));
        assert!(printed.contains("Insert=1"));
        assert!(printed.contains("OneToOne=1"));
    }
}
