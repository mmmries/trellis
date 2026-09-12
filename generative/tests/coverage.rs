//! Generator coverage meta-tests (issue #7, design doc §3 "Generator coverage
//! meta-tests"): no database, run in milliseconds. These check the
//! *generator's own surface*, not any oracle comparison — that coverage that
//! silently drops out (a type stops appearing in a derivation, an operator
//! stops being exercised, a widening quietly changes what used to be drawn)
//! is otherwise invisible until someone notices a property stopped catching
//! anything.

use std::collections::HashSet;

use engine::defs::ast::{Expr, Operator, ValueType};
use generative::generate::{Mutate, build_program, trivial_program, trivial_program_with};
use generative::model::{Op, Table};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;

/// `ValueType` has no `Hash` impl (it's an engine type, not owned by this
/// crate), so scalar-type-surface comparisons here go through a sorted `Vec`
/// instead of a `HashSet`.
fn sorted_value_types(types: impl IntoIterator<Item = ValueType>) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = types
        .into_iter()
        .map(|t| match t {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// Every scalar type appears both as a plain column and via a derivation.
///
/// Today's whole type surface is Numeric-only end to end (design doc §3):
/// the source table's columns are Numeric, and the sole definition's
/// derivation (`c1 + c2`) only ever references Numeric columns. This fails
/// loudly the day a `Text`/`Boolean`/`Uuid` column is added to the generated
/// schema without a matching derivation landing alongside it — exactly the
/// "coverage that silently drops out" the design doc warns about.
#[test]
fn every_column_scalar_type_appears_via_a_derivation() {
    let program = build_program(&[(Some(1), Some(2))], &[]);
    let source = &program.tables[0];

    let column_types = sorted_value_types(source.columns.iter().map(|c| c.value_type));
    assert_eq!(
        column_types,
        vec!["numeric"],
        "the generator's column type surface changed — widen this assertion (and the \
         derivation check below) alongside it, don't just let it pass silently"
    );

    let def = &program.defs[0];
    assert_eq!(def.fields.len(), 1, "expected exactly one calculated field");
    let mut derivation_types_raw = Vec::new();
    collect_column_types(&def.fields[0].expr, source, &mut derivation_types_raw);
    let derivation_types = sorted_value_types(derivation_types_raw);
    assert_eq!(
        derivation_types, column_types,
        "every column scalar type must appear via a derivation, not just as a column"
    );
}

fn collect_column_types(expr: &Expr, source: &Table, out: &mut Vec<ValueType>) {
    match expr {
        Expr::Column(name) => {
            let column = source
                .columns
                .iter()
                .find(|c| &c.name == name)
                .expect("the derivation must only reference declared columns");
            out.push(column.value_type);
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_column_types(lhs, source, out);
            collect_column_types(rhs, source, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_types(arg, source, out);
            }
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        Expr::RelationshipPath { .. } => {
            unreachable!(
                "the generator never constructs a RelationshipPath (issue #25 is grammar + AST only; no generator support yet)"
            )
        }
    }
}

/// Every operator the generator currently supports (today: just `+`) appears
/// over every argument type it supports (`Numeric, Numeric`).
///
/// `engine::defs::ast::Operator` also has `GreaterThan` (`Numeric, Numeric ->
/// Boolean`), but the generator does not draw it yet — that widening is
/// tracked separately (issue #65 added the operator to the engine grammar;
/// generating it is a future generator issue, not this one). This test scopes
/// itself to what the generator actually draws today, and will need a
/// `GreaterThan` case added the day that widening lands.
#[test]
fn every_supported_operator_appears_over_every_supported_argument_type() {
    let program = build_program(&[(Some(1), Some(2))], &[]);
    let def = &program.defs[0];
    let Expr::BinaryOp { op, lhs, rhs } = &def.fields[0].expr else {
        panic!("expected the sole field's derivation to be a binary op");
    };
    assert_eq!(
        *op,
        Operator::Add,
        "the generator's only operator today is `+`"
    );

    for operand in [lhs.as_ref(), rhs.as_ref()] {
        match operand {
            Expr::Column(name) => {
                let column = program.tables[0]
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .expect("operand column must be declared");
                assert_eq!(
                    column.value_type,
                    ValueType::Numeric,
                    "`+`'s only supported argument type today is Numeric"
                );
            }
            other => panic!("expected a column operand, got {other:?}"),
        }
    }
}

/// Every value drawn from an `Op::Insert`/`Op::Update` in `program`, in draw
/// order. Ignores `Op::Delete` (it carries no field values).
fn all_op_values(program: &generative::model::Program) -> Vec<Option<String>> {
    program
        .ops
        .iter()
        .flat_map(|op| match op {
            Op::Insert { row, .. } => row.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            Op::Update { changes, .. } => changes.iter().map(|(_, v)| v.clone()).collect(),
            Op::Delete { .. } => Vec::new(),
        })
        .collect()
}

/// With the awkward-value feature flag off, the generator's value draws are
/// structurally identical to before issue #7 widened the generator: a bare
/// `0..=VALUE_MAX` integer, never `None`/SQL `NULL`. Sampling many programs
/// (rather than instrumenting the exact strategy call sequence) is the
/// practical way to prove the *behavior* — what actually gets drawn — hasn't
/// silently changed; see `trivial_program_with`'s doc comment for why the
/// off-path strategy shape is deliberately kept byte-for-byte the same.
#[test]
fn awkward_values_off_never_draws_null() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program_with(false);
    for _ in 0..200 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(
            all_op_values(&program).iter().all(Option::is_some),
            "awkward_values=false must never draw NULL: {program:#?}"
        );
    }
}

/// With the awkward-value feature flag on, the new NULL shapes actually
/// appear — the other half of the same coverage meta-test (design doc §3).
#[test]
fn awkward_values_on_sometimes_draws_null() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program_with(true);
    let saw_null = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        all_op_values(&program).iter().any(Option::is_none)
    });
    assert!(
        saw_null,
        "awkward_values=true must draw NULL at least once across 500 samples"
    );
}

/// The default [`trivial_program`] strategy (awkward values on) also
/// occasionally draws a genuine `apply()`-failing duplicate-pk insert
/// (issue #6's gap) — sampled the same way as the NULL check above.
#[test]
fn trivial_program_sometimes_draws_a_duplicate_pk_insert() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_duplicate = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program.ops.iter().any(|op| matches!(op, Op::Insert { .. }))
            && has_duplicate_pk_insert(&program)
    });
    assert!(
        saw_duplicate,
        "the generator must sometimes draw a duplicate-pk insert across 500 samples"
    );
}

fn has_duplicate_pk_insert(program: &generative::model::Program) -> bool {
    let table = &program.tables[0];
    let mut seen = HashSet::new();
    for op in &program.ops {
        if let Op::Insert { row, .. } = op {
            let pk = row
                .iter()
                .find(|(name, _)| *name == table.pk_col)
                .and_then(|(_, v)| v.clone())
                .expect("insert must carry a pk value");
            if !seen.insert(pk) {
                return true;
            }
        }
    }
    false
}

/// Generator invariants survive generation (design doc §2's structural
/// invariant): the primary-key column is always declared on its table, and
/// every `Insert` always carries a non-NULL value for it. `Mutate` is a
/// hand-built enum with no representable state that could null or drop the
/// pk (see [`Mutate`] and `build_program`, which always writes
/// `Some(pk.to_string())` for the pk column of every insert it emits) — so
/// there is no proptest shrink step that could strand this invariant, and
/// sampling broadly here is a check on the generator's actual behavior, not
/// just its types.
#[test]
fn pk_column_is_never_null_or_missing_across_many_generated_programs() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    for _ in 0..200 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        for table in &program.tables {
            assert!(
                table.columns.iter().any(|c| c.name == table.pk_col),
                "every table must declare its own pk column: {program:#?}"
            );
        }
        for op in &program.ops {
            if let Op::Insert {
                table: table_name,
                row,
                ..
            } = op
            {
                let table = program
                    .tables
                    .iter()
                    .find(|t| &t.name == table_name)
                    .expect("insert must target a declared table");
                let pk_value = row
                    .iter()
                    .find(|(name, _)| name == &table.pk_col)
                    .map(|(_, v)| v);
                assert!(
                    matches!(pk_value, Some(Some(_))),
                    "the pk column must be present and non-NULL on every insert: {program:#?}"
                );
            }
        }
    }
}

/// Sanity check on the `Mutate` shape itself, independent of sampling: a
/// `DuplicateInsert` targets a seeded pk (never `seed_count + 1`, which would
/// miss and not be a duplicate). Cheap, no DB, and pins the invariant the
/// `mutate()` strategy relies on.
#[test]
fn duplicate_insert_is_a_distinct_mutate_from_update_and_delete() {
    let program = build_program(
        &[(Some(1), Some(2))],
        &[Mutate::DuplicateInsert {
            pk: 1,
            c1: Some(3),
            c2: Some(4),
        }],
    );
    assert_eq!(program.ops.len(), 2);
    assert!(matches!(program.ops[1], Op::Insert { .. }));
}

/// A2 (`local_docs/generative-suite-improvement-plan.md`): the [`Coverage`]
/// accumulator's floors — no database, no `Harness`, just the default
/// strategy sampled many times, same as every other test in this file. This
/// is the fast half of A2's payoff: if the generator's own machinery ever
/// stopped drawing one of these shapes (a `Delete`, a genuinely-failing op, a
/// zero-row no-op, `+`, a numeric column, a `OneToOne` def), this test would
/// catch it in milliseconds, without ever standing up a cluster. It
/// deliberately does not check NULL or duplicate-pk-insert specifically — the
/// two tests above already cover those, and `Coverage` itself only looks at
/// op kind/outcome/structure, never op values.
#[test]
fn a_real_run_of_the_default_strategy_meets_its_coverage_floors() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }

    for kind in ["Insert", "Update", "Delete"] {
        assert!(
            coverage.ops_by_kind.get(kind).copied().unwrap_or(0) > 0,
            "coverage floor failed: expected at least one {kind} op across 500 samples:\n{coverage}"
        );
    }

    for outcome in ["Succeeds", "Fails", "AffectsNoRows"] {
        assert!(
            coverage.ops_by_outcome.get(outcome).copied().unwrap_or(0) > 0,
            "coverage floor failed: expected at least one op with outcome {outcome} across 500 \
             samples:\n{coverage}"
        );
    }

    assert!(
        coverage.types_exercised.contains("numeric"),
        "coverage floor failed: expected \"numeric\" among types_exercised:\n{coverage}"
    );

    assert!(
        coverage.key_spaces.contains_key("OneToOne"),
        "coverage floor failed: expected \"OneToOne\" among key_spaces:\n{coverage}"
    );

    for shape in ["Column", "BinaryOp"] {
        assert!(
            coverage.expr_shapes.contains(shape),
            "coverage floor failed: expected {shape:?} among expr_shapes:\n{coverage}"
        );
    }

    assert!(
        coverage.operators.contains("Add"),
        "coverage floor failed: expected \"Add\" among operators:\n{coverage}"
    );
}
