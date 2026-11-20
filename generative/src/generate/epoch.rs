//! Epoch chains (#2187): many programs run back to back on one instance
//! and one database, without resetting it.
//!
//! An [`EpochCase`] is drawn as plain numbers: the first epoch's tables and
//! definitions, then a list of later epochs, each a list of changes at its
//! start ([`EpochChangeDraw`]) and a mutate stream per table. [`chain`]
//! resolves it into an [`EpochChain`]: one [`Program`] holding every
//! epoch's ops in order, and per epoch the steps taken before its ops
//! ([`EpochStep`]).
//!
//! # Resolution
//!
//! Changes resolve in epoch order against the definitions the earlier
//! epochs left, so an epoch may drop a definition the first epoch defined,
//! redefine one an earlier epoch renamed, or bring back one dropped many
//! epochs before. A draw that can't apply (a drop with nothing defined, a
//! restore with nothing dropped) resolves to nothing, rather than into
//! something else.
//!
//! Each table's mutates are rendered as one stream across every epoch, so
//! an op's expected outcome follows the rows the earlier epochs left. That
//! is what makes the case shrink well: proptest's `Vec` shrink removes
//! whole epochs first and then shrinks the ones left, and [`chain`]
//! re-renders whatever remains into a valid chain.
//!
//! # Seeds stay what they were
//!
//! The first epoch's `WHERE` draws are derived from the first epoch's
//! program alone ([`where_clause::with_predicates`]), so dropping a later
//! epoch leaves the definitions as they were.

use std::ops::Range;

use trellis::dev::defs::ast::TransformDef;

use super::where_clause;
use super::{
    DefShape, DerivedShape, Mutate, TableSpec, build_program_multi_with_shapes_and_derived,
    changed_definition,
};
use crate::model::{BurstAction, DefineShape, Program};

/// The most definitions a chain keeps defined at once: a
/// [`EpochChangeKind::Second`] past it resolves to nothing.
pub const MAX_LIVE_DEFS: usize = 6;

/// Which change an [`EpochChangeDraw`] asks for at an epoch's start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochChangeKind {
    /// `PAUSE` and `DROP TRANSFORM` of a defined definition. It may be
    /// brought back in a later epoch ([`EpochChangeKind::Restore`]).
    Drop,
    /// A drop of a defined definition and its definition again at once,
    /// under the same or a new name, unchanged or with a field changed.
    Redefine,
    /// The definition of one dropped in an earlier epoch, under the same or
    /// a new name, unchanged or with a field changed.
    Restore,
    /// A second definition over a defined one's source, under a new name.
    Second,
    /// A restart of the engine client.
    Restart,
}

/// One drawn change, before [`chain`] resolves it. `target` picks the
/// definition modulo how many qualify, and its higher digits pick the
/// shape, as [`BurstAction::Define`]'s resolution in
/// [`super::add_burst_actions`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochChangeDraw {
    pub kind: EpochChangeKind,
    pub target: usize,
}

/// One later epoch's draw: the changes at its start, in order, and a mutate
/// stream per table (`mutates[t]` for table `t`).
#[derive(Debug, Clone, PartialEq)]
pub struct EpochDraw {
    pub changes: Vec<EpochChangeDraw>,
    pub mutates: Vec<Vec<Mutate>>,
}

/// A drawn chain: the first epoch's tables (seeds and mutates) and
/// definitions, as `trivial_program` draws them, and the later epochs.
#[derive(Debug, Clone, PartialEq)]
pub struct EpochCase {
    pub tables: Vec<TableSpec>,
    pub defs: Vec<(usize, DefShape, Option<DerivedShape>)>,
    pub epochs: Vec<EpochDraw>,
}

/// One step taken at an epoch's start, before its ops, at a quiet point.
#[derive(Debug, Clone, PartialEq)]
pub enum EpochStep {
    /// A [`BurstAction::Drop`] or [`BurstAction::Define`].
    Act(BurstAction),
    /// [`crate::backend::Backend::restart`].
    Restart,
}

/// One resolved epoch: its steps, then `program.ops[ops]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Epoch {
    pub steps: Vec<EpochStep>,
    pub ops: Range<usize>,
}

/// A resolved chain. `program.defs` are the first epoch's definitions,
/// installed up front; later definitions come from the epochs'
/// [`BurstAction::Define`] steps.
#[derive(Debug, Clone, PartialEq)]
pub struct EpochChain {
    pub program: Program,
    pub epochs: Vec<Epoch>,
}

impl EpochChain {
    /// The definitions defined after epoch `epoch`'s steps, in the order
    /// they were defined.
    pub fn defined_at(&self, epoch: usize) -> Vec<TransformDef> {
        let mut live = self.program.defs.clone();
        for e in &self.epochs[..=epoch] {
            for step in &e.steps {
                apply_step(&mut live, step);
            }
        }
        live
    }
}

/// `live` as `step` leaves it.
pub fn apply_step(live: &mut Vec<TransformDef>, step: &EpochStep) {
    match step {
        EpochStep::Act(BurstAction::Drop { target }) => live.retain(|d| &d.target != target),
        EpochStep::Act(BurstAction::Define { def, .. }) => live.push((**def).clone()),
        _ => {}
    }
}

/// What `chain` reaches, by name, for the coverage report and its floors:
///
/// - `epochs`: epochs after the first;
/// - `drop`, `restart`;
/// - `define_<shape>`, one per [`DefineShape`];
/// - `restore`: a definition defined again in a later epoch than its drop;
/// - `define_unread_table`: a definition over a source no defined
///   definition has read since an earlier quiet point, so capture of it
///   starts again;
/// - `none_defined`: an epoch whose steps leave nothing defined.
pub fn chain_shapes(chain: &EpochChain) -> std::collections::BTreeMap<&'static str, usize> {
    let mut out = std::collections::BTreeMap::new();
    let mut add = |name: &'static str| *out.entry(name).or_insert(0) += 1;
    let mut live = chain.program.defs.clone();
    for (index, epoch) in chain.epochs.iter().enumerate() {
        if index > 0 {
            add("epochs");
        }
        let mut previous: Option<&EpochStep> = None;
        for step in &epoch.steps {
            match step {
                EpochStep::Restart => add("restart"),
                EpochStep::Act(BurstAction::Drop { .. }) => add("drop"),
                EpochStep::Act(BurstAction::Define { def, shape }) => {
                    add(match shape {
                        DefineShape::Second => "define_second",
                        DefineShape::Same => "define_same",
                        DefineShape::Changed => "define_changed",
                        DefineShape::Renamed => "define_renamed",
                        DefineShape::RenamedChanged => "define_renamed_changed",
                    });
                    let redefined =
                        matches!(previous, Some(EpochStep::Act(BurstAction::Drop { .. })));
                    if *shape != DefineShape::Second && !redefined {
                        add("restore");
                    }
                    if !redefined && !live.iter().any(|d| d.source == def.source) {
                        add("define_unread_table");
                    }
                }
                EpochStep::Act(_) => {}
            }
            apply_step(&mut live, step);
            previous = Some(step);
        }
        if index > 0 && live.is_empty() {
            add("none_defined");
        }
    }
    out
}

/// Resolves `case` into a chain (the module doc).
///
/// # Panics
///
/// If `case` has no table or definition, or an epoch's mutate streams
/// don't match the tables: generator bugs.
pub fn chain(case: &EpochCase) -> EpochChain {
    assert!(
        case.epochs
            .iter()
            .all(|e| e.mutates.len() == case.tables.len()),
        "chain: every epoch draws one mutate stream per table — a generator bug"
    );
    let (sources, rest): (Vec<(usize, DefShape)>, Vec<Option<DerivedShape>>) = case
        .defs
        .iter()
        .map(|(source, shape, derived)| ((*source, shape.clone()), derived.clone()))
        .unzip();

    // The definitions, with their `WHERE`s, from the first epoch alone.
    let first = build_program_multi_with_shapes_and_derived(&case.tables, &sources, &rest);
    let defs = where_clause::with_predicates(first).defs;

    // Every table's mutates as one stream across the epochs.
    let mut tables = case.tables.clone();
    for (t, table) in tables.iter_mut().enumerate() {
        for epoch in &case.epochs {
            table.mutates.extend(epoch.mutates[t].iter().cloned());
        }
    }
    let mut built = build_program_multi_with_shapes_and_derived(&tables, &sources, &rest);
    built.defs = defs;

    // `built.ops` is table by table: each table's seeds, then its mutates.
    // Cut it into epochs, each epoch's tables round-robin.
    let mut starts = Vec::with_capacity(tables.len());
    let mut at = 0;
    for table in &tables {
        starts.push(at);
        at += table.seed_values.len() + table.mutates.len();
    }
    let mut cursors = starts.clone();
    let mut per_epoch: Vec<Vec<usize>> = Vec::new();
    let mut take = |counts: Vec<usize>, cursors: &mut Vec<usize>| {
        let streams: Vec<Range<usize>> = counts
            .iter()
            .zip(cursors.iter_mut())
            .map(|(&n, cursor)| {
                let range = *cursor..*cursor + n;
                *cursor += n;
                range
            })
            .collect();
        let mut order = Vec::new();
        let longest = counts.iter().copied().max().unwrap_or(0);
        for i in 0..longest {
            for stream in &streams {
                if stream.start + i < stream.end {
                    order.push(stream.start + i);
                }
            }
        }
        per_epoch.push(order);
    };
    take(
        case.tables
            .iter()
            .map(|t| t.seed_values.len() + t.mutates.len())
            .collect(),
        &mut cursors,
    );
    for epoch in &case.epochs {
        take(epoch.mutates.iter().map(Vec::len).collect(), &mut cursors);
    }
    let order: Vec<usize> = per_epoch.iter().flatten().copied().collect();
    let mut ops = Vec::with_capacity(order.len());
    for &i in &order {
        ops.push(built.ops[i].clone());
    }
    built.ops = ops;

    // The steps, resolved against the definitions as the earlier epochs
    // left them.
    let mut live: Vec<TransformDef> = built.defs.clone();
    let mut dropped: Vec<TransformDef> = Vec::new();
    let mut fresh = 0usize;
    let mut epochs = Vec::with_capacity(per_epoch.len());
    let mut op_at = 0;
    for (index, order) in per_epoch.iter().enumerate() {
        let mut steps = Vec::new();
        let changes = if index == 0 {
            &[][..]
        } else {
            &case.epochs[index - 1].changes[..]
        };
        for draw in changes {
            resolve(
                draw,
                &built,
                &mut live,
                &mut dropped,
                &mut fresh,
                &mut steps,
            );
        }
        epochs.push(Epoch {
            steps,
            ops: op_at..op_at + order.len(),
        });
        op_at += order.len();
    }
    EpochChain {
        program: built,
        epochs,
    }
}

/// The shape bits of `target` past `modulus`: whether the definition is
/// renamed and whether a field is changed.
fn shape_bits(target: usize, modulus: usize) -> (bool, bool) {
    let rest = target / modulus.max(1);
    (rest % 2 == 1, (rest / 2) % 2 == 1)
}

/// `def` again as `(renamed, changed)` asks, and the shape that is.
fn again(
    def: &TransformDef,
    program: &Program,
    renamed: bool,
    changed: bool,
    fresh: &mut usize,
) -> (TransformDef, DefineShape) {
    let table = program
        .tables
        .iter()
        .find(|t| t.name == def.source)
        .expect("a definition reads a program table");
    let changed = if changed {
        changed_definition(def, table)
    } else {
        None
    };
    let shape = match (renamed, changed.is_some()) {
        (false, false) => DefineShape::Same,
        (false, true) => DefineShape::Changed,
        (true, false) => DefineShape::Renamed,
        (true, true) => DefineShape::RenamedChanged,
    };
    let mut again = changed.unwrap_or_else(|| def.clone());
    if renamed {
        again.target = format!("r{fresh}");
        *fresh += 1;
    }
    (again, shape)
}

fn resolve(
    draw: &EpochChangeDraw,
    program: &Program,
    live: &mut Vec<TransformDef>,
    dropped: &mut Vec<TransformDef>,
    fresh: &mut usize,
    steps: &mut Vec<EpochStep>,
) {
    let drop = |def: &TransformDef| {
        EpochStep::Act(BurstAction::Drop {
            target: def.target.clone(),
        })
    };
    let define = |def: &TransformDef, shape| {
        EpochStep::Act(BurstAction::Define {
            def: Box::new(def.clone()),
            shape,
        })
    };
    match draw.kind {
        EpochChangeKind::Restart => steps.push(EpochStep::Restart),
        EpochChangeKind::Drop => {
            if live.is_empty() {
                return;
            }
            let def = live.remove(draw.target % live.len());
            steps.push(drop(&def));
            dropped.push(def);
        }
        EpochChangeKind::Redefine => {
            if live.is_empty() {
                return;
            }
            let index = draw.target % live.len();
            let (renamed, changed) = shape_bits(draw.target, live.len());
            let (next, shape) = again(&live[index], program, renamed, changed, fresh);
            steps.push(drop(&live[index]));
            steps.push(define(&next, shape));
            live.remove(index);
            live.push(next);
        }
        EpochChangeKind::Restore => {
            if dropped.is_empty() || live.len() >= MAX_LIVE_DEFS {
                return;
            }
            let index = draw.target % dropped.len();
            let (renamed, changed) = shape_bits(draw.target, dropped.len());
            let def = dropped.remove(index);
            let (next, shape) = again(&def, program, renamed, changed, fresh);
            steps.push(define(&next, shape));
            live.push(next);
        }
        EpochChangeKind::Second => {
            if live.is_empty() || live.len() >= MAX_LIVE_DEFS {
                return;
            }
            let of = &live[draw.target % live.len()];
            let (_, changed) = shape_bits(draw.target, live.len());
            let (mut second, _) = again(of, program, false, changed, fresh);
            second.target = format!("s{fresh}");
            *fresh += 1;
            steps.push(define(&second, DefineShape::Second));
            live.push(second);
        }
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{DEFAULT_MAX_EPOCHS, epoch_case, epoch_case_with, max_epochs};

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use crate::generate::MAX_MUTATES;
    use crate::generate::strategy::{def_draw, mutate, table_spec};
    use crate::generate::{MAX_DEFS, MAX_TABLES};
    use proptest::prelude::*;

    /// The environment variable that sets [`max_epochs`].
    pub const MAX_EPOCHS_ENV: &str = "GENERATIVE_MAX_EPOCHS";

    /// [`max_epochs`] when `GENERATIVE_MAX_EPOCHS` isn't set.
    pub const DEFAULT_MAX_EPOCHS: usize = 12;

    /// The most epochs a chain draws after its first: `GENERATIVE_MAX_EPOCHS`
    /// when set (a deep run sets 50 or more), otherwise
    /// [`DEFAULT_MAX_EPOCHS`].
    pub fn max_epochs() -> usize {
        std::env::var(MAX_EPOCHS_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_EPOCHS)
    }

    /// The most changes one epoch's start draws.
    const MAX_CHANGES: usize = 3;

    /// One change draw. Earlier arms are simpler, so a shrink moves toward
    /// a drop.
    fn change() -> impl Strategy<Value = EpochChangeDraw> {
        let kind = prop_oneof![
            2 => Just(EpochChangeKind::Drop),
            3 => Just(EpochChangeKind::Redefine),
            3 => Just(EpochChangeKind::Restore),
            2 => Just(EpochChangeKind::Second),
            1 => Just(EpochChangeKind::Restart),
        ];
        (kind, 0usize..64).prop_map(|(kind, target)| EpochChangeDraw { kind, target })
    }

    /// One later epoch over tables of `seed_counts` seed rows: up to
    /// [`MAX_CHANGES`] changes and up to twice [`MAX_MUTATES`] mutates per
    /// table.
    fn epoch(seed_counts: Vec<usize>) -> impl Strategy<Value = EpochDraw> {
        let mutates: Vec<BoxedStrategy<Vec<Mutate>>> = seed_counts
            .into_iter()
            .map(|n| prop::collection::vec(mutate(n, true), 0..=2 * MAX_MUTATES).boxed())
            .collect();
        (prop::collection::vec(change(), 0..=MAX_CHANGES), mutates)
            .prop_map(|(changes, mutates)| EpochDraw { changes, mutates })
    }

    /// [`epoch_case_with`] up to [`max_epochs`] later epochs.
    pub fn epoch_case() -> impl Strategy<Value = EpochCase> {
        epoch_case_with(max_epochs())
    }

    /// A chain: the first epoch as `trivial_program` draws it (awkward values
    /// on), then one to `max` later epochs. The epochs come before the
    /// definitions in the draw, so a shrink drops whole epochs first, then
    /// shrinks within the ones left.
    pub fn epoch_case_with(max: usize) -> impl Strategy<Value = EpochCase> {
        let max = max.max(1);
        prop::collection::vec(table_spec(true), 1..=MAX_TABLES)
            .prop_flat_map(move |tables| {
                let seed_counts: Vec<usize> = tables.iter().map(|t| t.seed_values.len()).collect();
                let epochs = prop::collection::vec(epoch(seed_counts), 1..=max);
                let defs = prop::collection::vec(def_draw(tables.len()), 1..=MAX_DEFS);
                (Just(tables), epochs, defs)
            })
            .prop_map(|(tables, epochs, defs)| EpochCase {
                tables,
                defs,
                epochs,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Op;

    fn case(epochs: Vec<EpochDraw>) -> EpochCase {
        EpochCase {
            tables: vec![TableSpec::numeric_only(
                vec![(Some(1), Some(2)), (Some(3), Some(4))],
                vec![Mutate::Delete { pk: 1 }],
            )],
            defs: vec![(0, DefShape::OneToOne, None)],
            epochs,
        }
    }

    fn change(kind: EpochChangeKind, target: usize) -> EpochChangeDraw {
        EpochChangeDraw { kind, target }
    }

    /// Each table's stream carries across epochs: a row deleted in the
    /// first epoch is revived by a later epoch's insert, which succeeds.
    #[test]
    fn a_row_deleted_in_one_epoch_is_revived_in_a_later_one() {
        let chain = chain(&case(vec![EpochDraw {
            changes: Vec::new(),
            mutates: vec![vec![Mutate::DuplicateInsert {
                pk: 1,
                c1: Some(5),
                c2: None,
            }]],
        }]));
        assert_eq!(chain.epochs.len(), 2);
        assert_eq!(chain.epochs[0].ops, 0..3);
        assert_eq!(chain.epochs[1].ops, 3..4);
        assert!(matches!(
            &chain.program.ops[3],
            Op::Insert { expect, .. } if *expect == crate::model::OpOutcome::Succeeds
        ));
    }

    /// A drop in one epoch and its restore in a later one; a drop with
    /// nothing left to drop resolves to nothing.
    #[test]
    fn a_definition_dropped_in_one_epoch_comes_back_in_a_later_one() {
        let empty = || vec![Vec::new()];
        let chain = chain(&case(vec![
            EpochDraw {
                changes: vec![
                    change(EpochChangeKind::Drop, 0),
                    change(EpochChangeKind::Drop, 0),
                ],
                mutates: empty(),
            },
            EpochDraw {
                changes: vec![change(EpochChangeKind::Restore, 1)],
                mutates: empty(),
            },
        ]));
        let target = chain.program.defs[0].target.clone();
        assert_eq!(
            chain.epochs[1].steps,
            vec![EpochStep::Act(BurstAction::Drop {
                target: target.clone()
            })]
        );
        assert!(chain.defined_at(1).is_empty());
        let [EpochStep::Act(BurstAction::Define { def, shape })] = &chain.epochs[2].steps[..]
        else {
            panic!("{:?}", chain.epochs[2].steps)
        };
        assert_eq!(*shape, DefineShape::Renamed);
        assert_eq!(def.target, "r0");
        assert_eq!(chain.defined_at(2).len(), 1);
    }

    /// Dropping a later epoch leaves the earlier epochs' ops and steps as
    /// they were: what a shrink relies on.
    #[test]
    fn dropping_the_last_epoch_keeps_the_earlier_ones() {
        let one = EpochDraw {
            changes: vec![change(EpochChangeKind::Redefine, 3)],
            mutates: vec![vec![Mutate::Update {
                pk: 2,
                c1: Some(7),
                c2: Some(8),
            }]],
        };
        let two = EpochDraw {
            changes: vec![change(EpochChangeKind::Second, 0)],
            mutates: vec![vec![Mutate::Truncate]],
        };
        let long = chain(&case(vec![one.clone(), two]));
        let short = chain(&case(vec![one]));
        assert_eq!(short.program.defs, long.program.defs);
        assert_eq!(short.epochs[..], long.epochs[..2]);
        assert_eq!(
            short.program.ops[..],
            long.program.ops[..short.program.ops.len()]
        );
    }
}
