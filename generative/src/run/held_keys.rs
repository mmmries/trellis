//! The held-key run: a sequential convergence run whose programs draw
//! erroring rows on purpose ([`crate::generate::held_keys`]), with an oracle
//! that models what the engine does with them
//! (`docs/generative-test-suite.md` §10, "The oracle and erroring rows").
//!
//! A row a plain 1-1 definition's field raises for is a held key
//! ([ADR-0003](../../../docs/decisions/0003-quarantine-storage-and-api.md)):
//! the definition leaves it out of its apply, its target row stays as it was
//! when the key was held (absent if it had none), and every later change to
//! the key is parked, so the key stays held however its source row changes.
//! Once the definition holds [`TRANSFORM_FUSE`] keys the whole-transform
//! fuse trips: it goes `quarantined` and its target holds its current value.
//! [`DefModel`] is that contract, step by step; the run compares each target
//! with it after every op, and checks that `Trellis::status` and
//! `Trellis::sample_quarantined` report exactly the keys it holds.
//!
//! A held key blocks every engine convergence wait, so the run quiesces with
//! [`HeldKeys::Settle`]: everything but the held keys' parked changes drains.
//! A case with a held key therefore settles as fast as one without, and a
//! failing case shrinks without paying a timeout per step.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use trellis::dev::defs::ast::{KeySpace, ValueType};
use trellis::dev::pool::Pool;

use crate::backend::{Backend, DefinitionState, HeldKeyReport, HeldKeys, ManualBackend};
use crate::generate::isolation::{self, IsolationCase, IsolationDraw};
use crate::model::{Op, Program};
use crate::oracle::{self, Rows};

use super::pause::{RunKind, allowance, check_no_unexpected_pause};
use super::{Divergence, RunError, apply_and_check_outcome, source_table};

/// How many held keys trip a definition's whole-transform fuse
/// (`trellis::staging::quarantine::DEFAULT_TRANSFORM_DEATH_THRESHOLD`; the
/// status reference's "Limits and defaults").
pub const TRANSFORM_FUSE: usize = 5;

/// How long a settled run gives its ring's segments to read drained
/// ([`ManualBackend::ring_unretired`]): a stuck page never does.
const RING_RETIRE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// One plain 1-1 definition's held keys and expected target, as the engine's
/// contract has them after each settled check.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DefModel {
    /// The keys it holds.
    held: BTreeSet<String>,
    /// Each held key's target row when it was held; a key that had none
    /// isn't here.
    frozen: Rows,
    /// The target the last check expected.
    last_expected: Rows,
    /// Whether the whole-transform fuse tripped.
    quarantined: bool,
}

impl DefModel {
    /// The target expected once the source reads `rows`, with `raised` the
    /// keys whose row raises (both from [`oracle::sql_oracle_per_row`]).
    ///
    /// A raising key not yet held is held now, keeping the row the last
    /// check expected for it. A held key keeps that row whatever its source
    /// row now reads, until a `TRUNCATE` of the source (`truncated`) clears
    /// the target, its held keys' rows with it; the keys stay held. Every
    /// other key is `rows`' own. Once the fuse trips, the target expected at
    /// the trip holds.
    pub fn step(&mut self, rows: Rows, raised: BTreeSet<String>, truncated: bool) -> Rows {
        if self.quarantined {
            return self.last_expected.clone();
        }
        if truncated {
            self.frozen.clear();
            self.last_expected.clear();
        }
        for key in raised {
            if self.held.insert(key.clone())
                && let Some(row) = self.last_expected.get(&key)
            {
                self.frozen.insert(key, row.clone());
            }
        }
        let mut expected: Rows = rows
            .into_iter()
            .filter(|(key, _)| !self.held.contains(key))
            .collect();
        expected.extend(self.frozen.clone());
        self.quarantined = self.held.len() >= TRANSFORM_FUSE;
        self.last_expected = expected.clone();
        expected
    }

    /// What the definition should report about its held keys.
    pub fn expectation(&self) -> HeldExpectation {
        HeldExpectation {
            status: if self.quarantined {
                "quarantined"
            } else {
                "live"
            },
            keys: self.held.clone(),
        }
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    pub fn held(&self) -> &BTreeSet<String> {
        &self.held
    }
}

/// A definition's expected status and held keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldExpectation {
    pub status: &'static str,
    pub keys: BTreeSet<String>,
}

impl HeldExpectation {
    /// Whether `reported` says exactly this: the status, `held_keys.count`
    /// and the keys `sample_quarantined` lists.
    pub fn matches(&self, reported: &HeldKeyReport) -> bool {
        reported.status == self.status
            && reported.count == self.keys.len() as u64
            && reported.keys == self.keys
    }
}

impl fmt::Display for HeldExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, held keys {:?}", self.status, self.keys)
    }
}

/// What a held-key run reached, for its coverage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeldKeySummary {
    /// Keys held at the end, summed over the definitions.
    pub held: usize,
    /// Definitions the fuse quarantined.
    pub quarantined: usize,
    /// Definitions paused because their share of a page failed alone
    /// (T0247): the isolation draw's.
    pub paused_alone: usize,
}

/// Installs `program`, then applies each op and, once the engine settles
/// around its held keys, checks every target against the held-key model and
/// every definition's reported held keys against it. Sequential, like
/// [`super::run_convergence`]; a program with deferred installs, restarts or
/// scale-outs is a generator bug here. Once the last op settled, nothing
/// may keep the ring from retiring
/// ([`ManualBackend::ring_unretired`]).
pub async fn run_convergence_with_held_keys(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
) -> Result<HeldKeySummary, RunError> {
    run(backend, pool, program, None).await
}

/// [`run_convergence_with_held_keys`] over an isolation case
/// ([`crate::generate::isolation`]): the definitions are installed, then
/// the target-side refusal the draw names, then the ops run. Until the
/// pair, `iso_d` is held to the oracle like every definition. From the
/// pair on, the #2130 invariant: `iso_d` is paused with T0247, naming the
/// page's SQLSTATE and both of the pair's keys, and its target holds what
/// it had before the pair, while every other definition, `iso_s` over the
/// same rows included, keeps converging and none is paused; and once the
/// last op settled the ring retires. `iso_d` is the one target the run
/// excuses from the oracle, so it is where "status tells the truth" bites
/// ([`RunError::StatusLies`]): it may not report `live` while inexact.
pub async fn run_convergence_with_isolation(
    backend: &mut ManualBackend,
    pool: &Pool,
    case: &IsolationCase,
) -> Result<HeldKeySummary, RunError> {
    run(backend, pool, &case.program, Some(&case.draw)).await
}

async fn run(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    isolation: Option<&IsolationDraw>,
) -> Result<HeldKeySummary, RunError> {
    assert!(
        program.def_install_after_op.iter().all(|&at| at == 0)
            && program.restart_after_ops.is_empty()
            && program.scale_out_after_ops.is_empty(),
        "the held-key run installs every definition up front and never restarts"
    );
    backend.set_held_keys(HeldKeys::Settle);
    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    if let Some(draw) = isolation {
        for ddl in draw
            .kind
            .target_ddl(&backend.qualified_target(isolation::FAILING))
        {
            backend
                .execute_raw(&ddl)
                .await
                .map_err(|e| RunError::Install(format!("{ddl}: {e:?}")))?;
        }
    }
    let mut models: BTreeMap<String, DefModel> = BTreeMap::new();
    // `iso_d`'s target as the last check before the pair read it.
    let mut before_pair: Option<Rows> = None;
    let mut paused_alone = 0;

    for (op_index, op) in program.ops.iter().enumerate() {
        apply_and_check_outcome(backend, op_index, op, false).await?;
        backend
            .quiesce()
            .await
            .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;
        // From the pair on, `iso_d` is the isolation's to judge.
        let excused = |target: &str| {
            isolation.is_some_and(|draw| op_index >= draw.pair_op && target == isolation::FAILING)
        };
        let states = backend
            .definition_states(&program.defs)
            .await
            .map_err(|e| RunError::Snapshot(format!("definition states: {e:?}")))?;
        let judged: Vec<DefinitionState> = states
            .iter()
            .filter(|state| !excused(&state.target))
            .cloned()
            .collect();
        check_no_unexpected_pause(op_index, &judged, allowance(RunKind::HeldKeys, program))?;
        let snapshot = backend
            .snapshot()
            .await
            .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;

        let mut diverged = Vec::new();
        for def in &program.defs {
            let source = source_table(program, &program.defs, &def.source)
                .unwrap_or_else(|| panic!("{:?} reads no table of the program", def.target));
            let source_columns: HashMap<String, ValueType> = source
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.value_type))
                .collect();
            let target = snapshot.get(&def.target).ok_or_else(|| {
                RunError::Snapshot(format!("target {:?} missing from snapshot", def.target))
            })?;
            let report = if matches!(def.key_space, KeySpace::OneToOne) && !excused(&def.target) {
                let (rows, raised) = oracle::sql_oracle_per_row(pool, def, &source.pk_col)
                    .await
                    .map_err(|e| RunError::Oracle(format!("{e:?}")))?;
                let truncated = matches!(op, Op::Truncate { table, .. } if *table == def.source);
                let expected = models
                    .entry(def.target.clone())
                    .or_default()
                    .step(rows, raised, truncated);
                let target = oracle::target_fields(target, &source.pk_col);
                oracle::compare(program, def, &source_columns, &target, &expected)
            } else {
                oracle::check_over(
                    pool,
                    program,
                    &program.defs,
                    def,
                    &source.pk_col,
                    &source_columns,
                    target,
                )
                .await
                .map_err(|e| RunError::Oracle(format!("{e:?}")))?
            };
            if excused(&def.target) {
                let draw = isolation.expect("only an isolation run excuses a target");
                let state = states
                    .iter()
                    .find(|state| state.target == def.target)
                    .expect("a state per definition");
                if let Some(lie) =
                    super::status_lies(std::slice::from_ref(state), |_| report.diverged()).pop()
                {
                    return Err(RunError::StatusLies {
                        op_index,
                        state: lie.clone(),
                    });
                }
                let problem = isolation_problem(draw, state, target, before_pair.as_ref());
                if let Some(problem) = problem {
                    return Err(RunError::Isolation {
                        op_index,
                        target: def.target.clone(),
                        problem,
                        program: format!("{program:#?}"),
                    });
                }
                continue;
            }
            if isolation.is_some_and(|draw| op_index + 1 == draw.pair_op)
                && def.target == isolation::FAILING
            {
                before_pair = Some(target.clone());
            }
            if report.diverged() {
                diverged.push((def.target.clone(), report));
            }
        }
        if let Some(divergence) = Divergence::from_checked(op_index, diverged, states) {
            return Err(RunError::Diverged(divergence));
        }

        for def in &program.defs {
            if excused(&def.target) {
                continue;
            }
            let expected = models
                .get(&def.target)
                .map(DefModel::expectation)
                .unwrap_or(HeldExpectation {
                    status: "live",
                    keys: BTreeSet::new(),
                });
            let reported = backend
                .held_key_report(&def.target)
                .await
                .map_err(|e| RunError::Snapshot(format!("held keys of {}: {e:?}", def.target)))?;
            if !expected.matches(&reported) {
                return Err(RunError::HeldKeys {
                    op_index,
                    target: def.target.clone(),
                    expected: expected.to_string(),
                    reported: format!(
                        "{}, held_keys.count {}, sample_quarantined {:?}",
                        reported.status, reported.count, reported.keys
                    ),
                    program: format!("{program:#?}"),
                });
            }
        }
        if isolation.is_some_and(|draw| op_index == draw.pair_op) {
            paused_alone += 1;
        }
    }

    if let Some(unretired) = backend
        .ring_unretired(RING_RETIRE_WAIT)
        .await
        .map_err(|e| RunError::Snapshot(format!("ring: {e:?}")))?
    {
        return Err(RunError::Isolation {
            op_index: program.ops.len().saturating_sub(1),
            target: "<ring>".to_string(),
            problem: format!("the ring didn't retire once the run settled: {unretired}"),
            program: format!("{program:#?}"),
        });
    }

    Ok(HeldKeySummary {
        held: models.values().map(|m| m.held().len()).sum(),
        quarantined: models.values().filter(|m| m.is_quarantined()).count(),
        paused_alone,
    })
}

/// What's wrong with `iso_d` at a check from the pair on, or `None`: it must
/// be paused, with a T0247 reason naming the page's SQLSTATE and both of
/// the pair's keys, and its `target` must still read `before_pair`.
fn isolation_problem(
    draw: &IsolationDraw,
    state: &DefinitionState,
    target: &BTreeMap<String, BTreeMap<String, Option<String>>>,
    before_pair: Option<&Rows>,
) -> Option<String> {
    if state.status != "paused" || state.reported != "paused" {
        return Some(format!(
            "expected paused (T0247), found {state}; the {:?} pair should have paused it",
            draw.kind
        ));
    }
    let reason = state.reason.as_deref().unwrap_or("");
    let mut wants = vec![
        "T0247".to_string(),
        draw.kind.sqlstate().to_string(),
        format!("'{}'", isolation::FAILING),
    ];
    wants.extend(draw.pair_keys.iter().map(|key| format!("key={key}")));
    let missing: Vec<&String> = wants
        .iter()
        .filter(|want| !reason.contains(*want))
        .collect();
    if !missing.is_empty() {
        return Some(format!("its pause reason lacks {missing:?}: {reason:?}"));
    }
    match before_pair {
        Some(before) if before != target => Some(format!(
            "its target moved after the pause: before the pair {before:?}, now {target:?}"
        )),
        Some(_) => None,
        None => Some("no check read its target before the pair".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value: &str) -> BTreeMap<String, Option<String>> {
        BTreeMap::from([("derived".to_string(), Some(value.to_string()))])
    }

    fn rows(entries: &[(&str, &str)]) -> Rows {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), row(value)))
            .collect()
    }

    fn keys(keys: &[&str]) -> BTreeSet<String> {
        keys.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn a_key_that_raises_on_insert_is_held_with_no_row() {
        let mut model = DefModel::default();
        assert_eq!(
            model.step(rows(&[("1", "a")]), keys(&["2"]), false),
            rows(&[("1", "a")])
        );
        assert_eq!(model.expectation().keys, keys(&["2"]));
        assert_eq!(model.expectation().status, "live");
    }

    #[test]
    fn a_key_that_raises_on_update_keeps_its_row_through_later_changes() {
        let mut model = DefModel::default();
        model.step(rows(&[("1", "a"), ("2", "b")]), keys(&[]), false);
        assert_eq!(
            model.step(rows(&[("1", "a")]), keys(&["2"]), false),
            rows(&[("1", "a"), ("2", "b")])
        );
        // Fixed at the source, then deleted: still held, still the old row.
        assert_eq!(
            model.step(rows(&[("1", "a"), ("2", "c")]), keys(&[]), false),
            rows(&[("1", "a"), ("2", "b")])
        );
        assert_eq!(
            model.step(rows(&[("1", "a")]), keys(&[]), false),
            rows(&[("1", "a"), ("2", "b")])
        );
        assert_eq!(model.expectation().keys, keys(&["2"]));
        // A truncate clears the held key's row; the key stays held, and a
        // row written for it since stays out.
        assert_eq!(model.step(rows(&[]), keys(&[]), true), rows(&[]));
        assert_eq!(model.step(rows(&[("2", "d")]), keys(&[]), false), rows(&[]));
        assert_eq!(model.expectation().keys, keys(&["2"]));
    }

    #[test]
    fn the_fuse_trips_at_its_threshold_and_freezes_the_target() {
        let mut model = DefModel::default();
        model.step(rows(&[("9", "z")]), keys(&["1", "2", "3", "4"]), false);
        assert!(!model.is_quarantined());
        let at_trip = model.step(rows(&[("9", "z")]), keys(&["1", "2", "3", "4", "5"]), false);
        assert!(model.is_quarantined());
        assert_eq!(model.expectation().status, "quarantined");
        assert_eq!(at_trip, rows(&[("9", "z")]));
        // Nothing applies once quarantined, and no further key is held.
        assert_eq!(
            model.step(rows(&[("9", "y"), ("7", "x")]), keys(&["6"]), false),
            at_trip
        );
        assert_eq!(model.held().len(), TRANSFORM_FUSE);
    }
}
