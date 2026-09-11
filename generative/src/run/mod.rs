//! Drives a [`crate::model::Program`] through a [`crate::backend::Backend`]
//! and asserts the convergence property (design doc §4): install, then for
//! each op apply → quiesce → snapshot → compare the materialized target
//! against the [`crate::oracle`].
//!
//! The comparison is **per op**, not just once at the end, so a proptest
//! shrink localizes a failure to the *first* diverging op (design doc §4). An
//! op the backend *rejects* is not swallowed: the loop still quiesces,
//! snapshots, and compares afterward. A rejected DML statement leaves the
//! source unchanged, so the SQL oracle (which recomputes from the real source)
//! and the maintained target must still agree — "an op that errors changed
//! nothing" (design doc §4).
//!
//! This module names no cluster/harness types (`testkit`) and no generator
//! (`proptest`): it ties `backend` + `oracle` + `model` together over the
//! [`Backend`] trait and an [`engine::Pool`], so any backend and any source of
//! programs can reuse it.

mod coverage;

pub use coverage::Coverage;

use std::collections::HashMap;
use std::fmt;

use engine::Pool;
use engine::defs::ast::ValueType;

use crate::backend::{Backend, Snapshot};
use crate::model::{OpOutcome, Program};
use crate::oracle::{self, ThreeWayReport};

/// Classifies whether a property run counted as evidence at all (design doc
/// §6 "What keeps a green run meaningful"). This is deliberately a *separate*
/// question from whether the property held: [`run_convergence`] answers
/// `Outcome::Ran` for both a converged and a diverged program (the divergence
/// itself still fails the test via `Err(RunError::Diverged)`) — `Outcome`
/// exists to rule out the harness reporting green while never actually
/// exercising the backend.
///
/// [`Outcome::as_pass`] is the single decision point every property must
/// assert through, so a future variant can't quietly become "green" by
/// skipping it.
#[derive(Debug)]
pub enum Outcome {
    /// The backend was provisioned, stood up, and driven through a full
    /// run.
    Ran,
    /// Nothing was available to provision — genuinely inconclusive, and
    /// must never be treated as a pass.
    ///
    /// `testkit` self-provisions its own cluster for every run, so no code
    /// path in this crate currently produces this variant. It is kept so
    /// the classifier stays exhaustive/complete if an external-cluster mode
    /// (point the suite at an existing database instead of a disposable
    /// one) is ever added — see the design doc §6 and issue #4's open
    /// question.
    #[allow(dead_code)]
    Unavailable { reason: String },
    /// The backend was provisioned but could not stand up (e.g. the engine
    /// failed to connect or migrate). Carries the engine's error text; must
    /// never be a pass.
    BackendUnusable { error: String },
}

impl Outcome {
    /// The one place a property decides pass/fail from an `Outcome`. Only
    /// `Ran` passes.
    pub fn as_pass(&self) -> bool {
        matches!(self, Outcome::Ran)
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Outcome::Ran => write!(f, "Ran"),
            Outcome::Unavailable { reason } => write!(f, "Unavailable: {reason}"),
            Outcome::BackendUnusable { error } => write!(f, "BackendUnusable: {error}"),
        }
    }
}

/// Classifies a backend stand-up attempt (e.g. connecting/migrating a fresh
/// backend against a provisioned database) into an [`Outcome`]-shaped
/// result: `Ok` on success, or `Err(Outcome::BackendUnusable)` carrying the
/// engine's error text on failure. Callers that need the value on success
/// (not just whether it stood up) use this directly; callers that only care
/// about the classification can fold the `Result` into an `Outcome` with
/// `unwrap_or_else`/`match`.
pub fn classify_stand_up<T, E: fmt::Debug>(result: Result<T, E>) -> Result<T, Outcome> {
    result.map_err(|error| Outcome::BackendUnusable {
        error: format!("{error:?}"),
    })
}

/// A per-op convergence divergence: the op whose settled state disagreed with
/// the oracle, which definition's target it was, and the localized
/// [`ThreeWayReport`].
#[derive(Debug)]
pub struct Divergence {
    /// Index into `program.ops` of the op after which the divergence was
    /// observed.
    pub op_index: usize,
    /// The diverging definition's target table.
    pub def_target: String,
    pub report: ThreeWayReport,
}

/// Why a convergence run stopped. Backend and oracle errors are rendered to
/// strings so this stays free of the backend's associated error type.
#[derive(Debug)]
pub enum RunError {
    /// A definition or schema install was rejected — a hard failure, never a
    /// skip (design doc §3): the generator emits only valid programs, so a
    /// rejection is a generator or engine bug.
    Install(String),
    Quiesce(String),
    Snapshot(String),
    /// The oracle itself failed to compute (a DB/connection error), distinct
    /// from a divergence it successfully found.
    Oracle(String),
    /// The materialized state disagreed with the oracle after some op.
    Diverged(Divergence),
    /// An op's actual `apply()` outcome did not match what the generator
    /// expected of it (design doc §4 "operation errors are checked, not
    /// swallowed") — e.g. a `DuplicateInsert` the generator built to always
    /// collide with a seeded pk instead succeeded, meaning the backend's
    /// primary-key constraint (or the generator's own assumptions about it)
    /// silently stopped holding.
    UnexpectedOpOutcome {
        op_index: usize,
        expected: OpOutcome,
        actual: OpOutcome,
    },
}

/// Installs `program`, then applies each op and checks convergence after it.
///
/// The `pool` is the oracle's read handle into the same database `backend`
/// drives; it must have its `search_path` pinned to that schema (as
/// `testkit`'s isolated-database pool does).
///
/// Returns `Ok(Outcome::Ran)` once the full loop has executed — whether or
/// not the property held, since a divergence is a separate, still-fatal
/// condition surfaced via `Err(RunError::Diverged)`. There is no path to an
/// `Ok` outside having driven the backend through the whole program: the
/// single decision point every property asserts through
/// ([`Outcome::as_pass`]) can't be satisfied by quietly doing nothing.
pub async fn run_convergence<B: Backend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
) -> Result<Outcome, RunError> {
    // Improvement-plan workstream C, task C2: opt-in per-call timing around
    // each phase of the loop below, to find out where the suite's wall-clock
    // time actually goes (install, apply, snapshot, oracle recompute) rather
    // than guessing — see `local_docs/generative-suite-improvement-plan.md`
    // "C2" and its companion §1.3's cautionary tale about tuning before
    // instrumenting. Same convention as C1's `GENERATIVE_QUIESCE_TIMING` in
    // `crate::backend::manual::ManualBackend::quiesce`: an independent env
    // var (`GENERATIVE_COST_TIMING`), checked once per call site, silent and
    // free unless set.
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();

    let start = timing_enabled.then(std::time::Instant::now);
    let install_result = backend.install(program).await;
    if let Some(start) = start {
        eprintln!("COST_TIMING install {}", start.elapsed().as_millis());
    }
    install_result.map_err(|e| RunError::Install(format!("{e:?}")))?;

    for (op_index, op) in program.ops.iter().enumerate() {
        // A rejected op is a source no-op, not a skip: fall through to quiesce
        // and compare anyway (design doc §4). The actual outcome (not just
        // whether it errored) is classified and checked against what the
        // generator expected of this exact op — closing the gap where an op
        // that stopped erroring (or started affecting rows it shouldn't)
        // would go unnoticed.
        let start = timing_enabled.then(std::time::Instant::now);
        let apply_result = backend.apply(op).await;
        if let Some(start) = start {
            eprintln!("COST_TIMING apply {}", start.elapsed().as_millis());
        }
        let actual = match apply_result {
            Err(_) => OpOutcome::Fails,
            Ok(0) => OpOutcome::AffectsNoRows,
            Ok(_) => OpOutcome::Succeeds,
        };
        let expected = op.expect();
        if !expected.matches(&actual) {
            return Err(RunError::UnexpectedOpOutcome {
                op_index,
                expected: expected.clone(),
                actual,
            });
        }

        backend
            .quiesce()
            .await
            .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;

        let start = timing_enabled.then(std::time::Instant::now);
        let snapshot = backend.snapshot().await;
        if let Some(start) = start {
            eprintln!("COST_TIMING snapshot {}", start.elapsed().as_millis());
        }
        let snapshot = snapshot.map_err(|e| RunError::Snapshot(format!("{e:?}")))?;

        let start = timing_enabled.then(std::time::Instant::now);
        let checked = check_program(pool, program, &snapshot).await;
        if let Some(start) = start {
            eprintln!("COST_TIMING oracle {}", start.elapsed().as_millis());
        }
        if let Some((def_target, report)) = checked.map_err(RunError::Oracle)? {
            return Err(RunError::Diverged(Divergence {
                op_index,
                def_target,
                report,
            }));
        }
    }

    Ok(Outcome::Ran)
}

/// Runs the three-way oracle check for every definition in `program` against
/// `snapshot`, returning the first `(target, report)` that diverged, or `None`
/// if all converged.
///
/// Exposed (not just used by [`run_convergence`]) so a red/control test can
/// feed it a deliberately corrupted snapshot and confirm the harness reports
/// the divergence — proving it tells green from red (design doc §6/§7).
pub async fn check_program(
    pool: &Pool,
    program: &Program,
    snapshot: &Snapshot,
) -> Result<Option<(String, ThreeWayReport)>, String> {
    for def in &program.defs {
        let source = program
            .tables
            .iter()
            .find(|t| t.name == def.source)
            .unwrap_or_else(|| {
                panic!(
                    "program references source table {:?} it does not declare — a generator bug",
                    def.source
                )
            });
        let source_columns: HashMap<String, ValueType> = source
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.value_type))
            .collect();

        let target = snapshot.get(&def.target).ok_or_else(|| {
            format!(
                "target table {:?} missing from snapshot — the backend did not install it",
                def.target
            )
        })?;

        let report = oracle::check(pool, program, def, &source.pk_col, &source_columns, target)
            .await
            .map_err(|e| format!("{e:?}"))?;
        if report.diverged() {
            return Ok(Some((def.target.clone(), report)));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::Outcome;

    /// Design doc §6 / issue #4: only `Ran` passes. Fast, no database —
    /// this is the classifier logic itself, not the harness sanity check
    /// (that meta-test lives in `tests/meta.rs` and needs a real cluster).
    #[test]
    fn only_ran_passes() {
        assert!(Outcome::Ran.as_pass());
        assert!(
            !Outcome::Unavailable {
                reason: "nothing to provision".to_string(),
            }
            .as_pass()
        );
        assert!(
            !Outcome::BackendUnusable {
                error: "connection refused".to_string(),
            }
            .as_pass()
        );
    }
}
