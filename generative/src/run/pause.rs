//! The unexpected-pause check: a run fails when a definition is `paused` at
//! a settled check, unless the run is one that provokes pauses on purpose.
//!
//! Quiescing treats `paused` as settled (a frozen definition never
//! progresses, so waiting on it is waiting forever), so without this check a
//! paused definition shows up only as a target that differs from the oracle,
//! with nothing naming the cause.
//!
//! [`allowance`] is the allowlist: the only place that says which runs may
//! find a definition paused. A run not on it fails on any pause
//! ([`RunError::UnexpectedPause`]). Every runner takes its [`RunKind`], so a
//! new runner has to pick one.

use crate::backend::DefinitionState;
use crate::model::{Op, Program};

use super::RunError;

/// Which runner is checking, the part of a tier's identity the allowlist
/// reads. The rest is in the program ([`allowance`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RunKind {
    /// `run_convergence`: the serial tiers (convergence, bulk operations,
    /// backfill, mid-stream install, client lifecycle, tampering).
    Sequential,
    /// `run_convergence_bursty`.
    Bursty,
    /// `run_convergence_concurrent`: every concurrent, `JOIN`, 1-1 `JOIN`,
    /// star and chained-reader tier.
    Concurrent,
    /// `run_convergence_with_noise`: DDL and DML on untracked tables,
    /// checkpoints.
    Noise,
    /// `run_convergence_with_db_admin`: Postgres restarts and checkpoints.
    DbAdmin,
    /// `run_convergence_with_restore`, the original side. The restored side
    /// is held to a stricter rule (`PausedAfterRestore`), never to this one.
    Restore,
    /// `run_two_instance_convergence`.
    TwoInstance,
}

/// Why a run may find a definition paused, for the allowlist's reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Allowance {
    pub tier: &'static str,
    pub why: &'static str,
}

/// The allowlist: `Some` when the run is one that provokes pauses on
/// purpose, `None` when any pause fails it.
///
/// - **Disruption** (`DbAdmin`, `Restore`): the server restarts or stops
///   under the engine, which severs capture and may pause definitions.
/// - **Partition**: partition maintenance (`Op::Partition`: attach, detach,
///   drop, new leaves) changes a source's tree under a definition, and
///   capture may pause it. It runs in the sequential, bursty and concurrent
///   runners, so it is read off the program.
///
/// Schema change has no tier of its own: no generated op alters a tracked
/// source's columns. If one is added it joins this list, here.
///
/// Not on it, with why: the sequential, bursty and concurrent runs without
/// partition ops (every pause they take, `PAUSE TRANSFORM` or a column
/// pause, is resumed within the burst, so none is left at a check); the
/// noise tier (untracked tables never reach a definition); the two-instance
/// tier (an instance's definitions never read the other's source).
pub(super) fn allowance(kind: RunKind, program: &Program) -> Option<Allowance> {
    match kind {
        RunKind::DbAdmin | RunKind::Restore => Some(Allowance {
            tier: "disruption",
            why: "the server restarts or stops under the engine",
        }),
        RunKind::Sequential | RunKind::Bursty | RunKind::Concurrent
            if program
                .ops
                .iter()
                .any(|op| matches!(op, Op::Partition { .. })) =>
        {
            Some(Allowance {
                tier: "partition",
                why: "partition maintenance changes a source's tree under its definitions",
            })
        }
        RunKind::Sequential
        | RunKind::Bursty
        | RunKind::Concurrent
        | RunKind::Noise
        | RunKind::TwoInstance => None,
    }
}

/// `Ok` unless `allowed` is `None` and a definition of `states` is `paused`,
/// in which case it is [`RunError::UnexpectedPause`] naming each with its
/// reason.
pub(super) fn check_no_unexpected_pause(
    op_index: usize,
    states: &[DefinitionState],
    allowed: Option<Allowance>,
) -> Result<(), RunError> {
    if allowed.is_some() {
        return Ok(());
    }
    let paused: Vec<DefinitionState> = states.iter().filter(|s| s.is_paused()).cloned().collect();
    if paused.is_empty() {
        return Ok(());
    }
    let message = paused
        .iter()
        .map(|s| {
            format!(
                "unexpected pause: {}: {}",
                s.target,
                s.reason.as_deref().unwrap_or("no reason recorded")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Err(RunError::UnexpectedPause {
        op_index,
        message,
        paused,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OpOutcome, PartitionAction};

    fn state(target: &str, status: &str, reason: Option<&str>) -> DefinitionState {
        DefinitionState {
            target: target.to_string(),
            status: status.to_string(),
            reason: reason.map(str::to_string),
        }
    }

    fn program_with(ops: Vec<Op>) -> Program {
        Program {
            tables: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops,
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        }
    }

    #[test]
    fn a_pause_in_a_tier_off_the_allowlist_fails_with_its_reason() {
        let states = [
            state("t0", "live", None),
            state("t1", "paused", Some("capture: schema_changed on public.s")),
            state("t2", "paused", None),
        ];
        let program = program_with(Vec::new());
        for kind in [
            RunKind::Sequential,
            RunKind::Bursty,
            RunKind::Concurrent,
            RunKind::Noise,
            RunKind::TwoInstance,
        ] {
            let allowed = allowance(kind, &program);
            assert_eq!(allowed, None, "{kind:?}");
            match check_no_unexpected_pause(7, &states, allowed) {
                Err(RunError::UnexpectedPause {
                    op_index,
                    message,
                    paused,
                }) => {
                    assert_eq!(op_index, 7);
                    assert_eq!(
                        message,
                        "unexpected pause: t1: capture: schema_changed on public.s\n\
                         unexpected pause: t2: no reason recorded"
                    );
                    assert_eq!(paused.len(), 2);
                }
                other => panic!("{kind:?}: expected UnexpectedPause, got {other:?}"),
            }
        }
    }

    #[test]
    fn no_paused_definition_passes_everywhere() {
        let states = [state("t0", "live", None), state("t1", "quarantined", None)];
        check_no_unexpected_pause(0, &states, None).expect("nothing paused");
    }

    #[test]
    fn a_pause_in_an_allowlisted_tier_does_not_fail() {
        let states = [state("t0", "paused", Some("disruption: connection lost"))];
        let partition = program_with(vec![Op::Partition {
            table: "s".to_string(),
            action: PartitionAction::Detach {
                leaf: "s_p0".to_string(),
                concurrently: false,
            },
            expect: OpOutcome::Succeeds,
        }]);
        let plain = program_with(Vec::new());
        for (kind, program, tier) in [
            (RunKind::DbAdmin, &plain, "disruption"),
            (RunKind::Restore, &plain, "disruption"),
            (RunKind::Sequential, &partition, "partition"),
            (RunKind::Bursty, &partition, "partition"),
            (RunKind::Concurrent, &partition, "partition"),
        ] {
            let allowed = allowance(kind, program);
            assert_eq!(allowed.map(|a| a.tier), Some(tier), "{kind:?}");
            check_no_unexpected_pause(0, &states, allowed)
                .unwrap_or_else(|e| panic!("{kind:?}: {e:?}"));
        }
    }
}
