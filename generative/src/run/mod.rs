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

use std::collections::HashMap;

use engine::Pool;
use engine::defs::ast::ValueType;

use crate::backend::{Backend, Snapshot};
use crate::model::Program;
use crate::oracle::{self, ThreeWayReport};

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
}

/// Installs `program`, then applies each op and checks convergence after it.
///
/// The `pool` is the oracle's read handle into the same database `backend`
/// drives; it must have its `search_path` pinned to that schema (as
/// `testkit`'s isolated-database pool does).
pub async fn run_convergence<B: Backend>(
    backend: &mut B,
    pool: &Pool,
    program: &Program,
) -> Result<(), RunError> {
    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;

    for (op_index, op) in program.ops.iter().enumerate() {
        // A rejected op is a source no-op, not a skip: fall through to quiesce
        // and compare anyway (design doc §4). We deliberately drop the error.
        let _ = backend.apply(op).await;

        backend
            .quiesce()
            .await
            .map_err(|e| RunError::Quiesce(format!("{e:?}")))?;
        let snapshot = backend
            .snapshot()
            .await
            .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;

        if let Some((def_target, report)) = check_program(pool, program, &snapshot)
            .await
            .map_err(RunError::Oracle)?
        {
            return Err(RunError::Diverged(Divergence {
                op_index,
                def_target,
                report,
            }));
        }
    }

    Ok(())
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
