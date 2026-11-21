//! The epoch tier's runner (#2187): an [`EpochChain`]'s epochs run back to
//! back on one instance and one database, which is never reset between
//! them.

use trellis::dev::defs::ast::TransformDef;
use trellis::dev::pool::Pool;

use super::pause::RunKind;
use super::{Outcome, RunError, apply_and_check_outcome, quiesce_snapshot_and_check};
use crate::backend::ConcurrentBackend;
use crate::generate::epoch::{EpochChain, EpochStep, apply_step};
use crate::model::Program;

/// Installs `chain`'s tables and first definitions, then runs each epoch:
/// its steps at a quiet point (drops, definitions and restarts, through
/// [`ConcurrentBackend::act`] and [`crate::backend::Backend::restart`]),
/// then its ops one after another with no quiesce between them, each op's
/// outcome checked against [`crate::model::Op::expect`]. Once an epoch's
/// ops are applied, the engine is quiesced and every definition defined at
/// that point is checked against the oracle, then audited by
/// [`ConcurrentBackend::strict_audit`].
///
/// Every error is [`RunError::Epoch`], naming the epoch it came from, so a
/// failure says which epoch broke the instance. A divergence is localized
/// to its epoch: [`super::Divergence::op_index`] is the epoch's last op.
pub async fn run_epochs<B: ConcurrentBackend>(
    backend: &mut B,
    pool: &Pool,
    chain: &EpochChain,
) -> Result<Outcome, RunError> {
    let program = &chain.program;
    let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
    let in_epoch = |epoch: usize| {
        move |error: RunError| RunError::Epoch {
            epoch,
            error: Box::new(error),
        }
    };

    backend
        .expect(&super::concurrent::with_defined(
            program,
            chain
                .epochs
                .iter()
                .flat_map(|e| &e.steps)
                .filter_map(|step| match step {
                    EpochStep::Act(action) => Some(action),
                    _ => None,
                }),
        ))
        .map_err(|e| in_epoch(0)(RunError::Install(format!("{e:?}"))))?;
    backend
        .install(&Program {
            tables: program.tables.clone(),
            defs: program.defs.clone(),
            def_install_after_op: vec![0; program.defs.len()],
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        })
        .await
        .map_err(|e| in_epoch(0)(RunError::Install(format!("{e:?}"))))?;
    let mut defined: Vec<TransformDef> = program.defs.clone();

    for (index, epoch) in chain.epochs.iter().enumerate() {
        for step in &epoch.steps {
            match step {
                EpochStep::Restart => backend
                    .restart()
                    .await
                    .map_err(|e| in_epoch(index)(RunError::Restart(format!("{e:?}"))))?,
                EpochStep::Act(action) => {
                    backend.act(program, action).await.map_err(|e| {
                        in_epoch(index)(RunError::BurstAction {
                            burst: index,
                            action: action.clone(),
                            error: format!("{e:?}"),
                        })
                    })?;
                }
            }
            apply_step(&mut defined, step);
        }
        for op_index in epoch.ops.clone() {
            apply_and_check_outcome(backend, op_index, &program.ops[op_index], timing_enabled)
                .await
                .map_err(in_epoch(index))?;
        }
        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            epoch.ops.end.saturating_sub(1),
            &defined,
            timing_enabled,
            RunKind::Epoch,
        )
        .await
        .map_err(in_epoch(index))?;
        let targets: Vec<String> = defined.iter().map(|d| d.target.clone()).collect();
        let failures = backend
            .strict_audit(&targets)
            .await
            .map_err(|e| in_epoch(index)(RunError::Quiesce(format!("strict audit: {e:?}"))))?;
        if !failures.is_empty() {
            return Err(in_epoch(index)(RunError::Audit {
                burst: index,
                failures,
            }));
        }
    }
    Ok(Outcome::Ran)
}
