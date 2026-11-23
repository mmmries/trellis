//! Layer-3 bucket 5 (`docs/generative-test-suite.md` §7):
//! database-administration actions interleaved with a program's ops.
//!
//! Every action is an oracle identity: none of them changes the correct
//! converged state, so the per-op comparison is [`super::run_convergence`]'s
//! unchanged. What they add is a demand on the engine at the moment they
//! fire (see [`DbAdminAction`] for each).
//!
//! Like [`super::run_convergence_with_noise`], this is concrete over
//! [`ManualBackend`]: reconnecting its raw session after a restart is not
//! part of the generic [`crate::backend::Backend`] seam.
//!
//! Scope: the whole program installs up front. A program's
//! `def_install_after_op`, `restart_after_ops` and `scale_out_after_ops` are
//! not honoured here, the same as in the noise runner.

use std::time::Duration;

use testkit::XID_PAGE;
use trellis::dev::pool::Pool;

use crate::backend::{
    Backend, ClusterControl, ManualBackend, SERVER_STOP_RECLAIM_TTL, await_pool_usable,
};
use crate::model::{DbAdminAction, DbAdminPlan, LOCK_TARGETS_HOLD, Program};

use super::pause::RunKind;
use super::{Outcome, RunError, apply_and_check_outcome, quiesce_snapshot_and_check};

/// [`super::run_convergence`] with `plan`'s database-administration actions
/// interleaved. `cluster` restarts the server `backend` and
/// `pool` point at. `pool` is the oracle's handle; it is waited on after a
/// restart the same way the backend's own pool is.
pub async fn run_convergence_with_db_admin<C: ClusterControl>(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    plan: &DbAdminPlan,
    cluster: &C,
) -> Result<Outcome, RunError> {
    for event in &plan.events {
        assert!(
            event.op < program.ops.len(),
            "db-admin event {event:?} is anchored past the last op ({} ops) — a generator bug",
            program.ops.len()
        );
    }

    // A restart can strand a claim the engine then waits a reclaim TTL to
    // free (see `SERVER_STOP_RECLAIM_TTL`).
    backend.set_reclaim_ttl(SERVER_STOP_RECLAIM_TTL);
    // The engine's own clock, which `StepEngineClock` steps. The harness's
    // waits keep real time.
    let engine_clock = trellis::dev::clock::EngineClock::new();
    backend.set_engine_clock(engine_clock.clone());
    if let Some(limit) = plan.counters_near {
        // Before the install, while the ring is pristine and no engine runs:
        // the definitions, their builds and every later draw take ids past
        // the start.
        backend
            .advance_counters(limit)
            .await
            .map_err(|e| RunError::Install(format!("counters near {limit:?}: {e:?}")))?;
    }
    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;

    // Where the server's clock stands: the steps' sum.
    let mut clock_offset = 0i64;
    for (op_index, op) in program.ops.iter().enumerate() {
        let events: Vec<&DbAdminAction> = plan
            .events
            .iter()
            .filter(|e| e.op == op_index)
            .map(|e| &e.action)
            .collect();

        let reclaimed_before = engine_clock.claims_reclaimed();
        apply_and_check_outcome(backend, op_index, op, false).await?;

        let mut holds = Vec::new();
        let ttl = SERVER_STOP_RECLAIM_TTL;
        for &action in &events {
            match action {
                DbAdminAction::Checkpoint => {
                    backend
                        .execute_admin("CHECKPOINT")
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?;
                }
                DbAdminAction::RestartPostgres(mode) => {
                    cluster.restart_postgres(*mode);
                    reconnect(backend, pool, op_index, action).await?;
                }
                DbAdminAction::VacuumFreeze => {
                    backend
                        .execute_admin("VACUUM FREEZE")
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?;
                }
                DbAdminAction::WrapXids { tail } => {
                    cluster.age_xids_to_epoch_edge(XID_PAGE);
                    reconnect(backend, pool, op_index, action).await?;
                    cluster.burn_xids(XID_PAGE.saturating_sub(*tail));
                }
                DbAdminAction::StepClock(step) => {
                    clock_offset += step.seconds();
                    cluster.step_clock(clock_offset);
                }
                DbAdminAction::StepEngineClock(step) => engine_clock.step(step.seconds()),
                DbAdminAction::LockTargets => holds.push(
                    lock_targets(backend, program)
                        .await
                        .map_err(|e| admin_error(op_index, action, e))?,
                ),
            }
        }
        // The oldest a claim stood while a hold kept its page waiting. A
        // restart at the same op ends a hold early, which is no failure.
        let mut oldest_claim = None;
        for hold in holds {
            if let Ok(Some(age)) = hold.await {
                oldest_claim = oldest_claim.max(Some(age));
            }
        }

        quiesce_snapshot_and_check(
            backend,
            pool,
            program,
            op_index,
            &program.defs,
            false,
            RunKind::DbAdmin,
        )
        .await?;

        // No TTL expires a live process: past a quiesce, every claim the
        // sweeps took was one a stop stranded or a server step aged, and no
        // claim a hold kept waiting went a TTL without a refresh.
        let reclaimed = engine_clock.claims_reclaimed() - reclaimed_before;
        let expired = oldest_claim.filter(|age| *age > ttl);
        if (reclaimed > 0 || expired.is_some()) && !events.iter().any(|a| a.may_reclaim_claims()) {
            return Err(RunError::LiveClaimExpired {
                op_index,
                reclaimed,
                oldest_claim: expired,
                actions: events.into_iter().cloned().collect(),
            });
        }
    }

    Ok(Outcome::Ran)
}

/// Takes `ACCESS EXCLUSIVE` locks on every target of `program` on a session
/// of the operator's own and returns the task holding them for
/// [`LOCK_TARGETS_HOLD`] ([`DbAdminAction::LockTargets`]). While it holds
/// them it reads how long the oldest claim has gone unrefreshed, and returns
/// the most it saw (`None` if it never read one).
async fn lock_targets(
    backend: &ManualBackend,
    program: &Program,
) -> Result<tokio::task::JoinHandle<Option<Duration>>, tokio_postgres::Error> {
    let targets: Vec<String> = program
        .defs
        .iter()
        .map(|def| backend.qualified_target(&def.target))
        .collect();
    if targets.is_empty() {
        return Ok(tokio::spawn(async { None }));
    }
    let oldest_claim = format!(
        "select extract(epoch from clock_timestamp() - min(claimed_at))::float8 \
         from {}",
        backend.qualified_trellis_table("seg_claims")
    );
    let (admin, connection) =
        tokio_postgres::connect(&backend.roles().admin, tokio_postgres::NoTls).await?;
    let connection = tokio::spawn(connection);
    admin
        .batch_execute(&format!(
            "begin; lock table {} in access exclusive mode",
            targets.join(", ")
        ))
        .await?;
    Ok(tokio::spawn(async move {
        let until = tokio::time::Instant::now() + LOCK_TARGETS_HOLD;
        let mut oldest: Option<Duration> = None;
        while tokio::time::Instant::now() < until {
            tokio::time::sleep(CLAIM_AGE_POLL).await;
            let Ok(row) = admin.query_one(&oldest_claim, &[]).await else {
                break;
            };
            if let Some(age) = row.get::<_, Option<f64>>(0) {
                oldest = oldest.max(Some(Duration::from_secs_f64(age.max(0.0))));
            }
        }
        let _ = admin.batch_execute("commit").await;
        drop(admin);
        let _ = connection.await;
        oldest
    }))
}

/// How often a [`lock_targets`] hold reads the oldest claim's age.
const CLAIM_AGE_POLL: Duration = Duration::from_millis(250);

/// Reconnects the backend's raw session and waits until `pool` works again
/// after the server restarted under them.
async fn reconnect(
    backend: &mut ManualBackend,
    pool: &Pool,
    op_index: usize,
    action: &DbAdminAction,
) -> Result<(), RunError> {
    backend
        .reconnect_after_server_restart()
        .await
        .map_err(|e| admin_error(op_index, action, e))?;
    await_pool_usable(pool)
        .await
        .map_err(|e| admin_error(op_index, action, e))
}

fn admin_error(op_index: usize, action: &DbAdminAction, error: impl std::fmt::Debug) -> RunError {
    RunError::DbAdmin {
        op_index,
        action: action.clone(),
        error: format!("{error:?}"),
    }
}
