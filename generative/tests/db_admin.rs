//! Layer-3 bucket 5 (`docs/generative-test-suite.md` §7):
//! database-administration actions interleaved with a program's ops —
//! `CHECKPOINT`, a Postgres restart (fast, and immediate so the next
//! start runs crash recovery), `VACUUM FREEZE`, and transaction-id aging
//! across the 32-bit epoch boundary. Each is an oracle identity, so every op is
//! still judged by the unchanged recompute oracle at quiescence; see
//! `generative::run::run_convergence_with_db_admin` for what each action
//! demands of the engine.
//!
//! Every hand-built pin owns its cluster, because a restart severs every
//! connection on it. The property shares one per thread like the other
//! properties, restarting it under earlier cases' leftovers, which is
//! harmless: those cases are finished.

use generative::backend::{CaseDatabase, ManualBackend};
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    db_admin_plan_for, db_admin_plan_with_clock_for, trivial_program,
};
use generative::model::{
    ClockStep, CounterLimit, DbAdminAction, DbAdminEvent, DbAdminPlan, EngineClockStep, Op,
    Program, RestartMode,
};
use generative::run::{RunError, run_convergence_with_db_admin};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{
    Config as ProptestConfig, FileFailurePersistence, TestCaseError, TestRunner,
};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::pool::Pool;

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start().long_lived(),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12);
    ProptestConfig {
        cases,
        max_shrink_time: generative::shrink::max_shrink_time_ms(),
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs `program` with `plan` against a fresh database on `cluster`.
async fn run(cluster: &TestCluster, program: &Program, plan: &DbAdminPlan) -> Result<(), String> {
    run_counting(cluster, program, plan).await.map(|_| ())
}

/// [`run`], returning how far the engine's counters got
/// (`ManualBackend::counter_high_water`) and the definitions' ids.
async fn run_counting(
    cluster: &TestCluster,
    program: &Program,
    plan: &DbAdminPlan,
) -> Result<(i64, Vec<i64>), String> {
    let db = CaseDatabase::provision(cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    match run_convergence_with_db_admin(&mut backend, &pool, program, plan, cluster).await {
        Ok(outcome) if outcome.as_pass() => Ok((
            backend
                .counter_high_water()
                .await
                .expect("read the counters"),
            backend
                .definition_ids()
                .await
                .expect("read the definitions"),
        )),
        Ok(outcome) => Err(format!("run did not pass: {outcome}")),
        Err(RunError::Diverged(d)) => Err(format!(
            "convergence diverged after op {} (target {}) with db-admin plan {plan:?}:\n{}",
            d.op_index, d.def_target, d.report
        )),
        Err(other) => Err(format!("run error with db-admin plan {plan:?}: {other:?}")),
    }
}

/// A program and a plan sized to it, drawn together so the plan shrinks
/// with the program.
fn program_with_db_admin_plan() -> impl Strategy<Value = (Program, DbAdminPlan)> {
    trivial_program().prop_flat_map(|program| {
        let plan = db_admin_plan_for(&program);
        (Just(program), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// Checkpoints and Postgres restarts anywhere in a generated program
    /// never perturb convergence.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_db_admin_actions_never_perturb_convergence((program, plan) in program_with_db_admin_plan()) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map_err(TestCaseError::fail)
        })?;
    }
}

thread_local! {
    /// A thread's cluster with a fake clock, or `None` after a loud skip.
    static CLOCK_HARNESS: Option<Harness> = testkit::faketime::require("the clock-step property")
        .map(|lib| Harness {
            runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
            cluster: TestCluster::start_with_fake_clock(&lib).long_lived(),
        });
}

proptest! {
    #![proptest_config(proptest_config())]

    /// Wall-clock steps (and every other action) anywhere in a generated
    /// program never perturb convergence. Steps accumulate across a thread's
    /// cases, which is harmless: each case reads time only relative to its
    /// own database's writes.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_clock_steps_never_perturb_convergence(
        (program, plan) in trivial_program().prop_flat_map(|program| {
            let plan = db_admin_plan_with_clock_for(&program);
            (Just(program), plan)
        })
    ) {
        CLOCK_HARNESS.with(|h| {
            let Some(h) = h else { return Ok(()) };
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map_err(TestCaseError::fail)
        })?;
    }
}

/// No database: every drawn plan is anchored inside its program and holds at
/// most three events, and across enough draws every action shows up, so
/// none silently drops out of the sweep.
#[test]
fn drawn_plans_stay_in_bounds_and_cover_every_action() {
    let mut runner = TestRunner::deterministic();
    let strategy = program_with_db_admin_plan();
    let (mut checkpoint, mut fast, mut immediate, mut freeze, mut wrap) =
        (false, false, false, false, false);
    let mut engine_steps = [false; 3];
    for _ in 0..300 {
        let (program, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(plan.events.len() <= 3, "{plan:?}");
        for event in &plan.events {
            assert!(event.op < program.ops.len(), "{event:?} past the last op");
            match &event.action {
                DbAdminAction::Checkpoint => checkpoint = true,
                DbAdminAction::RestartPostgres(RestartMode::Fast) => fast = true,
                DbAdminAction::RestartPostgres(RestartMode::Immediate) => immediate = true,
                DbAdminAction::VacuumFreeze => freeze = true,
                DbAdminAction::WrapXids { tail } => {
                    assert!(*tail <= 256, "{event:?}");
                    wrap = true;
                }
                DbAdminAction::StepClock(_) => {
                    panic!("a plan for a cluster without a fake clock drew {event:?}")
                }
                DbAdminAction::StepEngineClock(step) => engine_steps[*step as usize] = true,
            }
        }
    }
    assert!(
        checkpoint && fast && immediate && freeze && wrap,
        "checkpoint={checkpoint} fast={fast} immediate={immediate} freeze={freeze} wrap={wrap}"
    );
    assert_eq!(
        engine_steps, [true; 3],
        "engine steps drawn: {engine_steps:?}"
    );
}

/// One source table with a 1-1 and an aggregate transform over it:
///
/// - ops 0-2 seed pks 1-3 (grains 0, 0, 1)
/// - op 3 updates pk 1
/// - op 4 deletes pk 2
/// - op 5 updates pk 3
fn one_to_one_and_aggregate_program() -> Program {
    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![
                (Some(10), Some(1)),
                (Some(20), Some(2)),
                (Some(30), Some(3)),
            ],
            text_values: vec![None, None, None],
            bool_values: vec![None, None, None],
            uuid_values: vec![None, None, None],
            grain_values: vec![
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
            ],
            mutates: vec![
                Mutate::Update {
                    pk: 1,
                    c1: Some(100),
                    c2: Some(5),
                },
                Mutate::Delete { pk: 2 },
                Mutate::Update {
                    pk: 3,
                    c1: Some(7),
                    c2: None,
                },
            ],
        }],
        &[
            (0, DefShape::OneToOne),
            (
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Sum(AggregateColumn::C1), AggregateFn::Count],
                },
            ),
        ],
    );
    assert_eq!(
        program.ops.len(),
        6,
        "3 seed inserts + 2 updates + 1 delete"
    );
    assert!(matches!(program.ops[3], Op::Update { .. }));
    assert!(matches!(program.ops[4], Op::Delete { .. }));
    assert!(matches!(program.ops[5], Op::Update { .. }));
    program
}

fn plan(events: &[(usize, DbAdminAction)]) -> DbAdminPlan {
    DbAdminPlan {
        events: events
            .iter()
            .map(|(op, action)| DbAdminEvent {
                op: *op,
                action: action.clone(),
            })
            .collect(),
        counters_near: None,
    }
}

/// Postgres restarts with each op's change still in flight: a fast restart
/// after a seed insert, an immediate one (crash recovery on the next start)
/// after the delete, and a checkpoint after the last update. The engine
/// reconnects on its own every time: the staging worker's maintenance loop
/// by reconnecting, the pool by recycling, and the harness never touches the
/// engine client.
#[tokio::test(flavor = "multi_thread")]
async fn postgres_restarts_with_work_in_flight_converge() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[
        (1, DbAdminAction::RestartPostgres(RestartMode::Fast)),
        (4, DbAdminAction::RestartPostgres(RestartMode::Immediate)),
        (5, DbAdminAction::Checkpoint),
    ]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The engine's own clock jumps two days forward with the first seed in
/// flight, back ten seconds after the third, and three hours forward after
/// the delete: every deadline, schedule and grace the engine keeps comes
/// due at once, then late. No definition pauses and every op converges.
#[tokio::test(flavor = "multi_thread")]
async fn engine_clock_steps_with_work_in_flight_converge() {
    let cluster = TestCluster::start();
    let plan = plan(&[
        (
            0,
            DbAdminAction::StepEngineClock(EngineClockStep::ForwardDays),
        ),
        (
            2,
            DbAdminAction::StepEngineClock(EngineClockStep::BackSeconds),
        ),
        (
            4,
            DbAdminAction::StepEngineClock(EngineClockStep::ForwardHours),
        ),
    ]);
    run(&cluster, &one_to_one_and_aggregate_program(), &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// Engine clock steps around Postgres restarts: a step back just before a
/// fast restart, two days forward while the leader is retaking its session
/// after it, and three hours forward after a crash restart. The leader's
/// lost-session schedule and every reconnect run on the stepped clock.
#[tokio::test(flavor = "multi_thread")]
async fn engine_clock_steps_compose_with_restarts() {
    let cluster = TestCluster::start();
    let plan = plan(&[
        (
            0,
            DbAdminAction::StepEngineClock(EngineClockStep::BackSeconds),
        ),
        (0, DbAdminAction::RestartPostgres(RestartMode::Fast)),
        (
            0,
            DbAdminAction::StepEngineClock(EngineClockStep::ForwardDays),
        ),
        (3, DbAdminAction::RestartPostgres(RestartMode::Immediate)),
        (
            3,
            DbAdminAction::StepEngineClock(EngineClockStep::ForwardHours),
        ),
    ]);
    run(&cluster, &one_to_one_and_aggregate_program(), &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The cluster's transaction ids aged past the 32-bit epoch boundary with
/// work in flight. A freeze after the first seed; a wrap after the second
/// lands the next id one commit-log page short of the boundary, and burns all
/// but 8 of the ids left, so the engine's drains and the next ops cross it
/// with ring rows, fences and the aggregate's bases from epoch 0 still live.
/// The seeds written before the wrap are frozen and more than `2^31` ids old
/// by then, so their `age(xmin)` reads negative. Every op converges.
///
/// With the `fence_xid32` plant armed (`TRELLIS_TEST_PLANT=fence_xid32`),
/// the drain's fences lose the changes from past the boundary, and this
/// fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_wrap_across_the_epoch_boundary_with_work_in_flight_converges() {
    const EPOCH: u64 = 1 << 32;
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = plan(&[
        (0, DbAdminAction::VacuumFreeze),
        (1, DbAdminAction::WrapXids { tail: 8 }),
        (4, DbAdminAction::VacuumFreeze),
    ]);
    run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let next = cluster.next_xid();
    assert!(
        next > EPOCH,
        "the run crossed the epoch boundary: next xid {next}"
    );
}

/// The engine's counters start one short of `i32::MAX`, so the two
/// definitions' ids straddle `2^31` and the ring's change ids and segment
/// numbers cross it mid-program, and every op converges.
#[tokio::test(flavor = "multi_thread")]
async fn counters_crossing_the_32_bit_limit_converge() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = DbAdminPlan {
        counters_near: Some(CounterLimit::Int32 { headroom: 1 }),
        ..DbAdminPlan::default()
    };
    let (high, ids) = run_counting(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let limit = i64::from(i32::MAX);
    assert!(high > limit, "the run crossed 2^31: high water {high}");
    assert_eq!(
        ids,
        [limit, limit + 1],
        "the definitions' ids straddle 2^31"
    );
}

/// The counters start a million short of `i64::MAX`, so every id is 19
/// digits, and every op converges.
#[tokio::test(flavor = "multi_thread")]
async fn counters_near_the_64_bit_limit_converge() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = DbAdminPlan {
        counters_near: Some(CounterLimit::Int64),
        ..DbAdminPlan::default()
    };
    let (high, ids) = run_counting(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let start = CounterLimit::Int64.target();
    assert!(high > start && high < i64::MAX, "high water {high}");
    assert!(
        ids.iter().all(|&id| id > start),
        "the definitions drew past the start: {ids:?}"
    );
}

/// How far the server's clock is ahead of this process's, in seconds, read
/// from a session on `cluster`.
async fn server_clock_lead(cluster: &TestCluster) -> f64 {
    let client = testkit::connect_with(&cluster.database_dsn("postgres"), "").await;
    let row = client
        .query_one("select extract(epoch from now())::float8", &[])
        .await
        .expect("read now()");
    let server: f64 = row.get(0);
    let local = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs_f64();
    server - local
}

/// The shim itself: a step moves `now()` for sessions already open and new
/// ones, steps replace one another (a step back lands behind real time), and a
/// restart keeps the offset.
#[tokio::test(flavor = "multi_thread")]
async fn a_clock_step_moves_now_and_survives_a_restart() {
    let Some(lib) = testkit::faketime::require("a_clock_step_moves_now_and_survives_a_restart")
    else {
        return;
    };
    let cluster = TestCluster::start_with_fake_clock(&lib);
    let near = |lead: f64, want: f64| (lead - want).abs() < 5.0;
    let lead = server_clock_lead(&cluster).await;
    assert!(near(lead, 0.0), "no step yet: {lead}");

    cluster.step_clock(ClockStep::ForwardHours.seconds());
    let lead = server_clock_lead(&cluster).await;
    assert!(near(lead, 10_800.0), "three hours forward: {lead}");

    cluster.step_clock(ClockStep::ForwardHours.seconds() + ClockStep::BackMinutes.seconds());
    let lead = server_clock_lead(&cluster).await;
    assert!(near(lead, 10_500.0), "five minutes back: {lead}");

    cluster.restart(testkit::StopMode::Fast);
    let lead = server_clock_lead(&cluster).await;
    assert!(near(lead, 10_500.0), "after a restart: {lead}");
}

/// A plan with the given clock steps, one per `(op, step)`.
fn clock_plan(steps: &[(usize, ClockStep)]) -> DbAdminPlan {
    plan(
        &steps
            .iter()
            .map(|(op, step)| (*op, DbAdminAction::StepClock(*step)))
            .collect::<Vec<_>>(),
    )
}

/// Runs `plan` on a cluster with a fake clock, or skips loudly.
async fn run_on_fake_clock(test: &str, plan: &DbAdminPlan) {
    let Some(lib) = testkit::faketime::require(test) else {
        return;
    };
    let cluster = TestCluster::start_with_fake_clock(&lib);
    run(&cluster, &one_to_one_and_aggregate_program(), plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

/// The clock steps three hours forward with the first seed in flight, then
/// ten minutes more after the second: no definition pauses, no claim or
/// registry row of the live engine reads as expired for good, and every op
/// converges.
#[tokio::test(flavor = "multi_thread")]
async fn forward_clock_steps_with_work_in_flight_converge() {
    run_on_fake_clock(
        "forward_clock_steps_with_work_in_flight_converge",
        &clock_plan(&[
            (0, ClockStep::ForwardHours),
            (1, ClockStep::ForwardMinutes),
            (4, ClockStep::ForwardHours),
        ]),
    )
    .await;
}

/// The clock steps back five minutes (an NTP correction) after the seeds,
/// so rows written in the future are older than the present, and again
/// mid-program.
#[tokio::test(flavor = "multi_thread")]
async fn backward_clock_steps_with_work_in_flight_converge() {
    run_on_fake_clock(
        "backward_clock_steps_with_work_in_flight_converge",
        &clock_plan(&[
            (0, ClockStep::ForwardHours),
            (2, ClockStep::BackMinutes),
            (3, ClockStep::BackMinutes),
            (5, ClockStep::ForwardMinutes),
        ]),
    )
    .await;
}

/// A clock step, a restart and an xid wrap in one program: the shim and the
/// other administration actions compose.
#[tokio::test(flavor = "multi_thread")]
async fn clock_steps_compose_with_restarts_and_wraps() {
    run_on_fake_clock(
        "clock_steps_compose_with_restarts_and_wraps",
        &plan(&[
            (0, DbAdminAction::StepClock(ClockStep::ForwardHours)),
            (1, DbAdminAction::RestartPostgres(RestartMode::Fast)),
            (2, DbAdminAction::StepClock(ClockStep::BackMinutes)),
            (3, DbAdminAction::WrapXids { tail: 8 }),
            (4, DbAdminAction::StepClock(ClockStep::ForwardMinutes)),
        ]),
    )
    .await;
}

/// A restart strands a claim mid-build, then the clock jumps three hours: the
/// stranded claim reads as TTL-old at once and is reclaimed, and the build
/// finishes. (A step back here instead holds the claim five minutes longer,
/// which is why the generator never draws one after a stop.)
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_step_after_a_restart_frees_the_stranded_claim() {
    run_on_fake_clock(
        "a_forward_step_after_a_restart_frees_the_stranded_claim",
        &plan(&[
            (0, DbAdminAction::RestartPostgres(RestartMode::Immediate)),
            (0, DbAdminAction::StepClock(ClockStep::ForwardHours)),
        ]),
    )
    .await;
}

/// The worker registry across a three-hour step. The live engine's row
/// reads as dead (`has_live_drain_workers` is false) until its next
/// heartbeat writes `last_seen` in the new time; then it is live again,
/// under the same worker id, and no definition paused.
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_step_reads_the_live_engine_as_dead_until_its_next_heartbeat() {
    use generative::backend::{Backend, SERVER_STOP_RECLAIM_TTL};
    let test = "a_forward_step_reads_the_live_engine_as_dead_until_its_next_heartbeat";
    let Some(lib) = testkit::faketime::require(test) else {
        return;
    };
    let cluster = TestCluster::start_with_fake_clock(&lib);
    let db = CaseDatabase::provision(&cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    // As the db_admin runner does: a heartbeat every few seconds.
    backend.set_reclaim_ttl(SERVER_STOP_RECLAIM_TTL);
    backend
        .install(&one_to_one_and_aggregate_program())
        .await
        .expect("install");
    let config = Config::from_dsn(backend.trellis_dsn().to_string()).expect("config");
    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect the operator");
    let raw = testkit::connect_with(db.dsn(), "").await;
    let registry = async || -> Vec<(String, std::time::SystemTime)> {
        raw.query(
            "select worker_id, last_seen from trellis.worker_registry order by worker_id",
            &[],
        )
        .await
        .expect("read the registry")
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
    };
    let live_within = async |budget: std::time::Duration| {
        let deadline = std::time::Instant::now() + budget;
        loop {
            if trellis.has_live_drain_workers().await.expect("liveness") {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    };
    let budget = SERVER_STOP_RECLAIM_TTL * 2;
    assert!(live_within(budget).await, "the engine never read as live");

    let before = registry().await;
    cluster.step_clock(ClockStep::ForwardHours.seconds());
    let live = trellis.has_live_drain_workers().await.expect("liveness");
    // Only a heartbeat between the step and the read could make it live.
    if registry().await == before {
        assert!(!live, "a row three hours old read as live: {before:?}");
    }

    assert!(
        live_within(budget).await,
        "the live engine still read as dead {budget:?} after the step"
    );
    let ids = |rows: Vec<(String, std::time::SystemTime)>| {
        rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
    };
    assert_eq!(
        ids(registry().await),
        ids(before),
        "the live engine's row was replaced"
    );
    let statuses = backend.definition_statuses().await.expect("statuses");
    assert!(
        statuses.iter().all(|(_, status)| status == "live"),
        "a definition left `live`: {statuses:?}"
    );
}

/// No database: plans for a fake-clock cluster draw every step, and each
/// step's direction is as named.
#[test]
fn drawn_clock_plans_cover_every_step() {
    let mut runner = TestRunner::deterministic();
    let strategy = trivial_program().prop_flat_map(|p| db_admin_plan_with_clock_for(&p));
    let mut seen = [false; 3];
    for _ in 0..300 {
        let events = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current()
            .events;
        for (i, event) in events.iter().enumerate() {
            if let DbAdminAction::StepClock(step) = event.action {
                seen[step as usize] = true;
            }
            // A step back never follows a stop at its op (see
            // `ClockStep::BackMinutes`, `EngineClockStep::BackSeconds`).
            if matches!(
                event.action,
                DbAdminAction::StepClock(ClockStep::BackMinutes)
                    | DbAdminAction::StepEngineClock(EngineClockStep::BackSeconds)
            ) {
                let stopped_before = events[..i].iter().any(|e| {
                    e.op == event.op
                        && matches!(
                            e.action,
                            DbAdminAction::RestartPostgres(_) | DbAdminAction::WrapXids { .. }
                        )
                });
                assert!(!stopped_before, "a step back after a stop: {events:?}");
            }
        }
    }
    assert_eq!(seen, [true; 3], "steps drawn: {seen:?}");
    assert!(ClockStep::ForwardMinutes.seconds() > 0);
    assert!(ClockStep::ForwardHours.seconds() > ClockStep::ForwardMinutes.seconds());
    assert!(ClockStep::BackMinutes.seconds() < 0);
}
