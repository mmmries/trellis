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
    db_admin_plan_for, trivial_program,
};
use generative::model::{
    CounterLimit, DbAdminAction, DbAdminEvent, DbAdminPlan, Op, Program, RestartMode,
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
/// (`ManualBackend::counter_high_water`).
async fn run_counting(
    cluster: &TestCluster,
    program: &Program,
    plan: &DbAdminPlan,
) -> Result<i64, String> {
    let db = CaseDatabase::provision(cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    match run_convergence_with_db_admin(&mut backend, &pool, program, plan, cluster).await {
        Ok(outcome) if outcome.as_pass() => Ok(backend
            .counter_high_water()
            .await
            .expect("read the counters")),
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

/// No database: every drawn plan is anchored inside its program and holds at
/// most three events, and across enough draws every action shows up, so
/// none silently drops out of the sweep.
#[test]
fn drawn_plans_stay_in_bounds_and_cover_every_action() {
    let mut runner = TestRunner::deterministic();
    let strategy = program_with_db_admin_plan();
    let (mut checkpoint, mut fast, mut immediate, mut freeze, mut wrap) =
        (false, false, false, false, false);
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
            }
        }
    }
    assert!(
        checkpoint && fast && immediate && freeze && wrap,
        "checkpoint={checkpoint} fast={fast} immediate={immediate} freeze={freeze} wrap={wrap}"
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

/// The engine's counters start just under `2^31` and cross it mid-program:
/// the ring's change ids and segment numbers, and the catalog's definition
/// and build ids, are all past 32 bits by the end, and every op converges.
#[tokio::test(flavor = "multi_thread")]
async fn counters_crossing_the_32_bit_limit_converge() {
    let cluster = TestCluster::start();
    let program = one_to_one_and_aggregate_program();
    let plan = DbAdminPlan {
        counters_near: Some(CounterLimit::Int32 { headroom: 4 }),
        ..DbAdminPlan::default()
    };
    let high = run_counting(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        high > i64::from(i32::MAX),
        "the run crossed 2^31: high water {high}"
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
    let high = run_counting(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert!(
        high >= CounterLimit::Int64.target() && high < i64::MAX,
        "high water {high}"
    );
}
