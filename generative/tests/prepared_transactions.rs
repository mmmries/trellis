//! Prepared transactions (#2181, 2d): a transaction left in `PREPARE
//! TRANSACTION` across a pause and resume (in a burst), across an engine
//! restart (by hand, as no concurrent plan restarts the engine), and the
//! harness's refusal to read the source while one is still prepared.

use generative::backend::{Backend, CaseDatabase, ManualBackend, PREPARED_GID_PREFIX};
use generative::model::{
    Burst, BurstAction, ConcurrentPlan, Isolation, NamePool, Op, OpOutcome, PrepareWrap, Program,
    Table, TimedAction, TxnWrap,
};
use generative::run::{RunError, check_none_prepared, check_program, run_convergence_concurrent};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::dev::pool::Pool;

fn program() -> (Program, String) {
    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let def = TransformDef {
        target: pool.next_table_name(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "copy".to_string(),
            expr: Expr::Column(a.clone()),
        }],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let insert = |id: &str, v: &str| Op::Insert {
        table: source.name.clone(),
        row: vec![
            (source.pk_col.clone(), Some(id.to_string())),
            (a.clone(), Some(v.to_string())),
        ],
        expect: OpOutcome::Succeeds,
    };
    let update = Op::Update {
        table: source.name.clone(),
        pk: "1".to_string(),
        changes: vec![(a.clone(), Some("8.00".to_string()))],
        expect: OpOutcome::Succeeds,
    };
    let target = def.target.clone();
    let program = Program {
        tables: vec![source.clone()],
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: vec![insert("1", "7.00"), insert("2", "2.00"), update],
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };
    (program, target)
}

/// One burst of one lane over the program's three ops, the last two a
/// transaction held prepared while the burst pauses and resumes `target`.
fn held_plan(target: &str, commit: bool) -> ConcurrentPlan {
    ConcurrentPlan {
        bursts: vec![Burst {
            lanes: vec![vec![0, 1, 2]],
            actions: vec![
                TimedAction {
                    after: 3,
                    action: BurstAction::Pause {
                        target: target.to_string(),
                        column: None,
                    },
                },
                TimedAction {
                    after: 3,
                    action: BurstAction::Resume {
                        target: target.to_string(),
                        column: None,
                    },
                },
            ],
        }],
        steady_load: None,
        settle_after_install: false,
        commit_stall: None,
        txns: vec![TxnWrap {
            ops: vec![1, 2],
            isolation: Isolation::RepeatableRead,
            read_first: true,
            think: std::time::Duration::ZERO,
            rehearse: false,
            savepoint: None,
            prepare: Some(PrepareWrap { commit, hold: true }),
        }],
        wiring: Vec::new(),
    }
}

/// A transaction of the program's last two ops, left prepared and resolved
/// as the burst ends, with the definition paused and resumed while it is in
/// doubt: the run converges, and nothing is left prepared.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_prepared_transaction_survives_a_pause_and_resume() {
    let cluster = TestCluster::start();
    for commit in [true, false] {
        let db = CaseDatabase::provision(&cluster).await;
        let (program, target) = program();
        let plan = held_plan(&target, commit);
        let mut backend = ManualBackend::connect_as(db.roles())
            .await
            .expect("connect manual backend");
        let oracle =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let run = run_convergence_concurrent(&mut backend, &oracle, &program, &plan)
            .await
            .unwrap_or_else(|e| panic!("commit={commit}: {e:?}"));
        assert!(run.outcome.as_pass(), "{}", run.outcome);
        check_none_prepared(&oracle).await.expect("none left");
        let snapshot = backend.snapshot().await.expect("snapshot");
        assert_eq!(snapshot[&target]["1"]["copy"], Some("8.00".to_string()));
        assert_eq!(snapshot[&target]["2"]["copy"], Some("2.00".to_string()));
    }
}

/// A transaction in doubt while the engine restarts, committed or rolled
/// back after: the target holds what the source commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_transaction_survives_an_engine_restart() {
    let cluster = TestCluster::start();
    for commit in [true, false] {
        let db = CaseDatabase::provision(&cluster).await;
        let (program, target) = program();
        let mut backend = ManualBackend::connect_as(db.roles())
            .await
            .expect("connect manual backend");
        backend.install(&program).await.expect("install");
        backend.apply(&program.ops[0]).await.expect("seed");
        backend.quiesce().await.expect("quiesce");

        let (app, connection) = tokio_postgres::connect(&db.roles().app, tokio_postgres::NoTls)
            .await
            .expect("connect as the application");
        tokio::spawn(connection);
        let t = &program.tables[0];
        app.batch_execute(&format!(
            "begin; insert into {n} values (2, 2.00); update {n} set {c} = 8.00 where {k} = 1; \
             prepare transaction '{PREPARED_GID_PREFIX}restart'",
            n = t.name,
            c = t.columns[1].name,
            k = t.pk_col,
        ))
        .await
        .expect("prepare");

        backend.restart().await.expect("restart the engine");
        let finish = if commit { "commit" } else { "rollback" };
        app.batch_execute(&format!("{finish} prepared '{PREPARED_GID_PREFIX}restart'"))
            .await
            .expect("resolve");
        backend.quiesce().await.expect("quiesce");

        let snapshot = backend.snapshot().await.expect("snapshot");
        let oracle =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let diverged = check_program(&oracle, &program, &snapshot)
            .await
            .expect("oracle runs");
        assert!(diverged.is_empty(), "commit={commit}: {diverged:?}");
        assert_eq!(snapshot[&target].contains_key("2"), commit);
    }
}

/// A prepared transaction left at the oracle fails the case.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_transaction_left_at_the_oracle_fails_the_case() {
    let cluster = TestCluster::start();
    let db = CaseDatabase::provision(&cluster).await;
    let (program, _) = program();
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    backend.install(&program).await.expect("install");
    let (app, connection) = tokio_postgres::connect(&db.roles().app, tokio_postgres::NoTls)
        .await
        .expect("connect as the application");
    tokio::spawn(connection);
    let t = &program.tables[0];
    app.batch_execute(&format!(
        "begin; insert into {} values (9, 9.00); \
         prepare transaction '{PREPARED_GID_PREFIX}left'",
        t.name
    ))
    .await
    .expect("prepare");
    let oracle = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match check_none_prepared(&oracle).await {
        Err(RunError::LeftPrepared(gids)) => {
            assert_eq!(gids, [format!("{PREPARED_GID_PREFIX}left")]);
        }
        other => panic!("expected LeftPrepared, got {other:?}"),
    }
    app.batch_execute(&format!("rollback prepared '{PREPARED_GID_PREFIX}left'"))
        .await
        .expect("resolve");
    check_none_prepared(&oracle).await.expect("none left");
}

/// A held transaction whose op has an outcome the program doesn't expect
/// fails the case, and is rolled back, not left prepared: the long-lived
/// cluster a sweep shares across cases has few prepared-transaction slots,
/// and a database with one prepared can't be dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_failing_held_transaction_is_not_left_prepared() {
    let cluster = TestCluster::start();
    let db = CaseDatabase::provision(&cluster).await;
    let (mut program, target) = program();
    let Op::Update { expect, .. } = &mut program.ops[2] else {
        unreachable!("the third op is the update");
    };
    *expect = OpOutcome::AffectsNoRows;
    let plan = held_plan(&target, true);
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let oracle = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match run_convergence_concurrent(&mut backend, &oracle, &program, &plan).await {
        Err(RunError::UnexpectedOpOutcome { op_index: 2, .. }) => {}
        other => panic!("expected the update's outcome to fail the case, got {other:?}"),
    }
    check_none_prepared(&oracle).await.expect("none left");
}

/// Generated mid-burst cases whose plan holds a prepared transaction in a
/// burst that pauses or resumes: the first `want` of a deterministic sample.
fn held_pause_cases(want: usize) -> Vec<generative::generate::ConcurrentCase> {
    use generative::generate::mid_burst_case;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let strategy = mid_burst_case();
    let mut runner = TestRunner::deterministic();
    let mut found = Vec::new();
    for _ in 0..4000 {
        let case = strategy.new_tree(&mut runner).expect("a case").current();
        let txns = &case.plan.txns;
        let held = |first: usize| {
            txns.iter()
                .any(|t| t.ops[0] == first && t.prepare.is_some_and(|p| p.hold))
        };
        let wanted = case.plan.bursts.iter().any(|b| {
            b.lanes.iter().flatten().any(|&i| held(i))
                && b.actions.iter().any(|a| {
                    matches!(
                        a.action,
                        BurstAction::Pause { .. } | BurstAction::Resume { .. }
                    )
                })
        });
        if wanted {
            found.push(case);
            if found.len() == want {
                break;
            }
        }
    }
    assert_eq!(found.len(), want, "too few generated held-pause cases");
    found
}

/// #2181 2d, the floor on real overlap: in generated cases that draw a held
/// transaction in a burst that pauses or resumes, an action starts while a
/// gid is in `pg_prepared_xacts` (read by the runner as the action starts),
/// in every such case.
#[tokio::test(flavor = "multi_thread")]
async fn generated_actions_run_while_a_transaction_is_in_doubt() {
    let cluster = TestCluster::start();
    let mut overlapped = 0;
    let cases = held_pause_cases(4);
    for (n, case) in cases.iter().enumerate() {
        let db = CaseDatabase::provision(&cluster).await;
        let mut backend = ManualBackend::connect_as_with_options(
            db.roles(),
            case.workers,
            Some(std::time::Duration::from_millis(case.seal_interval_ms)),
        )
        .await
        .expect("connect backend");
        if let Some(rows) = case.build_chunk_rows {
            backend.set_build_chunk_rows(rows);
        }
        let oracle =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
        let run = run_convergence_concurrent(&mut backend, &oracle, &case.program, &case.plan)
            .await
            .unwrap_or_else(|e| panic!("case {n}: {e:?}"));
        assert!(run.outcome.as_pass(), "case {n}: {}", run.outcome);
        assert!(
            run.in_doubt_actions > 0,
            "case {n}: no action ran while a transaction was in doubt"
        );
        overlapped += run.in_doubt_actions;
    }
    assert!(overlapped >= cases.len() as u64);
}

/// #2181 2d: in a burst that holds a prepared transaction, the runner moves
/// a pause drawn at an even count to the burst's end, and leaves one at an
/// odd count in place. Generated cases draw both, so a pause is still taken
/// mid-burst, with writes landing while it is paused, in some held bursts.
#[test]
fn held_bursts_pause_both_mid_burst_and_at_the_end() {
    let (mut moved, mut kept) = (false, false);
    for case in held_pause_cases(16) {
        let txns = &case.plan.txns;
        for burst in &case.plan.bursts {
            let holds = burst.lanes.iter().flatten().any(|&i| {
                txns.iter()
                    .any(|t| t.ops[0] == i && t.prepare.is_some_and(|p| p.hold))
            });
            if !holds {
                continue;
            }
            let ops = burst.ops().len();
            for a in &burst.actions {
                if matches!(a.action, BurstAction::Pause { .. }) && a.after < ops {
                    if a.after % 2 == 0 {
                        moved = true;
                    } else {
                        kept = true;
                    }
                }
            }
        }
    }
    assert!(moved && kept, "moved {moved}, kept {kept}");
}
