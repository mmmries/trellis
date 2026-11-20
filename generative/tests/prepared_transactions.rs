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

/// A transaction of the program's last two ops, left prepared and resolved
/// as the burst ends, with the definition paused and resumed while it is in
/// doubt: the run converges, and nothing is left prepared.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_prepared_transaction_survives_a_pause_and_resume() {
    let cluster = TestCluster::start();
    for commit in [true, false] {
        let db = CaseDatabase::provision(&cluster).await;
        let (program, target) = program();
        let plan = ConcurrentPlan {
            bursts: vec![Burst {
                lanes: vec![vec![0, 1, 2]],
                actions: vec![
                    TimedAction {
                        after: 3,
                        action: BurstAction::Pause {
                            target: target.clone(),
                            column: None,
                        },
                    },
                    TimedAction {
                        after: 3,
                        action: BurstAction::Resume {
                            target: target.clone(),
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
        };
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
