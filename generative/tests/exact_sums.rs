//! Float and `interval` `SUM`/`AVG` (#1119) end to end: hand-built cases of
//! the plain arm (`generate::exact_sums::with_plain_sums`, on the
//! `MIN`/`MAX` tier's table) and of the `JOIN` arm
//! (`generate::join::with_exact_args`), each converging against the exact-sum
//! oracle (`oracle::exact_sums`) under concurrent drains with a strict
//! `self_check` after every burst. The drawn arms run in the properties
//! `property_min_max_extremes_converge_under_concurrent_drains` and
//! `property_join_targets_converge_under_concurrent_drains`.
//!
//! An `interval` group whose sum leaves the type's range raises in the
//! group's evaluation: [`a_build_over_an_out_of_range_interval_sum_pauses_where_the_oracle_raises`]
//! checks the oracle names that group as raising.

use std::time::Duration;

use generative::backend::{Backend, ManualBackend};
use generative::generate::ConcurrentCase;
use generative::generate::exact_sums::{PlainSumsDraw, SumFn, SumType};
use generative::generate::join::{
    JoinBuildDraws, JoinDefSpec, JoinField, JoinGroups, JoinKey, JoinOpDraw, JoinOpKind, KeyShape,
    join_case, with_exact_args,
};
use generative::generate::min_max::{
    Arg, Grouping, MinMaxDefDraw, MinMaxDraws, build_min_max_case_with,
};
use generative::model::{Column, Op, OpOutcome, Program, Table};
use generative::oracle::sql_oracle;
use generative::run::{RunError, run_convergence_concurrent};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::dev::defs::{IntWidth, PgType};
use trellis::dev::pool::Pool;

/// Runs `program` under `plan` against a fresh database, with a strict
/// `self_check` after every burst, and requires every definition `live`.
async fn converges(case: &ConcurrentCase) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect_with_options(
        db.dsn(),
        case.workers,
        Some(Duration::from_millis(case.seal_interval_ms)),
    )
    .await
    .expect("connect the backend");
    backend.set_strict_audit(true);
    backend.set_housekeeping_interval(Duration::from_millis(20));
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match run_convergence_concurrent(&mut backend, &pool, &case.program, &case.plan).await {
        Ok(run) => assert!(run.outcome.as_pass(), "run did not pass: {}", run.outcome),
        Err(RunError::Diverged(d)) => panic!(
            "diverged in the burst ending at op {}:\n{}",
            d.op_index,
            d.all()
                .map(|(target, report)| format!("target {target}:\n{report}"))
                .collect::<Vec<_>>()
                .join("\n")
        ),
        Err(other) => panic!("run error: {other:?}"),
    }
    let statuses = backend.definition_statuses().await.expect("statuses");
    assert!(
        statuses.iter().all(|(_, s)| s == "live"),
        "a definition left `live`: {statuses:?}"
    );
}

/// The plain arm: two definitions, every `SUM`/`AVG` of the three types
/// between them, over the tier's op stream (inserts, updates, group moves,
/// deletes of extremes and groups, retention deletes).
#[tokio::test(flavor = "multi_thread")]
async fn plain_float_and_interval_sums_converge() {
    let all: Vec<(SumFn, SumType)> = [SumFn::Sum, SumFn::Avg]
        .into_iter()
        .flat_map(|f| SumType::ALL.map(|t| (f, t)))
        .collect();
    let draws = MinMaxDraws {
        seed: 1119,
        defs: vec![
            MinMaxDefDraw {
                grouping: Grouping::G1,
                extremes: vec![(true, Arg::F)],
                partials: true,
            },
            MinMaxDefDraw {
                grouping: Grouping::G1G2,
                extremes: vec![(false, Arg::Iv)],
                partials: false,
            },
        ],
        deferred: Some((1, 400)),
        seed_rows: 40,
        ops: 500,
        truncates: 0,
        burst_size: 120,
        lanes: 3,
        workers: 4,
        seal_interval_ms: 10,
        housekeeping_interval_ms: 50,
        actions: Vec::new(),
        pace_micros: 300,
        entry_stall_seals: None,
        commit_stall_ms: None,
    };
    let sums = PlainSumsDraw {
        seed: 7,
        fields: vec![all.clone(), all[..3].to_vec()],
    };
    converges(&build_min_max_case_with(&draws, Some(&sums))).await;
}

/// The `JOIN` arm: one `LEFT` and one `INNER` definition with every
/// `SUM`/`AVG` field on both sides, over three keys written on both sides
/// (fan-out), once per argument type with and without the special values;
/// each drawn value (`-20..60`) is written several times.
#[tokio::test(flavor = "multi_thread")]
async fn join_float_and_interval_sums_converge() {
    let spec = |left, groups| JoinDefSpec {
        left,
        key: JoinKey::Single,
        groups,
        fields: vec![
            JoinField::SumX,
            JoinField::AvgX,
            JoinField::SumY,
            JoinField::AvgY,
            JoinField::CountY,
        ],
    };
    let specs = [
        spec(true, JoinGroups::Joined),
        spec(false, JoinGroups::Both),
    ];
    let mut state = 0x1119u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let draws: Vec<JoinOpDraw> = (0..400)
        .map(|i| JoinOpDraw {
            joined: next() % 2 == 0,
            kind: match next() % 10 {
                0..=4 => JoinOpKind::Insert,
                5..=7 => JoinOpKind::Update,
                _ => JoinOpKind::Delete,
            },
            pick: next() as usize,
            key: (next() % 256) as u8,
            t: 0,
            group: Some((next() % 4) as u8),
            value: (i % 13 != 0).then(|| ((i * 7) % 80) as i8 - 20),
            columns: (next() % 16) as u8,
        })
        .collect();
    for (sum_type, specials) in SumType::ALL
        .into_iter()
        .flat_map(|t| [(t, false), (t, true)])
    {
        let mut case = join_case(
            &specs,
            KeyShape::FanOut,
            &draws,
            &JoinBuildDraws {
                installs: vec![0, 500],
                ..JoinBuildDraws::default()
            },
            90,
            3,
            4,
            20,
        );
        case.program = with_exact_args(case.program, sum_type, specials);
        converges(&ConcurrentCase {
            program: case.program,
            burst_size: case.burst_size,
            plan: case.plan,
            workers: case.workers,
            seal_interval_ms: case.seal_interval_ms,
            build_chunk_rows: None,
            reconcile_interval_ms: None,
            housekeeping_interval_ms: None,
        })
        .await;
    }
}

/// A build over rows whose `interval` sum in one group is out of range
/// stops at that group's evaluation, and the oracle names that group, and
/// only it, as raising (`interval out of range`); the other group's sum is
/// its exact value though Postgres's `sum()` raises on it in insert order.
#[tokio::test(flavor = "multi_thread")]
async fn a_build_over_an_out_of_range_interval_sum_pauses_where_the_oracle_raises() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let table = Table {
        name: "ov".to_string(),
        pk_col: "id".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                value_type: generative::model::PRIMARY_KEY_VALUE_TYPE,
            },
            Column {
                name: "g".to_string(),
                value_type: ValueType::Integer(IntWidth::Int4),
            },
            Column {
                name: "iv".to_string(),
                value_type: ValueType::Other(PgType::Interval),
            },
        ],
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
        source_key: None,
    };
    let def = TransformDef {
        target: "ov_t".to_string(),
        source: "ov".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["g".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "g".to_string(),
                expr: Expr::Column("g".to_string()),
            },
            FieldDef {
                name: "s".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("iv".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let rows = [
        (1, 1, "2147483647 days"),
        (2, 1, "1 day"),
        (3, 1, "-1 days"),
        (4, 2, "2147483647 days"),
        (5, 2, "1 day"),
    ];
    let program = Program {
        tables: vec![table],
        defs: Vec::new(),
        def_install_after_op: Vec::new(),
        ops: rows
            .iter()
            .map(|(id, g, iv)| Op::Insert {
                table: "ov".to_string(),
                row: vec![
                    ("id".to_string(), Some(id.to_string())),
                    ("g".to_string(), Some(g.to_string())),
                    ("iv".to_string(), Some((*iv).to_string())),
                ],
                expect: OpOutcome::Succeeds,
            })
            .collect(),
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect the backend");
    backend.install(&program).await.expect("install");
    for op in &program.ops {
        backend.apply(op).await.expect("apply");
    }
    // Installed after the rows, so its build reads them (a definition
    // installed over the empty table would instead meet them in the ring).
    // The quiesce waits for the definition to leave `backfilling`.
    backend
        .install(&Program {
            tables: Vec::new(),
            defs: vec![def.clone()],
            def_install_after_op: vec![0],
            ops: Vec::new(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        })
        .await
        .expect("install the definition over the rows");
    backend.quiesce().await.expect("the build settles");
    let statuses = backend.definition_statuses().await.expect("statuses");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let oracle = sql_oracle(&pool, &def, "id").await.expect("oracle");
    let key = |g: &str| generative::model::group_key(&[Some(g.to_string())]);
    assert_eq!(
        oracle[&key("1")]["s"].as_deref(),
        Some("2147483647 days"),
        "{oracle:?}"
    );
    assert_eq!(
        oracle[&key("2")]["s"].as_deref(),
        Some("ERROR: interval out of range"),
        "{oracle:?}"
    );
    eprintln!("statuses after the build: {statuses:?}");
    assert!(
        statuses.iter().all(|(_, s)| s == "paused"),
        "the build should pause at the group the oracle says raises: {statuses:?}"
    );
}

/// The drawn arms happen: about half the `min_max` cases and three in ten
/// non-`numeric` `JOIN` cases sum a float or `interval`, every type among
/// them, and a `JOIN` case of the arm keeps only top-level fields.
#[test]
fn the_tiers_draw_the_exact_sum_arms() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::deterministic();
    let min_max = generative::generate::min_max::min_max_case(false);
    let plain = (0..100)
        .filter(|_| {
            let case = min_max.new_tree(&mut runner).expect("a case").current();
            case.program.tables[0]
                .columns
                .iter()
                .any(|c| c.name == "ivs")
        })
        .count();
    assert!((25..=75).contains(&plain), "{plain} of 100 min_max cases");

    let join = generative::generate::join::join_case_strategy();
    let mut seen = Vec::new();
    for _ in 0..200 {
        let case = join.new_tree(&mut runner).expect("a case").current();
        let x = case.program.tables[0]
            .columns
            .iter()
            .find(|c| c.name == "x")
            .expect("ja.x")
            .value_type;
        if let Some(t) = SumType::ALL.into_iter().find(|t| t.value_type() == x) {
            seen.push(t);
            for def in &case.program.defs {
                for field in &def.fields {
                    assert!(
                        !matches!(field.name.as_str(), "sx1" | "sy0" | "sxy"),
                        "{t:?} case keeps {}",
                        field.name
                    );
                }
            }
        }
    }
    for t in SumType::ALL {
        assert!(seen.contains(&t), "no {t:?} JOIN case in 200: {seen:?}");
    }
}
