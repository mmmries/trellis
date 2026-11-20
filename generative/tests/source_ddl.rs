//! Source DDL in the op stream (#2184, `docs/generative-test-suite.md` §7,
//! bucket 3), judged by "never silently wrong"
//! (`generative::run::judge`): after each drawn change every definition is
//! exact, or paused with a reason and exact after its `RESUME`, or wrong
//! only as a register entry the shape names, which `self_check` reports and
//! the entry's repair fixes.
//!
//! The pins below each own a database on one cluster per test; the property
//! shares a cluster per thread like the other properties.

use generative::backend::{CaseDatabase, ManualBackend};
use generative::gaps::Gap;
use generative::generate::source_ddl::source_ddl_plan_for;
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    trivial_program,
};
use generative::model::{Program, SourceDdl, SourceDdlEvent, SourceDdlPlan};
use generative::run::judge::Verdict;
use generative::run::{
    RunError, SourceDdlRun, run_convergence_with_source_ddl,
    run_convergence_with_source_ddl_allowing,
};
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

/// Runs `program` with `plan` against a fresh database on `cluster`, as the
/// deployment roles.
async fn run(
    cluster: &TestCluster,
    program: &Program,
    plan: &SourceDdlPlan,
) -> Result<SourceDdlRun, String> {
    let db = CaseDatabase::provision(cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match run_convergence_with_source_ddl(&mut backend, &pool, program, plan).await {
        Ok(run) => Ok(run),
        Err(RunError::NeverSilentlyWrong {
            op_index,
            target,
            problem,
            detail,
        }) => Err(format!(
            "after op {op_index}, {target}: {problem}\nplan: {plan:?}\n{detail}"
        )),
        Err(other) => Err(format!("run error with plan {plan:?}: {other:?}")),
    }
}

fn program_with_source_ddl_plan() -> impl Strategy<Value = (Program, SourceDdlPlan)> {
    trivial_program().prop_flat_map(|program| {
        let plan = source_ddl_plan_for(&program);
        (Just(program), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// Source DDL anywhere in a generated program never leaves a definition
    /// silently wrong outside a listed gap.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_source_ddl_is_never_silently_wrong((program, plan) in program_with_source_ddl_plan()) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map(|_| ())
                .map_err(TestCaseError::fail)
        })?;
    }
}

/// No database: every drawn plan stays inside its program, its events don't
/// overlap, and across enough draws every action shows up, with and
/// without a pass inside, and with ops in flight.
#[test]
fn drawn_plans_stay_in_bounds_and_cover_every_action() {
    let mut runner = TestRunner::deterministic();
    let strategy = program_with_source_ddl_plan();
    let mut seen = std::collections::BTreeSet::new();
    let mut windows = false;
    for _ in 0..500 {
        let (program, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        let mut end = None;
        for event in &plan.events {
            assert!(event.last_op() < program.ops.len(), "{event:?}");
            assert!(
                program.defs.iter().any(|d| d.source == event.table),
                "{event:?} is on a table no definition reads"
            );
            assert!(end.is_none_or(|end| event.op > end), "{plan:?}");
            end = Some(event.last_op());
            windows |= event.window > 0;
            seen.insert((event.action.kind(), event.pass_inside));
        }
    }
    for kind in [
        "vacuum_full",
        "cluster",
        "reindex",
        "reindex_concurrently",
        "rewrite_column",
        "rewrite_beside_default",
        "disable_capture_triggers",
    ] {
        for pass in [false, true] {
            assert!(
                seen.contains(&(kind, pass)),
                "{kind} pass_inside={pass}: {seen:?}"
            );
        }
    }
    assert!(windows, "no event drew ops in flight");
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
    assert_eq!(program.ops.len(), 6);
    program
}

/// `c1`, the column both definitions read.
fn c1(program: &Program) -> String {
    program.tables[0].columns[1].name.clone()
}

fn event(
    program: &Program,
    op: usize,
    action: SourceDdl,
    window: usize,
    pass_inside: bool,
) -> SourceDdlEvent {
    SourceDdlEvent {
        op,
        table: program.tables[0].name.clone(),
        action,
        window,
        pass_inside,
    }
}

/// The verdicts at the check right after `event`, one per definition.
fn verdicts_after(run: &SourceDdlRun, event: &SourceDdlEvent) -> Vec<Verdict> {
    run.checks
        .iter()
        .find(|(fired, _)| fired.as_ref() == Some(event))
        .unwrap_or_else(|| panic!("no check after {event:?}"))
        .1
        .iter()
        .map(|judged| judged.verdict.clone())
        .collect()
}

async fn run_one(program: &Program, event: SourceDdlEvent) -> Vec<Verdict> {
    let cluster = TestCluster::start();
    let plan = SourceDdlPlan {
        events: vec![event.clone()],
    };
    let run = run(&cluster, program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    verdicts_after(&run, &event)
}

/// Every verdict is a pause by the pass's capture check, exact after its
/// `RESUME`.
fn assert_paused_by_the_capture_check(verdicts: Vec<Verdict>) {
    for verdict in verdicts {
        assert!(
            matches!(
                &verdict,
                Verdict::ResumedExact { reason }
                    if reason.contains("capture of public.") && reason.contains("was changed outside Trellis")
            ),
            "{verdict:?}"
        );
    }
}

/// #2184's decision 4: capture triggers disabled and enabled again by name,
/// with a delete and an update in between and no reconcile pass until
/// after, pause both readers with a reason; their `RESUME` makes them
/// exact.
#[tokio::test(flavor = "multi_thread")]
async fn capture_disabled_and_restored_inside_one_interval_pauses_its_readers() {
    let program = one_to_one_and_aggregate_program();
    let event = event(&program, 3, SourceDdl::DisableCaptureTriggers, 2, false);
    assert_paused_by_the_capture_check(run_one(&program, event).await);
}

/// The same with a pass while the triggers are disabled.
#[tokio::test(flavor = "multi_thread")]
async fn capture_disabled_across_a_pass_pauses_its_readers() {
    let program = one_to_one_and_aggregate_program();
    let event = event(&program, 3, SourceDdl::DisableCaptureTriggers, 2, true);
    assert_paused_by_the_capture_check(run_one(&program, event).await);
}

/// `VACUUM FULL`, `CLUSTER` and `REINDEX` (plain and `CONCURRENTLY`) with
/// changes in flight leave every definition exact: they move file nodes and
/// rebuild indexes, and change no value.
#[tokio::test(flavor = "multi_thread")]
async fn table_maintenance_with_changes_in_flight_keeps_every_definition_exact() {
    let program = one_to_one_and_aggregate_program();
    let cluster = TestCluster::start();
    let plan = SourceDdlPlan {
        events: vec![
            event(&program, 0, SourceDdl::VacuumFull, 1, false),
            event(&program, 2, SourceDdl::Cluster, 0, false),
            event(
                &program,
                3,
                SourceDdl::Reindex {
                    concurrently: false,
                },
                0,
                false,
            ),
            event(
                &program,
                4,
                SourceDdl::Reindex { concurrently: true },
                1,
                true,
            ),
        ],
    };
    let run = run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    for event in &plan.events {
        for verdict in verdicts_after(&run, event) {
            assert_eq!(verdict, Verdict::Exact, "{event:?}");
        }
    }
}

/// A same-type `USING` rewrite of the read column, with a delete in flight,
/// is re-read by the pass: no pause, every definition exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_same_type_rewrite_of_a_read_column_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let column = c1(&program);
    let event = event(&program, 3, SourceDdl::RewriteColumn { column }, 1, false);
    for verdict in run_one(&program, event).await {
        assert_eq!(verdict, Verdict::Exact);
    }
}

/// The register's `alter-type-rewrite` shape: a `SET DEFAULT` and a
/// same-type rewrite of the read column inside one reconcile interval. The
/// pass reads it as routine work, so both readers keep the old values;
/// `self_check` reports them and `request_backfill` repairs them.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_beside_a_default_inside_one_interval_is_the_listed_gap() {
    let program = one_to_one_and_aggregate_program();
    let column = c1(&program);
    let event = event(
        &program,
        3,
        SourceDdl::RewriteBesideDefault { column },
        1,
        false,
    );
    assert_eq!(event.listed_gaps(), vec![Gap::AlterTypeRewrite]);
    for verdict in run_one(&program, event).await {
        assert_eq!(verdict, Verdict::ListedGap(Gap::AlterTypeRewrite));
    }
}

/// The same with a pass between the `SET DEFAULT` and the rewrite: the next
/// pass sees the rewrite alone and re-reads the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_after_a_default_across_a_pass_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let column = c1(&program);
    let event = event(
        &program,
        3,
        SourceDdl::RewriteBesideDefault { column },
        1,
        true,
    );
    assert!(event.listed_gaps().is_empty());
    for verdict in run_one(&program, event).await {
        assert_eq!(verdict, Verdict::Exact);
    }
}

/// The control beside the listed-gap pin: the same shape judged as if the
/// register had no entry for it fails the run as silently wrong, so the
/// judge tells a listed gap from a missing one.
#[tokio::test(flavor = "multi_thread")]
async fn the_same_shape_without_its_register_entry_fails_as_silently_wrong() {
    let program = one_to_one_and_aggregate_program();
    let column = c1(&program);
    let plan = SourceDdlPlan {
        events: vec![event(
            &program,
            3,
            SourceDdl::RewriteBesideDefault { column },
            1,
            false,
        )],
    };
    let cluster = TestCluster::start();
    let db = CaseDatabase::provision(&cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match run_convergence_with_source_ddl_allowing(&mut backend, &pool, &program, &plan, |_| {
        Vec::new()
    })
    .await
    {
        Err(RunError::NeverSilentlyWrong {
            op_index, problem, ..
        }) => {
            assert_eq!(op_index, 4);
            assert!(problem.starts_with("silently wrong"), "{problem}");
        }
        other => panic!("expected NeverSilentlyWrong, got {other:?}"),
    }
}

/// The register's `alter-type-rewrite` entry, for a key column: a 1-1
/// target's rows under the old keys stay after the next write to each row,
/// which lands under its new key. The entry said that write repairs the
/// row; it repairs nothing for a key column.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_column_rewrite_beside_a_default_leaves_the_old_keys_in_a_one_to_one_target() {
    use generative::backend::Backend;
    use generative::run::judge::PassStepper;

    let program = one_to_one_and_aggregate_program();
    let table = format!("public.{}", program.tables[0].name);
    let pk = program.tables[0].pk_col.clone();
    let c1 = c1(&program);
    let cluster = TestCluster::start();
    let db = CaseDatabase::provision(&cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    backend.set_reconcile_interval(std::time::Duration::from_secs(3600));
    backend.install(&program).await.expect("install");
    let passes = PassStepper::connect(&backend).await.expect("pass session");
    for op in &program.ops {
        backend.apply(op).await.expect("apply");
    }
    passes.settle(&mut backend).await.expect("settle");

    // Inside one reconcile interval: a metadata change, the key rewrite,
    // and a write to every row through a column the 1-1 reads.
    for sql in [
        format!("ALTER TABLE {table} ALTER COLUMN {pk} SET DEFAULT 0"),
        format!("ALTER TABLE {table} ALTER COLUMN {pk} TYPE bigint USING {pk} + 10"),
        format!("UPDATE {table} SET {c1} = coalesce({c1}, 0) + 1"),
    ] {
        backend.execute_admin(&sql).await.expect(&sql);
    }
    passes.settle(&mut backend).await.expect("settle");

    let one_to_one = &program.defs[0].target;
    let states = backend
        .definition_states(&program.defs)
        .await
        .expect("definition states");
    assert_eq!(states[0].status, "live", "the 1-1 reports healthy");
    let snapshot = backend.snapshot().await.expect("snapshot");
    let keys: Vec<&String> = snapshot[one_to_one].keys().collect();
    // pk 2 was deleted; pks 1 and 3 are now 11 and 13.
    assert_eq!(
        keys,
        ["1", "11", "13", "3"].iter().collect::<Vec<_>>(),
        "the old keys stay beside the new ones"
    );
}

/// Found by the first sweep: one row in its group, then a same-type
/// rewrite of the aggregate's `GROUP BY` column with the insert's change in
/// flight.
fn one_row_grouped_program() -> Program {
    build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![(Some(79), None)],
            text_values: vec![None],
            bool_values: vec![None],
            uuid_values: vec![None],
            grain_values: vec![Some("0".to_string())],
            mutates: vec![
                Mutate::Delete { pk: 2 },
                Mutate::Update {
                    pk: 1,
                    c1: None,
                    c2: None,
                },
            ],
        }],
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![
                    AggregateFn::Sum(AggregateColumn::C2),
                    AggregateFn::Count,
                    AggregateFn::CountColumn(AggregateColumn::C2),
                    AggregateFn::Avg(AggregateColumn::C2),
                ],
            },
        )],
    )
}

/// The `GROUP BY` column rewritten on `program`'s only table after op `op`,
/// with the next op in flight; every definition must stay exact.
async fn rewrite_group_by_column(program: &Program, op: usize, pass_inside: bool) {
    let grain = program.tables[0].columns[6].name.clone();
    let event = event(
        program,
        op,
        SourceDdl::RewriteColumn { column: grain },
        1,
        pass_inside,
    );
    for verdict in run_one(program, event).await {
        assert_eq!(verdict, Verdict::Exact, "op={op} pass_inside={pass_inside}");
    }
}

/// #2219: with the insert's change still in flight, the target keeps the
/// old group beside the new one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "#2219: a GROUP BY column rewritten with an insert in flight keeps the old group"]
async fn a_rewrite_of_a_group_by_column_with_a_change_in_flight_is_reread() {
    let program = one_row_grouped_program();
    rewrite_group_by_column(&program, 0, false).await;
    rewrite_group_by_column(&program, 0, true).await;
}

/// The control beside #2219: the same rewrite once the insert has been
/// applied is re-read exactly.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_of_a_group_by_column_after_its_changes_settle_is_reread() {
    rewrite_group_by_column(&one_row_grouped_program(), 1, false).await;
}
