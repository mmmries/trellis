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
use generative::model::{
    Collation, Program, RetypeShape, Server, SourceDdl, SourceDdlEvent, SourceDdlPlan, SqlType,
};
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
use trellis::dev::defs::ast::{Expr, KeySpace, Operator};
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
    for _ in 0..2000 {
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
            if let SourceDdl::Retype {
                using: true,
                shape: RetypeShape::Key,
                ..
            } = &event.action
            {
                assert_eq!(
                    event.op + 1,
                    program.ops.len(),
                    "a key rewrite before the last op: {event:?}"
                );
            }
            if event.action.terminal() {
                assert_eq!(
                    (event.op + 1, event.window),
                    (program.ops.len(), 0),
                    "a column change later ops can't write through, before the last op: {event:?}"
                );
            }
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
        "retype_widen",
        "retype_widen_using",
        "retype_narrow",
        "retype_narrow_using",
        "retype_outgrow",
        "retype_outgrow_using",
        "retype_key",
        "retype_key_using",
        "rename_column",
        "drop_column",
        "drop_and_add_column",
        "add_column",
        "swap_column_names",
        "shadow_column",
        "generated_column",
        "flip_not_null",
        "set_default",
        "set_default_beside_vacuum",
        "collate_c",
        "collate_nondeterministic",
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

/// The table's last column, the aggregate's `GROUP BY` column: no column
/// after it is read, so a rewrite of it moves no other read column's row.
fn grain(program: &Program) -> String {
    let table = &program.tables[0];
    let grain = table.columns.last().expect("a column").name.clone();
    assert!(
        matches!(&program.defs[1].key_space, KeySpace::Aggregate { group_by } if group_by.as_slice() == std::slice::from_ref(&grain))
    );
    grain
}

/// Whether `cluster`'s server versions attributes ([`Server`]).
async fn server_versions_attributes(cluster: &TestCluster) -> bool {
    let db = CaseDatabase::provision(cluster).await;
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect the probe");
    tokio::spawn(connection);
    Server::probe(&client)
        .await
        .expect("probe the server")
        .versions_attributes
}

/// Both kinds of server, for a shape the register lists on either.
const SERVERS: [Server; 2] = [
    Server {
        versions_attributes: false,
        sets_expressions: true,
    },
    Server {
        versions_attributes: true,
        sets_expressions: true,
    },
];

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
/// same-type rewrite of the table's last read column (the aggregate's
/// `GROUP BY` column) inside one reconcile interval. The pass reads it as
/// routine work, so the aggregate keeps the old groups; `self_check`
/// reports them and `request_backfill` repairs them. The 1-1 doesn't read
/// the column.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_of_the_last_read_column_beside_a_default_is_the_listed_gap() {
    let program = one_to_one_and_aggregate_program();
    let column = grain(&program);
    let event = event(
        &program,
        3,
        SourceDdl::RewriteBesideDefault { column },
        1,
        false,
    );
    for server in SERVERS {
        assert_eq!(
            event.listed_gaps(&program, server),
            vec![Gap::AlterTypeRewrite]
        );
    }
    assert_eq!(
        run_one(&program, event).await,
        [Verdict::Exact, Verdict::ListedGap(Gap::AlterTypeRewrite)]
    );
}

/// The same shape on `c1`, which read columns follow. A server that
/// versions attributes moves their rows too, so the pass sees a same-type
/// rewrite on them and re-reads the table: every definition exact. An older
/// server leaves the listed gap.
#[tokio::test(flavor = "multi_thread")]
async fn a_rewrite_beside_a_default_is_reread_through_a_later_read_column_where_attributes_are_versioned()
 {
    let program = one_to_one_and_aggregate_program();
    let cluster = TestCluster::start();
    let expected = if server_versions_attributes(&cluster).await {
        Verdict::Exact
    } else {
        Verdict::ListedGap(Gap::AlterTypeRewrite)
    };
    let column = c1(&program);
    let event = event(
        &program,
        3,
        SourceDdl::RewriteBesideDefault { column },
        1,
        false,
    );
    assert_eq!(event.listed_gaps(&program, SERVERS[1]), Vec::new());
    assert_eq!(
        event.listed_gaps(&program, SERVERS[0]),
        vec![Gap::AlterTypeRewrite]
    );
    let plan = SourceDdlPlan {
        events: vec![event.clone()],
    };
    let run = run(&cluster, &program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    for verdict in verdicts_after(&run, &event) {
        assert_eq!(verdict, expected);
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
    for server in SERVERS {
        assert!(event.listed_gaps(&program, server).is_empty());
    }
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
    let column = grain(&program);
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
    match run_convergence_with_source_ddl_allowing(&mut backend, &pool, &program, &plan, |_, _| {
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

/// The register's `alter-type-rewrite` entry, for a key column: on a
/// server that doesn't version attributes, a 1-1 target's rows under the
/// old keys stay after the next write to each row, which lands under its
/// new key. That write repairs nothing for a key column. A server that
/// versions attributes moves the rows of the read columns after the key,
/// so the pass re-reads the table and only the new keys remain.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_column_rewrite_beside_a_default_leaves_the_old_keys_unless_attributes_are_versioned()
{
    use generative::backend::Backend;
    use generative::run::judge::PassStepper;

    let program = one_to_one_and_aggregate_program();
    let table = format!("public.{}", program.tables[0].name);
    let pk = program.tables[0].pk_col.clone();
    let c1 = c1(&program);
    let cluster = TestCluster::start();
    let versioned = server_versions_attributes(&cluster).await;
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
    if versioned {
        assert_eq!(keys, ["11", "13"].iter().collect::<Vec<_>>(), "re-read");
    } else {
        assert_eq!(
            keys,
            ["1", "11", "13", "3"].iter().collect::<Vec<_>>(),
            "the old keys stay beside the new ones"
        );
    }
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

/// The same with the insert's change still in flight (#2219): the
/// rebuild reads every ring image, so no old group stays beside the new one.
#[tokio::test(flavor = "multi_thread")]
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

// ---------------------------------------------------------------------
// `ALTER COLUMN TYPE` (#2184 slice 3). On `one_to_one_and_aggregate_program`
// the 1-1 reads `c1` in its calculated field `total` (`c1 + c2`) and the
// aggregate reads `c1` in `SUM(c1)` and groups by the grain column.
// ---------------------------------------------------------------------

fn retype(column: &str, to: SqlType, using: bool, shape: RetypeShape) -> SourceDdl {
    SourceDdl::Retype {
        column: column.to_string(),
        to,
        using,
        shape,
    }
}

const NUMERIC_16_2: SqlType = SqlType::Numeric {
    precision: 16,
    scale: 2,
};

/// The verdicts after each of `plan`'s events, in order.
async fn run_plan(program: &Program, plan: SourceDdlPlan) -> Vec<Vec<Verdict>> {
    let cluster = TestCluster::start();
    let run = run(&cluster, program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    plan.events
        .iter()
        .map(|event| verdicts_after(&run, event))
        .collect()
}

fn is_resumed(verdict: &Verdict) -> bool {
    matches!(verdict, Verdict::ResumedExact { .. })
}

/// A pure widening of a modifier (`numeric(14,2)` to `numeric(16,2)`, and to
/// a bare `numeric`) changes only the catalog: no copy has a modifier, so
/// nothing pauses or re-reads, and every definition stays exact with
/// changes in flight.
#[tokio::test(flavor = "multi_thread")]
async fn a_pure_widening_of_a_read_column_keeps_every_definition_exact() {
    let program = one_to_one_and_aggregate_program();
    let c1 = c1(&program);
    let grain = grain(&program);
    let plan = SourceDdlPlan {
        events: vec![
            event(
                &program,
                1,
                retype(&c1, NUMERIC_16_2, false, RetypeShape::Widen),
                1,
                false,
            ),
            event(
                &program,
                3,
                retype(&grain, SqlType::NumericFree, false, RetypeShape::Widen),
                1,
                true,
            ),
        ],
    };
    for verdicts in run_plan(&program, plan).await {
        assert_eq!(verdicts, [Verdict::Exact, Verdict::Exact]);
    }
}

/// The register's "widening that changes values" (`alter-type-rewrite`):
/// `USING` changes every value of the `GROUP BY` column, the table's last
/// read column, and the pass sees only a widening no copy outgrows, so the
/// aggregate keeps its old groups. `self_check` reports them and
/// `request_backfill` repairs them. The 1-1 doesn't read the column.
#[tokio::test(flavor = "multi_thread")]
async fn a_widening_that_changes_values_is_the_listed_gap() {
    let program = one_to_one_and_aggregate_program();
    let grain = grain(&program);
    for (to, pass_inside) in [(NUMERIC_16_2, false), (SqlType::NumericFree, true)] {
        let event = event(
            &program,
            3,
            retype(&grain, to, true, RetypeShape::Widen),
            1,
            pass_inside,
        );
        for server in SERVERS {
            assert_eq!(
                event.listed_gaps(&program, server),
                vec![Gap::AlterTypeRewrite]
            );
        }
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::ListedGap(Gap::AlterTypeRewrite)],
            "{to:?}"
        );
    }
}

/// A narrower `numeric` scale, with `USING`, of a column read as a field is
/// a changed column both readers are still valid over: the pass re-reads
/// the table. The same of a `GROUP BY` column changes values, not only their
/// text (ADR-0020), so the aggregate pauses, and its `RESUME` ends with
/// every group equal by value.
#[tokio::test(flavor = "multi_thread")]
async fn a_narrower_scale_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let narrower = SqlType::Numeric {
        precision: 14,
        scale: 1,
    };
    let plan = SourceDdlPlan {
        events: vec![
            event(
                &program,
                1,
                retype(&c1(&program), narrower, true, RetypeShape::Narrow),
                1,
                false,
            ),
            event(
                &program,
                3,
                retype(&grain(&program), narrower, false, RetypeShape::Narrow),
                1,
                true,
            ),
        ],
    };
    let verdicts = run_plan(&program, plan).await;
    assert_eq!(verdicts[0], [Verdict::Exact, Verdict::Exact]);
    assert_eq!(verdicts[1][0], Verdict::Exact);
    assert!(is_resumed(&verdicts[1][1]), "{verdicts:?}");
}

/// `numeric` to `integer` changes the type both definitions evaluate `c1`
/// as, so both pause, and `RESUME` re-types and rebuilds them. Then `integer`
/// to `bigint` (and, in the second case, with `USING`) outgrows the
/// `bigint` sum the `RESUME` made: it pauses again. The 1-1's `total`
/// holds no copy of the column, so the widening with `USING` is the
/// register's listed gap there, unless the server versions attributes: then
/// the rewrite moves the rows of the read columns after `c1`, and the pass
/// after the pause re-reads the 1-1.
#[tokio::test(flavor = "multi_thread")]
async fn a_widening_that_outgrows_a_copy_pauses_its_readers() {
    let program = one_to_one_and_aggregate_program();
    let c1 = c1(&program);
    let versioned = server_versions_attributes(&TestCluster::start()).await;
    for using in [false, true] {
        let plan = SourceDdlPlan {
            events: vec![
                event(
                    &program,
                    1,
                    retype(&c1, SqlType::Int4, using, RetypeShape::Narrow),
                    1,
                    false,
                ),
                event(
                    &program,
                    3,
                    retype(&c1, SqlType::Int8, using, RetypeShape::Outgrow),
                    1,
                    using,
                ),
            ],
        };
        let verdicts = run_plan(&program, plan).await;
        assert!(
            verdicts[0].iter().all(is_resumed),
            "using={using}: {verdicts:?}"
        );
        let one_to_one = if using && !versioned {
            Verdict::ListedGap(Gap::AlterTypeRewrite)
        } else {
            Verdict::Exact
        };
        assert_eq!(verdicts[1][0], one_to_one, "using={using}");
        assert!(is_resumed(&verdicts[1][1]), "using={using}: {verdicts:?}");
    }
}

/// The primary key narrowed from `bigint` to `integer` with changes in
/// flight: every stored key renders the same and every copy still holds
/// the column, so no definition pauses, and the pass re-reads the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_narrowing_every_copy_holds_keeps_every_definition_exact() {
    let program = one_to_one_and_aggregate_program();
    let pk = program.tables[0].pk_col.clone();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            2,
            retype(&pk, SqlType::Int4, false, RetypeShape::Key),
            2,
            pass_inside,
        );
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "pass_inside={pass_inside}"
        );
    }
}

/// The primary key made a `numeric`, which define refuses as a source key:
/// both readers pause, and their `RESUME` refuses with its code.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_retyped_to_a_refused_type_pauses_and_its_resume_refuses() {
    let program = one_to_one_and_aggregate_program();
    let pk = program.tables[0].pk_col.clone();
    let event = event(
        &program,
        2,
        retype(&pk, SqlType::NumericFree, false, RetypeShape::Key),
        1,
        false,
    );
    let verdicts = run_one(&program, event).await;
    for verdict in &verdicts {
        assert!(
            matches!(verdict, Verdict::ResumeRefused { code, .. } if code == "T0121"),
            "{verdicts:?}"
        );
    }
}

/// A same-type `USING` rewrite of the primary key at the program's last op:
/// every key changes. The pass sees the rewrite, so the 1-1 is rebuilt
/// under the new keys and no row stays under an old one.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_rewrite_at_the_last_op_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let pk = program.tables[0].pk_col.clone();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            5,
            retype(&pk, SqlType::Int8, true, RetypeShape::Key),
            0,
            pass_inside,
        );
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "pass_inside={pass_inside}"
        );
    }
}

/// The same rewrite narrowing the key to `integer` (#2254): the pass
/// re-reads the table, so the 1-1 keeps no row under an old key.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_narrowing_rewrite_at_the_last_op_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let pk = program.tables[0].pk_col.clone();
    let event = event(
        &program,
        5,
        retype(&pk, SqlType::Int4, true, RetypeShape::Key),
        0,
        false,
    );
    assert_eq!(
        run_one(&program, event).await,
        [Verdict::Exact, Verdict::Exact]
    );
}

/// The same of a `GROUP BY` key (#2254): `bigint` to `integer` with `USING`
/// is re-read, so the aggregate keeps no old group.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_by_key_narrowing_rewrite_is_reread() {
    let program = one_to_one_and_aggregate_program();
    let grain = grain(&program);
    let plan = SourceDdlPlan {
        events: vec![
            event(
                &program,
                1,
                retype(&grain, SqlType::Int8, false, RetypeShape::Narrow),
                0,
                false,
            ),
            event(
                &program,
                3,
                retype(&grain, SqlType::Int4, true, RetypeShape::Narrow),
                1,
                false,
            ),
        ],
    };
    let verdicts = run_plan(&program, plan).await;
    assert_eq!(
        verdicts[1],
        [Verdict::Exact, Verdict::Exact],
        "{verdicts:?}"
    );
}

// ---------------------------------------------------------------------
// Columns renamed, dropped and added (#2184 slice 4), on
// `one_to_one_and_aggregate_program`: the 1-1 reads `c1` and `c2` in
// `total` (`c1 + c2`), the aggregate `c1` in `SUM(c1)`.
// ---------------------------------------------------------------------

/// `c2`, the other numeric column, read only by the 1-1's `total`.
fn c2(program: &Program) -> String {
    program.tables[0].columns[2].name.clone()
}

/// A read column renamed away or dropped after the last op, with its
/// change in flight: both readers pause, and their `RESUME` refuses with a
/// code while the column is missing.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_column_renamed_or_dropped_pauses_and_its_resume_refuses() {
    let program = one_to_one_and_aggregate_program();
    let c1 = c1(&program);
    for action in [
        SourceDdl::RenameColumn { column: c1.clone() },
        SourceDdl::DropColumn { column: c1.clone() },
    ] {
        for pass_inside in [false, true] {
            let event = event(&program, 5, action.clone(), 0, pass_inside);
            let verdicts = run_one(&program, event).await;
            for verdict in &verdicts {
                assert!(
                    matches!(verdict, Verdict::ResumeRefused { code, .. } if code == "T0044"),
                    "{action:?} pass_inside={pass_inside}: {verdicts:?}"
                );
            }
        }
    }
}

/// A read column dropped and added again under its name and type, with
/// changes in flight: every row holds `NULL` under a new `attnum`, and the
/// pass re-reads the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_column_dropped_and_added_again_is_reread() {
    let program = one_to_one_and_aggregate_program();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            2,
            SourceDdl::DropAndAddColumn {
                column: c1(&program),
            },
            2,
            pass_inside,
        );
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "pass_inside={pass_inside}"
        );
    }
}

/// An unread column added with a volatile default rewrites the table and
/// changes nothing a definition reads.
#[tokio::test(flavor = "multi_thread")]
async fn an_unread_column_added_keeps_every_definition_exact() {
    let program = one_to_one_and_aggregate_program();
    let event = event(
        &program,
        2,
        SourceDdl::AddColumn {
            column: "added_2".to_string(),
        },
        2,
        false,
    );
    assert_eq!(
        run_one(&program, event).await,
        [Verdict::Exact, Verdict::Exact]
    );
}

/// [`one_to_one_and_aggregate_program`] with the 1-1's `total` as `c1 - c2`,
/// which a swap of the two changes, so a swap shows in both readers.
fn swap_reading_program() -> Program {
    let mut program = one_to_one_and_aggregate_program();
    let total = &mut program.defs[0].fields[0];
    assert_eq!(total.name, "total");
    let Expr::BinaryOp { op, .. } = &mut total.expr else {
        panic!("total is c1 + c2: {:?}", total.expr);
    };
    *op = Operator::Subtract;
    program
}

/// The register's `column-name-swap`: `c1`'s name taken by `c2` (a swap) or
/// by a column added under it, an update in between, and the names given
/// back with no pass in between. Capture images the column that has the
/// name, so the 1-1's `c1 - c2` and the aggregate's `SUM(c1)` take the other
/// column's value, and after the names are back the pass sees only `xmin`
/// moved. `self_check` reports it and `request_backfill` repairs it.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_columns_name_taken_and_given_back_inside_one_interval_is_the_listed_gap() {
    let program = swap_reading_program();
    let (c1, c2) = (c1(&program), c2(&program));
    let gap = Verdict::ListedGap(Gap::ColumnNameSwap);
    for action in [
        SourceDdl::SwapColumnNames {
            column: c1.clone(),
            other: c2.clone(),
        },
        SourceDdl::ShadowColumn { column: c1.clone() },
    ] {
        let event = event(&program, 2, action.clone(), 1, false);
        assert_eq!(
            event.listed_gaps(&program, Server::default()),
            vec![Gap::ColumnNameSwap]
        );
        assert_eq!(
            run_one(&program, event).await,
            [gap.clone(), gap.clone()],
            "{action:?}"
        );
    }
}

/// The same with a pass while the other column has the name: that pass
/// re-reads the table, and so does the one after the names are back, so
/// every definition ends exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_columns_name_taken_across_a_pass_is_reread() {
    let program = swap_reading_program();
    let (c1, c2) = (c1(&program), c2(&program));
    for action in [
        SourceDdl::SwapColumnNames {
            column: c1.clone(),
            other: c2.clone(),
        },
        SourceDdl::ShadowColumn { column: c1.clone() },
    ] {
        let event = event(&program, 2, action.clone(), 1, true);
        assert!(event.listed_gaps(&program, Server::default()).is_empty());
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "{action:?}"
        );
    }
}

/// A read column made a stored generated column after the last op, and
/// (PostgreSQL 17 and later) its expression replaced: the pass re-reads the
/// table for each, inside one interval or with a pass between.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_column_made_generated_and_its_expression_replaced_is_reread() {
    let program = one_to_one_and_aggregate_program();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            5,
            SourceDdl::GeneratedColumn {
                column: c1(&program),
            },
            0,
            pass_inside,
        );
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "pass_inside={pass_inside}"
        );
    }
}

/// `c1`'s `NOT NULL` set with changes in flight and dropped again at the
/// close, and a default set (beside a `VACUUM FULL` or not) and dropped:
/// each moves only the column's catalog row, so every reader stays exact,
/// inside one interval or across a pass.
#[tokio::test(flavor = "multi_thread")]
async fn not_null_and_default_changes_keep_every_definition_exact() {
    let program = one_to_one_and_aggregate_program();
    let c1 = c1(&program);
    for action in [
        SourceDdl::FlipNotNull {
            column: c1.clone(),
            key: false,
        },
        SourceDdl::SetDefault {
            column: c1.clone(),
            rewrite: false,
        },
        SourceDdl::SetDefault {
            column: c1.clone(),
            rewrite: true,
        },
    ] {
        for pass_inside in [false, true] {
            let event = event(&program, 2, action.clone(), 3, pass_inside);
            assert!(event.listed_gaps(&program, Server::default()).is_empty());
            assert_eq!(
                run_one(&program, event).await,
                [Verdict::Exact, Verdict::Exact],
                "{action:?} pass_inside={pass_inside}"
            );
        }
    }
}

/// [`one_to_one_and_aggregate_program`] with text values a collation
/// decides (`'OPEN' = 'open'` under a case-insensitive collation, `'B'`
/// sorting before `'b'` under `"C"` and not under the database's ICU
/// `en-US`), and the 1-1 also computing `text = 'open'`. Define refuses an
/// ordering comparison over `text` (T0081). The updates at ops 3 and 5
/// recompute rows `1` (`'B'`) and `3` (`'OPEN'`).
fn collation_reading_program() -> Program {
    let mut program = one_to_one_and_aggregate_program();
    let text = text_column(&program);
    for (op, value) in program.ops.iter_mut().take(3).zip(["B", "a", "OPEN"]) {
        let generative::model::Op::Insert { row, .. } = op else {
            panic!("ops 0-2 seed: {op:?}");
        };
        let cell = row
            .iter_mut()
            .find(|(c, _)| *c == text)
            .expect("a seed writes the text column");
        cell.1 = Some(value.to_string());
    }
    let compare = |op: Operator, literal: &str| Expr::BinaryOp {
        op,
        lhs: Box::new(Expr::Column(text.clone())),
        rhs: Box::new(Expr::StringLiteral(literal.to_string())),
    };
    program.defs[0]
        .fields
        .push(trellis::dev::defs::ast::FieldDef {
            name: "is_open".to_string(),
            expr: compare(Operator::Equal, "open"),
        });
    program
}

/// The table's `text` column.
fn text_column(program: &Program) -> String {
    program.tables[0]
        .columns
        .iter()
        .find(|c| c.value_type == trellis::dev::defs::ast::ValueType::Text)
        .expect("every table has a text column")
        .name
        .clone()
}

/// The register's `nondeterministic-collation`: the text column made
/// case-insensitive with the updates in flight, and given its collation back
/// at the close. The 1-1 computed row `3`'s `text = 'open'` under the new
/// collation, and the pass doesn't see the change, with or without a pass
/// between. `self_check` reports it, and with the collation back,
/// `request_backfill` repairs it. The aggregate doesn't read the column.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_column_made_case_insensitive_is_the_listed_gap() {
    let program = collation_reading_program();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            2,
            SourceDdl::Collate {
                column: text_column(&program),
                collation: Collation::CaseInsensitive,
            },
            3,
            pass_inside,
        );
        assert_eq!(
            event.listed_gaps(&program, Server::default()),
            vec![Gap::NondeterministicCollation]
        );
        assert_eq!(
            run_one(&program, event).await,
            [
                Verdict::ListedGap(Gap::NondeterministicCollation),
                Verdict::Exact
            ],
            "pass_inside={pass_inside}"
        );
    }
}

/// The text column given the deterministic `"C"` with the updates in
/// flight, and its collation back at the close: a deterministic
/// collation's `=` is byte equality, and define refuses the rest of what a
/// collation decides, so every reader stays exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_column_given_another_deterministic_collation_stays_exact() {
    let program = collation_reading_program();
    for pass_inside in [false, true] {
        let event = event(
            &program,
            2,
            SourceDdl::Collate {
                column: text_column(&program),
                collation: Collation::C,
            },
            3,
            pass_inside,
        );
        assert!(event.listed_gaps(&program, Server::default()).is_empty());
        assert_eq!(
            run_one(&program, event).await,
            [Verdict::Exact, Verdict::Exact],
            "pass_inside={pass_inside}"
        );
    }
}

/// [`one_to_one_and_aggregate_program`] read through `KEY (k0)`, `k0`
/// nullable (a `NULLS NOT DISTINCT` index) or not (a plain one). The seeds
/// write `chr(1)`, `'a' || chr(1) || chr(1)` and `''`, the keys the
/// encoding turns on, and op 3's update moves row `1` to a doubled
/// `chr(1)`; no op writes a `NULL` key.
fn keyed_program(nullable: bool) -> Program {
    let s = |v: &str| Some(v.to_string());
    generative::generate::source_key::key_source(
        one_to_one_and_aggregate_program(),
        &generative::generate::source_key::KeyDraw {
            shape: generative::generate::source_key::KeyShape {
                nullable: vec![nullable],
                primary_key: true,
            },
            inserts: vec![vec![s("\u{1}")], vec![s("a\u{1}\u{1}")], vec![s("")]],
            moves: vec![Some(vec![s("\u{1}\u{1}")]), None],
        },
    )
}

/// The `NOT NULL` of a `KEY (…)` column flipped with key changes in flight,
/// and flipped back at the close, with them still staged: the key's
/// encoding is recorded with it, so the flip changes nothing
/// (`docs/staging-and-claiming/capture-drift.md`, #2147). On a
/// `NULLS NOT DISTINCT` index the set and the drop both keep every reader
/// exact, inside one interval or across a pass. On a plain index the drop
/// leaves the key unbacked: inside one interval nothing sees it, and a pass
/// that does pauses the readers, which resume exact once it is back.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_columns_not_null_flipped_with_changes_in_flight_is_never_silently_wrong() {
    for nullable in [true, false] {
        let program = keyed_program(nullable);
        for pass_inside in [false, true] {
            let event = event(
                &program,
                2,
                SourceDdl::FlipNotNull {
                    column: "k0".to_string(),
                    key: true,
                },
                3,
                pass_inside,
            );
            assert!(event.listed_gaps(&program, Server::default()).is_empty());
            let verdicts = run_one(&program, event).await;
            for verdict in &verdicts {
                let expected = if !nullable && pass_inside {
                    matches!(verdict, Verdict::ResumedExact { reason } if reason.starts_with("capture:"))
                } else {
                    *verdict == Verdict::Exact
                };
                assert!(
                    expected,
                    "nullable={nullable} pass_inside={pass_inside}: {verdicts:?}"
                );
            }
        }
    }
}
