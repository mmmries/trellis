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
use generative::generate::source_ddl::{calls_collation_refusing_function, source_ddl_plan_for};
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    partition_table,
};
use generative::model::{
    Collation, IndexChange, PartitionStrategy, Program, RetypeShape, Server, SourceDdl,
    SourceDdlEvent, SourceDdlPlan, SqlType, reads,
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
    // As many drain workers as the plan has lanes, so the lanes' changes
    // are claimed by several at once.
    let mut backend = ManualBackend::connect_as_with_options(db.roles(), plan.lanes.max(1), None)
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

/// A [`trivial_program`], or one time in four one whose first
/// definition's source is read through a `KEY (…)`, so the key's index has
/// something to change.
fn program_with_source_ddl_plan() -> impl Strategy<Value = (Program, SourceDdlPlan)> {
    generative::generate::source_key::trivial_or_keyed_program().prop_flat_map(|program| {
        let plan = source_ddl_plan_for(&program);
        (Just(program), plan)
    })
}

/// The most ops a `JOIN` program keeps for this tier: every op is judged,
/// so the `JOIN` tiers' streams of hundreds are cut to their first ones.
const MAX_JOIN_OPS: usize = 16;

/// A `JOIN` tier's program as this tier runs it: its first
/// [`MAX_JOIN_OPS`] ops (a prefix of a stream is one: each op's outcome
/// depends only on those before it), every definition installed up front.
/// Its concurrent plan is the `JOIN` tier's, not this one's: the source-DDL
/// plan draws its own lanes.
fn for_source_ddl(mut program: Program) -> Program {
    program.ops.truncate(MAX_JOIN_OPS);
    program.def_install_after_op = vec![0; program.defs.len()];
    program.restart_after_ops.clear();
    program.scale_out_after_ops.clear();
    program
}

/// A `JOIN`, 1-1 `JOIN` or star program from those tiers' strategies
/// ([`for_source_ddl`]), with a source-DDL plan over the tables it reads,
/// joined ones included.
fn join_program_with_source_ddl_plan() -> impl Strategy<Value = (Program, SourceDdlPlan)> {
    use generative::generate::join::join_case_strategy;
    use generative::generate::join_one_to_one::{one_to_one_case_strategy, star_case_strategy};
    prop_oneof![
        join_case_strategy().prop_map(|case| case.program),
        one_to_one_case_strategy().prop_map(|case| case.program),
        star_case_strategy().prop_map(|star| star.case.program),
    ]
    .prop_map(for_source_ddl)
    .prop_flat_map(|program| {
        let plan = source_ddl_plan_for(&program);
        (Just(program), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// `property_source_ddl_is_never_silently_wrong` over the `JOIN`
    /// key spaces: a `JOIN`, a 1-1 `JOIN` and a star, the change on their
    /// `FROM` or a joined table.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_join_source_ddl_is_never_silently_wrong((program, plan) in join_program_with_source_ddl_plan()) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &plan))
                .map(|_| ())
                .map_err(TestCaseError::fail)
        })?;
    }

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
/// without a pass inside, and with ops in flight, some from several lanes.
#[test]
fn drawn_plans_stay_in_bounds_and_cover_every_action() {
    let mut runner = TestRunner::deterministic();
    let strategy = program_with_source_ddl_plan();
    let mut seen = std::collections::BTreeSet::new();
    let mut windows = false;
    let mut from_lanes = false;
    for _ in 0..3000 {
        let (program, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        let mut end = None;
        for event in &plan.events {
            assert!(event.last_op() < program.ops.len(), "{event:?}");
            assert!(
                program.defs.iter().any(|d| reads(d, &event.table)),
                "{event:?} is on a table no definition reads"
            );
            assert!(end.is_none_or(|end| event.op > end), "{plan:?}");
            end = Some(event.last_op());
            windows |= event.window > 0;
            from_lanes |= plan.lanes > 1 && event.window > 1 && event.action.issues_from_lanes();
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
            if let SourceDdl::Collate {
                collation: Collation::CaseInsensitive,
                ..
            } = &event.action
            {
                let table = program.tables.iter().find(|t| t.name == event.table);
                assert!(
                    !calls_collation_refusing_function(&program, table.expect("a drawn table")),
                    "a case-insensitive collation under STRPOS or REGEXP_COUNT: {event:?}"
                );
            }
            if let SourceDdl::KeyIndex {
                change: IndexChange::Drop,
                ..
            } = &event.action
            {
                assert!(
                    program.ops[event.op + 1..=event.last_op()]
                        .iter()
                        .all(|o| o.table() != event.table
                            || *o.expect() != generative::model::OpOutcome::Fails),
                    "a write the key's index refuses inside its drop: {event:?}"
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
        "rename_table",
        "move_schema",
        "rename_table_away",
        "recreate_table",
        "drop_key_index",
        "replace_key_index",
        "rebuild_key_index_concurrently",
    ] {
        for pass in [false, true] {
            assert!(
                seen.contains(&(kind, pass)),
                "{kind} pass_inside={pass}: {seen:?}"
            );
        }
    }
    assert!(windows, "no event drew ops in flight");
    assert!(
        from_lanes,
        "no event's window was issued from several lanes"
    );
}

/// No database: the `JOIN` key spaces' plans fire on joined tables as well
/// as `FROM` tables, in each key space, from several lanes with ops in
/// flight, and a 1-1 `JOIN`'s joined key's index is dropped; and a joined
/// table draws no column or attribute change.
#[test]
fn drawn_join_plans_reach_joined_tables_from_several_lanes() {
    let mut runner = TestRunner::deterministic();
    let strategy = join_program_with_source_ddl_plan();
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..1500 {
        let (program, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(program.ops.len() <= MAX_JOIN_OPS);
        for event in &plan.events {
            assert!(event.last_op() < program.ops.len(), "{event:?}");
            let readers: Vec<_> = program
                .defs
                .iter()
                .filter(|d| reads(d, &event.table))
                .collect();
            assert!(
                !readers.is_empty(),
                "{event:?} on a table no definition reads"
            );
            let kind = event.action.kind();
            assert!(
                !kind.starts_with("retype")
                    && ![
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
                    ]
                    .contains(&kind),
                "a column or attribute change on a table a JOIN reads: {event:?}"
            );
            let space = match &readers[0].key_space {
                KeySpace::Join(_) => "join",
                KeySpace::JoinOneToOne(spec) if spec.joins.len() > 1 => "star",
                KeySpace::JoinOneToOne(_) => "one_to_one_join",
                _ => "other",
            };
            let joined = readers.iter().all(|d| d.source != event.table);
            seen.insert((space, joined));
            if plan.lanes > 1 && event.window > 1 && event.action.issues_from_lanes() {
                seen.insert(("lanes", true));
            }
            if let SourceDdl::KeyIndex {
                columns,
                change: IndexChange::Drop,
            } = &event.action
                && joined
                && columns.iter().all(|c| c != "id")
            {
                seen.insert(("joined_key_drop", true));
            }
        }
    }
    for want in [
        ("join", false),
        ("join", true),
        ("one_to_one_join", false),
        ("one_to_one_join", true),
        ("star", false),
        ("star", true),
        ("lanes", true),
        ("joined_key_drop", true),
    ] {
        assert!(seen.contains(&want), "never drew {want:?}: {seen:?}");
    }
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
        lanes: 1,
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
        lanes: 1,
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
        lanes: 1,
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
        lanes: 1,
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
        lanes: 1,
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
        lanes: 1,
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
            lanes: 1,
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
        lanes: 1,
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

/// A table a definition reads through `REGEXP_COUNT` (or `STRPOS`) only
/// draws the `"C"` collation: under a case-insensitive one Postgres refuses
/// the call for every changed row, whose key is held (`docs/transforms.md`,
/// "Per row"), and the drain before the close would never converge.
#[test]
fn a_table_read_through_a_collation_refusing_function_draws_only_c() {
    let mut program = one_to_one_and_aggregate_program();
    let text = text_column(&program);
    program.defs[0]
        .fields
        .push(trellis::dev::defs::ast::FieldDef {
            name: "hits".to_string(),
            expr: Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![Expr::Column(text), Expr::StringLiteral("a".to_string())],
            },
        });
    assert!(calls_collation_refusing_function(
        &program,
        &program.tables[0]
    ));
    let mut runner = TestRunner::deterministic();
    let strategy = source_ddl_plan_for(&program);
    let mut collations = std::collections::BTreeSet::new();
    for _ in 0..2000 {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        for event in plan.events {
            if let SourceDdl::Collate { collation, .. } = event.action {
                collations.insert(format!("{collation:?}"));
            }
        }
    }
    assert_eq!(
        collations,
        std::collections::BTreeSet::from(["C".to_string()])
    );
}

/// One source table, `LIST`-partitioned by its grain (one leaf per value),
/// with a `SUM`/`COUNT` aggregate grouped by it, and the same ops as
/// [`one_to_one_and_aggregate_program`].
fn partitioned_aggregate_program() -> Program {
    let mut program = build_program_multi_with_shapes(
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
        &[(
            0,
            DefShape::Aggregate {
                functions: vec![AggregateFn::Sum(AggregateColumn::C1), AggregateFn::Count],
            },
        )],
    );
    assert_eq!(program.ops.len(), 6);
    program = partition_table(
        program,
        0,
        PartitionStrategy::List {
            values: 2,
            default: false,
        },
    );
    program
}

/// The verdicts after `action` on `program`'s first table: fired after op
/// 3 with ops 4 and 5 (a delete and an update) in its window, or after the
/// last op with none for a change later ops couldn't write through.
async fn run_table_action(program: &Program, action: SourceDdl, pass_inside: bool) -> Vec<Verdict> {
    let (op, window) = if action.terminal() { (5, 0) } else { (3, 2) };
    run_one(program, event(program, op, action, window, pass_inside)).await
}

/// Every verdict is a pause for the table gone from its name, exact after
/// its `RESUME`.
fn assert_paused_for_the_missing_table(verdicts: Vec<Verdict>) {
    for verdict in verdicts {
        assert!(
            matches!(
                &verdict,
                Verdict::ResumedExact { reason }
                    if reason.contains("the table public.t0 no longer exists at that name")
            ),
            "{verdict:?}"
        );
    }
}

/// A plain source renamed away and back, or moved to another schema and
/// back, inside one interval: the drain while it is away purges its staged
/// changes and pauses its readers with the gone-table reason, and their
/// `RESUME` once it is back makes them exact (#2270).
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_source_renamed_or_moved_and_back_inside_one_interval_pauses_its_readers() {
    let program = one_to_one_and_aggregate_program();
    for action in [SourceDdl::RenameTable, SourceDdl::MoveSchema] {
        let event = event(&program, 3, action.clone(), 2, false);
        assert!(event.listed_gaps(&program, Server::default()).is_empty());
        assert_paused_for_the_missing_table(run_one(&program, event).await);
    }
}

/// The register's `source-rename-roundtrip`: a partitioned source renamed
/// away and back, or moved to another schema and back, inside one interval
/// loses writes, since its capture stages nothing while its name doesn't
/// resolve to the root. `self_check` reports it and `PAUSE`/`RESUME`
/// repairs it.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_source_renamed_or_moved_and_back_inside_one_interval_is_the_listed_gap() {
    let program = partitioned_aggregate_program();
    for action in [SourceDdl::RenameTable, SourceDdl::MoveSchema] {
        let event = event(&program, 3, action.clone(), 2, false);
        assert_eq!(
            event.listed_gaps(&program, Server::default()),
            vec![Gap::SourceRenameRoundtrip]
        );
        let verdicts = run_one(&program, event).await;
        assert_eq!(
            verdicts,
            vec![Verdict::ListedGap(Gap::SourceRenameRoundtrip); program.defs.len()],
            "{action:?}"
        );
    }
}

/// The same with a pass while the source is away: the pass finds the table
/// gone from its name and pauses its readers, and their `RESUME` after it
/// is back makes them exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_renamed_or_moved_across_a_pass_pauses_its_readers() {
    for program in [
        one_to_one_and_aggregate_program(),
        partitioned_aggregate_program(),
    ] {
        for action in [SourceDdl::RenameTable, SourceDdl::MoveSchema] {
            assert_paused_for_the_missing_table(run_table_action(&program, action, true).await);
        }
    }
}

/// A source dropped and created again with its rows, as a table swap does,
/// pauses its readers pass or no pass: the new table carries no capture
/// trigger, and the window's writes reach it uncaptured. `RESUME` makes
/// them exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_source_dropped_and_created_again_pauses_its_readers() {
    let program = one_to_one_and_aggregate_program();
    for pass_inside in [false, true] {
        for verdict in run_table_action(&program, SourceDdl::RecreateTable, pass_inside).await {
            assert!(
                matches!(
                    &verdict,
                    Verdict::ResumedExact { reason }
                        if reason.contains("Trellis's capture of public.t0 was changed outside Trellis")
                            && reason.contains("is gone")
                ),
                "pass_inside={pass_inside}: {verdict:?}"
            );
        }
    }
}

/// A source renamed away for good pauses its readers, and their `RESUME`
/// refuses while it is missing (T0126).
#[tokio::test(flavor = "multi_thread")]
async fn a_source_renamed_away_for_good_pauses_and_its_resume_refuses() {
    for program in [
        one_to_one_and_aggregate_program(),
        partitioned_aggregate_program(),
    ] {
        for pass_inside in [false, true] {
            let verdicts =
                run_table_action(&program, SourceDdl::RenameTableAway, pass_inside).await;
            assert!(!verdicts.is_empty());
            for verdict in verdicts {
                assert!(
                    matches!(&verdict, Verdict::ResumeRefused { code, .. } if code == "T0126"),
                    "pass_inside={pass_inside}: {verdict:?}"
                );
            }
        }
    }
}

/// Every change to a key's index on `table`, fired after op 2 with ops 3
/// to 5 in its window, inside one interval and across a pass: each verdict
/// with the change and the pass, and each definition's pause reason just
/// after the in-window pass (none without one), before the close restores
/// the index.
type KeyIndexRun = (IndexChange, bool, Vec<Verdict>, Vec<Option<String>>);

async fn key_index_verdicts(program: &Program, table: &str, columns: &[&str]) -> Vec<KeyIndexRun> {
    let mut all = Vec::new();
    for change in [
        IndexChange::Drop,
        IndexChange::Replace,
        IndexChange::RebuildConcurrently,
    ] {
        for pass_inside in [false, true] {
            let event = SourceDdlEvent {
                op: 2,
                table: table.to_string(),
                action: SourceDdl::KeyIndex {
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                    change,
                },
                window: 3,
                pass_inside,
            };
            assert!(event.listed_gaps(program, Server::default()).is_empty());
            let cluster = TestCluster::start();
            let plan = SourceDdlPlan {
                lanes: 1,
                events: vec![event.clone()],
            };
            let run = run(&cluster, program, &plan)
                .await
                .unwrap_or_else(|e| panic!("{e}"));
            let at_pass = run
                .pass_states
                .iter()
                .filter(|(fired, _)| fired == &event)
                .flat_map(|(_, states)| states.iter().map(|s| s.reason.clone()))
                .collect();
            all.push((change, pass_inside, verdicts_after(&run, &event), at_pass));
        }
    }
    all
}

/// A `KEY (…)`'s index (plain, or `NULLS NOT DISTINCT` over a nullable
/// key), with key changes in flight: the pass checks it by shape, so
/// replaced in one transaction, or rebuilt `CONCURRENTLY` with the window
/// running over both, every reader stays exact. Dropped and created again,
/// with the window's changes drained while it is gone: a pass in between
/// pauses every reader, and inside one interval the drained change halts
/// them (T0118, `docs/transforms.md` "Key"). `RESUME` once it is back
/// makes them exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_index_dropped_pauses_its_readers_and_replaced_keeps_them_exact() {
    for nullable in [true, false] {
        let program = keyed_program(nullable);
        let table = program.tables[0].name.clone();
        for (change, pass_inside, verdicts, at_pass) in
            key_index_verdicts(&program, &table, &["k0"]).await
        {
            assert_eq!(verdicts.len(), program.defs.len());
            if change == IndexChange::Drop && pass_inside {
                assert_eq!(at_pass.len(), program.defs.len());
                for reason in &at_pass {
                    assert!(
                        reason
                            .as_deref()
                            .is_some_and(|r| r.contains("no valid unique index backs KEY (k0)")),
                        "nullable={nullable}: {at_pass:?}"
                    );
                }
            }
            for verdict in &verdicts {
                let expected = match (change, pass_inside) {
                    (IndexChange::Drop, false) => {
                        matches!(verdict, Verdict::ResumedExact { reason } if reason.contains("T0118"))
                    }
                    (IndexChange::Drop, true) => {
                        matches!(verdict, Verdict::ResumedExact { reason } if reason.starts_with("capture:"))
                    }
                    _ => *verdict == Verdict::Exact,
                };
                assert!(
                    expected,
                    "nullable={nullable} {change:?} pass_inside={pass_inside}: {verdicts:?}"
                );
            }
        }
    }
}

/// The same on a plain table's primary key: replaced or rebuilt
/// `CONCURRENTLY` (swapped in by `ADD PRIMARY KEY USING INDEX`) on the same
/// column keeps every reader exact. Dropped, with the window's changes
/// drained before it is back, the drained change halts its readers
/// (T0118), pass or no pass: a pass doesn't pause a table with no primary
/// key (`docs/transforms.md` "Key"). `RESUME` once it is back makes them
/// exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_primary_key_dropped_pauses_its_readers_and_replaced_keeps_them_exact() {
    let program = one_to_one_and_aggregate_program();
    let pk = program.tables[0].pk_col.clone();
    let table = program.tables[0].name.clone();
    for (change, pass_inside, verdicts, at_pass) in
        key_index_verdicts(&program, &table, &[&pk]).await
    {
        assert_eq!(verdicts.len(), program.defs.len());
        // The pass pauses nothing: the halt is the drained change's.
        assert!(
            at_pass.iter().all(Option::is_none),
            "{change:?}: {at_pass:?}"
        );
        for verdict in &verdicts {
            let expected = if change == IndexChange::Drop {
                matches!(verdict, Verdict::ResumedExact { reason } if reason.contains("T0118"))
            } else {
                *verdict == Verdict::Exact
            };
            assert!(
                expected,
                "{change:?} pass_inside={pass_inside}: {verdicts:?}"
            );
        }
    }
}

/// A `LEFT` 1-1 `JOIN` of `ja` to `jb` on `ja.k = jb.u`, `jb.u` unique,
/// over ten writes to both tables: `jb`'s key within its `ON` columns is
/// that unique constraint.
fn one_to_one_join_program() -> Program {
    use generative::generate::join::{JoinOpKind, KeyShape};
    use generative::generate::join_one_to_one::{
        OnShape, OneToOneField, OneToOneOpDraw, OneToOneSpec, build_one_to_one_program,
    };
    let spec = OneToOneSpec {
        left: true,
        on: OnShape::KeyUnique,
        fields: vec![OneToOneField::BId, OneToOneField::CoalesceXy],
        more: Vec::new(),
    };
    let draws: Vec<OneToOneOpDraw> = (0..10u8)
        .map(|i| OneToOneOpDraw {
            position: u8::from(i % 2 == 1),
            kind: match i {
                0..=3 | 6 => JoinOpKind::Insert,
                4 | 7 | 9 => JoinOpKind::Update,
                _ => JoinOpKind::Delete,
            },
            pick: usize::from(i),
            key: i % 3,
            id: i,
            t: i,
            half_null: false,
            value: Some(i as i8),
            columns: if i == 7 { 1 } else { 8 },
            own_keys: [0; 3],
            on_key: false,
            key_scope: None,
        })
        .collect();
    build_one_to_one_program(&[spec], KeyShape::Spread, &draws, &[])
}

/// A 1-1 `JOIN`'s `b` key (#2184 slice 7), with writes to both tables in
/// flight: dropped and created again, it pauses the `JOIN` only where a
/// pass sees it gone (`docs/transforms.md` "Key"), and `RESUME` once it is
/// back makes it exact; replaced or rebuilt `CONCURRENTLY` it stays exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_joined_tables_key_dropped_pauses_the_join_only_where_a_pass_sees_it() {
    let program = one_to_one_join_program();
    for (change, pass_inside, verdicts, _) in key_index_verdicts(&program, "jb", &["u"]).await {
        assert_eq!(verdicts.len(), 1);
        let expected = if change == IndexChange::Drop && pass_inside {
            matches!(
                &verdicts[0],
                Verdict::ResumedExact { reason } if reason.contains("the unique key on the ON columns of public.jb")
            )
        } else {
            verdicts[0] == Verdict::Exact
        };
        assert!(
            expected,
            "{change:?} pass_inside={pass_inside}: {verdicts:?}\n{:#?}",
            program.ops
        );
    }
}

/// The `JOIN` key spaces the slice-8 pins run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSpace {
    Join,
    OneToOneJoin,
    Star,
}

const JOIN_SPACES: [JoinSpace; 3] = [JoinSpace::Join, JoinSpace::OneToOneJoin, JoinSpace::Star];

/// The first deterministic draw of `space`'s tier ([`for_source_ddl`])
/// whose ops 2 to 5 write both `ja` and `jb`, `jb` at least twice, none
/// of them failing, and whose every definition reads `jb`: an event on
/// `jb` after op 1 with a window of 4 has changes in flight on both sides,
/// for every definition. Not after op 0: the run installs every definition
/// up front without waiting for its build, and only the check after op 0
/// settles them, so an event there can fire before a build has read `jb`
/// and find nothing to disrupt.
fn drawn_join_program(space: JoinSpace, want: impl Fn(&Program) -> bool) -> Program {
    use generative::generate::join::join_case_strategy;
    use generative::generate::join_one_to_one::{one_to_one_case_strategy, star_case_strategy};
    let strategy = match space {
        JoinSpace::Join => join_case_strategy().prop_map(|c| c.program).boxed(),
        JoinSpace::OneToOneJoin => one_to_one_case_strategy().prop_map(|c| c.program).boxed(),
        JoinSpace::Star => star_case_strategy()
            .prop_filter("a star joins three tables or more", |s| s.tables > 2)
            .prop_map(|s| s.case.program)
            .boxed(),
    };
    let mut runner = TestRunner::deterministic();
    for _ in 0..2000 {
        let program = for_source_ddl(
            strategy
                .new_tree(&mut runner)
                .expect("strategy must produce a value")
                .current(),
        );
        if program.ops.len() < 7 {
            continue;
        }
        let window = &program.ops[2..=5];
        let on = |table: &str| window.iter().filter(|o| o.table() == table).count();
        if on("ja") >= 1
            && on("jb") >= 2
            && window
                .iter()
                .all(|o| *o.expect() != generative::model::OpOutcome::Fails)
            && program.defs.iter().all(|d| reads(d, "jb"))
            && want(&program)
        {
            return program;
        }
    }
    panic!("no {space:?} draw fits the pins");
}

/// Runs `action` on `jb` of `program` after op 1, ops 2 to 5 in its
/// window issued from three lanes, and returns each definition's verdict
/// at the check after it.
async fn run_on_jb(
    cluster: &TestCluster,
    program: &Program,
    action: SourceDdl,
    pass_inside: bool,
) -> Vec<(String, Verdict)> {
    let event = SourceDdlEvent {
        op: 1,
        table: "jb".to_string(),
        action,
        window: 4,
        pass_inside,
    };
    let plan = SourceDdlPlan {
        lanes: 3,
        events: vec![event.clone()],
    };
    let run = run(cluster, program, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    run.checks
        .iter()
        .find(|(fired, _)| fired.as_ref() == Some(&event))
        .unwrap_or_else(|| panic!("no check after {event:?}"))
        .1
        .iter()
        .map(|judged| (judged.target.clone(), judged.verdict.clone()))
        .collect()
}

/// #2184 decision 4 over the `JOIN` key spaces (slice 8): `jb`'s capture
/// triggers disabled and enabled again by name, with changes in flight on
/// both sides from three lanes, pause every `JOIN`, 1-1 `JOIN` and star
/// reading it, pass or no pass; their `RESUME` makes them exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_joined_tables_capture_disabled_from_several_lanes_pauses_its_joins() {
    let cluster = TestCluster::start();
    for space in JOIN_SPACES {
        let program = drawn_join_program(space, |_| true);
        for pass_inside in [false, true] {
            let verdicts = run_on_jb(
                &cluster,
                &program,
                SourceDdl::DisableCaptureTriggers,
                pass_inside,
            )
            .await;
            assert!(!verdicts.is_empty());
            for (target, verdict) in verdicts {
                assert!(
                    matches!(
                        &verdict,
                        Verdict::ResumedExact { reason }
                            if reason.contains("capture of public.jb")
                                && reason.contains("was changed outside Trellis")
                    ),
                    "{space:?} pass_inside={pass_inside} {target}: {verdict:?}"
                );
            }
        }
    }
}

/// Table maintenance and the key's index rebuilt on a joined table, with
/// changes in flight on both sides from three lanes, keep every `JOIN`, 1-1
/// `JOIN` and star exact (`docs/staging-and-claiming/capture-drift.md`:
/// none of them changes a value or a key).
#[tokio::test(flavor = "multi_thread")]
async fn joined_table_maintenance_from_several_lanes_keeps_every_join_exact() {
    let cluster = TestCluster::start();
    let id = || vec!["id".to_string()];
    for space in JOIN_SPACES {
        let program = drawn_join_program(space, |_| true);
        for (action, pass_inside) in [
            (SourceDdl::VacuumFull, false),
            (SourceDdl::Cluster, true),
            (SourceDdl::Reindex { concurrently: true }, false),
            (
                SourceDdl::KeyIndex {
                    columns: id(),
                    change: IndexChange::Replace,
                },
                true,
            ),
            (
                SourceDdl::KeyIndex {
                    columns: id(),
                    change: IndexChange::RebuildConcurrently,
                },
                false,
            ),
        ] {
            for (target, verdict) in
                run_on_jb(&cluster, &program, action.clone(), pass_inside).await
            {
                assert_eq!(
                    verdict,
                    Verdict::Exact,
                    "{space:?} {action:?} pass_inside={pass_inside} {target}"
                );
            }
        }
    }
}

/// A joined table renamed away and back, with changes in flight from three
/// lanes: inside one interval, a drain while it is away purges its staged
/// changes and pauses every `JOIN` reading it (#2270); across a pass, the
/// pass pauses them. Either way they resume exact, and a reader no purge
/// reached stays exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_joined_table_renamed_and_back_pauses_its_joins_or_leaves_them_exact() {
    let cluster = TestCluster::start();
    let mut purged = false;
    for space in JOIN_SPACES {
        let program = drawn_join_program(space, |_| true);
        for pass_inside in [false, true] {
            for (target, verdict) in
                run_on_jb(&cluster, &program, SourceDdl::RenameTable, pass_inside).await
            {
                let paused = matches!(&verdict, Verdict::ResumedExact { reason }
                    if reason.contains("no longer exists at that name"));
                purged |= paused && !pass_inside;
                let expected = paused || (!pass_inside && verdict == Verdict::Exact);
                assert!(
                    expected,
                    "{space:?} pass_inside={pass_inside} {target}: {verdict:?}"
                );
            }
        }
    }
    assert!(purged, "no drain purged jb's changes while it was away");
}

/// The register's `alter-type-rewrite` on a `JOIN`'s joined table: a
/// same-type rewrite of `jb.y`, its last column and an aggregate's
/// argument, beside a `SET DEFAULT` inside one interval, with changes in
/// flight from three lanes, leaves the `JOIN` wrong, which `self_check`
/// reports and `request_backfill` repairs ("an aggregate or `JOIN` target
/// stays wrong until it's rebuilt"); across a pass it is re-read.
#[tokio::test(flavor = "multi_thread")]
async fn a_joined_tables_rewrite_beside_a_default_is_the_listed_gap_for_a_join() {
    let cluster = TestCluster::start();
    let program = drawn_join_program(JoinSpace::Join, |program| {
        program
            .tables
            .iter()
            .find(|t| t.name == "jb")
            .and_then(|t| t.column_type("y"))
            == Some(trellis::dev::defs::ValueType::Numeric)
    });
    for pass_inside in [false, true] {
        let action = SourceDdl::RewriteBesideDefault {
            column: "y".to_string(),
        };
        let verdicts = run_on_jb(&cluster, &program, action, pass_inside).await;
        let gap = Verdict::ListedGap(Gap::AlterTypeRewrite);
        for (target, verdict) in &verdicts {
            let expected = *verdict == Verdict::Exact || (!pass_inside && *verdict == gap);
            assert!(expected, "pass_inside={pass_inside} {target}: {verdict:?}");
        }
        assert!(
            pass_inside || verdicts.iter().any(|(_, v)| *v == gap),
            "no JOIN reading jb.y was left wrong: {verdicts:?}"
        );
    }
}
