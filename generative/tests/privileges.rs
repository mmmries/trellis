//! Privilege and ownership changes in the op stream (#2182, epic #2180 step
//! 3b), run as source-DDL events and judged by "never silently wrong"
//! (`generative::run::judge`). A capture-side refusal must end paused with
//! a reason (or a halt), and never fail an application write (ADR-0002
//! I6, `RunError::CaptureFailedWriter`). The drain's own refusal on a
//! target may also hold its page up while it lasts, reported as a
//! `drain_failure`, and the page commits once it is undone.
//!
//! Every case runs as `CaseDatabase` provisions the deployment roles, with
//! the program's application wiring (#2181), whose `AFTER` rewrite makes
//! capture re-read the table as the Trellis role.

use generative::backend::{CaseDatabase, ConcurrentBackend, ManualBackend};
use generative::generate::source_ddl::privilege_plan_for;
use generative::generate::wiring::with_wiring;
use generative::generate::{
    AggregateColumn, AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes,
    trivial_program,
};
use generative::model::{
    ConcurrentPlan, Program, Rewrite, SourceDdl, SourceDdlEvent, SourceDdlPlan, Timing, Wiring,
};
use generative::run::judge::Verdict;
use generative::run::privilege::RefusalSeen;
use generative::run::{RunError, SourceDdlRun, run_convergence_with_source_ddl};
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

/// Runs `program` with `wiring` and `plan` against a fresh database on
/// `cluster`, as the deployment roles.
async fn run(
    cluster: &TestCluster,
    program: &Program,
    wiring: &[Wiring],
    plan: &SourceDdlPlan,
) -> Result<SourceDdlRun, String> {
    let db = CaseDatabase::provision(cluster).await;
    let mut backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    backend.set_wiring(wiring);
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    match run_convergence_with_source_ddl(&mut backend, &pool, program, plan).await {
        Ok(run) => Ok(run),
        Err(RunError::NeverSilentlyWrong {
            op_index,
            target,
            problem,
            detail,
        }) => Err(format!(
            "after op {op_index}, {target}: {problem}\nplan: {plan:?}\nwiring: {wiring:?}\n{detail}"
        )),
        Err(other) => Err(format!(
            "run error with plan {plan:?} and wiring {wiring:?}: {other:?}"
        )),
    }
}

fn program_with_privilege_plan() -> impl Strategy<Value = (Program, Vec<Wiring>, SourceDdlPlan)> {
    trivial_program().prop_flat_map(|program| {
        let wiring = with_wiring(ConcurrentPlan::default(), &program).wiring;
        let plan = privilege_plan_for(&program);
        (Just(program), Just(wiring), plan)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    /// A privilege or ownership change anywhere in a generated program never
    /// fails an application write and never leaves a definition silently
    /// wrong outside a listed gap.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_privilege_changes_are_never_silently_wrong(
        (program, wiring, plan) in program_with_privilege_plan()
    ) {
        HARNESS.with(|h| {
            h.runtime
                .block_on(run(&h.cluster, &program, &wiring, &plan))
                .map(|_| ())
                .map_err(TestCaseError::fail)
        })?;
    }
}

/// No database: every drawn plan stays inside its program, a drain refusal
/// names a target reading the event's table, and across enough draws every
/// change shows up with and without a pass inside.
#[test]
fn drawn_privilege_plans_stay_in_bounds_and_cover_every_change() {
    let mut runner = TestRunner::deterministic();
    let strategy = program_with_privilege_plan();
    let mut seen = std::collections::BTreeSet::new();
    let mut wired = false;
    for _ in 0..500 {
        let (program, wiring, plan) = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        wired |= wiring.iter().any(|w| {
            w.rewrite
                .as_ref()
                .is_some_and(|r| r.timing == Timing::After)
        });
        let mut end = None;
        for event in &plan.events {
            assert!(event.last_op() < program.ops.len(), "{event:?}");
            assert!(event.action.privilege(), "{event:?}");
            assert!(end.is_none_or(|end| event.op > end), "{plan:?}");
            end = Some(event.last_op());
            // A foreign key's check reads its table as the owner.
            let referenced: Vec<&str> = wiring
                .iter()
                .filter(|w| w.shadow.as_ref().is_some_and(|s| s.fk.is_some()))
                .map(|w| w.table.as_str())
                .collect();
            match event.action {
                SourceDdl::RevokeSelect => {
                    assert!(!referenced.contains(&event.table.as_str()), "{event:?}")
                }
                SourceDdl::RevokeSchemaUsage => assert!(referenced.is_empty(), "{event:?}"),
                _ => {}
            }
            if let Some(target) = event.action.refused_target() {
                assert!(
                    program
                        .defs
                        .iter()
                        .any(|d| d.target == target && d.source == event.table),
                    "{event:?}"
                );
            }
            seen.insert((event.action.kind(), event.pass_inside));
        }
    }
    for kind in [
        "owner_to_heir",
        "owner_to_other",
        "revoke_select",
        "revoke_membership",
        "revoke_schema_usage",
        "rls",
        "rls_trellis_policy",
        "writer_change",
        "revoke_target_writes",
        "target_rls",
        "target_trigger_refused",
    ] {
        for pass in [false, true] {
            assert!(
                seen.contains(&(kind, pass)),
                "{kind} pass_inside={pass}: {seen:?}"
            );
        }
    }
    assert!(wired, "no program drew an AFTER rewrite, so no re-read");
}

/// One source table, a 1-1 and an aggregate over it, and an `AFTER`
/// rewrite of `c1` on even keys, which makes capture re-read the table:
///
/// - ops 0-3 seed pks 1-4
/// - op 4 updates pk 2, op 5 updates pk 4 (both re-read)
/// - op 6 deletes pk 1, op 7 updates pk 3
fn rewired_program() -> (Program, Vec<Wiring>) {
    let program = build_program_multi_with_shapes(
        &[TableSpec {
            seed_values: vec![
                (Some(10), Some(1)),
                (Some(20), Some(2)),
                (Some(30), Some(3)),
                (Some(40), Some(4)),
            ],
            text_values: vec![None; 4],
            bool_values: vec![None; 4],
            uuid_values: vec![None; 4],
            grain_values: vec![
                Some("0".to_string()),
                Some("0".to_string()),
                Some("1".to_string()),
                Some("1".to_string()),
            ],
            mutates: vec![
                Mutate::Update {
                    pk: 2,
                    c1: Some(100),
                    c2: Some(5),
                },
                Mutate::Update {
                    pk: 4,
                    c1: Some(7),
                    c2: None,
                },
                Mutate::Delete { pk: 1 },
                Mutate::Update {
                    pk: 3,
                    c1: Some(8),
                    c2: Some(6),
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
    assert_eq!(program.ops.len(), 8);
    let wiring = vec![Wiring {
        table: program.tables[0].name.clone(),
        shadow: None,
        rewrite: Some(Rewrite {
            column: program.tables[0].columns[1].name.clone(),
            timing: Timing::After,
            values: (Some("7.00".to_string()), Some("8.00".to_string())),
        }),
    }];
    (program, wiring)
}

/// The change after op 3, with the two re-reading updates in flight.
fn event(program: &Program, action: SourceDdl, pass_inside: bool) -> SourceDdlEvent {
    SourceDdlEvent {
        op: 3,
        table: program.tables[0].name.clone(),
        action,
        window: 2,
        pass_inside,
    }
}

/// Runs `action` on [`rewired_program`] on a cluster of its own, and gives
/// the run and the verdicts at the check right after it.
async fn run_one(action: SourceDdl, pass_inside: bool) -> (SourceDdlRun, Vec<Verdict>) {
    let (program, wiring) = rewired_program();
    let event = event(&program, action, pass_inside);
    let cluster = TestCluster::start();
    let plan = SourceDdlPlan {
        events: vec![event.clone()],
    };
    let run = run(&cluster, &program, &wiring, &plan)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    let verdicts = run
        .checks
        .iter()
        .find(|(fired, _)| fired.as_ref() == Some(&event))
        .unwrap_or_else(|| panic!("no check after {event:?}"))
        .1
        .iter()
        .map(|judged| judged.verdict.clone())
        .collect();
    (run, verdicts)
}

/// Every reader paused with a reason starting `prefix` and was exact after
/// its `RESUME`; a failure names `case`.
fn assert_all_paused(case: impl std::fmt::Debug, verdicts: &[Verdict], prefix: &str) {
    assert_eq!(verdicts.len(), 2, "{case:?}");
    for verdict in verdicts {
        assert!(
            matches!(verdict, Verdict::ResumedExact { reason } if reason.starts_with(prefix)),
            "{case:?}: {verdicts:?}"
        );
    }
}

/// The halt a refused read pauses with: the drain's, after capture's
/// re-read was refused and staged its marker (#2154), or the staging
/// pass's.
const HALTED: &str = "halt: the ";

/// #2154's repro, the first pin: the source's owner moves to a role
/// Trellis doesn't inherit while re-reading updates are in flight. The
/// application's updates succeed (the run fails on one that doesn't), and
/// both readers pause, exact after the owner is restored and they resume.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_trellis_does_not_inherit_pauses_the_readers_and_fails_no_write() {
    for pass_inside in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::OwnerToOther, pass_inside).await;
        assert_all_paused(("pass_inside", pass_inside), &verdicts, HALTED);
    }
}

/// An owner Trellis inherits changes nothing it can do: both stay exact.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_trellis_inherits_keeps_every_definition_exact() {
    let (_, verdicts) = run_one(SourceDdl::OwnerToHeir, true).await;
    assert_eq!(verdicts, vec![Verdict::Exact, Verdict::Exact]);
}

/// Trellis's `SELECT` revoked, with re-reads in flight: paused, no write
/// fails.
#[tokio::test(flavor = "multi_thread")]
async fn trellis_select_revoked_pauses_the_readers_and_fails_no_write() {
    for pass_inside in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::RevokeSelect, pass_inside).await;
        assert_all_paused(("pass_inside", pass_inside), &verdicts, HALTED);
    }
}

/// The owner role revoked from Trellis, the way an operator cuts it off
/// under the recommendations: paused, no write fails.
#[tokio::test(flavor = "multi_thread")]
async fn the_owner_revoked_from_trellis_pauses_the_readers_and_fails_no_write() {
    for pass_inside in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::RevokeMembership, pass_inside).await;
        assert_all_paused(("pass_inside", pass_inside), &verdicts, HALTED);
    }
}

/// `USAGE` on the sources' schema revoked: paused, no write fails.
#[tokio::test(flavor = "multi_thread")]
async fn schema_usage_revoked_pauses_the_readers_and_fails_no_write() {
    for pass_inside in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::RevokeSchemaUsage, pass_inside).await;
        assert_all_paused(("pass_inside", pass_inside), &verdicts, HALTED);
    }
}

/// Row-level security forced on the source, with and without a policy for
/// the Trellis role, across a pass: the pass sees it and pauses the
/// readers.
#[tokio::test(flavor = "multi_thread")]
async fn row_level_security_on_a_source_across_a_pass_pauses_the_readers() {
    for trellis_policy in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::Rls { trellis_policy }, true).await;
        assert_all_paused(
            ("trellis_policy", trellis_policy),
            &verdicts,
            "capture: row-level security on public.",
        );
    }
}

/// Row-level security forced on the source inside one reconcile interval,
/// with and without a policy for the Trellis role (#2227): no pass sees the
/// policies, but capture's re-reads run with `row_security = off`, so they
/// are refused rather than filtered, even by a policy that lets the Trellis
/// role see every row. The writes succeed and both readers halt, exact
/// after `RESUME`.
#[tokio::test(flavor = "multi_thread")]
async fn row_level_security_on_a_source_inside_one_interval_halts_the_readers() {
    for trellis_policy in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::Rls { trellis_policy }, false).await;
        assert_all_paused(("trellis_policy", trellis_policy), &verdicts, HALTED);
    }
}

/// The application's writes move to another role and back: nothing
/// Trellis does changes.
#[tokio::test(flavor = "multi_thread")]
async fn the_application_role_changed_mid_stream_keeps_every_definition_exact() {
    for pass_inside in [false, true] {
        let (_, verdicts) = run_one(SourceDdl::WriterChange, pass_inside).await;
        assert_eq!(verdicts, vec![Verdict::Exact, Verdict::Exact]);
    }
}

/// The drain refused its writes to the 1-1's target: it halts the writer
/// of the target, and the aggregate keeps flowing.
#[tokio::test(flavor = "multi_thread")]
async fn target_writes_revoked_halt_their_writer_only() {
    let (program, _) = rewired_program();
    let target = program.defs[0].target.clone();
    let (run, verdicts) = run_one(SourceDdl::RevokeTargetWrites { target }, false).await;
    assert!(
        matches!(&run.refusals[..], [(_, RefusalSeen::Halted { .. })]),
        "{:?}",
        run.refusals
    );
    assert!(
        matches!(verdicts[0], Verdict::ResumedExact { .. }),
        "{verdicts:?}"
    );
    assert_eq!(verdicts[1], Verdict::Exact);
}

/// Row-level security forced on the 1-1's target: Trellis's write is refused
/// and the catalog pins it on the target, so its writer halts, as for a
/// revoked write.
#[tokio::test(flavor = "multi_thread")]
async fn row_level_security_on_a_target_halts_its_writer_only() {
    let (program, _) = rewired_program();
    let target = program.defs[0].target.clone();
    let (run, verdicts) = run_one(SourceDdl::TargetRls { target }, false).await;
    assert!(
        matches!(&run.refusals[..], [(_, RefusalSeen::Halted { .. })]),
        "{:?}",
        run.refusals
    );
    assert!(
        matches!(verdicts[0], Verdict::ResumedExact { .. }),
        "{verdicts:?}"
    );
    assert_eq!(verdicts[1], Verdict::Exact);
}

/// The application's trigger on the 1-1's target writes a table Trellis
/// can't write: the catalog can't pin that on a table, so the drain holds
/// the page up, reports it as `drain_failure`, and commits it once the
/// trigger is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_target_trigger_refused_holds_the_page_up_until_dropped() {
    let (program, _) = rewired_program();
    let target = program.defs[0].target.clone();
    let (run, verdicts) = run_one(SourceDdl::TargetTriggerRefused { target }, false).await;
    assert!(
        matches!(&run.refusals[..], [(_, RefusalSeen::HeldUp { .. })]),
        "{:?}",
        run.refusals
    );
    assert_eq!(verdicts, vec![Verdict::Exact, Verdict::Exact]);
}

/// The I6 check's control: an application statement failing inside a
/// function named as a capture function is told apart by its error as the
/// backend reports it, and one failing in the application's own trigger
/// isn't.
#[tokio::test(flavor = "multi_thread")]
async fn a_writer_failing_inside_a_capture_function_is_recognised() {
    let cluster = TestCluster::start();
    let db = CaseDatabase::provision(&cluster).await;
    let backend = ManualBackend::connect_as(db.roles())
        .await
        .expect("connect manual backend");
    let raises = |name: &str| {
        format!(
            "create function public.{name}() returns trigger language plpgsql as \
             $$ begin raise exception 'refused'; end $$"
        )
    };
    for sql in [
        "create table public.w (k int primary key)".to_string(),
        raises("cap_insert_w_0123456789abcdef"),
        raises("app_w_check"),
        "create trigger a before insert on public.w for each row \
         execute function public.cap_insert_w_0123456789abcdef()"
            .to_string(),
    ] {
        backend.execute_raw(&sql).await.expect(&sql);
    }
    let error = backend
        .execute_raw("insert into public.w values (1)")
        .await
        .expect_err("the trigger raises");
    assert!(
        generative::run::raised_in_capture(&format!("{error:?}")),
        "{error:?}"
    );
    for sql in [
        "drop trigger a on public.w",
        "create trigger a before insert on public.w for each row \
         execute function public.app_w_check()",
    ] {
        backend.execute_raw(sql).await.expect(sql);
    }
    let error = backend
        .execute_raw("insert into public.w values (1)")
        .await
        .expect_err("the trigger raises");
    assert!(
        !generative::run::raised_in_capture(&format!("{error:?}")),
        "{error:?}"
    );
}
