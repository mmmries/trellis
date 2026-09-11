//! The convergence property, first end-to-end green (issue #6, design doc
//! §4). Generates trivial 1-1 numeric-`+` programs, drives each through the
//! real [`ManualBackend`] one op at a time, and after every op asserts the
//! materialized target equals the [`generative::oracle`] recompute.
//!
//! # Operational shape (design doc §9)
//!
//! `TestCluster::start()` runs its own `initdb` and is by far the most
//! expensive step, so the property shares **one** cluster across all its
//! proptest cases via a `thread_local` harness: proptest runs a test's cases
//! on the calling thread, so the harness is built once (first case) and its
//! `Drop` — the clean `pg_ctl stop` — runs when the test thread exits. Each
//! case still provisions a fresh *isolated database* (its own schema, slot,
//! and publication) inside that shared cluster, which is the accepted
//! per-case fallback the issue calls out: the `ManualBackend` seam (issue #3)
//! starts a client with a fixed slot per `install`, so reusing one slot while
//! resetting only the schema between cases would mean reworking that seam.
//! Reusing the slot/publication across cases is left as a follow-up.
//!
//! Case count defaults to 16 (design doc §9's 12–24 band) and is overridable
//! for deep sweeps with `PROPTEST_CASES` (e.g. `PROPTEST_CASES=500 cargo test
//! -p generative --test convergence`). Failing seeds are persisted to the
//! checked-in `tests/proptest-regressions/convergence.txt` and replayed first.

use engine::defs::qualified_target_table;
use engine::{Config, Pool};
use generative::backend::{Backend, ManualBackend};
use generative::generate::{Mutate, build_program, trivial_program};
use generative::run::{RunError, check_program, run_convergence};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;

/// One tokio runtime and one shared, initdb-once cluster for a test thread's
/// proptest cases. See the module doc comment for why this is thread-local.
///
/// `coverage` (A2, `local_docs/generative-suite-improvement-plan.md`)
/// accumulates every program `run_one` drives, pass or fail, so the property
/// can report what it actually exercised instead of a single green/red bit —
/// see [`Harness`]'s `Drop` impl below.
struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    coverage: std::cell::RefCell<generative::run::Coverage>,
}

impl Drop for Harness {
    /// Prints the accumulated [`generative::run::Coverage`] when the test
    /// thread exits. Print-only, deliberately: this runs during the shared
    /// thread-local's teardown, which can itself be *during* an unwind if the
    /// macro-generated proptest test is re-panicking with a shrunk failure —
    /// asserting or panicking here risks a double panic, which aborts the
    /// whole test process instead of cleanly failing one test. This is
    /// human/CI-visible reporting only, never a pass/fail gate (the floor
    /// assertions for that live in `tests/coverage.rs`, which never touches a
    /// database).
    fn drop(&mut self) {
        eprintln!(
            "generative: convergence run coverage:\n{}",
            self.coverage.borrow()
        );
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: {
            // Improvement-plan workstream C, task C2: opt-in per-call timing
            // around cluster startup (`initdb` + `pg_ctl start`), the most
            // expensive one-time cost the shared thread-local harness pays.
            // Silent and free unless `GENERATIVE_COST_TIMING` is set, same
            // convention as C1's `GENERATIVE_QUIESCE_TIMING` in
            // `generative/src/backend/manual.rs` (see
            // `local_docs/generative-suite-improvement-plan.md` "C2").
            let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
            let start = timing_enabled.then(std::time::Instant::now);
            let cluster = TestCluster::start();
            if let Some(start) = start {
                eprintln!("COST_TIMING cluster_startup {}", start.elapsed().as_millis());
            }
            cluster
        },
        coverage: std::cell::RefCell::new(generative::run::Coverage::new()),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// Runs one generated program against a fresh isolated database in the shared
/// cluster, returning a proptest-friendly result: a divergence (or any run
/// error) becomes a `TestCaseError::fail` carrying the full report, so proptest
/// shrinks toward the smallest program and prints it.
fn run_one(program: &generative::model::Program) -> Result<(), TestCaseError> {
    HARNESS.with(|h| {
        // Recorded unconditionally, before the run: even a case that goes on
        // to fail (and shrinks) is real evidence of what the generator drew.
        h.coverage.borrow_mut().record_program(program);

        h.runtime.block_on(async {
            // C2: per-case isolated-database provisioning (schema, slot,
            // publication) inside the shared cluster. See the cluster-startup
            // timing above for the env-var convention.
            let timing_enabled = std::env::var_os("GENERATIVE_COST_TIMING").is_some();
            let start = timing_enabled.then(std::time::Instant::now);
            let db = h.cluster.create_isolated_database().await;
            if let Some(start) = start {
                eprintln!("COST_TIMING db_provision {}", start.elapsed().as_millis());
            }
            let mut backend = ManualBackend::connect(db.dsn())
                .await
                .expect("connect manual backend");
            let pool =
                Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

            match run_convergence(&mut backend, &pool, program).await {
                Ok(outcome) => {
                    if outcome.as_pass() {
                        Ok(())
                    } else {
                        // Unreachable today: run_convergence's only `Ok` is
                        // `Outcome::Ran` (design doc §6). Asserted anyway so
                        // this stays the single decision point rather than
                        // a second, parallel pass/fail path.
                        Err(TestCaseError::fail(format!("run did not pass: {outcome}")))
                    }
                }
                Err(RunError::Diverged(d)) => Err(TestCaseError::fail(format!(
                    "convergence diverged after op {} (target {}):\n{}",
                    d.op_index, d.def_target, d.report
                ))),
                Err(other) => Err(TestCaseError::fail(format!("run error: {other:?}"))),
            }
        })
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn convergence_holds_for_trivial_programs(program in trivial_program()) {
        run_one(&program)?;
    }
}

/// The named "first end-to-end green": a fully-worked, hand-built convergent
/// program driven through the real convergence property (not the generator),
/// so a reader has one concrete, legible case that must pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_hand_built_program_converges_end_to_end() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    // Seed three rows, then update one and delete one — every mutate hits a
    // real row.
    let program = build_program(
        &[
            (Some(10), Some(1)),
            (Some(20), Some(2)),
            (Some(30), Some(3)),
        ],
        &[
            Mutate::Update {
                pk: 1,
                c1: Some(100),
                c2: Some(5),
            },
            Mutate::Delete { pk: 2 },
        ],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("hand-built program must converge end-to-end");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// Control (design doc §7): a program whose mutate stream includes ops that
/// name a never-seeded primary key. Postgres updates/deletes zero rows — a
/// source no-op, not an error — and the property must still converge, proving
/// "operation errors are checked, not swallowed" (design doc §4) does not
/// spuriously fail a run.
#[tokio::test(flavor = "multi_thread")]
async fn no_op_mutations_on_missing_rows_still_converge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(7), Some(8))],
        &[
            // pk 42 was never seeded: an update and a delete that hit nothing.
            Mutate::Update {
                pk: 42,
                c1: Some(1),
                c2: Some(1),
            },
            Mutate::Delete { pk: 42 },
            // A real update on the one seeded row, to prove the run still makes
            // progress after the no-ops.
            Mutate::Update {
                pk: 1,
                c1: Some(9),
                c2: Some(9),
            },
        ],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("no-op mutations must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// The red half of the control (design doc §6/§7): prove the harness can tell
/// green from red. Settle a program, then corrupt the persisted target
/// directly — not via a source change, so the pipeline won't repair it — and
/// confirm the exact comparison the property runs per op ([`check_program`])
/// reports the divergence, localized to `target != SQL`. Without this, a
/// convergence run that always reported "converged" would look identical to a
/// working one.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_detects_a_corrupted_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(&[(Some(10), Some(1)), (Some(20), Some(2))], &[]);
    let def = program.defs[0].clone();
    let pk_col = program.tables[0].pk_col.clone();

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // First converge cleanly through the real property.
    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("program must converge before we corrupt it");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");

    // Corrupt one target cell directly; the source is untouched, so both the
    // SQL oracle and the evaluator still compute the right value.
    let qualified = qualified_target_table("public", &def);
    let client = pool.get().await.expect("pool connection");
    client
        .execute(
            &format!("update {qualified} set \"total\" = 999 where \"{pk_col}\" = 1"),
            &[],
        )
        .await
        .expect("corrupt target");

    let snapshot = backend.snapshot().await.expect("snapshot after corruption");
    let found = check_program(&pool, &program, &snapshot)
        .await
        .expect("oracle check must run");

    let (target, report) = found.expect("the harness must detect the corrupted target");
    assert_eq!(target, def.target);
    assert_eq!(
        report.target_vs_sql.len(),
        1,
        "corruption localizes to target != SQL: {report}"
    );
    assert!(
        report.evaluator_vs_sql.is_empty(),
        "only the persisted target is wrong; the evaluator agrees with the SQL oracle: {report}"
    );
}

/// Closes the issue #6 gap: a program whose mutate stream includes a
/// [`Mutate::DuplicateInsert`] — a second `INSERT` at an already-seeded pk,
/// rejected by the source table's real primary-key constraint. Unlike the
/// missing-pk update/delete case (`no_op_mutations_on_missing_rows_still_converge`),
/// this is a genuine `apply()` `Err`, not a silent zero-row success. The
/// insert is atomic, so the source is unaffected, and the property must still
/// converge — "an op that errors changed nothing" (design doc §4) exercised
/// against a real rejection, not just a no-op.
#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_pk_insert_error_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(Some(1), Some(2)), (Some(3), Some(4))],
        &[
            // pk 1 is already seeded: this insert must be rejected by the
            // primary-key constraint, a real `apply()` error.
            Mutate::DuplicateInsert {
                pk: 1,
                c1: Some(999),
                c2: Some(999),
            },
            // A real update afterward, to prove the run still makes progress.
            Mutate::Update {
                pk: 1,
                c1: Some(10),
                c2: Some(20),
            },
        ],
    );

    // No separate pin re-applying the duplicate-pk insert against a
    // throwaway connection is needed here: `build_program` tags this op
    // `OpOutcome::Fails`, and `run_convergence` itself now classifies every
    // op's actual outcome and asserts it against that expectation (design
    // doc §4 "operation errors are checked, not swallowed"). If
    // `DuplicateInsert` ever stopped actually erroring (or started
    // affecting rows), `run_convergence` below would return
    // `Err(RunError::UnexpectedOpOutcome { .. })` and the `.expect` below
    // would fail with that mismatch spelled out — no separate pin required.
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("a rejected duplicate-pk insert must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}

/// A seeded row with `NULL` in a nullable column (design doc §3's awkward
/// values) converges: `NULL + n = NULL` on both sides (Postgres's `numeric`
/// arithmetic and `engine::defs::eval`'s `Operator::Add` arm agree), so the
/// SQL oracle and the maintained target must agree too.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_value_in_a_nullable_column_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let program = build_program(
        &[(None, Some(4)), (Some(5), None)],
        &[Mutate::Update {
            pk: 1,
            c1: None,
            c2: None,
        }],
    );

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    let outcome = run_convergence(&mut backend, &pool, &program)
        .await
        .expect("NULL values in nullable columns must not break convergence");
    assert!(outcome.as_pass(), "run did not pass: {outcome}");
}
