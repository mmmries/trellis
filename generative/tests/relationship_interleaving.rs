//! Issue #138 (epic #127 phase 2): stresses the settled-parent-projection
//! mechanism (#132's four correctness rules) for to-one relationships under
//! the specific timing #34's original relationship suite never exercised —
//! a parent (to-side) change landing close enough to a from-side change on
//! the *same* parent that the two can race across a seal boundary, an
//! intake-lag window, or an out-of-order segment drain.
//!
//! [`generative::generate::build_relationship_interleaving_scenario`] builds
//! the five scenario shapes (the canonical parent-field-update case, plus
//! #138's own named variants: parent insert, parent delete, FK re-point to a
//! nonexistent parent, and a NULL parent) via #34's existing
//! `build_program_multi_with_relationships`/`attach_relationship_fields`
//! machinery, with two deliberately-adjacent critical ops appended by hand
//! (see that function's module doc comment for why the ordinary generator
//! can't produce this shape at all: a relationship always points from a
//! lower-indexed table to a higher-indexed one, and ops are emitted one
//! table's entire stream at a time, so a from-side op and a parent-side op
//! can never land adjacent to each other in a generated stream).
//!
//! Every test below is a hand-built pin, not a proptest property: the
//! interesting variable here is a small, discrete set of *shapes* (which
//! variant, which timing), not a continuous value domain proptest's
//! shrinking would help explore — matching `tests/convergence.rs`'s own
//! relationship pins (one named test per engine-supported shape) rather than
//! `tests/convergence.rs`'s `trivial_program`-driven property.
//!
//! Two timing modes are driven per variant:
//!
//! - **`run_in_the_same_intake_window`** (#138 item 2, the intake-lag
//!   window): the parent-side and from-side critical ops are applied
//!   back-to-back with no intervening `quiesce()` call, so the from-side
//!   change may still be uncommitted-to-the-ring (behind the watermark) when
//!   the parent's reverse work is enumerated. Single-worker, no forced seal
//!   — the cheap, always-on half of the coverage.
//! - **`run_across_a_seal_boundary`** (#138 items 1 and 3, the seal-boundary
//!   and out-of-order-segment-drain scenarios): the parent-side change is
//!   sealed into its own segment before the from-side change is even
//!   applied, using [`ManualBackend::force_seal_active_segment`] — mirroring
//!   `trellis/tests/spike_102.rs`'s
//!   `spike_a2_a_from_side_insert_drains_before_the_parents_reverse_work`
//!   (branch `spike/issue-102-validation-v2`) — then a multi-worker backend
//!   quiesces both now-simultaneously-claimable segments, letting the real
//!   engine's own claim/drain scheduling (not the harness) decide which
//!   drains first.
//!
//! Both modes assert the same property every other file in this crate does
//! (design doc §4): once the backend reaches quiescence, the materialized
//! target must equal the independent oracle recompute
//! ([`generative::run::check_program`]) — regardless of which of the two
//! critical ops the engine happened to drain first.

use std::time::{Duration, Instant};

use generative::backend::{Backend, ManualBackend};
use generative::generate::{RelInterleavingVariant, build_relationship_interleaving_scenario};
use generative::model::{Op, OpOutcome, Program};
use generative::run::check_program;
use testkit::TestCluster;
use trellis::{Config, Pool};

/// Applies one op and asserts its actual outcome matches what the scenario
/// builder recorded on it (design doc §4 "operation errors are checked, not
/// swallowed") — the same check `generative::run::run_convergence` makes
/// internally, inlined here since this file drives `ManualBackend` directly
/// rather than through that loop (it needs to control exactly when
/// `quiesce()`/`force_seal_active_segment()` are called relative to the two
/// critical ops, which `run_convergence`'s fixed apply-then-quiesce-every-op
/// loop can't express).
async fn apply_checked(backend: &mut ManualBackend, op: &Op) {
    let actual = match backend.apply(op).await {
        Err(_) => OpOutcome::Fails,
        Ok(0) => OpOutcome::AffectsNoRows,
        Ok(_) => OpOutcome::Succeeds,
    };
    assert!(
        op.expect().matches(&actual),
        "op {op:?} expected {:?} but produced {actual:?}",
        op.expect()
    );
}

/// Polls [`ManualBackend::has_pending`] until a just-committed change has
/// actually reached the ring, or panics after `timeout`. See
/// [`ManualBackend::force_seal_active_segment`]'s doc comment for why a
/// caller must do this before sealing.
async fn wait_until_pending(backend: &ManualBackend, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if backend.has_pending().await.expect("check pending") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a committed change to reach the ring — intake may be stuck"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Quiesces, snapshots, and runs the three-way oracle check
/// ([`generative::run::check_program`]) — the shared tail of every scenario
/// below, regardless of which timing mode drove the two critical ops.
async fn assert_converges(pool: &Pool, program: &Program, backend: &mut ManualBackend) {
    backend.quiesce().await.expect("quiesce");
    let snapshot = backend.snapshot().await.expect("snapshot");
    if let Some((target, report)) = check_program(pool, program, &snapshot)
        .await
        .expect("oracle check")
    {
        panic!(
            "relationship interleaving scenario diverged on target {target:?}:\n{report}\n\
             program: {program:#?}"
        );
    }
}

/// #138 item 2 (the intake-lag window): the parent-side and from-side
/// critical ops commit back-to-back with no `quiesce()` in between, so
/// intake may not yet have staged the from-side change (or may not yet have
/// staged the parent's) when the other's processing begins. Single-worker,
/// no forced seal.
async fn run_in_the_same_intake_window(variant: RelInterleavingVariant) {
    let scenario = build_relationship_interleaving_scenario(variant);
    let program = &scenario.program;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(program).await.expect("install program");
    for op in &program.ops[..scenario.parent_op] {
        apply_checked(&mut backend, op).await;
    }
    backend.quiesce().await.expect("quiesce seeds");

    apply_checked(&mut backend, &program.ops[scenario.parent_op]).await;
    apply_checked(&mut backend, &program.ops[scenario.from_side_op]).await;

    assert_converges(&pool, program, &mut backend).await;
}

/// #138 items 1 and 3 (a seal boundary, then an out-of-order segment drain):
/// the parent-side change is sealed into its own segment before the
/// from-side change is even applied, so the two are guaranteed to land in
/// different, already-sealed segments — then a `workers`-worker backend
/// quiesces both, letting the engine's own claim/drain scheduling (not the
/// harness) pick which one actually drains first.
async fn run_across_a_seal_boundary(variant: RelInterleavingVariant, workers: usize) {
    let scenario = build_relationship_interleaving_scenario(variant);
    let program = &scenario.program;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // Widened maintenance_interval (mirroring `concurrent_convergence.rs`'s
    // `a_batch_that_exceeds_the_split_threshold_converges_across_workers`):
    // the engine's own maintenance loop also seals the active segment on its
    // normal 300ms cadence, which would otherwise race
    // `force_seal_active_segment`'s manual calls below often enough to make
    // which two segments the two critical ops actually land in
    // nondeterministic. 3s comfortably exceeds how long this test's brief
    // critical section takes (a handful of round trips plus a couple of
    // 10ms polls), so in practice the harness's own manual seals are the
    // only ones that fire during it — but it still stays well inside
    // `quiesce()`'s 30s timeout, so the maintenance loop's *other* jobs
    // (recovery, retirement) still run enough to let convergence complete;
    // widening it further (as that pin does, to 10s) is safe there only
    // because it never seals anything by hand and just waits the one
    // natural tick out.
    let mut backend =
        ManualBackend::connect_with_options(db.dsn(), workers, Some(Duration::from_secs(3)))
            .await
            .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    backend.install(program).await.expect("install program");
    for op in &program.ops[..scenario.parent_op] {
        apply_checked(&mut backend, op).await;
    }
    // Widening `maintenance_interval` above means nothing auto-seals the
    // seed batch either unless this does it by hand first — `quiesce()`
    // only waits for convergence, it never forces a seal on its own. Must
    // wait for intake to actually stage the seeds first (not just check
    // once): checking `has_pending` a single time right after the apply
    // loop can race intake's own WAL consumption and see nothing yet,
    // silently skipping the seal and leaving `quiesce()` to hang until the
    // next (3s-away) automatic tick — or, worse, race that automatic tick
    // mid-manual-seal below.
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    backend
        .force_seal_active_segment()
        .await
        .expect("seal the seed batch");
    backend.quiesce().await.expect("quiesce seeds");
    assert!(
        !backend.has_pending().await.expect("check pending"),
        "seeds must be fully drained before the critical section starts, or the forced seal \
         below could seal leftover seed rows instead of the parent's own change"
    );

    apply_checked(&mut backend, &program.ops[scenario.parent_op]).await;
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    let parent_seg = backend
        .force_seal_active_segment()
        .await
        .expect("seal the parent's own segment");

    apply_checked(&mut backend, &program.ops[scenario.from_side_op]).await;
    wait_until_pending(&backend, Duration::from_secs(5)).await;
    let from_side_seg = backend
        .force_seal_active_segment()
        .await
        .expect("seal the from-side segment");
    assert!(
        from_side_seg > parent_seg,
        "the from-side change (segment {from_side_seg}) must land in a segment strictly after \
         the parent's own (segment {parent_seg}) for this to actually be a seal-boundary crossing"
    );

    assert_converges(&pool, program, &mut backend).await;
}

/// How many application-worker tasks [`run_across_a_seal_boundary`] runs
/// with — `> 1` is the whole point (#138 item 3: more than one segment
/// simultaneously claimable), and small/fixed for the same reason
/// `tests/concurrent_convergence.rs`'s `PROPERTY_WORKERS` is: this property
/// isn't sweeping worker counts, just proving the race is survivable with
/// real concurrency in the picture.
const SEAL_BOUNDARY_WORKERS: usize = 2;

#[tokio::test(flavor = "multi_thread")]
async fn parent_field_update_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentFieldUpdate).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_insert_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentInsert).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_delete_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::ParentDelete).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_nonexistent_parent_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::RepointToNonexistentParent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_null_parent_converges_in_the_same_intake_window() {
    run_in_the_same_intake_window(RelInterleavingVariant::RepointToNullParent).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_field_update_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::ParentFieldUpdate,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_insert_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(RelInterleavingVariant::ParentInsert, SEAL_BOUNDARY_WORKERS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn parent_delete_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(RelInterleavingVariant::ParentDelete, SEAL_BOUNDARY_WORKERS).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_nonexistent_parent_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::RepointToNonexistentParent,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn repoint_to_null_parent_converges_across_a_seal_boundary() {
    run_across_a_seal_boundary(
        RelInterleavingVariant::RepointToNullParent,
        SEAL_BOUNDARY_WORKERS,
    )
    .await;
}
