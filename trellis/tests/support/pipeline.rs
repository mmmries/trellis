//! The engine's pipeline stepped by hand: connect, define, a capture or
//! reconcile pass, a seal, and a settle (seal and drain until nothing is
//! pending). Every test that drives the pipeline itself calls these rather
//! than a private copy, so a change to the seal or drain API is one edit
//! here. [`crate::drain_driver::Driver`] is built on them.
//!
//! Nothing here waits for convergence. [`settle`] is a bounded number of
//! explicit rounds, and [`seal`] waits only for the seal gate's or a full
//! ring's backpressure to clear, with a hard deadline and a panic naming
//! what held it.
//!
//! A test file declares it at its root, `#[path = "support/pipeline.rs"]
//! mod pipeline;`. A support module built on it declares it inside itself
//! (`#[path = "pipeline.rs"] mod pipeline;`), so its users need no edit.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use tokio_postgres::Client;
use trellis::capture::reconcile::{self, PassOutcome};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{StoredStatus, TransformStatus, ValueType};
use trellis::integer::IntWidth;
use trellis::staging::{
    ApplyOutcome, StagingError, apply, collect_tombstones, has_pending, retire_drained_segments,
    seal as seal_api,
};
use trellis::{Config, Trellis, TrellisOptions};

/// The wake channel [`settle`]'s seals and drains notify. Nothing listens
/// on it: the tests step the pipeline themselves.
pub const WAKE: &str = "interleave_wake";

/// How long [`seal`] waits for the seal gate or a full ring: long enough
/// for a server process stalled on a loaded box, short enough that a
/// transaction the test itself left open fails the test in one timeout.
pub const SEAL_GATE_WAIT: Duration = seal_api::SEAL_GATE_WAIT;

/// How many seal/drain rounds [`settle`] runs before it gives up.
pub const SETTLE_ROUNDS: usize = 16;

/// The reconcile budget [`capture_pass`] gives one capture pass.
pub const CAPTURE_PASS_BUDGET: Duration = Duration::from_secs(2);

#[allow(unused_imports)]
pub use testkit::{connect_raw, connect_with};

/// A plain connection with only the engine's schema first on the path, the
/// session [`crate::drain_driver::Driver`]'s clients have always had.
/// [`connect_raw`] also pins date and `bytea` output.
pub async fn connect(dsn: &str) -> Client {
    connect_with(dsn, &format!("set search_path to {DEFAULT_SCHEMA}, public")).await
}

/// A `Trellis` handle with default options: one that defines, alters and
/// reads status, and runs no background work of its own here.
pub async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, then the start of ready builds.
pub async fn reconcile_pass(raw: &mut Client, pool: &trellis::pool::Pool) {
    trellis::client::reconcile_pass(raw, pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

/// The capture half of one pass, within [`CAPTURE_PASS_BUDGET`]; panics if
/// any table failed.
pub async fn capture_pass(raw: &mut Client, pool: &trellis::pool::Pool) -> PassOutcome {
    let outcome = capture_pass_within(raw, pool, CAPTURE_PASS_BUDGET).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    outcome
}

/// The capture half of one pass within `budget`, failures and all.
pub async fn capture_pass_within(
    raw: &mut Client,
    pool: &trellis::pool::Pool,
    budget: Duration,
) -> PassOutcome {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    reconcile::reconcile(raw, DEFAULT_SCHEMA, &desired, Instant::now() + budget)
        .await
        .expect("capture pass")
}

/// Seals both phases and returns the sealed batch. Retires every drained
/// segment it can first, so a test never runs the four-slot ring full.
///
/// The seal gate holds while a transaction that was in flight at the
/// previous batch's fence is still open. That is backpressure, which the
/// engine's own sealer retries; here the likely holder is a server process
/// finishing on a loaded box (autovacuum's analyze, say, which takes an xid
/// for its statistics writes). A full ring is backpressure too, while the
/// slot the seal needs holds a drained segment: the retirement pass skips a
/// slot another session is reading at that instant ([`try_seal`]). So this
/// waits up to [`SEAL_GATE_WAIT`] for either to clear. A holder the test
/// itself leaves open never goes away, and the panic names every open
/// xid-holding session, and for a full ring the ring's slots and their lock
/// holders. A ring full of work not yet drained panics at once.
pub async fn seal(client: &mut Client) -> i64 {
    seal_within(client, SEAL_GATE_WAIT)
        .await
        .unwrap_or_else(|held| panic!("seal phase 1: {held}"))
}

/// [`seal`], waiting at most `wait` for the seal gate or a full ring; `Err`
/// names what still held it when it ran out: the sessions holding an xid
/// open, or the ring's registry rows and the locks held on its slots.
pub async fn seal_within(client: &mut Client, wait: Duration) -> Result<i64, String> {
    let deadline = Instant::now() + wait;
    loop {
        let refused = match seal_once(client).await {
            Ok(batch) => return Ok(batch),
            Err(refused) => refused,
        };
        if Instant::now() >= deadline {
            return Err(match refused {
                StagingError::RingFull { ring_slot } => format!(
                    "the ring stayed full for {wait:?}: ring slot {ring_slot} still holds a \
                     drained segment that retirement skipped; {}; transactions holding an \
                     xid: {}",
                    ring_state(client).await,
                    seal_api::xid_holders(client).await
                ),
                _ => format!(
                    "the seal gate stayed blocked for {wait:?} ({}); transactions holding an xid: {}",
                    gate_state(client).await,
                    seal_api::xid_holders(client).await
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// [`seal`], or `None` when the seal is refused as backpressure: the seal
/// gate (a transaction that may still write into the active segment's
/// predecessor is open) or a full ring whose needed slot holds a drained
/// segment, either of which a test running other sessions concurrently can
/// meet.
pub async fn try_seal(client: &mut Client) -> Option<i64> {
    seal_once(client).await.ok()
}

/// One retirement pass and one seal; `Err` is `seal_phase1`'s backpressure
/// refusal, `SealGateBlocked` or `RingFull`.
///
/// `RingFull` is backpressure here as it is to the engine's sealer, which
/// retries it on its next tick, when the slot the seal needs holds a drained
/// segment this pass could not retire: retirement takes the slot's table
/// `ACCESS EXCLUSIVE ... NOWAIT` and skips it while a concurrent reader of
/// the ring holds it (a convergence wait's poll reads every slot, so a
/// `self_check` running alongside the drain is one), and holds it back while
/// a session holding an xid from before the successor's seal is open. Work
/// that is not drained keeps the ring full until the test drains it, which
/// it cannot do while it waits here, so that panics at once, naming the
/// slots.
async fn seal_once(client: &mut Client) -> Result<i64, StagingError> {
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
    let outcome = match seal_api::seal_phase1(client).await {
        Ok(outcome) => outcome,
        Err(refused @ StagingError::SealGateBlocked) => return Err(refused),
        Err(refused @ StagingError::RingFull { ring_slot }) => {
            if undrained_work_holds(client, ring_slot).await {
                panic!(
                    "seal phase 1: {refused}; work not yet drained holds it: {}",
                    ring_state(client).await
                );
            }
            return Err(refused);
        }
        Err(err) => panic!("seal phase 1: {err}"),
    };
    seal_api::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
        .await
        .expect("seal phase 2");
    Ok(outcome.sealed_seg_seq)
}

/// Drains `batch` to completion on worker `worker`, in its own task on a
/// pool counted as a background worker's (a drain that holds more than
/// `MAX_BACKGROUND_HELD` connections at once aborts the test). Claims again
/// until the claim wins nothing, and returns the last page's outcome.
pub async fn drain(pool: &trellis::pool::Pool, batch: i64, worker: &str) -> Option<ApplyOutcome> {
    let pool = pool.counting_as_background();
    let worker = worker.to_string();
    tokio::spawn(trellis::pool::count_background_connections_in(async move {
        let mut last = None;
        while let Some(outcome) = apply::drain_once(&pool, batch, &worker, 1, WAKE).await? {
            last = Some(outcome);
        }
        Ok::<_, trellis::staging::ApplyError>(last)
    }))
    .await
    .expect("drain task")
    .unwrap_or_else(|e| panic!("drain batch {batch}: {e:?}"))
}

/// Seals and drains until nothing is pending anywhere in the ring, running
/// every 1-1 `JOIN`'s pending fan-out to its end and collecting the
/// tombstones the drained prefix no longer needs each round. At most
/// [`SETTLE_ROUNDS`] rounds: a bounded deterministic loop, not a wait for
/// convergence.
pub async fn settle(pool: &trellis::pool::Pool, client: &mut Client) {
    for _ in 0..SETTLE_ROUNDS {
        let batch = seal(client).await;
        drain(pool, batch, "settle").await;
        trellis::staging::one_to_one_join_run_pending_fan_outs(pool)
            .await
            .expect("run the pending fan-outs");
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        collect_tombstones(client)
            .await
            .expect("collect tombstones");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within {SETTLE_ROUNDS} seal/drain rounds");
}

/// Starts every registered definition's build, as the staging worker does
/// (registration only records a definition, ADR-0002), and runs each build
/// to completion.
pub async fn start_and_settle_builds(pool: &trellis::pool::Pool) {
    trellis::intake::harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
}

/// `target`'s stored status, read from the catalog by bare target name.
pub async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    StoredStatus::from_persisted(&text)
        .expect("a stored status")
        .into()
}

/// Every definition's stored status, by bare target name.
pub async fn statuses(raw: &Client) -> BTreeMap<String, TransformStatus> {
    raw.query(
        "select split_part(target_table, '.', 2), status from transform_definitions",
        &[],
    )
    .await
    .expect("read statuses")
    .into_iter()
    .map(|row| {
        let status = StoredStatus::from_persisted(row.get(1)).expect("a stored status");
        (row.get(0), status.into())
    })
    .collect()
}

/// Alternates [`reconcile_pass`], builds and [`settle`] until each of
/// `targets` is live: at most 8 rounds, not a wait for convergence.
pub async fn bring_live(raw: &mut Client, pool: &trellis::pool::Pool, targets: &[&str]) {
    for _ in 0..8 {
        bring_live_round(raw, pool).await;
        let all = statuses(raw).await;
        if targets
            .iter()
            .all(|t| all.get(*t) == Some(&TransformStatus::Live))
        {
            return;
        }
    }
    panic!(
        "{targets:?} did not all go live within 8 rounds: {:?}",
        statuses(raw).await
    );
}

/// [`bring_live`] for every definition, within 10 rounds.
pub async fn bring_live_all(raw: &mut Client, pool: &trellis::pool::Pool) {
    for _ in 0..10 {
        bring_live_round(raw, pool).await;
        if statuses(raw)
            .await
            .values()
            .all(|s| *s == TransformStatus::Live)
        {
            return;
        }
    }
    panic!("not every definition went live: {:?}", statuses(raw).await);
}

/// One staging-worker pass, the builds it starts, and a [`settle`].
async fn bring_live_round(raw: &mut Client, pool: &trellis::pool::Pool) {
    reconcile_pass(raw, pool).await;
    trellis::staging::build::settle_builds(pool).await;
    settle(pool, raw).await;
}

/// The two sides of the seal gate's comparison: the current snapshot's
/// `xmin` (the oldest xid running now) and the predecessor fence's `xmax`,
/// or that the predecessor has no fence, which holds the gate whatever is
/// running. A holder blocks only if its xid is below that `xmax`.
async fn gate_state(client: &Client) -> String {
    let row = client
        .query_one(
            "select pg_snapshot_xmin(pg_current_snapshot())::text, \
                    (select coalesce(pg_snapshot_xmax(s.fence_snapshot)::text, 'unfenced') \
                     from segments s, segment_pointer p \
                     where s.seg_seq = p.active_seq - 1)",
            &[],
        )
        .await
        .expect("read the seal gate's state");
    format!(
        "snapshot xmin {}, predecessor fence xmax {}",
        row.get::<_, String>(0),
        row.get::<_, Option<String>>(1)
            .unwrap_or_else(|| "none (no predecessor)".to_string())
    )
}

/// The registry rows and every lock another session holds on a ring slot's
/// table: what a `RingFull` refusal is waiting on.
async fn ring_state(client: &Client) -> String {
    let segments = rows(
        client,
        "select seg_seq, ring_slot, state::text from segments order by seg_seq",
    )
    .await;
    let locks = rows(
        client,
        "select l.pid, c.relname::text, l.mode, l.granted, \
                coalesce(left(a.query, 120), '') \
         from pg_locks l join pg_class c on c.oid = l.relation \
         left join pg_stat_activity a on a.pid = l.pid \
         where c.relname in ('seg_0', 'seg_1', 'seg_2', 'seg_3') \
           and l.pid <> pg_backend_pid()",
    )
    .await;
    format!(
        "segments (seg_seq, ring_slot, state): {}; ring slot locks held: {}",
        segments.join(", "),
        if locks.is_empty() {
            "none".to_string()
        } else {
            locks.join("; ")
        }
    )
}

/// Whether `ring_slot`'s segment stays unretirable until the test drains
/// more: it, its successor, or a segment older than it is not drained. That
/// is retirement's eligibility less its lock and its `xmin` condition, the
/// two that clear on their own.
async fn undrained_work_holds(client: &Client, ring_slot: i16) -> bool {
    client
        .query_one(
            "select exists (select 1 from segments o join segments s \
                              on s.seg_seq <= o.seg_seq + 1 \
                            where o.ring_slot = $1 and s.state <> 'drained')",
            &[&ring_slot],
        )
        .await
        .expect("read the ring slot's occupant")
        .get(0)
}

/// The `int4` column type.
pub fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// Each of `names` as a `numeric` column.
pub fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Each `(name, type)` pair as a column map.
pub fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

/// `sql`'s rows, each rendered as a row literal, `(1,25,2)`.
pub async fn rows(client: &Client, sql: &str) -> Vec<String> {
    client
        .query(&format!("select r::text from ({sql}) r"), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}
