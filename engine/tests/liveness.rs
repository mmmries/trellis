//! Integration tests for claim liveness (issue #15, stage 04's third
//! piece), run against a real, ephemeral Postgres instance via the shared
//! harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/04-claiming-and-the-fold.md, "Keeping a
//! claim alive", "Two ways a claim comes back", and "An orthogonal gate:
//! the pause lease" for the design these tests hold the implementation to.
//! Short real-time windows throughout (ttl ~300ms, daemon interval ~50ms)
//! so this runs fast against the real cluster.
//!
//! [`FenceMissBackoff`]'s pure sequence is covered in-module
//! (`engine/src/staging/liveness.rs`'s `#[cfg(test)]`), not here.

use std::time::Duration;

use engine::config::DEFAULT_SCHEMA;
use engine::staging::{
    HeartbeatDaemon, HeartbeatDaemonConfig, SegmentState, claim, liveness, seal,
};
use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};

/// Connects directly to `dsn` (bypassing `engine::Pool`), matching
/// `claims.rs`/`sealing.rs`/`fold.rs`'s convention.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn insert_recompute(client: &Client, table: &str, key: &str) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ('orders', $1, 'recompute', 0)"
            ),
            &[&key],
        )
        .await
        .expect("insert recompute row");
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn segment_state(client: &Client, seg_seq: i64) -> SegmentState {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    SegmentState::from_sql(&state).unwrap_or_else(|| panic!("unrecognized state {state:?}"))
}

async fn claimed_buckets(client: &Client, seg_seq: i64, claimed_by: &str) -> Vec<i16> {
    client
        .query(
            "select bucket from seg_claims where seg_seq = $1 and claimed_by = $2 order by bucket",
            &[&seg_seq, &claimed_by],
        )
        .await
        .expect("read seg_claims")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

/// A single-bucket sealed batch, ready to be claimed — the fixture every
/// test in this file starts from.
async fn seal_one_bucket_batch(client: &mut Client, key: &str) -> i64 {
    insert_recompute(client, "seg_0", key).await;
    seal_active_segment(client).await
}

#[tokio::test]
async fn reclaim_frees_a_stale_claim_and_a_fresh_claim_picks_it_up() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    let ttl = Duration::from_millis(300);

    let won = claim::claim(&client, seg_seq, "dead-worker", 1)
        .await
        .expect("initial claim");
    assert!(!won.is_empty(), "the initial claim must win the one bucket");

    // No heartbeat at all: after the TTL, the claim is stale.
    tokio::time::sleep(ttl + Duration::from_millis(100)).await;

    let reclaimed = liveness::reclaim_stale(&client, ttl)
        .await
        .expect("reclaim_stale");
    assert_eq!(reclaimed, 1, "the one stale claim must be reclaimed");
    assert!(
        claimed_buckets(&client, seg_seq, "dead-worker")
            .await
            .is_empty(),
        "the dead worker's claim row must be gone"
    );

    // A fresh claim by a different worker now wins those buckets — the
    // batch stayed `draining` throughout, so it just re-picks the freed
    // buckets normally.
    assert_eq!(
        segment_state(&client, seg_seq).await,
        SegmentState::Draining
    );
    let re_won = claim::claim(&client, seg_seq, "fresh-worker", 1)
        .await
        .expect("re-claim");
    assert_eq!(
        re_won, won,
        "the fresh worker must win exactly the buckets the dead worker lost"
    );

    // TODO(#11): assert the reclaimed worker's apply rolls back once apply
    // exists (blocked on aggregate transform-defs) — today's test only
    // covers the claim-side reclaim, not the apply-side rollback.
}

#[tokio::test]
async fn a_daemon_heartbeat_survives_a_bulk_drain_that_outlives_the_ttl() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let daemon_seg = seal_one_bucket_batch(&mut client, "daemon-kept").await;
    let plain_seg = seal_one_bucket_batch(&mut client, "never-heartbeat").await;

    let ttl = Duration::from_millis(300);
    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout: Duration::from_secs(60),
        },
    );

    let daemon_won = claim::claim(&client, daemon_seg, "bulk-worker", 1)
        .await
        .expect("claim daemon-tracked batch");
    assert!(!daemon_won.is_empty());
    daemon.register(daemon_seg, "bulk-worker").await;

    let plain_won = claim::claim(&client, plain_seg, "unwatched-worker", 1)
        .await
        .expect("claim plain batch");
    assert!(!plain_won.is_empty());

    // Simulate a bulk-shape drain: no `heartbeat_inline` call at all, sleep
    // well past the TTL. The daemon (registered, ticking every 50ms) is
    // the only thing keeping `daemon_seg`'s claim alive.
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;

    let reclaimed = liveness::reclaim_stale(&client, ttl)
        .await
        .expect("reclaim_stale");
    assert_eq!(
        reclaimed, 1,
        "exactly the plain (non-daemon-tracked) claim must be reclaimed"
    );

    assert_eq!(
        claimed_buckets(&client, daemon_seg, "bulk-worker").await,
        daemon_won,
        "the daemon-tracked claim must survive the sweep unchanged"
    );
    assert!(
        claimed_buckets(&client, plain_seg, "unwatched-worker")
            .await
            .is_empty(),
        "the claim nobody heartbeat must be gone"
    );

    daemon.deregister(daemon_seg, "bulk-worker").await;
}

#[tokio::test]
async fn release_is_scoped_to_claimed_by_and_leaves_other_workers_claims_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    client
        .execute(
            "update segments set bucket_count = 8 where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("widen bucket_count so both workers can hold a bucket");

    let won_a = claim::claim(&client, seg_seq, "worker-a", 2)
        .await
        .expect("worker a claim");
    let won_b = claim::claim(&client, seg_seq, "worker-b", 2)
        .await
        .expect("worker b claim");
    assert!(!won_a.is_empty());
    assert!(!won_b.is_empty());

    let released = liveness::release(&client, seg_seq, "worker-a")
        .await
        .expect("release worker-a");
    assert_eq!(
        released,
        won_a.len() as u64,
        "release must delete exactly the buckets worker-a held"
    );

    assert!(
        claimed_buckets(&client, seg_seq, "worker-a")
            .await
            .is_empty(),
        "worker-a's claim must be gone"
    );
    assert_eq!(
        claimed_buckets(&client, seg_seq, "worker-b").await,
        won_b,
        "worker-b's claim must be untouched by worker-a's release"
    );

    // Releasing again (already released) matches zero rows — a no-op, not
    // an error, and it must not touch worker-b either.
    let released_again = liveness::release(&client, seg_seq, "worker-a")
        .await
        .expect("release worker-a again");
    assert_eq!(released_again, 0);
    assert_eq!(claimed_buckets(&client, seg_seq, "worker-b").await, won_b);
}

#[tokio::test]
async fn pause_lease_gates_claim_unless_paused_and_cannot_be_resurrected_after_expiry() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    let ttl = Duration::from_millis(300);

    let acquired = liveness::acquire_pause_lease(&client, "auditor", "pauser-1", ttl)
        .await
        .expect("acquire pause lease");
    assert!(acquired, "no lease existed yet, so this must succeed");
    assert!(
        liveness::claiming_is_paused(&client)
            .await
            .expect("paused?")
    );

    let gated = liveness::claim_unless_paused(&client, seg_seq, "worker", 1)
        .await
        .expect("claim_unless_paused while paused");
    assert!(
        gated.is_none(),
        "claim_unless_paused must refuse to claim while the lease is active"
    );

    // A second holder can't steal a live lease.
    let stolen = liveness::acquire_pause_lease(&client, "auditor", "pauser-2", ttl)
        .await
        .expect("attempted steal");
    assert!(
        !stolen,
        "a live lease must not be acquirable by a second holder"
    );

    // The dead pauser's lease auto-expires — no wedge.
    tokio::time::sleep(ttl + Duration::from_millis(150)).await;
    assert!(
        !liveness::claiming_is_paused(&client)
            .await
            .expect("paused after expiry?"),
        "an expired lease must stop gating claims"
    );

    // A heartbeat that arrives after expiry must not resurrect the lease.
    let resurrected = liveness::heartbeat_pause_lease(&client, "auditor", "pauser-1", ttl)
        .await
        .expect("late heartbeat");
    assert!(
        !resurrected,
        "a heartbeat after expiry must report false, not resurrect the lease"
    );
    assert!(
        !liveness::claiming_is_paused(&client)
            .await
            .expect("paused after late heartbeat?"),
        "the lapsed lease must stay lapsed after a late heartbeat"
    );

    // Takeover safety (holder-scoping): a second pauser re-acquires the now-
    // lapsed lease_id, then the *original* pauser issues its late clean-
    // shutdown release and heartbeat. Neither must touch pauser-2's live
    // lease — otherwise pauser-1 silently un-pauses the fleet under an
    // auditor that still believes claiming is suspended.
    let reacquired = liveness::acquire_pause_lease(&client, "auditor", "pauser-2", ttl)
        .await
        .expect("pauser-2 re-acquires the lapsed lease");
    assert!(reacquired, "the lapsed lease must be re-acquirable");

    liveness::release_pause_lease(&client, "auditor", "pauser-1")
        .await
        .expect("pauser-1 late release (wrong holder — a no-op)");
    let stale_hb = liveness::heartbeat_pause_lease(&client, "auditor", "pauser-1", ttl)
        .await
        .expect("pauser-1 late heartbeat (wrong holder)");
    assert!(
        !stale_hb,
        "the original holder's heartbeat must not touch the successor's lease"
    );
    assert!(
        liveness::claiming_is_paused(&client)
            .await
            .expect("paused under pauser-2?"),
        "pauser-2's live lease must survive the original holder's late release/heartbeat"
    );

    // Drain pauser-2's lease so the tail of the test sees an unpaused fleet.
    liveness::release_pause_lease(&client, "auditor", "pauser-2")
        .await
        .expect("pauser-2 clean release");

    let now_allowed = liveness::claim_unless_paused(&client, seg_seq, "worker", 1)
        .await
        .expect("claim_unless_paused after expiry");
    assert!(
        now_allowed.is_some(),
        "claiming must proceed once the lease has lapsed"
    );
}

#[tokio::test]
async fn heartbeat_pause_lease_keeps_a_live_lease_alive() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let ttl = Duration::from_millis(300);
    liveness::acquire_pause_lease(&client, "auditor", "pauser-1", ttl)
        .await
        .expect("acquire");

    // Heartbeat before expiry, repeatedly, past what the original ttl alone
    // would have covered — the lease must stay live throughout.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let ok = liveness::heartbeat_pause_lease(&client, "auditor", "pauser-1", ttl)
            .await
            .expect("heartbeat");
        assert!(ok, "a heartbeat before expiry must succeed");
        assert!(
            liveness::claiming_is_paused(&client)
                .await
                .expect("paused?")
        );
    }

    liveness::release_pause_lease(&client, "auditor", "pauser-1")
        .await
        .expect("release");
    assert!(
        !liveness::claiming_is_paused(&client)
            .await
            .expect("paused?")
    );
}

#[tokio::test]
async fn daemon_opens_no_connection_for_a_claim_that_never_lasts_a_full_interval() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_secs(3600), // long enough this test never ticks
            idle_timeout: Duration::from_secs(60),
        },
    );

    daemon.register(1, "fast-worker").await;
    daemon.deregister(1, "fast-worker").await;
    // No tick has fired (the interval is an hour), so the registry's
    // register-then-deregister must never have been observed.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        daemon.connections_opened(),
        0,
        "a claim that never survives one full interval must never cost a connection"
    );
    assert!(!daemon.is_connected());
}

#[tokio::test]
async fn daemon_closes_its_connection_after_idle_timeout_with_no_registered_claims() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let seg_seq = seal_one_bucket_batch(&mut client, "k").await;
    claim::claim(&client, seg_seq, "worker", 1)
        .await
        .expect("claim");

    let daemon = HeartbeatDaemon::spawn(
        db.dsn(),
        DEFAULT_SCHEMA,
        HeartbeatDaemonConfig {
            interval: Duration::from_millis(50),
            idle_timeout: Duration::from_millis(200),
        },
    );

    daemon.register(seg_seq, "worker").await;
    // Let a few ticks pass so the daemon actually opens its connection.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        daemon.is_connected(),
        "the daemon must have opened a connection for a claim that outlived one interval"
    );
    assert_eq!(daemon.connections_opened(), 1);

    daemon.deregister(seg_seq, "worker").await;
    // Wait past the idle timeout (measured in ticks past the registry going
    // empty), plus slack for a couple of ticks to observe it.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !daemon.is_connected(),
        "the daemon must close its connection once idle for longer than idle_timeout"
    );
}
