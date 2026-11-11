//! Tests of the wait and connection helpers: each returns as soon as its
//! condition holds, and a wait that runs out panics naming what it waited
//! for and what it last saw.

use std::cell::Cell;
use std::time::{Duration, Instant};

use testkit::{TestCluster, connect_raw, connect_with, wait_blocked_behind, wait_for, wait_until};

#[tokio::test]
async fn wait_until_returns_once_the_probe_holds() {
    let probes = Cell::new(0);
    wait_until("the third probe", Duration::from_secs(30), async || {
        probes.set(probes.get() + 1);
        probes.get() == 3
    })
    .await;
    assert_eq!(probes.get(), 3);
}

#[tokio::test]
#[should_panic(expected = "timed out after 50ms waiting until the impossible")]
async fn wait_until_panics_naming_what_it_waited_for() {
    wait_until("the impossible", Duration::from_millis(50), async || false).await;
}

#[tokio::test]
async fn wait_until_probes_at_least_once_past_the_deadline() {
    // A zero timeout still checks the condition before giving up, so a slow
    // box never fails a wait whose condition already holds.
    wait_until("an immediate truth", Duration::ZERO, async || true).await;
}

#[tokio::test]
async fn wait_for_returns_the_probes_value() {
    let probes = Cell::new(0);
    let got = wait_for("the second probe", Duration::from_secs(30), async || {
        probes.set(probes.get() + 1);
        if probes.get() == 2 {
            Ok(probes.get() * 10)
        } else {
            Err(probes.get())
        }
    })
    .await;
    assert_eq!(got, 20);
}

#[tokio::test]
#[should_panic(expected = "waiting until the status is live; last seen: \"paused\"")]
async fn wait_for_panics_with_the_last_observation() {
    wait_for(
        "the status is live",
        Duration::from_millis(50),
        async || Err::<(), _>("paused"),
    )
    .await;
}

#[tokio::test]
async fn wait_for_keeps_to_its_deadline() {
    let started = Instant::now();
    let outcome = tokio::spawn(wait_for("never", Duration::from_millis(200), async || {
        Err::<(), _>(())
    }))
    .await;
    assert!(outcome.is_err(), "the wait panicked");
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis(200),
        "gave up early: {took:?}"
    );
    assert!(
        took < Duration::from_secs(10),
        "overran its deadline: {took:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_raw_pins_the_engine_session_and_connect_with_runs_only_its_setup() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let show = "select current_setting('search_path'), current_setting('datestyle'), \
                current_setting('bytea_output')";

    let raw = db.connect_raw().await;
    let row = raw.query_one(show, &[]).await.expect("show");
    assert_eq!(row.get::<_, String>(0), "trellis, public");
    assert_eq!(row.get::<_, String>(1), "ISO, YMD");
    assert_eq!(row.get::<_, String>(2), "hex");
    let same = connect_raw(db.dsn()).await;
    let row = same.query_one(show, &[]).await.expect("show");
    assert_eq!(row.get::<_, String>(0), "trellis, public");

    let plain = connect_with(db.dsn(), "").await;
    let row = plain.query_one(show, &[]).await.expect("show");
    assert_eq!(row.get::<_, String>(0), "\"$user\", public");

    let set = connect_with(db.dsn(), "set search_path to pg_catalog").await;
    let row = set.query_one(show, &[]).await.expect("show");
    assert_eq!(row.get::<_, String>(0), "pg_catalog");
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "session setup `set no_such_setting = 1`")]
async fn connect_with_panics_naming_a_setup_that_fails() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    connect_with(db.dsn(), "set no_such_setting = 1").await;
}

/// A session holding `public.t` locked, and one queued behind it.
async fn holder_and_waiter(
    dsn: &str,
) -> (tokio_postgres::Client, i32, tokio::task::JoinHandle<()>) {
    let holder = connect_with(dsn, "").await;
    holder
        .batch_execute("create table public.t (id int)")
        .await
        .expect("create the table");
    holder
        .batch_execute("begin; lock table public.t")
        .await
        .expect("take the lock");
    let holder_pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let waiter = connect_with(dsn, "").await;
    let queued = tokio::spawn(async move {
        waiter
            .batch_execute("select * from public.t")
            .await
            .expect("read once the lock is released");
    });
    (holder, holder_pid, queued)
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_blocked_behind_returns_the_queued_backend() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let observer = connect_with(db.dsn(), "").await;
    let (holder, holder_pid, queued) = holder_and_waiter(db.dsn()).await;

    let waiter_pid = wait_blocked_behind(&observer, holder_pid, Duration::from_secs(60), || false)
        .await
        .expect("a backend queued");
    let blocked_by: Vec<i32> = observer
        .query_one("select pg_blocking_pids($1)", &[&waiter_pid])
        .await
        .expect("read its blockers")
        .get(0);
    assert_eq!(blocked_by, vec![holder_pid]);

    holder.batch_execute("commit").await.expect("release");
    queued.await.expect("the waiter read");
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_blocked_behind_answers_none_once_its_alternative_holds() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let observer = connect_with(db.dsn(), "").await;
    let holder_pid: i32 = observer
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let checks = Cell::new(0);
    let got = wait_blocked_behind(&observer, holder_pid, Duration::from_secs(60), || {
        checks.set(checks.get() + 1);
        checks.get() == 3
    })
    .await;
    assert_eq!(got, None);
    assert_eq!(checks.get(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_blocked_behind_reports_a_waiter_seen_with_its_alternative() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let observer = connect_with(db.dsn(), "").await;
    let (holder, holder_pid, queued) = holder_and_waiter(db.dsn()).await;
    // Wait for the waiter to queue first, then ask with an alternative that
    // already holds: the waiter wins.
    wait_blocked_behind(&observer, holder_pid, Duration::from_secs(60), || false).await;
    let got = wait_blocked_behind(&observer, holder_pid, Duration::from_secs(60), || true).await;
    assert!(got.is_some());
    holder.batch_execute("commit").await.expect("release");
    queued.await.expect("the waiter read");
}

#[tokio::test(flavor = "multi_thread")]
#[should_panic(expected = "no backend queued behind pid")]
async fn wait_blocked_behind_panics_naming_the_pid_when_nothing_queues() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let observer = connect_with(db.dsn(), "").await;
    let idle = connect_with(db.dsn(), "").await;
    let idle_pid: i32 = idle
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    wait_blocked_behind(&observer, idle_pid, Duration::from_millis(100), || false).await;
}
