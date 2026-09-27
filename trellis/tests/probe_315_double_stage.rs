//! Scratch probe for #315 investigation (not for commit).

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;
use trellis::{Client as TrellisClient, ClientOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) and pins
/// `search_path`, matching `client_e2e.rs`'s helper of the same name.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

/// Polls `predicate` until it returns `true`, or panics with `message` and a
/// dump of every non-drained segment plus its ring rows once `timeout`
/// elapses — the diagnostic the issue's own investigation had to assemble by
/// hand, so a future regression reports the stuck `(src_table, key)` pair
/// directly instead of just "timed out".
async fn poll_until<F>(raw: &Client, timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out after {timeout:?}: {message}\n{}",
                dump(raw).await
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Renders every segment's slot/state/seq plus that slot's ring rows, for
/// [`poll_until`]'s panic message.
async fn dump(raw: &Client) -> String {
    let mut out = String::from("segments and ring contents:\n");
    let segments = raw
        .query(
            "select seg_seq, ring_slot, state from segments order by seg_seq",
            &[],
        )
        .await
        .expect("read segments");
    for seg in &segments {
        let seq: i64 = seg.get(0);
        let slot: i16 = seg.get(1);
        let state: String = seg.get(2);
        out.push_str(&format!("  seg_seq={seq} slot={slot} state={state}\n"));
        let rows = raw
            .query(
                &format!(
                    "select src_table, key, op from seg_{slot} order by src_table, key, change_id"
                ),
                &[],
            )
            .await
            .expect("read ring slot");
        for row in rows {
            let src_table: String = row.get(0);
            let key: String = row.get(1);
            let op: String = row.get(2);
            out.push_str(&format!("    {src_table:?} key={key:?} op={op}\n"));
        }
    }
    out
}

/// Asserts no segment is parked mid-drain — the observable signature of
/// issue #267's live-lock, distinct from "slow": a `draining` segment that
/// never reaches `drained` means the apply transaction is failing
/// deterministically and being retried forever.
async fn assert_no_stuck_segment(raw: &Client) {
    let stuck: i64 = raw
        .query_one(
            "select count(*) from segments where state = 'draining'",
            &[],
        )
        .await
        .expect("count draining segments")
        .get(0);
    assert_eq!(
        stuck,
        0,
        "a segment is stuck mid-drain — issue #267's duplicate-`src_table` live-lock:\n{}",
        dump(raw).await
    );
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Options every test here shares: a full client (staging + application
/// workers) with a reconcile cadence short enough that an intermediate hop
/// joins the publication within the test, rather than after it — the
/// precondition issue #267's workaround deliberately avoided by setting this
/// interval long.
fn live_options() -> ClientOptions {
    ClientOptions {
        staging_worker: true,
        application_threads: 4,
        source_tables: vec!["public.src".to_string()],
        reconcile_interval: Duration::from_millis(200),
        maintenance_interval: Duration::from_millis(2),
        poll_interval: Duration::from_millis(5),
        ..Default::default()
    }
}

/// Waits until `table` has actually joined the CDC publication. Every test
/// here waits for this *before* writing, so the duplicate-staging window is
/// guaranteed open rather than raced into: it is exactly the moment the
/// propagation path and the intake path both start observing the same write.
async fn wait_for_publication(raw: &Client, table: &str) {
    poll_until(
        raw,
        Duration::from_secs(30),
        &format!("{table} never joined the CDC publication"),
        async || {
            let published: i64 = raw
                .query_one(
                    "select count(*) from pg_publication_tables where tablename = $1",
                    &[&table],
                )
                .await
                .expect("read pg_publication_tables")
                .get(0);
            published > 0
        },
    )
    .await;
}

/// Reads `table` as an `id -> value` map, for the convergence assertions.
async fn snapshot(raw: &Client, table: &str, value_col: &str) -> HashMap<String, Option<String>> {
    raw.query(
        &format!("select id::text, {value_col}::text from {table}"),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("read {table}: {e}"))
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}


#[tokio::test]
async fn probe_double_staging_one_to_one_hop_into_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table public.src (id integer primary key, grp numeric, val numeric)")
        .await
        .expect("create src");

    let client = TrellisClient::start(db.dsn(), live_options()).expect("client start");
    let columns = numeric_columns(&["id", "grp", "val"]);
    install_definition(&db.pool, "TRANSFORM h1 FROM public.src SELECT grp AS grp, val AS val", &columns, "public")
        .await
        .expect("install h1");
    raw.batch_execute("alter table public.h1 replica identity full")
        .await
        .expect("widen h1");
    install_definition(
        &db.pool,
        "TRANSFORM agg FROM public.h1 GROUP BY grp SELECT COUNT(*) AS n, SUM(val) AS total",
        &columns,
        "public",
    )
    .await
    .expect("install agg");
    wait_for_publication(&raw, "h1").await;

    for i in 0..40 {
        raw.execute(
            "insert into public.src (id, grp, val) values ($1, 1, 10)",
            &[&i],
        )
        .await
        .expect("insert");
        tokio::time::sleep(Duration::from_millis(37)).await;
    }
    for i in 0..40 {
        raw.execute("update public.src set val = val + 1 where id = $1", &[&i])
            .await
            .expect("update");
        tokio::time::sleep(Duration::from_millis(29)).await;
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut last = (String::new(), String::new());
    loop {
        let row = raw
            .query_opt("select n::text, total::text from agg where grp = 1", &[])
            .await
            .expect("read agg");
        if let Some(row) = row {
            last = (row.get(0), row.get(1));
            if last == ("40".to_string(), "440".to_string()) {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            let h1: i64 = raw.query_one("select count(*) from h1", &[]).await.unwrap().get(0);
            panic!("agg never converged to n=40,total=440; last seen {last:?}; h1 rows={h1}\n{}", dump(&raw).await);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // stay converged?
    tokio::time::sleep(Duration::from_secs(3)).await;
    let row = raw.query_one("select n::text, total::text from agg where grp = 1", &[]).await.unwrap();
    let after: (String, String) = (row.get(0), row.get(1));
    assert_eq!(after, ("40".to_string(), "440".to_string()), "drifted after convergence");
    client.shutdown().await.expect("clean shutdown");
}
