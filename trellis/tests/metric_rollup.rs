//! Integration tests for issue #54's rollup/prune jobs
//! (`trellis::rollup`, `trellis/migrations/V22__metric_rollup.sql`), run
//! against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`) — the point of this issue is a real DB
//! write/prune cycle, not just the in-memory registry-reading logic
//! `trellis/src/metrics.rs`'s own unit tests already cover.
//!
//! `testkit::TestCluster::create_isolated_database` applies every migration
//! (`trellis::migrate`) before handing back a database, so every test below
//! is also an implicit "V22 applies cleanly" check — if the migration were
//! malformed, `create_isolated_database` itself would panic before any test
//! body ran.

use std::time::{Duration, SystemTime};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::metrics::{self, MetricKind, MetricSample};
use trellis::rollup;

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

/// One row per `metric_rollup`, read back with the columns these tests care
/// about — not a full 1:1 mirror of every column (`id`/`labels` are read
/// separately: `labels` via [`label`] below, using Postgres's own `jsonb
/// ->> 'key'` operator rather than parsing JSON in Rust — this crate has no
/// `serde_json` dependency, see `trellis::rollup`'s own doc comment for
/// why).
struct RollupRow {
    metric_name: String,
    metric_kind: String,
    value: Option<f64>,
    bucket_bounds: Option<Vec<f64>>,
    bucket_counts: Option<Vec<i64>>,
    histogram_sum: Option<f64>,
    histogram_count: Option<i64>,
}

async fn fetch_rows(client: &Client, metric_name: &str) -> Vec<RollupRow> {
    let rows = client
        .query(
            "select metric_name, metric_kind, value, bucket_bounds, bucket_counts, \
             histogram_sum, histogram_count \
             from metric_rollup where metric_name = $1 order by id",
            &[&metric_name],
        )
        .await
        .expect("select metric_rollup rows");
    rows.into_iter()
        .map(|row| RollupRow {
            metric_name: row.get(0),
            metric_kind: row.get(1),
            value: row.get(2),
            bucket_bounds: row.get(3),
            bucket_counts: row.get(4),
            histogram_sum: row.get(5),
            histogram_count: row.get(6),
        })
        .collect()
}

/// Reads one label value back out via Postgres's own `jsonb ->> 'key'`
/// operator — see [`RollupRow`]'s doc comment for why this test file avoids
/// parsing JSON in Rust.
async fn label(client: &Client, metric_name: &str, key: &str) -> Option<String> {
    client
        .query_one(
            "select labels ->> $2 from metric_rollup where metric_name = $1 order by id limit 1",
            &[&metric_name, &key],
        )
        .await
        .ok()
        .and_then(|row| row.get(0))
}

async fn row_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from metric_rollup", &[])
        .await
        .expect("count metric_rollup")
        .get(0)
}

/// Hand-built samples (not `metrics::snapshot()`'s output) so this test
/// controls the exact shape written, independent of whatever else this
/// binary's shared global registry happens to hold — one of each kind.
fn synthetic_samples(suffix: &str) -> Vec<MetricSample> {
    vec![
        MetricSample {
            name: format!("rollup_test_counter_{suffix}"),
            kind: MetricKind::Counter,
            labels: vec![("transform".to_string(), "orders".to_string())],
            value: Some(7.0),
            buckets: Vec::new(),
            sum: None,
            count: None,
        },
        MetricSample {
            name: format!("rollup_test_gauge_{suffix}"),
            kind: MetricKind::Gauge,
            labels: vec![("state".to_string(), "active".to_string())],
            value: Some(3.0),
            buckets: Vec::new(),
            sum: None,
            count: None,
        },
        MetricSample {
            name: format!("rollup_test_hist_{suffix}"),
            kind: MetricKind::Histogram,
            labels: vec![("transform".to_string(), "orders".to_string())],
            value: None,
            buckets: vec![(0.01, 0), (0.1, 2), (1.0, 5)],
            sum: Some(3.5),
            count: Some(5),
        },
    ]
}

#[tokio::test]
async fn write_snapshot_persists_counter_gauge_and_histogram_rows_with_correct_shape() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let samples = synthetic_samples("shape");
    let rolled_up_at = SystemTime::now();
    let written = rollup::write_snapshot(&client, &samples, rolled_up_at)
        .await
        .expect("write_snapshot");
    assert_eq!(written, 3, "one row per sample");

    let counter_rows = fetch_rows(&client, "rollup_test_counter_shape").await;
    assert_eq!(counter_rows.len(), 1);
    assert_eq!(counter_rows[0].metric_kind, "counter");
    assert_eq!(counter_rows[0].value, Some(7.0));
    assert_eq!(counter_rows[0].bucket_bounds, None);
    assert_eq!(counter_rows[0].bucket_counts, None);
    assert_eq!(counter_rows[0].histogram_sum, None);
    assert_eq!(counter_rows[0].histogram_count, None);
    assert_eq!(
        label(&client, "rollup_test_counter_shape", "transform").await,
        Some("orders".to_string())
    );

    let gauge_rows = fetch_rows(&client, "rollup_test_gauge_shape").await;
    assert_eq!(gauge_rows.len(), 1);
    assert_eq!(gauge_rows[0].metric_kind, "gauge");
    assert_eq!(gauge_rows[0].value, Some(3.0));
    assert_eq!(
        label(&client, "rollup_test_gauge_shape", "state").await,
        Some("active".to_string())
    );

    let hist_rows = fetch_rows(&client, "rollup_test_hist_shape").await;
    assert_eq!(hist_rows.len(), 1);
    let hist = &hist_rows[0];
    assert_eq!(hist.metric_kind, "histogram");
    assert_eq!(hist.value, None);
    assert_eq!(hist.bucket_bounds, Some(vec![0.01, 0.1, 1.0]));
    assert_eq!(hist.bucket_counts, Some(vec![0, 2, 5]));
    assert_eq!(hist.histogram_sum, Some(3.5));
    assert_eq!(hist.histogram_count, Some(5));
    assert_eq!(hist.metric_name, "rollup_test_hist_shape");
}

#[tokio::test]
async fn write_snapshot_round_trips_a_real_registry_snapshot() {
    // Proves the whole pipeline (`metrics::record_*` -> `metrics::snapshot`
    // -> `rollup::write_snapshot`) works end to end, not just a hand-built
    // `Vec<MetricSample>` — distinctive labels since `trellis::metrics`'s
    // registry is one process-wide global shared by every test in this
    // binary (see `trellis/src/metrics.rs`'s module doc comment).
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    metrics::record_transform_latency("metric_rollup_test_real_target", Duration::from_millis(250));
    metrics::increment_changes_applied("metric_rollup_test_real_target");

    let samples = metrics::snapshot();
    rollup::write_snapshot(&client, &samples, SystemTime::now())
        .await
        .expect("write_snapshot");

    // This is the only test in this file (and, since `trellis::metrics`'s
    // registry is one process-wide global — see `trellis/src/metrics.rs`'s
    // module doc comment — in this test *binary*, since each `tests/*.rs`
    // file compiles to its own process) that records against
    // `metric_rollup_test_real_target`, so there is exactly one row here.
    let hist_rows = fetch_rows(&client, "trellis_transform_latency_seconds").await;
    assert_eq!(hist_rows.len(), 1, "exactly one histogram row expected");
    let hist = &hist_rows[0];
    assert_eq!(hist.metric_kind, "histogram");
    assert_eq!(
        label(&client, "trellis_transform_latency_seconds", "transform").await,
        Some("metric_rollup_test_real_target".to_string())
    );
    assert!(
        hist.bucket_bounds.is_some(),
        "histogram row must carry raw buckets"
    );
    assert_eq!(hist.histogram_count, Some(1), "one recorded observation");

    let counter_rows = fetch_rows(&client, "trellis_changes_applied_total").await;
    assert_eq!(counter_rows.len(), 1);
    assert_eq!(counter_rows[0].value, Some(1.0));
}

#[tokio::test]
async fn prune_deletes_only_rows_older_than_retention() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let retention = Duration::from_secs(60);
    let now = SystemTime::now();
    let old_rolled_up_at = now - Duration::from_secs(120); // outside retention
    let recent_rolled_up_at = now - Duration::from_secs(10); // inside retention

    rollup::write_snapshot(&client, &synthetic_samples("old"), old_rolled_up_at)
        .await
        .expect("write old snapshot");
    rollup::write_snapshot(&client, &synthetic_samples("recent"), recent_rolled_up_at)
        .await
        .expect("write recent snapshot");

    assert_eq!(
        row_count(&client).await,
        6,
        "3 samples x 2 snapshots before pruning"
    );

    let deleted = rollup::prune(&client, now, retention).await.expect("prune");
    assert_eq!(
        deleted, 3,
        "only the old snapshot's 3 rows are outside retention"
    );

    assert_eq!(
        fetch_rows(&client, "rollup_test_counter_old").await.len(),
        0,
        "old snapshot rows must be pruned"
    );
    assert_eq!(
        fetch_rows(&client, "rollup_test_counter_recent")
            .await
            .len(),
        1,
        "recent snapshot rows must survive pruning"
    );
    assert_eq!(
        row_count(&client).await,
        3,
        "only the recent snapshot remains"
    );
}

#[tokio::test]
async fn prune_is_a_no_op_when_nothing_is_older_than_retention() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let now = SystemTime::now();
    rollup::write_snapshot(&client, &synthetic_samples("fresh"), now)
        .await
        .expect("write snapshot");

    let deleted = rollup::prune(&client, now, Duration::from_secs(3600))
        .await
        .expect("prune");
    assert_eq!(deleted, 0);
    assert_eq!(row_count(&client).await, 3, "nothing pruned");
}
