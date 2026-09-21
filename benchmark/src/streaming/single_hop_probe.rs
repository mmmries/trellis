//! Shared single-hop streaming probe (issue #266): the common scaffolding
//! B2 (throughput ramp) and B4 (transaction-shape sensitivity) both need —
//! a fresh isolated single-hop 1-1 chain, a real `Client`, a controlled-rate
//! offered load, a bounded drain-grace wait, and a [`ThroughputProbe`]
//! result. B2 fixes the commit cadence and derives `rows_per_commit` from
//! the target rate; B4 fixes `rows_per_commit` (the axis it's sweeping) and
//! derives the commit cadence instead — both go through [`run_probe`] here.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Client as TrellisClient, ClientOptions};

use super::chain::{create_chain_source_table, install_chain_hops, wait_for_chain_live, warm_up};
use super::load::{LoadConfig, run_controlled_load};
use super::metrics_scrape::{HistogramSnapshot, LE_P50, LE_P99, T1_BOUNDS, counter_value};

async fn connect_raw(dsn: &str) -> RawClient {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; \
             set bytea_output to 'hex'"
        ))
        .await
        .expect("set search_path");
    client
}

/// One probe's full result — a target rate delivered with a specific
/// `rows_per_commit` transaction shape.
#[derive(Debug)]
pub struct ThroughputProbe {
    pub target_rows_per_sec: f64,
    pub rows_per_commit: usize,
    pub commits_per_sec: f64,
    pub offered_duration_secs: f64,
    pub rows_issued: u64,
    pub achieved_rows_per_sec: f64,
    pub changes_applied: u64,
    pub backlog_after_grace: i64,
    pub sustained: bool,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
}

impl ThroughputProbe {
    pub fn to_json(&self, scenario: &str) -> String {
        format!(
            "{{\"scenario\":\"{}\",\"target_rows_per_sec\":{},\"rows_per_commit\":{},\
             \"commits_per_sec\":{:.2},\
             \"offered_duration_secs\":{},\"rows_issued\":{},\"achieved_rows_per_sec\":{:.1},\
             \"changes_applied\":{},\"backlog_after_grace\":{},\"sustained\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4}}}",
            scenario,
            self.target_rows_per_sec,
            self.rows_per_commit,
            self.commits_per_sec,
            self.offered_duration_secs,
            self.rows_issued,
            self.achieved_rows_per_sec,
            self.changes_applied,
            self.backlog_after_grace,
            self.sustained,
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
        )
    }
}

/// Runs one probe against a fresh, isolated single-hop chain: offers
/// `rows_per_commit`-shaped commits at `commits_per_sec` (so
/// `commits_per_sec * rows_per_commit` is the target rate) for
/// `offered_duration`, then waits up to `grace` for the backlog it created
/// to drain, and reports whether it did (`sustained`) plus the e2e latency
/// bucket fractions observed over the whole window.
pub async fn run_probe(
    rows_per_commit: usize,
    commits_per_sec: f64,
    application_threads: usize,
    offered_duration: Duration,
    grace: Duration,
) -> ThroughputProbe {
    let target_rows_per_sec = commits_per_sec * rows_per_commit as f64;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let prefix = "shp";
    let source = create_chain_source_table(&raw, prefix).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads,
        source_tables: vec![format!("public.{source}")],
        // See b1_hop_ladder's identical field for why this must stay long:
        // salesforce-misc/trellis#267 (duplicate src_table staging) stalls
        // any live chain once an intermediate hop joins the publication.
        // This probe is single-hop, so it isn't hit by #267 either way (the
        // one hop is terminal — nothing downstream reads it) — kept long
        // anyway for consistency and because this scenario never needs a
        // table added mid-run.
        reconcile_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let chain = install_chain_hops(&db.pool, &source, 1).await;
    wait_for_chain_live(&raw, &chain, Duration::from_secs(30)).await;
    warm_up(&raw, &chain, Duration::from_secs(30)).await;

    let terminal = chain.terminal();
    let rendered_before = trellis::metrics::Metrics::new().render_prometheus();
    let baseline = HistogramSnapshot::capture(
        &rendered_before,
        "trellis_end_to_end_latency_seconds",
        terminal,
        &T1_BOUNDS,
    );
    let changes_before = counter_value(&rendered_before, "trellis_changes_applied_total", terminal);

    let load = run_controlled_load(
        &raw,
        &chain.source,
        1,
        &LoadConfig {
            commits_per_sec,
            rows_per_commit,
            duration: offered_duration,
        },
    )
    .await;

    let grace_deadline = Instant::now() + grace;
    let changes_now = loop {
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        let changes_now = counter_value(&rendered, "trellis_changes_applied_total", terminal);
        if changes_now.saturating_sub(changes_before) >= load.rows_issued
            || Instant::now() >= grace_deadline
        {
            break changes_now;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let rendered_after = trellis::metrics::Metrics::new().render_prometheus();
    let after = HistogramSnapshot::capture(
        &rendered_after,
        "trellis_end_to_end_latency_seconds",
        terminal,
        &T1_BOUNDS,
    );
    let window = after.since(&baseline);
    let applied = changes_now.saturating_sub(changes_before);
    let backlog = load.rows_issued as i64 - applied as i64;

    client.shutdown().await.expect("client shutdown");

    let p50_frac = if window.count > 0 {
        window.bucket(LE_P50) as f64 / window.count as f64
    } else {
        0.0
    };
    let p99_frac = if window.count > 0 {
        window.bucket(LE_P99) as f64 / window.count as f64
    } else {
        0.0
    };

    ThroughputProbe {
        target_rows_per_sec,
        rows_per_commit,
        commits_per_sec,
        offered_duration_secs: offered_duration.as_secs_f64(),
        rows_issued: load.rows_issued,
        achieved_rows_per_sec: load.rows_issued as f64 / load.elapsed.as_secs_f64(),
        changes_applied: applied,
        backlog_after_grace: backlog,
        sustained: backlog <= 0,
        e2e_count: window.count,
        e2e_p50_bucket_frac: p50_frac,
        e2e_p99_bucket_frac: p99_frac,
    }
}
