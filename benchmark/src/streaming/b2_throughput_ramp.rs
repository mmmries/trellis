//! B2 — 1-1 throughput ramp (issue #266): single hop, ramp offered load
//! until the pipeline can't keep up, report the saturation rate plus e2e
//! latency at 25/50/80/95% of it. This is the T2 measurement (100k rows/sec
//! sustained, single-hop plain 1-1) and gives the load levels B3/B4/B5/B7/B8
//! should be run at.
//!
//! **What "sustained" means here is deliberately weaker than T2's own
//! definition.** T2 (issue #266) requires "held for >= 10 minutes with ring
//! depth and replication lag both flat." A ramp searching several candidate
//! rates at 10 minutes each is a multi-hour scenario before it's found
//! anything — impractical for the exploratory search this module does. Each
//! probe here instead offers load for a short, configurable window (default
//! 20s) and then checks whether the backlog it created (`rows offered -
//! rows applied`) fully drains within a bounded grace period. That is a
//! reasonable proxy — a rate whose backlog doesn't even drain in a
//! bounded grace period after a short offered window is certainly not
//! sustainable for 10 minutes — but it is not a substitute for a real T2
//! confirmation run: once this ramp identifies a candidate saturation rate,
//! confirming it holds for T2's actual 10-minute window is a separate,
//! longer run this module doesn't do automatically.
//!
//! Each probe offers its target rate through [`run_controlled_load`] (paced,
//! self-throttling), not the max-rate generator — a ramp needs to test
//! *specific* rates, not "as fast as this connection can go." At high
//! enough target rates the single generator connection can itself become
//! the bottleneck before the engine does (an `INSERT` taking longer than
//! one tick period), in which case `achieved_rows_per_sec` undershoots
//! `target_rows_per_sec` and a "sustained" verdict at that rate says more
//! about the generator than the engine — always read the two side by side,
//! never `target_rows_per_sec` alone.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Client as TrellisClient, ClientOptions};

use super::chain::{create_chain_source_table, install_chain_hops, wait_for_chain_live, warm_up};
use super::load::{LoadConfig, run_controlled_load};
use super::metrics_scrape::{HistogramSnapshot, T1_BOUNDS, counter_value};

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

/// One rate's full probe result.
#[derive(Debug)]
pub struct ThroughputProbe {
    pub target_rows_per_sec: f64,
    pub rows_per_commit: usize,
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
             \"offered_duration_secs\":{},\"rows_issued\":{},\"achieved_rows_per_sec\":{:.1},\
             \"changes_applied\":{},\"backlog_after_grace\":{},\"sustained\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4}}}",
            scenario,
            self.target_rows_per_sec,
            self.rows_per_commit,
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

/// A fixed generator cadence (issue #266's B4 axis holds transaction shape
/// constant while sweeping rate; this ramp holds it at a mid-sized batch —
/// neither B4's 1-row nor 10k-row extreme — and varies `rows_per_commit`
/// to hit the target rate instead, so the ramp isn't itself confounded by
/// a changing commit rate).
const PROBE_COMMITS_PER_SEC: f64 = 50.0;

/// Runs one throughput probe at `target_rows_per_sec` against a fresh,
/// isolated single-hop chain: offers load for `offered_duration`, then waits
/// up to `grace` for the backlog it created to drain, and reports whether it
/// did (`sustained`) plus the e2e latency bucket fractions observed over the
/// whole window.
pub async fn run_probe(
    target_rows_per_sec: f64,
    offered_duration: Duration,
    grace: Duration,
) -> ThroughputProbe {
    let rows_per_commit = ((target_rows_per_sec / PROBE_COMMITS_PER_SEC).round() as usize).max(1);

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let prefix = "b2";
    let source = create_chain_source_table(&raw, prefix).await;

    let options = ClientOptions {
        staging_worker: true,
        // More drain parallelism than B1's latency ladder: B2 is measuring
        // throughput, where H4 (issue #266) expects `SEG_BUCKETS = 8` worth
        // of useful claim parallelism per batch.
        application_threads: 8,
        source_tables: vec![format!("public.{source}")],
        // See b1_hop_ladder's identical field for why this must stay long:
        // salesforce-misc/trellis#267 (duplicate src_table staging) stalls
        // any live chain once an intermediate hop joins the publication.
        // B2 is single-hop, so it isn't hit by #267 either way (the one hop
        // is terminal — nothing downstream reads it, so it never gets the
        // automatic-propagation Recompute #267 is about) — kept long anyway
        // for consistency and because this scenario never needs a table
        // added mid-run.
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
            commits_per_sec: PROBE_COMMITS_PER_SEC,
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
        window.bucket(super::metrics_scrape::LE_P50) as f64 / window.count as f64
    } else {
        0.0
    };
    let p99_frac = if window.count > 0 {
        window.bucket(super::metrics_scrape::LE_P99) as f64 / window.count as f64
    } else {
        0.0
    };

    ThroughputProbe {
        target_rows_per_sec,
        rows_per_commit,
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

/// Ramps through `candidate_rates` (ascending) until a probe fails to drain
/// its backlog within `grace`, or the list is exhausted. Returns every probe
/// run, in order — the caller reads the last `sustained: true` entry (if
/// any) as the candidate saturation rate.
pub async fn run_ramp(
    candidate_rates: &[f64],
    offered_duration: Duration,
    grace: Duration,
) -> Vec<ThroughputProbe> {
    let mut probes = Vec::new();
    for &rate in candidate_rates {
        let probe = run_probe(rate, offered_duration, grace).await;
        let sustained = probe.sustained;
        probes.push(probe);
        if !sustained {
            break;
        }
    }
    probes
}
