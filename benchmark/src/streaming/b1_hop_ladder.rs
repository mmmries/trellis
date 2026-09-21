//! B1 — hop-depth latency ladder (issue #266): depths 1/2/3/5/10 of plain
//! 1-1 transforms at a low offered rate, reporting the T1 boundary-fraction
//! evaluation per depth. This is both the T1 measurement itself and the
//! direct test of H1 — whether the ~300ms default `maintenance_interval`
//! seal cadence (the only place a sealed batch is ever created;
//! `client.rs`'s `maintenance_loop`) puts a hard floor under per-hop
//! latency roughly `10x` T1's ~50ms-per-hop budget.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Client as TrellisClient, ClientOptions};

use super::chain::{create_chain_source_table, install_chain_hops, wait_for_chain_live, warm_up};
use super::load::{LoadConfig, run_controlled_load};
use super::metrics_scrape::{HistogramSnapshot, T1_BOUNDS, T1Evaluation, counter_value};

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `benchmark/src/scenario.rs`'s `connect_raw` and `trellis`'s own
/// integration tests.
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

/// One depth's full measurement. Hand-rolled `to_json` rather than
/// `serde_json`, matching `benchmark/src/scenario.rs::BenchResult`'s own
/// convention (every field here is an int/float/bool with no
/// escaping-sensitive content).
#[derive(Debug)]
pub struct HopLatencyResult {
    pub depth: usize,
    pub offered_commits_per_sec: f64,
    pub duration_secs: f64,
    pub commits_issued: u64,
    pub rows_issued: u64,
    pub actual_elapsed_secs: f64,
    pub changes_applied_terminal: u64,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_under_1s: bool,
    pub t1_p50_pass: bool,
    pub t1_p99_pass: bool,
    pub t1_all_pass: bool,
}

impl HopLatencyResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"hop-ladder\",\"depth\":{},\"offered_commits_per_sec\":{},\
             \"duration_secs\":{},\"commits_issued\":{},\"rows_issued\":{},\
             \"actual_elapsed_secs\":{:.3},\"changes_applied_terminal\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4},\
             \"e2e_max_under_1s\":{},\"t1_p50_pass\":{},\"t1_p99_pass\":{},\"t1_all_pass\":{}}}",
            self.depth,
            self.offered_commits_per_sec,
            self.duration_secs,
            self.commits_issued,
            self.rows_issued,
            self.actual_elapsed_secs,
            self.changes_applied_terminal,
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
            self.e2e_max_under_1s,
            self.t1_p50_pass,
            self.t1_p99_pass,
            self.t1_all_pass,
        )
    }
}

/// Runs one depth's measurement against a fresh, ephemeral cluster: builds a
/// `depth`-hop 1-1 chain, starts a real streaming `Client`
/// (`staging_worker: true`), warms the pipeline up, then offers
/// `commits_per_sec` for `duration` and evaluates T1 against the terminal
/// hop's `trellis_end_to_end_latency_seconds` histogram — diffed against a
/// pre-load baseline scrape (see `metrics_scrape`'s doc comment on why).
pub async fn run_depth(depth: usize, commits_per_sec: f64, duration: Duration) -> HopLatencyResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let prefix = format!("d{depth}");
    let source = create_chain_source_table(&raw, &prefix).await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 4,
        source_tables: vec![format!("public.{source}")],
        // Deliberately long, not short: a live upstream trellis bug
        // (salesforce-misc/trellis#267) means an intermediate
        // hop table that gets added to the CDC publication by the periodic
        // `reconcile_source_tables` pass races the *same* write's own
        // automatic downstream-propagation `Recompute` (staged in-transaction
        // by `apply.rs`, under the transform's *bare* target name) against
        // an independently CDC-decoded copy of it (staged under the
        // *fully-qualified* name) — two differently-spelled `src_table`
        // entries for one logical change that fold never coalesces,
        // producing a duplicate key in one batched apply
        // (`ON CONFLICT DO UPDATE command cannot affect row a second time`)
        // that then retries forever, permanently stalling the segment.
        // Every hop beyond the first is exactly this shape (a table that is
        // both a propagation target and a downstream transform's source), so
        // this harness sidesteps it entirely rather than working around a
        // product bug in benchmark code: with no periodic reconcile firing
        // during a run, intermediate hops never join the publication, so
        // propagation runs purely over the direct, in-transaction Recompute
        // path — which needs no publication membership at all — and CDC
        // never independently re-observes the same write. The root source
        // table is unaffected (it's already published via `source_tables`
        // at `Client::start` time, before this ever matters).
        reconcile_interval: Duration::from_secs(3600),
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let chain = install_chain_hops(&db.pool, &source, depth).await;
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
            rows_per_commit: 1,
            duration,
        },
    )
    .await;

    // Give the pipeline time to drain the tail of the offered load before
    // scraping: H1's seal cadence means the last handful of commits can
    // still be in flight for up to ~depth * maintenance_interval after the
    // load generator stops, plus normal claim/fold/apply latency on top.
    let drain_grace_deadline = Instant::now() + Duration::from_millis(300 * depth as u64 + 5_000);
    loop {
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        let changes_now = counter_value(&rendered, "trellis_changes_applied_total", terminal);
        if changes_now.saturating_sub(changes_before) >= load.commits_issued {
            break;
        }
        if Instant::now() >= drain_grace_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let rendered_after = trellis::metrics::Metrics::new().render_prometheus();
    let after = HistogramSnapshot::capture(
        &rendered_after,
        "trellis_end_to_end_latency_seconds",
        terminal,
        &T1_BOUNDS,
    );
    let window = after.since(&baseline);
    let changes_after = counter_value(&rendered_after, "trellis_changes_applied_total", terminal);

    let eval = T1Evaluation::evaluate(&window);

    client.shutdown().await.expect("client shutdown");

    HopLatencyResult {
        depth,
        offered_commits_per_sec: commits_per_sec,
        duration_secs: duration.as_secs_f64(),
        commits_issued: load.commits_issued,
        rows_issued: load.rows_issued,
        actual_elapsed_secs: load.elapsed.as_secs_f64(),
        changes_applied_terminal: changes_after.saturating_sub(changes_before),
        e2e_count: eval.count,
        e2e_p50_bucket_frac: eval.p50_frac,
        e2e_p99_bucket_frac: eval.p99_frac,
        e2e_max_under_1s: eval.max_ok,
        t1_p50_pass: eval.p50_pass,
        t1_p99_pass: eval.p99_pass,
        t1_all_pass: eval.all_pass(),
    }
}

/// The full ladder: depths 1/2/3/5/10, per the issue's B1 spec, at
/// `commits_per_sec` (the issue's own example: 10/s, "low offered rate") for
/// `duration` each — each depth against its own fresh `TestCluster`.
pub async fn run_ladder(commits_per_sec: f64, duration: Duration) -> Vec<HopLatencyResult> {
    let mut results = Vec::new();
    for depth in [1usize, 2, 3, 5, 10] {
        results.push(run_depth(depth, commits_per_sec, duration).await);
    }
    results
}
