//! Aggregate ingestion ceiling (issue #317): the highest rate of **new source
//! rows** an aggregate transform folds in *while they arrive*, held at steady
//! state, at a fixed group count.
//!
//! This answers a different question from its neighbours:
//!
//! - [`super::intake_ceiling`] has no transform at all. It bounds this number
//!   from above (every row an aggregate folds was first decoded and appended
//!   by intake) but says nothing about claim, fold and apply.
//! - [`super::fold_in`] offers one fixed target rate (T3's 400k) and derives
//!   the group count from it, so it gives a yes/no at that rate. Far above the
//!   ceiling, the generator's own writes compete with the engine for the same
//!   Postgres, so the rate it folds at under that overload is depressed and
//!   is not the ceiling either.
//! - Issue #277 characterizes *low* group counts, where drain workers contend
//!   on hot target rows. This scenario holds the group count fixed and
//!   searches over the rate, so the two can be read side by side.
//!
//! ## Method
//!
//! Each probe is a fresh cluster with one `SUM`/`COUNT` aggregate over
//! `groups` keys, offered a paced, insert-only load at `rate` rows/sec from the
//! multi-connection generator ([`run_parallel_load`]) for `window`. While the
//! load runs, a sampler reads, every [`SAMPLE_INTERVAL`]:
//!
//! - rows folded so far (`sum(row_count)` over the target — every source row
//!   lands in exactly one group's `COUNT(*)`),
//! - intake lag (`pg_current_wal_lsn()` minus `replication_progress.confirmed_lsn`,
//!   in WAL bytes): source WAL that intake has not yet staged into the ring,
//! - ring rows (staged, not yet applied and recycled).
//!
//! A probe **kept pace** when the generator delivered the rate (otherwise it is
//! [`generator_bound`] and says nothing) and the least-squares slope of rows
//! folded, fitted after the first [`SETTLE_FRACTION`] of the window, is at
//! least `1 - GENERATOR_UNDERSHOOT_TOLERANCE` of the rate. A pipeline that
//! keeps up tracks the offered rate at a constant lag, so its slope equals the
//! offered rate. One that falls behind folds at its own, lower rate. The lag
//! and ring slopes say *where* the backlog piled up when it did: in front of
//! intake (lag grows) or between intake and apply (ring grows until
//! `RING_SIZE` back-pressures intake, after which lag grows too).
//!
//! After the window the probe waits up to `grace` for the target to fold in
//! every row, then runs the aggregate oracle. A run that never drains reports
//! `oracle_ok: null` (unevaluated), not `false`.
//!
//! The ceiling search probes `max_rate`, then `min_rate`, then bisects between
//! the highest rate that kept pace and the lowest that didn't, until the two
//! are within `resolution` of each other. The reported ceiling is the highest
//! rate that kept pace. The bracket above it is part of the answer.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::config::DEFAULT_SCHEMA;

use crate::scenario::connect_raw;
use crate::streaming::chain::{numeric_columns, wait_for_live, warm_up_aggregate};
use crate::streaming::load::{
    GENERATOR_UNDERSHOOT_TOLERANCE, Pace, ParallelLoad, generator_bound, run_parallel_load,
};
use crate::streaming::scrape::{
    END_TO_END_LATENCY_METRIC, HistogramSnapshot, LE_MAX, LE_P50, LE_P99, T1_BOUNDS, scrape,
};
use crate::streaming::tuning::EngineTuning;

const SOURCE_TABLE: &str = "agg_ceiling_src";
const GROUP_COLUMN: &str = "grp";
const SETUP_TIMEOUT: Duration = Duration::from_secs(60);

/// How much of the window to skip before fitting the fold rate: the first
/// seal, claim and apply, plus the ramp of a lag that then stays constant.
pub const SETTLE_FRACTION: f64 = 1.0 / 3.0;
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// One probe's configuration.
#[derive(Debug, Clone, Copy)]
pub struct ProbeConfig {
    pub rate: f64,
    pub groups: usize,
    pub rows_per_commit: usize,
    pub connections: usize,
    pub window: Duration,
    pub grace: Duration,
}

/// One sample taken while load is arriving.
#[derive(Debug, Clone, Copy)]
struct Sample {
    /// Seconds since the window opened.
    at: f64,
    folded: f64,
    intake_lag_bytes: f64,
    ring_rows: f64,
}

#[derive(Debug)]
pub struct ProbeResult {
    pub cfg: ProbeConfig,
    pub application_threads: usize,
    pub rows_issued: u64,
    pub achieved_rows_per_sec: f64,
    pub generator_bound: bool,
    /// Slope of rows folded over the settled part of the window.
    pub folded_rows_per_sec: Option<f64>,
    /// Slope of intake lag (WAL bytes/sec) over the same span. Near zero
    /// when intake kept up.
    pub intake_lag_growth_bytes_per_sec: Option<f64>,
    /// Slope of rows sitting in the ring over the same span.
    pub ring_growth_rows_per_sec: Option<f64>,
    pub intake_lag_bytes_at_window_end: i64,
    pub ring_rows_at_window_end: i64,
    /// Rows offered but not yet folded when the window closed.
    pub backlog_at_window_end: i64,
    pub kept_pace: bool,
    /// Seconds after the window closed until every row was folded, or `None`
    /// if that didn't happen within the grace period.
    pub drained_after_window_secs: Option<f64>,
    pub e2e_count: u64,
    pub e2e_p50_bucket_frac: f64,
    pub e2e_p99_bucket_frac: f64,
    pub e2e_max_bucket_frac: f64,
    pub oracle_ok: Option<bool>,
    pub oracle_mismatched_groups: i64,
}

fn json_opt(v: Option<f64>) -> String {
    v.map_or_else(|| "null".to_string(), |v| format!("{v:.1}"))
}

impl ProbeResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"aggregate-ceiling\",\"rate\":{},\"groups\":{},\
             \"rows_per_commit\":{},\"connections\":{},\"application_threads\":{},\
             \"window_secs\":{},\"rows_issued\":{},\"achieved_rows_per_sec\":{:.1},\
             \"generator_bound\":{},\"folded_rows_per_sec\":{},\
             \"intake_lag_growth_bytes_per_sec\":{},\"ring_growth_rows_per_sec\":{},\
             \"intake_lag_bytes_at_window_end\":{},\"ring_rows_at_window_end\":{},\
             \"backlog_at_window_end\":{},\"kept_pace\":{},\"drained_after_window_secs\":{},\
             \"e2e_count\":{},\"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4},\
             \"e2e_max_bucket_frac\":{:.4},\"oracle_ok\":{},\"oracle_mismatched_groups\":{}}}",
            self.cfg.rate,
            self.cfg.groups,
            self.cfg.rows_per_commit,
            self.cfg.connections,
            self.application_threads,
            self.cfg.window.as_secs_f64(),
            self.rows_issued,
            self.achieved_rows_per_sec,
            self.generator_bound,
            json_opt(self.folded_rows_per_sec),
            json_opt(self.intake_lag_growth_bytes_per_sec),
            json_opt(self.ring_growth_rows_per_sec),
            self.intake_lag_bytes_at_window_end,
            self.ring_rows_at_window_end,
            self.backlog_at_window_end,
            self.kept_pace,
            json_opt(self.drained_after_window_secs),
            self.e2e_count,
            self.e2e_p50_bucket_frac,
            self.e2e_p99_bucket_frac,
            self.e2e_max_bucket_frac,
            self.oracle_ok
                .map_or_else(|| "null".to_string(), |ok| ok.to_string()),
            self.oracle_mismatched_groups,
        )
    }
}

/// Least-squares slope of `y` over `x`, or `None` with fewer than two
/// distinct `x`s.
fn slope(points: impl Iterator<Item = (f64, f64)>) -> Option<f64> {
    let points: Vec<_> = points.collect();
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let (mut sxy, mut sxx) = (0.0, 0.0);
    for (x, y) in &points {
        sxy += (x - mean_x) * (y - mean_y);
        sxx += (x - mean_x) * (x - mean_x);
    }
    (sxx > 0.0).then(|| sxy / sxx)
}

/// The probe's verdict: the generator delivered, and the target folded at the
/// offered rate (within tolerance) while it was offered.
fn kept_pace(generator_bound: bool, rate: f64, folded_rows_per_sec: Option<f64>) -> bool {
    !generator_bound
        && folded_rows_per_sec.is_some_and(|f| f >= rate * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE))
}

async fn folded_rows(raw: &RawClient, terminal: &str) -> i64 {
    let folded: Option<i64> = raw
        .query_one(
            &format!("select sum(row_count)::bigint from public.{terminal}"),
            &[],
        )
        .await
        .expect("sum folded row_count")
        .get(0);
    folded.unwrap_or(0)
}

/// Source WAL not yet staged by intake, in bytes.
async fn intake_lag_bytes(raw: &RawClient) -> i64 {
    raw.query_one(
        "select coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_lsn)), 0)::bigint \
         from replication_progress",
        &[],
    )
    .await
    .expect("read intake lag")
    .get(0)
}

/// The statement that counts every ring table's rows in one snapshot. Built
/// once per probe: the ring's table set is fixed after migration.
async fn ring_rows_sql(raw: &RawClient) -> String {
    let tables: Vec<String> = raw
        .query(
            "select tablename from pg_tables where schemaname = $1 and tablename ~ '^seg_[0-9]+$'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("list ring tables")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let sum = tables
        .iter()
        .map(|t| format!("(select count(*) from {DEFAULT_SCHEMA}.{t})"))
        .collect::<Vec<_>>()
        .join(" + ");
    format!("select ({sum})::bigint")
}

async fn sample_window(
    raw: &RawClient,
    terminal: &str,
    ring_sql: &str,
    start: Instant,
    window: Duration,
) -> Vec<Sample> {
    let end = start + window;
    let mut samples = Vec::new();
    while Instant::now() < end {
        let at = start.elapsed().as_secs_f64();
        let folded = folded_rows(raw, terminal).await as f64;
        let intake_lag_bytes = intake_lag_bytes(raw).await as f64;
        let ring_rows: i64 = raw
            .query_one(ring_sql, &[])
            .await
            .expect("count ring rows")
            .get(0);
        samples.push(Sample {
            at,
            folded,
            intake_lag_bytes,
            ring_rows: ring_rows as f64,
        });
        tokio::time::sleep(SAMPLE_INTERVAL).await;
    }
    samples
}

async fn check_oracle(
    raw: &RawClient,
    terminal: &str,
    def: &trellis::dev::defs::ast::TransformDef,
) -> i64 {
    let oracle_sql = trellis::dev::defs::oracle::render_aggregate_select_sql(def);
    raw.query_one(
        &format!(
            "select count(*) from ({oracle_sql}) o \
             full outer join public.{terminal} t on t.{GROUP_COLUMN} = o.{GROUP_COLUMN} \
             where t.{GROUP_COLUMN} is null or o.{GROUP_COLUMN} is null \
                or t.val is distinct from o.val \
                or t.row_count is distinct from o.row_count"
        ),
        &[],
    )
    .await
    .expect("compare aggregate target against oracle")
    .get(0)
}

/// Runs one probe against a fresh cluster.
pub async fn run_probe(cfg: ProbeConfig, tuning: &EngineTuning) -> ProbeResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(&format!(
        "create table public.{SOURCE_TABLE} \
             (id bigint primary key, {GROUP_COLUMN} bigint not null, val numeric)"
    ))
    .await
    .expect("create aggregate source table");

    let client = trellis::Client::start(
        db.dsn(),
        tuning.client_options(vec![format!("public.{SOURCE_TABLE}")]),
    )
    .expect("client start");

    let columns: HashMap<_, _> = numeric_columns(&["id", GROUP_COLUMN, "val"]);
    let source_text = format!(
        "TRANSFORM agg_ceiling FROM public.{SOURCE_TABLE} GROUP BY {GROUP_COLUMN} \
         SELECT {GROUP_COLUMN} AS {GROUP_COLUMN}, SUM(val) AS val, COUNT(*) AS row_count"
    );
    let def = trellis::dev::defs::install_definition(&db.pool, &source_text, &columns, "public")
        .await
        .expect("install aggregate definition");
    let terminal = def.def.target.clone();
    wait_for_live(&raw, &terminal, Instant::now() + SETUP_TIMEOUT).await;
    warm_up_aggregate(&raw, SOURCE_TABLE, &terminal, GROUP_COLUMN, SETUP_TIMEOUT).await;
    let ring_sql = ring_rows_sql(&raw).await;

    let e2e_baseline =
        HistogramSnapshot::capture(&scrape(), END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS);

    let load_cfg = ParallelLoad {
        connections: cfg.connections,
        rows_per_commit: cfg.rows_per_commit,
        duration: cfg.window,
        groups: Some(cfg.groups),
        pace: Pace::RowsPerSec(cfg.rate),
    };
    let start = Instant::now();
    let (load, samples) = tokio::join!(
        run_parallel_load(db.dsn(), SOURCE_TABLE, 1, &load_cfg),
        sample_window(&raw, &terminal, &ring_sql, start, cfg.window),
    );

    let folded_at_end = folded_rows(&raw, &terminal).await - 1; // minus the warm-up row
    let lag_at_end = intake_lag_bytes(&raw).await;
    let ring_at_end: i64 = raw
        .query_one(&ring_sql, &[])
        .await
        .expect("count ring rows")
        .get(0);

    let settle = cfg.window.as_secs_f64() * SETTLE_FRACTION;
    let settled = || samples.iter().filter(move |s| s.at >= settle);
    let folded_rows_per_sec = slope(settled().map(|s| (s.at, s.folded)));
    let lag_growth = slope(settled().map(|s| (s.at, s.intake_lag_bytes)));
    let ring_growth = slope(settled().map(|s| (s.at, s.ring_rows)));

    let expected = load.rows_issued as i64 + 1; // + the warm-up row
    let window_closed = Instant::now();
    let grace_deadline = window_closed + cfg.grace;
    let mut drained_after = None;
    while Instant::now() < grace_deadline {
        if folded_rows(&raw, &terminal).await >= expected {
            drained_after = Some(window_closed.elapsed().as_secs_f64());
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let window =
        HistogramSnapshot::capture(&scrape(), END_TO_END_LATENCY_METRIC, &terminal, &T1_BOUNDS)
            .since(&e2e_baseline);
    let (oracle_ok, oracle_mismatched_groups) = if drained_after.is_some() {
        let mismatched = check_oracle(&raw, &terminal, &def.def).await;
        (Some(mismatched == 0), mismatched)
    } else {
        (None, 0)
    };

    client.shutdown().await.expect("client shutdown");

    let achieved = load.achieved_rows_per_sec();
    // "Engine kept up" for the generator self-check is the in-window verdict,
    // not the eventual drain: a generator that undershot while the engine
    // folded everything it did get measured only itself.
    let engine_kept_up =
        folded_rows_per_sec.is_some_and(|f| f >= achieved * (1.0 - GENERATOR_UNDERSHOOT_TOLERANCE));
    let gen_bound = generator_bound(Some(cfg.rate), achieved, engine_kept_up);
    ProbeResult {
        cfg,
        application_threads: tuning.application_threads,
        rows_issued: load.rows_issued,
        achieved_rows_per_sec: achieved,
        generator_bound: gen_bound,
        folded_rows_per_sec,
        intake_lag_growth_bytes_per_sec: lag_growth,
        ring_growth_rows_per_sec: ring_growth,
        intake_lag_bytes_at_window_end: lag_at_end,
        ring_rows_at_window_end: ring_at_end,
        backlog_at_window_end: load.rows_issued as i64 - folded_at_end,
        kept_pace: kept_pace(gen_bound, cfg.rate, folded_rows_per_sec),
        drained_after_window_secs: drained_after,
        e2e_count: window.count,
        e2e_p50_bucket_frac: window.fraction(LE_P50),
        e2e_p99_bucket_frac: window.fraction(LE_P99),
        e2e_max_bucket_frac: window.fraction(LE_MAX),
        oracle_ok,
        oracle_mismatched_groups,
    }
}

/// The search's bracket and every probe it ran.
#[derive(Debug)]
pub struct CeilingSearch {
    /// Highest probed rate that kept pace, if any did.
    pub ceiling: Option<f64>,
    /// Lowest probed rate that didn't, if any didn't.
    pub first_failing: Option<f64>,
    pub probes: Vec<ProbeResult>,
}

/// Next rate to probe inside `(lo, hi)`, or `None` once the bracket is within
/// `resolution` (relative to `hi`).
fn next_rate(lo: f64, hi: f64, resolution: f64) -> Option<f64> {
    ((hi - lo) / hi > resolution).then(|| ((lo + hi) / 2.0 / 1000.0).round() * 1000.0)
}

/// Bisects for the ceiling between `min_rate` and `max_rate`. A
/// generator-bound probe aborts the search: it measured the generator, so
/// nothing about the engine can be concluded from it (raise
/// `--connections`). Prints each probe's JSON line as it completes, so a long
/// search is inspectable while it runs.
pub async fn search(
    template: ProbeConfig,
    min_rate: f64,
    max_rate: f64,
    resolution: f64,
    tuning: &EngineTuning,
) -> CeilingSearch {
    let mut probes = Vec::new();
    let run = |rate: f64| {
        let cfg = ProbeConfig { rate, ..template };
        async move { run_probe(cfg, tuning).await }
    };

    let top = run(max_rate).await;
    println!("{}", top.to_json());
    let top_kept = top.kept_pace;
    let top_bound = top.generator_bound;
    probes.push(top);
    if top_bound {
        return CeilingSearch {
            ceiling: None,
            first_failing: None,
            probes,
        };
    }
    if top_kept {
        return CeilingSearch {
            ceiling: Some(max_rate),
            first_failing: None,
            probes,
        };
    }

    let bottom = run(min_rate).await;
    println!("{}", bottom.to_json());
    let bottom_kept = bottom.kept_pace;
    let bottom_bound = bottom.generator_bound;
    probes.push(bottom);
    if bottom_bound || !bottom_kept {
        return CeilingSearch {
            ceiling: None,
            first_failing: (!bottom_bound).then_some(min_rate),
            probes,
        };
    }

    let (mut lo, mut hi) = (min_rate, max_rate);
    while let Some(rate) = next_rate(lo, hi, resolution) {
        let probe = run(rate).await;
        println!("{}", probe.to_json());
        let (kept, bound) = (probe.kept_pace, probe.generator_bound);
        probes.push(probe);
        if bound {
            break;
        }
        if kept {
            lo = rate;
        } else {
            hi = rate;
        }
    }
    CeilingSearch {
        ceiling: Some(lo),
        first_failing: Some(hi),
        probes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slope_recovers_a_constant_rate_behind_a_constant_lag() {
        // Folding 90k rows/sec in 300ms steps, half a second behind.
        let points = (0..80).map(|i| {
            let t = 10.0 + i as f64 * 0.25;
            let visible = ((t - 0.5) / 0.3).floor() * 0.3;
            (t, 90_000.0 * visible)
        });
        let s = slope(points).expect("enough points");
        assert!((s - 90_000.0).abs() < 90_000.0 * 0.01, "slope {s}");
    }

    #[test]
    fn kept_pace_needs_the_generator_and_the_fold_rate() {
        assert!(kept_pace(false, 100_000.0, Some(98_500.0)));
        assert!(!kept_pace(false, 100_000.0, Some(97_000.0)));
        assert!(!kept_pace(true, 100_000.0, Some(100_000.0)));
        assert!(!kept_pace(false, 100_000.0, None));
    }

    #[test]
    fn bisection_stops_at_the_resolution() {
        assert_eq!(next_rate(50_000.0, 200_000.0, 0.05), Some(125_000.0));
        assert_eq!(next_rate(96_000.0, 100_000.0, 0.05), None);
        assert_eq!(next_rate(94_000.0, 100_000.0, 0.05), Some(97_000.0));
    }
}
