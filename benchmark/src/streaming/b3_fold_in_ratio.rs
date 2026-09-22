//! B3 — aggregate throughput by fold-in ratio (issue #266/#268, T3): the
//! still-unmeasured target from #266 ("400k rows/sec aggregate, sustained,
//! fold-in ratio >= 10:1"). Unlike B2's 1-1 chain, where every source row
//! becomes exactly one target write, an aggregate transform's target write
//! count is bounded by its *group* count, not its row count — several
//! source rows landing in the same sealed batch for the same group key
//! collapse into one target write at claim-time fold. The hypothesis this
//! tests: throughput should scale *with* fold-in ratio (fewer groups per
//! row means fewer actual target writes per source row), not be flat like
//! a 1-1 chain's.
//!
//! `fold_in_ratio` and a candidate `target_rows_per_sec` together derive
//! `groups = target_rows_per_sec / fold_in_ratio` (rounded, floor 1) — the
//! group-key space size a paced load generator spreads `id % groups` across.
//! At ratio 10:1 offering 400k rows/sec, that's 40,000 groups each taking
//! ~10 rows/sec; at ratio 1000:1, 400 groups each taking ~1,000 rows/sec.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::dev::defs::{ValueType, install_definition};
use trellis::{Client as TrellisClient, ClientOptions};

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

/// Pushes `rows_per_commit`-sized batches at `commits_per_sec`, spreading
/// `grp = id % groups` across the group-key space — the fold-in axis this
/// scenario sweeps. Mirrors `load::run_controlled_load`'s pacing discipline
/// exactly, just with a third (group-key) column `load::insert_batch`
/// doesn't carry.
async fn run_grouped_load(
    raw: &RawClient,
    groups: usize,
    commits_per_sec: f64,
    rows_per_commit: usize,
    duration: Duration,
) -> (u64, u64, Duration) {
    let period = Duration::from_secs_f64(1.0 / commits_per_sec);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let start = Instant::now();
    let deadline = start + duration;
    let mut commits = 0u64;
    let mut next_id: i64 = 1;

    loop {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        let mut values = Vec::with_capacity(rows_per_commit);
        for _ in 0..rows_per_commit {
            let grp = next_id % groups as i64;
            values.push(format!("({next_id},{grp},{next_id})"));
            next_id += 1;
        }
        let sql = format!(
            "insert into public.b3_src (id, grp, val) values {}",
            values.join(",")
        );
        raw.execute(sql.as_str(), &[])
            .await
            .expect("insert grouped load batch");
        commits += 1;
    }

    (
        commits * rows_per_commit as u64,
        commits,
        start.elapsed(),
    )
}

#[derive(Debug)]
pub struct FoldInResult {
    pub fold_in_ratio: usize,
    pub groups: usize,
    pub target_rows_per_sec: f64,
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

impl FoldInResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"fold-in-ratio\",\"fold_in_ratio\":{},\"groups\":{},\
             \"target_rows_per_sec\":{},\"offered_duration_secs\":{},\"rows_issued\":{},\
             \"achieved_rows_per_sec\":{:.1},\"changes_applied\":{},\
             \"backlog_after_grace\":{},\"sustained\":{},\"e2e_count\":{},\
             \"e2e_p50_bucket_frac\":{:.4},\"e2e_p99_bucket_frac\":{:.4}}}",
            self.fold_in_ratio,
            self.groups,
            self.target_rows_per_sec,
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

/// One probe: installs a single `SUM`/`COUNT` aggregate transform over a
/// fresh source table, offers `target_rows_per_sec` for `offered_duration`
/// (paced, 200 rows/commit — a mid-sized shape, not B4's extremes) spread
/// across `groups = target_rows_per_sec / fold_in_ratio` group keys, then
/// waits up to `grace` for the backlog to drain.
///
/// `seal_mode`/`group_commit` (issue #268's X3/X4) default to stock
/// (`trellis::SealMode::Timer`/`None`) when `main.rs`'s `fold-in-ratio` CLI
/// arm doesn't pass `--seal-mode`/`--group-commit` — the combined re-run's
/// own question: does the best surviving combination of engine changes also
/// help (or hurt) T3's still-unmeasured aggregate throughput target, not
/// just T1/T2?
pub async fn run_probe_full(
    fold_in_ratio: usize,
    target_rows_per_sec: f64,
    offered_duration: Duration,
    grace: Duration,
    application_threads: usize,
    seal_mode: trellis::SealMode,
    group_commit: Option<trellis::GroupCommitConfig>,
) -> FoldInResult {
    let groups = ((target_rows_per_sec / fold_in_ratio as f64).round() as usize).max(1);
    const ROWS_PER_COMMIT: usize = 200;
    let commits_per_sec = target_rows_per_sec / ROWS_PER_COMMIT as f64;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table public.b3_src (id bigint primary key, grp bigint not null, val numeric); \
         alter table public.b3_src replica identity full;",
    )
    .await
    .expect("create b3 source table");

    let options = ClientOptions {
        staging_worker: true,
        application_threads,
        source_tables: vec!["public.b3_src".to_string()],
        // Same reasoning as b1_hop_ladder/single_hop_probe: this scenario
        // never adds a table mid-run, so the #267 workaround's only cost
        // (slower publication reconcile for a *new* table) never applies —
        // kept long for consistency with every other streaming scenario.
        reconcile_interval: Duration::from_secs(3600),
        seal_mode,
        group_commit,
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let columns: std::collections::HashMap<String, ValueType> = [
        ("id".to_string(), ValueType::Numeric),
        ("grp".to_string(), ValueType::Numeric),
        ("val".to_string(), ValueType::Numeric),
    ]
    .into_iter()
    .collect();
    let source_text = "TRANSFORM b3_totals FROM b3_src GROUP BY grp \
         SELECT grp AS grp, SUM(val) AS val, COUNT(*) AS row_count";
    let def = install_definition(&db.pool, source_text, &columns, "public")
        .await
        .expect("install b3_totals aggregate definition");
    let terminal = def.def.target.clone();

    // Same "must actually be live and flowing before the timed window
    // starts" discipline chain::wait_for_chain_live/warm_up use.
    let live_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status: Option<String> = raw
            .query_opt(
                "select status from transform_definitions where target_table = $1",
                &[&format!("public.{terminal}")],
            )
            .await
            .expect("read transform_definitions.status")
            .map(|row| row.get(0));
        if status.as_deref() == Some("live") {
            break;
        }
        assert!(
            Instant::now() < live_deadline,
            "b3_totals never reached 'live' status (last observed: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    raw.execute(
        "insert into public.b3_src (id, grp, val) values (-1, 0, -1)",
        &[],
    )
    .await
    .expect("insert warm-up row");
    let warm_up_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seen = raw
            .query_opt(
                &format!("select 1 from public.{terminal} where grp = 0"),
                &[],
            )
            .await
            .expect("poll warm-up row")
            .is_some();
        if seen {
            break;
        }
        assert!(
            Instant::now() < warm_up_deadline,
            "warm-up row never reached public.{terminal}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let rendered_before = trellis::metrics::Metrics::new().render_prometheus();
    let baseline = HistogramSnapshot::capture(
        &rendered_before,
        "trellis_end_to_end_latency_seconds",
        &terminal,
        &T1_BOUNDS,
    );
    let changes_before =
        counter_value(&rendered_before, "trellis_changes_applied_total", &terminal);

    let (rows_issued, _commits_issued, elapsed) = run_grouped_load(
        &raw,
        groups,
        commits_per_sec,
        ROWS_PER_COMMIT,
        offered_duration,
    )
    .await;

    // Unlike B2/B4's 1-1 targets, "caught up" here can't wait for
    // changes_applied to reach rows_issued — an aggregate transform's
    // applied-change count is bounded by *group* touches per batch, not row
    // count, so it converges to something at or below rows_issued (folded
    // rows collapse before ever reaching apply). This instead polls every
    // 200ms and calls the ring "drained" once a poll finds no new applied
    // changes since the previous one — `drained` (not a timeout) is what
    // `sustained` reports below.
    let grace_deadline = Instant::now() + grace;
    let mut changes_now = {
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        counter_value(&rendered, "trellis_changes_applied_total", &terminal)
    };
    let mut drained = false;
    while Instant::now() < grace_deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        let changes_next = counter_value(&rendered, "trellis_changes_applied_total", &terminal);
        if changes_next == changes_now {
            drained = true;
            break;
        }
        changes_now = changes_next;
    }

    let rendered_after = trellis::metrics::Metrics::new().render_prometheus();
    let after = HistogramSnapshot::capture(
        &rendered_after,
        "trellis_end_to_end_latency_seconds",
        &terminal,
        &T1_BOUNDS,
    );
    let window = after.since(&baseline);
    let applied = changes_now.saturating_sub(changes_before);

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

    FoldInResult {
        fold_in_ratio,
        groups,
        target_rows_per_sec,
        offered_duration_secs: elapsed.as_secs_f64(),
        rows_issued,
        achieved_rows_per_sec: rows_issued as f64 / elapsed.as_secs_f64(),
        changes_applied: applied,
        // Not a row-count backlog (see the drain-wait comment above) — this
        // scenario reports `sustained` off whether apply activity actually
        // went quiet before the grace deadline, not off a backlog count.
        backlog_after_grace: 0,
        sustained: drained,
        e2e_count: window.count,
        e2e_p50_bucket_frac: p50_frac,
        e2e_p99_bucket_frac: p99_frac,
    }
}
