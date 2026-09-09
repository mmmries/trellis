//! The M0 benchmark itself (issue #63): a deterministic
//! `posts` -> `posts_calc` -> `posts_totals` pipeline (source table, 1-1
//! calc transform, aggregate over the calc's output), matching the poc
//! shape the issue's diagnosis was measured against.
//!
//! Two phases are timed independently:
//!
//! - **calc**: `posts` (already fully loaded) -> `posts_calc`, a plain 1-1
//!   backfill. Fast regardless of `(n, g)` — reported for completeness, not
//!   as the headline number.
//! - **aggregate**: `posts_calc` (now fully populated, `n` rows) ->
//!   `posts_totals`, `GROUP BY author`. This is the phase the issue's
//!   milestones (M1 done, M2/M3/M4 pending) target, and the one the
//!   regression ceiling gates — it only starts once `posts_calc` is fully
//!   converged, so it exercises the same "backfill over an already-populated
//!   table" bulk-recompute path the poc measured (~1m50s post-M1,
//!   high-cardinality), not an incremental delta path.
//!
//! Both phases use a fresh [`engine::Client`] apiece (`staging_worker` +
//! application workers), started right before the definition that kicks off
//! that phase's backfill, and polled via
//! [`engine::staging::has_pending`] — the ring's own "is anything still
//! unconverged" gate — until it reports `false`. No fixed sleep, no bespoke
//! convergence heuristic.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use engine::config::DEFAULT_SCHEMA;
use engine::defs::{
    ValueType, create_aggregate_target_table, create_definition, create_target_table, parse,
    source_primary_key,
};
use engine::staging::has_pending;
use engine::{Client as TrellisClient, ClientOptions};
use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};

use crate::generate;

/// Connects directly to `dsn` (bypassing `engine::Pool`), matching the
/// convention `engine`'s own integration tests use — `has_pending` wants a
/// `tokio_postgres::GenericClient`, which the pool's wrapped client doesn't
/// implement, so polling goes through a raw connection instead.
async fn connect_raw(dsn: &str) -> RawClient {
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

/// How often the convergence poll checks `has_pending` again. Cheap (a
/// handful of indexed existence probes — see `converge::has_pending`'s own
/// doc comment), so a short interval doesn't meaningfully perturb the
/// measurement.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Upper bound on how long a single phase's convergence poll will wait
/// before giving up and panicking, independent of the scenario's regression
/// ceiling (which is checked, and can fail the run, only after a phase
/// actually converges). Generous: a hang here means the pipeline is stuck,
/// not just slow, and should fail loudly rather than after the ceiling's
/// own ordinary "too slow" message.
const WATCHDOG: Duration = Duration::from_secs(20 * 60);

async fn await_drained(raw: &RawClient) -> Duration {
    let start = Instant::now();
    loop {
        let pending = has_pending(raw).await.expect("has_pending");
        if !pending {
            return start.elapsed();
        }
        if start.elapsed() > WATCHDOG {
            panic!(
                "backfill did not converge within the {WATCHDOG:?} watchdog — the pipeline is \
                 stuck, not just slow"
            );
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// One scenario's full set of measurements, in the shape M0's spec asks
/// for: the timed backfill(s), plus the two same-box/same-data reference
/// floors (raw compute, bare write) the issue's milestones are held
/// against, plus a pass/fail correctness assertion independent of timing.
#[derive(Debug)]
pub struct BenchResult {
    pub scenario: String,
    pub n: i64,
    pub g: i64,
    pub load_ms: u128,
    pub calc_backfill_ms: u128,
    pub aggregate_backfill_ms: u128,
    pub total_backfill_ms: u128,
    pub raw_group_by_floor_ms: u128,
    pub write_floor_ms: u128,
    pub group_count: i64,
    pub correctness_ok: bool,
    pub ceiling_ms: u128,
    pub within_ceiling: bool,
}

impl BenchResult {
    /// Renders the result as one line of machine-readable JSON. Hand-rolled
    /// rather than pulled in via `serde_json`: every field here is either an
    /// integer, a bool, or a string with no characters that need escaping
    /// (`scenario` is always one of this file's own literal names), so a
    /// dependency buys nothing a `format!` doesn't already give for free.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"{}\",\"n\":{},\"g\":{},\"load_ms\":{},\"calc_backfill_ms\":{},\
             \"aggregate_backfill_ms\":{},\"total_backfill_ms\":{},\
             \"raw_group_by_floor_ms\":{},\"write_floor_ms\":{},\"group_count\":{},\
             \"correctness_ok\":{},\"ceiling_ms\":{},\"within_ceiling\":{}}}",
            self.scenario,
            self.n,
            self.g,
            self.load_ms,
            self.calc_backfill_ms,
            self.aggregate_backfill_ms,
            self.total_backfill_ms,
            self.raw_group_by_floor_ms,
            self.write_floor_ms,
            self.group_count,
            self.correctness_ok,
            self.ceiling_ms,
            self.within_ceiling,
        )
    }
}

/// Runs one full scenario end to end against a fresh, ephemeral Postgres
/// instance: loads `n` deterministic `posts` rows spread over `g` `author`
/// groups, backfills `posts_calc` then `posts_totals`, checks the result
/// against an independent oracle, and reports the timings above against
/// `ceiling` (the aggregate phase's regression ceiling).
pub async fn run(name: &str, n: i64, g: i64, ceiling: Duration) -> BenchResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    generate::create_posts_table(&db.pool).await;
    let load_elapsed = generate::load_posts(&db.pool, n, g).await;

    // --- Phase 1: posts -> posts_calc (1-1) ---------------------------
    let calc_source = "TRANSFORM posts_calc FROM posts SELECT author AS author, word_count + byte_size AS total_size";
    let posts_columns = numeric_columns(&["author", "word_count", "byte_size"]);

    let calc_start = Instant::now();
    let calc_def = parse(calc_source).expect("parse posts_calc definition");
    create_definition(&db.pool, calc_source, &posts_columns)
        .await
        .expect("create posts_calc definition");
    let posts_pk = source_primary_key(&db.pool, "posts")
        .await
        .expect("introspect posts primary key");
    create_target_table(&db.pool, &calc_def, "public", &posts_pk, &posts_columns)
        .await
        .expect("create posts_calc target table");

    let calc_client = TrellisClient::start(
        db.dsn(),
        ClientOptions {
            staging_worker: true,
            application_threads: 4,
            source_tables: vec![format!("{DEFAULT_SCHEMA}.posts")],
            ..Default::default()
        },
    )
    .expect("start posts_calc client");
    await_drained(&raw).await;
    let calc_backfill_ms = calc_start.elapsed().as_millis();
    calc_client
        .shutdown()
        .await
        .expect("shut down posts_calc client");

    let calc_rows: i64 = raw
        .query_one("select count(*) from posts_calc", &[])
        .await
        .expect("count posts_calc rows")
        .get(0);
    assert_eq!(
        calc_rows, n,
        "posts_calc must have exactly one row per posts row once converged"
    );

    // --- Reference floors, measured against the now-fully-populated
    // posts_calc, before the timed aggregate phase begins -----------------
    let raw_group_by_floor_ms = measure_raw_group_by_floor(&raw).await;
    let write_floor_ms = measure_write_floor(&raw).await;

    // --- Phase 2: posts_calc -> posts_totals (GROUP BY author) ------------
    let totals_source = "TRANSFORM posts_totals FROM posts_calc GROUP BY author \
         SELECT author AS author, SUM(total_size) AS total_size, COUNT(*) AS post_count";
    let calc_columns = numeric_columns(&["author", "total_size"]);

    let aggregate_start = Instant::now();
    let totals_def = parse(totals_source).expect("parse posts_totals definition");
    create_definition(&db.pool, totals_source, &calc_columns)
        .await
        .expect("create posts_totals definition");
    create_aggregate_target_table(&db.pool, &totals_def, "public", &calc_columns)
        .await
        .expect("create posts_totals target table");

    let totals_client = TrellisClient::start(
        db.dsn(),
        ClientOptions {
            staging_worker: true,
            application_threads: 4,
            // `posts_calc`'s target table lives in `public` (the
            // `target_schema` passed to `create_target_table` above), not
            // in `DEFAULT_SCHEMA` like the raw `posts` source table does.
            source_tables: vec!["public.posts_calc".to_string()],
            ..Default::default()
        },
    )
    .expect("start posts_totals client");
    await_drained(&raw).await;
    let aggregate_backfill_ms = aggregate_start.elapsed().as_millis();
    totals_client
        .shutdown()
        .await
        .expect("shut down posts_totals client");

    let (correctness_ok, group_count) = check_correctness(&raw, &totals_def).await;

    let total_backfill_ms = calc_backfill_ms + aggregate_backfill_ms;
    let within_ceiling = aggregate_backfill_ms <= ceiling.as_millis();

    BenchResult {
        scenario: name.to_string(),
        n,
        g,
        load_ms: load_elapsed.as_millis(),
        calc_backfill_ms,
        aggregate_backfill_ms,
        total_backfill_ms,
        raw_group_by_floor_ms,
        write_floor_ms,
        group_count,
        correctness_ok,
        ceiling_ms: ceiling.as_millis(),
        within_ceiling,
    }
}

/// The raw compute floor: how long Postgres alone takes to run the exact
/// `GROUP BY` `posts_totals`'s definition renders, with no write at all.
/// `count(*)` over a subquery still forces the planner to materialize every
/// group, so this isn't measuring a short-circuited/optimized-away query.
async fn measure_raw_group_by_floor(raw: &RawClient) -> u128 {
    let start = Instant::now();
    raw.query_one(
        "select count(*) from ( \
             select author, sum(total_size) as total_size, count(*) as post_count \
             from posts_calc group by author \
         ) t",
        &[],
    )
    .await
    .expect("run raw GROUP BY floor query");
    start.elapsed().as_millis()
}

/// The bare write floor: materializes the `GROUP BY`'s result set
/// (untimed), then times only writing those rows into a fresh table — no
/// grouping/aggregation work left to do, isolating pure write throughput for
/// however many groups this scenario has.
async fn measure_write_floor(raw: &RawClient) -> u128 {
    raw.batch_execute(
        "create temporary table posts_totals_materialized as \
         select author, sum(total_size) as total_size, count(*) as post_count \
         from posts_calc group by author",
    )
    .await
    .expect("materialize GROUP BY result for the write floor");
    raw.batch_execute(
        "create table posts_totals_write_floor ( \
             author bigint primary key, total_size numeric, post_count bigint)",
    )
    .await
    .expect("create write-floor scratch table");

    let start = Instant::now();
    raw.execute(
        "insert into posts_totals_write_floor select * from posts_totals_materialized",
        &[],
    )
    .await
    .expect("run bare write floor insert");
    let elapsed = start.elapsed();

    raw.batch_execute("drop table posts_totals_write_floor; drop table posts_totals_materialized")
        .await
        .expect("drop write-floor scratch tables");

    elapsed.as_millis()
}

/// Compares the backfilled `posts_totals` against an independent, from-
/// scratch `GROUP BY` over `posts_calc` (the same query
/// [`measure_raw_group_by_floor`] runs, rendered straight from the
/// definition rather than hand-duplicated) — an exact-value check, not just
/// a row count. Returns whether it matched, plus the group count for
/// reporting.
async fn check_correctness(raw: &RawClient, def: &engine::defs::ast::TransformDef) -> (bool, i64) {
    let oracle_sql = engine::defs::render_aggregate_select_sql(def);
    let oracle_sql =
        format!("select author::text, total_size::text, post_count::text from ({oracle_sql}) o");
    let oracle: HashMap<String, (Option<String>, Option<String>)> = raw
        .query(&oracle_sql, &[])
        .await
        .expect("run posts_totals oracle query")
        .into_iter()
        .map(|row| {
            let author: String = row.get(0);
            (author, (row.get(1), row.get(2)))
        })
        .collect();

    let target: HashMap<String, (Option<String>, Option<String>)> = raw
        .query(
            "select author::text, total_size::text, post_count::text from posts_totals",
            &[],
        )
        .await
        .expect("read posts_totals")
        .into_iter()
        .map(|row| {
            let author: String = row.get(0);
            (author, (row.get(1), row.get(2)))
        })
        .collect();

    (target == oracle, oracle.len() as i64)
}
