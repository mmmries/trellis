//! X6 — idle cost baseline (issue #268): a fully idle install (staging
//! worker plus `application_threads` drain workers, zero source traffic)
//! sampled for transactions/sec and WAL bytes/sec — the two efficiency
//! counters #268 asks to be sampled *from Postgres directly*
//! (`pg_stat_database.xact_commit`, `pg_current_wal_lsn()`), not new engine
//! metrics — plus seals/sec, read the same way (`segment_pointer.active_seq`
//! advances by exactly 1 per successful seal, so a before/after delta is an
//! exact count, not a new metric either). Run this against stock and again
//! after X2/X3 to see whether a latency win quietly costs background load —
//! the issue's own explicit concern ("if X3 improves latency but X6 gets
//! worse, that's a trade, and it should be reported as one").
//!
//! **On "queries/sec"**: #268's idle-cost prediction talks about queries/sec
//! (`~100 queries/sec doing nothing`), but there is no direct query-count
//! counter without `pg_stat_statements`, and this harness's ephemeral
//! `testkit::TestCluster` doesn't preload it (adding a `shared_preload_libraries`
//! dependency to the shared test-cluster bootstrap for one experiment's
//! reporting convenience was judged out of scope). In this idle regime,
//! though, almost every statement the wake path issues
//! (`register_drainer`, `next_claimable_segments`, the maintenance tick's
//! own reads) runs as its own autocommit round trip rather than batched
//! inside an explicit transaction — so `xact_commit`'s delta is already a
//! reasonable stand-in for query rate, not a wholly different quantity. This
//! module reports `xact_commit_per_sec` and republishes it as
//! `queries_per_sec_proxy` rather than pretending a second, independently
//! measured number exists.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Client as TrellisClient, ClientOptions};

use super::chain::create_chain_source_table;

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

async fn xact_commit(raw: &RawClient) -> i64 {
    raw.query_one(
        "select xact_commit from pg_stat_database where datname = current_database()",
        &[],
    )
    .await
    .expect("read pg_stat_database.xact_commit")
    .get(0)
}

async fn wal_lsn(raw: &RawClient) -> String {
    raw.query_one("select pg_current_wal_lsn()::text", &[])
        .await
        .expect("read pg_current_wal_lsn")
        .get(0)
}

/// The active segment pointer's `seg_seq` — each successful seal advances
/// this by exactly 1 (`seal_phase1`'s `next_seg_seq = active_seq + 1`), so a
/// before/after delta over a window is an exact seal count with no new
/// engine metric needed. Read directly from `segment_pointer`, the same
/// table `staging::seal`'s own `active_pointer` reads.
async fn active_seg_seq(raw: &RawClient) -> i64 {
    raw.query_one("select active_seq from segment_pointer", &[])
        .await
        .expect("read segment_pointer.active_seq")
        .get(0)
}

async fn wal_bytes_since(raw: &RawClient, start_lsn: &str) -> i64 {
    // `pg_wal_lsn_diff` returns `numeric`, which `tokio_postgres` has no
    // built-in `FromSql` for — cast to `bigint` explicitly (a WAL delta over
    // one benchmark run's idle window is nowhere near i64's range).
    raw.query_one(
        "select pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn)::bigint",
        &[&start_lsn],
    )
    .await
    .expect("compute wal lsn diff")
    .get(0)
}

#[derive(Debug)]
pub struct IdleCostResult {
    pub application_threads: usize,
    pub duration_secs: f64,
    pub xact_commit_delta: i64,
    pub xact_commit_per_sec: f64,
    pub wal_bytes_delta: i64,
    pub wal_bytes_per_sec: f64,
    pub seals_delta: i64,
    pub seals_per_sec: f64,
}

impl IdleCostResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"idle-cost\",\"application_threads\":{},\"duration_secs\":{:.3},\
             \"xact_commit_delta\":{},\"xact_commit_per_sec\":{:.2},\
             \"queries_per_sec_proxy\":{:.2},\
             \"wal_bytes_delta\":{},\"wal_bytes_per_sec\":{:.1},\
             \"seals_delta\":{},\"seals_per_sec\":{:.3}}}",
            self.application_threads,
            self.duration_secs,
            self.xact_commit_delta,
            self.xact_commit_per_sec,
            self.xact_commit_per_sec,
            self.wal_bytes_delta,
            self.wal_bytes_per_sec,
            self.seals_delta,
            self.seals_per_sec,
        )
    }
}

/// Starts a real streaming `Client` (staging worker + `application_threads`
/// drain workers) against a fresh cluster with a published, but never
/// written-to, source table — so there is truly zero source traffic for the
/// whole run — waits `warmup` for start-of-day work (publication reconcile,
/// first maintenance tick, drainer registration) to finish so it doesn't
/// pollute the steady-state reading, then samples `pg_stat_database.xact_commit`
/// and `pg_current_wal_lsn()` before/after a `duration`-long idle window.
/// Issue #268's X6: `seal_mode` lets this control measurement be re-run
/// after X2/X3 to check whether a latency win quietly costs background
/// load.
pub async fn run_with_seal_mode(
    application_threads: usize,
    warmup: Duration,
    duration: Duration,
    seal_mode: trellis::SealMode,
) -> IdleCostResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    let source = create_chain_source_table(&raw, "idle").await;

    let options = ClientOptions {
        staging_worker: true,
        application_threads,
        source_tables: vec![format!("public.{source}")],
        seal_mode,
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    tokio::time::sleep(warmup).await;

    let commit_before = xact_commit(&raw).await;
    let lsn_before = wal_lsn(&raw).await;
    let seg_seq_before = active_seg_seq(&raw).await;
    let start = Instant::now();

    tokio::time::sleep(duration).await;

    let elapsed = start.elapsed().as_secs_f64();
    let commit_after = xact_commit(&raw).await;
    let wal_delta = wal_bytes_since(&raw, &lsn_before).await;
    let seg_seq_after = active_seg_seq(&raw).await;

    client.shutdown().await.expect("client shutdown");

    let commit_delta = commit_after - commit_before;
    let seals_delta = seg_seq_after - seg_seq_before;
    IdleCostResult {
        application_threads,
        duration_secs: elapsed,
        xact_commit_delta: commit_delta,
        xact_commit_per_sec: commit_delta as f64 / elapsed,
        wal_bytes_delta: wal_delta,
        wal_bytes_per_sec: wal_delta as f64 / elapsed,
        seals_delta,
        seals_per_sec: seals_delta as f64 / elapsed,
    }
}
