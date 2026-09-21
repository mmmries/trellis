//! E3 — intake ceiling (issue #266, tests H2): a `staging_worker: true`
//! client with **zero transforms** and zero application threads — nothing
//! ever drains the ring, so every row this reports came only from CDC
//! intake's single-threaded `pgoutput` decode plus `staging::append::append`.
//! Reported as the hard ceiling every other throughput number in this suite
//! (B2/B3) sits under, per H2: "Whether that path alone clears 100k rows/s —
//! let alone 400k — is unknown and bounds every other number in this issue."

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client as RawClient, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Client as TrellisClient, ClientOptions};

use super::load::run_max_rate_load;

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

/// Total row count across every physical ring table (`trellis.seg_0` ..
/// `seg_<RING_SIZE-1>`), discovered dynamically via `pg_tables` rather than
/// hardcoding `RING_SIZE` (a private `staging::append` constant this crate
/// has no access to, by ADR-0012 design) — this is a live count across
/// whichever ring tables the schema actually has, sealed or active.
async fn total_ring_rows(raw: &RawClient) -> i64 {
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
    assert!(
        !tables.is_empty(),
        "expected at least one seg_N ring table after migration"
    );

    let mut total = 0i64;
    for table in tables {
        let count: i64 = raw
            .query_one(
                &format!("select count(*) from {DEFAULT_SCHEMA}.{table}"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("count ring table {table}: {e}"))
            .get(0);
        total += count;
    }
    total
}

#[derive(Debug)]
pub struct IntakeCeilingResult {
    pub rows_per_commit: usize,
    pub offered_duration_secs: f64,
    pub rows_offered: u64,
    pub offered_achieved_rows_per_sec: f64,
    pub ring_rows_appended: i64,
    pub append_achieved_rows_per_sec: f64,
    pub append_backlog: i64,
}

impl IntakeCeilingResult {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"scenario\":\"intake-ceiling\",\"rows_per_commit\":{},\
             \"offered_duration_secs\":{},\"rows_offered\":{},\
             \"offered_achieved_rows_per_sec\":{:.1},\"ring_rows_appended\":{},\
             \"append_achieved_rows_per_sec\":{:.1},\"append_backlog\":{}}}",
            self.rows_per_commit,
            self.offered_duration_secs,
            self.rows_offered,
            self.offered_achieved_rows_per_sec,
            self.ring_rows_appended,
            self.append_achieved_rows_per_sec,
            self.append_backlog,
        )
    }
}

/// Runs the intake-ceiling probe: `application_threads: 0` (nothing drains
/// the ring — see H2), pushes `rows_per_commit`-sized batches back to back
/// for `offered_duration` with no pacing (max rate), then polls up to
/// `catch_up_grace` for the ring's row count to catch up with what was
/// offered (decode lags slightly behind commit, so this isn't instantaneous)
/// before reporting the append-side achieved rate.
pub async fn run(
    rows_per_commit: usize,
    offered_duration: Duration,
    catch_up_grace: Duration,
) -> IntakeCeilingResult {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table public.e3_src (id bigint primary key, val numeric)")
        .await
        .expect("create intake-ceiling source table");

    let options = ClientOptions {
        staging_worker: true,
        application_threads: 0,
        source_tables: vec!["public.e3_src".to_string()],
        ..Default::default()
    };
    let client = TrellisClient::start(db.dsn(), options).expect("client start");

    let baseline_ring_rows = total_ring_rows(&raw).await;

    let load = run_max_rate_load(&raw, "e3_src", 1, rows_per_commit, offered_duration).await;

    let deadline = Instant::now() + catch_up_grace;
    let ring_rows_now = loop {
        let ring_rows_now = total_ring_rows(&raw).await;
        if (ring_rows_now - baseline_ring_rows) as u64 >= load.rows_issued
            || Instant::now() >= deadline
        {
            break ring_rows_now;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let appended = ring_rows_now - baseline_ring_rows;
    let backlog = load.rows_issued as i64 - appended;

    client.shutdown().await.expect("client shutdown");

    IntakeCeilingResult {
        rows_per_commit,
        offered_duration_secs: offered_duration.as_secs_f64(),
        rows_offered: load.rows_issued,
        offered_achieved_rows_per_sec: load.rows_issued as f64 / load.elapsed.as_secs_f64(),
        ring_rows_appended: appended,
        append_achieved_rows_per_sec: appended as f64 / load.elapsed.as_secs_f64(),
        append_backlog: backlog,
    }
}
