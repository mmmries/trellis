//! Issue #558 experiment 5: a plain aggregate built by ledger re-derive
//! chunks, applying CDC from the first chunk (`trellis::staging::ledger_build`).
//!
//! Every test here runs with `TRELLIS_EXP558_LEDGER=contrib` and
//! `TRELLIS_EXP558_BUILD=ledger`, set once per process by [`flags`] before
//! anything reads them (both are `OnceLock`s in the engine). The flag-off
//! behaviour is covered by the existing backfill tests, which run in their own
//! processes without these variables.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{
    Definition, ValueType, install_definition, parse, render_aggregate_select_sql,
};
use trellis::staging::{apply, has_pending, ledger_build, retire_drained_segments, seal};
use trellis::{Client as TrellisClient, ClientOptions};

const DEF: &str = "TRANSFORM agg FROM items GROUP BY grp \
                   SELECT grp AS grp, SUM(amount) AS total, COUNT(*) AS n, \
                          AVG(amount) AS mean, MIN(amount) AS lo";

/// Turns the experiment on for this process, once, before any engine code
/// reads the variables.
fn flags() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // SAFETY: `Once` runs this before any test in this binary touches the
        // engine or starts a thread of its own, and nothing else writes these.
        unsafe {
            std::env::set_var("TRELLIS_EXP558_LEDGER", "contrib");
            std::env::set_var("TRELLIS_EXP558_BUILD", "ledger");
        }
    });
}

async fn connect_raw(dsn: &str) -> Client {
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

fn columns() -> HashMap<String, ValueType> {
    ["id", "grp", "amount"]
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// `items` with `rows` rows over 997 groups (a NULL group among them) and a
/// NULL amount every 101st row. `REPLICA IDENTITY FULL`, so a delete and an
/// update carry the whole old row.
async fn seed(raw: &Client, rows: i64) {
    raw.batch_execute(&format!(
        "create table public.items (id bigint primary key, grp numeric, amount numeric); \
         alter table public.items replica identity full; \
         create index on public.items (grp); \
         insert into public.items (id, grp, amount) \
         select g, case when g % 1009 = 0 then null else g % 997 end, \
                case when g % 101 = 0 then null else round((g % 1000) / 10.0, 1) end \
         from generate_series(1, {rows}) g"
    ))
    .await
    .expect("seed items");
}

/// Every group of the target and of a fresh `GROUP BY` over the source, as
/// sorted text rows.
async fn target_and_oracle(raw: &Client) -> (Vec<Vec<Option<String>>>, Vec<Vec<Option<String>>>) {
    let def = parse(DEF).expect("parse");
    let cols = "grp::text, total::text, n::text, mean::text, lo::text";
    let read = async |sql: String| {
        let mut rows: Vec<Vec<Option<String>>> = raw
            .query(&sql, &[])
            .await
            .expect("read groups")
            .into_iter()
            .map(|r| (0..5).map(|i| r.get::<_, Option<String>>(i)).collect())
            .collect();
        rows.sort();
        rows
    };
    let target = read(format!("select {cols} from public.agg")).await;
    let oracle = read(format!(
        "select {cols} from ({}) o",
        render_aggregate_select_sql(&def)
    ))
    .await;
    (target, oracle)
}

async fn assert_matches_oracle(raw: &Client, context: &str) {
    let (target, oracle) = target_and_oracle(raw).await;
    assert_eq!(target.len(), oracle.len(), "{context}: group count");
    for (t, o) in target.iter().zip(&oracle) {
        assert_eq!(t, o, "{context}: first differing group");
    }
}

async fn status(raw: &Client, id: i64) -> String {
    raw.query_one(
        "select status from transform_definitions where id = $1",
        &[&id],
    )
    .await
    .expect("read status")
    .get(0)
}

async fn poll_until<F>(timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out after {timeout:?}: {message}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The ledger's entries: (total, with a group, with a basis).
async fn ledger_counts(raw: &Client) -> (i64, i64, i64) {
    let row = raw
        .query_one(
            "select count(*), count(group_key), count(basis) from public.agg__ledger",
            &[],
        )
        .await
        .expect("read ledger");
    (row.get(0), row.get(1), row.get(2))
}

/// 200k rows, eight drain workers, no writes: the build enqueues PK-range
/// chunks, every worker claims them, the definition goes straight to `live`,
/// the target equals a from-scratch `GROUP BY`, and the ledger holds one
/// entry per row, each with the snapshot it was read under.
#[tokio::test]
async fn a_ledger_build_on_eight_workers_matches_a_fresh_group_by() {
    flags();
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw, 200_000).await;

    let client = TrellisClient::start(
        db.dsn(),
        ClientOptions {
            staging_worker: false,
            application_threads: 8,
            ..Default::default()
        },
    )
    .expect("client start");
    let def = install_definition(&db.pool, DEF, &columns(), "public")
        .await
        .expect("install");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");
    let chunks: i64 = raw
        .query_one(
            "select count(*) from backfill_chunks where definition_id = $1 and hi is not null",
            &[&def.id],
        )
        .await
        .expect("count chunks")
        .get(0);
    assert_eq!(
        chunks,
        200_000 / ledger_build::chunk_rows(),
        "the build must be PK-range chunks"
    );

    poll_until(
        Duration::from_secs(120),
        "the build never went live",
        async || status(&raw, def.id).await == "live",
    )
    .await;

    assert_matches_oracle(&raw, "after the build").await;
    assert_eq!(
        ledger_counts(&raw).await,
        (200_000, 200_000, 200_000),
        "one entry per row, each in a group and with a basis"
    );
    client.shutdown().await.expect("clean shutdown");
}

/// The same build with a writer running through it: inserts, updates that
/// move rows between groups (and change amounts), deletes, and short-lived
/// rows, on keys whose chunk has and hasn't run yet. Once the writer stops and
/// the pipeline drains, the target equals the oracle.
#[tokio::test]
async fn a_ledger_build_under_concurrent_writes_converges_to_the_oracle() {
    flags();
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    seed(&raw, 200_000).await;

    let client = TrellisClient::start(
        db.dsn(),
        ClientOptions {
            staging_worker: true,
            application_threads: 8,
            ..Default::default()
        },
    )
    .expect("client start");

    let stop = Arc::new(AtomicBool::new(false));
    let during_build = Arc::new(AtomicU64::new(0));
    let building = Arc::new(AtomicBool::new(false));
    let writer = {
        let dsn = db.dsn().to_string();
        let stop = Arc::clone(&stop);
        let during_build = Arc::clone(&during_build);
        let building = Arc::clone(&building);
        tokio::spawn(async move {
            let w = connect_raw(&dsn).await;
            let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
            let mut next = move || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            let mut new_id: i64 = 1_000_000;
            while !stop.load(Ordering::Relaxed) {
                let id = (next() % 200_000) as i64 + 1;
                let grp = (next() % 997) as i32;
                let amount = (next() % 10_000) as i64;
                let sql = match next() % 6 {
                    0 => format!("update items set grp = {grp} where id = {id}"),
                    1 => format!(
                        "update items set amount = round({amount} / 7.0, 1) where id = {id}"
                    ),
                    2 => format!("update items set grp = {grp}, amount = null where id = {id}"),
                    3 => format!("delete from items where id = {id}"),
                    4 => {
                        new_id += 1;
                        format!(
                            "insert into items values ({}, {grp}, {amount}.5), ({new_id}, {grp}, 1.5) \
                             on conflict (id) do update set grp = excluded.grp",
                            id
                        )
                    }
                    _ => {
                        // A row born and gone in two quick transactions, often
                        // folded into one image-less change.
                        new_id += 1;
                        format!(
                            "insert into items values ({new_id}, {grp}, {amount}.5); \
                             delete from items where id = {new_id}"
                        )
                    }
                };
                w.batch_execute(&sql).await.expect("write");
                // Paced (about a thousand writes a second), so the drain keeps
                // up and the build, not a backlog, is what the writes race.
                tokio::time::sleep(Duration::from_millis(1)).await;
                if building.load(Ordering::Relaxed) {
                    during_build.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    let def = install_definition(&db.pool, DEF, &columns(), "public")
        .await
        .expect("install");
    building.store(true, Ordering::Relaxed);
    let started = std::time::Instant::now();
    poll_until(
        Duration::from_secs(180),
        "the build never went live",
        async || status(&raw, def.id).await == "live",
    )
    .await;
    building.store(false, Ordering::Relaxed);
    let build_time = started.elapsed();
    // Keep writing for a moment after the build, then stop.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer");
    let writes = during_build.load(Ordering::Relaxed);
    assert!(writes > 50, "only {writes} writes landed during the build");

    let settle = std::time::Instant::now();
    let mut last = None;
    let converged = {
        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        loop {
            let (target, oracle) = target_and_oracle(&raw).await;
            if target == oracle {
                break true;
            }
            last = Some((target, oracle));
            if std::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    if !converged {
        let (target, oracle) = last.expect("a mismatch was recorded");
        let diff: Vec<_> = oracle
            .iter()
            .filter(|o| !target.contains(o))
            .take(5)
            .map(|o| {
                let t = target.iter().find(|t| t[0] == o[0]);
                (o.clone(), t.cloned())
            })
            .collect();
        let ledger_vs_source: i64 = raw
            .query_one(
                "select count(*) from public.agg__ledger l \
                 full join public.items i on l.from_key = i.id::text \
                 where (i.id is null and l.group_key is not null) \
                    or (i.id is not null and l.group_key is null) \
                    or (i.grp is not null and l.group_key is distinct from i.grp::text)",
                &[],
            )
            .await
            .expect("compare the ledger with the source")
            .get(0);
        let pending = has_pending(&raw).await.expect("has_pending");
        panic!(
            "target never converged ({writes} writes during the build; ledger entries \
             disagreeing with the source: {ledger_vs_source}; ring pending: {pending}); \
             first differing (oracle, target) groups: {diff:?}"
        );
    }
    eprintln!(
        "exp5: build went live after {build_time:?} with {writes} writes during it; \
         converged {:?} after the writer stopped",
        settle.elapsed()
    );
    client.shutdown().await.expect("clean shutdown");
}

// ---------------------------------------------------------------------
// Deterministic interleavings: chunks run by hand, CDC staged by hand
// with the writer's real transaction id, drained through the engine.
// ---------------------------------------------------------------------

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

const IMAGE: &str = "jsonb_build_object('id', id::text, 'grp', grp::text, 'amount', amount::text)";

/// Runs `sql` (one statement touching only row `id`) in its own transaction
/// and stages its change the way intake would: the row's images before and
/// after as text, the transaction's id, and a position after its commit.
async fn write_and_stage(raw: &Client, id: i64, sql: &str) {
    raw.batch_execute("begin").await.expect("begin");
    let old: Option<String> = raw
        .query_opt(
            &format!("select {IMAGE}::text from items where id = $1"),
            &[&id],
        )
        .await
        .expect("old image")
        .map(|r| r.get(0));
    raw.batch_execute(sql).await.expect("write");
    let new: Option<String> = raw
        .query_opt(
            &format!("select {IMAGE}::text from items where id = $1"),
            &[&id],
        )
        .await
        .expect("new image")
        .map(|r| r.get(0));
    let xid: String = raw
        .query_one("select pg_current_xact_id()::text", &[])
        .await
        .expect("xid")
        .get(0);
    raw.batch_execute("commit").await.expect("commit");
    let op = match (&old, &new) {
        (None, Some(_)) => "insert",
        (Some(_), Some(_)) => "update",
        (Some(_), None) => "delete",
        (None, None) => panic!("the write touched nothing"),
    };
    let table = active_seg_table(raw).await;
    let lsn = testkit::wal_insert_lsn(raw).await;
    raw.execute(
        &format!(
            "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen, src_xid) \
             values ('public.items', $1, $2, $3, $4::text::jsonb, $5::text::jsonb, 0, $6::text::xid8)"
        ),
        &[&id.to_string(), &op, &lsn, &old, &new, &xid],
    )
    .await
    .expect("stage the change");
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "exp558_build_test",
            1,
            "trellis_exp558_build_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// A 30k-row source, installed and dispatched (three 10k-row chunks, none
/// run: no worker is running), with the definition and its chunk bounds.
async fn dispatched(
    db: &testkit::TestDatabase,
    raw: &Client,
) -> (Definition, Vec<(Option<String>, String)>) {
    seed(raw, 30_000).await;
    let def = install_definition(&db.pool, DEF, &columns(), "public")
        .await
        .expect("install");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");
    assert_eq!(status(raw, def.id).await, "backfilling");
    let chunks = raw
        .query(
            "select lo, hi from backfill_chunks where definition_id = $1 order by id",
            &[&def.id],
        )
        .await
        .expect("read chunks")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect::<Vec<(Option<String>, String)>>();
    assert_eq!(chunks.len(), 3, "three 10k-row chunks");
    (def, chunks)
}

async fn run_chunk(db: &testkit::TestDatabase, def: &Definition, chunk: &(Option<String>, String)) {
    ledger_build::execute_range(&db.pool, def, chunk.0.as_deref(), &chunk.1)
        .await
        .expect("run chunk");
}

/// Every order a change and its row's chunk can meet in, each checked against
/// the oracle once everything has drained:
///
/// - applied before its chunk runs (update, group move, delete, update-then-
///   delete folded into one change, a new row in a chunk's range);
/// - committed before its chunk runs but applied after it (skipped by the
///   chunk's basis);
/// - committed and applied after its chunk (the ledger's old side);
/// - a row born and gone inside one batch (an image-less change, re-derived).
#[tokio::test]
async fn changes_meet_their_chunks_in_every_order_and_land_exactly_once() {
    flags();
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    let (def, chunks) = dispatched(&db, &raw).await;

    // Applied before any chunk has run.
    write_and_stage(&raw, 5, "update items set grp = 900 where id = 5").await;
    write_and_stage(&raw, 6, "delete from items where id = 6").await;
    write_and_stage(&raw, 7, "update items set amount = 12345.0 where id = 7").await;
    write_and_stage(&raw, 0, "insert into items values (0, 3, 1.0)").await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    // A row that moves and is then deleted, folded into one change whose old
    // image names a group nothing was ever added to.
    write_and_stage(&raw, 12_001, "update items set grp = 901 where id = 12001").await;
    write_and_stage(&raw, 12_001, "delete from items where id = 12001").await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    // Committed before chunk 1 runs, applied after it: the chunk's snapshot
    // already counts it.
    write_and_stage(
        &raw,
        8,
        "update items set grp = 902, amount = 2.0 where id = 8",
    )
    .await;
    write_and_stage(&raw, 9, "delete from items where id = 9").await;
    run_chunk(&db, &def, &chunks[0]).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    // Committed and applied after chunk 1: the ledger's old side.
    write_and_stage(&raw, 10, "update items set grp = 903 where id = 10").await;
    write_and_stage(&raw, 11, "delete from items where id = 11").await;
    write_and_stage(&raw, 5, "update items set grp = 904 where id = 5").await;
    // A row born and gone in one batch: an image-less change.
    write_and_stage(&raw, 40_000, "insert into items values (40000, 7, 7.0)").await;
    write_and_stage(&raw, 40_000, "delete from items where id = 40000").await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    // Chunk 3 runs with a change to one of its rows committed but not yet
    // applied, and another applied before it.
    write_and_stage(&raw, 20_005, "update items set grp = 905 where id = 20005").await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    write_and_stage(&raw, 20_006, "update items set grp = 906 where id = 20006").await;
    run_chunk(&db, &def, &chunks[2]).await;
    run_chunk(&db, &def, &chunks[1]).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_matches_oracle(&raw, "after every chunk and change").await;
    let (_, with_group, with_basis) = ledger_counts(&raw).await;
    let rows: i64 = raw
        .query_one("select count(*) from items", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(with_group, rows, "one counted entry per live row");
    assert!(with_basis >= 29_990, "the chunks stamped their bases");
}

/// A chunk re-run (after a crash or a reclaim) changes nothing: its entries
/// already equal the live rows. Also a re-run after later changes applied.
#[tokio::test]
async fn re_running_a_chunk_leaves_the_target_unchanged() {
    flags();
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    let (def, chunks) = dispatched(&db, &raw).await;

    for chunk in &chunks {
        run_chunk(&db, &def, chunk).await;
    }
    assert_matches_oracle(&raw, "after the build").await;
    let (before, _) = target_and_oracle(&raw).await;
    run_chunk(&db, &def, &chunks[1]).await;
    run_chunk(&db, &def, &chunks[1]).await;
    let (after, _) = target_and_oracle(&raw).await;
    assert_eq!(before, after, "a re-run chunk must add nothing");

    write_and_stage(&raw, 10_500, "update items set grp = 950 where id = 10500").await;
    write_and_stage(&raw, 10_501, "delete from items where id = 10501").await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for chunk in &chunks {
        run_chunk(&db, &def, chunk).await;
    }
    assert_matches_oracle(&raw, "after re-running every chunk past applied changes").await;
}
