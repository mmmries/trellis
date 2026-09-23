//! Issue #330: `RESUME TRANSFORM` rebuilds a frozen target from the current
//! source by re-enumerating the source's *current* keys (`pending_backfill`'s
//! discharge, `publication::enumerate_and_append`). A target row whose every
//! source row was deleted while the transform was frozen is never visited by
//! that enumeration, so nothing removes it and it survives the rebuild.
//!
//! The deletes here are written straight to the source with no intake running,
//! which is exactly the frozen-transform gap: a paused definition's share of
//! the change stream is never folded into its target (and after a slot loss,
//! #310, the gap's changes never reach the ring at all). Both tests drive the
//! discharge and the drain by hand, so they wait on nothing.
//!
//! The issue as filed says a 1-1 transform already drops such rows. It does
//! not: the 1-1 path has the same gap, for the same reason, so it is pinned
//! here too.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::publication;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
use trellis::{Config, Trellis, TrellisOptions};

const TEST_NAME: &str = "resume_drops_deleted_keys_test";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

/// A define-only facade connection (no staging worker, no drain threads), for
/// the `PAUSE`/`RESUME` statements an operator would run.
async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

async fn drain_backfill_chunks(pool: &trellis::Pool) {
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, TEST_NAME, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                "public",
                TEST_NAME,
                Duration::from_secs(5),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, TEST_NAME)
                .await
                .expect("finish_chunk");
        }
    }
}

/// Seals and drains until nothing is pending anywhere in the ring. A bounded
/// deterministic loop, not a wait-for-convergence poll.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            TEST_NAME,
            1,
            "trellis_resume_drops_deleted_keys_test",
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

/// Discharges the marker `RESUME` parked, then drains what it staged.
async fn run_resume_rebuild(pool: &trellis::Pool, client: &mut Client) {
    // Consume an xid so the marker's fence, captured inside the resume's own
    // transaction, has settled.
    client
        .batch_execute("select txid_current()")
        .await
        .expect("consume an xid");
    publication::run_pending_backfills(
        client,
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the resume's backfill marker");
    let pending: i64 = client
        .query_one("select count(*) from pending_backfill", &[])
        .await
        .expect("count markers")
        .get(0);
    assert_eq!(pending, 0, "precondition: the resume's marker discharged");
    drain_backfill_chunks(pool).await;
    drain_to_quiescence(pool, client).await;
}

async fn status(client: &Client, target: &str) -> String {
    client
        .query_one(
            "select status from transform_definitions \
             where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0)
}

fn orders_columns() -> HashMap<String, ValueType> {
    [
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("a", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// `orders`: group `g = 0` holds ids 2, 4, 6; group `g = 1` holds 1, 3, 5.
async fn create_orders(client: &Client) {
    client
        .batch_execute(
            "create table orders (id bigint primary key, g bigint, a numeric); \
             alter table orders replica identity full; \
             insert into orders (id, g, a) select s, s % 2, s from generate_series(1, 6) s;",
        )
        .await
        .expect("create + seed orders");
}

/// The issue's repro: pause an aggregate, delete every source row of one
/// group, resume. The rebuilt target must not hold that group's row.
#[tokio::test]
#[ignore = "issue #330: fix needs a design decision; see the issue"]
async fn resume_drops_an_aggregate_group_whose_rows_were_all_deleted_while_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;

    install_definition(
        &db.pool,
        "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
        &orders_columns(),
        "public",
    )
    .await
    .expect("install the aggregate");
    drain_to_quiescence(&db.pool, &mut client).await;

    let operator = define_only(db.dsn()).await;
    operator
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause");
    // The gap: every row of group 0 goes away, and group 1 changes too, so the
    // rebuild has something observable to do for the surviving group.
    client
        .batch_execute(
            "delete from orders where g = 0; insert into orders (id, g, a) values (7, 1, 7);",
        )
        .await
        .expect("write to the source while paused");
    operator
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume");

    run_resume_rebuild(&db.pool, &mut client).await;

    assert_eq!(status(&client, "order_rollup").await, "live");
    let rows: Vec<(i64, String)> = client
        .query(
            "select g::bigint, total::text from order_rollup order by g",
            &[],
        )
        .await
        .expect("read order_rollup")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![(1, "16".to_string())],
        "the rebuild re-derived group 1 (1 + 3 + 5 + 7) and dropped group 0, whose \
         every source row was deleted while the transform was paused"
    );
    operator.shutdown().await.expect("shut down");
}

/// The 1-1 twin of the test above. A deleted source row's target row must be
/// gone after the rebuild, just as a live 1-1 transform would delete it.
#[tokio::test]
#[ignore = "issue #330: fix needs a design decision; see the issue"]
async fn resume_drops_a_one_to_one_row_whose_source_row_was_deleted_while_paused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_orders(&client).await;

    install_definition(
        &db.pool,
        "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
        &orders_columns(),
        "public",
    )
    .await
    .expect("install the 1-1");
    drain_backfill_chunks(&db.pool).await;
    // The chunked build's completion parks its own catch-up marker; discharge
    // it now so the only marker the rebuild below sees is the resume's.
    publication::run_pending_backfills(
        &mut client,
        "wake",
        &StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the build's catch-up marker");
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(status(&client, "order_doubles").await, "live");

    let operator = define_only(db.dsn()).await;
    operator
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause");
    client
        .batch_execute(
            "delete from orders where g = 0; insert into orders (id, g, a) values (7, 1, 7);",
        )
        .await
        .expect("write to the source while paused");
    operator
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume");

    run_resume_rebuild(&db.pool, &mut client).await;

    assert_eq!(status(&client, "order_doubles").await, "live");
    let rows: Vec<(i64, String)> = client
        .query(
            "select id::bigint, x::text from order_doubles order by id",
            &[],
        )
        .await
        .expect("read order_doubles")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "2".to_string()),
            (3, "6".to_string()),
            (5, "10".to_string()),
            (7, "14".to_string()),
        ],
        "the rebuild picked up id 7 and dropped ids 2, 4 and 6, whose source rows were \
         deleted while the transform was paused"
    );
    operator.shutdown().await.expect("shut down");
}
