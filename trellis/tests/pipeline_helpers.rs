//! Tests of `support/pipeline.rs`, the hand-stepped pipeline the other
//! suites drive: a settle leaves nothing pending and the target matching
//! its source, and a seal waits out the seal gate with a hard deadline and
//! a failure naming the transaction that held it.

#[path = "support/pipeline.rs"]
mod pipeline;

use std::time::Duration;

use pipeline::{
    capture_pass, connect, definer, reconcile_pass, seal, seal_within, settle, try_seal,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SUMS: &str = "TRANSFORM c_sums FROM public.c GROUP BY pid SELECT SUM(amount) AS total";
const ACTUAL: &str = "select pid::text, total::text from public.c_sums order by 1";
const ORACLE: &str = "select pid::text, sum(amount)::text from public.c group by pid order by 1";

async fn text_rows(raw: &tokio_postgres::Client, sql: &str) -> Vec<(String, String)> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

#[tokio::test]
async fn settle_brings_captured_writes_into_the_target_and_leaves_nothing_pending() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.c (id int primary key, pid int not null, amount int); \
         create index on public.c (pid); \
         insert into public.c select i, i % 3, i from generate_series(1, 6) i",
    )
    .await
    .expect("the source");
    let trellis = definer(db.dsn()).await;
    trellis.apply(SUMS).await.expect("define");
    let mut status = None;
    for _ in 0..10 {
        reconcile_pass(&mut raw, &db.pool).await;
        trellis::staging::build::settle_builds(&db.pool).await;
        settle(&db.pool, &mut raw).await;
        status = Some(
            trellis
                .status("c_sums")
                .await
                .expect("status")
                .expect("defined")
                .status,
        );
        if status == Some(TransformStatus::Live) {
            break;
        }
    }
    assert_eq!(status, Some(TransformStatus::Live));
    capture_pass(&mut raw, &db.pool).await;

    raw.batch_execute(
        "insert into public.c values (7, 1, 100); update public.c set amount = 50 where id = 2; \
         delete from public.c where id = 3",
    )
    .await
    .expect("write the source");
    assert!(
        trellis::staging::has_pending(&raw)
            .await
            .expect("has_pending"),
        "capture staged the writes"
    );
    settle(&db.pool, &mut raw).await;
    assert!(
        !trellis::staging::has_pending(&raw)
            .await
            .expect("has_pending"),
        "settle left nothing pending"
    );
    assert_eq!(text_rows(&raw, ACTUAL).await, text_rows(&raw, ORACLE).await);
}

#[tokio::test]
async fn seal_returns_successive_batches() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let first = seal(&mut raw).await;
    let second = seal(&mut raw).await;
    assert!(second > first, "{second} follows {first}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_seal_held_by_an_open_transaction_names_it_and_proceeds_once_it_ends() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin; select pg_current_xact_id()")
        .await
        .expect("hold an xid open");
    let holder_pid: i32 = holder
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    // The first seal fences with the holder in flight, so the next one's
    // gate holds until the holder ends.
    seal(&mut raw).await;
    assert_eq!(try_seal(&mut raw).await, None, "the gate holds");
    let held = seal_within(&mut raw, Duration::from_millis(200))
        .await
        .expect_err("the gate holds for the whole wait");
    assert!(
        held.contains("the seal gate stayed blocked") && held.contains(&holder_pid.to_string()),
        "names the gate and the holder: {held}"
    );
    holder
        .batch_execute("commit")
        .await
        .expect("end the holder");
    seal(&mut raw).await;
}
