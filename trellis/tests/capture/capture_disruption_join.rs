//! The table-gone check for a `JOIN`'s joined table. Its own
//! test binary: a `JOIN` needs the
//! group-basis knob, which is process-wide and would change every aggregate
//! build in `capture_disruption.rs`.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::TransformStatus;

#[path = "support/pipeline.rs"]
mod pipeline;

use pipeline::{bring_live_all, capture_pass, connect, definer};

/// The table-gone case for a `JOIN`'s joined table: it pauses its reader (the
/// 1-1 and aggregate sources are the matrix's). A `JOIN`'s joined table once
/// panicked the pass.
#[tokio::test]
async fn a_gone_joined_table_pauses_its_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.ja (id int primary key, k varchar(5), x int); \
         create table public.jb (id int primary key, k varchar(5), y int); \
         create index on public.ja (k); create index on public.jb (k);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM jl FROM public.ja LEFT JOIN public.jb ON ja.k = jb.k \
             GROUP BY ja.k WITHOUT SOURCE INDEX SELECT ja.k AS k, COUNT(*) AS n, SUM(jb.y) AS sy",
        )
        .await
        .expect("define jl");
    bring_live_all(&mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    app.batch_execute("drop table public.jb")
        .await
        .expect("drop the joined table");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let status = trellis.status("jl").await.unwrap().unwrap();
    assert_eq!(status.status, TransformStatus::Paused);
    let error = status.capture_failure.expect("a reason").error;
    assert!(error.contains("no longer exists"), "{error}");
}

/// How many `capture_triggers`, `capture_baselines` and `partition_sources`
/// rows `table` has.
async fn record_counts(raw: &Client, table: &str) -> (i64, i64, i64) {
    let mut counts = Vec::new();
    for record in ["capture_triggers", "capture_baselines", "partition_sources"] {
        let sql = format!("select count(*) from {record} where table_name = '{table}'");
        counts.push(
            raw.query_one(&sql, &[])
                .await
                .expect("count")
                .get::<_, i64>(0),
        );
    }
    (counts[0], counts[1], counts[2])
}

/// Dropping the last reader of a `JOIN`'s joined table forgets that table's
/// records; a table another definition still reads keeps them.
#[tokio::test]
async fn dropping_the_last_reader_of_a_joined_table_forgets_its_records() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.fa (id int primary key, k varchar(5), x int); \
         create table public.fb (id int primary key, k varchar(5), y int); \
         create index on public.fa (k); create index on public.fb (k);",
    )
    .await
    .unwrap();
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM fj FROM public.fa LEFT JOIN public.fb ON fa.k = fb.k \
         GROUP BY fa.k WITHOUT SOURCE INDEX SELECT fa.k AS k, COUNT(*) AS n, SUM(fb.y) AS sy",
        "TRANSFORM fa_plain FROM public.fa SELECT x AS x",
    ] {
        trellis.apply(text).await.expect(text);
    }
    capture_pass(&mut raw, &db.pool).await;
    for t in ["public.fa", "public.fb"] {
        assert!(record_counts(&raw, t).await.0 > 0, "{t} is recorded");
    }

    trellis.apply("PAUSE TRANSFORM fj").await.unwrap();
    trellis.apply("DROP TRANSFORM fj").await.unwrap();
    assert_eq!(record_counts(&raw, "public.fb").await, (0, 0, 0));
    assert!(
        record_counts(&raw, "public.fa").await.0 > 0,
        "fa_plain reads it"
    );
}
