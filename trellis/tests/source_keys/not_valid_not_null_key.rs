//! A `KEY (...)` column under Postgres 18's `NOT NULL … NOT VALID` (#2148):
//! `attnotnull` is set, but existing rows may still hold `NULL`, so Trellis
//! reads the column as nullable until `VALIDATE CONSTRAINT` runs. On a plain
//! (`NULLS DISTINCT`) unique index that is define's nullable-key refusal; on
//! a `NULLS NOT DISTINCT` index the `NULL`-keyed rows are rows of their own.
//! Postgres 15–17 have no `NOT VALID` not-null constraint: each case returns
//! early there.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for
//! convergence.

use std::time::Duration;

use crate::support::resume_passes;

use resume_passes::{connect, reconcile_pass, rows, settle, setup, status};
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::TransformStatus;
use trellis::staging::self_check::CheckPageExt;
use trellis::{SelfCheckMode, SelfCheckOutcome, Trellis};

async fn below_18(cluster: &TestCluster) -> bool {
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    let version: i32 = raw
        .query_one("select current_setting('server_version_num')::int", &[])
        .await
        .unwrap()
        .get(0);
    version < 180_000
}

/// The issue's repro: a `NULLS DISTINCT` unique index over a `NOT VALID`
/// not-null column holding two `NULL` rows backs no key.
#[tokio::test]
async fn a_not_valid_key_column_on_a_nulls_distinct_index_is_refused() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (_db, raw, trellis) = setup(&cluster, "select 1", &[]).await;
    raw.batch_execute(
        "create table public.nv (a text, v int not null); \
         insert into public.nv values (null, 1), (null, 2), ('', 3), ('x', 4); \
         alter table public.nv add constraint nv_a_nn not null a not valid; \
         create unique index on public.nv (a)",
    )
    .await
    .unwrap();
    let message = trellis
        .apply("TRANSFORM nv1 FROM public.nv KEY (a) SELECT v AS v")
        .await
        .map(|_| ())
        .expect_err("a NOT VALID column is nullable")
        .to_string();
    assert!(
        message.contains("no unique index backs KEY (a)")
            && message.contains(
                "over a nullable column and not NULLS NOT DISTINCT (a NOT NULL ... NOT VALID \
                 column is nullable until VALIDATE CONSTRAINT)"
            ),
        "{message}"
    );
}

const NND: &str = "create table public.nv (a text, v int not null); \
     insert into public.nv values (null, 1), ('', 3), ('x', 4); \
     alter table public.nv add constraint nv_a_nn not null a not valid; \
     create unique index on public.nv (a) nulls not distinct";
const NV_ONE: &str = "TRANSFORM nv1 FROM public.nv KEY (a) SELECT v AS v";
const ACTUAL: &str =
    "select (a is null)::text, a, v::text from public.nv1 order by 1, a collate \"C\"";
const ORACLE: &str =
    "select (a is null)::text, a, v::text from public.nv order by 1, a collate \"C\"";

/// `nv1` is live, equals its source, and `self_check` finds nothing.
async fn exact(trellis: &Trellis, raw: &Client) {
    let reported = status(trellis, "nv1").await;
    assert_eq!(reported.status, TransformStatus::Live, "{reported:?}");
    assert_eq!(reported.drain_failure, None);
    assert_eq!(rows(raw, ACTUAL).await, rows(raw, ORACLE).await);
    let report = trellis
        .self_check_to_end("nv1", SelfCheckMode::Strict, Duration::from_secs(30))
        .await
        .expect("self_check");
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "{:?}",
        report.outcome
    );
}

/// On a `NULLS NOT DISTINCT` index the `NULL`-keyed row is built, captured
/// apart from the `''` row, and stays exact through `VALIDATE CONSTRAINT`.
#[tokio::test]
async fn a_not_valid_key_column_keys_its_null_rows_until_validated() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (db, mut raw, trellis) = setup(&cluster, NND, &[NV_ONE]).await;
    exact(&trellis, &raw).await;

    // The constraint checks new row versions, so a `NULL`-keyed row can only
    // be deleted.
    for write in [
        "update public.nv set v = 30 where a = ''",
        "delete from public.nv where a is null",
    ] {
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        exact(&trellis, &raw).await;
    }

    // Validated with nothing in flight: the column is NOT NULL from here on.
    raw.batch_execute("alter table public.nv validate constraint nv_a_nn")
        .await
        .unwrap();
    reconcile_pass(&mut raw, &db.pool).await;
    settle(&db.pool, &mut raw).await;
    exact(&trellis, &raw).await;
    for write in [
        "insert into public.nv values (chr(1), 5)",
        "update public.nv set v = 40 where a = ''",
        "delete from public.nv where a = 'x'",
    ] {
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        exact(&trellis, &raw).await;
    }
}

/// A key on a `NULLS DISTINCT` index whose column's validated `NOT NULL` is
/// swapped for a `NOT VALID` one in one transaction (`attnotnull` never
/// changes) no longer backs the key: its readers pause, as for `DROP NOT
/// NULL`.
#[tokio::test]
async fn a_key_column_made_not_valid_pauses() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.nv (a text not null, v int not null); \
         insert into public.nv values ('', 3), ('x', 4); \
         create unique index on public.nv (a)",
        &[NV_ONE],
    )
    .await;
    reconcile_pass(&mut raw, &db.pool).await;
    raw.batch_execute(
        "begin; \
         alter table public.nv alter column a drop not null; \
         alter table public.nv add constraint nv_a_nn not null a not valid; \
         commit",
    )
    .await
    .unwrap();
    reconcile_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "nv1").await;
    assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");
    let reasons = reported.capture_failure.expect("why").reasons;
    assert!(
        reasons.iter().any(|r| r.contains("nulls not distinct")),
        "{reasons:?}"
    );
}
