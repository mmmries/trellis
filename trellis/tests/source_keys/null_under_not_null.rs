//! A `NULL` in a key column Postgres reads as `NOT NULL` (#2217): Postgres
//! 18's `SET NOT NULL` on a partitioned table validates over a `NOT VALID`,
//! `NULL`-holding partition (#2216), so the root reads `NOT NULL` while the
//! partition holds a `NULL`, and the planner folds `k is null` to false. A
//! key Trellis renders raw refuses at define and `RESUME`, pauses a build,
//! and pauses its readers when a change to the row reaches the drain, each
//! with T0253. An encoded key keys the `NULL` row, and a build finds it.
//! Postgres 15–17 can't produce the state: each case returns early there.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for
//! convergence.

use crate::support::resume_passes;

use resume_passes::{connect, reconcile_pass, rows, settle, setup, status};
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::TransformStatus;

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

/// The #2216 state: `public.p`'s root reads `k` as `NOT NULL`, its leaf
/// `p2` reads it as nullable and holds one `NULL`. The key `(a, k)` is
/// backed by a `NULLS NOT DISTINCT` index, so a define records it encoded.
const BROKEN: &str = "create table public.p (a int not null, k text, v int not null) \
         partition by list (a); \
     create table public.p1 partition of public.p for values in (1); \
     create table public.p2 partition of public.p for values in (2); \
     create unique index on public.p (a, k) nulls not distinct; \
     insert into public.p values (1, 'x', 10), (1, '', 11), (2, 'y', 20), (2, null, 21)";

const BREAK: &str = "alter table public.p2 add constraint p2_k_nn not null k not valid; \
     alter table public.p alter column k set not null";

const P_ONE: &str = "TRANSFORM p_sums FROM public.p KEY (a, k) GROUP BY a WITHOUT SOURCE INDEX \
     SELECT COUNT(*) AS n, SUM(v) AS total";
const P_ONE_ACTUAL: &str = "select a::text, n::text, total::text from public.p_sums order by a";
const P_ONE_ORACLE: &str =
    "select a::text, count(*)::text, sum(v)::text from public.p group by a order by a";

/// What a definition from before the key encoding was recorded leaves on a
/// `NULLS NOT DISTINCT` key over columns read as `NOT NULL`: a raw record.
const RAW: &str = "update trellis.definition_keys set encoded_columns = '{}' \
     where table_name = 'public.p'";

/// The paused definition's `capture_failure.error`.
async fn pause_error(trellis: &trellis::Trellis, target: &str) -> String {
    let reported = status(trellis, target).await;
    assert_eq!(
        reported.status,
        TransformStatus::Paused,
        "{target}: {reported:?}"
    );
    reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target} has a capture failure"))
        .error
}

async fn assert_state(raw: &Client) {
    // The planner folds `k is null` away; `num_nulls` sees the row.
    let folded: i64 = raw
        .query_one("select count(*) from public.p where k is null", &[])
        .await
        .unwrap()
        .get(0);
    let seen: i64 = raw
        .query_one("select count(*) from public.p where num_nulls(k) > 0", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!((folded, seen), (0, 1), "the #2216 state");
}

/// Define over a raw key in the #2216 state refuses with T0253: a declared
/// `KEY` on a plain unique index, and a `PRIMARY KEY` Postgres 18 added
/// over the state (study row 22).
#[tokio::test]
async fn define_refuses_a_raw_key_over_the_state() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (_db, raw, trellis) = setup(&cluster, "select 1", &[]).await;
    raw.batch_execute(&BROKEN.replace(" nulls not distinct", ""))
        .await
        .unwrap();
    raw.batch_execute(BREAK).await.unwrap();
    assert_state(&raw).await;
    let refused = trellis
        .apply(P_ONE)
        .await
        .map(|_| ())
        .expect_err("a raw key over a NULL")
        .to_string();
    assert!(refused.starts_with("T0253"), "{refused}");

    raw.batch_execute("alter table public.p add primary key (a, k)")
        .await
        .expect("Postgres 18 adds a primary key over the NULL");
    let refused = trellis
        .apply("TRANSFORM p_pk FROM public.p GROUP BY a WITHOUT SOURCE INDEX SELECT COUNT(*) AS n")
        .await
        .map(|_| ())
        .expect_err("a primary key over a NULL")
        .to_string();
    assert!(refused.starts_with("T0253"), "{refused}");
}

/// An encoded key over the state keys the `NULL` row, and a build finds it
/// though the planner folds `k is null` away: the build's own `NULL`-key
/// pass reads `num_nulls`.
#[tokio::test]
async fn an_encoded_key_builds_the_null_row_under_the_state() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (db, mut raw, trellis) = setup(&cluster, BROKEN, &[P_ONE]).await;
    raw.batch_execute(BREAK).await.unwrap();
    assert_state(&raw).await;
    trellis.request_backfill("public.p").await.expect("rebuild");
    resume_passes::bring_live(&mut raw, &db.pool, &trellis, &["p_sums"]).await;
    assert_eq!(
        rows(&raw, P_ONE_ACTUAL).await,
        rows(&raw, P_ONE_ORACLE).await
    );
    raw.batch_execute("update public.p set v = 22 where a = 2 and k = 'y'")
        .await
        .unwrap();
    settle(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, P_ONE_ACTUAL).await,
        rows(&raw, P_ONE_ORACLE).await
    );
    assert_eq!(
        status(&trellis, "p_sums").await.status,
        TransformStatus::Live
    );
}

/// A raw record over the state when a build starts (the state reached
/// between define and the build): the build pauses with T0253 before it
/// reads a row, and `RESUME` refuses with it.
#[tokio::test]
async fn a_build_over_a_raw_key_pauses_and_resume_refuses() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    let (db, mut raw, trellis) = setup(&cluster, BROKEN, &[]).await;
    trellis.apply(P_ONE).await.expect("define");
    raw.batch_execute(BREAK).await.unwrap();
    raw.batch_execute(RAW).await.unwrap();
    reconcile_pass(&mut raw, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    reconcile_pass(&mut raw, &db.pool).await;
    let error = pause_error(&trellis, "p_sums").await;
    assert!(error.contains("T0253"), "{error}");
    let refused = trellis
        .apply("RESUME TRANSFORM p_sums")
        .await
        .map(|_| ())
        .expect_err("RESUME re-validates")
        .to_string();
    assert!(refused.contains("T0253"), "{refused}");
}

/// A raw record over the state, live: a change that reaches the `NULL` row
/// (Postgres 18 accepts its `DELETE`, and an update that sets its key, but
/// not one that keeps the `NULL`, study row 21) pauses the table's readers
/// with T0253 before the drain applies it, and the row's real `''`
/// neighbour is left alone.
#[tokio::test]
async fn a_change_to_the_null_row_pauses_its_readers() {
    let cluster = TestCluster::start();
    if below_18(&cluster).await {
        return;
    }
    for write in [
        "delete from public.p where a = 2 and num_nulls(k) > 0",
        "update public.p set k = 'z' where a = 2 and num_nulls(k) > 0",
    ] {
        let (db, mut raw, trellis) = setup(&cluster, BROKEN, &[P_ONE]).await;
        raw.batch_execute(BREAK).await.unwrap();
        raw.batch_execute(RAW).await.unwrap();
        reconcile_pass(&mut raw, &db.pool).await;
        let before = rows(&raw, P_ONE_ACTUAL).await;
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        let error = pause_error(&trellis, "p_sums").await;
        assert!(error.starts_with("T0253"), "{write}: {error}");
        assert_eq!(
            rows(&raw, P_ONE_ACTUAL).await,
            before,
            "{write}: nothing applied"
        );
    }
}
