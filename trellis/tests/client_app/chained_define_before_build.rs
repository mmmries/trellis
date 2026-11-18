//! A define may chain off an upstream that hasn't been built yet
//! (`waiting_to_backfill`), and is refused only while the upstream's build
//! runs (`TransformNotLive`). A test database or a fresh `ecto.setup`
//! defines every migration's transforms with no worker running.
//!
//! - a chain defined with no worker (1-1, an aggregate over it, an aggregate
//!   over that, a `JOIN` reading a target) builds once a worker starts, and
//!   every target equals its query
//!   (`a_chain_defined_before_any_build_converges_once_a_worker_starts`)
//! - a define racing the upstream's build start waits for the start's commit
//!   and is refused
//!   (`a_define_racing_the_upstream_build_start_waits_for_it_and_is_refused`)
//! - a define racing a pause of the upstream that holds its row waits for it
//!   and is refused
//!   (`a_define_waits_for_a_pause_holding_its_upstream_row_and_is_refused`)

use crate::support::pipeline;
use crate::support::wait_live::wait_for_live;

use pipeline::{connect, definer};
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::catalog::CatalogError;
use trellis::{Config, TransformStatus, Trellis, TrellisError, TrellisOptions};

const TIMEOUT: Duration = Duration::from_secs(60);

async fn running(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("start the engine")
}

async fn status(trellis: &Trellis, target: &str) -> TransformStatus {
    trellis
        .status(target)
        .await
        .expect("read status")
        .expect("the definition is registered")
        .status
}

/// The rows of `actual` and `expected` that the other lacks, as text.
async fn difference(raw: &Client, actual: &str, expected: &str) -> Vec<String> {
    raw.query(
        &format!(
            "select format('%s %s', side, r) from ( \
                 (select 'target' as side, r::text from (({actual}) except all ({expected})) r) \
                 union all \
                 (select 'query', r::text from (({expected}) except all ({actual})) r)) z \
             order by 1"
        ),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("compare {actual}: {e}"))
    .iter()
    .map(|row| row.get(0))
    .collect()
}

async fn self_check_clean(trellis: &Trellis, target: &str) {
    use trellis::staging::self_check::CheckPageExt;
    let report = trellis
        .self_check_to_end(target, trellis::SelfCheckMode::Strict, TIMEOUT)
        .await
        .unwrap_or_else(|err| panic!("self_check {target}: {err}"));
    assert_eq!(
        report.outcome,
        trellis::SelfCheckOutcome::Converged,
        "{target}"
    );
}

#[tokio::test]
async fn a_chain_defined_before_any_build_converges_once_a_worker_starts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id bigint primary key, g bigint, a bigint); \
         create table public.groups (g bigint primary key, name text); \
         insert into public.orders select i, i % 4, i from generate_series(1, 200) i; \
         insert into public.groups select g, 'g' || g from generate_series(0, 3) g",
    )
    .await
    .expect("sources");

    // No worker: every definition stays `waiting_to_backfill`.
    let d = definer(db.dsn()).await;
    for statement in [
        "TRANSFORM o1 FROM orders SELECT g AS g, a * 2 AS twice",
        "TRANSFORM by_g FROM o1 GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, SUM(twice) AS total, COUNT(*) AS n",
        "TRANSFORM by_n FROM by_g GROUP BY n WITHOUT SOURCE INDEX \
         SELECT n AS n, SUM(total) AS total",
        "TRANSFORM named FROM by_g JOIN groups ON by_g.g = groups.g GROUP BY groups.name \
         WITHOUT SOURCE INDEX SELECT groups.name AS name, SUM(by_g.total) AS total",
    ] {
        d.apply(statement).await.expect(statement);
    }
    for target in ["o1", "by_g", "by_n", "named"] {
        assert_eq!(
            status(&d, target).await,
            TransformStatus::WaitingToBackfill,
            "{target}"
        );
    }
    d.shutdown().await.expect("shut the definer down");

    let t = running(db.dsn()).await;
    for target in ["o1", "by_g", "by_n", "named"] {
        wait_for_live(&t, target).await;
    }
    // A write after the builds reaches the whole chain too.
    raw.batch_execute("insert into public.orders values (201, 1, 1000)")
        .await
        .expect("a later write");
    let token = t.watermark_token().await.expect("token");
    t.await_converged(token, TIMEOUT)
        .await
        .expect("await_converged");

    for (target, query) in [
        (
            "select id, g, twice from public.o1",
            "select id, g, a * 2 from public.orders",
        ),
        (
            "select g, total, n from public.by_g",
            "select g, sum(a * 2), count(*) from public.orders group by g",
        ),
        (
            "select n, total from public.by_n",
            "select n, sum(total) from (select count(*) n, sum(a * 2) total \
             from public.orders group by g) x group by n",
        ),
        (
            "select name, total from public.named",
            "select gr.name, sum(o.a * 2) from public.orders o \
             join public.groups gr on gr.g = o.g group by gr.name",
        ),
    ] {
        assert_eq!(difference(&raw, target, query).await, Vec::<String>::new());
    }
    for target in ["o1", "by_g", "by_n", "named"] {
        self_check_clean(&t, target).await;
    }
    t.shutdown().await.expect("shutdown");
}

/// Waits until some backend of this database waits on a lock of `locktype`.
async fn wait_for_lock_wait(raw: &Client, locktype: &str, what: &str) {
    testkit::wait_until(what, TIMEOUT, async || {
        raw.query_one(
            "select exists (select 1 from pg_locks where locktype = $1 and not granted \
               and (database is null \
                    or database = (select oid from pg_database where datname = current_database())))",
            &[&locktype],
        )
        .await
        .expect("read pg_locks")
        .get(0)
    })
    .await;
}

/// The race between an upstream's build start and a reader's define, set up
/// rather than raced. The upstream's start is held after it has locked the
/// upstream's definition row (a second connection holds the upstream's
/// target, which the start reads). A define reading the upstream then waits
/// for the start's commit, reads `backfilling` and is refused before any DDL.
/// The other order, a define committed before the start, is the chain test
/// above.
#[tokio::test]
async fn a_define_racing_the_upstream_build_start_waits_for_it_and_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id bigint primary key, v bigint); \
         insert into public.src select i, i from generate_series(1, 50) i",
    )
    .await
    .expect("source");
    let d = definer(db.dsn()).await;
    d.apply("TRANSFORM up FROM src SELECT v AS v")
        .await
        .expect("up");

    let mut holder = connect(db.dsn()).await;
    let hold = holder.transaction().await.expect("begin the hold");
    hold.batch_execute("lock table public.up in access exclusive mode")
        .await
        .expect("hold up's target");
    let pool = d.pool().clone();
    let mut pass_client = connect(db.dsn()).await;
    let pass = tokio::spawn(async move {
        pipeline::reconcile_pass(&mut pass_client, &pool).await;
    });
    wait_for_lock_wait(&raw, "relation", "up's build start to wait on its target").await;
    assert_eq!(status(&d, "up").await, TransformStatus::WaitingToBackfill);

    let definer_dsn = db.dsn().to_string();
    let define = tokio::spawn(async move {
        let d2 = definer(&definer_dsn).await;
        let result = d2.apply("TRANSFORM down FROM up SELECT v + 1 AS w").await;
        d2.shutdown().await.expect("shut the second definer down");
        result
    });
    wait_for_lock_wait(&raw, "advisory", "the define to wait on the start").await;
    hold.rollback().await.expect("release up's target");
    pass.await.expect("the pass");
    assert_eq!(status(&d, "up").await, TransformStatus::Backfilling);

    match define
        .await
        .expect("the define")
        .expect_err("up's build is running")
    {
        TrellisError::Catalog(CatalogError::TransformNotLive { transform, status }) => {
            assert_eq!(transform, "up");
            assert_eq!(status, TransformStatus::Backfilling);
        }
        other => panic!("expected TransformNotLive, got {other:?}"),
    }
    let down_exists: bool = raw
        .query_one("select to_regclass('public.down') is not null", &[])
        .await
        .expect("probe down")
        .get(0);
    assert!(!down_exists, "the refusal comes before any DDL");
    d.shutdown().await.expect("shut the definer down");
}

/// The define's `FOR SHARE` on its upstream's definition row, which the
/// column-pause lock doesn't cover: a pause that takes no column-pause lock
/// (`lifecycle::pause_many`, as a restore's response does) holds the row
/// `FOR UPDATE` while it moves the upstream out of `waiting_to_backfill`. A
/// define reading the upstream waits for that commit and is refused, rather
/// than reading the old status and chaining off a paused upstream.
#[tokio::test]
async fn a_define_waits_for_a_pause_holding_its_upstream_row_and_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.src (id bigint primary key, v bigint)")
        .await
        .expect("source");
    let d = definer(db.dsn()).await;
    d.apply("TRANSFORM up FROM src SELECT v AS v")
        .await
        .expect("up");

    // The pause, by hand: the row `FOR UPDATE`, then the status.
    let mut pauser = connect(db.dsn()).await;
    let pause = pauser.transaction().await.expect("begin the pause");
    pause
        .batch_execute(
            "select 1 from transform_definitions where target_table = 'public.up' for update",
        )
        .await
        .expect("lock up's row");

    let definer_dsn = db.dsn().to_string();
    let define = tokio::spawn(async move {
        let d2 = definer(&definer_dsn).await;
        let result = d2.apply("TRANSFORM down FROM up SELECT v + 1 AS w").await;
        d2.shutdown().await.expect("shut the second definer down");
        result
    });
    wait_for_lock_wait(&raw, "transactionid", "the define to wait on up's row").await;
    pause
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where target_table = 'public.up'",
        )
        .await
        .expect("pause up");
    pause.commit().await.expect("commit the pause");

    match define.await.expect("the define").expect_err("up is paused") {
        TrellisError::Catalog(CatalogError::TransformNotLive { transform, status }) => {
            assert_eq!(transform, "up");
            assert_eq!(status, TransformStatus::Paused);
        }
        other => panic!("expected TransformNotLive, got {other:?}"),
    }
    d.shutdown().await.expect("shut the definer down");
}
