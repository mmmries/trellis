//! A staging worker stepped by hand for the resume tests: whole
//! passes, the capture half of one, backfill jobs and drains, each called
//! directly. Nothing polls for convergence.

#![allow(dead_code)]

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::Trellis;
use trellis::defs::TransformStatus;

#[path = "pipeline.rs"]
mod pipeline;

#[allow(unused_imports)]
pub use pipeline::{capture_pass, connect, definer, reconcile_pass, settle};

pub const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;

pub async fn status(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// Alternates staging-worker passes, builds and drains until `targets` are
/// live: a bounded number of explicit steps.
pub async fn bring_live(
    raw: &mut Client,
    pool: &trellis::pool::Pool,
    trellis: &Trellis,
    targets: &[&str],
) {
    for _ in 0..10 {
        reconcile_pass(raw, pool).await;
        trellis::staging::build::settle_builds(pool).await;
        settle(pool, raw).await;
        let mut all = true;
        for target in targets {
            let reported = status(trellis, target).await;
            assert_eq!(reported.backfill_failure, None, "{target}");
            assert_eq!(reported.drain_failure, None, "{target}");
            assert_eq!(reported.held_keys, None, "{target}");
            all &= reported.status == TransformStatus::Live;
        }
        if all {
            return;
        }
    }
    panic!("not every definition went live");
}

pub async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// The source every case starts from: `public.c (id pk, pid, amount)`, 6
/// rows, indexed by `pid`.
pub const SOURCE: &str = "create table public.c (id int primary key, pid int not null, amount int); \
     create index on public.c (pid); \
     insert into public.c select i, i % 3, i from generate_series(1, 6) i";

/// `public.c` recreated with the two-column key `(pid, id)`, holding other
/// rows.
pub const RECREATED: &str = "drop table public.c; \
     create table public.c (id int not null, pid int not null, amount int, \
         primary key (pid, id)); \
     create index on public.c (pid); \
     insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)";

/// A database with `source_ddl` run and `definitions` live over it.
pub async fn setup(
    cluster: &TestCluster,
    source_ddl: &str,
    definitions: &[&str],
) -> (testkit::TestDatabase, Client, Trellis) {
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(source_ddl).await.expect("the sources");
    let trellis = definer(db.dsn()).await;
    for text in definitions {
        trellis.apply(text).await.expect(text);
    }
    let targets: Vec<&str> = definitions
        .iter()
        .map(|t| t.split_whitespace().nth(1).expect("a target"))
        .collect();
    bring_live(&mut raw, &db.pool, &trellis, &targets).await;
    (db, raw, trellis)
}

/// Asserts the ring holds a change not yet drained: the one a case staged
/// under the old key before the drop.
pub async fn assert_staged(raw: &Client) {
    assert!(
        trellis::staging::has_pending(raw)
            .await
            .expect("has_pending"),
        "a change is staged"
    );
}
