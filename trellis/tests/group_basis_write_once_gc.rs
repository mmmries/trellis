//! Research prototype (#1917 X3, candidate A): the zero-group GC must not
//! remove a plain aggregate's hidden row between a fresh build's snapshot
//! `S1` and its commit (X1's counterexample, on the engine). One test, in a
//! binary of its own: it flips a process-wide test switch
//! (`retire::X3_GC_IGNORES_BUILD`) that would race any other test.
//!
//! `c_a` inserts a row into group 9 and commits before `S1`; `c_b` deletes
//! it after `S1`. A page drains both while the build's step 1 waits on a
//! lower group a frozen page holds: group 9's hidden row is `+1 −1 = 0` on
//! no basis. With the skip, the GC leaves it, the build only locks it, and
//! step 2 rewrites it to zero. Without it, the GC collects it, and the
//! build inserts group 9 from `S1` with `c_a`'s row, which nothing retracts.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::sync::atomic::Ordering;
use std::time::Duration;

use drain_driver::{Driver, Running, int4};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::staging::retire::X3_GC_IGNORES_BUILD;

const SRC: &str = "public.src";
const TARGET: &str = "public.agg";
const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v AS w";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";
const ACTUAL: &str = "select g, total, n from public.agg order by g";
const EXPECTED: &str = "select g, sum(v), count(*) from public.src group by g order by g";

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4())]
}

async fn build(d: &Driver) -> Running<()> {
    let dsn = d.db.dsn().to_string();
    d.run_frozen(&[], |_shared| async move {
        let pool = trellis::pool::Pool::new(&Config::from_dsn(dsn).expect("valid dsn"))
            .expect("the build's own pool");
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "build",
            1,
            &[chunk_queue::KIND_PLAN],
        )
        .await
        .expect("claim the plan job");
        assert_eq!(claimed.len(), 1, "one plan job to claim");
        build::run_claimed(&pool, &claimed[0], "build", &OPTIONS).await;
        Ok(())
    })
    .await
}

/// The scenario, with the GC's skip under a build off when `ignore`.
/// Returns the target, the oracle, group 9's partials after the build, and
/// how many rows the GC pass collected.
async fn scenario(ignore: bool) -> (Vec<String>, Vec<String>, Vec<String>, u64) {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, v integer); \
         create index on public.src (g)",
        &columns(),
        &[ONE],
        &[SRC],
    )
    .await;
    d.step("insert into public.src values (1, 5, 10)").await;
    let cols = columns()
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(d.pool(), AGG, &cols, "public")
        .await
        .expect("define the aggregate");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the build");
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    let hidden = format!("{DEFAULT_SCHEMA}.agg_{id}");
    let partials =
        format!("select __n, __count_1, __sum_1, __basis is not null from {hidden} where g = 9");
    // A page holding group 5.
    d.write("update public.src set v = 11 where id = 1").await;
    let held = d.seal().await;
    let mut holder = d
        .drain_frozen(held, "holder", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    let holding = holder.reached(PausePoint::AfterEntryLock).await;
    // c_a, before S1.
    d.write("insert into public.src values (2, 9, 3)").await;
    let building = build(&d).await;
    d.wait_blocked_behind(holding.backend_pid).await;
    // c_b, after S1; a page drains both.
    d.write("delete from public.src where id = 2").await;
    let both = d.seal().await;
    d.drain(both, "both").await;
    assert_eq!(
        d.rows(&partials).await,
        ["(0,0,0,f)"],
        "a zero row, no basis"
    );
    X3_GC_IGNORES_BUILD.store(ignore, Ordering::Relaxed);
    let collected = d.collect_tombstones().await;
    X3_GC_IGNORES_BUILD.store(false, Ordering::Relaxed);
    d.release(&mut holder, PausePoint::AfterEntryLock).await;
    holder.finish().await;
    building.finish().await;
    let after_build = d.rows(&partials).await;
    d.settle().await;
    (
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        after_build,
        collected,
    )
}

#[tokio::test]
async fn the_zero_group_gc_must_skip_a_definition_under_a_fresh_build() {
    let (actual, expected, group, collected) = scenario(false).await;
    assert_eq!(collected, 0, "the skip: the GC collected nothing");
    assert_eq!(group, ["(0,0,0,t)"], "the straggler, rewritten to zero");
    assert_eq!(actual, expected, "exact with the skip");

    let (actual, expected, group, collected) = scenario(true).await;
    assert!(collected >= 1, "without the skip the GC collects group 9");
    assert_eq!(
        group,
        ["(1,1,3,t)"],
        "the build inserted group 9 from S1, with c_a's row"
    );
    assert_ne!(
        actual, expected,
        "wrong without the skip: the counterexample"
    );
    assert!(actual.contains(&"(9,3,1)".to_string()), "{actual:?}");
}
