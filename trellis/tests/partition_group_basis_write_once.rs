//! Research prototype (#1917 X4, candidate A per leaf): a fresh build of a
//! plain aggregate over a partitioned source writes each leaf's absent
//! groups once, from its step 1's snapshot `S1`, in one statement that runs
//! the leaf's `GROUP BY` and the enumeration routed to the leaf under the
//! pinned settings with the session's path after them. These tests force
//! the leaf form's own interleavings. Not for merge.
//!
//! The source is `public.items (id, g, v)`, `RANGE` on `g` in four leaves
//! (`items_p0` holds groups 0-3, and so on), built first under `agg` from
//! empty and seeded through capture. The tests define a second aggregate,
//! `agg2`, over the seeded source: its build runs one transaction per leaf,
//! in oid order (`items_p0` first), then the remainder.
//!
//! To stall a leaf's step 1 after `S1` and before its key-ordered insert
//! reaches a group, a page holds a lower group's `agg2` row (frozen after
//! its lock statement): the build's insert waits on it.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::{Duration, Instant};

use drain_driver::{Driver, Running, int4};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;

const SOURCE: &str = "public.items";
const TARGET: &str = "public.agg2";
const AGG: &str = "TRANSFORM agg FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";
const AGG2: &str = "TRANSFORM agg2 FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n";
const ACTUAL: &str = "select g, total, n from public.agg2 order by g";
const EXPECTED: &str = "select g, sum(v), count(*) from public.items group by g order by g";

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4())]
}

/// `items` `RANGE`-partitioned on `g` into four leaves of four groups each.
fn tree() -> String {
    let leaves: String = (0..4)
        .map(|i| {
            format!(
                "create table public.items_p{i} partition of public.items \
                 for values from ({}) to ({});",
                i * 4,
                i * 4 + 4
            )
        })
        .collect();
    format!(
        "create table public.items (id integer not null, g integer not null, \
         v integer not null, primary key (g, id)) partition by range (g); {leaves}"
    )
}

/// The source captured under `agg`, its tree recorded, `rows` written
/// through capture and drained.
async fn seeded(defaults: &[(&str, &str)], rows: &str) -> Driver {
    let mut d =
        Driver::start_with_database_defaults(defaults, &tree(), &columns(), &[AGG], &[SOURCE])
            .await;
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    d.step(&format!("insert into public.items values {rows}"))
        .await;
    d
}

/// Defines `agg2` and enqueues its build's plan job.
async fn define(d: &Driver) {
    let columns = columns()
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(d.pool(), AGG2, &columns, "public")
        .await
        .expect("define agg2");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start its build");
}

async fn build_frozen(d: &Driver, points: &[PausePoint]) -> Running<()> {
    let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, TARGET)).collect();
    let dsn = d.db.dsn().to_string();
    d.run_frozen(&points, |_shared| async move {
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

async fn hidden(d: &Driver) -> String {
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg2'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// `agg2`'s hidden partials of group `g`: `(n, count, sum, has basis)`.
async fn partials(d: &Driver, g: i32) -> Vec<String> {
    d.rows(&format!(
        "select __n, __count_1, __sum_1, __basis is not null from {} where g = {g}",
        hidden(d).await
    ))
    .await
}

async fn assert_finished(d: &Driver) {
    assert_eq!(
        d.rows(
            "select status, (select count(*) from backfill_chunks \
                             where definition_id = d.id) \
             from transform_definitions d where target_table = 'public.agg2'"
        )
        .await,
        ["(live,0)"],
        "the build committed every leaf and flipped agg2 live"
    );
}

async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "agg2 (left) differs from the oracle (right)"
    );
    assert_eq!(
        d.rows(&format!(
            "select g from {} where __n < 0 or __count_1 < 0",
            hidden(d).await
        ))
        .await,
        Vec::<String>::new(),
        "no partial is negative"
    );
}

/// Ten rows in each of the groups 0, 1, 2, 5, 6, 9 and 13: the first leaf
/// has 0-2, the others one or two each.
const ROWS: &str = "(1, 0, 1), (2, 1, 2), (3, 2, 3), (4, 5, 4), (5, 6, 5), (6, 9, 6), (7, 13, 7), \
     (11, 0, 10), (12, 1, 20), (13, 2, 30), (14, 5, 40), (15, 6, 50), (16, 9, 60), (17, 13, 70)";

/// A page creates group 2's `agg2` row (in the first leaf, a group `S1`
/// sees) after the leaf's `S1` and before its insert reaches it, applying
/// a change `S1` didn't see. The insert finds the row, only locks it, and
/// step 2 rewrites the group from the leaf's rows under a later snapshot. A
/// build that wrote it from `S1` would lose the change
/// (`ab_stragglers_from_s1`). Group 3, new after `S1`, is no group of the
/// build's: its page's row stands.
#[tokio::test]
async fn a_page_creates_a_group_in_the_leaf_inside_step_one() {
    let mut d = seeded(&[], ROWS).await;
    define(&d).await;
    // A page holding group 0, the leaf's first in key order.
    d.write("update public.items set v = 100 where id = 1")
        .await;
    let held = d.seal().await;
    let mut holder = d
        .drain_frozen(held, "holder", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    let holding = holder.reached(PausePoint::AfterEntryLock).await;
    let building = build_frozen(&d, &[]).await;
    d.wait_blocked_behind(holding.backend_pid).await;
    // After the leaf's S1: a new group of the same leaf, drained by a page
    // that creates its row on no basis.
    d.write("insert into public.items values (21, 2, 1000), (24, 3, 1000)")
        .await;
    let after = d.seal().await;
    d.drain(after, "after").await;
    assert_eq!(partials(&d, 2).await, ["(1,1,1000,f)"], "the page made it");
    d.release(&mut holder, PausePoint::AfterEntryLock).await;
    holder.finish().await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, 2).await,
        ["(3,3,1033,t)"],
        "the straggler, rewritten from step 2's snapshot"
    );
    assert_eq!(partials(&d, 3).await, ["(1,1,1000,f)"], "the page's own");
    assert_eq!(partials(&d, 0).await, ["(2,2,110,t)"]);
    assert_oracle(&mut d).await;
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
}

/// A page creates `agg2` rows for group 5 (second leaf) and group 1 (first
/// leaf) while the build waits before the first leaf's step 1. The first
/// leaf's transaction rewrites group 1 (a straggler) and leaves group 5's
/// row exactly as the page made it: it routes elsewhere. The second leaf's
/// transaction then rewrites group 5. A leaf whose enumeration isn't routed
/// (`ab_leaf_unrouted`) locks group 5 as a straggler of the first leaf and
/// zeroes it from that leaf's rows, and each later leaf zeroes the groups
/// of the leaves before it.
#[tokio::test]
async fn a_leaf_build_leaves_another_leafs_groups_alone() {
    let mut d = seeded(&[], ROWS).await;
    define(&d).await;
    let mut building = build_frozen(
        &d,
        &[
            PausePoint::AfterRebuildEnumerate,
            PausePoint::BetweenRebuildStructures,
        ],
    )
    .await;
    building.reached(PausePoint::AfterRebuildEnumerate).await;
    d.step("insert into public.items values (22, 5, 1000), (23, 1, 1000)")
        .await;
    assert_eq!(partials(&d, 5).await, ["(1,1,1000,f)"]);
    assert_eq!(partials(&d, 1).await, ["(1,1,1000,f)"]);
    d.release(&mut building, PausePoint::AfterRebuildEnumerate)
        .await;
    building.reached(PausePoint::BetweenRebuildStructures).await;
    assert_eq!(
        partials(&d, 1).await,
        ["(3,3,1022,t)"],
        "the first leaf's straggler, rewritten"
    );
    assert_eq!(
        partials(&d, 5).await,
        ["(1,1,1000,f)"],
        "another leaf's group, untouched by the first leaf's transaction"
    );
    assert_eq!(
        partials(&d, 9).await,
        Vec::<String>::new(),
        "a later leaf's group, not yet written"
    );
    // Released, the point lets every later leaf through.
    d.release(&mut building, PausePoint::BetweenRebuildStructures)
        .await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(partials(&d, 5).await, ["(3,3,1044,t)"]);
    assert_eq!(partials(&d, 1).await, ["(3,3,1022,t)"]);
    assert_oracle(&mut d).await;
}

/// Group 2's rows (first leaf) deleted before the build, the change still
/// undrained at the leaf's `S1`: only the ring's old images, routed to the
/// leaf, name it. Step 1 inserts it at zero with basis `S1`, and the
/// delete's page is refused after the build (`ab_enum_no_ring`: it lands on
/// a fresh row and goes negative).
#[tokio::test]
async fn a_group_vacated_in_the_leaf_by_an_undrained_change_is_zeroed() {
    let mut d = seeded(&[], ROWS).await;
    d.write("delete from public.items where g = 2").await;
    let undrained = d.seal().await;
    define(&d).await;
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"], "named by the ring");
    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"], "refused");
    assert_oracle(&mut d).await;
}

/// The leaf form's settings: a database whose defaults are another
/// `DateStyle`, `TimeZone` and `search_path` (every Trellis session pins
/// its own over them), a `date`-ranged source whose bounds are date
/// literals, and an operator in `public` that would misroute under a path
/// listing `public` first. The per-leaf build is exact.
#[tokio::test]
async fn a_leaf_build_under_other_database_defaults_routes_exactly() {
    let ddl = "create table public.items (id integer not null, g date not null, \
                   v integer not null, primary key (g, id)) partition by range (g); \
               create table public.items_p0 partition of public.items \
                   for values from ('2020-01-01') to ('2020-03-01'); \
               create table public.items_p1 partition of public.items \
                   for values from ('2020-03-01') to ('2020-06-30'); \
               create table public.items_d partition of public.items default; \
               create function public.reversed_ge(date, date) returns boolean \
                   language sql immutable as 'select $1 < $2'; \
               create operator public.>= (leftarg = date, rightarg = date, \
                   procedure = public.reversed_ge)";
    let date = ValueType::Other(trellis::defs::pg_type::PgType::Date);
    let cols = vec![("id", int4()), ("g", date), ("v", int4())];
    let mut d = Driver::start_with_database_defaults(
        &[
            ("datestyle", "German, DMY"),
            ("timezone", "America/Los_Angeles"),
            ("search_path", "public, pg_catalog"),
        ],
        ddl,
        &cols,
        &["TRANSFORM agg FROM public.items GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total, COUNT(*) AS n"],
        &[SOURCE],
    )
    .await;
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    d.step(
        "insert into public.items select i, date '2020-01-01' + (i % 300), i \
         from generate_series(1, 900) i",
    )
    .await;
    let columns = cols.into_iter().map(|(n, t)| (n.to_string(), t)).collect();
    trellis::defs::install_definition(d.pool(), AGG2, &columns, "public")
        .await
        .expect("define agg2");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start its build");
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    d.settle().await;
    let have = d
        .rows("select g::text, total, n from public.agg2 order by g")
        .await;
    let want = d
        .rows("select g::text, sum(v), count(*) from public.items group by g order by g")
        .await;
    assert_eq!(have.len(), 300);
    assert_eq!(have, want, "agg2 (left) differs from the oracle (right)");
}
