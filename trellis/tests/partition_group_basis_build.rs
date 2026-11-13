//! A fresh build of a group-basis aggregate over a partitioned source
//! (ADR-0002, "An aggregate's build is one absolute write per structure"):
//! one transaction per leaf of the recorded tree, each writing the leaf's
//! groups once. Its first statement runs the leaf's `GROUP BY` and the
//! enumeration routed to the leaf under one set of settings (the pinned
//! settings with the session's path after them), inserts each group with no
//! hidden row and locks the rest, which a later statement rewrites from a
//! new snapshot.
//!
//! The source is `public.items (id, g, v)`, `RANGE` on `g` in four leaves
//! (`items_p0` holds groups 0-3, and so on), captured under `agg` and seeded
//! through capture. Each test defines a second aggregate, `agg2`, over the
//! seeded source: its build runs one transaction per leaf, in oid order
//! (`items_p0` first), then the remainder. Every test runs the plan job by
//! hand, frozen at its pause points where the scenario needs it; the one
//! wait is for the build to queue on a pause the test holds.

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

/// Runs a capture pass, which records the source's tree.
async fn record_tree(d: &mut Driver) {
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
}

/// The source captured under `agg`, its tree recorded, and `rows` written
/// through capture and drained.
async fn seeded(rows: &str) -> Driver {
    let mut d = Driver::start(&tree(), &columns(), &[AGG], &[SOURCE]).await;
    record_tree(&mut d).await;
    d.step(&format!("insert into public.items values {rows}"))
        .await;
    d
}

/// Defines `agg2` and starts its build's plan job.
async fn define(d: &Driver, cols: Vec<(&'static str, ValueType)>) {
    let columns = cols.into_iter().map(|(n, t)| (n.to_string(), t)).collect();
    trellis::defs::install_definition(d.pool(), AGG2, &columns, "public")
        .await
        .expect("define agg2");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start its build");
}

/// Claims `agg2`'s plan job and runs it in its own task, on a pool of its
/// own, frozen at each of `points` once reached.
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

/// Freezes the build inside the first leaf's first statement, once its
/// snapshot is taken and before its insert has reached any group. Only a
/// leaf's write-once form reaches this point.
async fn build_inside_the_first_leafs_write(d: &Driver) -> Running<()> {
    let mut building = build_frozen(d, &[PausePoint::InsideBuildWrite]).await;
    building.reached(PausePoint::InsideBuildWrite).await;
    d.wait_blocked(1).await;
    building
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
                             where definition_id = d.id), \
                    (select string_agg(last_error, '; ') from backfill_chunks) \
             from transform_definitions d where target_table = 'public.agg2'"
        )
        .await,
        ["(live,0,)"],
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

/// Two rows in each of the groups 0, 1, 2, 5, 6, 9 and 13: the first leaf
/// has 0-2, the others one or two each.
const ROWS: &str = "(1, 0, 1), (2, 1, 2), (3, 2, 3), (4, 5, 4), (5, 6, 5), (6, 9, 6), (7, 13, 7), \
     (11, 0, 10), (12, 1, 20), (13, 2, 30), (14, 5, 40), (15, 6, 50), (16, 9, 60), (17, 13, 70)";

/// A page creates group 2's `agg2` row (a group of the first leaf its
/// first statement's snapshot sees) after that snapshot and before the
/// insert reaches it, applying a change the snapshot didn't see. The insert
/// finds the row and only locks it, and the second statement rewrites the
/// group from a later snapshot; writing it from the first would lose the
/// change. Group 3, new after the snapshot, is no group of the build's: its
/// page's row stands.
#[tokio::test]
async fn a_page_creates_a_group_in_the_leaf_inside_its_first_statement() {
    let mut d = seeded(ROWS).await;
    define(&d, columns()).await;
    let mut building = build_inside_the_first_leafs_write(&d).await;
    d.write("insert into public.items values (21, 2, 1000), (24, 3, 1000)")
        .await;
    let after = d.seal().await;
    d.drain(after, "after").await;
    assert_eq!(partials(&d, 2).await, ["(1,1,1000,f)"], "the page made it");
    d.release(&mut building, PausePoint::InsideBuildWrite).await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, 2).await,
        ["(3,3,1033,t)"],
        "locked, and rewritten from the second snapshot"
    );
    assert_eq!(partials(&d, 3).await, ["(1,1,1000,f)"], "the page's own");
    assert_eq!(partials(&d, 0).await, ["(2,2,11,t)"]);
    assert_oracle(&mut d).await;
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
}

/// Pages create `agg2` rows for group 5 (second leaf) and group 1 (first
/// leaf) before the first leaf's first statement. Between the leaves'
/// transactions, the first leaf's has rewritten group 1 (a straggler) and
/// left group 5's row exactly as the page made it, since it routes to
/// another leaf; the second leaf's transaction then rewrites group 5. A
/// leaf whose enumeration isn't routed (`gb_build_leaf_unrouted`) locks
/// group 5 as a straggler of the first leaf and rewrites it from that
/// leaf's rows, to zero.
#[tokio::test]
async fn a_leaf_build_leaves_another_leafs_groups_alone() {
    let mut d = seeded(ROWS).await;
    define(&d, columns()).await;
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
/// undrained at the leaf's first statement: only the ring's old images,
/// routed to the leaf, name it. The statement inserts it at zero with its
/// snapshot as the basis, and the delete's page, drained after the build,
/// is refused. Without the ring branch the page would land on a fresh row
/// and drive it negative.
#[tokio::test]
async fn a_group_vacated_in_the_leaf_by_an_undrained_change_is_zeroed() {
    let mut d = seeded(ROWS).await;
    d.write("delete from public.items where g = 2").await;
    let undrained = d.seal().await;
    define(&d, columns()).await;
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"], "named by the ring");
    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, 2).await, ["(0,0,0,t)"], "refused");
    assert_oracle(&mut d).await;
}

/// The settings a leaf's first statement routes and reads under: a database
/// whose defaults are another `DateStyle`, `TimeZone` and `search_path`
/// (every Trellis session sets its own over them), a `date`-ranged source
/// whose bounds are date literals, and an operator in `public` that would
/// misroute under a path listing `public`. The build takes the leaf's
/// write-once form (it reaches the pause inside it) and is exact.
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
        &[AGG],
        &[SOURCE],
    )
    .await;
    record_tree(&mut d).await;
    d.step(
        "insert into public.items select i, date '2020-01-01' + (i % 300), i \
         from generate_series(1, 900) i",
    )
    .await;
    define(&d, cols).await;
    let mut building = build_inside_the_first_leafs_write(&d).await;
    d.release(&mut building, PausePoint::InsideBuildWrite).await;
    building.finish().await;
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
