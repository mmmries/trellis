//! Research prototype (#1917 X3, candidate A): a fresh build of a plain
//! aggregate writes each absent group once, from its first statement's
//! snapshot `S1`, and only locks a group that already has a hidden row (a
//! straggler), which a second statement rewrites from a later snapshot.
//! These tests force A's own interleavings. Not for merge.
//!
//! To stall the build inside its first statement, after `S1` and before its
//! key-ordered insert reaches a group, a page holds a lower group's hidden
//! row (frozen after its lock statement): the build's insert waits on it.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::time::Duration;

use drain_driver::{Driver, Running, int4};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;

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

/// `public.src (id, g, v)` captured, with an index on `g` when `indexed`
/// (step 2 then reads its stragglers by index probes; without one, by one
/// `GROUP BY`), and `rows` drained into it.
async fn seeded(rows: &str, indexed: bool) -> Driver {
    let index = if indexed {
        "; create index on public.src (g)"
    } else {
        ""
    };
    let mut d = Driver::start(
        &format!("create table public.src (id integer primary key, g integer, v integer){index}"),
        &columns(),
        &[ONE],
        &[SRC],
    )
    .await;
    d.step(&format!("insert into public.src values {rows}"))
        .await;
    d
}

async fn define(d: &Driver) {
    let columns = columns()
        .into_iter()
        .map(|(n, t)| (n.to_string(), t))
        .collect();
    trellis::defs::install_definition(d.pool(), AGG, &columns, "public")
        .await
        .expect("define the aggregate");
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the build");
}

async fn assert_oracle(d: &mut Driver) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "the target (left) differs from the oracle (right)"
    );
}

async fn hidden(d: &Driver) -> String {
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// The hidden partials of group `g` (`None`: the `NULL` group): `(n, count,
/// sum, has basis)`.
async fn partials(d: &Driver, g: Option<i32>) -> Vec<String> {
    let filter = match g {
        Some(g) => format!("g = {g}"),
        None => "g is null".to_string(),
    };
    d.rows(&format!(
        "select __n, __count_1, __sum_1, __basis is not null from {} where {filter}",
        hidden(d).await
    ))
    .await
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

async fn assert_finished(d: &Driver) {
    assert_eq!(
        d.rows(
            "select status, (select count(*) from backfill_chunks \
                             where definition_id = d.id), \
                    (select string_agg(last_error, '; ') from backfill_chunks) \
             from transform_definitions d where target_table = 'public.agg'"
        )
        .await,
        ["(live,0,)"],
        "the build committed and flipped the definition live"
    );
}

/// A page creates a group's hidden row after the build's snapshot `S1` and
/// before its insert reaches the group, applying a change `S1` didn't see.
/// The insert finds the row, only locks it, and step 2 rewrites the group
/// from a snapshot that sees the change. A build that wrote the group from
/// `S1` would lose it (`(1,1,1,t)`).
async fn page_creates_a_group_inside_step_one(g: Option<i32>, indexed: bool) {
    let lit = g.map_or("null".to_string(), |g| g.to_string());
    let mut d = seeded(&format!("(1, 5, 10), (2, {lit}, 1)"), indexed).await;
    define(&d).await;
    // A page holding group 5, below the group in key order.
    d.write("update public.src set v = 11 where id = 1").await;
    let held = d.seal().await;
    let mut holder = d
        .drain_frozen(held, "holder", &[(PausePoint::AfterEntryLock, TARGET)])
        .await;
    let holding = holder.reached(PausePoint::AfterEntryLock).await;
    // Step 1 takes S1, then its insert waits on group 5.
    let building = build_frozen(&d, &[]).await;
    d.wait_blocked_behind(holding.backend_pid).await;
    // After S1: a change to the group, drained by a page that creates its
    // hidden row on no basis.
    d.write(&format!("insert into public.src values (3, {lit}, 100)"))
        .await;
    let after = d.seal().await;
    d.drain(after, "after").await;
    assert_eq!(
        partials(&d, g).await,
        ["(1,1,100,f)"],
        "the page made the row"
    );
    d.release(&mut holder, PausePoint::AfterEntryLock).await;
    holder.finish().await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, g).await,
        ["(2,2,101,t)"],
        "the straggler, rewritten from step 2's snapshot"
    );
    assert_eq!(partials(&d, Some(5)).await, ["(1,1,11,t)"]);
    assert_oracle(&mut d).await;
    assert_eq!(d.deadlocks_logged(), Vec::<String>::new());
}

#[tokio::test]
async fn a_page_creates_a_group_inside_step_one_probe() {
    page_creates_a_group_inside_step_one(Some(9), true).await;
}

#[tokio::test]
async fn a_page_creates_a_group_inside_step_one_group_by() {
    page_creates_a_group_inside_step_one(Some(9), false).await;
}

#[tokio::test]
async fn a_page_creates_the_null_group_inside_step_one() {
    page_creates_a_group_inside_step_one(None, true).await;
}

/// A page blocked on a row step 1 inserted (uncommitted): after the
/// build's commit it reads basis `S1`, refuses the change `S1` saw (drained
/// after the build) and applies the one committed after `S1`, once.
#[tokio::test]
async fn a_page_blocked_on_an_inserted_row_applies_only_what_s1_missed() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)", true).await;
    d.write("insert into public.src values (3, 1, 1)").await;
    let before = d.seal().await;
    define(&d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let written = building.reached(PausePoint::AfterRebuildLock).await;
    assert_eq!(
        partials(&d, Some(1)).await,
        Vec::<String>::new(),
        "uncommitted"
    );
    d.write("insert into public.src values (4, 1, 7)").await;
    let during = d.seal().await;
    let page = d.drain_frozen(during, "during", &[]).await;
    d.wait_blocked_behind(written.backend_pid).await;
    assert!(!page.is_finished(), "the page waits on the inserted row");
    d.release(&mut building, PausePoint::AfterRebuildLock).await;
    building.finish().await;
    assert_finished(&d).await;
    page.finish().await;
    assert_eq!(
        partials(&d, Some(1)).await,
        ["(3,3,18,t)"],
        "S1's two rows, then the page's one"
    );
    d.drain(before, "before").await;
    assert_eq!(
        partials(&d, Some(1)).await,
        ["(3,3,18,t)"],
        "the change S1 saw is refused"
    );
    assert_oracle(&mut d).await;
}

/// Pages that apply before step 1 create hidden rows on no basis; step 1
/// enumerates them (the hidden branch, in S1), only locks them, and step 2
/// rewrites them. Today's build, whose enumeration came before the pages,
/// left the new group 9 to its pages (`group_basis_build`'s
/// `pages_between_the_enumeration_and_the_lock`, which pins that).
#[tokio::test]
async fn pages_before_step_one_make_stragglers() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5), (3, 3, 1), (7, null, 5)", true).await;
    define(&d).await;
    let mut building = build_frozen(&d, &[PausePoint::AfterRebuildEnumerate]).await;
    building.reached(PausePoint::AfterRebuildEnumerate).await;
    // The `NULL` group too: in S1 it is both a source group and a hidden
    // row, which step 1 must match as one group (`NULL` matching `NULL`).
    d.step("insert into public.src values (4, 1, 1), (5, 9, 4), (6, null, 2)")
        .await;
    assert_eq!(partials(&d, Some(1)).await, ["(1,1,1,f)"]);
    assert_eq!(partials(&d, None).await, ["(1,1,2,f)"]);
    assert_eq!(partials(&d, Some(9)).await, ["(1,1,4,f)"]);
    d.write("update public.src set v = v + 1 where id = 2")
        .await;
    d.release(&mut building, PausePoint::AfterRebuildEnumerate)
        .await;
    building.finish().await;
    assert_finished(&d).await;
    assert_eq!(partials(&d, Some(1)).await, ["(2,2,11,t)"]);
    assert_eq!(
        partials(&d, Some(9)).await,
        ["(1,1,4,t)"],
        "enumerated through the hidden table, rewritten by step 2"
    );
    assert_eq!(partials(&d, None).await, ["(2,2,7,t)"]);
    let undrained = d.seal().await;
    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, Some(2)).await, ["(1,1,6,t)"]);
    assert_oracle(&mut d).await;
}

// --- #1917 X6: pins of A's own enumeration rules ---------------------------

/// Group 2's last row deleted before the build, its change still undrained
/// at `S1`: only the ring's old image names the group. Step 1 inserts it
/// at zero with basis `S1`, and the delete's page, drained after the build,
/// is refused. Without the ring branch (`ab_enum_no_ring`) the page lands
/// on a fresh row with no basis and drives it negative.
#[tokio::test]
async fn a_group_vacated_by_an_undrained_change_is_zeroed() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)", true).await;
    d.write("delete from public.src where id = 2").await;
    let undrained = d.seal().await;
    define(&d).await;
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, Some(2)).await,
        ["(0,0,0,t)"],
        "named by the old image"
    );
    d.drain(undrained, "undrained").await;
    assert_eq!(partials(&d, Some(2)).await, ["(0,0,0,t)"], "refused");
    assert_oracle(&mut d).await;
}

/// Group 2's last row deleted after define and before the build runs, its
/// page drained (a hidden row on no basis, partials `-1`) and its segment
/// retired: at `S1` only the hidden table names the group (the target has
/// no row for it). Step 1 locks it as a straggler and step 2 zeroes it.
/// Without the hidden branch (`ab_enum_no_hidden`) it keeps the page's `-1`.
#[tokio::test]
async fn a_vacated_group_drained_before_step_one_is_zeroed() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)", true).await;
    define(&d).await;
    d.write("delete from public.src where id = 2").await;
    let batch = d.seal().await;
    d.drain(batch, "before").await;
    // The delete's segment retires once its successor is drained too.
    let next = d.seal().await;
    d.drain(next, "next").await;
    d.retire().await;
    assert_eq!(
        d.rows(&format!(
            "select count(*) from ({}) r where old_image is not null",
            (0..trellis::staging::append::RING_SIZE)
                .map(|slot| format!("select old_image from {DEFAULT_SCHEMA}.seg_{slot}"))
                .collect::<Vec<_>>()
                .join(" union all ")
        ))
        .await,
        ["(0)"],
        "no old image is left in the ring"
    );
    assert_eq!(partials(&d, Some(2)).await, ["(-1,-1,-5,f)"]);
    assert_eq!(
        d.rows("select count(*) from public.agg").await,
        ["(0)"],
        "the target names no group"
    );
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        partials(&d, Some(2)).await,
        ["(0,0,0,t)"],
        "locked through the hidden branch, zeroed by step 2"
    );
    assert_oracle(&mut d).await;
}

/// A resume keeps the target: a target row no source row, hidden row or
/// change backs (written by hand while paused) is named only by the
/// target branch, and step 1's insert of it at zero removes it. Without the
/// target branch (`ab_enum_no_target`) it stays.
#[tokio::test]
async fn a_resume_removes_a_target_row_only_the_target_names() {
    let mut d = seeded("(1, 1, 10), (2, 2, 5)", true).await;
    define(&d).await;
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.agg'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    d.ctl
        .batch_execute(
            "update transform_definitions set status = 'paused' \
             where target_table = 'public.agg'; \
             insert into public.agg (g, total, n) values (99, 5, 1)",
        )
        .await
        .expect("pause and write the target");
    trellis::staging::quarantine::resume_transform(d.pool(), "agg")
        .await
        .expect("resume");
    let pool = d.pool().clone();
    build::start_ready_builds(&mut d.ctl, &pool, &[id])
        .await
        .expect("start the resumed build");
    build_frozen(&d, &[]).await.finish().await;
    assert_finished(&d).await;
    assert_eq!(
        d.rows("select count(*) from public.agg where g = 99").await,
        ["(0)"],
        "the row only the target named is gone"
    );
    assert_oracle(&mut d).await;
}
