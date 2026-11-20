//! A plain 1-1 transform with a `WHERE` (#1991 step C; ADR-0002: "a row
//! whose predicate is not `TRUE` is treated as a row gone from the source").
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence. Each compares the target with
//! `SELECT … WHERE w > 0`, where `w` is read by the predicate alone. The
//! build interleavings run against a real `backfilling` state
//! ([`Driver::start_unbuilt`]): the pages that predate the build re-derive,
//! and the tombstone GC runs beside the chunk.

use crate::support::drain_driver;

use std::time::Duration;

use drain_driver::{Driver, Running, int4};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::staging::build;
use trellis::staging::build::one_to_one::{self, OneToOneOutcome, OneToOnePlan};
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

const SRC: &str = "public.src";
const CREATE: &str = "create table public.src \
     (id integer primary key, g integer, v integer, w integer);";
const ONE: &str = "TRANSFORM one FROM public.src SELECT g AS g, v AS v WHERE w > 0";
const ACTUAL: &str = "select id, g, v from public.one order by id";
const EXPECTED: &str = "select id, g, v from public.src where w > 0 order by id";

/// Keys 1 and 2 are members; 3 and 4 fail (`w = 0`, `NULL`).
const ROWS: &str = "insert into public.src values \
     (1, 1, 10, 5), (2, 2, 20, 1), (3, 1, 30, 0), (4, 2, 40, null);";

/// Every transition of `w > 0`.
const CROSSINGS: &[&str] = &[
    // member -> member, a field changes
    "update public.src set v = 11 where id = 1",
    // member -> member, only the predicate's column changes
    "update public.src set w = 6 where id = 1",
    // member -> failing, only the predicate's column changes
    "update public.src set w = 0 where id = 2",
    // failing -> member
    "update public.src set w = 4 where id = 3",
    // failing -> failing, with a field change
    "update public.src set w = -5, v = 41 where id = 4",
    // member -> NULL, NULL -> member
    "update public.src set w = null where id = 1",
    "update public.src set w = 1 where id = 4",
    // a key change that crosses out, and one that crosses in
    "update public.src set id = 13, w = 0 where id = 3",
    "update public.src set id = 12, w = 2 where id = 2",
    // inserts and deletes of failing and member rows
    "insert into public.src values (20, 1, 200, 0), (21, 6, 210, 1), (22, null, null, 3)",
    "delete from public.src where id in (20, 21)",
    "update public.src set w = 0 where id = 22",
    "delete from public.src where id = 22",
];

fn columns() -> Vec<(&'static str, trellis::defs::ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4()), ("w", int4())]
}

/// `definition` live over `public.src`, built over `rows`.
async fn start_over(definition: &str, rows: &str) -> Driver {
    Driver::start(
        &format!("{CREATE} {rows}"),
        &columns(),
        &[definition],
        &[SRC],
    )
    .await
}

async fn start() -> Driver {
    start_over(ONE, ROWS).await
}

/// Settles the pipeline and asserts the target equals the filtered query,
/// with nothing held.
async fn assert_oracle(d: &mut Driver, context: &str) {
    d.settle().await;
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "{context}: the target (left) differs from the oracle (right)"
    );
    assert_eq!(
        d.rows("select count(*) from poison").await,
        ["(0)"],
        "{context}: nothing is held"
    );
}

/// `(key, tombstone)` of every entry of the ledger.
async fn entries(d: &Driver) -> Vec<String> {
    d.rows("select __from_key, __tombstone from public.one__ledger order by 1")
        .await
}

/// The ledger at rest, after the tombstone GC, is the member rows.
async fn assert_ledger_at_rest(d: &mut Driver, context: &str) {
    d.retire().await;
    d.collect_tombstones().await;
    assert_eq!(
        d.rows("select __from_key from public.one__ledger order by __from_key::int")
            .await,
        d.rows("select s.id::text from public.src s where s.w > 0 order by s.id")
            .await,
        "{context}: the ledger at rest is the member rows"
    );
}

/// The build writes member rows only, and gives no failing row an entry.
/// Each transition then applies as a write or a delete.
#[tokio::test]
async fn every_transition_matches_the_filtered_query() {
    let mut d = start().await;
    assert_oracle(&mut d, "the build").await;
    assert_eq!(
        entries(&d).await,
        ["(1,f)", "(2,f)"],
        "the build left entries for the member rows only"
    );
    for sql in CROSSINGS {
        d.step(sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "{sql}");
    }
    assert_oracle(&mut d, "every crossing").await;
    assert_ledger_at_rest(&mut d, "every crossing").await;
    assert_eq!(walk(&d).await, []);
}

/// `WHERE w <> 0 SELECT v / w` builds, applies, rebuilds, and takes an
/// `ALTER TRANSFORM … ADD` of the same quotient with `w = 0` rows present:
/// no field is ever evaluated on a failing row (#1991 rule 9), so nothing is
/// held, charged or failed.
#[tokio::test]
async fn a_field_is_never_evaluated_on_a_failing_row() {
    const RATIO: &str =
        "TRANSFORM one FROM public.src SELECT CAST(v AS numeric) / w AS q WHERE w <> 0";
    const ACTUAL: &str = "select id, q from public.one order by id";
    const EXPECTED: &str = "select id, v::numeric / w from public.src where w <> 0 order by id";
    let mut d = start_over(RATIO, ROWS).await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "the build");
    for sql in [
        "insert into public.src values (30, 1, 7, 0), (31, 1, 8, 2)",
        "update public.src set w = 0 where id = 31",
        "update public.src set w = 3 where id = 30",
        "update public.src set w = 0, v = 9 where id = 30",
    ] {
        d.step(sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "{sql}");
    }
    request_rebuild(&d).await;
    rebuild(&d).await;
    d.settle().await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "the rebuild");

    let trellis = connect(&d).await;
    trellis
        .apply("ALTER TRANSFORM one ADD CAST(v AS numeric) / w AS q2")
        .await
        .expect("alter add");
    build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_eq!(
        d.rows("select id, q, q2 from public.one order by id").await,
        d.rows(
            "select id, v::numeric / w, v::numeric / w from public.src where w <> 0 order by id"
        )
        .await,
        "the field build"
    );
    for table in ["poison", "column_failures"] {
        assert_eq!(
            d.rows(&format!("select count(*) from {table}")).await,
            ["(0)"],
            "{table}"
        );
    }
    assert_eq!(
        d.rows("select count(*) from backfill_chunks where last_error is not null")
            .await,
        ["(0)"],
        "no build chunk failed"
    );
    let check = check_page(
        d.pool(),
        "one",
        SelfCheckScope {
            after: None,
            limit: 100,
        },
        SelfCheckMode::Standard,
        Duration::from_secs(30),
    )
    .await
    .expect("self_check");
    assert!(
        matches!(check.outcome, SelfCheckOutcome::Converged),
        "{:?}",
        check.outcome
    );
    trellis.shutdown().await.expect("shutdown");
}

// --------------------------------------------- one key, two pages

/// Two changes of one key, each sealed into its own batch, drained in
/// either order. With the second drained first, the tombstone GC runs before
/// the first drains: a tombstone the older change still needs must survive.
async fn two_pages(first: &str, second: &str, second_first: bool) {
    let context = format!("{first} | {second} | second first: {second_first}");
    let mut d = start().await;
    d.write(first).await;
    let b1 = d.seal().await;
    d.write(second).await;
    let b2 = d.seal().await;
    if second_first {
        d.drain(b2, "a").await;
        d.collect_tombstones().await;
        d.drain(b1, "b").await;
    } else {
        d.drain(b1, "a").await;
        d.drain(b2, "b").await;
    }
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "{context}: before the settle"
    );
    assert_oracle(&mut d, &context).await;
    assert_ledger_at_rest(&mut d, &context).await;
}

/// pass → fail, then fail → pass, of a key the build wrote.
#[tokio::test]
async fn one_key_out_then_in_in_both_orders() {
    for second_first in [false, true] {
        two_pages(
            "update public.src set w = 0 where id = 1",
            "update public.src set w = 7, v = 11 where id = 1",
            second_first,
        )
        .await;
    }
}

/// fail → pass, then pass → fail, of a key with no entry.
#[tokio::test]
async fn one_key_in_then_out_in_both_orders() {
    for second_first in [false, true] {
        two_pages(
            "update public.src set w = 3 where id = 3",
            "update public.src set w = 0 where id = 3",
            second_first,
        )
        .await;
    }
}

/// A new key inserted failing, then updated to pass; a member that leaves
/// and is then deleted; a key change that crosses; fail → fail, then pass.
#[tokio::test]
async fn other_two_page_orders() {
    for second_first in [false, true] {
        for (first, second) in [
            (
                "insert into public.src values (9, 1, 90, 0)",
                "update public.src set w = 4 where id = 9",
            ),
            (
                "update public.src set w = 0 where id = 2",
                "delete from public.src where id = 2",
            ),
            (
                "update public.src set id = 12, w = 0 where id = 2",
                "update public.src set w = 6 where id = 12",
            ),
            (
                "update public.src set w = -1, v = 31 where id = 3",
                "update public.src set w = 1 where id = 3",
            ),
        ] {
            two_pages(first, second, second_first).await;
        }
    }
}

/// A change that leaves a row failing writes a tombstone, even for a key
/// with no entry (#1991 rule 16). A key inserted as a member, then updated
/// to fail, drained second first: the failing Apply finds no entry and
/// must leave a tombstone, which survives the GC while the insert is
/// undrained and refuses it. With no tombstone the insert would write the
/// failing row back, and nothing would retract it (an end-to-end run with
/// in-order drains never shows this).
#[tokio::test]
async fn a_failing_apply_with_no_entry_leaves_a_tombstone() {
    let mut d = start().await;
    d.write("insert into public.src values (9, 1, 90, 4)").await;
    let insert = d.seal().await;
    d.write("update public.src set w = null where id = 9").await;
    let fail = d.seal().await;
    d.drain(fail, "a").await;
    assert_eq!(
        d.rows("select __tombstone from public.one__ledger where __from_key = '9'")
            .await,
        ["(t)"],
        "the failing Apply wrote a tombstone"
    );
    d.retire().await;
    d.collect_tombstones().await;
    assert_eq!(
        d.rows("select __tombstone from public.one__ledger where __from_key = '9'")
            .await,
        ["(t)"],
        "the tombstone outlives the GC while the insert is undrained"
    );
    d.drain(insert, "b").await;
    assert_eq!(
        d.rows("select id from public.one where id = 9").await,
        Vec::<String>::new(),
        "the older insert is refused"
    );
    assert_oracle(&mut d, "a failing Apply with no entry").await;
    assert_ledger_at_rest(&mut d, "a failing Apply with no entry").await;
}

/// A page Re-derive (an image-less `Recompute`) of a failing key and of a
/// member key, frozen after its read while a crossing change commits, and
/// in the other order: the Re-derive decides membership from the row it
/// read under the entry lock.
#[tokio::test]
async fn a_page_rederive_reads_membership_under_the_entry_lock() {
    for (key, change) in [
        ("1", "update public.src set w = 0 where id = 1"),
        ("3", "update public.src set w = 2 where id = 3"),
    ] {
        let mut d = start().await;
        d.stage_recomputes(SRC, &[key]).await;
        let r_batch = d.seal().await;
        let mut a = d
            .drain_frozen(
                r_batch,
                "a",
                &[(PausePoint::AfterRederiveRead, "public.one")],
            )
            .await;
        let frozen = a.reached(PausePoint::AfterRederiveRead).await;
        d.write(change).await;
        let c_batch = d.seal().await;
        let b = d.drain_frozen(c_batch, "b", &[]).await;
        d.wait_blocked_behind(frozen.backend_pid).await;
        d.release(&mut a, PausePoint::AfterRederiveRead).await;
        a.finish().await;
        b.finish().await;
        assert_oracle(&mut d, change).await;

        let mut d = start().await;
        d.write(change).await;
        let c_batch = d.seal().await;
        d.stage_recomputes(SRC, &[key]).await;
        let r_batch = d.seal().await;
        d.drain(r_batch, "a").await;
        assert_eq!(
            d.rows(ACTUAL).await,
            d.rows(EXPECTED).await,
            "{change}: the Re-derive alone wrote the membership"
        );
        d.drain(c_batch, "b").await;
        assert_oracle(&mut d, change).await;
    }
}

/// An image without the predicate's column (staged before a capture widen,
/// say) fails the page naming the column rather than reading it as `NULL`,
/// which would drop a member row silently (#1991 rule 11): the drain's
/// isolation then holds its key.
#[tokio::test]
async fn an_image_without_the_predicate_column_fails_the_page() {
    let d = start().await;
    let mut d = d;
    d.write("insert into public.src values (50, 1, 5, 3)").await;
    let slots: Vec<String> = d
        .ctl
        .query(
            "select c.relname::text from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname ~ '^seg_[0-9]+$'",
            &[&DEFAULT_SCHEMA],
        )
        .await
        .expect("slots")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let mut stripped = 0;
    for slot in slots {
        stripped += d
            .ctl
            .execute(
                &format!(
                    "update {DEFAULT_SCHEMA}.{slot} set \
                         new_image = new_image - 'w', \
                         transition_images = case \
                             when jsonb_typeof(transition_images -> 'new') = 'object' \
                             then transition_images #- '{{new,w}}' else transition_images end \
                     where key = '50'"
                ),
                &[],
            )
            .await
            .expect("strip the column");
    }
    assert_eq!(stripped, 1, "one ring row for key 50");
    let reaches = || {
        trellis::dev::plant::reached()
            .into_iter()
            .find(|(site, _)| *site == "o2o_where_column_not_imaged")
            .map(|(_, n)| n)
            .expect("the plant's site")
    };
    let reached_before = reaches();
    let batch = d.seal().await;
    let err = d
        .drain_frozen(batch, "a", &[])
        .await
        .finish_result()
        .await
        .expect_err("the page fails");
    assert!(
        format!("{err:?}").contains("the row has no column w"),
        "{err:?}"
    );
    assert!(
        reaches() > reached_before,
        "the failed page counts a reach of the plant's site"
    );
    assert_eq!(
        d.rows("select id from public.one where id = 50").await,
        Vec::<String>::new(),
        "nothing applied"
    );
}

// ------------------------------------------- rebuilds, pause and resume

async fn connect(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn request_rebuild(d: &Driver) {
    let trellis = connect(d).await;
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
    trellis.shutdown().await.expect("shutdown");
}

/// Starts the requested rebuild, as the staging worker does, and runs it.
async fn rebuild(d: &Driver) {
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the rebuild");
    build::settle_builds(d.pool()).await;
}

/// `request_backfill` repairs membership: a member row deleted from the
/// target, a failing row written into it, and a tampered cell. The rebuild
/// sweep deletes the failing row and tombstones its entry.
#[tokio::test]
async fn request_backfill_repairs_membership() {
    let mut d = start().await;
    d.write(
        "delete from public.one where id = 1; \
         insert into public.one (id, g, v) values (3, 1, 30); \
         insert into public.one__ledger (__from_key) values ('3'); \
         update public.one set v = 999 where id = 2",
    )
    .await;
    request_rebuild(&d).await;
    rebuild(&d).await;
    assert_oracle(&mut d, "the rebuild").await;
    assert_ledger_at_rest(&mut d, "the rebuild").await;
}

/// Crossings while the definition is paused are repaired by its resume.
#[tokio::test]
async fn a_resume_rebuilds_membership() {
    let mut d = start().await;
    let trellis = connect(&d).await;
    trellis.apply("PAUSE TRANSFORM one").await.expect("pause");
    d.write(
        "update public.src set w = 0 where id = 1; \
         update public.src set w = 3 where id = 3; \
         delete from public.src where id = 2; \
         insert into public.src values (7, 7, 70, 7), (8, 8, 80, 0)",
    )
    .await;
    d.settle().await;
    trellis.apply("RESUME TRANSFORM one").await.expect("resume");
    rebuild(&d).await;
    assert_oracle(&mut d, "the resume").await;
    assert_ledger_at_rest(&mut d, "the resume").await;
    trellis.shutdown().await.expect("shutdown");
}

// ----------------------------- a build chunk against a page, while building

/// [`ROWS`] with [`ONE`] defined and its build started, `backfilling` and
/// unclaimed: the state a build chunk runs in.
async fn start_build() -> (Driver, OneToOnePlan) {
    let d =
        Driver::start_unbuilt(&[], &format!("{CREATE} {ROWS}"), &columns(), &[ONE], &[SRC]).await;
    assert_eq!(
        d.rows("select status::text from transform_definitions")
            .await,
        ["(backfilling)"]
    );
    let plan = OneToOnePlan::load(d.pool(), "one")
        .await
        .expect("load the 1-1 plan")
        .expect("a plain 1-1 target takes the re-derive build");
    (d, plan)
}

async fn chunk_frozen(
    d: &Driver,
    plan: &OneToOnePlan,
    points: &[(PausePoint, &str)],
) -> Running<OneToOneOutcome> {
    let plan = plan.clone();
    d.run_frozen(points, move |pool| async move {
        let mut client = pool.get().await?;
        let txn = client.transaction().await?;
        let outcome = one_to_one::run_chunk(&txn, &plan, None, "100").await?;
        txn.commit().await?;
        Ok(outcome)
    })
    .await
}

async fn chunk(d: &Driver, plan: &OneToOnePlan) -> OneToOneOutcome {
    chunk_frozen(d, plan, &[]).await.finish().await
}

/// Runs the rest of the build, settles, and checks the target, the ledger
/// at rest and the definition's status.
async fn finish_build(d: &mut Driver, context: &str) {
    build::settle_builds(d.pool()).await;
    assert_oracle(d, context).await;
    assert_eq!(
        d.rows("select status::text from transform_definitions")
            .await,
        ["(live)"],
        "{context}"
    );
    assert_ledger_at_rest(d, context).await;
}

#[derive(Clone, Copy, Debug)]
enum Order {
    ChunkWriteApply,
    WriteChunkApply,
    WriteApplyChunk,
}

/// A chunk over every key and an Apply of one crossing change, in every
/// order around the chunk. The chunk locks and writes member rows only.
#[tokio::test]
async fn a_chunk_and_a_crossing_apply_agree() {
    for change in [
        "update public.src set w = 0 where id = 2",
        "update public.src set w = 9 where id = 3",
        "update public.src set w = 1, v = 41 where id = 4",
        "insert into public.src values (7, 1, 70, 0)",
        "insert into public.src values (7, 1, 70, 2)",
        "delete from public.src where id = 3",
    ] {
        for order in [
            Order::ChunkWriteApply,
            Order::WriteChunkApply,
            Order::WriteApplyChunk,
        ] {
            let context = format!("{change} / {order:?}");
            let (mut d, plan) = start_build().await;
            match order {
                Order::ChunkWriteApply => {
                    assert_eq!(
                        chunk(&d, &plan).await.keys,
                        2,
                        "{context}: member rows only"
                    );
                    assert_eq!(entries(&d).await, ["(1,f)", "(2,f)"], "{context}");
                    d.write(change).await;
                    let batch = d.seal().await;
                    d.drain(batch, "apply").await;
                }
                Order::WriteChunkApply => {
                    d.write(change).await;
                    let batch = d.seal().await;
                    chunk(&d, &plan).await;
                    d.drain(batch, "apply").await;
                }
                Order::WriteApplyChunk => {
                    d.write(change).await;
                    let batch = d.seal().await;
                    d.drain(batch, "apply").await;
                    chunk(&d, &plan).await;
                }
            }
            assert_eq!(
                d.rows(ACTUAL).await,
                d.rows(EXPECTED).await,
                "{context}: before the build ends"
            );
            finish_build(&mut d, &context).await;
        }
    }
}

/// C0 (row 3 passes) and C1 (row 3 fails) both commit before the chunk's
/// read, so the chunk never names row 3. Drained after the chunk, C0 writes
/// the row and C1 removes it: the chained reader may see the row come and
/// go, never stay.
#[tokio::test]
async fn a_failing_row_the_chunk_skipped_follows_its_changes() {
    let (mut d, plan) = start_build().await;
    d.write("update public.src set w = 5 where id = 3").await;
    let c0 = d.seal().await;
    d.write("update public.src set w = 0 where id = 3").await;
    let c1 = d.seal().await;
    assert_eq!(chunk(&d, &plan).await.keys, 2);
    assert_eq!(entries(&d).await, ["(1,f)", "(2,f)"], "no entry for row 3");
    d.drain(c0, "a").await;
    d.drain(c1, "b").await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await);
    finish_build(&mut d, "a skipped failing row").await;
}

/// C1 (row 3 leaves) drains before the chunk and leaves a tombstone; the GC
/// runs with C0 (row 3 enters) undrained, then C0 drains and is refused.
#[tokio::test]
async fn a_tombstone_from_before_the_chunk_orders_the_older_change() {
    let (mut d, plan) = start_build().await;
    d.write("update public.src set w = 5 where id = 3").await;
    let c0 = d.seal().await;
    d.write("update public.src set w = 0 where id = 3").await;
    let c1 = d.seal().await;
    d.drain(c1, "a").await;
    assert_eq!(entries(&d).await, ["(3,t)"]);
    assert_eq!(chunk(&d, &plan).await.keys, 2);
    d.retire().await;
    d.collect_tombstones().await;
    assert_eq!(
        entries(&d).await,
        ["(1,f)", "(2,f)", "(3,t)"],
        "the tombstone survives the chunk and the GC while C0 is undrained"
    );
    d.drain(c0, "b").await;
    assert_eq!(
        d.rows("select id from public.one where id = 3").await,
        Vec::<String>::new(),
        "C0 is refused"
    );
    finish_build(&mut d, "a tombstone from before the chunk").await;
}

/// A writer in progress at the chunk's snapshot, committing after the
/// chunk, crossing in each direction.
#[tokio::test]
async fn a_writer_in_flight_at_the_chunks_snapshot() {
    for change in [
        "update public.src set w = 0 where id = 2",
        "update public.src set w = 8 where id = 3",
        "insert into public.src values (7, 1, 70, 3)",
        "delete from public.src where id = 1",
    ] {
        let (mut d, plan) = start_build().await;
        let user = d.user().await;
        user.batch_execute(&format!("begin; {change}"))
            .await
            .expect("the writer's change");
        assert_eq!(chunk(&d, &plan).await.keys, 2, "{change}");
        user.batch_execute("commit").await.expect("commit");
        let batch = d.seal().await;
        d.drain(batch, "apply").await;
        assert_eq!(
            d.rows(ACTUAL).await,
            d.rows(EXPECTED).await,
            "{change}: before the build ends"
        );
        finish_build(&mut d, change).await;
    }
}

/// A crossing change that commits while the chunk holds its entries,
/// before its one read-and-write statement: the snapshot sees it, and the
/// page that drains it afterwards is refused or writes the same answer.
#[tokio::test]
async fn a_crossing_change_under_the_chunks_lock() {
    for change in [
        "update public.src set w = 0 where id = 2",
        "update public.src set w = 8 where id = 3",
    ] {
        let (mut d, plan) = start_build().await;
        let mut running =
            chunk_frozen(&d, &plan, &[(PausePoint::AfterEntryLock, "public.one")]).await;
        running.reached(PausePoint::AfterEntryLock).await;
        d.write(change).await;
        let batch = d.seal().await;
        d.release(&mut running, PausePoint::AfterEntryLock).await;
        running.finish().await;
        d.drain(batch, "apply").await;
        assert_eq!(
            d.rows(ACTUAL).await,
            d.rows(EXPECTED).await,
            "{change}: before the build ends"
        );
        finish_build(&mut d, change).await;
    }
}

// ------------------------------------------------------- self_check

/// Every page of the check of `one`, ten rows at a time: the divergences.
async fn walk(d: &Driver) -> Vec<Divergence> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            "one",
            SelfCheckScope { after, limit: 10 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(mut divergences) => found.append(&mut divergences),
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

/// One comparison of `one` with no re-check, whatever the ring holds: the
/// divergences and the keys it couldn't vouch for exactly.
async fn compare(d: &Driver) -> (Vec<Divergence>, Vec<String>, bool) {
    let mut after = None;
    let (mut found, mut uncertain, mut all) = (Vec::new(), Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), "one", SelfCheckScope { after, limit: 10 })
            .await
            .expect("a comparison");
        found.extend(page.divergences);
        uncertain.extend(page.uncertain_keys);
        all |= page.uncertain_all;
        after = page.next_after;
        if after.is_none() {
            return (found, uncertain, all);
        }
    }
}

/// A converged filtered target reports nothing: the audit applies the
/// `WHERE` to live rows (#1991 rule 13). Unapplied changes crossing the
/// predicate both ways are read back filtered too, so a comparison against
/// them finds nothing before and after they drain.
#[tokio::test]
async fn self_check_applies_the_where_to_live_rows_and_ring_images() {
    let mut d = start().await;
    assert_eq!(walk(&d).await, []);
    d.write(
        "update public.src set w = 0 where id = 1; \
         update public.src set w = 9 where id = 4; \
         insert into public.src values (40, 1, 4, 0), (41, 1, 4, 1)",
    )
    .await;
    let sealed = d.seal().await;
    d.write("update public.src set w = -2 where id = 4; update public.src set w = 3 where id = 3")
        .await;
    assert_eq!(compare(&d).await, (vec![], vec![], false));
    d.drain(sealed, "late").await;
    assert_eq!(compare(&d).await, (vec![], vec![], false));
    d.settle().await;
    assert_eq!(walk(&d).await, []);
}

/// A failing row written into the target is reported as a row that should
/// not be there, a member row missing from it as missing, and a tampered
/// cell as a cell; each clears once undone.
#[tokio::test]
async fn self_check_reports_a_failing_row_in_the_target() {
    let mut d = start().await;
    d.settle().await;
    d.write(
        "insert into public.one (id, g, v) values (3, 1, 30); \
         delete from public.one where id = 1; \
         update public.one set v = 21 where id = 2",
    )
    .await;
    let found = walk(&d).await;
    assert!(
        found
            .iter()
            .any(|f| matches!(f, Divergence::ExtraRow { key } if key == "3")),
        "the failing row 3 is reported: {found:?}"
    );
    assert!(
        found
            .iter()
            .any(|f| matches!(f, Divergence::MissingRow { key } if key == "1")),
        "the member row 1 is reported missing: {found:?}"
    );
    assert!(
        found
            .iter()
            .any(|f| matches!(f, Divergence::Cell { key, .. } if key == "2")),
        "the tampered cell of row 2 is reported: {found:?}"
    );
    assert_eq!(found.len(), 3, "{found:?}");
    d.write(
        "delete from public.one where id = 3; \
         insert into public.one (id, g, v) values (1, 1, 10); \
         update public.one set v = 20 where id = 2",
    )
    .await;
    assert_eq!(walk(&d).await, []);
}
