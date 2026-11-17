//! A plain aggregate with a `WHERE` (#1991 step D; ADR-0002: "partials count
//! member rows only; the predicate filters every staged image and every
//! source read; group naming is unfiltered").
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence. Each compares the target with
//! `SELECT … WHERE w > 0 GROUP BY g`, where `w` is read by the predicate
//! alone.

use crate::support::drain_driver;

use std::time::Duration;

use drain_driver::{Driver, Running, int4};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::chunk_queue;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

const SRC: &str = "public.src";
const CREATE: &str = "create table public.src \
     (id integer primary key, g integer, v integer, w integer); \
     create index on public.src (g);";
const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
     SELECT SUM(v) AS total, COUNT(*) AS n, AVG(v) AS av WHERE w > 0";
const ACTUAL: &str = "select g, total, n, av from public.agg order by g";
const EXPECTED: &str = "select g, sum(v), count(*), avg(v) from public.src \
     where w > 0 group by g order by g";

/// Keys 1 and 2 are members (groups 1 and 2); 3 and 4 fail (`w = 0`,
/// `NULL`); group 5 has only failing rows.
const ROWS: &str = "insert into public.src values \
     (1, 1, 10, 5), (2, 2, 20, 1), (3, 1, 30, 0), (4, 2, 40, null), (5, 5, 50, -1);";

/// Every transition of `w > 0`, with and without a group change.
const CROSSINGS: &[&str] = &[
    // member -> member, the argument changes
    "update public.src set v = 11 where id = 1",
    // member -> member, only the predicate's column changes
    "update public.src set w = 6 where id = 1",
    // member -> failing, only the predicate's column changes
    "update public.src set w = 0 where id = 2",
    // failing -> member
    "update public.src set w = 4 where id = 3",
    // failing -> failing, with an argument change
    "update public.src set w = -5, v = 41 where id = 4",
    // member -> NULL, NULL -> member
    "update public.src set w = null where id = 1",
    "update public.src set w = 1 where id = 4",
    // a group change that also crosses out, and one that crosses in
    "update public.src set g = 2, w = 0 where id = 3",
    "update public.src set g = 1, w = 2 where id = 2",
    // a group change of a member row, and of a failing row
    "update public.src set g = 3 where id = 4",
    "update public.src set g = 3 where id = 5",
    // the all-failing group gets a member, then loses it
    "update public.src set w = 1 where id = 5",
    "update public.src set w = 0 where id = 5",
    // a key change that crosses
    "update public.src set id = 17, w = 0 where id = 2",
    // inserts and deletes of failing and member rows, a NULL group and a
    // NULL argument
    "insert into public.src values (20, 1, 200, 0), (21, 6, 210, 1), (22, null, null, 3)",
    "delete from public.src where id in (20, 21)",
    "update public.src set w = 0 where id = 22",
];

fn columns() -> Vec<(&'static str, trellis::defs::ValueType)> {
    vec![("id", int4()), ("g", int4()), ("v", int4()), ("w", int4())]
}

/// [`AGG`] live over `public.src`, built over `rows` (written before
/// define).
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
    start_over(AGG, ROWS).await
}

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

async fn hidden(d: &Driver) -> String {
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.agg_{id}")
}

/// The hidden partials of every group, `(g, n, count, sum)`.
async fn all_partials(d: &Driver) -> Vec<String> {
    d.rows(&format!(
        "select g, __n, __count_1, __sum_1 from {} where __n <> 0 order by g",
        hidden(d).await
    ))
    .await
}

/// The hidden partials of group `g`: `(n, count, sum)`.
async fn partials(d: &Driver, g: i32) -> Vec<String> {
    d.rows(&format!(
        "select __n, __count_1, __sum_1 from {} where g = {g}",
        hidden(d).await
    ))
    .await
}

/// The build counts member rows only: a group of failing rows has no target
/// row, and a group with members has no failing row's contribution. Each
/// transition then applies as a delta.
#[tokio::test]
async fn every_transition_matches_the_filtered_query() {
    let mut d = start().await;
    assert_oracle(&mut d, "the build").await;
    assert_eq!(
        d.rows("select count(*) from public.agg where g = 5").await,
        ["(0)"],
        "a group whose rows all fail has no row"
    );
    for sql in CROSSINGS {
        d.step(sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "{sql}");
    }
    assert_oracle(&mut d, "every transition").await;
}

/// Each side of each transition is a delta of its own: two pages of one key
/// drained in both orders. An old image that fails retracts nothing, and a
/// new image that fails adds nothing.
#[tokio::test]
async fn membership_flips_in_two_pages_in_both_orders() {
    for (first, second) in [
        (
            "update public.src set w = 0 where id = 1",
            "update public.src set w = 7, v = 11 where id = 1",
        ),
        (
            "update public.src set w = 3 where id = 3",
            "update public.src set w = 0, g = 2 where id = 3",
        ),
        (
            "update public.src set w = -1, v = 31 where id = 3",
            "update public.src set g = 2 where id = 3",
        ),
        (
            "update public.src set g = 2, w = 0 where id = 1",
            "update public.src set g = 1, w = 1 where id = 4",
        ),
    ] {
        for second_first in [false, true] {
            let context = format!("{first} | {second} | second first: {second_first}");
            let mut d = start().await;
            d.write(first).await;
            let b1 = d.seal().await;
            d.write(second).await;
            let b2 = d.seal().await;
            if second_first {
                d.drain(b2, "a").await;
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
        }
    }
}

/// The build (rows written before define) and the deltas (the same rows
/// written after, then the same transitions) leave the same partials, and
/// so does a rebuild of the result.
#[tokio::test]
async fn build_and_apply_agree() {
    let mut built = start().await;
    let mut applied = start_over(AGG, "").await;
    applied.step(ROWS).await;
    assert_eq!(all_partials(&built).await, all_partials(&applied).await);
    for sql in CROSSINGS {
        built.step(sql).await;
        applied.step(sql).await;
    }
    assert_eq!(all_partials(&built).await, all_partials(&applied).await);
    // A rebuild of each reads the source anew, filtered as the deltas were.
    let before = all_partials(&applied).await;
    for d in [&mut built, &mut applied] {
        request_rebuild(d).await;
        rebuild_frozen(d, &[]).await.finish().await;
        assert_eq!(all_partials(d).await, before, "the rebuild agrees");
        assert_oracle(d, "the rebuild").await;
    }
}

/// An argument that would raise on a row that is no member is never
/// evaluated on one (#1991 rule 9): `v / w` under `WHERE w <> 0`, with
/// `w = 0` rows in the build, in the deltas and in a rebuild. Nothing is
/// held.
#[tokio::test]
async fn an_argument_is_never_evaluated_on_a_failing_row() {
    const RATIO: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
         SELECT SUM(CAST(v AS numeric) / w) AS q, COUNT(*) AS n WHERE w <> 0";
    const ACTUAL: &str = "select g, q, n from public.agg order by g";
    const EXPECTED: &str = "select g, sum(v::numeric / w), count(*) from public.src \
         where w <> 0 group by g order by g";
    let mut d = start_over(RATIO, ROWS).await;
    for sql in [
        "insert into public.src values (30, 1, 7, 0), (31, 1, 8, 2)",
        "update public.src set w = 0 where id = 31",
        "update public.src set w = 3 where id = 30",
        "update public.src set w = 0, g = 2 where id = 30",
    ] {
        d.step(sql).await;
        assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "{sql}");
    }
    request_rebuild(&d).await;
    rebuild_frozen(&d, &[]).await.finish().await;
    d.settle().await;
    assert_eq!(d.rows(ACTUAL).await, d.rows(EXPECTED).await, "the rebuild");
    assert_eq!(d.rows("select count(*) from poison").await, ["(0)"]);
}

// ------------------------------------------------- absolute writes

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

async fn request_rebuild(d: &Driver) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill("src")
        .await
        .expect("request a rebuild");
}

async fn rebuild_frozen(d: &Driver, points: &[PausePoint]) -> Running<()> {
    let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, "public.agg")).collect();
    d.run_frozen(&points, |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        build::run_claimed(&pool, &claimed[0], "rebuild", &OPTIONS).await;
        Ok(())
    })
    .await
}

/// Crossing updates around a rebuild (an absolute write, which reads the
/// source filtered): one sealed before it and drained after (in its
/// snapshot: refused), one committed while it holds the groups (in its
/// snapshot: refused), and two after (applied).
#[tokio::test]
async fn crossing_updates_around_a_rebuild() {
    let mut d = start().await;
    // Row 3 enters group 1; sealed, not drained.
    d.write("update public.src set w = 4 where id = 3").await;
    let before = d.seal().await;
    request_rebuild(&d).await;
    let mut rebuilding = rebuild_frozen(&d, &[PausePoint::AfterRebuildLock]).await;
    let locked = rebuilding.reached(PausePoint::AfterRebuildLock).await;
    // Row 2 leaves group 2 while the rebuild holds the groups.
    d.write("update public.src set w = 0 where id = 2").await;
    let during = d.seal().await;
    let page = d.drain_frozen(during, "during", &[]).await;
    d.wait_blocked_behind(locked.backend_pid).await;
    d.release(&mut rebuilding, PausePoint::AfterRebuildLock)
        .await;
    rebuilding.finish().await;
    page.finish().await;
    assert_eq!(
        partials(&d, 2).await,
        ["(0,0,0)"],
        "the rebuild counted row 2 as failing; the page's retraction was refused"
    );
    assert_eq!(partials(&d, 1).await, ["(2,2,40)"], "row 3 is a member");
    d.drain(before, "before").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(2,2,40)"],
        "the change from before the rebuild was in its snapshot, and refused"
    );
    d.step("update public.src set w = 1 where id = 4").await;
    d.step("update public.src set w = null where id = 1").await;
    assert_eq!(partials(&d, 2).await, ["(1,1,40)"]);
    assert_eq!(partials(&d, 1).await, ["(1,1,30)"]);
    assert_oracle(&mut d, "around a rebuild").await;
}

/// A page's `Recompute` of a key (an absolute read of its group) with a
/// crossing change of another row of the group committed before it, drained
/// before or after it.
#[tokio::test]
async fn a_crossing_update_around_a_group_recompute() {
    for change in [
        "update public.src set w = 0 where id = 1",
        "update public.src set w = 6 where id = 3",
    ] {
        for change_first in [false, true] {
            let context = format!("{change} / drained first: {change_first}");
            let mut d = start().await;
            d.write(change).await;
            let c_batch = d.seal().await;
            // Key 3 is in group 1, as key 1 is.
            d.stage_recomputes(SRC, &["3"]).await;
            let r_batch = d.seal().await;
            if change_first {
                d.drain(c_batch, "a").await;
                d.drain(r_batch, "b").await;
            } else {
                d.drain(r_batch, "a").await;
                assert_eq!(
                    d.rows(ACTUAL).await,
                    d.rows(EXPECTED).await,
                    "{context}: the recompute alone is already exact"
                );
                d.drain(c_batch, "b").await;
            }
            assert_eq!(
                d.rows(ACTUAL).await,
                d.rows(EXPECTED).await,
                "{context}: before the settle"
            );
            assert_oracle(&mut d, &context).await;
        }
    }
}

// ------------------------------------------ group naming is unfiltered

async fn hold(d: &Driver, key: &str) {
    d.ctl
        .execute(
            "insert into poison (transform_id, src_table, key, last_error) \
             select id, $1, $2, 'test' from transform_definitions \
             where target_table = 'public.agg'",
            &[&SRC, &key],
        )
        .await
        .expect("hold the key");
}

/// A held member key moves from group 1 to group 2, then leaves the
/// predicate; its changes park, naming their groups by images that hold only
/// the group column. The release recomputes those groups. Were group naming
/// filtered by the predicate, a group-only image would read `w` as `NULL`,
/// name nothing, and group 1 would keep the stale contribution.
#[tokio::test]
async fn a_release_names_groups_from_group_only_images() {
    let mut d = start().await;
    hold(&d, "1").await;
    d.step("update public.src set g = 2 where id = 1").await;
    d.step("update public.src set w = 0 where id = 1").await;
    assert_eq!(
        partials(&d, 1).await,
        ["(1,1,10)"],
        "group 1 drifts while the key is held"
    );
    trellis::staging::release_key(d.pool(), "agg", SRC, "1")
        .await
        .expect("release the key");
    assert_eq!(
        d.rows(ACTUAL).await,
        d.rows(EXPECTED).await,
        "after the release"
    );
    assert_oracle(&mut d, "the release").await;
}

/// A `Recompute` whose prior-image hint holds only the group column (what a
/// partition correction and a parked change carry) names that group, which
/// is recomputed from its member rows.
#[tokio::test]
async fn a_group_only_hint_names_its_group() {
    let mut d = start().await;
    d.ctl
        .batch_execute(&format!(
            "update {h} set __n = __n + 5, __sum_1 = __sum_1 + 100 where g = 2; \
             update public.agg set total = total + 100, n = n + 5 where g = 2",
            h = hidden(&d).await
        ))
        .await
        .expect("drift group 2");
    // Key 1 is in group 1; the hint names group 2 by its group column alone.
    d.stage_recomputes_with_prior(SRC, &[("1", Some(r#"{"g": 2}"#.to_string()))])
        .await;
    let batch = d.seal().await;
    d.drain(batch, "a").await;
    assert_oracle(&mut d, "a group-only hint").await;
}

/// An image without the predicate's column (staged before a capture widen,
/// say) fails the page naming the column rather than reading it as `NULL`,
/// which would drop a member row's contribution silently (#1991 rule 11).
/// The drain's isolation then holds its key, as for any record that can't
/// be applied.
#[tokio::test]
async fn an_image_without_the_predicate_column_fails_the_page() {
    let mut d = start().await;
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
    assert_eq!(partials(&d, 1).await, ["(1,1,10)"], "nothing applied");
}

// ------------------------------------------------------- self_check

/// Every page of the check of `agg`, ten groups at a time: the divergences.
async fn walk(d: &Driver) -> Vec<Divergence> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            "agg",
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

/// One comparison of `agg` with no re-check, whatever the ring holds: the
/// divergences and the groups it couldn't vouch for exactly.
async fn compare(d: &Driver) -> (Vec<Divergence>, Vec<String>, bool) {
    let mut after = None;
    let (mut found, mut uncertain, mut all) = (Vec::new(), Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), "agg", SelfCheckScope { after, limit: 10 })
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
/// `WHERE` to live rows. Unapplied changes crossing the predicate both ways
/// are subtracted filtered too, so a comparison against them finds nothing
/// before and after they drain.
#[tokio::test]
async fn self_check_applies_the_where_to_live_rows_and_ring_images() {
    let mut d = start().await;
    for sql in &CROSSINGS[..8] {
        d.step(sql).await;
    }
    assert_eq!(walk(&d).await, []);
    d.write(
        "update public.src set w = 0 where id = 1; \
         update public.src set w = 9 where id = 4; \
         update public.src set g = 1, w = 2 where id = 5",
    )
    .await;
    let sealed = d.seal().await;
    d.write(
        "update public.src set w = -2 where id = 5; insert into public.src values (40, 1, 4, 0)",
    )
    .await;
    assert_eq!(compare(&d).await, (vec![], vec![], false));
    d.drain(sealed, "late").await;
    assert_eq!(compare(&d).await, (vec![], vec![], false));
    d.settle().await;
    assert_eq!(walk(&d).await, []);
}

/// A failing row's contribution planted into a group's partials and visible
/// row is reported on both, and clears once undone.
#[tokio::test]
async fn self_check_reports_a_failing_rows_planted_contribution() {
    let mut d = start().await;
    d.settle().await;
    let hidden = hidden(&d).await;
    let name = hidden.rsplit('.').next().unwrap().to_string();
    // Row 3 (group 1, `v = 30`) fails `w > 0`: count it anyway.
    let plant = |sign: &str| {
        format!(
            "update {hidden} set __n = __n {sign} 1, __count_1 = __count_1 {sign} 1, \
                 __sum_1 = __sum_1 {sign} 30 where g = 1; \
             update public.agg set n = n {sign} 1, total = total {sign} 30 where g = 1"
        )
    };
    d.write(&plant("+")).await;
    let found = walk(&d).await;
    // The visible row agrees with the planted partials, so it is the
    // recompute from the source that reports them.
    for column in [
        format!("{name}.__n"),
        format!("{name}.__count_1"),
        format!("{name}.__sum_1"),
    ] {
        assert!(
            found.iter().any(|f| matches!(
                f,
                Divergence::Cell { key, column: c, .. } if key == "1" && *c == column
            )),
            "{column} of group 1 is reported: {found:?}"
        );
    }
    assert!(
        found.iter().all(|f| matches!(
            f,
            Divergence::Cell { key, .. } if key == "1"
        )),
        "only group 1: {found:?}"
    );
    d.write(&plant("-")).await;
    assert_eq!(walk(&d).await, []);
}
