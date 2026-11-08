//! A star 1-1 `JOIN`'s chunked fan-out (#1775, #1460 sub-step 3.9; #1464's
//! CX F10, PX8, PX9 and XT2 pins): a page leaves each key past its budget to
//! chunks under a pending marker at its position, and a chunk re-takes the
//! `R` entries of its batch, `FOR UPDATE`, sorted, before it reads any `M`
//! (the R-entry rule, #1459 Decision item 1). A `TRUNCATE` of any table
//! clears every position's markers.
//!
//! The fixture is #1459's n = 3 state (`support/star_cx.rs`), with the
//! fan-out chunk set to one row, so a key with two `a` rows at a position
//! is left to chunks there and a key with one is recomputed in the page.
//! Every pin runs under every kind tuple and ends with the oracle: the
//! target is the live multi-join, `R` is `a` and each `M_p` is `b{p}`. An
//! asymmetric family runs in both orientations.
//!
//! - **CX F10a**: a chunk at one position against a page at the other on
//!   the chunk's row, either one frozen first.
//! - **CX F10c**: chunks at both positions on one row.
//! - **PX8, PX9**: a held row and the fan-out at position 2.
//! - **XT2**: a `TRUNCATE` against a pending marker at another position.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;
#[path = "support/star_cx.rs"]
mod star_cx;

use drain_driver::Driver;
use star::{Shape, Star};
use star_cx::{N3, Orientation, SEED, knob};
use trellis::dev::defs::set_one_to_one_join_fan_out_chunk_rows;
use trellis::staging::OneToOneJoinFanOutStep;
use trellis::staging::interleave::PausePoint;
use trellis::staging::quarantine;

/// Puts the process-wide fan-out chunk back when a pin ends, even on a
/// panic.
struct Chunked;

impl Drop for Chunked {
    fn drop(&mut self) {
        set_one_to_one_join_fan_out_chunk_rows(0);
    }
}

/// Turns the knob on and sets a one-row chunk, until the guard drops.
fn chunked() -> Chunked {
    knob();
    set_one_to_one_join_fan_out_chunk_rows(1);
    Chunked
}

/// [`SEED`] with `a` row 5 on `(1, 1)`: key 1 has three rows at position 1
/// (1, 4 and 5) and two at position 2 (1 and 5), so a change of either
/// table's row 1 leaves it to chunks.
const SHARED_SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                           (1, 1, 1, 7, 'a1'), (2, 2, 2, 8, 'a2'), (3, 4, 4, 9, 'a3'), \
                           (4, 1, 5, 6, 'a4'), (5, 1, 1, 5, 'a5'); \
                           insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), \
                           (3, 3, 30, 'b3'); \
                           insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                           (3, 3, 300, 'e3'), (4, 4, 400, 'e4')";

/// `star`'s pending markers, as `(position, join key, cursor)`.
async fn markers(d: &Driver, star: Star) -> Vec<String> {
    d.rows(&format!(
        "select p.position, p.join_key, p.cursor from join1_fan_out p \
         join transform_definitions d on d.id = p.transform_id \
         where d.target_table = '{}' order by p.position, p.join_key",
        star.target()
    ))
    .await
}

/// Writes `sql` and drains it in a batch of its own.
async fn step(d: &mut Driver, sql: &str) {
    star::step(d, sql).await;
}

/// Starts one chunk step in its own task, frozen at each of `points`.
async fn chunk(
    d: &Driver,
    star: Star,
    points: &[PausePoint],
) -> drain_driver::Running<OneToOneJoinFanOutStep> {
    let target = star.target();
    let points: Vec<(PausePoint, &str)> = points.iter().map(|p| (*p, target.as_str())).collect();
    d.run_frozen(&points, |pool| async move {
        trellis::staging::one_to_one_join_fan_out_step(&pool).await
    })
    .await
}

/// Asserts nothing deadlocked.
fn assert_clean(d: &Driver, name: &str) {
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{name}: {deadlocks:?}");
}

// ---------------------------------------------------------------- CX F10

/// F10a, page first: a change of `b1`'s row 1 leaves key 1 (rows 1 and 4)
/// to chunks at position 1. A page changing `b2`'s row 1, which fans out in
/// the page to row 1, is frozen at its commit holding row 1's `R` entry and
/// its row; the chunk of row 1 then queues on that entry. A chunk that
/// didn't re-take it would read `b2`'s old entry, queue on the row the page
/// wrote, and write it over the page's once the page commits.
///
/// Pins the `star_chunk_no_r` plant (the oracle fails: row 1 keeps `b2`'s
/// old `w`).
#[tokio::test]
async fn cx_f10a_a_chunk_queues_on_the_r_entry_of_a_page_at_the_other_position() {
    let _serial = star::serial().await;
    let _chunked = chunked();
    for orientation in Orientation::BOTH {
        for star in N3.stars() {
            let name = format!("{} {orientation:?}", star.name());
            let mut d = star::start_seeded(&[star], &orientation.sql(SEED)).await;
            step(
                &mut d,
                &orientation.sql("update public.b1 set w = 11 where id = 1"),
            )
            .await;
            let at = match orientation {
                Orientation::AsWritten => 1,
                Orientation::Swapped => 2,
            };
            assert_eq!(
                markers(&d, star).await,
                [format!("({at},{{1}},)")],
                "{name}: key 1 is left to chunks"
            );
            star::write(
                &d,
                &orientation.sql("update public.b2 set w = 101 where id = 1"),
            )
            .await;
            let batch = d.seal().await;
            let target = star.target();
            let mut page = d
                .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &target)])
                .await;
            let reached = page.reached(PausePoint::BeforeCommit).await;
            let running = chunk(&d, star, &[]).await;
            d.wait_blocked_behind(reached.backend_pid).await;
            d.release(&mut page, PausePoint::BeforeCommit).await;
            page.finish().await;
            assert_eq!(
                running.finish().await,
                OneToOneJoinFanOutStep::Chunk,
                "{name}"
            );
            assert_clean(&d, &name);
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// F10a, chunk first: the chunk of row 1 is frozen once it holds row 1's
/// `R` entry, before it reads any `M`; the page changing `b2`'s row 1 queues
/// on that entry and commits after the chunk.
///
/// Pins the `star_chunk_no_r` plant (the page doesn't queue).
#[tokio::test]
async fn cx_f10a_a_page_at_the_other_position_queues_on_a_chunks_r_entry() {
    let _serial = star::serial().await;
    let _chunked = chunked();
    for orientation in Orientation::BOTH {
        for star in N3.stars() {
            let name = format!("{} {orientation:?}", star.name());
            let mut d = star::start_seeded(&[star], &orientation.sql(SEED)).await;
            step(
                &mut d,
                &orientation.sql("update public.b1 set w = 11 where id = 1"),
            )
            .await;
            let mut running = chunk(&d, star, &[PausePoint::AfterJoin1FanOutRLock]).await;
            let reached = running.reached(PausePoint::AfterJoin1FanOutRLock).await;
            star::write(
                &d,
                &orientation.sql("update public.b2 set w = 101 where id = 1"),
            )
            .await;
            let batch = d.seal().await;
            let page = d.drain_frozen(batch, "page", &[]).await;
            let blocked = star::queued_or_finished(&d, reached.backend_pid, &page).await;
            d.release(&mut running, PausePoint::AfterJoin1FanOutRLock)
                .await;
            assert_eq!(
                running.finish().await,
                OneToOneJoinFanOutStep::Chunk,
                "{name}"
            );
            page.finish().await;
            assert_clean(&d, &name);
            star::assert_oracle(&mut d, &[star]).await;
            assert!(
                blocked,
                "{name}: the page ran without queueing on the chunk's R entry"
            );
        }
    }
}

/// F10c: changes of `b1`'s and `b2`'s row 1 leave key 1 to chunks at both
/// positions, and the first chunk of each takes row 1. The first, at
/// position 1, is frozen once it holds row 1's `R` entry; the other, at
/// position 2 (its `J` row is free), queues on that entry.
///
/// Pins the `star_chunk_no_r` plant (the second chunk doesn't queue).
#[tokio::test]
async fn cx_f10c_chunks_at_two_positions_on_one_row_queue_on_its_r_entry() {
    let _serial = star::serial().await;
    let _chunked = chunked();
    for star in N3.stars() {
        let name = star.name();
        let mut d = star::start_seeded(&[star], SHARED_SEED).await;
        step(&mut d, "update public.b1 set w = 11 where id = 1").await;
        step(&mut d, "update public.b2 set w = 101 where id = 1").await;
        assert_eq!(
            markers(&d, star).await,
            ["(1,{1},)", "(2,{1},)"],
            "{name}: key 1 is left to chunks at both positions"
        );
        let mut first = chunk(&d, star, &[PausePoint::AfterJoin1FanOutRLock]).await;
        let reached = first.reached(PausePoint::AfterJoin1FanOutRLock).await;
        let second = chunk(&d, star, &[]).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &second).await;
        d.release(&mut first, PausePoint::AfterJoin1FanOutRLock)
            .await;
        assert_eq!(
            first.finish().await,
            OneToOneJoinFanOutStep::Chunk,
            "{name}"
        );
        assert_eq!(
            second.finish().await,
            OneToOneJoinFanOutStep::Chunk,
            "{name}"
        );
        assert_eq!(
            markers(&d, star).await,
            ["(1,{1},1)", "(2,{1},1)"],
            "{name}: each chunk recomputed row 1"
        );
        assert_clean(&d, &name);
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{name}: the chunk at position 2 ran without queueing on row 1's R entry"
        );
    }
}

// ---------------------------------------------------------------- XT2

/// XT2 at the root: a `TRUNCATE` of `a` against a pending marker at
/// position 2 deletes it in its page, as it does every position's.
///
/// Pins the `star_truncate_keeps_other_markers` plant.
#[tokio::test]
async fn xt2_a_truncate_of_a_clears_a_pending_marker_at_another_position() {
    let _serial = star::serial().await;
    let _chunked = chunked();
    for star in N3.stars() {
        let name = star.name();
        let mut d = star::start_seeded(&[star], SHARED_SEED).await;
        step(&mut d, "update public.b2 set w = 101 where id = 1").await;
        assert_eq!(markers(&d, star).await, ["(2,{1},)"], "{name}");
        step(&mut d, "truncate public.a").await;
        assert!(
            markers(&d, star).await.is_empty(),
            "{name}: the truncate's page left the marker at position 2"
        );
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// XT2: a `TRUNCATE` of `b1` against a pending marker at position 2 (#1464)
/// deletes it in its page: the rebuild it starts recomputes every row.
///
/// Pins the `star_truncate_keeps_other_markers` plant.
#[tokio::test]
async fn xt2_a_truncate_of_b_clears_a_pending_marker_at_another_position() {
    let _serial = star::serial().await;
    let _chunked = chunked();
    for star in N3.stars() {
        let name = star.name();
        let mut d = star::start_seeded(&[star], SHARED_SEED).await;
        step(&mut d, "update public.b2 set w = 101 where id = 1").await;
        assert_eq!(markers(&d, star).await, ["(2,{1},)"], "{name}");
        step(&mut d, "truncate public.b1").await;
        assert!(
            markers(&d, star).await.is_empty(),
            "{name}: the truncate's page left the marker at position 2"
        );
        star::assert_rebuilt(&mut d, &[star]).await;
    }
}

// ---------------------------------------------------------------- PX8, PX9

/// The fan-out at position 2 of three tables.
const AT_2: Shape = Shape {
    positions: 2,
    at: 2,
};

/// `a` rows `(id, k, x, y)` on key 10 at position 2: row 23's `total`
/// overflows `integer` once `b2`'s `w` passes 647 (position 1 adds 1000).
const HELD_A: &str = "(1, 10, 1, 'one'), (2, 10, 2, 'two'), (23, 10, 2147482000, 'big')";
const HELD_B: &str = "(1, 10, 100, 'ten')";

/// `star`'s row 23, as `(total, tag2)`.
async fn row_23(d: &Driver, star: Star) -> Vec<String> {
    d.rows(&format!(
        "select total, tag2 from {} where id = 23",
        star.target()
    ))
    .await
}

/// Holds row 23: `b2`'s `w` of 1000 overflows its `total`.
async fn hold_23(d: &mut Driver, star: Star) {
    step(d, "update public.b2 set w = 1000 where id = 1").await;
    assert_eq!(
        d.rows("select key from poison").await,
        ["(23)"],
        "{}: the fan-out holds row 23",
        star.name()
    );
}

/// PX8: a later change of `b2` on key 10 leaves the held row 23 out of its
/// fan-out, even once its fields would compute: the row stays as it was
/// until its release re-derives it.
///
/// Pins the `join1_fan_out_held_row` plant at position 2.
#[tokio::test]
async fn px8_a_fan_out_at_position_2_leaves_a_held_row_as_it_was() {
    let _serial = star::serial().await;
    knob();
    for star in AT_2.stars() {
        let name = star.name();
        let mut d = star::start(&[star], HELD_A, HELD_B).await;
        hold_23(&mut d, star).await;
        step(
            &mut d,
            "update public.b2 set w = 0, tag = 'zero' where id = 1",
        )
        .await;
        assert_eq!(
            row_23(&d, star).await,
            [format!("({},ten)", 2_147_482_000 + 1000 + 100)],
            "{name}: the held row isn't rewritten"
        );
        let released = quarantine::release_key(d.pool(), &name, "public.a", "23")
            .await
            .expect("release the key");
        assert_eq!(released, 1, "{name}");
        star::assert_oracle(&mut d, &[star]).await;
        assert!(d.rows("select key from poison").await.is_empty(), "{name}");
    }
}

/// PX9: a release of the held row 23 while a change of `b2` on its key
/// waits in an earlier batch: the change's fan-out runs before the release's
/// `Recompute` drains, so the release re-derives the row's `R` entry from its
/// live row in its own transaction, or the fan-out would read the state the
/// hold left and hold the row again.
///
/// Pins the `join1_release_no_rederive` plant at position 2.
#[tokio::test]
async fn px9_a_release_before_a_pending_fan_out_at_position_2_stays_released() {
    let _serial = star::serial().await;
    knob();
    for star in AT_2.stars() {
        let name = star.name();
        let mut d = star::start(&[star], HELD_A, HELD_B).await;
        hold_23(&mut d, star).await;
        step(&mut d, "update public.a set x = 23 where id = 23").await;
        star::write(&d, "update public.b2 set w = 1001 where id = 1").await;
        let pending = d.seal().await;
        let released = quarantine::release_key(d.pool(), &name, "public.a", "23")
            .await
            .expect("release the key");
        assert_eq!(released, 1, "{name}");
        d.drain(pending, "pending").await;
        assert!(
            d.rows("select key from poison").await.is_empty(),
            "{name}: the pending fan-out held the released row again"
        );
        star::assert_oracle(&mut d, &[star]).await;
    }
}
