//! A star 1-1 `JOIN`'s joined-table writers (#1774, #1460 sub-step 3.8;
//! #1464's CX and PS7 pins): a page of a joined table locks its own `J`
//! keys, then the `R` entries of every row on them, in the one `R` lock
//! statement with the page's root entries, before it reads any `M` (the
//! R-entry rule, #1459 Decision item 1). A joined entry's held key, and a
//! joined Re-derive's live key, are re-checked against the page's fan-out
//! keys at its position, not its whole `J` set (F11).
//!
//! The fixture is #1459's n = 3 state (`support/star_cx.rs`). Every pin runs
//! under every kind tuple, each writer frozen at its commit first while the
//! other queues on an `R` entry or `J` row the first holds, and ends with
//! the oracle: the target is the live multi-join, `R` is `a` and each `M_p`
//! is `b{p}`. An asymmetric family runs in both orientations.
//!
//! - **CX F1a–F1f**: a change of `b1` and one of `b2` meeting at one root
//!   row (value, insert, delete and key-move variants). Two joined pages at
//!   different positions share no `J` row; only the row's `R` entry orders
//!   them.
//! - **CX F4a–F4c**: a `b1` unique-key hand-off applied out of order, against
//!   a change of `b2` and a root insert on the key. The ring holds two
//!   undrained batches, so the hand-off's first half is written past capture
//!   and repaired by a Re-derive after the race, and F4c's third page runs
//!   after the other two.
//! - **CX F6e**: a Re-derive of a `b1` row whose key moved after its unlocked
//!   read, against a change of `b2` on a row of its new key.
//! - **CX F9c**: a root row moving to `NULL` at one position, against a page
//!   changing both joined tables.
//! - **CX F11a / PS7 (F11b)**: a mixed page whose `b1` entry's held key
//!   moved, after its key read, onto a key the page holds only for its root
//!   item: the page retries, and its fan-out then locks that key's rows.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;
#[path = "support/star_cx.rs"]
mod star_cx;

use star::Writer;
use star_cx::{CROSS_SEED, LATE_SEED, N3, Orientation, SEED};
use trellis::staging::interleave::PausePoint;

const SYM: &[Orientation] = &Orientation::ONE;
const ASYM: &[Orientation] = &Orientation::BOTH;

// ---------------------------------------------------------------- CX F1

/// F1a: `b1` row 1 and `b2` row 1 change value; both fan out to `a` row 1.
/// Planted `star_own_j_only`, `star_r_after_m` or `star_last_no_r`, the
/// second page reads the other's `M` before it commits and the row loses
/// one change.
#[tokio::test]
async fn cx_f1a_value_changes_at_two_positions_meet_at_one_root_row() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("update public.b1 set w = 11 where id = 1"),
        Writer::Change("update public.b2 set w = 101 where id = 1"),
        None,
        SYM,
    )
    .await;
}

/// F1b: a `b1` insert matching `a` row 3 ‖ a `b2` change on its row (under
/// `INNER` at position 1 the row has no target row yet, which is why the
/// rejected alternative of locking target rows loses it). Planted
/// `star_own_j_only` or `star_r_after_m`.
#[tokio::test]
async fn cx_f1b_an_insert_at_one_position_meets_a_change_at_the_other() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("insert into public.b1 values (4, 4, 40, 'b4')"),
        Writer::Change("update public.b2 set w = 401 where id = 4"),
        None,
        SYM,
    )
    .await;
}

/// F1c: a `b1` delete ‖ a `b2` change, both on `a` row 1; both orientations
/// (`star_last_no_r` fails it with the change at the last position).
#[tokio::test]
async fn cx_f1c_a_delete_at_one_position_meets_a_change_at_the_other() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("delete from public.b1 where id = 1"),
        Writer::Change("update public.b2 set w = 101 where id = 1"),
        None,
        ASYM,
    )
    .await;
}

/// F1d: `b1` row 1 and `b2` row 1 both move off key 1, leaving `a` row 1.
/// Planted `star_own_j_only`.
#[tokio::test]
async fn cx_f1d_moves_off_at_two_positions_meet_at_one_root_row() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("update public.b1 set k = 5 where id = 1"),
        Writer::Change("update public.b2 set k = 6 where id = 1"),
        None,
        SYM,
    )
    .await;
}

/// F1e: a `b1` change ‖ a `b2` insert matching `a` row 4 (`(1, 5)`), F1b's
/// mirror. Planted `star_own_j_only` or `star_r_after_m`.
#[tokio::test]
async fn cx_f1e_a_change_at_one_position_meets_an_insert_at_the_other() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("update public.b1 set w = 11 where id = 1"),
        Writer::Change("insert into public.b2 values (5, 5, 500, 'e5')"),
        None,
        SYM,
    )
    .await;
}

/// F1f: `b1` row 3 moves onto key 4, `a` row 3's ‖ a change of `b2` row 4,
/// on the same row; both orientations. Planted `star_own_j_only`.
#[tokio::test]
async fn cx_f1f_a_move_onto_a_key_meets_a_change_at_the_other_position() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("update public.b1 set k = 4 where id = 3"),
        Writer::Change("update public.b2 set w = 401 where id = 4"),
        None,
        ASYM,
    )
    .await;
}

// ---------------------------------------------------------------- CX F4

/// The first half of F4's hand-off: `b1` row 1 leaves key 1. Written past
/// capture, so `M` still holds it on key 1 while the second half applies
/// (out of order), and repaired by a Re-derive of the row after the race.
const LEAVE: (&str, &str) = ("public.b1", "update public.b1 set k = 6 where id = 1");
/// The Re-derive that applies [`LEAVE`] after the race.
const LEAVE_AFTER: Writer = Writer::Rederive("public.b1", &["1"]);
/// The second half: `b1` row 5 takes key 1.
const TAKE: &str = "insert into public.b1 values (5, 1, 50, 'b5')";
/// An `a` insert on `(1, 1)`.
const A5: &str = "insert into public.a (id, k1, k2, x, y) values (5, 1, 1, 5, 'a5')";

/// F4a: the take ‖ a page with a `b2` change on key 1 and an `a` insert on
/// `(1, 1)`, before the leave applies (`join1_no_placeholder`; the R-entry
/// lock now orders the pages without `J`, so `join1_m_before_j` passes it).
#[tokio::test]
async fn cx_f4a_a_hand_off_out_of_order_races_a_change_and_a_root_insert() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[LEAVE],
        Writer::Change(TAKE),
        Writer::Change(
            "update public.b2 set w = 101 where id = 1; \
             insert into public.a (id, k1, k2, x, y) values (5, 1, 1, 5, 'a5')",
        ),
        Some(LEAVE_AFTER),
        ASYM,
    )
    .await;
}

/// F4b: the take ‖ a page with the leave and a `b2` change on key 1, then an
/// `a` insert on `(1, 1)` after them.
#[tokio::test]
async fn cx_f4b_a_hand_off_out_of_order_races_its_first_half_and_a_change() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change(
            "update public.b1 set k = 6 where id = 1; update public.b2 set w = 101 where id = 1",
        ),
        Writer::Change(TAKE),
        Some(Writer::Change(A5)),
        ASYM,
    )
    .await;
}

/// F4c: the take ‖ a `b2` change on key 1, each its own page, then an `a`
/// insert on `(1, 1)` in a third page after them (the ring holds two
/// undrained batches, so not three in flight), then the leave.
#[tokio::test]
async fn cx_f4c_a_hand_off_out_of_order_races_a_change_then_a_root_insert() {
    let _serial = star::serial().await;
    for orientation in ASYM {
        for star in N3.stars() {
            for flip in [false, true] {
                let mut d = star::start_seeded(&[star], &orientation.sql(SEED)).await;
                star::write_uncaptured(&d, &orientation.sql(LEAVE.0), &orientation.sql(LEAVE.1))
                    .await;
                let take = orientation.writer(Writer::Change(TAKE)).seal(&mut d).await;
                let change = orientation
                    .writer(Writer::Change("update public.b2 set w = 101 where id = 1"))
                    .seal(&mut d)
                    .await;
                let (x, y) = if flip { (change, take) } else { (take, change) };
                star_cx::race_at(&mut d, star, x, y, PausePoint::BeforeCommit, false).await;
                let insert = orientation.writer(Writer::Change(A5)).seal(&mut d).await;
                d.drain(insert, "insert").await;
                let leave = orientation.writer(LEAVE_AFTER).seal(&mut d).await;
                d.drain(leave, "leave").await;
                star::assert_oracle(&mut d, &[star]).await;
            }
        }
    }
}

// ---------------------------------------------------------------- CX F6e

/// F6e: a Re-derive of `b1` row 1 reads its key (1) unlocked; the row then
/// moves to key 4, `a` row 3's, and a page changing `b2` row 4 (also `a` row
/// 3's) holds that row's `R` entry, frozen at its commit. The Re-derive's
/// read finds key 4 outside its locked set and retries; the retry's R-entry
/// lock on key 4's rows queues behind the `b2` page and reads its `M` entry
/// once it commits. Both orientations. Planted `star_own_j_only` or
/// `star_r_after_m`, the retry recomputes `a` row 3 from `b2`'s `M` before
/// that page commits.
#[tokio::test]
async fn cx_f6e_a_joined_rederive_whose_row_moved_races_a_change_at_the_other_position() {
    let _serial = star::serial().await;
    for orientation in ASYM {
        for star in N3.stars() {
            let mut d = star::start_seeded(&[star], &orientation.sql(SEED)).await;
            d.stage_recomputes(&orientation.sql("public.b1"), &["1"])
                .await;
            let rederive = d.seal().await;
            star::write(
                &d,
                &orientation.sql("update public.b2 set w = 401 where id = 4"),
            )
            .await;
            let joined = d.seal().await;
            let moves = trellis::dev::interleave::join_key_moves();
            let target = star.target();
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive",
                    &[(PausePoint::BeforeJoin1Lock, &target)],
                )
                .await;
            page.reached(PausePoint::BeforeJoin1Lock).await;
            // Captured, left for the settle: the Re-derive's basis sees it.
            star::write(
                &d,
                &orientation.sql("update public.b1 set k = 4 where id = 1"),
            )
            .await;
            let mut held = d
                .drain_frozen(joined, "joined", &[(PausePoint::BeforeCommit, &target)])
                .await;
            let reached = held.reached(PausePoint::BeforeCommit).await;
            d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
            let blocked = star::queued_or_finished(&d, reached.backend_pid, &page).await;
            d.release(&mut held, PausePoint::BeforeCommit).await;
            held.finish().await;
            let result = page.finish_result().await;
            let name = star.name();
            if let Err(err) = &result {
                panic!("{name} {orientation:?}: the Re-derive page failed: {err}");
            }
            let retried = trellis::dev::interleave::join_key_moves() - moves;
            star::assert_oracle(&mut d, &[star]).await;
            assert!(
                blocked,
                "{name} {orientation:?}: the Re-derive ran without queueing on the row's R entry"
            );
            assert_eq!(
                retried, 1,
                "{name} {orientation:?}: the Re-derive's read found a key it hadn't locked"
            );
        }
    }
}

// ---------------------------------------------------------------- CX F9c

/// F9c: `a` row 1 moves to `NULL` at position 2 ‖ a page changing `b1` row
/// 1 and `b2` row 1, its keys before the move; both orientations. The root
/// page locks `J` for the key `R` holds at position 2 (and its key at
/// position 1), which the joined page locks too, so `J` orders them here:
/// neither `star_own_j_only` nor `join1_no_entry_lock` fails it.
#[tokio::test]
async fn cx_f9c_a_root_row_moving_to_null_races_a_page_of_both_joined_tables() {
    let _serial = star::serial().await;
    star_cx::family(
        SEED,
        &[],
        Writer::Change("update public.a set k2 = null where id = 1"),
        Writer::Change(
            "update public.b1 set w = 11 where id = 1; update public.b2 set w = 101 where id = 1",
        ),
        None,
        ASYM,
    )
    .await;
}

// ---------------------------------------------------------------- F11

/// #1459's F11 shape: `b1` row 7 moves `71 → 5` (its own page, `P1`), then
/// `5 → 9` (past capture), then changes value at 9 in a mixed page
/// (`P2`) with a change of `a` row 7, which is on key 5 at position 1. `P2`
/// reads its key set while `M` still holds 71, so its fan-out keys at
/// position 1 are 71 and 9, and it locks key 5 only for its root item. `P1`
/// then commits, so `b1` row 7's entry holds key 5 when `P2` locks it: in
/// `J`, but not among its fan-out keys. `P2` retries (F11), and its fan-out
/// then locks the `R` entries on key 5, `a` rows 7 and 8.
///
/// With `third`, a page changing `b2` row 3 (`a` row 8's, F11a) is frozen at
/// its commit holding row 8's `R` entry when `P2` resumes, and `P2`'s retry
/// queues on it. Planted `star_recheck_j` (or `star_recheck_skip_last` in the
/// swapped orientation, the late key at the last position), `P2` doesn't
/// retry, and recomputes row 8 from `b2`'s `M` before that page commits.
async fn late_m_key(third: bool) {
    for orientation in Orientation::BOTH {
        for star in N3.stars() {
            let mut d = star::start_seeded(&[star], &orientation.sql(LATE_SEED)).await;
            let o = |sql: &str| orientation.sql(sql);
            star::write(&d, &o("update public.b1 set k = 5 where id = 7")).await;
            let p1 = d.seal().await;
            // The move `5 → 9`, past capture: `P2`'s image carries key 9.
            star::write_uncaptured(
                &d,
                &o("public.b1"),
                &o("update public.b1 set k = 9 where id = 7"),
            )
            .await;
            star::write(
                &d,
                &o("update public.b1 set w = 77 where id = 7; \
                    update public.a set x = 2 where id = 7"),
            )
            .await;
            let p2 = d.seal().await;
            let target = star.target();
            let name = format!("{} {orientation:?}", star.name());
            let mut mixed = d
                .drain_frozen(p2, "mixed", &[(PausePoint::BeforeJoin1Lock, &target)])
                .await;
            mixed.reached(PausePoint::BeforeJoin1Lock).await;
            d.drain(p1, "move").await;
            star::write(&d, &o("update public.b2 set w = 301 where id = 3")).await;
            let p3 = d.seal().await;
            let moves = trellis::dev::interleave::join_key_moves();
            let (blocked, result) = if third {
                let mut held = d
                    .drain_frozen(p3, "third", &[(PausePoint::BeforeCommit, &target)])
                    .await;
                let reached = held.reached(PausePoint::BeforeCommit).await;
                d.release(&mut mixed, PausePoint::BeforeJoin1Lock).await;
                let blocked = star::queued_or_finished(&d, reached.backend_pid, &mixed).await;
                d.release(&mut held, PausePoint::BeforeCommit).await;
                held.finish().await;
                (blocked, mixed.finish_result().await)
            } else {
                d.release(&mut mixed, PausePoint::BeforeJoin1Lock).await;
                (true, mixed.finish_result().await)
            };
            if let Err(err) = &result {
                panic!("{name}: the mixed page failed: {err}");
            }
            let retried = trellis::dev::interleave::join_key_moves() - moves;
            if !third {
                d.drain(p3, "third").await;
            }
            star::assert_oracle(&mut d, &[star]).await;
            assert!(
                blocked,
                "{name}: the mixed page's retry ran without queueing on a8's R entry"
            );
            assert_eq!(
                retried, 1,
                "{name}: the mixed page's b1 entry held a key outside its fan-out keys"
            );
        }
    }
}

/// CX F11a: F11 with the third page (`star_recheck_j`; swapped,
/// `star_recheck_skip_last`).
#[tokio::test]
async fn cx_f11a_a_joined_entry_whose_key_moved_onto_a_root_key_retries_under_a_third_page() {
    let _serial = star::serial().await;
    late_m_key(true).await;
}

/// PS7 (F11b): F11 with two writers. Nothing is lost without the retry, as
/// #1459's model found (only F11a fails there): no other writer is on key
/// 5's rows. The pin is the retry itself (`star_recheck_j`; swapped,
/// `star_recheck_skip_last`).
#[tokio::test]
async fn ps7_a_joined_entry_whose_key_moved_onto_a_root_key_retries() {
    let _serial = star::serial().await;
    late_m_key(false).await;
}

/// The crossing state's `b1` and `b2` change of one row each, in one page:
/// both positions fan out, to `a` rows 1 and 2 (a smoke check of the
/// fixture the crossing pins share).
#[tokio::test]
async fn a_page_of_both_joined_tables_fans_out_at_both_positions() {
    let _serial = star::serial().await;
    let stars = N3.stars();
    let mut d = star::start_seeded(&stars, CROSS_SEED).await;
    star::step(
        &mut d,
        "update public.b1 set w = 11 where id = 1; update public.b2 set w = 101 where id = 1",
    )
    .await;
    star::assert_oracle(&mut d, &stars).await;
}
