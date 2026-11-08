//! A star 1-1 `JOIN`'s crossing-position pages (#1774, #1460 sub-step 3.8;
//! #1464's CX F3b, F3d, F7a, F7b and its Q2 pin): two pages each writing
//! both joined tables, or a root row and a joined table, whose fan-outs
//! reach the same root rows from opposite positions.
//!
//! The R-entry rule locks every `R` entry a page recomputes in **one**
//! sorted statement with its root entries (#1459 Decision item 1), so two
//! such pages queue on the first row they share and never deadlock. Split
//! per position, each would lock one row and queue on the other's
//! (`star_r_split`). Once those entries are locked, the target may be
//! written in per-position batches (#1464 Q2): the `R` entries already order
//! every writer of a row.
//!
//! Each family runs under every kind tuple, either page first, frozen at
//! three points while the other queues on it: its commit, right after its
//! first `R` lock statement, and after its first position's fan-out write.
//! Every run asserts that nothing deadlocked (`deadlocks_logged()` is
//! empty) and ends with the oracle.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;
#[path = "support/star_cx.rs"]
mod star_cx;

use star_cx::{CROSS_SEED, N3, SEED, knob};
use trellis::staging::interleave::PausePoint;

/// The points the first page is frozen at.
const POINTS: [PausePoint; 3] = [
    PausePoint::BeforeCommit,
    PausePoint::AfterJoin1RLock,
    PausePoint::AfterJoin1PositionFanOut,
];

/// Two pages `first` and `second` (captured writes, each its own batch)
/// over `seed`, frozen in turn at each of `points`.
async fn crossing(seed: &str, first: &str, second: &str, points: &[PausePoint]) {
    for star in N3.stars() {
        for flip in [false, true] {
            for point in points {
                let mut d = star::start_seeded(&[star], seed).await;
                star::write(&d, first).await;
                let one = d.seal().await;
                star::write(&d, second).await;
                let two = d.seal().await;
                let (x, y) = if flip { (two, one) } else { (one, two) };
                star_cx::race_at(&mut d, star, x, y, *point, true).await;
            }
        }
    }
}

/// CX F3b (#1457 E2): `a` row 1 is on `(1, 2)` and row 2 on `(2, 1)`. One
/// page changes `b1` row 1 and `b2` row 1 (fanning out to rows 1 and 2),
/// the other `b1` row 2 and `b2` row 2 (rows 2 and 1). Planted
/// `star_r_split`, the first page frozen after its first `R` statement holds
/// row 1's entry, the second locks row 2's and queues on row 1's, and the
/// first's second statement closes the cycle.
#[tokio::test]
async fn cx_f3b_crossing_mixed_pages_of_both_joined_tables_queue_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    crossing(
        CROSS_SEED,
        "update public.b1 set w = 11 where id = 1; update public.b2 set w = 101 where id = 1",
        "update public.b1 set w = 21 where id = 2; update public.b2 set w = 201 where id = 2",
        &POINTS,
    )
    .await;
}

/// #1464 Q2: F3b's pages frozen after the first position's fan-out write,
/// holding row 1's target row: the second page queues on row 1's `R` entry
/// before writing anything, so the per-position target batches never
/// deadlock. Planted `star_own_j_only`, the second page writes row 2 at its
/// first position and queues on row 1's target row, and the first page's
/// second position closes the cycle.
#[tokio::test]
async fn crossing_mixed_pages_write_per_position_target_batches_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    crossing(
        CROSS_SEED,
        "update public.b1 set w = 11 where id = 1; update public.b2 set w = 101 where id = 1",
        "update public.b1 set w = 21 where id = 2; update public.b2 set w = 201 where id = 2",
        &[PausePoint::AfterJoin1PositionFanOut],
    )
    .await;
}

/// CX F3d: a page moving `a` row 1 at position 1 with a change of `b1` row 2
/// ‖ a page moving `a` row 2 at position 2 with a change of `b2` row 2.
/// They share `J` rows, which order them.
#[tokio::test]
async fn cx_f3d_crossing_a_root_move_and_a_joined_change_queue_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    crossing(
        CROSS_SEED,
        "update public.a set k1 = 2 where id = 1; update public.b1 set w = 21 where id = 2",
        "update public.a set k2 = 2 where id = 2; update public.b2 set w = 201 where id = 2",
        &[PausePoint::BeforeCommit, PausePoint::AfterJoin1RLock],
    )
    .await;
}

/// CX F7a: two mixed pages of all three tables over #1459's base state.
#[tokio::test]
async fn cx_f7a_mixed_pages_of_all_three_tables_queue_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    crossing(
        SEED,
        "update public.a set k1 = 2 where id = 1; update public.b1 set w = 21 where id = 2; \
         update public.b2 set w = 101 where id = 1",
        "update public.a set k2 = 1 where id = 2; update public.b1 set w = 11 where id = 1; \
         update public.b2 set w = 201 where id = 2",
        &POINTS,
    )
    .await;
}

/// CX F7b: a page of moves (`a` row 1 on both keys, `b1` row 1 off key 1, a
/// `b2` change) ‖ a page of deletes and inserts (`a` row 2, a `b1` change, a
/// `b2` insert).
#[tokio::test]
async fn cx_f7b_mixed_moves_race_mixed_deletes_and_inserts_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    crossing(
        SEED,
        "update public.a set k1 = 2, k2 = 2 where id = 1; update public.b1 set k = 7 where id = 1; \
         update public.b2 set w = 201 where id = 2",
        "delete from public.a where id = 2; update public.b1 set w = 21 where id = 2; \
         insert into public.b2 values (5, 5, 500, 'e5')",
        &[PausePoint::BeforeCommit, PausePoint::AfterJoin1RLock],
    )
    .await;
}
