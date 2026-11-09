//! A star 1-1 `JOIN`'s root writers (#1773, #1460 sub-step 3.7; #1464's PS
//! and CX pins): an `a` page, Re-derive or build chunk locks `J` at every
//! position, for its new key and the key `R` holds, re-checks every key at
//! every position against its locked set, and recomputes the row joining
//! every position's newest live `M` entry.
//!
//! The star is #1464's fixture: `a(id, k1, k2, x, y)` joined to `b1`
//! (position 1, the model's `b`) and `b2` (position 2, the model's `e`), and
//! `b3` at n = 4. Every pin runs under every kind tuple, each writer frozen at
//! its commit first while the other queues on a `J` row the first holds
//! (`star::race`), and ends with the oracle: the target is the live
//! multi-join, `R` is `a` and each `M_p` is `b{p}`.
//!
//! - **PS1a / PS1b** (F2c): a root row moving both keys ‖ a change of the
//!   joined table at the last / first position on its new key.
//! - **PS2** (F2d): the same ‖ a change on the key it leaves at the last
//!   position.
//! - **PS4a / PS4b** (F6b / F6a): a root Re-derive whose row moved at the last
//!   / first position after its unlocked read, while a joined page on the new
//!   key holds it: the late-key retry locks it.
//! - **PS5** (F6d): a root Re-derive of a row that moved past capture ‖ a
//!   change on its new key at the last position.
//! - **CX F2a / F2b**: a root move of both keys ‖ one page changing the new
//!   key at one position and the old key at the other.
//! - **CX F2e**: a root move of both keys ‖ a change on each new key, in
//!   pages of their own.
//! - **CX F3a / F3c**: two root moves crossing both keys, or crossed keys,
//!   queue on `J` without deadlock.
//! - **n = 4**: a root row moving all three keys ‖ a change on its new key at
//!   the middle position.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use star::{Shape, Writer};
use trellis::staging::interleave::PausePoint;

/// The star of three tables, its last position under test.
const N3: Shape = Shape {
    positions: 2,
    at: 2,
};

/// #1459's base state at n = 3: `a` rows `(id, k1, k2, x, y)`, `b1` rows on
/// keys 1 to 3 and `b2` rows on keys 1 to 4.
const SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                    (1, 1, 1, 7, 'a1'), (2, 2, 2, 8, 'a2'), (3, 4, 4, 9, 'a3'), (4, 1, 5, 6, 'a4'); \
                    insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), (3, 3, 30, 'b3'); \
                    insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                    (3, 3, 300, 'e3'), (4, 4, 400, 'e4')";

/// #1459's crossing state: `a` row 1 on `(1, 2)` and row 2 on `(2, 1)`.
const CROSS_SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                          (1, 1, 2, 7, 'a1'), (2, 2, 1, 8, 'a2'); \
                          insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'); \
                          insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2')";

const MOVE_BOTH: Writer = Writer::Change("update public.a set k1 = 2, k2 = 2 where id = 1");

/// PS1a (F2c): `a` row 1 moves both keys `(1, 1) → (2, 2)` ‖ a change of
/// `b2` row 2, on its new key at the last position. Each queues on `J` at
/// position 2 for key 2. Planted `star_root_skip_j_last`, the root page
/// leaves that key unlocked and one of them recomputes from a stale `M`.
#[tokio::test]
async fn ps1a_a_root_moving_both_keys_races_a_change_of_its_new_key_at_the_last_position() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change("update public.b2 set w = 201 where id = 2"),
    )
    .await;
}

/// PS1b (F2c mirrored): the same move ‖ a change of `b1` row 2, on its new
/// key at the first position. Planted `star_root_skip_j_first`.
#[tokio::test]
async fn ps1b_a_root_moving_both_keys_races_a_change_of_its_new_key_at_the_first_position() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change("update public.b1 set w = 21 where id = 2"),
    )
    .await;
}

/// PS2 (F2d): the same move ‖ a change of `b2` row 1, on the key `R` holds
/// at the last position, which the root page locks too. Planted
/// `star_root_skip_j_last`.
#[tokio::test]
async fn ps2_a_root_moving_both_keys_races_a_change_of_the_key_it_leaves_at_the_last_position() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change("update public.b2 set w = 101 where id = 1"),
    )
    .await;
}

/// PS4 (F6a / F6b): a Re-derive of `a` row 1 reads its keys unlocked; its
/// row then moves to key 2 at `position`, and a page changing the row of
/// the joined table there on key 2 (`joined_sql`) holds `J` for it,
/// frozen at its commit. The Re-derive's read finds the moved key outside
/// its locked set, rolls back and retries, and the retry queues behind the
/// joined page and reads its `M` entry once it commits.
async fn late_key_at(position: usize, joined_sql: &str) {
    let shape = N3;
    let late = format!("update public.a set k{position} = 2 where id = 1");
    for star in shape.stars() {
        let mut d = star::start_seeded(&[star], SEED).await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        star::write(&d, joined_sql).await;
        let joined = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let target = star.target();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive a",
                &[(PausePoint::BeforeJoin1Lock, &target)],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, &late).await;
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
        let retried = trellis::dev::interleave::join_key_moves() - moves;
        if let Err(err) = &result {
            panic!("{name}: the Re-derive page failed: {err}");
        }
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{name}: the Re-derive ran without queueing on the joined page's J row"
        );
        assert_eq!(
            retried, 1,
            "{name}: the Re-derive's read found a key at position {position} it hadn't locked"
        );
    }
}

/// PS4a (F6b): the late key at the last position. Planted
/// `star_late_skip_last`, the Re-derive doesn't retry, reads `b2`'s `M` on
/// key 2 unlocked and writes the row from the entry before the joined page.
#[tokio::test]
async fn ps4a_a_root_rederive_whose_key_moved_at_the_last_position_retries() {
    let _serial = star::serial().await;
    late_key_at(2, "update public.b2 set w = 201 where id = 2").await;
}

/// PS4b (F6a): the late key at the first position. Planted
/// `star_late_skip_first`.
#[tokio::test]
async fn ps4b_a_root_rederive_whose_key_moved_at_the_first_position_retries() {
    let _serial = star::serial().await;
    late_key_at(1, "update public.b1 set w = 21 where id = 2").await;
}

/// PS5 (F6d): `a` row 1 moved to key 2 at the last position past capture,
/// so `R` still holds key 1; a Re-derive of it reads key 2 live and locks it
/// with the key `R` holds ‖ a change of `b2` row 2 on key 2. Planted
/// `star_root_skip_j_last`.
#[tokio::test]
async fn ps5_a_root_rederive_races_a_change_of_its_moved_key_at_the_last_position() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[("public.a", "update public.a set k2 = 2 where id = 1")],
        Writer::Rederive("public.a", &["1"]),
        Writer::Change("update public.b2 set w = 201 where id = 2"),
    )
    .await;
}

/// CX F2a: `a` row 1 moves both keys ‖ one page changing `b1` row 2 (its new
/// key at position 1) and `b2` row 1 (its old key at position 2). Both
/// pages lock both keys in one sorted statement, so whichever comes second
/// queues. Planted `join1_m_before_j`, the root reads `M` before `J`.
#[tokio::test]
async fn cx_f2a_a_root_move_races_a_page_on_its_new_key_and_its_old_key() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change(
            "update public.b1 set w = 21 where id = 2; update public.b2 set w = 101 where id = 1",
        ),
    )
    .await;
}

/// CX F2b, F2a's mirror: the page changes `b1` row 1 (the old key at
/// position 1) and `b2` row 2 (the new key at position 2).
#[tokio::test]
async fn cx_f2b_a_root_move_races_a_page_on_its_old_key_and_its_new_key() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change(
            "update public.b1 set w = 11 where id = 1; update public.b2 set w = 201 where id = 2",
        ),
    )
    .await;
}

/// CX F2e: `a` row 1 moves both keys ‖ a change of `b1` row 2 and, in a page
/// of its own, of `b2` row 2: each on a new key. The four-slot ring holds
/// two undrained batches, so the three pages run as a race of two and a
/// third after: the root page frozen at its commit while one joined page
/// queues on it, or a joined page frozen while the root page queues on it,
/// then the other joined page. Planted `star_root_skip_j_last` (or
/// `_first`), the joined page at that position runs beside the root page.
#[tokio::test]
async fn cx_f2e_a_root_move_races_a_change_of_each_new_key() {
    let _serial = star::serial().await;
    let b1 = Writer::Change("update public.b1 set w = 21 where id = 2");
    let b2 = Writer::Change("update public.b2 set w = 201 where id = 2");
    for star in N3.stars() {
        for (racer, third) in [(b1, b2), (b2, b1)] {
            for root_first in [true, false] {
                let mut d = star::start_seeded(&[star], SEED).await;
                let root = MOVE_BOTH.seal(&mut d).await;
                let joined = racer.seal(&mut d).await;
                let (first, second) = if root_first {
                    (root, joined)
                } else {
                    (joined, root)
                };
                star::race(&mut d, star, first, second).await;
                let after = third.seal(&mut d).await;
                d.drain(after, "third").await;
                star::assert_oracle(&mut d, &[star]).await;
            }
        }
    }
}

/// CX F3a: `a` row 1 moves `(1, 1) → (2, 2)` ‖ row 2 moves `(2, 2) → (1,
/// 1)`. Both pages lock all four keys in one statement sorted by `(position,
/// key)`, so the second queues on the first and neither deadlocks.
#[tokio::test]
async fn cx_f3a_crossing_root_moves_of_both_keys_queue_without_deadlock() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        SEED,
        &[],
        MOVE_BOTH,
        Writer::Change("update public.a set k1 = 1, k2 = 1 where id = 2"),
    )
    .await;
}

/// CX F3c: from crossed keys, row 1 on `(1, 2)` moves to `(2, 1)` ‖ row 2 on
/// `(2, 1)` moves to `(1, 2)`: each page's keys at one position are the
/// other's at the other position.
#[tokio::test]
async fn cx_f3c_crossing_root_moves_of_crossed_keys_queue_without_deadlock() {
    let _serial = star::serial().await;
    star::family_seeded(
        N3,
        CROSS_SEED,
        &[],
        Writer::Change("update public.a set k1 = 2, k2 = 1 where id = 1"),
        Writer::Change("update public.a set k1 = 1, k2 = 2 where id = 2"),
    )
    .await;
}

/// n = 4 (#1464 N4 F14a's root half): `a` row 1 moves all three keys
/// `(1, 1, 1) → (2, 2, 2)` ‖ a change of `b2` row 2, on its new key at the
/// middle position. Planted `star_root_skip_j_middle`.
#[tokio::test]
async fn a_root_moving_every_key_races_a_change_of_its_new_key_at_the_middle_position() {
    let _serial = star::serial().await;
    star::family_seeded(
        Shape {
            positions: 3,
            at: 3,
        },
        "insert into public.a (id, k1, k2, k3, x, y) values \
         (1, 1, 1, 1, 7, 'a1'), (2, 2, 2, 2, 8, 'a2'); \
         insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'); \
         insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'); \
         insert into public.b3 values (1, 1, 1000, 'f1'), (2, 2, 2000, 'f2')",
        &[],
        Writer::Change("update public.a set k1 = 2, k2 = 2, k3 = 2 where id = 1"),
        Writer::Change("update public.b2 set w = 201 where id = 2"),
    )
    .await;
}
