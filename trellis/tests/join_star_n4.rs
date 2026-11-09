//! A star 1-1 `JOIN` of four tables (#1782, #1460 sub-step 3.16; #1464's n = 4
//! pins F14a, F15a and F13a): the root `a` joined to `b1`, `b2` and `b3`. Every
//! rule is written over positions, so these pins put the traffic where a
//! two-position star has none: a **middle** position, with a position on each
//! side of it.
//!
//! The star is `support/star.rs`'s fixture at three positions
//! (`a(id, k1, k2, k3, x, y)`; the model's `b`, `e` and `f`), run under every
//! kind tuple of #1459's strength-2 cover (`LLL`, `LII`, `ILI`, `IIL`). Every
//! scenario ends with the oracle: the target is the live multi-join, `R` is
//! `a` and each `M_p` is `b{p}`; every race asserts that nothing deadlocked.
//!
//! - **F14a**: a root row moving all three keys `(1, 1, 1) -> (2, 2, 2)`
//!   against writers on the new key at every position. The root page locks
//!   `J` at every position, including the middle one (`star_root_skip_j_*`);
//!   once it has applied, two pages of joined tables at different positions
//!   share no `J` row, only the `R` entry of the rows they fan out to
//!   (`star_own_j_only`, at every pair of positions).
//! - **F15a**: F11's late-key shape with the late `M` key at each position, the
//!   middle one among them (`star_recheck_skip_last` at the last), and the
//!   same late key on a root Re-derive and on a build chunk's insert-from-read
//!   at each position (`moved_key`'s check over every position).
//! - **F13a**: a 3-cycle of mixed pages, `b1`+`b2`, `b2`+`b3` and `b3`+`b1`,
//!   whose fan-outs reach rows 1 and 3 from different positions. One sorted
//!   `R` lock statement orders them; split `{1, 2}` then `{3}`
//!   (`star_r_split`), two of them close a cycle and deadlock.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;
#[path = "support/star_cx.rs"]
mod star_cx;

use star::{Shape, Writer};
use star_cx::leak;
use trellis::staging::build::{self, Step};
use trellis::staging::interleave::PausePoint;

/// The star of four tables, its last position under test.
const N4: Shape = Shape {
    positions: 3,
    at: 3,
};

/// The positions of the star, and the middle one.
const POSITIONS: [usize; 3] = [1, 2, 3];

/// #1459's base state at n = 4: `a` rows `(id, k1, k2, k3, x, y)`, `b1` rows
/// on keys 1 to 3, `b2` and `b3` rows on keys 1 to 4.
const SEED: &str = "insert into public.a (id, k1, k2, k3, x, y) values \
                    (1, 1, 1, 1, 7, 'a1'), (2, 2, 2, 2, 8, 'a2'), \
                    (3, 4, 4, 4, 9, 'a3'), (4, 1, 5, 3, 6, 'a4'); \
                    insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), (3, 3, 30, 'b3'); \
                    insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                    (3, 3, 300, 'e3'), (4, 4, 400, 'e4'); \
                    insert into public.b3 values (1, 1, 1000, 'f1'), (2, 2, 2000, 'f2'), \
                    (3, 3, 3000, 'f3'), (4, 4, 4000, 'f4')";

/// F13's cycle: `a` rows 1, 2 and 3 on `(1, 2, 3)`, `(2, 3, 1)` and
/// `(3, 1, 2)`, so each key is held by one row at each position.
const CYCLE: &str = "insert into public.a (id, k1, k2, k3, x, y) values \
                     (1, 1, 2, 3, 7, 'a1'), (2, 2, 3, 1, 8, 'a2'), (3, 3, 1, 2, 9, 'a3'); \
                     insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), (3, 3, 30, 'b3'); \
                     insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                     (3, 3, 300, 'e3'), (4, 4, 400, 'e4'); \
                     insert into public.b3 values (1, 1, 1000, 'f1'), (2, 2, 2000, 'f2'), \
                     (3, 3, 3000, 'f3'), (4, 4, 4000, 'f4')";

/// `a` row 1 moves all three keys.
const MOVE_ALL: Writer = Writer::Change("update public.a set k1 = 2, k2 = 2, k3 = 2 where id = 1");

/// A change of the joined table at `position`, row 2: on key 2, the new key
/// of [`MOVE_ALL`]'s row.
fn new_key_change(position: usize) -> Writer {
    Writer::Change(leak(format!(
        "update public.b{position} set w = {} where id = 2",
        position * 10_000 + 1
    )))
}

// -------------------------------------------------------------------- F14a

/// F14a, the root's half: `a` row 1 moves all three keys ‖ a change of the
/// joined row on its new key at one position, at each position in turn. The
/// root page locks `J` at all three positions, so a joined page at any of
/// them queues on it, whichever page is frozen first. Planted
/// `star_root_skip_j_first`, `star_root_skip_j_middle` or
/// `star_root_skip_j_last`, the joined page at that position runs beside the
/// root page and one of them recomputes the row from a stale `M`.
#[tokio::test]
async fn f14a_a_root_moving_every_key_races_a_change_of_its_new_key_at_each_position() {
    let _serial = star::serial().await;
    for position in POSITIONS {
        star::family_seeded(N4, SEED, &[], MOVE_ALL, new_key_change(position)).await;
    }
}

/// F14a, the root's half with the key it leaves: the move ‖ a change of the
/// joined row on the key `R` holds (key 1, row 1) at one position, which the
/// root page locks too. Planted `star_root_skip_j_*`.
#[tokio::test]
async fn f14a_a_root_moving_every_key_races_a_change_of_the_key_it_leaves_at_each_position() {
    let _serial = star::serial().await;
    for position in POSITIONS {
        let leaving = Writer::Change(leak(format!(
            "update public.b{position} set w = {} where id = 1",
            position * 10_000 + 2
        )));
        star::family_seeded(N4, SEED, &[], MOVE_ALL, leaving).await;
    }
}

/// F14a, the joined writers' half: the move has applied, so `a` rows 1 and 2
/// are both on key 2 at every position. Of the writers on that key at the
/// three positions, two race (every pair of positions, either frozen first) and
/// the third runs after. Two pages at different positions hold different `J`
/// rows; only the `R` entries of the rows they fan out to (`a` rows 1 and 2)
/// order them. Planted `star_own_j_only`, the second page reads the first's
/// `M` before it commits and the row loses one change.
#[tokio::test]
async fn f14a_joined_writers_on_the_new_key_at_two_positions_queue_on_the_rows_they_share() {
    let _serial = star::serial().await;
    for (x, y) in [(1, 2), (2, 3), (1, 3)] {
        let z = 6 - x - y;
        for star in N4.stars() {
            for flip in [false, true] {
                let mut d = star::start_seeded(&[star], SEED).await;
                let root = MOVE_ALL.seal(&mut d).await;
                d.drain(root, "the move").await;
                let one = new_key_change(x).seal(&mut d).await;
                let two = new_key_change(y).seal(&mut d).await;
                let (first, second) = if flip { (two, one) } else { (one, two) };
                star::race(&mut d, star, first, second).await;
                let third = new_key_change(z).seal(&mut d).await;
                d.drain(third, "the third").await;
                star::assert_oracle(&mut d, &[star]).await;
            }
        }
    }
}

// -------------------------------------------------------------------- F15a

/// F15a's state: [`SEED`] with `a` rows 7 and 8 on key 5 at `late`, and on
/// keys 1 and 3 (row 7's and row 8's) at the others, and the joined table at
/// `late` with a row 7 on key 71.
fn late_seed(late: usize) -> String {
    let keys = |other: i32| -> String {
        POSITIONS
            .iter()
            .map(|p| if *p == late { 5 } else { other }.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "{SEED}; insert into public.a (id, k1, k2, k3, x, y) values \
         (7, {}, 1, 'a7'), (8, {}, 2, 'a8'); \
         insert into public.b{late} values (7, 71, 7000, 'x7')",
        keys(1),
        keys(3)
    )
}

/// #1459's F11 shape at n = 4 with the late key at position `late`:
/// `b{late}` row 7 moves `71 -> 5` (its own page, `P1`), then `5 -> 9` (past
/// capture), then changes value at 9 in a mixed page (`P2`) with a change of
/// `a` row 7, which is on key 5 at `late`. `P2` reads its key set while `M`
/// still holds 71, so its fan-out keys at `late` are 71 and 9, and it locks
/// key 5 only for its root item. `P1` then commits, so row 7's entry holds
/// key 5 when `P2` locks it: in `J`, but not among its fan-out keys. `P2`
/// retries (F11), and its fan-out then locks the `R` entries on key 5, `a`
/// rows 7 and 8.
///
/// With `third`, a page changing the joined table at that position, row 3
/// (`a` row 8's key there), is frozen at its commit holding row 8's `R`
/// entry when `P2` resumes, and `P2`'s retry queues on it.
async fn late_m_key(late: usize, third: Option<usize>) {
    for star in N4.stars() {
        let mut d = star::start_seeded(&[star], &late_seed(late)).await;
        star::write(&d, &format!("update public.b{late} set k = 5 where id = 7")).await;
        let p1 = d.seal().await;
        // The move `5 -> 9`, past capture: `P2`'s image carries key 9.
        star::write_uncaptured(
            &d,
            &format!("public.b{late}"),
            &format!("update public.b{late} set k = 9 where id = 7"),
        )
        .await;
        star::write(
            &d,
            &format!(
                "update public.b{late} set w = 77 where id = 7; \
                 update public.a set x = 2 where id = 7"
            ),
        )
        .await;
        let p2 = d.seal().await;
        let target = star.target();
        let name = format!("{} (late key at position {late})", star.name());
        let mut mixed = d
            .drain_frozen(p2, "mixed", &[(PausePoint::BeforeJoin1Lock, &target)])
            .await;
        mixed.reached(PausePoint::BeforeJoin1Lock).await;
        d.drain(p1, "move").await;
        let other = third.map(|q| format!("update public.b{q} set w = 301 where id = 3"));
        let p3 = match &other {
            Some(sql) => {
                star::write(&d, sql).await;
                Some(d.seal().await)
            }
            None => None,
        };
        let moves = trellis::dev::interleave::join_key_moves();
        let (blocked, result) = match p3 {
            Some(p3) => {
                let mut held = d
                    .drain_frozen(p3, "third", &[(PausePoint::BeforeCommit, &target)])
                    .await;
                let reached = held.reached(PausePoint::BeforeCommit).await;
                d.release(&mut mixed, PausePoint::BeforeJoin1Lock).await;
                let blocked = star::queued_or_finished(&d, reached.backend_pid, &mixed).await;
                d.release(&mut held, PausePoint::BeforeCommit).await;
                held.finish().await;
                (blocked, mixed.finish_result().await)
            }
            None => {
                d.release(&mut mixed, PausePoint::BeforeJoin1Lock).await;
                (true, mixed.finish_result().await)
            }
        };
        if let Err(err) = &result {
            panic!("{name}: the mixed page failed: {err}");
        }
        let retried = trellis::dev::interleave::join_key_moves() - moves;
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{name}: the mixed page's retry ran without queueing on a8's R entry"
        );
        assert_eq!(
            retried, 1,
            "{name}: the mixed page's entry held a key outside its fan-out keys"
        );
    }
}

/// F15a: F11 with the third page, the late `M` key at the last position (the
/// model's `f`) and the third page at the first or the middle position
/// (`b`, `e`). Planted `star_recheck_j` or `star_recheck_skip_last`, `P2`
/// doesn't retry, and recomputes row 8 from the third page's `M` before it
/// commits.
#[tokio::test]
async fn f15a_a_late_m_key_at_the_last_position_retries_under_a_page_on_another_position() {
    let _serial = star::serial().await;
    for third in [1, 2] {
        late_m_key(3, Some(third)).await;
    }
}

/// F15a with the late `M` key at the middle position: the third page at the
/// first or the last. Planted `star_recheck_j`.
#[tokio::test]
async fn f15a_a_late_m_key_at_the_middle_position_retries_under_a_page_on_another_position() {
    let _serial = star::serial().await;
    for third in [1, 3] {
        late_m_key(2, Some(third)).await;
    }
}

/// F15a with the late `M` key at the first position: the third page at the
/// middle or the last. Planted `star_recheck_j`.
#[tokio::test]
async fn f15a_a_late_m_key_at_the_first_position_retries_under_a_page_on_another_position() {
    let _serial = star::serial().await;
    for third in [2, 3] {
        late_m_key(1, Some(third)).await;
    }
}

/// PS7 (F11b) at n = 4: the same with no third page, the late `M` key at
/// each position. Nothing is lost without the retry; the pin is the retry
/// itself.
#[tokio::test]
async fn a_joined_entry_whose_key_moved_onto_a_root_key_retries_at_each_position() {
    let _serial = star::serial().await;
    for late in POSITIONS {
        late_m_key(late, None).await;
    }
}

/// PS4 (F6a / F6b / F16) at n = 4: a Re-derive of `a` row 1 reads its keys
/// unlocked; its row then moves to key 2 at `position`, and a page changing
/// the joined row there on key 2 holds `J` for it, frozen at its commit. The
/// Re-derive's read finds the moved key outside its locked set, rolls back
/// and retries, and the retry queues behind the joined page and reads its
/// `M` entry once it commits.
async fn rederive_late_key_at(position: usize) {
    let late = format!("update public.a set k{position} = 2 where id = 1");
    for star in N4.stars() {
        let mut d = star::start_seeded(&[star], SEED).await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        let joined = new_key_change(position).seal(&mut d).await;
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

/// A root Re-derive whose row moved at the first position retries (F6a).
/// Planted `star_late_skip_first`.
#[tokio::test]
async fn a_root_rederive_whose_key_moved_at_the_first_position_retries_at_n4() {
    let _serial = star::serial().await;
    rederive_late_key_at(1).await;
}

/// The same at the middle position, the late-key check no named plant
/// reaches: a check that skips the middle position leaves its key unlocked.
#[tokio::test]
async fn a_root_rederive_whose_key_moved_at_the_middle_position_retries() {
    let _serial = star::serial().await;
    rederive_late_key_at(2).await;
}

/// The same at the last position (F6b). Planted `star_late_skip_last`.
#[tokio::test]
async fn a_root_rederive_whose_key_moved_at_the_last_position_retries_at_n4() {
    let _serial = star::serial().await;
    rederive_late_key_at(3).await;
}

/// `a` rows `(id, k, x, y)` on the position under test's keys 10 to 30, one
/// on `NULL`, and its table's rows `(id, k, w, tag)` on 10 to 30.
const A_ROWS: &str = "(1, 10, 1, 'a1'), (2, 20, 2, 'a2'), (3, 30, 3, 'a3'), (4, null, 4, 'a4'), \
                      (5, 10, 5, 'a5'), (6, 40, 6, 'a6')";
const B_ROWS: &str = "(1, 10, 100, 'b10'), (2, 20, 200, 'b20'), (3, 30, 300, 'b30')";

/// A build chunk's late key at `shape.at` (PP-C's C10 on the chunk's
/// insert-from-read, at n = 4): `a` row 1 moves from key 10 onto 70 there
/// after the chunk's unlocked read of its range, so its insert-from-read
/// finds key 70, which it holds no `J` row for. It rolls back and its retry
/// locks 70. A page inserting the row on 70 into the table at that position,
/// sealed before the chunk, drains while the chunk is frozen: a chunk that
/// skips the position fans out around the chunk's uncommitted entry, and the
/// chunk's row keeps no match.
async fn chunk_late_key(shape: Shape) {
    for star in shape.stars() {
        let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
        // No row on 70 at that position yet: the `M` pass reads none.
        assert_eq!(
            star::build_step(d.pool(), "planner", 100).await,
            Step::Planned
        );
        star::write(&d, &shape.sql("insert into {b} values (7, 70, 700, 'k7')")).await;
        let insert = d.seal().await;
        let target = star.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
            )
            .await;
        chunk.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, &shape.sql("update public.a set {k} = 70 where id = 1")).await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        // Frozen with its rows written (a chunk that skipped the position), or
        // rolled back.
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        d.drain(insert, "the insert on 70").await;
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "{} (position {}): the chunk rolled back on the moved key",
                star.name(),
                shape.at
            );
        }
        let moved = d.seal().await;
        d.drain(moved, "the move").await;
        build::settle_builds_with(d.pool(), 3).await;
        star::assert_oracle(&mut d, &[star]).await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        assert!(
            frozen.is_none(),
            "{} (position {}): the chunk went on past a key it hadn't locked",
            star.name(),
            shape.at
        );
    }
}

/// A build chunk racing a page, the late key at each position of the star,
/// the middle one among them: the chunk's check over its range's keys is the
/// root's (`moved_key`), at every position.
#[tokio::test]
async fn a_build_chunk_whose_row_moved_onto_a_key_it_did_not_lock_retries_at_each_position() {
    let _serial = star::serial().await;
    for at in POSITIONS {
        chunk_late_key(Shape::new(3, at)).await;
    }
}

// -------------------------------------------------------------------- F13a

/// F13a's pages: `b1`+`b2` (rows 1 and 3), `b2`+`b3` (rows 1 and 3) and
/// `b3`+`b1` (rows 1 and 3). Each fans out to `a` rows 1 and 3, from a
/// different pair of positions: the second page's row 1 is at position 2 and
/// its row 3 at position 3, the third's row 3 at position 1 and its row 1 at
/// position 3.
const CYCLE_PAGES: [&str; 3] = [
    "update public.b1 set w = 11 where id = 1; update public.b2 set w = 101 where id = 1",
    "update public.b2 set w = 201 where id = 2; update public.b3 set w = 2001 where id = 2",
    "update public.b3 set w = 3001 where id = 3; update public.b1 set w = 31 where id = 3",
];

/// F13a: two of the cycle's pages, either frozen first at its commit or
/// right after its first `R` lock statement while the other queues. The `R`
/// entries of a page are locked in one statement sorted by key, so the
/// second page queues on the first row it shares and never holds a row the
/// first needs. Planted `star_r_split` in its grouped form (positions 1 and
/// 2 in one statement, 3 in another), the second page locks row 3 and
/// queues on row 1 while the first's second statement needs row 3: a
/// deadlock.
#[tokio::test]
async fn f13a_a_cycle_of_mixed_pages_across_three_positions_queues_without_deadlock() {
    let _serial = star::serial().await;
    for (x, y) in [(0, 1), (1, 2), (0, 2)] {
        for star in N4.stars() {
            for flip in [false, true] {
                for point in [PausePoint::AfterJoin1RLock, PausePoint::BeforeCommit] {
                    let mut d = star::start_seeded(&[star], CYCLE).await;
                    star::write(&d, CYCLE_PAGES[x]).await;
                    let one = d.seal().await;
                    star::write(&d, CYCLE_PAGES[y]).await;
                    let two = d.seal().await;
                    let (first, second) = if flip { (two, one) } else { (one, two) };
                    star_cx::race_at(&mut d, star, first, second, point, true).await;
                }
            }
        }
    }
}
