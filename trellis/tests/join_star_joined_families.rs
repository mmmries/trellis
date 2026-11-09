//! #1375's and #1371's two-table families ported to a star 1-1 `JOIN`
//! (#1774, #1464's PP pins, joined-table writers): each body is the
//! two-table suite's (`join_one_to_one_interleavings.rs`), written once
//! against "the joined table at `at`" (`support/star.rs`), and run at
//! position 2 of a three-table star with position 1 present, matched and
//! idle, under every kind tuple. The root writers' halves are
//! `join_star_families.rs`'s.
//!
//! - **PP-R** (joined Re-derives): a Re-derive of the joined table against
//!   an `a` insert on its key, a change of its row, the row's move off the
//!   key, and a page with a root change; a Re-derive whose row moved onto a
//!   key a root page holds; a change between its read and its snapshot; a
//!   stale Apply after it.
//! - **PP-F** (#1371 F4, F7b, F12, F15, joined writers): a joined row moving
//!   off a key locks the key it leaves; the two pages of a unique-key
//!   hand-off race; an older Apply after a newer one; a new joined row's
//!   insert and update applied out of order.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use star::{Shape, Star, Writer};
use trellis::staging::interleave::PausePoint;

/// Position 2 of a three-table star; position 1 is idle.
const AT: Shape = Shape {
    positions: 2,
    at: 2,
};

const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

/// A `'static` copy of `sql` at `at`, for a [`Writer`].
fn at(sql: &str) -> &'static str {
    Box::leak(AT.sql(sql).into_boxed_str())
}

/// `star`'s target `total` for `a` row `id`, `None` for no row.
async fn total(d: &Driver, star: Star, id: i32) -> Option<i32> {
    d.user()
        .await
        .query_opt(
            &format!("select total from {} where id = $1", star.target()),
            &[&id],
        )
        .await
        .expect("read the target")
        .map(|row| row.get(0))
}

/// What a matched row's `total` holds: its `x`, the joined `w` at `at`, and
/// the idle positions'.
fn joined_total(x: i32, w: i32) -> Option<i32> {
    Some(x + w + AT.idle_total())
}

// --------------------------------------------------------------------- PP-R

/// Re-derives of the joined table at `at` against an `a` insert on its key,
/// an update of the same row, the row's move off the key, and a page with
/// the Re-derive and a root change against a change of the row: each pair
/// queues on a shared `J` row or `R` entry, nothing deadlocks, and the
/// target is the live join whichever page commits first
/// (`join1_snapshot_after_read`, `join1_m_before_j`).
#[tokio::test]
async fn r_joined_rederives_race_changes_of_either_side() {
    let _serial = star::serial().await;
    const B1: Writer = Writer::Rederive("public.b2", &["1"]);
    for (first, second) in [
        (
            B1,
            Writer::Change(at("insert into {a_ins} values (5, 10, 5, 'five')")),
        ),
        (
            B1,
            Writer::Change(at("update {b} set w = 101 where id = 1")),
        ),
        (B1, Writer::Change(at("update {b} set k = 50 where id = 1"))),
        (
            Writer::Both(
                "public.b2",
                &["1"],
                "update public.a set x = 9 where id = 1",
            ),
            Writer::Change(at("update {b} set w = 103 where id = 1")),
        ),
    ] {
        star::family_seeded_at(AT, A_ROWS, B_ROWS, first, second).await;
    }
}

/// R18 (`brd_nokey`): the joined row 1 moves onto key 30 after a Re-derive
/// page's unlocked read of it, while an `a` page inserting a row on 30
/// holds `J[30]` uncommitted. The Re-derive's read finds 30 outside its
/// fan-out keys, so it rolls back, and its retry queues on `J[30]`; its
/// fan-out then reaches the new `a` row. Planted `join1_brd_no_key`.
#[tokio::test]
async fn r18_a_joined_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = star::serial().await;
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let target = star.target();
        d.stage_recomputes(&AT.b(), &["1"]).await;
        let rederive = d.seal().await;
        star::write(&d, &AT.sql("insert into {a_ins} values (5, 30, 5, 'five')")).await;
        let insert = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::BeforeJoin1Lock, &target)],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        // Captured, left for the settle: the Re-derive's basis sees it.
        star::write(&d, &AT.sql("update {b} set k = 30 where id = 1")).await;
        let mut held = d
            .drain_frozen(insert, "a", &[(PausePoint::BeforeCommit, &target)])
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &page).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        let result = page.finish_result().await;
        let name = star.name();
        if let Err(err) = &result {
            panic!("{name}: the Re-derive page failed: {err}");
        }
        let retried = trellis::dev::interleave::join_key_moves() - moves;
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{name}: the Re-derive ran without queueing on J[30]"
        );
        assert_eq!(
            retried, 1,
            "{name}: the Re-derive's read found a key it hadn't locked"
        );
    }
}

/// R (`rd_latesnap` on `M`): a change of the joined row that commits after
/// its Re-derive's read statement is not in the row it wrote, so it must
/// not be in the entry's basis (`join1_snapshot_after_read`): its Apply,
/// drained after the Re-derive, applies and fans out.
#[tokio::test]
async fn a_joined_change_between_a_joined_rederives_read_and_its_snapshot_is_applied() {
    let _serial = star::serial().await;
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let label = star::m_label(&d, star, AT.at).await;
        d.stage_recomputes(&AT.b(), &["1"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::AfterRederiveRead, &label)],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        star::write(&d, &AT.sql("update {b} set w = 500 where id = 1")).await;
        let change = d.seal().await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        d.drain(change, "change").await;
        assert_eq!(total(&d, star, 1).await, joined_total(1, 500), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// R17 on `M`: a joined Re-derive read the row after two changes, then the
/// older change's Apply arrives: refused (`join1_lsn_only`), and the
/// fan-out keeps the re-derived `w`.
#[tokio::test]
async fn r17_a_stale_joined_apply_after_a_joined_rederive_is_refused() {
    let _serial = star::serial().await;
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &AT.sql("update {b} set w = 110 where id = 1")).await;
        let older = d.seal().await;
        star::write(&d, &AT.sql("update {b} set w = 120 where id = 1")).await;
        d.stage_recomputes(&AT.b(), &["1"]).await;
        let rederive = d.seal().await;
        d.drain(rederive, "rederive").await;
        assert_eq!(total(&d, star, 1).await, joined_total(1, 120), "{star:?}");
        d.drain(older, "older").await;
        assert_eq!(total(&d, star, 1).await, joined_total(1, 120), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

// --------------------------------------------------------------------- PP-F

/// F4 on the joined side: a joined row moving off key 10 locks the key it
/// leaves, which its `M` entry holds and its image's old key names
/// (`join1_old_key_unlocked`), so it queues behind an `a` page inserting a
/// row there, and its fan-out then finds that row.
#[tokio::test]
async fn f4_a_joined_row_moving_off_a_key_locks_the_key_it_leaves() {
    let _serial = star::serial().await;
    star::family(
        AT,
        A_ROWS,
        B_ROWS,
        "insert into {a_ins} values (5, 10, 5, 'five')",
        "update {b} set k = 50 where id = 1",
    )
    .await;
}

/// F7b: the two pages of a unique-key hand-off at `at` (row 1 leaves key
/// 10, row 2 takes it) racing each other. Both lock key 10, so the second
/// queues on it (`join1_no_j_lock`).
#[tokio::test]
async fn f7b_two_hand_off_pages_of_the_joined_table_race() {
    let _serial = star::serial().await;
    star::family_seeded_at(
        AT,
        A_ROWS,
        B_ROWS,
        Writer::Change(at("update {b} set k = 60 where id = 1")),
        Writer::Change(at("update {b} set k = 10 where id = 2")),
    )
    .await;
}

/// F12 on the joined side (`join1_vis_only`): an older Apply drained after a
/// newer one is refused by its `M` entry's applied position.
#[tokio::test]
async fn an_older_joined_apply_after_a_newer_one_is_refused() {
    let _serial = star::serial().await;
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &AT.sql("update {b} set w = 101 where id = 1")).await;
        let older = d.seal().await;
        star::write(&d, &AT.sql("update {b} set w = 102 where id = 1")).await;
        let newer = d.seal().await;
        d.drain(newer, "newer").await;
        d.drain(older, "older").await;
        assert_eq!(total(&d, star, 1).await, joined_total(1, 102), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// F15 on the joined side (`join1_no_placeholder`): a new joined row's
/// insert and update, drained newer first, the newer page frozen holding
/// its `M` placeholder: the insert's page queues on it rather than
/// overwriting it, and is then refused by I2.
#[tokio::test]
async fn f15_a_new_joined_rows_insert_and_update_applied_out_of_order() {
    let _serial = star::serial().await;
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let label = star::m_label(&d, star, AT.at).await;
        star::write(&d, &AT.sql("insert into {b} values (4, 30, 300, 'thirty')")).await;
        let older = d.seal().await;
        star::write(&d, &AT.sql("update {b} set w = 333 where id = 4")).await;
        let newer = d.seal().await;
        let mut held = d
            .drain_frozen(newer, "newer", &[(PausePoint::AfterPlaceholders, &label)])
            .await;
        let reached = held.reached(PausePoint::AfterPlaceholders).await;
        let queued = d.drain_frozen(older, "older", &[]).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &queued).await;
        d.release(&mut held, PausePoint::AfterPlaceholders).await;
        held.finish().await;
        queued.finish().await;
        assert_eq!(total(&d, star, 3).await, joined_total(3, 333), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{}: the older page queued on the placeholder",
            star.name()
        );
    }
}
