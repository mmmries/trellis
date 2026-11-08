//! #1375's and #1371's two-table families ported to a star 1-1 `JOIN`
//! (#1773, #1464's PP pins, root writers): each body is the two-table
//! suite's (`join_one_to_one_interleavings.rs`), written once against "the
//! joined table at `at`" (`support/star.rs`), and run at position 2 of a
//! three-table star with position 1 present, matched and idle, under every
//! kind tuple.
//!
//! - **PP-M** (M1–M6): mixed pages, an `a` move and the change of its new
//!   key in one page, racing another mixed page or a page of either side.
//! - **PP-N** (N1–N6): rows with a `NULL` key at the position, which take no
//!   `J` row there, ordered by their entry lock; and the both-`NULL` variant,
//!   a row that takes no `J` row at all.
//! - **PP-R** (R1–R19, root Re-derives): a Re-derive of `a` against a change
//!   of its row, of its key's joined row, a move onto its key, a stale
//!   Apply, and a move after its unlocked read.
//! - **PP-F** (#1371 F1–F5, F7, F12, root writers): an `a` insert, move or
//!   delete against a change of the joined table on its key.
//!
//! The two-table suites keep their own fixture; the same bodies run here.

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

fn knob() {
    trellis::dev::defs::set_star_join(true);
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

// --------------------------------------------------------------------- PP-M

/// M: a mixed page with an `a` move `10 → 20` and the change of key 20 at
/// `at` applies, in one page or as two pages in either order.
#[tokio::test]
async fn m_a_mixed_page_applies_an_a_move_and_the_change_of_its_new_key() {
    let _serial = star::serial().await;
    knob();
    let a_change = AT.sql("update public.a set {k} = 20 where id = 1");
    let b_change = AT.sql("update {b} set w = 250 where id = 2");
    let stars = AT.stars();
    let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
    star::write(&d, &a_change).await;
    star::write(&d, &b_change).await;
    let batch = d.seal().await;
    d.drain(batch, "mixed").await;
    star::assert_oracle(&mut d, &stars).await;
    for b_first in [true, false] {
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        star::write(&d, &a_change).await;
        let a = d.seal().await;
        star::write(&d, &b_change).await;
        let b = d.seal().await;
        for batch in if b_first { [b, a] } else { [a, b] } {
            d.drain(batch, "one").await;
        }
        star::assert_oracle(&mut d, &stars).await;
    }
}

/// M1–M6: two crossing mixed pages, each an `a` move and a change of the
/// key it moves to, queue on `J` at `at` without deadlock, in either order
/// (`join1_no_j_lock`, `join1_m_before_j`).
#[tokio::test]
async fn m_two_crossing_mixed_pages_queue_on_j_without_deadlock() {
    let _serial = star::serial().await;
    knob();
    for other_a in [
        "update public.a set {k} = 10 where id = 2",
        "update public.a set {k} = 10 where id = 3",
    ] {
        star::family_seeded_at(
            AT,
            A_ROWS,
            B_ROWS,
            Writer::Change(leak(AT.sql(
                "update public.a set {k} = 20 where id = 1; update {b} set w = 250 where id = 2",
            ))),
            Writer::Change(leak(format!(
                "{}; {}",
                AT.sql(other_a),
                AT.sql("update {b} set w = 150 where id = 1")
            ))),
        )
        .await;
    }
}

/// M: a mixed page racing a page of the joined table on the key its `a` row
/// moves to, and an `a` insert on the key it leaves.
#[tokio::test]
async fn m_a_mixed_page_races_pages_of_either_side() {
    let _serial = star::serial().await;
    knob();
    let mixed = Writer::Change(leak(AT.sql(
        "update public.a set {k} = 20 where id = 1; update {b} set w = 111 where id = 1",
    )));
    for other in [
        "update {b} set w = 222 where id = 2",
        "insert into {a_ins} values (5, 10, 5, 'five')",
    ] {
        star::family_seeded_at(
            AT,
            A_ROWS,
            B_ROWS,
            mixed,
            Writer::Change(leak(AT.sql(other))),
        )
        .await;
    }
}

/// A `'static` copy of `sql`, for a [`Writer`]: a test's few bodies.
fn leak(sql: String) -> &'static str {
    Box::leak(sql.into_boxed_str())
}

// --------------------------------------------------------------------- PP-N

/// N2–N6: two pages on `a` row 4, whose key at `at` is `NULL`, take no `J`
/// row there (its idle position's they share): its entry lock orders them
/// (`join1_no_entry_lock`). `both_null` also clears its key at position 1,
/// so it takes no `J` row at all.
async fn null_keyed_pages_queue_on_the_entry(both_null: bool) {
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        if both_null {
            star::step(&mut d, "update public.a set k1 = null where id = 4").await;
        }
        let label = star::r_label(&d, star).await;
        star::write(&d, "update public.a set x = 41 where id = 4").await;
        let first = d.seal().await;
        star::write(&d, "update public.a set x = 42 where id = 4").await;
        let second = d.seal().await;
        let mut held = d
            .drain_frozen(
                first,
                "first",
                &[(PausePoint::AfterJoin1REntryLock, &label)],
            )
            .await;
        let reached = held.reached(PausePoint::AfterJoin1REntryLock).await;
        let queued = d.drain_frozen(second, "second", &[]).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &queued).await;
        d.release(&mut held, PausePoint::AfterJoin1REntryLock).await;
        held.finish().await;
        queued.finish().await;
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{}: the second page queued on the entry",
            star.name()
        );
    }
}

#[tokio::test]
async fn n_two_pages_on_a_row_null_keyed_at_the_position_queue_on_its_entry() {
    let _serial = star::serial().await;
    knob();
    null_keyed_pages_queue_on_the_entry(false).await;
}

#[tokio::test]
async fn n_two_pages_on_a_row_null_keyed_at_every_position_queue_on_its_entry() {
    let _serial = star::serial().await;
    knob();
    null_keyed_pages_queue_on_the_entry(true).await;
}

/// N: a Re-derive of `a` row 4 (`NULL` at `at`), frozen after its read, and
/// an Apply of a change committed after that read: the Apply's page queues
/// on the entry and applies over the re-derived state
/// (`join1_no_entry_lock`). `both_null` as above.
async fn null_keyed_rederive_and_apply(both_null: bool) {
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        if both_null {
            star::step(&mut d, "update public.a set k1 = null where id = 4").await;
        }
        let label = star.target();
        d.stage_recomputes("public.a", &["4"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::AfterRederiveRead, &label)],
            )
            .await;
        let reached = page.reached(PausePoint::AfterRederiveRead).await;
        star::write(&d, "update public.a set x = 44 where id = 4").await;
        let batch = d.seal().await;
        let apply = d.drain_frozen(batch, "apply", &[]).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &apply).await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        apply.finish_result().await.expect("the Apply page");
        star::assert_oracle(&mut d, &[star]).await;
        assert!(blocked, "{}: the Apply queued on the entry", star.name());
    }
}

#[tokio::test]
async fn n_a_rederive_and_an_apply_of_a_row_null_keyed_at_the_position_queue_on_its_entry() {
    let _serial = star::serial().await;
    knob();
    null_keyed_rederive_and_apply(false).await;
}

#[tokio::test]
async fn n_a_rederive_and_an_apply_of_a_row_null_keyed_at_every_position_queue_on_its_entry() {
    let _serial = star::serial().await;
    knob();
    null_keyed_rederive_and_apply(true).await;
}

/// N: Re-derives move `a` rows onto and off a `NULL` key at `at` (changes
/// capture never saw), one of them `NULL` at both positions, and re-derive a
/// row that stays on one.
#[tokio::test]
async fn n_rederives_move_root_rows_onto_and_off_a_null_key() {
    let _serial = star::serial().await;
    knob();
    let stars = AT.stars();
    let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
    star::write_uncaptured(
        &d,
        "public.a",
        &AT.sql("update public.a set {k} = 10 where id = 4"),
    )
    .await;
    star::write_uncaptured(
        &d,
        "public.a",
        &AT.sql("update public.a set {k} = null where id = 2"),
    )
    .await;
    star::write_uncaptured(
        &d,
        "public.a",
        "update public.a set k1 = null, k2 = null where id = 3",
    )
    .await;
    d.stage_recomputes("public.a", &["2", "3", "4"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive").await;
    star::assert_oracle(&mut d, &stars).await;
    d.stage_recomputes("public.a", &["2", "3"]).await;
    let batch = d.seal().await;
    d.drain(batch, "again").await;
    star::assert_oracle(&mut d, &stars).await;
}

// --------------------------------------------------------------------- PP-R

/// Re-derives of `a` against a change of the joined table on its key, a
/// joined row moving onto its key, its own move, a joined Re-derive on its
/// key, and a page with a Re-derive and a change: each pair queues on a
/// shared `J` row at `at`, nothing deadlocks, and the target is the live
/// join whichever page commits first (`join1_snapshot_after_read`,
/// `join1_m_before_j`).
#[tokio::test]
async fn r_root_rederives_race_changes_of_either_side() {
    let _serial = star::serial().await;
    knob();
    const A1: Writer = Writer::Rederive("public.a", &["1"]);
    for (first, second) in [
        (
            A1,
            Writer::Change(leak(AT.sql("update {b} set w = 102 where id = 1"))),
        ),
        (
            Writer::Rederive("public.a", &["3"]),
            Writer::Change(leak(AT.sql("update {b} set k = 30 where id = 3"))),
        ),
        (
            A1,
            Writer::Change(leak(AT.sql("update public.a set {k} = 20 where id = 1"))),
        ),
        (A1, Writer::Rederive("public.b2", &["1"])),
        (
            Writer::Both(
                "public.a",
                &["1"],
                leak(AT.sql("update {b} set w = 9 where id = 2")),
            ),
            Writer::Change(leak(AT.sql("update {b} set w = 103 where id = 1"))),
        ),
    ] {
        star::family_seeded_at(AT, A_ROWS, B_ROWS, first, second).await;
    }
}

/// R17: a Re-derive read the row after two changes, then the older change's
/// Apply arrives: refused (`join1_lsn_only`).
#[tokio::test]
async fn r17_a_stale_root_apply_after_a_rederive_is_refused() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, "update public.a set x = 11 where id = 1").await;
        let older = d.seal().await;
        star::write(&d, "update public.a set x = 12 where id = 1").await;
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        d.drain(rederive, "rederive").await;
        assert_eq!(total(&d, star, 1).await, joined_total(12, 100));
        d.drain(older, "older").await;
        assert_eq!(total(&d, star, 1).await, joined_total(12, 100), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// R12 (F12 on `a`): an older Apply after a newer one is refused even with
/// no basis (`join1_vis_only`).
#[tokio::test]
async fn an_older_root_apply_after_a_newer_one_is_refused() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, "update public.a set x = 11 where id = 1").await;
        let older = d.seal().await;
        star::write(&d, "update public.a set x = 12 where id = 1").await;
        let newer = d.seal().await;
        d.drain(newer, "newer").await;
        d.drain(older, "older").await;
        assert_eq!(total(&d, star, 1).await, joined_total(12, 100), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// R19 (`rd_nokey`): `a` row 1 moves onto key 30 at `at` after a Re-derive
/// page's unlocked read of it, while a page inserting a joined row on 30
/// holds `J` for it uncommitted. The retry queues on it and reads the new
/// `M` entry (`join1_rd_no_key`).
#[tokio::test]
async fn r19_a_root_rederive_whose_row_moved_onto_a_held_key_retries() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let target = star.target();
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        star::write(&d, &AT.sql("insert into {b} values (4, 30, 300, 'thirty')")).await;
        let insert = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive a",
                &[(PausePoint::BeforeJoin1Lock, &target)],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, &AT.sql("update public.a set {k} = 30 where id = 1")).await;
        let mut held = d
            .drain_frozen(insert, "b", &[(PausePoint::BeforeCommit, &target)])
            .await;
        let reached = held.reached(PausePoint::BeforeCommit).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        let blocked = star::queued_or_finished(&d, reached.backend_pid, &page).await;
        d.release(&mut held, PausePoint::BeforeCommit).await;
        held.finish().await;
        page.finish_result().await.expect("the Re-derive page");
        star::assert_oracle(&mut d, &[star]).await;
        assert!(blocked, "{}: the retry queued on J", star.name());
        assert_eq!(trellis::dev::interleave::join_key_moves(), moves + 1);
        assert_eq!(total(&d, star, 1).await, joined_total(1, 300));
    }
}

/// A Re-derive whose row moved keys at `at` after the page's unlocked read
/// finds a key outside its locked set: it rolls back once and its retry
/// commits.
#[tokio::test]
async fn a_root_rederive_whose_row_moved_keys_after_the_unlocked_read_retries() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let target = star.target();
        d.stage_recomputes("public.a", &["1"]).await;
        let batch = d.seal().await;
        let moves = trellis::dev::interleave::join_key_moves();
        let mut page = d
            .drain_frozen(batch, "rederive", &[(PausePoint::BeforeJoin1Lock, &target)])
            .await;
        page.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, &AT.sql("update public.a set {k} = 20 where id = 1")).await;
        d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
        page.finish().await;
        assert_eq!(trellis::dev::interleave::join_key_moves(), moves + 1);
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// A Re-derive repairs root rows changed (at either position) or deleted
/// without capture, and writes one that has no entry yet on its first
/// attempt.
#[tokio::test]
async fn root_rederives_repair_uncaptured_changes() {
    let _serial = star::serial().await;
    knob();
    let stars = AT.stars();
    let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
    star::write_uncaptured(
        &d,
        "public.a",
        &AT.sql("update public.a set x = 100, {k} = 20 where id = 1"),
    )
    .await;
    star::write_uncaptured(&d, "public.a", "update public.a set k1 = 1005 where id = 3").await;
    star::write_uncaptured(&d, "public.a", "delete from public.a where id = 2").await;
    star::write_uncaptured(
        &d,
        "public.a",
        &AT.sql("insert into {a_ins} values (8, 10, 8, 'eight')"),
    )
    .await;
    d.stage_recomputes("public.a", &["1", "2", "3", "8"]).await;
    let batch = d.seal().await;
    let moves = trellis::dev::interleave::join_key_moves();
    d.drain(batch, "rederive").await;
    assert_eq!(trellis::dev::interleave::join_key_moves(), moves);
    star::assert_oracle(&mut d, &stars).await;
}

// --------------------------------------------------------------------- PP-F

/// #1371's root-writer families at `at`, each an `a` writer against a
/// writer of the joined table there, either one frozen at its commit while
/// the other queues on `J` (`join1_no_j_lock`, `join1_m_before_j`):
/// F1 an `a` insert ‖ the insert of its key; F2 ‖ the delete of it; F3 an
/// `a` move `10 → 20` ‖ an update of 20; F4 ‖ of 10, the key it leaves; F5
/// an `a` delete ‖ an update of its key.
#[tokio::test]
async fn f1_to_f5_a_root_writer_races_a_change_of_its_key() {
    let _serial = star::serial().await;
    knob();
    for (a_sql, b_sql) in [
        (
            "insert into {a_ins} values (5, 30, 5, 'five')",
            "insert into {b} values (4, 30, 300, 'thirty')",
        ),
        (
            "insert into {a_ins} values (5, 10, 5, 'five')",
            "delete from {b} where id = 1",
        ),
        (
            "update public.a set {k} = 20 where id = 1",
            "update {b} set w = 222 where id = 2",
        ),
        (
            "update public.a set {k} = 20 where id = 1",
            "update {b} set w = 111 where id = 1",
        ),
        (
            "delete from public.a where id = 1",
            "update {b} set w = 111 where id = 1",
        ),
    ] {
        star::family(AT, A_ROWS, B_ROWS, a_sql, b_sql).await;
    }
}

/// F7: a unique-key hand-off at `at` (row 1 leaves key 10, row 2 takes it)
/// drained in the wrong order, with an `a` insert on the key in the take's
/// page: while only the take has applied, the recompute reads the entry the
/// newer Apply wrote; the leave then recomputes the key again.
#[tokio::test]
async fn f7_a_hand_off_applied_out_of_order_with_a_root_insert() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &AT.sql("update {b} set k = 60 where id = 1")).await;
        let leave = d.seal().await;
        star::write(
            &d,
            &AT.sql(
                "update {b} set k = 10 where id = 2; insert into {a_ins} values (5, 10, 5, 'five')",
            ),
        )
        .await;
        let take = d.seal().await;
        d.drain(take, "take").await;
        assert_eq!(total(&d, star, 1).await, joined_total(1, 200), "{star:?}");
        assert_eq!(total(&d, star, 5).await, joined_total(5, 200), "{star:?}");
        d.drain(leave, "leave").await;
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// R (`rd_latesnap`): a change that commits after a root Re-derive's read
/// statement is not in the row it wrote, so it must not be in the entry's
/// basis (`join1_snapshot_after_read`): its Apply, drained after the
/// Re-derive, applies.
#[tokio::test]
async fn a_root_change_between_a_rederives_read_and_its_snapshot_is_applied() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let target = star.target();
        d.stage_recomputes("public.a", &["1"]).await;
        let rederive = d.seal().await;
        let mut page = d
            .drain_frozen(
                rederive,
                "rederive",
                &[(PausePoint::AfterRederiveRead, &target)],
            )
            .await;
        page.reached(PausePoint::AfterRederiveRead).await;
        star::write(&d, "update public.a set x = 500 where id = 1").await;
        let change = d.seal().await;
        d.release(&mut page, PausePoint::AfterRederiveRead).await;
        page.finish().await;
        d.drain(change, "change").await;
        assert_eq!(total(&d, star, 1).await, joined_total(500, 100), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// F15 on `a` (`join1_no_placeholder`): two pages on a root row no `R` entry
/// exists for yet, drained newer first: the newer page's placeholder is
/// uncommitted, so the older page queues on it rather than overwriting it.
#[tokio::test]
async fn two_root_pages_on_a_new_row_queue_on_the_placeholder() {
    let _serial = star::serial().await;
    knob();
    for star in AT.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let label = star::r_label(&d, star).await;
        star::write(&d, &AT.sql("insert into {a_ins} values (9, 10, 9, 'nine')")).await;
        let older = d.seal().await;
        star::write(&d, "update public.a set x = 99 where id = 9").await;
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
        assert_eq!(total(&d, star, 9).await, joined_total(99, 100), "{star:?}");
        star::assert_oracle(&mut d, &[star]).await;
        assert!(
            blocked,
            "{}: the older page queued on the placeholder",
            star.name()
        );
    }
}
