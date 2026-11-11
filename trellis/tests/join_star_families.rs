//! #1375's and #1371's 1-1 `JOIN` families, root writers (#1773, #1464's
//! PP pins, #1858): each body is written once against "the joined table at
//! `at`" (`support/star.rs`) and runs at every shape of [`SHAPES`], the
//! two-table `JOIN` and position 2 of a three-table star with position 1
//! present, matched and idle, under every kind tuple. The joined-table
//! writers' halves are `join_star_joined_families.rs`'s.
//!
//! - **M** (M1–M6): mixed pages, an `a` move and the change of its new key
//!   in one page, racing another mixed page or a page of either side.
//! - **N** (N1–N6): rows with a `NULL` key at the position, which take no
//!   `J` row there, ordered by their entry lock; and, where the star has an
//!   idle position, the variant `NULL` at every position, a row that takes
//!   no `J` row at all.
//! - **R** (R1–R19, root Re-derives): a Re-derive of `a` against a change
//!   of its row, of its key's joined row, a move onto its key, a stale
//!   Apply, and a move after its unlocked read.
//! - **F** (#1371 F1–F5, F7, F12, root writers): an `a` insert, move or
//!   delete against a change of the joined table on its key.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use star::{SHAPES, Shape, Star, Writer};
use trellis::staging::interleave::PausePoint;

const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

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

/// What a matched row's `total` holds at `shape`: its `x`, the joined `w`
/// at `at`, and the idle positions'.
fn joined_total(shape: Shape, x: i32, w: i32) -> Option<i32> {
    Some(x + w + shape.idle_total())
}

/// A `'static` copy of `sql`, for a [`Writer`]: a test's few bodies.
fn leak(sql: String) -> &'static str {
    Box::leak(sql.into_boxed_str())
}

// ----------------------------------------------------------------------- M

/// M: a mixed page with an `a` move `10 → 20` and the change of key 20 at
/// `at` applies, in one page or as two pages in either order.
#[tokio::test]
async fn m_a_mixed_page_applies_an_a_move_and_the_change_of_its_new_key() {
    for shape in SHAPES {
        let a_change = shape.sql("update public.a set {k} = 20 where id = 1");
        let b_change = shape.sql("update {b} set w = 250 where id = 2");
        let stars = shape.stars();
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
}

/// M1–M6: two crossing mixed pages queue on `J` at `at` without deadlock,
/// in either order (`join1_no_j_lock`, `join1_m_before_j`). In the first
/// pair one moves `a` row 1 from 10 to 20 and changes key 20, the other
/// moves row 2 from 20 to 10 and changes key 10. In the second the other
/// page moves row 3 from 30 onto 10 instead, so its own `a` write touches no
/// row the first page wrote, and only `J[10]` stops its fan-out from reading
/// row 1 on key 10 while the first page moves it off. Each page locks both
/// its keys in one sorted `J` statement, so the second queues on the first.
#[tokio::test]
async fn m_two_crossing_mixed_pages_queue_on_j_without_deadlock() {
    for shape in SHAPES {
        for other_a in [
            "update public.a set {k} = 10 where id = 2",
            "update public.a set {k} = 10 where id = 3",
        ] {
            star::family_seeded_at(
                shape,
                A_ROWS,
                B_ROWS,
                Writer::Change(leak(shape.sql(
                    "update public.a set {k} = 20 where id = 1; update {b} set w = 250 where id = 2",
                ))),
                Writer::Change(leak(format!(
                    "{}; {}",
                    shape.sql(other_a),
                    shape.sql("update {b} set w = 150 where id = 1")
                ))),
            )
            .await;
        }
    }
}

/// M: a mixed page racing a page of the joined table on the key its `a` row
/// moves to, and an `a` insert on the key it leaves.
#[tokio::test]
async fn m_a_mixed_page_races_pages_of_either_side() {
    for shape in SHAPES {
        let mixed = Writer::Change(leak(shape.sql(
            "update public.a set {k} = 20 where id = 1; update {b} set w = 111 where id = 1",
        )));
        for other in [
            "update {b} set w = 222 where id = 2",
            "insert into {a_ins} values (5, 10, 5, 'five')",
        ] {
            star::family_seeded_at(
                shape,
                A_ROWS,
                B_ROWS,
                mixed,
                Writer::Change(leak(shape.sql(other))),
            )
            .await;
        }
    }
}

// ----------------------------------------------------------------------- N

/// `sql` clearing `a` row `id`'s key at every idle position of `shape`, or
/// `None` at a shape with none.
fn null_at_the_idle_positions(shape: Shape, id: i32) -> Option<String> {
    let keys: Vec<String> = shape.idle().map(|q| format!("k{q} = null")).collect();
    (!keys.is_empty()).then(|| format!("update public.a set {} where id = {id}", keys.join(", ")))
}

/// N2–N6: two pages on `a` row 4, whose key at `at` is `NULL`, take no `J`
/// row there (only the idle positions', which they share): its entry lock
/// orders them (`join1_no_entry_lock`). `both_null` also clears its keys at
/// the idle positions, so it takes no `J` row at all; it runs only at the
/// shapes that have one.
async fn null_keyed_pages_queue_on_the_entry(both_null: bool) {
    for shape in SHAPES {
        let clear = null_at_the_idle_positions(shape, 4);
        if both_null && clear.is_none() {
            continue;
        }
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            if let (true, Some(clear)) = (both_null, &clear) {
                star::step(&mut d, clear).await;
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
}

#[tokio::test]
async fn n_two_pages_on_a_row_null_keyed_at_the_position_queue_on_its_entry() {
    null_keyed_pages_queue_on_the_entry(false).await;
}

#[tokio::test]
async fn n_two_pages_on_a_row_null_keyed_at_every_position_queue_on_its_entry() {
    null_keyed_pages_queue_on_the_entry(true).await;
}

/// N: a Re-derive of `a` row 4 (`NULL` at `at`), frozen after its read, and
/// an Apply of a change committed after that read: the Apply's page queues
/// on the entry and applies over the re-derived state
/// (`join1_no_entry_lock`). `both_null` as above. The joined table's half is
/// `join_star_joined_families.rs`'s.
async fn null_keyed_rederive_and_apply(both_null: bool) {
    for shape in SHAPES {
        let clear = null_at_the_idle_positions(shape, 4);
        if both_null && clear.is_none() {
            continue;
        }
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            if let (true, Some(clear)) = (both_null, &clear) {
                star::step(&mut d, clear).await;
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
}

#[tokio::test]
async fn n_a_rederive_and_an_apply_of_a_row_null_keyed_at_the_position_queue_on_its_entry() {
    null_keyed_rederive_and_apply(false).await;
}

#[tokio::test]
async fn n_a_rederive_and_an_apply_of_a_row_null_keyed_at_every_position_queue_on_its_entry() {
    null_keyed_rederive_and_apply(true).await;
}

/// N: Re-derives move `a` rows onto and off a `NULL` key at `at` (changes
/// capture never saw), one of them `NULL` at every position, and re-derive a
/// row that stays on one.
#[tokio::test]
async fn n_rederives_move_root_rows_onto_and_off_a_null_key() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        star::write_uncaptured(
            &d,
            "public.a",
            &shape.sql("update public.a set {k} = 10 where id = 4"),
        )
        .await;
        star::write_uncaptured(
            &d,
            "public.a",
            &shape.sql("update public.a set {k} = null where id = 2"),
        )
        .await;
        let every: Vec<String> = shape.all().map(|p| format!("k{p} = null")).collect();
        star::write_uncaptured(
            &d,
            "public.a",
            &format!("update public.a set {} where id = 3", every.join(", ")),
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
}

// ----------------------------------------------------------------------- R

/// Re-derives of `a` against a change of the joined table on its key, a
/// joined row moving onto its key, its own move, a joined Re-derive on its
/// key, and a page with a Re-derive and a change: each pair queues on a
/// shared `J` row at `at`, nothing deadlocks, and the target is the live
/// join whichever page commits first (`join1_snapshot_after_read`,
/// `join1_m_before_j`).
#[tokio::test]
async fn r_root_rederives_race_changes_of_either_side() {
    const A1: Writer = Writer::Rederive("public.a", &["1"]);
    for shape in SHAPES {
        for (first, second) in [
            (
                A1,
                Writer::Change(leak(shape.sql("update {b} set w = 102 where id = 1"))),
            ),
            (
                Writer::Rederive("public.a", &["3"]),
                Writer::Change(leak(shape.sql("update {b} set k = 30 where id = 3"))),
            ),
            (
                A1,
                Writer::Change(leak(shape.sql("update public.a set {k} = 20 where id = 1"))),
            ),
            (A1, Writer::Rederive(leak(shape.b()), &["1"])),
            (
                Writer::Both(
                    "public.a",
                    &["1"],
                    leak(shape.sql("update {b} set w = 9 where id = 2")),
                ),
                Writer::Change(leak(shape.sql("update {b} set w = 103 where id = 1"))),
            ),
        ] {
            star::family_seeded_at(shape, A_ROWS, B_ROWS, first, second).await;
        }
    }
}

/// R17, the #392 shape: a Re-derive read the row after two changes (the
/// newer change and the Re-derive request fold to one Re-derive), then the
/// older change's Apply arrives. The entry's basis saw its transaction, so
/// it is refused (`join1_lsn_only`): the target keeps the re-derived value.
#[tokio::test]
async fn r17_a_stale_root_apply_after_a_rederive_is_refused() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write(&d, "update public.a set x = 11 where id = 1").await;
            let older = d.seal().await;
            star::write(&d, "update public.a set x = 12 where id = 1").await;
            d.stage_recomputes("public.a", &["1"]).await;
            let rederive = d.seal().await;
            d.drain(rederive, "rederive").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 12, 100),
                "{star:?}"
            );
            d.drain(older, "older").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 12, 100),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// R12 (F12 on `a`): an Apply older than the entry's applied position is
/// refused even when the entry has no basis (`join1_vis_only`).
#[tokio::test]
async fn an_older_root_apply_after_a_newer_one_is_refused() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write(&d, "update public.a set x = 11 where id = 1").await;
            let older = d.seal().await;
            star::write(&d, "update public.a set x = 12 where id = 1").await;
            let newer = d.seal().await;
            d.drain(newer, "newer").await;
            d.drain(older, "older").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 12, 100),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// R19 (`rd_nokey`): `a` row 1 moves onto key 30 at `at` after a Re-derive
/// page's unlocked read of it, while a page inserting a joined row on 30
/// holds `J` for it uncommitted. The retry queues on it and reads the new
/// `M` entry (`join1_rd_no_key`, which writes the row on 30 without the
/// lock, from `M` as it was before the joined page).
#[tokio::test]
async fn r19_a_root_rederive_whose_row_moved_onto_a_held_key_retries() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let target = star.target();
            d.stage_recomputes("public.a", &["1"]).await;
            let rederive = d.seal().await;
            star::write(
                &d,
                &shape.sql("insert into {b} values (4, 30, 300, 'thirty')"),
            )
            .await;
            let insert = d.seal().await;
            let moves = d.join_key_moves();
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive a",
                    &[(PausePoint::BeforeJoin1Lock, &target)],
                )
                .await;
            page.reached(PausePoint::BeforeJoin1Lock).await;
            star::write(&d, &shape.sql("update public.a set {k} = 30 where id = 1")).await;
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
            assert_eq!(d.join_key_moves(), moves + 1, "{star:?}");
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 300),
                "{star:?}"
            );
        }
    }
}

/// A Re-derive whose row moved keys at `at` after the page's unlocked read
/// finds a key outside its locked set: it rolls back once and its retry
/// commits.
#[tokio::test]
async fn a_root_rederive_whose_row_moved_keys_after_the_unlocked_read_retries() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let target = star.target();
            d.stage_recomputes("public.a", &["1"]).await;
            let batch = d.seal().await;
            let moves = d.join_key_moves();
            let mut page = d
                .drain_frozen(batch, "rederive", &[(PausePoint::BeforeJoin1Lock, &target)])
                .await;
            page.reached(PausePoint::BeforeJoin1Lock).await;
            // Captured, into a later batch: the frozen page's read of `a`
            // would block a trigger toggle.
            star::write(&d, &shape.sql("update public.a set {k} = 20 where id = 1")).await;
            d.release(&mut page, PausePoint::BeforeJoin1Lock).await;
            page.finish().await;
            assert_eq!(d.join_key_moves(), moves + 1, "{star:?}");
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// A Re-derive repairs root rows changed (at the position, and at each idle
/// one) or deleted without capture. Row 1 moves off key 10, which no other
/// row of the page holds, so the page must lock the key its entry holds
/// (`join1_old_key_unlocked`).
#[tokio::test]
async fn root_rederives_repair_uncaptured_changes() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        star::write_uncaptured(
            &d,
            "public.a",
            &shape.sql("update public.a set x = 100, {k} = 20 where id = 1"),
        )
        .await;
        for q in shape.idle() {
            star::write_uncaptured(
                &d,
                "public.a",
                &format!("update public.a set k{q} = 1005 where id = 3"),
            )
            .await;
        }
        star::write_uncaptured(&d, "public.a", "delete from public.a where id = 2").await;
        d.stage_recomputes("public.a", &["1", "2", "3"]).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive").await;
        star::assert_oracle(&mut d, &stars).await;
    }
}

/// A Re-derive of a root row that has no `R` entry reads its live key before
/// the `J` lock, so it commits on its first attempt, and writes the row.
#[tokio::test]
async fn a_root_rederive_of_a_row_with_no_entry_commits_on_its_first_attempt() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write_uncaptured(
                &d,
                "public.a",
                &shape.sql("insert into {a_ins} values (8, 10, 8, 'eight')"),
            )
            .await;
            d.stage_recomputes("public.a", &["8"]).await;
            let batch = d.seal().await;
            let moves = d.join_key_moves();
            d.drain(batch, "rederive").await;
            assert_eq!(
                d.join_key_moves(),
                moves,
                "{star:?}: no retry for a row the entry doesn't hold yet"
            );
            assert_eq!(
                total(&d, star, 8).await,
                joined_total(shape, 8, 100),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

// ----------------------------------------------------------------------- F

/// One #1371 root-writer family at every shape: an `a` writer against a
/// writer of the joined table at `at`, either one frozen at its commit while
/// the other queues on `J`.
async fn root_family(a_sql: &str, b_sql: &str) {
    for shape in SHAPES {
        star::family(shape, A_ROWS, B_ROWS, a_sql, b_sql).await;
    }
}

/// F1: an `a` insert on a key no joined row has ‖ the insert of that key.
/// The page that runs second queues on `J[30]` and reads the other side's
/// copy only once it has the lock, so whichever commits first, the other
/// sees it (`join1_m_before_j`, `join1_no_j_lock`).
#[tokio::test]
async fn f1_an_a_page_on_a_key_reads_m_only_after_the_b_page_on_it_commits() {
    root_family(
        "insert into {a_ins} values (5, 30, 5, 'five')",
        "insert into {b} values (4, 30, 300, 'thirty')",
    )
    .await;
}

/// F2: an `a` insert ‖ the delete of its key.
#[tokio::test]
async fn f2_an_a_insert_races_the_b_delete_of_its_key() {
    root_family(
        "insert into {a_ins} values (5, 10, 5, 'five')",
        "delete from {b} where id = 1",
    )
    .await;
}

/// F3: an `a` key move `10 → 20` ‖ an update of 20.
#[tokio::test]
async fn f3_an_a_move_races_a_b_update_of_its_new_key() {
    root_family(
        "update public.a set {k} = 20 where id = 1",
        "update {b} set w = 222 where id = 2",
    )
    .await;
}

/// F4: an `a` key move `10 → 20` ‖ an update of 10, the key it leaves.
#[tokio::test]
async fn f4_an_a_move_races_a_b_update_of_the_key_it_leaves() {
    root_family(
        "update public.a set {k} = 20 where id = 1",
        "update {b} set w = 111 where id = 1",
    )
    .await;
}

/// F5: an `a` delete ‖ an update of its key.
#[tokio::test]
async fn f5_an_a_delete_races_a_b_update_of_its_key() {
    root_family(
        "delete from public.a where id = 1",
        "update {b} set w = 111 where id = 1",
    )
    .await;
}

/// F7: a unique-key hand-off at `at` (row 1 leaves key 10, row 2 takes it)
/// drained in the wrong order, with an `a` insert on the key in the take's
/// page: while only the take has applied, `M` holds two live entries on key
/// 10 and the recompute reads the one the newer Apply wrote; the leave then
/// recomputes the key again.
#[tokio::test]
async fn f7_a_hand_off_applied_out_of_order_with_a_root_insert() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write(&d, &shape.sql("update {b} set k = 60 where id = 1")).await;
            let leave = d.seal().await;
            star::write(
                &d,
                &shape.sql(
                    "update {b} set k = 10 where id = 2; \
                     insert into {a_ins} values (5, 10, 5, 'five')",
                ),
            )
            .await;
            let take = d.seal().await;
            d.drain(take, "take").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 200),
                "{star:?}"
            );
            assert_eq!(
                total(&d, star, 5).await,
                joined_total(shape, 5, 200),
                "{star:?}"
            );
            d.drain(leave, "leave").await;
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// R (`rd_latesnap`): a change that commits after a root Re-derive's read
/// statement is not in the row it wrote, so it must not be in the entry's
/// basis (`join1_snapshot_after_read`): its Apply, drained after the
/// Re-derive, applies.
#[tokio::test]
async fn a_root_change_between_a_rederives_read_and_its_snapshot_is_applied() {
    for shape in SHAPES {
        for star in shape.stars() {
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
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 500, 100),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// F15 on `a` (`join1_no_placeholder`): two pages on a root row no `R` entry
/// exists for yet, drained newer first: the newer page's placeholder is
/// uncommitted, so the older page queues on it rather than overwriting it.
#[tokio::test]
async fn two_root_pages_on_a_new_row_queue_on_the_placeholder() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let label = star::r_label(&d, star).await;
            star::write(
                &d,
                &shape.sql("insert into {a_ins} values (9, 10, 9, 'nine')"),
            )
            .await;
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
            assert_eq!(
                total(&d, star, 9).await,
                joined_total(shape, 99, 100),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
            assert!(
                blocked,
                "{}: the older page queued on the placeholder",
                star.name()
            );
        }
    }
}
