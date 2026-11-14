//! #1375's and #1371's 1-1 `JOIN` families, joined-table writers (#1774,
//! #1464's PP pins, #1858): each body is written once against "the joined
//! table at `at`" (`support/star.rs`) and runs at every shape of
//! [`SHAPES`], the two-table `JOIN` and position 2 of a three-table star
//! with position 1 present, matched and idle, under every kind tuple. The
//! root writers' halves are `join_star_families.rs`'s.
//!
//! - **R** (joined Re-derives): a Re-derive of the joined table against an
//!   `a` insert on its key, a change of its row, the row's move off the key,
//!   and a page with a root change; a Re-derive whose row moved onto a key a
//!   root page holds; a change between its read and its snapshot; a stale
//!   Apply after it.
//! - **N**: a joined row on a `NULL` key, which takes no `J` row, ordered by
//!   its entry lock; Re-derives moving rows of both tables onto and off a
//!   `NULL` key.
//! - **F** (#1371 F4, F7b, F12, F15, joined writers): a joined row moving
//!   off a key locks the key it leaves; the two pages of a unique-key
//!   hand-off race; an older Apply after a newer one; a new joined row's
//!   insert and update applied out of order.
//! - Joined pages otherwise: several changes to one row, two live `M`
//!   entries on a key, Re-derives repairing uncaptured changes, a Re-derive
//!   of a row with no entry, a page predating the build.

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

/// [`B_ROWS`] and a joined row on a `NULL` key.
const B_ROWS_NULL: &str =
    "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty'), (4, null, 4000, 'none')";

/// A `'static` copy of `sql` at `shape`, for a [`Writer`].
fn at(shape: Shape, sql: &str) -> &'static str {
    Box::leak(shape.sql(sql).into_boxed_str())
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

/// What a matched row's `total` holds at `shape`: its `x`, the joined `w`
/// at `at`, and the idle positions'.
fn joined_total(shape: Shape, x: i32, w: i32) -> Option<i32> {
    Some(x + w + shape.idle_total())
}

// ----------------------------------------------------------------------- R

/// Re-derives of the joined table at `at` against an `a` insert on its key,
/// an update of the same row, the row's move off the key, and a page with
/// the Re-derive and a root change against a change of the row: each pair
/// queues on a shared `J` row or `R` entry, nothing deadlocks, and the
/// target is the live join whichever page commits first
/// (`join1_snapshot_after_read`, `join1_m_before_j`).
#[tokio::test]
async fn r_joined_rederives_race_changes_of_either_side() {
    for shape in SHAPES {
        let b: &'static str = Box::leak(shape.b().into_boxed_str());
        let b1 = Writer::Rederive(b, &["1"]);
        for (first, second) in [
            (
                b1,
                Writer::Change(at(shape, "insert into {a_ins} values (5, 10, 5, 'five')")),
            ),
            (
                b1,
                Writer::Change(at(shape, "update {b} set w = 101 where id = 1")),
            ),
            (
                b1,
                Writer::Change(at(shape, "update {b} set k = 50 where id = 1")),
            ),
            (
                Writer::Both(b, &["1"], "update public.a set x = 9 where id = 1"),
                Writer::Change(at(shape, "update {b} set w = 103 where id = 1")),
            ),
        ] {
            star::family_seeded_at(shape, A_ROWS, B_ROWS, first, second).await;
        }
    }
}

/// R18 (`brd_nokey`): the joined row 1 moves onto key 30 after a Re-derive
/// page's unlocked read of it, while an `a` page inserting a row on 30
/// holds `J[30]` uncommitted. The Re-derive's read finds 30 outside its
/// fan-out keys, so it rolls back, and its retry queues on `J[30]`; its
/// fan-out then reaches the new `a` row. Planted `join1_brd_no_key` writes
/// the `M` entry and fans out on 30 without the lock, around the `a` row,
/// which keeps no match.
#[tokio::test]
async fn r18_a_joined_rederive_whose_row_moved_onto_a_held_key_retries() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let target = star.target();
            d.stage_recomputes(&shape.b(), &["1"]).await;
            let rederive = d.seal().await;
            star::write(
                &d,
                &shape.sql("insert into {a_ins} values (5, 30, 5, 'five')"),
            )
            .await;
            let insert = d.seal().await;
            let moves = d.join_key_moves();
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive",
                    &[(PausePoint::BeforeJoin1Lock, &target)],
                )
                .await;
            page.reached(PausePoint::BeforeJoin1Lock).await;
            // Captured, left for the settle: the Re-derive's basis sees it,
            // so its Apply is refused.
            star::write(&d, &shape.sql("update {b} set k = 30 where id = 1")).await;
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
            let retried = d.join_key_moves() - moves;
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
}

/// R (`rd_latesnap` on `M`): a change of the joined row that commits after
/// its Re-derive's read statement is not in the row it wrote, so it must
/// not be in the entry's basis (`join1_snapshot_after_read`): its Apply,
/// drained after the Re-derive, applies and fans out.
#[tokio::test]
async fn a_joined_change_between_a_joined_rederives_read_and_its_snapshot_is_applied() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let label = star::m_label(&d, star, shape.at).await;
            d.stage_recomputes(&shape.b(), &["1"]).await;
            let rederive = d.seal().await;
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive",
                    &[(PausePoint::AfterRederiveRead, &label)],
                )
                .await;
            page.reached(PausePoint::AfterRederiveRead).await;
            star::write(&d, &shape.sql("update {b} set w = 500 where id = 1")).await;
            let change = d.seal().await;
            d.release(&mut page, PausePoint::AfterRederiveRead).await;
            page.finish().await;
            d.drain(change, "change").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 500),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// R17 on `M`: a joined Re-derive read the row after two changes, then the
/// older change's Apply arrives: refused (`join1_lsn_only`), and the
/// fan-out keeps the re-derived `w`.
#[tokio::test]
async fn r17_a_stale_joined_apply_after_a_joined_rederive_is_refused() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write(&d, &shape.sql("update {b} set w = 110 where id = 1")).await;
            let older = d.seal().await;
            star::write(&d, &shape.sql("update {b} set w = 120 where id = 1")).await;
            d.stage_recomputes(&shape.b(), &["1"]).await;
            let rederive = d.seal().await;
            d.drain(rederive, "rederive").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 120),
                "{star:?}"
            );
            d.drain(older, "older").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 120),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

// ----------------------------------------------------------------------- N

/// N: a Re-derive of joined row 4, on a `NULL` key and so with no `J` row,
/// frozen after its read, and an Apply of a change of the row committed
/// after that read: only the row's `M` entry lock orders them
/// (`join1_no_entry_lock`), so the Apply's page queues on it and applies
/// over the re-derived state. Planted, the Apply runs first and the
/// Re-derive then writes the older row over it. The root's half is
/// `join_star_families.rs`'s.
#[tokio::test]
async fn n_a_joined_rederive_and_an_apply_of_a_row_null_keyed_queue_on_its_entry() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS_NULL).await;
            let label = star::m_label(&d, star, shape.at).await;
            d.stage_recomputes(&shape.b(), &["4"]).await;
            let rederive = d.seal().await;
            let mut page = d
                .drain_frozen(
                    rederive,
                    "rederive",
                    &[(PausePoint::AfterRederiveRead, &label)],
                )
                .await;
            let reached = page.reached(PausePoint::AfterRederiveRead).await;
            star::write(&d, &shape.sql("update {b} set w = 4400 where id = 4")).await;
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

/// N: one page of Re-derives moves rows of both tables onto and off a
/// `NULL` key at `at` (changes capture never saw), then re-derives a row of
/// each that stays on one.
#[tokio::test]
async fn n_rederives_move_rows_of_both_tables_onto_and_off_a_null_key() {
    for shape in SHAPES {
        let stars = shape.stars();
        let b = shape.b();
        let mut d = star::start(&stars, A_ROWS, B_ROWS_NULL).await;
        for (table, sql) in [
            ("public.a", "update public.a set {k} = 10 where id = 4"),
            ("public.a", "update public.a set {k} = null where id = 2"),
            (b.as_str(), "update {b} set k = 30 where id = 4"),
            (b.as_str(), "update {b} set k = null where id = 1"),
        ] {
            star::write_uncaptured(&d, table, &shape.sql(sql)).await;
        }
        d.stage_recomputes("public.a", &["2", "4"]).await;
        d.stage_recomputes(&b, &["1", "3", "4"]).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive").await;
        star::assert_oracle(&mut d, &stars).await;
        star::write_uncaptured(&d, &b, &shape.sql("update {b} set w = 1 where id = 1")).await;
        d.stage_recomputes("public.a", &["2"]).await;
        d.stage_recomputes(&b, &["1"]).await;
        let batch = d.seal().await;
        d.drain(batch, "again").await;
        star::assert_oracle(&mut d, &stars).await;
    }
}

// ----------------------------------------------------------------------- F

/// F4 on the joined side: a joined row moving off key 10 locks the key it
/// leaves, which its `M` entry holds and its image's old key names
/// (`join1_old_key_unlocked`), so it queues behind an `a` page inserting a
/// row there, and its fan-out then finds that row.
#[tokio::test]
async fn f4_a_joined_row_moving_off_a_key_locks_the_key_it_leaves() {
    for shape in SHAPES {
        star::family(
            shape,
            A_ROWS,
            B_ROWS,
            "insert into {a_ins} values (5, 10, 5, 'five')",
            "update {b} set k = 50 where id = 1",
        )
        .await;
    }
}

/// F7b: the two pages of a unique-key hand-off at `at` (row 1 leaves key
/// 10, row 2 takes it) racing each other. Both lock key 10, so the second
/// queues on it (`join1_no_j_lock`).
#[tokio::test]
async fn f7b_two_hand_off_pages_of_the_joined_table_race() {
    for shape in SHAPES {
        star::family_seeded_at(
            shape,
            A_ROWS,
            B_ROWS,
            Writer::Change(at(shape, "update {b} set k = 60 where id = 1")),
            Writer::Change(at(shape, "update {b} set k = 10 where id = 2")),
        )
        .await;
    }
}

/// F12 on the joined side (`join1_vis_only`): an older Apply drained after a
/// newer one is refused by its `M` entry's applied position.
#[tokio::test]
async fn an_older_joined_apply_after_a_newer_one_is_refused() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::write(&d, &shape.sql("update {b} set w = 101 where id = 1")).await;
            let older = d.seal().await;
            star::write(&d, &shape.sql("update {b} set w = 102 where id = 1")).await;
            let newer = d.seal().await;
            d.drain(newer, "newer").await;
            d.drain(older, "older").await;
            assert_eq!(
                total(&d, star, 1).await,
                joined_total(shape, 1, 102),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// F15 on the joined side (`join1_no_placeholder`): a new joined row's
/// insert and update, drained newer first, the newer page frozen holding
/// its `M` placeholder: the insert's page queues on it rather than
/// overwriting it, and is then refused by I2.
#[tokio::test]
async fn f15_a_new_joined_rows_insert_and_update_applied_out_of_order() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let label = star::m_label(&d, star, shape.at).await;
            star::write(
                &d,
                &shape.sql("insert into {b} values (4, 30, 300, 'thirty')"),
            )
            .await;
            let older = d.seal().await;
            star::write(&d, &shape.sql("update {b} set w = 333 where id = 4")).await;
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
                total(&d, star, 3).await,
                joined_total(shape, 3, 333),
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

// ------------------------------------------------- joined pages, other

/// A page with several changes to one joined row applies the newest, and
/// its fan-out covers the key the row left as well as the one it ends on.
#[tokio::test]
async fn a_page_with_several_changes_to_one_joined_row_applies_the_newest() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        for sql in [
            "update {b} set k = 30, w = 150 where id = 1",
            "update {b} set k = 50, w = 160 where id = 1",
            "delete from {b} where id = 2",
            "insert into {b} values (2, 10, 170, 'moved')",
        ] {
            star::write(&d, &shape.sql(sql)).await;
        }
        let batch = d.seal().await;
        d.drain(batch, "w").await;
        star::assert_oracle(&mut d, &stars).await;
    }
}

/// Two live `M` entries on one key (a hand-off between joined rows applied
/// out of order, #1397's): the recompute reads the one an Apply wrote over
/// one only a build or Re-derive wrote (no applied position), as the #1375
/// model orders them (`applied desc nulls last`). The second entry is
/// written by hand, as a joined page would leave it.
#[tokio::test]
async fn the_newest_m_entry_on_a_key_is_the_applied_one() {
    for shape in SHAPES {
        let star = shape.stars()[0];
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let m = star::m_table(&d, star, shape.at).await;
        // Joined row 1 holds key 10 from the build, with no applied position.
        d.ctl
            .batch_execute(&format!(
                "insert into {m} (__from_key, k, w, tag, __applied_lsn) \
                 values ('9', 10, 5000, 'handed', '0/1')"
            ))
            .await
            .expect("a second live M entry on key 10");
        star::step(&mut d, "update public.a set x = 2 where id = 1").await;
        assert_eq!(
            total(&d, star, 1).await,
            joined_total(shape, 2, 5000),
            "{}",
            star.name()
        );
    }
}

/// A Re-derive of joined rows repairs changes capture never saw: a `w`
/// update on a key with an `a` row (its fan-out), a move onto a key, a
/// delete, an insert onto a `NULL` key and one onto a key no `a` row has.
#[tokio::test]
async fn joined_rederives_repair_uncaptured_changes() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        let b = shape.b();
        for sql in [
            "update {b} set w = 150 where id = 1",
            "update {b} set k = 30, tag = 'thirty' where id = 3",
            "delete from {b} where id = 2",
            "insert into {b} values (4, null, 4000, 'none')",
            "insert into {b} values (5, 50, 500, 'fifty')",
        ] {
            star::write_uncaptured(&d, &b, &shape.sql(sql)).await;
        }
        d.stage_recomputes(&b, &["1", "2", "3", "4", "5"]).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive b").await;
        star::assert_oracle(&mut d, &stars).await;
    }
}

/// A Re-derive of a joined row with no `M` entry reads its live key before
/// the `J` lock, so it commits on its first attempt, and its fan-out reaches
/// the `a` row on the key.
#[tokio::test]
async fn a_joined_rederive_of_a_row_with_no_entry_commits_on_its_first_attempt() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let b = shape.b();
            star::write_uncaptured(
                &d,
                &b,
                &shape.sql("insert into {b} values (4, 30, 300, 'thirty')"),
            )
            .await;
            d.stage_recomputes(&b, &["4"]).await;
            let batch = d.seal().await;
            let moves = d.join_key_moves();
            d.drain(batch, "rederive b").await;
            assert_eq!(
                d.join_key_moves(),
                moves,
                "{star:?}: no retry for a row the entry doesn't hold yet"
            );
            assert_eq!(
                total(&d, star, 3).await,
                joined_total(shape, 3, 300),
                "{star:?}"
            );
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// A page of joined-table changes whose segment predates the build
/// re-derives its joined rows (the build-start rule), as a root page does:
/// an update, a key move and a delete. Nothing pauses.
#[tokio::test]
async fn a_joined_page_that_predates_the_build_rederives() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start_unbuilt(&stars, A_ROWS, B_ROWS).await;
        for sql in [
            "update {b} set w = 111 where id = 1",
            "update {b} set k = 30 where id = 3",
            "delete from {b} where id = 2",
        ] {
            star::write(&d, &shape.sql(sql)).await;
        }
        trellis::staging::build::settle_builds(d.pool()).await;
        star::assert_oracle(&mut d, &stars).await;
        let status = d
            .rows(
                "select d.status::text, \
                        (select f.error from capture_failures f where f.transform_id = d.id \
                         order by f.error limit 1) \
                 from transform_definitions d order by d.id",
            )
            .await;
        assert!(
            status.iter().all(|s| s.starts_with("(live,")),
            "{shape:?}: {status:?}"
        );
    }
}
