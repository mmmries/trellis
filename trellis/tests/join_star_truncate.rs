//! A `TRUNCATE` of any table of a star 1-1 `JOIN` (#1776, #1460 sub-step
//! 3.10; #1464's PP-T, PX4 to PX7, CX F5a/F5e and F5d, PS3 and XT1).
//!
//! A truncate's page takes the transform's lock row `FOR UPDATE`, so every
//! other page of the star, at any position, queues on it; it empties the
//! truncated table's ledger (`R`, or that position's `M_p`) and raises its
//! floor, and a `TRUNCATE a` deletes every target row. A `TRUNCATE` of a
//! joined table, at any position, starts a rebuild of the target, and a page
//! that truncates several joined tables takes the definition row once,
//! before the lock row, and starts one rebuild (ADR-0021, "Truncate").
//!
//! The star is #1464's fixture (`support/star.rs`): `a(id, k1, k2, x, y)`
//! joined to `b1` (position 1, the model's `b`) and `b2` (position 2, the
//! model's `e`). The per-position pins (PP-T, PX) run at position 2 of
//! n = 3 under every kind tuple; the cross-position pins (CX, PS3, XT1) use
//! #1459's n = 3 base state (`support/star_cx.rs`).
//!
//! The model (#1459) has no segment barrier, rebuild or floor (#1464 Q7), so
//! these pins are the only evidence for the truncate rules at n + 1 tables.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;
#[path = "support/star_cx.rs"]
mod star_cx;

use drain_driver::Driver;
use star::{Kind, Shape, Star};
use star_cx::{N3, Orientation, SEED};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::build::{self, Step};
use trellis::staging::interleave::PausePoint;

/// The position under test: the last of three tables.
const AT2: Shape = Shape {
    positions: 2,
    at: 2,
};

/// `a` rows `(id, k2, x, y)` and `b2` rows `(id, k, w, tag)`; position 1 is
/// matched and idle.
const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

/// The quoted, qualified `ledger_truncate_floor` key of `table` (`R` or an
/// `M`, as [`star::r_table`] and [`star::m_table`] name it).
fn floor_key(table: &str) -> String {
    let (schema, name) = table.split_once('.').expect("a qualified table");
    format!("\"{schema}\".\"{name}\"")
}

/// `star`'s status and how many of its build jobs are queued.
async fn build_state(d: &Driver, star: Star) -> (String, i64) {
    let row = d
        .ctl
        .query_one(
            "select d.status::text, \
                    (select count(*) from backfill_chunks c \
                     where c.definition_id = d.id and not c.done) \
             from transform_definitions d where d.target_table = $1",
            &[&star.target()],
        )
        .await
        .expect("read the definition's status");
    (row.get(0), row.get(1))
}

async fn count(d: &Driver, sql: &str) -> String {
    d.rows(sql).await.concat()
}

// ------------------------------------------------------------------ PP-T

/// PP-T (#1375 T1 to T7) at position 2: a `TRUNCATE` of `a` or of the table
/// at position 2 against a page of the other, alone or with the inserts
/// after it in one transaction, and both truncated and refilled. T8 is
/// [`pp_t8_a_stale_page_after_the_truncate_is_refused`].
const TRUNCATE_RACES: [(&str, &str, &str); 8] = [
    (
        "t1 truncate a vs b",
        "truncate public.a",
        "update {b} set w = 999 where id = 2",
    ),
    (
        "t1 truncate a vs a NULL-keyed b change",
        "truncate public.a",
        "insert into {b} values (4, null, 1, 'none')",
    ),
    (
        "t2 truncate b vs a",
        "truncate {b}",
        "insert into {a_ins} values (5, 20, 5, 'five')",
    ),
    (
        "t3 a vs truncate a",
        "update public.a set x = 99 where id = 1; \
         insert into {a_ins} values (5, 30, 5, 'five')",
        "truncate public.a",
    ),
    (
        "t4 b vs truncate b",
        "insert into {b} values (4, 30, 400, 'thirty')",
        "truncate {b}",
    ),
    (
        "t5 truncate a and insert vs b",
        "begin; truncate public.a; \
         insert into {a_ins} values (5, 10, 5, 'five'), (6, null, 6, 'six'); commit",
        "update {b} set w = 999 where id = 1",
    ),
    (
        "t6 truncate b and insert vs a",
        "begin; truncate {b}; \
         insert into {b} values (4, 10, 401, 'ten2'), (5, null, 1, 'none'); commit",
        "insert into {a_ins} values (5, 20, 5, 'five')",
    ),
    (
        "t7 both truncated and refilled vs b",
        "begin; truncate public.a; truncate {b}; \
         insert into {a_ins} values (5, 10, 5, 'five'); \
         insert into {b} values (4, 10, 400, 'ten3'); commit",
        "insert into {b} values (6, 50, 1, 'fifty')",
    ),
];

/// PP-T T1 to T7: each pair is two batches, drained in both orders, the
/// first frozen at its commit: the second queues on the star's lock row,
/// which the truncate's page holds `FOR UPDATE` and every other page `FOR
/// SHARE`, and after the rebuilds the target is the live join. Planted
/// `join1_truncate_share_lock`, the second page runs beside a `TRUNCATE a`
/// (a joined table's truncate is ordered by its definition row and its
/// rebuild's start too, `cx_f5a_…`).
#[tokio::test]
async fn pp_t1_to_t7_a_truncate_races_a_page_of_either_table() {
    let _serial = star::serial().await;
    for (name, x, y) in TRUNCATE_RACES {
        for star in AT2.stars() {
            for x_first in [true, false] {
                let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                star::write(&d, &AT2.sql(x)).await;
                let sealed_x = d.seal().await;
                star::write(&d, &AT2.sql(y)).await;
                let sealed_y = d.seal().await;
                let (first, second) = if x_first {
                    (sealed_x, sealed_y)
                } else {
                    (sealed_y, sealed_x)
                };
                star::race_rebuilt(&mut d, star, name, first, second).await;
            }
        }
    }
}

/// PP-T T8: a page of the truncated table from before the truncate, drained
/// after the truncate's page (the barrier never lets it; the driver claims
/// it out of order on purpose), is refused by that table's floor: `R`, or
/// `M` at position 2. Planted `join1_truncate_no_floor`, it applies.
#[tokio::test]
async fn pp_t8_a_stale_page_after_the_truncate_is_refused() {
    let _serial = star::serial().await;
    for star in AT2.stars() {
        for table in ["public.a", "{b}"] {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let stale = if table == "public.a" {
                "insert into {a_ins} values (9, 20, 9, 'nine'); \
                 update public.a set x = 99 where id = 1"
            } else {
                "insert into {b} values (9, 30, 900, 'thirty'); \
                 update {b} set w = 99 where id = 1"
            };
            star::write(&d, &AT2.sql(stale)).await;
            let earlier = d.seal().await;
            star::write(&d, &AT2.sql(&format!("truncate {table}"))).await;
            let truncate = d.seal().await;
            d.drain(truncate, "truncate").await;
            d.drain(earlier, "stale").await;
            let ledger = if table == "public.a" {
                star::r_table(&d, star).await
            } else {
                star::m_table(&d, star, 2).await
            };
            assert_eq!(
                count(
                    &d,
                    &format!("select count(*) from {ledger} where not __tombstone")
                )
                .await,
                "(0)",
                "{} {table}: the stale page wrote an entry",
                star.name()
            );
            if table == "public.a" {
                assert_eq!(
                    count(&d, &format!("select count(*) from {}", star.target())).await,
                    "(0)",
                    "{}: the stale page wrote a target row",
                    star.name()
                );
            }
            star::assert_rebuilt(&mut d, &[star]).await;
        }
    }
}

// -------------------------------------------------------------------- PX

/// PX4: after a `TRUNCATE a`, a change of the table at position 2 fans out
/// to no row: `R` holds none of the `a` rows that are gone. Planted
/// `join1_truncate_keeps_r`, it writes their rows back.
#[tokio::test]
async fn px4_a_truncate_of_a_empties_r_and_a_later_change_at_position_2_writes_nothing() {
    let _serial = star::serial().await;
    for star in AT2.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::step(&mut d, "truncate public.a").await;
        star::step(&mut d, &AT2.sql("update {b} set w = 999 where id = 1")).await;
        assert_eq!(
            count(&d, &format!("select count(*) from {}", star.target())).await,
            "(0)",
            "{}: a change at position 2 resurrected the rows of a rows that are gone",
            star.name()
        );
        star::assert_oracle(&mut d, &[star]).await;
    }
}

/// PX5: after a `TRUNCATE` of the table at position 2 and before the rebuild
/// reaches any row, an `a` change reads the `M` the truncate left there,
/// which is empty. Planted `join1_truncate_keeps_m`, the new row joins a row
/// that is gone.
#[tokio::test]
async fn px5_a_truncate_at_position_2_empties_its_m_before_the_rebuild_runs() {
    let _serial = star::serial().await;
    for star in AT2.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::step(&mut d, &AT2.sql("truncate {b}")).await;
        star::step(
            &mut d,
            &AT2.sql("insert into {a_ins} values (5, 10, 5, 'five')"),
        )
        .await;
        assert_eq!(
            count(
                &d,
                &format!(
                    "select count(*) from {} where id = 5 and tag2 is not null",
                    star.target()
                )
            )
            .await,
            "(0)",
            "{}: the new row joined a row of b2 that is gone",
            star.name()
        );
        star::assert_rebuilt(&mut d, &[star]).await;
    }
}

/// PX6: a `TRUNCATE` of the table at position 2 empties its `M` and raises
/// its floor in its page, leaves position 1's `M` as it was, and starts a
/// rebuild: the definition reads `backfilling` with the plan and sweep jobs
/// queued. Its rows keep the `b2` side they held until the rebuild reaches
/// them; then a `LEFT` row's is `NULL` and an `INNER` row is gone. Planted
/// `join1_truncate_no_rebuild`, nothing rewrites them.
#[tokio::test]
async fn px6_a_truncate_at_position_2_rebuilds_the_target() {
    let _serial = star::serial().await;
    for star in AT2.stars() {
        let name = star.name();
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        let m1 = star::m_table(&d, star, 1).await;
        let m2 = star::m_table(&d, star, 2).await;
        let m1_before = count(&d, &format!("select count(*) from {m1}")).await;
        star::step(&mut d, &AT2.sql("truncate {b}")).await;
        assert_eq!(
            count(&d, &format!("select count(*) from {m2}")).await,
            "(0)",
            "{name}: M at position 2 is empty"
        );
        assert_eq!(
            count(&d, &format!("select count(*) from {m1}")).await,
            m1_before,
            "{name}: M at position 1 is untouched"
        );
        assert_eq!(
            count(
                &d,
                &format!(
                    "select count(*) from ledger_truncate_floor where target_table = '{}'",
                    floor_key(&m2)
                )
            )
            .await,
            "(1)",
            "{name}: M's floor at position 2 rose"
        );
        assert_eq!(
            build_state(&d, star).await,
            ("backfilling".to_string(), 2),
            "{name}: a plan job and a sweep job are queued"
        );
        assert_eq!(
            count(
                &d,
                &format!(
                    "select count(*) from {} where tag2 is not null",
                    star.target()
                )
            )
            .await,
            "(2)",
            "{name}: the rows wait for the rebuild"
        );
        star::assert_rebuilt(&mut d, &[star]).await;
        assert_eq!(
            build_state(&d, star).await,
            ("live".to_string(), 0),
            "{name}: the rebuild finished"
        );
        let expected = match star.kind(2) {
            Kind::Left => "(4)",
            Kind::Inner => "(0)",
        };
        assert_eq!(
            count(&d, &format!("select count(*) from {}", star.target())).await,
            expected,
            "{name}"
        );
    }
}

/// A rebuild's start played by hand: its transaction holds `star`'s
/// definition row `FOR UPDATE` while the page of `batch` (a `TRUNCATE` of a
/// joined table) starts, then takes the lock row. The page queues on the
/// definition row holding nothing else, so the start never waits for it.
/// Planted `join1_truncate_def_after_lock`, the page holds the lock row
/// while it queues, and the start's lock closes the cycle.
async fn def_row_first(d: &mut Driver, star: Star, batch: i64) {
    let id = star::definition_id(d, star).await;
    let mut start = d.user().await;
    let txn = start.transaction().await.expect("begin");
    txn.execute(
        "select 1 from transform_definitions where id = $1 for update",
        &[&id],
    )
    .await
    .expect("the definition row");
    let page = d.drain_frozen(batch, "truncate", &[]).await;
    let pid: i32 = txn
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    assert!(
        star::queued_or_finished(d, pid, &page).await,
        "{}: the page didn't queue on the definition's row",
        star.name()
    );
    txn.execute(
        &format!("select 1 from {DEFAULT_SCHEMA}.join1_{id}_l for update"),
        &[],
    )
    .await
    .expect("the lock row, with no deadlock");
    txn.commit().await.expect("commit the start");
    page.finish().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    star::assert_rebuilt(d, &[star]).await;
}

/// PX7: a `TRUNCATE` page of the table at position 2 takes the definition
/// row before the lock row, as a rebuild's start does
/// ([`def_row_first`]).
#[tokio::test]
async fn px7_a_truncate_at_position_2_and_a_rebuild_start_take_the_definition_row_first() {
    let _serial = star::serial().await;
    let star = AT2.stars()[0];
    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
    star::write(&d, &AT2.sql("truncate {b}")).await;
    let truncate = d.seal().await;
    def_row_first(&mut d, star, truncate).await;
}

// -------------------------------------------------------------------- XT1

const TRUNCATE_BOTH: &str = "truncate public.b1, public.b2";

/// XT1: one page truncating both joined tables ‖ an `a` page, either first,
/// frozen at its commit while the other queues. The `a` page takes the
/// definition row `FOR KEY SHARE` before its lock row, and the truncate's
/// page `FOR UPDATE`, so the definition row orders the two before the lock
/// row does, and the rebuild's start takes the lock row `FOR UPDATE` before
/// the page commits: `join1_truncate_share_lock` alone doesn't fail this
/// pin. Only a `TRUNCATE a`, which starts no rebuild and takes no
/// definition row, rests on the page's own `FOR UPDATE` (PP-T's T1).
#[tokio::test]
async fn xt1_a_page_truncating_both_joined_tables_races_an_a_page() {
    let _serial = star::serial().await;
    for star in N3.stars() {
        for truncate_first in [true, false] {
            let mut d = star::start_seeded(&[star], SEED).await;
            star::write(&d, TRUNCATE_BOTH).await;
            let truncate = d.seal().await;
            star::write(
                &d,
                "insert into public.a (id, k1, k2, x, y) values (9, 1, 2, 5, 'a9'); \
                 update public.a set k2 = 3 where id = 1",
            )
            .await;
            let a = d.seal().await;
            let (first, second) = if truncate_first {
                (truncate, a)
            } else {
                (a, truncate)
            };
            star::race_rebuilt(&mut d, star, "xt1", first, second).await;
        }
    }
}

/// XT1: the page truncating both joined tables empties both `M`s and raises
/// both floors, and starts one rebuild: one plan job and one sweep job.
/// Planted `join1_truncate_keeps_m`, both `M`s keep their entries; planted
/// `join1_truncate_no_rebuild`, it starts none.
#[tokio::test]
async fn xt1_a_page_truncating_both_joined_tables_empties_both_ms_and_starts_one_rebuild() {
    let _serial = star::serial().await;
    for star in N3.stars() {
        let name = star.name();
        let mut d = star::start_seeded(&[star], SEED).await;
        star::step(&mut d, TRUNCATE_BOTH).await;
        for p in N3.all() {
            let m = star::m_table(&d, star, p).await;
            assert_eq!(
                count(&d, &format!("select count(*) from {m}")).await,
                "(0)",
                "{name}: M at position {p} is empty"
            );
            assert_eq!(
                count(
                    &d,
                    &format!(
                        "select count(*) from ledger_truncate_floor where target_table = '{}'",
                        floor_key(&m)
                    )
                )
                .await,
                "(1)",
                "{name}: M's floor at position {p} rose"
            );
        }
        assert_eq!(
            build_state(&d, star).await,
            ("backfilling".to_string(), 2),
            "{name}: one rebuild: a plan job and a sweep job"
        );
        star::assert_rebuilt(&mut d, &[star]).await;
    }
}

/// XT1: the page truncating both joined tables takes the definition row
/// once, before the lock row ([`def_row_first`]).
#[tokio::test]
async fn xt1_a_page_truncating_both_joined_tables_takes_the_definition_row_first() {
    let _serial = star::serial().await;
    let star = N3.stars()[0];
    let mut d = star::start_seeded(&[star], SEED).await;
    star::write(&d, TRUNCATE_BOTH).await;
    let truncate = d.seal().await;
    def_row_first(&mut d, star, truncate).await;
}

/// A `TRUNCATE` of the table at position 2, and one page truncating both
/// joined tables, queue behind a build chunk in flight, so a chunk never
/// writes rows from an `M` a truncate is emptying; the rebuild the page
/// starts rewrites the chunk's rows.
#[tokio::test]
async fn a_truncate_of_a_joined_table_queues_behind_a_build_chunk_in_flight() {
    let _serial = star::serial().await;
    for truncate in ["truncate public.b2", TRUNCATE_BOTH] {
        for star in AT2.stars() {
            let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
            assert_eq!(
                star::build_step(d.pool(), "planner", 100).await,
                Step::Planned
            );
            let target = star.target();
            let mut chunk = d
                .run_frozen(
                    &[(PausePoint::BeforeRebuildCommit, &target)],
                    |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
                )
                .await;
            let reached = chunk.reached(PausePoint::BeforeRebuildCommit).await;
            star::write(&d, truncate).await;
            let batch = d.seal().await;
            let page = d.drain_frozen(batch, "page", &[]).await;
            assert!(
                star::queued_or_finished(&d, reached.backend_pid, &page).await,
                "{} {truncate}: the page ran beside the chunk",
                star.name()
            );
            d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
            chunk.finish().await;
            page.finish().await;
            star::assert_rebuilt(&mut d, &[star]).await;
        }
    }
}

// --------------------------------------------------------------- CX, PS3

/// CX F5a / F5e (mirrors, run once): a `TRUNCATE` of `b1` ‖ a change of
/// `b2`, either first, frozen at its commit: the other position's writer
/// queues on the truncate's page. As in XT1's race, the definition row and
/// the rebuild's start order the two, so `join1_truncate_share_lock` alone
/// doesn't fail this pin, though #1464 names it.
#[tokio::test]
async fn cx_f5a_a_truncate_at_one_position_races_a_change_at_the_other() {
    let _serial = star::serial().await;
    for star in N3.stars() {
        for truncate_first in [true, false] {
            let mut d = star::start_seeded(&[star], SEED).await;
            star::write(&d, "truncate public.b1").await;
            let truncate = d.seal().await;
            star::write(
                &d,
                "update public.b2 set w = 101 where id = 1; \
                 insert into public.b2 values (5, 5, 500, 'e5')",
            )
            .await;
            let change = d.seal().await;
            let (first, second) = if truncate_first {
                (truncate, change)
            } else {
                (change, truncate)
            };
            star::race_rebuilt(&mut d, star, "f5a", first, second).await;
        }
    }
}

/// A root insert and a change of the other position's table on its key,
/// after a `TRUNCATE` of `b1`, in `orientation`: each page queues on that
/// key's `J` row at the other position, which a root page locks at every
/// position. With `rebuilt`, the truncate's rebuild runs before the race
/// (PS3, F5c); without, it is still pending (F5d).
async fn after_a_truncate(orientation: Orientation, rebuilt: bool) {
    let sql = |s: &str| orientation.sql(s);
    for star in N3.stars() {
        for root_first in [true, false] {
            let mut d = star::start_seeded(&[star], &sql(SEED)).await;
            star::step(&mut d, &sql("truncate public.b1")).await;
            if rebuilt {
                build::settle_builds_with(d.pool(), 3).await;
            }
            star::write(
                &d,
                &sql("insert into public.a (id, k1, k2, x, y) values (9, 1, 9, 5, 'a9')"),
            )
            .await;
            let root = d.seal().await;
            star::write(&d, &sql("insert into public.b2 values (9, 9, 900, 'e9')")).await;
            let joined = d.seal().await;
            let (first, second) = if root_first {
                (root, joined)
            } else {
                (joined, root)
            };
            star_cx::race_at(&mut d, star, first, second, PausePoint::BeforeCommit, false).await;
            star::assert_rebuilt(&mut d, &[star]).await;
        }
    }
}

/// PS3 (F5c, "e | a ins (after trunc b)"): after a `TRUNCATE` of `b1` and
/// its rebuild, a change of `b2` ‖ an `a` insert on its key. Planted
/// `star_root_skip_j_last`, the root page leaves `J` at position 2
/// unlocked and one of them misses the other.
#[tokio::test]
async fn ps3_a_root_insert_races_a_change_of_its_key_after_a_truncate_at_the_other_position() {
    let _serial = star::serial().await;
    after_a_truncate(Orientation::AsWritten, true).await;
}

/// CX F5d ("trunc b | e | a ins"), both orientations: the same race while
/// the truncate's rebuild is pending. Planted `star_root_skip_j_last`, the
/// orientation as written fails; `star_root_skip_j_first`, the swapped one.
#[tokio::test]
async fn cx_f5d_a_root_insert_races_a_change_of_its_key_while_the_truncates_rebuild_is_pending() {
    let _serial = star::serial().await;
    for orientation in Orientation::BOTH {
        after_a_truncate(orientation, false).await;
    }
}

// ------------------------------------------------- several tables, several stars

/// One page truncating every table of the star, with every kind tuple's
/// star defined over the same tables: each star's `R` and both `M`s are
/// emptied, its target is empty, and it starts one rebuild (a plan job and a
/// sweep job); refilled, every star matches the live join.
#[tokio::test]
async fn a_truncate_of_every_table_of_several_stars_empties_each_and_starts_one_rebuild_each() {
    let _serial = star::serial().await;
    let stars = N3.stars();
    let mut d = star::start_seeded(&stars, SEED).await;
    star::step(&mut d, "truncate public.a, public.b1, public.b2").await;
    for star in &stars {
        let name = star.name();
        let r = star::r_table(&d, *star).await;
        assert_eq!(
            count(&d, &format!("select count(*) from {r}")).await,
            "(0)",
            "{name} R"
        );
        for p in N3.all() {
            let m = star::m_table(&d, *star, p).await;
            assert_eq!(
                count(&d, &format!("select count(*) from {m}")).await,
                "(0)",
                "{name} M{p}"
            );
        }
        assert_eq!(
            count(&d, &format!("select count(*) from {}", star.target())).await,
            "(0)",
            "{name} target"
        );
        assert_eq!(
            build_state(&d, *star).await,
            ("backfilling".to_string(), 2),
            "{name}"
        );
    }
    star::step(
        &mut d,
        "insert into public.b1 values (1, 1, 10, 'b1'); \
        insert into public.a (id, k1, k2, x, y) values (1, 1, 1, 7, 'a1'), (2, 3, 1, 8, 'a2'); \
        insert into public.b2 values (1, 1, 100, 'e1')",
    )
    .await;
    star::assert_rebuilt(&mut d, &stars).await;
}

/// A `TRUNCATE` (and refill, in one transaction) of the table at position 2
/// that another transform reads as its `FROM`: that transform's target is
/// cleared and refilled, and the star rebuilds to the live join.
#[tokio::test]
async fn a_truncate_of_a_joined_table_another_transform_reads_settles_both() {
    let _serial = star::serial().await;
    let star = N3.stars()[0];
    let ddl = format!("{}; {SEED}", N3.ddl());
    let def = star.definition();
    let defs = [
        def.as_str(),
        "TRANSFORM b2copy FROM public.b2 SELECT w AS ww",
    ];
    let mut columns = N3.columns();
    columns.push((
        "w".to_string(),
        trellis::defs::ast::ValueType::Integer(trellis::integer::IntWidth::Int4),
    ));
    let columns: Vec<(&str, _)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let tables = N3.tables();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    let mut d = Driver::start(&ddl, &columns, &defs, &tables).await;
    assert_eq!(count(&d, "select count(*) from public.b2copy").await, "(4)");
    star::step(
        &mut d,
        "begin; truncate public.b2; insert into public.b2 values (7, 1, 700, 'e7'); commit",
    )
    .await;
    assert_eq!(count(&d, "select count(*) from public.b2copy").await, "(1)");
    star::assert_rebuilt(&mut d, &[star]).await;
    assert_eq!(count(&d, "select count(*) from public.b2copy").await, "(1)");
}
