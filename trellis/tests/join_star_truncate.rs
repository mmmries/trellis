//! A `TRUNCATE` of any table of a star 1-1 `JOIN` (#1776, #1460 sub-step
//! 3.10; #1375's T families, #1464's PP-T, PX4 to PX7, CX F5a/F5e and F5d,
//! PS3 and XT1), and the lock order of a truncate's page against a rebuild
//! start of several definitions and against seal phase 1.
//!
//! A truncate's page takes the transform's lock row `FOR UPDATE`, so every
//! other page of the star, at any position, queues on it; it empties the
//! truncated table's ledger (`R`, or that position's `M_p`) and raises its
//! floor, and a `TRUNCATE a` deletes every target row. A `TRUNCATE` of a
//! joined table, at any position, starts a rebuild of the target, and a page
//! that truncates several joined tables takes the definition row once,
//! before the lock row, and starts one rebuild (ADR-0021, "Truncate").
//!
//! The star is #1464's fixture (`support/star.rs`): `a(id, k1, .., x, y)`
//! joined to `b1`, `b2`, ... The per-position pins (T, PP-T, PX) run over
//! [`SHAPES`], the two-table `JOIN` and position 2 of a three-table star,
//! under every kind tuple; the cross-position pins (CX, PS3, XT1) use
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
use star::{Kind, SHAPES, Shape, Star};
use star_cx::{N3, Orientation, SEED};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::build::{self, Step};
use trellis::staging::interleave::PausePoint;

/// Position 2 of three tables, for the pins that name `b2`.
const AT2: Shape = Shape {
    positions: 2,
    at: 2,
};

/// `a` rows `(id, k, x, y)` and rows `(id, k, w, tag)` of the table under
/// test; the idle positions are matched.
const A_ROWS: &str =
    "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three'), (4, null, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

/// How many floors `table` (`R` or an `M`, qualified, as [`star::r_table`]
/// and [`star::m_table`] name it) has: 1 once a truncate raised it.
async fn floors(d: &Driver, table: &str) -> String {
    let (schema, name) = table.split_once('.').expect("a qualified table");
    count(
        d,
        &format!(
            "select count(*) from ledger_truncate_floor \
             where target_table = '\"{schema}\".\"{name}\"'"
        ),
    )
    .await
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

/// Each `M`'s row count, by position.
async fn m_counts(d: &Driver, star: Star) -> Vec<String> {
    let mut counts = Vec::new();
    for p in star.shape.all() {
        let m = star::m_table(d, star, p).await;
        counts.push(count(d, &format!("select count(*) from {m}")).await);
    }
    counts
}

// ------------------------------------------------------------------ PP-T

/// PP-T (#1375 T1 to T7): a `TRUNCATE` of `a` or of the table under test
/// against a page of the other (T1, T2) and of its own (T3, T4), a
/// `TRUNCATE` and the inserts after it in one transaction against a page of
/// the other (T5, T6), and both truncated and refilled (T7). T8 is
/// [`pp_t8_a_stale_page_after_the_truncate_is_refused`].
const TRUNCATE_RACES: [(&str, &str, &str); 8] = [
    (
        "t1 truncate a vs b",
        "truncate public.a",
        "update {b} set w = 999 where id = 2",
    ),
    (
        // A page of `b` that holds a `NULL` key reads no `R` entry, so it
        // meets the truncate's page on the lock row alone.
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

/// PP-T T1 to T7: each pair is two batches, sealed in order, drained in both
/// orders, the first frozen at its commit: the second queues on the star's
/// lock row, which the truncate's page holds `FOR UPDATE` and every other
/// page `FOR SHARE`, and after the rebuilds the target is the live join.
/// Planted `join1_truncate_share_lock`, the second page runs beside a
/// `TRUNCATE a` (a joined table's truncate is ordered by its definition row
/// and its rebuild's start too, `cx_f5a_…`).
#[tokio::test]
async fn pp_t1_to_t7_a_truncate_races_a_page_of_either_table() {
    for shape in SHAPES {
        for (name, x, y) in TRUNCATE_RACES {
            for star in shape.stars() {
                for x_first in [true, false] {
                    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                    star::write(&d, &shape.sql(x)).await;
                    let sealed_x = d.seal().await;
                    star::write(&d, &shape.sql(y)).await;
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
}

/// PP-T T8: a page of the truncated table from before the truncate, drained
/// after the truncate's page (the barrier never lets it; the driver claims
/// it out of order on purpose), is refused by that table's floor: `R`, or
/// the `M` of the table under test. Planted `join1_truncate_no_floor`, it
/// applies.
#[tokio::test]
async fn pp_t8_a_stale_page_after_the_truncate_is_refused() {
    for shape in SHAPES {
        for star in shape.stars() {
            for table in ["public.a", "{b}"] {
                let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                let stale = if table == "public.a" {
                    "insert into {a_ins} values (9, 20, 9, 'nine'); \
                     update public.a set x = 99 where id = 1"
                } else {
                    "insert into {b} values (9, 30, 900, 'thirty'); \
                     update {b} set w = 99 where id = 1"
                };
                star::write(&d, &shape.sql(stale)).await;
                let earlier = d.seal().await;
                star::write(&d, &shape.sql(&format!("truncate {table}"))).await;
                let truncate = d.seal().await;
                d.drain(truncate, "truncate").await;
                d.drain(earlier, "stale").await;
                let ledger = if table == "public.a" {
                    star::r_table(&d, star).await
                } else {
                    star::m_table(&d, star, shape.at).await
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
}

/// The barrier: a `TRUNCATE` of the table under test, then a later batch
/// with an insert into `a` and a change of that table (a mixed page).
/// Nothing past the truncate's batch is claimable until it drains, so the
/// mixed page can't apply its insert first and have the rebuild the
/// truncate starts treat it as older than it is.
#[tokio::test]
async fn a_batch_after_a_truncate_waits_for_it() {
    for shape in SHAPES {
        let star = shape.stars()[0];
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &shape.sql("truncate {b}")).await;
        let truncate = d.seal().await;
        star::write(
            &d,
            &shape.sql(
                "insert into {a_ins} values (5, 20, 5, 'five'); \
                 insert into {b} values (4, 20, 1, 'again')",
            ),
        )
        .await;
        let mixed = d.seal().await;
        let claimable = || async {
            trellis::staging::apply::next_claimable_segments(&d.ctl, 1_000_000)
                .await
                .expect("next claimable segments")
        };
        assert_eq!(claimable().await, [truncate], "the mixed page waits");
        d.drain(truncate, "truncate").await;
        assert_eq!(claimable().await, [mixed]);
        d.drain(mixed, "mixed").await;
        star::assert_rebuilt(&mut d, &[star]).await;
        assert_eq!(
            count(&d, &shape.sql("select count(*) from {b}")).await,
            "(1)",
            "the insert survived"
        );
    }
}

// -------------------------------------------------------------------- PX

/// PX4: a `TRUNCATE a` empties `R` and the target in its page, raises `R`'s
/// floor and leaves every `M` and `J` as they were, and starts no rebuild.
/// A later change of the joined table fans out to no row, since `R` holds
/// none of the `a` rows that are gone, and later inserts into `a` apply.
/// Planted `join1_truncate_keeps_r`, `R` keeps its entries, and the change
/// writes their rows back.
#[tokio::test]
async fn px4_a_truncate_of_a_empties_r_and_the_target_and_later_changes_apply() {
    for shape in SHAPES {
        for star in shape.stars() {
            let name = star.name();
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let id = star::definition_id(&d, star).await;
            let j = format!("select j::text from {DEFAULT_SCHEMA}.join1_{id}_j j order by 1");
            let (j_before, m_before) = (d.rows(&j).await, m_counts(&d, star).await);
            star::step(&mut d, "truncate public.a").await;
            let r = star::r_table(&d, star).await;
            let target = format!("select count(*) from {}", star.target());
            assert_eq!(
                count(&d, &target).await,
                "(0)",
                "{name}: the target is empty"
            );
            assert_eq!(
                count(&d, &format!("select count(*) from {r}")).await,
                "(0)",
                "{name}: R is empty"
            );
            assert_eq!(m_counts(&d, star).await, m_before, "{name}: M is untouched");
            assert_eq!(d.rows(&j).await, j_before, "{name}: J keeps its rows");
            assert_eq!(floors(&d, &r).await, "(1)", "{name}: R's floor rose");
            assert_eq!(
                build_state(&d, star).await,
                ("live".to_string(), 0),
                "{name}: a TRUNCATE of a starts no rebuild"
            );
            star::step(&mut d, &shape.sql("update {b} set w = 999 where id = 1")).await;
            assert_eq!(
                count(&d, &target).await,
                "(0)",
                "{name}: a change of the joined table resurrected the rows of a rows that \
                 are gone"
            );
            star::step(
                &mut d,
                &shape.sql("insert into {a_ins} values (7, 10, 7, 'seven')"),
            )
            .await;
            star::assert_oracle(&mut d, &[star]).await;
        }
    }
}

/// PX5: after a `TRUNCATE` of the table under test and before the rebuild
/// reaches any row, an `a` change reads the `M` the truncate left there,
/// which is empty. Planted `join1_truncate_keeps_m`, the new row joins a row
/// that is gone.
#[tokio::test]
async fn px5_a_truncate_of_a_joined_table_empties_its_m_before_the_rebuild_runs() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::step(&mut d, &shape.sql("truncate {b}")).await;
            star::step(
                &mut d,
                &shape.sql("insert into {a_ins} values (5, 10, 5, 'five')"),
            )
            .await;
            assert_eq!(
                count(
                    &d,
                    &format!(
                        "select count(*) from {} where id = 5 and tag{} is not null",
                        star.target(),
                        shape.at
                    )
                )
                .await,
                "(0)",
                "{}: the new row joined a row that is gone",
                star.name()
            );
            star::assert_rebuilt(&mut d, &[star]).await;
        }
    }
}

/// PX6: a `TRUNCATE` of the table under test empties its `M` and raises its
/// floor in its page, leaves the other positions' `M`s as they were, and
/// starts a rebuild: the definition reads `backfilling` with the plan and
/// sweep jobs queued and the segment it starts in recorded. Its rows keep the side they
/// held until the rebuild reaches them; then a `LEFT` row's is `NULL` and
/// an `INNER` row is gone. Planted `join1_truncate_no_rebuild`, nothing
/// rewrites them.
#[tokio::test]
async fn px6_a_truncate_of_a_joined_table_rebuilds_the_target() {
    for shape in SHAPES {
        for star in shape.stars() {
            let name = star.name();
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            let id = star::definition_id(&d, star).await;
            let seg = format!("select build_seg from transform_definitions where id = {id}");
            let built: i64 = d.ctl.query_one(&seg, &[]).await.expect("build_seg").get(0);
            let mut m_expected = m_counts(&d, star).await;
            m_expected[shape.at - 1] = "(0)".to_string();
            star::step(&mut d, &shape.sql("truncate {b}")).await;
            assert_eq!(
                m_counts(&d, star).await,
                m_expected,
                "{name}: only the M under test is empty"
            );
            let m = star::m_table(&d, star, shape.at).await;
            assert_eq!(floors(&d, &m).await, "(1)", "{name}: its M's floor rose");
            assert_eq!(
                build_state(&d, star).await,
                ("backfilling".to_string(), 2),
                "{name}: a plan job and a sweep job are queued"
            );
            let rebuilt: i64 = d.ctl.query_one(&seg, &[]).await.expect("build_seg").get(0);
            assert!(rebuilt > built, "{name}: the rebuild recorded its segment");
            let tagged = format!(
                "select count(*) from {} where tag{} is not null",
                star.target(),
                shape.at
            );
            assert_eq!(
                count(&d, &tagged).await,
                "(2)",
                "{name}: the rows wait for the rebuild"
            );
            star::assert_rebuilt(&mut d, &[star]).await;
            assert_eq!(
                build_state(&d, star).await,
                ("live".to_string(), 0),
                "{name}: the rebuild finished"
            );
            let expected = match star.kind(shape.at) {
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
}

/// A row of the table under test inserted after its `TRUNCATE` in the same
/// transaction is in its `M` once the truncate's page has applied it, and
/// the rebuild keeps it.
#[tokio::test]
async fn a_truncate_of_a_joined_table_and_a_later_insert_in_one_transaction() {
    for shape in SHAPES {
        for star in shape.stars() {
            let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
            star::step(
                &mut d,
                &shape.sql(
                    "begin; truncate {b}; \
                     insert into {b} values (4, 10, 401, 'ten2'), (5, null, 1, 'none'); commit",
                ),
            )
            .await;
            let m = star::m_table(&d, star, shape.at).await;
            assert_eq!(
                count(
                    &d,
                    &format!("select count(*) from {m} where not __tombstone")
                )
                .await,
                "(2)",
                "{}: the later inserts are in M",
                star.name()
            );
            star::assert_rebuilt(&mut d, &[star]).await;
        }
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

/// PX7: a `TRUNCATE` page of the table under test takes the definition row
/// before the lock row, as a rebuild's start does ([`def_row_first`]).
#[tokio::test]
async fn px7_a_truncate_of_a_joined_table_and_a_rebuild_start_take_the_definition_row_first() {
    for shape in SHAPES {
        let star = shape.stars()[0];
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &shape.sql("truncate {b}")).await;
        let truncate = d.seal().await;
        def_row_first(&mut d, star, truncate).await;
    }
}

/// A `TRUNCATE` of the table under test, read by every star of the shape,
/// whose ids run against their target names (the first defined sorts last:
/// `jl` before `ji`, `jll` before `jii`), against a rebuild start of all of
/// them, which takes their definitions' rows in id order and then their
/// lock rows. The page takes every definition's row in id order before any
/// lock row, so it queues on the first's holding nothing of any pair.
/// Taking each definition's row as it reached that target, it would hold a
/// later definition's row and lock row while it queued, and the start's next
/// lock would close the cycle.
#[tokio::test]
async fn a_truncate_page_of_several_joins_takes_their_definition_rows_in_id_order() {
    for shape in SHAPES {
        let stars = shape.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        let mut ids = Vec::new();
        for star in &stars {
            ids.push(star::definition_id(&d, *star).await);
        }
        let names: Vec<String> = stars.iter().map(|s| s.name()).collect();
        assert!(
            ids.windows(2).all(|w| w[0] < w[1]) && names.windows(2).all(|w| w[0] > w[1]),
            "the first defined sorts last: {names:?} {ids:?}"
        );
        star::write(&d, &shape.sql("truncate {b}")).await;
        let truncate = d.seal().await;
        let mut start = d.user().await;
        let txn = start.transaction().await.expect("begin");
        let lock_definition = |id: i64| {
            let txn = &txn;
            async move {
                txn.execute(
                    "select 1 from transform_definitions where id = $1 for update",
                    &[&id],
                )
                .await
            }
        };
        lock_definition(ids[0])
            .await
            .expect("the first definition row");
        let page = d.drain_frozen(truncate, "truncate", &[]).await;
        let pid: i32 = txn
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        assert!(
            star::queued_or_finished(&d, pid, &page).await,
            "{shape:?}: the page didn't queue on the first definition row"
        );
        for id in &ids[1..] {
            lock_definition(*id)
                .await
                .expect("the later definition rows, with no deadlock");
        }
        for id in ids.iter().rev() {
            txn.execute(
                &format!("select 1 from {DEFAULT_SCHEMA}.join1_{id}_l for update"),
                &[],
            )
            .await
            .expect("the lock rows, with no deadlock");
        }
        txn.commit().await.expect("commit the start");
        page.finish().await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        star::assert_rebuilt(&mut d, &stars).await;
    }
}

/// A `TRUNCATE` page of the table under test against seal phase 1 sealing
/// the segment after the truncate's. Phase 1 updates the truncate's segment
/// row (its `seal_step2`) and then the segment pointer; the page marks its
/// segment row drained and stamps the rebuild's segment under a share lock
/// on the pointer. The page takes them in phase 1's order, its segment row
/// first, so it queues on the row holding no lock on the pointer, and phase
/// 1 goes on. Phase 1 is played by hand, once the page has claimed its
/// segment.
#[tokio::test]
async fn a_truncate_page_takes_its_segment_row_before_the_segment_pointer() {
    for shape in SHAPES {
        let star = shape.stars()[0];
        let target = star.target();
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write(&d, &shape.sql("truncate {b}")).await;
        let truncate = d.seal().await;
        let mut page = d
            .drain_frozen(
                truncate,
                "truncate",
                &[(PausePoint::BeforeJoin1Recompute, &target)],
            )
            .await;
        page.reached(PausePoint::BeforeJoin1Recompute).await;
        let mut seal = d.user().await;
        let txn = seal.transaction().await.expect("begin");
        txn.execute(
            &format!(
                "update {DEFAULT_SCHEMA}.segments set seal_step2 = seal_step2 where seg_seq = $1"
            ),
            &[&truncate],
        )
        .await
        .expect("the truncate's segment row");
        d.release(&mut page, PausePoint::BeforeJoin1Recompute).await;
        let pid: i32 = txn
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        assert!(
            star::queued_or_finished(&d, pid, &page).await,
            "{}: the page didn't queue on its segment row",
            star.name()
        );
        txn.execute(
            &format!("update {DEFAULT_SCHEMA}.segment_pointer set active_seq = active_seq"),
            &[],
        )
        .await
        .expect("the segment pointer, with no deadlock");
        txn.rollback().await.expect("roll phase 1 back");
        page.finish().await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        star::assert_rebuilt(&mut d, &[star]).await;
    }
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
                floors(&d, &m).await,
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
    after_a_truncate(Orientation::AsWritten, true).await;
}

/// CX F5d ("trunc b | e | a ins"), both orientations: the same race while
/// the truncate's rebuild is pending. Planted `star_root_skip_j_last`, the
/// orientation as written fails; `star_root_skip_j_first`, the swapped one.
#[tokio::test]
async fn cx_f5d_a_root_insert_races_a_change_of_its_key_while_the_truncates_rebuild_is_pending() {
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
