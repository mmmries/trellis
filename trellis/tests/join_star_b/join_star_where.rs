//! A 1-1 `JOIN` with a `WHERE` (#2041, #1991 rule 18, ADR-0021): `R` and
//! every `M` keep every row, and the predicate decides whether a row is in
//! the target each time it is recomputed, on the row joined to the newest
//! live `M` entry at every position, above the presence test.
//!
//! Each family races two writers of one row, either one frozen at its
//! commit while the other queues ([`star::race`]), under predicates that
//! the writer the race is about flips (#2040's coverage note: a predicate
//! that reads the changed column can mask a lost update, so each family also
//! runs one that reads neither writer's column). They run at the two-table
//! shape and at position 2 of a three-table star, under every kind tuple.
//! An anti-join at an `INNER` position is empty by construction and is
//! skipped. Every scenario ends with [`star::assert_star`]: the target is the
//! live multi-join with the `WHERE`, `R` is all of `a`, each `M_p` all of
//! `b{p}`, and the engine's oracle and `self_check` find nothing.

use crate::support::drain_driver;
use crate::support::star;

use drain_driver::Driver;
use star::{First, Kind, SHAPES, Shape, Star};
use trellis::config::Config;
use trellis::{Trellis, TrellisOptions};

const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 10, 5, 'two'), (3, 20, 3, 'three'), \
                      (4, null, 4, 'four'), (5, 30, 7, 'five')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty'), (3, 40, 400, 'forty')";

/// `b`-only: the joined table at the position under test decides.
const ON_B: &str = "{b}.w > 150";
/// `a`-only.
const ON_A: &str = "a.x > 3";
/// Both tables, inseparably.
const CROSS: &str = "{b}.w > a.x";
/// The anti-join: only rows with no match at the position under test.
const ANTI: &str = "{b}.id IS NULL";
/// Reads neither writer's changed column in the families below, so a lost
/// update can't hide behind a row the predicate removes.
const STABLE: &str = "a.y <> 'two'";

/// Whether `star` can hold a row under `filter`: an anti-join at an
/// `INNER` position can't.
fn admits(star: Star, filter: &str) -> bool {
    filter != ANTI || star.kind(star.shape.at) == Kind::Left
}

/// One family: `first_sql` and `second_sql` (each a [`Shape::sql`]
/// template), each in a batch of its own, under every filter in `filters`,
/// at every shape of [`SHAPES`] and kind tuple, each writer's page frozen at
/// its commit in turn.
async fn family(name: &str, first_sql: &str, second_sql: &str, filters: &[&str]) {
    for shape in SHAPES {
        let (one, two) = (shape.sql(first_sql), shape.sql(second_sql));
        for star in shape.stars() {
            for filter in filters {
                if !admits(star, filter) {
                    continue;
                }
                let star = star.filtered(filter);
                for first in [First::A, First::B] {
                    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                    star::assert_star(&d, star).await;
                    star::write(&d, &one).await;
                    let a = d.seal().await;
                    star::write(&d, &two).await;
                    let b = d.seal().await;
                    eprintln!("{name}: {} {:?} {first:?}", star.name(), star.filter);
                    match first {
                        First::A => star::race(&mut d, star, a, b).await,
                        First::B => star::race(&mut d, star, b, a).await,
                    }
                }
            }
        }
    }
}

/// An `a` change that moves no key but crosses the predicate, against a `b`
/// change of its key that crosses it too: each writer flips the row under
/// one predicate or another, at a `LEFT` position as at an `INNER` one.
/// Under `CROSS` row 1 starts in (100 > 1) and ends out (300 > 500 fails),
/// whichever writer drains last.
///
/// Pins the `join1_where_delete_presence_only` plant (under `CROSS`, row 1
/// stays).
#[tokio::test]
async fn an_a_change_of_no_key_races_a_b_change_of_its_row() {
    family(
        "no key",
        "update public.a set x = 500 where id = 1",
        "update {b} set w = 300 where k = 10",
        &[ON_B, ON_A, CROSS, ANTI, STABLE],
    )
    .await;
}

/// A `b` change that makes the row on its key stop qualifying, against an
/// `a` change of it that the predicate doesn't read: under `ON_B` the `b`
/// writer removes the row (and would mask the `a` writer's lost update),
/// under `STABLE` and `CROSS` it doesn't.
///
/// Pins the `join1_where_fan_out_no_delete` plant (`ON_B`, every position
/// `LEFT`: row 3 stays).
#[tokio::test]
async fn a_b_change_of_no_key_races_an_a_change_of_its_row() {
    family(
        "b crosses out",
        "update public.a set y = 'tres' where id = 3",
        "update {b} set w = 50 where k = 20",
        &[ON_B, STABLE, CROSS],
    )
    .await;
}

/// An `a` row moving onto a key against the insert of the `b` row on it: the
/// anti-join's row appears and leaves, the others' joins.
#[tokio::test]
async fn an_a_move_onto_a_key_races_the_insert_of_its_b_row() {
    family(
        "move onto",
        "update public.a set {k} = 50 where id = 1",
        "insert into {b} values (4, 50, 500, 'fifty')",
        &[ANTI, ON_B, ON_A],
    )
    .await;
}

/// An `a` insert on a key against the delete of the key's `b` row: the
/// anti-join's row appears once the delete is applied, whichever drains
/// last.
#[tokio::test]
async fn an_a_insert_races_the_delete_of_its_b_row() {
    family(
        "insert vs delete",
        "insert into {a_ins} values (6, 20, 6, 'six')",
        "delete from {b} where k = 20",
        &[ANTI, ON_B, ON_A],
    )
    .await;
}

/// Two `a` changes of a row whose key at the position under test is `NULL`
/// (at the two-table shape, no `J` row at all), each crossing the predicate:
/// they queue on the row's `R` entry. The existing `NULL`-key pins guard
/// `join1_no_entry_lock`; this one keeps that guard under a `WHERE`.
#[tokio::test]
async fn two_a_changes_of_a_null_keyed_row_cross_the_predicate() {
    family(
        "null key",
        "update public.a set x = 9 where id = 4",
        "update public.a set x = 2 where id = 4",
        &[ON_A, STABLE, ANTI],
    )
    .await;
}

/// A key handed from one `b` row to another, the two pages in either order:
/// applied out of order, `M` holds two live entries on the key for a moment,
/// whose predicate values differ (`w` 100 and 900). The delete and the upsert
/// both read the newest, so the row follows it.
#[tokio::test]
async fn a_hand_off_of_a_key_between_b_rows_whose_predicate_values_differ() {
    family(
        "hand-off",
        "update {b} set k = 60 where id = 1",
        "insert into {b} values (5, 10, 900, 'handed')",
        &[ON_B, CROSS, ANTI],
    )
    .await;
}

/// A fan-out left to chunks flips the rows on its key out, then back in,
/// chunk by chunk: every kind runs the delete, `LEFT`-only included (#1991
/// rule 18), and each chunk deletes the rows it reaches.
///
/// Pins the `join1_where_fan_out_no_delete` plant (every position `LEFT`:
/// the rows stay).
#[tokio::test]
async fn a_left_only_fan_out_deletes_the_rows_that_stop_qualifying() {
    let a_rows = "(1, 10, 1, 'one'), (2, 10, 5, 'two'), (3, 10, 3, 'three'), \
                  (4, 10, 4, 'four'), (5, 20, 7, 'five')";
    for shape in SHAPES {
        for star in shape.stars() {
            let star = star.filtered(ON_B);
            let mut d =
                star::start(&[star], a_rows, "(1, 10, 200, 'ten'), (2, 20, 200, 'x')").await;
            star::assert_star(&d, star).await;
            d.set_fan_out_chunk_rows(1);
            let name = star.name();
            assert_eq!(qualifying(&d, star).await, ["(4)"], "{name}");
            star::step(&mut d, &shape.sql("update {b} set w = 50 where k = 10")).await;
            // The key's four rows are left to chunks of one row each.
            assert_eq!(qualifying(&d, star).await, ["(4)"], "{name}");
            trellis::staging::one_to_one_join_fan_out_step(d.pool())
                .await
                .expect("a chunk");
            assert_eq!(qualifying(&d, star).await, ["(3)"], "{name}: one chunk");
            star::assert_oracle(&mut d, &[star]).await;
            assert_eq!(qualifying(&d, star).await, ["(0)"], "{name}");
            star::step(&mut d, &shape.sql("update {b} set w = 900 where k = 10")).await;
            star::assert_oracle(&mut d, &[star]).await;
            assert_eq!(qualifying(&d, star).await, ["(4)"], "{name}");
        }
    }
}

/// How many of rows 1 to 4 (key 10) are in `star`'s target.
async fn qualifying(d: &Driver, star: Star) -> Vec<String> {
    d.rows(&format!(
        "select count(*) from {} where id <= 4",
        star.target()
    ))
    .await
}

/// A `TRUNCATE` of the joined table empties `M` and rebuilds: the anti-join
/// gains every row once the build reaches it, a `b`-only predicate loses
/// them. Until then the target stays as it was, membership included.
#[tokio::test]
async fn a_truncate_of_b_rebuilds_membership() {
    for shape in SHAPES {
        for star in shape.stars() {
            for filter in [ANTI, ON_B, CROSS] {
                if !admits(star, filter) {
                    continue;
                }
                let star = star.filtered(filter);
                let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                star::step(&mut d, &shape.sql("truncate {b}")).await;
                star::assert_rebuilt(&mut d, &[star]).await;
                star::step(
                    &mut d,
                    &shape.sql("insert into {b} values (9, 10, 900, 'back')"),
                )
                .await;
                star::assert_oracle(&mut d, &[star]).await;
            }
        }
    }
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// Changes made while the transform is paused, some making rows stop
/// qualifying and some making rows qualify: `RESUME` keeps the target, and
/// the build that follows deletes the rows that stopped qualifying (they
/// have a live `R` entry, so their chunk, not the orphan pass, removes them)
/// and writes the rest.
#[tokio::test]
async fn a_resume_rebuilds_the_rows_whose_membership_changed_while_paused() {
    for shape in SHAPES {
        for star in shape.stars() {
            for filter in [ON_B, ON_A, CROSS, ANTI] {
                if !admits(star, filter) {
                    continue;
                }
                let star = star.filtered(filter);
                let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
                let name = star.name();
                let t = trellis(&d).await;
                t.apply(&format!("PAUSE TRANSFORM {name}"))
                    .await
                    .expect("pause");
                for sql in [
                    "update public.a set x = 500 where id = 1",
                    "update public.a set x = 1 where id = 5",
                    "update {b} set w = 50 where k = 20",
                    "update {b} set w = 900 where k = 10",
                    "delete from {b} where k = 40",
                    "insert into {a_ins} values (6, 40, 8, 'six')",
                ] {
                    star::write(&d, &shape.sql(sql)).await;
                }
                d.settle().await;
                t.apply(&format!("RESUME TRANSFORM {name}"))
                    .await
                    .expect("resume");
                trellis::intake::harness::start_registered_builds(d.pool())
                    .await
                    .expect("start the resumed build");
                star::assert_rebuilt(&mut d, &[star]).await;
                t.shutdown().await.expect("shut down");
            }
        }
    }
}

/// `self_check` compares the target with the `WHERE` applied: a target row
/// the predicate excludes is an extra row, and a qualifying row missing from
/// the target is a missing one. `R` and `M` are compared unfiltered, so they
/// report nothing.
#[tokio::test]
async fn self_check_reports_a_row_the_predicate_excludes_and_one_it_admits() {
    for shape in SHAPES {
        let star = shape.stars()[0].filtered(ON_A);
        let d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::assert_star(&d, star).await;
        let target = star.target();
        // Row 1 (x = 1) is excluded; copy row 2's values onto its key.
        d.ctl
            .batch_execute(&format!(
                "insert into {target} (id, total, {tags}, y) \
                 select 1, total, {tags}, y from {target} where id = 2; \
                 delete from {target} where id = 5",
                tags = shape
                    .all()
                    .map(|p| format!("tag{p}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
            .await
            .expect("tamper with the target");
        let found = star::self_check(&d, &target, 1000).await;
        let found: Vec<String> = found.iter().map(|d| format!("{d:?}")).collect();
        assert_eq!(
            found,
            [r#"ExtraRow { key: "1" }"#, r#"MissingRow { key: "5" }"#],
            "{}",
            star.name()
        );
    }
}

/// A field guarded by the predicate is never evaluated on a row that
/// doesn't qualify (#1991 rule 9): `w = 0` rows build, apply and fan out
/// with nothing held. The predicate reads columns no field reads (`a.y`, the
/// position's `tag`), which `R` and `M` copy, and a change of one alone
/// moves the row in or out.
#[tokio::test]
async fn a_guarded_field_and_predicate_only_columns() {
    let filter = "{b}.w <> 0 AND a.y <> 'skip' AND {b}.tag <> 'hidden'";
    for shape in SHAPES {
        for star in shape.stars() {
            let star = star.filtered(filter);
            let b = format!("b{}", shape.at);
            let definition =
                star.definition_with(&format!("CAST(a.x AS numeric) / {b}.w AS ratio"));
            let ddl = format!(
                "{}; {}",
                shape.ddl(),
                shape.seed(A_ROWS, "(1, 10, 0, 'ten'), (2, 20, 200, 'twenty')")
            );
            let columns = shape.columns();
            let columns: Vec<(&str, trellis::defs::ValueType)> =
                columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
            let tables = shape.tables();
            let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
            let mut d = Driver::start(&ddl, &columns, &[&definition], &tables).await;
            let target = star.target();
            let expected = format!(
                "select a.id, (a.x::numeric / {b}.w)::text from public.a a {} where {} \
                 order by 1",
                star.live_joins(),
                star.filter.expect("filtered")
            );
            let check = async |d: &mut Driver, context: &str| {
                d.settle().await;
                let actual = d
                    .rows(&format!("select id, ratio::text from {target} order by 1"))
                    .await;
                assert_eq!(
                    actual,
                    d.rows(&expected).await,
                    "{} {context}: target (left) differs from the live join (right)",
                    star.name()
                );
                let held = d.rows("select count(*) from poison_held").await;
                assert_eq!(held, ["(0)"], "{} {context}: a row is held", star.name());
                let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &target)
                    .await
                    .expect("the oracle");
                assert!(
                    diverging.is_empty(),
                    "{} {context}: {diverging:?}",
                    star.name()
                );
                let found = star::self_check(d, &target, 1000).await;
                assert!(found.is_empty(), "{} {context}: {found:?}", star.name());
            };
            check(&mut d, "the build").await;
            for sql in [
                "update {b} set w = 0 where k = 20",
                "update {b} set w = 300 where k = 10",
                "update public.a set y = 'skip' where id = 1",
                "update {b} set tag = 'hidden' where k = 20",
                "update public.a set y = 'one' where id = 1",
                "update {b} set tag = 'shown' where k = 20",
                "update {b} set w = 0 where k = 10",
            ] {
                star::step(&mut d, &shape.sql(sql)).await;
                check(&mut d, sql).await;
            }
            // `R` holds `y` and `M` holds `tag` for every row, as their
            // sources do.
            let r = star::r_table(&d, star).await;
            assert_eq!(
                d.rows(&format!(
                    "select __from_key, y from {r} where not __tombstone order by 1"
                ))
                .await,
                d.rows("select id::text, y from public.a order by id::text")
                    .await
            );
            let m = star::m_table(&d, star, shape.at).await;
            assert_eq!(
                d.rows(&format!(
                    "select __from_key, tag from {m} where not __tombstone order by 1"
                ))
                .await,
                d.rows(&format!(
                    "select id::text, tag from {} order by id::text",
                    shape.b()
                ))
                .await
            );
        }
    }
}

/// A row held in quarantine (its field overflows after a fan-out) stays as it
/// was while held; a change that makes it stop qualifying is parked with it,
/// and the release's Re-derive deletes it.
#[tokio::test]
async fn a_held_row_that_stops_qualifying_leaves_at_its_release() {
    for shape in SHAPES {
        // `total` fits with `w = 100` and overflows with `w = 1000`.
        let big = i32::MAX - shape.idle_total() - 500;
        for star in shape.stars() {
            let star = star.filtered("a.y <> 'gone'");
            let name = star.name();
            let mut d = star::start(
                &[star],
                &format!("{A_ROWS}, (23, 10, {big}, 'big')"),
                B_ROWS,
            )
            .await;
            star::assert_star(&d, star).await;
            star::step(&mut d, &shape.sql("update {b} set w = 1000 where k = 10")).await;
            assert_eq!(
                d.rows("select src_table, key from poison_held").await,
                ["(public.a,23)"],
                "{name}: the fan-out holds row 23"
            );
            let row = format!("select id from {} where id = 23", star.target());
            assert_eq!(d.rows(&row).await, ["(23)"], "{name}: held as it was");
            star::step(
                &mut d,
                "update public.a set y = 'gone', x = 1 where id = 23",
            )
            .await;
            assert_eq!(d.rows(&row).await, ["(23)"], "{name}: still held");
            let released =
                trellis::staging::quarantine::release_key(d.pool(), &name, "public.a", "23")
                    .await
                    .expect("release the row");
            assert_eq!(released, 1, "{name}");
            let batch = d.seal().await;
            d.drain(batch, "release").await;
            star::assert_oracle(&mut d, &[star]).await;
            assert!(
                d.rows(&row).await.is_empty(),
                "{name}: deleted at its release"
            );
            assert!(
                d.rows("select key from poison_held").await.is_empty(),
                "{name}"
            );
        }
    }
}

/// A batch of a fan-out with a `WHERE` still drives its upsert by probes
/// of `R`'s primary key, never a pass over all of `R` (#1580): the
/// predicate sits above the fenced `R` probe.
#[tokio::test]
async fn a_filtered_fan_out_batch_probes_r_by_its_key() {
    let shape = Shape::TWO;
    for filter in [ON_B, ON_A, CROSS, ANTI] {
        let stars: Vec<Star> = shape
            .stars()
            .into_iter()
            .filter(|star| admits(*star, filter))
            .map(|star| star.filtered(filter))
            .collect();
        let seed = format!(
            "{}; {}",
            shape.seed("", B_ROWS),
            shape.sql(
                "insert into {a_ins} \
                 select g, case when g <= 10000 then 10 else 20 + g % 50 end, g, 'many' \
                 from generate_series(1, 30000) g"
            )
        );
        let d = star::start_seeded(&stars, &seed).await;
        let keys: Vec<String> = d
            .ctl
            .query(
                &shape.sql(
                    "select id::text from public.a where {k} = 10 order by id::text collate \"C\"",
                ),
                &[],
            )
            .await
            .expect("the hot key's rows")
            .iter()
            .map(|row| row.get(0))
            .collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        for star in stars {
            let r = star::r_table(&d, star).await;
            let m = star::m_table(&d, star, shape.at).await;
            let bare = r.rsplit('.').next().expect("R's name").to_string();
            d.ctl
                .batch_execute(&format!(
                    "analyze {r}; analyze {m}; analyze {}",
                    star.target()
                ))
                .await
                .expect("analyze");
            let plan = trellis::staging::one_to_one_join_explain_target_write(
                d.pool(),
                &star.target(),
                &keys,
            )
            .await
            .expect("explain the batch's upsert");
            let name = format!("{} WHERE {filter}", star.name());
            assert!(
                !plan.contains(&format!("Seq Scan on {bare} ")),
                "{name}: the batch does not scan R whole:\n{plan}"
            );
            assert!(
                plan.contains(&format!("Index Scan using {bare}_pkey on {bare} "))
                    || plan.contains(&format!("Bitmap Index Scan on {bare}_pkey ")),
                "{name}: each entry is a probe of R's primary key:\n{plan}"
            );
        }
    }
}
