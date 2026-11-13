//! A 1-1 `JOIN` across a schema change of its tables (ADR-0021, #1403,
//! #1460 sub-step 3.13, #1858), as an aggregate `JOIN` handles each
//! (ADR-0019, docs/transforms.md). Each body is written once against "the
//! joined table at `at`" (`support/star.rs`) and runs at the two-table `JOIN`
//! under both kinds and at position 2 of a three-table star under all `LEFT`
//! (see [`stars`]). Each pauses the whole transform with a `capture_failure`
//! naming the table and its column, and `RESUME` brings Trellis's copies to
//! the types define would give them now (the target's through the staging
//! worker, `R`, `M` and `J` created again) and rebuilds, after which the
//! target equals the live join:
//!
//! - a column a field reads widened, on `a` or the joined table; a column
//!   only `R` or `M` copies widened; `a`'s key widened;
//! - an `ON` column retyped on one side (either first), or on both;
//! - a column read renamed, dropped or retyped to a type the transform
//!   evaluates differently;
//! - a table renamed away, dropped, or attached as a partition;
//! - the joined table's unique key on its `ON` columns dropped, left
//!   `INVALID`, or replaced; `RESUME` is refused until the key is back, and
//!   the swaps and maintenance that keep a key in place pause nothing;
//! - a duplicate past a deferrable key under `session_replication_role =
//!   replica` (a gap `self_check` reports).
//!
//! The star-only cases are at position 2 of three tables: a key per position
//! on its own `ON` column, one table at two positions of two stars, the
//! primary key redefined (the joined table's, the root's), and an enum read
//! only there.
//!
//! Every test drives the pipeline and the staging worker's pass by hand
//! through `support/drain_driver.rs`; nothing polls or sleeps.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/schema_change.rs"]
mod schema_change;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use schema_change::{
    assert_retyping, assert_unpaused, column_type, failures, pass, paused_for, resume,
    resume_refused, status, trellis, write,
};
use star::{SHAPES, Shape, Star};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

/// Position 2 of n = 3, the position the star-only tests are at.
const SHAPE: Shape = Shape {
    positions: 2,
    at: 2,
};

/// Twelve `a` rows `(id, k, x, y)` on keys 0 to 30 (every fifth on `NULL`),
/// with `y` holding digits.
const A_ROWS: &str = "(1, 10, 1, '1'), (2, 20, 2, '2'), (3, 30, 3, '3'), (4, 0, 4, '4'), \
                      (5, null, 5, '5'), (6, 20, 6, '6'), (7, 30, 7, '7'), (8, 0, 8, '8'), \
                      (9, 10, 9, '9'), (10, null, 10, '10'), (11, 30, 11, '11'), \
                      (12, 0, 12, '12')";
/// Three `b` rows `(id, k, w, tag)` on keys 10 to 30: some `a` rows have no
/// match.
const B_ROWS: &str = "(1, 10, 100, 'k1'), (2, 20, 200, 'k2'), (3, 30, 300, 'k3')";

/// The stars a body runs at `shape`: both kinds at the two-table `JOIN`,
/// and the first kind tuple (all `LEFT`) at position 2 of a star. The checks
/// read the catalog, so the kinds of the other positions change nothing.
fn stars(shape: Shape) -> Vec<Star> {
    let stars = shape.stars();
    if shape == Shape::TWO {
        stars
    } else {
        stars.into_iter().take(1).collect()
    }
}

/// The shapes of the bodies the star file had no port of before: the
/// two-table `JOIN` alone. Their position-2 runs are one word away
/// (`SHAPES`), at the cost of CI time.
const TWO_ONLY: [Shape; 1] = [Shape::TWO];

/// Whether the root `a` is under test at `shape`: only at the two-table
/// `JOIN`, as what a root column or table does doesn't depend on the joined
/// positions.
fn root_at(shape: Shape) -> bool {
    shape == Shape::TWO
}

/// `star` over the seed, built and live.
async fn built(star: Star) -> Driver {
    star::start(&[star], A_ROWS, B_ROWS).await
}

/// `definition` over the seed of `star`'s shape, built and live.
async fn built_with(star: Star, definition: &str) -> Driver {
    let shape = star.shape;
    let columns = shape.columns();
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let tables = shape.tables();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    Driver::start(
        &format!("{}; {}", shape.ddl(), shape.seed(A_ROWS, B_ROWS)),
        &columns,
        &[definition],
        &tables,
    )
    .await
}

/// `star`'s first star of [`SHAPE`] (all `LEFT`), defined over the seed, its
/// build not run.
async fn start() -> (Driver, star::Star) {
    let star = SHAPE.stars()[0];
    (star::start_unbuilt(&[star], A_ROWS, B_ROWS).await, star)
}

/// Writes `sql` and drains it, and everything it leads to.
async fn step(d: &mut Driver, sql: &str) {
    star::step(d, sql).await;
    d.settle().await;
}

/// Runs a staging-worker pass and the pending builds, then asserts `star`'s
/// transform is live and its target equals the live join (`star::assert_oracle`:
/// rows, `R`, `M`, the engine's oracle and `self_check`), and nothing
/// deadlocked.
async fn assert_exact(d: &mut Driver, star: Star, step: &str) {
    pass(d).await;
    trellis::staging::build::settle_builds_with(d.pool(), 3).await;
    d.settle().await;
    assert_eq!(
        status(d, &star.target()).await,
        "live",
        "{} after {step}: {:?}",
        star.name(),
        failures(d).await
    );
    star::assert_oracle(d, &[star]).await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

// ------------------------------------------------ columns a field reads

/// A column one field reads widened, on either table (`a.x`, `b.w`, both
/// read by `total`, an `integer`): the pass pauses the transform, naming the
/// target's column it outgrew and every column it is typed from with its
/// type now. The pause names the table and column that widened, whichever
/// table's pass runs first (#1539). `RESUME` waits for the staging worker to
/// re-type `total` to `bigint`, then rebuilds, and a value past `integer`
/// lands on both sides. A pass after it pauses nothing.
#[tokio::test]
async fn a_widened_column_a_field_reads_pauses_and_resume_re_types_the_target() {
    for shape in SHAPES {
        for star in stars(shape) {
            for (table, column, big) in [
                (
                    "public.a".to_string(),
                    "x",
                    "update public.a set x = 3000000000 where id = 1".to_string(),
                ),
                (
                    shape.b(),
                    "w",
                    shape.sql("update {b} set w = 3000000000 where id = 1"),
                ),
            ]
            .into_iter()
            .filter(|(table, ..)| root_at(shape) || table != "public.a")
            {
                let mut d = built(star).await;
                let target = star.target();
                let at = format!("{} {table}.{column}", star.name());
                write(
                    &d,
                    &format!("alter table {table} alter column {column} type bigint"),
                )
                .await;
                pass(&mut d).await;
                assert_eq!(status(&d, &target).await, "paused", "{at}");
                let error = paused_for(&d, &target, &table, &[column]).await;
                assert!(
                    error.contains(&format!("{target}.total (integer, now bigint)"))
                        && error.contains(&format!("{table}.{column} (bigint)"))
                        && error.contains("Resume the definition"),
                    "{at}: {error}"
                );

                resume(&d, &star.name()).await;
                let error = assert_retyping(&d, &target).await;
                assert!(
                    error.contains(&format!("{target}.total from integer to bigint")),
                    "{error}"
                );
                pass(&mut d).await;
                assert_eq!(column_type(&d, &target, "total").await, "bigint");
                let hidden = if table == "public.a" {
                    star::r_table(&d, star).await
                } else {
                    star::m_table(&d, star, shape.at).await
                };
                assert_eq!(
                    column_type(&d, &hidden, column).await,
                    "bigint",
                    "{hidden} is created again at the column's new type"
                );
                assert_exact(&mut d, star, "the resume").await;
                step(&mut d, &big).await;
                assert_exact(&mut d, star, "a value past integer").await;
                pass(&mut d).await;
                assert_eq!(status(&d, &target).await, "live", "{at}");
            }
        }
    }
}

/// A column only `R` or `M` holds a copy of at its own type, widened: the
/// fields cast it to `numeric`, which holds every `bigint`, so nothing of the
/// target is outgrown, but `R.x` and `M.w` are. The pass pauses the
/// transform naming the hidden column, and `RESUME` creates `R` and `M`
/// again at `bigint`, with nothing to re-type, so a value past `integer`
/// lands rather than failing its write.
#[tokio::test]
async fn a_widened_column_only_a_hidden_table_copies_pauses_and_resume_creates_it_again() {
    for shape in TWO_ONLY {
        for star in stars(shape) {
            for (table, column) in [("public.a".to_string(), "x"), (shape.b(), "w")]
                .into_iter()
                .filter(|(table, _)| root_at(shape) || table != "public.a")
            {
                let definition = star.definition_with(
                    &shape.sql("CAST(a.x AS numeric) AS rn, CAST({b}.w AS numeric) AS bn"),
                );
                let mut d = built_with(star, &definition).await;
                let target = star.target();
                let at = format!("{} {table}.{column}", star.name());
                write(
                    &d,
                    &format!("alter table {table} alter column {column} type bigint"),
                )
                .await;
                pass(&mut d).await;
                let error = paused_for(&d, &target, &table, &[column]).await;
                assert!(
                    error.contains("join1_") && error.contains(&format!(".{column} (integer")),
                    "{at}: {error}"
                );
                resume(&d, &star.name()).await;
                assert_eq!(status(&d, &target).await, "waiting_to_backfill", "{at}");
                let hidden = if table == "public.a" {
                    star::r_table(&d, star).await
                } else {
                    star::m_table(&d, star, shape.at).await
                };
                assert_eq!(column_type(&d, &hidden, column).await, "bigint", "{at}");
                step(
                    &mut d,
                    &format!("update {table} set {column} = 3000000000 where id = 1"),
                )
                .await;
                pass(&mut d).await;
                trellis::staging::build::settle_builds_with(d.pool(), 3).await;
                d.settle().await;
                assert_eq!(status(&d, &target).await, "live", "{at}");
                assert_eq!(
                    d.rows(&format!(
                        "select t.id::text, t.rn::text, t.bn::text from {target} t order by t.id"
                    ))
                    .await,
                    d.rows(&format!(
                        "select a.id::text, a.x::text, b{}.w::text from public.a a {} \
                         order by a.id",
                        shape.at,
                        star.live_joins()
                    ))
                    .await,
                    "{at}"
                );
            }
        }
    }
}

/// `a`'s primary key widened: the target's key, a copy of it, is outgrown.
/// The pass pauses the transform naming `public.a` and `id`; `RESUME` re-types
/// the target's key and rebuilds, and an `a` row keyed past `integer` joins.
/// The key is the root's, whatever the shape, so this runs at the two-table
/// `JOIN` alone.
#[tokio::test]
async fn a_widened_key_of_a_pauses_and_resume_re_types_the_target_key() {
    let shape = Shape::TWO;
    for star in stars(shape) {
        let mut d = built(star).await;
        let target = star.target();
        write(&d, "alter table public.a alter column id type bigint").await;
        pass(&mut d).await;
        let error = paused_for(&d, &target, "public.a", &["id"]).await;
        assert!(error.contains(&format!("{target}.id (integer)")), "{error}");
        resume(&d, &star.name()).await;
        assert_retyping(&d, &target).await;
        pass(&mut d).await;
        assert_eq!(column_type(&d, &target, "id").await, "bigint");
        assert_exact(&mut d, star, "the resume").await;
        step(
            &mut d,
            &shape.sql(
                "insert into {a_ins} values (3000000000, 10, 5, '5'), (3000000001, null, 6, '6')",
            ),
        )
        .await;
        assert_exact(&mut d, star, "an a row keyed past integer").await;
    }
}

// ----------------------------------------------------------- the `ON` columns

/// An `ON` column retyped on one side only (`integer` to `bigint`) breaks the
/// pair's one type: the pass pauses the transform naming that table and
/// column, and `RESUME` refuses, naming the pair, until the other side
/// matches. Then `RESUME` creates `R`, `M` and `J` again at the new key type
/// (`J`'s key type changed) and rebuilds, keeping the target, and keys past
/// `integer` join. Either side first.
#[tokio::test]
async fn an_on_column_retyped_on_one_side_pauses_until_the_pair_matches_again() {
    for shape in SHAPES {
        for star in stars(shape) {
            let (a_key, b) = (shape.k(), shape.b());
            for (first, second) in [
                (("public.a", a_key.as_str()), (b.as_str(), "k")),
                ((b.as_str(), "k"), ("public.a", a_key.as_str())),
            ] {
                let mut d = built(star).await;
                let target = star.target();
                let held = d.rows(&format!("select * from {target} order by id")).await;
                write(
                    &d,
                    &format!(
                        "alter table {} alter column {} type bigint",
                        first.0, first.1
                    ),
                )
                .await;
                pass(&mut d).await;
                // The failure names every `ON` column of the root, as the
                // check reads them together.
                let named: Vec<String> = if first.0 == "public.a" {
                    shape.all().map(|p| format!("k{p}")).collect()
                } else {
                    vec!["k".to_string()]
                };
                let named: Vec<&str> = named.iter().map(String::as_str).collect();
                let error = paused_for(&d, &target, first.0, &named).await;
                assert!(
                    error.contains("don't match"),
                    "{} {}: {error}",
                    star.name(),
                    first.0
                );
                let refused = resume_refused(&d, &star.name()).await;
                let (a_type, b_type) = if first.0 == "public.a" {
                    ("bigint", "integer")
                } else {
                    ("integer", "bigint")
                };
                assert!(
                    refused.contains(&format!(
                        "ON joins a.{a_key} ({a_type}) to b{}.k ({b_type}), which don't match",
                        shape.at
                    )),
                    "names the pair: {refused}"
                );
                assert_eq!(status(&d, &target).await, "paused");
                assert_eq!(
                    d.rows(&format!("select * from {target} order by id")).await,
                    held,
                    "a refused resume leaves the target"
                );

                write(
                    &d,
                    &format!(
                        "alter table {} alter column {} type bigint",
                        second.0, second.1
                    ),
                )
                .await;
                resume(&d, &star.name()).await;
                assert_new_key_type(&d, star).await;
                assert_exact(&mut d, star, "the resume").await;
                step(
                    &mut d,
                    &shape.sql(
                        "insert into {b} values (9, 3000000000, 900, 'k9'); \
                         insert into {a_ins} values (100, 3000000000, 100, '100')",
                    ),
                )
                .await;
                assert_exact(&mut d, star, "a key past integer").await;
            }
        }
    }
}

/// Asserts `R`, `M` and `J` hold the position under test's key as `bigint`:
/// `R`'s `k{at}`, `M`'s `k`, and `J`'s `p{at}_k{at}` alone among that
/// position's columns.
async fn assert_new_key_type(d: &Driver, star: Star) {
    let shape = star.shape;
    let k = shape.k();
    for (table, column) in [
        (star::r_table(d, star).await, k.clone()),
        (star::m_table(d, star, shape.at).await, "k".to_string()),
    ] {
        assert_eq!(
            column_type(d, &table, &column).await,
            "bigint",
            "{}: {table} is created again at the pair's new type",
            star.name()
        );
    }
    let id = star::definition_id(d, star).await;
    let j = format!("{DEFAULT_SCHEMA}.join1_{id}_j");
    let columns = d
        .rows(&format!(
            "select attname::text, pg_catalog.format_type(atttypid, atttypmod) \
             from pg_catalog.pg_attribute \
             where attrelid = '{j}'::regclass and attnum > 0 and not attisdropped \
               and attname like 'p{}\\_%' order by attnum",
            shape.at
        ))
        .await;
    assert_eq!(
        columns,
        [format!("(p{}_{k},bigint)", shape.at)],
        "{}: {j}",
        star.name()
    );
}

/// Both `ON` columns widened before any pass: the pair still has one type, so
/// nothing refuses, but `R`, `M` and `J` hold the key as `integer`, which the
/// new keys outgrow. The pass pauses the transform naming the key column,
/// and `RESUME` creates them again at `bigint` (nothing of the target to
/// re-type) and rebuilds.
#[tokio::test]
async fn an_on_pair_widened_on_both_sides_pauses_and_resume_creates_j_again() {
    for shape in TWO_ONLY {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            write(
                &d,
                &shape.sql(
                    "alter table public.a alter column {k} type bigint; \
                     alter table {b} alter column k type bigint",
                ),
            )
            .await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "paused", "{}", star.name());
            let error: String = d
                .ctl
                .query_one("select error from capture_failures", &[])
                .await
                .expect("the reason")
                .get(0);
            assert!(error.contains("join1_"), "{}: {error}", star.name());
            resume(&d, &star.name()).await;
            assert_eq!(status(&d, &target).await, "waiting_to_backfill");
            assert_new_key_type(&d, star).await;
            assert_exact(&mut d, star, "the resume").await;
            step(
                &mut d,
                &shape.sql(
                    "insert into {b} values (9, 3000000000, 900, 'k9'); \
                     insert into {a_ins} values (100, 3000000000, 100, '100')",
                ),
            )
            .await;
            assert_exact(&mut d, star, "a key past integer").await;
        }
    }
}

// --------------------------------------------------- columns renamed or dropped

/// A column a field reads renamed (`b.w`) or dropped (`a.y`, `b.tag`) pauses
/// the transform naming that table and column, by the pass or by the drain
/// that a write marks it for. Once it is back, `RESUME` rebuilds.
#[tokio::test]
async fn a_column_either_side_reads_renamed_or_dropped_pauses_naming_it() {
    for shape in SHAPES {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let b = shape.b();
            pass(&mut d).await;
            assert_unpaused(&d, &target, "the first pass").await;

            write(&d, &format!("alter table {b} rename column w to w2")).await;
            pass(&mut d).await;
            let error = paused_for(&d, &target, &b, &["w"]).await;
            assert!(error.contains("renamed or dropped"), "{error}");
            let refused = resume_refused(&d, &star.name()).await;
            assert!(refused.contains('w'), "{refused}");
            write(&d, &format!("alter table {b} rename column w2 to w")).await;
            step(&mut d, &shape.sql("update {b} set w = 7 where id = 2")).await;
            resume(&d, &star.name()).await;
            assert_exact(&mut d, star, "the resume after a rename").await;

            if root_at(shape) {
                write(&d, "alter table public.a drop column y").await;
                // A write marks it before any pass, and the drain pauses on it.
                step(&mut d, "update public.a set x = 50 where id = 2").await;
                let error = paused_for(&d, &target, "public.a", &["y"]).await;
                assert!(error.contains("renamed or dropped"), "{error}");
                write(
                    &d,
                    "alter table public.a add column y text; update public.a set y = id::text",
                )
                .await;
                pass(&mut d).await;
                resume(&d, &star.name()).await;
                // The first pass installs capture of the re-added column, which
                // holds the rebuild until the next.
                pass(&mut d).await;
                d.settle().await;
                assert_exact(&mut d, star, "the resume after a drop").await;
            }

            write(&d, &format!("alter table {b} drop column tag")).await;
            pass(&mut d).await;
            let error = paused_for(&d, &target, &b, &["tag"]).await;
            assert!(error.contains("renamed or dropped"), "{error}");
            write(
                &d,
                &format!("alter table {b} add column tag text; update {b} set tag = 'b' || id"),
            )
            .await;
            pass(&mut d).await;
            resume(&d, &star.name()).await;
            pass(&mut d).await;
            d.settle().await;
            assert_exact(&mut d, star, "the resume after b's drop").await;
        }
    }
}

/// A column a field passes through retyped to another family (`a.y`, `text`
/// to `integer`), which the transform evaluates as another type: the pass
/// pauses it, naming the column, and `RESUME` re-types the target's copy and
/// rebuilds. The column is the root's, whatever the shape, so this runs at
/// the two-table `JOIN` alone.
#[tokio::test]
async fn a_column_retyped_to_a_type_evaluated_differently_pauses_and_resume_rebuilds() {
    for star in stars(Shape::TWO) {
        let mut d = built(star).await;
        let target = star.target();
        write(
            &d,
            "alter table public.a alter column y type integer using y::integer",
        )
        .await;
        pass(&mut d).await;
        let error = paused_for(&d, &target, "public.a", &["y"]).await;
        assert!(error.contains("evaluates"), "{}: {error}", star.name());
        resume(&d, &star.name()).await;
        assert_retyping(&d, &target).await;
        pass(&mut d).await;
        assert_eq!(column_type(&d, &target, "y").await, "integer");
        assert_exact(&mut d, star, "the resume").await;
        step(&mut d, "update public.a set y = 77 where id = 3").await;
        assert_exact(&mut d, star, "a change after the resume").await;
    }
}

// ----------------------------------------------------------------- the tables

/// Either table renamed away, or dropped and created again, pauses the
/// transform with a disruption naming it. Put back, `RESUME` rebuilds from the
/// table now at that name.
#[tokio::test]
async fn either_table_renamed_away_or_dropped_pauses_the_transform() {
    for shape in SHAPES {
        for star in stars(shape) {
            let target = star.target();
            for (table, bare) in [
                ("public.a".to_string(), "a".to_string()),
                (shape.b(), format!("b{}", shape.at)),
            ]
            .into_iter()
            .filter(|(table, _)| root_at(shape) || table != "public.a")
            {
                let mut d = built(star).await;
                write(&d, &format!("alter table {table} rename to {bare}_x")).await;
                pass(&mut d).await;
                assert_eq!(
                    status(&d, &target).await,
                    "paused",
                    "{} {table}",
                    star.name()
                );
                let failure = d
                    .rows("select source_table, kind from capture_failures")
                    .await;
                assert_eq!(
                    failure,
                    [format!("({table},disruption)")],
                    "{} {table}",
                    star.name()
                );
                write(&d, &format!("alter table public.{bare}_x rename to {bare}")).await;
                write(
                    &d,
                    &shape.sql(
                        "update public.a set x = x + 1 where id = 1; \
                         update {b} set w = w + 1 where id = 1",
                    ),
                )
                .await;
                resume(&d, &star.name()).await;
                pass(&mut d).await;
                assert_exact(&mut d, star, "the resume after a rename").await;
            }
            let mut d = built(star).await;
            write(
                &d,
                &shape.sql(
                    "drop table {b}; \
                     create table {b} (id integer primary key, k integer, w integer, tag text); \
                     create unique index on {b} (k); \
                     insert into {b} values (1, 10, 1, 'one'), (4, 0, 4, 'four')",
                ),
            )
            .await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "paused", "{}", star.name());
            resume(&d, &star.name()).await;
            pass(&mut d).await;
            assert_exact(&mut d, star, "the resume after the table was created again").await;
        }
    }
}

/// The joined table attached as a partition of another pauses the transform
/// (capture can't see writes through the hierarchy), naming it, and `RESUME`
/// is refused until it is detached; then `RESUME` rebuilds.
#[tokio::test]
async fn a_side_attached_as_a_partition_pauses_the_transform() {
    for shape in SHAPES {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let b = shape.b();
            write(
                &d,
                &format!(
                    "create table public.bp (id integer not null, k integer, w integer, \
                         tag text) partition by range (id); \
                     alter table public.bp attach partition {b} \
                         for values from (minvalue) to (maxvalue)"
                ),
            )
            .await;
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "paused", "{}", star.name());
            let failure = d.rows("select source_table from capture_failures").await;
            assert_eq!(failure, [format!("({b})")], "{}", star.name());
            let refused = resume_refused(&d, &star.name()).await;
            assert!(refused.contains(&b), "{refused}");
            write(
                &d,
                &shape.sql(
                    "alter table public.bp detach partition {b}; \
                     update {b} set w = 5 where id = 3",
                ),
            )
            .await;
            resume(&d, &star.name()).await;
            pass(&mut d).await;
            assert_exact(&mut d, star, "the resume once detached").await;
        }
    }
}

// ------------------------------------------ the joined table's unique key (#1536)

/// The unique index `create unique index on {b} (k)` names: `b{at}_k_idx`.
fn key_index(shape: Shape) -> String {
    format!("b{}_k_idx", shape.at)
}

/// Asserts `star`'s transform is paused because the joined table has no
/// unique key on `k`, its `ON` column, with the remedy.
async fn assert_paused_for_lost_key(d: &Driver, star: Star) {
    let b = star.shape.b();
    let error = paused_for(d, &star.target(), &b, &["k"]).await;
    assert!(
        error.starts_with(&format!(
            "the unique key on the ON columns of {b} (k) is gone"
        )) && error.contains("resume the definition to rebuild it"),
        "{}: {error}",
        star.name()
    );
}

/// Asserts a `RESUME` of `star`'s transform is refused because the joined
/// table has no unique key on `k`: define's refusal.
async fn assert_resume_refused_for_lost_key(d: &Driver, star: Star) {
    let b = star.shape.b();
    let refused = resume_refused(d, &star.name()).await;
    assert!(
        refused.contains(&format!(
            "a 1-1 JOIN needs the ON columns of '{b}' (k) to be unique"
        )),
        "{refused}"
    );
    assert_eq!(status(d, &star.target()).await, "paused", "{}", star.name());
}

/// Asserts `self_check` finds `star`'s target converged.
async fn assert_self_check_converged(d: &Driver, star: Star) {
    use trellis::staging::self_check::CheckPageExt;
    let report = trellis(d)
        .await
        .self_check_to_end(
            &star.target(),
            trellis::SelfCheckMode::Strict,
            std::time::Duration::from_secs(30),
        )
        .await
        .unwrap_or_else(|e| panic!("self_check {}: {e}", star.name()));
    assert_eq!(
        report.outcome,
        trellis::SelfCheckOutcome::Converged,
        "{}",
        star.name()
    );
}

/// The joined table's only unique key dropped, with a duplicate on `k = 10`
/// written and drained before the next pass: that pass pauses the transform,
/// naming the table and `k`. `RESUME` is refused while the key is gone. With
/// the duplicate removed and the index created again, it rebuilds to the
/// live join, and `self_check` finds it converged.
#[tokio::test]
async fn dropping_b_s_only_unique_key_pauses_the_transform_until_it_is_back() {
    for shape in TWO_ONLY {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let (b, index) = (shape.b(), key_index(shape));
            pass(&mut d).await;
            assert_eq!(status(&d, &target).await, "live", "{}", star.name());
            write(&d, &format!("drop index public.{index}")).await;
            step(
                &mut d,
                &format!("insert into {b} values (4, 10, 400, 'k4')"),
            )
            .await;
            pass(&mut d).await;
            assert_paused_for_lost_key(&d, star).await;
            assert_resume_refused_for_lost_key(&d, star).await;

            write(
                &d,
                &format!("delete from {b} where id = 4; create unique index {index} on {b} (k)"),
            )
            .await;
            resume(&d, &star.name()).await;
            assert_exact(&mut d, star, "the resume once the key is back").await;
            assert_self_check_converged(&d, star).await;
            step(&mut d, &shape.sql("update {b} set w = 9 where id = 1")).await;
            assert_exact(&mut d, star, "a change after the resume").await;
        }
    }
}

/// A failed `CREATE UNIQUE INDEX CONCURRENTLY` (over a duplicate written
/// while the old index was gone) leaves an `INVALID` index as the joined
/// table's only unique key on `k`, which is no key: the pass pauses the
/// transform, and a resume is refused until the duplicate is removed and the
/// index rebuilt.
#[tokio::test]
async fn an_invalid_unique_index_as_the_only_key_pauses_the_transform() {
    for shape in SHAPES {
        for star in stars(shape) {
            let mut d = built(star).await;
            let (b, index) = (shape.b(), key_index(shape));
            write(&d, &format!("drop index public.{index}")).await;
            step(
                &mut d,
                &format!("insert into {b} values (4, 10, 400, 'k4')"),
            )
            .await;
            d.user()
                .await
                .batch_execute(&format!(
                    "create unique index concurrently {index} on {b} (k)"
                ))
                .await
                .expect_err("k = 10 is duplicated");
            let valid = d
                .rows(&format!(
                    "select indisvalid::text from pg_catalog.pg_index \
                     where indexrelid = 'public.{index}'::regclass"
                ))
                .await;
            assert_eq!(valid, ["(false)"], "the failed build left an INVALID index");
            pass(&mut d).await;
            assert_paused_for_lost_key(&d, star).await;
            assert_resume_refused_for_lost_key(&d, star).await;

            write(
                &d,
                &format!("delete from {b} where id = 4; reindex index public.{index}"),
            )
            .await;
            resume(&d, &star.name()).await;
            assert_exact(&mut d, star, "the resume once the index is valid").await;
            assert_self_check_converged(&d, star).await;
        }
    }
}

/// Replacing the joined table's unique key the way that keeps one in place at
/// every moment pauses nothing, with a pass between every two statements: a
/// swap that creates the new index first (`CREATE UNIQUE INDEX
/// CONCURRENTLY`, `DROP INDEX CONCURRENTLY` of the old, rename), a swap in
/// one transaction, the index promoted to a constraint (`ADD CONSTRAINT ...
/// UNIQUE USING INDEX`), and the maintenance that rewrites it in place
/// (`REINDEX` in both forms, `VACUUM FULL`, `CLUSTER`): #1536's study's
/// routine rows. The target stays the live join.
#[tokio::test]
async fn swaps_that_keep_a_unique_key_in_place_pause_nothing() {
    for shape in SHAPES {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let (b, index, bare) = (shape.b(), key_index(shape), format!("b{}", shape.at));
            for statement in [
                format!("create unique index concurrently {bare}_k_new on {b} (k)"),
                format!("drop index concurrently public.{index}"),
                format!("alter index public.{bare}_k_new rename to {index}"),
                format!(
                    "begin; drop index public.{index}; \
                     create unique index {index} on {b} (k); commit"
                ),
                format!("alter table {b} add constraint {bare}_k_c unique using index {index}"),
                format!("reindex index public.{bare}_k_c"),
                format!("reindex table concurrently {b}"),
                format!("vacuum full {b}"),
                format!("cluster {b} using {bare}_k_c"),
            ] {
                write(&d, &statement).await;
                pass(&mut d).await;
                assert_eq!(
                    status(&d, &target).await,
                    "live",
                    "{} after {statement}",
                    star.name()
                );
                assert_unpaused(&d, &target, &statement).await;
            }
            step(&mut d, &shape.sql("update {b} set w = 9 where id = 1")).await;
            assert_exact(&mut d, star, "the swaps").await;
        }
    }
}

/// A closure reading how many unique-key checks the pass has made for
/// `star`'s definition.
async fn key_check_counter(d: &Driver, star: Star) -> impl Fn() -> u64 + use<> {
    let id = star::definition_id(d, star).await;
    let database: String = d
        .ctl
        .query_one("select current_database()::text", &[])
        .await
        .expect("the database")
        .get(0);
    move || trellis::staging::unique_key_checks(&database, DEFAULT_SCHEMA, id)
}

/// The pass skips the key's catalog query while the joined table's
/// fingerprint is the one its last check found the key at, and a drop with no
/// other change between two passes still changes the fingerprint: the next
/// pass checks that table's key alone and pauses the transform.
#[tokio::test]
async fn a_drop_between_two_passes_with_no_other_change_is_detected() {
    for shape in SHAPES {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let checks = key_check_counter(&d, star).await;
            pass(&mut d).await;
            let checked = checks();
            assert!(
                checked >= shape.positions as u64,
                "{}: the first pass checks every position's key",
                star.name()
            );
            pass(&mut d).await;
            pass(&mut d).await;
            assert_eq!(
                checks(),
                checked,
                "{}: skipped while the tables are unchanged",
                star.name()
            );
            assert_eq!(status(&d, &target).await, "live", "{}", star.name());

            write(&d, &format!("drop index public.{}", key_index(shape))).await;
            pass(&mut d).await;
            assert_eq!(
                checks(),
                checked + 1,
                "{}: the drop forces the check of its table alone",
                star.name()
            );
            assert_paused_for_lost_key(&d, star).await;
        }
    }
}

/// A swap that drops the old index before the new one exists pauses the
/// transform when a pass lands between the two statements, though no
/// duplicate arrives (the Decision's accepted cost). The pause holds once the
/// new index is valid, until `RESUME` rebuilds. After the resume the pass
/// records the key again and skips while the table is unchanged, and a
/// second drop is caught all the same.
#[tokio::test]
async fn a_drop_first_swap_with_a_pass_in_between_pauses_until_resumed() {
    for shape in TWO_ONLY {
        for star in stars(shape) {
            let mut d = built(star).await;
            let (b, index) = (shape.b(), key_index(shape));
            let checks = key_check_counter(&d, star).await;
            pass(&mut d).await;
            write(&d, &format!("drop index public.{index}")).await;
            pass(&mut d).await;
            assert_paused_for_lost_key(&d, star).await;
            write(
                &d,
                &format!("create unique index concurrently {index} on {b} (k)"),
            )
            .await;
            pass(&mut d).await;
            assert_paused_for_lost_key(&d, star).await;
            resume(&d, &star.name()).await;
            assert_exact(&mut d, star, "the resume after the swap").await;

            pass(&mut d).await;
            let checked = checks();
            pass(&mut d).await;
            assert_eq!(
                checks(),
                checked,
                "{}: skipped after the resume",
                star.name()
            );
            write(&d, &format!("drop index public.{index}")).await;
            pass(&mut d).await;
            assert_paused_for_lost_key(&d, star).await;
        }
    }
}

/// The gap entry "A duplicate in `b` past a deferrable key under
/// `session_replication_role = replica`" in `docs/known-correctness-gaps.md`,
/// as it says: the joined table's key on `k` is a deferrable constraint
/// (initially deferred for `LEFT`, initially immediate for `INNER`; put in
/// place by a create-first swap, which pauses nothing), and a role that is no
/// superuser but is granted `SET` on `session_replication_role` inserts a
/// second row on `k = 10` under `replica`. Postgres skips the constraint's
/// recheck, so the row commits and the key stays valid; the capture triggers
/// fire, so the drain applies it; the pass finds the key and pauses nothing.
/// The target holds one row per `a` row on the key where the join gives two,
/// and `self_check` reports it. Removing the duplicate, then `PAUSE` and
/// `RESUME`, repairs it. On a server older than 15 (no `GRANT SET ON
/// PARAMETER`) the test returns without running.
#[tokio::test]
async fn a_duplicate_past_a_deferrable_key_under_replica_is_captured_and_reported() {
    use trellis::staging::self_check::CheckPageExt;
    for shape in TWO_ONLY {
        for star in stars(shape) {
            let mut d = built(star).await;
            let target = star.target();
            let (b, index, bare) = (shape.b(), key_index(shape), format!("b{}", shape.at));
            let k = shape.k();
            let version: i32 = d
                .ctl
                .query_one("select current_setting('server_version_num')::int", &[])
                .await
                .expect("read the server version")
                .get(0);
            if version < 150_000 {
                eprintln!(
                    "skipped: GRANT SET ON PARAMETER needs PostgreSQL 15, the server is {version}"
                );
                return;
            }
            let initially = match star.kind(shape.at) {
                star::Kind::Left => "deferred",
                star::Kind::Inner => "immediate",
            };
            for statement in [
                format!(
                    "alter table {b} add constraint {bare}_k_d unique (k) \
                     deferrable initially {initially}"
                ),
                format!("drop index public.{index}"),
            ] {
                write(&d, &statement).await;
                pass(&mut d).await;
                assert_eq!(
                    status(&d, &target).await,
                    "live",
                    "{} after {statement}",
                    star.name()
                );
            }
            write(
                &d,
                &format!(
                    "do $$ begin \
                       if not exists (select from pg_roles where rolname = 'replica_writer') then \
                         create role replica_writer nosuperuser; \
                       end if; end $$; \
                     grant set on parameter session_replication_role to replica_writer; \
                     grant usage on schema public to replica_writer; \
                     grant insert on {b} to replica_writer"
                ),
            )
            .await;
            let user = d.user().await;
            user.batch_execute("set role replica_writer")
                .await
                .expect("set role");
            let duplicate = format!("insert into {b} values (4, 10, 400, 'k4')");
            user.batch_execute(&duplicate)
                .await
                .expect_err("the recheck refuses it outside replica");
            user.batch_execute(&format!(
                "set session_replication_role = replica; {duplicate}; \
                 reset session_replication_role; reset role"
            ))
            .await
            .expect("under replica the duplicate commits");
            drop(user);
            assert_eq!(
                d.rows(&format!(
                    "select count(*)::text, bool_and(i.indisvalid)::text from {b} b, \
                     pg_catalog.pg_index i where b.k = 10 \
                     and i.indexrelid = 'public.{bare}_k_d'::regclass"
                ))
                .await,
                ["(2,true)"],
                "two rows on k = 10 under a valid key"
            );
            let batch = d.seal().await;
            d.drain(batch, "the duplicate").await;
            d.settle().await;
            let m = star::m_table(&d, star, shape.at).await;
            assert_eq!(
                d.rows(&format!(
                    "select count(*)::text from {m} where k = 10 and not __tombstone"
                ))
                .await,
                ["(2)"],
                "{}: the duplicate was captured and applied",
                star.name()
            );
            pass(&mut d).await;
            assert_eq!(
                status(&d, &target).await,
                "live",
                "{}: nothing paused",
                star.name()
            );
            let in_target = d
                .rows(&format!(
                    "select count(*)::text from {target} t join public.a a on a.id = t.id \
                     where a.{k} = 10"
                ))
                .await;
            let in_join = d
                .rows(&format!(
                    "select count(*)::text from public.a a join {b} b on a.{k} = b.k \
                     where a.{k} = 10"
                ))
                .await;
            let a_rows = d
                .rows(&format!(
                    "select count(*)::text from public.a where {k} = 10"
                ))
                .await;
            assert_eq!(
                in_target,
                a_rows,
                "{}: one target row per a row",
                star.name()
            );
            assert_ne!(
                in_target,
                in_join,
                "{}: the join gives one per pair",
                star.name()
            );
            let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &target)
                .await
                .expect("self_check");
            assert!(
                !diverging.is_empty(),
                "{}: self_check reports it",
                star.name()
            );
            let report = trellis(&d)
                .await
                .self_check_to_end(
                    &target,
                    trellis::SelfCheckMode::Strict,
                    std::time::Duration::from_secs(30),
                )
                .await
                .expect("self_check");
            assert!(
                matches!(report.outcome, trellis::SelfCheckOutcome::Diverged(_)),
                "{}: {:?}",
                star.name(),
                report.outcome
            );

            step(&mut d, &format!("delete from {b} where id = 4")).await;
            trellis(&d)
                .await
                .apply(&format!("PAUSE TRANSFORM {}", star.name()))
                .await
                .expect("pause");
            resume(&d, &star.name()).await;
            assert_exact(&mut d, star, "the duplicate removed, PAUSE and RESUME").await;
        }
    }
}

// ---------------------------------------------------------- star-only cases

// ------------------------------------------- a position's unique key (#1536)

/// A star whose `ON` columns differ by position: position 1 joins `b1` on
/// `k`, position 2 joins `b2` on `code`, each with its own unique index.
const KEYED_DDL: &str = "create table public.a (id integer primary key, k1 integer, \
                             k2 integer, x integer); \
                         create table public.b1 (id integer primary key, k integer, w integer); \
                         create unique index b1_k_unique on public.b1 (k); \
                         create table public.b2 (id integer primary key, code integer, \
                             w integer); \
                         create unique index b2_code_unique on public.b2 (code); \
                         create index on public.a (k1); create index on public.a (k2); \
                         insert into public.b1 values (1, 10, 1), (2, 20, 2); \
                         insert into public.b2 values (1, 100, 10), (2, 200, 20); \
                         insert into public.a values (1, 10, 100, 5), (2, 20, 300, 6)";

const KEYED: &str = "TRANSFORM v FROM public.a \
                     LEFT JOIN public.b1 ON a.k1 = b1.k \
                     LEFT JOIN public.b2 ON a.k2 = b2.code \
                     SELECT a.x + COALESCE(b1.w, 0) + COALESCE(b2.w, 0) AS total";

async fn start_keyed() -> Driver {
    let int4 = ValueType::Integer(IntWidth::Int4);
    Driver::start_unbuilt(
        &[],
        KEYED_DDL,
        &[("id", int4), ("k1", int4), ("k2", int4), ("x", int4)],
        &[KEYED],
        &["public.a", "public.b1", "public.b2"],
    )
    .await
}

/// Each position's key is checked against that position's own `ON` columns:
/// with both keys in place, passes pause nothing (`b2` has no column `k`, so
/// a check of position 1's `ON` columns against it would find no key).
/// Position 2's unique index dropped, the next pass pauses the whole
/// transform naming `b2` and `code`, and `RESUME` is refused with define's
/// refusal until the index is back. Position 1's dropped pauses it naming
/// `b1` and `k`.
#[tokio::test]
async fn a_lost_unique_key_at_position_2_pauses_naming_it() {
    let mut d = start_keyed().await;
    let target = "public.v";
    pass(&mut d).await;
    pass(&mut d).await;
    assert_unpaused(&d, target, "two passes over intact keys").await;

    write(&d, "drop index public.b2_code_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, target, "public.b2", &["code"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (code) is gone")
            && error.contains("resume the definition to rebuild it"),
        "{error}"
    );
    let refused = resume_refused(&d, "v").await;
    assert!(
        refused.contains("a 1-1 JOIN needs the ON columns of 'public.b2' (code) to be unique"),
        "{refused}"
    );
    assert_eq!(status(&d, target).await, "paused");

    write(&d, "create unique index b2_code_unique on public.b2 (code)").await;
    resume(&d, "v").await;
    pass(&mut d).await;
    assert_unpaused(&d, target, "the resume once the key is back").await;

    write(&d, "drop index public.b1_k_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, target, "public.b1", &["k"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b1 (k) is gone"),
        "{error}"
    );
}

/// One table at different positions of two stars, on different `ON` columns
/// (`b2` at position 2 of `v1` on `code`, at position 3 of `v2` on `alt`),
/// and the root of a third transform (`v3`): each definition is checked at
/// its own position against its own `ON` columns. `b2`'s key on `alt`
/// dropped pauses `v2` alone; its key on `code` replaced by a partial unique
/// index (built first, so a unique index on `code` exists at every moment)
/// pauses `v1` alone, as define refuses a partial index as the key; the
/// transform `b2` is the root of pauses neither time.
#[tokio::test]
async fn one_table_at_two_positions_of_two_stars_is_checked_per_definition() {
    let int4 = ValueType::Integer(IntWidth::Int4);
    let mut d = Driver::start_unbuilt(
        &[],
        "create table public.a (id integer primary key, k1 integer, k2 integer, x integer); \
         create table public.c (id integer primary key, k1 integer, k2 integer, k3 integer, \
             x integer); \
         create table public.b1 (id integer primary key, k integer, w integer); \
         create unique index b1_k_unique on public.b1 (k); \
         create table public.b3 (id integer primary key, k integer, w integer); \
         create unique index b3_k_unique on public.b3 (k); \
         create table public.b2 (id integer primary key, code integer, alt integer, \
             w integer); \
         create unique index b2_code_unique on public.b2 (code); \
         create unique index b2_alt_unique on public.b2 (alt); \
         create index on public.a (k1); create index on public.a (k2); \
         create index on public.c (k1); create index on public.c (k2); \
         create index on public.c (k3); \
         insert into public.b1 values (1, 10, 1); insert into public.b3 values (1, 30, 3); \
         insert into public.b2 values (1, 100, 1000, 30); \
         insert into public.a values (1, 10, 100, 5); \
         insert into public.c values (1, 10, 1000, 30, 6)",
        &[
            ("id", int4),
            ("k1", int4),
            ("k2", int4),
            ("k3", int4),
            ("x", int4),
            ("code", int4),
            ("alt", int4),
            ("w", int4),
        ],
        &[
            "TRANSFORM v1 FROM public.a \
             LEFT JOIN public.b1 ON a.k1 = b1.k \
             LEFT JOIN public.b2 ON a.k2 = b2.code \
             SELECT a.x + COALESCE(b1.w, 0) + COALESCE(b2.w, 0) AS total",
            "TRANSFORM v2 FROM public.c \
             LEFT JOIN public.b1 ON c.k1 = b1.k \
             LEFT JOIN public.b3 ON c.k3 = b3.k \
             LEFT JOIN public.b2 ON c.k2 = b2.alt \
             SELECT c.x + COALESCE(b2.w, 0) AS total",
            "TRANSFORM v3 FROM public.b2 LEFT JOIN public.b3 ON b2.w = b3.k \
             SELECT b2.code + COALESCE(b3.w, 0) AS total",
        ],
        &[
            "public.a",
            "public.c",
            "public.b1",
            "public.b2",
            "public.b3",
        ],
    )
    .await;
    pass(&mut d).await;
    pass(&mut d).await;
    for target in ["public.v1", "public.v2", "public.v3"] {
        assert_unpaused(&d, target, "two passes over intact keys").await;
    }

    write(&d, "drop index public.b2_alt_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, "public.v2", "public.b2", &["alt"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (alt) is gone"),
        "{error}"
    );
    for target in ["public.v1", "public.v3"] {
        assert_ne!(
            status(&d, target).await,
            "paused",
            "{target} after alt's drop"
        );
    }

    write(
        &d,
        "create unique index b2_code_partial on public.b2 (code) where code > 0",
    )
    .await;
    pass(&mut d).await;
    assert_ne!(
        status(&d, "public.v1").await,
        "paused",
        "a partial index added"
    );
    write(&d, "drop index public.b2_code_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, "public.v1", "public.b2", &["code"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (code) is gone"),
        "{error}"
    );
    assert_ne!(
        status(&d, "public.v3").await,
        "paused",
        "the root's transform"
    );
}

// ------------------------------------------------------------- keys redefined

/// The primary key of the table at position 2 redefined (`(id)` to
/// `(id, tag)`) pauses the transform naming `b2`. `RESUME` records `b2`'s
/// new key as the one it keys `b2` by, and goes through.
#[tokio::test]
async fn position_2_s_primary_key_redefined_pauses_and_resume_records_the_new_key() {
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;

    write(
        &d,
        &format!("alter table {b} drop constraint b2_pkey, add primary key (id, tag)"),
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    let failure = d.rows("select source_table from capture_failures").await;
    assert_eq!(failure, [format!("({b})")]);
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume").await;
    let id = star::definition_id(&d, star).await;
    let recorded = d
        .rows(&format!(
            "select column_name from definition_key_types \
             where transform_id = {id} and table_name = '{b}' order by 1"
        ))
        .await;
    assert_eq!(recorded, ["(id)", "(k)", "(tag)"], "{b}'s key types");
}

/// The root's primary key redefined (`(id)` to `(id, x)`) pauses the star,
/// and `RESUME` re-keys its target to the new key (#1746) and goes through.
#[tokio::test]
async fn the_root_s_primary_key_redefined_and_resume_re_keys_the_target() {
    let (mut d, star) = start().await;
    let target = star.target();
    pass(&mut d).await;

    write(
        &d,
        "alter table public.a drop constraint a_pkey, add primary key (id, x)",
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume").await;
    let key = d
        .rows(&format!(
            "select a.attname::text from pg_catalog.pg_index i \
             cross join lateral unnest(i.indkey::int2[]) with ordinality k(n, o) \
             join pg_catalog.pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.n \
             where i.indrelid = '{target}'::regclass and i.indisprimary order by k.o"
        ))
        .await;
    assert_eq!(key, ["(id)", "(x)"], "{target}'s key");
}

// ------------------------------------------------------- an enum at position 2

/// A value of an enum the star reads only at position 2 (`b2.tone`)
/// relabelled: the pass pauses the transform with the relabel's reason, as
/// for the joined table at position 1 (the column's token sits in
/// `definition_join_tables`, not `join_columns`).
#[tokio::test]
async fn an_enum_value_read_at_position_2_relabelled_pauses_the_transform() {
    let mut d = start_enum().await;
    pass(&mut d).await;
    assert_unpaused(&d, "public.v", "the first pass").await;

    write(&d, "alter type public.tone rename value 'calm' to 'cool'").await;
    pass(&mut d).await;
    assert_eq!(status(&d, "public.v").await, "paused");
    let failures = failures(&d).await;
    assert!(
        failures.len() == 1
            && failures[0].contains("enum value 'calm' of type public.tone was renamed to 'cool'"),
        "{failures:?}"
    );
}

/// The column of an enum the star reads only at position 2 (`b2.tone`)
/// re-typed to another enum: the pass pauses the transform with the re-type's
/// reason, found through the column's token in `definition_join_tables`, and
/// records the new type there, as for the joined table at position 1.
#[tokio::test]
async fn an_enum_column_read_at_position_2_re_typed_to_another_enum_pauses_the_transform() {
    let mut d = start_enum().await;
    pass(&mut d).await;
    assert_unpaused(&d, "public.v", "the first pass").await;

    write(
        &d,
        "create type public.mood as enum ('fast', 'slow'); \
         alter table public.b2 alter column tone type public.mood using \
         (case tone::text when 'brisk' then 'fast' else 'slow' end)::public.mood",
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, "public.v").await, "paused");
    let failures = failures(&d).await;
    assert!(
        failures.len() == 1
            && failures[0].contains(
                "of public.b2, which this definition reads, changed type from enum public.tone \
                 to enum public.mood"
            ),
        "{failures:?}"
    );
    assert_eq!(
        d.rows(
            "select columns->>'tone' from definition_join_tables where table_name = 'public.b2'"
        )
        .await,
        ["(enum:public.mood)"]
    );
}

/// A star whose position 2 (`b2`) is the only table with an enum column the
/// star reads (`b2.tone`), defined, its build not run.
async fn start_enum() -> Driver {
    let int4 = ValueType::Integer(IntWidth::Int4);
    Driver::start_unbuilt(
        &[],
        "create type public.tone as enum ('calm', 'brisk'); \
         create table public.a (id integer primary key, k1 integer, k2 integer, x integer); \
         create table public.b1 (id integer primary key, k integer, w integer); \
         create unique index on public.b1 (k); \
         create table public.b2 (id integer primary key, code integer, tone public.tone); \
         create unique index on public.b2 (code); \
         insert into public.b1 values (1, 10, 1); \
         insert into public.b2 values (1, 100, 'calm'), (2, 200, 'brisk'); \
         insert into public.a values (1, 10, 100, 5), (2, 20, 200, 6)",
        &[("id", int4), ("k1", int4), ("k2", int4), ("x", int4)],
        &["TRANSFORM v FROM public.a \
           LEFT JOIN public.b1 ON a.k1 = b1.k \
           LEFT JOIN public.b2 ON a.k2 = b2.code \
           SELECT a.x + COALESCE(b1.w, 0) AS total, b2.tone AS tone"],
        &["public.a", "public.b1", "public.b2"],
    )
    .await
}
