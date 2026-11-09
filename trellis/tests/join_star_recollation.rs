//! A star 1-1 `JOIN` (#1460 sub-step 3.13) whose text `ON` columns at
//! position 2 are re-collated after define, between deterministic
//! collations (#1567): the joined-table cells of
//! `join_one_to_one_recollation.rs`, ported to position 2 of n = 3.
//!
//! Byte equality is unchanged, so nothing pauses; `M2` and `J`'s position-2
//! columns keep the collation they were created with, and every statement
//! over them runs under it. A `RESUME` re-validates the definition as define
//! does, so it is refused while position 2's `ON` pair is left at two
//! collations, and goes through once the pair is one again.
//!
//! Each cell checks the pass and the `RESUME` over a defined star. It also
//! builds the star, applies changes of `a` and `b2` after the re-collation, rebuilds through
//! the `RESUME`, and compares the target with the multi-join over the source
//! tables, the `ON` text compared by its bytes, after each. Nothing polls or
//! sleeps: changes are drained until nothing is pending, and builds are
//! stepped by hand to idle.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step};
use trellis::{Trellis, TrellisOptions};

const ICU: &str = "und-x-icu";

/// `a` joins `b1` on an integer key and `b2` on the text `kt`; every text
/// column starts under ICU.
fn ddl() -> String {
    format!(
        "create table public.a (id integer primary key, k1 integer, \
             kt text collate \"{ICU}\", v integer); \
         create table public.b1 (id integer primary key, k integer, w integer); \
         create unique index on public.b1 (k); \
         create table public.b2 (id integer primary key, kt text collate \"{ICU}\", \
             nm text collate \"{ICU}\", y integer); \
         create unique index on public.b2 (kt); \
         create index on public.a (k1); create index on public.a (kt); \
         insert into public.b1 select i, i, i * 1000 from generate_series(1, 3) i; \
         insert into public.a \
             select i, i % 4, 'k' || i % 6, i from generate_series(1, 12) i; \
         insert into public.b2 \
             select i, 'k' || i, (array['Xy', 'xy'])[1 + i % 2] || i, i * 10 \
             from generate_series(1, 4) i"
    )
}

fn columns() -> Vec<(&'static str, ValueType)> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    vec![
        ("id", int4),
        ("k1", int4),
        ("kt", ValueType::Text),
        ("v", int4),
    ]
}

fn definition(kind: &str) -> String {
    format!(
        "TRANSFORM t FROM public.a LEFT JOIN public.b1 ON a.k1 = b1.k \
         {kind} JOIN public.b2 ON a.kt = b2.kt \
         SELECT b2.y AS y, a.v AS v, b2.nm AS bnm, b1.w AS w"
    )
}

/// The multi-join over the source tables, position 2's `ON` text compared by
/// its bytes, in the target's columns.
fn oracle(kind: &str) -> String {
    format!(
        "select a.id, b2.y, a.v, b2.nm, b1.w from public.a a \
         left join public.b1 b1 on a.k1 = b1.k \
         {kind} join public.b2 b2 on a.kt = b2.kt collate \"C\" order by a.id"
    )
}

const TARGET: &str = "select id, y, v, bnm, w from public.t order by id";

fn alter(table: &str, column: &str, collation: &str) -> String {
    format!("alter table public.{table} alter column {column} type text collate \"{collation}\"")
}

/// One cell: the statements that re-collate, and those that bring
/// position 2's `ON` pair back to one collation, which a `RESUME` needs
/// (empty when the pair is still one).
struct Cell {
    name: &'static str,
    recollate: Vec<String>,
    realign: Vec<String>,
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

async fn status(d: &Driver) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = 'public.t'",
            &[],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// Asserts the definition isn't paused and nothing is recorded against it.
async fn assert_unpaused(d: &Driver, case: &str) {
    let failures = d
        .rows("select source_table, error from capture_failures")
        .await;
    assert!(failures.is_empty(), "{case}: {failures:?}");
    assert_ne!(status(d).await, "paused", "{case}");
}

async fn work_to_idle(d: &Driver) {
    for _ in 0..200 {
        let step = build::work_once(d.pool(), "builder", &star::build_options(3))
            .await
            .expect("a build step");
        if matches!(step, Step::Idle) {
            return;
        }
    }
    panic!("the build never went idle");
}

async fn assert_matches(d: &Driver, kind: &str, case: &str) {
    assert_eq!(d.rows(TARGET).await, d.rows(&oracle(kind)).await, "{case}");
}

/// Changes of `a` and of `b2`, each side applied by its own pages: read
/// columns updated, `ON` keys moved to and from a match, inserts, deletes.
async fn apply_changes(d: &mut Driver, round: i32, case: &str) {
    for sql in [
        format!(
            "update public.a set v = v + {round}00 where v % 3 = 0; \
             update public.a set kt = 'k{kt}' where v % 7 = 1; \
             delete from public.a where v in (4, 9); \
             insert into public.a select id + {round}00, k1, 'k{hit}', v + {round}000 \
                 from public.a where v < 6",
            kt = 20 + round,
            hit = round % 4 + 1
        ),
        format!(
            "update public.b2 set y = y + 1 where y < 30; \
             update public.b2 set kt = 'k{kt}' where y = 20 or y = 21; \
             update public.b2 set nm = nm || 'q' where y = 40; \
             insert into public.b2 select id + {round}000, 'k{r5}', nm, 77 + {round} \
                 from public.b2 where y = 10 or y = 11; \
             delete from public.b2 where y = 30 or y = 31",
            kt = 20 + round,
            r5 = 5 + round
        ),
    ] {
        d.user()
            .await
            .batch_execute(&sql)
            .await
            .unwrap_or_else(|e| panic!("{case}: {sql}: {e}"));
        d.settle().await;
    }
}

async fn run_cell(cell: &Cell, kind: &str) {
    let case = format!("{} ({kind})", cell.name);
    let case = case.as_str();
    let mut d = Driver::start(
        &ddl(),
        &columns(),
        &[&definition(kind)],
        &["public.a", "public.b1", "public.b2"],
    )
    .await;
    assert_matches(&d, kind, &format!("{case}: built")).await;
    for stmt in &cell.recollate {
        d.ctl
            .batch_execute(stmt)
            .await
            .unwrap_or_else(|e| panic!("{case}: {stmt}: {e}"));
    }
    pass(&mut d).await;
    assert_unpaused(&d, &format!("{case}: the pass after the re-collation")).await;
    apply_changes(&mut d, 1, case).await;
    assert_matches(&d, kind, &format!("{case}: applied")).await;

    let trellis = trellis(&d).await;
    trellis
        .apply("PAUSE TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{case}: pause: {e}"));
    apply_changes(&mut d, 2, case).await;
    if !cell.realign.is_empty() {
        let refused = trellis
            .apply("RESUME TRANSFORM t")
            .await
            .expect_err("a resume with position 2's pair at two collations is refused")
            .to_string();
        assert!(refused.contains("collation"), "{case}: {refused}");
        assert_eq!(status(&d).await, "paused", "{case}");
        for stmt in &cell.realign {
            d.ctl
                .batch_execute(stmt)
                .await
                .unwrap_or_else(|e| panic!("{case}: {stmt}: {e}"));
        }
    }
    trellis
        .apply("RESUME TRANSFORM t")
        .await
        .unwrap_or_else(|e| panic!("{case}: resume: {e}"));
    pass(&mut d).await;
    assert_unpaused(&d, &format!("{case}: the pass after the resume")).await;
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
    work_to_idle(&d).await;
    assert_eq!(status(&d).await, "live", "{case}: resumed");
    assert_matches(&d, kind, &format!("{case}: resumed")).await;
    apply_changes(&mut d, 3, case).await;
    assert_matches(&d, kind, &format!("{case}: applied after the resume")).await;
}

async fn run(cell: Cell) {
    let _serial = star::serial().await;
    for kind in ["", "LEFT"] {
        run_cell(&cell, kind).await;
    }
}

/// The root's `ON` column for position 2 re-collated, `b2`'s left alone:
/// `R` and `J`'s position-2 column hold its keys under the collation they
/// were created with.
#[tokio::test]
async fn the_root_s_on_column_for_position_2_re_collated() {
    run(Cell {
        name: "a.kt icu then POSIX",
        recollate: vec![alter("a", "kt", "POSIX")],
        realign: vec![alter("b2", "kt", "POSIX")],
    })
    .await;
}

/// Position 2's joined table's `ON` column re-collated, `a`'s left alone:
/// its keys sit in `M2` and `J` under the collation they were created with.
#[tokio::test]
async fn position_2_s_joined_on_column_re_collated() {
    run(Cell {
        name: "b2.kt icu then POSIX",
        recollate: vec![alter("b2", "kt", "POSIX")],
        realign: vec![alter("a", "kt", "POSIX")],
    })
    .await;
}

/// Both of position 2's `ON` columns re-collated to one collation: the pair
/// stays one, so the `RESUME` goes through as it is.
#[tokio::test]
async fn both_of_position_2_s_on_columns_re_collated() {
    run(Cell {
        name: "a.kt and b2.kt icu then POSIX",
        recollate: vec![alter("a", "kt", "POSIX"), alter("b2", "kt", "POSIX")],
        realign: Vec::new(),
    })
    .await;
}

/// A text column position 2 copies, not in its `ON`, re-collated.
#[tokio::test]
async fn a_text_column_copied_at_position_2_re_collated() {
    run(Cell {
        name: "b2.nm icu then C",
        recollate: vec![alter("b2", "nm", "C")],
        realign: Vec::new(),
    })
    .await;
}
