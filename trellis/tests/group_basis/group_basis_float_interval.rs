//! `SUM`/`AVG` over `real`, `double precision` and `interval` (#1119): exact
//! sums in a `numeric` argument's partials, each value converted exactly.
//!
//! The values here sum exactly in any order (binary fractions, whole
//! microseconds), so Postgres's own `sum()` and `avg()` are the oracle, by
//! text; the values whose `sum()` depends on its scan order are pinned to
//! the exact result instead. Each step compares the target, then
//! `self_check` must find nothing, on a plain aggregate, an inner and a
//! `LEFT JOIN`.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

use crate::support::drain_driver;

use std::time::Duration;

use drain_driver::{Driver, int4};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType};
use trellis::float::FloatWidth;
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};

/// The rows of `actual` and `expected` (same columns) that the other lacks.
async fn difference(d: &Driver, actual: &str, expected: &str) -> Vec<String> {
    d.rows(&format!(
        "(select 'target' as side, * from (({actual}) except all ({expected})) x) \
         union all \
         (select 'postgres' as side, * from (({expected}) except all ({actual})) y) \
         order by 1, 2"
    ))
    .await
}

/// Every divergence `self_check` reports on `target`, all pages.
async fn self_check(d: &Driver, target: &str) -> Vec<String> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            target,
            SelfCheckScope { after, limit: 100 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(divergences) => {
                found.extend(divergences.iter().map(|d| format!("{d:?}")));
            }
            other => panic!("unexpected outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

/// Runs `steps`, checking `targets` (target, its rows, the expected rows)
/// after each, and fails with every mismatch found on the way.
async fn run(d: &mut Driver, steps: &[&str], targets: &[(&str, &str, &str)]) {
    let mut failures = Vec::new();
    for sql in steps {
        d.step(sql).await;
        for (target, actual, expected) in targets {
            let diff = difference(d, actual, expected).await;
            if !diff.is_empty() {
                failures.push(format!("{target} after {sql}: {diff:?}"));
            }
            let found = self_check(d, target).await;
            if !found.is_empty() {
                failures.push(format!("{target} self_check after {sql}: {found:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn columns() -> [(&'static str, ValueType); 5] {
    [
        ("id", int4()),
        ("g", int4()),
        ("r", ValueType::Float(FloatWidth::Float4)),
        ("f", ValueType::Float(FloatWidth::Float8)),
        ("i", ValueType::Other(PgType::Interval)),
    ]
}

const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, \
                                                r real, f double precision, i interval); \
                       create index on public.src (g, f)";

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g \
                   SELECT g AS g, SUM(r) AS sr, AVG(r) AS ar, SUM(f) AS sf, AVG(f) AS af, \
                          SUM(i) AS si, AVG(i) AS ai, SUM(f) * 2 AS twice, MIN(f) AS lo, \
                          COUNT(f) AS cf, COUNT(*) AS n";

const AGG_ROWS: &str = "select g, sr::text, ar::text, sf::text, af::text, si::text, ai::text, \
                               twice::text, lo::text, cf, n, \
                               pg_typeof(sr)::text, pg_typeof(ar)::text, pg_typeof(si)::text \
                        from public.agg";
const AGG_EXPECTED: &str = "select g, sum(r)::text, avg(r)::text, sum(f)::text, avg(f)::text, \
                                   sum(i)::text, avg(i)::text, (sum(f) * 2)::text, \
                                   min(f)::text, count(f), count(*), \
                                   'real', 'double precision', 'interval' \
                            from public.src group by g";

/// Values in and out of their groups, `NULL`s, `NaN` and both infinities,
/// moves between groups: the target follows Postgres at every step.
#[tokio::test]
async fn plain_sums_follow_postgres_through_every_change() {
    let mut d = Driver::start(SRC_DDL, &columns(), &[AGG], &["public.src"]).await;
    run(
        &mut d,
        &[
            "insert into public.src values \
                 (1, 1, 1.5, 0.25, '1 mon 2 days 03:00:00.5'), \
                 (2, 1, -2.75, 1e15, '-7 days 1 microsecond'), \
                 (3, 1, null, null, null), \
                 (4, 2, 3, 0.5, '25 hours')",
            "update public.src set f = 'NaN' where id = 1",
            "update public.src set f = 'Infinity', r = 'Infinity' where id = 4",
            "insert into public.src values (5, 2, '-Infinity', '-Infinity', '-1 year')",
            "update public.src set f = 0.75 where id = 1",
            "delete from public.src where id = 4",
            "update public.src set g = 1 where id = 5",
            "update public.src set r = 4.5, f = 2.5 where id = 5",
            "update public.src set g = 3 where id in (1, 2)",
            "delete from public.src where g = 3",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// Values whose `sum()` depends on its scan order read the exact sum,
/// whichever order they arrive in, built or kept by deltas: `{1e20, 1,
/// -1e20}` sums to 1, ten `0.1::real` to `1`, and `2147483647 days` with a
/// day in and a day out to `2147483647 days` (Postgres raises in one order).
#[tokio::test]
async fn order_dependent_sums_read_the_exact_sum() {
    let seed = "insert into public.src values \
                    (1, 1, 0.1, 1e20, '2147483647 days'), (2, 1, 0.1, 1, '1 day'), \
                    (3, 1, 0.1, -1e20, '-1 day')";
    let mut d = Driver::start(
        &format!("{SRC_DDL}; {seed}"),
        &columns(),
        &[AGG],
        &["public.src"],
    )
    .await;
    let exact = "select sf::text, af::text, si::text, ai::text from public.agg where g = 1";
    let pinned = [
        "(1,0.3333333333333333,\"2147483647 days\",\"715827882 days 08:00:00.003433\")".to_string(),
    ];
    assert_eq!(d.rows(exact).await, pinned, "built from the source");
    assert_eq!(self_check(&d, "agg").await, Vec::<String>::new());
    // The same values arriving as changes, in the other order.
    d.step(
        "delete from public.src; \
         insert into public.src values (3, 2, 0.1, -1e20, '-1 day'); \
         insert into public.src values (2, 2, 0.1, 1, '1 day'), (1, 2, 0.1, 1e20, '2147483647 days'); \
         insert into public.src select 10 + n, 2, 0.1 from generate_series(1, 7) n",
    )
    .await;
    assert_eq!(
        d.rows("select sf::text, af::text, si::text, ai::text, sr::text from public.agg")
            .await,
        [
            "(1,0.3333333333333333,\"2147483647 days\",\"715827882 days 08:00:00.003433\",1)"
                .to_string()
        ],
        "kept by deltas"
    );
    assert_eq!(self_check(&d, "agg").await, Vec::<String>::new());
}

/// `self_check` compares the hidden sums exactly with the source's
/// (ADR-0013): a float sum off by `2^-1074`'s decimal tail, which no
/// visible `double precision` shows, and an `interval` sum off by a
/// microsecond are each reported.
#[tokio::test]
async fn self_check_reports_a_sum_off_by_its_last_digit() {
    let mut d = Driver::start(SRC_DDL, &columns(), &[AGG], &["public.src"]).await;
    d.step("insert into public.src values (1, 1, 1.5, 0.1, '1 day'), (2, 1, 2, 0.2, '2 hours')")
        .await;
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.agg'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    let hidden = format!("{DEFAULT_SCHEMA}.agg_{id}");
    for (plant, column) in [
        ("__sum_2 = __sum_2 + 1e-1074", "__sum_2"),
        ("__sum_3 = __sum_3 + 1", "__sum_3"),
    ] {
        d.ctl
            .batch_execute(&format!("update {hidden} set {plant} where g = 1"))
            .await
            .expect(plant);
        let found = self_check(&d, "agg").await;
        assert!(
            found
                .iter()
                .any(|f| f.contains(&format!("agg_{id}.{column}\""))),
            "{plant}: {found:?}"
        );
        d.ctl
            .batch_execute(&format!(
                "update {hidden} set {} where g = 1",
                plant.replace(" + ", " - ")
            ))
            .await
            .expect("undo");
    }
    assert_eq!(self_check(&d, "agg").await, Vec::<String>::new());
}

const JOIN_DDL: &str = "create table public.o (id integer primary key, k integer, \
                                               f double precision, i interval); \
                        create table public.c (id integer primary key, k integer, region text, \
                                               r real); \
                        create index on public.o (k); create index on public.c (k)";

fn join_columns() -> [(&'static str, ValueType); 4] {
    [
        ("id", int4()),
        ("k", int4()),
        ("f", ValueType::Float(FloatWidth::Float8)),
        ("i", ValueType::Other(PgType::Interval)),
    ]
}

fn join(name: &str, kind: &str) -> String {
    format!(
        "TRANSFORM {name} FROM public.o {kind} public.c ON o.k = c.k GROUP BY c.region \
         WITHOUT SOURCE INDEX SELECT c.region AS region, SUM(o.f) AS sf, AVG(o.f) AS af, \
                SUM(o.i) AS si, AVG(o.i) AS ai, SUM(c.r) AS sr, AVG(c.r) AS ar, COUNT(*) AS n"
    )
}

fn join_rows(name: &str) -> String {
    format!(
        "select region, sf::text, af::text, si::text, ai::text, sr::text, ar::text, n, \
                pg_typeof(sf)::text, pg_typeof(si)::text, pg_typeof(sr)::text \
         from public.{name}"
    )
}

fn join_expected(kind: &str) -> String {
    format!(
        "select c.region, sum(o.f)::text, avg(o.f)::text, sum(o.i)::text, avg(o.i)::text, \
                sum(c.r)::text, avg(c.r)::text, count(*), \
                'double precision', 'interval', 'real' \
         from public.o {kind} public.c on o.k = c.k group by c.region"
    )
}

/// On an inner and a `LEFT JOIN`, a float and an `interval` on the `FROM`
/// side and a `real` on the joined side, each multiplied by its matches:
/// the targets follow Postgres at every step.
#[tokio::test]
async fn join_sums_follow_postgres_through_every_change() {
    let (inner, left) = (join("ij", "JOIN"), join("lj", "LEFT JOIN"));
    let mut d = Driver::start(
        JOIN_DDL,
        &join_columns(),
        &[inner.as_str(), left.as_str()],
        &["public.o", "public.c"],
    )
    .await;
    let (ij_rows, lj_rows) = (join_rows("ij"), join_rows("lj"));
    let (ij_expected, lj_expected) = (join_expected("join"), join_expected("left join"));
    run(
        &mut d,
        &[
            "insert into public.c values (1, 1, 'a', 0.5), (2, 2, 'b', -1.25), (3, 1, 'b', 2)",
            "insert into public.o values (1, 1, 1.5, '1 day 02:00:00'), (2, 1, 'NaN', '-3 mons'), \
                                         (3, 2, 2.25, '00:00:00.000001'), (4, 9, 'Infinity', '7 days')",
            "update public.o set f = 0.125 where id = 2",
            "insert into public.c values (4, 9, 'b', 'Infinity')",
            "delete from public.c where id = 4",
            "update public.o set k = 2 where id = 1",
            "update public.c set r = null where id = 2",
            "delete from public.o where id in (2, 3)",
        ],
        &[
            ("ij", ij_rows.as_str(), ij_expected.as_str()),
            ("lj", lj_rows.as_str(), lj_expected.as_str()),
        ],
    )
    .await;
}
