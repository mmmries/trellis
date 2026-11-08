//! `SUM`/`AVG` over a `numeric` with no declared scale (#1120): an
//! unconstrained column, an expression over one, and a `numeric` column of a
//! Trellis target.
//!
//! Postgres shows `sum(numeric)` at the largest display scale among the
//! group's current finite values (`sum({1.5, 2.000})` is `3.500`, and `1.5`
//! once `2.000` leaves), and `avg(numeric)` divides that sum, so the scale
//! feeds its value. `NaN` makes both `NaN`, and so do `Infinity` and
//! `-Infinity` together; one infinity alone is that infinity. The partials
//! keep the finite values' sum, a count per scale, and the three special
//! values' counts, so every step here must match Postgres's own aggregate
//! **as text**, then `self_check` must find nothing, on a plain aggregate
//! and on an inner and a `LEFT JOIN`.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page};

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// The rows of `actual` and `expected` (same columns, each cast to text by
/// the caller) that the other lacks: equal text, not equal value.
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

/// Checks every target of `targets` (`(name, actual, expected)`) against
/// Postgres as text, and with `self_check`, collecting what differs.
async fn check(
    d: &Driver,
    after: &str,
    targets: &[(&str, &str, &str)],
    failures: &mut Vec<String>,
) {
    for (target, actual, expected) in targets {
        let diff = difference(d, actual, expected).await;
        if !diff.is_empty() {
            failures.push(format!("{target} after {after}: {diff:?}"));
        }
        let found = self_check(d, target).await;
        if !found.is_empty() {
            failures.push(format!("{target} self_check after {after}: {found:?}"));
        }
    }
}

/// Runs `steps`, one page each, checking every target after each.
async fn run(d: &mut Driver, steps: &[&str], targets: &[(&str, &str, &str)]) {
    let mut failures = Vec::new();
    for sql in steps {
        step(d, sql).await;
        check(d, sql, targets, &mut failures).await;
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const SRC_DDL: &str = "create table public.src (id integer primary key, g integer, x numeric); \
                       create index on public.src (g)";

const AGG: &str = "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
                   SELECT g AS g, SUM(x) AS total, AVG(x) AS mean, COUNT(x) AS cx, \
                          COUNT(*) AS n, SUM(x) / 3 AS third";

const AGG_ROWS: &str = "select g, total::text, mean::text, cx, n, third::text from public.agg";
const AGG_EXPECTED: &str = "select g, sum(x)::text, avg(x)::text, count(x), count(*), \
                                   (sum(x) / 3)::text from public.src group by g";

fn src_columns() -> Vec<(&'static str, ValueType)> {
    vec![("id", int4()), ("g", int4()), ("x", ValueType::Numeric)]
}

async fn start_agg() -> Driver {
    Driver::start(SRC_DDL, &src_columns(), &[AGG], &["public.src"]).await
}

/// The largest scale of a group leaves and comes back, a value at a smaller
/// scale stays, zeros with a scale, a `NULL`, huge and tiny values: the sum
/// and the average follow Postgres's text at every step.
#[tokio::test]
async fn the_largest_scale_empties_and_refills() {
    let mut d = start_agg().await;
    run(
        &mut d,
        &[
            "insert into public.src values (1, 1, 1.5), (2, 1, 2.000), (3, 2, 7), (4, 1, null)",
            "delete from public.src where id = 2",
            "insert into public.src values (2, 1, 0.00000)",
            "update public.src set x = 2.25 where id = 2",
            "update public.src set g = 2 where id = 1",
            "insert into public.src values (5, 1, 1e20), (6, 1, 0), (7, 1, 0)",
            "update public.src set x = 1.5e-20 where id = 6",
            "delete from public.src where id = 6",
            "update public.src set x = -1e20 where id = 5",
            "delete from public.src where g = 1",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// Two pages on one group, the second queued behind the first's lock: the
/// first takes the group's only value at its largest scale away, the second
/// brings a value at a smaller one, then a third brings the largest back.
/// Each applies against the scale counts the other committed.
#[tokio::test]
async fn the_largest_scale_empties_and_refills_across_two_concurrent_pages() {
    let mut d = start_agg().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1.5), (2, 1, 2.000)",
    )
    .await;
    write(&d, "delete from public.src where id = 2").await;
    let first = d.seal().await;
    write(&d, "insert into public.src values (3, 1, 0.25)").await;
    let second = d.seal().await;
    let mut held = d
        .drain_frozen(first, "a", &[(PausePoint::AfterGroupUpsert, "public.agg")])
        .await;
    let frozen = held.reached(PausePoint::AfterGroupUpsert).await;
    let page = d.drain_frozen(second, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut held, PausePoint::AfterGroupUpsert).await;
    held.finish().await;
    page.finish().await;
    let mut failures = Vec::new();
    check(
        &d,
        "the two pages",
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
        &mut failures,
    )
    .await;
    assert_eq!(
        d.rows("select total::text from public.agg").await,
        ["(1.75)"],
        "the leftover scale of 2.000 is gone"
    );

    // The other order: the page bringing the largest scale back waits for
    // the one taking a smaller value away.
    write(&d, "delete from public.src where id = 3").await;
    let first = d.seal().await;
    write(&d, "insert into public.src values (4, 1, 0.1250)").await;
    let second = d.seal().await;
    let mut held = d
        .drain_frozen(first, "a", &[(PausePoint::AfterGroupUpsert, "public.agg")])
        .await;
    let frozen = held.reached(PausePoint::AfterGroupUpsert).await;
    let page = d.drain_frozen(second, "b", &[]).await;
    d.wait_blocked_behind(frozen.backend_pid).await;
    d.release(&mut held, PausePoint::AfterGroupUpsert).await;
    held.finish().await;
    page.finish().await;
    check(
        &d,
        "the second pair",
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
        &mut failures,
    )
    .await;
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `NaN` in and out; `Infinity` and `-Infinity` together (`NaN`), then
/// apart (the one left); an infinity leaving the finite sum behind at its
/// scale.
#[tokio::test]
async fn special_values_in_and_out() {
    let mut d = start_agg().await;
    run(
        &mut d,
        &[
            "insert into public.src values (1, 1, 1.25), (2, 1, 'NaN'), (3, 2, 4)",
            "delete from public.src where id = 2",
            "insert into public.src values (2, 1, 'Infinity')",
            "insert into public.src values (4, 1, '-Infinity')",
            "update public.src set x = 'NaN' where id = 2",
            "update public.src set x = 'Infinity' where id = 2",
            "delete from public.src where id = 4",
            "update public.src set x = '-Infinity', g = 2 where id = 2",
            "update public.src set x = 3.000 where id = 2",
            "delete from public.src where id in (1, 2, 3)",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// A value at the largest scale Postgres keeps, 16383: the sum shows every
/// place, and leaving takes them away again.
#[tokio::test]
async fn a_value_at_scale_16383() {
    let mut d = start_agg().await;
    run(
        &mut d,
        &[
            "insert into public.src values (1, 1, 2.5), (2, 1, round(1, 16383))",
            "update public.src set x = round(-0.5, 16383) where id = 2",
            "delete from public.src where id = 2",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// Expression arguments: arithmetic over an unconstrained column and over a
/// `numeric(p,s)` one (Postgres drops the modifier there), and a `CASE`
/// whose `ELSE 0` mixes scales.
#[tokio::test]
async fn expression_arguments() {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, x numeric, \
                                  y numeric(10, 2)); \
         create index on public.src (g)",
        &[
            ("id", int4()),
            ("g", int4()),
            ("x", ValueType::Numeric),
            ("y", ValueType::Numeric),
        ],
        &[
            "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
           SELECT g AS g, SUM(x * 2) AS doubled, AVG(y / 3) AS third, \
                  SUM(CASE WHEN x > 0 THEN x ELSE 0 END) AS positive, COUNT(*) AS n",
        ],
        &["public.src"],
    )
    .await;
    let rows = "select g, doubled::text, third::text, positive::text, n from public.agg";
    let expected = "select g, sum(x * 2)::text, avg(y / 3)::text, \
                           sum(case when x > 0 then x else 0 end)::text, count(*) \
                    from public.src group by g";
    run(
        &mut d,
        &[
            "insert into public.src values (1, 1, 1.5, 1.00), (2, 1, -2.125, 2.50), \
                                           (3, 2, 'NaN', 7)",
            "delete from public.src where id = 1",
            "update public.src set x = 0.0001 where id = 2",
            "update public.src set x = 5 where id = 3",
        ],
        &[("agg", rows, expected)],
    )
    .await;
}

/// A build over rows already there (scales, `NaN`s and infinities across
/// groups) gives Postgres's text, and pages after it keep it.
#[tokio::test]
async fn a_build_then_pages() {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, x numeric); \
         create index on public.src (g); \
         insert into public.src values (1, 1, 1.5), (2, 1, 2.000), (3, 2, 'NaN'), \
                                       (4, 2, 1), (5, 3, 'Infinity'), (6, 3, 0.1), \
                                       (7, 4, '-Infinity'), (8, 4, 'Infinity'), (9, 5, null)",
        &src_columns(),
        &[AGG],
        &["public.src"],
    )
    .await;
    run(
        &mut d,
        &[
            "select 1",
            "delete from public.src where id in (2, 3, 5, 8)",
            "insert into public.src values (2, 5, 0.001)",
        ],
        &[("agg", AGG_ROWS, AGG_EXPECTED)],
    )
    .await;
}

/// A corrupted scale count or special-value count is reported on its own
/// hidden column.
#[tokio::test]
async fn self_check_reports_a_wrong_scale_or_special_count() {
    let mut d = start_agg().await;
    step(
        &mut d,
        "insert into public.src values (1, 1, 1.5), (2, 1, 2.000), (3, 1, 'Infinity')",
    )
    .await;
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.agg'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    let hidden = format!("{DEFAULT_SCHEMA}.agg_{id}");
    for (corrupt, column, persisted, recomputed) in [
        (
            "__scales_1 = '{\"1\": 1, \"3\": 2}'",
            "__scales_1",
            "{\\\"1\\\": 1, \\\"3\\\": 2}",
            "{\\\"1\\\": 1, \\\"3\\\": 1}",
        ),
        ("__pinf_1 = 0", "__pinf_1", "0", "1"),
        ("__ninf_1 = 1", "__ninf_1", "1", "0"),
        ("__nan_1 = 3", "__nan_1", "3", "0"),
    ] {
        d.ctl
            .batch_execute(&format!(
                "begin; \
                 create temp table saved on commit drop as select * from {hidden}; \
                 update {hidden} set {corrupt} where g = 1; commit"
            ))
            .await
            .expect(corrupt);
        let found = self_check(&d, "agg").await;
        let wanted = format!(
            "agg_{id}.{column}\", persisted: Some(\"{persisted}\"), \
             recomputed: Some(\"{recomputed}\")"
        );
        assert!(
            found.iter().any(|f| f.contains(&wanted)),
            "{wanted} in {found:?}"
        );
        // Put the right count back.
        let right = match column {
            "__scales_1" => "'{\"1\": 1, \"3\": 1}'::jsonb".to_string(),
            _ => recomputed.to_string(),
        };
        d.ctl
            .batch_execute(&format!(
                "update {hidden} set {column} = {right} where g = 1"
            ))
            .await
            .expect("repair");
        assert_eq!(
            self_check(&d, "agg").await,
            Vec::<String>::new(),
            "{corrupt}"
        );
    }
}

/// A `SUM` and an `AVG` over a `numeric` column of a Trellis target (which
/// has no declared scale): the upstream's sums carry Postgres's scales, and
/// the downstream follows them as text.
#[tokio::test]
async fn a_chained_sum_over_a_targets_numeric_column() {
    let mut d = Driver::start(
        "create table public.src (id integer primary key, g integer, h integer, \
                                  x numeric(12, 3)); \
         create index on public.src (g, h)",
        &[
            ("id", int4()),
            ("g", int4()),
            ("h", int4()),
            ("x", ValueType::Numeric),
        ],
        &[
            "TRANSFORM up FROM public.src GROUP BY g, h WITHOUT SOURCE INDEX \
           SELECT g AS g, h AS h, SUM(x) AS s, AVG(x) AS a",
        ],
        &["public.src"],
    )
    .await;
    let columns: HashMap<String, ValueType> = [
        ("g", int4()),
        ("h", int4()),
        ("s", ValueType::Numeric),
        ("a", ValueType::Numeric),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect();
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM down FROM public.up GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, SUM(s) AS ss, AVG(a) AS aa, COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install the reader");
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    let rows = "select g, ss::text, aa::text, n from public.down";
    let expected = "select g, sum(s)::text, avg(a)::text, count(*) from ( \
                        select g, h, sum(x) as s, avg(x) as a from public.src group by g, h \
                    ) u group by g";
    let mut failures = Vec::new();
    for sql in [
        "insert into public.src values (1, 1, 1, 1.500), (2, 1, 2, 2.25), (3, 1, 2, 'NaN')",
        "delete from public.src where id = 3",
        "update public.src set x = 0.125 where id = 1",
        "delete from public.src where id = 2",
    ] {
        step(&mut d, sql).await;
        // The reader's page is the upstream's seam rows: one more drain.
        d.settle().await;
        check(&d, sql, &[("down", rows, expected)], &mut failures).await;
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

const JOIN_DDL: &str = "create table public.o (id integer primary key, k integer, x numeric); \
                        create table public.c (id integer primary key, k integer, region text); \
                        create index on public.o (k); create index on public.c (k)";

const INNER: &str = "TRANSFORM ij FROM public.o JOIN public.c ON o.k = c.k GROUP BY c.region \
                     WITHOUT SOURCE INDEX SELECT c.region AS region, SUM(o.x) AS total, \
                            AVG(o.x) AS mean, COUNT(o.x) AS cx, COUNT(*) AS n";

const LEFT: &str = "TRANSFORM lj FROM public.o LEFT JOIN public.c ON o.k = c.k \
                    GROUP BY c.region \
                    WITHOUT SOURCE INDEX SELECT c.region AS region, SUM(o.x) AS total, \
                           AVG(o.x) AS mean, COUNT(o.x) AS cx, COUNT(*) AS n";

const INNER_ROWS: &str = "select region, total::text, mean::text, cx, n from public.ij";
const INNER_EXPECTED: &str = "select c.region, sum(o.x)::text, avg(o.x)::text, count(o.x), \
                                     count(*) \
                              from public.o join public.c on o.k = c.k group by c.region";
const LEFT_ROWS: &str = "select region, total::text, mean::text, cx, n from public.lj";
const LEFT_EXPECTED: &str = "select c.region, sum(o.x)::text, avg(o.x)::text, count(o.x), \
                                    count(*) \
                             from public.o left join public.c on o.k = c.k group by c.region";

/// On a `JOIN`: scales multiplied by the match count, a key with
/// `-Infinity` and no match (its count times no match is nothing; in a
/// `LEFT JOIN` it lands in the `NULL` group), a match arriving for it and
/// leaving, and the largest scale emptying on one key while another holds a
/// smaller one.
#[tokio::test]
async fn join_groups_follow_scales_and_infinities_matched_or_not() {
    let mut d = Driver::start(
        JOIN_DDL,
        &[("id", int4()), ("k", int4()), ("x", ValueType::Numeric)],
        &[INNER, LEFT],
        &["public.o", "public.c"],
    )
    .await;
    run(
        &mut d,
        &[
            "insert into public.c values (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'b')",
            "insert into public.o values (1, 1, 1.5), (2, 1, 2.000), (3, 2, 0.25), \
                                         (4, 9, '-Infinity'), (5, 9, 7)",
            "delete from public.o where id = 2",
            "insert into public.c values (4, 9, 'b')",
            "insert into public.o values (6, 2, 'Infinity')",
            "delete from public.c where id = 4",
            "update public.o set x = 0.00001 where id = 6",
            "delete from public.c where id = 2",
            "insert into public.c values (2, 2, 'a')",
            "update public.o set k = 9 where id = 1",
            "delete from public.o where id in (4, 5)",
        ],
        &[
            ("ij", INNER_ROWS, INNER_EXPECTED),
            ("lj", LEFT_ROWS, LEFT_EXPECTED),
        ],
    )
    .await;
}

/// A `JOIN` built over rows already there agrees with Postgres as text,
/// and pages after it keep it.
#[tokio::test]
async fn a_join_build_then_pages() {
    let mut d = Driver::start(
        &format!(
            "{JOIN_DDL}; \
             insert into public.c values (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'b'); \
             insert into public.o values (1, 1, 1.5), (2, 1, 2.000), (3, 2, 'NaN'), \
                                         (4, 9, '-Infinity'), (5, 2, 0.1)"
        ),
        &[("id", int4()), ("k", int4()), ("x", ValueType::Numeric)],
        &[INNER, LEFT],
        &["public.o", "public.c"],
    )
    .await;
    run(
        &mut d,
        &[
            "select 1",
            "delete from public.o where id in (2, 3)",
            "insert into public.c values (4, 9, 'a')",
        ],
        &[
            ("ij", INNER_ROWS, INNER_EXPECTED),
            ("lj", LEFT_ROWS, LEFT_EXPECTED),
        ],
    )
    .await;
}

/// [`JOIN_DDL`] with `o.x` a `numeric(10,2)`, which declares its scale: the
/// statements specialise for it (strict stages, builds and products).
const DECLARED_JOIN_DDL: &str = "create table public.o (id integer primary key, k integer, x numeric(10,2)); \
     create table public.c (id integer primary key, k integer, region text); \
     create index on public.o (k); create index on public.c (k)";

/// A `JOIN` and an aggregate over a `numeric(10,2)` argument, built over rows
/// already there (a `NaN` among them), then pages, `NaN` in and out: as text,
/// Postgres's at every step. A page or build with a `NaN` can't be staged
/// strictly and is staged again the general way.
#[tokio::test]
async fn a_declared_scale_build_then_pages() {
    const AGG_O: &str = "TRANSFORM ao FROM public.o GROUP BY k WITHOUT SOURCE INDEX \
                         SELECT k AS k, SUM(x) AS total, AVG(x) AS mean, COUNT(x) AS cx";
    const AGG_O_ROWS: &str = "select k, total::text, mean::text, cx from public.ao";
    const AGG_O_EXPECTED: &str =
        "select k, sum(x)::text, avg(x)::text, count(x) from public.o group by k";
    let mut d = Driver::start(
        &format!(
            "{DECLARED_JOIN_DDL}; \
             insert into public.c values (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'b'); \
             insert into public.o values (1, 1, 1.5), (2, 1, 2), (3, 2, 'NaN'), \
                                         (4, 9, 7.25), (5, 2, 0.1)"
        ),
        &[("id", int4()), ("k", int4()), ("x", ValueType::Numeric)],
        &[INNER, LEFT, AGG_O],
        &["public.o", "public.c"],
    )
    .await;
    run(
        &mut d,
        &[
            "select 1",
            "insert into public.o values (6, 1, 3.33), (7, 2, null)",
            "delete from public.o where id = 3",
            "insert into public.c values (4, 9, 'a')",
            "update public.o set x = 'NaN' where id = 1",
            "update public.o set x = 0.01 where id = 1",
            "delete from public.c where id = 1",
            "update public.o set k = 9 where id in (2, 6)",
            "delete from public.o where id in (4, 5)",
        ],
        &[
            ("ij", INNER_ROWS, INNER_EXPECTED),
            ("lj", LEFT_ROWS, LEFT_EXPECTED),
            ("ao", AGG_O_ROWS, AGG_O_EXPECTED),
        ],
    )
    .await;
}

/// A declared scale is only a hint: a `JOIN` cell whose map counts another
/// scale (here written by hand, as a change of the column's type could leave
/// one behind) makes the strict product refuse, and the page's product runs
/// the general way, carrying the other scale into `T`.
#[tokio::test]
async fn a_map_off_its_declared_scale_is_multiplied_the_general_way() {
    let mut d = Driver::start(
        &format!(
            "{DECLARED_JOIN_DDL}; \
             insert into public.c values (1, 1, 'a'); \
             insert into public.o values (1, 1, 1.5), (2, 1, 2)"
        ),
        &[("id", int4()), ("k", int4()), ("x", ValueType::Numeric)],
        &[INNER],
        &["public.o", "public.c"],
    )
    .await;
    step(&mut d, "select 1").await;
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.ij'",
            &[],
        )
        .await
        .expect("id")
        .get(0);
    let (p, t) = (
        format!("{DEFAULT_SCHEMA}.join_{id}_p"),
        format!("{DEFAULT_SCHEMA}.join_{id}_t"),
    );
    d.ctl
        .batch_execute(&format!(
            "update {p} set __scales_1 = '{{\"2\": 1, \"5\": 1}}' where k = 1"
        ))
        .await
        .expect("a map off its declared scale");
    // A second match for key 1: `T` gains `P_old ⊗ ΔM`, read from the cell.
    step(&mut d, "insert into public.c values (2, 1, 'a')").await;
    let scales: String = d
        .ctl
        .query_one(
            &format!("select __scales_1::text from {t} where region = 'a'"),
            &[],
        )
        .await
        .expect("T's map")
        .get(0);
    // The build's `{"2": 2}`, plus the hand-written cell times its new
    // match. A strict product would have derived `{"2": 4}` from the counts.
    assert_eq!(scales, "{\"2\": 3, \"5\": 1}");
}
