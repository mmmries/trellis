//! A join aggregate with a `WHERE` (#1991 step G, ADR-0019 "`WHERE`"): each
//! top-level `AND` conjunct reads one table and filters that table's rows
//! before they reach its partials, `a`'s in `P` and `b`'s in `M`; `J`, `T`,
//! the product and the locks are unchanged. `self_check` compares the target
//! with the definition's own `JOIN … GROUP BY … WHERE`, as written.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence. Each compares the target with the direct
//! query after every step, and asserts that nothing is held: a field's
//! division by a predicate column is never evaluated on a row the `WHERE`
//! drops.

use crate::support::drain_driver;

use std::time::{Duration, Instant};

use drain_driver::{Driver, int4};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::staging::partition_correction::{self, Options};
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};

/// `a.s` and `b.f` are read only by the `WHERE` and by a division that is
/// safe only on the rows it keeps.
const TABLES: &str = "create table public.a (id integer primary key, ga integer, k integer, \
                          x numeric, s integer); \
                      create index on public.a (k);";
const B_PLAIN: &str = "create table public.b (id integer primary key, k integer, gb integer, \
                           y numeric, f integer); \
                       create index on public.b (k);";

/// `b` partitioned by `LIST` on its join key `k` (ADR-0017: `M`'s cells hold
/// the partition key), four leaves of four keys each.
const B_PARTITIONED: &str = "create table public.b (id integer not null, k integer not null, \
                                 gb integer, y numeric, f integer, primary key (k, id)) \
                             partition by list (k); \
                             create table public.b_p0 partition of public.b for values in (0, 1, 2, 3); \
                             create table public.b_p1 partition of public.b for values in (4, 5, 6, 7); \
                             create table public.b_p2 partition of public.b for values in (8, 9, 10, 11); \
                             create table public.b_p3 partition of public.b for values in (12, 13, 14, 15);";

/// Rows on both sides passing and failing their conjuncts (`0`, negative
/// and `NULL` filter values), matched and unmatched keys, a `NULL` join key
/// on each side, and a key all of whose `b` rows fail.
const SEED: &str = "insert into public.a values \
         (1, 1, 1, 10, 1), (2, 1, 1, 20, 0), (3, 2, 2, 30, 2), (4, 2, 3, 40, null), \
         (5, 3, 4, 50, 5), (6, 3, null, 60, 1), (7, 1, 9, 70, 3), (8, 2, 2, 80, -1); \
     insert into public.b values \
         (1, 1, 10, 100, 1), (2, 1, 20, 200, 0), (3, 2, 10, 300, 3), (4, 3, 30, 400, 2), \
         (5, 4, 10, 500, null), (6, 4, 20, 600, -2), (7, 2, 20, 700, 1), (8, 5, 10, 800, 4);";

/// An inner join with a conjunct on each side.
const INNER: &str = "TRANSFORM i FROM public.a JOIN public.b ON a.k = b.k \
     GROUP BY a.ga, b.gb WITHOUT SOURCE INDEX \
     SELECT a.ga AS ga, b.gb AS gb, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y / b.f) AS ratio \
     WHERE a.s > 0 AND b.f > 0";

/// A left join with conjuncts on `a` only (a `b` conjunct is refused).
const LEFT: &str = "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k \
     GROUP BY b.gb WITHOUT SOURCE INDEX \
     SELECT b.gb AS gb, COUNT(*) AS n, SUM(a.x / a.s) AS ratio, COUNT(b.y) AS cy \
     WHERE a.s <> 0 AND a.x > 0";

/// Each target, its rows and the direct query's, both ordered.
const ORACLES: [(&str, &str, &str); 2] = [
    (
        "i",
        "select ga, gb, n, sx, ratio from public.i order by ga, gb",
        "select a.ga, b.gb, count(*), sum(a.x), sum(b.y / b.f) \
         from public.a join public.b on a.k = b.k \
         where a.s > 0 and b.f > 0 group by a.ga, b.gb order by a.ga, b.gb",
    ),
    (
        "l",
        "select gb, n, ratio, cy from public.l order by gb",
        "select b.gb, count(*), sum(a.x / a.s), count(b.y) \
         from public.a left join public.b on a.k = b.k \
         where a.s <> 0 and a.x > 0 group by b.gb order by b.gb",
    ),
];

/// Every crossing of each side's conjuncts, the join key and a group moving
/// with them, and inserts and deletes of passing and failing rows.
const CROSSINGS: &[&str] = &[
    // `a`: only the predicate's column changes, out, in, to `NULL` and back
    "update public.a set s = 0 where id = 1",
    "update public.a set s = 4 where id = 2",
    "update public.a set s = null where id = 3",
    "update public.a set s = 2 where id = 4",
    // `a`: a join-key change that also crosses out, and one that crosses in
    "update public.a set k = 4, s = 0 where id = 3",
    "update public.a set k = 5, s = 1 where id = 8",
    // `b`: only the predicate's column changes, out and in
    "update public.b set f = 0 where id = 1",
    "update public.b set f = 5 where id = 2",
    // `b`: every row of key 4 now passes, then all fail again
    "update public.b set f = 1 where id in (5, 6)",
    "update public.b set f = -1 where k = 4",
    // `b`: a group change that crosses in, a join-key change that crosses out
    "update public.b set gb = 30, f = 2 where id = 6",
    "update public.b set k = 9, f = 0 where id = 3",
    // inserts and deletes, passing and failing, on both sides
    "insert into public.a values (20, 1, 2, 5, 1), (21, 2, 2, 6, 0), (22, null, 1, 7, 2)",
    "insert into public.b values (20, 2, null, 9, 3), (21, 9, 10, 1, 0)",
    "delete from public.a where id in (20, 5)",
    "delete from public.b where id in (20, 8)",
];

/// Both tables' columns, as the definitions read them.
fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", int4()),
        ("ga", int4()),
        ("k", int4()),
        ("x", ValueType::Numeric),
        ("s", int4()),
    ]
}

/// `definitions` live over `a` and `b` (`b_ddl`), built over [`SEED`].
async fn start(b_ddl: &str, definitions: &[&str]) -> Driver {
    Driver::start(
        &format!("{TABLES} {b_ddl} {SEED}"),
        &columns(),
        definitions,
        &["public.a", "public.b"],
    )
    .await
}

/// A capture pass: records a partitioned table's tree, and queues the
/// corrections its changes need.
async fn pass(d: &mut Driver) {
    let desired = trellis::defs::tables_to_capture(d.pool())
        .await
        .expect("tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        &mut d.ctl,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

/// Pauses every definition, runs `sql` while they are paused, and resumes
/// `targets`: each rebuilds from its tables.
async fn rebuild(d: &mut Driver, targets: &[&str], sql: &str) {
    d.ctl
        .batch_execute("update transform_definitions set status = 'paused'")
        .await
        .expect("pause");
    d.write(sql).await;
    for target in targets {
        trellis::staging::quarantine::resume_transform(d.pool(), target)
            .await
            .expect("resume");
    }
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the builds");
    trellis::staging::build::settle_builds(d.pool()).await;
    assert_eq!(
        d.rows("select distinct status from transform_definitions")
            .await,
        ["(live)"]
    );
}

/// The targets named in `names` against their direct queries, nothing held.
async fn assert_oracles(d: &mut Driver, names: &[&str], context: &str) {
    d.settle().await;
    for (name, actual, expected) in ORACLES {
        if !names.contains(&name) {
            continue;
        }
        assert_eq!(
            d.rows(actual).await,
            d.rows(expected).await,
            "{context}: {name} (left) differs from its query (right)"
        );
    }
    assert_eq!(
        d.rows("select count(*) from poison").await,
        ["(0)"],
        "{context}: nothing is held"
    );
}

/// Every page of `self_check` of `target`: the outcomes that aren't
/// `Converged`.
async fn divergences(d: &Driver, target: &str) -> Vec<String> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            target,
            SelfCheckScope { after, limit: 3 },
            SelfCheckMode::Standard,
            Duration::from_secs(30),
        )
        .await
        .expect("a page of the check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            other => found.push(format!("{other:?}")),
        }
        after = page.next_after;
        if after.is_none() {
            return found;
        }
    }
}

/// One comparison of `target` with no re-check, whatever the ring holds:
/// its divergences, and whether it vouched for every key.
async fn compare(d: &Driver, target: &str) -> (Vec<Divergence>, bool) {
    let mut after = None;
    let (mut found, mut certain) = (Vec::new(), true);
    loop {
        let page = compare_page(d.pool(), target, SelfCheckScope { after, limit: 3 })
            .await
            .expect("a comparison");
        found.extend(page.divergences);
        certain &= page.uncertain_keys.is_empty() && !page.uncertain_all;
        after = page.next_after;
        if after.is_none() {
            return (found, certain);
        }
    }
}

/// Definition `target`'s hidden table `part`, qualified.
async fn hidden(d: &Driver, target: &str, part: &str) -> String {
    let id = d
        .rows(&format!(
            "select id from transform_definitions where target_table = 'public.{target}'"
        ))
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    format!("{DEFAULT_SCHEMA}.join_{id}_{part}")
}

/// `i`'s `P` holds only `a`'s passing rows, its `M` only `b`'s, and its
/// `J.matches` counts `M`'s rows (ADR-0019's invariants over the passing
/// rows).
async fn assert_partials(d: &Driver, context: &str) {
    let checks = [
        (
            format!(
                "select ga, k, __n from {} where __n <> 0 order by ga, k",
                hidden(d, "i", "p").await
            ),
            "select ga, k, count(*) from public.a where s > 0 \
             group by ga, k order by ga, k",
        ),
        (
            format!(
                "select k, gb, __n from {} where __n <> 0 order by k, gb",
                hidden(d, "i", "m").await
            ),
            "select k, gb, count(*) from public.b where f > 0 and k is not null \
             group by k, gb order by k, gb",
        ),
        (
            format!(
                "select k, __matches from {} where __matches <> 0 order by k",
                hidden(d, "i", "j").await
            ),
            "select k, count(*) from public.b where f > 0 and k is not null \
             group by k order by k",
        ),
    ];
    for (have, want) in checks {
        assert_eq!(d.rows(&have).await, d.rows(want).await, "{context}: {have}");
    }
}

/// Built over mixed rows, then every crossing on each side: both targets
/// equal their queries at each step, `P`, `M` and `J` hold only passing
/// rows, and `self_check`, rendering the `WHERE` as written, finds nothing.
#[tokio::test]
async fn each_side_filters_its_own_rows_through_build_and_every_crossing() {
    let mut d = start(B_PLAIN, &[INNER, LEFT]).await;
    assert_oracles(&mut d, &["i", "l"], "built").await;
    assert_partials(&d, "built").await;
    for sql in CROSSINGS {
        d.step(sql).await;
        assert_oracles(&mut d, &["i", "l"], sql).await;
    }
    assert_partials(&d, "crossed").await;
    for target in ["i", "l"] {
        assert_eq!(
            divergences(&d, target).await,
            Vec::<String>::new(),
            "{target}"
        );
    }
}

/// `TRUNCATE b` refills a `LEFT` target from `P`, which holds only `a`'s
/// passing rows; an inner one empties. A `RESUME` rebuilds both filtered.
#[tokio::test]
async fn truncate_and_resume_keep_the_filter() {
    let mut d = start(B_PLAIN, &[INNER, LEFT]).await;
    d.step("truncate public.b").await;
    assert_oracles(&mut d, &["i", "l"], "truncate b").await;
    d.step("insert into public.b values (1, 1, 10, 100, 1), (2, 2, 20, 200, 0), (3, 3, 10, 7, 2)")
        .await;
    assert_oracles(&mut d, &["i", "l"], "refilled").await;
    rebuild(
        &mut d,
        &["i", "l"],
        "update public.a set s = 0 where id = 7; update public.b set f = 9 where id = 2",
    )
    .await;
    assert_oracles(&mut d, &["i", "l"], "resumed").await;
    for target in ["i", "l"] {
        assert_eq!(
            divergences(&d, target).await,
            Vec::<String>::new(),
            "{target}"
        );
    }
}

/// A comparison with changes still in the ring, crossing both sides'
/// conjuncts both ways: the unapplied images are weighted through the same
/// `WHERE`, so it finds nothing before or after they drain.
#[tokio::test]
async fn self_check_weighs_unapplied_crossings_through_the_where() {
    let mut d = start(B_PLAIN, &[INNER, LEFT]).await;
    d.write(
        "update public.a set s = 0 where id = 1; update public.a set s = 7 where id = 2; \
         update public.b set f = 0 where id = 3; update public.b set f = 2 where id = 2",
    )
    .await;
    let sealed = d.seal().await;
    d.write("update public.a set s = -1 where id = 3; update public.b set f = 1 where id = 6")
        .await;
    for target in ["i", "l"] {
        assert_eq!(
            compare(&d, target).await,
            (vec![], true),
            "{target}, undrained"
        );
    }
    d.drain(sealed, "late").await;
    for target in ["i", "l"] {
        assert_eq!(
            compare(&d, target).await,
            (vec![], true),
            "{target}, half drained"
        );
    }
    assert_oracles(&mut d, &["i", "l"], "drained").await;
    for target in ["i", "l"] {
        assert_eq!(
            divergences(&d, target).await,
            Vec::<String>::new(),
            "{target}"
        );
    }
}

/// A `b`-side filter on a partitioned joined side: the per-leaf build, the
/// crossings, and a correction after a leaf holding passing and failing rows
/// is detached each read `M` through `b`'s conjunct.
#[tokio::test]
async fn a_joined_side_filter_holds_on_a_partitioned_joined_table() {
    // Defined over empty tables, so capture records `b`'s tree first.
    let mut d = Driver::start(
        &format!("{TABLES} {B_PARTITIONED}"),
        &columns(),
        &[INNER],
        &["public.a", "public.b"],
    )
    .await;
    pass(&mut d).await;
    d.step(SEED).await;
    assert_oracles(&mut d, &["i"], "seeded").await;
    // A rebuild reads each leaf through the filter.
    rebuild(&mut d, &["i"], "update public.b set f = 3 where id = 5").await;
    assert_oracles(&mut d, &["i"], "rebuilt").await;
    assert_partials(&d, "rebuilt").await;
    for sql in CROSSINGS {
        d.step(sql).await;
        assert_oracles(&mut d, &["i"], sql).await;
    }
    // Leaf `b_p0` (keys 0-3) holds passing and failing rows.
    assert_eq!(
        d.rows("select f > 0 from public.b_p0 group by 1 order by 1")
            .await,
        ["(f)", "(t)"],
    );
    d.ctl
        .batch_execute("alter table public.b detach partition public.b_p0")
        .await
        .expect("detach");
    pass(&mut d).await;
    let corrected = partition_correction::run_pending(d.pool(), &Options::default())
        .await
        .expect("run the pending corrections");
    assert_eq!(corrected.completed, 1);
    assert_oracles(&mut d, &["i"], "detached").await;
    assert_partials(&d, "detached").await;
    assert_eq!(divergences(&d, "i").await, Vec::<String>::new());
}
