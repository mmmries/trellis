//! Two-table 1-1 `JOIN` scenarios with no position of a star (ADR-0021,
//! epic #1392): each needs a fixture of its own, which `support/star.rs`'s
//! star doesn't lay out. A chained joined table fed by the target-mutation
//! seam, a collated text key, and the two tables keyed differently with
//! columns named like the page's aliases. Each drives the pipeline by hand
//! (`support/drain_driver.rs`) and checks the target against the live join
//! and `self_check` after every step.
//!
//! Every other interleaving of the drain on a 1-1 `JOIN` is written once
//! against "the joined table at `at`" and runs at the two-table shape and on
//! a star: `join_star_families.rs` (root writers),
//! `join_star_joined_families.rs` (joined-table writers),
//! `join_star_truncate.rs` (truncates) and `join_star_harness.rs` (the build
//! and plain changes).

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::{Driver, int4};
use trellis::defs::ValueType;

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed batch.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// Runs `sql` on `table` with its capture triggers disabled, in one
/// transaction: a change no entry will ever hear about. (Capture triggers
/// are `ENABLE ALWAYS`, so `session_replication_role = replica` doesn't skip
/// them.)
async fn write_uncaptured_on(d: &Driver, table: &str, sql: &str) {
    let toggle = |action: &str| {
        format!(
            "do $$ declare t text; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = '{table}'::regclass and not tgisinternal loop \
                     execute format('alter table {table} {action} trigger %I', t); \
                 end loop; \
             end $$;"
        )
    };
    d.ctl
        .batch_execute(&format!(
            "begin; {} {sql}; {} commit;",
            toggle("disable"),
            toggle("enable always")
        ))
        .await
        .expect("write past capture");
}

/// A `b` side that is another transform's 1-1 target, joined on its primary
/// key: the upstream's writes reach the join through the target-mutation
/// seam's image feed as Applies with images, which the `b` page applies like
/// captured ones (an update, a delete, a re-insert), and an `a` move after
/// them reads the new `M`. A Re-derive of the upstream's rows (a seam
/// `Recompute`) reads the target live.
#[tokio::test]
async fn a_chained_b_side_applies_the_seams_images() {
    let mut d = Driver::start(
        "create table public.a (id integer primary key, k integer, x integer); \
         create table public.bsrc (id integer primary key, w integer); \
         insert into public.a values (1, 10, 1), (2, 10, 2), (3, 20, 3); \
         insert into public.bsrc values (10, 100), (20, 200)",
        &[("w", int4())],
        &["TRANSFORM tb FROM public.bsrc SELECT w AS w"],
        &["public.bsrc"],
    )
    .await;
    // The join can only be defined over a live upstream.
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM jc FROM public.a LEFT JOIN public.tb ON a.k = tb.id \
         SELECT a.x + COALESCE(tb.w, 0) AS total",
        &std::collections::HashMap::from([("total".to_string(), int4())]),
        "public",
    )
    .await
    .expect("define the join over the 1-1 target");
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, "public.a")
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(
        &mut d.ctl,
        trellis::config::DEFAULT_SCHEMA,
        &spec,
        None,
    )
    .await
    .expect("capture a")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    for sql in [
        "update public.bsrc set w = 111 where id = 10",
        "delete from public.bsrc where id = 20",
        "insert into public.bsrc values (20, 222)",
        "update public.a set k = 20 where id = 1",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        let actual = d.rows("select id, total from public.jc order by id").await;
        let expected = d
            .rows(
                "select a.id, a.x + coalesce(b.w, 0) from public.a a \
                 left join public.bsrc b on a.k = b.id order by a.id",
            )
            .await;
        assert_eq!(actual, expected, "after {sql}");
        let status = d
            .rows("select status::text from transform_definitions order by id")
            .await;
        assert_eq!(status, ["(live)", "(live)"], "after {sql}");
    }
    d.stage_recomputes("public.tb", &["10", "20"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive tb").await;
    d.settle().await;
    let actual = d.rows("select id, total from public.jc order by id").await;
    let expected = d
        .rows(
            "select a.id, a.x + coalesce(b.w, 0) from public.a a \
             left join public.bsrc b on a.k = b.id order by a.id",
        )
        .await;
    assert_eq!(actual, expected, "after a Re-derive of tb");
    let status = d
        .rows("select status::text from transform_definitions order by id")
        .await;
    assert_eq!(status, ["(live)", "(live)"], "after a Re-derive of tb");
}

/// A text join key with a collation: `K`, the held-key checks and the
/// fan-out cast each side's key to `J`'s type and collation before comparing
/// its text.
#[tokio::test]
async fn a_collated_text_key_applies_both_sides() {
    let mut d = Driver::start(
        "create table public.c (id integer primary key, k text collate \"C\", x integer); \
         create table public.e (id integer primary key, k text collate \"C\", w integer); \
         create unique index on public.e (k); \
         insert into public.c values (1, 'p', 1), (2, 'p', 2), (3, 'q', 3); \
         insert into public.e values (1, 'p', 100), (2, 'q', 200)",
        &[("id", int4()), ("k", ValueType::Text), ("x", int4())],
        &[
            "TRANSFORM ct FROM public.c LEFT JOIN public.e ON c.k = e.k \
           SELECT c.x + COALESCE(e.w, 0) AS total",
        ],
        &["public.c", "public.e"],
    )
    .await;
    for sql in [
        "update public.e set w = 111 where id = 1",
        "update public.c set k = 'q' where id = 1",
        "update public.e set k = 'r' where id = 2",
        "update public.e set k = 'q', w = 150 where id = 1",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        let actual = d.rows("select id, total from public.ct order by id").await;
        let expected = d
            .rows(
                "select c.id, c.x + coalesce(e.w, 0) from public.c c \
                 left join public.e e on c.k = e.k order by c.id",
            )
            .await;
        assert_eq!(actual, expected, "after {sql}");
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.ct")
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "after {sql}: {diverging:?}");
    }
}

/// The two sides keyed differently (`a` by text, `b` by `bigint`), with
/// columns named like the aliases the page's statements give their
/// relations (`r`, `s`, `b`, `m`, `l`, `doc`, `img`, `k0`). A page whose
/// first change of the target is a `b` change still writes the target by
/// `a`'s key, in a `b`-only page, a mixed page and a `b` key move, on both
/// join kinds. Then Re-derives of either side, alone and in one page, repair
/// changes capture never saw.
#[tokio::test]
async fn sides_keyed_differently_with_alias_named_columns_apply() {
    // `Driver::rows` reads its rows as `r`, so the checks below name their
    // columns apart from it.
    let mut d = Driver::start(
        "create table public.o (order_no text primary key, s integer, r integer, l text); \
         create table public.cu (cust_id bigint primary key, m integer, b integer, doc text, \
             img text, k0 integer); \
         create unique index on public.cu (m); \
         insert into public.o values ('A1', 1, 1, 'x'), ('A2', 1, 2, 'y'), ('A3', 2, 3, 'z'), \
             ('A4', 3, 4, 'w'); \
         insert into public.cu values (10, 1, 100, 'd1', 'i1', 7), (20, 2, 200, 'd2', 'i2', 8)",
        &[
            ("order_no", ValueType::Text),
            ("s", int4()),
            ("r", int4()),
            ("l", ValueType::Text),
        ],
        &[
            "TRANSFORM ol FROM public.o LEFT JOIN public.cu ON o.s = cu.m \
             SELECT o.r + COALESCE(cu.b, 0) AS s, cu.doc AS b, cu.img AS r, cu.k0 AS m, o.l AS l",
            "TRANSFORM oi FROM public.o INNER JOIN public.cu ON o.s = cu.m \
             SELECT o.r + COALESCE(cu.b, 0) AS s, cu.doc AS b, cu.img AS r, cu.k0 AS m, o.l AS l",
        ],
        &["public.o", "public.cu"],
    )
    .await;
    for sql in [
        "update public.cu set b = 111, doc = 'd1b' where cust_id = 10",
        "update public.o set r = 5 where order_no = 'A1'; \
         update public.cu set b = 222, img = 'i2b' where cust_id = 20",
        "update public.cu set m = 3, k0 = 9 where cust_id = 20; \
         update public.o set s = 2 where order_no = 'A2'",
        "insert into public.cu values (30, 2, 300, 'd3', 'i3', 1)",
        "delete from public.cu where cust_id = 10",
    ] {
        step(&mut d, sql).await;
        d.settle().await;
        assert_ordered_join(&d, sql).await;
    }
    for (table, sql, keys) in [
        (
            "public.cu",
            "update public.cu set b = 333, doc = 'd2c', k0 = 4 where cust_id = 20",
            &["20"][..],
        ),
        (
            "public.cu",
            "update public.cu set m = 1, img = 'i3b' where cust_id = 30",
            &["30"][..],
        ),
        (
            "public.o",
            "update public.o set s = 1, r = 6, l = 'v' where order_no = 'A4'",
            &["A4"][..],
        ),
    ] {
        write_uncaptured_on(&d, table, sql).await;
        d.stage_recomputes(table, keys).await;
        let batch = d.seal().await;
        d.drain(batch, "rederive").await;
        d.settle().await;
        assert_ordered_join(&d, sql).await;
    }
    // Both sides' Re-derives in one page.
    write_uncaptured_on(
        &d,
        "public.cu",
        "update public.cu set b = 444 where cust_id = 30",
    )
    .await;
    write_uncaptured_on(
        &d,
        "public.o",
        "update public.o set r = 7 where order_no = 'A1'",
    )
    .await;
    d.stage_recomputes("public.cu", &["20", "30"]).await;
    d.stage_recomputes("public.o", &["A1", "A3"]).await;
    let batch = d.seal().await;
    d.drain(batch, "rederive both").await;
    d.settle().await;
    assert_ordered_join(&d, "a mixed Re-derive page").await;
}

/// [`sides_keyed_differently_with_alias_named_columns_apply`]'s check: each
/// target equals its live join and self_check finds nothing, after `step`.
async fn assert_ordered_join(d: &Driver, step: &str) {
    for (target, kind) in [("public.ol", "left"), ("public.oi", "inner")] {
        let actual = d
            .rows(&format!(
                "select order_no, s as c1, b as c2, r as c3, m as c4, l as c5 \
                 from {target} order by order_no"
            ))
            .await;
        let expected = d
            .rows(&format!(
                "select o.order_no, o.r + coalesce(cu.b, 0) as c1, cu.doc as c2, cu.img as c3, \
                 cu.k0 as c4, o.l as c5 \
                 from public.o o {kind} join public.cu cu on o.s = cu.m order by o.order_no"
            ))
            .await;
        assert_eq!(actual, expected, "{target} after {step}");
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), target)
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "{target} after {step}: {diverging:?}");
    }
}
