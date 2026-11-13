//! A 1-1 `JOIN` across a schema change, where the scenario needs a fixture of
//! its own and so has no `Shape` of `support/star.rs` (ADR-0021, #1403,
//! #1858): copies of one column that keep their types through a `RESUME`,
//! a chained 1-1 `a` side, a chained 1-1 `b` side, and an aggregate's
//! target as `a` keyed by a nullable group column, each under `LEFT` and
//! `INNER`. The schema changes of the tables themselves are
//! `join_star_schema_changes.rs`'s, at the two-table `JOIN` and position 2 of
//! a star.
//!
//! Every test drives the pipeline and the staging worker's pass by hand
//! through `support/drain_driver.rs`; nothing polls or sleeps.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/schema_change.rs"]
mod schema_change;

use std::collections::HashMap;

use drain_driver::{Driver, int4};
use schema_change::{
    column_type, pass, paused_for, resume, resume_refused, status, trellis, write,
};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    Left,
    Inner,
}

const KINDS: [Kind; 2] = [Kind::Left, Kind::Inner];

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Left => "jl",
            Kind::Inner => "ji",
        }
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT",
            Kind::Inner => "INNER",
        }
    }

    fn target(self) -> String {
        format!("public.{}", self.name())
    }
}

/// Writes `sql` and drains it, and everything it leads to.
async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
    d.settle().await;
}

async fn definition_id(d: &Driver, target: &str) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// The type of `column` in definition `target`'s hidden table `part`
/// (`r`, `m` or `j`).
async fn hidden_type(d: &Driver, target: &str, part: &str, column: &str) -> String {
    let id = definition_id(d, target).await;
    column_type(d, &format!("{DEFAULT_SCHEMA}.join1_{id}_{part}"), column).await
}

/// A field that names one column is a copy of that column, at the column's
/// own type with no modifier, whatever define's inference would call it: an
/// enum, a `varchar(5)` and a `numeric(6,2)`. A pass and a `PAUSE`/`RESUME`
/// with nothing changed re-type none of them (the resume goes straight to its
/// rebuild), and the target keeps the enum.
#[tokio::test]
async fn a_copy_of_one_column_keeps_its_type_through_a_resume() {
    for kind in KINDS {
        let mut d = Driver::start(
            "create type public.mood as enum ('sad', 'ok', 'happy'); \
             create table public.a (id integer primary key, k integer, \
                 amount numeric(6,2)); \
             create table public.b (bid integer primary key, k integer unique, \
                 m public.mood, tag varchar(5)); \
             create index on public.a (k); \
             insert into public.a select i, i % 3, i * 1.25 from generate_series(1, 6) i; \
             insert into public.b values (1, 1, 'ok', 'one'), (2, 2, 'happy', 'two')",
            &[("k", int4()), ("amount", ValueType::Numeric)],
            &[&format!(
                "TRANSFORM {} FROM public.a {} JOIN public.b ON a.k = b.k \
                 SELECT b.m AS m, b.tag AS tag, a.amount AS amount",
                kind.name(),
                kind.word()
            )],
            &["public.a", "public.b"],
        )
        .await;
        let target = kind.target();
        pass(&mut d).await;
        assert_eq!(status(&d, &target).await, "live", "{kind:?}");
        let trellis = trellis(&d).await;
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", kind.name()))
            .await
            .expect("pause");
        resume(&d, kind.name()).await;
        assert_eq!(
            status(&d, &target).await,
            "waiting_to_backfill",
            "{kind:?}: nothing to re-type"
        );
        for (column, ty) in [
            ("m", "mood"),
            ("tag", "character varying"),
            ("amount", "numeric"),
        ] {
            assert_eq!(column_type(&d, &target, column).await, ty, "{kind:?}");
        }
        pass(&mut d).await;
        trellis::staging::build::settle_builds_with(d.pool(), 3).await;
        d.settle().await;
        assert_eq!(status(&d, &target).await, "live", "{kind:?}");
        assert_eq!(
            d.rows(&format!(
                "select t.id::text, t.m::text, t.tag::text, t.amount::text \
                 from {target} t order by t.id"
            ))
            .await,
            d.rows(&format!(
                "select a.id::text, b.m::text, b.tag::text, a.amount::text from public.a a \
                 {} join public.b b on a.k = b.k order by a.id",
                kind.word()
            ))
            .await,
            "{kind:?}"
        );
    }
}

// ------------------------------------------------ a chained 1-1 side as `a`

/// `ta` (a 1-1 target of `asrc`) as `a`, joined to the plain `b`. A widening
/// of `asrc.x` outgrows `ta.x`, so `ta` pauses; its `RESUME` re-types `ta.x`,
/// which outgrows the join's copy of it, `x`: the join is paused with `ta`'s
/// resume as its cause, and resumes on its own once `ta` is live, re-typing
/// `x` and rebuilding from `ta`.
#[tokio::test]
async fn a_widening_under_a_chained_a_side_follows_its_upstream_resume() {
    for kind in KINDS {
        let mut d = Driver::start(
            "create table public.asrc (id integer primary key, k integer, x integer); \
             create table public.b (bid integer primary key, k integer, w integer); \
             create unique index on public.b (k); \
             insert into public.asrc select i, (i % 4) * 10, i from generate_series(1, 10) i; \
             insert into public.b select i, i * 10, i * 100 from generate_series(1, 2) i",
            &[("k", int4()), ("x", int4())],
            &["TRANSFORM ta FROM public.asrc SELECT k AS k, x AS x"],
            &["public.asrc", "public.b"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.ta {} JOIN public.b ON ta.k = b.k \
                 SELECT ta.x AS x, b.w AS w",
                kind.word()
            ),
            &HashMap::from([("x".to_string(), int4()), ("w".to_string(), int4())]),
            "public",
        )
        .await
        .expect("define the join over ta");
        pass(&mut d).await;
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        chained_exact(&d, kind).await;

        write(&d, "alter table public.asrc alter column x type bigint").await;
        pass(&mut d).await;
        assert_eq!(status(&d, "public.ta").await, "paused");
        resume(&d, "ta").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(column_type(&d, "public.ta", "x").await, "bigint");
        assert_eq!(column_type(&d, "public.jn", "x").await, "bigint");
        let statuses = d
            .rows("select target_table, status::text from transform_definitions order by id")
            .await;
        assert_eq!(
            statuses,
            ["(public.ta,live)", "(public.jn,live)"],
            "{kind:?}"
        );
        chained_exact(&d, kind).await;
        step(&mut d, "update public.asrc set x = 3000000000 where id = 1").await;
        chained_exact(&d, kind).await;
    }
}

/// `jn` over `ta` against the join of `asrc` and `b`, and `self_check` clean.
async fn chained_exact(d: &Driver, kind: Kind) {
    assert_eq!(
        d.rows("select t.id::text, t.x::text, t.w::text from public.jn t order by t.id")
            .await,
        d.rows(&format!(
            "select a.id::text, a.x::text, b.w::text from public.asrc a \
             {} join public.b b on a.k = b.k order by a.id",
            kind.word()
        ))
        .await,
        "{kind:?}"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
}

// ------------------------------------------------ a chained 1-1 side as `b`

/// `tb` (a 1-1 target of `bsrc`) as `b`, joined on its primary key. A
/// widening of `bsrc.w` outgrows `tb.w`, so `tb` pauses; its `RESUME` re-types
/// `tb.w`, which outgrows the join's copies of it (`M.w` and the passthrough
/// `w`), read from `tb` alone: the join is paused with `tb`'s resume as its
/// cause and resumes on its own once `tb` is live. So is one with a field over
/// both tables (`a.r + tb.w`), whichever table's pass runs first: the pass of
/// `a` leaves it to the pass of `tb`, whose input widened (#1539). It rebuilds
/// equal to the join, and a value past `integer` lands.
#[tokio::test]
async fn a_widening_under_a_chained_b_side_follows_its_upstream_resume() {
    for kind in KINDS {
        for (select, oracle) in [
            ("tb.w AS w", "b.w"),
            ("a.r + COALESCE(tb.w, 0) AS w", "a.r + coalesce(b.w, 0)"),
        ] {
            let mut d = Driver::start(
                "create table public.a (id integer primary key, bid integer, r integer); \
                 create table public.bsrc (bid integer primary key, w integer); \
                 create index on public.a (bid); \
                 insert into public.a select i, i % 4, i from generate_series(1, 10) i; \
                 insert into public.bsrc select i, i * 100 from generate_series(1, 3) i",
                &[("w", int4())],
                &["TRANSFORM tb FROM public.bsrc SELECT w AS w"],
                &["public.a", "public.bsrc"],
            )
            .await;
            trellis::defs::install_definition(
                d.pool(),
                &format!(
                    "TRANSFORM jn FROM public.a {} JOIN public.tb ON a.bid = tb.bid \
                     SELECT {select}",
                    kind.word()
                ),
                &HashMap::from([("w".to_string(), int4())]),
                "public",
            )
            .await
            .expect("define the join over tb");
            pass(&mut d).await;
            trellis::intake::harness::settle_builds(d.pool()).await;
            d.settle().await;
            let actual = "select t.id::text, t.w::text from public.jn t order by t.id";
            let expected = format!(
                "select a.id::text, ({oracle})::text from public.a a {} join public.bsrc b \
                 on a.bid = b.bid order by a.id",
                kind.word()
            );
            assert_eq!(d.rows(actual).await, d.rows(&expected).await, "{kind:?}");

            write(&d, "alter table public.bsrc alter column w type bigint").await;
            pass(&mut d).await;
            assert_eq!(status(&d, "public.tb").await, "paused");
            assert_eq!(status(&d, "public.jn").await, "live");
            resume(&d, "tb").await;
            for _ in 0..3 {
                pass(&mut d).await;
                trellis::staging::build::settle_builds_with(d.pool(), 3).await;
                d.settle().await;
            }
            assert_eq!(status(&d, "public.tb").await, "live", "{kind:?} {select}");
            assert_eq!(status(&d, "public.jn").await, "live", "{kind:?} {select}");
            assert_eq!(column_type(&d, "public.jn", "w").await, "bigint");
            assert_eq!(hidden_type(&d, "public.jn", "m", "w").await, "bigint");
            step(
                &mut d,
                "update public.bsrc set w = 3000000000 where bid = 1",
            )
            .await;
            assert_eq!(
                d.rows(actual).await,
                d.rows(&expected).await,
                "{kind:?} {select}"
            );
            let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
                .await
                .expect("self_check");
            assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
        }
    }
}

// ------------------------------- an aggregate's target as `a`, keyed by NULL

/// `agg` (counts per nullable `g`, so a group is keyed `NULL`) as `a`, joined
/// on its key to `b.k`. A widening of `src.g` pauses `agg`; its resume
/// re-types `agg.g`, which is the join's `a` key and `ON` column: the pair
/// no longer matches, so the join pauses naming `public.agg` and `g`, and its
/// `RESUME` refuses until `b.k` widens too. Then it rebuilds, the `NULL`-keyed
/// row among the rest.
#[tokio::test]
async fn a_widened_key_of_an_aggregate_a_side_keeps_its_null_keyed_rows() {
    for kind in KINDS {
        let mut d = Driver::start(
            "create table public.src (id integer primary key, g integer, v integer); \
             create table public.b (id integer primary key, k integer, w integer); \
             create unique index on public.b (k); \
             insert into public.src values \
                 (1, 1, 10), (2, null, 20), (3, 2, 5), (4, null, 7), (5, 3, 1); \
             insert into public.b values (1, 1, 100), (2, 2, 200), (3, null, 300)",
            &[("g", int4()), ("v", int4())],
            &[
                "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
               SELECT count(*) AS n",
            ],
            &["public.src"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.agg {} JOIN public.b ON agg.g = b.k \
                 SELECT agg.n + COALESCE(b.w, 0) AS total",
                kind.word()
            ),
            &HashMap::from([("total".to_string(), ValueType::Integer(IntWidth::Int8))]),
            "public",
        )
        .await
        .expect("define the join over agg");
        pass(&mut d).await;
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        let word = kind.word();
        let oracle = format!(
            "select agg.g::text, (agg.n + coalesce(b.w, 0))::text from public.agg agg \
             {word} join public.b b on agg.g = b.k order by 1"
        );
        let actual = "select g::text, total::text from public.jn order by 1";
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");

        write(&d, "alter table public.src alter column g type bigint").await;
        pass(&mut d).await;
        assert_eq!(status(&d, "public.agg").await, "paused");
        resume(&d, "agg").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(status(&d, "public.agg").await, "live");
        assert_eq!(column_type(&d, "public.agg", "g").await, "bigint");
        let error = paused_for(&d, "public.jn", "public.agg", &["g"]).await;
        assert!(
            error.contains("don't match") && error.contains("A resume of public.agg re-typed"),
            "{kind:?}: {error}"
        );
        resume_refused(&d, "jn").await;

        write(&d, "alter table public.b alter column k type bigint").await;
        resume(&d, "jn").await;
        for _ in 0..3 {
            pass(&mut d).await;
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
        }
        assert_eq!(status(&d, "public.jn").await, "live", "{kind:?}");
        assert_eq!(column_type(&d, "public.jn", "g").await, "bigint");
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");
        if kind == Kind::Left {
            let nulls = d.rows("select total from public.jn where g is null").await;
            assert_eq!(nulls.len(), 1, "the NULL-keyed group's row is kept");
        }
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), "public.jn")
            .await
            .expect("self_check");
        assert!(diverging.is_empty(), "{kind:?}: {diverging:?}");
        step(
            &mut d,
            "insert into public.src values (6, null, 1), (7, 3000000000, 1)",
        )
        .await;
        assert_eq!(d.rows(actual).await, d.rows(&oracle).await, "{kind:?}");
    }
}
