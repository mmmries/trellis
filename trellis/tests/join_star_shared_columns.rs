//! A star 1-1 `JOIN` (#1460) whose positions join on the same root column:
//! `a.k` is the `ON` column of both `b1` and `b2`. `R` holds a join key per
//! position, so that column is in `R`'s key twice, and every statement that
//! writes `R`'s key columns writes it once.
//!
//! Found by the generative star tier's reach sweep (#1781): a root row
//! deleted or moved off every position cleared `R`'s key with
//! `set k = null, k = null`, which Postgres refuses ("multiple assignments
//! to same column"), so the row was held in quarantine and its target row
//! kept. Each case ends with the oracle: the target is the live multi-join,
//! nothing is held, and `self_check` finds nothing.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;

fn ddl() -> &'static str {
    "create table public.a (id integer primary key, k integer, v integer); \
     create table public.b1 (id integer primary key, k integer unique, w integer); \
     create table public.b2 (id integer primary key, k integer unique, y integer); \
     create index on public.a (k); \
     insert into public.b1 select i, i, i * 10 from generate_series(1, 4) i; \
     insert into public.b2 select i, i, i * 100 from generate_series(1, 4) i; \
     insert into public.a select i, i % 5, i from generate_series(1, 10) i"
}

fn columns() -> Vec<(&'static str, ValueType)> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    vec![("id", int4), ("k", int4), ("v", int4)]
}

fn definition(kinds: [&str; 2]) -> String {
    format!(
        "TRANSFORM t FROM public.a {} JOIN public.b1 ON a.k = b1.k \
         {} JOIN public.b2 ON a.k = b2.k \
         SELECT a.v AS v, b1.w AS w, b2.y AS y",
        kinds[0], kinds[1]
    )
}

fn oracle(kinds: [&str; 2]) -> String {
    format!(
        "select a.id, a.v, b1.w, b2.y from public.a a \
         {} join public.b1 b1 on a.k = b1.k \
         {} join public.b2 b2 on a.k = b2.k order by a.id",
        kinds[0], kinds[1]
    )
}

const TARGET: &str = "select id, v, w, y from public.t order by id";

async fn assert_matches(d: &Driver, kinds: [&str; 2], case: &str) {
    assert_eq!(
        d.rows(TARGET).await,
        d.rows(&oracle(kinds)).await,
        "{case} {kinds:?}"
    );
    assert_eq!(
        d.rows("select key from poison_held").await,
        Vec::<String>::new(),
        "{case} {kinds:?}: held rows"
    );
    let found = star::self_check(d, "public.t", 1000).await;
    assert!(
        found.is_empty(),
        "{case} {kinds:?}: self_check finds {found:?}"
    );
}

/// Root rows deleted, moved to a `NULL` key and moved between keys, joined
/// rows changed under the shared key, and a truncate of a joined table (and
/// its rebuild) and of the root, each drained and then checked by the oracle
/// and by `self_check`, under every kind pair.
#[tokio::test]
async fn a_star_joining_two_positions_on_one_root_column_applies_root_deletes_and_moves() {
    let _serial = star::serial().await;
    let was = star::star_join_enabled();
    trellis::dev::defs::set_star_join(true);
    for kinds in [
        ["LEFT", "LEFT"],
        ["LEFT", "INNER"],
        ["INNER", "LEFT"],
        ["INNER", "INNER"],
    ] {
        let mut d = Driver::start(
            ddl(),
            &columns(),
            &[&definition(kinds)],
            &["public.a", "public.b1", "public.b2"],
        )
        .await;
        assert_matches(&d, kinds, "built").await;
        for (case, sql) in [
            ("a delete", "delete from public.a where id in (1, 6)"),
            (
                "a move to NULL",
                "update public.a set k = null where id = 2",
            ),
            ("a move", "update public.a set k = 4 where id = 3"),
            (
                "a re-insert",
                "insert into public.a values (1, 3, 99), (6, null, 98)",
            ),
            (
                "a joined change",
                "update public.b1 set w = w + 1 where k = 4",
            ),
            ("a joined delete", "delete from public.b2 where k = 3"),
            ("a truncate of a joined table", "truncate public.b1"),
            (
                "a joined table re-seeded",
                "insert into public.b1 select i, i, i * 10 from generate_series(1, 4) i",
            ),
            ("a truncate of the root", "truncate public.a"),
            (
                "the root re-seeded",
                "insert into public.a select i, i % 5, i from generate_series(1, 10) i",
            ),
            ("every root row deleted", "delete from public.a"),
        ] {
            d.user()
                .await
                .batch_execute(sql)
                .await
                .unwrap_or_else(|e| panic!("{case}: {sql}: {e}"));
            d.settle().await;
            // A truncate of a joined table starts a rebuild.
            trellis::staging::build::settle_builds_with(d.pool(), 3).await;
            d.settle().await;
            assert_matches(&d, kinds, case).await;
        }
    }
    trellis::dev::defs::set_star_join(was);
}
