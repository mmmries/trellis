//! A `WHERE` other than `TRUE` through define (#1991 step A): the
//! predicate's own refusals come first, then the key-space refusal every key
//! space gives until its step applies a filter. Nothing is created.

use testkit::TestCluster;

use crate::support::pipeline::{connect_with, definer};

const SETUP: &str = "\
    create table public.s (id int primary key, g int, qty int, amount numeric, \
        status text, note text, active boolean); \
    create table public.u (id int primary key, flag boolean)";

#[tokio::test]
async fn define_refuses_a_filter_and_creates_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_with(db.dsn(), "").await;
    raw.batch_execute(SETUP).await.expect("setup");
    let trellis = definer(db.dsn()).await;

    for (text, expected) in [
        (
            "TRANSFORM t1 FROM public.s SELECT qty AS q WHERE qty > 0",
            "T0237",
        ),
        (
            "TRANSFORM t2 FROM public.s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE active",
            "T0237",
        ),
        (
            "TRANSFORM t3 FROM public.s JOIN public.u ON s.id = u.id GROUP BY s.g \
             SELECT s.g AS g, COUNT(*) AS n WHERE u.flag",
            "T0237",
        ),
        (
            "TRANSFORM t4 FROM public.s JOIN public.u ON s.id = u.id \
             SELECT s.qty AS q WHERE u.flag",
            "T0237",
        ),
        (
            "TRANSFORM t5 FROM public.s SELECT qty AS q WHERE amount / qty > 5",
            "T0236",
        ),
        (
            "TRANSFORM t6 FROM public.s SELECT qty AS q WHERE status = note",
            "T0236",
        ),
        (
            "TRANSFORM t7 FROM public.s SELECT qty AS q WHERE qty",
            "T0233",
        ),
        (
            "TRANSFORM t8 FROM public.s SELECT amount AS amt WHERE amt > 1",
            "T0235",
        ),
    ] {
        let err = trellis.apply(text).await.expect_err(text);
        let message = err.to_string();
        assert!(message.contains(expected), "{text}: {message}");
    }
    let targets: i64 = raw
        .query_one(
            "select count(*) from pg_class where relname like 't_' and relkind = 'r'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(targets, 0, "nothing is created");

    // `WHERE TRUE` keeps its meaning.
    trellis
        .apply("TRANSFORM t9 FROM public.s SELECT qty AS q WHERE TRUE")
        .await
        .expect("WHERE TRUE is accepted");
    trellis.shutdown().await.expect("shutdown");
}
