//! A `WHERE` other than `TRUE` through define (#1991): the predicate's own
//! refusals come first, then the key-space refusal each key space gives
//! until its step applies a filter. A refused definition creates nothing.

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
            "T0238",
        ),
        (
            "TRANSFORM t2 FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, MIN(qty) AS lo WHERE active",
            "T0239",
        ),
        (
            "TRANSFORM t3 FROM public.s LEFT JOIN public.u ON s.id = u.id GROUP BY s.g \
             SELECT s.g AS g, COUNT(*) AS n WHERE u.flag",
            "T0244",
        ),
        (
            "TRANSFORM tb FROM public.s JOIN public.u ON s.id = u.id GROUP BY s.g \
             SELECT s.g AS g, COUNT(*) AS n WHERE s.active AND s.qty > u.id",
            "T0243",
        ),
        (
            "TRANSFORM tc FROM public.s JOIN public.u ON s.id = u.id GROUP BY s.g \
             SELECT s.g AS g, COUNT(*) AS n WHERE FALSE",
            "T0245",
        ),
        (
            "TRANSFORM t4 FROM public.s JOIN public.u ON s.id = u.id \
             SELECT s.qty AS q WHERE s.qty",
            "T0234",
        ),
        (
            "TRANSFORM t5 FROM public.s SELECT qty AS q WHERE amount / qty > 5",
            "T0237",
        ),
        (
            "TRANSFORM t6 FROM public.s SELECT qty AS q WHERE status = note",
            "T0237",
        ),
        (
            "TRANSFORM t7 FROM public.s SELECT qty AS q WHERE qty",
            "T0234",
        ),
        (
            "TRANSFORM t8 FROM public.s SELECT amount AS amt WHERE amt > 1",
            "T0236",
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

    // A 1-1 `JOIN` applies its filter (#2041).
    trellis
        .apply(
            "TRANSFORM j1 FROM public.s JOIN public.u ON s.id = u.id \
             SELECT s.qty AS q WHERE u.flag",
        )
        .await
        .expect("a 1-1 JOIN takes a WHERE");

    // `WHERE TRUE` keeps its meaning.
    trellis
        .apply("TRANSFORM t9 FROM public.s SELECT qty AS q WHERE TRUE")
        .await
        .expect("WHERE TRUE is accepted");
    let status = trellis.status("t9").await.unwrap().expect("t9");
    assert_eq!(status.filter, None, "an unfiltered definition");

    // A plain aggregate applies its filter, and `status` says so.
    trellis
        .apply(
            "TRANSFORM g1 FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, COUNT(*) AS n WHERE active AND qty > 0",
        )
        .await
        .expect("a filtered aggregate is accepted");
    let status = trellis.status("g1").await.unwrap().expect("g1");
    assert_eq!(status.filter.as_deref(), Some("(active AND (qty > 0))"));
    // A join aggregate applies one conjunct per table.
    trellis
        .apply(
            "TRANSFORM ja FROM public.s JOIN public.u ON s.id = u.id GROUP BY s.g \
             WITHOUT SOURCE INDEX SELECT s.g AS g, COUNT(*) AS n WHERE s.active AND u.flag",
        )
        .await
        .expect("a filtered join aggregate is accepted");
    let status = trellis.status("ja").await.unwrap().expect("ja");
    assert_eq!(status.filter.as_deref(), Some("(s.active AND u.flag)"));
    // `WHERE (TRUE)` is a filter, with this release's floor: `status` says
    // so as its definition text reads, not as the bare `TRUE` it isn't.
    trellis
        .apply(
            "TRANSFORM g2 FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, COUNT(*) AS n WHERE (TRUE)",
        )
        .await
        .expect("a parenthesized TRUE is accepted");
    let status = trellis.status("g2").await.unwrap().expect("g2");
    assert_eq!(status.filter.as_deref(), Some("(TRUE)"));
    trellis.shutdown().await.expect("shutdown");
}
