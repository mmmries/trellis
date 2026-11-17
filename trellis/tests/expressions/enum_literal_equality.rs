//! An enum column `=` a string literal (#2037): Postgres reads the literal as
//! a value of the enum, define refuses a label the enum lacks, and a
//! relabelled value fails the definition at its next `RESUME`.
//!
//! Each running test compares the target with the same query written by hand
//! (ADR-0013) and then runs `self_check` in strict mode, which renders the
//! comparison with its own audit renderer.

use crate::support::pipeline::connect;
use crate::support::relabel::{H, RELABEL};
use crate::support::wait_live;

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::TransformStatus;
use trellis::staging::self_check::{CheckPageExt, SelfCheckScope};
use trellis::{Config, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisOptions};

async fn connect_trellis(dsn: &str, staging: bool) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect")
}

async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the pipeline converges");
}

async fn audit_converges(trellis: &Trellis, target: &str) {
    let page = trellis
        .self_check_page(
            target,
            SelfCheckScope {
                after: None,
                limit: 1000,
            },
            SelfCheckMode::Strict,
            Duration::from_secs(60),
        )
        .await
        .unwrap_or_else(|e| panic!("self_check {target}: {e}"));
    assert!(
        matches!(page.outcome, SelfCheckOutcome::Converged),
        "self_check {target}: {:?}",
        page.outcome
    );
}

async fn differences(raw: &Client, target: &str, columns: &str, oracle: &str) -> i64 {
    raw.query_one(
        &format!(
            "with expected as ({oracle}), actual as (select {columns} from {target}) \
             select (select count(*) from (table expected except table actual) missing) \
                  + (select count(*) from (table actual except table expected) extra)"
        ),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("difference query for {target}: {e:?}"))
    .get(0)
}

const SOURCE: &str = "create type ticket_state as enum ('open', 'closed', 'it''s'); \
     create table s (id bigint primary key, state ticket_state, g integer); \
     create index on s (g)";

const WORKLOAD: &[&str] = &[
    "insert into s values (1, 'open', 1), (2, 'closed', 1), (3, null, 2), (4, 'it''s', 2)",
    "update s set state = 'closed' where id = 1",
    "update s set state = 'open' where id = 3",
    "insert into s values (5, 'open', 1), (6, 'open', 3)",
    "delete from s where id = 2",
    "update s set state = 'it''s', g = 3 where id = 5",
];

/// A 1-1 field, an aggregate argument and an aggregate's `WHERE` compare the
/// enum with a label, in either order, under `NOT`, in a `CASE` and read by
/// another field; the target matches the hand-written query across inserts,
/// updates and deletes.
#[tokio::test]
async fn an_enum_equals_a_label_in_a_one_to_one_field_and_an_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute(WORKLOAD[0]).await.expect("seed");
    let definer = connect_trellis(db.dsn(), false).await;
    for statement in [
        "TRANSFORM t FROM s SELECT state = 'open' AS is_open, 'closed' = state AS is_closed, \
           NOT state = 'open' AS not_open, state = 'it''s' AS quoted, \
           CASE WHEN state = 'open' THEN 1 ELSE 0 END AS flag, is_open AND g > 1 AS late",
        "TRANSFORM a FROM s GROUP BY g SELECT g AS g, \
           COUNT(CASE WHEN state = 'open' THEN 1 END) AS n_open, \
           SUM(CASE WHEN 'closed' = state THEN id ELSE 0 END) AS closed_ids",
        "TRANSFORM w FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE 'open' = state",
    ] {
        definer
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    definer.shutdown().await.expect("shut the definer down");
    let trellis = connect_trellis(db.dsn(), true).await;
    wait_live::wait_for_live(&trellis, "t").await;
    wait_live::wait_for_live(&trellis, "a").await;
    wait_live::wait_for_live(&trellis, "w").await;
    for sql in &WORKLOAD[1..] {
        raw.execute(*sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    converge(&trellis).await;

    assert_eq!(
        differences(
            &raw,
            "t",
            "id, is_open, is_closed, not_open, quoted, flag, late",
            "select id, state = 'open', 'closed' = state, not state = 'open', state = 'it''s', \
             case when state = 'open' then 1 else 0 end, state = 'open' and g > 1 from s",
        )
        .await,
        0
    );
    assert_eq!(
        differences(
            &raw,
            "a",
            "g, n_open, closed_ids",
            "select g, count(case when state = 'open' then 1 end), \
             coalesce(sum(case when 'closed' = state then id else 0 end), 0) from s group by g",
        )
        .await,
        0
    );
    // As in Postgres, a group with no member row has no target row.
    assert_eq!(
        differences(
            &raw,
            "w",
            "g, n",
            "select g, count(*) from s where 'open' = state group by g",
        )
        .await,
        0
    );
    audit_converges(&trellis, "t").await;
    audit_converges(&trellis, "a").await;
    audit_converges(&trellis, "w").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A literal that is no label is refused at define, naming the enum.
#[tokio::test]
async fn a_literal_that_is_no_label_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    let trellis = connect_trellis(db.dsn(), false).await;
    for text in [
        "TRANSFORM t FROM s SELECT state = 'Open' AS x",
        "TRANSFORM t FROM s SELECT 'pending' = state AS x",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(CASE WHEN state = 'nope' THEN 1 END) AS n",
        // A `WHERE` reads the literal on every row too.
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE state = 'nope'",
    ] {
        let refusal = trellis.apply(text).await.expect_err(text).to_string();
        assert!(
            refusal.contains("T0241") && refusal.contains(".ticket_state"),
            "{text}: {refusal}"
        );
    }
    // `<>` and `NULLIF` stay refused.
    for text in [
        "TRANSFORM t FROM s SELECT state <> 'open' AS x",
        "TRANSFORM t FROM s SELECT NULLIF(state, 'open') AS x",
    ] {
        assert!(trellis.apply(text).await.is_err(), "{text}");
    }
    trellis.shutdown().await.expect("shutdown");
}

/// A renamed label pauses the definition (as for every enum reader) and its
/// `RESUME` fails with define's refusal: the literal names no label. The
/// remedy is to drop the transform and define it again.
#[tokio::test]
async fn a_relabelled_value_fails_the_definition_at_resume() {
    let mut h = H::new(
        "create table public.s (id int primary key, tone public.tone); \
         insert into public.s values (1, 'brisk'), (2, 'calm');",
        &["TRANSFORM t FROM public.s SELECT tone = 'brisk' AS is_brisk"],
    )
    .await;
    h.write(RELABEL).await;
    h.capture_pass().await;
    assert_eq!(h.status("t").await, TransformStatus::Paused);
    let refusal = h
        .trellis
        .apply("RESUME TRANSFORM t")
        .await
        .expect_err("the literal is no longer a label")
        .to_string();
    assert!(
        refusal.contains("T0241") && refusal.contains("'brisk'"),
        "{refusal}"
    );
    assert_eq!(h.status("t").await, TransformStatus::Paused);
    h.trellis.apply("DROP TRANSFORM t").await.expect("drop");
    h.trellis
        .apply("TRANSFORM t FROM public.s SELECT tone = 'zippy' AS is_zippy")
        .await
        .expect("define again with the new label");
}

/// The rule the validator mirrors (ADR-0022), checked against Postgres: a
/// bare literal beside an enum is a value of the enum, in either order; the
/// same literal cast to `text` has no `=` operator; and a literal that is no
/// label fails the statement, which is why define refuses it.
#[tokio::test]
async fn postgres_resolves_a_bare_literal_to_the_enum_and_refuses_the_rest() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(SOURCE).await.expect("source");
    raw.batch_execute("insert into s values (1, 'open', 1), (2, 'it''s', 1)")
        .await
        .expect("rows");
    let ids = |sql: &'static str| {
        let raw = &raw;
        async move {
            raw.query(sql, &[])
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(ids("select id from s where state = 'open'").await, [1]);
    assert_eq!(ids("select id from s where 'open' = state").await, [1]);
    assert_eq!(ids("select id from s where state = 'it''s'").await, [2]);
    for sql in [
        "select id from s where state = 'open'::text",
        "select id from s where state = 'nope'",
        "select id from s where state = 'Open'",
    ] {
        assert!(raw.query(sql, &[]).await.is_err(), "{sql}");
    }
}
