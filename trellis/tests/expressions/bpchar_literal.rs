//! A string literal beside a `char(n)` (`bpchar`) value reads as Postgres
//! types it (#2000): `bpchar` against `bpchar`, which ignores trailing
//! padding, not the column cast to `text` against a `text` literal. A `name`
//! column is compared as a control. The write path and `self_check`'s audit
//! both render such a literal bare.

use crate::support::pipeline::connect;
use crate::support::wait_live::wait_for_live;
use std::time::Duration;
use testkit::TestCluster;
use trellis::{Config, Trellis, TrellisOptions};

/// Each 1-1 field over `ch character(4)` and `nm name` beside a string
/// literal, padded and unpadded.
const FIELDS: &[(&str, &str)] = &[
    ("f1", "ch = 'ab  '"),
    ("f2", "ch <> 'ab  '"),
    ("f3", "'ab' = ch"),
    ("f4", "ch IS DISTINCT FROM 'ab '"),
    ("f5", "NULLIF(ch, 'ab ')"),
    ("f6", "COALESCE(ch, 'zz  ') = 'zz'"),
    ("f7", "CASE WHEN id = 1 THEN ch ELSE 'x  ' END"),
    ("f9", "nm = 'ab'"),
];

/// An aggregate argument over the same column.
const AGGREGATE: &str = "COUNT(CASE WHEN ch = 'ab  ' THEN 1 END)";

#[tokio::test]
async fn a_literal_beside_a_bpchar_column_matches_postgres() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table s (id bigint primary key, ch character(4), nm name); \
         create index on s (nm); \
         insert into s values (1, 'ab', 'ab'), (2, 'abcd', 'abcd'), (3, null, null)",
    )
    .await
    .expect("source");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect");
    let fields: Vec<String> = FIELDS
        .iter()
        .map(|(name, expr)| format!("{expr} AS {name}"))
        .collect();
    for statement in [
        format!("TRANSFORM t FROM s SELECT {}", fields.join(", ")),
        format!("TRANSFORM g FROM s GROUP BY nm SELECT {AGGREGATE} AS n"),
    ] {
        trellis
            .apply(&statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    for target in ["t", "g"] {
        wait_for_live(&trellis, target).await;
    }
    // The build wrote the first rows; apply writes these.
    raw.batch_execute("insert into s values (4, 'ab', 'ab'), (5, null, 'x'), (6, 'ab  ', 'y')")
        .await
        .expect("insert");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("converges");

    let rows = async |sql: String| -> Vec<String> {
        raw.query(&sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .iter()
            .map(|row| row.get(0))
            .collect()
    };
    let line = |columns: Vec<String>| format!("concat_ws('|', id, {})", columns.join(", "));
    let actual = rows(format!(
        "select {} from t order by id",
        line(FIELDS.iter().map(|(n, _)| format!("{n}::text")).collect())
    ))
    .await;
    let expected = rows(format!(
        "select {} from s order by id",
        line(FIELDS.iter().map(|(_, e)| format!("({e})::text")).collect())
    ))
    .await;
    assert_eq!(actual, expected, "1-1 fields");

    let actual = rows("select concat_ws('|', nm, n) from g order by nm".to_string()).await;
    let expected = rows(format!(
        "select concat_ws('|', nm, {AGGREGATE}) from s group by nm order by nm"
    ))
    .await;
    assert_eq!(actual, expected, "aggregate argument");

    // `self_check`'s own renderer reads the literals as Postgres does too.
    use trellis::staging::self_check::CheckPageExt;
    for target in ["t", "g"] {
        let report = trellis
            .self_check_to_end(
                target,
                trellis::SelfCheckMode::Strict,
                Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|err| panic!("self_check {target}: {err}"));
        assert_eq!(
            report.outcome,
            trellis::SelfCheckOutcome::Converged,
            "{target}"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

/// Each definition's stored floor, by target.
async fn floor_of(raw: &tokio_postgres::Client, target: &str) -> String {
    raw.query_one(
        "select floor_version from transform_definitions \
         where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("floor")
    .get(0)
}

/// A definition a release before 0.4.0 stored, which cast the literal to
/// `text`, is rebuilt once on upgrade when the literal sits beside a
/// `char(n)` value, and stamped with the feature's floor (#2000); one whose
/// literal sits beside a `text` value is left as it was. The 0.3.x state is
/// made here by hand: the floor it stamped, and the value it wrote.
#[tokio::test]
async fn an_affected_definition_is_rebuilt_once_on_upgrade() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table s (id bigint primary key, ch character(4), t text); \
         create index on s (t); \
         insert into s values (1, 'ab', 'ab'), (2, 'zz', 'zz')",
    )
    .await
    .expect("source");
    // The upgrade's attach, as a release starting over 0.3.x's catalog runs
    // it, before any worker reads a definition.
    let attach = async || {
        let upgrade = Trellis::connect(
            Config::from_dsn(db.dsn().to_string()).expect("dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect");
        upgrade.migrate().await.expect("the upgrade's attach");
        upgrade.shutdown().await.expect("shutdown");
    };
    let connect_trellis = async || {
        Trellis::connect(
            Config::from_dsn(db.dsn().to_string()).expect("dsn"),
            TrellisOptions {
                staging: true,
                drain_threads: 1,
                ..Default::default()
            },
        )
        .await
        .expect("connect")
    };
    let trellis = connect_trellis().await;
    for statement in [
        "TRANSFORM a FROM s SELECT ch = 'ab  ' AS same",
        "TRANSFORM u FROM s SELECT t = 'ab' AS same",
        "TRANSFORM g FROM s GROUP BY t SELECT COUNT(CASE WHEN ch = 'ab  ' THEN 1 END) AS n",
        "TRANSFORM p FROM s SELECT ch = 'ab  ' AS same",
    ] {
        trellis.apply(statement).await.expect(statement);
    }
    for target in ["a", "u", "g", "p"] {
        wait_for_live(&trellis, target).await;
    }
    trellis.apply("PAUSE TRANSFORM p").await.expect("pause");
    assert_eq!(floor_of(&raw, "a").await, "0.4.0");
    assert_eq!(floor_of(&raw, "g").await, "0.4.0");
    assert_eq!(floor_of(&raw, "u").await, "0.2.0");
    trellis.shutdown().await.expect("shutdown");

    // As 0.3.x left them: its floors, and `a` and `g` holding the values
    // its `'ab  '::text` computed. `u`'s row is changed too, so a rebuild of
    // it would show.
    raw.batch_execute(
        "update transform_definitions set floor_version = '0.3.0'; \
         update a set same = false where id = 1; \
         update g set n = 0 where t = 'ab'; \
         update p set same = false where id = 1; \
         update u set same = null where id = 1",
    )
    .await
    .expect("the 0.3.x state");

    attach().await;
    assert_eq!(floor_of(&raw, "a").await, "0.4.0", "re-stamped at attach");
    assert_eq!(floor_of(&raw, "g").await, "0.4.0", "re-stamped at attach");
    assert_eq!(floor_of(&raw, "u").await, "0.3.0", "left alone");
    // A paused one is stamped, and its resume rebuilds it.
    assert_eq!(floor_of(&raw, "p").await, "0.4.0", "re-stamped at attach");
    let trellis = connect_trellis().await;
    for target in ["a", "u", "g"] {
        wait_for_live(&trellis, target).await;
    }
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("converges");
    let same = async |target: &str| -> Option<bool> {
        raw.query_one(&format!("select same from {target} where id = 1"), &[])
            .await
            .expect("row")
            .get(0)
    };
    assert_eq!(same("a").await, Some(true), "rebuilt");
    let n: i64 = raw
        .query_one("select n from g where t = 'ab'", &[])
        .await
        .expect("group")
        .get(0);
    assert_eq!(n, 1, "the aggregate rebuilt");
    assert_eq!(same("p").await, Some(false), "paused, not rebuilt");
    trellis.apply("RESUME TRANSFORM p").await.expect("resume");
    wait_for_live(&trellis, "p").await;
    assert_eq!(same("p").await, Some(true), "rebuilt by its resume");
    assert_eq!(same("u").await, None, "not rebuilt");
    trellis.shutdown().await.expect("shutdown");

    // Once: a later start rebuilds nothing.
    raw.execute("update a set same = null where id = 2", &[])
        .await
        .expect("mark");
    attach().await;
    let trellis = connect_trellis().await;
    wait_for_live(&trellis, "a").await;
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("converges");
    let row = raw
        .query_one("select same from a where id = 2", &[])
        .await
        .expect("row");
    assert_eq!(row.get::<_, Option<bool>>(0), None, "rebuilt once only");
    trellis.shutdown().await.expect("shutdown");
}
