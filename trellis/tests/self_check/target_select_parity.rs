//! `trellis::target_select_sql` computes what the engine maintains, for the
//! shapes the client parity fixture (`clients/parity/cases.json`,
//! `refresh_target`) doesn't reach: `MIN`/`MAX` (which need a source index),
//! a composite primary key, an enum and a `varchar(n)` column, a partitioned
//! source, a schema-qualified `FROM` with aliases, a three-table 1-1 `JOIN`
//! with a `NULL` join key, and a 1-1 transform chained off a join
//! aggregate's target.
//!
//! Each target is built, then changed by inserts, updates and deletes that
//! drain. Inside a transaction rolled back after, it is refilled from its
//! rendered `SELECT` and compared with the maintained rows, as values and as
//! text (so a numeric's scale counts too).

#[path = "support/wait_live.rs"]
mod wait_live;

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::{Config, Trellis, TrellisOptions};

async fn run(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect")
}

/// The rows of `target` as text that its refill from `target_select_sql`
/// lacks, and those it adds, on the test's own connection (whose
/// `search_path` resolves the definitions' bare names).
async fn refill_differences(raw: &Client, target: &str) -> (Vec<String>, Vec<String>) {
    let row = raw
        .query_one(
            "select definition_text, grammar_version from trellis.transform_definitions \
             where target_table = 'public.' || $1",
            &[&target],
        )
        .await
        .expect("read the definition");
    let columns: Vec<String> = raw
        .query(
            "select attname::text from pg_attribute where attrelid = to_regclass($1) \
             and attnum > 0 and not attisdropped order by attnum",
            &[&format!("public.{target}")],
        )
        .await
        .expect("read the target's columns")
        .iter()
        .map(|r| r.get(0))
        .collect();
    let select = trellis::target_select_sql(row.get(0), row.get(1), &columns).expect("renders");
    let column_list = columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    raw.batch_execute(&format!(
        "begin; create temp table maintained on commit drop as table public.{target}; \
         delete from public.{target}; insert into public.{target} ({column_list}) {select}"
    ))
    .await
    .unwrap_or_else(|e| panic!("{target}: {select}: {e}"));
    let maintained: i64 = raw
        .query_one("select count(*) from maintained", &[])
        .await
        .expect("count")
        .get(0);
    assert!(
        maintained > 0,
        "{target} is empty, so the comparison proves nothing"
    );
    let as_values: i64 = raw
        .query_one(
            &format!(
                "select (select count(*) from (table maintained except all table public.{target}) l) \
                      + (select count(*) from (table public.{target} except all table maintained) r)"
            ),
            &[],
        )
        .await
        .expect("compare as values")
        .get(0);
    let mut differences = Vec::new();
    for (from, minus) in [
        ("maintained", format!("public.{target}")),
        (&*format!("public.{target}"), "maintained".to_string()),
    ] {
        differences.push(
            raw.query(
                &format!(
                    "select r from (select t::text r from {from} t except all \
                     select t::text from {minus} t) d order by 1"
                ),
                &[],
            )
            .await
            .expect("compare as text")
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        );
    }
    raw.batch_execute("rollback").await.expect("roll back");
    assert_eq!(as_values, 0, "{target}: {differences:?}");
    let adds = differences.pop().expect("two");
    let lacks = differences.pop().expect("two");
    (lacks, adds)
}

#[tokio::test]
async fn a_refill_from_target_select_sql_equals_the_maintained_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = testkit::connect_with(db.dsn(), "").await;
    raw.batch_execute(
        "create schema shop; \
         create type mood as enum ('sad', 'ok', 'happy'); \
         create table public.s (a integer, b text, v numeric(10,3), q integer, name varchar(8), \
            m mood, d date, primary key (a, b)); \
         create index on public.s (q, v); \
         create table shop.orders (id integer primary key, customer_id integer, region_id integer, \
            amount numeric(12,2), qty integer); \
         create table public.customers (id integer primary key, tier text, discount numeric); \
         create table public.regions (id integer primary key, name text); \
         create table public.ps (id integer, g integer, v integer, primary key (id, g)) \
            partition by list (g); \
         create table public.ps1 partition of public.ps for values in (1, 2); \
         create table public.ps2 partition of public.ps for values in (3, 4, 5); \
         create index on public.ps (g, v); \
         insert into public.s values (1,'x',1.5,1,'abc','ok','2024-01-02'), \
            (2,'y',null,1,'Zed','sad','2024-03-04'), (3,'z',2.125,null,null,null,null), \
            (4,'w',-7,2,'q','happy','2023-12-31'); \
         insert into public.customers values (1,'gold',0.9), (2,'gold',null), (3,null,0.5); \
         insert into public.regions values (1,'north'), (2,'south'); \
         insert into shop.orders values (1,1,1,10.50,2), (2,1,2,3,1), (3,2,1,7.25,4), \
            (4,3,null,1.125,3), (5,null,2,4,null), (6,9,1,2.5,5); \
         insert into public.ps values (1,1,10), (2,1,20), (3,3,5), (4,5,null)",
    )
    .await
    .expect("set up the sources");

    let trellis = run(db.dsn()).await;
    for statement in [
        "TRANSFORM one FROM s SELECT CHAR_LENGTH(name) AS len, ROUND(v, 1) AS rv, \
         COALESCE(q, 0) * 2 AS q2, CASE WHEN v > 0 THEN 'pos' ELSE 'neg' END AS sign, \
         name AS name, m AS m, DATE_TRUNC('month', CAST(d AS timestamp)) AS month, v / 3 AS third",
        "TRANSFORM agg FROM s GROUP BY q SELECT q AS q, MIN(v) AS lo, MAX(v) AS hi, AVG(v) AS mean, \
         SUM(q) AS sq, COUNT(name) AS named, COUNT(*) AS n",
        "TRANSFORM pagg FROM ps GROUP BY g SELECT g AS g, MAX(v) AS hi, SUM(v) AS total, COUNT(*) AS n",
        "TRANSFORM jagg FROM shop.orders AS o LEFT JOIN customers c ON o.customer_id = c.id \
         GROUP BY c.tier WITHOUT SOURCE INDEX \
         SELECT c.tier AS tier, SUM(o.amount) AS total, AVG(o.qty) AS mean_qty, COUNT(*) AS n",
        "TRANSFORM jone FROM shop.orders AS o LEFT JOIN customers c ON o.customer_id = c.id \
         JOIN regions r ON o.region_id = r.id \
         SELECT c.tier AS tier, r.name AS region, o.amount * COALESCE(c.discount, 1) AS discounted",
    ] {
        trellis
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    for target in ["one", "agg", "pagg", "jagg", "jone"] {
        wait_live::wait_for_live(&trellis, target).await;
    }
    trellis
        .apply("TRANSFORM jchain FROM jagg SELECT n * 2 AS n2, total AS total")
        .await
        .expect("define the chained transform");
    wait_live::wait_for_live(&trellis, "jchain").await;

    raw.batch_execute(
        "insert into public.s values (5,'v',3.3333,2,'abcdefgh','ok','2025-05-05'), \
            (6,'u',null,null,'n','sad',null); \
         update public.s set v = 100 where a = 1; delete from public.s where a = 4; \
         update shop.orders set customer_id = 3 where id = 2; \
         insert into shop.orders values (7,null,null,1,1); \
         update public.customers set tier = 'silver' where id = 2; \
         delete from shop.orders where id = 1; \
         insert into public.ps values (5,4,7), (6,2,null); \
         update public.ps set v = 99 where id = 1; delete from public.ps where id = 3",
    )
    .await
    .expect("change the sources");
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the pipeline converges");

    for target in ["one", "agg", "pagg", "jagg", "jone", "jchain"] {
        let (lacks, adds) = refill_differences(&raw, target).await;
        assert!(
            lacks.is_empty() && adds.is_empty(),
            "{target}'s refill lacks {lacks:?} and adds {adds:?}"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}
