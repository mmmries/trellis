//! A transform over a mixed-case table in a mixed-case schema
//! (`"Shop"."OrderItems"`) installs, backfills and applies CDC.
//!
//! As in Postgres, `FROM "Shop"."OrderItems"` names that table and an
//! unquoted name folds to lower case (ADR-0022). Every catalog lookup of the
//! table hands `to_regclass` each part quoted, so the mixed-case names reach
//! Postgres as written.
//!
//! A definition stored under grammar 1, which kept an unquoted name's case,
//! reads its names as if each were quoted (ADR-0018): beside a lower-case
//! twin column, table and schema it keeps reading the one it read before,
//! and nothing pauses.
//!
//! Capture is the real triggers, and the ring is drained by hand
//! (`support/trigger_pipeline.rs`), so nothing here polls for convergence.

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;

#[path = "support/trigger_pipeline.rs"]
mod trigger_pipeline;

use trigger_pipeline::Pipeline;

const SOURCE_DDL: &str = "create schema \"Shop\"; \
     create table \"Shop\".\"OrderItems\" (id bigint primary key, order_id bigint, qty integer); \
     insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (1, 10, 2), (2, 10, 3), (3, 20, 5)";

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn rows(pairs: &[(&str, &str)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

async fn install(db: &testkit::TestDatabase, text: &str) {
    install_over(db, text, &["id", "order_id", "qty"]).await;
}

async fn install_over(db: &testkit::TestDatabase, text: &str, source_columns: &[&str]) {
    install_definition(&db.pool, text, &numeric_columns(source_columns), "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    trellis::intake::harness::settle_builds(&db.pool).await;
}

/// A 1-1 transform from `"Shop"."OrderItems"` into a mixed-case target in
/// the same mixed-case schema, `"Shop"."ItemCopy"`.
#[tokio::test]
async fn a_one_to_one_transform_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL)
        .await
        .expect("seed the source");
    install(
        &db,
        "TRANSFORM \"Shop\".\"ItemCopy\" FROM \"Shop\".\"OrderItems\" SELECT qty AS qty",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const COPY: &str = "select id::text, qty::text from \"Shop\".\"ItemCopy\"";
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("1", "2"), ("2", "3"), ("3", "5")]),
        "the backfill copied every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set qty = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("2", "30"), ("3", "5"), ("4", "7")]),
        "CDC insert, update and delete all reached the target"
    );
}

/// An aggregate over `"Shop"."OrderItems"`, grouped by a column of it.
#[tokio::test]
async fn an_aggregate_over_a_mixed_case_table_backfills_and_applies_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL)
        .await
        .expect("seed the source");
    install(
        &db,
        "TRANSFORM qty_by_order FROM \"Shop\".\"OrderItems\" GROUP BY order_id \
         WITHOUT SOURCE INDEX SELECT SUM(qty) AS total",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const TOTALS: &str = "select order_id::text, total::text from qty_by_order";
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "5"), ("20", "5")]),
        "the backfill folded every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set qty = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1; \
             insert into \"Shop\".\"OrderItems\" (id, order_id, qty) values (5, 30, 1)",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "30"), ("20", "12"), ("30", "1")]),
        "CDC insert, update and delete all reached the aggregate"
    );
}

/// A composed field and an expression argument over mixed-case
/// columns of a mixed-case table follow CDC, with the group and argument
/// columns carrying those names quoted.
#[tokio::test]
async fn recomputed_fields_over_mixed_case_columns_follow_cdc() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create schema \"Shop\"; \
         create table \"Shop\".\"OrderItems\" (id bigint primary key, \"OrderId\" bigint, \"Qty\" integer); \
         insert into \"Shop\".\"OrderItems\" (id, \"OrderId\", \"Qty\") values (1, 10, 2), (2, 10, 3), (3, 20, 5)",
    )
    .await
    .expect("seed the source");
    // The columns' real types: `SUM(Qty + 1)` over a `numeric` would be an
    // expression with no declared scale, which the group basis doesn't take.
    let int = |w| ValueType::Integer(w);
    let columns: HashMap<String, ValueType> = [
        ("id", int(trellis::integer::IntWidth::Int8)),
        ("OrderId", int(trellis::integer::IntWidth::Int8)),
        ("Qty", int(trellis::integer::IntWidth::Int4)),
    ]
    .into_iter()
    .map(|(n, t)| (n.to_string(), t))
    .collect();
    install_definition(
        &db.pool,
        "TRANSFORM qty_span FROM \"Shop\".\"OrderItems\" GROUP BY \"OrderId\" \
        WITHOUT SOURCE INDEX SELECT COUNT(\"Qty\") AS lo, SUM(\"Qty\" + 1) AS hi1, \
        SUM(\"Qty\") + COUNT(\"Qty\") AS ends",
        &columns,
        "public",
    )
    .await
    .expect("install qty_span");
    trellis::intake::harness::settle_builds(&db.pool).await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.OrderItems"]).await;
    const SPANS: &str = "select \"OrderId\"::text, concat_ws(',', lo, hi1, ends) from qty_span";
    assert_eq!(
        pipeline.rows(SPANS).await,
        rows(&[("10", "2,7,7"), ("20", "1,6,6")]),
        "the backfill folded every pre-existing row"
    );

    pipeline
        .raw
        .batch_execute(
            "insert into \"Shop\".\"OrderItems\" (id, \"OrderId\", \"Qty\") values (4, 20, 7); \
             update \"Shop\".\"OrderItems\" set \"Qty\" = 30 where id = 2; \
             delete from \"Shop\".\"OrderItems\" where id = 1; \
             delete from \"Shop\".\"OrderItems\" where id = 3",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(SPANS).await,
        rows(&[("10", "1,31,31"), ("20", "1,8,8")]),
        "CDC retracted the old rows and recomputed each group"
    );
}

/// The twins beside `"Shop"."Orders"`: a lower-case column `amount` beside
/// `"Amount"`, a table `"Shop".orders` and a schema `shop` holding an
/// `"Orders"`. Folding grammar 1's names would read them instead.
const TWINS_DDL: &str = "create schema \"Shop\"; create schema shop; \
     create table \"Shop\".\"Orders\" (id bigint primary key, \"OrderId\" bigint, \
         \"Amount\" bigint, amount bigint); \
     create table \"Shop\".orders (id bigint primary key, \"OrderId\" bigint, \
         \"Amount\" bigint, amount bigint); \
     create table shop.\"Orders\" (id bigint primary key, \"OrderId\" bigint, \
         \"Amount\" bigint, amount bigint); \
     insert into \"Shop\".\"Orders\" values (1, 10, 5, 500), (2, 10, 7, 700), (3, 20, 1, 100); \
     insert into \"Shop\".orders values (1, 10, 1000, 1000); \
     insert into shop.\"Orders\" values (1, 10, 2000, 2000)";

/// Stores `text` as definition `target`'s, under grammar 1: a definition
/// a 0.1.0 release wrote.
async fn store_as_grammar_1(raw: &tokio_postgres::Client, target: &str, text: &str) {
    let stored = raw
        .execute(
            "update transform_definitions set definition_text = $2, grammar_version = 1 \
             where target_table = $1",
            &[&target, &text],
        )
        .await
        .expect("store the grammar-1 text");
    assert_eq!(stored, 1, "{target}");
}

/// Grammar-1 definitions whose every name has upper-case letters (target,
/// schema, table, `GROUP BY` key, column, label) keep reading the quoted
/// names beside lower-case twins after the upgrade: the attach pauses
/// nothing, CDC follows `"Amount"`, not `amount`, and an `ALTER` rewrites the
/// text into grammar 2, quoting each name.
#[tokio::test]
async fn grammar_1_definitions_keep_reading_the_names_they_read_beside_lower_case_twins() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(TWINS_DDL).await.expect("seed the twins");
    let int8 = ValueType::Integer(trellis::integer::IntWidth::Int8);
    let columns: HashMap<String, ValueType> = ["id", "OrderId", "Amount", "amount"]
        .into_iter()
        .map(|n| (n.to_string(), int8))
        .collect();
    for (target, grammar_2, grammar_1) in [
        (
            "public.OrderTotals",
            "TRANSFORM \"OrderTotals\" FROM \"Shop\".\"Orders\" GROUP BY \"OrderId\" \
             WITHOUT SOURCE INDEX SELECT SUM(\"Amount\") AS \"Total\"",
            "TRANSFORM OrderTotals FROM Shop.Orders GROUP BY OrderId \
             WITHOUT SOURCE INDEX SELECT SUM(Amount) AS Total",
        ),
        (
            "public.ItemCopy",
            "TRANSFORM \"ItemCopy\" FROM \"Shop\".\"Orders\" SELECT \"Amount\" AS \"Amount\"",
            "TRANSFORM ItemCopy FROM Shop.Orders SELECT Amount AS Amount",
        ),
    ] {
        install_definition(&db.pool, grammar_2, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install {grammar_2:?}: {e}"));
        store_as_grammar_1(&raw, target, grammar_1).await;
    }
    trellis::intake::harness::settle_builds(&db.pool).await;

    let config = trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("attach");
    let paused: i64 = raw
        .query_one(
            "select count(*) from transform_definitions where status = 'paused'",
            &[],
        )
        .await
        .expect("count paused")
        .get(0);
    assert_eq!(paused, 0, "the attach pauses no grammar-1 definition");

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["Shop.Orders"]).await;
    const TOTALS: &str = "select \"OrderId\"::text, \"Total\"::text from \"OrderTotals\"";
    const COPY: &str = "select id::text, \"Amount\"::text from \"ItemCopy\"";
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "12"), ("20", "1")])
    );
    pipeline
        .raw
        .batch_execute(
            "update \"Shop\".\"Orders\" set \"Amount\" = 50 where id = 1; \
             update \"Shop\".\"Orders\" set amount = 9999 where id = 2; \
             insert into \"Shop\".\"Orders\" values (4, 20, 3, 300)",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "57"), ("20", "4")]),
        "the totals follow \"Amount\", never its twin amount"
    );
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[("1", "50"), ("2", "7"), ("3", "1"), ("4", "3")])
    );

    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect");
    trellis
        .apply("ALTER TRANSFORM \"ItemCopy\" ADD \"Amount\" + 1 AS \"Next\"")
        .await
        .expect("alter the grammar-1 definition");
    let row = pipeline
        .raw
        .query_one(
            "select definition_text, grammar_version from transform_definitions \
             where target_table = 'public.ItemCopy'",
            &[],
        )
        .await
        .expect("read the rewritten text");
    let (text, version): (String, i32) = (row.get(0), row.get(1));
    assert_eq!(
        text,
        "TRANSFORM \"ItemCopy\" FROM \"Shop\".\"Orders\" SELECT \"Amount\" AS \"Amount\", \
         (\"Amount\" + 1) AS \"Next\""
    );
    assert_eq!(version, trellis::defs::grammar::CURRENT_GRAMMAR_VERSION);
}

/// The silent case grammar 1's compatibility rule exists for: a grammar-1
/// definition whose only upper-case name is a column beside a lower-case
/// twin of the same type. Folding it would still load, validate and run,
/// reading `amount` instead of `"Amount"`, and nothing would pause; the rule
/// keeps it reading `"Amount"`.
#[tokio::test]
async fn a_grammar_1_column_beside_a_lower_case_twin_keeps_reading_it_without_pausing() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.invoices (id bigint primary key, customer_id bigint, \
             \"Amount\" bigint, amount bigint); \
         insert into public.invoices values (1, 10, 5, 500), (2, 10, 7, 700), (3, 20, 1, 100)",
    )
    .await
    .expect("seed the twins");
    let int8 = ValueType::Integer(trellis::integer::IntWidth::Int8);
    let columns: HashMap<String, ValueType> = ["id", "customer_id", "Amount", "amount"]
        .into_iter()
        .map(|n| (n.to_string(), int8))
        .collect();
    install_definition(
        &db.pool,
        "TRANSFORM cust_totals FROM invoices GROUP BY customer_id \
         WITHOUT SOURCE INDEX SELECT SUM(\"Amount\") AS total",
        &columns,
        "public",
    )
    .await
    .expect("install");
    store_as_grammar_1(
        &raw,
        "public.cust_totals",
        "TRANSFORM cust_totals FROM invoices GROUP BY customer_id \
         WITHOUT SOURCE INDEX SELECT SUM(Amount) AS total",
    )
    .await;
    trellis::intake::harness::settle_builds(&db.pool).await;

    let config = trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("attach");
    let mut pipeline = Pipeline::attach(cluster, db, raw, &["public.invoices"]).await;
    const TOTALS: &str = "select customer_id::text, total::text from cust_totals";
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "12"), ("20", "1")])
    );
    pipeline
        .raw
        .batch_execute(
            "update public.invoices set \"Amount\" = 50 where id = 1; \
             update public.invoices set amount = 9999 where id = 2; \
             insert into public.invoices values (4, 20, 3, 300)",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(TOTALS).await,
        rows(&[("10", "57"), ("20", "4")]),
        "the totals follow \"Amount\", never its twin amount"
    );
    let status: String = pipeline
        .raw
        .query_one(
            "select status from transform_definitions where target_table = 'public.cust_totals'",
            &[],
        )
        .await
        .expect("read the status")
        .get(0);
    assert_ne!(status, "paused", "nothing pauses");
}

/// An `ALTER TRANSFORM` of a grammar-1 definition that keeps a label over
/// 63 bytes (Postgres truncated the target column it named) is refused: its
/// rewritten text is text the current grammar refuses, which no load could
/// read back. The stored text is left as it was.
#[tokio::test]
async fn an_alter_that_would_store_a_grammar_1_label_over_63_bytes_is_refused() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(TWINS_DDL).await.expect("seed the source");
    let int8 = ValueType::Integer(trellis::integer::IntWidth::Int8);
    let columns: HashMap<String, ValueType> = ["id", "OrderId", "Amount", "amount"]
        .into_iter()
        .map(|n| (n.to_string(), int8))
        .collect();
    let short = "l".repeat(63);
    let long = "l".repeat(70);
    install_definition(
        &db.pool,
        &format!("TRANSFORM long_copy FROM \"Shop\".\"Orders\" SELECT \"Amount\" AS {short}"),
        &columns,
        "public",
    )
    .await
    .expect("install");
    let released = format!("TRANSFORM long_copy FROM Shop.Orders SELECT Amount AS {long}");
    store_as_grammar_1(&raw, "public.long_copy", &released).await;
    trellis::intake::harness::settle_builds(&db.pool).await;
    let config = trellis::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("attach");
    let pipeline = Pipeline::attach(cluster, db, raw, &["Shop.Orders"]).await;

    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("connect");
    let err = trellis
        .apply("ALTER TRANSFORM long_copy ADD \"Amount\" + 1 AS nxt")
        .await
        .expect_err("the rewrite would hold a name over 63 bytes");
    assert!(err.to_string().contains("at most 63 bytes"), "{err}");
    let row = pipeline
        .raw
        .query_one(
            "select definition_text, grammar_version from transform_definitions \
             where target_table = 'public.long_copy'",
            &[],
        )
        .await
        .expect("read the text");
    assert_eq!(
        (row.get::<_, String>(0), row.get::<_, i32>(1)),
        (released, 1)
    );
}
