//! A comparison of two text columns whose collations conflict is refused at
//! define. Postgres raises `42P22` ("could not determine which collation to
//! use") on every row where both are not `NULL`, so a definition over an
//! empty table would define, then fail each write (confirmed against the
//! engine before the refusal: every drain failed with that error).
//!
//! `"C"` and `"POSIX"` are two deterministic collations with different OIDs.
//! The database default yields to either, so it never conflicts.

use crate::support::pipeline;

use pipeline::columns;

use testkit::TestCluster;
use trellis::defs::{
    CatalogError, ConflictingCollations, Statement, ValidationError, ValueType, alter_transform,
    install_definition, parse_statement,
};

const DDL: &str = r#"create table s (id bigint primary key, g integer, a text collate "C",
                         p text collate "POSIX", d text, a2 text collate "C");
                     create table o (id bigint primary key, p text collate "POSIX")"#;

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

fn s_columns() -> std::collections::HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("g", ValueType::Numeric),
        ("a", ValueType::Text),
        ("p", ValueType::Text),
        ("d", ValueType::Text),
        ("a2", ValueType::Text),
    ])
}

fn assert_conflict(err: CatalogError, field: &str, construct: &str, first: &str, second: &str) {
    let message = err.to_string();
    let CatalogError::Validate(ValidationError::ConflictingCollations(conflict)) = err else {
        panic!("expected ConflictingCollations, got {err:?}");
    };
    let ConflictingCollations {
        field: got_field,
        construct: got_construct,
        first_column,
        second_column,
        ..
    } = *conflict;
    assert_eq!(
        (got_field.as_str(), got_construct.as_str()),
        (field, construct),
        "{message}"
    );
    assert_eq!(
        (first_column.as_str(), second_column.as_str()),
        (first, second),
        "{message}"
    );
    assert!(message.starts_with("T0233"), "{message}");
}

/// A 1-1 refuses `a = p` over an empty table, where no row has failed yet,
/// and leaves no target behind.
#[tokio::test]
async fn a_one_to_one_comparing_two_collations_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, DDL).await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM one FROM s SELECT a = p AS same",
        &s_columns(),
        "public",
    )
    .await
    .expect_err("a conflict between collations must be refused");
    assert_conflict(err, "same", "=", "trellis.s.a", "trellis.s.p");
    let exists: bool = db
        .pool
        .get()
        .await
        .expect("get connection")
        .query_one("select to_regclass('public.one') is not null", &[])
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition leaves no target");
}

/// An aggregate's argument and a 1-1 `JOIN`'s two sides conflict the same way.
#[tokio::test]
async fn an_aggregate_and_a_one_to_one_join_comparing_two_collations_are_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, DDL).await;
    let err = install_definition(
        &db.pool,
        "TRANSFORM agg FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(CASE WHEN a = p THEN 1 END) AS n",
        &s_columns(),
        "public",
    )
    .await
    .expect_err("an aggregate argument that conflicts must be refused");
    assert_conflict(err, "n", "=", "trellis.s.a", "trellis.s.p");
    let err = install_definition(
        &db.pool,
        "TRANSFORM j FROM s JOIN o ON s.id = o.id \
         SELECT s.a = o.p AS same",
        &std::collections::HashMap::new(),
        "public",
    )
    .await
    .expect_err("a JOIN comparing a column of each side must be refused");
    assert_conflict(err, "same", "=", "s.a", "o.p");
}

/// Comparisons Postgres accepts still define: one collation, the database
/// default beside another collation, and a conflicting pair nothing compares.
#[tokio::test]
async fn comparisons_with_a_single_collation_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, DDL).await;
    install_definition(
        &db.pool,
        "TRANSFORM ok FROM s SELECT a = a2 AS same, a = d AS vs_default, \
         COALESCE(a, p) AS merged, CHAR_LENGTH(COALESCE(a, p)) AS len",
        &s_columns(),
        "public",
    )
    .await
    .expect("none of these has conflicting collations");
}

/// `ALTER TRANSFORM ... ADD` refuses the same, and adds no column.
#[tokio::test]
async fn an_added_field_comparing_two_collations_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, DDL).await;
    install_definition(
        &db.pool,
        "TRANSFORM ok FROM s SELECT a = a2 AS same",
        &s_columns(),
        "public",
    )
    .await
    .expect("defines");
    batch(
        &db.pool,
        "update transform_definitions set status = 'live' where target_table = 'public.ok'",
    )
    .await;
    let Statement::AlterTransform(alter) =
        parse_statement("ALTER TRANSFORM ok ADD NULLIF(a, p) AS n").expect("parse")
    else {
        panic!("expected an ALTER TRANSFORM statement");
    };
    let err = alter_transform(&db.pool, &alter)
        .await
        .expect_err("an added conflicting field must be refused");
    assert_conflict(err, "n", "NULLIF", "trellis.s.a", "trellis.s.p");
}
