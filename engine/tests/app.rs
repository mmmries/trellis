//! Integration tests for the `Trellis` facade's CDC source-table seeding
//! (issue #83 WI3), run against a real, ephemeral Postgres instance via the
//! shared harness (`testkit::TestCluster`).

use std::collections::HashMap;

use engine::app::qualified_source_tables;
use engine::defs::{create_definition, create_relationship};
use testkit::TestCluster;

/// A bare table with an integer primary key named `id`.
async fn create_table_with_pk(pool: &engine::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!("create table {name} (id serial primary key)"))
        .await
        .expect("create table with pk");
}

/// A bare table with its own primary key plus a plain (non-unique) integer
/// `fk_col` column, suitable as a to-many relationship's to-side.
async fn create_table_with_fk_column(pool: &engine::pool::Pool, name: &str, fk_col: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (id serial primary key, {fk_col} integer)"
        ))
        .await
        .expect("create table with fk column");
}

/// Issue #83 WI3: `qualified_source_tables` (the seed for
/// [`engine::ClientOptions::source_tables`] at staging startup) must seed the
/// full transitive closure of source tables (`engine::defs::all_source_tables`),
/// not just each definition's direct anchor. A definition anchored on
/// `authors` with a to-many relationship to `posts` (the shape a
/// `count(posts.id)`-style calculated field on `authors` reads through) must
/// seed both `authors` and `posts`, schema-qualified — otherwise a live write
/// to `posts` before the maintenance-reconcile loop catches up wouldn't be
/// captured by the CDC publication.
#[tokio::test]
async fn includes_relationship_to_tables_not_just_direct_anchors() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_table_with_pk(&db.pool, "authors").await;
    create_table_with_fk_column(&db.pool, "posts", "author").await;
    // To-many to-side prerequisite (#41): the join key must survive into
    // delete/re-parent pre-images.
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute("alter table posts replica identity full")
        .await
        .expect("set replica identity full");

    create_definition(
        &db.pool,
        "TRANSFORM authors_calc FROM authors SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author",
    )
    .await
    .expect("valid to-many relationship should be stored");

    let mut tables = qualified_source_tables(&db.pool)
        .await
        .expect("seeding query should succeed");
    tables.sort();
    assert_eq!(
        tables,
        vec!["trellis.authors".to_string(), "trellis.posts".to_string()]
    );
}
