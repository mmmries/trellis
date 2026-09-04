//! Integration tests for the relationship catalog (issue #26), run against a
//! real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).

use engine::defs::{CatalogError, EdgeKind, create_relationship, edges_from, relationship_by_name};
use testkit::TestCluster;

#[tokio::test]
async fn a_relationship_round_trips_through_the_catalog() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let created = create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    assert_eq!(created.def.name, "product");
    assert_eq!(created.def.from_table, "order_line_items");
    assert_eq!(created.def.from_col, "product_id");
    assert_eq!(created.def.to_table, "products");
    assert_eq!(created.def.to_col, "id");

    let read_back = relationship_by_name(&db.pool, "order_line_items", "product")
        .await
        .expect("read query")
        .expect("relationship should be found");

    assert_eq!(
        read_back, created,
        "re-parsed read-back must match the created value"
    );
}

#[tokio::test]
async fn relationship_by_name_returns_none_for_an_unknown_pair() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let missing = relationship_by_name(&db.pool, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}

/// Acceptance criterion: the declared relationship appears as a typed edge
/// in the resolver — a `Relationship`-kind `schema_edges` row from the
/// from-table's node to the to-table's node, distinct from a `Source` edge.
#[tokio::test]
async fn a_relationship_appears_as_a_typed_edge_in_the_resolver() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO users.id",
    )
    .await
    .expect("valid relationship should be stored");

    let relationship_edges = edges_from(&db.pool, "posts", EdgeKind::Relationship)
        .await
        .expect("query edges");
    assert_eq!(relationship_edges.len(), 1);
    assert_eq!(relationship_edges[0].kind, EdgeKind::Relationship);

    let from_node = engine::defs::node_for_table(&db.pool, "posts")
        .await
        .expect("query node")
        .expect("posts node should exist");
    let to_node = engine::defs::node_for_table(&db.pool, "users")
        .await
        .expect("query node")
        .expect("users node should exist");
    assert_eq!(relationship_edges[0].from_node_id, from_node.id);
    assert_eq!(relationship_edges[0].to_node_id, to_node.id);

    // No `Source` edge was created by declaring a relationship — the two
    // edge kinds stay distinct in the graph.
    let source_edges = edges_from(&db.pool, "posts", EdgeKind::Source)
        .await
        .expect("query edges");
    assert!(source_edges.is_empty());
}

/// ADR-0006's "Naming and scope": a relationship name is unique **per
/// from-table**, not global — declaring the same name twice on the same
/// from-table is rejected, but the same name on two different from-tables is
/// fine.
#[tokio::test]
async fn a_duplicate_name_on_the_same_from_table_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.author_id TO users.id",
    )
    .await
    .expect("first relationship should be stored");

    let err = create_relationship(
        &db.pool,
        "RELATIONSHIP author FROM posts.editor_id TO users.id",
    )
    .await
    .unwrap_err();

    assert!(matches!(err, CatalogError::Db(_)));
    let message = err.to_string();
    assert!(
        message.contains("duplicate key value violates unique constraint"),
        "expected the underlying Postgres detail in the error message, got: {message}"
    );
}

#[tokio::test]
async fn the_same_relationship_name_is_allowed_on_different_from_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP owner FROM posts.owner_id TO users.id",
    )
    .await
    .expect("first relationship should be stored");

    create_relationship(
        &db.pool,
        "RELATIONSHIP owner FROM comments.owner_id TO users.id",
    )
    .await
    .expect("same name on a different from-table should be allowed");

    let posts_owner = relationship_by_name(&db.pool, "posts", "owner")
        .await
        .expect("read query")
        .expect("posts.owner should exist");
    let comments_owner = relationship_by_name(&db.pool, "comments", "owner")
        .await
        .expect("read query")
        .expect("comments.owner should exist");
    assert_ne!(posts_owner.id, comments_owner.id);
}

/// No DDL is ever issued against `from_table`/`to_table` (ADR-0005): storing
/// a relationship never requires those tables to physically exist — nothing
/// in this test creates `order_line_items`/`products` as real relations,
/// only `create_relationship`'s catalog-only writes.
#[tokio::test]
async fn creating_a_relationship_issues_no_ddl_against_the_source_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP product FROM order_line_items.product_id TO products.id",
    )
    .await
    .expect("valid relationship should be stored");

    let client = db.pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select exists (
                select 1 from information_schema.tables
                where table_name in ('order_line_items', 'products')
            )",
            &[],
        )
        .await
        .expect("query")
        .get(0);
    assert!(
        !exists,
        "no physical table should have been created for either endpoint"
    );
}

/// Invalid source text fails to parse and leaves no row behind.
#[tokio::test]
async fn an_unparseable_relationship_is_rejected_and_leaves_no_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_relationship(&db.pool, "RELATIONSHIP product FROM order_line_items TO")
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Parse(_)));

    let missing = relationship_by_name(&db.pool, "order_line_items", "product")
        .await
        .expect("read query");
    assert!(missing.is_none());
}
