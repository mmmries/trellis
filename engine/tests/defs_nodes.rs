//! Integration tests for the first-class schema-node model (issue #20), run
//! against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use engine::defs::{NodeKind, ValueType, create_definition, node_for_table, resolve_node};
use testkit::TestCluster;

#[tokio::test]
async fn creating_a_definition_resolves_its_source_and_target_as_nodes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    let source = node_for_table(&db.pool, "orders")
        .await
        .expect("query source node")
        .expect("source node must exist after create_definition");
    assert_eq!(source.table_name, "orders");
    assert!(source.is_source);
    assert!(!source.is_target);

    let target = node_for_table(&db.pool, "order_totals")
        .await
        .expect("query target node")
        .expect("target node must exist after create_definition");
    assert_eq!(target.table_name, "order_totals");
    assert!(target.is_target);
    assert!(!target.is_source);
}

#[tokio::test]
async fn a_table_with_no_definitions_has_no_node() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let node = node_for_table(&db.pool, "nonexistent")
        .await
        .expect("query mapping");
    assert!(node.is_none());
}

#[tokio::test]
async fn resolving_the_same_table_and_kind_twice_is_idempotent() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let first = resolve_node(&db.pool, "orders", NodeKind::Source)
        .await
        .expect("first resolution");
    let second = resolve_node(&db.pool, "orders", NodeKind::Source)
        .await
        .expect("second resolution is idempotent");

    assert_eq!(first.id, second.id);
    assert!(second.is_source);
    assert!(!second.is_target);
}

/// Resolving a table under the *other* kind merges into the same node
/// rather than erroring — a transform's target is a completely ordinary
/// table a later transform can subscribe to as its source
/// (chained/multi-hop transforms), so the same physical table legitimately
/// ends up resolved under both roles. See
/// [`engine::defs::NodeKind`]'s doc comment.
#[tokio::test]
async fn resolving_a_table_under_the_other_kind_merges_into_one_dual_role_node() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let first = resolve_node(&db.pool, "orders", NodeKind::Source)
        .await
        .expect("first resolution as a source");

    let second = resolve_node(&db.pool, "orders", NodeKind::Target)
        .await
        .expect("resolving the other role merges rather than erroring");

    assert_eq!(first.id, second.id);
    assert!(second.is_source);
    assert!(second.is_target);
}

/// A definition's target table naming an already-known *source* table
/// (from an unrelated transform) merges "orders" into a dual-role node
/// rather than being rejected — see [`engine::defs::NodeKind`]'s doc
/// comment on chained/multi-hop transforms.
#[tokio::test]
async fn a_definitions_target_matching_an_existing_source_node_merges_roles() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("first definition establishes 'orders' as a source node");

    create_definition(
        &db.pool,
        "TRANSFORM orders FROM upstream SELECT 1 AS out",
        &HashMap::new(),
    )
    .await
    .expect("second definition merges 'orders' into a dual-role node");

    let node = node_for_table(&db.pool, "orders")
        .await
        .expect("query orders node")
        .expect("orders node must exist");
    assert!(node.is_source);
    assert!(node.is_target);
}
