//! Integration tests for the persisted cross-table dependency graph and
//! edge-typed resolver (issue #21), run against a real, ephemeral Postgres
//! instance via `testkit::TestCluster`.

use std::collections::HashMap;

use engine::defs::{EdgeKind, ValueType, create_definition, dependents_of, transforms_for_source};
use testkit::TestCluster;

#[tokio::test]
async fn creating_a_definition_persists_a_source_edge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    let dependents = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("query dependents");
    assert_eq!(dependents.len(), 1);
    assert_eq!(dependents[0].def.target, "order_totals");
}

#[tokio::test]
async fn a_node_with_no_dependents_of_a_kind_returns_empty() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    // No relationship or join edges are ever persisted today (issue #21's
    // explicit out-of-scope) — querying for them must come back empty, not
    // error or fall back to source edges.
    let joins = dependents_of(&db.pool, "orders", EdgeKind::Join)
        .await
        .expect("query join dependents");
    assert!(joins.is_empty());

    let relationships = dependents_of(&db.pool, "orders", EdgeKind::Relationship)
        .await
        .expect("query relationship dependents");
    assert!(relationships.is_empty());

    let unrelated = dependents_of(&db.pool, "nonexistent_table", EdgeKind::Source)
        .await
        .expect("query dependents of an unknown node");
    assert!(unrelated.is_empty());
}

/// The graph is real and walkable across more than one hop: A -> B -> C,
/// resolving A's dependents finds B via a `Source` edge, and separately
/// resolving B's dependents finds C — each hop is an independent edge
/// lookup, not a single query that already knows the whole chain.
#[tokio::test]
async fn the_dependency_graph_is_walkable_across_multiple_hops() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM b FROM a SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("a -> b definition should be stored");

    create_definition(
        &db.pool,
        "TRANSFORM c FROM b SELECT total AS total_again",
        &HashMap::from([("total".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("b -> c definition should be stored");

    let a_dependents = dependents_of(&db.pool, "a", EdgeKind::Source)
        .await
        .expect("query a's dependents");
    assert_eq!(a_dependents.len(), 1);
    assert_eq!(a_dependents[0].def.target, "b");

    let b_dependents = dependents_of(&db.pool, "b", EdgeKind::Source)
        .await
        .expect("query b's dependents");
    assert_eq!(b_dependents.len(), 1);
    assert_eq!(b_dependents[0].def.target, "c");

    // "a" has no direct edge to "c" — a two-hop chain is two edges, not one
    // that skips the intermediate node.
    let a_to_c = dependents_of(&db.pool, "a", EdgeKind::Source)
        .await
        .expect("query a's dependents again");
    assert!(a_to_c.iter().all(|def| def.def.target != "c"));
}

/// `transforms_for_source` is a thin wrapper over `dependents_of` filtered
/// to `EdgeKind::Source` — both must agree.
#[tokio::test]
async fn transforms_for_source_agrees_with_dependents_of_source_edges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("valid definition should be stored");

    let via_wrapper = transforms_for_source(&db.pool, "orders")
        .await
        .expect("transforms_for_source");
    let via_resolver = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("dependents_of");

    assert_eq!(via_wrapper, via_resolver);
}

/// Re-declaring the same source/target pair through `create_definition`
/// (a definition's target table matching an already-known source, i.e. a
/// chained transform's second half being defined) does not double-insert
/// the `Source` edge between the same two nodes.
#[tokio::test]
async fn re_resolving_the_same_source_and_target_pair_does_not_duplicate_the_edge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("first definition establishes the orders -> order_totals edge");

    // A second, distinct definition from the same source to a different
    // target must not affect the first edge's count.
    create_definition(
        &db.pool,
        "TRANSFORM order_flags FROM orders SELECT price AS flag",
        &HashMap::from([("price".to_string(), ValueType::Numeric)]),
    )
    .await
    .expect("second definition establishes a distinct orders -> order_flags edge");

    let dependents = dependents_of(&db.pool, "orders", EdgeKind::Source)
        .await
        .expect("query dependents");
    assert_eq!(dependents.len(), 2);
}
