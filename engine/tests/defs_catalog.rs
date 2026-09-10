//! Integration tests for the transform-definition catalog (issue #23),
//! run against a real, ephemeral Postgres instance via the shared harness
//! (`testkit::TestCluster`).

use std::collections::HashMap;

use engine::defs::{
    CatalogError, ValidationError, ValueType, all_source_tables, create_definition,
    create_relationship, transforms_for_source,
};
use testkit::TestCluster;

fn columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|s| (s.to_string(), ValueType::Numeric))
        .collect()
}

/// Creates a minimal backing relation for a definition's source table
/// (issue #23's backfill enumerates it for real, via a live `regclass`/
/// catalog lookup) — a bare PK column is enough, since `validate()` checks
/// column references against the passed-in `source_columns` map, not the
/// live schema. Left unqualified so it lands via the pool's ambient
/// `search_path` (Trellis schema first), matching the schema
/// `create_definition` assumes for `def.source` today.
async fn create_bare_source_table(pool: &engine::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!("create table {name} (id serial primary key)"))
        .await
        .expect("create bare source table");
}

/// A table usable as either endpoint of a relationship declared by
/// [`create_relationship`] (issue #65's `all_source_tables` tests): `id` is
/// a serial primary key, suitable as a relationship's unique `to_col`
/// (cardinality `ToOne`, avoiding the to-many replica-identity requirement
/// these tests don't care about); `fk_col` is a plain, non-unique integer
/// column of the same type family, suitable as a relationship's `from_col`.
async fn create_bare_relationship_table(pool: &engine::pool::Pool, name: &str) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(&format!(
            "create table {name} (id serial primary key, fk_col integer)"
        ))
        .await
        .expect("create bare relationship table");
}

#[tokio::test]
async fn valid_definition_is_stored_and_retrievable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let def = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &columns(&["price", "tax"]),
    )
    .await
    .expect("valid definition should be stored");

    assert_eq!(def.source_version, 1);
    assert_eq!(def.def.target, "order_totals");
    assert_eq!(def.def.source, "orders");

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(subscribers[0].id, def.id);
    assert_eq!(subscribers[0].def.target, "order_totals");
}

/// Issue #63's write-path gap: the source-column type map a definition was
/// validated against must be persisted, not just returned transiently from
/// `create_definition` — `transforms_for_source` (what the physical apply
/// path loads) must read the exact same map back, mixed value types
/// included.
#[tokio::test]
async fn transforms_for_source_returns_the_persisted_source_column_types() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let source_columns: HashMap<String, ValueType> = HashMap::from([
        ("price".to_string(), ValueType::Numeric),
        ("label".to_string(), ValueType::Text),
        ("active".to_string(), ValueType::Boolean),
        ("author".to_string(), ValueType::Uuid),
    ]);

    let created = create_definition(
        &db.pool,
        "TRANSFORM order_labels FROM orders SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("valid definition should be stored");
    assert_eq!(created.source_columns, source_columns);

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(
        subscribers[0].source_columns, source_columns,
        "the persisted type map must survive a fresh read, not just the in-memory return value"
    );
}

/// Issue #69's batched read path: `transforms_for_source` now decodes every
/// subscriber's `source_columns` via a single `left join lateral
/// jsonb_each_text(...)` query rather than one query per row. A `left`
/// (not inner) join matters specifically for a definition whose
/// `source_columns` is empty — a literal-only field references no source
/// column at all — since an inner join would drop such a definition from
/// the result entirely instead of returning it with zero entries.
#[tokio::test]
async fn a_definition_with_no_source_columns_survives_the_left_join_read() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "widgets").await;

    let def = create_definition(
        &db.pool,
        "TRANSFORM constants FROM widgets SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("a literal-only definition needs no source columns");
    assert!(def.source_columns.is_empty());

    let subscribers = transforms_for_source(&db.pool, "widgets")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 1);
    assert_eq!(subscribers[0].id, def.id);
    assert!(
        subscribers[0].source_columns.is_empty(),
        "a definition with no source columns must still come back, with an empty map, \
         not disappear from the result"
    );
}

/// Issue #69's by-id grouping: with more than one definition subscribed to
/// the same source table, the single lateral-joined query's rows (one per
/// definition per source-column entry) must regroup correctly per
/// definition rather than smearing one definition's `source_columns` into
/// another's.
#[tokio::test]
async fn transforms_for_source_groups_multiple_subscribers_by_id() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let totals_columns = HashMap::from([
        ("price".to_string(), ValueType::Numeric),
        ("tax".to_string(), ValueType::Numeric),
    ]);
    let first = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &totals_columns,
    )
    .await
    .expect("first definition");

    let labels_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let second = create_definition(
        &db.pool,
        "TRANSFORM order_labels FROM orders SELECT label AS out",
        &labels_columns,
    )
    .await
    .expect("second definition against the same source table");

    assert!(first.id < second.id);

    let subscribers = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping");
    assert_eq!(subscribers.len(), 2);

    // `order by t.id` in the query must be reflected in the grouped result.
    assert_eq!(subscribers[0].id, first.id);
    assert_eq!(subscribers[0].def.target, "order_totals");
    assert_eq!(subscribers[0].source_columns, totals_columns);

    assert_eq!(subscribers[1].id, second.id);
    assert_eq!(subscribers[1].def.target, "order_labels");
    assert_eq!(subscribers[1].source_columns, labels_columns);
}

#[tokio::test]
async fn creating_a_new_definition_bumps_the_source_tables_version() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;
    create_bare_source_table(&db.pool, "customers").await;

    let first = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition");
    assert_eq!(first.source_version, 1);

    let second = create_definition(
        &db.pool,
        "TRANSFORM order_discounts FROM orders SELECT price AS discount",
        &columns(&["price"]),
    )
    .await
    .expect("second definition against the same source table");
    assert_eq!(second.source_version, 2);

    // A definition against an unrelated source table starts its own,
    // independent version counter.
    let unrelated = create_definition(
        &db.pool,
        "TRANSFORM customer_names FROM customers SELECT name AS full_name",
        &columns(&["name"]),
    )
    .await
    .expect("definition against a different source table");
    assert_eq!(unrelated.source_version, 1);
}

#[tokio::test]
async fn a_column_cycle_within_a_target_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT y AS x, x AS y",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::Cycle { .. }) => {}
        other => panic!("expected a Cycle validation error, got {other:?}"),
    }
}

/// Issue #63's end-to-end passthrough bar: a `Text`-typed source column
/// parses, validates, and is stored as a calculated field with no operator
/// applied to it.
#[tokio::test]
async fn a_text_column_passthrough_is_stored_and_retrievable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "widgets").await;

    let source_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let def = create_definition(
        &db.pool,
        "TRANSFORM labels FROM widgets SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("text passthrough should be a valid definition");

    assert_eq!(def.def.target, "labels");
}

/// Issue #63's type-mismatch bar: `text_col + 1` must be rejected at
/// validation time with a clear [`ValidationError::TypeMismatch`], not a
/// panic.
#[tokio::test]
async fn adding_a_text_column_to_a_number_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let source_columns = HashMap::from([("label".to_string(), ValueType::Text)]);
    let err = create_definition(
        &db.pool,
        "TRANSFORM labels FROM widgets SELECT label + 1 AS out",
        &source_columns,
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::TypeMismatch {
            field,
            expected,
            found,
        }) => {
            assert_eq!(field, "out");
            assert_eq!(expected, ValueType::Numeric);
            assert_eq!(found, ValueType::Text);
        }
        other => panic!("expected a TypeMismatch validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unresolved_column_reference_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT mystery_column AS x",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnresolvedColumn { column, .. }) => {
            assert_eq!(column, "mystery_column");
        }
        other => panic!("expected an UnresolvedColumn validation error, got {other:?}"),
    }
}

#[tokio::test]
async fn cross_join_and_partial_data_definitions_are_rejected_cleanly() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let cross_join = create_definition(
        &db.pool,
        "TRANSFORM t FROM s JOIN other ON s.id = other.id SELECT a AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();
    assert!(matches!(cross_join, CatalogError::Parse(_)));

    let partial_data = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a AS x WHERE a = b",
        &columns(&["a"]),
    )
    .await
    .unwrap_err();
    assert!(matches!(partial_data, CatalogError::Parse(_)));

    // None of the rejected attempts should have left a row behind.
    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

/// A `<rel>.<column>` relationship path is real grammar (issue #25) that
/// parses successfully and is now validated against catalog-resolved
/// relationship metadata (issue #40). With no relationship named `product`
/// declared, the head is unknown, so validation rejects it with
/// `UnknownRelationship` — naming the offending field — rather than the
/// old blanket "relationship paths are unsupported" rejection. This exercises
/// that the catalog's parse-then-validate pipeline resolves relationships and
/// routes an unknown one through validation, not a parse-time short-circuit
/// the way `JOIN` still does.
#[tokio::test]
async fn a_relationship_path_to_an_unknown_relationship_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT product.category_name AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnknownRelationship { field, rel }) => {
            assert_eq!(field, "x");
            assert_eq!(rel, "product");
        }
        other => panic!("expected an UnknownRelationship validation error, got {other:?}"),
    }

    // The rejected attempt should not have left a row behind.
    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

/// Unlike `JOIN`/relationship paths, `GROUP BY` is a real, now-supported
/// construct, so an aggregate definition parses successfully and instead
/// fails at validation if its grouping column isn't a real source column —
/// exercising that the catalog's parse-then-validate pipeline routes an
/// aggregate definition through validation rather than short-circuiting at
/// parse time the way the still-unsupported constructs above do.
#[tokio::test]
async fn an_aggregate_definition_with_an_unresolvable_group_by_column_is_rejected_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = create_definition(
        &db.pool,
        "TRANSFORM t FROM s GROUP BY a SELECT a AS x",
        &HashMap::new(),
    )
    .await
    .unwrap_err();

    match err {
        CatalogError::Validate(ValidationError::UnresolvedGroupByColumn { column }) => {
            assert_eq!(column, "a");
        }
        other => panic!("expected an UnresolvedGroupByColumn validation error, got {other:?}"),
    }

    let subscribers = transforms_for_source(&db.pool, "s")
        .await
        .expect("query mapping");
    assert!(subscribers.is_empty());
}

#[tokio::test]
async fn source_to_transform_mapping_reflects_a_newly_created_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    let before = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping before creation");
    assert!(before.is_empty());

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("create definition");

    let after = transforms_for_source(&db.pool, "orders")
        .await
        .expect("query mapping after creation");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].def.target, "order_totals");

    // A different source table's mapping is unaffected.
    let unrelated = transforms_for_source(&db.pool, "customers")
        .await
        .expect("query unrelated mapping");
    assert!(unrelated.is_empty());
}

/// Issue #80: `CatalogError::Db`'s `Display` must surface the real Postgres
/// error text (e.g. the duplicate-key detail), not just the bare "db error"
/// `tokio_postgres::Error`'s own `Display` prints on its own.
#[tokio::test]
async fn a_duplicate_target_table_surfaces_the_underlying_postgres_detail() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;
    create_bare_source_table(&db.pool, "customers").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("first definition should be stored");

    let err = create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM customers SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, CatalogError::Db(_)));
    let message = err.to_string();
    assert!(
        message.contains("duplicate key value violates unique constraint"),
        "expected the underlying Postgres detail in the error message, got: {message}"
    );
    assert!(
        message.contains("transform_definitions_target_table_key"),
        "expected the violated constraint's name in the error message, got: {message}"
    );
}

/// Issue #65, case 1: with no relationships declared at all, `all_source_tables`
/// must still behave exactly as it did pre-#65 — just the anchor
/// `source_table` of each registered transform.
#[tokio::test]
async fn all_source_tables_with_no_relationships_returns_only_anchor_tables() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_source_table(&db.pool, "orders").await;

    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price AS total",
        &columns(&["price"]),
    )
    .await
    .expect("valid definition should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(tables, vec!["orders".to_string()]);
}

/// Issue #65, case 2: a transform anchored on `a`, plus a relationship
/// `a -> b` (`a` is `from_table`, `b` is `to_table`), must pull `b` into the
/// result too — the CDC gap the issue reports, where a calculated field on
/// `a` reading a relationship path into `b` needs `b`'s writes captured.
#[tokio::test]
async fn all_source_tables_follows_a_single_relationship_hop() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "a").await;
    create_bare_relationship_table(&db.pool, "b").await;

    create_definition(
        &db.pool,
        "TRANSFORM a_calc FROM a SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM a.fk_col TO b.id")
        .await
        .expect("valid relationship should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(tables, vec!["a".to_string(), "b".to_string()]);
}

/// Issue #65, case 3: a multi-hop chain — relationship `a -> b` and
/// `b -> c` — with the transform anchored only on `a`, must resolve the
/// transitive closure, pulling in both `b` and `c`, not just the direct
/// hop.
#[tokio::test]
async fn all_source_tables_follows_a_multi_hop_relationship_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "a").await;
    create_bare_relationship_table(&db.pool, "b").await;
    create_bare_relationship_table(&db.pool, "c").await;

    create_definition(
        &db.pool,
        "TRANSFORM a_calc FROM a SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM a.fk_col TO b.id")
        .await
        .expect("a -> b relationship should be stored");
    create_relationship(&db.pool, "RELATIONSHIP r2 FROM b.fk_col TO c.id")
        .await
        .expect("b -> c relationship should be stored");

    let mut tables = all_source_tables(&db.pool).await.expect("query mapping");
    tables.sort();
    assert_eq!(
        tables,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

/// Issue #65, case 4: a relationship declared on a table that is not any
/// transform's `source_table` must not leak its `to_table` into the
/// result — only tables reachable from an actual registered transform's
/// anchor should appear. `x -> y` here is never seeded (neither `x` nor `y`
/// anchors a transform), so both must be absent even though the
/// relationship itself is validly stored; only `z`, the real transform's
/// anchor, should come back.
#[tokio::test]
async fn all_source_tables_does_not_leak_relationships_unreachable_from_any_transform() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_bare_relationship_table(&db.pool, "x").await;
    create_bare_relationship_table(&db.pool, "y").await;
    create_bare_source_table(&db.pool, "z").await;

    create_relationship(&db.pool, "RELATIONSHIP r1 FROM x.fk_col TO y.id")
        .await
        .expect("valid relationship should be stored");

    create_definition(
        &db.pool,
        "TRANSFORM z_calc FROM z SELECT 1 AS x",
        &HashMap::new(),
    )
    .await
    .expect("valid definition should be stored");

    let tables = all_source_tables(&db.pool).await.expect("query mapping");
    assert_eq!(tables, vec!["z".to_string()]);
}

/// Issue #36's exact repro: a 1-1 transform with a pass-through field named
/// the same as the source column it reads (`author AS author`) must not be
/// rejected as a self-referencing cycle, even alongside other calculated
/// fields on the same target. `is_self_passthrough` in `validate.rs`
/// already exempts `column == field.name` when `column` is a source
/// column — this pins that exemption against the issue's literal schema
/// and DSL so a regression here fails loudly.
#[tokio::test]
async fn a_passthrough_field_sharing_its_source_columns_name_is_not_a_cycle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let client = db.pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 name character varying not null,
                 constraint authors_pkey primary key (id)
             );
             create table posts (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 title text,
                 body text,
                 author integer not null,
                 constraint posts_pkey primary key (id),
                 constraint posts_author_fkey foreign key (author)
                     references authors(id) on update cascade on delete cascade
             );",
        )
        .await
        .expect("seed authors and posts");
    drop(client);

    let source_columns: HashMap<String, ValueType> = HashMap::from([
        ("author".to_string(), ValueType::Numeric),
        ("body".to_string(), ValueType::Text),
    ]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author, \
         regexp_count(body, '(^|[^A-Za-z0-9_])') as word_count, \
         octet_length(body) as byte_size",
        &source_columns,
    )
    .await
    .expect("author AS author passthrough must not be rejected as a self-reference cycle");
}

/// Seeds the `authors`/`posts` schema from `poc/schema_dump.sql` (the same
/// DDL issue #36's test above uses) — the exact repro schema issue #47's
/// report is filed against.
async fn create_authors_and_posts(pool: &engine::pool::Pool) {
    let client = pool.get().await.expect("get connection");
    client
        .batch_execute(
            "create table authors (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 name character varying not null,
                 constraint authors_pkey primary key (id)
             );
             create table posts (
                 id integer not null,
                 created_at timestamp with time zone default now() not null,
                 title text,
                 body text,
                 author integer not null,
                 constraint posts_pkey primary key (id),
                 constraint posts_author_fkey foreign key (author)
                     references authors(id) on update cascade on delete cascade
             );",
        )
        .await
        .expect("seed authors and posts");
}

/// Issue #47: a 1-1 transform (`posts_calc`) against `posts` must succeed
/// regardless of `posts`'s replica identity — [`KeySpace::OneToOne`]
/// derivations are a pure function of the *current* row and never need an
/// old image.
#[tokio::test]
async fn a_one_to_one_transform_succeeds_regardless_of_replica_identity() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author",
        &source_columns,
    )
    .await
    .expect("a 1-1 transform needs no old image, so it must succeed at default replica identity");
}

/// Issue #47's exact repro: an aggregate (`GROUP BY`) transform's
/// delta-maintenance path (`apply_aggregate.rs`) needs the source row's old
/// image on delete/update/re-parent to find which group to decrement — a
/// requirement `defs::catalog::create_definition` never checked, so defining
/// `posts_totals` (`GROUP BY author`) against `posts` at its default (PK-only)
/// replica identity must be rejected at define time, naming the exact `ALTER
/// TABLE posts REPLICA IDENTITY FULL;` fix — not silently accepted only to
/// corrupt totals later on a delete or non-key update.
#[tokio::test]
async fn an_aggregate_transform_against_default_replica_identity_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    // The 1-1 transform above is unaffected by `posts`'s replica identity
    // and should still succeed even though the aggregate attempt below will
    // be rejected.
    create_definition(
        &db.pool,
        "TRANSFORM posts_calc FROM posts SELECT author AS author",
        &source_columns,
    )
    .await
    .expect("1-1 transform should succeed regardless of replica identity");

    let err = create_definition(
        &db.pool,
        "TRANSFORM posts_totals FROM posts GROUP BY author SELECT author AS author, \
         COUNT(*) AS post_count",
        &source_columns,
    )
    .await
    .unwrap_err();

    match &err {
        CatalogError::ReplicaIdentityRequired(_) => {}
        other => panic!("expected ReplicaIdentityRequired, got {other:?}"),
    }
    let message = err.to_string();
    assert!(
        message.contains("ALTER TABLE posts REPLICA IDENTITY FULL;"),
        "expected the exact ALTER TABLE fix in the error message, got: {message}"
    );

    // The rejected attempt must not have left a row behind.
    let subscribers = transforms_for_source(&db.pool, "posts")
        .await
        .expect("query mapping");
    assert_eq!(
        subscribers.len(),
        1,
        "only posts_calc should be registered; posts_totals must not have been persisted"
    );
    assert_eq!(subscribers[0].def.target, "posts_calc");
}

/// Issue #47: the same aggregate definition succeeds once `posts` has
/// `REPLICA IDENTITY FULL`, which puts every column (including `author`) into
/// delete/update pre-images.
#[tokio::test]
async fn an_aggregate_transform_against_replica_identity_full_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    create_authors_and_posts(&db.pool).await;
    {
        let client = db.pool.get().await.expect("get connection");
        client
            .batch_execute("alter table posts replica identity full")
            .await
            .expect("set replica identity full");
    }

    let source_columns: HashMap<String, ValueType> =
        HashMap::from([("author".to_string(), ValueType::Numeric)]);

    create_definition(
        &db.pool,
        "TRANSFORM posts_totals FROM posts GROUP BY author SELECT author AS author, \
         COUNT(*) AS post_count",
        &source_columns,
    )
    .await
    .expect("aggregate transform with REPLICA IDENTITY FULL should be accepted");
}
