//! Integration tests for target-table DDL generation (issue #25), run
//! against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    DdlError, create_aggregate_target_table, create_target_table, source_primary_key,
};

fn order_totals_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
    }
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

#[tokio::test]
async fn target_table_is_created_with_inherited_pk_and_numeric_calculated_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    assert_eq!(pk.name, "id");
    assert_eq!(pk.data_type, "integer");

    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("create target table");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");

    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("total".to_string(), "numeric".to_string()),
        ]
    );

    let pk_columns = client
        .query(
            "select a.attname::text
             from pg_index i
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary",
            &[&def.target],
        )
        .await
        .expect("introspect target primary key");
    let pk_columns: Vec<String> = pk_columns.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(pk_columns, vec!["id".to_string()]);
}

#[tokio::test]
async fn target_table_defaults_to_the_public_schema_not_the_trellis_instance_schema() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");

    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("create target table");

    let schema: String = client
        .query_one(
            "select table_schema from information_schema.tables where table_name = $1",
            &[&def.target],
        )
        .await
        .expect("introspect target table's schema")
        .get(0);
    assert_eq!(schema, "public");
    assert_ne!(
        schema, "trellis",
        "target table must not default into the trellis instance schema"
    );
}

#[tokio::test]
async fn target_table_is_created_in_a_configured_non_default_schema() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             create schema analytics",
        )
        .await
        .expect("seed source table and target schema");

    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");

    create_target_table(
        &db.pool,
        &def,
        "analytics",
        &pk,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("create target table in the configured target schema");

    let schemas: Vec<String> = client
        .query(
            "select table_schema from information_schema.tables where table_name = $1",
            &[&def.target],
        )
        .await
        .expect("introspect target table's schema")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(schemas, vec!["analytics".to_string()]);
}

#[tokio::test]
async fn creating_the_target_table_twice_is_a_no_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");

    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("first create");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &numeric_columns(&["price", "tax"]),
    )
    .await
    .expect("second create is idempotent");
}

#[tokio::test]
async fn text_and_boolean_calculated_fields_get_matching_target_column_types() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute("create table widgets (id integer primary key, label text, active boolean)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "widget_summaries".to_string(),
        source: "widgets".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "label_out".to_string(),
                expr: Expr::Column("label".to_string()),
            },
            FieldDef {
                name: "active_out".to_string(),
                expr: Expr::Column("active".to_string()),
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([
        ("label".to_string(), ValueType::Text),
        ("active".to_string(), ValueType::Boolean),
    ]);

    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table with text/boolean columns");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("label_out".to_string(), "text".to_string()),
            ("active_out".to_string(), "boolean".to_string()),
        ]
    );
}

/// Issue #79: a `uuid` source column must be passthrough-able onto a target
/// table (bare `col AS col`, no arithmetic/regex support required).
#[tokio::test]
async fn uuid_column_passthrough_gets_a_matching_target_column_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table comments (
                 id uuid primary key default gen_random_uuid(),
                 author uuid not null,
                 body text
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "comments_calc".to_string(),
        source: "comments".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "author".to_string(),
            expr: Expr::Column("author".to_string()),
        }],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([("author".to_string(), ValueType::Uuid)]);

    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    assert_eq!(pk.data_type, "uuid");

    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table with a uuid passthrough column");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "uuid".to_string()),
            ("author".to_string(), "uuid".to_string()),
        ]
    );
}

/// Issue #45: a bare passthrough of an integer-family source column keeps the
/// source column's *concrete* Postgres type on the target (`integer` stays
/// `integer`, `bigint` stays `bigint`, `smallint` stays `smallint`), instead
/// of collapsing through `ValueType::Numeric` to `numeric` — so the target
/// column stays eligible as a relationship join key. A non-passthrough field
/// (arithmetic over the same columns) still widens to `numeric`, since its
/// result genuinely can't be narrower.
#[tokio::test]
async fn integer_family_passthrough_keeps_its_concrete_type_but_arithmetic_widens() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table posts (
                 id integer primary key,
                 author integer,
                 views bigint,
                 rank smallint
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "posts_calc".to_string(),
        source: "posts".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            // Rename passthrough: `author AS author_id` — still a bare
            // reference to one integer source column.
            FieldDef {
                name: "author_id".to_string(),
                expr: Expr::Column("author".to_string()),
            },
            FieldDef {
                name: "views".to_string(),
                expr: Expr::Column("views".to_string()),
            },
            FieldDef {
                name: "rank".to_string(),
                expr: Expr::Column("rank".to_string()),
            },
            // Arithmetic over an integer column is not a passthrough, so it
            // keeps widening to `numeric`.
            FieldDef {
                name: "author_plus_one".to_string(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("author".to_string())),
                    rhs: Box::new(Expr::NumberLiteral("1".to_string())),
                },
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = numeric_columns(&["author", "views", "rank"]);

    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("author_id".to_string(), "integer".to_string()),
            ("views".to_string(), "bigint".to_string()),
            ("rank".to_string(), "smallint".to_string()),
            ("author_plus_one".to_string(), "numeric".to_string()),
        ]
    );
}

/// Issue #45 review: a passthrough of a genuinely `numeric`-family source
/// column (`numeric(10,2)`, `real`, `double precision` — as opposed to the
/// `integer`/`bigint`/`smallint` family the fix targets) must still get its
/// own correct concrete type on the target table, not something wrong.
/// `information_schema.columns.data_type` strips precision/scale modifiers
/// (`numeric(10,2)` reads back as just `numeric`), so this queries
/// `pg_catalog.format_type` directly to confirm the modifiers survive too.
#[tokio::test]
async fn numeric_family_passthrough_keeps_its_own_concrete_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table posts (
                 id integer primary key,
                 price numeric(10,2),
                 ratio real,
                 amount double precision
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "posts_calc".to_string(),
        source: "posts".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "price".to_string(),
                expr: Expr::Column("price".to_string()),
            },
            FieldDef {
                name: "ratio".to_string(),
                expr: Expr::Column("ratio".to_string()),
            },
            FieldDef {
                name: "amount".to_string(),
                expr: Expr::Column("amount".to_string()),
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = numeric_columns(&["price", "ratio", "amount"]);

    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table");

    let columns = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attnum > 0
               and not a.attisdropped
             order by a.attnum",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("price".to_string(), "numeric(10,2)".to_string()),
            ("ratio".to_string(), "real".to_string()),
            ("amount".to_string(), "double precision".to_string()),
        ]
    );
}

/// Issue #79's downstream case: a `uuid` column carried through a 1-1
/// passthrough must also work as an aggregate `GROUP BY` key.
#[tokio::test]
async fn uuid_column_works_as_an_aggregate_group_by_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table comments (
                 id uuid primary key default gen_random_uuid(),
                 author uuid not null,
                 word_count numeric
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "comments_by_author".to_string(),
        source: "comments".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["author".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "author".to_string(),
                expr: Expr::Column("author".to_string()),
            },
            FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("word_count".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([
        ("author".to_string(), ValueType::Uuid),
        ("word_count".to_string(), ValueType::Numeric),
    ]);

    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table with a uuid group-by key");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("author".to_string(), "uuid".to_string()),
            ("total_words".to_string(), "numeric".to_string()),
            ("__total_words_count".to_string(), "bigint".to_string()),
        ]
    );

    let pk_columns = client
        .query(
            "select a.attname::text
             from pg_index i
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary",
            &[&def.target],
        )
        .await
        .expect("introspect target primary key");
    let pk_columns: Vec<String> = pk_columns.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(pk_columns, vec!["author".to_string()]);
}

#[tokio::test]
async fn aggregate_target_table_gets_a_composite_primary_key_from_the_grouping_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "order_totals".to_string(),
        source: "order_line_items".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["order_id".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "order_id".to_string(),
                expr: Expr::Column("order_id".to_string()),
            },
            FieldDef {
                name: "total_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);

    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("order_id".to_string(), "numeric".to_string()),
            ("total_amount".to_string(), "numeric".to_string()),
            ("__total_amount_count".to_string(), "bigint".to_string()),
        ],
        "a SUM field gets a hidden running-count partial alongside its visible column \
         (issue #11 review fix: SUM needs to distinguish \"sum of nothing\" from \"sum \
         that nets to zero\", the same way AVG already does)"
    );

    let pk_columns = client
        .query(
            "select a.attname::text
             from pg_index i
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary",
            &[&def.target],
        )
        .await
        .expect("introspect target primary key");
    let pk_columns: Vec<String> = pk_columns.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(pk_columns, vec!["order_id".to_string()]);
}

/// Issue #48: `SUM(amount)` and `AVG(amount)` aggregate the exact same
/// argument expression, so they can safely share one hidden running-count
/// partial instead of each getting its own `__{field}_count` column — the
/// two counts would always be identical since they count non-null values of
/// the same expression over the same rows.
#[tokio::test]
async fn aggregate_columns_sharing_an_argument_share_one_count_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_line_items (
                 id integer primary key, order_id integer, amount numeric
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "order_totals".to_string(),
        source: "order_line_items".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["order_id".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "order_id".to_string(),
                expr: Expr::Column("order_id".to_string()),
            },
            FieldDef {
                name: "total_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
            FieldDef {
                name: "avg_amount".to_string(),
                expr: Expr::FunctionCall {
                    name: "AVG".to_string(),
                    args: vec![Expr::Column("amount".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([
        ("order_id".to_string(), ValueType::Numeric),
        ("amount".to_string(), ValueType::Numeric),
    ]);

    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("order_id".to_string(), "numeric".to_string()),
            ("total_amount".to_string(), "numeric".to_string()),
            ("__total_amount_count".to_string(), "bigint".to_string()),
            ("avg_amount".to_string(), "numeric".to_string()),
            ("__avg_amount_sum".to_string(), "numeric".to_string()),
        ],
        "total_amount (SUM(amount)) and avg_amount (AVG(amount)) aggregate the same \
         argument, so only ONE shared count column should be created — named after \
         whichever field is encountered first — instead of a duplicate per field \
         (issue #48)"
    );
}

/// Issue #48's own example (`SUM(word_count)` and `SUM(byte_size)`) aggregates
/// two DIFFERENT source columns. Unlike the same-argument case above, these
/// must NOT share a count column: the two source columns can have different
/// NULL patterns across the same group's rows, so their non-null counts can
/// legitimately diverge. Merging them would silently corrupt SUM's
/// NULL-vs-zero disambiguation for whichever field's count "wins".
#[tokio::test]
async fn aggregate_columns_over_different_arguments_keep_separate_count_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table posts_calc (
                 id integer primary key, author integer, word_count numeric, byte_size numeric
             )",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "posts_totals".to_string(),
        source: "posts_calc".to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["author".to_string()],
        },
        fields: vec![
            FieldDef {
                name: "author".to_string(),
                expr: Expr::Column("author".to_string()),
            },
            FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("word_count".to_string())],
                },
            },
            FieldDef {
                name: "total_bytes".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::Column("byte_size".to_string())],
                },
            },
        ],
        predicate: Predicate::True,
    };
    let source_columns = HashMap::from([
        ("author".to_string(), ValueType::Numeric),
        ("word_count".to_string(), ValueType::Numeric),
        ("byte_size".to_string(), ValueType::Numeric),
    ]);

    create_aggregate_target_table(&db.pool, &def, "public", &source_columns)
        .await
        .expect("create aggregate target table");

    let columns = client
        .query(
            "select column_name, data_type
             from information_schema.columns
             where table_name = $1
             order by ordinal_position",
            &[&def.target],
        )
        .await
        .expect("introspect target columns");
    let columns: Vec<(String, String)> = columns
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("author".to_string(), "numeric".to_string()),
            ("total_words".to_string(), "numeric".to_string()),
            ("__total_words_count".to_string(), "bigint".to_string()),
            ("total_bytes".to_string(), "numeric".to_string()),
            ("__total_bytes_count".to_string(), "bigint".to_string()),
        ],
        "total_words and total_bytes sum different source columns, so each keeps its \
         own count column even though issue #48's naive request would have merged them \
         into one — doing so would be unsafe whenever the two columns' NULL patterns \
         diverge across a group's rows"
    );
}

#[tokio::test]
async fn a_source_table_without_a_primary_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute("create table orders (id integer, price numeric)")
        .await
        .expect("seed source table without a primary key");

    let err = source_primary_key(&db.pool, "orders").await.unwrap_err();
    match err {
        DdlError::NoPrimaryKey { source_table } => assert_eq!(source_table, "orders"),
        other => panic!("expected NoPrimaryKey, got {other:?}"),
    }
}

#[tokio::test]
async fn a_source_table_with_a_composite_primary_key_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("connection");

    client
        .batch_execute(
            "create table order_lines (order_id integer, line_no integer, price numeric,
             primary key (order_id, line_no))",
        )
        .await
        .expect("seed source table with a composite primary key");

    let err = source_primary_key(&db.pool, "order_lines")
        .await
        .unwrap_err();
    match err {
        DdlError::CompositePrimaryKeyUnsupported { source_table } => {
            assert_eq!(source_table, "order_lines")
        }
        other => panic!("expected CompositePrimaryKeyUnsupported, got {other:?}"),
    }
}
