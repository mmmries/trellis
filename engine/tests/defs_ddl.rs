//! Integration tests for target-table DDL generation (issue #25), run
//! against a real, ephemeral Postgres instance via `testkit::TestCluster`.

use std::collections::HashMap;

use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::{
    DdlError, create_aggregate_target_table, create_target_table, source_primary_key,
};
use testkit::TestCluster;

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

    create_target_table(&db.pool, &def, &pk, &numeric_columns(&["price", "tax"]))
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

    create_target_table(&db.pool, &def, &pk, &numeric_columns(&["price", "tax"]))
        .await
        .expect("first create");
    create_target_table(&db.pool, &def, &pk, &numeric_columns(&["price", "tax"]))
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
    create_target_table(&db.pool, &def, &pk, &source_columns)
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

    create_target_table(&db.pool, &def, &pk, &source_columns)
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

    create_aggregate_target_table(&db.pool, &def, &source_columns)
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

    create_aggregate_target_table(&db.pool, &def, &source_columns)
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
