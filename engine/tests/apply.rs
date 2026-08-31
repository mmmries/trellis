//! Integration tests for apply ∪ mark-drained (issue #11, stage 05),
//! 1-1/scalar subset only, run against a real, ephemeral Postgres instance
//! via the shared harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md for the
//! design these tests hold the implementation to. Every test builds its own
//! source table, definition, and target table by hand (mirroring
//! `defs_ddl.rs`/`defs_oracle.rs`'s convention) and stages changes directly
//! into the ring (mirroring `claims.rs`/`fold.rs`'s convention), rather than
//! going through CDC intake — intake is out of scope here.

use std::collections::HashMap;

use engine::config::DEFAULT_SCHEMA;
use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::{create_definition, create_target_table, recompute, source_primary_key};
use engine::staging::apply::{self, ApplyError};
use engine::staging::{SegmentState, TRUNCATE_SENTINEL_KEY, claim, fold};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

/// Connects directly to `dsn` (bypassing `engine::Pool`), matching
/// `claims.rs`/`fold.rs`'s convention.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use engine::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn segment_state(client: &Client, seg_seq: i64) -> SegmentState {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    SegmentState::from_sql(&state).unwrap_or_else(|| panic!("unrecognized state {state:?}"))
}

async fn drained_mask(client: &Client, seg_seq: i64) -> i64 {
    client
        .query_one(
            "select drained_mask from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("read drained_mask")
        .get(0)
}

/// Stages one image-bearing (CDC-shaped) change directly into `table`.
async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

/// Stages a truncate sentinel directly into `table`, for `src_table`.
async fn insert_truncate_row(client: &Client, table: &str, src_table: &str) {
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ($1, $2, 'truncate', 0)"
            ),
            &[&src_table, &TRUNCATE_SENTINEL_KEY],
        )
        .await
        .unwrap_or_else(|e| panic!("insert truncate row into {table} failed: {e}"));
}

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

/// Runs one full drain attempt against `seg_seq` the same way [`drain_once`]
/// does, and panics with the underlying error on failure — this test file's
/// standard "drain and expect it to succeed" step.
async fn drain(pool: &engine::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    apply::drain_once(pool, seg_seq, claimed_by, 1, "trellis_apply_test")
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something")
}

#[tokio::test]
async fn drain_matches_the_oracle_across_an_insert_update_and_delete() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // The live source table's *final* state: order 3 has already been
    // deleted (a real CDC delete would have removed it from `orders` too;
    // this test's "live" table always reflects that end state), 1 and 2
    // are present at the values their staged changes carry.
    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    // Pre-populate a target row for order 3, standing in for data an
    // earlier drain wrote before this batch's delete arrives.
    client
        .execute("insert into order_totals (id, total) values (3, 999)", &[])
        .await
        .expect("pre-populate target row for the key this batch deletes");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "update",
        Some(r#"{"price":"15.00","tax":"1.00"}"#),
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "3",
        "delete",
        Some(r#"{"price":"5.00","tax":"0.50"}"#),
        None,
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2, "orders 1 and 2 must be written");
    assert_eq!(outcome.keys_deleted, 1, "order 3 must be deleted");

    let oracle = recompute(&db.pool, &def, &pk.name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len(), oracle.len());
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

#[tokio::test]
async fn a_fully_drained_single_bucket_batch_flips_the_segment_to_drained() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Sealed);

    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert!(
        outcome.batch_drained,
        "a lone worker's one claim over a single-bucket batch must drain it fully"
    );
    assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Drained);
    assert_eq!(drained_mask(&client, seg_seq).await, 1);
}

#[tokio::test]
async fn a_claim_lost_mid_drain_rolls_back_and_applies_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    // Phase 1 by hand: claim, own, fold, commit — exactly what
    // `drain_once`'s opening block does.
    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    assert!(!folded.is_empty());

    // Simulate the claim being reclaimed out from under this worker in the
    // gap between phase 1 and phase 3 (e.g. a TTL sweep).
    client
        .execute(
            "delete from seg_claims where seg_seq = $1 and claimed_by = 'worker'",
            &[&seg_seq],
        )
        .await
        .expect("simulate a reclaim");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");
    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(&txn, seg_seq, "worker", &plan, "trellis_apply_test")
        .await
        .expect_err("the claim is gone; completion must fail");
    assert!(matches!(err, ApplyError::ClaimLost), "got {err:?}");
    txn.rollback().await.expect("rollback phase 3");

    // Nothing was applied: the insert never got past the rolled-back
    // transaction.
    let count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(
        count, 0,
        "the rolled-back apply must not have written anything"
    );
    assert_eq!(
        drained_mask(&client, seg_seq).await,
        0,
        "the rolled-back completion must not have marked any bucket drained"
    );
}

#[tokio::test]
async fn a_definition_change_on_a_touched_source_trips_the_version_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // A second definition landing against `orders` after phase 2 loaded its
    // version — a real concurrent `create_definition` call would do this;
    // bumping the row directly is equivalent and avoids re-parsing a second
    // definition text just to move the counter.
    client
        .execute(
            "update source_table_versions set version = version + 1 where source_table = 'orders'",
            &[],
        )
        .await
        .expect("bump orders' version, simulating a concurrent definition change");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(&txn, seg_seq, "worker", &plan, "trellis_apply_test")
        .await
        .expect_err("orders' version moved since compute; the fence must trip");
    match &err {
        ApplyError::VersionFenceMiss { src_table } => assert_eq!(src_table, "orders"),
        other => panic!("expected VersionFenceMiss, got {other:?}"),
    }
    txn.rollback().await.expect("rollback phase 3");

    let count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(count, 0, "a fence miss must not have written anything");
}

#[tokio::test]
async fn a_definition_change_on_an_unrelated_source_does_not_trip_the_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             create table widgets (id integer primary key, cost numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source tables");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    // `widgets` is never touched by this batch — its own version bump below
    // must not be in this batch's fence set at all.
    let widget_columns = numeric_columns(&["id", "cost"]);
    create_definition(
        &db.pool,
        "TRANSFORM widget_costs FROM widgets SELECT cost + cost AS doubled",
        &widget_columns,
    )
    .await
    .expect("create widget_costs definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // Change `widgets`, not `orders` — this batch never evaluated against
    // `widgets`, so it must not be in the fence set.
    client
        .execute(
            "update source_table_versions set version = version + 1 where source_table = 'widgets'",
            &[],
        )
        .await
        .expect("bump widgets' version");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let outcome =
        apply::apply_and_mark_drained(&txn, seg_seq, "worker", &plan, "trellis_apply_test")
            .await
            .expect("an unrelated source's version change must not trip this batch's fence");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(outcome.keys_written, 1);
}

#[tokio::test]
async fn a_write_that_changes_nothing_is_suppressed_as_a_no_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    // Pre-populate the target with the exact value the staged update would
    // also produce (10.00 + 1.50 = 11.50) — the write this batch stages
    // physically changes nothing.
    client
        .execute(
            "insert into order_totals (id, total) values (1, 11.50)",
            &[],
        )
        .await
        .expect("pre-populate target row with the same value");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "update",
        Some(r#"{"price":"5.00","tax":"1.50"}"#),
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(
        outcome.keys_written, 0,
        "a write that changes nothing must be suppressed, not counted as written"
    );

    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read target row")
        .get(0);
    assert_eq!(total, "11.50");
}

#[tokio::test]
async fn a_truncate_clears_every_target_row_but_a_same_batch_post_truncate_insert_survives() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (4, 40.00, 4.00)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create order_totals table");

    // A downstream reader of order_totals, to check that keys the truncate
    // physically clears also propagate downstream like any other
    // physically-changed key.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summary definition");
    create_target_table(&db.pool, &summary_def.def, &pk, &order_totals_columns)
        .await
        .expect("create order_summary table");

    // Rows an earlier drain left behind — the truncate must clear every one
    // of them, and its clear must propagate downstream to order_summary's
    // own already-derived rows for the same keys.
    client
        .execute(
            "insert into order_totals (id, total) values (1, 100), (2, 200), (3, 300)",
            &[],
        )
        .await
        .expect("pre-populate target rows the truncate must clear");
    client
        .execute(
            "insert into order_summary (id, grand_total) values (1, 200), (2, 400), (3, 600)",
            &[],
        )
        .await
        .expect("pre-populate order_summary's own already-derived rows");

    // The truncate sentinel, followed by a post-truncate insert for a new
    // key — both land in `seg_0`, so both are in the one single-bucket batch
    // `seal::seal_phase1` forces whenever `has_truncate` is true.
    insert_truncate_row(&client, "seg_0", "orders").await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "4",
        "insert",
        None,
        Some(r#"{"price":"40.00","tax":"4.00"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(
        outcome.keys_deleted, 3,
        "the truncate must clear all 3 pre-existing rows"
    );
    assert_eq!(
        outcome.keys_written, 1,
        "the post-truncate insert must still apply in the same batch"
    );

    let remaining: Vec<(String, String)> = client
        .query(
            "select id::text, total::text from order_totals order by id",
            &[],
        )
        .await
        .expect("read target table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        remaining,
        vec![("4".to_string(), "44.00".to_string())],
        "only the post-truncate row must remain"
    );

    // Downstream propagation: the cleared keys 1-3 and the written key 4
    // must all have staged a recompute trigger for order_summary.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(
        outcome2.keys_deleted, 3,
        "order_summary must have its own rows for 1-3 deleted, driven by the \
         cleared order_totals keys' recompute triggers"
    );
    assert_eq!(
        outcome2.keys_written, 1,
        "order_summary must have written the surviving key 4"
    );
    let summary_remaining: Vec<String> = client
        .query("select id::text from order_summary", &[])
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        summary_remaining,
        vec!["4".to_string()],
        "downstream propagation of the truncate's clear must reach order_summary too"
    );
}

#[tokio::test]
async fn next_claimable_segment_never_hands_out_a_segment_past_an_undrained_truncate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Segment 1: ordinary, no truncate.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "a",
        "insert",
        None,
        Some(r#"{"v":"a"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    // Segment 2: bears a truncate — forces bucket_count = 1.
    insert_truncate_row(&client, "seg_1", "orders").await;
    let seg2 = seal_active_segment(&mut client).await;

    // Segment 3: ordinary again, sealed after the truncate.
    insert_cdc_row(
        &client,
        "seg_2",
        "orders",
        "b",
        "insert",
        None,
        Some(r#"{"v":"b"}"#),
    )
    .await;
    let seg3 = seal_active_segment(&mut client).await;

    // All three sealed and undrained: the barrier must return the lowest,
    // seg1 — never skipping ahead to the truncate or past it.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(next, Some(seg1));

    // Mark seg1 fully drained directly (isolating the barrier query itself
    // from the claim/apply machinery, which is exercised elsewhere).
    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg1],
        )
        .await
        .expect("mark seg1 drained");

    // seg2 (the truncate) is now the lowest undrained segment, and also `B`
    // itself — the barrier must hand it out, not skip past it to seg3.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(
        next,
        Some(seg2),
        "the barrier must not skip the undrained truncate segment"
    );

    // Mark seg2 fully drained too.
    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg2],
        )
        .await
        .expect("mark seg2 drained");

    // Only now, with the truncate itself drained, does seg3 become
    // claimable.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(
        next,
        Some(seg3),
        "seg3 must become claimable only once the truncate segment below it has drained"
    );
}

#[tokio::test]
async fn a_change_propagates_two_hops_downstream_then_stops() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create order_totals table");

    // A second definition reading `order_totals` itself — the downstream
    // hop this test exercises. `order_summary` has no downstream reader of
    // its own, so propagation must stop after this hop.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summary definition");
    create_target_table(&db.pool, &summary_def.def, &pk, &order_totals_columns)
        .await
        .expect("create order_summary table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    // Hop 0: orders -> order_totals, direct apply.
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome1.keys_written, 1);
    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(total, "11.50");

    // The apply must have staged a Recompute trigger for order_totals's own
    // downstream reader, landing in the ring's active segment.
    let staged: i64 = client
        .query_one(
            "select count(*) from (
                 select src_table, key from seg_0
                 union all select src_table, key from seg_1
                 union all select src_table, key from seg_2
                 union all select src_table, key from seg_3
             ) rows where src_table = 'order_totals' and key = '1'",
            &[],
        )
        .await
        .expect("count staged recompute rows")
        .get(0);
    assert_eq!(
        staged, 1,
        "order_totals must have staged exactly one recompute trigger for order_summary"
    );

    // Hop 1: order_totals -> order_summary, driven by the recompute
    // trigger, which carries no image and must re-read order_totals live.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(outcome2.keys_written, 1);
    let grand_total: String = client
        .query_one(
            "select grand_total::text from order_summary where id = 1",
            &[],
        )
        .await
        .expect("read order_summary")
        .get(0);
    assert_eq!(grand_total, "23.00", "11.50 + 11.50");

    // No further propagation: order_summary has no downstream reader, so
    // nothing new should have been staged for it.
    let further_staged: i64 = client
        .query_one(
            "select count(*) from (
                 select src_table, key from seg_0
                 union all select src_table, key from seg_1
                 union all select src_table, key from seg_2
                 union all select src_table, key from seg_3
             ) rows where src_table = 'order_summary'",
            &[],
        )
        .await
        .expect("count staged rows for order_summary")
        .get(0);
    assert_eq!(
        further_staged, 0,
        "propagation must stop once a target has no downstream reader"
    );
}

/// Issue #63's write-path gap: a text-column passthrough must round-trip
/// through `compute()`/apply with the persisted `source_columns` type map
/// (`catalog::create_definition`), not default every column to Numeric and
/// fail to parse. Also exercises a numeric-*looking* text value ("007") to
/// prove it isn't misparsed as a number and corrupted.
#[tokio::test]
async fn a_text_column_passthrough_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table items (id integer primary key, label text); \
             insert into items (id, label) values (1, 'hello world'), (2, '007')",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "labels".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("label".to_string()),
        }],
        predicate: Predicate::True,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("label", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM labels FROM items SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"label":"hello world"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"label":"007"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, out from labels order by id", &[])
        .await
        .expect("read target table");
    let out1: String = rows[0].get(1);
    let out2: String = rows[1].get(1);
    assert_eq!(out1, "hello world");
    assert_eq!(
        out2, "007",
        "a numeric-looking text value must not be misparsed as a number"
    );
}

/// A string-literal calculated field (`SELECT 'hi' AS out`) must write its
/// literal text, not silently collapse to NULL (the pre-fix write-path
/// extraction only unwrapped `Value::Numeric`).
#[tokio::test]
async fn a_string_literal_field_writes_its_value_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table items (id integer primary key); \
             insert into items (id) values (1)",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "literals".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::StringLiteral("hi".to_string()),
        }],
        predicate: Predicate::True,
    };
    let source_columns = numeric_columns(&["id"]);
    create_definition(
        &db.pool,
        "TRANSFORM literals FROM items SELECT 'hi' AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(&client, "seg_0", "items", "1", "insert", None, Some("{}")).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 1);

    let out: String = client
        .query_one("select out from literals where id = 1", &[])
        .await
        .expect("read target row")
        .get(0);
    assert_eq!(out, "hi");
}

/// A boolean-column passthrough must round-trip through `compute()`/apply as
/// a real `boolean` column, not collapse to NULL.
#[tokio::test]
async fn a_boolean_column_passthrough_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table items (id integer primary key, flag boolean); \
             insert into items (id, flag) values (1, true), (2, false)",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "flags".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("flag".to_string()),
        }],
        predicate: Predicate::True,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("flag", ValueType::Boolean)]);
    create_definition(
        &db.pool,
        "TRANSFORM flags FROM items SELECT flag AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"flag":"t"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"flag":"f"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, out from flags order by id", &[])
        .await
        .expect("read target table");
    let out1: bool = rows[0].get(1);
    let out2: bool = rows[1].get(1);
    assert!(out1);
    assert!(!out2);
}

/// `SELECT strpos(name, 'foo') > 0 AS has_foo` (issue #65's composed
/// function-call-plus-comparison example) must round-trip through
/// `compute()`/apply into a real `boolean` target column, for both the
/// keyword-present and keyword-absent cases.
#[tokio::test]
async fn a_function_call_composed_with_greater_than_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table items (id integer primary key, name text); \
             insert into items (id, name) values (1, 'has foo in it'), (2, 'no match here')",
        )
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "keyword_flags".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "has_foo".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::FunctionCall {
                    name: "STRPOS".to_string(),
                    args: vec![
                        Expr::Column("name".to_string()),
                        Expr::StringLiteral("foo".to_string()),
                    ],
                }),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }],
        predicate: Predicate::True,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM keyword_flags FROM items SELECT strpos(name, 'foo') > 0 AS has_foo",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, &pk, &source_columns)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"name":"has foo in it"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"name":"no match here"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, has_foo from keyword_flags order by id", &[])
        .await
        .expect("read target table");
    let present: bool = rows[0].get(1);
    let absent: bool = rows[1].get(1);
    assert!(present, "row with 'foo' in name should have has_foo = true");
    assert!(
        !absent,
        "row without 'foo' in name should have has_foo = false"
    );
}
