//! Integration tests for column-level quarantine
//! (`docs/decisions/0003-quarantine-storage-and-api.md`'s 2026-09-12
//! amendment, `docs/public-api-design.md` decision 5): the per-`(transform,
//! column)` fuse layered alongside the pre-existing row-level/transform-wide
//! one (`engine/tests/quarantine.rs`, unmodified by this feature — see
//! `an_existing_row_level_fuse_scenario_is_unaffected` below for a targeted
//! regression check of that claim in this file too).
//!
//! Follows `engine/tests/quarantine.rs`'s own conventions: a real, ephemeral
//! Postgres instance per test (`testkit::TestCluster`), and "reach past the
//! mechanism, insert directly" for whichever half of a scenario the
//! mechanism under test doesn't itself produce (staging a malformed CDC
//! image directly into the ring, rather than routing through a live
//! replication slot).

use std::collections::HashMap;

use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::{create_definition, create_target_table, source_primary_key};
use engine::staging::apply::{self, ApplyError};
use engine::staging::quarantine::{self, DEFAULT_COLUMN_DEATH_THRESHOLD};
use engine::{BlockingTrellis, Config, Trellis, TrellisOptions};
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

// ---------------------------------------------------------------------
// Shared scaffolding (mirrors `tests/quarantine.rs`)
// ---------------------------------------------------------------------

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("set search_path to trellis, public")
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

/// The ring table `insert_cdc_row` must target *right now* — `seg_0` is only
/// correct for a test's first, never-yet-sealed batch; every batch after
/// that has rotated the active ring slot forward (`seal_active_segment`
/// advances `segment_pointer`), and inserting into a already-sealed table
/// would silently stage into the wrong (already-closed) batch. Every test
/// below that stages more than one batch reads this fresh before each one.
async fn active_segment_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    match ring_slot {
        0 => "seg_0",
        1 => "seg_1",
        2 => "seg_2",
        3 => "seg_3",
        other => panic!("unexpected ring slot {other}"),
    }
    .to_string()
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

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

/// Creates `orders` (unpopulated) plus the `order_totals` 1-1 definition and
/// target table — the single-column fixture most tests below trip the
/// column fuse against.
async fn seed_order_totals(db: &TestDatabase, client: &Client) -> TransformDef {
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let def = order_totals_def();
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns)
        .await
        .expect("create target table");
    def
}

/// Adds a second, chained 1-1 transform (`order_summaries`, reading straight
/// from `order_totals`'s own `total` column) — the fixture the cascade tests
/// use. Mirrors `tests/quarantine.rs`'s `order_summary` hop-bound fixture.
async fn seed_order_summaries(db: &TestDatabase, orders_def: &TransformDef) {
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summaries FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summaries definition");
    let pk = source_primary_key(&db.pool, &orders_def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &order_totals_columns,
    )
    .await
    .expect("create order_summaries table");
}

/// Stages one malformed (`price` = `"not-a-number"`) CDC insert per id in
/// `ids`, all into the *same* segment, seals it, and drains it once —
/// expecting an evaluator failure to surface (the drive-by-real-failures
/// pattern `tests/quarantine.rs`'s own eviction test uses, just for the
/// column fuse instead of the row-level one).
///
/// Deliberately one segment per call, not one per `id`: none of these
/// batches ever actually reaches `drained` (a lone bad key's own
/// `key_deaths` never crosses the *row-level* fuse's threshold on its own,
/// so `drain_once` always gives up and propagates rather than evicting and
/// retrying to success) — the ring only has `RING_SIZE` (4) physical slots,
/// and nothing here ever retires a stuck segment, so batching every id this
/// call needs into one segment keeps every test's total segment count under
/// that ceiling instead of exhausting the ring.
async fn stage_bad_orders(client: &mut Client, pool: &engine::Pool, ids: &[i64]) {
    let table = active_segment_table(client).await;
    for id in ids {
        insert_cdc_row(
            client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
        )
        .await;
    }
    let seg_seq = seal_active_segment(client).await;
    let result =
        apply::drain_once(pool, seg_seq, "worker", 1, "trellis_column_quarantine_test").await;
    assert!(
        matches!(result, Err(ApplyError::Eval(_))),
        "a malformed numeric field must still surface as an evaluator failure, got {result:?}"
    );
}

async fn column_status_row(
    client: &Client,
    transform: &str,
    column: &str,
) -> Option<(bool, Option<String>)> {
    client
        .query_opt(
            "select local_fuse, last_error from column_status \
             where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_status")
        .map(|row| (row.get(0), row.get(1)))
}

async fn column_deaths_count(client: &Client, transform: &str, column: &str) -> Option<i32> {
    client
        .query_opt(
            "select deaths from column_deaths where transform_table = $1 and column_name = $2",
            &[&transform, &column],
        )
        .await
        .expect("read column_deaths")
        .map(|row| row.get(0))
}

async fn cascade_edge_exists(
    client: &Client,
    downstream_transform: &str,
    downstream_column: &str,
    upstream_transform: &str,
    upstream_column: &str,
) -> bool {
    client
        .query_opt(
            "select 1 from column_pause_cascades \
             where downstream_transform = $1 and downstream_column = $2 \
               and upstream_transform = $3 and upstream_column = $4",
            &[
                &downstream_transform,
                &downstream_column,
                &upstream_transform,
                &upstream_column,
            ],
        )
        .await
        .expect("read column_pause_cascades")
        .is_some()
}

// ---------------------------------------------------------------------
// (a) The column fuse trips after the threshold is crossed, and not before.
// ---------------------------------------------------------------------

#[tokio::test]
async fn column_fuse_trips_only_once_the_threshold_is_crossed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    assert_eq!(
        DEFAULT_COLUMN_DEATH_THRESHOLD, 5,
        "test assumes the default"
    );

    // `DEFAULT_COLUMN_DEATH_THRESHOLD - 1` distinct bad rows, one batch:
    // charged, but not paused.
    let below_threshold: Vec<i64> = (1..DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &below_threshold).await;
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(DEFAULT_COLUMN_DEATH_THRESHOLD - 1),
        "every distinct bad row below threshold must be charged exactly once"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "must not be paused before the threshold is reached"
    );

    // One more distinct bad row, its own batch: trips the fuse.
    stage_bad_orders(
        &mut client,
        &db.pool,
        &[DEFAULT_COLUMN_DEATH_THRESHOLD as i64],
    )
    .await;

    let status = column_status_row(&client, "order_totals", "total")
        .await
        .expect("the column must be paused now that the threshold is crossed");
    assert!(status.0, "a threshold trip is a local fuse, not a cascade");
    assert!(status.1.is_some(), "the tripping error must be recorded");
    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        None,
        "the counter must reset once the fuse trips"
    );
}

// ---------------------------------------------------------------------
// (b) A paused column's value freezes across subsequent CDC deltas.
// ---------------------------------------------------------------------

#[tokio::test]
async fn paused_column_freezes_instead_of_going_null_or_being_overwritten() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;

    // A healthy row, computed successfully before anything pauses.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "100",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the healthy row");
    let frozen_total: String = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(frozen_total, "11.50");

    // Trip the column fuse via `DEFAULT_COLUMN_DEATH_THRESHOLD` distinct bad
    // rows, none of which is id 100.
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the fuse must have tripped"
    );

    // A brand-new, perfectly healthy delta to the *already-written* row: its
    // `total` must stay exactly as it was, not be recomputed, not go null.
    let table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &table,
        "orders",
        "100",
        "update",
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
        Some(r#"{"price":"999.00","tax":"999.00"}"#),
    )
    .await;
    let seg_seq2 = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq2,
        "worker",
        1,
        "trellis_column_quarantine_test",
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain the update, minus the paused column");

    let total_after: Option<String> = client
        .query_one("select total::text from order_totals where id = 100", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(
        total_after,
        Some("11.50".to_string()),
        "a paused column's value must freeze at its last successfully computed value, not go \
         null and not be overwritten by new (even valid) source data"
    );
}

// ---------------------------------------------------------------------
// (c) Cascading a paused column's pause to a dependent (chained) transform.
// ---------------------------------------------------------------------

#[tokio::test]
async fn a_dependent_transforms_column_cascades_to_paused_when_its_upstream_column_pauses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some(),
        "the upstream column must have paused"
    );
    let downstream_status = column_status_row(&client, "order_summaries", "grand_total")
        .await
        .expect("the dependent column must have cascaded to paused");
    assert!(
        !downstream_status.0,
        "a purely cascaded pause is not this column's own local fuse"
    );
    assert!(
        cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the cascade edge must be recorded so resume can later un-cascade it correctly"
    );
}

// ---------------------------------------------------------------------
// (d) Resume clears the pause, recomputes, and respects an independent
//     reason a dependent has to stay paused.
// ---------------------------------------------------------------------

#[tokio::test]
async fn resume_recomputes_and_does_not_un_pause_a_dependent_with_its_own_reason() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    let orders_def = seed_order_totals(&db, &client).await;
    seed_order_summaries(&db, &orders_def).await;

    // Real, valid rows in the actual source table — the malformed CDC
    // images below are purely staged/synthetic (as in every other test in
    // this file), so resume's recompute (which reads the live table) finds
    // clean data once the fuse trips and is resumed.
    client
        .batch_execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.00), (2, 20.00, 2.00), (3, 30.00, 3.00), \
             (4, 40.00, 4.00), (5, 50.00, 5.00)",
        )
        .await
        .expect("seed valid orders rows");

    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    assert!(
        column_status_row(&client, "order_totals", "total")
            .await
            .is_some()
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some()
    );

    // Give the dependent its *own*, independent reason to stay paused —
    // reached past the mechanism directly, the same convention
    // `tests/quarantine.rs` uses for seeding counters/markers by hand.
    client
        .execute(
            "update column_status set local_fuse = true \
             where transform_table = 'order_summaries' and column_name = 'grand_total'",
            &[],
        )
        .await
        .expect("mark the dependent as also independently paused");

    // The batch that tripped the fuse rolled back entirely (it never
    // resolved), so `order_totals` has no rows for ids 1-5 yet — retry them
    // now that `total` is paused/excluded: this batch succeeds (nothing left
    // to error on) and creates the bare rows resume's recompute will then
    // fill in, exactly like a real drain loop retrying a previously-failing
    // batch once the column that broke it is out of the way.
    let table = active_segment_table(&client).await;
    for id in 1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64 {
        insert_cdc_row(
            &client,
            &table,
            "orders",
            &id.to_string(),
            "insert",
            None,
            Some(&format!(
                r#"{{"price":"{}.00","tax":"{}.00"}}"#,
                id * 10,
                id
            )),
        )
        .await;
    }
    let seg_seq = seal_active_segment(&mut client).await;
    apply::drain_once(
        &db.pool,
        seg_seq,
        "worker",
        1,
        "trellis_column_quarantine_test",
    )
    .await
    .expect("drain_once")
    .expect("must claim and drain now that the broken column is excluded");

    let resumed = quarantine::resume_column(&db.pool, "order_totals", "total")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())],
        "the dependent must NOT be auto-resumed — it has its own independent reason"
    );

    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "the resumed column itself must no longer be paused"
    );
    assert!(
        column_status_row(&client, "order_summaries", "grand_total")
            .await
            .is_some(),
        "the dependent must remain paused: its own local_fuse reason still holds"
    );
    assert!(
        !cascade_edge_exists(
            &client,
            "order_summaries",
            "grand_total",
            "order_totals",
            "total"
        )
        .await,
        "the specific cascade edge from the now-resumed upstream must be gone"
    );

    for id in 1..=5i32 {
        let total: String = client
            .query_one("select total::text from order_totals where id = $1", &[&id])
            .await
            .unwrap_or_else(|e| panic!("read order_totals for id {id}: {e}"))
            .get(0);
        let expected = match id {
            1 => "11.00",
            2 => "22.00",
            3 => "33.00",
            4 => "44.00",
            5 => "55.00",
            _ => unreachable!(),
        };
        assert_eq!(
            total, expected,
            "resume must have recomputed against the real (valid) source row for id {id}"
        );
    }
}

// ---------------------------------------------------------------------
// (e) The three read methods and the resume method, on both `Trellis` and
//     `BlockingTrellis`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
    stage_bad_orders(&mut client, &db.pool, &bad_ids).await;

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect");

    let list = trellis.quarantined().await.expect("quarantined");
    assert!(
        list.iter().any(|entry| entry.target
            == engine::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status");
    assert_eq!(
        status.state,
        engine::app::QuarantineState::Paused,
        "the column address must report paused"
    );
    assert!(status.paused_at.is_some());

    let whole_transform_status = trellis
        .quarantine_status("order_totals")
        .await
        .expect("quarantine_status for the bare transform");
    assert_eq!(
        whole_transform_status.state,
        engine::app::QuarantineState::Live,
        "the transform's own lifecycle status is untouched by a column pause"
    );

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .await
        .expect("sample_quarantined");
    assert_eq!(
        sample.len(),
        DEFAULT_COLUMN_DEATH_THRESHOLD as usize,
        "every distinct poisoned row that contributed to the trip must be sampleable: {sample:?}"
    );

    let resumed = trellis
        .resume_column("order_totals.total")
        .await
        .expect("resume_column");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())]
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .await
        .expect("quarantine_status after resume");
    assert_eq!(status_after.state, engine::app::QuarantineState::Live);

    let bare_transform_err = trellis.resume_column("order_totals").await;
    assert!(
        matches!(
            bare_transform_err,
            Err(engine::TrellisError::ColumnAddressRequired)
        ),
        "resume_column must reject a bare transform address, got {bare_transform_err:?}"
    );
}

#[test]
fn blocking_trellis_exposes_the_read_and_resume_methods() {
    let cluster = TestCluster::start();

    let setup_runtime = tokio::runtime::Runtime::new().expect("build scratch setup runtime");
    let db = setup_runtime.block_on(cluster.create_isolated_database());
    setup_runtime.block_on(async {
        let mut client = connect_raw(db.dsn()).await;
        seed_order_totals(&db, &client).await;
        let bad_ids: Vec<i64> = (1..=DEFAULT_COLUMN_DEATH_THRESHOLD as i64).collect();
        stage_bad_orders(&mut client, &db.pool, &bad_ids).await;
    });
    drop(setup_runtime);

    let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let trellis =
        BlockingTrellis::connect(config, TrellisOptions::default()).expect("connect (sync)");

    let list = trellis.quarantined().expect("quarantined (sync)");
    assert!(
        list.iter().any(|entry| entry.target
            == engine::app::QuarantineTarget::Column(
                "order_totals".to_string(),
                "total".to_string()
            )),
        "the paused column must appear in the flat list: {list:?}"
    );

    let status = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status (sync)");
    assert_eq!(status.state, engine::app::QuarantineState::Paused);

    let sample = trellis
        .sample_quarantined("order_totals.total", None, 10)
        .expect("sample_quarantined (sync)");
    assert_eq!(sample.len(), DEFAULT_COLUMN_DEATH_THRESHOLD as usize);

    let resumed = trellis
        .resume_column("order_totals.total")
        .expect("resume_column (sync)");
    assert_eq!(
        resumed,
        vec![("order_totals".to_string(), "total".to_string())]
    );

    let status_after = trellis
        .quarantine_status("order_totals.total")
        .expect("quarantine_status after resume (sync)");
    assert_eq!(status_after.state, engine::app::QuarantineState::Live);

    trellis.shutdown().expect("shutdown (sync)");
}

// ---------------------------------------------------------------------
// (f) The existing row-level/transform-wide fuse still works unchanged.
// ---------------------------------------------------------------------

/// A targeted regression check living alongside the new feature's own
/// tests, on top of `engine/tests/quarantine.rs`'s full existing suite
/// (unmodified, still green): a single stubborn key retried past the
/// row-level threshold must still evict via `key_deaths`/`poison` exactly as
/// before, and — the specific new-feature interaction this test is really
/// about — must NOT also trip the column fuse, since it is one row failing
/// repeatedly, not a breadth of distinct rows (see `column_failures`'
/// migration comment / `charge_column_failure`'s doc comment on why the
/// column fuse counts distinct rows, not attempts).
#[tokio::test]
async fn an_existing_row_level_fuse_scenario_is_unaffected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    seed_order_totals(&db, &client).await;
    client
        .batch_execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
        )
        .await
        .expect("seed orders rows");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let outcome = loop {
        match apply::drain_once(
            &db.pool,
            seg_seq,
            "worker",
            1,
            "trellis_column_quarantine_test",
        )
        .await
        {
            Ok(Some(outcome)) => break outcome,
            Ok(None) => panic!("drain_once claimed nothing on a still-undrained segment"),
            Err(_) => continue,
        }
    };
    assert_eq!(outcome.keys_written, 1, "only the survivor, key 2, writes");

    let poisoned: bool = client
        .query_one(
            "select exists(select 1 from poison where src_table = 'orders' and key = '1')",
            &[],
        )
        .await
        .expect("read poison")
        .get(0);
    assert!(poisoned, "the row-level fuse must still evict as before");

    assert_eq!(
        column_deaths_count(&client, "order_totals", "total").await,
        Some(1),
        "one repeatedly-retried key must charge the column counter exactly once, not once per \
         attempt — it never crosses the column fuse's own threshold alone"
    );
    assert_eq!(
        column_status_row(&client, "order_totals", "total").await,
        None,
        "a single stubborn row must never trip the column fuse on its own"
    );
}
