//! Integration tests for issue #52's end-to-end latency histogram (ADR-0009
//! decision 2): time from a source commit to a *terminal* transform's apply,
//! keyed by terminal transform only.
//!
//! Mirrors `apply.rs`'s conventions: source/target tables and definitions
//! built by hand, changes staged directly into the ring, drains run via
//! `apply::drain_once` (intake is out of scope here, same as `apply.rs`).
//! `apply.rs`'s `a_change_propagates_two_hops_downstream_then_stops` is the
//! DAG-shape fixture these tests crib from — that test already proves
//! propagation stops at a transform with no downstream reader; these tests
//! layer `trellis::metrics::render_for_test()` assertions on top of the same
//! shape to prove the *metric* also only fires there.
//!
//! Transform/target names below are deliberately distinctive
//! (`e2e_latency_*`) rather than reusing `apply.rs`'s `order_totals`/
//! `order_summary` names: `trellis::metrics`'s registry is a single
//! process-wide global (see `metrics.rs`'s module doc comment), shared by
//! every test in this binary, so a name any other test also uses as a
//! *terminal* target could taint an assertion here that a given label never
//! received an end-to-end observation.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{create_definition, create_target_table, source_primary_key};
use trellis::staging::apply;

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

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
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// The ring table (`seg_0`..`seg_3`) currently active — needed by any test
/// that stages more than one change across seals, since each seal rotates
/// the active slot forward (`seal::seal_phase1`'s `next_ring_slot =
/// (ring_slot + 1) % RING_SIZE`); hardcoding `"seg_0"` (fine for a single
/// insert-then-seal, what every other helper's caller does today) would
/// stage a later insert into a slot nothing is currently draining from.
async fn active_segment_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read active ring slot")
        .get(0);
    format!("seg_{ring_slot}")
}

/// Stages one image-bearing (CDC-shaped) change directly into `table`,
/// mirroring `apply.rs`'s helper of the same name — except this one also
/// sets `src_changed` to `now()` (`apply.rs`'s own helper leaves it `NULL`,
/// which is fine for its purely functional assertions, but these tests are
/// specifically about the latency histograms that only fire off a non-NULL
/// `src_changed`, per [`FoldedChange::src_changed`]'s doc comment).
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
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen, \
                 src_changed) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, now())"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

async fn drain(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    apply::drain_once(pool, seg_seq, claimed_by, 1, "trellis_e2e_latency_test")
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something")
}

/// A one-histogram-family text scrape of `rendered` for `metric{transform="target"}`
/// — enough to tell whether `target` ever received an observation under
/// `metric` without parsing full Prometheus text exposition.
fn metric_mentions_transform(rendered: &str, metric: &str, target: &str) -> bool {
    rendered
        .lines()
        .any(|line| line.starts_with(metric) && line.contains(&format!("transform=\"{target}\"")))
}

/// Linear chain: `orders -> e2e_latency_chain_totals -> e2e_latency_chain_summary`,
/// the second definition being the only terminal transform.
///
/// **Why this test stages two separate `orders`-side-equivalent CDC rows
/// instead of just following the one automatic hop-to-hop propagation:**
/// `StagedChange::Recompute` — what `apply_and_mark_drained` automatically
/// stages to propagate a change to a downstream reader
/// (`trellis/src/staging/apply.rs`'s "4. Downstream propagation") — carries
/// no `src_changed` of its own (see `staging::append::StagedChange`'s doc
/// comment and `ChangeRow::from`'s `Recompute` arm: always `None`).
/// ADR-0009 decision 5 is explicit that only `StagedChange::Cdc`/`Truncate`
/// carry `src_changed` forward. So a transform reached purely through an
/// automatically-staged recompute trigger — the common case for any hop
/// beyond the first — folds to a [`FoldedChange`] with `src_changed: None`,
/// and *neither* the per-transform histogram (#51) *nor* the end-to-end one
/// (#52) can observe anything for it: there's no origin left to diff
/// against. That's a pre-existing characteristic of #51's already-committed
/// mechanism (not something this change alters or extends), reused as-is
/// here rather than threading `src_changed` through `Recompute` too — see
/// this crate's issue #52 implementation notes for why that's flagged as a
/// follow-up rather than folded into this change.
///
/// This test exercises that first hop faithfully (proving the intermediate
/// transform gets its own per-transform latency but never an end-to-end
/// observation) and then, matching this whole test file's established
/// "changes staged directly into the ring, intake is out of scope"
/// convention, stages a second, independent CDC-shaped change directly
/// against `e2e_latency_chain_totals` (exactly as the first hop's CDC row
/// was staged directly against `orders`) to drive the terminal hop with a
/// real origin timestamp — proving the terminal transform *does* get an
/// end-to-end observation once one is reachable, while the intermediate
/// transform still never does.
#[tokio::test]
async fn end_to_end_latency_fires_only_at_the_terminal_transform_in_a_linear_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let totals_def = TransformDef {
        target: "e2e_latency_chain_totals".to_string(),
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
    };
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_chain_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create chain_totals definition");
    let pk = source_primary_key(&db.pool, &totals_def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &totals_def, "public", &pk, &source_columns)
        .await
        .expect("create chain_totals table");

    // The terminal hop: reads chain_totals, has no downstream reader itself.
    let totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_chain_summary FROM e2e_latency_chain_totals \
         SELECT total + total AS grand_total",
        &totals_columns,
    )
    .await
    .expect("create chain_summary definition");
    create_target_table(&db.pool, &summary_def.def, "public", &pk, &totals_columns)
        .await
        .expect("create chain_summary table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

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

    // Hop 0: orders -> chain_totals (intermediate; has a downstream reader).
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome1.keys_written, 1);

    let after_hop0 = trellis::metrics::render_for_test();
    assert!(
        metric_mentions_transform(
            &after_hop0,
            "trellis_transform_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "the intermediate hop must still get its own per-transform latency (issue #51): {after_hop0}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop0,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "an intermediate hop (has a downstream reader) must never get an end-to-end \
         observation: {after_hop0}"
    );

    // Hop 1: chain_totals -> chain_summary (terminal), driven by the
    // recompute trigger chain_totals' apply staged automatically — an
    // image-less trigger with no `src_changed` of its own (see this test's
    // doc comment). Drained here purely to move the ring forward before
    // this test stages its own second hop below; neither latency histogram
    // is expected to observe anything from it.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(outcome2.keys_written, 1);

    let after_recompute_hop = trellis::metrics::render_for_test();
    assert!(
        !metric_mentions_transform(
            &after_recompute_hop,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "a terminal transform reached only through an origin-less recompute trigger must not \
         get a spurious end-to-end observation: {after_recompute_hop}"
    );
    assert!(
        !metric_mentions_transform(
            &after_recompute_hop,
            "trellis_transform_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "same origin-less trigger: no per-transform latency either (issue #51's own existing \
         behavior, unaffected by this change): {after_recompute_hop}"
    );

    // Now drive the terminal hop with a real origin: stage a second,
    // independent CDC-shaped change directly against
    // `e2e_latency_chain_totals` itself (this test file's own established
    // "stage directly into the ring" convention, applied to the second hop
    // exactly as it already was to the first).
    let active_table = active_segment_table(&client).await;
    insert_cdc_row(
        &client,
        &active_table,
        "e2e_latency_chain_totals",
        "1",
        "update",
        Some(r#"{"id":"1","total":"11.50"}"#),
        Some(r#"{"id":"1","total":"20.00"}"#),
    )
    .await;
    let seg3 = seal_active_segment(&mut client).await;
    let outcome3 = drain(&db.pool, seg3, "worker").await;
    assert_eq!(outcome3.keys_written, 1);

    let after_hop1 = trellis::metrics::render_for_test();
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "the terminal transform must get an end-to-end observation once it's reached with a \
         real origin timestamp: {after_hop1}"
    );
    assert!(
        !metric_mentions_transform(
            &after_hop1,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_chain_totals",
        ),
        "the intermediate hop must still never get an end-to-end observation, even after the \
         terminal hop applies: {after_hop1}"
    );
    assert!(
        metric_mentions_transform(
            &after_hop1,
            "trellis_transform_latency_seconds",
            "e2e_latency_chain_summary",
        ),
        "the terminal transform's own per-transform latency (issue #51) must still be \
         recorded too: {after_hop1}"
    );
}

/// DAG fan-out: one source (`orders`) feeding two terminal transforms
/// directly (`e2e_latency_fanout_a`, `e2e_latency_fanout_b`), neither read
/// by anything else. One drain of one source-committed change must record
/// an end-to-end observation for *both* terminal transforms.
#[tokio::test]
async fn end_to_end_latency_fires_for_every_terminal_transform_in_a_fan_out() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let source_columns = numeric_columns(&["id", "price", "tax"]);

    let def_a = TransformDef {
        target: "e2e_latency_fanout_a".to_string(),
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
    };
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_fanout_a FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create fanout_a definition");
    let pk = source_primary_key(&db.pool, &def_a.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def_a, "public", &pk, &source_columns)
        .await
        .expect("create fanout_a table");

    let def_b = TransformDef {
        target: "e2e_latency_fanout_b".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "price_only".to_string(),
            expr: Expr::Column("price".to_string()),
        }],
        predicate: Predicate::True,
    };
    create_definition(
        &db.pool,
        "TRANSFORM e2e_latency_fanout_b FROM orders SELECT price AS price_only",
        &source_columns,
    )
    .await
    .expect("create fanout_b definition");
    create_target_table(&db.pool, &def_b, "public", &pk, &source_columns)
        .await
        .expect("create fanout_b table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

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

    let seg1 = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome.keys_written, 2, "one write per fan-out branch");

    let rendered = trellis::metrics::render_for_test();
    assert!(
        metric_mentions_transform(
            &rendered,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_fanout_a",
        ),
        "terminal branch a must get an end-to-end observation: {rendered}"
    );
    assert!(
        metric_mentions_transform(
            &rendered,
            "trellis_end_to_end_latency_seconds",
            "e2e_latency_fanout_b",
        ),
        "terminal branch b must get an end-to-end observation: {rendered}"
    );
}
