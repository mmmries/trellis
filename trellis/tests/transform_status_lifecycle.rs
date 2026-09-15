//! Integration tests for issue #55 (epic #49): the transform lifecycle
//! status (`waiting_to_backfill` -> `backfilling` -> `live`, plus
//! `quarantined`) actually transitions, rather than sitting wherever
//! creation first put it — see docs/observability.md's "Transform status
//! lifecycle" and docs/decisions/0009-observability-decisions.md's decision
//! 4.
//!
//! Two scenarios, each exercising a real fence/marker rather than a
//! shortcut:
//!
//! - A fresh transform whose source table's `pending_backfill` marker
//!   (`intake::publication`) is pinned by a concurrent straggler
//!   transaction sits in `waiting_to_backfill` until that transaction
//!   commits and the marker's `xmin` fence settles, then reaches `live` —
//!   mirroring `intake_robustness.rs`'s own
//!   `table_add_backfills_existing_rows_once_the_fence_settles_and_retries_safely`
//!   straggler pattern for the fence mechanics, plus asserting the status
//!   column this issue wires up.
//! - `quarantine::resume_transform` drops an (manually) `quarantined`
//!   definition back to `waiting_to_backfill` and re-parks a catch-up
//!   marker, which the same `run_pending_backfills` discharge carries
//!   through to `live` again, repopulating the target from current source
//!   state.
//!
//! Both backfill mechanisms (the chunked plain-1-1 path and the
//! direct/set-based path) reaching `live` in the ordinary (no unsettled
//! marker) case is already covered extensively by `defs_install_definition.rs`
//! and `defs_backfill_chunk_queue.rs`; this file only adds the piece those
//! didn't cover: the deferred-to-`waiting_to_backfill` path this issue adds,
//! and whole-transform quarantine resume.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use testkit::crash::OpenTransaction;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{
    TransformStatus, ValueType, chunk_queue, create_definition, install_definition,
};
use trellis::intake::publication;
use trellis::staging::apply;
use trellis::staging::quarantine;
use trellis::staging::{has_pending, retire_drained_segments};

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

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// Seals and drains repeatedly until nothing is pending anywhere in the
/// ring — the same harness `defs_install_definition.rs` uses.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(pool, seg, "status_lifecycle_test", 1, "trellis_status_test")
            .await
            .expect("drain_once")
            .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// Claims and executes every pending direct-build backfill chunk until none
/// remain — the harness stand-in for a running drain worker, matching
/// `defs_install_definition.rs`'s `drain_backfill_chunks`.
async fn drain_backfill_chunks(pool: &trellis::Pool, target_schema: &str) {
    const CLAIMED_BY: &str = "status_lifecycle_test_backfill_worker";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                target_schema,
                CLAIMED_BY,
                Duration::from_secs(5),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn status_of(client: &Client, target_table: &str) -> TransformStatus {
    let text: String = client
        .query_one(
            "select status from transform_definitions where target_table = $1",
            &[&target_table],
        )
        .await
        .expect("query status")
        .get(0);
    TransformStatus::from_persisted(&text).unwrap_or_else(|| panic!("unrecognized status {text}"))
}

/// A fresh transform whose source table's publication-join fence is pinned
/// by a concurrent straggler transaction sits in `waiting_to_backfill` for
/// as long as that straggler is open — never silently `live` or
/// `backfilling` while its rows are genuinely unpopulated — then reaches
/// `backfilling`/`live` once the straggler commits and
/// `run_pending_backfills` discharges the marker, with the target correctly
/// populated from every pre-existing source row.
#[tokio::test]
async fn a_fresh_transform_waits_on_the_xmin_fence_then_reaches_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s (id bigint primary key, a numeric); \
         insert into s (id, a) select g, g from generate_series(1, 25) g; \
         create publication test_pub;",
    )
    .await
    .expect("seed source table and publication");

    // A straggler holds an xid open before the table joins the publication,
    // so the marker `reconcile_publication` leaves must name it as
    // in-flight — same pattern as
    // `intake_robustness.rs`'s `table_add_backfills_existing_rows_once_the_fence_settles_and_retries_safely`.
    let straggler = OpenTransaction::begin(db.dsn()).await;
    straggler.execute("select txid_current()").await;

    publication::reconcile_publication(&mut raw, "test_pub", &[format!("{DEFAULT_SCHEMA}.s")])
        .await
        .expect("reconcile adds s and leaves an unsettled pending_backfill marker");

    // The definition is created *while the marker is still unsettled* — the
    // scenario issue #55 closes: without the fix, this would persist
    // `backfilling` (the chunked path's usual speculative status) and
    // silently start enumerating/building right away, racing the straggler.
    let cols = numeric(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition defers instead of racing the fence");

    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "a definition created while its source table's fence is unsettled must defer, not \
         speculatively persist backfilling/live"
    );
    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::WaitingToBackfill,
        "the persisted row must agree with the returned Definition"
    );

    // No chunk work was enqueued and no direct build ran — the target
    // exists (DDL always runs) but is empty.
    let target_rows: i64 = raw
        .query_one("select count(*) from t", &[])
        .await
        .expect("count t")
        .get(0);
    assert_eq!(
        target_rows, 0,
        "a deferred definition's target must not be populated until the fence settles"
    );
    let chunk_count: i64 = raw
        .query_one(
            "select count(*) from backfill_chunks where definition_id = $1",
            &[&def.id],
        )
        .await
        .expect("count backfill_chunks")
        .get(0);
    assert_eq!(
        chunk_count, 0,
        "a deferred definition must not enqueue chunk work it can't safely run yet"
    );

    // A second run_pending_backfills pass while the straggler is still open
    // must leave the definition exactly where it was — no flicker, no
    // partial progress.
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills (unsettled)");
    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::WaitingToBackfill,
        "an unsettled fence must not advance the deferred definition"
    );

    // The straggler settles.
    straggler.commit().await;

    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills (settled)");

    // `run_pending_backfills` only *stages* the enumeration into the ring;
    // draining it is what actually populates the target and is where the
    // deferred definition's own promotion to `backfilling` (transient,
    // inside that same discharge) resolves to `live`.
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        status_of(&raw, "t").await,
        TransformStatus::Live,
        "once the fence settles and the deferred backfill drains, the definition must be live"
    );

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .expect("compare s and t")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the deferred backfill must populate every pre-existing source row correctly"
    );
}

/// Whole-transform quarantine resume (ADR-0003's coarser fuse tier):
/// `quarantine::resume_transform` requires `quarantined`, drops the
/// definition to `waiting_to_backfill`, and re-parks a catch-up marker that
/// `run_pending_backfills` carries back through to `live`, with the target
/// correctly re-populated. Since nothing in this codebase yet *trips* a
/// definition to `quarantined` automatically (see `resume_transform`'s own
/// doc comment), the quarantine itself is simulated with a direct status
/// write — this test is about the resume half of the contract.
#[tokio::test]
async fn quarantine_resume_drops_to_waiting_to_backfill_and_re_backfills_to_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s2 (id bigint primary key, a numeric); \
         insert into s2 (id, a) select g, g from generate_series(1, 25) g;",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t2 FROM s2 SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition");
    // The plain 1-1 fast path backgrounds its build as chunk work; drive it
    // to completion (this also parks its own post-build catch-up marker —
    // `complete_direct_backfill` — which the next `run_pending_backfills`
    // call below discharges so it can't be mistaken for the marker
    // `resume_transform` parks later).
    drain_backfill_chunks(&db.pool, "public").await;
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("discharge the post-build catch-up marker");
    assert_eq!(status_of(&raw, "t2").await, TransformStatus::Live);

    // Break it: mutate a source row so the target visibly diverges, then
    // simulate the fuse tripping (no automatic trip exists yet — see the
    // module doc comment above).
    raw.execute("update s2 set a = 999 where id = 1", &[])
        .await
        .expect("mutate a source row so the divergence is checkable");
    raw.execute(
        "update transform_definitions set status = 'quarantined' where target_table = 't2'",
        &[],
    )
    .await
    .expect("simulate the fuse tripping");

    quarantine::resume_transform(&db.pool, "t2")
        .await
        .expect("resume_transform");

    assert_eq!(
        status_of(&raw, "t2").await,
        TransformStatus::WaitingToBackfill,
        "resume must drop straight to waiting_to_backfill, never directly to backfilling/live"
    );

    // No concurrent transaction pins the fence this time, so a single
    // discharge pass both settles and processes the re-parked marker.
    publication::run_pending_backfills(&mut raw, "wake")
        .await
        .expect("run_pending_backfills discharges the resume's own marker");
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        status_of(&raw, "t2").await,
        TransformStatus::Live,
        "the re-backfill must reach live again, exactly like a fresh transform's own initial \
         backfill"
    );

    let mismatches: i64 = raw
        .query_one(
            "select count(*) from s2 left join t2 on t2.id = s2.id \
             where t2.id is null or t2.x is distinct from s2.a + s2.a",
            &[],
        )
        .await
        .expect("compare s2 and t2")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "the re-backfill must re-derive every row from current source state, including the row \
         mutated while quarantined"
    );
}

/// `resume_transform` refuses a target that isn't currently quarantined —
/// caller error, not a silent no-op, mirroring `resume_column`'s
/// `ColumnNotPaused` discipline.
#[tokio::test]
async fn resume_transform_refuses_a_target_that_is_not_quarantined() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table s3 (id bigint primary key, a numeric); \
         insert into s3 (id, a) values (1, 1);",
    )
    .await
    .expect("seed source table");

    let cols = numeric(&["a"]);
    create_definition(&db.pool, "TRANSFORM t3 FROM s3 SELECT a + a AS x", &cols)
        .await
        .expect("create_definition (ring path, live immediately)");

    let err = quarantine::resume_transform(&db.pool, "t3")
        .await
        .expect_err("t3 is live, not quarantined");
    match err {
        apply::ApplyError::TransformNotQuarantined { transform } => {
            assert_eq!(transform, "t3");
        }
        other => panic!("expected TransformNotQuarantined, got {other:?}"),
    }
}

/// `resume_transform` reports `TransformNotFound` for a target with no
/// `transform_definitions` row at all, rather than a generic DB error.
#[tokio::test]
async fn resume_transform_reports_not_found_for_an_unregistered_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let err = quarantine::resume_transform(&db.pool, "no_such_transform")
        .await
        .expect_err("no such transform is registered");
    match err {
        apply::ApplyError::TransformNotFound { transform } => {
            assert_eq!(transform, "no_such_transform");
        }
        other => panic!("expected TransformNotFound, got {other:?}"),
    }
}

/// Regression coverage (this issue's items 2/3): with no unsettled marker at
/// creation time, both backfill mechanisms still reach `live` directly, the
/// same as before this issue's deferral logic was added — the deferral
/// check must be a no-op on the ordinary path.
#[tokio::test]
async fn both_backfill_mechanisms_still_reach_live_with_no_unsettled_marker() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table plain_s (id bigint primary key, a numeric); \
         insert into plain_s (id, a) select g, g from generate_series(1, 10) g; \
         create table agg_s (id bigint primary key, grp bigint, a numeric); \
         alter table agg_s replica identity full; \
         insert into agg_s (id, grp, a) select g, g % 3, g from generate_series(1, 10) g;",
    )
    .await
    .expect("seed source tables");

    // Chunked path: a plain (non-relationship) 1-1 definition.
    let cols = numeric(&["a"]);
    let plain = install_definition(
        &db.pool,
        "TRANSFORM plain_t FROM plain_s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition (chunked path)");
    assert_eq!(plain.status, TransformStatus::Backfilling);
    drain_backfill_chunks(&db.pool, "public").await;
    assert_eq!(status_of(&raw, "plain_t").await, TransformStatus::Live);

    // Direct/set-based path: an aggregate definition.
    let agg_cols = numeric(&["grp", "a"]);
    let agg = install_definition(
        &db.pool,
        "TRANSFORM agg_t FROM agg_s GROUP BY grp SELECT grp AS grp, SUM(a) AS total",
        &agg_cols,
        "public",
    )
    .await
    .expect("install_definition (direct/aggregate path)");
    assert_eq!(
        agg.status,
        TransformStatus::Live,
        "the direct/set-based path still builds synchronously and ends up live in-call"
    );
    assert_eq!(status_of(&raw, "agg_t").await, TransformStatus::Live);
}
