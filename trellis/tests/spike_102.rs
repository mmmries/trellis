//! Validation spikes for issue #102 (to-one relationship fields as GROUP BY
//! keys in aggregate transforms).
//!
//! These are *spikes*, not feature tests: they probe the existing engine for
//! the preconditions issue #102's proposed "image-aware reverse delta" design
//! depends on. Nothing here changes engine behavior.
//!
//! * `spike_a_*` — is the linchpin true? Does the real logical-replication
//!   stream deliver both the old and the new parent image on an
//!   `UPDATE posts SET author = ...`, and does the reverse path enumerate the
//!   affected from-side rows?
//! * `spike_a2_*` — can a from-side change be drained in a *strictly earlier*
//!   batch than the reverse work a parent change queued for it? That ordering
//!   is what makes the delta design unsound (see the issue comment).

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition};
use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
use trellis::{Client as TrellisClient, ClientOptions};

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

async fn poll_until<F>(timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "timed out: {message}");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Issue #102's schema. `posts` is `REPLICA IDENTITY FULL` so its pre-image
/// carries `author` — the whole premise of Spike A.
const SCHEMA: &str = "\
    create table posts (id integer primary key, author text, word_count integer); \
    alter table posts replica identity full; \
    create table post_tags (post integer not null, tag text not null, primary key (post, tag)); \
    alter table post_tags replica identity full; \
    create index on post_tags (post);";

/// The same schema with a surrogate single-column primary key on `post_tags`
/// — what the engine actually requires today (see
/// `spike_a3_a_composite_pk_from_side_cannot_drain_at_all`).
const SCHEMA_SURROGATE: &str = "\
    create table posts (id integer primary key, author text, word_count integer); \
    alter table posts replica identity full; \
    create table post_tags (id integer primary key, post integer not null, tag text not null); \
    alter table post_tags replica identity full; \
    create index on post_tags (post);";

fn post_tags_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("post".to_string(), ValueType::Numeric),
        ("tag".to_string(), ValueType::Text),
    ])
}

fn post_tags_columns_surrogate() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("post".to_string(), ValueType::Numeric),
        ("tag".to_string(), ValueType::Text),
    ])
}

/// The #94 transform that ships today — the closest thing in the engine to
/// #102's target shape (same relationship join, same aggregate, grouping key
/// still a bare source column).
const TAG_TOTALS: &str = "TRANSFORM tag_totals FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words";

async fn install(pool: &trellis::Pool, columns: &HashMap<String, ValueType>) {
    create_relationship(pool, "RELATIONSHIP post FROM post_tags.post TO posts.id")
        .await
        .expect("create to-one relationship");
    install_definition(pool, TAG_TOTALS, columns, "public")
        .await
        .expect("install tag_totals");
}

/// Every row currently sitting in any ring segment, as
/// `(seg_slot, src_table, key, op, old_image, new_image, hop_gen)`.
async fn ring_rows(
    client: &Client,
) -> Vec<(
    i32,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    i32,
)> {
    let mut out = Vec::new();
    for slot in 0..4i32 {
        let sql = format!(
            "select {slot}, src_table, key, op, old_image::text, new_image::text, hop_gen \
             from seg_{slot} order by change_id"
        );
        for r in client.query(sql.as_str(), &[]).await.expect("read ring") {
            out.push((
                r.get(0),
                r.get(1),
                r.get(2),
                r.get(3),
                r.get(4),
                r.get(5),
                r.get(6),
            ));
        }
    }
    out
}

// =====================================================================
// SPIKE A — the linchpin: are old + new parent images actually available
// on the real logical-replication stream?
// =====================================================================

#[tokio::test]
async fn spike_a_real_replication_delivers_both_parent_images_on_an_author_change() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SCHEMA).await.expect("schema");
    raw.batch_execute(
        "insert into posts values (1, 'ada', 100), (2, 'bob', 250); \
         insert into post_tags values (1,'rust'), (1,'db'), (2,'rust');",
    )
    .await
    .expect("seed");
    install(&db.pool, &post_tags_columns()).await;

    // Staging-only: intake stages real CDC into the ring, but nothing drains
    // it, so the raw staged rows survive for inspection.
    let options = ClientOptions {
        staging_worker: true,
        application_threads: 0,
        source_tables: vec![
            format!("{DEFAULT_SCHEMA}.posts"),
            format!("{DEFAULT_SCHEMA}.post_tags"),
        ],
        ..Default::default()
    };
    let _client = TrellisClient::start(db.dsn(), options).expect("client start");

    // Give intake time to reconcile the publication and create the slot
    // before producing the change we want captured.
    poll_until(Duration::from_secs(30), "slot never appeared", async || {
        raw.query_one("select count(*) from pg_replication_slots", &[])
            .await
            .map(|r| r.get::<_, i64>(0) > 0)
            .unwrap_or(false)
    })
    .await;

    raw.execute(
        "update posts set author = 'ada2', word_count = 111 where id = 1",
        &[],
    )
    .await
    .expect("change the parent's grouping column");

    poll_until(
        Duration::from_secs(30),
        "the posts UPDATE never reached the ring",
        async || {
            ring_rows(&raw)
                .await
                .iter()
                .any(|r| r.1.ends_with("posts") && r.3 == "update")
        },
    )
    .await;

    let rows = ring_rows(&raw).await;
    let upd = rows
        .iter()
        .find(|r| r.1.ends_with("posts") && r.3 == "update")
        .expect("a staged posts update");

    println!("SPIKE A staged posts UPDATE:");
    println!("  src_table = {}", upd.1);
    println!("  key       = {:?}", upd.2);
    println!("  op        = {}", upd.3);
    println!("  old_image = {:?}", upd.4);
    println!("  new_image = {:?}", upd.5);

    let old = upd.4.as_deref().expect("old image present");
    let new = upd.5.as_deref().expect("new image present");
    assert!(
        old.contains("ada") && !old.contains("ada2"),
        "old image must carry the PRE-change author: {old}"
    );
    assert!(
        new.contains("ada2"),
        "new image must carry the post-change author: {new}"
    );
    assert!(
        old.contains("100"),
        "old image must carry the pre-change word_count: {old}"
    );
    assert!(
        new.contains("111"),
        "new image must carry the post-change word_count: {new}"
    );
    // Issue #56: REPLICA IDENTITY FULL marks every column is_key in pgoutput,
    // but intake overrides that with the real primary key, so the staged key
    // is still just the PK.
    assert_eq!(
        upd.2, "1",
        "staged key is the primary key, not every column"
    );
}

// =====================================================================
// SPIKE A2 — the ordering hazard: a from-side change drained STRICTLY
// BEFORE the reverse work its parent's change queued.
// =====================================================================

async fn active_seg_table(client: &Client) -> String {
    let slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("segment pointer")
        .get(0);
    format!("seg_{slot}")
}

async fn stage_cdc(
    client: &Client,
    src: &str,
    key: &str,
    op: &str,
    old: Option<&str>,
    new: Option<&str>,
) {
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1,$2,$3,$4,$5::text::jsonb,$6::text::jsonb,0)"
            ),
            &[&src, &key, &op, &lsn, &old, &new],
        )
        .await
        .expect("stage cdc");
}

async fn seal_and_drain_one(pool: &trellis::Pool, client: &mut Client) -> i64 {
    let outcome = seal::seal_phase1(client).await.expect("seal 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal 2");
    let seg = outcome.sealed_seg_seq;
    while apply::drain_once(pool, seg, "spike102", 1, "trellis_spike102")
        .await
        .expect("drain_once")
        .is_some()
    {}
    retire_drained_segments(client).await.expect("retire");
    seg
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        seal_and_drain_one(pool, client).await;
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("no quiescence");
}

/// The ordering issue #102's delta design cannot tolerate: a `posts` change is
/// drained in batch N, queueing reverse work for batch N+1 or later; a
/// `post_tags` INSERT that commits *after* that `posts` change is staged into
/// the next segment and drained in between. The insert's forward delta would
/// resolve its group against the ALREADY-UPDATED parent, and the reverse delta
/// would then move that same row out of a group it was never in.
///
/// This test asserts only the *schedule* — that such an interleaving is
/// reachable — since the delta itself doesn't exist in the engine.
#[tokio::test]
async fn spike_a2_a_from_side_insert_drains_before_the_parents_reverse_work() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SCHEMA_SURROGATE).await.expect("schema");
    raw.batch_execute(
        "insert into posts values (1,'ada',100); insert into post_tags values (10,1,'rust');",
    )
    .await
    .expect("seed");
    install(&db.pool, &post_tags_columns_surrogate()).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    // Batch N: the parent's author change, alone in its segment.
    raw.execute("update posts set author='ada2' where id=1", &[])
        .await
        .expect("author change");
    stage_cdc(
        &raw,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"author\":\"ada\",\"word_count\":100}"),
        Some("{\"id\":1,\"author\":\"ada2\",\"word_count\":100}"),
    )
    .await;

    // Seal it, so the next change lands in a *different* segment.
    let parent_outcome = seal::seal_phase1(&mut raw).await.expect("seal 1");
    seal::seal_phase2(&mut raw, parent_outcome.sealed_seg_seq)
        .await
        .expect("seal 2");
    let parent_seg = parent_outcome.sealed_seg_seq;

    // A from-side INSERT commits AFTER the author change and stages into the
    // now-active segment.
    raw.execute("insert into post_tags values (11,1,'db')", &[])
        .await
        .expect("insert post_tag");
    stage_cdc(
        &raw,
        "post_tags",
        "11",
        "insert",
        None,
        Some("{\"id\":11,\"post\":1,\"tag\":\"db\"}"),
    )
    .await;
    // Seal that one too, so the reverse work the parent batch queues can only
    // land in a segment *after* it.
    let child_outcome = seal::seal_phase1(&mut raw).await.expect("seal 1");
    seal::seal_phase2(&mut raw, child_outcome.sealed_seg_seq)
        .await
        .expect("seal 2");
    let child_seg = child_outcome.sealed_seg_seq;

    // Drain the parent batch. Its reverse work is appended into whatever is
    // ACTIVE now -- which is a segment strictly after `child_seg`.
    while apply::drain_once(&db.pool, parent_seg, "spike102", 1, "trellis_spike102")
        .await
        .expect("drain parent")
        .is_some()
    {}

    let active_after: i64 = raw
        .query_one("select active_seq from segment_pointer", &[])
        .await
        .expect("pointer")
        .get(0);
    let rows = ring_rows(&raw).await;
    let reverse: Vec<_> = rows
        .iter()
        .filter(|r| r.3 == "recompute" && r.1.ends_with("post_tags"))
        .collect();

    println!("SPIKE A2 schedule:");
    println!("  parent (posts UPDATE) batch seg_seq = {parent_seg}");
    println!("  child  (post_tags INSERT) batch seg_seq = {child_seg}");
    println!("  active seg_seq after draining the parent = {active_after}");
    for r in &reverse {
        println!(
            "  queued reverse work: src={} key={:?} hop_gen={}",
            r.1, r.2, r.6
        );
    }

    assert!(
        !reverse.is_empty(),
        "the parent change must queue reverse work for post_tags"
    );
    assert!(
        child_seg < active_after,
        "the from-side INSERT's batch ({child_seg}) must be drained strictly before the \
         segment the parent's reverse work landed in ({active_after}) -- this is the \
         interleaving that makes a reverse DELTA unsound"
    );
    // And the queued reverse work covers the row that was inserted after the
    // parent change: the enumeration is a LIVE read of post_tags.
    assert!(
        reverse.iter().any(|r| r.2 == "11"),
        "reverse enumeration is live, so it picks up the row inserted after the parent change: {reverse:?}"
    );
}

// =====================================================================
// SPIKE A3 — an unconditional blocker for issue #102's own example: the
// natural `post_tags (post, tag)` composite primary key.
// =====================================================================

/// Issue #102 lists "requires `post_tags` to gain a single-column surrogate
/// PK" as a cost of the *3-transform workaround* that the native grouping key
/// would avoid. It does not: `apply::compute` introspects every batch's source
/// primary key through `ddl::source_primary_key`, which rejects a composite
/// key outright, and the reverse path does the same again for the
/// relationship's from-side. A composite-PK source cannot drain at all today,
/// native grouping key or not.
#[tokio::test]
async fn spike_a3_a_composite_pk_from_side_cannot_drain_at_all() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(SCHEMA).await.expect("schema");
    raw.batch_execute(
        "insert into posts values (1,'ada',100); insert into post_tags values (1,'rust');",
    )
    .await
    .expect("seed");
    install(&db.pool, &post_tags_columns()).await;

    stage_cdc(
        &raw,
        "post_tags",
        "1\u{1f}db",
        "insert",
        None,
        Some("{\"post\":1,\"tag\":\"db\"}"),
    )
    .await;
    let outcome = seal::seal_phase1(&mut raw).await.expect("seal 1");
    seal::seal_phase2(&mut raw, outcome.sealed_seg_seq)
        .await
        .expect("seal 2");

    let err = apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        "spike102",
        1,
        "trellis_spike102",
    )
    .await
    .expect_err("a composite-PK source must fail to drain");
    println!("SPIKE A3: draining a composite-PK source fails with: {err:?}");
    assert!(
        format!("{err:?}").contains("CompositePrimaryKeyUnsupported"),
        "expected CompositePrimaryKeyUnsupported, got {err:?}"
    );
}
