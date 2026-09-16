//! Integration tests for issue #131 (epic #127): the to-one relationship
//! reverse **delta** — a parent-keyed record carrying the parent's old/new
//! image and a `(prev_lsn, lsn)` chain, applied as paired subtract-old/
//! add-new over the parent's from-side rows, replacing the pre-#131
//! image-less from-side `Recompute` for to-one relationships specifically.
//! See `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §2 and §7 Phase 1
//! steps 4-5, and `staging::apply`'s own module doc comment (the "Issue
//! #131, epic #127" section) for the mechanism.
//!
//! Reuses issue #94's `defs_aggregate_relationship.rs` schema/fixture
//! (`post_tags` from-side, `posts` to-side, `SUM`/`COUNT` grouped by `tag`)
//! since it's already exactly the fully-invertible, single-relationship
//! shape this issue's fast path targets, and its dangling reference (a
//! `post_tags` row pointing at `post = 999`, which doesn't exist in the seed
//! data) doubles as a ready-made parent-insert fixture.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::defs::{
    create_definition, create_relationship, create_target_table, install_definition,
    relationship_projection, source_primary_key,
};
use trellis::staging::apply::{self, ApplyPlan};
use trellis::staging::{StagedWatermark, claim, fold, has_pending, retire_drained_segments};

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

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// Stages one image-bearing (CDC-shaped) change into the active ring
/// segment, at an explicit `lsn` — unlike most of this crate's other test
/// files' `stage_cdc` helpers, which pin every row to `lsn = 1`, several
/// tests here need distinct, ordered LSNs (the fold's "latest wins" rule,
/// and the reverse record's own `lsn`/`prev_lsn` chain, both key off it).
async fn stage_cdc_at_lsn(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    lsn: u64,
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(lsn);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Issue #133's own staging helper: [`stage_cdc_at_lsn`] plus an explicit
/// `group_key` — standing in for what real intake's `cdc_change`/
/// `touched_group_key` would have populated from the row's own old/new
/// images for its outbound relationship's `from_col`, since these tests
/// stage directly into the ring rather than running a real replication
/// stream.
async fn stage_cdc_with_group_key_at_lsn(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    lsn: u64,
    group_key: &[&str],
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(lsn);
    let group_key: Vec<&str> = group_key.to_vec();
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen, group_key) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, $7)"
            ),
            &[
                &src_table, &key, &op, &lsn, &old_image, &new_image, &group_key,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark for the tests
    // that just want the pipeline to converge — the guard (a)-specific
    // tests below build their own `StagedWatermark` by hand instead of
    // using this helper.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "reverse_test",
            1,
            "trellis_reverse_test",
            &watermark,
        )
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

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

fn post_tags_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("post", ValueType::Numeric),
        ("tag", ValueType::Text),
    ])
}

const TAG_TOTALS: &str = "TRANSFORM tag_totals FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words";

/// Issue #94's exact schema (see `defs_aggregate_relationship.rs`), reused
/// verbatim: `post_tags` row 13 (`post = 999`, tag `rust`) is a dangling
/// reference to a post that doesn't exist yet — this file's parent-insert
/// test inserts it.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer); \
             create table post_tags (id integer primary key, post integer, tag text); \
             alter table post_tags replica identity full; \
             alter table posts replica identity full; \
             create index on post_tags (post); \
             insert into posts (id, word_count) values (1, 100), (2, 250), (3, null); \
             insert into post_tags (id, post, tag) values \
               (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db'), \
               (13, 999, 'rust'), (14, 3, 'db')",
        )
        .await
        .expect("create + seed issue #94's schema");
}

type Totals = HashMap<String, (Option<String>, Option<String>)>;

async fn target_totals(client: &Client) -> Totals {
    client
        .query(
            "select tag, post_count::text, total_words::text from tag_totals",
            &[],
        )
        .await
        .expect("read tag_totals")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

async fn projection_table_for(pool: &trellis::Pool, relationship_id: i64) -> String {
    relationship_projection(pool, relationship_id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection")
        .projection_table
}

async fn projection_lsn(client: &Client, projection_table: &str, id: i32) -> Option<PgLsn> {
    client
        .query_one(
            &format!("select __trellis_lsn from {projection_table} where id = $1"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read {projection_table}'s lsn for id {id}: {e}"))
        .get(0)
}

/// Drives `claim`/`fold`/`compute` by hand for one segment, stopping short
/// of applying it — the building block both the guard-(d) stopgap tests in
/// this file share, so two (or more) segments' Phase 2 can be forced to run
/// before either's Phase 3 commits (the out-of-order-drain race guard (d)
/// exists for; `drain_once` always applies a segment immediately after
/// computing it, so it can't construct this scenario on its own).
async fn claim_fold_compute(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> ApplyPlan {
    let mut phase1 = pool.get().await.expect("connection");
    let txn = phase1.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, claimed_by, 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by)
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    apply::compute(pool, &folded).await.expect("compute")
}

async fn projection_row_exists(client: &Client, projection_table: &str, id: i32) -> bool {
    client
        .query_one(
            &format!("select exists (select 1 from {projection_table} where id = $1)"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("probe {projection_table} for id {id}: {e}"))
        .get(0)
}

/// Issue #132 guard (b)'s own column — read the same way [`projection_lsn`]
/// reads guard (d)'s.
async fn projection_gen(client: &Client, projection_table: &str, id: i32) -> i64 {
    client
        .query_one(
            &format!("select __trellis_gen from {projection_table} where id = $1"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read {projection_table}'s gen for id {id}: {e}"))
        .get(0)
}

/// The `key`s of every image-less `recompute` row currently staged for
/// `src_table` in the *active* ring segment — the guard-rejection stopgap's
/// own signal (see the guard-(d) tests above for the same pattern), reused
/// by issue #132's guard (a)/(b)/(c) tests below.
async fn staged_recompute_keys(client: &Client, src_table: &str) -> Vec<String> {
    let table = active_seg_table(client).await;
    let src_table = qualify_fixture_table(src_table);
    client
        .query(
            &format!(
                "select key from {table} where src_table = $1 and op = 'recompute' order by key"
            ),
            &[&src_table],
        )
        .await
        .expect("read staged recompute rows")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

// ---------------------------------------------------------------------
// 1. A to-side update is a real delta, not a live recompute.
// ---------------------------------------------------------------------

/// A to-side (parent) update that changes a `SUM`-read column advances the
/// settled parent projection's `__trellis_lsn` to the change's own `lsn` —
/// a signature only issue #131's reverse-delta apply produces. The pre-#131
/// mechanism (an image-less from-side `Recompute`) never wrote to the
/// projection at all, so this is a direct, positive proof the new path ran,
/// not just an end-state convergence check (which an eventually-correct
/// live recompute would also pass).
#[tokio::test]
async fn to_side_update_advances_the_projection_lsn_via_the_delta_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "the reverse-delta apply must advance the projection's own lsn chain \
         to the applied record's lsn"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string())))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("2".to_string()), Some("400".to_string())))
    );
}

// ---------------------------------------------------------------------
// 2. N parent changes to the same key in one batch fold to one record.
// ---------------------------------------------------------------------

/// Two updates to the same to-side row, staged into the *same* segment
/// before it seals, fold (via the ordinary, pre-existing CDC fold — see
/// `RelationshipReverseRecord`'s doc comment on why no new ring plumbing was
/// needed) to one reverse record: `old_image` from the earliest raw change,
/// `new_image`/`lsn` from the latest. The target must reflect only the
/// *net* effect (100 -> 500, never a transient 100 -> 400 -> 500 that a
/// naive per-raw-row apply would double-count), and the projection's lsn
/// must land on the *latest* staged lsn, not the first.
#[tokio::test]
async fn two_parent_changes_in_one_batch_fold_to_one_record() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("update the related post to its final value");
    // Two raw CDC rows for the same key, same batch: 100->400, then
    // 400->500. The pre-existing fold collapses them to old=100, new=500.
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(200)),
        "the folded record's lsn must be the latest of the two raw changes"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("750".to_string()))),
        "net effect only: 250 (post 2) + 500 (post 1, its *final* value) + null"
    );
}

// ---------------------------------------------------------------------
// 3. Parent insert and parent delete.
// ---------------------------------------------------------------------

/// A parent INSERT (`old_image` absent): `post_tags` row 13's dangling
/// reference to `post = 999` starts contributing once that post exists,
/// and the projection gets a brand-new row (not an update to one that
/// never existed).
#[tokio::test]
async fn parent_insert_is_picked_up_by_the_reverse_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert!(
        !projection_row_exists(&client, &projection_table, 999).await,
        "post 999 doesn't exist yet, so the projection must not have a row for it"
    );

    client
        .execute("insert into posts (id, word_count) values (999, 999)", &[])
        .await
        .expect("insert the previously-dangling post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "999",
        "insert",
        None,
        Some("{\"id\":999,\"word_count\":999}"),
        50,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert!(
        projection_row_exists(&client, &projection_table, 999).await,
        "the reverse path must insert a projection row for a brand-new parent"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 999).await,
        Some(PgLsn::from(50))
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("1349".to_string()))),
        "100 (post 1) + 250 (post 2) + 999 (the newly-inserted post 999)"
    );
}

/// A parent DELETE (`new_image` absent): deleting post 2 removes its
/// contribution to `rust`'s total and removes its projection row outright.
#[tokio::test]
async fn parent_delete_is_picked_up_by_the_reverse_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert!(projection_row_exists(&client, &projection_table, 2).await);

    client
        .execute("delete from posts where id = 2", &[])
        .await
        .expect("delete the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "2",
        "delete",
        Some("{\"id\":2,\"word_count\":250}"),
        None,
        75,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert!(
        !projection_row_exists(&client, &projection_table, 2).await,
        "the reverse path must delete the projection row for a deleted parent"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("100".to_string()))),
        "only post 1's 100 remains; post 2 is gone and post 999 never existed"
    );
}

// ---------------------------------------------------------------------
// 4. The ordering stopgap (guard (d), pre-#132) actually rejects a stale
//    prev_lsn, and the pipeline still converges afterward.
// ---------------------------------------------------------------------

/// Simulates two reverse records for the *same* parent key both captured
/// (Phase 2) before either applied (Phase 3) — the scenario the plan doc's
/// guard (d) exists for (§2: "two parent changes in different segments can
/// drain out of order"). Driven by hand at the `compute`/
/// `apply_and_mark_drained` level (rather than `drain_once`, which always
/// applies a segment immediately after computing it) so both computes can
/// be forced to run before either apply commits.
///
/// The first apply succeeds and advances the projection's `lsn`. The
/// second's `prev_lsn` (captured before the first applied) no longer
/// matches, so it must **not** apply its own delta directly — asserted by
/// checking the projection's `lsn` is still the first record's, not the
/// second's, immediately after the second apply commits. The stopgap then
/// re-stages the second record's from-side rows as an image-less
/// `Recompute`, so a further drain to quiescence must still converge to the
/// correct final total (500's contribution, not 400's or double-counted).
#[tokio::test]
async fn a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    // First parent change: 100 -> 400, its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;

    // Phase 2 for segment A — captures `prev_lsn` against the pre-update
    // projection state.
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Second parent change: 400 -> 500 (live value, matching what A's own
    // apply hasn't landed yet), its own later segment — captured *before*
    // A's Phase 3 runs, so it reads the exact same stale `prev_lsn`.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // Apply A: matches, advances the projection to lsn 100.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100))
    );

    // Apply B: its `prev_lsn` (captured before A applied) no longer matches
    // the projection's current lsn (100, not the pre-update seed) — the
    // ordering stopgap must reject B's own delta.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B (rejected internally, but the drain call itself still succeeds)");
    txn.commit().await.expect("commit B");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "B's stale prev_lsn must be rejected — the projection stays at A's lsn, \
         not advanced to B's"
    );

    // The stopgap re-staged B's from-side rows as an image-less recompute;
    // draining that to quiescence must still converge on the *true* final
    // value (500, the live value by now), not 400 (A's value, stale) and
    // not double-counted.
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("750".to_string()))),
        "250 (post 2) + 500 (post 1's true final value, recovered via the stopgap \
         fallback) + null (post 999)"
    );
}

/// Review follow-up to issue #131: the guard-(d) stopgap's fallback loop
/// used to enumerate `old_key`/`new_key` with no dedup, so an *ordinary*
/// same-key parent attribute update (`old_key == new_key`, the common case
/// — a parent PK change or insert/delete is what actually needs both sides)
/// iterated the identical join key twice and staged every matching
/// from-side row's image-less `Recompute` *twice*. Not a correctness bug (a
/// `Recompute` re-derives live state idempotently), but it doubled the ring
/// writes and live-DB reads every time the stopgap fired on an ordinary
/// update.
///
/// Same two-segment setup as `a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges`,
/// but both parent changes touch the same `id = 1` (an ordinary attribute
/// update on both sides, not a repoint) — the exact shape the bug affected
/// — and instead of draining to quiescence, this inspects the ring
/// directly right after B's rejected apply commits: exactly one `recompute`
/// row per from-side row matching post 1 (ids 10 and 12), not two apiece.
#[tokio::test]
async fn a_same_key_stopgap_stages_exactly_one_recompute_per_from_side_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // First parent change: 100 -> 400, its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Second parent change: same key (id = 1), 400 -> 500 — old_key ==
    // new_key, the common case the bug affected. Captured before A applies,
    // so it reads the same stale prev_lsn A is about to consume.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // Apply A: matches, advances the projection to lsn 100.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");

    // Apply B: prev_lsn is stale — the guard-(d) stopgap fires and falls
    // back to Recompute for post_tags rows 10 and 12 (both point at
    // post 1). Neither A's fast-path delta (no downstream readers of
    // tag_totals) nor B's own stopgap branch (it `continue`s past the
    // `needs_recompute_fallback`/projection-advance code) stages anything
    // else, so whatever lands in the now-active ring segment is exactly
    // this test's signal.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B");
    txn.commit().await.expect("commit B");

    let table = active_seg_table(&client).await;
    let post_tags_table = qualify_fixture_table("post_tags");
    let recompute_keys: Vec<String> = client
        .query(
            &format!(
                "select key from {table} where src_table = $1 and op = 'recompute' order by key"
            ),
            &[&post_tags_table],
        )
        .await
        .expect("read staged recompute rows")
        .into_iter()
        .map(|r| r.get(0))
        .collect();

    assert_eq!(
        recompute_keys,
        vec!["10".to_string(), "12".to_string()],
        "exactly one fallback Recompute per from-side row matching post 1 \
         (ids 10 and 12) — not two apiece from redundantly enumerating \
         old_key and new_key when they're equal"
    );
}

// ---------------------------------------------------------------------
// 6. Issue #132's guards (a), (b), (c) — guard (d) is #4 above (issue
//    #131's original stopgap, formalized as one of these four guards in
//    `apply.rs`'s `check_reverse_guards`).
//
// Each test drives `compute`/`apply_and_mark_drained` by hand (the same
// `claim_fold_compute` building block guard (d)'s tests use above) so the
// specific precondition each guard checks can be violated deliberately,
// independent of the other three, mirroring
// `a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges`'s own
// pattern: assert the guard rejected (the projection did not advance, and
// an image-less fallback recompute was staged for the parent's from-side
// rows), then drive the pipeline to quiescence and assert it still
// converges on the true final value.
// ---------------------------------------------------------------------

/// Guard (a) (plan doc §2; ablation 125/3000, and the precondition that
/// makes guard (c) trustworthy at all): a reverse record whose captured
/// watermark `X` (the source's write frontier as of Phase 2) is still ahead
/// of what intake has *staged* must not apply — even though nothing else
/// about the record looks wrong (no concurrent forward apply, no sibling
/// reverse, no in-flight child). Driven with a [`StagedWatermark`]
/// constructed fresh (starts at LSN 0 — see its own doc comment) and never
/// advanced: no live `intake::Intake` runs in this test at all, so a
/// watermark that never moves is exactly "intake hasn't caught up yet."
#[tokio::test]
async fn guard_a_watermark_barrier_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Guard (a): this database has already generated plenty of real WAL
    // (schema DDL, seed inserts, the drain-to-quiescence above) by the time
    // `claim_fold_compute` captured its own `X` a moment ago, so a
    // watermark that starts at (and stays at) LSN 0 is certain to be
    // behind it.
    let unstaged_watermark = StagedWatermark::new();
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &unstaged_watermark,
    )
    .await
    .expect("apply (rejected internally by guard (a), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (a) must reject before ever touching the projection"
    );
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        vec!["10".to_string(), "12".to_string()],
        "guard (a) must fall back to an image-less recompute of post 1's \
         from-side rows"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "the pipeline must still converge on post 1's true value (400) via \
         the fallback, even though guard (a) rejected the direct delta — \
         an aborting reverse defers and retries, it does not stall the batch"
    );
}

/// Guard (b) (plan doc §2; ablation 82/3000): a reverse record whose
/// captured `prev_gen` no longer matches the projection row's current
/// `__trellis_gen`, re-read under `FOR UPDATE` in Phase 3, must not apply —
/// a forward apply resolved this same parent through the projection
/// between Phase 2's capture and Phase 3's lock. Simulated by bumping
/// `__trellis_gen` directly (reaching past the mechanism, matching this
/// suite's own convention for standing in for a piece another issue builds
/// — see e.g. `insert_poison_held` in `quarantine.rs`): building a *real*
/// concurrent forward apply here would need a second, independent
/// from-side change unrelated to this scenario, which is exactly what
/// `apply_and_mark_drained_many`'s "3c" step's gen-bump already is regardless
/// of what triggered it.
#[tokio::test]
async fn guard_b_generation_check_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;
    let baseline_gen = projection_gen(&client, &projection_table, 1).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Simulate "a forward apply landed between Phase 2's capture and Phase
    // 3's lock" by bumping the projection row's gen directly — exactly the
    // effect `apply_and_mark_drained_many`'s "3c" step has, regardless of
    // what change actually triggered it.
    client
        .execute(
            &format!(
                "update {projection_table} set __trellis_gen = __trellis_gen + 1 where id = 1"
            ),
            &[],
        )
        .await
        .expect("bump the projection's gen, simulating a concurrent forward apply");

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (b), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (b) must reject before ever touching the projection's lsn"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 1).await,
        baseline_gen + 1,
        "the projection's gen must stay exactly at the simulated forward \
         apply's bump — guard (b) must not advance it further"
    );
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        vec!["10".to_string(), "12".to_string()],
        "guard (b) must fall back to an image-less recompute of post 1's \
         from-side rows"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "the pipeline must still converge on post 1's true value (400) via \
         the fallback, even though guard (b) rejected the direct delta"
    );
}

/// Issue #133's own regression pin — the exact trace from the issue body
/// (plan doc §3.1): a from-side row (`articles`) is inserted pointing at
/// category 3, then re-pointed to category 2, **within the same batch**.
/// The fold collapses this to `old_image = NULL` (born in the batch),
/// `new_image` naming category 2 — category 3 appears in *neither* folded
/// endpoint. Before #133, nothing signalled that category 3 was touched at
/// all, so its projection row's `gen` never bumped, and a reverse for
/// category 3 holding an enumeration captured before the re-point would
/// wrongly pass guard (b) and move a row (`articles` id 100) that never
/// left. This test proves both halves directly: (1) the erasing batch's own
/// Phase 3 still bumps category 3's `gen` (a positive, direct signal —
/// before #133 this assertion alone fails, since nothing in the folded
/// old/new image ever named category 3), and (2) a stale reverse captured
/// before that bump is then correctly rejected by guard (b) rather than
/// silently applying against a from-side set that already moved on.
///
/// Deliberately uses the `articles`/`categories` `KeySpace::OneToOne`
/// fixture (`article_cat_def`, below), **not** this file's usual
/// `post_tags`/`posts` aggregate fixture: an aggregate definition's forward
/// path goes through `apply_aggregate`'s `rel_joins`/`force_every_group`
/// mechanism (out of scope for this issue — see the plan doc §7 step 8 and
/// this crate's own review notes), which never calls
/// `build_relationship_context` and so never reaches the #133 gen-bump code
/// at all. A `KeySpace::OneToOne` definition reading a relationship is the
/// shape that actually exercises it on the forward path.
#[tokio::test]
async fn issue_133_a_within_batch_repoint_still_bumps_the_erased_intermediate_parents_gen() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full; \
             insert into categories (id, name) values (1, 'A'), (2, 'B'), (3, 'C')",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &articles_columns(),
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &article_cat_def(),
        "public",
        &pk,
        &articles_columns(),
        &article_cat_def().source,
    )
    .await
    .expect("create target table");

    // The pre-existing child: article 100, already pointing at category 3
    // before either of this test's two batches runs — the row guard (b)'s
    // fallback must (correctly) still move, and must be the *only* one it
    // moves. Both the live row (guard (b)'s fallback live-enumerates the
    // *current* table, not the ring) and its staged CDC record (so the
    // forward path picks it up too) are needed.
    client
        .execute(
            "insert into articles (id, category_id) values (100, 3)",
            &[],
        )
        .await
        .expect("insert article 100 pointing at category 3");
    stage_cdc_at_lsn(
        &client,
        "articles",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"category_id\":3}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn_3 = projection_lsn(&client, &projection_table, 3).await;
    let baseline_gen_3 = projection_gen(&client, &projection_table, 3).await;

    // The reverse candidate: a genuine parent change to category 3 (rename
    // it), Phase 2-captured *before* the erasing batch below ever runs — so
    // its `prev_gen` is `baseline_gen_3`, about to go stale.
    client
        .execute("update categories set name = 'C-renamed' where id = 3", &[])
        .await
        .expect("rename category 3");
    stage_cdc_at_lsn(
        &client,
        "categories",
        "3",
        "update",
        Some("{\"id\":3,\"name\":\"C\"}"),
        Some("{\"id\":3,\"name\":\"C-renamed\"}"),
        100,
    )
    .await;
    let seg_r = seal_active_segment(&mut client).await;
    let plan_r = claim_fold_compute(&db.pool, seg_r, "worker_r").await;

    // The erasing batch: `articles` row 200 inserted pointing at category
    // 3, then re-pointed to category 2 — both within the *same*,
    // still-active segment. `group_key` is set explicitly on each raw row
    // (standing in for what real intake's `touched_group_key` would have
    // read off these same old/new images), exactly as issue #133 populates
    // it: the insert's own touched value (3), then the update's own
    // touched values (3 and 2).
    client
        .execute(
            "insert into articles (id, category_id) values (200, 3)",
            &[],
        )
        .await
        .expect("insert an article pointing at category 3");
    stage_cdc_with_group_key_at_lsn(
        &client,
        "articles",
        "200",
        "insert",
        None,
        Some("{\"id\":200,\"category_id\":3}"),
        200,
        &["3"],
    )
    .await;
    client
        .execute("update articles set category_id = 2 where id = 200", &[])
        .await
        .expect("re-point article 200 to category 2");
    stage_cdc_with_group_key_at_lsn(
        &client,
        "articles",
        "200",
        "update",
        Some("{\"id\":200,\"category_id\":3}"),
        Some("{\"id\":200,\"category_id\":2}"),
        300,
        &["3", "2"],
    )
    .await;

    // Fully drain the erasing batch on its own — its Phase 3 is what must
    // bump category 3's gen via the #133 `group_key` signal.
    let seg_e = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        seg_e,
        "worker_e",
        1,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("drain_once seg_e")
    .is_some()
    {}
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");

    // (1) The positive, direct signal: category 3's projection row's gen
    // must have bumped, even though category 3 is invisible in the erasing
    // batch's own folded old/new images — this is exactly the assertion
    // that fails without #133 (the folded-endpoint-only signal never names
    // category 3 at all: old_image is NULL, born in the batch; new_image
    // names category 2).
    assert_eq!(
        projection_gen(&client, &projection_table, 3).await,
        baseline_gen_3 + 1,
        "the erased intermediate parent (category 3) must still get its gen \
         bumped — the folded old/new images alone never name it"
    );

    // (2) Guard (b): the reverse captured *before* that bump must now be
    // rejected, not silently applied against a from-side set that already
    // moved on (article 200 no longer points at category 3).
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg_r,
        "worker_r",
        &plan_r,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (b), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 3).await,
        baseline_lsn_3,
        "guard (b) must reject before ever touching the projection's lsn"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 3).await,
        baseline_gen_3 + 1,
        "guard (b) must not advance gen any further than the erasing batch's own bump"
    );
    assert_eq!(
        staged_recompute_keys(&client, "articles").await,
        vec!["100".to_string()],
        "guard (b) must fall back to an image-less recompute of category 3's \
         *current* from-side rows — article 100 (the original, undisturbed \
         pointer), not article 200 (which re-pointed away and must not be \
         wrongly moved)"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Not "C-renamed": the mechanism-level assertions above are this test's
    // actual #133 proof (the erased parent's gen bumped; guard (b) then
    // correctly rejected the stale reverse and fell back to recomputing
    // exactly article 100, not article 200). The rejected reverse's own
    // projection-advance step is a documented no-op on rejection — see
    // `apply_and_mark_drained_many`'s "3d" step's own comment ("Do NOT
    // touch any target table or the projection for this record") — so the
    // settled parent projection for category 3 stays at its pre-rename
    // value until something re-attempts that reverse. Retrying it is #134's
    // still-unbuilt deferral/retry plumbing, explicitly out of scope here;
    // a `KeySpace::OneToOne` target has no live-join fallback the way an
    // aggregate's `rel_joins` path does (that's why this fixture — not
    // TAG_TOTALS — is what exercises guard (b) at all, per this test's own
    // opening comment), so its recompute re-reads the same (still-stale)
    // projection rather than the live `categories` table.
    assert_eq!(
        target_category_name(&client, 100).await,
        Some("C".to_string()),
        "article 100's target stays on category 3's pre-rename value — the \
         rejected reverse's projection-advance is a no-op by design (#134), \
         not a regression in #133's own fix"
    );
    assert_eq!(
        target_category_name(&client, 200).await,
        Some("B".to_string()),
        "article 200 must reflect category 2 (where it actually re-pointed to), \
         not category 3 (where guard (b) correctly refused to move it)"
    );
}

/// Guard (c) (plan doc §2; ablation 1174/3000 — the guard doing most of the
/// correctness work): a reverse record must not apply while any staged
/// from-side change for its parent's join key, committed at or before `X`,
/// is still undrained — otherwise the paired subtract-old/add-new delta
/// would be computed against a from-side row set that doesn't yet equal
/// what the target actually reflects. Built by staging a brand-new
/// `post_tags` row pointing at post 1 into the *new* active segment (left
/// deliberately unsealed, hence undrained) before computing/applying the
/// *earlier*, already-sealed segment holding post 1's own parent change —
/// guard (a) and guard (d) both pass here (a saturated watermark and an
/// unmoved `prev_lsn`), isolating guard (c) as the one guard that catches
/// this.
#[tokio::test]
async fn guard_c_in_flight_check_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;
    let baseline_gen = projection_gen(&client, &projection_table, 1).await;

    // The parent change, sealed alone into its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;

    // A brand-new from-side row pointing at the same parent, staged into
    // the (new) active segment right after — never sealed, so it stays
    // undrained for the rest of this test.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (20, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tags row pointing at post 1");
    stage_cdc_at_lsn(
        &client,
        "post_tags",
        "20",
        "insert",
        None,
        Some("{\"id\":20,\"post\":1,\"tag\":\"rust\"}"),
        50,
    )
    .await;

    // Phase 2 for the parent's segment — captured *after* the from-side
    // insert above already committed, so `X` (guard (a)'s watermark) is
    // certain to cover it.
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (c), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (c) must reject before ever touching the projection"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 1).await,
        baseline_gen,
        "guard (c) must reject before ever touching the projection"
    );
    // The fallback's own live enumeration picks up every from-side row
    // currently matching post 1 — including the brand-new row 20, since
    // that enumeration is a plain live read, not itself guarded (see
    // `from_side_rows_for_join_txn`'s doc comment).
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        vec!["10".to_string(), "12".to_string(), "20".to_string()],
        "guard (c) must fall back to an image-less recompute of every \
         from-side row currently matching post 1"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("4".to_string()), Some("1050".to_string()))),
        "the pipeline must still converge on post 1's true value (400), \
         counting the new row 20 exactly once: 400 (post 1, via row 10) + \
         250 (post 2) + null (post 999) + 400 (post 1, via the new row 20)"
    );
}

/// Positive case: an ordinary parent update where all four guards
/// genuinely pass — including guard (a), proven with a real
/// [`StagedWatermark`] advanced to (not merely defaulted past) the current
/// `pg_current_wal_lsn()`, not [`StagedWatermark::saturated`]. A direct
/// positive signal that the true-delta path ran (the projection's lsn
/// advances to this record's own lsn) rather than an indirect "the final
/// total happens to be right" check, and that *no* fallback recompute was
/// staged — the fast path fully covered this record.
#[tokio::test]
async fn all_four_guards_pass_and_the_delta_applies_in_the_ordinary_case() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Guard (a) genuinely passes here: seeded at LSN 0, then advanced to
    // the real current `pg_current_wal_lsn()` — strictly past whatever `X`
    // Phase 2 captured a moment ago, since nothing else has committed on
    // this connection since.
    let watermark = StagedWatermark::new();
    let now: PgLsn = client
        .query_one("select pg_current_wal_lsn()", &[])
        .await
        .expect("read the current wal lsn")
        .get(0);
    watermark.advance(now);

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("apply");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "all four guards passed, so the true-delta path must have advanced \
         the projection to this record's own lsn"
    );
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        Vec::<String>::new(),
        "all four guards passed, so no fallback recompute should have been \
         staged for post 1's children"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string())))
    );
}

/// A combined, hand-interleaved scenario loosely modeling the plan doc's
/// own generative ablation campaign (§4): two segments draining
/// out-of-order (guard (d)'s own scenario) *and* a from-side child staged
/// in between, still undrained when the pipeline finally gets back around
/// to it. Not a substitute for that 3,000-run campaign (explicitly out of
/// this issue's scope — see #138), just a hand-built check that the four
/// guards still compose correctly (each one's rejection doesn't corrupt or
/// skip what the others are responsible for) under more than one hazard at
/// once.
#[tokio::test]
async fn out_of_order_segments_plus_an_in_flight_child_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // Segment A: 100 -> 400.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Segment B: 400 -> 500 (the live value), captured before A applies —
    // the same stale-`prev_lsn` setup as guard (d)'s own test.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // A applies cleanly.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");

    // A brand-new from-side row, staged into the current active segment
    // (never sealed here) — still undrained by the time B's rejected apply
    // runs below.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (21, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tags row pointing at post 1");
    stage_cdc_at_lsn(
        &client,
        "post_tags",
        "21",
        "insert",
        None,
        Some("{\"id\":21,\"post\":1,\"tag\":\"rust\"}"),
        150,
    )
    .await;

    // B's `prev_lsn` (captured before A applied) is already stale, so
    // guard (d) rejects it regardless of the in-flight child above — this
    // exercises both hazards landing on the same key in the same window,
    // not just one at a time.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B (rejected internally, but the drain call itself still succeeds)");
    txn.commit().await.expect("commit B");

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("4".to_string()), Some("1250".to_string()))),
        "500 (post 1's true final value, via row 10) + 250 (post 2) + null \
         (post 999) + 500 (post 1's true final value, via the new row 21) \
         — recovered correctly despite the out-of-order segments and the \
         in-flight child both landing on the same key"
    );
}

// ---------------------------------------------------------------------
// 5. To-many relationships are unaffected.
// ---------------------------------------------------------------------

fn articles_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
    ])
}

fn article_cat_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

async fn target_category_name(client: &Client, id: i32) -> Option<String> {
    client
        .query_one(
            "select category_name from article_cat where id = $1",
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read article_cat id {id}: {e}"))
        .get(0)
}

/// A `KeySpace::OneToOne` target reading a to-one relationship is design
/// fork 1 in `build_reverse_relationship_shape`'s doc comment — it has no
/// additive semantics to delta, so it stays on the pre-#131 image-less
/// `Recompute` mechanism (`ReverseRelationshipShape::needs_recompute_fallback`)
/// rather than being folded into the new fast path. This just proves that
/// fallback still converges correctly post-#131 — a regression check for
/// the exact scenario `apply_relationship_forward.rs`'s fixture uses.
#[tokio::test]
async fn a_one_to_one_target_still_converges_via_the_fallback_mechanism() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             insert into articles (id, category_id) values (1, 10)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &articles_columns(),
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &article_cat_def(),
        "public",
        &pk,
        &articles_columns(),
        &article_cat_def().source,
    )
    .await
    .expect("create target table");
    stage_cdc_at_lsn(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_category_name(&client, 1).await,
        Some("Tech".to_string())
    );

    client
        .execute("update categories set name = 'Renamed' where id = 10", &[])
        .await
        .expect("rename the category");
    stage_cdc_at_lsn(
        &client,
        "categories",
        "10",
        "update",
        Some("{\"id\":10,\"name\":\"Tech\"}"),
        Some("{\"id\":10,\"name\":\"Renamed\"}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_category_name(&client, 1).await,
        Some("Renamed".to_string()),
        "a 1-1 target reading the relationship must still re-derive via the \
         pre-#131 fallback, unchanged"
    );
}
