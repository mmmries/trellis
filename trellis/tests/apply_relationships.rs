//! Integration tests for the relationship *reverse* recompute (issue #30):
//! when a to-side (related) row changes, every from-side row whose enrichment
//! reads it must be re-derived, converging to a Postgres LEFT JOIN (to-one) or
//! correlated aggregate (to-many).
//!
//! Like `apply.rs`, each test builds its own source/to-side tables and target
//! by hand and stages changes directly into the ring. The relationship-enriched
//! transform is created via a valid placeholder definition (so all the catalog
//! plumbing — nodes, edges, target table — is set up the normal way) and its
//! `definition_text` is then rewritten to the relationship form: the validator
//! still rejects a `<rel>.<column>` path at `create_definition` time (that
//! front-door wiring is a later epic issue), but `compute` re-parses the stored
//! text, which is all this issue's staging path needs.
//!
//! Reverse recompute stages *new* `Recompute` rows into the active segment, so
//! a single seal+drain never settles: `drain_to_quiescence` re-seals and drains
//! until nothing is pending.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::ast::{
    Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef, ValueType,
};
use trellis::defs::{
    create_definition, create_relationship, create_target_table, render_relationship_select_sql,
    source_primary_key,
};
use trellis::staging::apply;
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

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

/// The ring table backing the currently-active segment (where `append` — and
/// so reverse recompute — writes). The ring is a fixed 4-slot set `seg_0..3`.
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// Stages one image-bearing (CDC-shaped) change into the active ring segment.
async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let table = active_seg_table(client).await;
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
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
/// Reverse recompute appends fresh `Recompute` rows into the (new) active
/// segment as it drains, so convergence takes more than one seal.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(pool, seg, "reverse_test", 1, "trellis_apply_test")
            .await
            .expect("drain_once")
            .is_some()
        {}
        // Free the drained ring slots so repeated seals don't exhaust the ring.
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

// ---------------------------------------------------------------------
// To-one: a bare `category.name` enrichment
// ---------------------------------------------------------------------

fn to_one_oracle_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![
            FieldDef {
                name: "id".to_string(),
                expr: Expr::Column("id".to_string()),
            },
            FieldDef {
                name: "category_name".to_string(),
                expr: Expr::RelationshipPath {
                    rel: "category".to_string(),
                    column: "name".to_string(),
                },
            },
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// A valid (relationship-free) stand-in with the same target and column shape
/// (id pk + `category_name` text) as the real definition, so `create_target_table`
/// — which type-infers and would reject a relationship path — can build the
/// target. The real `definition_text` is written in afterward.
fn to_one_placeholder_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::Column("title".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

fn category_rel() -> HashMap<String, RelationshipDef> {
    HashMap::from([(
        "category".to_string(),
        RelationshipDef {
            name: "category".to_string(),
            from_table: "articles".to_string(),
            from_col: "category_id".to_string(),
            to_table: "categories".to_string(),
            to_col: "id".to_string(),
        },
    )])
}

/// The Postgres LEFT JOIN authority: `article_cat` must equal this after every
/// mutation, keyed by id (text) to `category_name` (text).
async fn oracle_to_one(client: &Client) -> HashMap<String, Option<String>> {
    let base = render_relationship_select_sql(&to_one_oracle_def(), &category_rel());
    let sql = format!("select id::text, category_name::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query to-one oracle")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn target_to_one(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select id::text, category_name::text from article_cat", &[])
        .await
        .expect("read article_cat")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test]
async fn reverse_recompute_to_one_converges_across_related_row_mutations() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // To-side keeps Postgres's DEFAULT replica identity (its primary key) — the
    // point of this test is that the reverse path needs no `REPLICA IDENTITY
    // FULL` on the to-side, because a to-one's `to_col` *is* that primary key.
    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text)",
        )
        .await
        .expect("create tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    // Placeholder definition: valid (no relationship path) so `create_definition`
    // sets up nodes/edges/target, then rewritten to the relationship form.
    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
        ("title", ValueType::Text),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT title AS category_name",
        &source_columns,
    )
    .await
    .expect("create placeholder definition");
    let pk = source_primary_key(&db.pool, "articles")
        .await
        .expect("introspect articles pk");
    create_target_table(
        &db.pool,
        &to_one_placeholder_def(),
        "public",
        &pk,
        &source_columns,
        &to_one_placeholder_def().source,
    )
    .await
    .expect("create target table");
    client
        .execute(
            // Issue #73: `target_table` is persisted fully-qualified now —
            // `article_cat` was created via `create_target_table(..., "public", ...)`
            // above, so it landed under `DEFAULT_TARGET_SCHEMA`.
            &format!(
                "update transform_definitions \
                 set definition_text = 'TRANSFORM article_cat FROM articles SELECT category.name AS category_name' \
                 where target_table = '{DEFAULT_TARGET_SCHEMA}.article_cat'"
            ),
            &[],
        )
        .await
        .expect("rewrite definition to relationship form");

    // Step 1 — insert two articles pointing at not-yet-existent categories
    // (forward eval: enrichment resolves to NULL, LEFT JOIN no-match).
    client
        .batch_execute(
            "insert into articles (id, category_id, title) values \
             (1, 10, 'a1'), (2, 20, 'a2')",
        )
        .await
        .expect("insert articles");
    stage_cdc(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10,\"title\":\"a1\"}"),
    )
    .await;
    stage_cdc(
        &client,
        "articles",
        "2",
        "insert",
        None,
        Some("{\"id\":2,\"category_id\":20,\"title\":\"a2\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after article inserts"
    );

    // Step 2 — insert category 10: reverse recompute re-derives article 1.
    client
        .execute("insert into categories (id, name) values (10, 'Tech')", &[])
        .await
        .expect("insert category 10");
    stage_cdc(
        &client,
        "categories",
        "10",
        "insert",
        None,
        Some("{\"id\":10,\"name\":\"Tech\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category insert"
    );

    // Step 3 — update category 10's name: reverse recompute re-derives article 1.
    client
        .execute(
            "update categories set name = 'Technology' where id = 10",
            &[],
        )
        .await
        .expect("update category name");
    stage_cdc(
        &client,
        "categories",
        "10",
        "update",
        Some("{\"id\":10,\"name\":\"Tech\"}"),
        Some("{\"id\":10,\"name\":\"Technology\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category name update"
    );

    // Step 4 — re-parent the category's own key 10 -> 20: article 1 (still
    // pointing at 10) is orphaned; article 2 (pointing at 20) now matches.
    // The OLD key (10) rides in the update's pre-image, the NEW key (20) in the
    // post-image — both from the default replica identity.
    client
        .execute("update categories set id = 20 where id = 10", &[])
        .await
        .expect("re-parent category id");
    stage_cdc(
        &client,
        "categories",
        "20",
        "update",
        Some("{\"id\":10,\"name\":\"Technology\"}"),
        Some("{\"id\":20,\"name\":\"Technology\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category re-parent"
    );

    // Step 5 — delete category 20: article 2 falls back to NULL.
    client
        .execute("delete from categories where id = 20", &[])
        .await
        .expect("delete category 20");
    stage_cdc(
        &client,
        "categories",
        "20",
        "delete",
        Some("{\"id\":20,\"name\":\"Technology\"}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after category delete"
    );
}

// ---------------------------------------------------------------------
// To-many: reverse resolver stages the right from-side recomputes
// ---------------------------------------------------------------------
//
// The aggregate-enrichment eval (`sum(<rel>.<col>)`, #29) can't yet be exercised
// end-to-end here: the parser rejects an aggregate call in a OneToOne target, so
// no `definition_text` produces that AST (a separate grammar/validate wiring
// issue). What #30 owns for to-many — the reverse *resolver* — is cardinality-
// agnostic and testable directly: a to-side (related) row change must stage an
// image-less recompute of exactly the from-side keys whose join key matches,
// which this test asserts by reading the staged `Recompute` rows.

/// The from-side `Recompute` keys the reverse resolver staged into the active
/// ring segment, distinct and sorted.
async fn staged_from_side_recomputes(client: &Client, from_table: &str) -> Vec<String> {
    let seg = active_seg_table(client).await;
    let sql = format!("select distinct key from {seg} where src_table = $1 order by key");
    client
        .query(sql.as_str(), &[&from_table])
        .await
        .expect("read staged recomputes")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// Seals the segment holding the just-staged to-side change and drains it; the
/// reverse recompute the resolver stages lands in the NEW active segment, whose
/// from-side keys are returned. Then flushes to quiescence so the next step
/// starts from a clean, retired ring.
async fn reverse_keys_for_to_side_change(
    pool: &trellis::Pool,
    client: &mut Client,
    from_table: &str,
) -> Vec<String> {
    let seg = seal_active_segment(client).await;
    while apply::drain_once(pool, seg, "reverse_test", 1, "trellis_apply_test")
        .await
        .expect("drain_once")
        .is_some()
    {}
    let keys = staged_from_side_recomputes(client, from_table).await;
    drain_to_quiescence(pool, client).await;
    keys
}

#[tokio::test]
async fn reverse_recompute_to_many_stages_from_side_recomputes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // A to-many's join column (`comments.article_id`) is NOT the to-side primary
    // key, so it is absent from a delete/re-parent's DEFAULT replica-identity
    // pre-image. REPLICA IDENTITY FULL puts it in the old image — the same
    // requirement issue #7 already imposes on aggregate sources whose old image
    // a derivation needs. (A to-*one*'s join column IS the primary key, which is
    // why the to-one test above needs no FULL.) The from-side needs only live
    // rows: the reverse resolver stages recomputes off the relationship graph,
    // independent of whether a from-side transform exists yet.
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             alter table comments replica identity full; \
             insert into articles (id, title) values (1, 'a1'), (2, 'a2')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create to-many relationship");

    // Insert: a comment on article 1 -> recompute article 1.
    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5)",
            &[],
        )
        .await
        .expect("insert comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string()],
        "comment insert re-derives its article"
    );

    // Update word_count (same article) -> recompute article 1.
    client
        .execute("update comments set word_count = 9 where id = 100", &[])
        .await
        .expect("update comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "update",
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
        Some("{\"id\":100,\"article_id\":1,\"word_count\":9}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string()],
        "comment update re-derives its article"
    );

    // Re-parent the comment from article 1 to article 2 -> recompute BOTH. The
    // OLD article_id (1) comes from the FULL pre-image, the NEW (2) from the
    // post-image.
    client
        .execute("update comments set article_id = 2 where id = 100", &[])
        .await
        .expect("re-parent comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "update",
        Some("{\"id\":100,\"article_id\":1,\"word_count\":9}"),
        Some("{\"id\":100,\"article_id\":2,\"word_count\":9}"),
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["1".to_string(), "2".to_string()],
        "re-parent re-derives both old and new article"
    );

    // Delete the comment (now on article 2) -> recompute article 2, join key
    // from the FULL pre-image.
    client
        .execute("delete from comments where id = 100", &[])
        .await
        .expect("delete comment");
    stage_cdc(
        &client,
        "comments",
        "100",
        "delete",
        Some("{\"id\":100,\"article_id\":2,\"word_count\":9}"),
        None,
    )
    .await;
    assert_eq!(
        reverse_keys_for_to_side_change(&db.pool, &mut client, "articles").await,
        vec!["2".to_string()],
        "comment delete re-derives its article"
    );
}

// ---------------------------------------------------------------------
// Issue #79: two to-many relationships sharing one from_table must dedupe
// ---------------------------------------------------------------------

/// The number of `Recompute` rows staged for `from_table`/`key` in the active
/// segment — deliberately *not* `distinct`, unlike [`staged_from_side_recomputes`]
/// above: issue #79 is exactly a case where the same key is staged more than
/// once, which a `distinct` read would silently hide.
async fn staged_recompute_count(client: &Client, from_table: &str, key: &str) -> i64 {
    let seg = active_seg_table(client).await;
    let sql = format!("select count(*) from {seg} where src_table = $1 and key = $2");
    client
        .query_one(sql.as_str(), &[&from_table, &key])
        .await
        .expect("count staged recomputes")
        .get(0)
}

#[tokio::test]
async fn reverse_recompute_dedupes_across_relationships_sharing_from_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Two independent to-many relationships into the same from-table
    // (`articles`), mirroring issue #79's `posts`/`comments` -> `authors`
    // shape: a single batch that touches article 1 through *both* relationships
    // must stage exactly one recompute for article 1, not one per relationship.
    client
        .batch_execute(
            "create table articles (id integer primary key, title text); \
             create table comments (id integer primary key, article_id integer, word_count integer); \
             create table likes (id integer primary key, article_id integer); \
             alter table comments replica identity full; \
             alter table likes replica identity full; \
             insert into articles (id, title) values (1, 'a1')",
        )
        .await
        .expect("create tables and seed from-side rows");

    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM articles.id TO comments.article_id",
    )
    .await
    .expect("create comments relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP likes FROM articles.id TO likes.article_id",
    )
    .await
    .expect("create likes relationship");

    client
        .execute(
            "insert into comments (id, article_id, word_count) values (100, 1, 5)",
            &[],
        )
        .await
        .expect("insert comment");
    client
        .execute("insert into likes (id, article_id) values (200, 1)", &[])
        .await
        .expect("insert like");

    // Both to-side changes land in the *same* batch (one seal drains both),
    // so `compute`'s single call sees inbound relationships from two
    // different source tables (`comments`, `likes`) that share `articles`
    // as their common from_table.
    stage_cdc(
        &client,
        "comments",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"article_id\":1,\"word_count\":5}"),
    )
    .await;
    stage_cdc(
        &client,
        "likes",
        "200",
        "insert",
        None,
        Some("{\"id\":200,\"article_id\":1}"),
    )
    .await;

    let seg = seal_active_segment(&mut client).await;
    while apply::drain_once(&db.pool, seg, "reverse_test", 1, "trellis_apply_test")
        .await
        .expect("drain_once")
        .is_some()
    {}

    assert_eq!(
        staged_recompute_count(&client, "articles", "1").await,
        1,
        "article 1 must be staged exactly once even though two relationships \
         (comments, likes) both touched it in this batch"
    );

    drain_to_quiescence(&db.pool, &mut client).await;
}
