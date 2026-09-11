//! Integration tests for `defs::catalog::install_definition` (issue #63 C1):
//! the front door that creates a definition's target table once, then either
//! builds it directly via the fast set-based backfill
//! (`backfill::backfill_definition`) or falls back to the ring-based
//! `create_definition` when the definition's shape is `Unsupported` by the
//! direct build.
//!
//! Each branch leaves a distinct, checkable signature in the ring: the fast
//! path persists via `create_definition_without_backfill`, which stages no
//! enumeration `Recompute` rows, while the ring fallback's `create_definition`
//! enumerates every existing source row into the active segment. Both tests
//! assert on that signature directly, rather than only on the end-to-end
//! target contents, to confirm each branch actually ran the code path it
//! claims to.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! mirrors `apply_relationships.rs`/`defs_relationship_frontdoor.rs`; see
//! those files for the ring/seal mechanics.

use std::collections::HashMap;

use engine::config::DEFAULT_SCHEMA;
use engine::defs::ast::{Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef};
use engine::defs::{
    ValueType, create_relationship, install_definition, render_relationship_select_sql,
};
use engine::staging::apply;
use engine::staging::{has_pending, retire_drained_segments};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

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

/// The ring table backing the currently-active segment.
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// How many rows are currently staged in the active segment for `src_table` —
/// the fast path's/ring fallback's distinguishing signature (see module doc).
async fn staged_count_for_source(client: &Client, src_table: &str) -> i64 {
    let seg = active_seg_table(client).await;
    let sql = format!("select count(*) from {seg} where src_table = $1");
    client
        .query_one(&sql, &[&src_table])
        .await
        .expect("count staged rows")
        .get(0)
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
async fn drain_to_quiescence(pool: &engine::Pool, client: &mut Client) {
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(pool, seg, "install_def_test", 1, "trellis_install_def_test")
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

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
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

// ---------------------------------------------------------------------
// Fast-path success branch: a plain (non-relationship) 1-1 definition.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_target_without_staging_the_ring() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 500) g",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition via the fast path");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "target built directly and matches every source row"
    );

    // The fast path persists via `create_definition_without_backfill`, which
    // stages no ring-enumeration `Recompute` rows — the opposite of the ring
    // fallback exercised below. Zero staged rows for `s` is the positive
    // signal that the direct build actually ran, not a silent fallback.
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: a plain 1-1 field that references another
// calculated field's alias (issue #83 — `double_price + tax AS total` where
// `double_price = price + price`). WI1 made this shape fall back to the ring
// (safe, but slow); the direct build now inlines the alias chain
// (`substitute_all_fields`) and builds it set-based, so the source is never
// enumerated into the ring. (Supersedes WI1's ring-fallback assertion for
// this shape — expected and correct.)
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_plain_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, price numeric, tax numeric); \
             insert into s (id, price, tax) values (1, 10, 1), (2, 20, 2), (3, 30, 3)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["price", "tax"]);
    // `total` references `double_price`, itself a calculated field
    // (`price + price`). Substitution inlines it to `(price + price) + tax`,
    // so the direct build renders self-contained source SQL.
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT price + price AS double_price, \
         double_price + tax AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds the alias chain directly");

    // The direct build populates the target synchronously and stages nothing
    // in the ring — the fast-path signature (see the sibling fast-path test).
    let mut rows: Vec<(i64, String, String)> = client
        .query(
            "select id, double_price::text, total::text from t order by id",
            &[],
        )
        .await
        .expect("read t")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    rows.sort_by_key(|(id, ..)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "20".to_string(), "21".to_string()),
            (2, "40".to_string(), "42".to_string()),
            (3, "60".to_string(), "63".to_string()),
        ],
        "direct build computes the alias chain correctly for every row"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Unsupported/ring-fallback branch: a bare to-one relationship lookup
// (no aggregate wrapper) — a shape `backfill_relationship_one_to_one`
// explicitly rejects, since it only renders to-many aggregates.
// ---------------------------------------------------------------------

fn to_one_def() -> TransformDef {
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

/// Oracle counterpart of [`to_one_def`], projecting the source PK `id` too so
/// the outer wrapper can key on it — the target table carries `id` as its own
/// PK column, added by `install_definition`'s DDL step.
fn to_one_oracle_def() -> TransformDef {
    let mut def = to_one_def();
    def.fields.insert(
        0,
        FieldDef {
            name: "id".to_string(),
            expr: Expr::Column("id".to_string()),
        },
    );
    def
}

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
async fn install_definition_falls_back_to_ring_for_relationship_enriched_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             insert into categories (id, name) values (10, 'Tech'), (20, 'News'); \
             insert into articles (id, category_id, title) values \
             (1, 10, 'a1'), (2, 20, 'a2'), (3, 99, 'a3')",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
        ("title", ValueType::Text),
    ]);

    // `uses_relationships` routes this to `backfill_relationship_one_to_one`,
    // but a bare to-one lookup (`category.name`, no aggregate) is a shape
    // `collect_agg_leaves` still rejects. `install_definition` must catch the
    // resulting `BackfillError::Unsupported` and fall back to the ring-based
    // `create_definition`, having already created the target table itself
    // (a second `create_target_table` call in the fallback would have errored
    // on the already-existing relation, which never happens here).
    install_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition falls back to the ring path for a relationship-enriched shape");

    // The ring fallback enumerates every existing source row into the active
    // segment — the opposite signal from the fast-path sibling test above —
    // and the target starts empty, since `create_definition` never builds
    // rows synchronously.
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.articles")).await,
        3,
        "ring fallback enumerated every existing source row"
    );
    let empty: i64 = client
        .query_one("select count(*) from article_cat", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(empty, 0, "ring fallback does not build rows synchronously");

    // Draining the ring builds the target from the enumerated backfill,
    // converging to the LEFT JOIN oracle (article 3 -> NULL: category 99
    // doesn't exist) — proof the fallback left the definition and target in a
    // valid, usable state.
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after draining the ring-fallback backfill"
    );

    // The ring path is live going forward: a fresh CDC insert on the source
    // drains through to a correct row too.
    client
        .execute(
            "insert into articles (id, category_id, title) values (4, 10, 'a4')",
            &[],
        )
        .await
        .expect("insert article 4");
    stage_cdc(
        &client,
        "articles",
        "4",
        "insert",
        None,
        Some("{\"id\":4,\"category_id\":10,\"title\":\"a4\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after a live CDC insert following the fallback"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: a relationship-enriched 1-1 field that references
// other (to-many-aggregate) fields' aliases (issue #83 — the original
// report: `count(posts.id), count(comments.id), post_count + comment_count
// AS total`, which used to HARD-CRASH). Substitution inlines `total` to
// `count(posts.id) + count(comments.id)`; the two shared `count` leaves are
// deduped into one staged column each, and the whole tree renders against
// them — so the definition builds directly, no ring enumeration.
// (Supersedes WI1's ring-fallback assertion for this shape.)
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_relationship_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer); \
             create table comments (id integer primary key, author_id integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id) values (100, 1), (101, 1); \
             insert into comments (id, author_id) values (200, 1), (201, 1), (202, 1)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         COUNT(posts.id) AS post_count, \
         COUNT(comments.id) AS comment_count, \
         post_count + comment_count AS total",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds the relationship alias chain directly");

    // Direct build: target populated synchronously, nothing staged in the ring.
    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "select id::text, post_count::text, comment_count::text, total::text \
             from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "5".to_string()
            ),
            (
                "2".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string()
            ),
        ],
        "direct build computes the relationship alias chain correctly"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: a coalesce-wrapped to-many aggregate
// (`coalesce(sum(posts.word_count), 0)`, issue #83) — the normal way to write
// a nullable aggregate. The aggregate leaf nested inside `coalesce` is now
// recognized, staged, and rendered as `coalesce(<staged-ref>, 0)`, so the
// definition builds directly instead of falling back to the ring.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_coalesce_wrapped_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer, word_count integer); \
             alter table posts replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id, word_count) values \
             (100, 1, 10), (101, 1, 20)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         coalesce(sum(posts.word_count), 0) AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds a coalesce-wrapped aggregate directly");

    let mut rows: Vec<(String, String)> = client
        .query(
            "select id::text, total_words::text from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "30".to_string()),
            // No posts -> coalesce(NULL, 0) = 0, not NULL.
            ("2".to_string(), "0".to_string()),
        ],
        "coalesce(sum(...), 0) builds directly with the empty set coalesced to 0"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: the deepest nesting issue #83's repro implies —
// an alias-derived field summing two coalesce-wrapped aggregates over
// *different* relationships (`total_words = total_posted_words +
// total_commented_words`, each itself `coalesce(sum(...), 0)`). Substitution
// inlines both, giving `coalesce(sum(posts.word_count), 0) +
// coalesce(sum(comments.word_count), 0)`; the two distinct SUM leaves are
// staged (one per relationship) and the tree renders against them.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_nested_coalesce_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer, word_count integer); \
             create table comments (id integer primary key, author_id integer, word_count integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id, word_count) values (100, 1, 10), (101, 1, 20); \
             insert into comments (id, author_id, word_count) values (200, 1, 3), (201, 1, 4)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         coalesce(sum(posts.word_count), 0) AS total_posted_words, \
         coalesce(sum(comments.word_count), 0) AS total_commented_words, \
         total_posted_words + total_commented_words AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds the nested coalesce alias chain directly");

    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "select id::text, total_posted_words::text, total_commented_words::text, \
             total_words::text from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            // posts 10+20=30, comments 3+4=7, total 37.
            (
                "1".to_string(),
                "30".to_string(),
                "7".to_string(),
                "37".to_string()
            ),
            // No related rows -> every coalesce(NULL, 0) = 0.
            (
                "2".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string()
            ),
        ],
        "nested coalesce+alias chain builds directly with correct sums"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}
