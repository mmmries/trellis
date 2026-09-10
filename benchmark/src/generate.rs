//! Deterministic `posts` row generation and bulk loading (issue #63,
//! milestone 0).
//!
//! Every derived value is a pure function of `id` — no RNG, no seed — so a
//! given `(n, g)` always produces byte-identical source data across runs,
//! machines, and milestones. That's what lets M0's numbers be compared
//! apples-to-apples against M2/M3/M4 later.

use engine::Pool;
use std::time::{Duration, Instant};

/// `author` is a pure function of `id` and the group count `g`: evenly
/// distributes `n` rows across `g` groups (`id=1..n`, so `author` ranges
/// over `0..g`).
pub fn author_for(id: i64, g: i64) -> i64 {
    id % g
}

/// Every 1000th author (by id) deliberately gets zero children in both
/// `posts` and `comments` — [`load_posts_with_holdout`]/[`load_comments_with_holdout`]
/// skip rows destined for these authors. Without this, `author_for`'s even
/// distribution gives every author at least one child, so the
/// relationship-aggregate benchmark's `COUNT -> 0`/`SUM -> NULL` no-match
/// path (already covered at small scale by
/// `engine/tests/defs_backfill_relationship.rs`) would never be flexed at
/// benchmark scale.
const ZERO_CHILDREN_MODULUS: i64 = 1000;

/// Whether `author` should receive any children under
/// [`ZERO_CHILDREN_MODULUS`]'s deliberate no-match holdout.
pub fn author_has_children(author: i64) -> bool {
    author % ZERO_CHILDREN_MODULUS != 0
}

/// `word_count` is a pure function of `id` alone, bounded to a plausible
/// small range so downstream sums stay easy to sanity-check by eye.
pub fn word_count_for(id: i64) -> i64 {
    100 + (id % 900)
}

/// `byte_size` derives from `word_count` (itself a pure function of `id`)
/// plus a small `id`-dependent wobble, so it's neither a constant multiple
/// of `word_count` nor independent of it.
pub fn byte_size_for(id: i64) -> i64 {
    word_count_for(id) * 6 + (id % 13)
}

const LOAD_BATCH: i64 = 50_000;

/// Creates the `posts` source table: `id` is the primary key (`1..=n`),
/// `author`/`word_count`/`byte_size` are the deterministic columns above.
pub async fn create_posts_table(pool: &Pool) {
    let client = pool.get().await.expect("acquire connection");
    client
        .batch_execute(
            "create table posts ( \
                 id bigint primary key, \
                 author bigint not null, \
                 word_count bigint not null, \
                 byte_size bigint not null \
             )",
        )
        .await
        .expect("create posts table");
}

/// Bulk-loads `n` deterministic rows (`id = 1..=n`) into `posts`, batched
/// via `unnest`-bound array parameters (one round trip per
/// [`LOAD_BATCH`]-sized chunk) rather than one `INSERT` per row. Returns how
/// long the load took — reported for context, but excluded from the
/// benchmark's timed backfill span, which starts only once this data is
/// already sitting in the source table (matching the poc's from-scratch
/// backfill shape).
pub async fn load_posts(pool: &Pool, n: i64, g: i64) -> Duration {
    let start = Instant::now();
    let client = pool.get().await.expect("acquire connection");

    let mut lo = 1i64;
    while lo <= n {
        let hi = (lo + LOAD_BATCH - 1).min(n);
        let batch_len = (hi - lo + 1) as usize;
        let mut ids = Vec::with_capacity(batch_len);
        let mut authors = Vec::with_capacity(batch_len);
        let mut word_counts = Vec::with_capacity(batch_len);
        let mut byte_sizes = Vec::with_capacity(batch_len);
        for id in lo..=hi {
            ids.push(id);
            authors.push(author_for(id, g));
            word_counts.push(word_count_for(id));
            byte_sizes.push(byte_size_for(id));
        }

        client
            .execute(
                "insert into posts (id, author, word_count, byte_size) \
                 select * from unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[])",
                &[&ids, &authors, &word_counts, &byte_sizes],
            )
            .await
            .unwrap_or_else(|e| panic!("bulk-load posts rows {lo}..={hi} failed: {e}"));

        lo = hi + 1;
    }

    start.elapsed()
}

/// Creates the `authors` table used by the relationship-aggregate benchmark
/// scenario (issue #63, C3): the `KeySpace::OneToOne` parent that
/// `posts`/`comments` are to-many children of.
pub async fn create_authors_table(pool: &Pool) {
    let client = pool.get().await.expect("acquire connection");
    client
        .batch_execute("create table authors (id bigint primary key)")
        .await
        .expect("create authors table");
}

/// Loads `g` deterministic author rows (`id = 0..=g-1`), matching the domain
/// [`author_for`] distributes `posts`/`comments` rows over — a single
/// `generate_series` insert rather than a batched round trip, since there's
/// no per-row derived data to compute in Rust.
pub async fn load_authors(pool: &Pool, g: i64) -> Duration {
    let start = Instant::now();
    let client = pool.get().await.expect("acquire connection");
    client
        .execute(
            "insert into authors (id) select generate_series(0, $1::bigint - 1)",
            &[&g],
        )
        .await
        .expect("bulk-load authors rows");
    start.elapsed()
}

/// Creates the second to-many child table used by the relationship-aggregate
/// benchmark scenario: like `posts`, but with no value column of its own —
/// enough to give `COUNT(comments.id)` something to aggregate.
pub async fn create_comments_table(pool: &Pool) {
    let client = pool.get().await.expect("acquire connection");
    client
        .batch_execute(
            "create table comments ( \
                 id bigint primary key, \
                 author bigint not null \
             )",
        )
        .await
        .expect("create comments table");
}

/// Like [`load_posts`], but skips rows destined for a
/// [`ZERO_CHILDREN_MODULUS`]-held-out author, so those authors end up with
/// zero posts — used only by the relationship-aggregate scenario, which
/// wants some authors to exercise the no-match (`SUM -> NULL`) path at
/// scale. [`load_posts`] itself is left untouched: the plain `GROUP BY`
/// scenario asserts every source row lands in its 1-1 calc table, which a
/// holdout would break.
pub async fn load_posts_with_holdout(pool: &Pool, n: i64, g: i64) -> Duration {
    let start = Instant::now();
    let client = pool.get().await.expect("acquire connection");

    let mut lo = 1i64;
    while lo <= n {
        let hi = (lo + LOAD_BATCH - 1).min(n);
        let mut ids = Vec::new();
        let mut authors = Vec::new();
        let mut word_counts = Vec::new();
        let mut byte_sizes = Vec::new();
        for id in lo..=hi {
            let author = author_for(id, g);
            if !author_has_children(author) {
                continue;
            }
            ids.push(id);
            authors.push(author);
            word_counts.push(word_count_for(id));
            byte_sizes.push(byte_size_for(id));
        }

        client
            .execute(
                "insert into posts (id, author, word_count, byte_size) \
                 select * from unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[])",
                &[&ids, &authors, &word_counts, &byte_sizes],
            )
            .await
            .unwrap_or_else(|e| panic!("bulk-load posts rows {lo}..={hi} failed: {e}"));

        lo = hi + 1;
    }

    start.elapsed()
}

/// Like [`load_comments`], with the same [`ZERO_CHILDREN_MODULUS`] holdout
/// as [`load_posts_with_holdout`], and for the same reason.
pub async fn load_comments_with_holdout(pool: &Pool, n: i64, g: i64) -> Duration {
    let start = Instant::now();
    let client = pool.get().await.expect("acquire connection");

    let mut lo = 1i64;
    while lo <= n {
        let hi = (lo + LOAD_BATCH - 1).min(n);
        let mut ids = Vec::new();
        let mut authors = Vec::new();
        for id in lo..=hi {
            let author = author_for(id, g);
            if !author_has_children(author) {
                continue;
            }
            ids.push(id);
            authors.push(author);
        }

        client
            .execute(
                "insert into comments (id, author) select * from unnest($1::bigint[], $2::bigint[])",
                &[&ids, &authors],
            )
            .await
            .unwrap_or_else(|e| panic!("bulk-load comments rows {lo}..={hi} failed: {e}"));

        lo = hi + 1;
    }

    start.elapsed()
}
