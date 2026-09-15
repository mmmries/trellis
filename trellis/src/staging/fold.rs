//! The claim-time fold (issue #10, stage 04): collapsing a sealed batch's
//! fenced window into one record per `(src_table, key)`, entirely in SQL —
//! ordered aggregates run in Postgres, not Rust-side aggregation. See
//! docs/staging-and-claiming/04-claiming-and-the-fold.md, "The claim-time
//! fold" and "The two kinds of missing image".
//!
//! Scope: this module produces folded records. It does not claim buckets
//! (that's `seg_claims`/`ON CONFLICT`, `super::claim`), does not compute
//! deltas or apply them (#11), and does not evaluate `f()` (a later,
//! Rust-side concern). `BucketFilter` exists so the fold is
//! bucket-parameterizable; `super::claim::owned_bucket_filter` is what
//! turns a real claim's buckets into one, but nothing in *this* module
//! populates buckets itself — [`BucketFilter::all`] is the whole unsplit
//! batch.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

use super::error::StagingError;
use super::seal::fenced_window;

/// Sorts the fold's ordered aggregates in memory rather than spilling to
/// disk on a large batch (docs/.../04-claiming-and-the-fold.md, "Practical
/// notes on the fold"). `LOCAL` scopes it to the caller's transaction —
/// [`fold`] takes a `Transaction` rather than any `GenericClient` precisely
/// so this can't evaporate before the query below it runs.
const FOLD_WORK_MEM: &str = "SET LOCAL work_mem = '64MB'";

/// `route % bucket_count = ANY(buckets)` — the **one** bucket definition,
/// applied inside the fenced window so a key never folds across a boundary
/// its worker doesn't own (docs/.../04-claiming-and-the-fold.md, "Partitioning
/// a batch across workers"). This struct carries no claim state; it is pure
/// SQL parameterization. [`BucketFilter::all`] selects the whole batch
/// (`bucket_count = 1`, `buckets = [0]`), which is the only constructor this
/// stage needs — the claim machinery that would populate real buckets is
/// #14/#15, not here.
#[derive(Debug, Clone)]
pub struct BucketFilter {
    bucket_count: i64,
    buckets: Vec<i64>,
}

impl BucketFilter {
    /// The whole (single-bucket) batch: `route % 1 = 0` is trivially true
    /// for every row.
    pub fn all() -> Self {
        Self {
            bucket_count: 1,
            buckets: vec![0],
        }
    }

    /// A named subset of buckets out of `bucket_count` — what
    /// `super::claim::owned_bucket_filter` hands the fold, built from a
    /// real claim's `seg_claims` rows. Nothing in this module populates
    /// buckets; this constructor only lets a caller (or a test) express
    /// "restrict to these buckets" against the one SQL bucket definition
    /// below, rather than reimplementing `route % bucket_count` in Rust.
    pub fn buckets(bucket_count: i64, buckets: Vec<i64>) -> Self {
        Self {
            bucket_count,
            buckets,
        }
    }

    /// Whether this filter names no buckets at all — the signal
    /// [`super::apply::drain_once`] uses to short-circuit a claim attempt
    /// that won (and owns) nothing, before folding or computing anything.
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

/// One key's folded record: the fenced window's `(src_table, key)` group
/// collapsed to the seven fold outputs the doc's four-rule table (plus
/// `lsn`/`hop_gen`/`first_seen`) specifies. Images cross the wire as text —
/// see `append.rs`'s doc comment on why this crate binds jsonb via
/// `::text` rather than a `serde_json::Value` `FromSql`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedChange {
    pub src_table: String,
    pub key: String,
    /// LAST image-bearing row's post-image, by `(lsn, change_id)` — highest.
    pub new_image: Option<String>,
    /// FIRST image-bearing row's pre-image, by `(lsn, change_id)` — lowest.
    /// A key born inside the batch (insert-then-update) folds this to
    /// `None`: the insert has no pre-image, and that's a fact about the
    /// change, not a missing value to paper over with `COALESCE`.
    pub old_image: Option<String>,
    /// Whether the record is a source change, carried as the latest
    /// non-null `src_changed` timestamp among the group's rows — `Some`
    /// iff *any* row is a source change (an `OR`, expressed via `max`,
    /// which is `NULL` exactly when every input is `NULL`), `None` iff
    /// none are. Pinned representation: the struct has one
    /// `Option<SystemTime>` field, not a separate bool + timestamp, per
    /// the issue's field list.
    pub src_changed: Option<SystemTime>,
    /// LEAST non-null `origin_lsn` over the group.
    pub origin_lsn: Option<PgLsn>,
    /// GREATEST `lsn` over *every* row in the group, image-less rows
    /// included, so the watermark still covers them.
    pub lsn: Option<PgLsn>,
    /// Reset to 0 if any row in the group is a source change. Otherwise —
    /// underspecified by the doc for the all-non-source case — pinned to
    /// `MAX(hop_gen)`: doc 05 describes `hop_gen` as a schema-derived bound
    /// on propagation depth ("one past its deepest trigger"), checked
    /// against the graph's cross-table depth to catch runaway propagation.
    /// Folding to the *shallowest* row instead (MIN, or an arbitrary carry)
    /// would under-report depth and could mask a wave that should have
    /// tripped the bound — MAX is the conservative choice that cannot
    /// itself cause a missed-detection of infinite propagation. FLAG:
    /// confirm this against #11/the hop-bound check once it exists.
    pub hop_gen: i32,
    /// `min(appended_at)` per key — the end-to-end latency origin, per-row
    /// rather than the batch's creation timestamp (see doc: an idle active
    /// batch's age is unbounded).
    pub first_seen: SystemTime,
    /// A representative `group_key`: the first non-null value by append
    /// order (`lsn`, then `change_id` to break ties, matching the same
    /// order the arg-extremes use). For today's `KeySpace::OneToOne` slice
    /// every row of a key should carry the same `group_key` anyway; this is
    /// the "keep it simple" choice the doc allows rather than a real merge.
    pub group_key: Option<String>,
    /// Whether any row in this group is a truncate sentinel (`bool_or(op =
    /// 'truncate')`) — issue #60. In practice only the sentinel-key group
    /// (see `append::TRUNCATE_SENTINEL_KEY`) is ever `true`: no real key's
    /// rows ever carry `op = 'truncate'`. `compute` (`staging::apply`) uses
    /// this to find, per drain, which `src_table`s were truncated, without
    /// filtering `op` anywhere upstream of this one flag.
    pub is_truncate: bool,
}

/// The fenced window's full column projection the fold needs, with jsonb
/// images cast to text at the source (this crate has no `serde_json`
/// dependency — see `append.rs`). `route` rides along so the bucket filter
/// can be applied to the union of both slots' rows, not to one slot alone.
/// `op` rides along too — issue #60's truncate-void filter and `is_truncate`
/// both need it, even though the fold otherwise deliberately never filters
/// on `op` (see the discriminator comment below).
const FOLD_COLUMNS: &str = "src_table, key, old_image::text as old_image, \
     new_image::text as new_image, lsn, origin_lsn, src_changed, hop_gen, \
     group_key, appended_at, change_id, route, op";

/// Runs the claim-time fold over `seg_seq`'s fenced window, restricted to
/// `bucket`. One [`FoldedChange`] per `(src_table, key)` present in that
/// window. See the module doc and docs/.../04-claiming-and-the-fold.md for
/// the rules this SQL encodes.
///
/// Takes a `&Transaction` rather than any `GenericClient`: the fold is meant
/// to run inside the claim's transaction, atomically with the apply (#11),
/// and the type enforces that rather than relying on a caller to remember
/// it — `SET LOCAL work_mem` would otherwise silently evaporate on a bare
/// autocommit `Client`.
pub async fn fold(
    txn: &Transaction<'_>,
    seg_seq: i64,
    bucket: BucketFilter,
) -> Result<Vec<FoldedChange>, StagingError> {
    txn.batch_execute(FOLD_WORK_MEM).await?;

    let (window_sql, fence_params) = fenced_window(txn, seg_seq, FOLD_COLUMNS).await?;
    let bucket_count_idx = fence_params.len() + 1;
    let buckets_idx = fence_params.len() + 2;

    // The discriminator (docs/.../04-claiming-and-the-fold.md, "The two
    // kinds of missing image"): "does this row carry any image at all", not
    // "is this image column null". Scoping both arg-extremes to
    // `image_bearing` rows is what lets a born-in-batch insert's honest
    // `old_image IS NULL` survive, while a bare recompute trigger (both
    // images NULL) never wins either extreme. `op` is deliberately not
    // filtered anywhere here as a row-level WHERE — the `truncate` sentinel
    // is image-less but load-bearing, and filtering `op` would fold it away.
    //
    // Both `new_image` (LAST, highest `(lsn, change_id)`) and `old_image`
    // (FIRST, lowest) use the `array_agg(... ORDER BY ...) FILTER (...))[1]`
    // idiom for a grouped arg-extreme: ORDER BY sorts the (value, sort-key)
    // pairs together, so `[1]` after filtering is the image from the actual
    // highest/lowest-ordered row, not an independently-sorted value.
    //
    // The one exception to "never filter on `op`" is the truncate-void
    // filter below (issue #60): a truncate is whole-keyspace, but the fold
    // is per-key, so a key's image-bearing row landing *at or below* a
    // truncate for its own `src_table` in this fenced window is stale — the
    // truncate erased whatever it recorded — and must not survive into the
    // group at all, not just lose an arg-extreme. `(t.lsn, t.change_id) >
    // (f.lsn, f.change_id)` is the "strictly above" test; a truncate and an
    // insert in the *same* source transaction share one commit `lsn`, so
    // `change_id` (intake's append order = execution order) is what orders
    // a same-transaction truncate-then-insert correctly.
    //
    // Load-bearing subtlety: a recompute row's `lsn` is NULL, and Postgres's
    // row-comparison against a NULL component evaluates to NULL, not TRUE —
    // so `f`'s own EXISTS test above is not satisfied by *any* truncate for a
    // recompute row, regardless of the truncate's position, and the
    // recompute survives unconditionally. That is correct: a recompute
    // re-reads *live* current source state (already reflecting the
    // truncate) when it's later evaluated, so its position relative to the
    // truncate is irrelevant — only image-bearing rows carry a stale
    // snapshot that must be voided. Referencing the un-bucket-filtered
    // `fenced` CTE (not `filtered`) matters too: a truncate sentinel's own
    // key (the sentinel) could in principle route to a different bucket
    // than the keys it voids, though in practice every truncate-bearing
    // batch seals with `bucket_count = 1` (see `seal::seal_phase1`), making
    // that moot today.
    let sql = format!(
        "with fenced as ({window_sql}), \
         filtered as ( \
             select * from fenced f \
             where route % ${bucket_count_idx}::bigint = any(${buckets_idx}::bigint[]) \
               and not exists ( \
                   select 1 from fenced t \
                   where t.op = 'truncate' and t.src_table = f.src_table \
                     and (t.lsn, t.change_id) > (f.lsn, f.change_id) \
               ) \
         ) \
         select \
             src_table, \
             key, \
             (array_agg(new_image order by lsn desc, change_id desc) \
                 filter (where old_image is not null or new_image is not null))[1] as new_image, \
             (array_agg(old_image order by lsn asc, change_id asc) \
                 filter (where old_image is not null or new_image is not null))[1] as old_image, \
             max(src_changed) as src_changed, \
             min(origin_lsn) as origin_lsn, \
             max(lsn) as lsn, \
             case when bool_or(src_changed is not null) then 0 else max(hop_gen) end as hop_gen, \
             min(appended_at) as first_seen, \
             (array_agg(group_key order by lsn asc, change_id asc) \
                 filter (where group_key is not null))[1] as group_key, \
             bool_or(op = 'truncate') as is_truncate \
         from filtered \
         group by src_table, key"
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = fence_params
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .collect();
    params.push(&bucket.bucket_count);
    params.push(&bucket.buckets);

    let rows = txn.query(&sql, &params).await?;
    Ok(rows
        .into_iter()
        .map(|row| FoldedChange {
            src_table: row.get(0),
            key: row.get(1),
            new_image: row.get(2),
            old_image: row.get(3),
            src_changed: row.get(4),
            origin_lsn: row.get(5),
            lsn: row.get(6),
            hop_gen: row.get(7),
            first_seen: row.get(8),
            group_key: row.get(9),
            is_truncate: row.get(10),
        })
        .collect())
}

/// Merges several segments' already-folded change lists — issue #63
/// Milestone 2's segment-coalescing seam. Each element of `per_segment` is
/// one segment's own [`fold`] output, already unique per `(src_table,
/// key)`; `per_segment` itself must be ordered **ascending by seg_seq**
/// (oldest segment first), since [`merge_pair`] assumes its `earlier`
/// argument really did seal before its `later` one.
///
/// A key touched in only one segment passes through unchanged. A key
/// touched in more than one segment is combined via [`merge_pair`], applied
/// left-to-right in seal order — exactly the same reduction [`fold`]'s own
/// `array_agg(... order by lsn, change_id)` arg-extremes compute for one
/// segment's raw rows, just run here over already-reduced per-segment
/// records instead of raw ones. Deduplicating by key here, before the
/// combined list ever reaches [`super::apply::compute`], matters beyond
/// bookkeeping: [`super::apply::apply_target`]'s upsert binds every write in
/// one `INSERT ... ON CONFLICT` statement, and Postgres rejects a statement
/// that would update the same conflict target row twice
/// (`ON CONFLICT DO UPDATE command cannot affect row a second time`) — so
/// coalescing segments without this merge would make any key touched by
/// more than one of them fail outright instead of silently misapplying.
pub fn merge_folded_changes(per_segment: Vec<Vec<FoldedChange>>) -> Vec<FoldedChange> {
    let mut merged: Vec<FoldedChange> = Vec::new();
    let mut index: HashMap<(String, String), usize> = HashMap::new();
    for segment in per_segment {
        for change in segment {
            let dedup_key = (change.src_table.clone(), change.key.clone());
            match index.get(&dedup_key) {
                Some(&i) => {
                    let earlier = std::mem::replace(&mut merged[i], change.clone());
                    merged[i] = merge_pair(earlier, change);
                }
                None => {
                    index.insert(dedup_key, merged.len());
                    merged.push(change);
                }
            }
        }
    }
    merged
}

/// Combines two [`FoldedChange`] records for the same `(src_table, key)`,
/// one from an earlier-sealed segment and one from a later one, into the
/// record [`fold`] would have produced had both segments' underlying rows
/// been folded together in one pass. Field-by-field, mirroring [`fold`]'s
/// own SQL rules (see [`FoldedChange`]'s doc comments):
///
/// - `new_image`/`old_image`: whichever side actually carries image
///   evidence (either image field set) wins its half — `later` for the
///   post-image (its window is strictly the more recent), `earlier` for the
///   pre-image. A side with neither image set contributed no image
///   information at all (a bare recompute trigger folded alone), so it
///   defers entirely to the other side rather than overwriting real
///   evidence with `None`.
/// - `src_changed`/`lsn`: `Option::max` — `None` sorts below every `Some`,
///   and among two `Some`s the greater watermark/timestamp wins, matching
///   "OR across the group" and "GREATEST over every row" respectively.
/// - `origin_lsn`: the lesser of the two, ignoring a missing side —
///   `Option::min` would wrongly let a `None` beat a real `Some`.
/// - `hop_gen`: 0 if the merged `src_changed` is `Some` (a source change
///   resets propagation depth), else the greater of the two hop generations.
/// - `first_seen`: the earlier of the two — first append into either
///   segment.
/// - `group_key`: `earlier`'s, if it has one — "first non-null value by
///   append order".
/// - `is_truncate`: OR. In practice always `false` here: the batching layer
///   that builds `per_segment` never coalesces a truncate-bearing segment
///   with any other (a truncate is its own drain barrier — see
///   `apply::next_claimable_segments`), so no truncate row can reach this
///   function paired with anything. Kept as a real OR anyway rather than
///   assumed away, so a future caller that breaks that invariant fails
///   toward "still marked as a truncate" rather than toward silently
///   dropping one.
fn merge_pair(earlier: FoldedChange, later: FoldedChange) -> FoldedChange {
    let earlier_has_image = earlier.old_image.is_some() || earlier.new_image.is_some();
    let later_has_image = later.old_image.is_some() || later.new_image.is_some();
    let src_changed = earlier.src_changed.max(later.src_changed);
    let hop_gen = if src_changed.is_some() {
        0
    } else {
        earlier.hop_gen.max(later.hop_gen)
    };
    let origin_lsn = match (earlier.origin_lsn, later.origin_lsn) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };

    FoldedChange {
        src_table: earlier.src_table,
        key: earlier.key,
        new_image: if later_has_image {
            later.new_image
        } else {
            earlier.new_image
        },
        old_image: if earlier_has_image {
            earlier.old_image
        } else {
            later.old_image
        },
        src_changed,
        origin_lsn,
        lsn: earlier.lsn.max(later.lsn),
        hop_gen,
        first_seen: earlier.first_seen.min(later.first_seen),
        group_key: earlier.group_key.or(later.group_key),
        is_truncate: earlier.is_truncate || later.is_truncate,
    }
}

#[cfg(test)]
mod merge_tests {
    use std::time::Duration;

    use super::*;

    fn base(key: &str) -> FoldedChange {
        FoldedChange {
            src_table: "orders".to_string(),
            key: key.to_string(),
            new_image: None,
            old_image: None,
            src_changed: None,
            origin_lsn: None,
            lsn: None,
            hop_gen: 0,
            first_seen: SystemTime::UNIX_EPOCH,
            group_key: None,
            is_truncate: false,
        }
    }

    /// A key touched in only one of the coalesced segments passes straight
    /// through, untouched by the merge — the common case for a burst where
    /// most keys are touched once.
    #[test]
    fn untouched_keys_pass_through_unmerged() {
        let seg_a = vec![base("1")];
        let seg_b = vec![base("2")];
        let mut merged = merge_folded_changes(vec![seg_a, seg_b]);
        merged.sort_by(|a, b| a.key.cmp(&b.key));
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].key, "1");
        assert_eq!(merged[1].key, "2");
    }

    /// A key updated in two coalesced segments folds to exactly one
    /// [`FoldedChange`] — the "never affect the same ON CONFLICT row twice"
    /// invariant [`merge_folded_changes`]'s doc comment calls out — carrying
    /// the earliest pre-image and the latest post-image across both
    /// windows, as if the whole span had been folded in one pass.
    #[test]
    fn same_key_across_segments_merges_to_one_record_with_earliest_old_and_latest_new_image() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());
        first.lsn = Some(PgLsn::from(10));
        first.first_seen = SystemTime::UNIX_EPOCH + Duration::from_secs(1);

        let mut second = base("1");
        second.old_image = Some(r#"{"v":2}"#.to_string());
        second.new_image = Some(r#"{"v":3}"#.to_string());
        second.lsn = Some(PgLsn::from(20));
        second.first_seen = SystemTime::UNIX_EPOCH + Duration::from_secs(2);

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1, "one row per key, never two");
        let merged = &merged[0];
        assert_eq!(merged.old_image, Some(r#"{"v":1}"#.to_string()));
        assert_eq!(merged.new_image, Some(r#"{"v":3}"#.to_string()));
        assert_eq!(merged.lsn, Some(PgLsn::from(20)));
        assert_eq!(
            merged.first_seen,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1)
        );
    }

    /// A later segment's bare recompute trigger (no image at all — the
    /// image-less shape a reverse-recompute or backfill enumeration stages)
    /// must not blot out an earlier segment's real image evidence for the
    /// same key.
    #[test]
    fn image_less_later_segment_defers_to_earlier_segments_image() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());

        let second = base("1"); // no image at all: a bare recompute trigger

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].new_image, Some(r#"{"v":2}"#.to_string()));
        assert_eq!(merged[0].old_image, Some(r#"{"v":1}"#.to_string()));
    }

    /// A genuine delete (`new_image: None`, `old_image: Some(..)`) in a
    /// later segment must survive the merge as a delete, not be treated as
    /// "no new information" just because `new_image` is `None`.
    #[test]
    fn later_segment_delete_overrides_earlier_segments_write() {
        let mut first = base("1");
        first.old_image = Some(r#"{"v":1}"#.to_string());
        first.new_image = Some(r#"{"v":2}"#.to_string());

        let mut second = base("1");
        second.old_image = Some(r#"{"v":2}"#.to_string());
        second.new_image = None; // deleted in the later segment

        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].new_image, None,
            "the later segment's delete must win, not be papered over by the earlier write"
        );
        assert_eq!(merged[0].old_image, Some(r#"{"v":1}"#.to_string()));
    }

    /// `hop_gen` resets to 0 once any contributing segment carries a real
    /// source change, and otherwise takes the max across segments —
    /// mirroring `fold`'s own per-segment rule applied across segments too.
    #[test]
    fn hop_gen_resets_on_a_source_change_else_takes_the_max() {
        let mut first = base("1");
        first.hop_gen = 3;
        let mut second = base("1");
        second.hop_gen = 5;
        let merged = merge_folded_changes(vec![vec![first.clone()], vec![second.clone()]]);
        assert_eq!(merged[0].hop_gen, 5, "no source change: max of the two");

        second.src_changed = Some(SystemTime::UNIX_EPOCH);
        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(
            merged[0].hop_gen, 0,
            "a source change in either segment resets hop_gen"
        );
    }

    /// `origin_lsn` takes the lesser of the two non-null sides — a plain
    /// `Option::min` would wrongly let a missing side beat a real one.
    #[test]
    fn origin_lsn_takes_the_lesser_non_null_side() {
        let mut first = base("1");
        first.origin_lsn = None;
        let mut second = base("1");
        second.origin_lsn = Some(PgLsn::from(7));
        let merged = merge_folded_changes(vec![vec![first], vec![second]]);
        assert_eq!(merged[0].origin_lsn, Some(PgLsn::from(7)));
    }

    /// More than two contributing segments still fold to exactly one
    /// record, taking the latest post-image across all of them — the
    /// realistic burst shape this milestone targets.
    #[test]
    fn three_segments_touching_the_same_key_merge_to_one_record() {
        let mut a = base("1");
        a.new_image = Some("\"a\"".to_string());
        a.lsn = Some(PgLsn::from(1));
        let mut b = base("1");
        b.new_image = Some("\"b\"".to_string());
        b.lsn = Some(PgLsn::from(2));
        let mut c = base("1");
        c.new_image = Some("\"c\"".to_string());
        c.lsn = Some(PgLsn::from(3));

        let merged = merge_folded_changes(vec![vec![a], vec![b], vec![c]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].new_image, Some("\"c\"".to_string()));
        assert_eq!(merged[0].lsn, Some(PgLsn::from(3)));
    }
}
