//! Issue #558 experiment 5 (prototype, behind `TRELLIS_EXP558_BUILD=ledger`
//! with `TRELLIS_EXP558_LEDGER=contrib`): a plain aggregate's build as
//! Re-derive over chunks of the source's key space, with the definition
//! applying CDC from the first chunk. The design note's "Backfill" paragraph:
//! *A build is Re-derive over chunks of the key space, each chunk one
//! transaction under I1 and I5. The definition applies from the first chunk.*
//!
//! Scope: an aggregate definition with one source and no relationship
//! anywhere ([`apply_aggregate::ledger_build_eligible`]). Anything else keeps
//! today's build and today's Apply.
//!
//! # The build
//!
//! The backfill discharge plans `(lo, hi]` primary-key ranges of the source
//! ([`backfill::plan_pk_range_chunks`], [`chunk_rows`] rows each) and
//! enqueues them as ordinary range chunks, so every drain worker claims them
//! concurrently. One chunk is one fenced transaction ([`execute_chunk`]):
//!
//! 1. hold the claim ([`ClaimFence::hold`]);
//! 2. read the keys in `(lo, hi]` (`ddl::pk_key_sql_expr` text, the identity
//!    intake gives every CDC change for the same row);
//! 3. create a placeholder ledger entry for every key that has none, then
//!    lock the keys' entries in key order
//!    ([`apply_aggregate::lock_ledger_keys_with_placeholders`]);
//! 4. in one statement, read `pg_current_snapshot()` and the keys' current
//!    rows; per key, the group delta is the live contribution minus the
//!    entry's, and the entry is rewritten with the live group, contribution
//!    and the snapshot as its basis (`apply_aggregate::rederive_via_ledger`);
//! 5. lock the touched group rows in ascending order and apply the deltas
//!    additively, creating groups that don't exist yet
//!    ([`apply_aggregate::apply_aggregate_target`], the same statements
//!    Apply uses), through the target-mutation seam.
//!
//! A key inserted between steps 2 and 4 isn't in the chunk's key set: its
//! entry isn't locked, so the chunk ignores its row and leaves it to its
//! insert's change. The chunk is idempotent: a re-run finds every entry equal
//! to the live row and adds nothing. When the last chunk finishes the
//! definition goes straight to `live`: there is no go-live catch-up and no
//! orphan sweep, because no change was ever dropped.
//!
//! # Apply while building
//!
//! `catalog::dependents_of` includes an eligible `backfilling` definition, so
//! its changes apply from the moment its chunks are enqueued. Every Apply of
//! a plain aggregate target (building or live) follows the same rules while
//! the flag is on:
//!
//! - **No entry means nothing counted.** A key with no ledger entry (or a
//!   placeholder, or a tombstone) has contributed nothing to any group: its
//!   chunk hasn't run, or it didn't exist when it ran. So a change for it is
//!   applied as an insert of its NEW image, whatever its OLD image says: the
//!   OLD side's Phase 2 delta is undone in reconcile, because nothing was ever
//!   added for it. A delete of such a key subtracts nothing and records a
//!   tombstone; the chunk that later reads the key finds no row and a
//!   tombstone, and does nothing.
//! - **Absent entries are locked too.** Apply inserts the same placeholders
//!   before its ordered ledger lock, so a chunk and an Apply that both find
//!   no entry serialize on the key instead of both adding the row.
//! - **An entry's old side is the ledger's.** A change for a key with an
//!   entry subtracts the entry's contribution from the entry's group, never
//!   the image's (unchanged from experiment 4).
//! - **The skip rule replaces the horizons.** A change whose transaction the
//!   entry's basis already shows, or whose position is at or below the
//!   entry's applied position, is skipped. `__trellis_recompute_lsn` and the
//!   extinct horizon are never read, stamped or raised for these targets.
//! - **Existence comes from the ledger.** A group exists while some entry
//!   counts a member in it, read after the group pre-lock. The source can't
//!   answer that during the build, because it holds rows no chunk has counted.
//! - **An image-less change re-derives its key** through the same
//!   `rederive_via_ledger` a chunk uses, under the same lock, instead of
//!   forcing a full recompute of its groups.
//!
//! Why a chunk's snapshot and an Apply agree: both hold the entry's lock
//! while they read and write it. A chunk takes its snapshot after acquiring
//! the lock, so it shows every change an earlier Apply folded into the entry
//! (that change committed before the Apply ran). A change applied after the
//! chunk is either in the snapshot (skipped by the basis) or not (applied
//! against the entry the chunk wrote, which is the snapshot's state).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::defs::ast::{GroupByKey, KeySpace, ValueType, group_by_contains};
use crate::defs::backfill::{self, BackfillError};
use crate::defs::chunk_queue::ClaimFence;
use crate::defs::ddl;
use crate::defs::model::Definition;
use crate::pool::Pool;

use super::apply::ApplyError;
use super::apply_aggregate::{self, AggregateTargetPlan, RederiveCtx};
use super::target_mutations::TargetMutations;

/// Experiment 5 counters, process-wide, read by the `build-under-load`
/// benchmark through `trellis::dev::staging::exp5_counters`.
///
/// Changes checked against a ledger basis whose in-progress list (`xip`) held
/// the change's transaction: decided "not visible" and applied, never
/// re-derived (experiment 1b's decidability result, counted at scale).
pub(crate) static IN_XIP_CHANGES: AtomicU64 = AtomicU64::new(0);
/// Changes skipped because the entry's basis or applied position already
/// showed them (a chunk read the row after the change committed).
pub(crate) static SKIPPED_CHANGES: AtomicU64 = AtomicU64::new(0);
/// Keys re-derived for an image-less (or recompute-folded) change.
pub(crate) static IMAGELESS_REDERIVES: AtomicU64 = AtomicU64::new(0);
/// Source keys the build chunks read.
pub(crate) static CHUNK_KEYS: AtomicU64 = AtomicU64::new(0);

/// The experiment 5 counters, see [`IN_XIP_CHANGES`] and its siblings.
#[derive(Debug, Clone, Copy, Default)]
pub struct Exp5Counters {
    pub in_xip_changes: u64,
    pub skipped_changes: u64,
    pub imageless_rederives: u64,
    pub chunk_keys: u64,
}

pub fn exp5_counters() -> Exp5Counters {
    Exp5Counters {
        in_xip_changes: IN_XIP_CHANGES.load(Ordering::Relaxed),
        skipped_changes: SKIPPED_CHANGES.load(Ordering::Relaxed),
        imageless_rederives: IMAGELESS_REDERIVES.load(Ordering::Relaxed),
        chunk_keys: CHUNK_KEYS.load(Ordering::Relaxed),
    }
}

/// Source rows per build chunk, from `TRELLIS_EXP558_BUILD_CHUNK_ROWS`
/// (default 10,000: small enough that a few hundred thousand rows spread
/// over every drain worker).
pub fn chunk_rows() -> i64 {
    static ROWS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *ROWS.get_or_init(|| {
        std::env::var("TRELLIS_EXP558_BUILD_CHUNK_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(10_000)
    })
}

/// The plan Apply would build for `definition` (a plain aggregate), with no
/// changes in it: the same classification, rendered fields and group typing
/// as `apply::compute`'s aggregate branch.
fn empty_plan(definition: &Definition) -> Result<AggregateTargetPlan, ApplyError> {
    let def = &definition.def;
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("ledger_build::empty_plan called on a non-aggregate definition");
    };
    let substituted_exprs = backfill::substituted_field_exprs(def)?;
    let group_by_types: Vec<ValueType> = group_by
        .iter()
        .map(|key| match key {
            GroupByKey::Column(c) => definition
                .source_columns
                .get(c)
                .copied()
                .unwrap_or(ValueType::Numeric),
            GroupByKey::RelationshipPath { .. } => {
                unreachable!("a ledger-built aggregate reads no relationship")
            }
        })
        .collect();
    let field_plans = apply_aggregate::classify_fields(
        def,
        group_by,
        &definition.source_columns,
        &substituted_exprs,
        &HashMap::new(),
    )?;
    let field_exprs = substituted_exprs
        .into_iter()
        .filter(|(name, _)| !group_by_contains(group_by, name))
        .collect();
    let mut plan = AggregateTargetPlan::new(
        group_by,
        group_by_types,
        field_plans,
        definition.source_table.clone(),
        definition.target_table.clone(),
        field_exprs,
        Vec::new(),
    );
    plan.rederive_ctx = Some(RederiveCtx::for_plain(def, &definition.source_columns));
    Ok(plan)
}

/// Runs one build chunk, `(lo, hi]` of `definition`'s source, as one
/// transaction (see the module doc). With a `fence`, the transaction holds the
/// claim first and writes nothing ([`BackfillError::Superseded`]) when it no
/// longer holds.
pub(crate) async fn execute_chunk(
    pool: &Pool,
    definition: &Definition,
    lo: Option<&str>,
    hi: &str,
    fence: Option<ClaimFence<'_>>,
) -> Result<(), BackfillError> {
    let plan = empty_plan(definition)?;
    let mut client = pool.get().await?;
    {
        let txn = client.transaction().await?;
        let ledger = apply_aggregate::ledger_ident(&plan.target);
        apply_aggregate::ensure_ledger(&txn, &plan.target, &ledger).await?;
        txn.commit().await?;
    }
    let txn = client.transaction().await?;
    if let Some(fence) = fence
        && !fence.hold(&*txn).await?
    {
        return Err(BackfillError::Superseded);
    }
    run_chunk_in_txn(&txn, plan, &definition.source_table, lo, hi).await?;
    txn.commit().await?;
    Ok(())
}

/// [`execute_chunk`] with no claim to hold: runs `(lo, hi]` of the
/// definition's source as one build chunk, however many times it is called
/// (tests drive chunks, re-runs included, in a chosen order with this).
pub async fn execute_range(
    pool: &Pool,
    definition: &Definition,
    lo: Option<&str>,
    hi: &str,
) -> Result<(), BackfillError> {
    execute_chunk(pool, definition, lo, hi, None).await
}

/// [`execute_chunk`]'s body, inside the caller's transaction.
async fn run_chunk_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    mut plan: AggregateTargetPlan,
    source_table: &str,
    lo: Option<&str>,
    hi: &str,
) -> Result<(), BackfillError> {
    let pk = ddl::source_primary_key_in_txn(txn, source_table).await?;
    plan.rederive_keys = backfill::keys_in_pk_range(txn, source_table, &pk, lo, hi).await?;
    let keys = plan.rederive_keys.len();
    CHUNK_KEYS.fetch_add(keys as u64, Ordering::Relaxed);
    let target = plan.target.clone();
    let mut plans = HashMap::from([(target.clone(), plan)]);
    apply_aggregate::ledger_stage(txn, &mut plans).await?;
    let plan = plans.remove(&target).expect("the chunk's own plan");
    let mut mutations = TargetMutations::new();
    let (written, deleted) =
        apply_aggregate::apply_aggregate_target(txn, &target, &plan, &mut mutations).await?;
    mutations.flush(txn).await?;
    tracing::debug!(
        target = %target,
        keys,
        groups = plan.groups.len(),
        written,
        deleted,
        "exp5: ledger build chunk"
    );
    Ok(())
}
