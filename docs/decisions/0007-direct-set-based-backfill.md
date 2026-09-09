---
status: accepted
date: 2026-09-09
deciders: Michael Ries
consulted:
informed:
---

# Direct, Set-Based Backfill Bypasses the Staging Ring

When a definition is created (or explicitly backfilled), Trellis must build its
target table from the entire, already-populated source. Historically this went
through the same machinery as live CDC: enumerate every source row, append it to
the staging ring, then claim/fold/compute/apply it back out. That path is
correct but pathologically slow for a from-scratch build — issue #63 measured a
1M-row / 100k-group aggregate at ~55s on the dev box (~1m50s on the poc
cluster), dominated by ring bookkeeping over data that is entirely present up
front and has no concurrent deltas to reconcile.

## Decision

The initial build of a target is computed **directly, set-based, source→target**,
bypassing the ring entirely. The ring is left to do only what it is uniquely good
at: reconciling live CDC deltas after the build fence.

Implemented in [`engine::defs::backfill`]:

- **1-1 (calc) transforms** walk the source primary key in half-open `(lo, hi]`
  ranges, each range built with `INSERT INTO target SELECT <exprs> FROM source
  WHERE <pk> > lo AND <pk> <= hi ON CONFLICT (<pk>) DO UPDATE`. Range bounds are
  discovered by `max()`-over-`LIMIT`, so every source row falls in exactly one
  range regardless of gaps in the key values.
- **Aggregates** aggregate the whole source in a **single** full-table scan
  into a connection-scoped temp *staging* table (`CREATE TEMP TABLE … AS SELECT
  <group_cols>, <aggs> FROM source WHERE <keys not null> GROUP BY <group_cols>`),
  then chunk the **writes** from that staging table into the target by group-key
  range — each range an `INSERT INTO target SELECT … FROM staging WHERE
  (<group_cols>) > lo AND (<group_cols>) <= hi ON CONFLICT DO UPDATE`. Because a
  group is a single point in group-key space, it lands wholly in exactly one
  chunk.

Each chunk write is one bounded transaction. The build is synchronous and
complete on return.

## Single-pass aggregation, then chunked writes — not chunked aggregation

The aggregate build must **not** chunk by group-key range directly over the
*source*. An earlier revision did — each chunk ran `INSERT … SELECT … FROM
source WHERE (<group_cols>) > lo AND (<group_cols>) <= hi GROUP BY …`. The source
has no index on the GROUP BY columns (only its PK; an index on the GROUP BY
columns was tried in an earlier milestone and abandoned as ineffective, see ADR
0005), so every chunk did a full **sequential scan of the entire source**
filtered to one key range. With `C` chunks that is `O(C × source_size)` total
scan work — the exact "re-scan the whole table per chunk" pathology milestones 1
and 2 fixed elsewhere in #63, reintroduced by the new bulk-build mechanism. It
was confirmed empirically: 100k groups → 10 chunks → ~1.25s (~125ms/chunk, each
roughly one full-table scan) against a ~65ms single-pass `GROUP BY` floor, and
projected to ~12.5s at 1M groups (100 chunks) — past the 10s ceiling and ~200x
the compute floor.

The accepted design scans the source **exactly once** (the `CREATE TEMP TABLE …
AS SELECT … GROUP BY`), and every later read — boundary discovery and every
chunk write — hits the staging table, which is *group-count*-sized, not
*source*-sized. A primary key on staging's group columns (valid: the group tuple
is unique in an aggregated result) makes each chunk's range-write an index range
scan rather than a staging seq scan, so even at pathological cardinality (nearly
one group per source row) the writes never degrade into repeated full scans.
Total scan work is `O(source_size)` for the one aggregation pass plus
`O(group_count)` for the writes — never `O(C × source_size)`. Empirically this
took the 100k-group phase from ~1.25s to ~0.75s. The staging table is dropped
before creation (in case a crashed prior backfill on a reused pooled connection
left one behind) and after the writes complete, so it never leaks back into the
pool.

## Overwrite-by-group-key, not additive-by-PK

Issue #63's sketch suggested an *additive* merge (`col = target.col +
excluded.col`) walking the source PK for aggregates. We instead **overwrite**
(`col = excluded.col`) and chunk by **group key**. The reasons:

- **Non-additive fields.** `MIN`/`MAX` and composed/`RecomputeOnly` expressions
  cannot be merged additively across chunks. Overwriting a whole group in one
  pass computes each field with its own aggregate (`sum`, `count`, `min`, `max`,
  and AVG's `sum/count` partials), exactly as the ring's bulk-recompute path
  does.
- **Idempotency / crash recovery.** Overwrite is order-independent and
  idempotent: re-running the backfill (the recovery story after a crash partway
  through) recomputes each group to the same value rather than doubling an
  additive sum. This gives the build the same concurrency-safety profile as the
  ring's image-less recompute path.
- **Bounded work per statement.** Group-key chunking bounds the hash-aggregate
  working set per transaction, and — unlike #58's VALUES-list writes — the
  server-side `INSERT … SELECT` carries only the range bounds as bind
  parameters, so there is no bind-parameter ceiling to respect.

## NULL group keys are not built

An aggregate target's `GROUP BY` columns are its primary key, so Postgres forbids
a NULL there: a group with a NULL key has no representable target row, and the
ring can't store one either. The direct build excludes such groups when it builds
the staging table (`WHERE <keys> IS NOT NULL`), so they never reach the target,
rather than attempting to insert them.

## Wiring

`create_definition` still enumerates into the ring and is unchanged (it has many
callers and the publication-ADD backfill path depends on it). Callers that build
the target directly instead pair `create_definition_without_backfill` with
`backfill_definition`, so the from-scratch build does not also flood the ring.
1-1 relationship-enriched definitions are not yet supported by the direct path
and continue to use the ring.

The one real (non-benchmark, non-test) consumer of the definition-creation API,
the generative harness's `ManualBackend` (`generative/src/backend/manual.rs`),
is wired onto the direct path: it creates the target table, runs
`backfill_definition`, and then persists the definition with
`create_definition_without_backfill`; a `BackfillError::Unsupported` result
falls back to the original `create_definition` (ring enumeration), the same path
that definition took before. Today's generator only emits `KeySpace::OneToOne`
definitions with no relationship paths, so that fallback is dead-but-safe
insurance — the fast path handles every definition the harness produces.

## Consequences

- The M0 benchmark's aggregate phase drops from ~55s to ~0.75s (100k groups) and
  ~0.04s (100 groups) on the dev box; the two cardinalities now diverge sharply,
  since the direct build's cost tracks group count. Regression ceilings tightened
  to 10s / 5s accordingly.
- The ring is no longer on the critical path for a from-scratch build, only for
  live deltas after it.
- Relationship-enriched 1-1 backfills remain on the ring until the direct path
  learns to render their join.
