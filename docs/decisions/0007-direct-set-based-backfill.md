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
- **Aggregates** chunk by *group-key* range (ordered distinct group tuples,
  every N-th tuple a boundary) and build each chunk with `INSERT … SELECT …
  GROUP BY … ON CONFLICT DO UPDATE`. Because a group is a single point in
  group-key space, it lands wholly in exactly one chunk.

Each chunk is one bounded transaction. The build is synchronous and complete on
return.

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
ring can't store one either. The direct build excludes such groups (they fall out
of every row-value range comparison naturally, since a comparison against a NULL
bound is itself NULL) rather than attempting to insert them.

## Wiring

`create_definition` still enumerates into the ring and is unchanged (it has many
callers and the publication-ADD backfill path depends on it). Callers that build
the target directly instead pair `create_definition_without_backfill` with
`backfill_definition`, so the from-scratch build does not also flood the ring.
1-1 relationship-enriched definitions are not yet supported by the direct path
and continue to use the ring.

## Consequences

- The M0 benchmark's aggregate phase drops from ~55s to ~1.3s (100k groups) and
  ~0.1s (100 groups) on the dev box; the two cardinalities now diverge sharply,
  since the direct build's cost tracks group count. Regression ceilings tightened
  to 10s / 5s accordingly.
- The ring is no longer on the critical path for a from-scratch build, only for
  live deltas after it.
- Relationship-enriched 1-1 backfills remain on the ring until the direct path
  learns to render their join.
