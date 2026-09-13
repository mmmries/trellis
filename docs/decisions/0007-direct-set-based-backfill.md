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

**Amendment (2026-09-12):** "The build is synchronous and complete on return"
(below) doesn't scale to the sizes the public API design
([docs/public-api-design.md](../public-api-design.md)) needs to support — a
1B-row table's direct build still takes real wall-clock time, and a
synchronous in-call loop means an interrupted process (the caller's, or the
one running the loop) loses everything and starts over. This amendment keeps
every algorithmic decision below (range/group-key chunking, overwrite
semantics, single-pass aggregation into a staging table) — those are exactly
what makes the rest of this workable — but changes *who runs the chunks and
when*: chunk execution moves from an in-call loop to a durable, claimable work
queue that running drain (application) threads pick up, the same way they
already claim sealed ring segments. See "Backgrounding and resumability"
below.

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

`create_definition` (bare ring enumeration) is unchanged and still exists, but it
is no longer the primary entry point a real caller should reach for. That role
belongs to `engine::defs::install_definition`: it creates the target table, tries
`backfill_definition` (the direct path), persists via
`create_definition_without_backfill` on success, and falls back to
`create_definition` (ring enumeration) on `BackfillError::Unsupported`. This
supersedes an earlier version of this ADR's wiring account, which described the
direct path as reachable only through the generative harness's `ManualBackend`
(`generative/src/backend/manual.rs`) — at that point true, but not what a real,
non-test/non-benchmark caller of the definition-creation API would hit, since
`ManualBackend` is a correctness/oracle fuzz harness, not a production consumer.
`install_definition` closes that gap: it is the shared implementation both
`ManualBackend` and any real caller of `create_definition` should use, and
`ManualBackend` has been rewired onto it rather than hand-rolling the same
create-table → backfill → fallback sequence itself.

1-1 relationship-enriched definitions were entirely unsupported by the direct
path at first (any `uses_relationships(def)` definition returned
`BackfillError::Unsupported` and fell back to the ring unconditionally). That
was too broad: it caught the shape that actually motivated this issue — a
`KeySpace::OneToOne` transform whose fields `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over a
to-many relationship (e.g. an `authors` table aggregating over related `posts`/
`comments`) — leaving it on the slow ring path regardless of how fast the plain
aggregate/1-1 cases became. `backfill_relationship_one_to_one` now handles that
specific shape directly: one `GROUP BY`-aggregated temp staging table per
referenced to-many relationship, `LEFT JOIN`ed back to the source on its primary
key and chunked by source PK range (reusing the same range-walk as the plain 1-1
path), matching the ring/oracle's no-match semantics (`COUNT` → 0, `SUM`/`MIN`/
`MAX`/`AVG` → NULL on an empty child set). The `Unsupported` boundary is now
narrower and shape-specific: a bare to-one lookup (`category.name`, no aggregate
wrapper), an aggregate over a to-one relationship, or a relationship reference
nested inside a larger expression still falls back to the ring — only the
to-many-aggregate shape is direct-built.

## Consequences

- The M0 benchmark's aggregate phase drops from ~55s to ~0.75s (100k groups) and
  ~0.04s (100 groups) on the dev box; the two cardinalities now diverge sharply,
  since the direct build's cost tracks group count. Regression ceilings tightened
  to 10s / 5s accordingly.
- The ring is no longer on the critical path for a from-scratch build, only for
  live deltas after it.
- Relationship-enriched `OneToOne` **to-many aggregates** are also off the ring
  now: the committed `relationship-aggregate` benchmark scenario (100k authors,
  1M posts, 4.5M comments, `SUM`/`COUNT` over both) measures ~0.6-0.7s through
  `install_definition` on the direct path. The ~1 minute figure it improves on is
  the ring-path baseline from the original poc report that motivated this work,
  not a number the committed benchmark suite reproduces on this side of the
  comparison (the benchmark doesn't carry an unpatched ring-path variant to
  re-measure against) — so "~90x" is provenance from that report, not a
  same-benchmark before/after. Bare to-one lookups and any other relationship
  shape not listed above remain on the ring until the direct path learns to
  render them.

## Backgrounding and resumability (amendment)

`install_definition` today creates the target table, then runs
`backfill_definition` (every chunk, back to back, in one call) before ever
persisting the definition to the catalog — the caller's connection blocks for
however long the whole build takes. That's the piece this amendment changes.

**Chunks become a durable, claimable work queue**, not an in-call loop. Once
the target table exists and the coverage fence is captured (both already fast,
metadata-only operations — this ordering is unchanged from today, see issue
#79 bug B's fence-before-build requirement), `install_definition` enumerates
the same chunk boundaries `backfill_definition` computes today (PK ranges for
1-1, group-key ranges for aggregates) but persists them as pending work items
instead of executing them, then returns immediately — the definition is
recorded and visible (`definitions()` lists it, status `waiting_to_backfill`)
well before a single row of the target is built.

**Drain (application) threads execute the queue.** Per-fleet clarification:
`staging_worker` is only about keeping up with the logical replication slot
(intake + ring maintenance); it is `application_threads` — the drain
workers — that own finishing transform work, backfill included. Concretely,
this reuses the exact claim/heartbeat/reclaim-stale machinery
`engine/src/client.rs`'s app-worker loop already runs for sealed ring segments
(`register_drainer`, `next_claimable_segments`, `HeartbeatDaemon`,
`staging::reclaim_stale`): a backfill chunk becomes a second kind of claimable
unit alongside a sealed segment, claimed by whichever drain thread gets to it
next — including drain threads in entirely different processes across the
fleet. This is also a throughput win, not just a resumability one: a huge
table's chunks can be worked in parallel by every drain thread currently
running, not just whichever single connection happened to call `define()`.

**Crash safety** falls out of reusing that machinery rather than needing new
machinery: a claimed-but-not-yet-committed chunk is protected by the same
heartbeat + `reclaim_stale` TTL every ring-segment claim already relies on, so
a drain thread dying mid-chunk doesn't strand that range — another drain
thread reclaims and redoes it once the claim goes stale. Because each chunk's
write is `overwrite`, not additive (already decided above, for the same
reason), redoing a half-finished or fully-finished chunk is safe either way.

**CDC deltas arriving during the build are parked, not released per-chunk.**
While a definition sits in `waiting_to_backfill`/`backfilling`, any CDC delta
for its source table(s) parks in the ring exactly the way `pending_backfill`
already parks deltas for the ring-fallback path today — deliberately *not*
per-chunk released as each range finishes (that would require re-deriving the
fence/ordering guarantee per chunk instead of once per definition, real added
complexity with no immediate need). Once every chunk work item for a
definition is committed, the definition flips to `live` in one step and parked
deltas discharge in one shot, the same `run_pending_backfills`-shaped event
that already exists — just now the trigger for it is "every chunk claimed and
done" instead of "the ring path's one-shot enumeration finished." The
tradeoff this accepts: a very long build means a correspondingly long queue of
parked deltas to fold through on discharge; acceptable for now, revisit if it
becomes the actual bottleneck.

**Open problem — the aggregate path's staging table.** The single-pass
`GROUP BY` scan aggregates into a **connection-scoped `TEMP TABLE`** today,
cleaned up on connection teardown. That doesn't survive being read by many
different drain threads' own separate connections over however long the
backfill takes. This needs to become a durable, definition-scoped staging
table with its own explicit lifecycle (created once, dropped once every write
chunk referencing it has committed or the definition is dropped) rather than
riding on session teardown for cleanup. Leaning toward making the aggregation
pass itself the definition's first work item — singly claimed, must complete
before any write-chunk items are enumerated/claimable — rather than solving
general multi-writer access to an in-progress aggregation. Not settled.

Undecided:

* Exact shape of the chunk work-item table (columns/claim semantics) — almost
  certainly parallel to the existing `drainers`/segment-claim tables rather
  than novel, but not drafted.
* How the aggregate path's durable staging table gets cleaned up if a
  definition is dropped/redefined mid-backfill (orphaned staging table).
* Whether a stalled backfill (every chunk claimed, none completing — e.g. a
  chunk that deterministically errors) needs its own fuse, distinct from the
  per-column quarantine fuse in
  [ADR-0003](0003-quarantine-storage-and-api.md)'s amendment, since a failure
  here happens before a target row ever exists to attribute a quarantine
  entry to.
