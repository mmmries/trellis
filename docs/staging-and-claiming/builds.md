# Builds: filling a target while it applies

← [Applying](05-apply-and-exactly-once-deltas.md) · up → [Staging and claiming](README.md)

How a definition's target comes to reflect the rows its source already held.
Why a build applies from its first moment, and the bars it is held to, are in
ADR-0002 ([a 1-1 build](../decisions/0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk),
[an aggregate's](../decisions/0002-async-data-flow.md#an-aggregates-build-is-one-absolute-write-per-structure)).
The code is `staging::build`.

## The start

Once the reconcile pass finds a definition ready
([01](01-capture-by-triggers.md#which-definitions-may-start-a-build)), one
transaction moves it `waiting_to_backfill -> backfilling`, enqueues its plan
job and records `build_seg`, the segment active at its commit. From that commit
Apply folds every change to the source into the definition, as for a `live`
one, so nothing needs a catch-up afterwards. Registration only registers.

Every change committed before the start is in a batch at or below
`build_seg`, since seal phase 1 can't move the active segment until the start
commits. A 1-1 page re-derives rather than applies the keys of such a batch,
whose images may be stale
([05](05-apply-and-exactly-once-deltas.md#absolute-writes-do-not-commute-the-1-1-ledger));
an aggregate's sides commute and need no such rule.

## A 1-1 build: Re-derive over chunks

1. **Plan.** A drain thread walks the source's primary key and enqueues
   chunks of `build_chunk_rows` rows (10,000 by default), in batches. The
   ranges come from `max()` over a `LIMIT`, so every row falls in exactly one,
   and the walk records the key's collations so a re-collation mid-build
   doesn't reorder the ranges.
2. **Chunk.** One short transaction per range
   (`staging::build::one_to_one`): it reads the range's keys, leaving out the
   ones quarantined for the definition and any with a `NULL` part; locks
   their entries in key order as a page does (an entry for each key with
   none, then the others `for update`); then one statement takes
   `pg_current_snapshot()` and the active segment, reads the source rows and
   rewrites the entries (basis := that snapshot, `__applied_lsn` left alone,
   a key with no row a tombstone) and the target rows. A change the snapshot
   saw is refused when it drains; one it didn't applies over the entry the
   chunk wrote. A key a page inserted after the chunk read its keys isn't
   locked, and its own change applies it.
3. **Live.** The transaction that finds the plan job and every chunk done,
   under the definition's row lock, moves the definition to `live` and
   deletes the build's queue rows.

A chunk that fails on its data splits until the key fails alone, and the key
is quarantined as a drain eviction would quarantine it. A chunk is heartbeated
and reclaimed like a sealed segment; a crashed worker's chunk is re-run, since
Re-derive is idempotent. A chunk whose entry lock times out is retried after a
short backoff without a charge (I7). A resume supersedes held chunks through
the claim fence, so no stale write lands after it. A pause freezes the build
where it is.

Drain threads take build work only after their segments, and a chunk only
while the ring's undrained backlog is under its bound and the next seal has a
slot (`chunk_allowed`), so a build never starves the drain or holds off a
seal.

**Rebuild and resume.** A paused or quarantined definition keeps its ledger.
Its resume, `request_backfill` and a capture re-install start a Re-derive build
over the existing entries with no truncate, plus a **sweep** that runs once the
chunks are done: it re-derives each live entry no chunk did (a key deleted
while the definition was frozen) and each target row with no live entry (one
added or re-keyed by hand). A resume whose source key columns, by name and in
key order, are no longer the ones recorded is refused, since the target is
keyed by them.

**Field builds.** An `ALTER TRANSFORM` that adds or changes fields, and a
column resume, run chunks that take the keys' entry locks and rewrite just
those fields of the existing target rows from one snapshot, leaving the
entries alone. A field that reads a column capture doesn't image yet waits
for it ([01](01-capture-by-triggers.md#which-definitions-may-start-a-build)).
A 1-1 `JOIN` has no field build, so its column resume rebuilds the target in
place ([ADR-0021](../decisions/0021-one-to-one-join-transforms.md#a-row-a-page-fails-to-write-is-held-in-the-page)).

**`NULL` keys.** A source that is an aggregate's target is keyed by its
nullable `GROUP BY` columns, and a key with a `NULL` part is in no chunk's
range (the range is a row comparison). The plan job stages an image-less
`Recompute` for each such key when its walk ends, which a page re-derives
under the entry lock. Those rows can still be in the ring when the build flips
`live`; their origin is unknown, so a token taken after the flip waits for
them, as for any seam row.

## A seam-fed source takes a fence

A source that is another definition's target is never captured: its writes
reach Apply through the target-mutation seam, whose writer reads the list of
applying definitions inside its own transaction. A writer that read it before
the build started can commit after a chunk's snapshot, with its rows staged
for no one. So the plan job first takes a fence, the id of a transaction begun
after the start commit (`backfill_chunks.fence_xid`), and enqueues no chunk
until the oldest running transaction in the cluster began after it. The seam's
check takes the writer's transaction id before it reads the list, so every
writer that read it too early has an id below the fence. An aggregate over
another definition's target, and a `JOIN` either of whose tables is one, wait
on the same fence before their one transaction. While it waits,
`status().build_wait` reads `BuildWait::Fence { xid }`, naming the id to look
for in `pg_stat_activity`
([observability](../observability.md#a-builds-fence-on-a-seam-fed-source)). Only these builds wait, and only on
transactions older than the fence.

## An aggregate's or a `JOIN`'s build

An aggregate and a `JOIN` keep each group's partials in hidden tables, not a
per-row ledger, so the plan job is the whole build: one absolute write per
structure, applied while pages apply. Its statements are in
[the group basis](group-basis.md#absolute-writes): a fresh build or a resume
inserts each group with no hidden row from its own read and locks the rest,
which a later statement rewrites from a new snapshot; a rebuild in place locks
every group, then replaces them all from one snapshot. The zero-group GC
collects nothing of the definition from the build's start to its commit.

- **Over a partitioned source**, or a partitioned table of a `JOIN`, the write
  is one transaction per leaf of the recorded tree, in oid order, then a
  remainder: the groups of the target and the ring that route to no recorded
  leaf, read through the root, which zeroes each. The job's cursor holds the
  last leaf committed (`side:oid` for a `JOIN`, whose unpartitioned sides
  go whole, then each partitioned side's leaves and remainder, then a final
  `T` step), so a job that dies resumes after it. A page between two
  transactions sees the earlier leaves' new bases and the later leaves' old
  ones, which is exact per group. A leaf of a tree that has changed since it
  was recorded is read through the root, and the change's own correction
  ([ADR-0017](../decisions/0017-partitioned-sources.md)) repairs what it
  moved; a leaf whose routing text was never recorded halts the rebuild, so
  no statement ever covers more than one leaf's groups.
- **A resume** drops and recreates the hidden tables and writes every group
  again, keeping the target's rows.
- **A rebuild of a `live` definition** (`request_backfill`, a capture
  re-install, an automatic rebuild) is the sweep job alone, which recomputes
  every group in place in the same transactions. Readers see the old values
  until each commits, then the new ones, never an empty or partial target.
  The status reads `backfilling` until every structure has committed.
- **A write that fails on the source's data** (a `SUM` argument that
  overflows) fails whole and is retried, and its fifth failure pauses the
  definition, keeping the error on its build job; no key is quarantined, so a
  `RESUME` once the row is fixed builds it.

While a structure's transaction is open, pages of its groups wait, and no ring
segment retires, so once the ring's slots fill no segment seals and every
source's drain waits for the commit too. Writers aren't held back; the active
segment grows meanwhile.
