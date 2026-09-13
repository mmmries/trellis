---
status: draft
date: 2026-08-20
deciders: Michael Ries
consulted: 
informed:
---

# Quarantine Storage and Error API

Because [data-flow](../data-flow.md) commits invalid source data before a
derivation runs, a failing transform or bad row must be **quarantined** rather
than block the write. This ADR settles the questions of where per-row status
lives, and what API applications use to discover and clear quarantines — plus a
guardrail for when quarantine itself is the wrong tool.

**Amendment (2026-09-12):** the original proposal below tracked one exception
per `(target_row_pk, transform_id)` and fused a whole transform at once. In
practice (see [Public API design](../public-api-design.md) for the discussion
that surfaced this) a single `TRANSFORM` can compute several calculated
columns, and a failure in one column's formula (a broken relationship, a
function that only errors for one field) shouldn't force every other healthy
column on that same transform into quarantine. This amendment adds a **column**
dimension throughout: exception detail is still one record per poisoned
*source* row (matching what's actually implemented — see below), but that
record's payload now names which transform/column pairs failed for it, and the
fuse trips per `(transform, column)` rather than per transform. A transform's
overall lifecycle status ([transforms — Status](../transforms.md#status)) is
unaffected by this — it still describes the whole keyspace — but a `live`
transform can now carry one or more individually `paused` columns without the
whole transform going `quarantined`. The sections below are updated in place;
nothing from the original proposal survives unchanged except the sparse-table
rationale in "Options considered".

## Proposal

Track quarantine status in a **separate, sparse exception table**, populated
only for source rows that fail to propagate. A healthy row never appears, so
the common case costs nothing beyond the neighbor-table write it already
makes. This matches what's actually implemented (`poison`, `V13__quarantine.sql`)
rather than the original proposal's `(target_row_pk, transform_id)` keying: a
single *source* row's change can fan out to several downstream
transforms/columns at once, so the natural exception key is the source row
(`src_table`, `key`) that failed to fold, not any one of its downstream
targets. That row's exception record carries a `failures` array — one entry
per `(transform, column)` pair that failed while propagating this row, each
with its own error message — so one poisoned source row can implicate several
transforms/columns without needing several exception rows.

Expose this through three client-library reads (list what's currently
paused/quarantined across every transform and column, then page sample rows
for one, per the addressing scheme below) rather than the original's two — see
"Client library API". If poisoned-row counts for a given `(transform, column)`
pair cross a threshold, trip a **column-level fuse**: stop per-row tracking for
that column and mark it `paused` until a backfill clears it. A transform's
whole keyspace can still go `quarantined` (the original all-or-nothing fuse),
but that's now a coarser, separate tier from column pausing — see "Fuse" below
for how the two relate.

## Options considered

* **In the neighbor table.** Status columns beside the derived values.
  Cheapest to read (no join), but a target row can be written by several
  transforms, so avoiding conflated failures needs one column per transform —
  the table grows wider with every transform chained onto it.
* **A dense `(row, transform)` status table.** One row per key regardless of
  health, written alongside every neighbor-table write. Right granularity, but
  doubles write amplification on the hot path — at 180k+ source changes/sec,
  a second write for every successful row, not just failures.
* **Sparse exception table (chosen).** Dense-table shape, but only failing
  rows are inserted; a row's absence *is* its healthy status. Steady-state
  write cost stays near zero (>99% of rows never touch it) while still
  answering "is this row valid" and giving the error API a table to query.

The tradeoff: checking validity on read means checking for *absence*, not
reading a column in hand. We mitigate with the primary key on `(src_table,
key)` plus a GIN index on `failures` (see "Exception table shape") and expect
most consumers to ask "is transform X (or column X.Y) quarantining anything"
(via the API below) rather than check rows inline.

## Exception table shape

One record per poisoned **source** row — extending the existing `poison`
table (`src_table`, `key` primary key) rather than replacing it:

* `src_table`, `key` — the poisoned source row, as today.
* `poisoned_at` — as today.
* `failures` — a `jsonb` array, one element per `(transform, column)` pair
  that failed while propagating this row, each shaped roughly
  `{"transform": ..., "column": ..., "error_message": ..., "failed_at": ...}`.
  `column` is null for a failure that isn't attributable to one calculated
  field (e.g. a key-shape/DDL-level failure that dooms the whole row for that
  transform) — the same definition-level/data-level distinction the original
  proposal wanted, just carried per array element instead of per row. This
  replaces the current single `last_error text` column.

A GIN index on `failures` (`jsonb_path_ops`, containment queries) is what the
per-column fuse check (below) and the "list paused/quarantined" read both lean
on — finding "every row with a failure entry for transform X, column Y" without
scanning the whole table.

Retry-with-backoff vs. immediate quarantine, and whether older entries move to
a dead-letter area, are still open — this ADR fixes storage and the read APIs,
not retry policy.

## Column status table

Separate from the exception detail above: a small, dense table recording each
transform's currently-paused columns, so "is anything paused right now" is a
cheap read over a handful of rows rather than a scan/aggregate over
`poison.failures`. Roughly:

* `transform_id`, `column_name` — primary key.
* `paused_at`.
* `last_error` — the most recent failure's message, for a quick glance without
  paging exception detail.

A transform with no rows here has every column live. This is the table the
per-column fuse writes to when it trips, and clears from when a backfill
resumes the column — mirroring how the transform-wide `quarantined` lifecycle
state already works, just at column grain. It does **not** replace the
transform's own overall lifecycle status
([transforms — Status](../transforms.md#status)): a transform can be `live`
overall while this table lists one or more of its columns as paused. Whether
enough paused columns should ever *escalate* the transform's overall status to
`quarantined` is open — see "Undecided".

## Client library API

Three calls now, still separated by cost so a caller can cheaply poll before
paying for detail:

1. **List paused/quarantined targets** — a flat list across every transform,
   each entry addressed as `transform` (the whole keyspace, from the
   transform-wide fuse/lifecycle) or `transform.column` (a single paused
   column, from the column status table above). Cheap — reads the column
   status table plus each transform's own lifecycle status, no join against
   exception detail. This is the one dashboards/health-checks poll.
2. **Status for one target** — given `transform` or `transform.column`, its
   current state (`live`/`paused`/`quarantined` as applicable) and, for a
   paused column, when it tripped and its last error.
3. **Sample quarantined rows** — for a `transform` or `transform.column`
   target, a paginated batch of `(src_table, key, error_message)` triples
   pulled from `poison.failures`, to diagnose and clear the cause. Clearing a
   row's relevant `failures` entry (not necessarily the whole exception
   record — a source row can still be poisoned for a different
   transform/column after this one clears) happens once the source data or
   definition is fixed and the row re-evaluates cleanly.

## Fuse: per-column, then transform-wide

Per-row quarantine assumes failures are the exception. When a large fraction of
one column's rows fail (a broken formula for that field, an incompatible
upstream schema change touching just the columns it reads), tracking each one
individually stops being useful — the application doesn't need a million
identical error rows, it needs to know that one column is broken.

When poisoned-row counts for a `(transform, column)` pair cross a threshold,
the **column fuse** trips: row-level tracking for that pair stops, its
`failures` entries are cleared from `poison` (other columns' entries on the
same source rows are untouched), and the column is marked `paused` in the
column status table above. Resuming a single paused column re-runs the
backfill for just that column's formula against already-built rows, without
touching the rest of the transform's columns or its overall lifecycle status.

The original **transform-wide fuse** still exists as a coarser, separate tier:
if a failure isn't attributable to one column (e.g. a key-shape/DDL failure
that dooms every column's write for that row alike), it trips the whole
transform to `quarantined` exactly as before, and resuming re-runs the full
backfill (see [data-flow](../data-flow.md)). `quarantined` remains one state of
a transform's broader **lifecycle status**
(`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`).

Undecided:

* The threshold (fixed count vs. percentage of row count) for the column fuse,
  and whether it's configurable per transform/column — same open question the
  original transform-wide fuse had, now at finer grain.
* Whether the column-fuse count is a live aggregate over `poison.failures`
  (via the GIN index above) or an incrementally-maintained counter table
  alongside it, the same way `key_deaths` avoids re-scanning the ring for the
  per-key fuse. Leaning toward the latter, for the same write-amplification
  reasons `key_deaths` exists, but not settled.
* Whether enough paused columns on one transform should ever auto-escalate it
  to transform-wide `quarantined`, or whether the two tiers stay fully
  independent (a transform can sit at "every column but one paused"
  indefinitely).
* What a paused column's target value does in the meantime: freeze at its
  last successfully computed value, or go `null`. Freezing is more useful to
  downstream readers but gives no in-band signal that the value is stale
  without also checking status — may need a per-row/column staleness marker
  of its own.
* Whether tripping either fuse also pauses dependent (chained) transforms that
  read the fused transform/column's output, or lets them keep consuming
  whatever it last wrote.
