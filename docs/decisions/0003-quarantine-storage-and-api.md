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

## Proposal

Track quarantine status in a **separate, sparse exception table** keyed on
`(target_row_pk, transform_id)`, populated only for rows that fail. A healthy
row never appears, so the common case costs nothing beyond the neighbor-table
write it already makes. Expose the table through two client-library reads
(list affected transforms, then page sample rows/errors for one). If a
transform's exception rows grow past a threshold, trip a **fuse**: stop
per-row tracking and quarantine the whole transform until a backfill clears
it.

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
reading a column in hand. We mitigate with an index on
`(transform_id, target_row_pk)` and expect most consumers to ask "is transform
X quarantining anything" (via the API below) rather than check rows inline.

## Exception table shape

Roughly:

* `transform_id`
* `target_row_pk` — the affected row(s); null for a definition-level failure
  (bad formula) rather than a data-level one (bad row)
* `error_message` / `error_class` — enough to distinguish definition-level vs.
  data-level failures per the error-API requirement in
  [open-questions](../open-questions.md)
* `quarantined_at`

Retry-with-backoff vs. immediate quarantine, and whether older entries move to
a dead-letter area, are still open — this ADR fixes storage and the read APIs,
not retry policy.

## Client library API

Two calls, separated so a caller can cheaply poll "is anything wrong" before
paying for row-level detail:

1. **List quarantined transforms** — which transforms currently hold
   quarantined data. Cheap, for polling/dashboards.
2. **Sample quarantined rows** — for a transform, a paginated batch of
   `(target_row_pk, error_message)` pairs, to diagnose and clear the cause.
   Clearing deletes the exception rows once the source data or definition is
   fixed and the row re-evaluates cleanly.

## Fuse: transform-wide quarantine

Per-row quarantine assumes failures are the exception. When a large fraction of
a transform's rows fail (bad formula, incompatible upstream schema change),
tracking each one stops being useful — the application doesn't need a million
identical error rows, it needs to know the transform is broken.

When exception rows for a transform cross a threshold, the fuse trips:
row-level tracking stops, the transform's exception rows are cleared, and the
transform is marked quarantined. Resuming requires a full backfill, the same
mechanism that populates a newly-defined transform (see
[data-flow](../data-flow.md)).

`quarantined` is one state of a transform's broader **lifecycle status**
(`waiting_to_backfill` → `backfilling` → `live`, plus `quarantined`) — see
[transforms — Status](../transforms.md#status).

Undecided:

* The threshold (fixed count vs. percentage of row count) and whether it's
  configurable per transform.
* Whether tripping the fuse also pauses dependent (chained) transforms or lets
  them keep consuming whatever the fused transform last wrote.
