# Data Flow

How a source-data change travels through Trellis to the calculated tables it
feeds — the **physical** flow and timing that [transforms](transforms.md)
deliberately omits. Why this flow is asynchronous is recorded in
[ADR-0002](decisions/0002-async-data-flow.md).

The short version: Trellis watches source tables over Postgres **logical
replication**, pulls committed changes in batches, collapses each batch into the
minimal set of writes against the affected calculated rows, and re-feeds those
writes as input to any downstream transforms — all outside the application's
write path.

This document is the **logical** map of that flow. The concrete machinery that
implements it — the durable staging ring, sealing changes into immutable
batches, claiming and folding them, applying exactly-once deltas, and the
read-your-writes predicate — is written up stage by stage in
[staging-and-claiming](staging-and-claiming/README.md). Each section below
links to the stage that realizes it.

## Why asynchronous

The full tradeoff lives in [ADR-0002](decisions/0002-async-data-flow.md). Three
properties shape the rest of this design:

* **Batching** — many source changes that fold into one aggregate collapse into
  the minimal number of target writes, not one trigger per row.
* **No writer contention** — derivation work leaves the application's hot path,
  so concurrent updates to related source rows don't serialize on locks over
  shared target rows.
* **Quarantine, not block** — a failing derivation runs after the source commit,
  so it can be isolated while the rest keep flowing.

The costs we design around: eventual consistency (see
[Reading derived data](#reading-derived-data)), replication-slot management, and
a single-CPU ceiling on WAL decoding throughput.

## Ingestion via logical replication

Trellis subscribes to the source tables through a Postgres **logical replication
slot**, which delivers a committed, LSN-ordered stream of row-level changes
(insert / update / delete) for the tables feeding any transform. The mechanics —
how a decoded change becomes a durable staged row, and why the slot is
acknowledged only *after* that stage commits — are
[stage 01](staging-and-claiming/01-intake-and-lsn-confirmation.md).

* **Calculated columns live on a neighbor table**, never on the source row.
  Writing them back onto a replicated source row would produce WAL events for our
  own writes and feed them into ingestion.
* At least one Trellis client should stay connected while writes may happen, so
  the slot doesn't accumulate unbounded WAL and block source writes. Changes
  drain into a **staging area** to bound how long we hold the slot's position.
  Its physical shape — an append-only ring of segments, nothing on the hot path
  ever updated — is [stage 02](staging-and-claiming/02-the-staging-ring.md).
* WAL decoding is capped at ~1 CPU by Postgres, the primary throughput ceiling
  for the async path.
* The slot's confirmed position is a durable **LSN watermark** — the point up to
  which all changes have been ingested. Downstream progress is tracked in the
  same LSN space (see [Reading derived data](#reading-derived-data)).

## Staging and batching

Staged changes are **collapsed** before touching any target:

* An **aggregate** target's N staged changes to a group reduce to one
  recompute/delta write for that group's row.
* **1-1** and **cross-join** targets reduce to the distinct set of target rows
  (or key pairs) affected.

Collapsing turns a burst of source writes into the minimal set of target writes.
It is the same machinery used for backfill (a new transform stages its existing
source rows as one large batch).

Physically, a batch boundary is cut in transaction-visibility space by *sealing*
the active segment ([stage 03](staging-and-claiming/03-sealing-and-the-fence.md)),
and the collapse itself is the **claim-time fold** — grouping a sealed batch's
raw changes to one record per key
([stage 04](staging-and-claiming/04-claiming-and-the-fold.md)).

## Evaluation and dependency order

Within a batch, calculated columns are evaluated in **dependency order** over the
dependency graph (see
[transforms — Chaining and cycle detection](transforms.md#chaining-and-cycle-detection)).
Cycles are rejected at definition time, so a valid ordering always exists.

Because formulas use only **immutable** functions, re-evaluating one on unchanged
inputs always yields the same result — the property the
[correctness oracle](#correctness) relies on.

Trellis **evaluates formulas in its own execution layer**, not by issuing SQL to
Postgres per batch. Modeled on Postgres's own implementation (see
[0004-transform-definition-grammar](decisions/0004-transform-definition-grammar.md)),
it computes each target from the collapsed batch as the **minimal write** — for
an aggregate, a small atomic delta against the group's existing value rather than
a full recompute of the group; for 1-1 and cross-join, only the target rows the
batch actually touched. This is what keeps incremental maintenance cheaper than
re-running the definition while still landing on the same result.

How that delta is applied **exactly once** — never twice, never zero times — on
a non-idempotent path, with apply and mark-complete in a single transaction, is
[stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md).

## Writing to calculated tables and chaining

Evaluated results are written to the calculated (neighbor / target) tables. Those
writes are themselves changes, so any **chained** transform receives the
just-written rows as input to a subsequent batch, walking the dependency graph
across tables. This is how transitive derivations are fulfilled without
special-casing: each layer's output feeds back into the staging area as the next
layer's input. Crucially, a worker propagating downstream appends into the
**active** segment, never the batch it is draining — the property that keeps a
claimed batch immutable
([stage 05 — downstream propagation](staging-and-claiming/05-apply-and-exactly-once-deltas.md)).

## Failure handling and quarantine

When a derivation fails (formula error, constraint violation), the async model
lets us **quarantine the minimal affected slice** rather than failing the
already-committed source write: one bad transform or source row affects only its
dependents, and the rest stays readable.

The concrete storage of quarantine status and the API to discover and clear
quarantines are **not yet decided** — see [open-questions](open-questions.md).
Quarantine is one state of a transform's broader **lifecycle status** — see
[observability — Transform status lifecycle](observability.md#transform-status-lifecycle).
For how a killer change is isolated, evicted, and its parked work kept honest
(so a quarantined key still blocks a waiting reader), see
[stage 06](staging-and-claiming/06-cleanup-and-reclaim.md).

## Reading derived data

Derived tables are eventually consistent. Two ways to reason about staleness:

* **`await(LSN, timeout)`** — an opt-in primitive that blocks until every
  derivation up to a given LSN has landed, for a strong read when one is needed.
* **Lag telemetry** — always-on metrics exposing how far derived data trails the
  source LSN.

The predicate behind `await` — the one that must never falsely report
"converged" — is [stage 07](staging-and-claiming/07-convergence-and-await.md).

## Correctness

The correctness bar for the [generative suite](../README.md#structure): once
Trellis has caught up to a given LSN, each incrementally-maintained target must
be **exactly equal to a full recompute** of its definition against the source
data at that LSN, for any interleaving of source changes. Incremental maintenance
is only ever an optimization over that result, never a different answer. The
delta path is held to exactly this bar — byte-identical convergence to a
from-scratch `GROUP BY` oracle after every op and every drain interleaving
([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md)).
