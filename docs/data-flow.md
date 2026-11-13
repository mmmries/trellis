# Data Flow

How a source-data change travels through Trellis to the calculated tables it
feeds — the **physical** flow and timing that [transforms](transforms.md)
deliberately omits. Why it's asynchronous:
[ADR-0002](decisions/0002-async-data-flow.md).

Trellis captures each change to a source table with **triggers** that write it
into a staging ring in the writer's own transaction, pulls the staged changes
in batches, collapses each batch into the minimal set of writes against the
affected calculated rows, and re-feeds those writes to any downstream
transforms. Only the capture runs on the application's write path.

This is the **logical** map. The machinery that implements it — durable staging
ring, sealing, claiming and folding, exactly-once deltas, the read-your-writes
predicate — is written up stage by stage in
[staging-and-claiming](staging-and-claiming/README.md); each section below links
to the stage that realizes it.

## Why asynchronous

Three properties shape the rest of this design:

* **Batching** — many source changes that fold into one aggregate collapse into
  the minimal number of target writes, not one trigger per row.
* **No writer contention** — derivation work leaves the application's hot path,
  so concurrent updates to related source rows don't serialize on locks over
  shared target rows.
* **Quarantine, not block** — a failing derivation runs after the source commit,
  so it can be isolated while the rest keep flowing.

The costs we design around: eventual consistency (see
[Reading derived data](#reading-derived-data)), and the capture's cost on every
write to a captured table.

## Capture by triggers

Each source table some transform reads carries statement-level `AFTER`
triggers. They append the statement's row-level changes (insert / update /
delete, with old and new images keyed by primary key) to the staging ring in
the writer's own transaction, so a change is staged exactly when it commits.
The staging worker installs them in the background once a transform reads the
table, under a short `lock_timeout` retry so a writer never queues behind the
install. How the triggers work, what they cost the writer and how they're
installed, widened and removed is
[stage 01](staging-and-claiming/01-capture-by-triggers.md).

* **Calculated columns live on a neighbor table**, never on the source row.
  Writing them back onto a captured source row would feed our own writes into
  capture.
* Changes land in a **staging area** — an append-only ring of segments, nothing
  on the hot path ever updated
  ([stage 02](staging-and-claiming/02-the-staging-ring.md)).
* Each staged row carries the WAL insert position at capture, below its
  commit's position, and a read-your-writes token is a WAL position, so
  downstream progress is tracked in the same LSN space (see
  [Reading derived data](#reading-derived-data)).

The triggers capture changes only. The rows a table already holds when a
transform starts reading it are captured separately, by the path below.

## Capturing a table's existing rows

A new transform's target has to reflect every row its source already holds,
not only the changes that arrive after it's defined. The triggers don't see
those rows, so Trellis reads them from the table, and the read and the
triggers have to meet exactly: every commit to the source is either seen by
the read or captured by the triggers, and a commit that both see is counted
once. Mechanics:
[builds](staging-and-claiming/builds.md).

### Registration

Registering a transform (`Trellis::apply` with a `TRANSFORM` statement)
validates the definition, creates the target table and the tables Trellis
keeps beside it,
writes the catalog row as `waiting_to_backfill`, and returns. It reads no
source rows and installs no triggers, so its latency doesn't depend on the
table's size. The table and the catalog row commit in one transaction, so a
registration that fails leaves neither behind and can be retried as is. If
any relation already exists under the target's qualified name, registration
is refused with an error naming it: Trellis adopts a table it didn't create
only when the definition asks for it (`ADOPT EXISTING TABLE`) and the table
is empty and shaped as define would create it, because `DROP` later drops the
target table
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)). Which sources and
targets are accepted is
[Supported sources and targets](transforms.md#supported-sources-and-targets).

Everything after registration runs in the background, driven by the staging
worker's maintenance loop and executed on drain threads. Only the staging
worker changes capture, and the catalog is its only source of truth for what
to capture: a drop removes catalog rows, and the next reconcile pass
uninstalls the triggers of a table nothing reads any more.

### Builds

A plain 1-1 definition
is built by **Re-derive over chunks** of its source's key, and an aggregate or
a `JOIN` by **one absolute write per structure**; either way the definition
applies from the moment its build starts, so nothing needs a catch-up
afterwards, and it moves to `live` once the build is done. The steps, the
fence a seam-fed source takes, resumes, rebuilds and field builds are in
[builds](staging-and-claiming/builds.md).

### Re-reading a table for its applying readers

When the staging worker installs capture on a leaf new to a captured
partitioned source, the definitions already applying from the source have
missed the rows the leaf already held. An aggregate or a `JOIN` that the
partition correction engine serves is corrected for that leaf alone
([ADR-0017](decisions/0017-partitioned-sources.md)); any other reader is
re-read. Capture triggers disabled, dropped or recreated outside Trellis, and
a database restored from a logical dump, pause the readers rather than re-read them
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)). An explicit
`Trellis::request_backfill` on a captured table asks for the same re-read. It
takes the table's `schema.table` name, or a bare name when exactly one captured
table carries it; a bare name two captured tables share is refused, naming both.
Either one is repaired in the transaction that makes the call:

- **An aggregate or a `JOIN`** reading the table is rebuilt in place
  ([its build](staging-and-claiming/builds.md#an-aggregates-or-a-joins-build)). The transaction moves it
  `live -> backfilling` and enqueues one job, which recomputes every group.
- **A plain 1-1 transform** over the captured table
  ([the Re-derive build](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks)) is rebuilt.
  The transaction moves it
  `live -> backfilling` and enqueues one plan job and one sweep job, two
  rows per definition, so the call returns quickly whatever the table's size.
  It keeps applying, the plan re-derives every row the table has, and the
  sweep re-derives the entries the table no longer backs. The status is
  `backfilling` when the call returns, so polling for `live` waits out the
  rebuild. A second request while a rebuild runs enqueues another plan and
  sweep.

### What it asks of a deployment

- **No transform is `live` when `apply` returns.** Poll `Trellis::status`
  until it is ([embedding — Poll to `live`](embedding.md#poll-to-live-dont-wait)),
  then take `Trellis::watermark_token` and `await_converged` on it. `live`
  is the steady state: a token awaited after it covers every commit at or
  before the token ([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).
  `await_converged` checks the ring, never a definition's status, so on its
  own it doesn't wait for a build.
- **A staging worker must be running**, and drain threads for any build
  ([embedding — Who runs what](embedding.md#who-runs-what),
  [the silent-stall hazard](embedding.md#the-silent-stall-hazard)).
- **Only the staging worker needs to own the source tables**, because it
  installs their capture triggers. A process that only registers or drops
  transforms needs catalog access and the right to create target tables; the
  reconcile pass uninstalls a dropped transform's capture up to one
  `reconcile_interval` (5s by default) later, and apply skips the changes
  staged meanwhile because nothing reads the table.

## Staging and batching

Staged changes are **collapsed** before touching any target:

* An **aggregate** target's N staged changes to a group reduce to one
  recompute/delta write for that group's row.
* A **`JOIN`** target's changes reduce to one delta per partial cell and per
  visible group.
* **1-1** and **1-1 `JOIN`** targets reduce to the distinct set of target rows
  affected.

Collapsing turns a burst of source writes into the minimal set of target
writes.

Physically, a batch boundary is cut by *sealing* the active segment
([stage 03](staging-and-claiming/03-sealing-and-the-fence.md)), and the collapse
is the **claim-time fold** — grouping a sealed batch's raw changes to one record
per key ([stage 04](staging-and-claiming/04-claiming-and-the-fold.md)).

## Computing fields and dependency order

Within a batch, calculated columns are computed in **dependency order** over the
dependency graph (see
[transforms — Chaining and cycle detection](transforms.md#chaining-and-cycle-detection)).
Cycles are rejected at definition time, so a valid ordering always exists.

Because formulas use only **immutable** functions, recomputing one on unchanged
inputs always yields the same result — the property the
[correctness oracle](#correctness) relies on.

**Postgres computes every field.** Trellis renders each formula into the SQL
statement that writes the target and computes no value itself (see
[0004-transform-definition-grammar](decisions/0004-transform-definition-grammar.md)
and [0002-async-data-flow](decisions/0002-async-data-flow.md)). Each statement
is the **minimal write** for the collapsed batch — for an aggregate, a small
atomic delta against the group's existing value rather than a full recompute;
for a 1-1 or 1-1 `JOIN` target, only the target rows the batch touched. This keeps
incremental maintenance cheaper than re-running the definition while landing on
the same result.

How that delta is applied **exactly once** — never twice, never zero times — with
apply and mark-complete in a single transaction, is
[stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md).

### A 1-1 `JOIN`'s hidden state

A 1-1 `JOIN` ([transforms](transforms.md#1-1-join),
[ADR-0021](decisions/0021-one-to-one-join-transforms.md)) never reads a source
table while it writes: each page reads Trellis-owned copies of its tables,
kept in the catalog schema and named by the transform's id. A star of `a` and
up to four joined tables, numbered by position from 1 in `JOIN` order, keeps:

* **`R`**, one entry per `a` row: the 1-1 ledger's ordering state
  ([ADR-0002](decisions/0002-async-data-flow.md)), the row's join key at every
  position and the `a` columns the fields read, with one index per position on
  that position's join key.
* **An `M` per position** (`M`, then `M2` to `M4`), one entry per row of the
  table joined there, keyed by its primary key and indexed on its join key.
* **`J`**, one table of lock rows, one per `(position, join key)`, and the
  transform's truncate lock row.
* **Pending fan-out markers** (`join1_fan_out`), one per position and key whose
  change reaches more `a` rows than one chunk, until the last chunk commits.

The two-table `JOIN` is the one-position case: the same tables, one `M`.

Every transaction that touches a target row first holds that row's `R`
entry, so the entry orders every pair of writers of the row whatever
positions they write. An `a` change locks `J` at every position for its old
and new keys; a change of the table at position `i` locks `J` at position `i`
only, then the `R` entries of every row on its keys there, in one statement
over every position the page touches, before it reads any `M`. A page takes
its locks in one global order over the union of its keys:

1. the definition rows, in id order: `FOR UPDATE` for a target whose joined
   table the page truncates, and `FOR KEY SHARE` for one whose page may hold
   a row in quarantine, so a page queues behind a rebuild's start, which takes
   them in the same order;
2. the truncate lock row: `FOR UPDATE` in a page that truncates a table,
   `FOR SHARE` in any other, which orders a `TRUNCATE a` against the pages in
   flight;
3. `J`, in one statement sorted by `(position, key)`;
4. the `R` entries, in one statement sorted by entry key;
5. the `M` entries, sorted by `(position, id)`;
6. the target rows.

Placeholder inserts follow the same order, so no two transactions take two
rows of `J`, `R` or `M` in opposite orders. Why each rule is needed at every
position, and the model that checked them, is ADR-0021's
[lock rules](decisions/0021-one-to-one-join-transforms.md#no-writer-reads-another-tables-source-only-its-copy-under-j-and-the-rows-r-entry).

## Writing to calculated tables and chaining

Computed results are written to the calculated tables. Those writes are
themselves changes, so any **chained** transform receives the just-written rows
as input to a subsequent batch, walking the dependency graph across tables: each
layer's output feeds the staging area as the next layer's input, so transitive
derivations need no special-casing. Crucially, a worker propagating downstream
appends into the **active** segment, never the batch it is draining, keeping a
claimed batch immutable
([stage 05 — downstream propagation](staging-and-claiming/05-apply-and-exactly-once-deltas.md)).

## Failure handling and quarantine

When a derivation fails (formula error, constraint violation), the async model
lets us **quarantine the minimal affected slice** rather than failing the
already-committed source write: one bad transform or source row affects only its
dependents, and the rest stays readable.

Quarantine storage and the API to discover and clear quarantines are
[ADR-0003](decisions/0003-quarantine-storage-and-api.md). Quarantine is
one state of a transform's broader **lifecycle status** (see
[the status reference](status-reference.md)).
For how a killer change is isolated, evicted, and its parked work kept honest so
a quarantined key still blocks a waiting reader, see
[stage 06](staging-and-claiming/06-cleanup-and-reclaim.md).

## Reading derived data

Derived tables are eventually consistent. Two ways to reason about staleness:

* **`await_converged`** — an opt-in primitive that blocks until every
  derivation up to a watermark token has landed, for a strong read when one is
  needed.
* **Lag telemetry** — always-on metrics exposing how far derived data trails the
  source LSN.

**The contract.** A value read before `await_converged` may be inconsistent.
After it, values are exact: `await_converged` is how a reader gets exact
values.

What inconsistent looks like depends on the size of the group an aggregate
reads:

* **Groups of many rows** (thousands or more) read as slightly behind or ahead
  by the changes still in flight. A count doesn't go negative and a group
  doesn't disappear.
* **Small or hot groups** (a handful of rows, fast concurrent writers) can
  briefly show values no source state could produce: a negative `COUNT(x)`,
  `COUNT(x)` above `COUNT(*)`, an `AVG` far off, or a group missing for a
  moment. A `JOIN` target
  ([ADR-0019](decisions/0019-join-transforms.md)) can show the same.

An aggregate or `JOIN` target holds a group's row only while the group's row
count is above 0, so a group whose count falls to 0 or below, even for a
moment, has no row.

This is by design. Pages apply in parallel and commit out of order, and the
deltas commute, so the converged state is exact while a state between two
out-of-order pages can describe no real data
([ADR-0002](decisions/0002-async-data-flow.md#readers-see-a-group-only-while-its-row-count-is-above-0)).

The predicate behind `await` — the one that must never falsely report
"converged" — is [stage 07](staging-and-claiming/07-convergence-and-await.md).

A rebuild of a `live` transform (`Trellis::request_backfill`, a capture
re-install, or an automatic rebuild) never empties its target: it rewrites
the target's rows in place while changes keep applying, so a reader sees
each row's previous value until the rebuild rewrites it. The transform reads
`backfilling` until the rebuild is done
([status reference](status-reference.md#backfilling)).

## Correctness

The correctness bar for the [generative suite](../README.md#structure): once
Trellis has caught up to a given LSN, each incrementally-maintained target must
be **exactly equal to a full recompute** of its definition against the source
data at that LSN, for any interleaving of source changes. Incremental maintenance
is only ever an optimization over that result, never a different answer — held to
byte-identical convergence against a from-scratch `GROUP BY` oracle after every op
and every drain interleaving
([stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md)). The
ways an operator can still leave a target stale are listed in
[known correctness gaps](known-correctness-gaps.md).
