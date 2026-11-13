---
status: accepted
date: 2026-10-23
deciders: Michael Ries
---

# Data Flow: Synchronous Capture, Asynchronous Exactly-Once Derivation

*The design elements that carry the architecture are marked **Decision
point**. Logical-replication capture and apply-layer ordering of absolute
writes are rejected alternatives below, with the measurements that rejected
them.*

Trellis derives tables from other tables. Two questions decide the whole
design: *where* a derivation runs, and *how* a source change is captured and
ordered so that the derivation is applied exactly once. This ADR answers
both. [ADR-0017](0017-partitioned-sources.md) extends its capture and truncate
decisions to partitioned sources, and
[ADR-0019](0019-join-transforms.md) builds JOIN transforms on its group-level
basis.

**Derivation is asynchronous.** Source changes are batched, and derived
values are computed and written by drain workers outside the application's
transactions, for the reasons in
[Derivation is asynchronous](#derivation-is-asynchronous).

**Capture is synchronous.** A source change is recorded into Trellis's
staging ring by an `AFTER … FOR EACH STATEMENT` trigger inside the writer's
own transaction. The ring row's transaction id is therefore the source
commit's, and that identity is what makes every derived write orderable.

**Ordering is owned by the target, at the grain the target is kept at.** A
1-1 target keeps a slim ledger with one entry per source row: the ring
position last applied to the row and the snapshot it was last derived under.
An aggregate target keeps the same ordering state once per *group*, beside
hidden per-group partials (a row count, non-null counts and sums)
that every admitted aggregate is a function of. Every write that derives a
value from a *read* rather than from carried images locks the entry or group
first and reads in the same statement as its snapshot; every change is
applied or skipped by checking its transaction against that snapshot (and,
per row, its position against the last applied one). A build is the same
operation over chunks of the source, and a definition applies changes from
its first chunk.

The reason for the ordering state is the class of write that ordering by
position alone leaves unordered. A **delta** (`+f(new) − f(old)` into a
group) commutes with every other delta, so parallel workers may apply deltas
in any order and the exactly-once argument of
[stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md)
holds. An **absolute write**, any value derived from a read, does not
commute: it is right only if nothing later in the change order has already
been applied to the same row or group. Trellis has several producers of
absolute writes (a build, a rebuild, an image-less recompute, a partition
correction), each reading live `READ COMMITTED` state and each racing
out-of-order parallel drains. Ordering them by position needs one precedence
rule per pairing of producers; one ordering mechanism for every absolute
write replaces those rules. My measurements also found scaling cliffs in the
alternatives, a logical-replication capture, a one-pass build and a per-row
ledger for aggregates, that this design avoids:

- I measured a logical replication slot staging at most about 120k rows/s
  whatever the writers do, and it never captured a 10M-row `COPY`;
- I ran a one-pass go-live re-read at 100M rows on a 31 GB box and it could not
  finish, because a drain batch held its whole share of a segment in memory;
- I measured a per-row aggregate ledger at 15.1–15.3 GB for a 100M-row source
  in 50k groups, where the group basis keeps 5 MB, and its 100M build at
  370–380 s and 58–71 GB of WAL, where one `GROUP BY` per partition took
  1.4–1.9 s.

Conventions: **must** is a requirement the evidence forces; **should** is a
recommendation the evidence supports but does not force.

A concrete aggregate, over `order_line_items(id bigint primary key, order_id
bigint, amount numeric(12,2))`:

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
```

Its target holds one visible row per `order_id` with at least one line item.
Beside it, Trellis keeps per group the row count, the non-null count and the
`numeric` sum of `amount`, the snapshot of the last absolute write and that
write's segment stamp. A line item moving from order 7 to order 9 is two
sides, `−1` and `−amount` against order 7 and `+1` and `+amount` against order
9, each checked against its own group's snapshot.

## Derivation is asynchronous

Update derived tables in the transaction that changes the source, or later,
in batches? Trellis derives **later**, and that choice shapes the user
experience.

Synchronous derivation (triggers or functions that write the derived table in
the source transaction) is consistent by construction and fast for shallow
same-row derivations, but it fails where Trellis's use-cases live:

- **Cross-table formulas are unsound.** Concurrent transactions don't
  see each other's changes until they commit, so a derivation that reads a
  row of another table inside the writer's transaction can compute from a
  value that is already stale when it commits.
- **No batching.** Five hundred updates folding into one aggregate row run
  five hundred derivations and serialize on that row's lock. Asynchronous
  batches collapse them into one write.
- **One failed derivation fails the whole write.** There is no way to
  quarantine the one broken dependent and let the other ninety-nine proceed.
- **Highly connected rows get slow.** Updating an account with millions of
  line items becomes a huge transaction on the application's commit path.

Asynchronous derivation keeps derivation cost and latency off the
application's write path, batches N source changes into M target writes,
quarantines a failing dependent instead of failing the write
([ADR-0003](0003-quarantine-storage-and-api.md)), and lets Trellis lag under
load rather than slow the source. Its costs are the ones Trellis documents:
derived data is eventually consistent, with `await_converged` as the
read-your-writes primitive
([stage 07](../staging-and-claiming/07-convergence-and-await.md)); errors are
reported through an API rather than at write time; and calculated columns live
on a neighbour table.

Only *derivation* is asynchronous. The next section moves *capture* into the
writer's transaction, and the objections above do not apply to it: capture is
one append of the key and the read columns, it reads no related row, it
serializes on nothing but the ring's append path, and a derivation failure
still quarantines. Two objections do carry over to capture and are taken on as
invariants below: a disabled trigger strands data (I6's audit), and a
Trellis-side schema change must not fail the application's statement (I6's
never-fail rule).

## Invariants

The design is these invariants; every element below exists to hold one of
them. I0 to I5 are the ordering rules as my experiments refined them, stated
for both grains: a 1-1 target's ledger entry per source row, and an aggregate
target's group. I6 to I8 are the rules the capture and build experiments
forced. I1 and I5 are defined in
[ADR-0028](0028-locking-and-writing.md), the one definition of how writers
lock and write, which also holds the rules for a build's bulk writes.

| | Invariant | Evidence and refinements |
|---|---|---|
| **I0** | **One database.** Sources, the ring, every ledger, every group's state and every target live in one Postgres database, so one snapshot orders every commit Trellis will see. | Design premise, made exact by trigger capture: a ring row's transaction is the source commit's. |
| **I1** | **Read after lock.** Every live read that feeds an absolute write happens after the writer holds the lock on the state it will write, and the snapshot it stores is taken in the same statement as the read; [ADR-0028](0028-locking-and-writing.md#i1-a-writer-reads-after-its-lock-under-read-committed) defines it. | In ADR-0028. |
| **I2** | **Visibility-checked application.** A change is applied to a 1-1 row iff its transaction is not visible in the row's basis snapshot and its ring position is above the row's `applied_lsn`; a *side* (a group, a transaction) is applied to a group iff its transaction is not visible in the group's basis. Both refuse a change at or below the target's truncate floor ([Truncate](#truncate-ddl-drop)). Skipping is exact, never "maybe counted, re-derive". Ordering state belongs to this cluster, so a logical restore pauses every definition and its `RESUME` resets that state ([cleanup](../staging-and-claiming/06-cleanup-and-reclaim.md)). | In the interleaving experiment, skip-iff-visible alone fails three scenarios and stamping Apply's own snapshot two more, while visible-or-`applied_lsn` passes all seventeen; the group basis passes every one that applies to an aggregate on visibility alone, because its writes to a group commute. |
| **I3** | **Ordering state at the target's grain; group values are functions of sums.** A 1-1 row's ordering state is its ledger entry; a group's is its basis and stamp, and its partials are the sum of the sides applied since its last absolute write, which chain per row (R3). | In the interleaving experiment, eight workers creating the same groups at once, and a row passing through a group within one folded batch while that group rebuilds, both hold with groups as sums and no pre-lock or probe. |
| **I4** | **Tombstones live until the batch watermark passes.** A deleted row's entry, and a zero group that has a basis, stay until the contiguous drained prefix passes their segment stamp ([Convergence and status](#convergence-and-status)). | In the interleaving experiment an older update resurrects a deleted row unless the tombstone is kept; in my group-basis model a GC that ignored the drained prefix was wrong in every soak run. |
| **I5** | **One lock order, taken as one sorted batch (R7).** Every writer takes its locks in one order, each class in key order in one statement, never in a per-row loop. [ADR-0028](0028-locking-and-writing.md#i5-every-writer-takes-its-locks-in-one-order-each-class-in-one-sorted-statement) defines the order. | In ADR-0028. |
| **I6** | **Never block, and never fail, an application writer.** No Trellis transaction takes a lock an application write can queue behind, except the join and drop fences, which are bounded by `lock_timeout` and retried, and a `MIN`/`MAX` probe's `ACCESS SHARE` lock, which only an `ACCESS EXCLUSIVE` statement queues behind ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#ddl-on-a-user-table-never-blocks-a-writer-adr-0002-i6)). A schema change to a read column never fails the application's statement. | I measured a bare `CREATE TRIGGER` stalling every writer for 25 s and, with a 50 ms `lock_timeout` retry, a worst wait of 52 ms; a rename of a read column under the capture function failed every insert until regeneration. |
| **I7** | **No Trellis transaction waits for a lock while it holds a snapshot open.** Every lock statement runs under a `lock_timeout`; on timeout the transaction rolls back and the work is retried from outside any transaction. | In my 100M chunked-build run, a drain batch's ledger insert waited 1 h 50 min behind chunk transactions, holding back vacuum and the sealer for the whole wait. |
| **I8** | **Memory is bounded by a batch cap, never by segment size.** A drain batch folds, re-reads and applies at most a fixed number of changes and pages through a larger bucket. | In my one-pass build, a drain held ~650 B per staged row per worker: 6.32 GB at 10M, OOM at 100M; fewer workers bound nothing. |

## Capture by statement triggers

**Decision point.** Trellis code runs inside the application's transactions.
What it does there is one append. The mechanism is
[stage 01](../staging-and-claiming/01-capture-by-triggers.md).

- **Must:** statement triggers on each captured table (`AFTER` insert,
  update and delete with transition tables, `AFTER TRUNCATE`, and a
  `BEFORE … FOR EACH STATEMENT` trigger that marks a statement's span) append
  the statement's rows to the active ring segment in the writer's
  transaction. The ring row's `row_txid` (`DEFAULT pg_current_xact_id()`) is
  therefore the source commit's `xid8`, the identity I2 checks. *Evidence:* I
  measured statement triggers beating row triggers 5–10x above 1 row/commit;
  capture scaling with writers to ~1.9M rows/s at 1,000 rows/commit and ~190k
  at 1 row/commit; and, at 1 row/commit on disk, the tax vanishing under a
  real fsync (within 1% of no capture at 16 writers).
- **Must:** the generated function names only the key and the columns the
  table's definitions read, emits their old and new images, renders every
  value with `format('%s', col)` under five output settings pinned as `SET`
  clauses on the function, skips an update touching no read column, is
  `SECURITY DEFINER` owned by the Trellis role with a pinned `search_path`,
  is `ENABLE ALWAYS`, and reads the active slot through the sequence mirror
  ([The seal fence](#the-seal-fence-under-application-writers)). *Evidence:*
  across 38 type families only `format()` and `hstore` with pinned settings
  are byte-identical to `tuple_to_json`, and an unpinned image depends on the
  application session's settings; an encoding that reads every column cost
  3.3 ms and 323 KB per row on a 100 KB column, 40x the control, where naming
  only the read columns cost +30 µs.
- **Must:** the new image is the live row, re-read only when another write
  to the table ran inside the statement's span. *Evidence:* an unconditional
  re-read failed 50–87% of concurrent `SERIALIZABLE` auto-increment inserts
  with `40001`; gated, none fail.
- **Must (R3, the image chain):** a row's images form a chain, each change's
  old image being the previous change's new image, so a group-basis reader's
  sides telescope to the row's current contribution. Where the live re-read
  breaks the chain, a group-basis reader counts every transition row exactly
  once ([stage 01](../staging-and-claiming/01-capture-by-triggers.md#every-transition-row-counts-once)).
  *Evidence:* in my group-basis model the live images left groups wrong in
  every soak run with application triggers that rewrite rows (about 1,200
  wrong groups per version); unpaired transition images were exact in every
  run and every nested-write and cascade shape.
- **Must:** the application needs nothing on Trellis's schema, and the
  Trellis role owns each source table or is a member of its owning role,
  because `ENABLE ALWAYS` needs ownership. No superuser
  ([ADR-0027](0027-operating-principles.md), rule 1).
- **Must (I6):** `CREATE TRIGGER` and `DROP TRIGGER` run under a short
  `lock_timeout` and are retried. The join's commit is the **join fence**:
  its `SHARE ROW EXCLUSIVE` lock waits out every writer in flight, so every
  commit either precedes the trigger and is visible to any snapshot taken
  after it, or ran the trigger. No separate fence, marker or generation makes
  a table's join gap-free.
- **Must (I6):** a schema change to a read column never fails the
  application's write. The function stages a *schema-changed* marker
  instead; the drain pauses every definition reading a missing column before
  it applies anything later, the others keep applying, and the staging
  worker regenerates the function. *Evidence:* a rename or drop of a named
  column failed every insert until regenerated; the guard costs +3.4–5 µs
  per single-row statement. Event triggers are unavailable on some hosts, so
  regeneration does not depend on one.
- **Must:** the self-check audit
  ([ADR-0013](0013-self-check-production-recompute-audit.md)) verifies from
  the catalog that every captured table's triggers exist, are enabled
  `ALWAYS` and call functions the Trellis role owns with the privileges they
  use, and that the table hasn't joined a partition or inheritance
  hierarchy, and reports any fault before any recompute comparison. A
  disabled, dropped or recreated trigger pauses the table's readers until a
  `RESUME`
  ([known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes)).
- **Only the staging worker creates or drops triggers**, from the catalog
  alone. Registration does no source reads and no capture work, so
  registering processes need catalog access and the right to create target
  tables, nothing on the sources.
- There is one capture path: no replication slot, publication or
  `wal_level = logical`, and no second path needing its own proof of the
  ordering argument.
- **Cost to state.** On disk at 1,000 rows/commit with 16 writers the
  writer's throughput is 0.49x no-capture and the ring row is ~2.9x the
  source row's WAL bytes. No synchronous capture can lose less than 25% at
  1,000 rows/commit, so the bar is: batched writes cost no more than one
  expression index's CPU per row; p99 commit latency at 1 row/commit rises by
  less than 2.5x on tmpfs and is unchanged on disk. A bulk-load pattern (drop
  the triggers, load, rebuild the readers) is documented and exact.

### The seal fence under application writers

The ring, the two-phase seal and its fence
([stage 02](../staging-and-claiming/02-the-staging-ring.md),
[stage 03](../staging-and-claiming/03-sealing-and-the-fence.md)) carry over.
A ring writer is the application, at whatever isolation level it chose.

- **Must:** the writer reads the active slot through the sequence mirror,
  which the seal sets before its `xmax` bump, and its xid is assigned in the
  same statement as that read. *Evidence:* `REPEATABLE READ` and
  `SERIALIZABLE` writers across seals read a pointer two flips old from the
  table, and their rows landed in a slot no batch would read.
- **Must:** the truncate barrier decides `has_truncate` after the batch's
  contents are fixed.
- **Per-key order is `(lsn, change_id)`**, with the `lsn` read in the
  trigger. Cross-key commit order is not available from triggers and, under
  I3, not needed. *Evidence:* with concurrent writers on one key per-key
  order held in every interleaving, because a second writer runs its trigger
  only after the first commits; a `CACHE`d sequence alone inverted it.
- Nothing relies on what drains before or after a seal, or on which process
  seals.

## The 1-1 ledger: one target-owned entry per source row

**Decision point.** The target, not the ring and not a projection, owns the
state that orders its writes. A 1-1 target owns it per source row
([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger)).

- **Must:** a 1-1 target's ledger is a slim side table beside an unchanged
  target table, one entry per source key holding the position and segment
  last applied, the basis snapshot and a tombstone flag; the target row holds
  the values. It is written in the same transaction as the target, entries
  locked in key order before any target row is touched. A tombstone row in
  the target would leave a deleted row readable after `await_converged`
  until GC runs; a side table gives a never-seen key a single row to lock.
  Its cost is one more row write per change: ~200 B of WAL per row in
  `throughput-ramp` (about 500 B right after a checkpoint).
- **Must:** exactly two operations exist, and every producer is one of them.
  - **Re-derive(r).** Lock r's entry (inserting a placeholder if absent;
    a build chunk instead inserts an absent entry from its read, I1).
    In one statement: take the snapshot and read r's source row. Then write
    the target row and replace the entry's basis with the snapshot, leaving
    `applied_lsn` as it was; a row gone from the source deletes the target
    row and makes the entry a tombstone. It never stores the read's position
    as `applied_lsn`: a change whose trigger ran before the read but which
    committed after it would sit below that position, invisible to the
    basis, and be skipped for good. The basis alone, the full `pg_snapshot`
    of the read (I1), records what the read saw.
  - **Apply(r, C, image).** Lock r's entry. If C is visible in the entry's
    basis, or C's position is at or below `applied_lsn`, stop (I2).
    Otherwise write the target row from C's new image and set `applied_lsn`
    to C's position, basis unchanged. A delete makes the entry a tombstone.
- **Must:** Postgres evaluates every field, over the source row cast back to
  the source columns' own types and collations, and Rust computes no
  values, so a field means over the row what it means over the source
  table. *Evidence:* the `one-to-one-apply` benchmark (1M inserts into one
  1-1 target, disk-backed, against a control that computed the fields in
  Rust) ran 14–29% more rows/s at about 26 µs of Postgres CPU per row instead
  of 33–36, and a page's transaction averaged 140–181 ms instead of 236–296.

  | Producer | Operation |
  |---|---|
  | Captured insert/update/delete | Apply |
  | Re-derive build, resume rebuild, `request_backfill`, quarantine release, column resume, `ALTER TRANSFORM` added column | Re-derive over the definition's key space, chunked |
  | Source `TRUNCATE` | The ledger is emptied, the target's truncate floor is raised to the truncate's ring position, and the target is cleared ([Truncate](#truncate-ddl-drop)) |
  | Chained hop | Apply, where C is the upstream apply transaction ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)) |

## Aggregates: a basis per group, kept by deltas

**Decision point.** An aggregate target keeps its ordering state per group,
not per source row. Each group carries hidden partials, its basis (the
`pg_snapshot` of the last absolute write to it) and its stamp (the newest
segment that snapshot saw). Live changes reach it only as signed deltas
computed from captured images; absolute writes reach it only through the
group lock. Maintaining it reads the source on the hot path in one case
only: a retraction that removes a group's current `MIN` or `MAX` re-reads
that group's extreme with one index probe, after the group's lock
([`MIN` and `MAX` recompute on retract](#min-and-max-recompute-on-retract)).
Every other admitted aggregate is kept from the changes alone and needs no
source index to stay exact.

A per-row ledger is not needed for old values: the ring carries every
change's old image. It exists for ordering, and one row's changes form a
prefix in any snapshot, so in any region (a row, a group, a partition, a
build chunk) the changes a region's basis doesn't see sum exactly to what
the region still owes. I modelled this path on real Postgres 15 and 18:
121 deterministic scenarios (89 exact, 25 negative controls each removing
one rule and each wrong as intended, the rest out of scope or
characterizing a failure), every scenario of the interleaving experiment
that applies to an aggregate among them, and a soak of 800 runs, 4.7M
writes, 618k pages, 25k
recomputes, 754 source truncates and about 1,140 partition DDLs, every run
ending with 0 differences from a fresh `GROUP BY` and no deadlock on any
Trellis path. An independent review mutated the model; three mutations got
past every scenario, and the scenarios that now catch them are the R4, R5
and R6 rules below. The group-level rules are labelled R1 to R9 where they
are stated: R1 and R2 restate I1 and I2 per group, R3 is the image chain,
R4 restates I4, R7 is I5, and R9 is the truncate barrier.

### The admitted aggregates are the ones partials keep exact

- **Must:** a plain aggregate, or a JOIN transform
  ([ADR-0019](0019-join-transforms.md)), admits exactly these aggregates:
  - `COUNT(*)` and `COUNT(col)`;
  - `SUM(col)` and `AVG(col)`, where `col` is `int2`, `int4`, `int8` or
    `numeric`, with or without a declared scale (next section);
  - on a plain aggregate only, `MIN(col)` and `MAX(col)`, kept by recompute
    on retract under rules Rx1 to Rx8
    ([`MIN` and `MAX` recompute on retract](#min-and-max-recompute-on-retract)).
- **Must:** an argument of `COUNT`, `SUM` or `AVG` is a column or an
  expression over one row's columns in the transform grammar (`SUM(a + b)`,
  `COUNT(<expr>)`), as long as its result type is admitted. The group basis evaluates it on each
  change's old and new images, so its delta is `f(new) − f(old)`, exactly
  as for a column. On a JOIN an argument may use only one side's columns; an
  argument mixing both sides is refused at define.
- **Must:** a field may compose admitted aggregates (`SUM(a) + COUNT(b)`,
  `COALESCE(SUM(a), 0)`). It is computed from the group's partials whenever
  the group's row is written. Neither expression arguments nor composed
  fields change lock order or apply order.
- **Must:** result types match Postgres: `count` is `bigint`,
  `sum(int2|int4)` is `bigint`, `sum(int8|numeric)` is `numeric`, and `avg`
  is `numeric`.
- **Must:** every admitted aggregate but `MIN` and `MAX` is a function of
  per-group partials: the row count, each argument's non-null count, and
  each `SUM` or `AVG` argument's sum, kept as `numeric` whatever the
  argument type so a sum (or, on a JOIN, a product of a sum and a count)
  never overflows where Postgres's own `sum()` wouldn't; a `numeric`
  argument adds counts of its scales and special values (next section).
- **Must:** every other aggregate is refused at define, named by family:
  `MIN`/`MAX` on a JOIN; `bool_and`/`bool_or` and `bit_and`/`bit_or`;
  `SUM`/`AVG` over a float, `interval` or any other type; and `array_agg`, `string_agg` and `jsonb_agg`. The
  [Supported sources and targets](../transforms.md#supported-sources-and-targets)
  table is the reference. Each can be added later, through hidden counters,
  a value-count table or a recompute, without changing what is admitted
  now.

Keeping the per-row ledger for the aggregates partials can't keep would ship
two aggregate engines with different concurrency rules, so `MIN` and `MAX`
keep the group basis and add to it.

### `MIN` and `MAX` recompute on retract

Adding a row to a group can only lower its minimum or raise its maximum,
which a delta handles. Removing the current extreme can't be undone from the
group's state: the next extreme isn't stored anywhere. So a group keeps each
extreme as a value and re-reads it from the source when a retraction reaches
it. I modelled this (R) and a hidden value-count table per (group, value)
(V) on Postgres 15 and 18: both were exact in all 276 scenarios, each of
ten necessity mutations broke at least one scenario, and both designs'
extremes matched a fresh `GROUP BY` in all 680 soak runs.

- **Must:** a `MIN` or `MAX` argument is a plain source column. An
  expression argument is refused at define: a probe reads an index endpoint,
  and Postgres matches an expression index only to the exact expression.
- **Must:** a type whose `=` admits several textually distinct values
  (a float, `interval`, unconstrained `numeric`) is admitted. The extreme is
  one of the equal values, as with Postgres's own `min()`, and `self_check`
  compares it by the type's `=`
  ([ADR-0020](0020-type-identity.md),
  [ADR-0013](0013-self-check-production-recompute-audit.md#comparison-semantics-exact-text-for-1-1-equal-values-for-aggregates-and-joins)).
- **Must:** a source that is another Trellis target is refused for
  `MIN`/`MAX`: the index the probe needs would be a long `CREATE INDEX` on a
  large target Trellis owns.
- **Must:** the probe reads one endpoint of a valid, non-partial btree whose
  leading columns are the `GROUP BY` columns in order, then the argument, in
  the argument's collation; define requires it. Without it a probe scans
  the group, exact but able to outrun `statement_timeout` on a large group,
  so an index dropped after define pauses the definition after a grace that
  lets a rebuild dropping the old index first finish. The
  [Supported sources and targets](../transforms.md#supported-sources-and-targets)
  table states the refusal, the grace and what a definition defined
  `WITHOUT SOURCE INDEX` may do.
- **Must (Rx1, hidden state):** a group keeps each extreme (one per distinct
  call). The group's extremes share a basis of their own, `xbasis`, and a
  stamp of their own, `xstamp`, beside the partials' basis and stamp
  ([layout](../staging-and-claiming/group-basis.md#where-a-groups-state-lives));
  one `xbasis` serves them all because a probe re-reads every extreme of the
  group. One basis for the partials and the extremes is wrong: a probe that
  stamped the group's basis would make the partials refuse the counts of a
  change the probe saw, and in my model that mutation left wrong counts.
- **Must (Rx2, I2 for extremes):** a (group, transaction) sum applies to the
  extremes only when its transaction isn't visible in `xbasis`, and to the
  partials only when it isn't visible in the basis, both above the truncate
  floor.
- **Must (Rx3, insertions):** an insertion moves an extreme by
  `least`/`greatest`, and is filtered by `xbasis` like any side. `least` is
  idempotent, but an insertion of a row since deleted, applied after the
  deletion's probe, would bring the deleted value back.
- **Must (Rx4, the trigger):** a group is probed when a retraction side
  reaches an extreme (at or below a minimum, at or above a maximum), or when
  its extreme is `NULL` and it has a non-`NULL` retraction: a retraction can
  arrive before the insertion it undoes.
- **Must (Rx5, the probe, I1 for extremes):** the probe runs in the page's
  transaction, after its lock statement and its apply, and one statement
  takes the snapshot and the newest segment, re-reads the group's extremes
  and writes the snapshot as `xbasis` and the segment as `xstamp`
  ([the probe](../staging-and-claiming/group-basis.md#a-page)). A probe read
  before the lock misses a change another page applied in between; in my
  model that mutation was wrong.
- **Must (Rx6, absolute writes):** a build, rebuild, recompute, release and
  partition correction write the extremes, `xbasis` and `xstamp` from the
  snapshot they write the partials from (R1, R5).
- **Must (Rx7, the GC):** R4 holds for both stamps: a zero group is
  collected only once the drained prefix passes its stamp and its `xstamp`.
  A probe's snapshot can have seen an insertion whose page hasn't drained.
- **Must (Rx8, truncate):** a truncate's wipe of the group state removes the
  extremes with the partials.
- **Must:** a `MIN` or `MAX` on a JOIN stays refused
  ([ADR-0019](0019-join-transforms.md)).

The drain role reads the source on the hot path, so it needs `SELECT` on
the source, and row-level security must not apply to it.

Costs accepted with R: one index per `MIN`/`MAX` argument; and on a source
under retention deletes, vacuum must keep up, since every retracting probe
otherwise walks the dead index entries at the extreme's end. I measured 7–9
ms per probe after 111k deletes, repeated on every probe while an older
snapshot is open.

### `numeric` sums count their scales and special values

Postgres shows `sum(numeric)` at the largest display scale among the
group's current finite values: `sum({1.5, 2.000})` is `3.500`, and `1.5`
once `2.000` leaves. A running sum keeps the widest scale it ever held, and
`avg(numeric)` divides the sum with a result scale taken from the sum's, so
a leftover scale changes the average's value, not only its text (I ran it on
Postgres 17 and 18). `NaN`, and `Infinity` and `-Infinity` together, make
`sum()` and `avg()` `NaN`; one infinity alone is that infinity. No running
sum can take a special value back out: `NaN − NaN` is `NaN`, `Infinity −
Infinity` is `NaN`, and so is `NaN × 0` in a JOIN product. Postgres's own
aggregate state counts `NaN`, `Infinity` and `-Infinity` apart for the same
reason.

- **Must:** a `SUM` or `AVG` over a `numeric` argument (a column with or
  without a declared scale, a `numeric` column of a Trellis target, or an
  expression whose result is `numeric`) keeps, per group, the sum of its
  finite values and a count of its values per display scale and per special
  value, as partials every partial operation keeps; a group is zero only
  when they are all zero. Through a build and through pages alike, the sum
  and the average then carry exactly the scale and value Postgres would show.
- **Must:** every `numeric` argument has this one layout; only its reference
  scale (the scale its column declares) differs. Values finite at the
  reference scale aren't counted, so a `numeric(p,s)` argument's counts stay
  empty while its values are finite
  ([layout](../staging-and-claiming/group-basis.md#numeric-sums)).
- **Must:** a retype of an argument column after define goes through the
  capture pass's re-validation like any other. A `numeric(p,s)` column
  re-typed to an unconstrained `numeric` keeps every value's text, and its
  stored maps keep their reference scale, so nothing pauses or rebuilds:
  values at other scales are counted from then on. A change between two
  declared scales re-renders values, so its readers rebuild.

### Apply sums sides per group and transaction

- **Must (R1, I1 per group):** a page locks every group it touches in one
  sorted statement, then reads each group's basis in a later statement.
- **Must (R2, I2 per group):** a page splits each change into *sides*: the
  old image's contribution, negated, against the old image's group, and the
  new image's against the new image's group. It sums the sides per (group,
  `row_txid`) and applies a sum only when its transaction is not visible in
  that group's basis and its position is above the truncate floor. A group
  move is therefore checked against each group's own basis.
- **Must:** sides are never folded per key. A fold (first old image, last
  new image) is wrong when a group's basis falls between two of a key's
  changes, since the fold's old side carries the refused change's
  transaction and its new side the applied one's, and wrong for a key that
  passes through three groups, since it names only the first and last. In
  my model, folding left wrong groups in every soak run. Summing per
  (group, transaction) costs about 30% more statement time than the fold on
  a 100k-change page where every key changed twice.
- **Must:** the page returns, per group, the change it actually applied, for
  the derived structures of a JOIN transform
  ([ADR-0019](0019-join-transforms.md)).

### Absolute group writes lock first and cover every group they could owe

The producers of absolute group writes are a build or rebuild, a
quarantine release that recomputes its groups, a partition correction
([ADR-0017](0017-partitioned-sources.md)) and the recompute of a group an
image-less change names.

- **Must (R1):** an absolute write locks its groups in a sorted statement
  first; the snapshot, the source `GROUP BY` over those groups, the new
  partials, the basis and the stamp are then one later statement
  ([ADR-0028](0028-locking-and-writing.md), I1). A fresh build's insert of a
  group that has no row is I1's one exception for a group.
- **Must (R5):** the stamp's segment is read in the same statement as the
  snapshot, or after it, never before.
- **Must (R6):** a recompute is never a single `UPDATE … FROM (GROUP BY)`
  with no prior lock statement.
- **Must (R8):** each producer states the groups it covers, and covers every
  group it could owe: the source's groups, plus the groups named by the old
  images of ring changes not yet drained, plus the target's own groups. A
  build that enumerated only the source's groups missed a group whose only
  pre-existing row moved out before the enumeration, so the move's old side
  later subtracted a row nothing had counted; in my model that naive
  enumeration was wrong in 39 of 50 soak runs. A partition correction is a
  recompute of the groups routed to the partition, never a blind zero:
  `TRUNCATE` of a partition and an `INSERT` into it in one transaction
  leave a row that a zero would erase and the basis would then refuse.
  Which groups an absolute write must cover is where a new producer's bugs
  would appear, so each producer's enumeration is stated and tested on its
  own ([per producer](../staging-and-claiming/group-basis.md#absolute-writes)).
- A recompute holds its groups' locks for its whole `GROUP BY`: 1.3 ms for
  a 10k-row group, 9 ms at 100k and about 80 ms at 890k rows, with an index
  whose leading column is the group key. A page of 5,000 changes to one
  group holds the same lock for 18–28 ms.

### Readers see a group only while its row count is above 0

Pages apply in parallel and commit out of order, so between them a group's
partials can describe no real data: a negative count, `count(x) >
count(*)`, a sum no source state could produce. The converged state is
exact.

- **Must:** the partials are hidden from readers. A group's row is visible
  in the target only while its row count is above 0, and is written in the
  same transaction as the partials. Nothing else filters, orders or checks
  what readers see.
- **Must:** the target is a plain table in the schema the user named,
  holding exactly the declared columns and the `UNIQUE NULLS NOT DISTINCT`
  group identity, with no partial columns, so users can query, join and
  index it, and a chained reader finds its key through the constraint. The
  partials, basis and stamp of every group, zero groups and transiently
  negative ones included, live in a hidden table readers get no grant on
  ([where it lives](../staging-and-claiming/group-basis.md#where-a-groups-state-lives)).
- **The contract:** before `await_converged`, aggregate values may be
  inconsistent; after it they are exact. Groups of many rows (thousands or
  more) read as slightly behind or ahead by the changes in flight, with no
  negative count and no vanished group. Small or hot groups can briefly show
  combinations no source state could produce: a negative `COUNT(x)`, `COUNT(x)`
  above `COUNT(*)`, an `AVG` far off, a group missing for a moment. In my
  model, draining every batch up to a cut and none above it left exactly the
  state at the cut, and 5.0% of mid-drain reads of the raw partials showed a
  group with a negative or hidden count.

Writing the visible row only from partials that could be real (counts
non-negative, `0 <= count(x) <= count(*)`) was rejected for 0.1.0. It costs
a few comparisons per group per page against 25–35 µs of apply CPU per row,
and it doesn't touch builds or rebuilds, but it changes nothing for the
large-group workloads 0.1.0 targets: at 2B rows over ~25k groups a group holds
~80k rows, so an impossible value needs more than 80k in-flight deletes in
one group, and a mid-run value is off only by the changes in flight. The
anomalies sit on small or hot groups, where the check can't catch a wrong sum
(the column's domain is unknown) and an `AVG` over a transiently small count
can still be far off. Applying each group's changes strictly in order was
rejected because it serializes hot groups, giving up the out-of-order
parallelism the group basis relies on. Readers that need consistency use
`await_converged`; stronger consistency as an opt-in is
[#1185](https://github.com/salesforce-misc/trellis/issues/1185).

### Apply is one set-based statement per batch per target, and the batch is bounded

- **Should:** with `row_txid` exact, I2's skip rule is one predicate over the
  batch joined to the ordering state, so a batch's Apply on a target is one
  sorted lock statement and one statement that writes
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-three-phases)).
- **Must (fold):** for a 1-1 target, an image-less `op = 'delete'` is the
  key's final state within a fold window, whatever precedes it. *Evidence:*
  in a 1M-row build dry run, a delete sharing a fold window with an earlier
  write of the same key was dropped by the "latest row with any image" rule,
  leaving 11–12 groups wrong. An aggregate target doesn't fold per key
  ([Apply sums sides](#apply-sums-sides-per-group-and-transaction)).
- **Must (fold):** the fold statement never plans a nested loop over stale
  ring statistics.
- **Must (I8):** a batch folds, re-reads and applies at most a fixed number
  of changes (on the order of 10^5) and pages through a larger bucket in
  key-range pages, a key never split across pages. A segment's pages
  become visible to the drainers in one step, never page by page
  ([stage 04](../staging-and-claiming/04-claiming-and-the-fold.md#paging-a-share-larger-than-the-cap)).
- **Must (I7):** every lock statement in Apply runs under `lock_timeout`; a
  timeout releases the claim's transaction and retries the page after a
  backoff, outside any transaction.

## A build is Re-derive over chunks, and applies from its first chunk

**Decision point.** A new or rebuilt definition is not built by one
set-based pass with its changes withheld and recovered afterwards. It is
built by the same operation apply uses, over chunks of the source, while
applying: Re-derive per row for a 1-1 target, and one absolute write per
structure for an aggregate
([An aggregate's build](#an-aggregates-build-is-one-absolute-write-per-structure)).
The steps are in [builds](../staging-and-claiming/builds.md).

- **Must:** the definition is **applying from the commit that starts its
  build**; registration does no more than register. Nothing is skipped, so
  there is no go-live re-read, no orphan sweep and no catch-up. A change
  committed before the start that drains after it is in a batch at or below
  the start's segment, whose keys a 1-1 page re-derives rather than applies.
  *Evidence:* in four 10M-row runs of a chunked-build prototype under 2,000
  writes/s the oracle matched, with 196–361k changes correctly skipped by
  chunk bases and 0 in-progress ids.
- **Must:** a chunk is one short transaction. One statement takes the
  snapshot, reads the range and inserts, in key order, the entry and target
  row of every key that has none, written from that read. Then the chunk
  locks the other keys' entries in key order, and one more statement takes a
  snapshot, reads them and replaces their entries and rows. The insert's
  snapshot is taken before its uniqueness check, so the insert relies on "no
  entry" meaning "no Apply since the snapshot". The tombstone GC is the one
  thing that removes an entry an Apply wrote, and it skips a definition under
  a build (I4); a page's deleted placeholder holds no applied change (I1).
- **Must:** a source that is another definition's target takes a per-build
  fence. A seam writer decides inside its own apply transaction whether a
  definition applies, so one that decided before the build's start can
  commit after a chunk's snapshot, and neither the chunk nor the ring carries
  its write. The build plans no chunk, and a group-basis or `JOIN` build
  writes nothing, until every transaction older than the fence has ended;
  `status` names the fence. The seam stays, so this fence stays. *Evidence:*
  a test freezes an upstream writer after its reader check and starts the
  downstream: without the fence the downstream misses the writer's row.
- **Every definition has a build.** Define refuses a definition whose fields
  inline into more than a build can render (a chain of fields that each read
  the previous one more than once).
- **Must (I7):** chunk transactions are short (one range, under
  `lock_timeout`), and a captured-change batch never waits inside its
  transaction for a chunk's lock.
- **Must (backpressure):** build work yields to sealing and to the drain of
  captured changes, and the sealer is never refused for the duration of a
  build. *Evidence:* in my 10M chunked-build runs without backpressure the
  seal was refused for the whole build, with a tail of 90–213 s after `live`;
  in the 100M run a 6.09M-row segment sat undrained.
- **Must (acceptance):** per-row build cost is independent of table size;
  a build of N rows runs in bounded memory and bounded WAL retention, in
  time linear in N, and converges to the oracle at 100M under load.
  *Evidence:* per-chunk cost grew from 3.2 to 5.7 worker-seconds per 10k
  rows between 1M and 10M, and the chunk rate fell from 0.96 to 0.4/s at
  100M.
- **Should:** the peak `xmin` hold during a 1-1 build is one chunk's
  duration, not the build's (270–658 s at 10M in the prototype).
- **Must:** chunks are a durable, claimable work queue on drain threads, not
  an in-call loop. A crashed worker's chunk is re-run, a failing chunk
  reports its error on `Trellis::status` and never starves other work, no
  stale chunk write lands after a resume, and no definition is ever
  `backfilling` without a chunk row driving it forward.
- **Cost to state.** At 10M with the prototype's shape (group locks inside
  chunks), define-to-live was 2.0–3.2x the one-pass build's and WAL 3.7–5.1x.
  The implementation's 10M build converges in 65 s against the old path's
  791 s; the acceptance bullet above stays the bar.

| Build at 10M rows, 8 workers, 2,000 writes/s, disk | define → `live` | WAL | oracle | tail after `live` |
|---|---|---|---|---|
| one-pass `GROUP BY` + go-live re-read | 226 s | 5.8 GB | ok | 153 s |
| prototype, 10k-row chunks | 672–716 s | 29.3–29.8 GB | ok | 90–213 s |
| prototype, 100k-row chunks | 450 s | 21.7 GB | ok | 61 s |
| one-pass at 100M | 2,305–2,322 s | – | none: OOM in the drain | – |
| prototype at 100M | never: 4,131 of 10,001 chunks in 2 h 8 min | 190 GB retained | none: stopped by the disk guard | – |

### An aggregate's build is one absolute write per structure

An aggregate's build keeps no per-row entries and needs no delta table,
merger or chunk. A fresh build, a resume's, and a rebuild in place of a
`live` definition are **one absolute write per structure**: one per source,
or one per leaf of a partitioned source. It is the ordinary group recompute
applied to all of a structure's groups in one `READ COMMITTED` transaction,
with pages applying from the build's first moment
([builds](../staging-and-claiming/builds.md#an-aggregates-or-a-joins-build)).

- **Must:** a fresh build's or a resume's first statement takes the
  snapshot, enumerates the groups (R8), and in key order inserts each group
  that has no hidden row from that read and locks each group that has one.
  That pass is the write's lock statement (R6, R7), and the insert from its
  own read is [ADR-0028](0028-locking-and-writing.md)'s exception to I1. The
  groups that had a row are rewritten by a later statement from a new
  snapshot (R1, R5), never the first: a page can have applied to such a group
  after it.
- **Must:** the zero-group GC collects nothing of a definition under a build,
  from before the write's first snapshot to its commit. The insert relies on
  "no row" meaning "no page has applied to the group".
- **Must:** the write touches only the groups it enumerated and locked. A
  group first seen after the enumeration belongs to its pages, which apply
  each of its changes once. Pages apply from the build's first moment and
  wait only on the groups the build holds.
- **Must:** a source truncate has no build state to reset. A truncate's page
  wipes a build that committed before it, and the barrier (R9) re-applies
  what follows.
- **Must:** a rebuild over a target that already has groups recomputes
  every group in place, without pausing the definition: it locks the groups
  in key order in a statement of its own (R6), then **replaces**, not adds
  to, each group's partials and visible row from one snapshot. It covers the
  enumeration union (R8); a group with no source rows goes to zero and is
  collected under R4. Readers see the old values until a structure's
  statement commits, then the new ones: the target never empties and no
  group shows a partial build. Clearing the target and building fresh would
  show readers an empty, then filling target, and a shadow build and swap
  would make pages write to both states; both are rejected.
- **Should:** the build's `xmin` hold is its transaction: the whole source
  when unpartitioned, one leaf at a time when partitioned.
- **Accepted cost.** On an unpartitioned source the build's lock window,
  `xmin` hold and ring-retirement stall grow with the source, and the
  source's drain waits on the groups the build holds. On a `JOIN` over an
  unpartitioned 100M-row fact table, the engine's fresh build held `xmin` for
  23.0 s grouped by a dimension attribute, and for 932 s with 15.9 GB of WAL
  grouped by day and attribute, against 84 s in my `JOIN` model. A
  partitioned source is bounded per leaf: in my group-basis model the peak
  `xmin` hold was 0.3–0.5 s per partition for a 100M-row source in 32
  partitions.

*Evidence:* the build-path model (PostgreSQL 15 and 18) was exact in every
deterministic scenario and soak run with pages applying throughout, and each
rule above was necessary: moving the snapshot before the locks, leaving a
`NULL`-key group unlocked, or writing groups outside the locked set each
failed a scenario. The fresh build's insert from its read was exact in my
model and in a prototype on the engine, per leaf as well; I measured a
10M-group fresh build at 82 s and 5.9 GB of WAL against 1,535 s and 103 GB
for a lock statement that creates zero rows and an update of every one, and
91 s against 144 s over 32 leaves. The group-basis build of a 10M-row source
took 0.28 s and 11 MB of WAL with one `GROUP BY` per partition, against
26–32 s and 5.0 GB for the per-row ledger's chunked build, and 1.4–1.9 s at
100M against 370–380 s.

## Convergence and status

- **Should:** the watermark token is `pg_current_wal_insert_lsn()` read after
  the caller's commit, and a ring row's `origin_lsn` is the trigger's
  pre-commit insert position
  ([stage 07](../staging-and-claiming/07-convergence-and-await.md#watermark-tokens)).
- **Must:** three stored statuses: `waiting_to_backfill` (registered, chunk
  plan not yet written; normally momentary), `backfilling` (applying, chunks
  outstanding), `live`. Plus `paused` and `quarantined` from
  [ADR-0014](0014-pause-and-drop-a-transform.md) and
  [ADR-0003](0003-quarantine-storage-and-api.md).
- **Must:** a `live` reader of an upstream (another definition's target it
  reads, directly or as a JOIN side) that is not `live` reports the
  upstream's state, transitively, computed when status is read and never
  stored.
- **Must (I4):** tombstone GC by a batch watermark: a tombstone is collected
  only once the **contiguous drained prefix**, not the highest drained
  segment, passes the segment it was last stamped with, and never while its
  definition is under a build
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger)).
- **Must (R4, I4 per group):** a group whose partials are all zero and that
  has a basis stays until the contiguous drained prefix passes its stamp,
  and its `xstamp` too (Rx7). Only an absolute write sets the stamp: the
  newest segment holding a change its snapshot saw (R5). A zero group with
  no basis may go at once, except under a build; a group with any non-zero
  partial is never collected, whatever its row count. The GC never waits on
  a page or a recompute
  ([the group basis](../staging-and-claiming/group-basis.md#zero-groups-r4-rx7)).
  *Evidence:* in my group-basis model a GC that tested the row count alone
  was wrong in a deterministic scenario, and in my JOIN model 4,771 zero
  groups left on 10 hot keys made pages 16x slower until collected.
- **Every repair of a definition is a rebuild**, safe against any pending
  change by I2: a resume, `request_backfill` and a capture re-install each
  move the definition to `backfilling` in the call's own transaction, so a
  repair a caller waits on is visible in the status, never parked where only
  the repair knows it hasn't run. The one exception is a quarantine release
  (`release_key`): a per-key Re-derive on a 1-1 target, and a recompute of
  the key's groups on an aggregate.
- **Must (a release on an aggregate):** while a key is held its changes are
  skipped for the aggregate, so the groups it feeds drift until the release;
  `await_converged` and `self_check` hold back on a held key with parked
  changes. A release, and the
  [automatic retry](0003-quarantine-storage-and-api.md#retry-policy) of a
  held key, recompute every group the key could owe (R8), as an absolute
  write under R1, R5 and R6; the key's changes still in the ring apply as
  deltas afterwards. The parked changes are discarded, not replayed, so the
  recompute is exact whatever other absolute writes reached those groups
  during the hold. On a JOIN side the release recomputes the side's `P`/`M`
  cells and carries the change to `T` under the `J` lock (JR1–JR3 in
  [ADR-0019](0019-join-transforms.md)). A resume releases every held key and
  its rebuild covers these groups
  ([ADR-0003](0003-quarantine-storage-and-api.md#resume-releases-the-transforms-held-keys)).

### A change Trellis never saw stays wrong until it is detected and repaired

A per-row ledger repaired a missed change (capture switched off around a
write, a partial restore) the next time the row was written, because its
Apply compared the new image with the entry. A group-basis Apply computes
each delta from the change's own old image, so a group that missed a change
stays wrong until something recomputes it.

- **Must:** nothing repairs an aggregate on its own: no background sweep and
  no automatic recompute. `self_check` detects the drift
  ([ADR-0013](0013-self-check-production-recompute-audit.md)), and the
  operator repairs it with `request_backfill` or `PAUSE`/`RESUME`, both
  rebuilds.

A rolling background recompute would put a continuous source `GROUP BY`
load on the database, hold group locks that compete with pages, need a
cycle-length setting, and silently mask Trellis's own bugs. A targeted
repair of only the groups `self_check` names is additive, on top of the
locked group recompute, so it can come later.

### What `live` promises

`live` tells an operator that a
transform is in its **steady state**. Once a definition reports `live`, a
watermark token taken after any commit and awaited with
`Trellis::await_converged` guarantees that the transform's values reflect
every commit at or before that token. Nothing else is needed.

For that to hold, `live` is strict: it is reported only once every chunk has
committed. Every captured change
since registration has been applied (the definition applied from its first
chunk), and every commit before registration was read by a chunk under a
snapshot later than the join fence, so nothing is outstanding.

The two signals stay separate on purpose. `await_converged` is a pure LSN
wait over captured changes. It never reads definition status, so it can
return while a definition that isn't `live` yet is still building. Backfill
is an operator concern, reported by `Trellis::status`. A reader that needs a
settled target checks both: `live` from status, then its token. Because a
rebuild is a status transition made in the repairing call's own transaction,
"poll `live`, then take a token" also covers a repair made before the poll.
`self_check` reads the status too: it compares only a `live` definition, and
reports any other as `NotLive` with its status, instead of comparing a target
that is still changing.

Before the token is reached, an aggregate target makes no promise about the
combination of values a reader sees
([Readers see a group only while its row count is above 0](#readers-see-a-group-only-while-its-row-count-is-above-0)).

## Truncate, DDL, drop

- **Must (R9, the barrier holds on both sides):** a source `TRUNCATE` stages
  one truncate row, and its page waits for every earlier page of its table
  and every later page waits for it. It passes the same claim check as any
  page. Sealed segments are planned strictly in order, or the barrier is
  evaluated when a page is claimed, not when it is planned. *Evidence:* in my
  group-basis model the floor alone let a page that had locked its groups
  before the wipe, and read the floor before it was raised, re-create counts
  the wipe had removed; a planner that could plan a segment before its
  predecessor lost rows across a truncate in 7 of 220 runs with the race
  widened, and 0 of 280 once it planned in order.
- **A source truncate resets the target and raises a floor**, an O(1)
  write rather than a tombstone per entry or group. Its page empties the
  target's ordering state, clears the target and raises the target's
  truncate floor to the truncate's ring position; I2 then refuses any change
  at or below the floor, a second line of defence behind the barrier
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#aggregate-groups-the-group-basis)).
  A truncate of a leaf or middle of a partitioned source resets nothing: it
  queues that node's correction
  ([ADR-0017](0017-partitioned-sources.md#a-leaf-or-middle-truncate-queues-that-nodes-correction)).
- Source `ALTER TABLE` on a read column follows the schema-changed marker
  and regeneration rule; on any other column it is invisible. Dropping the
  last definition on a table drops its triggers under I6's retry.

## Multi-hop: a target's writes reach its readers through the mutation seam

A target is never captured by triggers. A definition that reads another
definition's target (a chained hop) learns of its changes from rows the
writer stages in the same transaction as the write, in the ring row shape the
triggers write, so intake and the drain are shared. A worker propagating
downstream appends to the active segment, never the batch it is draining.

**Decision point.** The seam's image feed is how a target is a change
source. An image-less `Recompute` of a key tells a group-basis reader the
row's new group but not the group it left, so whenever the reader's group
depends on the upstream row's values the old group keeps a member it lost.
A group-basis or JOIN reader needs exact images, as capture gives a source.

- **Must:** every target with at least one applying aggregate or JOIN
  reader stages, per changed key, the exact old and new images, ordered by
  the write token, with the writer's transaction as `row_txid`. An upstream
  group whose row count is not above 0 has no visible row, so it appears in
  the images as absent, and the reader's sums telescope. The image chain (I3)
  holds because the upstream writes a row only under its lock, in
  write-token order.
- **Must (one feed per target):** if any reader needs images, the target
  stages images, and its 1-1 readers consume the same rows. A target whose
  readers are all 1-1 may stage key-only `Recompute` rows. A target with no
  readers stages nothing.
- **Must:** a write that never reaches the seam rebuilds its readers, which
  read `backfilling` until the rebuild is done, so `live` holds along a
  chain. The transaction that completes a target's build also retries the
  keys its direct readers hold
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md#downstream-propagation-and-why-it-terminates)).
- **Must:** a writer that resolved the target's feed before a new reader's
  define committed must not leave the reader's build missing its write
  ([the build's fence](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
- **The chains this serves:** 1-1 over a 1-1 or aggregate target; an
  aggregate over a 1-1 target (the computed-measure pattern: a 1-1 transform
  computes what an aggregate argument can't express, and an aggregate then
  sums it); an aggregate over an aggregate target (a rollup); and a JOIN side
  that is a target.
- **Cost to state.** A target with an aggregate or JOIN reader pays a
  re-read by key per changed key and wider staged rows. A terminal target
  pays nothing. *Evidence:* in my group-basis model, a downstream grouped by
  an upstream value was wrong under key-only recomputes and exact under
  images, with upstream pages drained out of order and a downstream
  recompute between them.

Refusing aggregates and JOIN sides over a target would lose the
computed-measure pattern, and a rollup kept by recomputing its groups from
the upstream would be a narrow special case with refusal rules of its own.
A trigger on a target would add per-row cost to every target write and
re-derive what the writer, Trellis being a target's only writer, already has
in hand, including `hop_gen` and cycle detection, which stay in process;
and cascading truncates on a captured target would need handling of their
own.

## Rejected alternatives

Each alternative below is recorded with the number that rejected it.

| Alternative | Why not |
|---|---|
| **Capture by logical replication** (a `pgoutput` slot decoded by an intake thread). It keeps everything off the write path and needs no trigger on user tables. | I measured staging capped at ~120k rows/s at 1,000 rows/commit and 18.6k at 1 row/commit whatever the writers do, with a 10M-row `COPY` never staged, and total CPU per row 4–5x the trigger's, paid later. A stopped Trellis pins the whole database's WAL rather than growing a ring at ~188 B/row. The decoder's 32-bit xid must be widened to `xid8` for I2, and I found the naive widening silently wrong for every id before an epoch boundary. Decoding needs `REPLICA IDENTITY FULL` on every source an aggregate reads, for its old images. Slot loss, failover and the replication privilege are operational surface the trigger has none of. |
| **Row-level triggers.** | I measured 5–10x the statement trigger's cost above 1 row/commit. |
| **Column-agnostic image encodings** (`to_jsonb`, `jsonb_each_text`, `::text`, `hstore(NEW)`). | Not byte-identical to `tuple_to_json`, or identical but reading every column, which I measured costing 40x on a 100 KB column. |
| **Ordering by LSN with live reads guarded by horizons and basis checks:** the recompute horizon and extinct horizon, a minimum-image LSN, the build horizon, per-key basis locks, the watermark wait before enumeration, recompute and vanished-image flags, bucketed advisory locks, a keep-every-image fold, prior image on enumeration. | Each closes one pairing of one producer against one drain order, found in review of the previous fix at roughly two new problems per fix, and the generative suite found none of them. A live read has no way to tell which commits it reflects; only a snapshot does (I2). |
| **Visibility-only I2** on the per-row ledger, and **Apply stamping its own snapshot as the basis.** | Each fails scenarios of the interleaving experiment: visibility-only fails three, and stamping Apply's own snapshot fails two. A group basis needs no position check, because its writes to a group commute. |
| **Group rows as absolute writes ordered by position:** forced recomputes from a live `GROUP BY`, an existence probe and pre-lock before incrementing. | Not orderable without a horizon; the probe's scan is the 40k-group cliff where nothing drains; the pre-lock deadlocks on new groups. The group basis orders the same absolute writes by a snapshot per group, taken in the statement that reads, after a sorted lock. |
| **A per-row ledger for aggregates**: one entry per source row holding its group, contribution and ordering state. | I measured 15.1–15.3 GB of ledger for a 100M-row source in 50k groups (151–153 B per entry, its basis 33–39 B), against 5 MB of group state; a 100M build at 370–380 s and 58–71 GB of WAL against 1.4–1.9 s; a page of 100k changes at 3.2–3.35 s against 0.18–0.31 s; and a partition `DROP` of 10M rows at 45–51 s and 6.2 GB of WAL against 10–30 ms. What the ledger gave in return was self-repair on a row's next write, and `MIN`/`MAX` without a source read. |
| **A hidden value-count table for `MIN`/`MAX`** (V): one row per (group, value) with its count, the extreme the first or last key of the group's slice of an index. | Exact in my model, like R, but its state is one row per distinct (group, value): about one per source row for a continuous argument such as an amount or a timestamp. A required source index costs less. It stays the route for `MIN`/`MAX` on a JOIN and for `COUNT(DISTINCT)`. |
| **Choosing R or V per argument by its cardinality** (a define-time estimate from `pg_stats`). | The estimate drifts after define, and switching means rebuilding the field; R needs none of what V would add. |
| **Deferred probes**: marking a group and re-reading its extreme after the page commits. | Unmodelled, a second producer of absolute writes, and a window in which readers see a deleted extreme. |
| **One set-based `GROUP BY` build with changes withheld, then a go-live re-read and orphan sweep.** | The re-read is a full enumeration per build that scales with table size, not with what changed; I ran it at 100M, where it reaches `live` after 2,305 s and its drain then runs out of memory. Nothing can tell a table unchanged since the build from one changed and changed back, so the re-read could never be skipped. A 1-1 build is per-row work and its acceptance bar is linear time; an aggregate's build keeps the one-pass `GROUP BY` and drops the withholding, because its absolute write applies alongside pages from its first moment. |
| **`catching_up` as a stored status with marker fences, generations and go-live catch-ups.** | Needed only by a build that skips changes. Every build applies from its first chunk, so there is nothing to catch up; `catching_up` is the reported state of a reader whose upstream isn't `live` ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)). |
| **A chunked aggregate build under a chunk gate:** primary-key-range chunks (a hybrid above a size threshold), or key-range chunks, each adding its region's partials under the group locks, with a gate in every page path (a page locks the chunk rows its sides fall in, before the page row; a chunk that hasn't run skips the side, a done chunk refuses what its snapshot saw, a retired one applies it), retirement once the drained prefix passes the chunk's stamp, a recompute that counts only done or retired chunks, a `NULL`-key chunk, and a truncate page that resets the chunks. | It is the only option that bounds a fresh build on a large unpartitioned source, but a chunk is paid per (chunk, cell) pair: about 5–6x the build time and 12x the WAL on many-cell shapes, against one `GROUP BY` per structure at 2–12x less in my `JOIN` model (84 s against 412–509 s; 213 s against 1,258 s), and it leaves in-place rebuilds unbounded. Key-range chunks bound locks and rows but reread the heap about 2x per chunk, so not I/O. The gate adds a lock to every page and machinery to every truncate and recompute. A build is one absolute write per structure instead, and a large source is partitioned ([#1276](https://github.com/salesforce-misc/trellis/issues/1276) keeps the bounded alternatives open). |
| **Bounding drain memory by running fewer workers.** | Each worker then holds more buckets; I measured that it bounds nothing. |
| **Fixing the build's cost by chunk size.** | I ran 10x larger chunks and they left 2.0x the time and 3.7x the WAL. |
| **Resolve every image-less trigger into an image at staging time**, so Phase 2 never reads live state. | Under trigger capture the only image-less change is a delete, and every other image-less producer is an enumeration this ADR replaces with Re-derive or a recompute under lock. What this leaves untouched on a 1-1 target is out-of-order parallel batches for one key, which needs `applied_lsn`, not images. A resolver on the seal path is itself a live read racing the drains. |
| **Serialize absolute writes per target** through one writer or one advisory lock. | Does not fix identity: a serialized write still reads live state ahead of pending deltas and still needs a horizon. Serializes the build, the largest producer of absolute writes, which is already CPU-bound on eight workers at 14–15k rows/s. |
| **Thread source transaction ids through the ring.** | Adopted, in the exact form `row_txid` gives for free under trigger capture. |

## Consequences and costs

- **Capture is on the application's write path.** The cost is one ring
  append per statement and about 2.9x the source row's WAL bytes on disk.
  It is paid by the writer at commit, not by a background process later, so
  it is visible and it cannot fall behind: there is no capture lag, only
  drain lag. The write-heavy user gets a documented bulk-load pattern.
- **Trellis runs inside every writer's transaction.** Every future change to
  the ring's shape, the seal, or maintenance inherits I6: a lock the
  application can queue behind is a bug, and a condition that fails the
  application's statement is a bug. Both get a written rule and a test
  ([validation 6](#validation-and-acceptance)).
- **Storage grows with the source only for 1-1 targets.** A 1-1 target
  carries a slim ledger of one row per source row; an aggregate target
  carries state per group (5 MB for 50k groups, whatever the source's
  size). No full old row is ever staged (the ring carries the imaged columns
  only) and `REPLICA IDENTITY FULL` is not required anywhere.
- **An aggregate doesn't repair itself.** A change Trellis never saw leaves
  its groups wrong until `self_check` finds them and an operator rebuilds
  the definition, which at a billion rows is a full build
  ([A change Trellis never saw](#a-change-trellis-never-saw-stays-wrong-until-it-is-detected-and-repaired)).
- **Readers before convergence see a weaker picture than a per-row ledger
  gave.** The ledger counted each row at most once, so its counts were never
  negative; the group basis can show impossible combinations between pages
  ([Readers](#readers-see-a-group-only-while-its-row-count-is-above-0)).
- **The aggregate surface is narrow.** `COUNT`, `SUM`/`AVG` over
  integers and `numeric`, and on a plain aggregate `MIN`/`MAX` over a
  plain column with its index; every other aggregate is refused at define,
  and a definition using one doesn't work until its family is added.
- **`MIN`/`MAX` read the source on the hot path.** A page that retracts a
  group's extreme holds that group's lock across an index probe of the
  source, the drain role needs `SELECT` on the source without row-level
  security, each argument needs an index, and a source under retention
  deletes needs vacuum to keep up
  ([`MIN` and `MAX` recompute on retract](#min-and-max-recompute-on-retract)).
- **Each producer of an absolute group write owns its enumeration** (R8).
  That is where a new producer's bugs would appear, so each is stated and
  tested on its own.
- **The failure mode of a broken capture is loud, not silent.** A schema
  change to a read column never fails the statement (I6). A missing ring
  table or revoked privilege does, with a `CONTEXT` line naming the capture
  function:
  the application sees the fault at once, not after data is lost. An owner who disables, drops or recreates a
  plain table's trigger, or replaces its function, pauses the table's readers
  at the next pass; the silent-uncapture paths left are in
  [known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes).
- **Hosted compatibility** is core Postgres for everything required
  (statement triggers, transition tables, `SECURITY DEFINER`, the `TRIGGER`
  privilege); no event trigger is needed.
  No replication privilege, slot or publication is needed, and a
  physical backup-and-restore carries the ring with the data. A logical dump
  carries it too, and the restore discards it ([I2](#invariants)).
- **Drain backpressure becomes the operator's number.** Capture can outrun
  the drain by ~20x (1.9M rows/s against ~85k folded rows/s with the
  ledger), and I measured the ring in the application's database at ~188 B per
  captured row when nothing drains.
  The bound on that growth is designed but not built:
  - **Gauges** report the ring's rows and bytes, the age of its oldest
    undrained row, and its undrained bytes per source table.
  - **A soft bound, `ring_warn_bytes`, is on by default.** Above it,
    `self_check` and `status()` report a backlog warning, and the drain takes
    priority over optional work, as a build already yields to it
    ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
    Nothing is paused.
  - **A hard bound, `ring_max_bytes`, is opt-in and off by default.** At it,
    capture pauses for the definitions whose source tables hold the most
    undrained bytes, largest first, until the ring is back under the bound.
    Their pause kind is `backpressure`, their triggers are dropped under the
    `lock_timeout` fence (I6), and their ring rows are discarded. `status()`
    shows them paused with the reason, and `RESUME` rebuilds them from the
    source: they are stale until then, never silently wrong. No application
    writer is slowed or failed, and no captured row is skipped while capture
    is on.
  - **No bound applies while every Trellis process is down.** The capture
    triggers keep appending, and only the database's own disk alerts guard
    the disk. On restart, the bounds are checked first. Sizing the disk for
    an outage:
    [recommendations — Ring sizing](../recommendations.md#ring-sizing).
- **A 1-1 build is per-row work on every worker**, not one pass. The
  acceptance bar is linear time with a per-row cost independent of table
  size, checked at 100M ([validation 4](#validation-and-acceptance)). An
  aggregate's build is one `GROUP BY` per structure, and holds the `xmin`
  horizon for its transaction.
- **Registration latency does not depend on table size**, and no definition
  is `live` when registration returns: callers poll `Trellis::status` to
  `live`, then use a token
  ([embedding](../embedding.md#poll-to-live-dont-wait)).
- **I took every number above on one box.** The capture-throughput
  experiments and the first four ledger experiments ran on tmpfs; the
  disk-backed numbers are from a single NVMe with ~0.4 ms fsyncs and, for the
  ledger experiments' disk tier, a box that was not always quiet.
  Within-session ratios stand; absolute bars are restated in
  [validation 5](#validation-and-acceptance).

## Validation and acceptance

Nothing below polls for convergence.

1. **One deterministic interleaving test per scenario of the interleaving
   experiment** (all seventeen) on the real engine, hand-driven with advisory
   locks, plus one for each of the nine superseded problems in the inventory of
   producer pairings that motivated this design.
2. **One deterministic test per scenario of the group-basis model** that
   applies to the engine: the read-after-lock and stale-basis cases, the
   fold that straddles a basis, the image chains under nested writes and
   cascades, the enumeration of a build over vacated groups, zero groups
   and the GC racing a recompute, the truncate barrier's earlier side,
   a build's absolute write against pages before, during and after each of
   its steps, and the chained-source cases
   ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)).
   Each scenario the soak can't reach is the only evidence for its rule.
3. **The generative concurrent tier** with planted violations (skip the
   lock, compare LSN instead of visibility, delete tombstones early, fold
   sides per key, collect zero groups early), each caught within the
   nightly budget. **Merge gate.**
4. **`build-under-load` at 100M** on the implementation branch, disk-backed,
   under a 16 GB memory cap: oracle ok, memory bounded, WAL retention
   bounded, time linear in N, per-row cost flat from 1M to 100M.
   **Milestone exit for the build.**
5. **A quiet-box, disk-backed benchmark round** with a checkpoint inside the
   window and storage stated on every number: `fold-in-ratio`,
   `group-contention` (400/4k/40k), `throughput-ramp`, `build-under-load`,
   and the two capture benchmarks (statement against row triggers, and
   scaling with writers, including the cost of the transition image a
   re-read row carries), each with a same-session control on the
   pre-change code: the group basis against the per-row ledger. The bars in
   this ADR are restated from that round. **Milestone exit for the group
   basis.**
6. **Writer-coupling tests:** schema change of a read column under load
   (writes succeed, definition pauses, regeneration restores capture); join
   and drop under a 30 s open transaction (worst writer wait under 100 ms);
   revoked privilege and disabled trigger (audit reports); `REPEATABLE
   READ` and `SERIALIZABLE` writers straddling a seal (no lost rows).
7. **Drain paging:** a segment several times the batch cap drains with peak
   RSS bounded by the cap; a key whose changes straddle two segments, a
   paged truncate segment, and a reclaim between two pages all converge.

## Reusable material

Prototype branches on the fork (mmmries/trellis) hold the ledger Apply
experiments with their results and handoff notes, the chunked ledger build
with `build-under-load` (the disk columns and the memory sampler), a
trigger-capture variant of that build, and the trigger-capture spike (the
capture SQL with the `new_only` and `skip_noop` shapes, and the phase-1
harness). The fence mirror is already in `main`. The group-basis model is a
SQL and Python harness against real Postgres, with its scenarios, soak
matrix and negative controls, each re-runnable from one script.

## More information

- [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
  for the ADR form ([ADR-0001](0001-use-adr.md)).
