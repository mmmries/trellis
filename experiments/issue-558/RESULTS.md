# Issue #558 experiments

Scratch experiments for the design note in #558 (epic #556), run in the order the first
comment lists. Everything here runs against a throwaway Postgres 17 cluster (`cluster.sh`),
with no Trellis code. Reproduce with:

```
./cluster.sh init /home/mike/exp558/a && ./cluster.sh start /home/mike/exp558/a
./cluster.sh jump-epoch /home/mike/exp558/a 7 FFFF8000      # experiment 1a only
cargo build --release
./target/release/exp558 exp1-epoch
./target/release/exp558 exp1-snapshot --rows 1000 --think-ms 2 --secs 10
./target/release/exp558 exp2 --mode lsn --enumeration ledger+source
```

## Experiment 1: can the basis be exact?

### 1a. Widening a 32-bit decoder xid to xid8 across an epoch boundary — PASS

Method. `cluster.sh jump-epoch` uses `pg_resetwal -e 7 -x FFFF8000` (staged in two
sub-2^31 jumps with a `VACUUM FREEZE` of every database between them, so the wraparound
stop limit never trips) to put the cluster 32,768 transactions before the epoch 7→8
boundary. The driver then commits 32,805 single-statement transactions on one connection,
recording `pg_current_xact_id()` inside each as ground truth, and afterwards reads the
pgoutput slot with `pg_logical_slot_get_binary_changes(... 'proto_version','1' ...)` and
takes the 32-bit xid from each `B`(egin) message, the way intake would. It widens every
one against a single anchor read at staging time, after the boundary:

```
anchor = pg_snapshot_xmax(pg_current_snapshot())        -- xid8, no xid assigned
widen(x32) = anchor - ((low32(anchor) - x32) mod 2^32)
```

The staged id was assigned before the anchor's snapshot, so it lies in
`(anchor - 2^32, anchor]` and the candidate with matching low bits is unique.

Result.

| | |
|---|---|
| transactions committed | 32,805 (40 of them in epoch 8; first post-wrap xid8 is 8·2^32 + 3, as expected: 0, 1, 2 are skipped) |
| BEGIN messages decoded | 32,805 (after forcing a WAL flush; see the note below) |
| `widen()` exact against the recorded xid8 | 32,805 / 32,805 |
| naive widening (anchor's epoch ∥ x32) wrong | 32,765 / 32,805 (every pre-boundary id) |
| BEGIN payload xid vs the SRF's `xid` column | 0 mismatches |
| pre-boundary id visible in a post-boundary snapshot | yes; post-boundary id also visible; `pg_current_snapshot()` text keeps the full 64-bit values (`34359738411:34359738411:`) |

Two things worth carrying into the ADR:

- The anchor must be taken *after* the change is received, never cached from before.
  Anchoring by "current epoch" is wrong for every change staged across the boundary, and
  it is wrong silently (the widened id is 2^32 too high, so it is never visible in any
  basis and every such change is applied even when it should be skipped).
- The lag between a change's xid and the anchor must stay under 2^32 ids. Postgres stops
  accepting writes ~2^31 ids after the oldest unfrozen xid, so a ring that lags by that
  much cannot exist; no extra guard is needed.

(Harness note, not a design finding: logical decoding only reads *flushed* WAL, so with
`synchronous_commit=off` the slot stopped ~4k transactions short until the session forced
synchronous commits. Intake already consumes a streaming slot, which waits for the flush.)

### 1b. Snapshot bases under a 16-connection write load

Method. `rows_(id, v)` with N rows; 16 writers each loop `update rows_ set v = v+1`
on a random row and record `(pg_current_xact_id(), row)` in the same transaction;
optionally hold the transaction open for `think_ms` first. 4 samplers loop the
Re-derive shape: `select … from ledger where id=$1 for update`, then snapshot + read in
**one** statement (`insert into bases select $1, pg_current_snapshot(), …, r.v from rows_ r
where r.id=$1`). One sampler also runs the control: `pg_current_snapshot()` in one
statement, then again in the next.

Result (16 writers, 4 samplers, 10s per shape, fsync off, unix socket):

| rows | think time | commits/s | bases | xip non-empty | mean xip | max xip | bases with ≥1 in-flight change *for the locked row* | share of a row's later changes that were in flight at its basis |
|---|---|---|---|---|---|---|---|---|
| 100k | 0 | 387k | 230,318 | 94.2% | 2.5 | 19 | 0.002% | 0.0000% |
| 100k | 2 ms | 5.3k | 728,176 | 100% | 15.9 | 19 | 0.016% | 0.0000% |
| 1k | 0 | 395k | 270,207 | 94.3% | 2.4 | 19 | 0.070% | 0.0000% |
| 1k | 2 ms | 5.2k | 809,245 | 99.9% | 16.0 | 19 | 1.57% | 0.034% |
| 16 (hot) | 0 | 358k | 266,325 | 96.6% | 3.4 | 18 | 9.0% | 0.0001% |
| 16 (hot) | 2 ms | 3.2k | 768,708 | 100% | 16.1 | 19 | 59.8% | 0.050% |

Decidability: over ~1.1M (base, commit) pairs per shape, classifying each commit as below
xmin / in xip / between-not-in-xip / at-or-above xmax and comparing with
`pg_visible_in_snapshot`: **0 disagreements** on every shape. Basis exactness: for 200 bases
per shape, the value the sampler read equals the count of that row's commits the stored
snapshot calls visible: **0 disagreements**.

Reading the numbers against the note's falsification bar ("more than 5% of snapshots under
load would need the re-derive fallback"):

- The raw "in-progress list is non-empty" rate is 94–100% under any real load, so a design
  that *re-derives whenever xip is non-empty* would thrash. That is not the right test.
- The list is tiny (≤ 19 entries, bounded by the number of concurrent writers, 8 bytes each)
  and `pg_snapshot` stores it. With the list stored, an in-flight id is **decidable**: it is
  not visible, so the change is applied. There is no ambiguous case and no re-derive
  fallback at all. Scenario 10 in experiment 2 exercises exactly this.
- What actually matters for a row is how often one of *its own* later changes was in flight
  when its basis was taken. Uniform loads: well under 0.1% of bases. Only a 16-row hot set
  with 2 ms transactions pushes it to 60% of bases, and even there it is 0.05% of the
  changes those rows will ever check. Storing the list costs nothing there either.
- Note: `xip` only lists in-flight ids **below xmax**. An in-flight transaction whose id is
  the newest in the system sits at or above xmax and is simply "future"; it is decided as
  not visible the same way. (Scenario 10 had to insert a later commit to get the id into
  xip at all.)

**Two design corrections from 1b:**

1. `pg_current_snapshot()` **must be evaluated in the same statement as the read** (or the
   transaction must run REPEATABLE READ). In READ COMMITTED every statement takes its own
   snapshot; the control showed 99.7% of "snapshot in one statement, read in the next" pairs
   under load saw a different snapshot (41,478 of 41,746 on the hot shape). A basis taken by
   a separate `select pg_current_snapshot()` before the read is wrong, not merely loose.
2. The basis stores the whole `pg_snapshot` (xmin, xmax, xip), not just xmin/xmax. The
   "store only when non-empty" optimisation from the note is moot: it is almost always
   non-empty and always small.

**Experiment 1 verdict: not falsified.** Widening is exact across an epoch; visibility is
exact with the stored list; the in-progress case never needs a re-derive.

## Experiment 2: do the two operations survive the known interleavings?

Scratch tables (`src`, `parent`, `ledger`, `groups`), plpgsql implementations of the two
operations exactly as the note describes them (`src/exp2.sql`), separate connections for
the application, two drain workers and the driver, and advisory locks to freeze a step
between its read and its write. Every scenario asserts `groups` against a from-scratch
`GROUP BY` after the last step. The target is `sum(amt), count(*)` of `src` rows grouped by
the **name of the parent they reference**, so every scenario runs through a relationship.

Three readings of Apply's skip rule were run, because the note's text underdetermines it:

- `literal`: skip iff C is visible in the entry's basis. Apply never writes a basis.
- `literal-snap`: as literal, but Apply also stamps `basis := its own snapshot` (one reading
  of "the deleting commit's basis" in I4).
- `lsn`: literal, plus skip iff C's commit position ≤ the entry's `applied_lsn`, which Apply
  advances. Re-derive leaves `applied_lsn` alone.

And three enumerations for the reverse path (which children a to-side change re-derives):
`ledger` (join-key index only, as the note proposes), `ledger+source` (union with a live
scan of the from-side by join key), `ledger+deplock` (join-key index, but Apply/Re-derive
take a shared advisory lock on the parent they read live and the reverse path takes it
exclusively before enumerating).

Results (✓ = target equals the oracle and the expected skip/apply happened; ✗ = wrong value;
enumeration only matters for 6b):

| # | scenario | literal | literal-snap | lsn |
|---|---|---|---|---|
| 2 | #344 enumeration's read stalls, CDC for the same key drains (C committed before the enumeration's snapshot) | ✓ apply blocked on the I1 lock, then `skip:visible` | ✓ | ✓ |
| 2b | same, C committed after the enumeration's snapshot | ✓ blocked, then applied | ✓ | ✓ |
| 3 | #321 source commit lands before the group's re-derive, its CDC drains after | ✓ `skip:visible` | ✓ | ✓ |
| 4 | #389/#539 8 workers × 30 batches × 40 rows creating the same 20 groups; ledger entries locked in key order, one sorted increment | ✓ 0 deadlocks | ✓ | ✓ |
| 4c | control: per-row interleaved locking (ledger row, group, ledger row, group …) | 19–22 deadlocks in 20 s | same | same |
| 5 | #494 a→z→b folded to one record while z's members are being re-derived (held after read) | ✓ | ✓ | ✓ |
| 5b | #494 a→z and z→b in two batches drained **out of order** | **✗** r ends in z: `z=60(2), b=60(1)` vs oracle `z=50(1), b=70(2)` | ✓ | ✓ `skip:lsn` |
| 6 | #516 parent rename with a from-side update for a child pending | ✓ reverse re-derives the child; the pending apply is `skip:visible` | ✓ | ✓ |
| 6b | #528 child moving **into** the parent is mid-Apply (to-side read done, ledger write uncommitted) when the rename commits and its reverse path enumerates | **✗ with `ledger`**: `one=20(1), uno=10(1)` vs oracle `uno=30(2)` — the child keeps the old name. ✓ with `ledger+source` and with `ledger+deplock` (both wait for the move) | same | same |
| 6c | #520 to-side TRUNCATE with a from-side update pending | ✓ children re-derived to no group; pending apply `skip:visible` | ✓ | ✓ |
| 7 | #549 forced recompute of one child joins the live to-side, then the rename's reverse record drains | ✓ `rederived 1, skipped 1` (the recomputed child's basis sees the rename) | ✓ | ✓ |
| 8 | #531 rename's reverse record lost with a dropped slot; older from-side CDC pending; catch-up re-derives the key space | ✓ pending apply `skip:visible` | ✓ | ✓ |
| 8b | #529 parent **deleted**, reached through the ledger's join-key index | ✓ | ✓ | ✓ |
| 9 | tombstone: delete drains, then an older update for the same key | **✗** resurrected: `one=35(2)` vs `20(1)` | ✓ | ✓ `skip:lsn` |
| 9b | delete drains, then a **newer** re-insert of the same key | ✓ | **✗** re-insert lost: `20(1)` vs `50(2)` | ✓ |
| 9c | two updates of one key drained out of order | **✗** older wins: `35` vs `37` | ✓ | ✓ `skip:lsn` |
| 9d | two updates of one key drained **in order** | ✓ | **✗** second lost: `35` vs `37` | ✓ |
| 10 | the change's transaction was in flight (listed in xip) when the basis was taken | ✓ applied, no re-derive | ✓ | ✓ |

### What experiment 2 says

**The invariants I1 and I2 hold where they apply; the note's Apply is underspecified for
same-key order, and the reverse path's enumeration has a gap.**

1. **Same-key changes across batches (5b, 9, 9b, 9c, 9d).** Batches drain out of order
   ([04-claiming-and-the-fold](../../docs/staging-and-claiming/04-claiming-and-the-fold.md)),
   so two Applies for one key can arrive in either order. An image is an *absolute* value, so
   Apply does not commute, and the visibility check alone cannot order two changes that are
   both invisible in the basis: `literal` regresses the row (5b, 9, 9c). Stamping the basis
   from Apply's own snapshot (`literal-snap`) over-claims and loses every not-yet-drained
   change that committed before the Apply ran (9b, 9d), including the plain in-order case.
   Transaction ids cannot order them either (assigned at start, not at commit).

   The fix that passes everything: the entry keeps **`applied_lsn`**, the commit position of
   the last applied change, and Apply skips a change at or below it. This is exact for one
   key because same-row writes are serialized by the row lock, so their commit records are
   in LSN order and their visibility order agrees. It stays exact across a Re-derive: any
   change below `applied_lsn` is already visible in any later snapshot of that row. And it
   gives I4 its rule directly: a tombstone is an entry with `applied_lsn` set, and it can be
   physically removed once the ring's drained watermark passes that position. So I2 becomes:
   *a change C for row r is skipped iff C is visible in r's basis snapshot, or C's commit
   position ≤ r's applied position.* The LSN is already on every ring row; nothing new is
   staged. (The planted-bug "compare LSN instead of visibility" in experiment 6 is still a
   bug: LSN is exact only for the same row; the snapshot is still needed for Re-derive bases.)

   With that rule the fold's "mixed → re-derive" case disappears for plain from-side CDC: if
   the last commit of a folded record is skipped, every earlier one is too, and if it is
   not, its image already reflects them. Re-derive is needed only where there is no image.

2. **Reverse-path enumeration from the ledger alone misses in-flight moves (6b).** A child
   moving *into* parent p has read p live (I1 holds: it locked its own entry first) but its
   new join key is uncommitted when p's rename commits and the reverse path scans the
   join-key index. The scan is READ COMMITTED and cannot see it; the child commits with the
   old name and nothing ever revisits it. I1 orders writes to *one* row; this is a
   cross-row dependency recorded by the very transaction the enumeration races. Two fixes
   both pass:
   - `ledger+source`: enumerate from the ledger index **union** the live from-side rows with
     that join key. The in-flight child is committed in the *source* before its Apply ever
     started, so the source scan finds it and the Re-derive then waits on its entry lock.
     Cost: an index (or seq) scan on the user's from-side by join key per to-side change,
     i.e. the "live from-side scan" the note wanted to delete stays, alongside the ledger
     index (which is still needed for *stale* memberships the source no longer shows).
   - `ledger+deplock`: Apply/Re-derive take a shared transaction-scoped advisory lock on
     the parent key they read live, before reading; the reverse path takes it exclusively
     around its enumeration only. No source scan, but a second lock scheme (I5 says there is
     none). Held only for the enumeration, so it does not serialize the re-derives.

   The same race applies to a backfill chunk's Re-derive (it creates the entry inside its
   own transaction), so it is not specific to Apply.

3. **I5 has to be implemented as two phases, not a loop.** The first version of the reverse
   path re-derived children one at a time inside one transaction (lock entry, bump groups,
   lock next entry, …) and deadlocked in 6b against an Apply holding one child's entry while
   waiting on a group row the reverse path already held. Locking *every* entry of the batch
   in key order first, then reading all sources in one statement, then one sorted group
   increment, passes. The 4c control shows the per-row interleaving deadlocks under plain
   concurrent inserts too (19–22 in 20 s with 8 workers), and the sorted two-phase batch never
   does over 240 batches. This is the note's I5 read strictly; it is worth writing into the
   ADR that "as few statements as possible" is a correctness requirement, not a tuning note.

4. **Things that held exactly as written:** read-after-lock (2, 2b: the concurrent Apply
   demonstrably blocks on the held Re-derive's entry lock and then decides correctly in
   both orderings), visibility-checked application (3, 6, 6c, 7, 8, 8b, 10), groups as
   sums with no pre-lock or probe (4, 5), the deleted-parent path through the join-key
   index (8b), and the in-flight case needing no re-derive (10).

**Experiment 2 verdict: the invariants survive with two amendments (applied position on the
entry; reverse enumeration must see in-flight dependents) and one implementation
constraint (I5 as a two-phase batch).** Neither amendment adds a mechanism outside the
ledger; both should go into the note before experiment 3 prototypes Apply.

## Experiment 3: what does the ledger cost on the hot path?

Prototype (commit "Experiment 3 prototype" on this branch): plain single-source aggregates
only, Apply only, nothing removed. `accumulate_changes` records one `LedgerRow` per
delta-path change (from key, group key, per-field contribution text, the change's commit
position); `apply_aggregate_target` upserts them into `<target>__ledger`
(`from_key text primary key, group_key text, contrib text, applied_lsn pg_lsn, basis
pg_snapshot`, index on `group_key`) **in key order, before the group pre-lock, in the same
Phase 3 transaction** as today's delta apply:

```sql
insert into <ledger> as l (from_key, group_key, contrib, applied_lsn)
select k, g, c, x from unnest($1::text[], $2::text[], $3::text[], $4::pg_lsn[]) as v(k, g, c, x)
on conflict (from_key) do update set group_key = excluded.group_key, contrib = excluded.contrib,
  applied_lsn = greatest(l.applied_lsn, excluded.applied_lsn)
```

`TRELLIS_EXP558_LEDGER` selects `off` (byte-identical to `main`; the same-session control),
`contrib` (membership + contributions + position) or `membership` (no contributions). The
fold-in benchmark gained `wal_bytes` / `wal_bytes_per_row` over the probe (source writes and
engine writes together, offer open → drained). Runs go through `bench` (exclusive lock), via
`bench3.sh`: `fold-in-ratio --ratios 1,10,100,1000` (20 s offer, 400k rows/s target, 8
workers) and the #326 shape `group-contention --groups 400,4000,40000 --threads 1,8`.

Two harness bugs cost the first attempt (both fixed on the branch, neither a design finding):
concurrent `create table if not exists` from several first batches raced on the catalog, and
a process-wide "ledger exists" cache keyed by target name survived across the benchmark's
isolated databases. The final matrix is one clean pass per mode, `off` measured in the same
session as the control.

Results: in-window folded rows/s (least-squares fold rate while load was arriving), and WAL
bytes per source row over the probe. Every drained probe's oracle passed in every mode.

| shape | groups | workers | off | contrib | ratio | membership | ratio | WAL/row off | contrib | x | membership | x |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| group-contention | 400 | 1 | 85.3k | 69.3k | 0.81 | 70.0k | 0.82 | 448 | 736 | 1.64 | 728 | 1.62 |
| group-contention | 400 | 8 | 88.4k | 86.7k | 0.98 | 87.9k | 0.99 | 469 | 757 | 1.61 | 749 | 1.60 |
| group-contention | 4k | 1 | 72.3k | 58.5k | 0.81 | 58.4k | 0.81 | 456 | 756 | 1.66 | 744 | 1.63 |
| group-contention | 4k | 8 | 72.6k | 66.0k | 0.91 | 75.3k | 1.04 | 476 | 790 | 1.66 | 775 | 1.63 |
| group-contention | 40k | 1 | 2.8k | 0.8k | 0.27 | 2.9k | 1.04 | 452 | 612 | 1.35 | 627 | 1.39 |
| group-contention | 40k | 8 | 2.5k | 2.3k | 0.94 | 2.9k | 1.18 | 460 | 688 | 1.50 | 487 | 1.06 |
| fold-in-ratio 1000:1 | 400 | 8 | 89.6k | 85.5k | 0.95 | 88.5k | 0.99 | 468 | 757 | 1.62 | 749 | 1.60 |
| fold-in-ratio 100:1 | 4k | 8 | 75.9k | 76.8k | 1.01 | 72.4k | 0.95 | 466 | 783 | 1.68 | 779 | 1.67 |
| fold-in-ratio 10:1 | 40k | 8 | 1.9k | 2.6k | 1.38 | 3.0k | 1.60 | 461 | 486 | 1.06 | 487 | 1.06 |
| fold-in-ratio 1:1 | 400k | 8 | 0 | 0 | – | 2.0k | – | 451 | 458 | 1.02 | 490 | 1.09 |

Reading it:

- **Throughput.** With 8 workers (the shipped default) the ledger costs 1–9% at 400 and 4k
  groups, and nothing measurable at 40k+. With a single worker it costs 19% on both the 400
  and 4k shapes: the extra statement's round trip and lock time land on the one worker's
  critical path, where 8 workers overlap it. The 40k-group shapes never drain in any mode
  (the #326 existence-probe sequential scan dominates; ~2–3k rows/s), so their ratios are
  noise — the single-worker `contrib` 0.27 and the `membership` 1.04 on the identical shape
  bracket the same behaviour, and no one should read either as a ledger effect.
- **WAL.** 1.6–1.7x per source row for either variant. The ledger row itself is what costs,
  not the contributions: `membership` (no `contrib` column) writes within 1–2% of `contrib`.
  For this target (one numeric SUM, one COUNT) the contribution text is ~10 bytes; a wider
  target would widen the gap, but the fixed cost of a heap tuple + primary-key index entry
  per source row is the floor.
- **Not measured here:** the win the note expects at high group counts from deleting the
  probe and pre-lock. This prototype keeps every existing mechanism (as the experiment
  specifies), so the 40k shape still pays the probe and the ledger cannot show its upside.
  That upside is exactly the #326 cost the `off` column shows.

**Against the proposed bar** (within 25% of `main` at ratio 1 and 10 on folded rows/s, no
regression at 40k groups, WAL under 2x): passes on every 8-worker shape and on WAL; the
single-worker shapes sit at 19%, inside the bar but not comfortably. Ratio 1 and 10 (400k and
40k groups) cannot be judged on this prototype because `main` itself never drains them.

**Experiment 3 verdict: not falsified.** The ledger write is affordable with batching, and the
membership-only variant buys nothing on WAL, so the design can keep contributions. The
single-worker cost is the number to re-measure once the probe and pre-lock are actually
removed (experiment 5's build, or a later prototype that deletes them).

## Experiment 4: does a relationship survive without a projection?

Probe: `bench rel-churn` (`benchmark/src/streaming/rel_churn.rs`). `children(id, grp, parent, val)`
references `parents(id, weight)` through `RELATIONSHIP parent`; the target is
`GROUP BY grp SELECT SUM(parent.weight), SUM(val), COUNT(*)` (1,000 groups), so every parent
update changes every child's contribution. 1M children split into parents of 10, 1k or 100k
children (so 100k, 1k or **10** parents). For a 20 s window, 8 writers issue paced single-row
parent updates (100/s or 1,000/s) and, at the same time, single-row child updates (1,000/s).
"Converged" is the first moment the target equals a from-scratch oracle; the tail is how long
that took past the window.

### Results

The first baseline run (before the probe waited for a quiet ring) converged everywhere in
21–62 s; every later run showed the same protocol on `main` failing, and a pre-existing fold
pathology (#581: a refilled ring slot's stale statistics make the fold plan an O(n²) nested
loop) turned out to contaminate everything with 50k-row segments. The matrix below is the
final one: `main` is a true `main` checkout plus the probe and the one-line fold fix
(`set local enable_nestloop = off` before the fold statement), the ledger modes carry the
same fix, and every mode waits for a quiet ring before seeding and before the window.

| children/parent | parents | parent upd/s | `main` converged (tail) | `contrib` converged (tail) | contrib / main | `membership` | WAL MB main / contrib | deadlocks main / contrib |
|---|---|---|---|---|---|---|---|---|
| 10 | 100k | 100 | 113.4 s (93.4) | **20.7 s (0.7)** | 0.18 | wrong | 132 / 262 | 28 / 0 |
| 10 | 100k | 1,000 | **never, wrong** | **42.6 s (22.6)** | – | wrong | 139 / 345 | 134 / 0 |
| 1k | 1k | 100 | 79.5 s (59.5) | **39.8 s (19.8)** | 0.50 | wrong | 361 / 1,004 | 1 / 0 |
| 1k | 1k | 1,000 | 112.6 s (92.6) | **76.8 s (56.8)** | 0.68 | wrong | 404 / 2,100 | 7 / 0 |
| 100k | 10 | 100 | **24.0 s (4.0)** | 73.8 s (53.8) | **3.08** | wrong | 390 / 2,203 | 0 / 0 |
| 100k | 10 | 1,000 | **24.0 s (4.0)** | 54.3 s (34.3) | **2.26** | 397 / 1,693 | 0 / 0 |

`contrib` matched the oracle on all six shapes with zero deadlocks, zero guard rejections and
zero fallbacks. `main` logged 12,177 guard rejections, 534 fairness escalations and 27
"transient failure retries exhausted … deadlock detected" batch failures across the six, and
the 10-children / 1,000 updates-per-second shape **never converged and ended with a wrong
target** (weights short by 200–260 per group, `val` short by ~20: lost updates), reproducibly
across three runs. `membership` (no stored contributions) produced wrong targets on both
shapes it ran, as predicted: without a stored old side, Apply's old side comes from the
image, and a child whose parent moved between Phase 2 and Phase 3 is diffed against the wrong
prior state.

### What experiment 4 says

- **Against the pass bar** (100k-child case within 3x of `main`'s fast path, 10 and 1k cases
  within 1.5x): the 10 and 1k shapes are not merely within 1.5x, they are 1.5–5x **faster**
  than `main` and, unlike `main`, correct. The 100k-child shapes are 2.26x and 3.08x: one
  point just over the bar. Note what that shape is: 10 parents in total, so 20,000 updates
  fold to a handful of reverse records per batch, and `main` scans each parent's 100k
  children once per batch while the ledger **rewrites** 100k entries per touched parent per
  batch. That rewrite, not the read, is the cost.
- **Write amplification is the number the note said would decide the design, and here it is:**
  2.6–5.6x `main`'s WAL at 1k and 100k fan-out (1–2.2 GB for a 20 s window). Each parent
  update rewrites every child's entry (contribution + basis snapshot, ~100 bytes of tuple).
  The note's fallback for a hot parent, the set-based delta from stored contributions,
  removes the *compute* but not the write: the contributions must still be rewritten or a
  later child Apply subtracts a stale old side (exactly `membership`'s failure). A design
  that wants to keep this shape cheap needs the to-side value factored out of the stored
  contribution (store the from-side part and the join key; derive the to-side part from the
  parent's *current* value under I1), so a parent change touches the group and the parent,
  not every child. That is a real change to I3 and worth deciding on before experiment 5.
- **What the ledger buys:** no guards, no ring scans, no deferrals, no fallback recomputes,
  no deadlocks, and correctness under a load that makes `main` lose updates. The forward
  validation against the live to-side (the projection as a pure cache) fired on ~4% of
  child changes under churn and was always sufficient.
- **Two more things the prototype forced into the open:** the fold's planner pathology
  (#581), and that this probe finds a `main` correctness failure the suite does not (filed
  separately).

**Experiment 4 verdict: at the bar, not clearly past it.** Correctness and the realistic
shapes pass decisively; the hot-parent shape sits at 3.08x with 5x WAL, and that is the
per-child rewrite the note itself flagged as the deciding cost. The user's call: accept the
hot-parent cost, or factor the to-side value out of I3 before experiment 5.

### The ledger side (prototype, same branch, `TRELLIS_EXP558_LEDGER=contrib|membership`)

What was built on top of experiment 3's ledger, all of it behind the flag:

- **The source xid is staged.** Intake keeps the BEGIN xid on its transaction buffer and
  `append` widens it to `xid8` in SQL against the ring writer's own `pg_current_xact_id()`
  (experiment 1a's rule; migration `V52` adds `src_xid` to the ring). The fold carries the
  xid of the last image-bearing row (`FoldedChange::src_xid`).
- **Ledger entries carry `join_key`** (indexed) and `basis` (`pg_snapshot`), set only by a
  Re-derive.
- **Apply under I1/I2** (`reconcile_with_ledger`, run for every aggregate target of the batch
  in target order before any group row is locked, I5 across targets): lock the batch's
  entries in key order; a change is **skipped** if its xid is visible in the entry's basis,
  or its position is at or below the entry's applied position, or a Re-derive in this same
  batch rewrote the key; a skipped change's Phase 2 delta is undone. A kept change's **old
  side comes from the ledger** (its stored group and contribution), not from the image; the
  image's old side is undone. Then the to-side is read live for every join key in the batch
  and a row whose parent no longer carries the values Phase 2 computed with (the projection
  is a cache) is recomputed from the live parent.
- **A to-side attribute update re-derives its children through the ledger**
  (`rederive_children_via_ledger`): lock the entries whose `join_key` is the parent, in key
  order; in one statement (one snapshot, the basis) read every live child of that key **plus**
  every locked entry's row by primary key (experiment 2's 6b union), each joined to its
  parent read live; diff each child's stored contribution against its live one; rewrite the
  entries with the new contribution and basis. No guards, no ring scans, no deferral, no
  fallback: I1 orders it against every concurrent Apply. The projection is still advanced,
  as a cache only. Catch-up records, parent inserts/deletes and key changes keep `main`'s
  path in this prototype (the build writes no ledger here, so their old side is unknown).
- **The probe seeds the ledger** after backfill with a no-op update of every child through
  the real Apply path (standing in for the build's ledger write), and waits for the ring to
  be quiet before seeding and before the offer window in every mode, because the go-live
  catch-up floods the ring with recomputes that force anything staged meanwhile.

Bugs found while getting the prototype to pass the oracle under combined churn, none of them
design findings: the batch's aggregate plans are keyed by the bare target name while the
reverse shape names the qualified one, so the reverse plan was applied beside the forward
plan instead of merged into it (a child's delta counted twice); the go-live catch-up's
image-less parent records were being taken by the ledger reverse path with an unknown old
side; and the seeding rows folded with the catch-up's recomputes.
