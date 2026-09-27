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

## Experiment 4b: the to-side value factored out of I3

Branch `exp/issue-558-factored` (lane g, on top of this branch), `TRELLIS_EXP558_LEDGER=factored`.
Asked for after experiment 4: factor the to-side value out of the ledger entry so a parent
change touches the group and the parent, not every child, "similar to how we treat multi-step
hops with chained tables", and say whether that makes the 1-1 case or the aggregate fold-in
case more expensive.

### The design

Three tables per relationship-reading aggregate target, in place of experiment 4's one:

| table | key | holds | written by |
|---|---|---|---|
| `<target>__ledger` (L) | `from_key` | `group_key`, `join_key`, the **from-side part** of the row's contribution (a factored field's part is blank), `applied_lsn`, `basis` | Apply (child change) |
| `<target>__partial` (P) | `(group_key, join_key)` | `n`, the number of ledger entries with that group and parent (in general, the sum of each factored field's from-side factor) | Apply (child change), `±1` per (group, parent) the row left or entered |
| `<target>__parent` (T) | `join_key` | the parent's **applied** to-side values, `applied_lsn`, `basis` | created by the first Apply that needs the parent (read live under the lock, the read's snapshot as basis); advanced by the parent's own change |

A factored field is one whose contribution is linear in the to-side value with a from-side
factor: `SUM(parent.weight)` (factor 1, so P's `n` is the whole partial), `AVG(parent.weight)`
(the same, with the hidden count), and, once the evaluator has `*`, `SUM(val * parent.weight)`
(factor `val`, so P would carry `Σ val` per factored field beside `n`). The group's value of
such a field is, by construction, `Σ_parent P[group, parent] × T[parent]`.

- **Apply, a child change** (`reconcile_with_ledger` in `factored` mode): lock the batch's L
  entries in key order; lock, in **one** sorted statement, the T row of every parent the batch
  reads (the rows' new join keys, the locked entries' old join keys, and every parent with a
  reverse record in the batch), creating a missing one from the live parent under that lock
  (`lock_parent_rows`); the new contribution is computed with **T's applied values**, never the
  projection's and never the live parent's; the old side is the entry's from-side part with
  T's applied value of its old parent filled in; the P deltas are `−1` at the old (group,
  parent) and `+1` at the new; then L, P and T are written and the groups are locked and
  updated as before. Lock order everywhere: L → T → P → groups, each sorted.
- **Apply, a parent attribute change** (`rederive_partials`): T[p] is already locked by the
  statement above; skip iff the change's xid is visible in T's basis or its position is at or
  below T's applied position (I2, exact per parent because T[p]'s row lock serialises the
  parent's changes); read P's rows for p in group order `for update`; each group gets
  `n × (new − old)` per factored field (as a diff, so hidden counts net to zero); T[p] := new.
  **No child is read or written.** Cost is the number of distinct groups under p, at most
  `min(children_p, groups)`.
- **6b disappears for aggregates.** Experiment 2's amendment (enumerate ledger index ∪ live
  from-side scan, or a per-parent dependency lock) was needed because a child moving into p is
  invisible to p's enumeration until it commits. Here p is never enumerated: a child's Apply
  and p's Apply both take T[p], so the child either sees p's applied value (and P counts it
  after p's read) or is counted in P before p's read. The T row *is* the dependency lock the
  scenario asked for, materialised, and it carries I2's state for the parent too.
- **The chained-hop reading.** P is exactly the first hop of the two-definition chain
  `GROUP BY grp, parent SELECT COUNT(*)` → `GROUP BY grp SELECT SUM(parent.weight × n)`, and the
  target is the second hop. The engine-internal form keeps one transaction and one ring row per
  child change instead of a seam round trip and a second batch of latency, and the second hop's
  "parent change re-derives its dependents" is the P read above with fan-out = groups, not
  children.

What stays per-child: a **1-1 target** reading parent columns (one target row per child; a
parent change must rewrite those rows under any design), and an aggregate whose argument mixes
child and parent non-linearly (`SUM(GREATEST(val, parent.w))`). `MIN`/`MAX(parent.w)` and
`COUNT(parent.w)` are group-level over P ∪ T (the group's min is the min over its P rows'
parents), so they need no per-child write either, only a group recompute over P. For the
per-child cases the "like a hop" mechanism is to **re-stage the dependent keys into the ring**
(one ring row per child, drained by every worker in batches, each child's Apply under I1/I2)
instead of rewriting them inside the parent's transaction. That is #354's shape and was not
prototyped here; experiment 4's `rederive_children_via_ledger` is the inline form of it.

### Does it make the 1-1 case or the fold-in case more expensive?

- **1-1: no, by construction.** A 1-1 row's ledger entry is `(from_key, join_key, applied_lsn,
  basis)` with no contribution at all (the target row is the state); nothing in it is factored
  or unfactored. Its reverse path enumerates the parent's children either way. The only thing
  this design offers the 1-1 case is the T row as its dependency lock, replacing the live
  from-side scan of 6b with one row lock per parent.
- **Plain aggregates (fold-in): no, by construction.** P and T exist only for a target with a
  relationship join; the single-source path emits the same statements as `contrib`. Measured
  below to confirm nothing leaked into the shared code.
- **Relationship aggregates, the forward path: yes, by one row.** Every child change upserts one
  P row (two if it moved) beside its L row, and locks one T row per distinct parent in the
  batch. At fan-out 1 P is as large as L (a second heap tuple and index entry per change); at
  high fan-out P is small and its rows are hot (every child batch touching group g under
  parent p updates `P[g, p]`), like group rows are today. Measured below.

### Results

Same probe as experiment 4 (`bench rel-churn`, 1M children, 8 writers, 20 s of paced parent
updates plus 1,000 child updates/s, converged = target equals the from-scratch oracle), same
`main` and `contrib` rows as experiment 4's table, `factored` from lane g
(`TRELLIS_EXP558_LEDGER=factored`, `logs/exp4-factored-rel-churn.jsonl`). tmpfs, so CPU and
locks only; the disk-backed rows are in the disk tier section below.

| children/parent | parent upd/s | `main` converged (tail) | `contrib` (tail) | `factored` (tail) | WAL MB main / contrib / factored | deadlocks main / contrib / factored |
|---|---|---|---|---|---|---|
| 10 | 100 | 113 s (93) | 21 s (0.7) | **21 s (0.7)** | 132 / 263 / 209 | 28 / 0 / 0 |
| 10 | 1,000 | never, wrong target | 43 s (23) | **35 s (15)** | 139 / 346 / 237 | 134 / 0 / 0 |
| 1k | 100 | 80 s (60) | 40 s (20) | **21 s (0.7)** | 362 / 1,005 / 227 | 1 / 0 / 0 |
| 1k | 1,000 | 113 s (93) | 77 s (57) | **21 s (0.6)** | 404 / 2,101 / 248 | 7 / 0 / 0 |
| 100k | 100 | 24 s (4) | 74 s (54) | **21 s (0.6)** | 390 / 2,203 / 263 | 0 / 0 / 0 |
| 100k | 1,000 | 24 s (4) | 54 s (34) | **21 s (0.6)** | 398 / 1,694 / 263 | 0 / 0 / 0 |

- `factored` matched the oracle on all six shapes with zero deadlocks. Five of the six converge
  within a second of the write window closing: the drain keeps up with the offered load, so the
  tail is the last batch. The exception is 10 children per parent at 1,000 parent updates/s,
  the shape with the most distinct parents touched per batch (each one a T row lock and a P
  scan), where the tail is 15 s against `contrib`'s 23 s.
- **The hot-parent shape is no longer special.** At 100k children per parent a parent change
  rewrites 100k ledger entries under `contrib` (2.2 GB of WAL in the window, 3.08x `main`) and
  touches one T row plus that parent's P rows under `factored`: 263 MB, below `main`'s 390 MB,
  and the same 21 s as every other shape. The parent's children are never read or written.
- **WAL is below `main` on every shape** (0.6–1.6x, against `contrib`'s 2.0–5.6x), because the
  ledger's per-child rewrite is gone and the projection refresh that `main` pays for is not
  there either.

**Forward path in isolation** (child updates only, 5,000/s for 20 s, no parent updates; the cost
of the P row per child change; `logs/exp4-{contrib,factored}-forward-only.jsonl`):

| children/parent | `contrib` converged (tail) | `factored` converged (tail) | WAL MB contrib → factored |
|---|---|---|---|
| 10 | 20.7 s (0.7) | 20.7 s (0.7) | 367 → 503 (1.37x) |
| 1k | 20.6 s (0.6) | 20.6 s (0.6) | 369 → 353 (0.96x) |

Both kept up with 5,000 child updates/s, so this is a WAL measurement, not a throughput one.
At fan-out 10 the P row costs 37% more WAL: with ~1M distinct (group, parent) pairs the P heap
and index are as large as L's, and a random child update touches a cold P page (a full-page
image after every checkpoint, even on tmpfs). At fan-out 1k P has ~1k parents' worth of rows,
its pages stay hot, and the extra row is free. The forward-path cost is therefore a
low-fan-out cost and a disk cost, which the disk tier should size before it is accepted.

**Plain aggregates** (`fold-in-ratio` 100 and 1000, tmpfs, 8 workers): the first `factored`
runs folded 33–49k rows/s in the window and never drained, against `off`'s 76–90k measured the
day before, which would have contradicted "unchanged by construction". A same-lane, same-hour
control settled it: the box was slower that day for every mode (another session's harness was
running trigger-capture correctness runs with 16 pgbench clients between my measurements, and
part of its work does not sit under the benchmark lock).

| mode (lane g, same binary, back to back) | 100:1 / 4k groups | 1000:1 / 400 groups | WAL/row |
|---|---|---|---|
| `off` | 55.5k | 44.2k | 419 |
| `contrib` | 32.0k | 41.5k | 677 |
| `factored` | 50.4k | 41.4k | 669–682 |

In-window folded rows/s; none of the six drained within the 120 s grace. `factored` is at or
above `contrib` on both shapes and within noise of `off` on the second; the day-before `off`
figures (`logs/exp3-off-fold-in-ratio.jsonl`, 76–90k) are the clean ones. WAL per row is the
same as `contrib` (669–682 vs 677 B), as it must be: P and T are never touched without a
relationship. Answer for the fold-in case: no measurable cost beyond `contrib`'s, and no WAL
beyond it.

### What experiment 4b says

- The hot-parent shape stops being a shape: a parent change costs one T row, that parent's P
  rows and the group deltas, so 100k-fan-out converges in the same 21 s as 10-fan-out, with
  0.6–1.6x `main`'s WAL instead of `contrib`'s 2.6–5.6x, on tmpfs and on disk.
- The 1-1 case is untouched by construction; the plain-aggregate path emits the same statements
  as `contrib` and measures the same.
- What it costs: one P row per child change (the forward path), which is a cold-page cost at
  low fan-out (+37% WAL at fan-out 10 in the forward-only probe, nothing at fan-out 1k), and
  the restriction that only fields separable in the to-side value (SUM/AVG of a bare to-one
  path, times a from-side factor) can be factored; a target that reads the to-side
  non-linearly keeps the per-child path.
- It is more code than `contrib` (two more tables per relationship target, a partial rederive
  path, a parent-row lock statement), but it removes the 6b "index ∪ live scan" amendment:
  the T row is the dependency lock, so a parent change never enumerates children at all.

## Disk tier: the cheap shapes disk-backed

Every number above is a tmpfs number (`/tmp`, 16 GB tmpfs): fsync, full-page writes and WAL
bandwidth are free, so they measure CPU and locks. These reruns put the cluster on the box's
NVMe (`/home/mike/exp558/tmpdisk`, btrfs with `chattr +C`, the same layout `bench --disk`
uses; Postgres's default durability, `fsync`, `synchronous_commit` and `full_page_writes` on).
Same session, same binaries as the tmpfs runs they are compared with; the `main` control is
the true-`main` lane. Logs under `logs/disk/`.

### Experiment 4's shapes, all six, disk-backed

| children/parent | parent upd/s | `main` converged (tail) | `contrib` (tail) | `factored` (tail) | WAL MB main / contrib / factored |
|---|---|---|---|---|---|
| 10 | 100 | 96 s (76) | 21 s (0.7) | **21 s (0.7)** | 131 / 305 / 221 |
| 10 | 1,000 | never, wrong target (991 groups) | 53 s (33) | **41 s (21)** | 142 / 418 / 268 |
| 1k | 100 | 78 s (58) | 56 s (36) | **21 s (0.7)** | 353 / 1,374 / 228 |
| 1k | 1,000 | 104 s (84) | 105 s (85) | **21 s (1.0)** | 392 / 1,981 / 246 |
| 100k | 100 | 24 s (4) | 68 s (48) | **21 s (0.6)** | 397 / 1,646 / 258 |
| 100k | 1,000 | 23 s (3) | 57 s (37) | **21 s (0.7)** | 403 / 1,505 / 272 |

- The disk moves `contrib`, not `main` or `factored`: `contrib`'s per-child rewrite goes from
  40 s → 56 s and 77 s → 105 s at 1k fan-out (1.4–2 GB of WAL through a real device, full-page
  images on every rewritten ledger page), which is the write-amplification cost the note
  predicted and tmpfs hid. `factored` writes less WAL than `main` and converges in the same
  21 s on disk as on tmpfs. `main`'s lost-update failure at 10 / 1,000 reproduces on disk.
- Oracle matched on every `contrib` and `factored` run. `factored` had 9 deadlocks on 1k / 100
  and 16 / 9 on the two 100k shapes (0 on tmpfs), all retried within the batch retry budget;
  on disk a parent batch holds its T and P locks for longer, which is where a child batch
  arriving in the other order meets it. Sorted P and T locking is in the prototype; the
  remaining cycle is between a parent batch's P rows and a child batch's group pre-lock and
  needs a look before this is more than an experiment.
- The 100k shape, the one the I3 decision turns on: `contrib` 2.4–2.9x `main` on disk
  (2.3–3.1x on tmpfs), `factored` 0.9x `main` with 0.65x its WAL.

### #565 E1 (write-path tax) disk-backed, same harness, `TC565_BASE` on the NVMe

| variant | rows/commit | clients | rows/s tmpfs → disk | p50 / p99 ms tmpfs | p50 / p99 ms disk | WAL B/row tmpfs → disk | disk top wait |
|---|---|---|---|---|---|---|---|
| none | 1 | 1 | 70,961 → 1,624 | 0.013 / 0.02 | 0.358 / 5.0 | 186.7 → 188.5 | IO:WalSync 94% |
| none | 1 | 16 | 400,067 → 11,863 | 0.032 / 0.098 | 0.746 / 9.491 | 200.3 → 200.3 | LWLock:WALWrite 91% |
| none | 1000 | 1 | 1,610,206 → 454,169 | 0.586 / 1.14 | 1.544 / 13.01 | 138.6 → 138.6 | CPU:- 40% |
| none | 1000 | 16 | 5,285,981 → 736,155 | 2.643 / 6.592 | 9.129 / 133.361 | 152.3 → 152.3 | LWLock:WALWrite 73% |
| slot | 1 | 1 | 63,768 → 1,319 | 0.014 / 0.026 | 0.394 / 6.462 | 462.9 → 476.7 | IO:WalSync 71% |
| slot | 1 | 16 | 313,267 → 13,291 | 0.042 / 0.126 | 0.711 / 7.478 | 485.6 → 485.7 | LWLock:WALWrite 89% |
| slot | 1000 | 1 | 1,511,029 → 492,909 | 0.604 / 1.521 | 1.102 / 16.164 | 417.6 → 417.6 | CPU:- 47% |
| slot | 1000 | 16 | 4,166,015 → 826,682 | 3.471 / 7.926 | 7.027 / 329.06 | 438.9 → 438.9 | LWLock:WALWrite 72% |
| stmt | 1 | 1 | 32,648 → 1,293 | 0.029 / 0.043 | 0.435 / 6.267 | 462.7 → 464.6 | IO:WalSync 89% |
| stmt | 1 | 16 | 188,551 → 11,744 | 0.073 / 0.2 | 0.791 / 9.29 | 486.8 → 486.7 | LWLock:WALWrite 87% |
| stmt | 1000 | 1 | 413,615 → 190,833 | 2.33 / 4.317 | 3.671 / 28.296 | 417.5 → 417.5 | CPU:- 46% |
| stmt | 1000 | 16 | 1,875,880 → 363,526 | 8.214 / 13.637 | 25.71 / 215.381 | 440.3 → 440.2 | LWLock:WALWrite 52% |

- **At 1 row per commit the tax disappears into the fsync.** A single writer does 1.3–1.6k
  commits/s on this NVMe whatever the capture (`IO:WalSync` 71–94% of wait time); 16 writers
  reach 11.7–13.3k, WAL-write-lock bound, and the statement trigger is within 1% of no capture.
  The +16 µs that doubled a 13 µs tmpfs commit is invisible under a 0.4 ms fsync.
- **At 1,000 rows per commit the trigger's cost survives and grows.** 16 writers: no capture
  736k rows/s, slot 827k, statement trigger 364k (0.49x), with p99 commit latency 133 → 215 ms
  and `LWLock:WALWrite` on top. That is the WAL-bandwidth ceiling the trigger halves by writing
  ~440 B/row of ring instead of ~150 B/row of source (2.9x the WAL, the same ratio as tmpfs).
  The slot's decoding reads WAL that on tmpfs was free; on disk it still keeps pace here.
- p99 at 1 row/commit: 5–9.5 ms on disk against 20–200 µs on tmpfs, all variants alike.

### Experiment 3's shapes disk-backed: nothing drains, and the day was noisy

`fold-in-ratio` 100:1 / 1000:1 and `group-contention` 400 / 4k groups x 1 / 8 workers, 400k
offered rows/s, 120 s grace (`logs/disk/exp3-*.jsonl`; the `off` fold-in pair was rerun quietly,
`exp3-off-fold-in-ratio-rerun.jsonl`, and matched the first pass):

| shape | workers | `off` in-window rows/s | `contrib` in-window rows/s | WAL/row off → contrib |
|---|---|---|---|---|
| fold-in 100:1 / 4k groups | 8 | 22.8k (rerun 31.3k) | 30.3k | 394–405 → 602 |
| fold-in 1000:1 / 400 groups | 8 | 29.7k (rerun 23.3k) | 20.6k | 391–422 → 642 |
| group-contention 400 | 1 / 8 | 34.6k / 22.2k | 24.8k / 21.2k | 368–413 → 599–657 |
| group-contention 4k | 1 / 8 | 23.1k / 7.1k | 21.1k / 18.1k | 414–506 → 632–717 |

- No run drained in either mode: on disk the drain folds 7–35k rows/s against 70–90k on
  tmpfs, so at this offered rate the shapes are I/O-bound before the ledger matters.
- `contrib` vs `off` is inside the run-to-run spread both ways (30k vs 23k, 21k vs 30k). The
  ledger's extra write is not visible at this level; its WAL ratio is the tmpfs one (1.5–1.6x).
- **These absolute numbers are not clean.** The same day, the same binaries on tmpfs measured
  33–55k where experiment 3 had measured 76–90k (`logs/exp3-off-fold-in-ratio-today.jsonl`,
  lane c's binary; `logs/exp3-*-laneg.jsonl`, lane g's): another session's trigger-capture
  correctness harness (16 pgbench clients plus a trellis drain) ran between and, in part,
  alongside these runs without holding the benchmark lock. Within-session comparisons stand;
  the disk-vs-tmpfs ratio needs a quiet-box rerun before it is quoted. The rel-churn disk rows
  above are less exposed (converged-time, not throughput, and `main` was measured in the same
  hour), but carry the same caveat.

## Experiment 5: a ledger-writing chunked build under write load (#617)

The question (#617): does a build that writes the ledger and applies CDC from its first chunk
converge to the oracle at scale, under a write load, with no go-live re-read, and what does it
cost? `build-under-load` (`benchmark/src/streaming/build_under_load.rs`): `agg_src(id, grp,
amt)` COPY-loaded, then 8 paced writers at 2,000 statements/s (70% `amt` updates, 15% group
moves, 10% inserts, 5% deletes) from 2 s before `define` until 20 s after `live`; `GROUP BY grp
SELECT SUM(amt), COUNT(*)`; 8 drain workers; the target must equal a from-scratch `GROUP BY`
once the writers stop and the ring drains. Disk-backed: the NVMe under btrfs `+C`,
`shared_buffers=1GB checkpoint_timeout=1min max_wal_size=4GB`, default durability. The control
is today's path (flags unset: one `GROUP BY` into the target, then the go-live re-read); the
ledger build is `TRELLIS_EXP558_LEDGER=contrib TRELLIS_EXP558_BUILD=ledger`
(`trellis/src/staging/ledger_build.rs`: 10k-row PK-range chunks claimed by every drain worker,
each one transaction that placeholder-locks its keys' ledger entries, reads the rows and
`pg_current_snapshot()` in one statement and adds deltas; the definition applies CDC from the
first chunk and goes straight to `live`). Logs under `logs/exp5/`.

Columns added for #617: `peak_xmin_age_xids` / `peak_xmin_hold_secs` (the oldest client
backend `xmin` from define to the writers' stop, and the longest one value stayed oldest),
`peak_xact_secs` / `peak_xact_query` (the oldest open client transaction with an xid),
`ledger_bytes` (every `*__ledger` table, heap + indexes + toast, after convergence),
`in_xip_changes` (changes whose xid sat in an entry's basis in-progress list: applied, never
re-derived), `skipped_changes` (changes a chunk's basis already showed), `imageless_rederives`.
The first ledger run's xmin columns also counted autovacuum workers (fixed before run 2) and
it has no `peak_xact_*` columns.

### Step 2: 10M rows, 100k groups, slot capture

| | control (today's path) | ledger build, run 1 | ledger build, run 2 |
|---|---|---|---|
| build (define → built) | 4.6 s, 1 chunk | 672 s, 1,001 chunks (1.50/s) | 716 s, 1,001 chunks (1.41/s) |
| define → `live` | **226 s** | **672 s** | **716 s** |
| converged (from define) / tail after writers stop | 399 s / 153 s | 782 s / 90 s | 949 s / 213 s |
| oracle | ok | ok | ok |
| WAL MB/s / total | 14.5 / 5.8 GB | 36.9 / 29.3 GB | 31.1 / 29.8 GB |
| fsyncs/s | 502 | 468 | 486 |
| checkpoint buffers (timed + requested) | 359k (6 + 0) | 741k (8 + 5) | 699k (13 + 2) |
| writer p99 overall / during build; rate | 21.1 / 20.9 ms; 1,930/s | 44.3 / 42.8 ms; 1,894/s | 36.1 / 35.1 ms; 1,901/s |
| deadlocks / rollbacks | 3 / 672 | 6 / 4,820 | 15 / 5,852 |
| ledger on disk (source 0.79–0.82 GB) | – | 2.34 GB | 2.56 GB |
| target on disk | – | 0.39 GB | 0.78 GB |
| peak `xmin` hold / age | not sampled | (272 s, autovacuum included) | 658 s / 1.30M xids |
| skipped by basis / in-progress ids / image-less re-derives | – | 196k / 0 / 169 | 361k / 0 / 401 |

- **Correct.** The oracle matched on both ledger runs with no re-read and no orphan sweep. The
  chunks' bases skipped 196–361k changes the chunk had already read; no change's xid was in a
  basis's in-progress list (expected to be rare: a writer must be in flight on one of the
  chunk's 10k keys, below `xmax`, at the chunk's snapshot); 169–401 image-less changes were
  re-derived under the entry lock.
- **Slower to `live`, not faster.** define-to-live is 3.0–3.2x the control's (672–716 s vs
  226 s), and it is the build itself: 8 workers do 1.4–1.5 chunks/s, about 14–15k source rows/s.
  The per-chunk cost grows with the table: 3.2, 3.8 and 5.7 worker-seconds per 10k-row chunk at
  1M, 5M and 10M rows (smoke and diagnostic runs, `logs/exp5/diag-*`). The control's build is one
  4.6 s `GROUP BY`; its 226 s is the re-read lapping the ring.
- **5x the WAL.** 29.3–29.8 GB against 5.8 GB, 2.1–2.5x the MB/s over a window twice as long:
  the ledger is 3x the source's size and is written once per row by the build and again per
  change, with full-page images after every 1-minute checkpoint. The target bloats to 2x
  (additive group updates, 100k groups rewritten by every chunk that touches them). fsyncs/s
  are level with the control.
- **Long transactions.** One client transaction held the oldest `xmin` for 658 s of run 2's
  716 s build (1.3M xids of age); in the 5M diagnostic the oldest open transaction reached
  143 s, caught in the ledger build's group-existence read. Chunks and Apply batches meet on
  the same ledger placeholders (sorted, so they queue rather than deadlock; the 6–15 deadlocks
  and 4.8–5.9k rollbacks are retried), and a batch that queues behind several chunks holds its
  xid the whole time. The ring backed up during the build: seal was refused ("would lap unretired
  work") repeatedly from the first minute of run 1's build to its last (the control logged
  the same for 2 minutes of its re-read). The drain workers were busy with chunks, and the
  tail after `live` (90–213 s) is that backlog.
- Writers kept about 95% of their target rate in both paths (1,894–1,930/s of 2,000); p99 commit
  latency is 1.7–2.1x the control's during the ledger build.

Bar (#617 step 2): oracle ok — **met**; define-to-live about equal to the build time (no
re-read) — **met** (they are the same number), but the build is 3x the control's whole
define-to-live; tail small — **not met** (90–213 s against the control's 153 s); WAL and fsync
rate beside the control's — above. Whether that is out of proportion is the user's call.

### Step 4 dry run: trigger capture at 1M rows — the oracle mismatches with the `new_only` shape

Branch [`exp/issue-558-exp5-trigger`](https://github.com/mmmries/trellis/tree/exp/issue-558-exp5-trigger)
is this branch rebased onto `spike/565-trigger-capture`, plus the hooks trigger capture needs.
`build-under-load --capture trigger` installs `capture_sql.py`'s statement triggers on
`agg_src` after the load and before the writers start, with the settings the spike's own
`v_e2e.py` uses (slot-mirror pointer, `format` encoding with pinned output settings) and the
`new_only` + `skip_noop` shape #617 specifies. Trellis runs with
`TRELLIS_SPIKE_TRIGGER_CAPTURE=1`. Under that flag the ledger's xid is the ring row's
`row_txid`: the fold reads it as `src_xid`, and V52 stays empty. The reconcile parks the
registration markers without touching a publication, and a discharge doesn't wait for intake.
Without the marker the definition sat in `waiting_to_backfill` forever. A dry run at 1M rows
and 10k groups, on disk, is where it failed. Logs: `logs/exp5/trigger-dry-1m-*`.

| shape | build / live | converged (tail) | oracle | deletes issued | image-less re-derives | in-progress ids |
|---|---|---|---|---|---|---|
| `new_only,skip_noop` run 1 | 33.4 s | never (120 s grace) | **11 groups off** | 4,603 | 4,087 | 2 |
| `new_only,skip_noop` run 2 | 39.8 s | never (60 s grace) | **12 groups off** | 5,223 | 4,693 | 0 |
| `skip_noop` (old images kept), diagnostic | 40.5 s | 51.2 s (0.6 s) | ok | 5,297 | 15 | 0 |

The failing shape: every mismatched group counts exactly one extra member (target `n` = oracle
`n` + 1, and `total` too high by one row's `amt`), and the ledger agrees with the target. The
extra member is always a **deleted** row. Some were inserted by a writer and then deleted
(`basis` NULL, only ever applied by CDC); some were loaded rows the build had counted and
later deleted (`basis` set by their chunk). Every one has an `applied_lsn`, so the delete was
applied as something other than a delete.

Cause, read from `staging/fold.rs`: the fold picks a key's net NEW image as "the latest ring
row with any image" and never looks at `op`. A `new_only` delete carries no image at all, so
when a key's delete shares a fold window with an earlier insert or update of the same key, the
fold drops the delete, and the net change is that earlier row's NEW image with the delete's
LSN. Apply then counts the row. A delete alone in its window comes through image-less and is
re-derived correctly under the ledger (the ~4,100–4,700 image-less re-derives, about one per
delete); only the shared-window ones are lost (about 0.2% of deletes). Keeping the old images
(`skip_noop` only) converges with a 0.6 s tail. So the ledger build itself holds up under
trigger capture. What breaks is how the `new_only` shape meets today's fold. The fold would have
to treat an image-less `op = 'delete'` as the key's final state, or the trigger would have to
keep the OLD image on deletes.

Per #617 this stops step 4: no 100M trigger run was made with this shape, and the shape was not
changed on my own.


### Step 3 blocked: today's path runs out of memory draining the go-live re-read

Step 3 (100M rows, 1M groups, on disk) never finished a control run. Three attempts went down
in the phase after `live`, and so did a 20M sizing run. The first died of ENOSPC. The other two,
and the 20M run, were reported as "container restarts", but `journalctl -k` shows the kernel's
global OOM killer killing `benchmark` each time. It ran inside `claude-rc.service`, so the
session went down with it:

| run | killed | `benchmark` anon-rss at the kill |
|---|---|---|
| 20M sizing control (`sizing-control-20m-disk.*`) | about 24 min after the re-read's segment began draining, after swapping | 23.3 GB |
| 100M control, attempt 2 (`control-100m-disk-restart-killed.*`) | about 5 min after `live` (live at 2,322 s) | 27.0 GB |
| 100M control, attempt 3 (`control-100m-disk-oom-killed-2.*`) | about 4 min after `live` (live at 2,305 s) | 23.9 GB |

The memory is the **engine's**, not the harness's. The harness holds a few counters, two
fixed-size latency histograms and the xmin sampler's last values, and its oracle is one
server-side `count(*)` over a `FULL OUTER JOIN` (`mismatched_groups`). No run lived long enough
to reach the oracle. Small runs, each under
`systemd-run --user --scope -p MemoryMax=16G -p MemorySwapMax=0`, with
`tools/memsample.sh` sampling `VmRSS`/`VmHWM` and the ring's segments every 1–5 s
(`logs/exp5/mem/*.tsv`, runs `logs/exp5/memprobe-*`):

| run | RSS up to `live` | go-live segment rows | rows per drain batch (1 of 8 buckets) | peak RSS | outcome |
|---|---|---|---|---|---|
| control 5M / 50k groups | 87–142 MB | 5,212,157 | ~650k | **3.47 GB** | converged, tail 47 s, oracle ok |
| control 10M / 100k groups | 84–127 MB | 10,433,347 | ~1.30M | **6.32 GB** | converged, tail 119 s, oracle ok |
| control 20M / 200k groups | 97–122 MB | 20,885,602 | 2,510,829–2,516,748 (logged) | **≥13.8 GB**, cap kill 2 min 20 s into the drain | killed by the 16G cap (memcg OOM, anon-rss 14.7 GB); the session was unaffected |
| ledger build 10M / 100k groups | 730–860 MB | – (no re-read) | backlog segment 607,005 rows | **3.68 GB** | converged, tail 5.1 s, oracle ok |

The shape in every control run: RSS stays flat at about 100 MB through the load, the build and
the whole catch-up, then jumps the moment the segment holding the go-live re-read is sealed and
starts draining (5M: 133 MB to 1.3 GB in 3 s; 20M: 97 MB to 2.9 GB in 7 s). The sampler catches the re-read landing: at 5M the active segment goes from 210,273 rows to 5,212,157 between two 1 s samples, when the discharge commits. It climbs while that
segment drains and does not come back down afterwards (5M sits at 2.75 GB for the rest of the
run: glibc keeps the freed heap). Peak scales linearly with the segment: about 650 B per staged
row at 5M and 10M, with every bucket in flight at once.

**Where it goes.** The go-live catch-up's discharge (`intake/publication.rs`,
`run_pending_backfills`) enumerates every current source key as an image-less `Recompute`,
10k rows per `FETCH`, into the active segment, all in the discharge's one transaction. So one
segment holds the whole source plus the writes, 20.9M rows at 20M and about 100M at 100M. The
seal splits it into `SEG_BUCKETS` = 8 buckets (`staging/claim.rs`). Each drain worker claims
`ceil(free / live_workers)` of them and `drain_many` (`staging/apply.rs`) then:

1. `fold::fold` runs the claim-time fold for its bucket as one `txn.query` and collects every
   row into a `Vec<FoldedChange>` (`staging/fold.rs`). There is no row bound, so it is 1/8 of
   the segment per worker, 2.5M `FoldedChange`s at 20M.
2. `compute` re-reads the live source row of every image-less key in one query,
   `read_live_rows_batch` (a `Vec<Row>` of key/field/value triples, then a
   `HashMap<String, HashMap<..>>`), and builds the plan for the whole batch before apply.

At 20M the apply span's debug log shows six 2.51M-change batches starting within 2 min
(16:29:25, 16:29:44, 16:30:10, 16:30:34, 16:31:01, 16:31:27 UTC). RSS rose by about
2.5–3 GB with each: roughly 1 KB per change while a batch is folded, re-read and planned. So
memory grows with the rows in the segment and, over the drain, with how many of its batches
are in flight together. Nothing bounds that total: fewer drain workers only give each one more
buckets per batch, and there is no batch-size or segment-size setting to turn down. At 100M a
single bucket is about 12.5M changes, over 10 GB by the 20M rate, and two in flight exceed the
box's 31 GB. That matches the kills 4–5 min after `live` at 24–27 GB.

**Reproduction.** `build-under-load --rows 5000000 --groups 50000` (control, flags unset) under
the cap, with `tools/memsample.sh <out.tsv> <disk-dir> 1` alongside. It peaks at 3.5 GB about
2 minutes after define, and the peak is the drain of the segment the sampler shows as
`N@slot:draining:5212157`.

**The ledger build.** It has no re-read, so the problem above does not reach it, but its
memory is not flat either. It holds 730–860 MB while its chunks run. The ring backed up again:
seal was refused for about 5 min while the drain workers were busy with chunks, and the active
segment reached 607k CDC rows. Draining that segment took RSS to 3.4–3.7 GB, about 4 KB per
row: these are image-bearing changes, which cost more per row than image-less recomputes. That
segment's size depends on how long seal stays refused, which is write rate × time, not table
size. The 100M build runs about 10x longer, so a 100M ledger run is not safe to assume under a
16G cap. It would have to be run to find out.

**Also seen, not investigated:** `seal` fails with `deadlock detected` a few times per run,
during the catch-up (a tuple lock on one relation, 4-process cycles;
`control-100m-disk-oom-killed-2.log`, `memprobe-control-10m.log`). It is retried, and the
"ring slot N still holds a live registry row" failures that follow are the ring's backpressure
while the big segment drains, not a separate fault.

Per the manager's instruction the engine was not patched. Whether to bound the drain (a row cap
on the fold/compute batch, or a go-live re-read that seals as it pages) or to run step 3 some
other way is the user's call. Every benchmark run now goes under the 16G scope:
`tools/queue-617-step3.sh` wraps each run in `systemd-run --user --scope -p MemoryMax=$MEMCAP
-p MemorySwapMax=0`, samples RSS into `logs/exp5/mem/<tag>.tsv`, and stops the queue if a run
leaves no result.
