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
