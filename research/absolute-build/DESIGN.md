# #1917: write-once absolute build for the aggregate — analysis and experiment design

Research only. Nothing here is meant to merge. Rule ids (R1, R4, R5, R6, R8, I1, I5, W1–W3) are ADR-0002's and ADR-0028's.

## The mechanism today (plain aggregate, `staging::group_basis`)

Hidden table `agg_<id>`: one row per group: partials (`__n`, counts, sums), `__basis` (the `pg_snapshot` of the last
absolute write), `__stamp`, generated `__zero`. Unique on the group key, `NULLS NOT DISTINCT`.

A page: stage sides per (group, `row_txid`); **lock** = `insert into agg (key) select distinct key … order by key
on conflict (key) do update set __n = excluded.__n where false` (inserts a zero row with a NULL basis for an absent
group, locks an existing one, one key-ordered pass); **apply** in a later statement = add each (group, txid) sum whose
txid is not visible in the group's `__basis` (NULL basis sees nothing).

A build (one `READ COMMITTED` transaction): enumerate (source groups ∪ groups named by old images of undrained ring
changes ∪ hidden/target groups) → the same lock statement over the enumeration → one statement that takes the
snapshot `S` and the stamp, runs the `GROUP BY`, and **updates** every enumerated row to its absolute partials with
basis `S`. Every new group is written twice (zero row, then update that flips the indexed `__zero`).

Why the lock precedes `S`: a page that applies a change **not visible in `S`** between `S` and the build's write of
that group would be overwritten (lost). With the lock first, such a page waits for the build's commit, then reads
basis `S` and applies exactly the changes `S` does not see.

## Candidate A: the lock statement writes an absent group from its own read

Generalises I1's one exception (the 1-1 build chunk's insert-from-read) to groups, in a single key-ordered pass.

1. **One statement**: take `S1` and the stamp; compute the enumeration and the `GROUP BY`; then
   `insert into agg (key, partials…, __basis, __stamp) select … order by key
   on conflict (key) do update set __n = excluded.__n where false returning key`.
   An absent group's row is inserted **once, with its final values** and basis `S1`. An existing row is locked and
   not written. Enumerated keys that were not returned are the *stragglers*; all of them are now locked.
2. **One later statement** (only when there are stragglers): take `S2`, `GROUP BY` over the straggler groups, update
   them to absolute values with basis `S2`. This is today's write, R1 intact (lock, then read).

Argument for step 1. A row inserted for `g` means no row for `g` existed at the uniqueness check, which comes after
the read. If nothing ever removes a row a page applied to, "no row" means no page has applied anything to `g`, so in
particular nothing since `S1`: there is no delta to lose. From the insert until the build commits a page for `g`
waits on the uncommitted row (its own lock statement conflicts with it), then reads basis `S1`. A change `S1` saw is
refused; a later one applies once. If a page got to `g` first, the row exists, the build only locks it, and step 2
recomputes it after the lock, as today.

Deadlock: the build takes every enumerated group (by inserting or by locking) in **one** key-ordered pass, the same
order a page's lock statement uses, so I5 holds unchanged. A two-pass form (insert the absent, then lock the others)
would take locks out of key order and can deadlock with a page; it is a negative control below.

**What A needs that today's protocol does not.** Something does remove rows pages applied to: the zero-group GC.
Counterexample: `c_a` inserts a row into `g` and commits before `S1`; `c_b` deletes it and commits after `S1`. Pages
drain both before the build's insert reaches `g`: the row is `+1 −1 = 0` with a NULL basis and the GC collects it at
once. The build then inserts `g` from `S1` (count 1, basis `S1`). `c_b` is already drained, so the group stays at 1:
wrong. So A requires **the zero-group GC to skip a definition under a build**, the rule I4 already has for the 1-1
ledger. (A source truncate cannot commit between `S1` and the build's commit: the build's read holds the source's
`ACCESS SHARE`.)

Enumeration (R8) under A: step 1's candidate rows are the source's groups at `S1`, plus zero rows for the groups
named by old images of ring changes undrained at `S1`, plus zero rows for hidden rows visible at `S1` (these always
conflict, so they are locked and become stragglers). A drain marks its changes drained in the transaction that
applies them, so a change is either undrained at `S1` (enumerated by the ring) or its row is visible at `S1`
(enumerated by the hidden table).

Expected behaviour: in a fresh build the stragglers are the groups pages touched before the build's insert reached
them. A rebuild in place has every row existing, so step 1's aggregate would be wasted there: either a rebuild keeps
today's lock statement (its absent groups are few), or the rule is uniform and a rebuild pays two aggregates. To be
measured.

Pages wait less than today: today a page on an enumerated group waits from the lock statement to the commit
(the whole `GROUP BY`). Under A it waits only from the moment the build's insert reaches its group.

## Candidate B: a build gate per definition

A lock per definition (advisory, `pg_advisory_xact_lock[_shared]`; no superuser). A page takes it shared before its
lock statement; a build takes it exclusive, then runs **one** statement: snapshot, `GROUP BY`,
`insert … order by key on conflict (key) do update set <absolute>`. Every delta a page applied before the gate was
granted is of a transaction committed before `S`, so replacing is correct, for new and existing rows alike, fresh
build or rebuild.

Costs: a new lock class at the head of I5's order; "a page waits only on the groups the build holds" becomes "a page
of the definition waits for the build" (in practice today's enumeration already covers every existing group, so only
pages that touch brand-new groups lose); on a partitioned source every leaf's transaction blocks all the
definition's pages unless the gate is per leaf.

## Candidate C: keep two writes, make them cheap (direction 3)

Today's statements with both in key order (W2), and the update made HOT if possible (`__zero` is indexed and flips;
the row also grows by the basis). Does not meet W1. Measured as the fallback.

## Direction 1 (additive write): not pursued

The build would add its absolute values to whatever pages already put in the row. That is right only if the row
holds exactly the deltas of transactions `S` does not see, which a page cannot know before `S` exists. Publishing
`S` to pages first needs a snapshot held across transactions (a second connection at `REPEATABLE READ`, or deltas
withheld per transaction and merged), which is the "withhold and recover" class ADR-0002 rejects. With a gate it
reduces to B.

## Experiments

### X1. Correctness model (soak + hand-driven scenarios, negative controls)

A standalone model of the group basis (source table, trigger capture into a ring with `row_txid`, appliers that
claim batches out of order, the page protocol above, stamps, the zero-group GC, repeated builds: first fresh, then
in place) on real Postgres, with these build variants:

| id | build | expectation |
|---|---|---|
| T0 | today's protocol | exact (control: the harness can pass) |
| A | candidate A, GC skips under a build | exact, no deadlock |
| A-gc | A, GC not skipped | wrong (the counterexample) |
| A-2pass | A as insert `do nothing`, then `select … for update` of the rest | deadlocks |
| A-noenum | A without the ring-old-image / hidden enumeration | wrong (R8) |
| B | candidate B | exact, no deadlock |
| N | single upsert that overwrites, no gate | wrong (lost deltas) |
| T0-early | today's with the snapshot before the lock | wrong (R1) |

Every wrong/deadlock expectation must actually be observed, else the harness has no teeth for that rule.
Hand-driven scenarios stall the build between its read and its insert.

Kill signal for A: a divergence or deadlock under A that needs anything beyond the GC rule.

### X2. Statement micro-benchmark

E5's method and data (1M rows/100k groups, 1M/1M, 10M/10M), fresh build and rebuild in place: today's statements;
C (sorted, and HOT attempt); A (with 0, 0.1 %, 1 %, 10 % stragglers); B; plain `INSERT … SELECT` as the reference.
Time, WAL bytes, FPI, table size, HOT updates. ADR-0028's goal is twice the plain query.

### X3 (round 2, if A or B survives). Prototype in the engine on a research branch

Change `group_basis`'s build to the surviving form, run the group-basis suites (interleavings, build, rebuild,
truncate, partition), the planted-violation generative tier, and E5's end-to-end build at 1M and 10M groups.
