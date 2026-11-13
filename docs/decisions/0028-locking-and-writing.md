---
status: accepted
date: 2026-11-12
deciders: Michael Ries
---

# Locking and Writing

Every Trellis writer of a ledger, a hidden table or a target (a page, a build
chunk, a build, a recompute, a truncate's wipe) takes row locks, reads the
state it will change and writes. Two things go wrong without one statement of
how. Writers that lock in different orders, or read before they lock, deadlock
or lose and double count changes. And a build that writes a whole table with
the statements shaped for a page of a few hundred keys writes every row twice,
in random order: on PostgreSQL 18 with 128 MB of shared buffers, every
transform shape I measured built 1M rows at 10 to 17 times the cost of the
plain `INSERT … SELECT` of its query, and a `GROUP BY` over 10M groups took
25 minutes and wrote 102 GB of WAL for a 0.8 GB target, 82 times its plain
query. That was one run on a busy machine, but the WAL figure doesn't depend
on load.

This ADR is the one definition of how writers lock and write. It holds the
lock order, the lock statement and read-after-lock, which
[ADR-0002](0002-async-data-flow.md), [ADR-0019](0019-join-transforms.md) and
[ADR-0021](0021-one-to-one-join-transforms.md) cite by the ids kept here (I1
and I5; R1 and R7 for a group), and it adds three rules and a cost goal for a
**bulk write**: a statement expected to write a large share of a hidden table,
a ledger or a target, as a build, a `RESUME` and a rebuild in place do.

Out of scope: what state each shape keeps and which row stands as the lock for
what, which stay in those three ADRs; which groups an absolute write must
cover, the truncate barrier and lock timeouts
([ADR-0002](0002-async-data-flow.md), R8, R9 and I7); and how any one writer's
statements are laid out, which lives in the
[stage docs](../staging-and-claiming/README.md).

## Decisions

### I5: Every writer takes its locks in one order, each class in one sorted statement

A lock class is one kind of row in one table: a target's ledger entries, a
hidden table's groups, a JOIN transform's `J` rows. A writer locks all the rows
it needs of a class in one statement, sorted by key, and takes the classes in
one order. Every statement that locks them follows the order, including a
truncate's wipe and a chunk's retirement. A writer never locks in a per-row
loop. (R7 is this rule for a group.)

The order:

- **Every writer:** build chunk rows in id order, then the page row.
- **1-1 and aggregate targets:** a page's 1-1 targets, then its aggregate
  targets, each in target order; per target its ledger entries, then its rows
  or groups.
- **A `JOIN … GROUP BY` transform** ([ADR-0019](0019-join-transforms.md)): the
  pair's truncate lock row, `J` rows sorted by key (the tuple of join
  columns), `P` and `M` cells, then `T` groups.
- **A 1-1 JOIN transform** ([ADR-0021](0021-one-to-one-join-transforms.md)),
  over the union of a page's key sets: the truncate lock row; `J`, in one
  statement sorted by `(position, key)`; the `R` entries, in one statement
  sorted by entry key; the `M` entries, sorted by `(position, id)`, in
  statements that follow that order; then the target rows. The target rows
  need no order of their own: every writer of a target row holds its `R` entry
  first, so two writers of one row queue on that entry before either reaches
  the row.

In my interleaving experiment a per-row loop deadlocked 19–22 times in 20 s;
the sorted batch never did. The implemented paths held it in my benchmark
round (no deadlock in any benchmark, and group moves and the hot-key case
converge with none). In my group-basis model, a truncate wipe and a chunk
retirement that locked in scan order deadlocked with pages tens of times per
soak matrix; sorted, 0 times. In my model of the `JOIN … GROUP BY` rules
([ADR-0019](0019-join-transforms.md)), each rule proved necessary: taking
the locks unsorted was one of the changes that produced wrong answers or
deadlocks.

### A lock statement creates the rows its keys lack, and locks the rest

A key with no row yet can't be locked, so the lock statement of a class
inserts a row for each such key (a placeholder ledger entry, a group row with
zero partials, a `J` row) and locks the rows that exist, in the class's sorted
order: one sorted `insert … on conflict do nothing` and one sorted
`select … for update`, or one statement that does both. Placeholder inserts
follow the lock order too, so no two transactions take two rows of a class in
opposite orders. The insert carries the write as well in two cases: W3 below,
and a fresh aggregate build, whose lock statement inserts an absent group with
its final values (I1).

### I1: A writer reads after its lock, under `READ COMMITTED`

Every live read that feeds an absolute write happens after the writer holds
the lock on the state it will write (a ledger entry, or a group), and the
snapshot it stores is taken **in the same statement** as the read.

- A group-level absolute write (a recompute, a build batch, a partition
  correction) locks its groups first in one sorted statement; the snapshot,
  the `GROUP BY`, the new partials, the basis and the stamp are then one later
  statement. (R1 is this rule for a group.)
- A page reads each group's basis in a statement after its lock statement.
- A writer of a JOIN transform's partials reads the other side's partials and
  its match counts only in statements after its `J` lock statement.

The rule needs `READ COMMITTED`, where each statement reads from a snapshot of
its own, so a read after the lock sees what the lock waited for; at
`REPEATABLE READ` or `SERIALIZABLE` it would read from the transaction's first
snapshot, taken before the lock. Every session Trellis opens sets
`default_transaction_isolation` to `read committed`, whatever the server's,
database's or role's default, and a transaction that needs one snapshot for
several reads asks for its level explicitly.

Two writes read first, each the insert of a row that doesn't exist yet, from a
read in the same statement.

The first is a 1-1 build chunk's insert of the entry of a key that has none. Its uniqueness check comes after
the read, and finding no entry there means no Apply has written the key since
the read's snapshot, because the tombstone GC, the one thing that removes an
entry an Apply wrote, skips a ledger under a build (I4). A page deletes only a
placeholder it wrote no change to, which holds no applied change, and a source
truncate commits before the chunk's first read, which holds the source's lock,
so it empties no change the snapshot doesn't see. A key that has an entry by
then is locked and read afresh.

The second is a fresh aggregate build's insert of a group that has no row
([ADR-0002](0002-async-data-flow.md#an-aggregates-build-is-one-absolute-write-per-structure)).
One statement takes the snapshot, reads the groups and their partials, and in
key order inserts each absent group with its final values and that snapshot as
its basis, and locks each group that has a row without writing it. That one
pass is the build's lock statement (I5). Finding no row at the uniqueness check
means no page has applied a change to the group, because nothing removes a
group's row between the build's snapshot and its commit: the zero-group GC
collects nothing of a definition under a build, and the build's read holds the
source's lock against a truncate. A page that reaches the group later waits on
the uncommitted row, then reads the basis. A group that has a row by then is
locked by the same pass and read afresh in a later statement, as the rule
requires. Inserting the absent groups and locking the others in two passes
takes the locks out of key order and is not allowed.

In my interleaving experiment (seventeen hand-driven scenarios of Apply racing
a Re-derive and other producers), the scenarios where a stalled Re-derive
meets a change to the same key, committed before or after its snapshot, hold,
and Apply demonstrably blocks on the entry lock. In my model of the aggregate build
(twenty hand-driven scenarios and 165 soak runs of this form, with pages, the
GC and repeated builds running), the insert from the read was exact with no
deadlock; letting the GC collect under a build, dropping a part of the
enumeration, or rewriting the groups that had a row from the first snapshot
were each wrong, and the two-pass form deadlocked in every run. In a snapshot-timing
experiment I measured a snapshot taken in a separate statement differing from
the read's in 99.7% of samples under load; the stored basis is the full
`pg_snapshot`, not `xmin`/`xmax`. In my group-basis model a page that read a
group's basis before its lock wait double counted; read after the lock it was
exact. In my model of the `JOIN … GROUP BY` rules, reading the other side
before the lock was another such change.

### A writer's locks, reads and writes are one transaction

What a writer derives from the state it locked, it writes in the transaction
that holds the lock. A JOIN writer adds its change to `T` in the same
transaction as its change to `P` or `M`; writing `T` in a second transaction
was a third.

### W1: A bulk write writes each row once

A row inserted to hold a lock and then updated leaves a dead version behind,
and a second entry in every index when the update changes an indexed column.
The table and its indexes end at about twice their size, and the WAL carries
both versions.

I measured the `T` write of a `JOIN … GROUP BY` build at 3M fact rows both
ways. Insert the lock rows, then update them: 28.8 s and 2.28 GB of WAL. One
statement that writes each row once: 11.5 s and 0.77 GB, with `T`'s heap at
240 MB instead of 480 MB and its indexes at 111 MB instead of 332 MB. A
standalone model of the same join and aggregate agreed: the build that wrote
each row twice took 2.5 times as long and twice the WAL at 1M rows.

### W2: A bulk write writes in the destination's order

New rows go in the order of the unique key that identifies them. Existing rows
are updated in the order they are stored, which a table filled in key order
shares with its key.

A write in any other order lands each row on a different page of the heap and
of the index. Once either is larger than shared buffers, each page is read
back, changed and evicted again, and every checkpoint logs it in full the next
time it is touched. Inserting 19M cells into a hidden table with a 734 MB
unique index took 213 s, 14.8 GB of WAL and 1.89M full-page images unsorted,
and 26.5 s, 3.6 GB and 83 full-page images in key order: the same statement
and the same rows with an `ORDER BY`. The update of a join's match counts at 3M fact
rows went from 37.0 s to 9.5 s the same way.

W1 and W2 together account for the 10M-group build above. Its two
statements run alone took 1,999 s, 101 GB of WAL and 12.86M full-page images.
One statement that wrote the same rows once, in key order, took 83 s, 4.6 GB
and 54. That statement took no locks against pages, so it shows what the two
rules are worth and isn't the build's statement.

### W3: Where a page can write the same rows, the one write is a sorted upsert, and it is the lock

Pages apply from a build's first moment
([ADR-0002](0002-async-data-flow.md#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)), so a build never has a hidden table or
a target to itself: a page can create or change any row the build is about to
write. A plain insert would fail on a row a page created, and a write that
didn't lock would lose a page's change.

So the single write of W1 is `insert … on conflict do update`, in key
order. Postgres locks the row on either path, which makes the statement the
lock statement of its class: it sits at that class's place in the lock order
(I5) and is sorted as that order requires. Its conflict path must be correct
against the latest committed row. For state that changes by deltas (count and
sum partials, and `T`) it adds the build's delta to the row. Every row in the
build's lock set is written, including one whose delta is zero, so a
concurrent page waits on it exactly as it waits on a lock row.

The upsert costs about twice a sorted plain insert, and about the same as an
unsorted one (11.5 s against 12.0 s for `T` at 3M). That is the price of
running pages during a build. A plain insert is right only for a table no
other session can reach, which today means a session's own temporary working
tables.

Because the rows a build writes this way are exactly the rows it locks, the
check that turns a delta for an unlocked row into an error
([ADR-0019](0019-join-transforms.md)) can't fire in the build. It stays for
pages, whose lock set is computed separately from what they write.

W3 is for writes that add a delta. An absolute write meets W1 another way: a
1-1 build chunk and a fresh aggregate build insert a row that doesn't exist
from their own read (I1). A row that exists is locked, read afresh and updated
once, in W2's order; that is all of a rebuild in place.

### The goal for a build is twice the cost of its plain query

A build's cost is measured against the plain `INSERT … SELECT` of its query.
The goal is a build within twice that time. It is a goal and not a release
gate: builds are at 10 to 17 times today, and I don't know that every shape can
reach it. In the model, filling a join's state with plain inserts alone cost
2.2 to 2.5 times the plain query, and the upsert of W3 costs more than
that. The goal also says when to stop: a build at twice its plain query isn't
worth optimizing further.

### Pages keep their forms

A page touches a few hundred keys. Its two-step write, with the error for an
unlocked row, stays as it is.

Why not pick the form by the size of the batch, so that a large page gets the
bulk form too? In the model the bulk form overtook the page form somewhere
between 0.1% and 1% of keys touched, so there would be a gain. But a page's
lock set is assembled from both sides of a join and from group moves, and the
error for an unlocked row is what makes a gap in it fail loudly. A build's
size is known when it starts, so the rules apply there first.

## Example

`revenue_by_region` from [ADR-0019](0019-join-transforms.md) is built over
100M orders and 50k customers. Each `P` and `M` cell is written once, in cell
order (W1 and W2). The build then carries both sides into `T` as one
statement: an insert of every region the build enumerated, ordered by region,
that on conflict adds the build's count and sum to the row already there
(W3). A page for a customer first seen after the build enumerated its
keys runs alongside the build. If it reaches the `EMEA` row first, the
build's statement waits for it and adds to what it committed; if it arrives
second, it waits on the row the build wrote. A region whose delta is zero
still gets its row, and holds its lock, until the build commits.

## Consequences

- A bulk-written structure has two write forms, the build's and the page's,
  and they must produce the same rows. Tests compare a built target with its
  plain query and with the same state reached by pages.
- A build loses the error for an unlocked row on the structures it upserts. A
  defect in how a build computes its lock set no longer fails there; it can
  only show as a wrong target, which the comparison with the plain query has to
  catch.
- Zero rows written by a build stay until the collection that removes them
  runs, as lock rows do today.
- A build is measured against its plain query, and by WAL bytes and full-page
  images as well as time, at a size where the hidden tables are larger than
  shared buffers. None of the costs above shows on small data.
- Builds that don't meet the rules yet are being changed to, one structure at
  a time; W2 can be applied to a statement without W1 or W3.
