---
status: draft
date: 2026-11-12
deciders: Michael Ries
---

# Bulk Writes

A build, a `RESUME` and a rebuild in place write most of a hidden table, a
ledger or a target in one transaction. The statements that do it grew out of
the ones a page uses, which are shaped for a few hundred keys: insert the rows
that hold the locks, then update them, in whatever order the plan produces.
Over a whole table that shape writes every row twice and dirties pages in
random order, and its cost grows faster than the table does. On PostgreSQL 18
with 128 MB of shared buffers, every transform shape I measured built 1M rows
at 10 to 17 times the cost of the plain `INSERT … SELECT` of its query. A
`GROUP BY` over 10M groups took 25 minutes and wrote 102 GB of WAL for a
0.8 GB target, 82 times its plain query. That was one run on a busy machine,
but the WAL figure doesn't depend on load.

This ADR settles three rules and a cost goal for a **bulk write**: a statement expected to
write a large share of a hidden table, a ledger or a target. It extends the
lock order of [ADR-0002](0002-async-data-flow.md) (I5) and of
[ADR-0019](0019-join-transforms.md) (JR1), which already sort every lock
statement, to the statements that write. It leaves a page's writes as they
are, and it doesn't decide how any one build's statements are laid out: that
lives in the [stage docs](../staging-and-claiming/README.md).

## Decisions

### 1. A bulk write writes each row once

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

### 2. A bulk write writes in the destination's order

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

Rules 1 and 2 together account for the 10M-group build above. Its two
statements run alone took 1,999 s, 101 GB of WAL and 12.86M full-page images.
One statement that wrote the same rows once, in key order, took 83 s, 4.6 GB
and 54. That statement took no locks against pages, so it shows what the two
rules are worth and isn't the build's statement.

### 3. Where a page can write the same rows, the one write is a sorted upsert, and it is the lock

Pages apply from a build's first moment
([ADR-0002](0002-async-data-flow.md)), so a build never has a hidden table or
a target to itself: a page can create or change any row the build is about to
write. A plain insert would fail on a row a page created, and a write that
didn't lock would lose a page's change.

So the single write of rule 1 is `insert … on conflict do update`, in key
order. Postgres locks the row on either path, which makes the statement the
lock statement of its class: it sits at that class's place in the lock order
and is sorted as that order requires. Its conflict path must be correct
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

Rules 1 and 3 are settled for writes that add a delta. A build that writes
absolute values it read after taking its locks (an aggregate's build, a 1-1
build chunk; [ADR-0002](0002-async-data-flow.md), R1) keeps its two statements,
both in rule 2's order; how such a write can write each row once is an open
research question.

### The goal for a build is twice the cost of its plain query

A build's cost is measured against the plain `INSERT … SELECT` of its query.
The goal is a build within twice that time. It is a goal and not a release
gate: builds are at 10 to 17 times today, and I don't know that every shape can
reach it. In the model, filling a join's state with plain inserts alone cost
2.2 to 2.5 times the plain query, and the upsert of rule 3 costs more than
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
order (rules 1 and 2). The build then carries both sides into `T` as one
statement: an insert of every region the build enumerated, ordered by region,
that on conflict adds the build's count and sum to the row already there
(rule 3). A page for a customer first seen after the build enumerated its
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
  a time; rule 2 can be applied to a statement without rule 1 or 3.
