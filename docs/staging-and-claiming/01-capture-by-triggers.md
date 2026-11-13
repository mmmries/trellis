# Stage 1 — Capture by triggers

← [Overview](README.md) · next → [The staging ring](02-the-staging-ring.md)

**What this stage owns:** turning a source change into a durable row in the
staging ring.

**The guarantee:** *a change's ring rows commit in the writer's own
transaction.* A change is in the ring exactly when the write that made it
committed. A crash at any instant loses nothing and replays nothing, because
there is no second transaction to lose or replay.

[ADR-0002](../decisions/0002-async-data-flow.md) records why capture works
this way. The code is in `trellis/src/capture/`.

## The capture function

Each table some definition reads carries four `AFTER … FOR EACH STATEMENT`
triggers: insert, update and delete with transition tables, and truncate. Each
calls a function generated for that table and event
(`capture::sql`), which appends the statement's changes to the active ring
segment with one `INSERT … SELECT`:

- **Keys.** Every ring row is keyed by the table's key, in declared
  order, joined with `\x1f` for a composite key: its primary key, or the
  `KEY (…)` its definitions name ([ADR-0026](../decisions/0026-source-keys.md)).
  A table with neither, a partitioned table without a primary key, or a table in a partition or inheritance
  hierarchy is refused (see
  [supported sources and targets](../transforms.md#supported-sources-and-targets)).
  A table can join a hierarchy after define (`ATTACH PARTITION`, `INHERIT`),
  or be dropped and recreated as one. The reconcile pass checks every table
  it captures with the same check define runs (`defs::hierarchy`), before
  anything else: it pauses every definition that reads one, with its
  `capture_failure`, and installs or changes no capture on it. A resume
  refuses until the table is plain again. A table that joins and leaves a
  hierarchy between two passes is never seen
  ([known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes)).
- **Images.** `old_image` and `new_image` hold the primary key plus every
  column some reader of the table needs (`capture::columns`), rendered with
  `format('%s', col)` under the same five pinned output settings as every
  Trellis session (`DateStyle`, `TimeZone`, `IntervalStyle`, `bytea_output`,
  `extra_float_digits`), set as the function's own `SET` clauses. So an image
  doesn't depend on the writing session's settings. `old_image` comes from
  the OLD transition table, which carries the whole row, detoasted.
- **`new_image` is the live row.** When another write to the table ran during
  the statement, capture re-reads each row by primary key (see "When capture
  re-reads" below); otherwise it images the transition table. The re-read
  needs `SELECT` on the table, which the Trellis role holds as the table's
  owner; the capture audit reports it missing. A table whose row-level
  security hides rows from the Trellis role is refused (see
  [supported sources and targets](../transforms.md#supported-sources-and-targets)).
- **Updates** pair the OLD and NEW transition tables by primary key. A row
  whose key changed has no partner, so it becomes a delete of the old key and
  an insert of the new one.
- **An update that changed nothing imaged stages nothing.** A paired row
  whose imaged columns all hold the values they had, compared with
  `record_image_ne` over the typed values, is dropped. That covers an update
  of only columns no reader reads, and one that sets a column to its own
  value. A binary comparison works for every type, including those with no
  equality operator (`json`, `point`, `xml`), and is never looser than the
  images: an equal value that renders differently (`numeric` `1.5` to `1.50`)
  still stages. A statement trigger can't take
  a `WHEN` clause when it has transition tables, so the filter is in the
  function.
- **`row_txid`** is the ring's default, `pg_current_xact_id()`: the source
  commit's own `xid8` (exact identity, invariant I0).
- **`lsn` and `origin_lsn`** are `pg_current_wal_insert_lsn()` when the
  trigger runs, below the commit's position.
- **`src_changed`** is `clock_timestamp()` when the statement's trigger runs:
  change time, not commit time.
- **The slot.** The function reads the active ring slot from
  `ring_slot_mirror` in the same expression that assigns the writer's xid,
  so the seal's fence ([03](03-sealing-and-the-fence.md)) holds for
  application writers at every isolation level, prepared transactions
  included.
- **Truncate** stages one image-less `truncate` row keyed by the sentinel
  `\x1ftrellis-truncate-sentinel`. The seal records it
  (`segments.has_truncate`), and the drain empties the table's 1-1 ledgers,
  hidden tables and targets and raises their truncate floor.

### A partitioned source's truncate

Every node of a partitioned source's tree carries the root's triggers, which
call the root's functions, so every row stages under the root's
`src_table` ([ADR-0017](../decisions/0017-partitioned-sources.md)). A
`TRUNCATE` fires the truncate trigger of each table it empties: each table it
names, then the partitions under that table, a parent before its children.

- **The root's truncate** stages the `truncate` sentinel, as a plain table's
  does: the target is cleared once.
- **A leaf's or middle's truncate** stages a `node_truncate` marker instead
  (key `\x1ftrellis-node-truncate`, `new_image` `{"node":
  "<schema>.<table>", "oid": <oid>}`, no `old_image`). It
  raises no truncate floor, is no barrier, and the fold leaves it out. The
  seal queues a `truncated` correction of the node for each marker in the
  segment's fenced window, in the statement that publishes the fence
  ([03](03-sealing-and-the-fence.md)), unless an unstarted correction of the
  node is already queued. The correction re-derives the keys whose group
  routes to the node and those of its current rows
  (`staging::partition_correction`), so other nodes' groups are untouched.
- **A table under one already truncated stages nothing.** The function keeps
  a transaction-local list of the tables of the tree truncated since capture
  last staged a row (`trellis_capture.t_<hash>`, next to the statement-span
  state). A node whose ancestor, or itself, is on the list stages nothing:
  nothing captured has been written to the source since that truncate. So a
  root `TRUNCATE` stages one sentinel and a middle's one marker, not one row
  per table. A `TRUNCATE` naming several nodes, or emptying several by
  `CASCADE`, stages one marker per node. A leaf whose trigger fires before
  its root's (the leaf named first, or a `CASCADE` through a foreign key the
  root's partitions inherit, which adds the tables in oid order) stages its
  marker as well as the root's sentinel: one correction too many, no more.

The functions are `SECURITY DEFINER`, owned by the role that owns the ring (the
role that ran the migrations), with `search_path` pinned. That need not be the
schema's owner: a DBA can pre-create the schema as another role. The
application needs no privilege on Trellis's schema. The triggers are
`ENABLE ALWAYS`, so a session in `session_replication_role = replica` is
captured too. A logical-replication subscription's apply worker fires only
row-level triggers, so a table a subscription replicates into is refused (see
[supported sources and targets](../transforms.md#supported-sources-and-targets)).
Nothing in the capture path issues `NOTIFY`, because `NOTIFY` takes a
database-wide lock at commit.

What is installed lives only in Postgres's catalog: the triggers in
`pg_trigger`, and each function's column set in its `COMMENT ON FUNCTION`.
`pg_dump` carries both.

## Installing, widening, narrowing and uninstalling

The staging worker's maintenance loop brings every table's capture to what the
catalog needs, on every reconcile pass, before it starts builds
(`capture::reconcile`):

| Change | Operation | Table lock |
|---|---|---|
| A definition reads a table nothing captures | install | `SHARE ROW EXCLUSIVE` |
| A new reader needs a column the functions don't image | widen: replace the functions | `SHARE ROW EXCLUSIVE` |
| The last reader of a column is dropped | narrow: replace the functions | none |
| The last reader of the table is dropped | uninstall: drop the triggers and functions | `ACCESS EXCLUSIVE` |

Every locking attempt waits at most 50 ms for the table, so an application
writer never queues behind one for longer (ADR-0002 I6), and is retried. A
pass spends at most one second on locked tables in all; the first attempt on
each table always runs, so one blocked table doesn't delay another's install.
Nothing cancels a lock holder, an autovacuum included: a table
whose lock stays held is left for the next pass. While a definition waits on
such a table, its status carries `capture_wait`, naming the sessions holding
the lock.

Defining a transform never waits for any of this. `apply` validates and
records the definition; the install happens in the background.

## The install is the fence

An install creates the triggers in one transaction under the table lock, so no
writer of the table is in flight at that moment. A write committed before the
install is visible to the snapshot of any build that starts after it, which
reads the rows the triggers never saw; one after it runs the trigger. The
install also rebuilds the table's applying readers, in the same transaction
(`intake::catch_up::park_table_catch_ups`), since they missed whatever
committed while the table was uncaptured.

## Which definitions may start a build

A waiting definition starts applying ring rows as soon as its build starts, so
it must not meet a row whose image lacks a column it reads. Two things could
put one in front of it, and the pass rules out both:

1. **Rows the replaced function staged.** An install or widen records a
   capture gate for the table (`capture_gates`), the insert position read
   while it holds the table lock. A definition's build doesn't start while any
   change to the table at or below the gate is pending
   (`capture::gate`); a gate no change holds any more is deleted.
2. **A `JOIN`'s joined table.** A `JOIN` sourced from `U` that reads a column
   of its joined table `T` would not be held by `U`'s gate, which `T`'s doesn't
   cover. So a definition starts only once every table it reads is captured
   and current, and its joined table has no gate a pending change holds.

The pass hands the build start the list of definitions it found ready, from the
same catalog snapshot it built the column sets from, so a definition
registered after the pass read the catalog waits for the next pass.

An `ALTER TRANSFORM` field is the same problem for a definition that is
already applying. The edit pauses the fields it adds or changes while its
field build runs. When a field reads a source column the definition didn't
read before, that pause stays until the installed capture images the column
(`column_status.awaiting_capture`). The field build's plan then releases the
field only once the capture images the column and the widen's gate is clear,
so every row the old function staged has drained past the paused field, and
its chunks write the field for every row. A field that reads only columns the
definition already read is released when its build starts.

## Order

Per-key order is `(lsn, change_id)`. A second writer of a key runs its
trigger only after the first commits, because it waits on the row lock, so its
rows sort after the first's. Cross-key order is not promised. In
particular, an `ON DELETE CASCADE` child's capture runs before its parent's
statement trigger, the reverse of the WAL's order; nothing depends on it.

## Convergence

Because a change's ring rows commit with the change, every commit at or below
a watermark token already has its rows in the ring when the token is read.
The read-your-writes predicate ([07](07-convergence-and-await.md)) only asks
the ring, and a waiter writes nothing.

## Schema changes and drift

DDL on a captured table can leave a trigger's images short of a column, or a
column's values changed with no trigger fired. The functions guard a missing
column and the capture pass checks the rest, pausing or rebuilding the
definitions that read it: [capture drift](capture-drift.md).

### Capture changed outside Trellis

A trigger disabled, dropped or recreated by hand, or a table swapped, pauses
its readers: [capture drift](capture-drift.md#capture-changed-outside-trellis).

### What rebuilds, what pauses, and what's missed

The response to each kind of change no trigger sees is tabled in
[capture drift](capture-drift.md#what-rebuilds-what-pauses-and-whats-missed).


## Nested writes to the same key

When an application `AFTER ROW` trigger, or a self-referencing cascade,
rewrites a row its own statement wrote, the nested statement's capture runs
first, so the outer statement's ring row has the higher `lsn`. The outer
row's `new_image` is the live row, the final one, so a `GROUP BY` or 1-1
target keeps the final values (`tests/capture_join.rs`,
`tests/one_to_one_ledger_interleavings.rs`). Its `old_image` is the statement's OLD row;
the fold keeps the earliest old image anyway. A nested write that re-keys a
row is covered by the [known correctness gaps](../known-correctness-gaps.md).

### Every transition row counts once

A reader that sums each ring row's `f(new) − f(old)`, as a group basis does,
needs a transaction's rows to telescope to the final row minus the first. The
live `new_image` breaks that: when a trigger rewrites `a → b` to `c`, the
nested capture stages `(b, c)` and the outer one `(a, c)`, which sums to
`2c − a − b`. A 1-1 target still needs the live row at the highest `lsn`, so
the re-read stays, and a re-read row also records the statement's own
transition rows wherever its images aren't them:

- `transition_images` is `{"old": …, "new": …}`, each side the transition
  row's image or JSON `null`. It is set where the live row is gone (staged as
  a delete, whose `old_image` is the transition NEW row if the statement
  inserted it), where the live row differs from the transition NEW row, and
  where a delete finds its key put back (staged as an update). Otherwise it is
  `NULL` and `old_image`/`new_image` are the transition rows. A capture that
  doesn't re-read never sets it, so the common path writes nothing new.
- Each transition row is in exactly one ring row. An update's re-read pairs
  OLD and NEW rows one to one per key, so a key with two versions on a side
  pairs each once.

A summing reader takes a row's sides from `transition_images` when it is set.
A statement's rows then sum to `Σ f(NEW) − Σ f(OLD)` (a dropped no-op pair
cancels), and a transaction's telescope. 1-1 readers ignore the column.
`tests/capture_transition_images.rs` checks the sum, and each key's final
state, for every nested-write shape.

## When capture re-reads

`new_image` is the live row: the one the transaction holds once the
statement and its own `AFTER ROW` triggers are done, not the transition
table's version. When another write to the table ran during the statement
(below), each row the statement wrote is re-read from the table by primary
key. If the key is gone, a nested write deleted or re-keyed it, and the row
staged is a delete. A delete's key is re-read too: a nested write that put
the key back stages an update to the live row.

The re-read never sees another transaction's change: each row it joins was
written or deleted by this statement, so this transaction holds its row lock
(or, for a new key, its unique-index entry) until commit. The probe keeps only
a version this transaction wrote (`age(xmin) <= 0`), because under
`REPEATABLE READ` and `SERIALIZABLE` a version another transaction deleted
after the snapshot stays visible beside the one this statement re-created. It
also requires the version's key to render as the row's own, because the ring
keys by text and a type's `=` can be looser (`numeric` `1.0 = 1.00`). The
probe is a `LATERAL … LIMIT 1` per row, and the function runs with
`enable_seqscan` off: PL/pgSQL plans once per session, often against a table
that was empty then, and a cached sequential scan would read the whole table
for every captured row once it grows (`tests/capture_reread.rs`, at every
isolation level).

Under `SERIALIZABLE` the re-read's index probe takes a predicate (SIREAD)
lock on the key's btree leaf page, and concurrent serializable writers on
neighbouring keys then form the read-write conflict chains Postgres cancels
with `40001`. Every insert of an auto-increment id lands on the rightmost
leaf, so with an unconditional re-read 50% of single-row serializable
inserts failed at 4 writers and 87% at 8 and 16, where none fail without
capture or with the gate below (`benchmark` scenario `ssi-tax`).

So capture re-reads only when the live row can differ from the transition
row: when some other write to the same table changed it after the
statement's row change and before its capture. Each such write is itself a
statement on the table (a nested statement from an application trigger, an
FK cascade or a function the statement calls, or a sibling event of the
same statement: a writable CTE, `MERGE`, `INSERT … ON CONFLICT DO UPDATE`),
so its own capture runs first. A fifth trigger, `<schema>_capture_begin`
(`BEFORE INSERT OR UPDATE OR DELETE … FOR EACH STATEMENT`), marks where each
statement's span starts in a transaction-local setting, and a capture
re-reads only if another capture of the table staged rows since then.
Otherwise it images the transition tables and reads no relation, so a
statement nothing else touched takes no predicate lock on the table. The
inexact cases all err towards re-reading: a disabled or missing begin
trigger makes every capture re-read (and the capture audit reports it).

The re-read runs under the writing session's `row_security`, so a policy
that comes to apply to the ring's owner hides rows from it, and a hidden row
reads as gone: the capture stages a delete for a row that still exists.
Setting `row_security` in the capture functions would turn the policy into a
failed application write instead. A target that reads the table directly
doesn't show the wrong delete: a 1-1 apply re-reads a delete's row
live as the drain's role, which is either exempt from the policy and reads
the row, or refused, which halts the definition, and the group basis sums
the statement's own transition rows.
The next reconcile pass pauses the table's readers
([transforms — Supported sources and targets](../transforms.md#supported-sources-and-targets)).

One write isn't a statement of its own: the update a foreign key's action
makes (`ON UPDATE CASCADE`, `SET NULL` or `SET DEFAULT`, or `ON DELETE SET
NULL` or `SET DEFAULT`). Postgres runs it inside the trigger query level of
the statement that fired it, where it fires no `BEFORE` statement trigger
once one has fired, and its rows join the transition tables of the table's
update already queued there: one capture call covers both. A row both
updated (a self-referencing key that cascades, or two cascading keys on one
row) is in the transition tables twice, and pairing by key can image the
intermediate version last. A row that references itself through a
self-referencing `ON UPDATE CASCADE` can repeat a key among the new rows
only, where pairing by key counts one old row twice. So an update capture
also re-reads when a key occurs twice on either side, and the re-read pairs
one to one.
`tests/capture_ssi.rs` pins the gate, and `capture::sql`'s `function_body`
has the argument.

## What it costs the writer

The capture runs inside the application's transaction, so the writer pays for
it: about 16 µs per single-row statement on tmpfs, and 1.3–1.7× one
expression index's CPU per row for batched writes, with about 300 bytes of
WAL per row (`benchmark` scenario `write-tax`). The begin
trigger and the span bookkeeping add about 4.4 µs per statement,
and nothing per row. An update's two repeated-key checks, which group its old
and new rows by the typed key, add about 2.5 µs per statement each and well
under 1 µs per row (`write-tax` shapes `upd1x1`, `upd1000x1`). When a statement does
re-read, the probe adds about 0.65–1.0 µs per row in batched writes and
3–10 µs per single-row statement. An application trigger that rewrites every
row it inserted makes every statement re-read and stage transition images:
captured, `nest100x1` keeps 8.5% of its uncaptured rate.

## Trellis migrations and the write path

A Trellis migration that alters a ring table (`seg_0`–`seg_3`) takes
`ACCESS EXCLUSIVE` on it, and every captured writer's trigger inserts into
the ring. So while such a migration runs, every application write to a
captured table waits for it. Run those migrations in a quiet window.
