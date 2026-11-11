# Status reference

Every defined transform carries an observable **status**: one of six words,
and a `DefinitionStatus` whose fields say why it is where it is.
`Trellis::status(target)` reports it (`status` in each binding, `trellis
status` on the CLI), and `Trellis::definitions()` lists every definition. This
page defines each word and each field.

* [Operations](operations.md#when-a-definition-needs-you) says what to do when
  a definition stops, with every reason Trellis pauses one, and how `PAUSE`,
  `RESUME` and the other repairs behave.
* [Observability](observability.md#stops-and-waits) explains the mechanics
  behind some fields: a halting failure, a build's fence, a build that keeps
  failing, and a version handover.
* [Embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)
  has the polling pattern.

`status` takes a target's `target_table` as `definitions()` lists it, or the
bare table name; every call that names a transform takes either form. An
application can list its transforms and read each one's status, which is
enough to tell a newly defined transform is still populating, without a
metrics pipeline.

## The lifecycle

Rather than instrument the backfill with bespoke metrics, Trellis gives every
transform a status. This is the same lever quarantine uses
([ADR-0003](decisions/0003-quarantine-storage-and-api.md) marks a fused
transform `quarantined`, and
[ADR-0014](decisions/0014-pause-and-drop-a-transform.md) freezes one as
`paused`; both resume by re-running the *same* backfill), so backfill,
quarantine and pause are arcs of one lifecycle:

```
(new transform)──► waiting_to_backfill ──► backfilling ◄──► live
                          ▲                                    │
                          │ RESUME (re-runs the backfill)      │ PAUSE, auto-pause,
                          └──── paused / quarantined ◄─────────┘ or the fuse trips
```

### `waiting_to_backfill`

Defined, but its source's existing rows haven't been read yet. Every new
transform starts here: `define` (and `apply` with a `TRANSFORM`) returns once
the definition is registered, with it here, before any source row is read.
It stays until the staging worker's reconcile pass finds its source (and a
`JOIN`'s joined table) captured with the columns it reads, and no change
staged before the capture install or widen is still pending
([stage 01](staging-and-claiming/01-capture-by-triggers.md#which-definitions-may-start-a-build)),
then starts its build
([data-flow — Capturing a table's existing rows](data-flow.md#capturing-a-tables-existing-rows)).
A transaction holding a table lock the capture install needs holds it here
until it ends ([`capture_wait`](#capture_wait)). A resume returns a
definition here.

### `backfilling`

The staging worker has started the transform's build, which runs on drain
threads. The target is partial. A plain 1-1 transform (over a table rather
than another transform's target) is built by the Re-derive build
([data-flow — The Re-derive build](data-flow.md#the-re-derive-build)), and an
aggregate or a `JOIN` by one absolute write per structure
([data-flow — An aggregate's or a `JOIN`'s build](data-flow.md#an-aggregates-or-a-joins-build)).
It is maintained from the start, its live changes applied while it is built,
so it goes from `backfilling` straight to `live` once its last chunk (or, for
an aggregate or a `JOIN`, its write) has committed. Its resume rebuilds it the
same way, a 1-1 target's over the ledger the freeze left (an aggregate's or a
`JOIN`'s partials are written afresh), and so does a re-read of its source
(`Trellis::request_backfill`) and the rebuild of an upstream whose target it
reads. A build over another transform's target can wait here on its fence
([`build_wait`](#build_wait)).

A `live` transform comes back here for a rebuild after:

* an `ALTER TRANSFORM` that adds or changes a field, or a resumed column (only
  those columns are rebuilt, in the background;
  [Changing a definition](transforms.md#changing-a-definition));
* a re-read of a table it reads (`Trellis::request_backfill`, the table's
  capture triggers reinstalled after a restore brought them back disabled, or
  an automatic rebuild counted in `capture_auto_rebuilds_total`), from the
  call's own transaction (or the capture pass that starts the automatic
  rebuild), returning to `live` when the rebuild is done. Its target keeps its
  rows meanwhile: the rebuild rewrites them in place;
* a rebuild of the transform whose target it reads, written outside the
  target-mutation seam;
* a partition change: a `live` aggregate over a partitioned source, or a
  `live` `JOIN` with a partitioned table on either side, comes back here while
  it is corrected for a partition of that table that was attached, detached,
  dropped or truncated, or for rows it still counts that route to no current
  partition ([ADR-0017](decisions/0017-partitioned-sources.md)). The staging
  worker pass that detects the change queues the correction and moves the
  transform `backfilling` in the same transaction; until that pass, at most
  one pass after the DDL, it still reads `live`. It goes back to `live` once
  the correction's keys are re-derived and folded into its target, as at the
  end of a Re-derive build. Until a group that routes to the partition is
  corrected, rows an `ATTACH` added aren't counted in it yet, and rows a
  `DROP`, `DETACH` or `TRUNCATE` removed still are; groups of other partitions
  are exact. The correction's progress is persisted, so a restart resumes it
  and the transform still reads `backfilling`. [`correcting`](#correcting)
  lists the partitions.

### `catching_up`

Reported, never stored: a `live` transform that reads an upstream which isn't
`live` (paused, quarantined, rebuilding, or itself reporting `catching_up`,
down the whole chain) keeps applying, but its target doesn't reflect the
source yet, so a watermark token wouldn't wait for what is missing. It reads
`live` again once the upstream does. `live` holds along a chain
([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).

So does a `live` transform that reads a paused column of an upstream 1-1
target (its fuse tripped, or `PAUSE TRANSFORM t.col`): an aggregate or `JOIN`
keeps applying over the column's frozen values, exact against the target and
stale against the source. [`paused_upstream_columns`](#paused_upstream_columns)
names the columns. A 1-1 reader's own field is paused with the column
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)), and an aggregate
that reads only other columns stays `live`.

### `live`

The steady state: once a transform reports `live`, a watermark token taken
after a commit and awaited with `Trellis::await_converged` guarantees its
target reflects that commit
([embedding — Reading your own writes](embedding.md#reading-your-own-writes)).
The rows a 1-1 build staged into the ring for keys with a `NULL` part, which
no chunk's range reaches, may still be draining when it flips, but they gate
every token, so the await covers them
([ADR-0002](decisions/0002-async-data-flow.md#what-live-promises)).

A paused column of the definition's own 1-1 target (its fuse tripped, or
`PAUSE TRANSFORM t.col`) leaves the status `live`: the column stops updating
and the rest of the target keeps applying. `status` doesn't show it;
`Trellis::quarantined()` and `quarantine_status("t.col")` show it as `paused`,
with `last_error` when the column failed itself (none for a pause by an
operator, or one cascaded from an upstream column it reads). The CLI's
`trellis status` prints one `column` line under the definition for each, with
the error and the remedy, `RESUME TRANSFORM t.col` once the cause is fixed.

### `quarantined`

Broken and no longer maintained: the whole-transform fuse tripped, because too
many of its source keys failed to apply
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)). The target stops
being written to and holds its current, now-stale value. The fuse can trip
during the build, so a transform can reach `quarantined` without ever
reporting `live`. `quarantined` and `sample_quarantined` show which rows
failed and why.

Resuming drops the transform back to `waiting_to_backfill`, deletes the keys
it holds in quarantine (its own only; another transform's held keys stay
held) and re-derives every key from the source. The fuse starts again from
zero, so the transform gets a fresh eviction budget rather than re-tripping on
the next one, and a key whose cause is still there is quarantined again.

### `paused`

Frozen, holding its last value, and not maintained
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)): the same frozen
state `quarantined` is, reached by another trigger. The target stops being
written to and holds its current, now-stale value. Either an operator ran
`PAUSE TRANSFORM`, or Trellis paused it because it can't keep it correct; a
pause by Trellis records why on [`capture_failure`](#capture_failure), except
a build that kept failing, whose error is on
[`backfill_failure`](#backfill_failure).
[Operations — Paused definitions](operations.md#paused-definitions) lists
every reason, with its message and its repair. `RESUME` re-validates it as
define would, then rebuilds it from `waiting_to_backfill`
([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)).

## `DefinitionStatus` fields

None of these fields changes the status word: a field can be set on a `live`
definition, so a check reads the field, not the word.

### `backfill_failure`

Set while one of the definition's build chunks keeps failing: it is waiting
out a backoff before it is retried, or is being narrowed to the key it fails
on. It carries the failing chunk's source table, error, attempt count and
next attempt time, and clears once the chunk finishes. A key the narrowing
quarantines is reported in [`held_keys`](#held_keys) instead, and the build
finishes without it. A definition `paused` with this set was paused by its
build: a chunk kept failing in a way no retry or narrowing gets past (a
missing table or column, say), and it keeps the error here until it is
resumed. A partition correction that keeps failing is reported here too.
[Observability — A build that keeps failing](observability.md#a-build-that-keeps-failing)
has the retry rules.

### `capture_wait`

Set while the staging worker can't yet take the brief lock it needs to install
or widen the capture triggers on a table the definition reads (its source or
a `JOIN`'s joined table): it names the table, the operation, the lock mode and
the sessions holding it (an autovacuum worker, say). The worker retries every
reconcile pass, and it clears on its own once the lock holder lets go. A
definition waits on capture while it is `waiting_to_backfill`, or while an
`ALTER TRANSFORM` field it gained waits for capture to image the column it
reads (it is `backfilling` then, its field build waiting to start). One a
schema change paused doesn't: it waits for the resume its `capture_failure`
asks for. The worker records it in the catalog, so every process's `status()`
reports it, wherever the worker runs.

### `capture_failure`

Set while capture of a table the definition reads is broken and only fixing
the cause gets it going again: Trellis paused it (every reason is in
[operations — Paused definitions](operations.md#paused-definitions)), or,
while it waits on capture as for `capture_wait`, the install or widen failed
for a reason other than a lock (no primary key, a statement that fails), which
every pass retries and which clears once one succeeds. The staging worker
records it in the catalog, so every process reports it. A definition an
operator paused has none until one of these is seen.

* **`kind`**: `halt` when the drain halted on it or its build couldn't start
  ([observability — A halting failure](observability.md#a-halting-failure));
  `disruption` when Trellis's capture of a table it reads was changed outside
  Trellis, or the table is gone, or the database was restored from a logical
  dump, while it was running; `capture` for every other case, and for a
  disruption or restore seen on a definition already frozen (an operator's
  pause stays the operator's to resume). A kind this release doesn't know
  reads `unknown`.
* **`source_table`**: the qualified table it is about. For a halt, the table
  (or tables, joined with `", "`) the failure named, or the target, for an
  aggregate the group basis can't apply or a build that couldn't start; for
  row-level security on the target, the target.
* **`columns`**: the columns it is about (the renamed or dropped ones, the old
  key columns of a redefined key, the key columns whose type or collation
  changed, the read columns that are now virtual generated columns), empty for
  a failure that isn't about a column and for a halt.
* **`error`**: one message. A definition can be paused for several reasons at
  once (a restore while a column it reads is renamed, a key change and a
  re-type in one migration). Each reason is kept, with the state its `RESUME`
  needs (a restore's reset of the ordering state, a relabel's wait for the old
  labels to drain), and `RESUME` honours all of them. The message is every
  reason, the newest first, each older one after "It was already paused:",
  then what `RESUME` would do now, as the staging worker's pass worked it out:
  "RESUME would re-type the columns Trellis created to the types define would
  give them now (`<column>` from `<type>` to `<type>`), then rebuild
  `<target>` from `<source>`" (with "reset the ordering state the dump brought
  from the original database" for a restore, "wait for the changes staged
  before the enum relabel or re-type to drain" first of all for a relabel,
  and, before the re-type, "re-record `<table>`'s key as (`<columns>`)" for a
  table whose primary key changed while a transform reading it was paused, or
  one read with `KEY (…)` that was created again, and, first of all, "re-key
  `<target>` by (`<columns>`) in place, adding the key column …, which empties
  it until the rebuild, dropping the former key column …" for a 1-1 target
  its source's key changed under, or that an earlier release keyed by its
  source key's `INCLUDE` columns), or "RESUME would refuse: …" naming what
  blocks it (anything define would refuse now, a key it can't re-record, a key
  column whose name a column of the target has, an object of yours over a
  column or key constraint the re-keying would drop, a missing target or
  ledger table). A definition this release can't run gets no outcome: its
  reason says what to do instead. While a `RESUME` is in progress the message
  reads "resuming: …" instead.
* **`reasons`**: the reasons alone, the newest first. Empty once every cause
  is gone.
* **`resume_outcome`**: the outcome alone. Unset just after a pause, until the
  next pass, and while a `RESUME` is in progress.
* **`detected_at`**: when it was recorded.

**The message stays current.** The staging worker's pass reads the
fingerprints of the tables a paused definition reads in its one fingerprint
query, and evaluates the definition again only when one of their columns or
indexes changed since its message was written, so a paused definition whose
tables don't change costs nothing. On a change it checks each reason it can
read from the schema: a renamed or dropped column that is back, a redefined
primary key that is put back, a unique index backing a `KEY (…)` or a
`MIN`/`MAX` probe index that exists again, leaves the message; a key redefined
again is named as it is now; and a column it reads that is gone now is added
as a reason. With no reason left the message reads "the cause it was paused
for is gone", followed by what `RESUME` would do. The refresh only rewrites
the message: it never resumes, reinstalls or rebuilds anything, and the
definition stays paused until `RESUME`. A change the fingerprint doesn't see
(a view created on the target, a grant, a row-security policy, an upstream
going `live`) doesn't refresh it; `RESUME` checks those itself and refuses
naming them. Running `RESUME` accepts what it does then, which re-validates
everything define would and acts on the schema as it is at that moment, not
on the message: a change made in the seconds since the last pass is acted on
without having been shown.

**Finding halted definitions.** A halted definition reports `paused`, the
same word as an operator pause, so check the halt rather than the word.
`Trellis::definitions()` carries a halt's `capture_failure` as
`DefinitionSummary::halt`, set only on a halted definition, so one call finds
every halted definition; the CLI's `trellis status` prints it on an indented
line under each one. For an aggregate the group basis can't apply, its table
is the aggregate's target, not a source. In Elixir, `halt` is a
`%Trellis.CaptureFailure{kind: :halt}` or `nil`; in Ruby, a
`Trellis::CaptureFailure` with `kind` `:halt`, or `nil`:

```rust
let halted: Vec<_> = trellis
    .definitions()
    .await?
    .into_iter()
    .filter(|summary| summary.halt.is_some())
    .collect();
if !halted.is_empty() {
    // Each stays paused, and its target stale, until it is fixed and
    // resumed — page someone.
}
```

```elixir
halted = Enum.filter(Trellis.definitions!(trellis), & &1.halt)
```

```ruby
halted = Trellis.definitions.select(&:halt)
```

Poll it on the same timer as the worker checks
([embedding — The silent-stall hazard](embedding.md#the-silent-stall-hazard)).
Clear a halt by fixing its cause and resuming each halted definition, in any
order; resuming while the cause persists halts it again.

### `held_keys`

Set while the definition holds source keys in quarantine, whatever its status,
`live` included: how many, and when the one held longest was poisoned. A key
whose change kept failing in this definition's apply (a value its target
refuses, say) is left out of its apply, and its target row stays as it was;
every other definition reading the key keeps applying it. A key with changes
parked holds back every watermark token taken since, so `await_converged` and
`self_check` don't converge past it until it is released. A finished
`self_check` job's report carries the same `held_keys`, so a `live` definition
can't hide one. With the automatic retry on, it also carries
`max_retry_attempts`, the most retries any held key has had, and
`next_retry_at`, the earliest time one is due (reported by a connection with
`held_key_retry` set; `self_check` reports none), and each
`sample_quarantined` row carries its `retry_attempts` and `next_retry_at`.
`Trellis::sample_quarantined` lists the keys with their errors;
[operations — Releasing held keys](operations.md#releasing-held-keys) says
how to clear them.

### `drain_failure`

Set while the drain keeps failing on a page holding changes to a table the
definition reads (its source or a `JOIN`'s joined table), and the failure is
charged to no key and pauses nothing: a refused read or write the catalog
can't pin on a table (a column grant, a function's `EXECUTE`, one of
Trellis's own tables;
[observability — A halting failure](observability.md#a-halting-failure)),
records that fail only together, a record that fails only in two separate
combinations of definitions
([operations](operations.md#a-page-that-fails-only-for-several-definitions-together)),
isolation stopping at its probe limit, or a failure that kept reproducing
until the page's retries ran out. The drain records such a page as a *drain
holdup*, and every pass retries it, so the definition's target stops short of
it and every watermark token taken since waits on it. The field carries the
segment (`seg_seq`), the source tables on the page (`tables`), the latest
`error` and its `sqlstate` (`None` for a failure that didn't come from
Postgres), when the drain first (`since`) and last (`last_seen`) failed on
it, and how many passes have (`attempts`). Every definition that isn't paused
or quarantined and reads one of those tables reports it, the oldest holdup
first; the status word stays what it was. It clears in the transaction that
commits the page; fix the cause the error names and the next pass does, with
nothing to resume. A transient failure (a lock timeout, a serialization
failure, a dropped connection) is never recorded, since retrying is how it
resolves. `self_check` reports every open holdup with every audit, as
`drain_failures`, whichever definition it audits: it is an instance-level
finding, and the CLI's `trellis status` prints it on an indented line under
each definition it holds back. The holdup is separate from `capture_failure`:
it pauses and charges nothing
([ADR-0003](decisions/0003-quarantine-storage-and-api.md#retry-policy)).

In Elixir, `drain_failure` is a `%Trellis.DrainFailure{}` or `nil`, and
`self_check`'s report lists them in `drain_failures`; in Ruby, a
`Trellis::DrainFailure` or `nil`, and `SelfCheckReport#drain_failures`. Both
carry the same fields, `since` and `last_seen` as a `DateTime` in Elixir and
a `Time` in Ruby:

```rust
for summary in trellis.definitions().await? {
    if let Some(status) = trellis.status(&summary.target_table).await?
        && let Some(held) = &status.drain_failure
    {
        // The drain has failed on segment `held.seg_seq` since `held.since`,
        // `held.attempts` times: page someone with `held.error`.
    }
}
```

```elixir
held_up =
  Enum.flat_map(Trellis.definitions!(trellis), fn summary ->
    case Trellis.status!(trellis, summary.target_table) do
      %Trellis.Status{drain_failure: %Trellis.DrainFailure{} = held} ->
        [{summary.target_table, held}]

      _ -> []
    end
  end)
```

```ruby
held_up = Trellis.definitions.filter_map do |summary|
  held = Trellis.status(summary.target_table)&.drain_failure
  [summary.target_table, held] if held
end
```

### `source_index_gaps`

The source indexes a `GROUP BY` or `JOIN` definition needs and lacks now. A
`GROUP BY` aggregate on the group basis needs a btree index leading with the
`GROUP BY` columns, in `GROUP BY` order (`create index on orders
(customer_id)`); a `JOIN` needs one on each side leading with that side's `ON`
columns, in `ON` order. A primary key or unique constraint that leads with
them is enough. You create them alongside the definition; Trellis never does.
Define and a `RESUME` refuse a definition without them, naming the same role
and the `create index` to run, unless it says `WITHOUT SOURCE INDEX` after its
`GROUP BY` or `JOIN` clause, so an entry is an index dropped or made invalid
since, or one a definition that opted out runs without
([Supported sources and targets](transforms.md#supported-sources-and-targets)).

One entry per table and leading columns: `table`, `columns`, a `role`
(`IndexRole`, the word `group_by`, `join_from` or `join_joined` in the
bindings: the `GROUP BY` columns, or a `JOIN`'s `ON` columns on its from side
or its joined side) that names the part of the definition that needs the
index, `opted_out`, and a `create index` statement as the fix
(`SourceIndexGap::fix`), which Trellis doesn't run. It is a warning: nothing
is paused and the target is correct, but `self_check` refuses to walk a
definition whose entry is not opted out (an opted-out one walks, scanning the
source once per page). It is read from the catalog on each call, is empty for
every other target and when the indexes exist, and is one field for every
target kind. In `trellis-embed` it is `PlainDefinitionStatus::source_index_gaps`.
The CLI's `trellis status` prints one warning line per entry.

### `extreme_index_gaps`

For a `GROUP BY` definition with `MIN` or `MAX` fields, the source index each
such argument's probe reads and the source lacks: an index leading with the
`GROUP BY` columns, then the argument (`table`, `columns`, `argument`,
`opted_out`, and `ExtremeIndexGap::fix`, a `create index` Trellis doesn't
run). A retraction that removes a group's minimum or maximum re-reads the
group's extreme from the source; with the index that is one index entry, and
without it the read scans the group's rows, or the whole source for each such
group and extreme when the `GROUP BY` index is missing too (a
`source_index_gaps` entry). It is a warning: the target stays exact and
`self_check` still walks. Once the index has been missing on every staging
worker pass for 5 minutes, the pass pauses the definition
([operations — Paused definitions](operations.md#paused-definitions)). Define
and a whole-transform `RESUME` refuse a definition that lacks one, so an entry
is an index dropped or invalidated since
([Supported sources and targets](transforms.md#supported-sources-and-targets),
the `MIN`/`MAX` row). It is read from the catalog on each call, so it clears
once the index exists, and it is empty for every other definition. The CLI's
`trellis status` prints one warning line per entry.

### `join`

Set for a `JOIN` definition (aggregate or 1-1) and `None` for any other. Read
live on each call, it reports three things, none of which changes the status:

- `hidden_tables`: the estimated rows (`pg_class.reltuples`, 0 before the
  first analyze) and total on-disk size in bytes of `P`, `M`, `J` and `T`, the
  definition's hidden tables ([ADR-0019](decisions/0019-join-transforms.md)).
  Estimates keep the call cheap on large tables; they are as fresh as the
  table's last `ANALYZE` or autovacuum.
- `partials_warning`: set once `P`'s estimated rows reach half of the `FROM`
  table's (and the `FROM` table has at least 1000 estimated rows). `P` is the
  `FROM` table grouped by its `GROUP BY` columns and join key, so a `P` near
  the table's size means the grouping barely compresses it and the hidden
  table costs about as much as the table it summarises. A `FROM` table joined
  on a unique key (its primary key, say) has one `P` cell per row, so such a
  `JOIN` warns once the table passes 1000 rows.
- `building`: the step the definition's build (a fresh build, a `RESUME` or a
  `request_backfill` rebuild) runs now, or runs next while no worker holds its
  job; `None` when no build is under way or the definition is paused or
  quarantined. A `JOIN` whose sides are both unpartitioned, or partitioned
  with no rows, builds in one transaction (`Whole`). One with a partitioned
  side that has rows builds in steps: each unpartitioned side whole, then each
  partitioned side (`FROM` first) one leaf at a time in oid order (`Leaf`,
  named `schema.table`) and then its `Remainder` (the cells that route to no
  recorded leaf), then a `Final` step that writes `T` and the target and
  completes the build. A side whose partition tree isn't recorded, or has a
  leaf without a usable constraint text, is `Unroutable`: the build fails
  there and `backfill_failure` says why. The report comes from the build
  job's cursor, which each step writes in its own transaction, through a plain
  read that takes no row lock, so a status call neither waits on a running
  step nor holds one up; it names the step after the last one that committed.

A 1-1 `JOIN` ([ADR-0021](decisions/0021-one-to-one-join-transforms.md)) sets
`join` as well, with these differences:

- `hidden_tables` lists `R`, `M` and `J` (the entries of each side, and a lock
  row per join key), with the estimates described above. `J` keeps a row for
  every join key either table has held and never shrinks, so its size only
  grows; `R` and `M` keep a tombstone for a deleted row until the tombstone
  collection reaches it.
- `building` is a `OneToOne` step: `JoinedRows`, `JoinedNullKeys` and
  `JoinedGone` for the plan job's pass over each position's `M`, naming the
  position (1 for the first joined table), `PlanningChunks` while it walks
  `a`'s primary key, `Chunks { remaining }` while the chunks of that key run,
  and `SweepStale` and `SweepOrphans` for the two passes of the sweep that
  ends a rebuild in place. It is read from the jobs' cursors as above.
- `fan_out_warnings` holds a warning for each joined position whose largest
  group of live `R` entries on one join key holds more rows than the
  threshold, in position order: a change of that position's row on the key
  rewrites all of them, in chunks of 10,000 rows, before a watermark token
  past it converges. Each names the joined position (1 for the first joined
  table), the key (`a`'s columns in that position's `ON`, their values joined
  by `,`), the group's rows and the threshold. Each position is sampled over
  its own index on `R`. They are the only runtime sign of a lookup table (few
  keys, many rows on each) joined at any position. The group is found by
  sampling `R` (2000 pages of it, all of a smaller `R`) and counting the keys
  met most often over `R`'s index on that position's join key, not from
  `pg_stats`, which is stale after a build. A count stops at ten times the
  threshold. In a very large `R` a group just past the threshold can go
  unreported until it grows. The threshold is 250,000 by default and
  `TRELLIS_ONE_TO_ONE_JOIN_FAN_OUT_WARNING` sets another. The benchmark
  (`one-to-one-join-fan-out`) measures a `LEFT` join's chunked fan-out at
  about 10 µs per row (10.6 s at a million), so a `b` change on a key past the
  default takes a few seconds to reach every row. An `a` write on the key
  waits for one chunk at most.

The `J` lock wait is a metric rather than a field:
[`join_lock_wait_seconds`](observability.md#metrics), labelled by
`transform`, the time a `JOIN` page spends in its sorted `select … for update`
of its `J` rows. It is mostly the wait for another page or writer holding one
of its join keys, so a rising tail shows contention on hot keys.

`trellis status` prints a `join tables` line, a `join build` line while a
build is under way, and one warning line per finding (a 1-1 `JOIN`'s fan-out
included). The Elixir and Ruby bindings' `status` carries `join` too (`nil`
for any other definition), with the build step flattened to a `step` word and
the `side`, `part`, `leaf` or `remaining` its kind of step has; a step, side
or part newer than the binding reads `unknown`.

### `correcting`

The partitions of a partitioned source the definition is being corrected for
while it reads `backfilling` for them (see
[`backfilling`](#backfilling)), one entry per partition, oldest first: `node`,
the schema-qualified partition (the source's root, for rows that route to no
partition); `since`, when the staging worker's pass queued the correction;
and `keys_remaining`, the keys still to re-derive from the correction's
persisted progress (`None` until its first page has counted them, and an
estimate while it reads the partition's own rows). An entry leaves once its
keys are re-derived. A paused or quarantined definition lists none; a
correction that keeps failing is reported in
[`backfill_failure`](#backfill_failure). In `trellis-embed`,
`PlainDefinitionStatus` carries it as `correcting`, each entry with `node`,
`since_micros` and `keys_remaining`. The CLI's `trellis status` prints one
`correcting` line per entry under the definition.

### `paused_upstream_columns`

The paused columns of upstream 1-1 targets that make a `live` definition
report [`catching_up`](#catching_up), each `target.column` as
`quarantine_status` addresses it, sorted: the paused columns it reads, and
those behind the definitions it reads, transitively. Empty when no such
column is read. Nothing is paused or rebuilt: resuming the column writes
through to the reader, the list empties and the status returns to `live`. In
`trellis-embed` it is `PlainDefinitionStatus::paused_upstream_columns`. The
CLI's `trellis status` prints a line under the definition.

### `build_wait`

Set while the definition's Re-derive build waits on something other than its
own work, and cleared once the wait is over: `BuildWait::Fence { xid }` while
a build over another transform's target waits on its fence, until every
transaction older than it in the cluster has ended. Look for a `backend_xid`
at or below `xid` in `pg_stat_activity`. The definition reads `backfilling`
meanwhile. The Elixir and Ruby bindings report the `xid` as `build_wait_xid`.
[Observability — A build's fence on a seam-fed source](observability.md#a-builds-fence-on-a-seam-fed-source)
explains why it is safe and how to clear it.

### `version_handover`

Set while a Trellis process of another version is live on the instance: this
binary's background processes stand by and its state-changing calls refuse
with the `version_handover` code. It names this binary's version and each
live process of the other version
([observability — Another version is live](observability.md#another-version-is-live)).
The Elixir and Ruby bindings report it as `version_handover`.

### `migrations_pending`

Set while this binary's migrations haven't run on the instance: it names them,
and `cannot_run` says why this handle's background process couldn't run them,
when it tried and was refused, with the remedy
([observability — Another version is live](observability.md#another-version-is-live)).
The Elixir and Ruby bindings don't carry it yet.

### `below_catalog_floor`

Set while this binary's version is below the instance's catalog floor: it
names the floor and this binary's version
([observability — Below the catalog floor](observability.md#below-the-catalog-floor)).
The Elixir and Ruby bindings report it as `below_catalog_floor`.

### `floor`

The oldest Trellis release that can run the definition, as
`major.minor.patch` ([defining transforms — Floors](transforms.md#floors)):
the latest release any grammar construct, key space, option or hidden-table
layout it uses arrived in. Trellis records it whenever it writes the
definition (define, `ALTER TRANSFORM`) or rebuilds it on a resume; a pause
never changes it. `trellis status` prints it as `floor=`, and the bindings
report it as `floor`. Before rolling back to an earlier release, drop each
definition whose floor is above it
([upgrading — Before rolling back](upgrading.md#before-rolling-back)).

## Limits and defaults

The fixed values behind the fields above. None is an option unless the row
says so.

| Limit | Value | What it governs |
|---|---|---|
| Key fuse | 5 failed attempts | A key whose change fails this many times in one definition's apply is held ([`held_keys`](#held_keys)). |
| Column fuse | 5 rows | A field that fails for this many distinct rows is paused (its column reads `paused` in `quarantined()`). |
| Whole-transform fuse | 5 keys | A definition holding this many keys goes [`quarantined`](#quarantined). |
| Build chunk attempts | 5 charged attempts | A chunk failure that is neither transient nor narrowed to a key, on its fifth charged attempt, pauses the definition ([`backfill_failure`](#backfill_failure)). The first retry waits 1 second, doubling after each further failure, capped at 5 seconds. |
| `MIN`/`MAX` probe index grace | 5 minutes | How long a probe index can be missing on every staging worker pass before the definition is paused ([`extreme_index_gaps`](#extreme_index_gaps)). Wall-clock time, kept in the process's memory, so a restart starts it again. |
| 1-1 `JOIN` fan-out warning | 250,000 rows per key | Set by `TRELLIS_ONE_TO_ONE_JOIN_FAN_OUT_WARNING` ([`join`](#join)). |
| Reclaim TTL | 30 seconds | How long a process killed without a clean shutdown counts as live ([`version_handover`](#version_handover)). |
| Standby poll | 5 seconds | How often a background process standing by checks again ([`version_handover`](#version_handover), [`migrations_pending`](#migrations_pending), [`below_catalog_floor`](#below_catalog_floor)). |
| Refused `migrate` retry | 1 minute | How long a background process waits before running a `migrate` refused for a cause an operator must fix again ([`migrations_pending`](#migrations_pending)). |
