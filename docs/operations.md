# Operations: stops and repairs

What to do when Trellis stops a definition, and how each repair behaves.
Trellis stops rather than guess: it pauses a definition it can't keep
correct, holds a key it can't apply, and holds back a page it can't commit,
and it says so on `status()`. A stopped target is behind, not wrong.
The [status reference](status-reference.md) defines each status and field.
[Known correctness gaps](known-correctness-gaps.md) lists the ways a target
can go wrong without a stop, and names the repair for each.

## When a definition needs you

### Paused definitions

A definition Trellis paused reads `paused`, with the reason on its
[`capture_failure`](status-reference.md#capture_failure): each reason below
is one sentence of its `error`, which begins as the first column shows
(`<…>` marks a name or value the message fills in), names its cause and says
what to do. A definition paused for several reasons lists them all, the
newest first, and `RESUME` honours all of them. Every row's repair ends with
`RESUME`, which re-validates the definition and rebuilds it
([`PAUSE` and `RESUME`](#pause-and-resume)), or with `DROP TRANSFORM` and
define it again. Trellis never resumes a definition on its own, except where
a row says so.

| The reason begins | It means | Do |
|---|---|---|
| (no `capture_failure`, and no `backfill_failure`) | An operator ran `PAUSE TRANSFORM` | `RESUME` when you're ready |
| (no `capture_failure`; the error is on `backfill_failure`) | Its build kept failing in a way no retry gets past: a chunk failure that is neither transient nor narrowed to a key, on its fifth charged attempt, or an aggregate's build, which can't be narrowed to a key ([A build that keeps failing](observability.md#a-build-that-keeps-failing)) | Fix what the error names, then `RESUME` |
| `column <columns> of <table>, which this definition reads, was renamed or dropped` | A source column it reads was renamed or dropped; capture now leaves it out ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-renamed-or-dropped-column)). The staging worker drops the reason once the column is back | Restore the column, then `RESUME`; or `DROP TRANSFORM` and define it again |
| `the primary key of <table> changed from (<columns>) to (<columns>)` | Its source's primary key was redefined, or another definition's resume re-recorded the table's key while this one ran. The staging worker drops the reason once the key is put back | `RESUME`, which rebuilds it over the new key, re-keying a 1-1 target in place first ([`PAUSE` and `RESUME`](#pause-and-resume)) |
| `no valid unique index backs KEY (<columns>) on <table> any more` | The unique index backing a declared `KEY (…)` was dropped or made invalid, so the table can take two rows with one key. The staging worker drops the reason once an index backs it | Create the index the message names (build a replacement before dropping the old one), then `RESUME`. On a partitioned table, re-run `ALTER INDEX … ATTACH PARTITION` for a repaired leaf's index |
| `no valid index on <table> has led with (<columns>) in that order for 5 minutes` | A `MIN`/`MAX` field's probe index was missing on every staging worker pass for 5 minutes ([`extreme_index_gaps`](status-reference.md#extreme_index_gaps)). The staging worker drops the reason once the index exists | Create the index the message names, then `RESUME` |
| `the unique key on the ON columns of <table> (<columns>) is gone` | A 1-1 `JOIN`'s joined table lost the unique key on its `ON` columns, so a row of the `FROM` table can match more than one of its rows | Create the unique index again (remove duplicates first), then `RESUME` |
| `column "<column>" of <table> is a virtual generated column` | A column it reads became a virtual generated column, which a capture trigger can't read | Make the column `STORED`, then `RESUME`; or define it again with the expression in place of the column |
| `column "<column>" of <table>, <key use>, …`, `column "<column>" of <table>, which this definition reads, changed type from …` or `… was dropped and re-added as …`, `ON joins …`, `column "<column>" of <table> widened to …`, or `Trellis keeps columns typed from …` | A column it reads, keys by, or keeps a typed copy of changed type or collation: define would now refuse it, a `JOIN`'s `ON` columns no longer match, its stored keys render differently, or a copy Trellis created can't hold the new type (or was re-typed outside Trellis) | Make the schema acceptable (the message says how; a resume refuses while define would), then `RESUME`, which re-types Trellis's copies and rebuilds; or `DROP TRANSFORM` and define it again |
| `the resume of transform <name> re-typed <columns>, which this definition reads` | An upstream's resume re-typed columns of its target that this definition's own columns or keys were typed from | Nothing: it resumes on its own once `<name>` is `live`. A `PAUSE` withdraws that, and the message says so; then `RESUME` it |
| A define refusal, then `A resume of <table> re-typed <columns>` | An upstream's resume re-typed its target into something define would refuse for this definition | Fix what the message names, then `RESUME`; or `DROP TRANSFORM` and define it again |
| `row-level security on <table> applies to role <role>` or `row-level security on target <table> applies to role <role>` | Row-level security came to apply to a role Trellis reads a source or writes its target as, so its policies would filter Trellis's reads or writes ([row-level security](transforms.md#row-level-security-on-a-role-trellis-runs-as)) | Exempt the role (`BYPASSRLS`, or make it the owner without `FORCE ROW LEVEL SECURITY`), then `RESUME` |
| `<table> is replicated into by logical-replication subscription <subscription>` | A subscription replicates into a table it reads, and capture never sees the changes its apply worker applies | Stop replicating into the table, or drop the subscription, then `RESUME` |
| `<table> is a partitioned table`, `<table> became a partition of <parent>`, or `… inherits from …` | A captured plain table joined a partition or inheritance hierarchy | Make it a plain table again, then `RESUME` |
| `partitioned table <table> …` | A partitioned source stopped meeting the shape define accepts: its partition-key column, the leaf limit, the server version | Make the tree comply ([the refusal table](transforms.md#supported-sources-and-targets)), then `RESUME` |
| `enum value '<old>' of type <type> was renamed to '<new>'`, or `column "<column>" of <table>, which this definition reads, changed type from enum <type> to enum <type>` | `ALTER TYPE … RENAME VALUE`, or a re-type to another enum, on a column it reads: the changes it staged and the keys it holds name the old label ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#an-enum-value-relabelled)). Every definition reading a column of the type pauses | `RESUME`, which waits for the changes staged before the relabel to drain, then rebuilds the target from the source |
| `Trellis's capture of <table> was changed outside Trellis: …` | A capture trigger is gone, was dropped and created again, is disabled, is enabled in `ORIGIN` or `REPLICA` mode, or was altered; a capture function was replaced; or the table, or a partitioned root, was dropped and created again or swapped in by renames (on a partitioned source's node, "(on `<node>`)" follows each finding). Changes made meanwhile may be missing ([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#capture-changed-outside-trellis)). Its `kind` is `disruption` | Put the table back if it was a mistake, then `RESUME`, which reinstalls capture and rebuilds |
| `the table <table> no longer exists at that name` | The table was dropped, renamed away, moved to another schema, or replaced by something that isn't a table. Its `kind` is `disruption` | Put a table there again, then `RESUME`, which re-validates against it (it refuses while the table is missing) |
| `the database was restored from a logical dump` | Every definition, whatever its kind and source, pauses: the ordering state the dump brought belongs to the original database ([ADR-0002](decisions/0002-async-data-flow.md), [ADR-0017](decisions/0017-partitioned-sources.md)). Its `kind` is `disruption` (`capture` on one already paused) | `RESUME` each definition, which resets that state and rebuilds it ([After a restore or an upgrade](#runbooks)) |
| `transform <target> reads <upstream>, whose transform the restore of the schema dump stored paused` | A [schema dump](embedding.md#schema-dumps) restore stored its upstream paused | `RESUME` the upstream, then this one |
| `this release can't run transform <target>: …` | Its `floor` is above this release, or it was written under a newer grammar, or a newer release stored a value for it (its status, its build, a build chunk's kind, a build job's cursor tag or a partition correction's phase), or the partition tree of a source it reads, that this one doesn't know: a later release defined, rebuilt or ran it, and this one was rolled back to ([ADR-0024](decisions/0024-compatibility-floors-and-rollback.md)). Trellis pauses it when it attaches, when a background process takes over, and before each state-changing call, and never reads its text. Its target keeps its rows and stops updating; the rest of the instance runs | Roll forward to the release the message names or later, then `RESUME` it there, which rebuilds it, or `DROP` it there ([before rolling back](upgrading.md#before-rolling-back)) |
| `the stored definition was written under grammar version <n>, and this binary reads grammar versions <oldest> and later`, or `the stored definition of transform <target> is refused by this release` | Its stored text is older than this release reads, or this release refuses it | `DROP TRANSFORM` and define it again in the current grammar |
| `transform <target> was built by an earlier release over <table>'s key with its INCLUDE columns`, or `transform <target> reads <source>, which an earlier release built over …` | A release before 0.2.0 read a source key's `INCLUDE` columns (`primary key (id) include (x)`) as key columns, so the definition's target, ledger and staged keys are keyed by them too, as is a 1-1 transform reading its 1-1 target. The upgrade pauses each such definition once | `RESUME` (a reader's waits for its upstream's), which re-keys a 1-1 target in place (its rows, your indexes and grants stay; the former key column it no longer creates is dropped) and rebuilds; it refuses, naming them, while a view or index of yours depends on that column. Or `DROP TRANSFORM` and define it again |
| `transform <target> reads <table>, which has no primary key`, `transform <target> reads <table>, whose primary key (<columns>) is deferrable`, or `<target> must name its key as KEY (...)` | An earlier release keyed it by a key Trellis no longer takes: a unique index it chose for a table with no primary key, or a deferrable primary key ([the key entry](transforms.md#the-key-a-transform-keys-each-table-by)). The upgrade pauses it | Add the primary key the message names (not deferrable), then `RESUME`; or `DROP TRANSFORM` and define it again with a valid `KEY (…)` |
| `the drain halted: …` or `the re-derive build of <target> couldn't start: …`, kind `halt` | A failure every key reproduces (a source key the drain can't use, a propagation wave past the hop bound, an aggregate the group basis can't apply, a read or write refused to a role Trellis runs as: a missing privilege, naming the role and the table, or row-level security), or a build start that fails the same way on every pass. It pauses the definitions it reaches and everything downstream of them ([A halting failure](observability.md#a-halting-failure)) | Fix the cause (grant the privilege, or `BYPASSRLS`), then `RESUME` each halted definition, in any order |

A definition this release can't run gets that reason first, with any reason
it was already paused for after it, and no "RESUME would …" outcome. For a
source whose recorded partition tree this release can't read, the capture pass
records the reason on every definition reading the source, leaving the
record as it is. Until the definition is paused, `status()` of one whose
status this release doesn't know refuses with the `version_floor` code, and a
`capture_failure` whose kind it doesn't know reports kind `unknown`.
`RESUME`, `ALTER TRANSFORM` and `self_check` of it refuse with the
`version_floor` code while what this release can't run is still stored: its
floor or grammar, a value a newer release stored for it other than its status
or a capture failure's kind (the pause replaces those, so this release
resumes it), or a source's unreadable partition record. `DROP TRANSFORM` of
one above its floor refuses too.

Two more messages describe a paused definition's state rather than a reason:

| The message begins | It means | Do |
|---|---|---|
| `resuming: …` | A `RESUME` is re-typing copies, waiting for an upstream to go `live`, or waiting for the changes staged before an enum relabel, or a re-type to another enum, to drain | Wait. A `PAUSE` withdraws the resume |
| `the cause it was paused for is gone` | Every reason it was paused for was fixed while it stayed paused (a column renamed back, a primary key put back); the staging worker never resumes it on its own ([`capture_failure`](status-reference.md#capture_failure)) | `RESUME`, which does what the message's "RESUME would …" says, as the schema stands when it runs |

### Other stops and waits

| You see | It means | Do |
|---|---|---|
| `quarantined` | Too many of its keys failed to apply, so the whole-definition fuse tripped | Fix the cause `sample_quarantined` shows, then `RESUME` |
| A column `paused`, with a `last_error`, in `Trellis::quarantined()` (`quarantine_status("<target>.<column>")`, the `trellis.column_status` table) on a `live` definition | One field failed for too many rows (Postgres couldn't compute it: an overflow, a division by zero, a constant Postgres fails while planning), so its column fuse tripped. Rows already written keep their last value of it, new rows get `NULL` for it, and its other fields keep updating | Fix the cause `sample_quarantined("<target>.<column>")` shows, then `RESUME TRANSFORM <target>.<column>`, which rebuilds that column |
| `held_keys` | Keys that keep failing to apply are held, with their target rows frozen | Fix the cause, then [release them](#releasing-held-keys) |
| `drain_failure` | A page that fails in a way no key can be charged for is held back, and every watermark token taken since waits on it | Fix what the error names; the next pass commits the page ([A page that fails only for several definitions together](#a-page-that-fails-only-for-several-definitions-together)) |
| `backfill_failure` on a definition that isn't paused | A build or correction chunk keeps failing | Fix what the error names; it retries |
| `capture_wait` | Installing or widening capture is waiting for a table lock | Wait for the lock holder it names |
| A `self_check` report with divergences | The target differs from a recompute of it | Find the gap in [known correctness gaps](known-correctness-gaps.md) and use its repair |
| `version_handover`, and state-changing calls refused with the `version_handover` code | A Trellis process of another version is live on the instance: this version's background processes stand by and its changes refuse ([version handover](decisions/0023-version-handover.md)) | Stop the other version's processes; this version takes over within a standby poll (5 seconds), or the reclaim TTL (30 seconds) after a process killed without a clean shutdown ([rolling deploys](embedding.md#rolling-deploys)) |
| `migrations_pending` with `cannot_run`, or `status()` refused with the `version_handover` code naming pending migrations | This version's migrations haven't run: a process of another version is still live, or this process's `migrate` was refused (`cannot_run` says why, often a role that can't alter Trellis's tables). Its background processes stand by, checking every 5 seconds, and try a refused `migrate` again a minute later | Stop the other version's processes; for a refusal, fix its cause or run `migrate` with a role that can ([upgrading](upgrading.md#upgrading)) |
| `below_catalog_floor`, and `migrate` and state-changing calls refused with the `version_floor` code | The catalog floor, the oldest Trellis version that can read the catalog, is above this version: a later release migrated the database ([ADR-0024](decisions/0024-compatibility-floors-and-rollback.md)). This version's background processes stand by meanwhile | Run the version the message names, or a later one ([the catalog floor](upgrading.md#the-catalog-floor)) |

## Repair tools

`PAUSE`, `RESUME` and `DROP` are statements: run them through
`Trellis::apply` or `trellis apply '<statement>'`.

### `request_backfill`

`request_backfill(source_table)` (Rust, Ruby and Elixir) queues a re-read of
one source table for every definition that reads it, without pausing them.
Each reader reports `backfilling` and is rebuilt in place, and returns to
`live` when the rebuild is done. It's the cheapest repair when you know which table missed changes.

It takes the table's `schema.table` name, or a bare name when exactly one
captured table carries it; a bare name two captured tables share is refused,
naming both. It repairs what a rebuild repairs, with the same exceptions
(below).

### `PAUSE` and `RESUME`

`PAUSE TRANSFORM <target>` freezes a definition, holding its last values
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md)).
`RESUME TRANSFORM <target>` rebuilds it from its sources, returning it to
`waiting_to_backfill`. A `PAUSE` of a definition whose resume is still
waiting (below) withdraws the resume, and its `capture_failure` says so.

**What a resume checks.** A resume first runs define's validation against the
live schema, and refuses while define would (a column it reads is gone or
retyped into something it can't use), leaving the definition paused and
untouched; the error, `ApplyError::ResumeRefused`, is define's own, naming
the column and what to change. A whole-transform resume also
refuses, as define does, while a source index the definition's `self_check`
pages by is missing, unless it was defined `WITHOUT SOURCE INDEX`; create the
index the message names, then resume. It also refuses in two cases
define doesn't, where the only repair is to `DROP TRANSFORM` and define again:

* its target table is missing;
* its ledger table is missing.

A `JOIN` has no ledger. Its resume drops and recreates its hidden tables
instead, keeps the target, and rebuilds both sides and the target's groups
from the sources, in one transaction, or per side and leaf when a side is a
partitioned table with rows. Its build rebuilds the target's readers when it
commits, so they need no pause. The resume of an aggregate, which has no
ledger either, drops and recreates its hidden partials table and keeps the
visible target, which its rebuild rewrites; its build rebuilds the target's
readers likewise. A field resume
(`RESUME TRANSFORM <target>.<column>`) runs define's validation and the
source-key check, but not the ledger check, since it rebuilds nothing, and
refuses a field whose capture is still being widened.
It also refuses while the definition is paused, so when one field's failures
trip both its column fuse and the whole-transform fuse, run `RESUME TRANSFORM
<target>` first and then `RESUME TRANSFORM <target>.<column>`.

**A changed key.** A resume takes each table's key as define would now
([transforms — the key](transforms.md#the-key-a-transform-keys-each-table-by)).
An aggregate or `JOIN` reading a table by its primary key takes the primary
key the table has now, for every transform that reads the table, and
rebuilds; with no primary key left it refuses rather than take another unique
index, until you add one. A `KEY (…)`'s columns never change: its resume refuses until a unique
index backs them again. A target that lost the index over its key refuses
until a unique index over exactly its key columns is created again
(`UNIQUE NULLS NOT DISTINCT` if one can be `NULL`). Each refusal names the
table, the key and the unique indexes it has.

A 1-1 target is keyed by its source's key (a 1-1 `JOIN`'s, by its `FROM`
table's), so its resume first re-keys the target in place to the new key,
under the rebuild's lock, keeping the table, your indexes and grants:

* A new key column the transform doesn't select is added as define copies
  one (its type with no modifier, and its collation), which empties the
  target until the rebuild.
* A former key column it doesn't create is dropped, and the rows the
  narrower key repeats are deleted until the rebuild writes them again. On a
  plain 1-1 transform, `ALTER TRANSFORM … ADD <column> AS <column>` once it
  is `live` adds it back as a field.
* The primary key is replaced (`DROP CONSTRAINT …, ADD PRIMARY KEY (…)`).

The pause message says which columns the resume will add and drop. The
target's new key is recorded, which pauses the transforms reading the
target, and their own resumes follow these rules over it. The re-key refuses,
naming the conflict, while define would refuse the transform, when a new key
column's name is taken by a column of the target that isn't a copy of it (a
field of that name, or a column you added), and when a view, index or foreign
key of yours depends on a column it would drop or on the key constraint it
replaces. Drop the object it names, or drop or rename your column; for a
field of the new key column's name, restore the key, resume, and rename the
field with `ALTER TRANSFORM`; or `DROP TRANSFORM` and define it again.

**What a resume changes.**

* It brings every column Trellis created with a type from the source back to
  the type define would give it now: key, passthrough and `GROUP BY` copies,
  calculated and aggregate columns. The `RESUME`
  returns at once; the definition stays `paused`,
  its `capture_failure` reading "resuming: …", until the staging worker has
  re-typed them (an `ALTER … TYPE` that waits for the table's lock) and
  started the rebuild. A re-type that fails leaves its
  error on `capture_failure` and the target's rows in place. When values
  can't be converted (a key moved from `text` to `uuid` by a `USING` that
  isn't a cast), the repair is to `DROP TRANSFORM` and define it again:
  Trellis doesn't empty a target the application reads on its own. A
  re-type of a target pauses the definitions chained off it whose own
  columns were typed from the old type: their `capture_failure` names the
  upstream's resume, and the staging worker resumes them once the upstream is
  `live` again ([paused definitions](#paused-definitions)).
* A definition that reads another definition's target while that one isn't
  `live` waits for it: the `RESUME` returns at once, and the definition stays
  `paused`, its `capture_failure` reading "resuming: waiting for transform …
  to be live", until the upstream is; then the staging worker resumes it. An
  upstream `catching_up` only for a paused column it reads doesn't hold it.
* A definition paused for an enum relabel waits for the changes staged
  before the relabel to drain: the `RESUME` returns at once, and the
  definition stays `paused`, its `capture_failure` reading "resuming:
  waiting for the changes staged before an enum value … was renamed to
  drain", until none is pending. A target keyed by a source key with an
  enum column is then emptied and rebuilt as define builds it.
* It re-reads every current source row and rewrites the target from it. It
  doesn't replay the changes it skipped while paused (the change stream is
  drained for the definition's siblings meanwhile), so its cost scales with
  the data, not with the length of the pause.
* It releases every key the definition holds in quarantine, and only that
  definition's. A key whose cause is still there fails again and is held
  again.

**What a rebuild doesn't repair.** A rebuild repairs anything that's wrong
because a change was missed. On a 1-1 target it also deletes every row no
source row backs, so it repairs rows edited, deleted, added or re-keyed by
hand, and rows left under the old key of a `varchar` key column narrowed
in place. It keeps a hand edit of a `JOIN`'s hidden tables, which a `PAUSE`
and `RESUME` recreates.

`PAUSE`, `DROP TRANSFORM` and define it again repairs it
([known correctness gaps](known-correctness-gaps.md): [Rows written into a target, a hidden table or Trellis's schema](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema),
[A column's values rewritten in place by `ALTER COLUMN TYPE`](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type)).

### `DROP TRANSFORM` and define again

`DROP TRANSFORM <target>` removes a paused or quarantined definition with
its target, its side tables and its held keys
([ADR-0014](decisions/0014-pause-and-drop-a-transform.md#drop-always-removes-the-associated-data)).
It refuses a definition in any other status, and one another definition
still reads. It first waits for any drain page still writing the definition
that was planned before the pause. Defining it again builds the target from
empty. Indexes and
grants on the target go with it, so give readers `SELECT` through default
privileges
([recommendations](recommendations.md#application-roles-read-targets-and-nothing-else)).
It's the repair when a resume refuses, when copies can't be re-typed, and
when a rebuild keeps something wrong.

### Releasing held keys

A key that keeps failing to apply is held for the definition whose apply
failed, with its parked changes and the error that poisoned it
([ADR-0003](decisions/0003-quarantine-storage-and-api.md#releasing-held-keys)).
That definition's target row for the key stays as it was, and a held key with
changes parked holds back every watermark token taken since. Every other
definition reading the same source keeps applying the key, so two definitions
can disagree on it. A key that fails only when several definitions apply it
together is held for each definition in every failing combination, though
only one of their writes may be at fault.

The definition's `held_keys` reports how many it holds and since when,
whatever its status, `live` included, and `self_check` reports them with every
audit. What clears a held key:

* `Trellis::release_key(transform, source_table, key)` (`trellis release` on
  the CLI, `release_key` in the bindings), once its cause is fixed: every
  definition reading it re-derives it from its current row.
* `RESUME TRANSFORM` releases every key the definition holds.
* `DROP TRANSFORM` deletes them with the definition.
* The automatic retry, off by default (`TrellisOptions::held_key_retry`),
  releases due keys with a growing backoff
  ([ADR-0003 — Retry policy](decisions/0003-quarantine-storage-and-api.md#retry-policy)).
* A build of the target the definition reads (an automatic rebuild,
  `request_backfill`, a capture re-install or a `RESUME` of that upstream)
  retries every key the definition holds once it completes, so a key held
  for a failure the upstream caused while it was being rebuilt clears by
  itself. A key whose failure persists is held again after one attempt, and
  a paused definition keeps its keys.

A released key whose cause is still there is held again. A release first
waits for the drain pages in flight on the key's table to commit, so it can't
miss a change one of them is parking for the key. If that wait runs past the
lock timeout every Trellis connection has (30 seconds at most), the release
fails with a `timeout` error that says so, and changes nothing: the key is
still held, and running the release again releases it once the pages commit.

`trellis status` prints each held key, its table and its transform quoted for
the shell where they need it, so they paste into `trellis release` as
printed: text goes inside `'...'`, with a quote written `'\''`, and a control
character, such as the separator between a composite key's columns, is
written `$'\ooo'` in octal, which bash and zsh read. A value that is exactly
`-h`, `--help`, `-d` or `--database-url` goes after a `--`: `trellis release
-- <transform> <table> <key>`.

After a type a definition reads is renamed or moved to another schema, the
capture pass records its new name and then releases the keys held for a cast
by the old one (`42704`), in that pass and each pass for a minute more. A key
held later than that stays held.

### A page that fails only for several definitions together

A trigger or constraint you add can refuse a change only when several
definitions' targets are written in one transaction. The drain isolates which
definitions a failing change belongs to by applying it without each one in
turn. A single failing pair is charged to both of its definitions, which hold
the key. When the change fails in combinations no one definition is in every
one of (two separate pairs, `a` with `b` and `c` with `d`, or any two of
three), leaving any one out still leaves a failing combination, so it's
charged to nobody. The page is a drain holdup: the drain fails on it every
pass, every target reading the page's tables stops short of it, along with
every watermark token taken since, and `drain_failure` and `self_check`
report it ([ADR-0003](decisions/0003-quarantine-storage-and-api.md#retry-policy)).

Fix the trigger or constraint, and the next pass commits the page. Or pause
one definition from each failing combination: once some definition is in
every combination left, isolation charges the ones it pins, and they hold the
key until it's released or they're resumed.

### `self_check`

`self_check` finds a target that differs from its definition, and repairs
nothing. It's a background job
([embedding — `self_check` is a background job](embedding.md#self_check-is-a-background-job)).
It compares every shape of target, hidden tables included; which wrong rows
it can find is the "Detected by" column of
[known correctness gaps](known-correctness-gaps.md#summary).

## Rewrites that rebuild by themselves

A statement that rewrites a read column's values without firing a trigger
makes the staging worker's next reconcile pass rebuild every transform
applying from the table, pausing none. `ALTER COLUMN … SET EXPRESSION` on a
stored generated column (PostgreSQL 17 and later) is one: the transforms come
back with the values of the new expression, and need nothing from you. So is a
same-type `ALTER COLUMN … TYPE … USING`, except in the shapes
[known correctness gaps](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type)
lists. The pass logs a `WARN` naming the table, the cause and
the transforms it rebuilt, and counts
[`capture_auto_rebuilds_total`](observability.md#metrics).

Each rebuild is one full re-read of the table per transform applying from it,
the same as a [`request_backfill`](#request_backfill) of the table: a few
minutes at 100M rows (about 5 minutes with no writers, about 16 with eight
writers at 2,000 writes a second, on our test machine). A `SET EXPRESSION`
that changes no value rebuilds all the same: one to an expression written
differently that computes the same values (`2 * img` for `img * 2`), or one
that sets the expression the column already has.

A rewrite that leaves every value as it was rebuilds nothing: `VACUUM FULL`,
`CLUSTER`, and an `ADD COLUMN` with a volatile default such as
`clock_timestamp()`. Neither does pg_repack, in table or index mode, on a
source or a target: it keeps an equivalent valid index at every moment, so no
index a transform relies on goes missing. Its table mode refuses an
aggregate target whose group column can be `NULL` (the target has no primary
key and no `NOT NULL` unique key); its index mode (`pg_repack -i`) works on
it.

## What a repair costs

* **A resume that re-types copies holds `ACCESS EXCLUSIVE` on them.** After a
  widening, the resume's `ALTER … TYPE` on the target (and its ledger) waits
  for every reader of the table, and readers
  queue behind it.
* **A re-type changes the table's row type**, so a client that prepared a
  statement returning one of its columns gets `cached plan must not change
  result type` the next time it runs it, once per prepared statement.
* **`integer` to `bigint` rewrites the table**, so at a billion rows reads of
  the target block for minutes, and the capture pass waits with it. Trellis
  sets no `statement_timeout` and honours the one on its role or database: a
  rewrite that outlasts it is cancelled, rolled back and retried on the next
  pass, and the third cancellation ends the request. The definition stays
  `paused`, its `capture_failure` naming both remedies: raise the timeout for
  Trellis's role or database, restart Trellis (the setting reaches only new
  connections) and `RESUME` again, or `DROP TRANSFORM` and define it again
  ([recommendations — `statement_timeout`](recommendations.md#statement_timeout)).
* **A resume that re-types a target pauses the definitions chained off it,
  and they rebuild one after another.** A definition reading the target whose
  own columns were typed from the old type pauses, its `capture_failure`
  naming the upstream resume, and the capture pass resumes it once the
  upstream is `live` again. Its resume re-types its own target, which pauses
  the next definition down, so a chain of depth k stays paused below the head
  for k full rebuilds in a row. A definition resumed while an upstream isn't
  `live` waits the same way, so a stack resumed at once rebuilds one level at
  a time. A `PAUSE` of a waiting definition withdraws its resume. One whose
  resume is refused once its upstream is live, because the schema changed
  after the pause, stays paused with the refusal and isn't tried again:
  resume it by hand once that's fixed, or drop it and define it again.

## Encrypted connections

Trellis connects to Postgres with TLS when the connection string says so,
and tries TLS first when it says nothing (`sslmode=prefer`). The options, the
rules for a unix-socket host and the file handling are in
[Embedding Trellis](embedding.md#tls-to-postgres). What an operator needs to
know:

* **Check, don't assume.** `prefer` keeps working against a server that has
  no TLS, in plaintext. Set `sslmode=require` (or `verify-full`) where
  plaintext would be a fault, and confirm with `pg_stat_ssl` that Trellis's
  sessions report `ssl = true`.
* **Environment-only setups.** When the connection comes from `PGHOST`
  and friends, `PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT` and `PGSSLKEY` set
  the TLS options; with `--database-url` or `TRELLIS_DATABASE_URL`, put them
  in the URL.
* **A server that only accepts TLS** (`hostssl` entries in `pg_hba.conf`)
  refuses `sslmode=disable` with a "no pg_hba.conf entry ... no encryption"
  error.
* **Rotating certificates.** A new server certificate needs nothing from
  Trellis. A new client certificate or CA file needs a restart of the
  process, or a new `Trellis` or `Client` handle, because the files are read once.

## Partitioned sources

[Transforms — Partitioned sources](transforms.md#partitioned-sources) says what a
partition operation does to the counts and how to avoid the window. This is what
each one costs and what to expect around it
([ADR-0017](decisions/0017-partitioned-sources.md)).

* **A correction costs the work of the groups that route to the partition.**
  The correction of a dropped partition of 1,000,000 rows over 50,000 groups
  took 1.46 s in the engine, one transaction with 19 MB of WAL, plus 0.05 s and
  2 MB to settle the target and a 0.30 s pass to detect the drop (release
  build, PostgreSQL 17, a 1,000-leaf tree on disk). The correction of a
  `DROP`, `DETACH` or `TRUNCATE` took 27-34 ms and about 0.6 MB of WAL for a
  partition of 1M or 10M rows over 1,560 groups (a debug build), and the
  correction of an `ATTACH`, which also reads the partition's rows, took
  0.5-0.6 s at 1M rows and 3.9-4.7 s at 10M. Moving rows with DML through the
  parent instead costs about 4-5 s and 0.5-0.6 GB of WAL per 1M-row partition
  and has no window.
* **A retention `DROP` of a large partition holds the definition out of `live`
  only for the correction.** The definition reads `backfilling` with the
  partition in `correcting` until it drains, and every other partition's
  groups stay exact.
* **A `DROP` or `DETACH` of a partition can deadlock against a rebuild or a
  page, and Postgres may abort your DDL with `40P01`.** The DDL takes the
  parent `ACCESS EXCLUSIVE`, then waits for the partition. A per-partition
  rebuild of a group-basis aggregate or a `JOIN` (a build, `RESUME` or rebuild
  in place), or a group-basis correction of another change to the partition,
  holds the partition while it waits for its groups, and a drain page
  holding those groups can wait for the parent, so the three wait on each
  other. Postgres breaks the cycle by aborting one of them. The job and
  the page retry. If the DDL is the one aborted, you get `deadlock detected`
  (`40P01`): run the DDL again. Nothing is lost. A `TRUNCATE` of the partition
  doesn't lock the parent and takes no part in this.
* **Installing capture on a large tree stalls writers briefly, once.** A tree
  of up to 100 nodes installs in one transaction, which blocks writers for up to
  about 50 ms (34-44 ms at 100 leaves). A larger tree installs node by node: the
  slowest application-writer commit at 1,000 leaves was typically 12-65 ms, with
  occasional spikes of 0.2-1.5 s, and the install took 1.6-4.6 s in all, against
  0.2-0.7 s in one transaction.
* **A pass over a 1,000-leaf tree takes about a quarter of a second.** The
  fingerprints take 2.6 ms and a steady pass 233-252 ms. A build statement holds
  about 2,000 locks and plans in 25-28 ms, within the default lock table.
* **Some PostgreSQL minor releases refuse to `ATTACH` a table that still
  carries the capture triggers.** 16.8 refuses (`trigger
  "trellis_capture_delete" prevents table … from becoming a partition`), while
  15.19, 16.15, 17.11 and 18.6 attach it. On a server that refuses, drop the
  table's `trellis_capture_*` triggers before attaching it. The next pass
  installs capture on it.
* **A logical dump and restore** pauses every definition of the database, with
  a `capture_failure` naming the restore, discards the changes staged before it
  and the corrections queued, and runs no correction from the recorded
  partition state: the dump's partition oids and `HASH` bounds are no longer
  the live ones. `RESUME` each definition, which rebuilds it from the restored
  source ([After a restore or an upgrade](#runbooks)).
* **`pg_upgrade` needs nothing.** The pass sees the new cluster, compares the
  recorded partitions by oid and bound, and records the upgrade's reset
  `pg_inherits` and `pg_class` `xmin`s and its rewritten bound texts without
  reading any partition as new, so no rebuild or correction runs. Let a pass run
  after partition maintenance before an upgrade, and after the upgrade before
  more maintenance
  ([known correctness gaps](known-correctness-gaps.md)).
* **pg_partman's maintenance needs no operator step on Trellis's side.** The
  opt-in test tier (`trellis/tests/pg_partman.rs`, [testing
  strategy](testing-strategy.md)) drove pg_partman 5.2.4 on PostgreSQL 15.19,
  16.15, 17.11 and 18.6 through six modes (time daily, time without a
  `DEFAULT`, id range, id list, epoch seconds, and time with id
  sub-partitions), with premake, `run_maintenance`, `partition_data_*`,
  retention by detach and by drop, `undo_partition`, its background worker
  running beside Trellis's pass, and concurrent writers through the parent and
  straight to a partition. The readers matched the source after every step and
  none paused. Rows left in pg_partman's `DEFAULT` within its premake horizon
  make pg_partman's own `run_maintenance` fail on `ATTACH`, and its worker fail
  on every interval, until `partition_data_*` moves them, with or without
  Trellis. Other partition managers and pg_partman versions are not tested.

## Runbooks

* **Upgrading or rolling back Trellis:** follow
  [upgrading and rolling back](upgrading.md). In short, start the new version
  beside the old (it stands by, and `status()` shows `version_handover`), and
  stop the old: the new version runs its migrations itself and takes over (a
  worker whose role can't alter Trellis's tables stands by until another role
  runs `migrate`, and `status()` shows why in `migrations_pending`). Before rolling back, drop,
  under the newer release, each definition whose `floor` is above the release
  you roll back to; the older release pauses any you leave, and only a
  release that meets the floor resumes or drops them
  ([before rolling back](upgrading.md#before-rolling-back)).
* **After a table swap, a bulk load with triggers disabled, or a fixture load
  that ends with `ENABLE TRIGGER ALL`:** the readers of the table are paused.
  `RESUME` each one, which rebuilds it
  ([embedding](embedding.md#what-the-staging-worker-needs-from-the-database)).
* **After a restore or an upgrade:**
  * a `pg_upgrade` needs nothing, and a whole-database physical backup or
    PITR needs only a `request_backfill` of each unlogged source
    ([known correctness gaps](known-correctness-gaps.md#an-unlogged-source-emptied-by-a-crash-or-failover));
  * a logical dump (`pg_dump`, `pg_restore`), into another cluster, another
    database or over the same one, pauses every transform of every kind on
    the capture pass's first look, with a `capture_failure` of kind
    `disruption` that says the database was restored from a logical dump, and
    discards the changes staged before it. Nothing rebuilds by itself: `RESUME`
    each transform. The resume resets the ordering state the dump brought
    (the ledger's positions, a group basis's snapshots, truncate floors) and
    rebuilds the target from the restored source, so an update, a delete or a
    group move of a key that existed before the dump applies afterwards. A
    restore into the same cluster costs the same resumes, though its counters
    only move forward;
  * after a logical restore, start a staging worker. A drain-only client
    (`staging: false`) that starts before it claims nothing until the
    staging worker's first pass has responded. One already running when the
    database is restored under it can apply rows staged before the detection,
    until a staging pass runs; the `RESUME` rebuild overwrites them;
  * the sources restored alone pause their readers: `RESUME` them;
  * Trellis's schema or the targets restored without the rest need the
    affected transforms paused, dropped and defined again, or the whole
    database restored from one backup.
* **Moving an instance's definitions to another database** (a staging copy, a
  fresh test database, another cluster), without its data:
  1. `trellis dump --out dump.sql` against the database. It runs `pg_dump
     --schema-only --no-owner` (version 15 or later, no older than the
     server, the first on `PATH`) over every schema but Trellis's own,
     leaves out the 1-1 targets' ledgers and the capture triggers on source
     tables, and appends one line per definition
     ([schema dumps](embedding.md#schema-dumps)). It doesn't migrate the
     database, and refuses a database with another Trellis instance's
     schema in it. `--definitions-only` writes just those lines, to append
     to a dump built through the API
     ([your own dump](embedding.md#schema-dumps)): a plain `pg_dump` keeps
     the capture triggers, and the file doesn't load.
  2. `createdb` the new database, then `trellis restore dump.sql` against
     it. It refuses a database with anything in it but an empty `public`
     schema, loads the file with `psql` in one transaction (nothing is
     loaded if a statement fails), then migrates, which restores each
     definition into its dumped target, unbuilt and unpaused.
  3. `trellis run` against the new database builds every definition from
     its source rows.

  `TRELLIS_SCHEMA` has to name the same Trellis schema on both sides, and
  `TRELLIS_TARGET_SCHEMA` has to resolve each definition's bare names to the
  tables the dump recorded; a definition that doesn't, or whose target can't
  be adopted, fails the restore naming it. The ones before it stay restored,
  and the next command that migrates (`trellis run`, `trellis apply`)
  restores the rest once the cause is fixed. `pg_dump` 15.14, 16.10, 17.6
  and later write `\restrict` lines with a key chosen per run, so two dumps
  of the same database differ in those two lines; the definitions are the
  same bytes. Loading those lines needs `psql` from the same releases on;
  an older one loads nothing.
* **After a crash or a failover,** `request_backfill` each unlogged source
  ([known correctness gaps](known-correctness-gaps.md#an-unlogged-source-emptied-by-a-crash-or-failover)).
* **After an `ALTER COLUMN … TYPE … USING` that changes a read column's
  values,** `request_backfill` the table
  ([known correctness gaps](known-correctness-gaps.md#a-columns-values-rewritten-in-place-by-alter-column-type)).
* **After granting a missing privilege or `BYPASSRLS`:** `RESUME` the paused
  definitions. A drain holdup on a refusal commits on the next pass, which
  clears its `drain_failure`.
