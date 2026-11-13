# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is two separately-chosen pieces: the
**granularity** of the target's primary-key space, and the
[**calculated fields**](expressions.md) that populate its non-key columns.

Trellis maintains these tables **incrementally** as source data changes. This
document is the logical model: how changes propagate is
[data-flow](data-flow.md), and why asynchronously is
[0002-async-data-flow](decisions/0002-async-data-flow.md).

## Statements

`Trellis::apply(text)`, or `trellis apply '<statement>'` from a shell, takes
one statement, with no `;`. Keywords are case-insensitive.

```text
TRANSFORM <table> [ADOPT EXISTING TABLE]
  FROM <table> [KEY (<column>, ...)] [[AS] <alias>]
  [<join> ...]
  [GROUP BY <column>, ...]
  [WITHOUT SOURCE INDEX]
  SELECT <expression> AS <field>, ...
  [WHERE TRUE]

<table> ::= [<schema>.]<name>
<join>  ::= [INNER | LEFT [OUTER]] JOIN <table> [KEY (<column>, ...)] [[AS] <alias>]
              ON <side>.<column> = <side>.<column> [AND ...]
<side>  ::= <table> | <alias>

ALTER TRANSFORM <transform> <change>, ...
<change> ::= ADD <expression> AS <field> | ALTER <field> AS <expression> | DROP <field>

PAUSE  TRANSFORM <transform> | <name>.<field>
RESUME TRANSFORM <transform> | <name>.<field>
DROP   TRANSFORM <transform>

<transform> ::= <table>, the target
```

- The clauses between `FROM` and `SELECT` choose the
  [granularity](#granularity): none is 1-1, `GROUP BY` an aggregate, one
  `JOIN` with `GROUP BY` a join aggregate, `JOIN` alone a 1-1 `JOIN` (more
  than one `JOIN`: a star).
- `ADOPT EXISTING TABLE` builds into an existing empty table instead of
  creating one ([details](#a-target-whose-name-is-taken)), and `KEY (...)`
  keys a table by those columns instead of its primary key
  ([details](#the-key-a-transform-keys-each-table-by)).
- Aliases are for a `JOIN` only. There every column, in `ON`, `GROUP BY` and
  the fields, is written `<side>.<column>`, and `ON` takes only equalities
  joined by `AND`.
- `WITHOUT SOURCE INDEX` (with `GROUP BY` only) skips
  [the index `self_check` pages by](#a-source-without-the-index-its-self_check-walk-pages-by).
- `<expression>` is the [expression grammar](expressions.md); `<field>` names
  a target column.
- `WHERE` takes only `TRUE`: every source row participates
  ([#804](https://github.com/salesforce-misc/trellis/issues/804)).
- `ALTER TRANSFORM` edits the fields of a 1-1 transform of one table, and
  `<name>.<field>` pauses or resumes one field
  ([Changing a definition](#changing-a-definition)).

What a definition may read is the [refusal table](#refusals); why the
statement has this shape is
[0004-transform-definition-grammar](decisions/0004-transform-definition-grammar.md),
with aliases from [0019](decisions/0019-join-transforms.md) and the 1-1 `JOIN`
and star from [0021](decisions/0021-one-to-one-join-transforms.md).

## Supported sources and targets

Trellis reads tables it does not own ([0005](decisions/0005-source-schema-is-user-owned.md)),
so it validates them when a definition is applied and refuses what it can't
maintain correctly, with an error naming the table or column and the fix. These
refusals are the supported-subset contract, not bugs. Each rule is checked for
a transform's source, for each table a `JOIN` reads, and (where noted) for its
target. Trellis never adds a key, index or policy itself.

Each refusal is one row of [the refusal table](#refusals): the condition, the
error, what happens when the condition appears after define, and the remedy. A
row links to an entry under [Details](#details) where it needs more room.
What an expression may contain is refused in [its own table](expressions.md#refusals).

### After define

A condition define refuses can still arise later, through DDL on a table the
definition reads. What Trellis does then depends on what the rule reads, so
the table's "After define" column names one of the classes below, or reads
n/a where the condition matters only at define (a name taken, an upstream not
yet `live`). A pause is
not a loss: the definition keeps its last values, `status()`'s
[`capture_failure`](status-reference.md#capture_failure) gives the reason, and
`RESUME` rebuilds the target from source once the cause is fixed. Every
`RESUME` first runs define's validation against the schema as it is then, and
while define would refuse, it refuses with define's error
([ADR-0027](decisions/0027-operating-principles.md), rule 4). Each reason's
repair is in [operations — Paused definitions](operations.md#paused-definitions).
What a resume checks and changes is in
[`PAUSE` and `RESUME`](operations.md#pause-and-resume). Other ways a source
can drift are in [known correctness gaps](known-correctness-gaps.md).

#### Text

The rule reads only the definition's text, which doesn't change, so the
condition can't arise later. An `ALTER TRANSFORM` is checked as define is.

#### Retype

The rule reads a column's type. When the staging worker's capture pass finds
a column a definition reads retyped, or dropped and added again, it runs
define's validation again
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).
A definition that still validates keeps running. Its [floor](#floors) is recorded again over the new types, and its target is
rebuilt in place unless the change was a pure widening. A definition pauses
when define would now refuse it, when it would evaluate the column as another
type (`integer` to `double precision`), or when the new type outgrew a column
Trellis copied ([the widening entry](#a-column-trellis-copies-widened)). A key
column (a source key, `GROUP BY` key or `ON` column) whose new type or
collation define refuses, or whose stored keys now render differently, pauses
its readers too. A source key's collation changed to another deterministic
one pauses nothing, though the rest of any build then scans the source, and
`self_check` reads the source by scan rather than by index (it pages under the
target key's collation). A read column renamed or dropped pauses its readers
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-renamed-or-dropped-column)).

#### Shape

The rule reads the table's shape: its place in a partition or inheritance
hierarchy, its partition tree, its row-level security, a subscription into it,
or whether a column it reads is virtual. The staging worker's next reconcile
pass (every 5 seconds by default) pauses every definition reading the table
that the new shape breaks, and `self_check` reports a `capture` divergence.
The resume's rebuild repairs any writes that capture missed before that pass.
Row-level security on a target also pauses the definition that writes it. On
a role a drain logs in as, the refused read or write halts the definitions it
reaches on the next drain (`capture_failure` kind `halt`). Only a
subscription's apply worker escapes capture: a session with
`session_replication_role = replica` fires the capture triggers, which are
`ENABLE ALWAYS` ([stage 1](staging-and-claiming/01-capture-by-triggers.md)).

#### Key

The rule needs a key, or a unique index that backs it. These pause with no
grace period, whether or not a write arrived:

- a declared `KEY (…)` left with no valid backing index: every reader pauses
  on the next pass;
- a 1-1 `JOIN`'s joined table losing the unique key on its `ON` columns: the
  `JOIN` pauses on the next pass;
- a primary key redefined, or the table created again with another one: the
  readers keyed by it pause on the next drained change or the next pass.

A primary key made deferrable halts its readers on the next drain of a change,
unless a unique index that isn't deferrable is over exactly its columns, which
the drain then keys the table by. A dropped primary key leaves the table
uncapturable (a capture holdup in `status()`), and halts its readers on the
next drain of a change to it. A table read through `KEY (…)` is keyed by it
whatever its primary key does. A target that lost the index over its key has
no detector: `RESUME` and a rebuild refuse until the index is back, and
Trellis never keys the target by another index. These pause nothing: an
equivalent index built before the old one is dropped, `REINDEX CONCURRENTLY`,
a rename and pg_repack.

#### Index

The rule needs an index for speed, not for correctness
([ADR-0005](decisions/0005-source-schema-is-user-owned.md)). When the index a
`self_check` walk pages by is dropped or invalid, nothing pauses. `status()`
lists it in [`source_index_gaps`](status-reference.md#source_index_gaps), and
unless the definition was defined `WITHOUT SOURCE INDEX`, `self_check` refuses
to walk it and a whole-transform `RESUME` refuses. A `MIN`/`MAX` probe
index is listed in
[`extreme_index_gaps`](status-reference.md#extreme_index_gaps). Once no valid
one has existed on any pass for 5 minutes, the transform pauses. A restart or
a new leader starts that clock again.

#### Disruption

The condition can't exist at define: the table was dropped, renamed, swapped
or replaced, or its capture was changed outside Trellis. The next pass pauses
every reader with a `capture_failure` of kind `disruption`. A logical restore
of the database pauses every definition.

#### Resume only

Nothing detects the change, and the definition keeps running. `RESUME`
refuses with define's error until the change is undone.

#### Per row

Postgres refuses the computation for each changed row, so the row's key is
held ([`held_keys`](status-reference.md#held_keys)). The transform is
quarantined once its held keys reach the fuse threshold. Nothing pauses.

### Refusals

| Condition | Error | After define | Remedy |
|---|---|---|---|
| **Source tables and partitioning** | | | |
| A table the role Trellis runs as can't use: it lacks `USAGE` on the table's schema, or neither owns the table nor holds a privilege on it | `TableNotAccessible` | [Shape](#shape): a revoked table privilege or schema `USAGE` halts the readers on the next drain; a revoked schema `USAGE` also fails the capture pass for every table until granted back ([#1925](https://github.com/salesforce-misc/trellis/issues/1925)) | `GRANT USAGE ON SCHEMA`, or membership in the owner ([roles](recommendations.md#one-trellis-role)) |
| A partition (a leaf or a middle), a partitioned root that is itself a partition, or a table in an inheritance hierarchy: a statement trigger fires only for the table a statement names | `SourceNotChangeKeyed` | [Shape](#shape) (`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or a drop and recreate as one) | Use a plain table; after a change, `DETACH PARTITION` or `NO INHERIT`, then `RESUME` |
| [A source dropped, renamed, swapped, or its capture disrupted](#a-source-dropped-renamed-swapped-or-its-capture-disrupted) | n/a | [Disruption](#disruption) | Put the table back, then `RESUME` |
| A partitioned root ([0017](decisions/0017-partitioned-sources.md)) whose partition key, at any level, the group key omits: an aggregate's `GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from that table | `PartitionedSource`, naming the column | [Shape](#shape): a sub-level added later | Add the column to the `GROUP BY` or join on it; after a change, detach the sub-level, then `RESUME` |
| A partitioned root whose key level is an expression, or whose partition-key column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a non-default operator class or a nondeterministic collation | `PartitionedSource` | [Shape](#shape): a sub-level added later | Partition by a plain listed column; after a change, detach the sub-level, then `RESUME` |
| A partitioned root with more than 1,000 leaves, or on PostgreSQL before 15: every build statement opens every leaf | `PartitionedSource` | [Shape](#shape) | Merge partitions to 1,000 or fewer leaves, or upgrade the server |
| A partitioned root as a 1-1 transform's `FROM` (partitioned targets are not supported) | `PartitionedSource` | [Disruption](#disruption): the source dropped and created again partitioned; `RESUME` then refuses | Aggregate over it, or use a plain table |
| **Source keys** | | | |
| An aggregate target, or a 1-1 target over one, as another Trellis instance's source: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint, not a primary key. Inside the owning instance it can be chained off | `NoPrimaryKey` | [Disruption](#disruption): only by the table being replaced by such a target; `RESUME` then refuses | Chain within the instance ([instance-identity](instance-identity.md)), group over a 1-1 transform, or name the group key with `KEY (…)` |
| [The key a transform keys each table by](#the-key-a-transform-keys-each-table-by) | `NoPrimaryKey`, `RecordedKeyMissing`, `DeferrablePrimaryKey`, `KeyNotBacked`, `KeyOnTrellisTarget`, `KeyDiffersFromReaders`, `UnsupportedPrimaryKeyType` | [Key](#key) | Name a `KEY (…)` or add a primary key; [details](#the-key-a-transform-keys-each-table-by) |
| **Row-level security and replication** | | | |
| [Row-level security on a role Trellis runs as](#row-level-security-on-a-role-trellis-runs-as) | `RowSecurityApplies`, `ExtremeSourceNotReadable` | [Shape](#shape) | `BYPASSRLS`, or the owner without `FORCE ROW LEVEL SECURITY`; then `RESUME` |
| A table a logical-replication subscription in the same database replicates into, in any sync state or disabled: its apply worker fires only row-level triggers. Publishing a table is fine | `Subscribed` | [Shape](#shape) | Remove the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME` |
| **Generated columns and collations** | | | |
| [A virtual generated column a definition reads](#a-virtual-generated-column-a-definition-reads) | `VirtualGeneratedColumn` | [Shape](#shape) | Make it `STORED`, or write its expression in the definition |
| A nondeterministic collation on a key column (a source key, `GROUP BY` key or `ON` column), on a column passed to `STRPOS` or `REGEXP_COUNT`, or on one compared as text (`=`, `<>`, `!=`, `IS [NOT] DISTINCT FROM`, `NULLIF`), directly or through `COALESCE`, a `CASE` result or a field | `NondeterministicKeyCollation`, `NondeterministicCollationTextFunction`, `JoinNondeterministicCollation` | On a key, [Retype](#retype); on a column only a text function reads, [Per row](#per-row); on a column only compared, nothing detects it ([known correctness gaps](known-correctness-gaps.md#a-read-column-given-a-nondeterministic-collation)) | Keep the collation deterministic ([type-support](type-support.md#collation)) |
| **Retyped and widened columns** | | | |
| A key column (a source key, `GROUP BY` key or `ON` column) retyped so stored values render differently: `timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or `integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal precision or `varchar(n)` | n/a | [Retype](#retype) | `RESUME`, which re-types Trellis's copies and rebuilds |
| [A column Trellis copies, widened](#a-column-trellis-copies-widened) | n/a | [Retype](#retype) | `RESUME`, which re-types the copies and rebuilds |
| **Target columns and widths** | | | |
| Two `GROUP BY` keys that share a target column name (a key listed twice, or a `JOIN`'s `orders.region` and `customers.region`); keys can't be aliased | `GroupByKeysShareTargetColumn` | [Text](#text) | List a key once; for a `JOIN`, join a 1-1 target that renames one side's column |
| A key of more than 32 columns (`INDEX_MAX_KEYS`): a `GROUP BY`, or a `JOIN` side's `ON` and `GROUP BY` columns together; a 1-1 `JOIN`'s `ON` at most 31 | `IndexKeyTooWide` | [Text](#text) | Group by or join on fewer columns |
| [A table of more than 1,600 columns](#a-table-of-more-than-1600-columns) | `TableTooWide` | [Text](#text) | Fewer fields or distinct aggregate arguments |
| **Source indexes** | | | |
| [A source without the index its `self_check` walk pages by](#a-source-without-the-index-its-self_check-walk-pages-by) | `SourceIndexMissing` | [Index](#index) | Create the index the message names, or define `WITHOUT SOURCE INDEX` |
| **`JOIN` transforms** | | | |
| [An unsupported `JOIN` shape](#an-unsupported-join-shape) | `UnsupportedKeySpace`, `TableAlias`, `AliasedTableName`, `JoinSelf`, `JoinTooManyTables`, `JoinOnSameSide`, `JoinOnRepeatedPair`, `JoinUnqualifiedColumn`, `JoinUnknownTable`, `JoinUnresolvedColumn`, `JoinMixedSideArgument`, `JoinFieldShape` | [Text](#text) | Rewrite it in the admitted shape |
| [A 1-1 `JOIN`](#a-1-1-join) | `OneToOneJoinKeyNotUnique`, `OneToOneJoinAggregate`, `OneToOneJoinUnsupported`, `PartitionedSource`, `JoinOnRepeatedColumn` | [Key](#key), [Retype](#retype), [Shape](#shape) | Join `b` on a unique key; give an aggregate a `GROUP BY` |
| [1-1 `JOIN` of more than two tables (star)](#1-1-join-of-more-than-two-tables-star) | `StarJoinTooManyTables`, `JoinOnReadsAnotherJoinedTable`, the 1-1 `JOIN`'s at each position | As for a 1-1 `JOIN`, at each position | At most 5 tables, each joined to `a` on a unique key |
| `ON` columns that differ in type or collation (a modifier never counts; `text` and `varchar` are one type), are off the join-key allowlist in [type-support](type-support.md) (a domain always is), or have a nondeterministic collation; a `GROUP BY` key of a type that can't be one | `JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`, `JoinNondeterministicCollation`, `UnsupportedGroupByKeyType` | [Retype](#retype); a change between deterministic collations pauses nothing, but `RESUME` refuses until they match | Alter one column to match the other, then `RESUME` |
| **Aggregate functions** | | | |
| [`MIN` or `MAX` in a plain aggregate](#min-or-max-in-a-plain-aggregate) | `ExtremeOfExpression`, `UnsupportedExtremeArgument`, `ChainedExtreme`, `ExtremesWithoutSourceIndex`, `ExtremeIndexMissing` | [Index](#index), [Retype](#retype) | Create the index the message names; a plain column |
| A field reading a column that is neither a `GROUP BY` key nor inside an aggregate, or named after a key but not that key (`GROUP BY id SELECT SUM(id) AS id`) | `UngroupedColumnReference`, `GroupingColumnFieldMustBePassthrough` | [Text](#text) | Aggregate the column, or rename the field |
| `MIN` or `MAX` in a `JOIN` transform | `DeferredAggregate` (`MIN/MAX`) | [Text](#text) | Take it in a plain aggregate over one table |
| `bool_and`, `bool_or`, `bit_and` or `bit_or` | `DeferredAggregate` | [Text](#text) | Not supported yet: [#1118](https://github.com/salesforce-misc/trellis/issues/1118) |
| `SUM` or `AVG` over anything but `smallint`, `integer`, `bigint` and `numeric`: a running float sum drifts from Postgres's | `DeferredAggregate` (`float and interval SUM/AVG`), `UnsupportedAggregateArgument` for another type | [Retype](#retype) | An integer or `numeric` column ([#1119](https://github.com/salesforce-misc/trellis/issues/1119)) |
| `array_agg`, `string_agg` or `jsonb_agg` | `DeferredAggregate` | [Text](#text) | Not supported yet: [#1040](https://github.com/salesforce-misc/trellis/issues/1040) |
| **Filters and chaining** | | | |
| <a id="a-where-other-than-true"></a>A `WHERE` other than `TRUE`: partial data is not supported ([#804](https://github.com/salesforce-misc/trellis/issues/804)) | `UnsupportedPredicate` | [Text](#text) | None |
| A target that is its own source, directly or through a [chain](#chaining-and-cycle-detection) | `TargetEqualsSource`, `TableCycle` | [Text](#text) | Another target |
| Chaining off a transform that is not `live` | `TransformNotLive` | n/a | Wait for the upstream to go `live` |
| **Target names** | | | |
| [A target whose name is taken](#a-target-whose-name-is-taken) | `TargetTableExists`, `TargetTableSuffixCollision`, `TargetNotAdoptable` | n/a | Another name, or `ADOPT EXISTING TABLE` |
| A target that would take the name of a table another transform reads as its `FROM` or joined table, one dropped or renamed away since, or one `ADOPT EXISTING TABLE` would adopt: the reader would read the new target's change feed | `TargetNameHasReaders`, naming each reader | n/a: later, a new reader of the name is a chain, and the target can't be dropped while one reads it (`DependentsBlockDrop`) | `DROP TRANSFORM` each reader named (paused first), or another name |
| A target name ending in `__ledger`, or over 55 bytes: its ledger, `<target>__ledger`, must fit Postgres's 63 | `ReservedTargetSuffix`, `TargetNameTooLong` | [Text](#text) | Another name |
| A target in this instance's catalog schema, or in a schema holding another instance's `trellis_instance` table, whose uninstall would drop it ([instance-identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)) | `TargetInCatalogSchema` | [Resume only](#resume-only) | An ordinary schema; after define, `DROP TRANSFORM` and define it there |

### Details

#### A source dropped, renamed, swapped, or its capture disrupted

A source table or a `JOIN`'s joined table can be dropped, renamed away, moved
to another schema, swapped for another table or replaced by a view after
define. Its capture triggers or functions can also be disabled, dropped,
recreated or replaced outside Trellis. Any of these pauses every reader with a
`capture_failure` of kind `disruption`, which names the finding and says that
changes made meanwhile may be missing. Nothing is reinstalled or rebuilt until
a `RESUME`.

On a partitioned source, the root and every node of its tree are checked, and
the finding names the node. A root dropped and created again, swapped by
renames or renamed away is a disruption, as is a plain table created again as
a partitioned one, or the reverse. Partition maintenance (an attach, detach,
new leaf, dropped leaf, `DEFAULT` or `HASH` re-split) is not.

`RESUME` refuses while the table is missing. Otherwise it re-validates the
definition against the table now at that name, reinstalls capture and rebuilds
from that table. If the table was created again with another primary key, what
Trellis stored under the old key is discarded. A 1-1 target is first re-keyed
in place, or the resume refuses
([`PAUSE` and `RESUME`](operations.md#pause-and-resume)). Plan a scripted `RESUME` after a swap, or after a bulk load with
triggers disabled ([embedding](embedding.md)).

#### The key a transform keys each table by

A transform keys each table it reads by the table's primary key, or by the
columns a `KEY (…)` written directly after the table names
(`FROM orders KEY (customer_id, id)`, `JOIN b KEY (…) ON`). Another
transform's target is keyed by the key Trellis gave it. A target is keyed by
its source's key (1-1, and a 1-1 `JOIN` by its `FROM` table's), or by its
`UNIQUE NULLS NOT DISTINCT` group key (aggregate, `JOIN`). The rules and their
reasons are [ADR-0026](decisions/0026-source-keys.md). Define refuses:

- no primary key and no `KEY` (`NoPrimaryKey`, naming both remedies, or
  `RecordedKeyMissing` once a transform has recorded the table's key);
- a deferrable primary key and no `KEY` (`DeferrablePrimaryKey`). Recreate it
  in one statement, as the message says
  (`ALTER TABLE … DROP CONSTRAINT …, ADD PRIMARY KEY (…)`): Postgres can't
  make a key not deferrable in place;
- a `KEY` no unique index backs (`KeyNotBacked`). The index must be valid, not
  deferrable, not partial and over no expression, with exactly the `KEY`'s
  columns in any order, default operator classes and the columns' own
  collations, and every column `NOT NULL` or the index `NULLS NOT DISTINCT`. A
  partitioned table's unique index holds its partition key, so its `KEY` names
  those columns too;
- a `KEY` on another transform's target (`KeyOnTrellisTarget`);
- a key column off the [join-key allowlist](type-support.md)
  (`UnsupportedPrimaryKeyType`);
- a key other than the one the table's other transforms use
  (`KeyDiffersFromReaders`, naming the key to write), so a second transform
  over a `KEY` table repeats the `KEY`.

Trellis records each table's key the first time a transform keys by it, tied
to that table. A unique index added later, one an adopted target already had,
the order a dump restore creates constraints in and `REINDEX CONCURRENTLY`
change nothing. After define the key is the [Key](#key) class. What `RESUME`
takes as the key, and how it re-keys a 1-1 target in place, is in
[`PAUSE` and `RESUME`](operations.md#pause-and-resume).

#### Row-level security on a role Trellis runs as

Row-level security applies to a role when RLS is enabled and the role neither
owns the table (directly or through membership) nor has `BYPASSRLS`, or owns
it and the table is `FORCE ROW LEVEL SECURITY`. A superuser is exempt. Every
Trellis connection runs with `row_security = off`, so a read or write the
policies would filter fails rather than skipping rows.

Define checks a source or target for the role that owns the ring, and checks a
table that is another transform's target for the defining role. A `MIN`/`MAX`
aggregate's source is also checked for the ring owner's `SELECT`
(`ExtremeSourceNotReadable`), at define and on a whole-transform `RESUME`. A
drain logs in as that role or a member of it, and re-reads a group's extreme
from the source when the current one is deleted.

After define, a refused build chunk pauses its transform after its charged
retries, with the error on `backfill_failure`. On a partitioned source, define
and the pass check the root only. A policy on a partition below it reaches
none of the reads that follow writes, which all go through the root. The
correction of that partition reads the partition itself, though, after a
`TRUNCATE` of it, an attach of it with rows, or direct writes into it as a new
leaf. That read is refused, and it pauses the transform after its charged
retries in the same way. The refusals that only log or retry:
[observability — A halting failure](observability.md#a-halting-failure). For
a `MIN`/`MAX` source, the remedy is `GRANT SELECT` on it to the ring's owner.

#### A virtual generated column a definition reads

This is a column defined `GENERATED ALWAYS AS (…) VIRTUAL` (PostgreSQL 18) that
a definition reads: a 1-1 field, a `GROUP BY` key, an aggregate's argument, or
a `JOIN`'s `ON` column or field on either table. A statement trigger's
transition table holds no value for a virtual column, so capture can't image
it. Define refuses it, as do an `ALTER TRANSFORM` that adds such a field and a
`RESUME`, naming the table, the column and its expression. A definition on the
same table that doesn't read the column is unaffected.

A read column can become virtual after define: dropped and added again as
virtual, or another column renamed onto its name. Until the next pass, the
application's inserts and deletes on the table fail
(`unexpected virtual generated column reference`). An update stages the column
as `NULL`, or stages nothing when no other column capture reads changed. The
resume's rebuild repairs the targets. A virtual column renamed onto the name
and back before a pass pauses nothing
([known correctness gaps](known-correctness-gaps.md#a-read-columns-name-swapped-with-anothers-and-back-within-one-pass)).

Write the expression in the definition in place of the column where the
grammar covers it (in a 1-1 transform, under a field name no source column
has), or make the column `STORED`.

#### A column Trellis copies, widened

Trellis creates some columns with a source column's type, and widening that
source column can outgrow them. These columns are:

- a 1-1 target's key, passthrough and calculated columns;
- an aggregate's `GROUP BY` key (copied into its target and its hidden table)
  and its field columns;
- a `JOIN`'s `GROUP BY` keys and `ON` columns (copied into its target and its
  hidden tables, each with its type, no modifier, and its collation), and its
  field columns;
- a 1-1 `JOIN`'s key, its field columns, and the columns of its hidden `R`,
  `M` and `J`.

The widenings are `integer` → `bigint` (and `smallint` up), an integer →
`numeric` (unless every value of the `numeric` fits the integer), and `real` →
`double precision`. A wider modifier is no widening: a copy has none
([ADR-0020](decisions/0020-type-identity.md),
[the types of the columns Trellis creates](type-support.md#the-types-of-the-columns-trellis-creates)).
A change of a modifier (a string bound, a `numeric` precision, a temporal
precision, a move to `text`) needs no re-type, no pause and no rebuild.

After a widening, the staging worker pauses every transform that owns a column
the new type outgrew, naming the source column and each of those columns. A
narrowing pauses nothing here, except a `varchar(n)` key's. `RESUME` makes the
staging worker `ALTER` each column whose type differs from the one define
would give it now, under `ACCESS EXCLUSIVE`, then rebuilds. That is a table
rewrite for `integer` → `bigint`, and reads of the target wait. A `JOIN`'s or
a 1-1 `JOIN`'s hidden tables are created again at the new types instead
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-re-typed-key-column-or-an-outgrown-copy)).

#### A table of more than 1,600 columns

Postgres caps one table at 1,600 columns (`MaxHeapAttributeNumber`). Trellis
creates a target with a column per key and field, and hidden tables whose
width follows the fields too:

- an aggregate's hidden table has its `GROUP BY` columns, a column counting
  each distinct argument, a column summing each one a `SUM` or `AVG` reads,
  and a column for each distinct `MIN` or `MAX` call, with two more for their
  basis and stamp when there is one;
- a `JOIN`'s hidden partials tables do the same, per side and for the groups;
- a 1-1 `JOIN`'s ledger for a side has a column for every column that side's
  fields read.

So about 800 distinct summed aggregate arguments overflow a hidden table that
their target fits in. The error names the table's role and the limit. It is
checked for the definition, `RESUME` and an `ALTER TRANSFORM … ADD`. An
`ALTER TRANSFORM` also counts the columns earlier edits dropped, and the
column a `DROP z, ADD … AS z` replaces, since Postgres counts a dropped column
until the table is created again. If dropped fields filled the limit, drop the
transform and define it again.

An aggregate meets smaller limits first, and define does not refuse them. From
about 270 distinct summed arguments, the statements that build and apply it
exceed Postgres's 1,664-entry target list: its build pauses, or its changed
groups are quarantined, with Postgres's error
([#1518](https://github.com/salesforce-misc/trellis/issues/1518)). From about
500, a hidden table's row can outgrow a heap page
([#1516](https://github.com/salesforce-misc/trellis/issues/1516)).

#### A source without the index its `self_check` walk pages by

This applies to a `GROUP BY` aggregate or a `JOIN` whose source lacks the
index its `self_check` walk pages by. For an aggregate, that is a valid,
non-partial btree index on the plain source columns, whose leading columns are
the `GROUP BY` columns in `GROUP BY` order. For a `JOIN`, each side needs one
whose leading columns are that side's `ON` columns in `ON` order.

A primary key or unique constraint that leads with them counts. These don't:

- an expression or partial index;
- an index under a non-default operator class, such as `text_pattern_ops`;
- an index whose leading columns aren't all ascending with `NULLS LAST` (the
  default) or all `DESC` with `NULLS FIRST` (the `DESC` default, read
  backward), such as `(a, b DESC)`.

A 1-1 target needs nothing, since its walk pages by the primary key. A
`JOIN`'s `T` part, with group columns from both tables, is served by no single
table's index, so it needs none.

The error names the table, the columns, and the part of the definition that
needs the index (the `GROUP BY` columns, or a `JOIN`'s `ON` columns on its
from or joined side). It also gives a ready-to-run `create index`. Writing
`WITHOUT SOURCE INDEX` after the `GROUP BY` or `JOIN` clause
(`TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT …`) opts that
definition out. A definition with a `MIN` or `MAX` field can't opt out: it
needs more ([the `MIN`/`MAX` entry](#min-or-max-in-a-plain-aggregate)). Builds
need no index. An opted-out definition's gap is still listed in
`source_index_gaps`, and its `self_check` still runs, scanning the source once
per page.

#### An unsupported `JOIN` shape

Only `INNER` and `LEFT` equi-joins of different tables are admitted. A join
with a `GROUP BY` takes two tables (a 1-1 `JOIN` may read more:
[the star entry](#1-1-join-of-more-than-two-tables-star)). Every column is
written `<table>.<column>`, or `<alias>.<column>` for a table given an alias.
Each table is held to every rule here, as a `FROM` is. Either table may be
another transform's target, read through that target's change feed. The build
first waits out the transactions that began before it
([data flow](data-flow.md#the-re-derive-build),
[ADR-0019](decisions/0019-join-transforms.md#a-side-may-be-another-transforms-target-through-the-target-change-feed)).

A field is either a `GROUP BY` key from either table, or a composition of
aggregates with literals, operators and functions
(`COALESCE(SUM(items.qty), 0)`). Each aggregate is `COUNT(*)`, or
`COUNT`/`SUM`/`AVG` of a column or of an expression over one table's columns
(`SUM(items.qty + 1)`). Each argument is kept as a running total on its own
table, so an argument that reads both tables has no such total.

Refused, with or without a `GROUP BY` (a join without one is a
[1-1 `JOIN`](#a-1-1-join)):

- **Join type.** `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `LATERAL`, `USING` and
  comma joins (`UnsupportedKeySpace`). For `RIGHT JOIN`, swap the tables and use
  `LEFT JOIN`.
- **Tables.** A table joined to itself, even under two aliases, since one
  captured change would feed both sides (`JoinSelf`). One table joined twice,
  under any alias. Two tables with the same bare name
  (`shop.orders JOIN archive.orders`). With a `GROUP BY`, a third table
  (`JoinTooManyTables`).
- **Aliases.** An alias that is a word Postgres reserves (`AS left`), that the
  other table goes by (its alias or bare name), or that a single-table
  definition gives its `FROM` table (`TableAlias`). An aliased table's column
  read through the table's name, such as `orders.total` after `orders AS o`
  (`AliasedTableName`).
- **`ON`.** Anything but equalities between a column of each table, joined by
  `AND`. That rules out `OR`, `<`, a literal, an expression, two columns of one
  table (`JoinOnSameSide`), a repeated column pair (`JoinOnRepeatedPair`), and
  a column named by two terms (`JoinOnRepeatedColumn`).
- **Columns.** A column without its table (`SUM(qty)`, `GROUP BY region`:
  `JoinUnqualifiedColumn`), or one read through a table that is neither side
  (`JoinUnknownTable`, `JoinUnresolvedColumn`).
- **Fields, with a `GROUP BY`.** An aggregate whose argument reads both tables
  (`SUM(orders.x + items.y)`: `JoinMixedSideArgument`). A field that is
  neither a `GROUP BY` key selected on its own nor a composition of
  aggregates: a column outside an aggregate, a nested aggregate, or no
  aggregate at all (`JoinFieldShape`).

#### A 1-1 `JOIN`

A 1-1 `JOIN` is `FROM a [LEFT|INNER] JOIN b ON … SELECT …` with no `GROUP BY`
([ADR-0021](decisions/0021-one-to-one-join-transforms.md),
[1-1 JOIN](#1-1-join)). Refused:

- `b`'s `ON` columns are not unique (`OneToOneJoinKeyNotUnique`);
- an aggregate field (`OneToOneJoinAggregate`);
- a column that starts with `__` is read (`OneToOneJoinUnsupported`);
- a side is partitioned (`PartitionedSource`);
- the `ON` names one column in two terms (`JoinOnRepeatedColumn`);
- the `ON` reads two columns of `a` whose names share their first 60 bytes;
- `b` is another transform's 1-1 target, joined on columns that leave out part
  of that target's primary key;
- `WITHOUT SOURCE INDEX` (a parse error).

`b`'s `ON` columns must contain a unique key of `b`. That can be its primary
key, a unique constraint, or a non-partial unique index on plain columns
(deferrable, or with `INCLUDE` columns, is fine for this check). It can also
be the group key of an aggregate or `JOIN` target. `b`'s row-identity key (its
primary key or `KEY`) is a separate rule: it must not be deferrable
([the key entry](#the-key-a-transform-keys-each-table-by)), even when a
deferrable key is what makes its `ON` columns unique.

When `b` is another transform's 1-1 target, its `ON` columns must also include
every column of its primary key, so a unique index added to the target never
admits a join on another column. Trellis orders changes per primary-key value
only, so only the primary key is unique at every instant while the upstream
drains. An `a` that is a 1-1 target may be joined on any columns. How the
target is built and kept is in [Granularity](#1-1-join).

After define, a widening of a column it copies (`a`'s key, a field, `R`, `M`
and `J`) is handled as [the widening entry](#a-column-trellis-copies-widened)
describes. A field over both tables is reported against the table whose column
widened, naming every column it is typed from. `b`'s unique key on its `ON`
columns lost is the [Key](#key) class. A duplicate Postgres admits past a
deferrable key under `session_replication_role = replica` leaves the key valid
and pauses nothing
([known correctness gaps](known-correctness-gaps.md#a-duplicate-in-b-past-a-deferrable-key-under-session_replication_role--replica)).
`RESUME` refuses a `b` whose `ON` columns are no longer unique. It re-keys the
target to a changed key of `a` as for a 1-1 transform
([`PAUSE` and `RESUME`](operations.md#pause-and-resume)).

To fix a refusal, join `b` on a unique key: a 1-1 target's, on columns that
include its primary key, or join the table that transform reads. Give an
aggregate a `GROUP BY`. Rename a column of `a` whose name's first 60 bytes
another `ON` column of `a` shares. For `b`'s unique key, restore the key, then
`RESUME`
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).

#### 1-1 `JOIN` of more than two tables (star)

A star is a 1-1 `JOIN` of more than two tables
(`FROM a JOIN b1 ON … LEFT JOIN b2 ON … SELECT …`, [1-1 JOIN](#1-1-join)).
Refused:

- more than 5 tables (`a` and 4 joined tables: `StarJoinTooManyTables`);
- a joined table whose `ON` reads a joined table other than its own
  (`JoinOnReadsAnotherJoinedTable`): a snowflake such as
  `JOIN b2 ON b1.id = b2.id`, or an `ON` that reads `a` and two joined tables;
- anything that breaks a rule of [the 1-1 `JOIN` entry](#a-1-1-join) or of
  [the `JOIN` shape entry](#an-unsupported-join-shape) at any position. The
  error names the table at that position.

The joins read left to right, as Postgres reads them without parentheses, and
each joined table is joined to `a` directly. Every rule of the 1-1 `JOIN`
holds at each position:

- its `ON` columns contain a unique key of its table
  (`OneToOneJoinKeyNotUnique` names that table);
- its row-identity key is not deferrable;
- a joined 1-1 target is joined on columns that include its primary key;
- no table is partitioned (`PartitionedSource`);
- no two `ON` columns of `a` share their first 60 bytes;
- the target isn't named after a table it reads (`TargetEqualsSource`).

Every refusal of the `JOIN` shape holds at every position, each a parse error
that names it: `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `LATERAL`, `USING`, a comma
join, and a table read twice (`a` included) under any alias. A `JOIN` with a
`GROUP BY` still reads two tables (`JoinTooManyTables`). A joined table with
few distinct keys (a lookup) is admitted at any position, and `status()` warns
of its fan-out ([1-1 JOIN](#1-1-join)).

After define, each joined table is handled as the 1-1 `JOIN` handles `b`, at
whichever position it is joined. That includes a value of an enum read there
being relabelled. `RESUME` re-validates every position as define does, and re-keys the
target to a changed key of `a`. Join a snowflake's outer table through a
transform of its own, and join that target.

#### `MIN` or `MAX` in a plain aggregate

A plain aggregate is a `GROUP BY` over one table. `MIN` or `MAX` in one is
refused when its argument:

- is not a plain source column, such as `MAX(a + b)`, a cast or a calculated
  field (`ExtremeOfExpression`);
- is of a type other than `smallint`, `integer`, `bigint`, `numeric`, `real`,
  `double precision`, `date`, `time`, `timestamp`, `timestamptz`, `interval`
  or `oid`, so `text` is refused (`UnsupportedExtremeArgument`);
- is a column of another transform's target (`ChainedExtreme`).

It is also refused in a definition declared `WITHOUT SOURCE INDEX`
(`ExtremesWithoutSourceIndex`). And it is refused when the source lacks, for
each argument, a valid, non-partial btree index that leads with the `GROUP BY`
columns in order, then the argument, under the index rules of
[the source-index entry](#a-source-without-the-index-its-self_check-walk-pages-by)
(`ExtremeIndexMissing`).

The index error names the table, the columns and a ready-to-run
`create index`, one per argument, which also serves as the `GROUP BY` index. A
group keeps each extreme. A deletion or update that removes it re-reads the
group's next one from the source, after the group's lock, as one endpoint of
that index ([type support](type-support.md#min-and-max)). Without the index,
each re-read scans the group. With no `GROUP BY` index either, it scans the
whole source, once per group. A page can then outrun `statement_timeout` and
retry forever, so `WITHOUT SOURCE INDEX` doesn't apply. Another transform's
target has no such index, and building one is a long `CREATE INDEX` on a table
Trellis owns. Take `MIN`/`MAX` of a plain column in the transform that reads
the source (a stored generated column, for an expression).

When the probe index is lost after define, it is the [Index](#index) class.
A probe index lost while the transform runs leaves the target exact. When the
transform pauses, the `capture_failure` names the table, the columns and a
ready-to-run `create index`. On a partitioned source, it also says to re-run
`ALTER INDEX … ATTACH PARTITION` after repairing a leaf's index, whose repair
alone leaves the root's index invalid. A rebuild that drops the old index
first and takes longer than 5 minutes pauses
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).

#### A target whose name is taken

The name is taken when a relation of any kind already exists in the target
schema under the target's name, or under the name of a table Trellis keeps
beside it (`<target>__ledger`). That includes another definition's target, in
this instance or in another instance sharing the target schema. The error is
`TargetTableExists`, a `conflict` error. Two of an instance's definitions
can't share a bare target name in two schemas either
(`TargetTableSuffixCollision`). Trellis writes only to target tables
it creates, or to an empty table it adopts on request, and `DROP TRANSFORM`
drops the target, so it never takes over an existing relation unasked. Choose
another target name, give the instance its own target schema
([instance-identity](instance-identity.md)), or drop or rename the existing
relation.

With `ADOPT EXISTING TABLE` after the target
(`TRANSFORM t ADOPT EXISTING TABLE FROM s …`), define builds into the existing
table instead. The table must be a plain, empty table that the defining role
owns (or holds its owner's privileges) and can write, shaped exactly as define
would create it. Define records the key it would create, so a unique
constraint the table already has never keys it instead ([the key entry](#the-key-a-transform-keys-each-table-by)). The
table may carry anything a user may add to any target, with the outcomes
[any target has](#target-tables-are-trellis-owned): indexes, unique or not;
constraints; triggers; rules; foreign keys, its own or another table's
referencing it; grants; comments; row security policies, subject to
[the row-security entry](#row-level-security-on-a-role-trellis-runs-as);
extended statistics; publications; views over it; storage parameters; and a
replica identity.

Anything else refuses with `TargetNotAdoptable`, a `validation` error (a
`not_found` one for `missing`). Its message names the first difference found,
by code, checked in the order below (the column rows one column at a time):

| Code | The table | Remedy |
|---|---|---|
| `missing` | doesn't exist | Define without `ADOPT EXISTING TABLE` to create it |
| `not_a_table` | is a view, materialized view, foreign table, sequence, index or other relation, not a table | Adopt a plain table, or define under another name |
| `not_owner` | is owned by a role whose privileges the defining role doesn't inherit, so Trellis couldn't alter or drop it later | `ALTER TABLE … OWNER TO` the defining role, or grant it membership in the owner `WITH INHERIT TRUE` |
| `privilege` | lacks `SELECT`, `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` for the defining role, which Trellis writes it as | Grant the privilege the message names |
| `not_empty` | has rows | Empty it, or define under another name without the opt-in |
| `partitioned` | is partitioned | Adopt a plain table |
| `partition` | is a partition of another table | `DETACH PARTITION` it |
| `inheritance` | has an inheritance parent or child | `ALTER TABLE <child> NO INHERIT <parent>`, for its parent and for each child |
| `typed_table` | is a typed table (`OF <type>`) | `ALTER TABLE … NOT OF` |
| `unlogged` | is `UNLOGGED` | `ALTER TABLE … SET LOGGED` |
| `extension_member` | belongs to an extension | `ALTER EXTENSION … DROP TABLE`, as the extension's owner |
| `access_method` | uses another table access method than define's | `ALTER TABLE … SET ACCESS METHOD` |
| `missing_column` | lacks a column define creates | Add the column |
| `extra_column` | has a column define doesn't create | Drop the column |
| `column_order` | has define's columns in another order | Create the table again in define's order |
| `generated_column` | has a generated column | `ALTER COLUMN … DROP EXPRESSION`; a virtual one can't be, so create the table again |
| `identity_column` | has an identity column | `ALTER COLUMN … DROP IDENTITY` |
| `column_type` | has a column of another type, or with a type modifier (define creates none) | `ALTER COLUMN … TYPE` to the type the message names |
| `column_collation` | has a column with another collation | Alter the column to the collation the message names |
| `column_nullability` | has a column `NOT NULL` where define's isn't, or the reverse | `SET` or `DROP NOT NULL` |
| `column_default` | has a column with a default | `ALTER COLUMN … DROP DEFAULT` |
| `key` | isn't keyed as define keys it: the primary key, or an aggregate's or `JOIN`'s `UNIQUE NULLS NOT DISTINCT` group key, over the same columns in the same order, not deferrable and with no `INCLUDE` columns | Add the key the message names, dropping a primary key that differs (`DROP CONSTRAINT …, ADD PRIMARY KEY (…)`) |
| `owned_sequence` | has a sequence owned by one of its columns | `ALTER SEQUENCE … OWNED BY NONE`, or drop it |
| `unrecognized` | has a constraint or dependent object the check doesn't know, so a new Postgres feature refuses until Trellis learns it | Drop the object the message names |

The emptiness and shape are checked under a lock that keeps writes and DDL out
until define commits. Another definition's target in the same instance refuses
with `TargetTableExists`, empty or not. Another instance's empty target can't
be told from a user's table, so don't adopt one: both instances would write
it. Define always creates the ledger (`<target>__ledger`) and the hidden
tables, and one whose name is taken refuses as it would without the opt-in.
An adopted target is an ordinary one after define. Rows written into it by
hand afterwards are the
[known correctness gap](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)
any target has.

### Partitioned sources

A declaratively partitioned table (`RANGE`, `LIST` or `HASH`, at any depth,
with or without a `DEFAULT`) can be the `FROM` of an aggregate or either table
of a `JOIN`, on PostgreSQL 15 or later, with up to 1,000 leaves, when the group
key holds every partition-key column at every level: an aggregate's
`GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from that table.
Each group then lives in one partition, so a fact table partitioned on the
column it joins by needs nothing more, while one partitioned on its own id is
refused ([the refusal table](#refusals),
[ADR-0017](decisions/0017-partitioned-sources.md)). A target is always a plain
table.

```text
-- order_line_items is partitioned BY HASH (order_id)
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount
```

Every partition carries the capture triggers, and partition maintenance needs
no operator step: the staging worker's pass corrects the groups each change to
the tree touched. What an operation does to the counts until then, and what it
costs, is in [operations — Partitioned sources](operations.md#partitioned-sources).

## Granularity

Granularity is the target's key space: what one target row stands for. There
are four: 1-1, aggregate, join aggregate and 1-1 `JOIN`. Every target is
eventually consistent: a value read before `await_converged` may be
inconsistent, and an aggregate's small or hot group can briefly show a
combination no source state produces; after it, values are exact
([Reading derived data](data-flow.md#reading-derived-data)). The state that
keeps an aggregate or a `JOIN` exact lives in hidden tables in Trellis's
catalog schema, named by the transform's internal id, which readers get no
grant on.

### 1-1

One target row per source row, keyed by the source's key: its primary key or
the `KEY (…)` the definition names, or, over an aggregate or `JOIN` target
whose group key can be `NULL`, a `UNIQUE NULLS NOT DISTINCT` constraint over
the same columns. The target carries the key columns, so a field can't reuse a
key column's name ([expressions — Refusals](expressions.md#refusals)). It
reads one table: one row per row of a table with columns of a second is a
[1-1 `JOIN`](#1-1-join), and one row per group of a join a
[join aggregate](#join-aggregate).

### Aggregate (`GROUP BY`)

One target row per distinct combination of grouping values, matching the
equivalent `GROUP BY` query, and only while the group has source rows. Each key
becomes a target column named after its bare column, and keys can't be
aliased. A field selects a key, or composes aggregates with literals,
operators and functions (`COALESCE(SUM(amount), 0) + COUNT(*)`). The
aggregates are `COUNT(*)`, `COUNT(<expr>)`, `SUM` and `AVG` over `smallint`,
`integer`, `bigint` or `numeric`, each over a column or an expression of the
row, and `MIN` and `MAX` of a plain column; every other is
[refused](#refusals). Result types, `numeric` scales and the special values are
Postgres's ([type support](type-support.md#numeric-and-floating-point)).

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
```

Throughput falls as more distinct groups receive writes at once
([recommendations](recommendations.md#benchmark-aggregates-over-many-concurrently-written-groups)).

### Join aggregate

An aggregate over the join of two tables (`FROM a [LEFT|INNER] JOIN b ON …
GROUP BY …`), with one target row per group: a column per key and per field.
Its aggregates are a plain aggregate's except `MIN` and `MAX`, each argument
reading one table's columns. Every column is written `<table>.<column>`, or
`<alias>.<column>` for an aliased table, which its own name then no longer
qualifies, as in Postgres. Each table is captured and checked as a `FROM` is.

```text
TRANSFORM sales_by_tier FROM shop.orders AS o JOIN customers c
  ON o.customer_id = c.id
GROUP BY c.tier
SELECT c.tier AS tier, SUM(o.amount) AS total
```

### 1-1 `JOIN`

A `JOIN` with no `GROUP BY` keeps one target row per row of `a`, with the
columns of the row of `b` it joins to
([ADR-0021](decisions/0021-one-to-one-join-transforms.md)): a value computed
from both tables that a reader queries and indexes without repeating the join.

```text
TRANSFORM order_view FROM orders LEFT JOIN customers
  ON orders.customer_id = customers.id
SELECT
  customers.name AS customer_name,
  orders.amount * COALESCE(customers.discount, 1) AS discounted
```

The target is keyed by `a`'s key, whose columns it carries as a 1-1 target
does. `LEFT` keeps an `a` row with no match, with `NULL` for every `b` column;
`INNER` (or a bare `JOIN`) leaves it out until a match arrives. `b`'s `ON`
columns contain a unique key of `b`, so an `a` row joins at most one row. A
field is any expression a 1-1 field admits over columns of either table,
written as in a join aggregate; it has no aggregate and reads no other field.
Each row is recomputed from its current inputs, so no aggregate restriction
applies.

A **star** joins up to four tables to `a`, each `JOIN` clause one more
**position** (1 for the first), each `LEFT` or `INNER` on its own, on its own
unique key, with an `ON` that reads `a` and that table only. The two-table
`JOIN` is a star with one position, and what is said of `b` holds at every
position.

```text
TRANSFORM order_view FROM orders
  LEFT JOIN customers ON orders.customer_id = customers.id
  JOIN regions ON orders.region_id = regions.id
SELECT
  customers.name AS customer_name,
  regions.name AS region
```

Trellis keeps a copy of each table and a lock row per position and join key
([data flow](data-flow.md#a-1-1-joins-hidden-state)); the joined tables need
no source index, and the target stays readable across a `RESUME`'s rebuild. A
change of an `a` row rewrites its target row. A change of a joined row
recomputes every `a` row on its join key, in chunks, and `status()` warns of a
key with many rows ([status reference](status-reference.md#join)). A
`TRUNCATE` of `a` empties the target; one of a joined table rebuilds it. A row
whose field Postgres can't compute is held in quarantine, and the page
applies the rest
([ADR-0021](decisions/0021-one-to-one-join-transforms.md#a-row-a-page-fails-to-write-is-held-in-the-page)).
A joined table that is edited often, or has few distinct keys, fans each edit
out over many rows
([recommendations](recommendations.md#join-small-often-edited-tables-at-query-time)).

## Calculated Fields

A field's formula, its names and its refusals are in [expressions](expressions.md).

## Chaining and cycle detection

A field may read another field of its transform, and a target may be another
transform's source, aggregated or joined, once that transform is `live`
(`TransformNotLive` otherwise). Each write to a target reaches its readers in
the write's own transaction, never through capture triggers, which is why an
aggregate target can be chained off at all; the hand-off doesn't cross into
another instance
([data flow](data-flow.md#writing-to-calculated-tables-and-chaining)).
Trellis computes in dependency order over one graph of fields and tables, and
refuses at define any definition that would close a cycle in it, directly or
transitively.

## Target tables are Trellis-owned

A target is a plain table in the target schema: query it, join it, index it,
grant it. Its contents and shape belong to Trellis
([0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md)),
its only writer, which doesn't guard it
([ADR-0027](decisions/0027-operating-principles.md), rule 5). So don't write
its rows, add, drop, rename or retype its columns, change its key constraint,
or `TRUNCATE` or `DROP` it: an edited row stays wrong until something
recomputes it, `self_check` reports the divergence without repairing it, and
whether the edit reaches a transform chained off the table is undefined
([known correctness gaps](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)).
`PAUSE` then `RESUME` rebuilds it
([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)); a field
changes through [`ALTER TRANSFORM`](#changing-a-definition), and the table
goes with `DROP TRANSFORM`. Row-level security on a target is fine while the
role that writes it is exempt
([the row-security entry](#row-level-security-on-a-role-trellis-runs-as)).
While a transform is `backfilling`, its target is partial
([status reference](status-reference.md#backfilling)).

Indexes, grants, constraints, triggers, rules and foreign keys are yours to
add, to a target define created or adopted; `DROP TRANSFORM` takes them with
the table. What they do to Trellis's writes:

* A unique index, constraint or trigger that refuses a row Trellis writes
  fails that row's key like any value the target refuses: the key is held
  ([`held_keys`](status-reference.md#held_keys)), rows that fail only together
  hold their page as a `drain_failure`, and an aggregate's or a `JOIN`'s build
  pauses with a `backfill_failure`. A unique index off the key is the likely
  case: changes to different keys apply in any order, so two rows can hold one
  value for a moment though the source never does.
* Another table's foreign key referencing the target holds the key of a
  referenced row Trellis deletes, and a source `TRUNCATE`'s page while any row
  is referenced. `DROP TRANSFORM`, and the `RESUME` that empties a 1-1 target
  keyed by a relabelled enum, fail with the database's error while it exists:
  drop it first.
* An `INSERT` or `UPDATE` rule, or most `DELETE` rules (`DO INSTEAD NOTHING`
  among them), make Postgres refuse the `INSERT … ON CONFLICT` and
  `DELETE … RETURNING` Trellis writes with: a build pauses, and each key a
  change reaches is held.
* A trigger or rule that changes or drops the rows Trellis writes leaves the
  target wrong without a refusal, the
  [known correctness gap](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)
  of rows written into a target.

Another Trellis instance may read a 1-1 target keyed by a primary key as a
source. An aggregate target, and a 1-1 target over one, needs a `KEY (…)`
naming its group key ([the refusal table](#refusals)).

## Status

A transform's status is one of `waiting_to_backfill`, `backfilling`,
`catching_up`, `live`, `quarantined` or `paused`: the
[status reference](status-reference.md) defines each, and
[operations](operations.md#when-a-definition-needs-you) says what to do when
one stops.

## Changing a definition

Every change is one of the [statements](#statements), run through `apply`,
which only registers it: no statement reads source rows, and the build runs in
the background.

**Addressing.** A transform is named by its target table, bare
(`widget_prices`) or schema-qualified as define returns it
(`public.widget_prices`); a qualified name must match exactly. The statements
and the `Trellis` methods that name a transform (`status`,
`quarantine_status`, `sample_quarantined`, `release_key`, `self_check`) take
either form. `PAUSE` and `RESUME` also take `<transform>.<field>`, with the
bare transform name, for one calculated field. That reading wins when an
address reads both ways: `a.b` is field `b` of transform `a` when `a` declares
a field `b`, and otherwise the transform registered as `a.b` (name that one
bare when it collides). `DROP TRANSFORM a.b` refuses an address whose `a` is a
transform and that names no transform itself: dropping a field is
`ALTER TRANSFORM a DROP b`.

**`ALTER TRANSFORM`** edits the fields of a 1-1 transform of one table, never
its granularity or key. It validates the definition it would leave against the
source as it is at its commit, with define's rules, and never re-types a
column: an `ALTER` that would is refused, and the remedy is `DROP` the field,
then `ADD` it again. A field it adds or changes, like a field `RESUME`, is
built in the background: the transform reads `backfilling`, and a row's new
field can be empty or hold the old value until it reads `live` again
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)).
The rules are [0015-transform-redefinition](decisions/0015-transform-redefinition.md).

**`PAUSE`, `RESUME` and `DROP`** are
[0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md);
what a resume checks and rebuilds is
[operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume).

## Grammar versions

Every stored definition records the grammar version it was written in, so an
upgrade never re-interprets it: a release that changes what accepted text
means bumps the version and keeps reading older text under its old rules.
Grammar 2 reads [names](expressions.md#names) as Postgres does; a grammar-1
definition, written by 0.1.x, keeps its names' case and reads a bare `TRUE`,
`FALSE` or `NULL` as a name. The rules, and which versions a release reads,
are [0018-grammar-versioning](decisions/0018-grammar-versioning.md); what an
upgrade pauses is in [upgrading](upgrading.md#upgrading).

## Floors

Every stored definition also records its **floor**, the oldest Trellis release
that can run it, reported as `floor` by `status()`. Define, `ALTER TRANSFORM`,
a resume's rebuild and a [retype](#retype) record it again. What it counts is
[0024-compatibility-floors-and-rollback](decisions/0024-compatibility-floors-and-rollback.md#recording-definition-floors);
reading it before a rollback is
[upgrading — Reading the floors](upgrading.md#reading-the-floors).
