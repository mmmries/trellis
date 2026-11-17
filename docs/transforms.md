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
  [WHERE <expression>]

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
- `WHERE` takes a `boolean` [expression](expressions.md#where) over source
  columns that can't raise an error. A plain 1-1 transform holds only the rows
  it is `TRUE` for, and a plain aggregate's groups count only those rows. A
  `JOIN` refuses any `WHERE` but `TRUE` for now
  ([#1991](https://github.com/salesforce-misc/trellis/issues/1991)).
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
error, and what happens when the condition appears after define. The error
links to its remedy in [errors](errors.md). A row links to an entry under
[Details](#details) where it needs more room.
What an expression may contain is refused in [its own table](expressions.md#refusals).

### After define

A condition define refuses can still arise later, through DDL on a table the
definition reads. The table's "After define" column names what Trellis does
then, one of the classes below, or reads n/a where the condition matters only
at define (a name taken, an upstream not yet `live`). A paused definition
keeps its last values, with the reason in
[`capture_failure`](status-reference.md#capture_failure); every `RESUME` first
runs define's validation and refuses with define's error while it would
([ADR-0027](decisions/0027-operating-principles.md), rule 4). Each reason's
repair is in [operations — Paused definitions](operations.md#paused-definitions),
and other ways a source can drift are in
[known correctness gaps](known-correctness-gaps.md).

#### Text

The rule reads only the definition's text, which doesn't change, so the
condition can't arise later. An `ALTER TRANSFORM` is checked as define is.

#### Retype

The rule reads a column's type. When the staging worker's capture pass finds
a column a definition reads retyped, or dropped and added again, it runs
define's validation again
([capture drift](staging-and-claiming/capture-drift.md#a-column-changed-or-rewritten-without-a-trigger)).
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
([capture drift](staging-and-claiming/capture-drift.md#a-renamed-or-dropped-column)).

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

| Condition | Error | After define |
|---|---|---|
| **Source tables and partitioning** | | |
| A table the role Trellis runs as can't use: it lacks `USAGE` on the table's schema, or neither owns the table nor holds a privilege on it | [T0128 TableNotAccessible](errors.md#t0128-tablenotaccessible) | [Shape](#shape): a revoked table privilege or schema `USAGE` halts the readers on the next drain, and a revoked schema `USAGE` at the next staging pass too |
| A partition (a leaf or a middle), a partitioned root that is itself a partition, or a table in an inheritance hierarchy: a statement trigger fires only for the table a statement names | [T0144 SourceNotChangeKeyed](errors.md#t0144-sourcenotchangekeyed) | [Shape](#shape) (`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or a drop and recreate as one) |
| [A source dropped, renamed, swapped, or its capture disrupted](#a-source-dropped-renamed-swapped-or-its-capture-disrupted) | n/a | [Disruption](#disruption) |
| A partitioned root ([0017](decisions/0017-partitioned-sources.md)) whose partition key, at any level, the group key omits: an aggregate's `GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from that table | [T0145 PartitionedSource](errors.md#t0145-partitionedsource), naming the column | [Shape](#shape): a sub-level added later |
| A partitioned root whose key level is an expression, or whose partition-key column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a non-default operator class or a nondeterministic collation | [T0145 PartitionedSource](errors.md#t0145-partitionedsource) | [Shape](#shape): a sub-level added later |
| A partitioned root with more than 1,000 leaves, or on PostgreSQL before 15: every build statement opens every leaf | [T0145 PartitionedSource](errors.md#t0145-partitionedsource) | [Shape](#shape) |
| A partitioned root as a 1-1 transform's `FROM` (partitioned targets are not supported) | [T0145 PartitionedSource](errors.md#t0145-partitionedsource) | [Disruption](#disruption): the source dropped and created again partitioned; `RESUME` then refuses |
| **Source keys** | | |
| An aggregate target, or a 1-1 target over one, as another Trellis instance's source: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint, not a primary key. Inside the owning instance it can be chained off | [T0113 NoPrimaryKey](errors.md#t0113-noprimarykey) | [Disruption](#disruption): only by the table being replaced by such a target; `RESUME` then refuses |
| [The key a transform keys each table by](#the-key-a-transform-keys-each-table-by) | [T0113 NoPrimaryKey](errors.md#t0113-noprimarykey), [T0118 RecordedKeyMissing](errors.md#t0118-recordedkeymissing), [T0114 DeferrablePrimaryKey](errors.md#t0114-deferrableprimarykey), [T0116 KeyNotBacked](errors.md#t0116-keynotbacked), [T0115 KeyOnTrellisTarget](errors.md#t0115-keyontrellistarget), [T0117 KeyDiffersFromReaders](errors.md#t0117-keydiffersfromreaders), [T0121 UnsupportedPrimaryKeyType](errors.md#t0121-unsupportedprimarykeytype) | [Key](#key) |
| **Row-level security and replication** | | |
| [Row-level security on a role Trellis runs as](#row-level-security-on-a-role-trellis-runs-as) | [T0146 RowSecurityApplies](errors.md#t0146-rowsecurityapplies), [T0136 ExtremeSourceNotReadable](errors.md#t0136-extremesourcenotreadable) | [Shape](#shape) |
| A table a logical-replication subscription in the same database replicates into, in any sync state or disabled: its apply worker fires only row-level triggers. Publishing a table is fine | [T0147 Subscribed](errors.md#t0147-subscribed) | [Shape](#shape) |
| **Generated columns and collations** | | |
| [A virtual generated column a definition reads](#a-virtual-generated-column-a-definition-reads) | [T0133 VirtualGeneratedColumn](errors.md#t0133-virtualgeneratedcolumn) | [Shape](#shape) |
| A nondeterministic collation on a key column (a source key, `GROUP BY` key or `ON` column), on a column passed to `STRPOS` or `REGEXP_COUNT`, or on one compared as text (`=`, `<>`, `!=`, `IS [NOT] DISTINCT FROM`, `NULLIF`), directly or through `COALESCE`, a `CASE` result or a field | [T0108 NondeterministicKeyCollation](errors.md#t0108-nondeterministickeycollation), [T0109 NondeterministicCollationTextFunction](errors.md#t0109-nondeterministiccollationtextfunction), [T0063 JoinNondeterministicCollation](errors.md#t0063-joinnondeterministiccollation) | On a key, [Retype](#retype); on a column only a text function reads, [Per row](#per-row); on a column only compared, nothing detects it ([known correctness gaps](known-correctness-gaps.md#a-read-column-given-a-nondeterministic-collation)) |
| **Retyped and widened columns** | | |
| A key column (a source key, `GROUP BY` key or `ON` column) retyped so stored values render differently: `timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or `integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal precision or `varchar(n)` | n/a | [Retype](#retype) |
| [A column Trellis copies, widened](#a-column-trellis-copies-widened) | n/a | [Retype](#retype) |
| **Target columns and widths** | | |
| Two `GROUP BY` keys that share a target column name (a key listed twice, or a `JOIN`'s `orders.region` and `customers.region`); keys can't be aliased | [T0101 GroupByKeysShareTargetColumn](errors.md#t0101-groupbykeyssharetargetcolumn) | [Text](#text) |
| A key of more than 32 columns (`INDEX_MAX_KEYS`): a `GROUP BY`, or a `JOIN` side's `ON` and `GROUP BY` columns together; a 1-1 `JOIN`'s `ON` at most 31 | [T0059 IndexKeyTooWide](errors.md#t0059-indexkeytoowide) | [Text](#text) |
| [A table of more than 1,600 columns](#a-table-of-more-than-1600-columns) | [T0060 TableTooWide](errors.md#t0060-tabletoowide) | [Text](#text) |
| **Source indexes** | | |
| [A source without the index its `self_check` walk pages by](#a-source-without-the-index-its-self_check-walk-pages-by) | [T0134 SourceIndexMissing](errors.md#t0134-sourceindexmissing) | [Index](#index) |
| **`JOIN` transforms** | | |
| [An unsupported `JOIN` shape](#an-unsupported-join-shape) | [T0005 UnsupportedKeySpace](errors.md#t0005-unsupportedkeyspace), [T0015 TableAlias](errors.md#t0015-tablealias), [T0016 AliasedTableName](errors.md#t0016-aliasedtablename), [T0050 JoinSelf](errors.md#t0050-joinself), [T0051 JoinTooManyTables](errors.md#t0051-jointoomanytables), [T0055 JoinOnSameSide](errors.md#t0055-joinonsameside), [T0057 JoinOnRepeatedPair](errors.md#t0057-joinonrepeatedpair), [T0053 JoinUnqualifiedColumn](errors.md#t0053-joinunqualifiedcolumn), [T0052 JoinUnknownTable](errors.md#t0052-joinunknowntable), [T0054 JoinUnresolvedColumn](errors.md#t0054-joinunresolvedcolumn), [T0056 JoinMixedSideArgument](errors.md#t0056-joinmixedsideargument), [T0064 JoinFieldShape](errors.md#t0064-joinfieldshape) | [Text](#text) |
| [A 1-1 `JOIN`](#a-1-1-join) | [T0066 OneToOneJoinKeyNotUnique](errors.md#t0066-onetoonejoinkeynotunique), [T0065 OneToOneJoinAggregate](errors.md#t0065-onetoonejoinaggregate), [T0067 OneToOneJoinUnsupported](errors.md#t0067-onetoonejoinunsupported), [T0145 PartitionedSource](errors.md#t0145-partitionedsource), [T0058 JoinOnRepeatedColumn](errors.md#t0058-joinonrepeatedcolumn) | [Key](#key), [Retype](#retype), [Shape](#shape) |
| [1-1 `JOIN` of more than two tables (star)](#1-1-join-of-more-than-two-tables-star) | [T0068 StarJoinTooManyTables](errors.md#t0068-starjointoomanytables), [T0069 JoinOnReadsAnotherJoinedTable](errors.md#t0069-joinonreadsanotherjoinedtable), the 1-1 `JOIN`'s at each position | As for a 1-1 `JOIN`, at each position |
| `ON` columns that differ in type or collation (a modifier never counts; `text` and `varchar` are one type), are off the join-key allowlist in [type-support](type-support.md) (a domain always is), or have a nondeterministic collation; a `GROUP BY` key of a type that can't be one | [T0061 JoinKeyTypeMismatch](errors.md#t0061-joinkeytypemismatch), [T0062 JoinUnsupportedKeyType](errors.md#t0062-joinunsupportedkeytype), [T0063 JoinNondeterministicCollation](errors.md#t0063-joinnondeterministiccollation), [T0100 UnsupportedGroupByKeyType](errors.md#t0100-unsupportedgroupbykeytype) | [Retype](#retype); a change between deterministic collations pauses nothing, but `RESUME` refuses until they match |
| **Aggregate functions** | | |
| [`MIN` or `MAX` in a plain aggregate](#min-or-max-in-a-plain-aggregate) | [T0073 ExtremeOfExpression](errors.md#t0073-extremeofexpression), [T0072 UnsupportedExtremeArgument](errors.md#t0072-unsupportedextremeargument), [T0074 ChainedExtreme](errors.md#t0074-chainedextreme), [T0075 ExtremesWithoutSourceIndex](errors.md#t0075-extremeswithoutsourceindex), [T0135 ExtremeIndexMissing](errors.md#t0135-extremeindexmissing) | [Index](#index), [Retype](#retype) |
| A field reading a column that is neither a `GROUP BY` key nor inside an aggregate, or named after a key but not that key (`GROUP BY id SELECT SUM(id) AS id`) | [T0102 UngroupedColumnReference](errors.md#t0102-ungroupedcolumnreference), [T0106 GroupingColumnFieldMustBePassthrough](errors.md#t0106-groupingcolumnfieldmustbepassthrough) | [Text](#text) |
| `DISTINCT` or `ALL` before an aggregate's arguments, `ORDER BY` after them, or a `FILTER (WHERE …)` clause: Postgres accepts each | [T0240 UnsupportedAggregateModifier](errors.md#t0240-unsupportedaggregatemodifier) | [Text](#text) |
| `MIN` or `MAX` in a `JOIN` transform | [T0070 DeferredAggregate](errors.md#t0070-deferredaggregate) (`MIN/MAX`) | [Text](#text) |
| `bool_and`, `bool_or`, `bit_and` or `bit_or` | [T0070 DeferredAggregate](errors.md#t0070-deferredaggregate) | [Text](#text) |
| `SUM` or `AVG` over anything but a number (`smallint`, `integer`, `bigint`, `numeric`, `real`, `double precision`) or an `interval` | [T0071 UnsupportedAggregateArgument](errors.md#t0071-unsupportedaggregateargument) | [Retype](#retype) |
| `array_agg`, `string_agg` or `jsonb_agg` | [T0070 DeferredAggregate](errors.md#t0070-deferredaggregate) | [Text](#text) |
| **Filters and chaining** | | |
| <a id="a-where-other-than-true"></a>A `WHERE` other than `TRUE` on a key space that doesn't apply one yet: a `JOIN` ([#1991](https://github.com/salesforce-misc/trellis/issues/1991)). The predicate's own refusals are in [expressions](expressions.md#refusals) | [T0238 FilterNotYetSupported](errors.md#t0238-filternotyetsupported) | [Text](#text) |
| `MIN` or `MAX` in a plain aggregate with a `WHERE` other than `TRUE`: an extreme is read from a source index, which holds the rows the `WHERE` drops too | [T0239 FilteredExtreme](errors.md#t0239-filteredextreme) | [Text](#text) |
| A target that is its own source, directly or through a [chain](#chaining-and-cycle-detection) | [T0048 TargetEqualsSource](errors.md#t0048-targetequalssource), [T0047 TableCycle](errors.md#t0047-tablecycle) | [Text](#text) |
| Chaining off a transform that is not `live` | [T0154 TransformNotLive](errors.md#t0154-transformnotlive) | n/a |
| **Target names** | | |
| [A target whose name is taken](#a-target-whose-name-is-taken) | [T0141 TargetTableExists](errors.md#t0141-targettableexists), [T0140 TargetTableSuffixCollision](errors.md#t0140-targettablesuffixcollision), [T0143 TargetNotAdoptable](errors.md#t0143-targetnotadoptable) | n/a |
| A target that would take the name of a table another transform reads as its `FROM` or joined table, one dropped or renamed away since, or one `ADOPT EXISTING TABLE` would adopt: the reader would read the new target's change feed | [T0142 TargetNameHasReaders](errors.md#t0142-targetnamehasreaders), naming each reader | n/a: later, a new reader of the name is a chain, and the target can't be dropped while one reads it ([T0153 DependentsBlockDrop](errors.md#t0153-dependentsblockdrop)) |
| A target name ending in `__ledger`, or over 55 bytes: its ledger, `<target>__ledger`, must fit Postgres's 63 | [T0076 ReservedTargetSuffix](errors.md#t0076-reservedtargetsuffix), [T0077 TargetNameTooLong](errors.md#t0077-targetnametoolong) | [Text](#text) |
| A target in this instance's catalog schema, or in a schema holding another instance's `trellis_instance` table, whose uninstall would drop it ([instance-identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)) | [T0139 TargetInCatalogSchema](errors.md#t0139-targetincatalogschema) | [Resume only](#resume-only) |

### Details

#### A source dropped, renamed, swapped, or its capture disrupted

A source or joined table dropped, renamed away, moved to another schema,
swapped for another table or replaced by a view, or its capture triggers or
functions disabled, dropped or replaced outside Trellis, is a
[Disruption](#disruption). The `capture_failure` names the finding and says
changes made meanwhile may be missing, and nothing is reinstalled until a
`RESUME`. On a partitioned source every node of the tree is checked: a root
dropped and created again, swapped or renamed away is a disruption, as is a
plain table created again partitioned or the reverse; partition maintenance is
not. `RESUME` refuses while the table is missing; otherwise it re-validates
against the table now at that name and rebuilds from it
([`PAUSE` and `RESUME`](operations.md#pause-and-resume)). Plan a scripted
`RESUME` after a swap, or after a bulk load with triggers disabled
([embedding](embedding.md)).

#### The key a transform keys each table by

A transform keys each table it reads by its primary key, or by the columns of
a `KEY (…)` written directly after the table (`FROM orders KEY (customer_id,
id)`, `JOIN b KEY (…) ON`). Another transform's target is keyed by the key
Trellis gave it. The rules and their reasons are
[ADR-0026](decisions/0026-source-keys.md). Define refuses:

- no primary key and no `KEY` (`NoPrimaryKey`, or
  `RecordedKeyMissing` once a transform has recorded the table's key);
- a deferrable primary key and no `KEY` (`DeferrablePrimaryKey`);
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
  (`KeyDiffersFromReaders`, naming the key to write).

After define the key is the [Key](#key) class. What `RESUME` takes as the key,
and how it re-keys a 1-1 target, is in
[`PAUSE` and `RESUME`](operations.md#pause-and-resume).

#### Row-level security on a role Trellis runs as

Row-level security applies to a role when RLS is enabled and the role neither
owns the table (directly or through membership) nor has `BYPASSRLS`, or owns
it and the table is `FORCE ROW LEVEL SECURITY`; a superuser is exempt. Every
Trellis connection runs with `row_security = off`, so a read or write the
policies would filter fails rather than skipping rows.

Define checks a source for the role that owns the ring, and a table that is
another transform's target for the defining role. It creates the target as
the defining role, which so owns it, and checks it for that role too: that
fires only when DDL around the creation (an event trigger) forces RLS on it or
hands it to another owner. A `MIN`/`MAX` aggregate's source is also checked
for the ring owner's `SELECT`
([T0136 ExtremeSourceNotReadable](errors.md#t0136-extremesourcenotreadable)), at
define and on a whole-transform `RESUME`: a drain logs in as that role or a
member of it and re-reads a group's extreme from the source.

After define, a refused build chunk pauses its transform after its charged
retries, with the error on `backfill_failure`
([observability — A halting failure](observability.md#a-halting-failure)). On
a partitioned source, define and the pass check the root only. A policy on a
partition reaches only the correction that reads that partition (after its
`TRUNCATE`, an attach with rows, or direct writes to it as a new leaf), which
then pauses the transform the same way.

#### A virtual generated column a definition reads

A column `GENERATED ALWAYS AS (…) VIRTUAL` (PostgreSQL 18) that a definition
reads, anywhere: a statement trigger's transition table holds no value for it,
so capture can't image it. Define, an `ALTER TRANSFORM` that adds such a field
and a `RESUME` refuse it, naming the table, the column and its expression. A
definition on the same table that doesn't read it is unaffected.

A read column can become virtual after define (dropped and added again, or
another renamed onto its name). Until the next pass the application's inserts
and deletes on the table fail (`unexpected virtual generated column
reference`), and an update stages the column as `NULL`; the resume's rebuild
repairs the targets
([known correctness gaps](known-correctness-gaps.md#a-read-columns-name-swapped-with-anothers-and-back-within-one-pass)).

#### A column Trellis copies, widened

Trellis creates some columns with a source column's type: a 1-1 target's key,
passthrough and calculated columns; an aggregate's `GROUP BY` keys and fields,
in its target and hidden table; a `JOIN`'s `GROUP BY` keys, `ON` columns and
fields, in its target and hidden tables; and a 1-1 `JOIN`'s key, fields, `R`,
`M` and `J`. A widening can outgrow them: `integer` → `bigint` (and `smallint`
up), an integer → `numeric` (unless every value of the `numeric` fits the
integer), and `real` → `double precision`. A modifier change is no widening,
since a copy has none, and needs no re-type, pause or rebuild
([ADR-0020](decisions/0020-type-identity.md),
[type support](type-support.md#the-types-of-the-columns-trellis-creates)).

After a widening, the staging worker pauses every transform that owns a column
the new type outgrew, naming the source column and each copy; a narrowing
pauses nothing here, except a `varchar(n)` key's. `RESUME` re-types each copy
under `ACCESS EXCLUSIVE` (a table rewrite for `integer` → `bigint`, which
reads of the target wait for) and rebuilds; a `JOIN`'s and a 1-1 `JOIN`'s
hidden tables are created again instead
([capture drift](staging-and-claiming/capture-drift.md#a-re-typed-key-column-or-an-outgrown-copy)).

#### A table of more than 1,600 columns

Postgres caps a table at 1,600 columns. Trellis creates a target with a column
per key and field, and hidden tables whose width follows the fields: an
aggregate's has its `GROUP BY` columns, a count per distinct argument, a sum
per argument a `SUM` or `AVG` reads, and a column per distinct `MIN` or `MAX`
(plus two for their basis and stamp); a `JOIN`'s partials do the same per side
and for the groups; a 1-1 `JOIN`'s ledger for a side has a column per column
its fields read. So about 800 distinct summed arguments overflow a hidden
table their target fits in. The error names the table's role and the limit.
It is checked at define, `RESUME` and `ALTER TRANSFORM … ADD`, which also
counts the columns earlier edits dropped (Postgres counts them until the table
is created again): if they filled the limit, drop the transform and define it
again. Define doesn't refuse the smaller limits an aggregate meets first: from
about 270 distinct summed arguments its statements exceed Postgres's
1,664-entry target list, so its build pauses or its changed groups are
quarantined with Postgres's error
([#1518](https://github.com/salesforce-misc/trellis/issues/1518)), and from
about 500 a hidden row can outgrow a heap page
([#1516](https://github.com/salesforce-misc/trellis/issues/1516)).

#### A source without the index its `self_check` walk pages by

A `GROUP BY` aggregate's source needs a valid, non-partial btree index on
plain columns whose leading columns are the `GROUP BY` columns in order; each
table of a `JOIN` needs one leading with its `ON` columns in `ON` order. A
primary key or unique constraint that leads with them counts. An expression
or partial index, a non-default operator class (`text_pattern_ops`) and mixed
directions (`(a, b DESC)`; all ascending `NULLS LAST` or all `DESC NULLS FIRST`
is fine) don't. A 1-1 target needs nothing, nor does a `JOIN`'s `T` part,
which no single table's index serves.

The error names the table, the columns and the part of the definition that
needs the index, with a ready-to-run `create index`. Writing
`WITHOUT SOURCE INDEX` after the `GROUP BY` or `JOIN` clause opts the
definition out: its gap is still listed in `source_index_gaps`, and its
`self_check` scans the source once per page. A definition with a `MIN` or
`MAX` field can't opt out ([the `MIN`/`MAX` entry](#min-or-max-in-a-plain-aggregate)).
Builds need no index.

#### An unsupported `JOIN` shape

Only `INNER` and `LEFT` equi-joins of different tables are admitted, and a
join with a `GROUP BY` takes two tables. Each table is held to every rule
here, as a `FROM` is, and may be another transform's target, read through its
change feed
([ADR-0019](decisions/0019-join-transforms.md#a-side-may-be-another-transforms-target-through-the-target-change-feed)).
A field is a `GROUP BY` key from either table, or a composition of aggregates
(`COALESCE(SUM(items.qty), 0)`), each `COUNT(*)` or `COUNT`/`SUM`/`AVG` over one
table's columns (`SUM(items.qty + 1)`), kept as a running total on that table.

Refused, with or without a `GROUP BY` (without one it is a
[1-1 `JOIN`](#a-1-1-join)):

- **Join type.** `RIGHT`, `FULL`, `CROSS`, `NATURAL`, `LATERAL`, `USING` and
  comma joins (`UnsupportedKeySpace`).
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

A [1-1 `JOIN`](#1-1-join) is refused when:

- `b`'s `ON` columns are not unique (`OneToOneJoinKeyNotUnique`);
- a field is an aggregate (`OneToOneJoinAggregate`);
- it reads a column that starts with `__` (`OneToOneJoinUnsupported`);
- a side is partitioned (`PartitionedSource`);
- the `ON` names one column in two terms (`JoinOnRepeatedColumn`);
- the `ON` reads two columns of `a` whose names share their first 60 bytes
  (rename one);
- `b` is another transform's 1-1 target, joined on columns that leave out part
  of that target's primary key: Trellis orders changes per primary-key value
  only, so only that key is unique at every instant while the upstream drains
  (join on its whole key, or join the table that transform reads; an `a` that
  is a 1-1 target may be joined on any columns);
- it says `WITHOUT SOURCE INDEX` (a parse error).

`b`'s `ON` columns contain a unique key of `b` when they hold its primary key,
a unique constraint, a non-partial unique index on plain columns (deferrable,
or with `INCLUDE` columns, is fine here), or the group key of an aggregate or
`JOIN` target. `b`'s row-identity key is a separate rule
([the key entry](#the-key-a-transform-keys-each-table-by)): it must not be
deferrable, even when a deferrable key is what makes the `ON` columns unique.

After define, a widened copy is [the widening entry](#a-column-trellis-copies-widened),
reported for a field over both tables against the table whose column widened.
`b`'s unique key lost is the [Key](#key) class, and `RESUME` refuses until it
is back
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).
A duplicate Postgres admits past a deferrable key under
`session_replication_role = replica` pauses nothing
([known correctness gaps](known-correctness-gaps.md#a-duplicate-in-b-past-a-deferrable-key-under-session_replication_role--replica)).

#### 1-1 `JOIN` of more than two tables (star)

A [star](#1-1-join) is refused when it has more than 5 tables (`a` and 4
joined: `StarJoinTooManyTables`), or a joined table's `ON` reads a joined table
other than its own (`JoinOnReadsAnotherJoinedTable`), as in a snowflake
(`JOIN b2 ON b1.id = b2.id`). Every rule of [the 1-1 `JOIN`](#a-1-1-join) and
of [the `JOIN` shape](#an-unsupported-join-shape) holds at each position,
including that the target isn't named after a table it reads
(`TargetEqualsSource`), and the error names the table at that position. The
joins read left to right, as Postgres reads them without parentheses. A
lookup table with few distinct keys is admitted, and `status()` warns of its
fan-out. After define, each joined table is handled as `b` is, and `RESUME`
re-validates every position. Join a snowflake's outer table through a
transform of its own, and join that target.

#### `MIN` or `MAX` in a plain aggregate

A plain aggregate is a `GROUP BY` over one table. `MIN` or `MAX` in one is
refused when its argument:

- is not a plain source column, such as `MAX(a + b)`, a cast or a calculated
  field (`ExtremeOfExpression`);
- is of a type other than `smallint`, `integer`, `bigint`, `numeric`, `real`,
  `double precision`, `date`, `time`, `timestamp`, `timestamptz`, `interval`
  or `oid`, so `text` is refused (`UnsupportedExtremeArgument`);
- is a column of another transform's target (`ChainedExtreme`), whatever
  indexes that target has.

It is also refused `WITHOUT SOURCE INDEX` (`ExtremesWithoutSourceIndex`), and
when the source lacks, per argument, a valid, non-partial btree index leading
with the `GROUP BY` columns in order, then the argument, under the rules of
[the source-index entry](#a-source-without-the-index-its-self_check-walk-pages-by)
(`ExtremeIndexMissing`). A deletion that removes a group's extreme re-reads
the next one through that index
([type support](type-support.md#min-and-max)); without it each re-read scans
the group or the whole source, and a page can outrun `statement_timeout`. The
error names the table, the columns and a ready-to-run `create index`, which
also serves as the `GROUP BY` index. Take `MIN`/`MAX` of a plain column in the
transform that reads the source (a stored generated column, for an
expression).

A probe index lost after define is the [Index](#index) class, and the target
stays exact meanwhile. The pause's `capture_failure` names a ready-to-run
`create index`, and on a partitioned source says to re-run
`ALTER INDEX … ATTACH PARTITION` after repairing a leaf's index
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).

#### A target whose name is taken

The name is taken when a relation of any kind exists in the target schema
under the target's name or `<target>__ledger`, another definition's target
included, in this instance or another sharing the schema
([T0141 TargetTableExists](errors.md#t0141-targettableexists), a `conflict`
error). Two of an instance's definitions can't share a bare target name in two
schemas either
([T0140 TargetTableSuffixCollision](errors.md#t0140-targettablesuffixcollision)).
Trellis never takes over a relation unasked, since `DROP TRANSFORM` drops the
target.

With `ADOPT EXISTING TABLE` (`TRANSFORM t ADOPT EXISTING TABLE FROM s …`),
define builds into the existing table instead, for example one restored from a
schema dump. It must be a plain, empty table the defining role owns (or holds
its owner's privileges) and can write, shaped exactly as define would create
it; define records the key it would create, so a unique constraint the table
has never keys it instead. It may carry anything
[any target may](#target-tables-are-trellis-owned): indexes, constraints,
triggers, rules, foreign keys, grants, comments, policies (subject to
[the row-security entry](#row-level-security-on-a-role-trellis-runs-as)),
extended statistics, publications, views, storage parameters and a replica
identity. Anything else refuses with
[T0143 TargetNotAdoptable](errors.md#t0143-targetnotadoptable), a `validation`
error (`not_found` for `missing`), naming the first difference.

The emptiness and shape are checked under a lock that keeps writes and DDL out
until define commits. Another definition's target in the same instance refuses
with `TargetTableExists`; another instance's empty target can't be told from a
user's table, so don't adopt one: both instances would write it. Define always
creates the ledger and hidden tables, refusing a taken name as without the
opt-in. An adopted target is an ordinary one after define.

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

One target row per source row the `WHERE` admits, keyed by the source's key:
its primary key or the `KEY (…)` the definition names, or, over an aggregate
or `JOIN` target whose group key can be `NULL`, a `UNIQUE NULLS NOT DISTINCT`
constraint over the same columns. The target carries the key columns, so a field can't reuse a
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

#### Counting distinct values

`COUNT(DISTINCT x)` is not supported. Chain two transforms: the first groups
by the key and the value, the second counts its rows per key.

```text
TRANSFORM g_x FROM events GROUP BY g, x
SELECT g AS g, x AS x, COUNT(*) AS n

TRANSFORM g_distinct FROM g_x GROUP BY g
SELECT g AS g, COUNT(x) AS distinct_x
```

Count `x`, not `*`: a `NULL` value is its own `(g, NULL)` row in the first
target, and `COUNT(DISTINCT x)` skips `NULL`s.

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
([the apply stage](staging-and-claiming/05-apply-and-exactly-once-deltas.md#a-1-1-joins-hidden-state)); the joined tables need
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
column: an `ALTER` that would is refused
([T0157 UnsupportedAlter](errors.md#t0157-unsupportedalter)). A field it adds or changes, like a field `RESUME`, is
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
