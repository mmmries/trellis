# Defining Transforms

A transform describes how one derived (target) table is produced from one or
more source tables. Its definition is two separately-chosen pieces: the
**granularity** of the target's primary-key space, and the **calculated
fields** that populate its non-key columns. Every source row participates: a
`WHERE` other than `TRUE` is refused
([#804](https://github.com/salesforce-misc/trellis/issues/804)).

Trellis maintains these tables **incrementally** as source data changes, trading
spikey read load for a steady write load that keeps the table cheap to read (see
the README for fuller motivation).

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
define's validation again. A definition that still validates keeps running.
Its [floor](#floors) is recorded again over the new types, and its target is
rebuilt in place unless the change was a pure widening. A definition pauses
when define would now refuse it, when it would evaluate the column as another
type (`integer` to `double precision`), or when the new type outgrew a column
Trellis copied ([the widening entry](#a-column-trellis-copies-widened)). A key
column (a source key, `GROUP BY` key or `ON` column) whose new type or
collation define refuses, or whose stored keys now render differently, pauses
its readers too. So does a read column renamed or dropped
([capture by triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).

#### Shape

The rule reads the table's shape: its place in a partition or inheritance
hierarchy, its partition tree, its row-level security, a subscription into it,
or whether a column it reads is virtual. The staging worker's next reconcile
pass (every 5 seconds by default) pauses every definition reading the table
that the new shape breaks. The resume's rebuild repairs any writes that
capture missed before that pass. Row-level security on a target pauses the
definition that writes it. On a role a drain logs in as, the refused read or
write halts the definitions it reaches on the next drain (`capture_failure`
kind `halt`). Only a
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
- a primary key redefined: its readers pause on the next drained change or
  the next pass.

A primary key made deferrable halts its readers on the next drain of a change.
A dropped primary key leaves the table uncapturable (a capture holdup in
`status()`), and its readers pause when its next change is applied. A target
that lost the index over its key has no detector: `RESUME` and a rebuild
refuse until the index is back. These pause nothing: an equivalent index
built before the old one is dropped, `REINDEX CONCURRENTLY`, a rename and
pg_repack.

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
| A partition (a leaf or a middle), a partitioned root that is itself a partition, or a table in an inheritance hierarchy: a statement trigger fires only for the table a statement names | `SourceNotChangeKeyed` | [Shape](#shape) (`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or a drop and recreate as one) | Use a plain table; after a change, `DETACH PARTITION` or `NO INHERIT`, then `RESUME` |
| [A source dropped, renamed, swapped, or its capture disrupted](#a-source-dropped-renamed-swapped-or-its-capture-disrupted) | n/a | [Disruption](#disruption) | Put the table back, then `RESUME` |
| A partitioned root ([0017](decisions/0017-partitioned-sources.md)) whose partition key, at any level, the group key omits: an aggregate's `GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from that table | `PartitionedSource`, naming the column | [Shape](#shape): a sub-level added later | Add the column to the `GROUP BY` or join on it; after a change, detach the sub-level, then `RESUME` |
| A partitioned root whose key level is an expression, or whose partition-key column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a non-default operator class or a nondeterministic collation | `PartitionedSource` | [Shape](#shape): a sub-level added later | Partition by a plain listed column; after a change, detach the sub-level, then `RESUME` |
| A partitioned root with more than 1,000 leaves, or on PostgreSQL before 15: every build statement opens every leaf | `PartitionedSource` | [Shape](#shape) | Merge partitions to 1,000 or fewer leaves, or upgrade the server |
| A partitioned root as a 1-1 transform's `FROM` (partitioned targets are not supported) | `PartitionedSource` | [Shape](#shape): the source dropped and created again partitioned | Aggregate over it, or use a plain table |
| **Source keys** | | | |
| An aggregate target, or a 1-1 target over one, as another Trellis instance's source: its identity is a `UNIQUE NULLS NOT DISTINCT` constraint, not a primary key. Inside the owning instance it can be chained off | `NoPrimaryKey` | [Key](#key) | Chain within the instance ([instance-identity](instance-identity.md)), group over a 1-1 transform, or name the group key with `KEY (…)` |
| [The key a transform keys each table by](#the-key-a-transform-keys-each-table-by) | `NoPrimaryKey`, `DeferrablePrimaryKey`, `KeyNotBacked`, `KeyOnTrellisTarget`, `KeyDiffersFromReaders` | [Key](#key) | Name a `KEY (…)` or add a primary key; [details](#the-key-a-transform-keys-each-table-by) |
| **Row-level security and replication** | | | |
| [Row-level security on a role Trellis runs as](#row-level-security-on-a-role-trellis-runs-as) | `RowSecurityApplies`, `ExtremeSourceNotReadable` | [Shape](#shape) | `BYPASSRLS`, or the owner without `FORCE ROW LEVEL SECURITY`; then `RESUME` |
| A table a logical-replication subscription in the same database replicates into, in any sync state or disabled: its apply worker fires only row-level triggers. Publishing a table is fine | `Subscribed` | [Shape](#shape) | Remove the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME` |
| **Generated columns, collations and patterns** | | | |
| [A virtual generated column a definition reads](#a-virtual-generated-column-a-definition-reads) | `VirtualGeneratedColumn` | [Shape](#shape) | Make it `STORED`, or write its expression in the definition |
| A nondeterministic collation on a key column (a source key, `GROUP BY` key or `ON` column), on a column passed to `STRPOS` or `REGEXP_COUNT`, or on one compared as text (`=`, `<>`, `!=`, `IS [NOT] DISTINCT FROM`, `NULLIF`), directly or through `COALESCE`, a `CASE` result or a field | `NondeterministicKeyCollation`, `NondeterministicCollationTextFunction`, `JoinNondeterministicCollation` | On a key, [Retype](#retype); on a column only a text function reads, [Per row](#per-row) | Keep the collation deterministic ([type-support](type-support.md#collation)) |
| A `REGEXP_COUNT` pattern that isn't a string literal, or that Postgres rejects (`(?P<n>a)`, `a++`, `\pL`); define, `ALTER TRANSFORM` and `RESUME` probe it with `regexp_count('', pattern)` | `NonLiteralRegexPattern`, `InvalidRegexPattern` (with the server's message) | [Text](#text); a Postgres upgrade that changes the verdict is [Per row](#per-row) | Write a pattern Postgres's `regexp_count` accepts |
| **Constants** | | | |
| [A literal zero divisor](#a-literal-zero-divisor) | `ConstantDivisorZero` | [Text](#text) | A nonzero divisor or a column; `x / NULLIF(col, 0)` |
| [A typed literal not in its type's canonical spelling](#a-typed-literal-not-in-its-types-canonical-spelling) | `InvalidTypedLiteral` | [Text](#text) | The spelling Postgres prints: `DATE '2024-01-05'` |
| **Comparisons** | | | |
| [A float compared with an exact numeric](#a-float-compared-with-an-exact-numeric) | `MixedFloatExactComparison` | [Retype](#retype) | Compare two floats or two exact values |
| A comparison between two different temporal types (`date` and `timestamp`, `timestamp` and `timestamptz`, `time` and `timetz`): a conversion to or from `timestamptz` reads the session time zone | `MixedTemporalComparison` | [Retype](#retype) | Cast one side so both are one type |
| `<`, `<=`, `>`, `>=` over `text`, whose order depends on the collation. `=`, `<>`, `!=` and `IS [NOT] DISTINCT FROM` are admitted | `TextOrderingComparison` | [Retype](#retype) | Order by a non-text value, or by `CHAR_LENGTH` |
| A comparison or `NULLIF` between types no family covers: `text` with a number, `boolean` with a number, `uuid` with `text`, and every type outside the families (`bytea`, `jsonb`, `interval`, enums, `inet`, …) | `ComparisonTypeMismatch` | [Retype](#retype) | Compare values of one family; `IS NULL` takes any type |
| Chained comparisons (`a < b < c`, `a = b = c`, `IS DISTINCT FROM` followed by another `IS` form), which Postgres reads as a syntax error | `NonAssociativeComparison` | [Text](#text) | Join them with `AND`, or add parentheses |
| **Grammar forms and typing** | | | |
| `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN`, and the simple `CASE <operand> WHEN <value> THEN …` | `UnsupportedIsForm`, `SimpleCaseUnsupported` | [Text](#text) | `x IS NOT DISTINCT FROM TRUE` (`IS DISTINCT FROM` for the `NOT` forms), `x IS NULL`; `CASE WHEN <operand> = <value> THEN …` |
| [An untyped `NULL` literal](#an-untyped-null-literal) | `UntypedNull` | [Text](#text) | `CAST(NULL AS <type>)` |
| A `CASE` condition that isn't `boolean`, or `CASE` results that don't unify as `COALESCE`'s do (`THEN 1 ELSE 'x'`). Results unify to a common type: two integers to the wider, an exact numeric with a float to the float, otherwise `numeric` | `CaseConditionNotBoolean`, `CaseResultTypeMismatch` | [Retype](#retype) | Make the results one type, with `CAST` |
| `NULLIF(a, b)` where Postgres casts `a` to compare it: an integer `a` with a `numeric` `b`, or an exact `a` with a float `b` | `NullIfFirstArgumentCast` | [Retype](#retype) | `NULLIF(CAST(a AS numeric), b)`, or the float first |
| **Expression size** | | | |
| An expression nested more than 100 levels deep (operands chained by operators, or nested parentheses or calls); a flat list of many terms is not deep | `ExpressionTooDeep` | [Text](#text) | Split it into calculated fields |
| Calculated fields that read each other so often or so deeply that inlining them passes 100 levels or more than a build can render | `FieldExpansionTooLarge` | [Text](#text) | Shorten the chain, or read source columns in the later fields |
| An expression whose fields inline to more than 100,000 nodes (each `WHEN x = 1 THEN 1` branch is four) | `FieldExpansionTooLarge` | [Text](#text) | Shorten it, or split it into calculated fields |
| **Arithmetic, functions and casts** | | | |
| `/` between two integers (`qty / 2`): Postgres truncates the quotient. `numeric` and float quotients are admitted | `IntegerDivision` | [Retype](#retype) | `CAST(qty AS numeric) / 2` |
| `%` with a float operand: Postgres has no float modulo. `%` takes integers and `numeric` | `FloatModulo` | [Retype](#retype) | Use `numeric` operands |
| [An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` does not admit](#an-argument-abs-ceil-floor-round-or-trunc-does-not-admit) | `NumericFunctionArgument`, `ScaleNotIntegerLiteral`, `FunctionArgTypeMismatch` | [Retype](#retype) | A `numeric` value; a literal scale |
| [A `CAST` that is not an exact conversion](#a-cast-that-is-not-an-exact-conversion) | `UnsupportedCast`, `TemporalOperand`, `UnsupportedCastTarget` | [Retype](#retype) | Widen the column, or compute in `numeric`; `tz AT TIME ZONE 'UTC'` |
| **Dates and times** | | | |
| [Date and time arithmetic](#date-and-time-arithmetic) | `TemporalOperand` | [Retype](#retype) | An `integer` day count; `EXTRACT(EPOCH FROM …)` differences |
| `DATE_TRUNC` of a `date` or `timestamptz`, `EXTRACT` of any type but `date` and `timestamp`, and `date_part` (an unsupported function) | `TemporalOperand` | [Retype](#retype) | `DATE_TRUNC('month', CAST(d AS timestamp))`; `… tz AT TIME ZONE 'UTC'`; `EXTRACT` for `date_part` |
| [A `DATE_TRUNC` unit or `EXTRACT` field that is not admitted](#a-date_trunc-unit-or-extract-field-that-is-not-admitted) | `TemporalLiteral` | [Text](#text) | A listed spelling |
| `AT TIME ZONE` with a zone other than the literal `'UTC'`, or over anything but a `timestamptz`: zone offsets change with time zone database releases | `TemporalLiteral` (the zone), `TemporalOperand` (the operand) | [Retype](#retype) | Convert in the source; `CAST(d AS timestamp)` for a date |
| **Retyped and widened columns** | | | |
| A key column (a source key, `GROUP BY` key or `ON` column) retyped so stored values render differently: `timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or `integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal precision or `varchar(n)` | n/a | [Retype](#retype) | `RESUME`, which re-types Trellis's copies and rebuilds |
| [A column Trellis copies, widened](#a-column-trellis-copies-widened) | n/a | [Retype](#retype) | `RESUME`, which re-types the copies and rebuilds |
| **Target columns and widths** | | | |
| Two `GROUP BY` keys that share a target column name (a key listed twice, or a `JOIN`'s `orders.region` and `customers.region`); keys can't be aliased | `GroupByKeysShareTargetColumn` | [Text](#text) | List a key once; for a `JOIN`, join a 1-1 target that renames one side's column |
| A key of more than 32 columns (`INDEX_MAX_KEYS`): a `GROUP BY`, or a `JOIN` side's `ON` and `GROUP BY` columns together; a 1-1 `JOIN`'s `ON` at most 31 | `IndexKeyTooWide` | [Text](#text) | Group by or join on fewer columns |
| [A table of more than 1,600 columns](#a-table-of-more-than-1600-columns) | `TableTooWide` | [Text](#text) | Fewer fields or distinct aggregate arguments |
| **Field, column and table names** | | | |
| A field named after a 1-1 source key column or a 1-1 `JOIN`'s `a` key column (`id AS id`; `id AS order_id` is fine), or a field or `GROUP BY` column whose name starts with `__` | `FieldNamedAfterPrimaryKey`, `ReservedFieldName`, `ReservedGroupByColumnName` | [Text](#text) | Rename the field |
| An aggregate whose argument names another aggregate field no source column is named after (`SUM(val) AS total, SUM(total)`) | `AggregateOfAggregateField` | [Text](#text) | Aggregate the source column |
| A bare name that is both a source column and a field whose expression isn't just that column, read by another field (`(grp + 1) AS val, SUM(val) AS total`; see [Calculated Fields](#calculated-fields)) | `AmbiguousColumnReference` | [Resume only](#resume-only): a source column added under a field's name | Write `src.val` for the column, or rename the field |
| [An unquoted reserved word as a name](#an-unquoted-reserved-word-as-a-name) | `ReservedWord` | [Retype](#retype): renaming a read column is a rename | Quote it (`"order"`), or qualify it (`orders.order`) |
| An unquoted target, `FROM` or `JOIN` table name that Postgres reserves (`FROM user`, `FROM order`); `public.order` is fine | `ReservedTableName` | [Text](#text) | Quote it as the catalog spells it (`FROM "order"`) |
| A name over 63 bytes after folding and unescaping, in any position: Postgres would truncate it | `NameTooLong` | [Text](#text); a definition 0.1.x stored with a longer `AS` label keeps running, its `ALTER TRANSFORM` refuses, and the upgrade that writes a `KEY (…)` into it pauses it | 63 bytes or fewer; drop and define such a stored definition again |
| [An unsupported quoted name](#an-unsupported-quoted-name) | `ZeroLengthQuotedIdentifier`, `UnicodeEscapeIdentifier`, `UnsupportedQuotedIdentifier` | [Text](#text) | ASCII names; function and type names unquoted |
| A qualified name whose table isn't the `FROM` table (`product.cost`), or whose schema isn't its schema; a `JOIN`'s are [below](#an-unsupported-join-shape) | `QualifiedColumnNotFromSource`, `GroupByKeyNotFromSource` | [Text](#text) | Name the `FROM` table, or read the other table with a `JOIN` |
| **Source indexes** | | | |
| [A source without the index its `self_check` walk pages by](#a-source-without-the-index-its-self_check-walk-pages-by) | `SourceIndexMissing` | [Index](#index) | Create the index the message names, or define `WITHOUT SOURCE INDEX` |
| **`JOIN` transforms** | | | |
| [An unsupported `JOIN` shape](#an-unsupported-join-shape) | `TableAlias`, `AliasedTableName`, `JoinTooManyTables`, `JoinSelf`, … | [Text](#text) | Rewrite it in the admitted shape |
| [A 1-1 `JOIN`](#a-1-1-join) | `OneToOneJoinKeyNotUnique`, `OneToOneJoinAggregate`, `OneToOneJoinUnsupported`, `PartitionedSource`, `JoinOnRepeatedColumn` | [Key](#key), [Retype](#retype), [Shape](#shape) | Join `b` on a unique key; give an aggregate a `GROUP BY` |
| [1-1 `JOIN` of more than two tables (star)](#1-1-join-of-more-than-two-tables-star) | `StarJoinTooManyTables`, `JoinOnReadsAnotherJoinedTable`, the 1-1 `JOIN`'s at each position | As for a 1-1 `JOIN`, at each position | At most 5 tables, each joined to `a` on a unique key |
| `ON` columns that differ in type or collation (a modifier never counts; `text` and `varchar` are one type), are off the join-key allowlist in [type-support](type-support.md) (a domain always is), or have a nondeterministic collation; a `GROUP BY` key of a type that can't be one | `JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`, `JoinNondeterministicCollation`, `UnsupportedGroupByKeyType` | [Retype](#retype); a change between deterministic collations pauses nothing, but `RESUME` refuses until they match | Alter one column to match the other, then `RESUME` |
| **Aggregate functions** | | | |
| [`MIN` or `MAX` in a plain aggregate](#min-or-max-in-a-plain-aggregate) | `ExtremeOfExpression`, `UnsupportedExtremeArgument`, `ChainedExtreme`, `ExtremesWithoutSourceIndex`, `ExtremeIndexMissing` | [Index](#index), [Retype](#retype) | Create the index the message names; a plain column |
| `MIN` or `MAX` in a `JOIN` transform | `DeferredAggregate` (`MIN/MAX`) | [Text](#text) | Take it in a plain aggregate over one table |
| `bool_and`, `bool_or`, `bit_and` or `bit_or` | `DeferredAggregate` | [Text](#text) | Not supported yet: [#1118](https://github.com/salesforce-misc/trellis/issues/1118) |
| `SUM` or `AVG` over anything but `smallint`, `integer`, `bigint` and `numeric`: a running float sum drifts from Postgres's | `DeferredAggregate` (`float and interval SUM/AVG`), `UnsupportedAggregateArgument` for another type | [Retype](#retype) | An integer or `numeric` column ([#1119](https://github.com/salesforce-misc/trellis/issues/1119)) |
| `array_agg`, `string_agg` or `jsonb_agg` | `DeferredAggregate` | [Text](#text) | Not supported yet: [#1040](https://github.com/salesforce-misc/trellis/issues/1040) |
| **Filters and chaining** | | | |
| <a id="a-where-other-than-true"></a>A `WHERE` other than `TRUE`: partial data is not supported ([#804](https://github.com/salesforce-misc/trellis/issues/804)) | parse error | [Text](#text) | None |
| Chaining off a transform that is not `live` | `TransformNotLive` | n/a | Wait for the upstream to go `live` |
| **Target names** | | | |
| [A target whose name is taken](#a-target-whose-name-is-taken) | `TargetTableExists`, `TargetNotAdoptable` | n/a | Another name, or `ADOPT EXISTING TABLE` |
| A target that would take the name of a table another transform reads as its `FROM` or joined table, one dropped or renamed away since, or one `ADOPT EXISTING TABLE` would adopt: the reader would read the new target's change feed | `TargetNameHasReaders`, naming each reader | n/a: later, a new reader of the name is a chain, and the target can't be dropped while one reads it (`DependentsBlockDrop`) | `DROP TRANSFORM` each reader named, or another name |
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
in place, or the resume refuses, as [the key entry](#the-key-a-transform-keys-each-table-by)
describes. Plan a scripted `RESUME` after a swap, or after a bulk load with
triggers disabled ([embedding](embedding.md)).

#### The key a transform keys each table by

**Condition:** The key a transform keys each table by: its source's and a
`JOIN`'s joined table's primary key, or the key its definition names with
`KEY (…)` directly after the table (`FROM orders KEY (customer_id, id)`,
`JOIN b KEY (…) ON`); for another transform's target, the key Trellis gave that
target; and its own target's key (a 1-1 target's is its source's key, an
aggregate's or `JOIN`'s its `UNIQUE NULLS NOT DISTINCT` group key).

A source with no primary key and no `KEY`, a source or joined table whose
primary key is deferrable (`DEFERRABLE`, initially immediate or deferred) and
no `KEY`, a `KEY` no index backs, a `KEY` on another transform's target, and a
key other than the one the table's other transforms use.

**Refused at define:** Yes: `NoPrimaryKey` (naming `KEY (…)` and
`ADD PRIMARY KEY` as remedies), `DeferrablePrimaryKey` (naming the key and the
one-statement recreation), `KeyNotBacked`, `KeyOnTrellisTarget` and
`KeyDiffersFromReaders` (naming the key to write). Trellis never keys a table
by a unique index it picked itself, nor by a deferrable key, primary or not:
one can hold duplicate keys inside a transaction.

A `KEY` needs a unique index whose key columns are exactly its columns, in any
order (an `INCLUDE` column is not a key column), valid, not deferrable, not
partial, over no expression, each column under its type's default operator
class and its own collation, with every column `NOT NULL` or the index
`NULLS NOT DISTINCT`; any such index backs it, so a nullable key works through
`KEY` on a `NULLS NOT DISTINCT` index. Its column types are held to the
key-type allowlist as a primary key's are.

A partitioned table's unique index must hold its partition key, so its `KEY`
names those columns too. A table is keyed one way for every transform that
reads it, as its `FROM` or joined table alike: a second definition over a `KEY`
table repeats the `KEY`, and one over a table its other transforms key by its
primary key writes none.

Trellis records one key per table, the first time a transform keys by it, and
keys the table by the primary key or unique index over exactly those columns
for every transform that reads or writes it, in the key's own column order. A
unique index or constraint added to the table later, one an adopted target
(`ADOPT EXISTING TABLE`) already had, one a dump restore creates before the key
(it creates constraints in name order), and the key rebuilt by
`REINDEX CONCURRENTLY` change nothing.

The record belongs to the table it was taken from: a table dropped and created
again is keyed by its own key, whatever transforms of the old one remain.

**If it appears after define:** A source's primary key redefined in place, or
the table created again with another one, pauses the transforms that key it by
its primary key (the rows above). `RESUME` of an aggregate or `JOIN` takes the
table's primary key now as its key, for every transform that reads the table,
and rebuilds; with no primary key it refuses rather than take another unique
index.

A 1-1 transform's target is keyed by its source's key (a 1-1 `JOIN`'s, by its
`FROM` table's): its `RESUME` re-validates it as define would and, where define
would accept it, re-keys the target in place to the new key under the rebuild's
lock.

A new key column the transform doesn't select is added as a key column, copied
as define copies one (its type with no modifier, and its collation), which
empties the target until the rebuild; a former key column it doesn't create is
dropped, and the rows the narrower key repeats are deleted until the rebuild
writes them again (on a plain 1-1 transform,
`ALTER TRANSFORM … ADD <column> AS <column>` once it is live adds it back as a
field); and the primary key is replaced
(`DROP CONSTRAINT …, ADD PRIMARY KEY (…)`), keeping the table, your indexes and
grants.

The pause message says which columns `RESUME` will add and drop. The target's
new key is recorded, which pauses the transforms reading the target, and their
own `RESUME` follows these rules over it.

It refuses, naming the conflict, while define would refuse the transform (over
a partitioned source, say), when a new key column's name is taken by a column
of the target that isn't a copy of it (a field of that name, or a column you
added), and when a view, index or foreign key of yours depends on a column it
would drop or on the key constraint it replaces.

A primary key made deferrable after define (dropped and added again
`DEFERRABLE`) halts every transform reading the table on the next drain of a
change to it, with a `capture_failure` naming the key and the remedy, unless a
unique index that isn't deferrable is over exactly its columns, which the drain
then keys the table by; `RESUME` refuses as define does either way.

A table its transforms key by `KEY` is keyed by it whatever its primary key
does: a primary key added, redefined, made deferrable or dropped pauses
nothing. Its backing index may be replaced by an equivalent one at any time
(build the new one before dropping the old): `REINDEX CONCURRENTLY`, a rename,
pg_repack and `ADD PRIMARY KEY USING INDEX` change nothing.

With none left valid (dropped, a failed build's invalid index, or a partitioned
root's index left invalid), the staging worker's next reconcile pass pauses
every transform reading the table, with no grace and whether or not a write
arrived, naming the table, the key and a ready-to-run `create unique index`; a
swap that drops the old index first pauses when a pass runs between the two.

A partitioned root's index stays invalid after a leaf's index is repaired until
`ALTER INDEX … ATTACH PARTITION` runs again for that leaf, and the message says
so. `RESUME` and a rebuild refuse until an index backs the key again, naming
the table, the key and the unique indexes it has.

A target, its own or one it reads, left with no valid, non-partial,
non-deferrable unique index over its key (dropped, or never created by a
restore): `RESUME` and a rebuild refuse, naming the table, the key and the
unique indexes it has, and Trellis never keys the table by another index.

**Remedy:** Name the key with `KEY (…)`, backed by a unique index over exactly
those columns, or add a primary key (`ALTER TABLE … ADD PRIMARY KEY`, or
`ADD PRIMARY KEY USING INDEX` over an existing unique index); write the key the
message names. Recreate a deferrable primary key not deferrable in one
statement (`ALTER TABLE … DROP CONSTRAINT …, ADD PRIMARY KEY (…)`: Postgres
can't change it in place), then `RESUME`, or name a `KEY (…)` over a unique
index that isn't deferrable.

For an aggregate or `JOIN`, `RESUME`, after adding a primary key if the table
has none. For a 1-1 transform, `RESUME`; when it refuses, drop the object it
names, drop or rename your column it names, or, for a field of the new key
column's name, restore the key, resume and rename the field with
`ALTER TRANSFORM`; or `DROP TRANSFORM` and define it again. For a `KEY`, create
a unique index backing it again (on a partitioned table, re-run
`ALTER INDEX … ATTACH PARTITION` for a repaired leaf's index), then `RESUME`.

For a target, create a unique index over exactly its key columns again
(`UNIQUE NULLS NOT DISTINCT` if one can be `NULL`), then `RESUME`; or
`DROP TRANSFORM` and define it again.

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

#### A literal zero divisor

A `/` or `%` whose divisor is written as a literal zero is refused. That covers
a number literal (`n / 0`, `n / 0.0`, `n % -0`) or a float typed literal
(`f / REAL '0'`, `f / DOUBLE PRECISION '-0'`), under any `CAST` or prefix `-`,
whatever the dividend, and wherever the division sits. It can be in a `CASE`
branch no row takes, after a constant that ends a `COALESCE`, `AND` or `OR`,
beside a `NULL` constant that makes Postgres drop the division
(`(n / 0) + NULL`), or in an aggregate's argument. Postgres raises `22012` for
every row that has a dividend. The error names the field and the division as
written.

Only the text is read. A divisor that computes to zero defines (`n / (1 - 1)`,
`n / COALESCE(0, 1)`, `n / CEIL(0.0)`, or a calculated field that is `0`). Its
rows fail at run time like any other per-row error, as `x / col` does where
`col` is zero.

#### A typed literal not in its type's canonical spelling

This covers a `DATE`, `TIMESTAMP`, `TIME`, `TIMETZ`, `BYTEA`, `OID`, `REAL`,
`DOUBLE PRECISION`, `INET`, `CIDR`, `MACADDR`, `MACADDR8`, `VARBIT` or `JSONB`
literal, or the `CAST('…' AS <type>)` spelling of one, whose text is not what
Postgres's output function prints for the value: `DATE '2024-1-5'`,
`DATE 'today'`, `REAL '1.50'`, `JSONB '{"b": 1, "a": 2}'`. Postgres accepts many
of these, but relative date and time spellings are not immutable, and the
check needs no Postgres
([ADR-0004](decisions/0004-transform-definition-grammar.md)). The error names
the field and the literal, and describes the spelling the type expects.
`SELECT CAST('2024-1-5' AS date)::text` shows the canonical spelling.

#### A float compared with an exact numeric

This is a comparison (`=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`,
`IS [NOT] DISTINCT FROM`, and `NULLIF`'s `=`) with a float (`real`,
`double precision`) on one side, and an exact numeric (`smallint`, `integer`,
`bigint` or `numeric`) column, calculated field or expression on the other,
including a calculated field or `COALESCE` that is a float. Postgres casts the
exact side to `double precision`, so the comparison inherits float rounding
(`0.1::numeric + 0.2 = 0.3::float8` is false). The error names the field and
both operand types. A float against a float, and an exact numeric against an
exact numeric, are admitted, and `+` admits the mix.

A bare number literal against a float is admitted, so `f > 0.5` and `f = 1`
define. The literal is cast to `double precision`, and a `real` operand widens
to it, as in Postgres, so `r > 0.1` over `r = 0.1::real` is true. Write
`f > REAL '0.1'` to compare as `real`.

#### An untyped `NULL` literal

This is a `NULL` literal its context gives no type. It covers both operands of
an arithmetic operator (`NULL + NULL`), and the operand of prefix `-` or of
`AT TIME ZONE`. It also covers an operand beside a type that no operator define
admits takes on both sides: `date_col + NULL`, which Postgres finds ambiguous,
and `ts_col - NULL`, which it reads as the `interval`-producing
`timestamp - timestamp`. An argument of a function or aggregate (`abs(NULL)`,
`SUM(NULL)`, `COUNT(NULL)`) counts too, directly or through a field that is
only `NULL`. Postgres finds some of these ambiguous, and types the rest by its
function-candidate rules (`abs(NULL)` is `double precision`), which define
does not model. The error names the field and the construct. Write
`abs(CAST(NULL AS integer))`.

#### An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` does not admit

`ABS` takes an integer, `numeric` or float, and returns its type. `CEIL`,
`FLOOR` and `TRUNC` take `numeric` or `double precision`. `ROUND(a)`,
`ROUND(a, n)` and `TRUNC(a, n)` take a `numeric` `a` and an `n` that is an
integer literal (negative allowed: `ROUND(a, -2)`), and return `numeric`.

These are refused:

- An integer argument: it is already whole, and Postgres would cast it to
  `double precision` and change the result's type.
- A `real` argument to `CEIL`, `FLOOR` or `TRUNC`: it is promoted to
  `double precision`.
- A float argument to `ROUND`: a float rounds half to even and `numeric` half
  away from zero, so one name would carry two rules.
- A scale that is a column or an expression.

The errors are `NumericFunctionArgument` (with the reason),
`ScaleNotIntegerLiteral`, and `FunctionArgTypeMismatch` for a non-numeric
argument, each naming the field and the function. Use the integer itself, or
`CAST(qty AS numeric)`.

#### A `CAST` that is not an exact conversion

Only exact conversions that read no setting are admitted:

- an integer to the same or a wider integer (`CAST(smallint_col AS integer)`,
  `AS bigint`);
- any integer to `numeric`;
- a `date` to `timestamp`;
- a cast to the operand's own type (`numeric` to `numeric`, `timestamp` to
  `timestamp`). Postgres runs it as the identity, so `numeric(12, 2)` `1.50`
  stays `1.50`, and a `timestamp(3)` keeps its fractional digits.

The accepted target spellings are `integer` (or `int`, `int4`), `bigint` (or
`int8`), `numeric` (or `decimal`) and `timestamp`, in any case, and only in the
target position: a column named `int` is still a column. An alias is typed and
checked as its canonical target, so `CAST(bigint_col AS int)` narrows and is
refused, and `ALTER TRANSFORM` shows the canonical spelling. A `date` past
294276-12-31 is a value `timestamp` cannot hold: its row raises `22008` and is
quarantined.

These are refused:

- `UnsupportedCast`, naming both types: a narrowing (`bigint` to `integer`), a
  cast from a float or a non-numeric type, or a cast to or from a float.
- `TemporalOperand`: a `timestamptz` to `timestamp`, which converts through
  the session `TimeZone`. Use `tz AT TIME ZONE 'UTC'` instead.
- `UnsupportedCastTarget`: any other target spelling (`smallint`, `text`,
  `real`, `int2`, `dec`, `timestamptz`, `date`), or a modifier
  (`numeric(10, 2)`, `timestamp(3)`), which rounds and can overflow.

The cast of a quoted literal (`CAST('1.5' AS real)`) is a typed literal, with
its own rules. After define, a widening can turn a cast into a narrowing
(`smallint` to `bigint` under `CAST(c AS integer)`), and that pauses the
transform. A retype that makes a cast an identity (`integer` to `bigint` under
`CAST(c AS bigint)`) pauses nothing.

#### Date and time arithmetic

`+` and `-` over a date or time operand admit only `date ± integer`,
`integer + date` and `date - date`, where the count of days is a `smallint` or
an `integer`. Refused, with `TemporalOperand` naming the operator, the operand
type and the reason:

- a `date` with a `bigint`, `numeric` or float (`d + n`): Postgres has no
  `date + bigint` or `date + numeric`;
- `date + date`, or a number minus a date;
- a difference of two timestamps, or of a date and a timestamp (`ts2 - ts1`,
  `tz2 - tz1`);
- any operand of type `interval`;
- a number added to a `timestamp`, `timestamptz`, `time` or `timetz`.

A difference of timestamps is an `interval`, which has no literal. Its month
part makes a result depend on the date it is added to
(`'2024-01-31' + 1 month - 1 month` is not `'2024-01-31'`), so nothing here
produces or consumes one. For a duration,
`EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)` is exact `numeric` seconds.

`DATE_TRUNC('month', d)` resolves to the `timestamptz` overload, which is
`STABLE`. Both `DATE_TRUNC` and `EXTRACT` read the session `TimeZone` on a
`timestamptz`, and `date_part` returns `double precision`. `EXTRACT` returns an
unconstrained `numeric`, which `SUM`, `AVG` and `COUNT` take. To group by a
bucket, chain two transforms: a 1-1 transform computes
`DATE_TRUNC('month', ts) AS bucket`, and an aggregate over its target uses
`GROUP BY bucket`.

#### A `DATE_TRUNC` unit or `EXTRACT` field that is not admitted

A unit or field must be a literal, written in any case.

- **`DATE_TRUNC` units:** `microseconds`, `milliseconds`, `second`, `minute`,
  `hour`, `day`, `week` (truncates to Monday), `month`, `quarter`, `year`,
  `decade`, `century`, `millennium`.
- **`EXTRACT` fields of a `date`:** `year`, `quarter`, `month`, `week`, `day`,
  `dow`, `isodow`, `doy`, `isoyear`, `decade`, `century`, `millennium`,
  `epoch`. A `timestamp` also has `hour`, `minute`, `second`, `milliseconds`
  and `microseconds`.

Refused: `EXTRACT(hour FROM <date>)`, `julian`, the `timezone` fields,
abbreviations and plurals (`y`, `days`, `mon`), and a unit given as a column.
A field Postgres lacks for the type raises in Postgres, and `julian` counts
from a different origin for each type. The error names the construct, the
text and what is admitted. `'UTC'` in `AT TIME ZONE` is parsed under
`timezone_abbreviations`, which Trellis's sessions pin to `Default`.

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

#### An unquoted reserved word as a name

This is an unquoted name that Postgres reserves, used in an expression, a
`GROUP BY` or `KEY (…)` column, or a `JOIN`'s `ON` and `GROUP BY` qualifier. A
reserved name is any word `pg_get_keywords()` marks reserved (`R`) or reserved
as a type or function name (`T`), such as `order`, `user`, `end`, `not`,
`from`, `left`, `case`, `and` and `is`. Postgres needs such a column quoted:
`GROUP BY current_user` reads the function there, and `GROUP BY order` is a
syntax error. The error names the word and its quoted spelling.

These are not bare names and are fine:

- a quoted name (`"order"`);
- a name after a `.` (`t.order`);
- the label after `AS` (`val AS limit`);
- a call (`left(x, 2)`).

A non-reserved keyword (`extract`, `position`, `values`, `between`,
`coalesce`, `nullif`, `char`) is still a column. A source table with a column
of a reserved name defines, and an expression reads the column quoted or
qualified. `"order" AS "order"` copies it under its own name.

#### An unsupported quoted name

Refused, each as a parse error:

- a zero-length quoted name (`""`), which Postgres refuses too;
- the Unicode-escape spelling `U&"…"`;
- a quoted name holding a `.` or a non-ASCII character;
- a quoted function or type name (`"lower"(x)`, `CAST(x AS "int4")`,
  `"date" '2024-01-31'`).

Postgres accepts the last of these, and looks a quoted function or type name
up exactly, so `"int4"` is a type there and `"INT4"` and `"integer"` are not.
Trellis addresses a table by its `<schema>.<table>` text, which a `.` inside a
name would make ambiguous. Rename the table or column, or write the name in
ASCII characters.

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
  comma joins (parse errors). For `RIGHT JOIN`, swap the tables and use
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
drains. An `a` that is a 1-1 target may be joined on any columns. A field is
any expression over `<table>.<column>` columns of either table. The target is
keyed by `a`'s primary key.

The joined table needs no source index. Trellis builds the target in chunks of
`a`'s primary key, keeps it readable across a `RESUME`, and applies changes,
re-derives and `TRUNCATE`s of either table. A `TRUNCATE` of `a` empties the
target. A `TRUNCATE` of `b` starts a rebuild, so the transform reads
`backfilling`, and a row keeps the `b` columns it held (an `INNER` row stays)
until the rebuild reaches it.

A `b` that is another transform's target is never truncated. A `TRUNCATE` of
that transform's source reaches the `JOIN` as a delete of every row of `b`,
like any other change of `b`. A page whose changes fail on more rows of `a`
than its bisection can name pauses the transform and everything downstream of
it, with a `capture_failure` of kind `halt`. The same goes for a chunk of a
`b` change's fan-out. Otherwise, a row of `a` that a page fails to write, for
its own change or a change of `b`, is held in quarantine on its first failure,
and the page applies every other change.

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
([the key entry](#the-key-a-transform-keys-each-table-by)).

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
being relabelled. A `TRUNCATE` of a joined table, at any position, starts a
rebuild. One statement that truncates several of the star's tables is one
rebuild. `RESUME` re-validates every position as define does, and re-keys the
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
`TargetTableExists`, a `conflict` error. Trellis writes only to target tables
it creates, or to an empty table it adopts on request, and `DROP TRANSFORM`
drops the target, so it never takes over an existing relation unasked. Choose
another target name, give the instance its own target schema
([instance-identity](instance-identity.md)), or drop or rename the existing
relation.

With `ADOPT EXISTING TABLE` after the target
(`TRANSFORM t ADOPT EXISTING TABLE FROM s …`), define builds into the existing
table instead. The table must be plain, empty, and shaped exactly as define
would create it:

- the same columns, in the same order;
- each column with the type (no modifier), collation and nullability define
  gives it;
- the key define creates: the primary key, or an aggregate's or `JOIN`'s
  `UNIQUE NULLS NOT DISTINCT` group key, not deferrable and with no `INCLUDE`
  columns.

Define records that key, so a unique constraint the table already has never
keys it instead ([the key entry](#the-key-a-transform-keys-each-table-by)). The
table may carry anything a user may add to any target, with the outcomes
[any target has](#target-tables-are-trellis-owned): indexes, unique or not;
constraints; triggers; rules; foreign keys, its own or another table's
referencing it; grants; comments; row security policies, subject to
[the row-security entry](#row-level-security-on-a-role-trellis-runs-as);
extended statistics; publications; views over it; storage parameters; and a
replica identity.

Anything else refuses with `TargetNotAdoptable`, a `validation` error whose
message names the property by code:

- `missing`: no such table (a `not_found` error);
- `not_a_table`, `partitioned`, `partition`, `inheritance`, `typed_table`,
  `unlogged`, `extension_member`, `access_method`, `not_empty`;
- `not_owner`: the defining role is neither the table's owner nor a member
  that inherits the owner's privileges, so it couldn't alter or drop the table
  later;
- `privilege`: the role lacks `SELECT`, `INSERT`, `UPDATE`, `DELETE` or
  `TRUNCATE` on the table;
- `missing_column`, `extra_column`, `column_order`, `column_type`,
  `column_collation`, `column_nullability`, `column_default`,
  `generated_column`, `identity_column`, `key`, `owned_sequence`;
- `unrecognized`: a constraint or dependent object the check doesn't know, so
  a new Postgres feature refuses until Trellis learns it.

The message names the first difference. To fix it, empty the table, fix the
column, `ALTER TABLE … OWNER TO` the defining role or grant it membership in
the owner `WITH INHERIT TRUE`, or grant the missing privilege.

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
with or without a `DEFAULT`) can be a source on PostgreSQL 15 or later, with up
to 1,000 leaves. It can be the `FROM` of an aggregate, or either table of a
`JOIN`. A target is always a plain table. Define accepts it when the group key
holds every partition-key column at every level: an aggregate's `GROUP BY`, or
a `JOIN` table's `GROUP BY` and `ON` columns from that table. Each group then
lives in exactly one partition, so a change to the tree touches only the
groups that route to one partition, and the catalog says which those are. A
fact table partitioned on the column it joins by needs nothing more in its
`GROUP BY`, while one partitioned on its own id is refused. Everything else is
refused at define, or pauses a definition later, as the
[refusal table](#refusals) lists
([ADR-0017](decisions/0017-partitioned-sources.md)).

```text
-- order_line_items is partitioned BY HASH (order_id)
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount
```

Every partition carries the capture triggers, so a write named on the parent or
on any partition is counted once, and an `UPDATE` through the parent that moves
a row to another partition counts as a delete and an insert. A write to a table
that has left the tree counts for nothing.

Partition maintenance needs no operator step. The staging worker's reconcile
pass (every 5 seconds by default) detects each change to the tree, installs
capture on a new partition, and queues a correction for the partition the
change touched, which recomputes the groups that route to it (a `JOIN`
recomputes the partitioned side's partials that route to it, and carries the
change into its target). A `DEFAULT`
whose siblings changed is corrected like a new partition. Until the pass sees
the DDL, at most one pass, the definition still reads `live`; from then until
the correction has drained it reads `backfilling`, with the partition in
`correcting` ([Status](#status)). From the DDL to the end of the correction,
the **window**, counts are wrong in one direction:

| Operation | Window | Direction | Avoiding the window |
|---|---|---|---|
| `ATTACH PARTITION` of a table that holds rows | At most one pass, plus the correction of that partition. | Rows added: their groups are undercounted. | Create the partition empty and move the rows with DML through the parent: about 4-5 s and 0.5-0.6 GB of WAL per 1M-row partition. |
| A new partition written to directly before the pass installs capture on it | At most one pass, plus the correction of that partition. | Rows added: undercounted. | Create the partition empty and write through the parent. That has no window and costs nothing. |
| `DETACH PARTITION` (also `CONCURRENTLY`, whose correction starts once the detach completes) or `DROP` of a partition | At most one pass, plus the correction of that partition, routed by the partition's recorded bound. | Rows removed: their groups are overcounted. | Delete the partition's rows with DML through the parent first. |
| `TRUNCATE` of a partition or of a middle partitioned table | At most one pass, plus the correction of that partition. | Rows removed: overcounted. | Delete the rows with DML through the parent first. |
| Groups that route to no current partition, with no `DEFAULT` to take them (found by the pass's sweep of the bounds' complement) | At most one pass, plus the correction. | Rows removed: overcounted. | The operations above. |
| `INSERT`, `UPDATE` and `DELETE` through the parent or straight to a partition, including an `UPDATE` that moves a row to another partition | None. | n/a | n/a |

A `TRUNCATE` of the parent clears the target as it does for a plain table.
The cases that leave a target stale with no correction are in
[known correctness gaps](known-correctness-gaps.md#a-merge-that-moves-a-row-to-another-partition)
and the entries after it.
What a correction
costs, and what a dump and restore, a `pg_upgrade` and pg_partman do, is in
[operations — Partitioned sources](operations.md#partitioned-sources).

## Names

Names follow Postgres: an unquoted name folds to lower case, and a quoted name
is exact, with `""` standing for a `"`. That holds wherever a definition or a
statement names something: the target, the `FROM` and `JOIN` tables and their
schemas, columns, `KEY (…)`, `GROUP BY`, `AS` labels, table aliases, and the
transform `PAUSE`, `RESUME`, `DROP` and `ALTER TRANSFORM` address. So
`SUM(Total)` reads the column `total`, as in psql, and a name created with
upper-case letters is written quoted. A Prisma model's table `"User"` with a
column `"orgId"`:

```sql
TRANSFORM org_users FROM "User" GROUP BY "orgId" SELECT COUNT(*) AS members
```

A quoted name is never a keyword: `"order"` is a column, and `FROM "user"` a
table. The names Trellis refuses (over 63 bytes, an unquoted reserved table
name, a few quoted forms) are in
[Supported sources and targets](#supported-sources-and-targets). A name that
finds nothing while one differing
from it only in case exists is refused naming the quoted spelling:
`DROP TRANSFORM OrderTotals` beside a target created as `"OrderTotals"` refuses
with `did you mean "OrderTotals"?`, and drops nothing. Rendered text, such as
the definition `ALTER TRANSFORM` stores, quotes every name that needs it.

## Granularity

Granularity determines the target's primary-key space — what a single target row
represents relative to its source row(s). The grammar has four: 1-1, aggregate, join aggregate and 1-1 `JOIN`.

### 1-1

Exactly one target row per source row, inheriting the source's identity: its
primary key or the `KEY (…)` the definition names, or, when the source's key is nullable (an aggregate or `JOIN`
target's grouping columns), a `UNIQUE NULLS NOT DISTINCT` constraint over the
same columns, so a group whose key has a `NULL` part has its row here too.
Insertion/deletion maps 1-1. Always a single source table: a target with one
row per row of one table and columns of a second is a [1-1 `JOIN`](#1-1-join),
and one with a row per group of a join is a [join aggregate](#join-aggregate).

The target always carries the source's key columns, so a field can't
reuse a key column's name (see [Supported sources and targets](#supported-sources-and-targets)).

### Aggregate (`GROUP BY`)

Each distinct combination of grouping values produces one target row, matching
the equivalent `GROUP BY` query against the source. The primary key is the tuple
of grouping columns; many source rows can map to one target row, and adding,
removing, or changing a source row can insert, delete, or update a target row.

Grouping-key columns may be referenced directly; any other source column must be
wrapped in exactly one aggregate: `COUNT(*)`, which counts the rows in the
group, `COUNT(<expr>)`, which counts the rows where `<expr>` isn't null, and
`SUM` and `AVG` over `smallint`, `integer`, `bigint` or `numeric`. An
argument may be a column or an expression over the row (`SUM(qty + 1)`), and a
field may compose aggregates (`COALESCE(SUM(amount), 0) + COUNT(*)`). These are
the aggregates Trellis keeps exact from each change alone, without reading the
source again. `MIN` and `MAX` over a plain column of a number, date or time
type are kept too: a deletion that removes a group's extreme re-reads the
group's next one from the source, through an index on the `GROUP BY` columns
then the argument, which define requires
([type support](type-support.md#min-and-max)). Every other aggregate is refused
at define, named by its family
([Supported sources and targets](#supported-sources-and-targets)). Result
types match Postgres's: `COUNT` is `bigint`, `SUM` over `smallint` or `integer`
is `bigint`, `SUM` over `bigint` or `numeric`, and `AVG`, are `numeric`, and
`MIN` and `MAX` have their argument's type. A `SUM` over a `numeric` shows the
scale Postgres's `sum()` shows, the largest among the group's current values
(`1.5` and `2.000` sum to `3.500`, and to `1.5` once `2.000` leaves), and an
`AVG` the value Postgres's `avg()` gives; `NaN`, `Infinity` and `-Infinity`
count as they do in Postgres ([type support](type-support.md#numeric-and-floating-point)).

Each key becomes a target column named after its bare column, and keys can't be
aliased. The target holds exactly the declared columns: the state that keeps
it exact (each group's row count and running sums) lives in a table Trellis
keeps in its own catalog schema, named by the transform's internal id, which
readers get no grant on. A group has a row in the target only while it has
rows in the source. Name limits are in
[Supported sources and targets](#supported-sources-and-targets).

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
```

**Throughput falls as more distinct groups receive writes at once.** A page
locks every group it touches until it commits, so drain workers whose pages
share groups queue behind each other for those rows. On a disk-backed
benchmark (`benchmark group-contention`, 8 writer threads on a 16-core box), a
single `SUM`/`COUNT(*)` aggregate applied about 233k to 308k source rows/sec
over 10 to 400 groups, 190k to 200k over 4,000 and 75k to 90k over 40,000.
Throughput rises with writer threads up to about 8 (about 70k rows/sec with
one, 230k with eight) and stays flat beyond that. If you're planning an
aggregate with tens of thousands of groups written concurrently, benchmark it
on your own storage first (`benchmark group-contention --groups <n>`).

Values read before `await_converged` may be inconsistent, and are exact after
it; on small or hot groups that can include a combination no source state
produces, such as a negative `COUNT(x)`. See
[Reading derived data](data-flow.md#reading-derived-data).

### Join aggregate

An aggregate over the join of two tables (`FROM a [LEFT|INNER] JOIN b ON …
GROUP BY …`). Its target holds one row per group: a column per `GROUP BY` key
and per field, and nothing else. The state that keeps it exact lives in tables
Trellis keeps in its own catalog schema, named by the transform's internal id,
which readers get no grant on. Define registers it and creates its target, and
Trellis builds it from both tables, then applies every change captured on
either. Its aggregates are a plain aggregate's, each argument reading one
table's columns.
[Supported sources and targets](#supported-sources-and-targets) lists the
checks.

Every column is written with its table, `<table>.<column>`, in `ON`,
`GROUP BY` and fields alike. Either table may be given an alias, with or
without `AS`, and is then named by its alias alone, as in Postgres: its
columns are written `<alias>.<column>`, and its own name no longer qualifies
them.

```text
TRANSFORM sales_by_tier FROM shop.orders AS o JOIN customers c
  ON o.customer_id = c.id
GROUP BY c.tier
SELECT c.tier AS tier, SUM(o.amount) AS total
```

Both tables are read the way a `FROM` is: each is captured, and a column the
transform reads that either table loses, or a key column either retypes,
pauses the whole transform on the staging worker's next pass, naming that
table. `RESUME` checks both tables again.

The same read contract applies: before `await_converged` a small or hot group
can show a combination no source state produces, and after it values are exact
([Reading derived data](data-flow.md#reading-derived-data)).

### 1-1 `JOIN`

A `JOIN` with no `GROUP BY` (`FROM a [LEFT|INNER] JOIN b ON … SELECT …`)
keeps one target row per row of `a`, with the columns of the row of `b` it
joins to ([ADR-0021](decisions/0021-one-to-one-join-transforms.md)). It is for
a value computed from both tables that a reader queries and indexes without
repeating the join:

```text
TRANSFORM order_view FROM orders LEFT JOIN customers
  ON orders.customer_id = customers.id
SELECT
  customers.name AS customer_name,
  orders.amount * COALESCE(customers.discount, 1) AS discounted
```

The target is keyed by `a`'s key (its primary key, or its `KEY (…)`), whose columns it carries as a 1-1
target does, and holds them and the declared fields. `LEFT` keeps an `a` row
with no matching `b` row, with `NULL` for every `b` column; `INNER` (or a bare
`JOIN`) leaves it out until a `b` row on its key arrives. `b`'s `ON` columns
are unique, so an `a` row joins at most one row of `b`: they contain `b`'s
primary key, a unique constraint or unique index, or the group key of an
aggregate or `JOIN` target. The `ON` follows a join aggregate's rules: `AND`ed
equalities between a column of each table, of one type and collation.

A 1-1 `JOIN` may join up to four tables to `a`, a **star**: each `JOIN` clause
joins one more table at the next **position** (1 for the first joined table),
each on its own unique key, and its `ON` reads `a` and that table only:

```text
TRANSFORM order_view FROM orders
  LEFT JOIN customers ON orders.customer_id = customers.id
  JOIN regions ON orders.region_id = regions.id
SELECT
  customers.name AS customer_name,
  regions.name AS region
```

Each position is `LEFT` or `INNER` on its own: the target has a row for an `a`
row only where every `INNER` position has a match, with `NULL` for the columns
of a `LEFT` position that has none. The two-table `JOIN` is a star with one
position, and everything below holds at every position: what is said of `b`
holds of the table joined at each position.

A field is any expression a 1-1 field admits, floats and text included, over
columns of either table, each written `<table>.<column>` (or
`<alias>.<column>`, as for a join aggregate); there is no
aggregate, and a field doesn't read another field. Each row is recomputed
from its current inputs rather than maintained by deltas, so none of an
aggregate's restrictions on its arguments apply. Either table may be another
transform's target, read through that target's change feed as for a join
aggregate. [Supported sources and targets](#supported-sources-and-targets)
lists the checks.

Trellis keeps a copy of each table (`R` for `a`, an `M` for the table at each
position) and a lock row per position and join key (`J`) in its own catalog
schema ([data flow](data-flow.md#a-1-1-joins-hidden-state)), named by the transform's
internal id, which readers get no grant on. Define registers the transform,
creates its target and builds it from both tables in chunks of `a`'s primary
key. Then:

- A change of an `a` row rewrites its target row.
- A change of a `b` row recomputes every `a` row on its join key at its
  position. Its page
  recomputes the keys whose rows fit one chunk of 10,000, and leaves each other
  key to chunks of 10,000 rows, each its own transaction, which the drain
  workers run after their segments. An `a` write on the key waits for one
  chunk at most, and a watermark token past the `b` change converges once the
  last chunk commits. `status()` warns, per position, when a join key holds
  more than 250,000 `a` rows, a key whose `b` change takes seconds to reach
  every row ([status reference](status-reference.md#join)).
- A `TRUNCATE` of `a` empties the target; one of a joined table, at any
  position, rebuilds it.
- A row whose field Postgres can't compute (an overflow, a division by zero)
  is held in quarantine on its first failure, and the page applies the rest.
  A row held by a joined table's change says so in its error
  (`sample_quarantined`): the table, its position and the row's join key there.

A schema change of any of its tables pauses the whole transform as it does a
join aggregate. `self_check` compares the target, every copy and the uniqueness
of each joined table's join key.
The same read contract applies: a value read before `await_converged` may be
inconsistent, and is exact after it
([Reading derived data](data-flow.md#reading-derived-data)).

A joined table that is edited often, or that has few distinct keys, fans each
edit out over many target rows; see [Join small, often-edited tables at query
time](recommendations.md#join-small-often-edited-tables-at-query-time).

## Calculated Fields

Every target table has a set of **calculated fields** — non-key columns populated
by a formula rather than copied from a source column. A formula may reference:

* Source columns feeding the calculated row, following the shape its granularity
  requires (see [Granularity](#granularity)).
* Other calculated columns on the same target table, by name.

A bare name means a field when no source column has that name, and the source
column when no field does. A name that is both a source column and a field whose
expression is something else is refused at define as ambiguous, with both
meanings in the error ([Supported sources and targets](#supported-sources-and-targets)).
A 1-1 target refuses such a field outright, whether or not anything reads it.
Two reads of such a name are not ambiguous:

* A field reading its own name reads the source column (`SUM(val) AS val`), and
  a field that passes its column through (`val AS val`, or qualified,
  `src.val AS val`) is that column.
* In an aggregate target, an aggregate's argument never reads a column that is
  itself an aggregate: a name there means the source column, as in SQL.
  `SUM(val) AS val, COUNT(val) AS n` counts the source column `val`, not the
  sum. A column that isn't an aggregate (`(id + 1) AS adj,
  SUM(adj)`) is still read by name, as long as no source column is named `adj`.

A **qualified name** always reads the source column: `src.val`, or
`public.src.val`, where `src` is the `FROM` table. So `(grp + 1) AS val,
SUM(src.val) AS total` sums the source column beside the field that shadows it.
A two-part name whose head is not the `FROM` table is refused at define; a
second table is read with a `JOIN`. A three-part name must name the `FROM`
table and its schema. A field has no qualified form: to
read a field a source column shadows, rename the field.

A formula compares and combines values with `=`, `<>` (or `!=`), `<`, `<=`,
`>`, `>=`, `IS [NOT] DISTINCT FROM`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, the
searched `CASE WHEN <boolean> THEN <value> [WHEN ...] [ELSE <value>] END` and
`NULLIF(a, b)`, over the literals `TRUE`, `FALSE` and `NULL` among other
operands. Postgres evaluates them, so `NULL` follows its three-valued
logic: `NULL = NULL` is `NULL`, `NULL IS NOT DISTINCT FROM NULL` is true,
`NULL AND FALSE` is false, `NaN = NaN` is true. A `NULL` takes its type from
its context, as in Postgres: the other operand's (`int_col + NULL` is
`integer`), the other `CASE` results' or `COALESCE` and `NULLIF` arguments',
`boolean` under `AND`, `OR`, `NOT` and `WHEN`, and `CAST(NULL AS date)` names
it; a field that is only `NULL` is a `text` column ([type
support](type-support.md#comparison-and-logic)). A comparison, `IS NULL` or logic field is a `boolean`
column, and another field may read it. Binding, loosest first: `OR`, `AND`,
`NOT`, `IS ...`, the comparisons, `+` and `-`, `*` `/` `%`, prefix `-`; so
`NOT a = b` is `NOT (a = b)` and `a = b IS NULL` is `(a = b) IS NULL`. The
comparisons do not chain (`a < b < c` is refused, as in Postgres). Postgres
promises no evaluation order inside `AND` and `OR`, so `y <> 0 AND x / y > 1`
can still divide by zero; guard a division with `CASE WHEN y <> 0 THEN x / y
END` or `x / NULLIF(y, 0)`. A failure inside a `CASE` branch is raised only
for the rows that take the branch, except a constant Postgres folds while
planning (a `2147483647 + 1` in a branch fails every row). In
an aggregate, `COUNT(CASE WHEN c THEN 1 END)` and `SUM(CASE WHEN c THEN qty
ELSE 0 END)` over an integer define, and so does `SUM(CASE WHEN c THEN amount
ELSE 0 END)` over a `numeric(10,2)` column, whose sum shows the scale Postgres
shows.
`NOT` and `CASE` are keywords wherever an operand follows (a `-` begins one:
`NOT -x > 0` is `NOT ((-x) > 0)`, as in Postgres). A word Postgres reserves is
not an unquoted column name in an expression (`not`, `case`, `end`, `and`,
`order`, `user`...); a source column with such a name is read quoted
(`"order"`) or qualified with its table (`orders.order`), as in Postgres; see
[Supported sources and targets](#supported-sources-and-targets). Families, types and
refusals are in [Supported sources and
targets](#supported-sources-and-targets) and [type
support](type-support.md#comparison-and-logic).

A formula's arithmetic is Postgres's: `+`, `-`, `*`, `/`, `%`, prefix `-`,
`ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNC` and the `CAST` of an integer
(to `integer` or `int`/`int4`, `bigint` or `int8`, `numeric` or `decimal`; result types in [type support](type-support.md#arithmetic)). `*`, `/` and `%`
bind tighter than `+` and `-`, prefix `-` tighter than all of them, and
operators of one tier group left to right: `a - b * c` is `a - (b * c)`,
`-a * b` is `(-a) * b`, `a - b - c` is `(a - b) - c`. A minus before a number
literal is one negative literal, typed by its value as Postgres types it, so
`-2147483648` is an `integer`. What Postgres would do differently from what a
reader expects is refused at define
([Supported sources and targets](#supported-sources-and-targets)). A value an
operation cannot compute for one row (an overflow, a division by zero) raises
`22003` or `22012` for that row's record, which is quarantined and charged to
the field. Define doesn't evaluate constant expressions as Postgres's planner
does, so a definition with a constant Postgres fails while planning
(`2147483647 + 1`, `DATE '2024-01-01' + 2147483647`, a `COALESCE(real_col,
...)` whose constant is past `real`'s range) is accepted, and every row it
computes fails with Postgres's error, as any per-row error does
([ADR-0003](decisions/0003-quarantine-storage-and-api.md)). A 1-1 applying
changes holds its first failing keys, then its column fuse pauses the field:
new rows get `NULL` for it, its other fields keep updating and the definition
stays `live` ([a paused column](operations.md#when-a-definition-needs-you)).
A 1-1 or `JOIN` build over existing rows, and a `JOIN` applying changes,
quarantine the definition, and an aggregate's build fails whole and pauses
it. Only a divisor written as a literal zero is refused.

A formula's dates are Postgres's too. `date + n`, `n + date` and `date - n` add
or subtract a count of days (a `smallint` or `integer`) and give a `date`;
`date - date` is the days between, an `integer`. `DATE_TRUNC('<unit>', ts)`
truncates a `timestamp` (`week` to Monday); `EXTRACT(<field> FROM x)` reads a
part of a `date` or `timestamp` as a `numeric`; `tz AT TIME ZONE 'UTC'` turns a
`timestamptz` into the `timestamp` it is in UTC, and `CAST(d AS timestamp)`
turns a `date` into midnight. `AT TIME ZONE` binds tighter than `*` and looser
than prefix `-`. A duration is `EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)`,
in exact `numeric` seconds; there is no `interval`. `infinity` and `-infinity`
follow Postgres (`infinity + 1` is `infinity`, `EXTRACT(year FROM 'infinity'::date)`
is `Infinity`, `EXTRACT(day FROM ...)` of it is `NULL`), and a row whose date
leaves the range (`date + n` past 5874897-12-31, two dates one of which is
infinite, a date past 294276-12-31 cast to `timestamp`) raises `22008` for that
row's record, which is quarantined; a constant that does fails every row.
A bucket for an aggregate is computed in a 1-1 transform and grouped by in
another (`DATE_TRUNC('month', ts) AS bucket`, then `GROUP BY bucket`), since a
`GROUP BY` key is a column; `EXTRACT` is an unconstrained `numeric`, which an
aggregate counts, sums or averages. `AT` is a keyword only after an operand and
before `TIME ZONE`, and `EXTRACT` only before `(`, so columns of those names
(and `year`, `time`, `zone`) are still columns. Types and refusals are in
[Supported sources and targets](#supported-sources-and-targets) and [type
support](type-support.md#dates-and-times).

Formulas may only use **immutable** functions and operators — those whose output
depends solely on their inputs. Anything depending on database state outside the
row (current time, timezone, collation, session settings, random values) is
disallowed: incremental maintenance requires that re-evaluating a formula on
unchanged inputs always yields the same result.

### Chaining and cycle detection

Calculated columns may reference each other, letting users build derivations out
of small named steps (a `margin` column defined in terms of `revenue` and `cost`).
Transforms also chain: a target table may itself be a source for another —
aggregated or joined. Trellis builds a
dependency graph spanning columns within a table and tables across chained
transforms, and computes in dependency order.

A transform can only chain off a target once that target's own transform is
`live`. Defining it while the upstream is still building (for example, a plain
1-1 target whose chunked backfill hasn't finished) is refused with
`TransformNotLive`. Each write to a target reaches the transforms reading it inside the
same transaction as the write; Trellis never installs capture triggers on a
target. That in-transaction hand-off
is why an aggregate target, which has no primary key, can be chained off at all.
It does not cross into another instance.

**Cycles are rejected at definition time across the whole graph.** A definition
that would introduce a cycle, directly or transitively, is invalid and rejected
before it runs, keeping the computation order well-defined.

## Target tables are Trellis-owned

A target is materialized as a plain table in the target schema, and reading it
is the whole point: query it, join it, index it, grant it to whoever needs it.
Its contents and shape, though, belong to Trellis
([0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md)).
Trellis is its only writer and doesn't guard it
([ADR-0027](decisions/0027-operating-principles.md), rule 5): whether a change
made behind its back reaches anything chained off the table is undefined
([known correctness gaps — Rows written into a target, a hidden table or Trellis's schema](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)).
Treat a target as read-only, and specifically:

* **Do not insert, update or delete rows.** An edited row stays wrong until the
  next event that recomputes it, and a row added or re-keyed by hand stays
  until a rebuild. `self_check`
  ([0013](decisions/0013-self-check-production-recompute-audit.md)) reports the
  divergence; it does not repair it. `PAUSE` then `RESUME` rebuilds the table
  from source and deletes the rows the source doesn't back
  ([operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)).
* **Do not add, drop, rename or retype columns.** Trellis addresses its columns
  by name and type. Redefining the transform is how a column changes
  ([Changing a definition](#changing-a-definition)).
* **Do not change the key constraints.** A 1-1 target's key (its primary key,
  or its unique constraint over a nullable-keyed source) and an aggregate
  target's unique grouping constraint are how Trellis addresses a row when it
  upserts or deletes it.
* **Do not `TRUNCATE` or `DROP` the table yourself.** `DROP TRANSFORM` removes
  the table and its definition together, and refuses while another definition
  still chains off it. A hand `TRUNCATE` leaves the table empty until the
  transform is paused and resumed. A resume refuses while the table is
  missing, and the repair is `DROP TRANSFORM` and defining it again.
* **Don't let row-level security apply to the role that writes it.**
  Trellis writes a target as the role each worker connects as, with
  `row_security = off`, so a write that policies applying to that role would
  filter is refused rather than skipping rows.
  Trellis creates the target as the role that defines it, so that role owns
  it and is exempt unless the table is `FORCE ROW LEVEL SECURITY`. Enabling
  row-level security on a target for your application's readers is fine as
  long as the workers run as the table's owner (or a member of it) or have
  `BYPASSRLS`. Those readers see their policies on their own connections;
  Trellis keeps its connections, which run with `row_security = off`, to
  itself. See
  [Supported sources and targets](#supported-sources-and-targets) for what
  happens when that stops holding. A definition is also refused if
  DDL around its target's creation, such as an event trigger that forces
  row-level security on every new table, makes the policies apply to the
  defining role.
* **Expect a partial table while `backfilling`, and after `RESUME`.** The
  [status](#status) says when the table is complete; a reader that needs
  completeness checks it.

Indexes, and grants to other roles, are yours to add, to a target define
created or one it adopted (`ADOPT EXISTING TABLE`). They live and die with the
table, so `DROP TRANSFORM` takes them with it. So are unique indexes,
constraints, triggers, rules and foreign keys, with these outcomes:

* A unique index, constraint or trigger that refuses a row Trellis writes,
  whether it checks the statement or is deferred to the commit, fails that
  row's key like any value its target refuses: the key ends up held in
  quarantine, enough held keys trip the fuse and the definition reads
  `quarantined`, and rows that fail only together hold their page back as a
  `drain_failure` (see [Status](#status)). An aggregate's or a `JOIN`'s
  build writes its rows together, so a refusal there pauses the build with
  a `backfill_failure` naming it instead. A unique index on a column other
  than the key is the likely case: changes to different keys apply in any
  order, so two rows can hold one value for a moment even when the source
  never does, and the index refuses one of them.
* Another table's foreign key referencing the target fails Trellis's deletes
  of a referenced row: the key whose row is still referenced is held. A
  source `TRUNCATE` deletes every row of the target, and while one of them
  is referenced that page is held back as a `drain_failure` until the
  referencing rows are gone. Two statements fail with the database's error
  while the foreign key exists, and leave the transform as it was: `DROP
  TRANSFORM`, and the `RESUME` that empties a 1-1 target keyed by an enum
  column with `TRUNCATE` after one of the enum's values is relabelled. Drop
  the foreign key first.
* Postgres refuses `INSERT … ON CONFLICT`, which Trellis writes rows with,
  on a table with an `INSERT` or `UPDATE` rule, and refuses the deletes
  Trellis runs (`DELETE … RETURNING`, some inside a `WITH`) on a table with
  most `DELETE` rules, `DO INSTEAD NOTHING` among them. Such a rule fails
  those writes: a build pauses with a `backfill_failure`, and each key a
  change reaches is held.
* A trigger or rule that changes or drops the rows Trellis writes leaves the
  target wrong without a refusal: the
  [known correctness gap](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)
  of rows written into a target.

Another Trellis instance may read
a 1-1 target keyed by a primary key as a source, exactly as it would any table
with a primary key. An aggregate target, and a 1-1 target over one, has no
primary key, so it is read that way only with a `KEY (…)` naming its group key
(see [Supported sources and targets](#supported-sources-and-targets)).

## Status

Every defined transform carries an observable **status**, one of six words:

* **`waiting_to_backfill`**: defined, but its source's existing rows not yet read.
* **`backfilling`**: its target is being built, or rebuilt.
* **`catching_up`**: maintained, but reading an upstream (or an upstream column) that isn't `live`.
* **`live`**: the steady state, where a watermark token guarantees read-your-writes.
* **`quarantined`**: frozen because too many of its keys failed to apply.
* **`paused`**: frozen by `PAUSE TRANSFORM`, or by Trellis because it can't keep the target correct.

The [status reference](status-reference.md) defines each word and every field
of the status. [Operations](operations.md#when-a-definition-needs-you) lists
every reason a transform stops and what to do, and
[embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)
has the polling pattern.

## Changing a definition

Every definition-changing operation is one statement of the same grammar a
definition itself is written in, run through one entrypoint —
`Trellis::apply(text)`, or `trellis apply '<statement>'` from a shell:

```text
TRANSFORM <target> [ADOPT EXISTING TABLE] FROM <source> [KEY (<columns>)] [GROUP BY <keys>] SELECT <fields> [WHERE <predicate>]

PAUSE  TRANSFORM <target>[.<column>]
RESUME TRANSFORM <target>[.<column>]
DROP   TRANSFORM <target>
```

**Adopting an existing target.** `ADOPT EXISTING TABLE` after the target
builds the transform into a table of the target's name that already exists,
instead of creating it: for example a table restored from a schema dump. The
table must be empty and shaped exactly as define would create it, and anything
else is refused (the target row of
[Supported sources and targets](#supported-sources-and-targets)). Every key
space takes it. Once defined, the target is Trellis's like any other: `DROP
TRANSFORM` drops it, and the rest of
[Target tables are Trellis-owned](#target-tables-are-trellis-owned) applies.
A definition that writes it carries the release that added it as its
[floor](#floors).

**Naming a source's key.** `KEY (<column>, …)` directly after a `FROM` or
`JOIN` table, before its alias (`FROM orders KEY (customer_id, id) AS o`),
names the columns Trellis keys that table by, in the order written, instead of
its primary key. A table with no primary key needs one. Every key space takes
it, on either table of a `JOIN`. The key must be backed by a unique index over
exactly those columns, and every transform reading the table names the same
key (the [key row](#the-key-a-transform-keys-each-table-by) of Supported sources and targets).
The rendered definition, and so a schema dump, carries it. A definition that
writes it carries the release that added it as its [floor](#floors).

**Addressing.** A transform is named by its target table, bare (`widget_prices`)
or schema-qualified as `define` returns it (`public.widget_prices`); `ALTER
TRANSFORM` and `DROP TRANSFORM` take either form, and so do the `Trellis` methods
that name a transform (`status`, `quarantine_status`, `sample_quarantined`,
`release_key`, `self_check`). A qualified name must match exactly, so a wrong
schema names nothing. `PAUSE` and `RESUME` also take `<transform>.<column>`,
pausing or resuming a single calculated field rather than the whole definition.
The column form always takes the bare transform name, and it wins when an
address reads both ways: `a.b` is column `b` of transform `a` when `a` declares
a field `b`, and otherwise the transform registered as `a.b` (name that
transform bare when it collides). `DROP TRANSFORM a.b` refuses an address
whose `a` is a transform and that names no transform itself, since dropping a
field is `ALTER TRANSFORM a DROP b`.

**Semantics** are covered by
[0014-pause-and-drop-a-transform](decisions/0014-pause-and-drop-a-transform.md).
In short: `PAUSE` and `DROP` are idempotent; `RESUME` re-validates the
transform against the live schema, refusing while define would or while the
target's own tables aren't what define would create, and rebuilds
by a fresh backfill rather than catching up on changes that happened during
the pause. A `RESUME` of a transform whose upstream (a transform whose target
it reads) isn't `live` is accepted and waits for it, and is re-validated once
it is, so a whole stack resumed at once, in any order, comes up in dependency
order; a `PAUSE` of a waiting
transform withdraws its resume;
`DROP` removes the target table's data along with the definition, and is refused
— naming the blockers — while another registered definition still chains off the
subject, so a chain is retired from the leaves inward.

`DROP TRANSFORM <target>.<column>` is not a statement: removing one
calculated field is an `ALTER TRANSFORM` operation, not a drop.

An `ALTER TRANSFORM` validates the definition it would leave against the source's
live columns, the schema as of its own commit and not as of define, with define's
rules: it refuses what define would refuse, naming the column and what to change.
A column the host added since define is readable by an added or changed field,
and a field named after it is refused. A column the host dropped, or retyped so
that a field reading it no longer validates, refuses an edit that leaves that
field, so drop or change the field in the same statement. An added or changed
field can't read a column whose type the host changed since the transform was
defined or last resumed, even one it would validate over: `PAUSE` and `RESUME` the
transform first, which re-types it over the source as it is now, then edit it.

An `ALTER` of a field compares the column's current type with the type `DEFINE` would
give the altered field: a bare passthrough takes its source column's type with no
modifier (so `varchar` and `text`, or `numeric(10,2)` and `numeric`, are one type and need
no change), any other expression its value family's. An `ALTER` never re-types a column.
One that would (a change of integer or float width, or of type family) would rewrite the
target under `ACCESS EXCLUSIVE`, so it is refused, naming the remedy: `DROP` the field,
then `ADD` it again.

**`apply` only registers.** Every statement returns once the change is
recorded; none reads the transform's source rows. A new transform is built in the
background from `waiting_to_backfill`. An `ALTER TRANSFORM` that adds or
changes fields, and a `RESUME TRANSFORM <target>.<column>`, start a
*field build*: the transform goes `backfilling`, the changed fields are
computed for every new change from that moment on, and background chunks
rewrite them across the existing rows, reading the source once however many
fields changed. The transform reads `live` again once they are all done, so
poll its status before relying on the new fields
([embedding — Poll to `live`, don't wait](embedding.md#poll-to-live-dont-wait)).
Until then a row's new field can still be empty (an added field) or hold the
old formula's value (a changed or resumed one). A field whose formula reads a
source column the source's capture doesn't record yet waits, paused, until
capture records it before its build starts.

## Grammar versions

Every stored definition records the grammar version it was written in. A
breaking release that changes what accepted text means bumps that version and
keeps reading the older one under its old rules, so an upgrade never
re-interprets a stored definition. Each major version reads every grammar
version the previous two majors wrote. A transform written under an older
version than that is paused until you drop it and define it again, and a version
newer than the binary knows, from a downgrade, is refused. A release that
refuses a form Postgres rejects or reads differently keeps the version: a
transform whose stored text uses that form is paused when the upgraded binary
attaches, its `capture_failure` naming the refusal, until you drop it and
define it again; the other transforms keep running. A stored definition
can be rewritten to the current version without a rebuild. Text is stamped
with the oldest version that reads it the same, so a definition with no
quoted name, no upper-case letter in a name and no `TRUE`, `FALSE` or `NULL`
literal is grammar 1 whichever release wrote it. Grammar 2 reads
[names](#names) as Postgres does; a grammar-1 definition, written by 0.1.x,
keeps its names' case, as if each were quoted, and `ALTER TRANSFORM` stores it
quoted (`SUM("Amount")`). Grammar 1 had no `TRUE`, `FALSE` or `NULL` literal:
a bare one, in any case, named a column or calculated field, and a grammar-1
definition keeps reading that name (`COALESCE(null, a)` reads the column
`"null"`). The rules are in
[0018-grammar-versioning](decisions/0018-grammar-versioning.md).

## Floors

Every stored definition also records its **floor**: the oldest Trellis
release that can run it, the latest release any grammar construct, key space,
option or hidden-table layout it uses arrived in, or any argument value
(`EXTRACT` field, `DATE_TRUNC` unit, `AT TIME ZONE` zone), type rule (which
operand types an operator, cast, function or aggregate takes) or key type (of
a `GROUP BY` or `JOIN ... ON` key) define's validation admitted it by over its
columns' types, or the type of a primary key or partition key it reads that
define admitted. Define, `ALTER TRANSFORM` and a resume's rebuild record it
from what the definition uses then, so an `ALTER` that removes the only newer
construct lowers it; a pause never changes it. When a column it reads changes type, the staging worker's next capture
pass validates it again over the new type: if it still validates, the floor
is recorded again, and can go up or down; if it doesn't, the definition
pauses (the rows above). `status()` reports it as `floor`, and `trellis status` prints it as
`floor=` on each definition's line. It is what to read before rolling an
application back to an earlier Trellis release
([upgrading — Rolling back](upgrading.md#rolling-back),
[0024-compatibility-floors-and-rollback](decisions/0024-compatibility-floors-and-rollback.md)).

## Scope

This document describes the logical model only, not the physical storage or
timing of derived data; see [data-flow](data-flow.md) for how changes propagate
and [0002-async-data-flow](decisions/0002-async-data-flow.md) for why that flow
is asynchronous.
