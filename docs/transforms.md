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
refusals are the supported-subset contract, not bugs. Everything in the table
is checked for a transform's source, for each table a `JOIN` reads, and (where
noted) for its target. Trellis never adds
a key, index or policy itself.

| Condition | Refused at define | After define |
|---|---|---|
| **Source tables and partitioning** | | |
| [A partition, a partitioned root below another, or an inheritance table](#a-partition-a-partitioned-root-below-another-or-an-inheritance-table) | Yes | Pauses readers; `RESUME` refuses until undone |
| [A source dropped, renamed, swapped, or its capture disrupted](#a-source-dropped-renamed-swapped-or-its-capture-disrupted) | n/a | Pauses readers (`disruption`) |
| [A partitioned root whose partition key the group key omits](#a-partitioned-root-whose-partition-key-the-group-key-omits) | Yes | Pauses readers |
| [A partitioned root with an unsupported partition key](#a-partitioned-root-with-an-unsupported-partition-key) | Yes | Pauses readers |
| [A partitioned root with more than 1,000 leaves](#a-partitioned-root-with-more-than-1000-leaves) | Yes | Pauses readers |
| [A partitioned root as a 1-1 transform's source](#a-partitioned-root-as-a-1-1-transforms-source) | Yes | Pauses readers; `RESUME` refuses |
| [A partitioned root of a servable shape](#a-partitioned-root-of-a-servable-shape) | No | A restore pauses every definition |
| **Source keys** | | |
| [An aggregate target used as another instance's source](#an-aggregate-target-used-as-another-instances-source) | Yes | n/a |
| [The key a transform keys each table by](#the-key-a-transform-keys-each-table-by) | Yes (`NoPrimaryKey`, `DeferrablePrimaryKey`, `KeyNotBacked`, …) | Pauses or halts readers; `RESUME` re-keys or refuses |
| **Row-level security and replication** | | |
| [Row-level security on a role Trellis runs as](#row-level-security-on-a-role-trellis-runs-as) | Yes (`ExtremeSourceNotReadable`) | Pauses readers |
| [A table a logical-replication subscription replicates into](#a-table-a-logical-replication-subscription-replicates-into) | Yes | Pauses readers |
| **Generated columns, collations and patterns** | | |
| [A virtual generated column a definition reads](#a-virtual-generated-column-a-definition-reads) | Yes (`VirtualGeneratedColumn`) | Pauses readers |
| [A nondeterministic collation on a key or compared column](#a-nondeterministic-collation-on-a-key-or-compared-column) | Yes | Pauses readers, or holds keys |
| [A `REGEXP_COUNT` pattern that isn't a literal or that Postgres rejects](#a-regexp_count-pattern-that-isnt-a-literal-or-that-postgres-rejects) | Yes (`NonLiteralRegexPattern`, `InvalidRegexPattern`) | Rows quarantined after a Postgres upgrade |
| **Constants** | | |
| [A literal zero divisor](#a-literal-zero-divisor) | Yes (`ConstantDivisorZero`) | n/a |
| [A typed literal not in its type's canonical spelling](#a-typed-literal-not-in-its-types-canonical-spelling) | Yes (`InvalidTypedLiteral`) | n/a |
| **Comparisons** | | |
| [A float compared with an exact numeric](#a-float-compared-with-an-exact-numeric) | Yes (`MixedFloatExactComparison`) | A retype pauses the transform |
| [A comparison between two temporal types](#a-comparison-between-two-temporal-types) | Yes (`MixedTemporalComparison`) | A retype pauses the transform |
| [An ordering comparison over `text`](#an-ordering-comparison-over-text) | Yes (`TextOrderingComparison`) | A retype pauses the transform |
| [A comparison between types no family covers](#a-comparison-between-types-no-family-covers) | Yes (`ComparisonTypeMismatch`) | A retype pauses the transform |
| [Chained comparisons](#chained-comparisons) | Yes (`NonAssociativeComparison`) | n/a |
| **Grammar forms and typing** | | |
| [`IS TRUE` forms and the simple `CASE`](#is-true-forms-and-the-simple-case) | Yes (`UnsupportedIsForm`, `SimpleCaseUnsupported`) | n/a |
| [An untyped `NULL` literal](#an-untyped-null-literal) | Yes (`UntypedNull`) | n/a |
| [A non-boolean `CASE` condition or `CASE` results that don't unify](#a-non-boolean-case-condition-or-case-results-that-dont-unify) | Yes (`CaseConditionNotBoolean`, `CaseResultTypeMismatch`) | A retype pauses the transform |
| [A `NULLIF` that casts its first argument](#a-nullif-that-casts-its-first-argument) | Yes (`NullIfFirstArgumentCast`) | A retype pauses the transform |
| **Expression size** | | |
| [An expression nested more than 100 levels deep](#an-expression-nested-more-than-100-levels-deep) | Yes (`ExpressionTooDeep`) | n/a |
| [Calculated fields that inline too large or too deep](#calculated-fields-that-inline-too-large-or-too-deep) | Yes (`FieldExpansionTooLarge`) | n/a |
| [An expression that inlines to more than 100,000 nodes](#an-expression-that-inlines-to-more-than-100000-nodes) | Yes (`FieldExpansionTooLarge`) | n/a |
| **Arithmetic, functions and casts** | | |
| [Integer division](#integer-division) | Yes (`IntegerDivision`) | A retype pauses the transform |
| [Modulo with a float operand](#modulo-with-a-float-operand) | Yes (`FloatModulo`) | A retype pauses the transform |
| [An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` does not admit](#an-argument-abs-ceil-floor-round-or-trunc-does-not-admit) | Yes (`NumericFunctionArgument`, `ScaleNotIntegerLiteral`, `FunctionArgTypeMismatch`) | A retype pauses the transform |
| [A `CAST` that is not an exact conversion](#a-cast-that-is-not-an-exact-conversion) | Yes (`UnsupportedCast`, `TemporalOperand`, `UnsupportedCastTarget`) | A retype pauses the transform |
| **Dates and times** | | |
| [Date and time arithmetic](#date-and-time-arithmetic) | Yes (`TemporalOperand`) | A retype pauses the transform |
| [`DATE_TRUNC` or `EXTRACT` of an unadmitted type, and `date_part`](#date_trunc-or-extract-of-an-unadmitted-type-and-date_part) | Yes (`TemporalOperand`) | A retype pauses the transform |
| [A `DATE_TRUNC` unit or `EXTRACT` field that is not admitted](#a-date_trunc-unit-or-extract-field-that-is-not-admitted) | Yes (`TemporalLiteral`) | Unaffected |
| [`AT TIME ZONE` other than `'UTC'` over a `timestamptz`](#at-time-zone-other-than-utc-over-a-timestamptz) | Yes (`TemporalLiteral`, `TemporalOperand`) | A retype pauses the transform |
| **Retyped and widened columns** | | |
| [A key column retyped so stored values render differently](#a-key-column-retyped-so-stored-values-render-differently) | n/a | Pauses readers; `RESUME` re-types |
| [A column Trellis copies, widened](#a-column-trellis-copies-widened) | n/a | Pauses readers whose copy it outgrows |
| **Target columns and widths** | | |
| [Two `GROUP BY` keys that share a target column name](#two-group-by-keys-that-share-a-target-column-name) | Yes (`GroupByKeysShareTargetColumn`) | n/a |
| [A key of more than 32 columns](#a-key-of-more-than-32-columns) | Yes (`IndexKeyTooWide`) | n/a |
| [A table of more than 1,600 columns](#a-table-of-more-than-1600-columns) | Yes (`TableTooWide`) | n/a |
| **Field, column and table names** | | |
| [A field named after a key column or starting with `__`](#a-field-named-after-a-key-column-or-starting-with-__) | Yes | n/a |
| [An aggregate over another aggregate field](#an-aggregate-over-another-aggregate-field) | Yes (`AggregateOfAggregateField`) | n/a |
| [A bare name that could mean a source column or a field](#a-bare-name-that-could-mean-a-source-column-or-a-field) | Yes (`AmbiguousColumnReference`) | Not detected |
| [An unquoted reserved word as a name](#an-unquoted-reserved-word-as-a-name) | Yes (`ReservedWord`) | A rename pauses readers |
| [An unquoted reserved table name](#an-unquoted-reserved-table-name) | Yes (`ReservedTableName`) | n/a |
| [A name over 63 bytes](#a-name-over-63-bytes) | Yes (`NameTooLong`) | A stored 0.1.x definition keeps running |
| [An unsupported quoted name](#an-unsupported-quoted-name) | Yes (`ZeroLengthQuotedIdentifier`, `UnicodeEscapeIdentifier`, `UnsupportedQuotedIdentifier`) | n/a |
| [A qualified name that doesn't name the `FROM` table](#a-qualified-name-that-doesnt-name-the-from-table) | Yes (`QualifiedColumnNotFromSource`, `GroupByKeyNotFromSource`) | n/a |
| **Source indexes** | | |
| [A source without the index its `self_check` walk pages by](#a-source-without-the-index-its-self_check-walk-pages-by) | Yes (`SourceIndexMissing`) | Reported; pauses nothing |
| **`JOIN` transforms** | | |
| [A `JOIN` transform that passes the checks below](#a-join-transform-that-passes-the-checks-below) | No | A schema change pauses it |
| [An unsupported `JOIN` shape](#an-unsupported-join-shape) | Yes (`TableAlias`, `AliasedTableName`, `JoinTooManyTables`, …) | n/a |
| [A 1-1 `JOIN`](#a-1-1-join) | Yes (`OneToOneJoinKeyNotUnique`, `OneToOneJoinAggregate`, `OneToOneJoinUnsupported`, …) | A schema change pauses it |
| [1-1 `JOIN` of more than two tables (star)](#1-1-join-of-more-than-two-tables-star) | Yes (`StarJoinTooManyTables`, `JoinOnReadsAnotherJoinedTable`, the [row above](#a-1-1-join)'s at each position) | A schema change at any position pauses it |
| [`JOIN` key columns that differ or can't be keys](#join-key-columns-that-differ-or-cant-be-keys) | Yes (`JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`, `JoinNondeterministicCollation`, …) | Pauses the transform |
| **Aggregate functions** | | |
| [`MIN` or `MAX` in a plain aggregate](#min-or-max-in-a-plain-aggregate) | Yes (`ExtremeOfExpression`, `UnsupportedExtremeArgument`, `ChainedExtreme`, …) | `RESUME` refuses; a missing index pauses after 5 minutes |
| [`MIN` or `MAX` in a `JOIN` transform](#min-or-max-in-a-join-transform) | Yes (`DeferredAggregate`) | n/a |
| [`bool_and`, `bool_or`, `bit_and` or `bit_or`](#bool_and-bool_or-bit_and-or-bit_or) | Yes (`DeferredAggregate`) | n/a |
| [`SUM` or `AVG` over an unsupported type](#sum-or-avg-over-an-unsupported-type) | Yes (`DeferredAggregate`) | A retype pauses the transform |
| [`array_agg`, `string_agg` or `jsonb_agg`](#array_agg-string_agg-or-jsonb_agg) | Yes (`DeferredAggregate`) | n/a |
| **Filters and chaining** | | |
| [A `WHERE` other than `TRUE`](#a-where-other-than-true) | Yes (parse error) | n/a |
| [Chaining off a transform that is not `live`](#chaining-off-a-transform-that-is-not-live) | Yes (`TransformNotLive`) | n/a |
| **Target names** | | |
| [A target whose name is taken](#a-target-whose-name-is-taken) | Yes (`TargetTableExists`, `TargetNotAdoptable`) | n/a |
| [A target that would take a read table's name](#a-target-that-would-take-a-read-tables-name) | Yes (`TargetNameHasReaders`) | n/a |
| [A target in a Trellis catalog schema](#a-target-in-a-trellis-catalog-schema) | Yes (`TargetInCatalogSchema`) | Pauses nothing |

A refusal after define is a pause, not a loss: `RESUME` rebuilds the target from
source once the condition is fixed ([Status](#status)). `status()`'s
`capture_failure` carries the reason. Every `RESUME`, of a transform or of one
field, first runs define's validation against the live schema, and refuses,
leaving the transform paused, while define would refuse it; the error names the
column and what to change. A resume also refuses while the target's own tables
are no longer what define would create (a 1-1 target whose source key was
redefined, or whose ledger table is missing; [operations — `PAUSE` and `RESUME`](operations.md#pause-and-resume)), and
the error says to drop the transform and define it again; a field resume makes
only the source-key check. When a resume has copies to re-type it returns
at once, the transform still `paused` with a `capture_failure` that says so,
and the staging worker re-types them and starts the rebuild. The re-type honours
the `statement_timeout` of Trellis's role or database: one cancelled three times ends
the request, the transform still `paused`, and the `capture_failure` names the two
remedies, raising the timeout, restarting Trellis and resuming again, or `DROP
TRANSFORM` and defining again ([recommendations](recommendations.md#statement_timeout)). Other ways a
source can drift after define are in
[known correctness gaps](known-correctness-gaps.md).

Other writers are not affected by the [subscription
rule](#a-table-a-logical-replication-subscription-replicates-into): a session with
`session_replication_role = replica` fires the capture triggers, which are
`ENABLE ALWAYS` ([stage 1](staging-and-claiming/01-capture-by-triggers.md)).
Changing a source key's collation to another deterministic one is safe, though
the rest of any build then scans the source and `self_check` reads the source
by scan rather than by index (it pages under the target key's collation).

### Source tables and partitioning

#### A partition, a partitioned root below another, or an inheritance table

**Condition:** A partition (a leaf or a middle) named as a source, a
partitioned root that is itself a partition, or a table in an inheritance
hierarchy. A partitioned root is checked by the rows below. A source with no
primary key is the [key row](#the-key-a-transform-keys-each-table-by) below.

**Refused at define:** Yes. A statement trigger fires only for the table a
statement names, so capture would miss writes made through the rest of the
hierarchy.

**If it appears after define:** A table that joins a hierarchy after define
(`ATTACH PARTITION`, `INHERIT`, a child created `INHERITS` it, or the table
dropped and recreated as a partition or an inheritance child) pauses every
transform that reads it on the staging worker's next reconcile pass, with the
reason in `status()`'s `capture_failure`, and gets no capture installed.
`self_check` reports a `capture` divergence.

Writes made through the hierarchy before that pass are missed, or, on an
inheritance parent, a child's rows are staged as the parent's own; the resume's
rebuild re-reads the table and repairs both. A dropped key leaves the table
uncapturable: `status()` shows a capture holdup, and the transforms that read
it pause with the reason when the table's next change is applied. A retyped key
pauses them too (see the [key rows](#the-key-a-transform-keys-each-table-by)
below).

**Remedy:** Use a plain table. After a later change, undo it
(`DETACH PARTITION`, `NO INHERIT`, or recreate the table as a plain table),
then `RESUME`, which refuses while the table is still in a hierarchy.

#### A source dropped, renamed, swapped, or its capture disrupted

**Condition:** A source table or a `JOIN`'s joined table that is dropped,
renamed away, moved to another schema, swapped for another table or replaced by
a view after define, or whose capture triggers or functions are disabled,
dropped, recreated or replaced outside Trellis.

**Refused at define:** n/a (a definition can't name a table that isn't there).

**If it appears after define:** Pauses every transform that reads the table on
the staging worker's next reconcile pass, with a `capture_failure` of kind
`disruption` naming what it found and saying that changes made meanwhile may be
missing, so correctness can't be guaranteed. Nothing is reinstalled or rebuilt
until a `RESUME`.

A partitioned source's root and every node of its tree are checked, and the
finding names the node; a root dropped and recreated (empty or with leaves),
swapped by renames or renamed away is a disruption too, while partition
maintenance (an attach, detach, new leaf, dropped leaf, `DEFAULT` or `HASH`
re-split) is none. A plain table dropped and created again as a partitioned
one, or the reverse, is a disruption as well.

**Remedy:** Put the table back (or finish the swap) and `RESUME`, which refuses
while the table is missing, re-validates the definition against the table now
at that name, reinstalls capture and rebuilds from that table. A table created
again with another primary key (a plain table converted to a partitioned one
widens it by the partition key) is rebuilt the same way: what Trellis stored or
staged under the old key is discarded, not read against the new one.

A 1-1 transform's target is keyed by its source's key, so its resume first
re-keys the target in place to the new key, or refuses, as the [key
row](#the-key-a-transform-keys-each-table-by) below describes. Plan a scripted
`RESUME` after a swap or a bulk load with triggers disabled (the Ruby and
Elixir clients have a test helper for fixture loads:
[embedding](embedding.md)).

#### A partitioned root whose partition key the group key omits

**Condition:** A partitioned root
([0017](decisions/0017-partitioned-sources.md); `RANGE`, `LIST` or `HASH`, any
depth, with or without a `DEFAULT`) as an aggregate's `FROM` whose `GROUP BY`
omits a partition-key column at any level, or as either table of a `JOIN` whose
`GROUP BY` columns and `ON` columns from that table omit one.

**Refused at define:** Yes, naming the column. With every partition-key column
in the group key, each group lives in exactly one partition, so a change to the
tree touches only the groups routing to one node.

**If it appears after define:** A sub-level added later whose partition key the
group key (a `JOIN` table's `GROUP BY` and `ON` columns) doesn't hold pauses
every transform that reads the source on the staging worker's next reconcile
pass, with the column in `status()`'s `capture_failure`. `RESUME` refuses until
the tree complies, then rebuilds.

**Remedy:** Add the named column to the `GROUP BY`, or join on it. After a
later change, detach the sub-level, then `RESUME`.

#### A partitioned root with an unsupported partition key

**Condition:** A partitioned root whose key level is an expression (no column
to route by), whose partition-key column has a type other than `smallint`,
`integer`, `bigint`, `numeric`, `text`, `varchar`, `uuid`, `date`, `timestamp`,
`timestamptz` or `boolean`, a non-default operator class or a nondeterministic
collation.

**Refused at define:** Yes, naming the table and the column (for an expression,
its key position). Trellis routes a group to its partition by evaluating the
recorded bound over the group key, and it checks that only for these keys.

**If it appears after define:** A sub-level added later with such a key pauses
every transform that reads the source, as the row above describes.

**Remedy:** Partition by a plain column of a listed type with the type's
default operator class and a deterministic collation. After a later change,
detach the sub-level, then `RESUME`.

#### A partitioned root with more than 1,000 leaves

**Condition:** A partitioned root with more than 1,000 leaves, or on PostgreSQL
before 15.

**Refused at define:** Yes, stating the limit. No build statement prunes, so
each one plans, locks and opens every leaf, and the default lock table runs out
for one statement at about 5,500 leaves with the default
`max_locks_per_transaction`.

**If it appears after define:** A tree that grows past 1,000 leaves pauses
every transform that reads it on the staging worker's next reconcile pass, with
a `capture_failure` naming the limit. `RESUME` refuses until the tree has 1,000
or fewer.

**Remedy:** Merge partitions to 1,000 or fewer leaves, or upgrade the server.

#### A partitioned root as a 1-1 transform's source

**Condition:** A partitioned root as a 1-1 transform's `FROM`.

**Refused at define:** Yes. A 1-1 target over a partitioned source would need
its own partitioning, and partitioned targets are not supported.

**If it appears after define:** A 1-1 transform's source dropped and created
again as a partitioned table pauses the readers the same way, and its `RESUME`
refuses as define does.

**Remedy:** Aggregate over the partitioned table instead, or use a plain table.

#### A partitioned root of a servable shape

**Condition:** A partitioned root of a servable shape (every row above passes).

**Refused at define:** No. Capture covers every node of the tree. The reconcile
pass queues a correction for each partition attached, detached, dropped or
truncated, and for groups in a target that route to no partition, and an
aggregate recomputes the groups that route to the partition while it reports
`backfilling`. A `JOIN` with the partitioned table on either side recomputes
that side's partials that route to the partition, and carries the change into
its target, while it reports `backfilling`.

**If it appears after define:** After a dump and restore of the database, the
next reconcile pass pauses every definition, those that read a partitioned
source among them, with a `capture_failure` naming the restore, and runs no
correction queued before it. A `pg_upgrade`, or the root dropped and recreated
or swapped in by renames, is no restore.

**Remedy:** After a restore, `RESUME`, which rebuilds.

### Source keys

#### An aggregate target used as another instance's source

**Condition:** An aggregate target, or a 1-1 target over one, used as a source
(it has no primary key: its identity is a `UNIQUE NULLS NOT DISTINCT`
constraint).

**Refused at define:** Yes, for another Trellis instance, unless the definition
names that constraint's columns with `KEY (…)` (the [key
row](#the-key-a-transform-keys-each-table-by) below): to that instance it is a
table with no primary key. Inside the owning instance it can be chained off,
because writes to a target reach its readers in the writing transaction, never
through capture triggers, and it is keyed by its group key.

**If it appears after define:** n/a.

**Remedy:** Chain within the instance
([instance-identity](instance-identity.md)), group over a 1-1 transform, or
name the group key with `KEY (…)`.

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

### Row-level security and replication

#### Row-level security on a role Trellis runs as

**Condition:** Row-level security that applies to a role Trellis runs as on a
source or on a target. Policies apply to a role when RLS is enabled and the
role neither owns the table (directly or through membership) nor has
`BYPASSRLS`, or owns it and the table is `FORCE ROW LEVEL SECURITY`. A
superuser is exempt. Every Trellis connection runs with `row_security = off`,
so a read or write the policies would filter fails rather than skipping rows.

**Refused at define:** Yes, checked for the role that owns the ring, and for
the defining role on a table that is another transform's target. A `MIN`/`MAX`
aggregate's source is also checked for the ring's owner's `SELECT`
(`ExtremeSourceNotReadable`), at define and on a whole-transform `RESUME`: a
drain, which logs in as that role or a member of it, re-reads a group's extreme
from the source when the current one is deleted.

**If it appears after define:** For the ring's owner, the staging worker's
reconcile pass pauses every transform that reads the table (or, for a target,
the transform that writes it), with the reason in `status()`'s
`capture_failure`, and `self_check` reports a `capture` divergence. For a role
a drain logs in as, its refused read or write pauses the same transforms, and
everything downstream of them, with `capture_failure` of kind `halt`. A refused
build chunk pauses its transform after its charged retries, with the error on
`backfill_failure`.

On a partitioned source, define and the pass check the root only: a policy on a
partition below it reaches none of the reads that follow writes, which all go
through the root, but the correction of that partition after a `TRUNCATE` of
it, an attach of it with rows, or direct writes into it as a new leaf reads the
partition itself, is refused, and pauses the transform after its charged
retries the same way. The refusals that only log or retry: [observability — A
halting failure](observability.md#a-halting-failure).

**Remedy:** Give the role `BYPASSRLS` (not inherited), or make it the table's
owner without `FORCE ROW LEVEL SECURITY`; then `RESUME`. For a `MIN`/`MAX`
source, `GRANT SELECT` on it to the ring's owner.

#### A table a logical-replication subscription replicates into

**Condition:** A table a logical-replication subscription in the same database
replicates into.

**Refused at define:** Yes, in any sync state, including a disabled
subscription. The subscription's apply worker fires only row-level triggers, so
its changes never reach capture. Publishing a table to another database is
fine.

**If it appears after define:** Paused and reported as for RLS on the ring's
owner.

**Remedy:** Remove the table from the publication and
`ALTER SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then
`RESUME`.

### Generated columns, collations and patterns

#### A virtual generated column a definition reads

**Condition:** A column a definition reads that is a virtual generated column
(`GENERATED ALWAYS AS (…) VIRTUAL`, PostgreSQL 18): a 1-1 field, a `GROUP BY`
key, an aggregate's argument, or a `JOIN`'s `ON` column or field on either
table.

**Refused at define:** Yes (`VirtualGeneratedColumn`), for the definition, an
`ALTER TRANSFORM` that adds such a field, and a `RESUME`, naming the table, the
column and its expression. A statement trigger's transition table holds no
value for a virtual column, so capture can't image it. A definition on the same
table that doesn't read the column is unaffected.

**If it appears after define:** A read column dropped and added again as
virtual, or another column renamed onto its name, pauses its readers on the
staging worker's next reconcile pass, with the table, the column and its
expression in `status()`'s `capture_failure`; the pass after that stops imaging
the column.

Until the first pass, the application's inserts and deletes on the table fail
(`unexpected virtual generated column reference`), and an update stages the
column as `NULL`, or stages nothing when no other column capture reads changed,
so the readers' targets miss its new values; the rebuild on `RESUME` repairs
them. A virtual column renamed onto the name and back before a pass pauses
nothing ([known correctness
gaps](known-correctness-gaps.md#a-read-columns-name-swapped-with-anothers-and-back-within-one-pass)).

Definitions that don't read the column keep running.

**Remedy:** Write the expression in the definition in place of the column where
the grammar covers it (in a 1-1 transform, under a field name no source column
has), or make the column `STORED`. After a later change, make the column
`STORED` and `RESUME`, which refuses while it is still virtual, or drop the
transform and define it again with the expression.

#### A nondeterministic collation on a key or compared column

**Condition:** A nondeterministic collation (an ICU collation with
`deterministic = false`) on a key column: a source primary key, a `GROUP BY`
key or a `JOIN`'s `ON` column. Also on a column passed to `STRPOS` or
`REGEXP_COUNT`, or compared as text (`=`, `<>`, `!=`, `IS [NOT] DISTINCT FROM`,
`NULLIF`), directly or through `COALESCE`, a `CASE` result or another field.

**Refused at define:** Yes, for the definition (a `JOIN` checks the column of
whichever table the read names), a `RESUME` and an `ALTER TRANSFORM` that adds
or alters such a field. Trellis matches keys by exact text; such a collation's
`=` would fold keys it keeps apart. `CHAR_LENGTH`, `OCTET_LENGTH` and
`IS [NOT] NULL` accept any column. Such a collation's `=` would call two
different texts equal, which Trellis compares exactly.

**If it appears after define:** On a key column, the staging worker pauses
every transform that keys by it, with the reason in `status()`'s
`capture_failure`, and `RESUME` refuses until it's deterministic again. On a
column only a text function reads, Postgres refuses the function, so each
changed row's write fails: its key is held for the transform, and the transform
is quarantined once its held keys reach the fuse threshold.

**Remedy:** Keep the column's collation deterministic
([type-support](type-support.md#collation)).

#### A `REGEXP_COUNT` pattern that isn't a literal or that Postgres rejects

**Condition:** A `REGEXP_COUNT` pattern that isn't a string literal, or that
Postgres rejects (`(?P<n>a)`, `a++`, `\pL`).

**Refused at define:** Yes (`NonLiteralRegexPattern`; `InvalidRegexPattern` for
a pattern Postgres rejects, carrying the server's message). Postgres alone
judges the pattern: define, `ALTER TRANSFORM` and `RESUME` probe each literal
with `regexp_count('', pattern)` and refuse on `invalid_regular_expression`.
Back-references, lookahead and `\m`/`\M` are admitted.

**If it appears after define:** Only a Postgres upgrade that changes the
verdict: each changed row's write then fails with `invalid_regular_expression`
and is quarantined like any row Postgres can't compute, and a `RESUME` probes
again and refuses with the server's message.

**Remedy:** Write a pattern Postgres's `regexp_count` accepts.

### Constants

#### A literal zero divisor

**Condition:** A `/` or `%` whose divisor is written as a literal zero: a
number literal (`n / 0`, `n / 0.0`, `n % -0`) or a float typed literal
(`f / REAL '0'`, `f / DOUBLE PRECISION '-0'`), under any `CAST` or prefix `-`,
whatever the dividend and wherever the division sits: in a `CASE` branch no
row takes, after a constant that ends a `COALESCE`, `AND` or `OR`, beside a
`NULL` constant that makes Postgres drop the division (`(n / 0) + NULL`), or
in an aggregate's argument. Postgres raises `22012` for every row that has a
dividend, so the division is never useful.

Only the text is read. A divisor that computes to zero (`n / (1 - 1)`,
`n / COALESCE(0, 1)`, `n / CEIL(0.0)`, a calculated field that is `0`)
defines, and its rows fail at run time like any other per-row error, as
`x / col` does for the rows where `col` is zero.

**Refused at define:** Yes (`ConstantDivisorZero`), naming the field and the
division as written, for the definition and an `ALTER TRANSFORM`.

**If it appears after define:** n/a: the text does not change.

**Remedy:** Use a nonzero divisor, or a column; `x / NULLIF(col, 0)` gives
`NULL` where `col` is zero.

#### A typed literal not in its type's canonical spelling

**Condition:** A `DATE`, `TIMESTAMP`, `TIME`, `TIMETZ`, `BYTEA`, `OID`,
`REAL`, `DOUBLE PRECISION`, `INET`, `CIDR`, `MACADDR`, `MACADDR8`, `VARBIT`
or `JSONB` literal (or the `CAST('…' AS <type>)` spelling of one) whose text
is not what Postgres's output function prints for the value: `DATE '2024-1-5'`,
`DATE 'today'`, `REAL '1.50'`, `JSONB '{"b": 1, "a": 2}'`. Postgres accepts
many of these, but relative date and time spellings are not immutable, and
the check needs no Postgres (see
[ADR-0004](decisions/0004-transform-definition-grammar.md)).

**Refused at define:** Yes (`InvalidTypedLiteral`), naming the field and the
literal and describing the spelling the type expects, for the definition and
an `ALTER TRANSFORM`.

**If it appears after define:** n/a: the text does not change.

**Remedy:** Write the spelling Postgres prints for the value
(`SELECT CAST('2024-1-5' AS date)::text` shows it): `DATE '2024-01-05'`.

### Comparisons

#### A float compared with an exact numeric

**Condition:** A comparison (`=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`,
`IS [NOT] DISTINCT FROM`, and `NULLIF`'s `=`) with a float (`real`,
`double precision`) on one side and an exact numeric (`smallint`, `integer`,
`bigint` or `numeric`) column, calculated field or expression on the other,
including a calculated field or `COALESCE` that is a float. A bare number
literal against a float is admitted (below), so `f > 0.5` and `f = 1` define.

**Refused at define:** Yes (`MixedFloatExactComparison`), naming the field and
both operand types, for the definition and an `ALTER TRANSFORM`. Postgres casts
the exact side to `double precision`, so the comparison inherits float rounding
(`0.1::numeric + 0.2 = 0.3::float8` is false). A float against a float and an
exact numeric against an exact numeric are admitted; `+` admits the mix.

**If it appears after define:** A column retyped into the other family
(`integer` → `double precision`) pauses the transform on the staging worker's
next capture pass, with the reason in `status()`'s `capture_failure`, and
`RESUME` refuses while the mix remains (the [row on a widened
column](#a-column-trellis-copies-widened) below).

**Remedy:** Compare values of one family: rewrite so both sides are floats or
both are exact. A bare number literal is cast to `double precision` and a
`real` operand widens to it, as in Postgres, so `r > 0.1` over `r = 0.1::real`
is true; write `f > REAL '0.1'` to compare as `real`.

#### A comparison between two temporal types

**Condition:** A comparison between two different temporal types (`date` and
`timestamp`, `timestamp` and `timestamptz`, `time` and `timetz`). Same-type
comparisons of `date`, `timestamp`, `timestamptz`, `time` and `timetz` are
admitted, as are `boolean` and `uuid` against their own type and exact numerics
against each other.

**Refused at define:** Yes (`MixedTemporalComparison`), naming both types, for
the definition and an `ALTER TRANSFORM`. Postgres converts one side first, and
a conversion to or from `timestamptz` depends on the session's time zone.

**If it appears after define:** A column retyped to another temporal type
pauses the transform, as in the rows above.

**Remedy:** Cast one side so both are one type, or compare two columns of one
type.

#### An ordering comparison over `text`

**Condition:** `<`, `<=`, `>`, `>=` over `text`. `=`, `<>`, `!=` and
`IS [NOT] DISTINCT FROM` over text are admitted (under a deterministic
collation, [row above](#a-nondeterministic-collation-on-a-key-or-compared-column)).

**Refused at define:** Yes (`TextOrderingComparison`). The order of text
depends on the collation, which Trellis does not evaluate.

**If it appears after define:** A column retyped to `text` pauses the
transform, as in the rows above.

**Remedy:** Order by a non-text value, or compare `CHAR_LENGTH` of the text.

#### A comparison between types no family covers

**Condition:** A comparison or `NULLIF` between types no family covers: `text`
with a number, `boolean` with a number, `uuid` with `text`, and every type
outside the families (`bytea`, `jsonb`, `interval`, enums, `inet` and the
rest).

**Refused at define:** Yes (`ComparisonTypeMismatch`), naming the operator and
both types.

**If it appears after define:** A column retyped into a type outside the family
pauses the transform, as in the rows above.

**Remedy:** Compare two values of one family; `IS NULL` takes any type.

#### Chained comparisons

**Condition:** Comparisons that chain: `a < b < c`, `a = b = c`, `a < b = c`,
and `IS DISTINCT FROM` followed by another `IS` form. Postgres reads these as a
syntax error.

**Refused at define:** Yes (`NonAssociativeComparison`, a parse error).

**If it appears after define:** n/a.

**Remedy:** Join them with `AND` (`a < b AND b < c`) or add parentheses.

### Grammar forms and typing

#### `IS TRUE` forms and the simple `CASE`

**Condition:** `IS TRUE`, `IS NOT TRUE`, `IS FALSE`, `IS NOT FALSE`,
`IS UNKNOWN`, `IS NOT UNKNOWN`; the simple
`CASE <operand> WHEN <value> THEN ...`.

**Refused at define:** Yes (`UnsupportedIsForm`, `SimpleCaseUnsupported`, parse
errors). Only `IS [NOT] NULL` and `IS [NOT] DISTINCT FROM` are admitted, and
only the searched `CASE`.

**If it appears after define:** n/a.

**Remedy:** `x IS NOT DISTINCT FROM TRUE` is `x IS TRUE` (likewise `FALSE`, and
`IS DISTINCT FROM` for the `NOT` forms), and `x IS NULL` is `x IS UNKNOWN`;
write `CASE WHEN <operand> = <value> THEN ...`.

#### An untyped `NULL` literal

**Condition:** A `NULL` literal its context gives no type: both operands of an
arithmetic operator (`NULL + NULL`), the operand of prefix `-` or of
`AT TIME ZONE`, an operand beside a type that no operator define admits takes
on both sides (`date_col + NULL`, which Postgres finds ambiguous;
`ts_col - NULL`, which it reads as the `interval`-producing
`timestamp - timestamp`), or an argument of a function or aggregate
(`abs(NULL)`, `SUM(NULL)`, `COUNT(NULL)`), directly or through a field that is
only `NULL`.

Postgres finds some of these ambiguous and types the rest by its
function-candidate rules (`abs(NULL)` is `double precision`), which define does
not model.

**Refused at define:** Yes (`UntypedNull`), naming the field and the construct,
for the definition and an `ALTER TRANSFORM`.

**If it appears after define:** n/a: a literal does not change.

**Remedy:** Write `CAST(NULL AS <type>)` with the type you mean
(`abs(CAST(NULL AS integer))`).

#### A non-boolean `CASE` condition or `CASE` results that don't unify

**Condition:** A `CASE` condition that is not a `boolean`, or `CASE` results
that Postgres cannot unify the way `COALESCE` does
(`CASE WHEN c THEN 1 ELSE 'x' END`). Results unify to a common type: two
integers the wider, an exact numeric with a float the float, otherwise
`numeric`, so `CASE WHEN c THEN 1 ELSE 2.5 END` is `numeric` and `real` with
`numeric` is `real`.

**Refused at define:** Yes (`CaseConditionNotBoolean`,
`CaseResultTypeMismatch`), for the definition and an `ALTER TRANSFORM`.

**If it appears after define:** A column retyped so the results no longer unify
pauses the transform, as in the rows above.

**Remedy:** Make the results one type, with `CAST` where an integer meets a
float.

#### A `NULLIF` that casts its first argument

**Condition:** `NULLIF(a, b)` where Postgres casts `a` to compare it, so the
result is not `a`'s type: an integer `a` with a `numeric` `b` (the result is
`numeric`), and an exact `a` with a float `b` (the result is
`double precision`). `NULLIF(a, b)` otherwise has `a`'s type, for any pair `=`
admits, and `x / NULLIF(y, 0)` guards a division.

**Refused at define:** Yes (`NullIfFirstArgumentCast`), naming both types and
the type Postgres gives.

**If it appears after define:** A column retyped into such a pair pauses the
transform, as in the rows above.

**Remedy:** `NULLIF(CAST(a AS numeric), b)`, or write the float first.

### Expression size

#### An expression nested more than 100 levels deep

**Condition:** An expression nested more than 100 levels deep: a chain of more
than 100 operands joined by operators, or nested parentheses or calls to the
same depth. A flat list of many columns, as `COALESCE` arguments or terms of
one sum, is not deep.

**Refused at define:** Yes (`ExpressionTooDeep`), for `CREATE`,
`ALTER TRANSFORM` and a `JOIN`'s fields alike. Every pass that walks an
expression recurses once per level, so the bound keeps a long expression from
overflowing the stack.

**If it appears after define:** n/a (the definition is stored only once it
passes).

**Remedy:** Split the expression into calculated fields that read each other.
Fields that read each other are bounded by the next row.

#### Calculated fields that inline too large or too deep

**Condition:** Calculated fields that read each other so many times, or so
deeply, that inlining them into one expression yields more than a build can
render (a chain of fields that each read the previous one more than once), or
an expression that is more than 100 levels high once the fields it reads are
inlined (each read of another field costs at least one level).

**Refused at define:** Yes (`FieldExpansionTooLarge`). Every build reads each
field as one self-contained expression.

**If it appears after define:** n/a (the fields are fixed at define; an
`ALTER TRANSFORM` is checked the same way).

**Remedy:** Shorten the chain, or read the source columns in the later fields.

#### An expression that inlines to more than 100,000 nodes

**Condition:** An expression whose calculated fields inline to more than
100,000 nodes in total, a very wide `CASE` included (each `WHEN x = 1 THEN 1`
branch is four nodes, so about 25,000 of them).

**Refused at define:** Yes (`FieldExpansionTooLarge`).

**If it appears after define:** n/a.

**Remedy:** Shorten the expression or split it into calculated fields.

### Arithmetic, functions and casts

#### Integer division

**Condition:** `/` between two integers (`smallint`, `integer`, `bigint`), such
as `qty / 2`.

**Refused at define:** Yes (`IntegerDivision`), naming both operand types.
Postgres truncates an integer quotient (`7 / 2` is `3`), which reads as a
rounding rule a `numeric` quotient does not have. `numeric / integer`,
`integer / numeric`, `numeric / numeric` and every float quotient are admitted.

**If it appears after define:** A column retyped from `numeric` to an integer
type pauses the transform on the staging worker's next capture pass, with the
reason in `status()`'s `capture_failure`, and `RESUME` refuses while define
would refuse the definition.

**Remedy:** Divide as `numeric`: `CAST(qty AS numeric) / 2`.

#### Modulo with a float operand

**Condition:** `%` with a float operand (`real`, `double precision`). `%` takes
integers and `numeric`: two integers give the wider width, anything with a
`numeric` gives `numeric`, and the sign follows the dividend (`-7 % 2` is
`-1`).

**Refused at define:** Yes (`FloatModulo`). Postgres has no float modulo.

**If it appears after define:** A column retyped into a float pauses the
transform, as in the row above.

**Remedy:** Use `numeric` operands.

#### An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` does not admit

**Condition:** An argument to `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` that
the function does not admit. `ABS` takes an integer, `numeric` or float and
returns its type. `CEIL`, `FLOOR` and `TRUNC` take `numeric` or
`double precision`. `ROUND(a)`, `ROUND(a, n)` and `TRUNC(a, n)` take a
`numeric` `a` and an `n` that is an integer literal (negative allowed,
`ROUND(a, -2)`), and return `numeric`.

Refused: an integer argument (it is already whole, and Postgres would cast it
to `double precision` and change the result's type), a `real` argument to
`CEIL`, `FLOOR` or `TRUNC` (promoted to `double precision`), a float argument
to `ROUND` (a float rounds half to even, `numeric` half away from zero, so one
name would carry two rules), a scale that is a column or an expression.

**Refused at define:** Yes (`NumericFunctionArgument` with the reason,
`ScaleNotIntegerLiteral`, or `FunctionArgTypeMismatch` for a non-numeric
argument), naming the field and the function, for the definition and an
`ALTER TRANSFORM`.

**If it appears after define:** A column retyped into a refused type pauses the
transform, as in the rows above.

**Remedy:** Use the integer itself; use a `numeric` value, or
`CAST(qty AS numeric)` for an integer one; write the scale as a literal.

#### A `CAST` that is not an exact conversion

**Condition:** A `CAST(<expr> AS <type>)` that is not an exact conversion that
reads no setting: an integer to the same or a wider integer
(`CAST(smallint_col AS integer)`, `AS bigint`, `CAST(bigint_col AS bigint)`),
any integer to `numeric`, a `date` to `timestamp`, and a cast to the operand's
own type (an integer to its own width, `numeric` to `numeric`, `timestamp` to
`timestamp`; Postgres runs it as the identity, so `numeric(12, 2)` `1.50` stays
`1.50` and a `timestamp(3)` keeps its fractional digits) are admitted.

Refused: a narrowing (`bigint` to `integer`), a cast from a float or a
non-numeric type, a cast to or from a float, a `timestamptz` to `timestamp` (it
converts through the session `TimeZone`), any other target spelling
(`smallint`, `text`, `real`, `int2`, `dec`, `timestamptz`, `date`), and a
modifier (`numeric(10, 2)`, `decimal(10, 2)`: it rounds and can overflow;
`timestamp(3)`).

The accepted target spellings are `integer` (or `int`, `int4`), `bigint` (or
`int8`), `numeric` (or `decimal`) and `timestamp`, in any case, in the target
position only: a column named `int` is still a column. A `date` past
294276-12-31 is a value `timestamp` cannot hold: its row raises `22008` and is
quarantined. An alias is typed and checked as its canonical target
(`CAST(bigint_col AS int)` narrows, so it is refused), and `ALTER TRANSFORM`
shows the canonical spelling.

**Refused at define:** Yes (`UnsupportedCast` naming both types for an operand
that does not convert; `TemporalOperand` for a `timestamptz`, with
`AT TIME ZONE 'UTC'` as the remedy; `UnsupportedCastTarget` for a target or a
modifier the grammar has no spelling for), for the definition and an
`ALTER TRANSFORM`. The cast of a quoted literal (`CAST('1.5' AS real)`) is a
typed literal, with its own rules.

**If it appears after define:** A column retyped so its cast no longer converts
(`integer` → `numeric` under `CAST(c AS bigint)`) pauses the transform, as in
the rows above. So does a widening that makes it a narrowing (`smallint` →
`bigint` under `CAST(c AS integer)`): the `capture_failure` names the column,
both types and the refusal, and `RESUME` refuses until the definition is
altered to one that validates or the column is changed back. A retype that
makes a cast an identity (`integer` → `bigint` under `CAST(c AS bigint)`)
pauses nothing.

**Remedy:** Widen the column instead, or compute in `numeric`; for a
`timestamptz`, `tz AT TIME ZONE 'UTC'`.

### Dates and times

#### Date and time arithmetic

**Condition:** `+` or `-` over a date or time operand other than
`date ± integer`, `integer + date` and `date - date`: a `date` with a `bigint`,
`numeric` or float (`d + n`), `date + date`, a number minus a date, a
difference of two timestamps or of a date and a timestamp (`ts2 - ts1`,
`tz2 - tz1`), any operand of type `interval`, and a number added to a
`timestamp`, `timestamptz`, `time` or `timetz`. `date ± integer` takes a
`smallint` or an `integer` count of days.

**Refused at define:** Yes (`TemporalOperand`), naming the operator, the
operand type and the reason, for the definition and an `ALTER TRANSFORM`.
Postgres has no `date + bigint` or `date + numeric`. A difference of timestamps
is an `interval`, which has no literal and whose month part makes a result
depend on the date it is added to (`'2024-01-31' + 1 month - 1 month` is not
`'2024-01-31'`), so nothing here produces or consumes one.

**If it appears after define:** A column retyped into an unadmitted pair
(`integer` → `bigint` beside a `date`) pauses the transform, as in the rows
above.

**Remedy:** Use an `integer` or `smallint` day count. For a duration,
`EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)` is exact `numeric` seconds.

#### `DATE_TRUNC` or `EXTRACT` of an unadmitted type, and `date_part`

**Condition:** `DATE_TRUNC` of a `date` or a `timestamptz`, or `EXTRACT` of a
`timestamptz`, `time`, `interval` or any type but `date` and `timestamp`;
`date_part`.

**Refused at define:** Yes (`TemporalOperand` for the first two; `date_part` is
an unsupported function). `DATE_TRUNC('month', d)` resolves to the
`timestamptz` overload, which is `STABLE` and returns `timestamptz`; both
functions on a `timestamptz` read the session `TimeZone`; `date_part` returns
`double precision`.

`EXTRACT` returns an unconstrained `numeric`, which `SUM`, `AVG` and `COUNT`
take; grouping by a bucket goes through a chain (a 1-1 transform computes
`DATE_TRUNC('month', ts) AS bucket`, an aggregate over that target uses
`GROUP BY bucket`), define.

**If it appears after define:** A column retyped into a refused type pauses the
transform, as in the rows above.

**Remedy:** `DATE_TRUNC('month', CAST(d AS timestamp))`;
`DATE_TRUNC('day', tz AT TIME ZONE 'UTC')`;
`EXTRACT(hour FROM tz AT TIME ZONE 'UTC')`; `EXTRACT` for `date_part`.

#### A `DATE_TRUNC` unit or `EXTRACT` field that is not admitted

**Condition:** A `DATE_TRUNC` unit or an `EXTRACT` field that is not admitted,
or that is not a literal. Units: `microseconds`, `milliseconds`, `second`,
`minute`, `hour`, `day`, `week` (truncates to Monday), `month`, `quarter`,
`year`, `decade`, `century`, `millennium`, in any case. Fields of a `date`:
`year`, `quarter`, `month`, `week`, `day`, `dow`, `isodow`, `doy`, `isoyear`,
`decade`, `century`, `millennium`, `epoch`; a `timestamp` also has `hour`,
`minute`, `second`, `milliseconds`, `microseconds`.

Refused: `EXTRACT(hour FROM <date>)`, `julian`, the `timezone` fields,
abbreviations and plurals (`y`, `days`, `mon`) and a unit given as a column.

**Refused at define:** Yes (`TemporalLiteral`), naming the construct, the text
and what is admitted. A field Postgres lacks for the type raises in it;
`julian` counts from a different origin for each type; the others have one
spelling here.

**If it appears after define:** A column retyped does not change a literal.

**Remedy:** Use a listed spelling.

#### `AT TIME ZONE` other than `'UTC'` over a `timestamptz`

**Condition:** `AT TIME ZONE` with a zone other than the string literal `'UTC'`
(`'America/New_York'`, `'utc'`, `'+05:00'`, a column), or over anything but a
`timestamptz`: a `timestamp` gives a `timestamptz`, and a `date` converts
through the session `TimeZone`.

**Refused at define:** Yes (`TemporalLiteral` for the zone, `TemporalOperand`
for the operand). Another zone's offsets come from the time zone database,
which a release changes under past results, and `AT TIME ZONE` on a `timestamp`
produces a `timestamptz`, which nothing here needs. `'UTC'` is parsed under
`timezone_abbreviations`, which Trellis's sessions pin to `Default`.

**If it appears after define:** A column retyped to another type pauses the
transform, as in the rows above.

**Remedy:** Convert in the source, or `CAST(d AS timestamp)` for a date.

### Retyped and widened columns

#### A key column retyped so stored values render differently

**Condition:** A key column (a source key, a `GROUP BY` key or a `JOIN`'s `ON`
column) retyped so that the values Trellis stored render differently:
`timestamp` ↔ `timestamptz`, `date` → `timestamp`, `text` → `uuid` or
`integer`, `varchar` → `character(n)`, or a narrower `numeric` scale, temporal
precision or `varchar(n)` (the rewrite rounds, or strips trailing spaces).

**Refused at define:** n/a: define sees one type.

**If it appears after define:** An `ALTER COLUMN … TYPE` rewrites the table
without firing a trigger. The staging worker pauses every transform keyed by
the column, with the old and new type in `capture_failure`.

**Remedy:** `RESUME`: it re-types Trellis's copies of the column and rebuilds
from the source. Or drop and define again.

#### A column Trellis copies, widened

**Condition:** A column Trellis creates a column from with its type, widened.
Those columns are a 1-1 target's key, passthrough and calculated columns, an
aggregate's `GROUP BY` key (copied into its target and its hidden table) and
its field columns, a `JOIN`'s `GROUP BY` keys and `ON` columns (copied into its
target and its hidden tables, each with its type, no modifier, and collation)
and its field columns, and a 1-1 `JOIN`'s key, field columns and the columns of
its hidden `R`, `M` and `J`.

Widenings are `integer` → `bigint` (and `smallint` up), an integer → `numeric`
(unless every value of the `numeric` fits the integer), `real` →
`double precision`. A wider modifier is no widening: a copy has none
([ADR-0020](decisions/0020-type-identity.md)).

**Refused at define:** n/a: each column takes its type from the source at
define, or when an `ALTER TRANSFORM … ADD` adds it. A column Trellis copies
has the source column's type with no modifier (a 1-1 key column and a `JOIN`'s
`GROUP BY` and `ON` columns also keep its collation; any other takes the
database default), and a calculated column, an aggregate's field and a plain
aggregate's `GROUP BY` key take their value family's type, which has no
modifier either ([the types of the columns Trellis
creates](type-support.md#the-types-of-the-columns-trellis-creates)), so a change of a modifier (a string bound, a `numeric`
precision, a temporal precision, a move to `text`) changes nothing there.

**If it appears after define:** A widening of a modifier that changes only the
catalog needs no re-type, no pause and no rebuild. Otherwise the column keeps
its type, so a value it can't hold would fail its write. The staging worker
pauses every transform that owns a column the type define would give it now
outgrew (a 1-1 target's copy of `qty` after `qty` goes `integer` → `bigint`),
naming the source column and each of those columns. A narrowing pauses nothing
here, except a `varchar(n)` key's (the row above).

A narrowing, a field's move to another type family (`integer` →
`double precision`), or any other change that isn't a pure widening, rebuilds
the table's readers on its own, and pauses a transform define would now refuse,
or one that evaluates the column as another type ([capture by
triggers](staging-and-claiming/01-capture-by-triggers.md#a-column-changed-or-rewritten-without-a-trigger)).

**Remedy:** `RESUME`: the staging worker `ALTER`s each column whose type
differs from the one define would give it now, under `ACCESS EXCLUSIVE` (a
table rewrite for `integer` → `bigint`; reads of the target wait), then
rebuilds. A `JOIN`'s or a 1-1 `JOIN`'s hidden tables are created again at the
new types instead. Or drop and define again.

### Target columns and widths

#### Two `GROUP BY` keys that share a target column name

**Condition:** Two `GROUP BY` keys that share a target column name (a key
listed twice, or a `JOIN`'s `orders.region` and `customers.region`); keys can't
be aliased.

**Refused at define:** Yes (`GroupByKeysShareTargetColumn`). Each key becomes a
target column named after its bare column name.

**If it appears after define:** n/a.

**Remedy:** List a key once. For a `JOIN`, join a 1-1 transform's target that
selects one side's column under another name (`region AS customer_region`).

#### A key of more than 32 columns

**Condition:** A key of more than 32 columns: a `GROUP BY` of more than 32
columns, or a `JOIN` side whose `ON` columns and `GROUP BY` columns not among
them together number more than 32 (a 1-1 `JOIN` has no `GROUP BY`, so its `ON`
alone, which may number at most 31: its hidden `R` indexes them followed by the
row's key, which a `b` change's chunks walk). Postgres caps an index at 32
columns (`INDEX_MAX_KEYS`) and Trellis keys the target and a `JOIN`'s hidden
tables on these columns.

**Refused at define:** Yes (`IndexKeyTooWide`, naming the clause and the
limit). A source key is at most 32 columns too, so a 1-1 target's key never
exceeds it.

**If it appears after define:** n/a.

**Remedy:** Group by or join on fewer columns.

#### A table of more than 1,600 columns

**Condition:** A table of more than 1,600 columns, Postgres's cap on one table
(`MaxHeapAttributeNumber`).

Trellis creates a target with a column per key and field, and hidden tables
whose width follows the fields too: an aggregate's hidden table has its
`GROUP BY` columns, a column counting each distinct argument, a column summing
each one a `SUM` or `AVG` reads, and a column for each distinct `MIN` or `MAX`
call (with two more for their basis and stamp when there is one); a `JOIN`'s
hidden partials tables do the same, per side and for the groups; a 1-1 `JOIN`'s
ledger for a side has a column for every column that side's fields read.

So about 800 distinct summed aggregate arguments overflow a hidden table that
their target fits in.

**Refused at define:** Yes (`TableTooWide`, naming the table's role and the
limit), for the definition, `RESUME` and an `ALTER TRANSFORM … ADD`. An
`ALTER TRANSFORM` counts the columns earlier edits dropped from the target, and
the column a `DROP z, ADD … AS z` replaces, since Postgres keeps counting a
dropped column until the table is created again.

An aggregate meets smaller limits first, which define does not refuse: from
about 270 distinct summed arguments the statements that build and apply it
exceed Postgres's 1,664-entry target list, so its build pauses or its changed
groups are quarantined, with Postgres's error
([#1518](https://github.com/salesforce-misc/trellis/issues/1518)), and from
about 500 a hidden table's row can outgrow a heap page
([#1516](https://github.com/salesforce-misc/trellis/issues/1516)).

**If it appears after define:** n/a: the tables are created with the
definition, and only an `ALTER TRANSFORM … ADD` of a 1-1 transform adds a
column.

**Remedy:** Define fewer fields, or fewer distinct aggregate arguments. If
dropped fields filled the limit, drop the transform and define it again.

### Field, column and table names

#### A field named after a key column or starting with `__`

**Condition:** A field named after a 1-1 source key column or a 1-1 `JOIN`'s
`a` key column (`id AS id`), or any field or `GROUP BY` column whose name
starts with `__`.

**Refused at define:** Yes. The target already carries the key columns, and
`__` names are reserved for Trellis's hidden columns. `id AS order_id` is an
ordinary column.

**If it appears after define:** n/a.

**Remedy:** Rename the field.

#### An aggregate over another aggregate field

**Condition:** An aggregate whose argument names another aggregate field when
no source column has that name (`SUM(val) AS total, SUM(total)`), directly or
through a field that reads one.

**Refused at define:** Yes (`AggregateOfAggregateField`). The argument would
nest one aggregate in another, which no build can run.

**If it appears after define:** n/a.

**Remedy:** Aggregate the source column instead (`SUM(val)`).

#### A bare name that could mean a source column or a field

**Condition:** A bare name that could mean a source column or a field: the name
of a source column and of a field whose expression is not just that column,
read by another field (`(grp + 1) AS val, SUM(val) AS total`). An aggregate
field's name inside another aggregate's argument is not ambiguous: it is the
source column ([Calculated Fields](#calculated-fields)).

**Refused at define:** Yes (`AmbiguousColumnReference`), naming both meanings.
`val AS val` (or `src.val AS val`) passes the column through, so reading `val`
elsewhere is not ambiguous.

**If it appears after define:** A source column added later under the name of a
field: nothing is detected, and the transform keeps reading the field. An
`ALTER TRANSFORM` that adds a field of that name is refused, as define would.
`RESUME` re-validates and refuses what define would: in an aggregate target, a
field another field reads by that name, and in a 1-1 target, any field of that
name.

**Remedy:** At define, write `src.val` or `public.src.val` to read the source
column, or rename the field and read it by its new name. After a later column,
rename or drop that column and `RESUME`, or drop the transform and define it
again.

#### An unquoted reserved word as a name

**Condition:** An unquoted name that Postgres reserves in an expression, a
`GROUP BY` or `KEY (…)` column, or a `JOIN`'s `ON` and `GROUP BY` qualifier:
any word `pg_get_keywords()` marks reserved (`R`) or reserved as a type or
function name (`T`), such as `order`, `user`, `end`, `not`, `from`, `left`,
`case`, `and`, `is`. Postgres needs such a column quoted.
`GROUP BY current_user` reads the function in Postgres, and `GROUP BY order` is
a syntax error there.

A quoted name (`"order"`), a name after a `.` (`t.order`), the label after `AS`
(`val AS limit`) and a call (`left(x, 2)`) are not bare names and are fine; a
non-reserved keyword (`extract`, `position`, `values`, `between`, `coalesce`,
`nullif`, `char`) is still a column.

**Refused at define:** Yes (`ReservedWord`), for the definition and an
`ALTER TRANSFORM`, naming the word and the quoted spelling. A source table with
a column of that name defines; an expression reads it quoted or qualified.

**If it appears after define:** Adding or renaming a column to a reserved word
needs quotes in Postgres; it is a rename like any other. A transform that reads
the old name pauses, and one that doesn't read the column is unaffected.

**Remedy:** Quote it (`"order"`, and `"order" AS "order"` copies it under its
own name), or qualify it with its table (`orders.order`).

#### An unquoted reserved table name

**Condition:** An unquoted table name that Postgres reserves (`R` or `T`, as in
the row above), as the target, the `FROM` table or the `JOIN` table:
`FROM user` reads the `current_user` function in Postgres, and `FROM order` is
a syntax error there. A schema-qualified name's table part (`public.order`) is
fine, as in Postgres.

**Refused at define:** Yes (`ReservedTableName`, a parse error), naming the
word.

**If it appears after define:** n/a: a table name does not change under a
stored definition's text.

**Remedy:** Quote the table's name as the catalog spells it (`FROM "User"`,
`FROM "order"`).

#### A name over 63 bytes

**Condition:** A name over 63 bytes, after folding to lower case and unescaping
`""`, in any position (a table, schema, column, `KEY` or `GROUP BY` column,
`AS` label, alias or addressed transform). Postgres truncates such a name, so
two long names sharing their first 63 bytes would be one.

**Refused at define:** Yes (`NameTooLong`, a parse error), naming the name and
the 63-byte name Postgres would truncate it to.

**If it appears after define:** A definition 0.1.x stored with a longer `AS`
label (Postgres truncated its target column) keeps running, but its text can't
be written in the current grammar: an `ALTER TRANSFORM` of it is refused
(`NameTooLong`), and the upgrade that writes a `KEY (…)` into it pauses it
instead.

**Remedy:** Use a name of 63 bytes or fewer. Drop such a stored definition and
define it again.

#### An unsupported quoted name

**Condition:** A quoted name that is zero-length (`""`, which Postgres refuses
too), the Unicode-escape spelling `U&"…"`, a quoted name holding a `.` or a
non-ASCII character, or a quoted function or type name (`"lower"(x)`,
`CAST(x AS "int4")`, `"date" '2024-01-31'`), which Postgres accepts. Postgres
looks a quoted function or type name up exactly, so `"int4"` is a type there
and `"INT4"` and `"integer"` are not. Trellis addresses a table by its
`<schema>.<table>` text, which a `.` inside a name would make ambiguous.

**Refused at define:** Yes (`ZeroLengthQuotedIdentifier`,
`UnicodeEscapeIdentifier`, `UnsupportedQuotedIdentifier`, parse errors).

**If it appears after define:** n/a.

**Remedy:** Rename the table or column, or write the name with its ASCII
characters. Write function and type names unquoted (`lower(x)`,
`CAST(x AS int4)`).

#### A qualified name that doesn't name the `FROM` table

**Condition:** A qualified name that doesn't name the `FROM` table: a two-part
name whose head is another table (`product.cost`), or a three-part name whose
schema isn't the `FROM` table's. A `JOIN`'s are in the `JOIN` rows below.

**Refused at define:** Yes (`QualifiedColumnNotFromSource`; a `GROUP BY` key is
a parse error, `GroupByKeyNotFromSource`). `ALTER TRANSFORM` refuses it in an
added or altered field.

**If it appears after define:** n/a.

**Remedy:** Name the `FROM` table, or read the second table with a `JOIN`.

### Source indexes

#### A source without the index its `self_check` walk pages by

**Condition:** A `GROUP BY` aggregate or a `JOIN` whose source lacks the index
its `self_check` walk pages by: for an aggregate, a valid, non-partial btree
index on the plain source columns whose leading columns are the `GROUP BY`
columns, in `GROUP BY` order; for a `JOIN`, on each side, one whose leading
columns are that side's `ON` columns in `ON` order.

A primary key or unique constraint that leads with them counts; an expression
or partial index, one under a non-default operator class such as
`text_pattern_ops`, or one whose leading columns are not all ascending with
`NULLS LAST` (the default) or all `DESC` with `NULLS FIRST` (the `DESC`
default, read backward), such as `(a, b DESC)`, doesn't.

A 1-1 target needs nothing (its walk pages by the primary key), and a `JOIN`'s
`T` part, with group columns from both tables, is served by no one table's
index and needs none.

**Refused at define:** Yes (`SourceIndexMissing`), naming the table, the
columns, the part of the definition that needs the index (the `GROUP BY`
columns, or a `JOIN`'s `ON` columns on its from or joined side) and a
ready-to-run `create index`. `WITHOUT SOURCE INDEX` after the `GROUP BY` or
`JOIN` clause (`TRANSFORM t FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT …`)
opts out for that definition, except one with a `MIN` or `MAX` field, which
needs more (the [`MIN`/`MAX` row](#min-or-max-in-a-plain-aggregate) below).
Builds need no index.

**If it appears after define:** Dropping or invalidating the index later pauses
nothing, since it affects speed, never correctness. `status()` reports it in
`source_index_gaps` (an opted-out definition's gap is listed too), `self_check`
refuses to walk a definition that did not opt out, and a whole-transform
`RESUME` refuses with the define message. An opted-out definition's
`self_check` still runs, scanning the source once per page.

**Remedy:** Create the index the message names; or define it
`WITHOUT SOURCE INDEX`.

### `JOIN` transforms

#### A `JOIN` transform that passes the checks below

**Condition:** A `JOIN` transform
(`FROM a [LEFT|INNER] JOIN b ON … GROUP BY … SELECT …`) that passes the checks
in the rows below.

**Refused at define:** No. Define registers it and creates its target, and
Trellis builds it from both tables. Each table is held to the rows above, as a
`FROM` is. Either table may be another transform's target, read through that
target's change feed; the build first waits out the transactions that began
before it ([data flow](data-flow.md#the-re-derive-build),
[ADR-0019](decisions/0019-join-transforms.md#a-side-may-be-another-transforms-target-through-the-target-change-feed)).

**If it appears after define:** A column the transform reads from either table
dropped or renamed, or a key column of either (its primary key, an `ON` or a
`GROUP BY` column) retyped as the key rows above describe, pauses the whole
transform on the staging worker's next pass, naming that table and column in
`status()`'s `capture_failure`, and so does a column of either table retyped to
a type the transform evaluates differently. A widening of a column it copies is
handled as the [widening row](#a-column-trellis-copies-widened) above
describes.

Either table dropped or renamed pauses it, as the [row for a dropped
source](#a-source-dropped-renamed-swapped-or-its-capture-disrupted) describes.
`RESUME` checks both tables and refuses, naming the table, while define would
refuse the transform.

**Remedy:** After a later change, undo it, then `RESUME`.

#### An unsupported `JOIN` shape

**Condition:** A `JOIN`, with or without a `GROUP BY` (one without is a 1-1
`JOIN`, the [next row](#a-1-1-join)), that is `RIGHT`, `FULL`, `CROSS`,
`NATURAL`, `LATERAL`, `USING` or a comma join; joins a table to itself (even
under two aliases: one captured change would feed both sides), or two tables
with the same bare name (`shop.orders JOIN archive.orders`); gives a table an
alias that is a word Postgres reserves (`AS left`), that the other table goes
by (its alias or bare name), or that a single-table definition gives its `FROM`
table; reads an aliased table's column through the table's name (`orders.total`
after `orders AS o`); joins one table twice, under any alias; with a
`GROUP BY`, joins a third table; has an `ON` that is anything but equalities
between a column of each table joined by `AND` (`OR`, `<`, a literal, an
expression, two columns of one table, a column pair repeated, a column named by
two terms); reads a column without its table (`SUM(qty)`, `GROUP BY region`) or
through a table that is neither side; or, with a `GROUP BY`, has an aggregate
whose argument reads both tables (`SUM(orders.x + items.y)`), or a field that
is neither a `GROUP BY` key selected on its own nor a composition of aggregates
(a column outside an aggregate, a nested aggregate, no aggregate at all).

**Refused at define:** Yes (parse error, `TableAlias` or `AliasedTableName` for
an alias, or `JoinTooManyTables`, `JoinSelf`, `JoinOnSameSide`,
`JoinOnRepeatedPair`, `JoinOnRepeatedColumn`, `JoinUnqualifiedColumn`,
`JoinUnknownTable`, `JoinUnresolvedColumn`, `JoinMixedSideArgument`,
`JoinFieldShape`).

Only `INNER` and `LEFT` equi-joins of different tables are admitted, two tables
with a `GROUP BY` (a 1-1 `JOIN` may read more, the [row after
next](#1-1-join-of-more-than-two-tables-star)), with every column written
`<table>.<column>`, or `<alias>.<column>` for a table given an alias.

A field is a `GROUP BY` key from either table, or composes aggregates with
literals, operators and functions (`COALESCE(SUM(items.qty), 0)`); each
aggregate is `COUNT(*)`, or `COUNT`/`SUM`/`AVG` of a column or an expression
over one table's columns (`SUM(items.qty + 1)`). Each argument is kept as a
running total on its own table, so one reading both tables has no such total.

**If it appears after define:** n/a.

**Remedy:** Rewrite the definition in the admitted shape. For `RIGHT JOIN`,
swap the two tables and use `LEFT JOIN`. Qualify an aliased table's columns
with its alias.

#### A 1-1 `JOIN`

**Condition:** A 1-1 `JOIN` (`FROM a [LEFT|INNER] JOIN b ON … SELECT …`, no
`GROUP BY`, [ADR-0021](decisions/0021-one-to-one-join-transforms.md), [1-1
JOIN](#1-1-join)): one whose `b` `ON` columns are not unique, that has an
aggregate field, that reads a column that starts with `__`, whose side is
partitioned, whose `ON` names one column in two terms, whose `ON` reads two
columns of `a` whose names share their first 60 bytes, or whose `b` is another
transform's 1-1 target joined on columns that leave out part of that target's
primary key.

**Refused at define:** Yes (`OneToOneJoinKeyNotUnique`,
`OneToOneJoinAggregate`, `OneToOneJoinUnsupported`, `PartitionedSource`,
`JoinOnRepeatedColumn`; a `WITHOUT SOURCE INDEX` is a parse error). `b`'s `ON`
columns must contain a unique key of `b`: its primary key, a unique constraint
or a non-partial unique index on plain columns (deferrable, or with `INCLUDE`
columns, is fine for this check), or the group key of an aggregate or `JOIN`
target.

`b`'s row-identity key, its primary key or `KEY`, is a separate rule: it must
not be deferrable (the [key row](#the-key-a-transform-keys-each-table-by)
above), even when a deferrable key is what makes its `ON` columns unique.

When `b` is another transform's 1-1 target, its `ON` columns must also include
every column of its primary key, so a unique index added to the target never
admits a join on another column: Trellis orders changes per primary-key value
only, so only the primary key is unique at every instant while the upstream
drains. An `a` that is a 1-1 target may be joined on any columns. A field is
any expression over `<table>.<column>` columns of either table. The target is
keyed by `a`'s primary key.

The joined table needs no source index. Trellis builds the target in chunks of
`a`'s primary key, keeps it readable across a `RESUME`, and applies changes,
Re-derives and `TRUNCATE`s of either table. A `TRUNCATE` of `a` empties the
target; one of `b` starts a rebuild of it, so the transform reads
`backfilling`, and a row keeps the `b` columns it held (an `INNER` row stays)
until the rebuild reaches it.

A `b` that is another transform's target is never truncated: a `TRUNCATE` of
that transform's source reaches the `JOIN` as a delete of every row of `b`, a
change of `b` like any other. A page whose changes of either table, or a chunk
of a `b` change's fan-out, fail on more rows of `a` than its bisection can
name, pauses the transform, and everything downstream of it, with a
`capture_failure` of kind `halt`.

A row of `a` a page fails to write, for its own change or a change of `b`, is
otherwise held in quarantine on its first failure, and the page applies every
other change.

**If it appears after define:** As for a `JOIN` (the [row
above](#a-join-transform-that-passes-the-checks-below)): a column the transform
reads from either table dropped or renamed, a key column of either (`a`'s
primary key, an `ON` column) retyped as the key rows above describe, or a
column retyped to a type the transform evaluates differently, pauses the whole
transform, naming that table and column in `status()`'s `capture_failure`.

A widening of a column it copies (`a`'s key, a field, `R`, `M` and `J`) is
handled as the [widening row](#a-column-trellis-copies-widened) above
describes; a field over both tables is reported against the table whose column
widened, naming every column it is typed from. Either table dropped, renamed or
joining a hierarchy pauses it, as for a source.

`b`'s unique key on its `ON` columns dropped, or left `INVALID` by a failed
build, pauses the whole transform on the staging worker's next pass, naming `b`
and its `ON` columns, whether or not a duplicate arrived; a duplicate Postgres
admits past a deferrable key under `session_replication_role = replica`, which
leaves the key valid, pauses nothing ([known correctness
gaps](known-correctness-gaps.md#a-duplicate-in-b-past-a-deferrable-key-under-session_replication_role--replica)).

`RESUME` refuses a `b` whose `ON` columns are no longer unique, naming the
table, and re-keys the target to a changed key of `a` as for a 1-1 transform
(the [key row](#the-key-a-transform-keys-each-table-by) above).

**Remedy:** Join `b` on a unique key (a 1-1 target's, on columns that include
its primary key, or join the table that transform reads), and give an aggregate
a `GROUP BY`. Rename a column of `a` whose name's first 60 bytes another `ON`
column of `a` shares. After a later change, undo it, then `RESUME`; a widening
needs only the `RESUME`. For `b`'s unique key, restore the key, then `RESUME`
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).

#### 1-1 `JOIN` of more than two tables (star)

**Condition:** A 1-1 `JOIN` of more than two tables, a star
(`FROM a JOIN b1 ON … LEFT JOIN b2 ON … SELECT …`, [1-1 JOIN](#1-1-join)): one
of more than 5 tables (`a` and 4 joined tables); one where a joined table's
`ON` reads a joined table other than its own (a snowflake, `JOIN b2 ON b1.id =
b2.id`, or an `ON` that reads `a` and two joined tables); or one that breaks a
rule of the [row above](#a-1-1-join) or of the [`JOIN` shape
row](#an-unsupported-join-shape) at any position.

**Refused at define:** Yes (`StarJoinTooManyTables`,
`JoinOnReadsAnotherJoinedTable`, and the refusals of the rows above, naming
the table at the position that breaks them).

The joins read left to right, as Postgres reads them without parentheses; each
joined table is joined to `a` directly, and every rule of the [row
above](#a-1-1-join) holds at each position: its `ON` columns contain a unique
key of its table (`OneToOneJoinKeyNotUnique` names that table), its row-identity
key is not deferrable, a joined 1-1 target is joined on columns that include its
primary key, no table is partitioned (`PartitionedSource`), no two `ON` columns
of `a` share their first 60 bytes, and the target isn't named after a table it
reads (`TargetEqualsSource`). Every refusal of the [`JOIN` shape
row](#an-unsupported-join-shape) holds at every position: `RIGHT`, `FULL`,
`CROSS`, `NATURAL`, `LATERAL`, `USING` and a comma join, and a table read twice,
`a` included, under any alias (each a parse error that names it). A `JOIN` with a `GROUP BY` still
reads two tables (`JoinTooManyTables`). A joined table with few distinct keys (a
lookup) is admitted at any position; `status()` warns of its fan-out
([1-1 JOIN](#1-1-join)).

**If it appears after define:** Each joined table is handled as the [row
above](#a-1-1-join) describes for `b`, at whichever position it is joined,
naming that table and its column:

- **Schema change.** A column the star reads there dropped, renamed or retyped,
  a widening, the table dropped, renamed or joining a hierarchy, or a value of
  an enum read there relabelled, pauses the whole transform.
- **Unique key.** The table's unique key on its own `ON` columns dropped or
  left `INVALID` pauses the whole transform on the staging worker's next pass;
  a duplicate past a deferrable key under `session_replication_role = replica`
  pauses nothing, at any position ([known correctness
  gaps](known-correctness-gaps.md#a-duplicate-in-b-past-a-deferrable-key-under-session_replication_role--replica)).
- **`TRUNCATE`.** A `TRUNCATE` of `a` empties the target. One of a joined
  table, at any position, starts a rebuild, so the transform reads
  `backfilling`; a row keeps the columns it held at every position until the
  rebuild reaches it. One statement that truncates several of the star's
  tables is one rebuild.

`RESUME` re-validates every position as define does, refusing a position whose
`ON` columns are no longer unique or whose `ON` pair no longer has one type and
collation, and re-keys the target to a changed key of `a`.

**Remedy:** At most 5 tables, each joined to `a` on a unique key of its own.
Join a snowflake's outer table through a transform of its own and join that
target. After a later change, as for the [row above](#a-1-1-join): undo it,
then `RESUME`.

#### `JOIN` key columns that differ or can't be keys

**Condition:** A `JOIN` whose `ON` columns differ in type or collation, are off
the join-key allowlist, or have a nondeterministic collation; or a `GROUP BY`
key of a type that can't be one.

**Refused at define:** Yes (`JoinKeyTypeMismatch`, `JoinUnsupportedKeyType`,
`JoinNondeterministicCollation`, `UnsupportedGroupByKeyType`). Each `ON`
column's type must be on the join-key allowlist in
[type-support.md](type-support.md) (a domain never is), with a deterministic
collation. The two columns of each pair need the same type and the same
collation, because Trellis never casts a join key.

A type modifier never counts, and `text` and `varchar` are one type:
`varchar(10)` pairs with `varchar(50)`, `text` or a Trellis target's copy, and
`timestamp(0)` with `timestamp(3)`, while `integer` against `bigint` is
refused. A `GROUP BY` key follows the `GROUP BY` key rules.

**If it appears after define:** An `ON` column retyped to another type
(`integer` to `bigint`) so its pair no longer matches, or onto a refused type
or a nondeterministic collation, pauses the whole transform on the staging
worker's next pass, naming that table and column in `status()`'s
`capture_failure`. A change of one column's modifier leaves the pair matching,
so a widening pauses nothing; a change that re-renders the keys Trellis stored
(a narrower bound or precision) pauses the transform, and `RESUME` rebuilds it.

A change between deterministic collations pauses nothing (byte equality is
unchanged), but `RESUME` refuses the pair until the collations match too.

**Remedy:** Alter one column to match the other, then `RESUME`.

### Aggregate functions

#### `MIN` or `MAX` in a plain aggregate

**Condition:** `MIN` or `MAX` in a plain aggregate (`GROUP BY` over one table)
whose argument is not a plain source column (`MAX(a + b)`, a cast, a calculated
field), is of a type other than `smallint`, `integer`, `bigint`, `numeric`,
`real`, `double precision`, `date`, `time`, `timestamp`, `timestamptz`,
`interval` and `oid` (`text` among them), or is a
column of another transform's target; a definition with such a field declared
`WITHOUT SOURCE INDEX`; or one whose source lacks, for each argument, a valid,
non-partial btree index whose leading columns are the `GROUP BY` columns in
order, then the argument (under the index rules of the [source-index
row](#a-source-without-the-index-its-self_check-walk-pages-by) above).

**Refused at define:** Yes (`ExtremeOfExpression`,
`UnsupportedExtremeArgument`, `ChainedExtreme`, `ExtremesWithoutSourceIndex`,
`ExtremeIndexMissing`), naming the call and, for an index, the table, the
columns and a ready-to-run `create index`, one per argument, which also serves
as the `GROUP BY` index. A group keeps each extreme, and a deletion or update
that removes it re-reads the group's next one from the source after the group's
lock, as one endpoint of that index ([type
support](type-support.md#min-and-max)).

Without the index each re-read scans the group, and with no `GROUP BY` index
either the whole source, once per group: a page can then outrun
`statement_timeout` and retry forever, so `WITHOUT SOURCE INDEX` doesn't apply.
Another transform's target has no such index, and building one is a long
`CREATE INDEX` on a table Trellis owns.

**If it appears after define:** A whole-transform `RESUME` refuses with
define's message while a probe index is missing or invalid, and refuses the
other conditions as define does. A probe index dropped or invalidated while the
transform runs leaves the target exact: `status()` lists it in
`extreme_index_gaps`, and each re-read scans its group.

Once no valid probe index has existed on any of the staging worker's passes for
5 minutes (wall-clock time, a fixed constant; a pass that finds one resets it,
and a restart or a new leader starts it again), the pass pauses the transform,
with a `capture_failure` naming the table, the columns and a ready-to-run
`create index` (for a partitioned source, also to re-run
`ALTER INDEX … ATTACH PARTITION` after repairing a leaf's index, whose repair
alone leaves the root's index invalid).

An equivalent index built before the old one is dropped, `REINDEX CONCURRENTLY`
and a rename pause nothing; a rebuild that drops the old index first and takes
longer than 5 minutes pauses
([recommendations](recommendations.md#keep-the-indexes-a-definition-relies-on)).
An argument re-typed to a refused type pauses the transform, as the retype rows
above describe.

**Remedy:** Create the index the message names; drop `WITHOUT SOURCE INDEX`;
take `MIN`/`MAX` of a plain column (a stored generated column for an
expression), in the transform that reads the source. After a pause, create the
index the message names, then `RESUME`.

#### `MIN` or `MAX` in a `JOIN` transform

**Condition:** `MIN` or `MAX` in a `JOIN` transform.

**Refused at define:** Yes (`DeferredAggregate`, family `MIN/MAX`). A `JOIN`
target's extreme is the extreme of each joined key's extreme over the keys that
still match, each of them a re-read of the fact side, which Trellis doesn't
model.

**If it appears after define:** n/a.

**Remedy:** Take the `MIN`/`MAX` in a plain aggregate over one table.

#### `bool_and`, `bool_or`, `bit_and` or `bit_or`

**Condition:** `bool_and`, `bool_or`, `bit_and` or `bit_or` in an aggregate or
a `JOIN` transform.

**Refused at define:** Yes (`DeferredAggregate`, family
`bool_and/bool_or/bit_and/bit_or`).

**If it appears after define:** n/a.

**Remedy:** Not supported yet:
[#1118](https://github.com/salesforce-misc/trellis/issues/1118).

#### `SUM` or `AVG` over an unsupported type

**Condition:** `SUM` or `AVG` over a float (`real`, `double precision`), an
`interval`, or any type but `smallint`, `integer`, `bigint` and `numeric`, in
an aggregate or a `JOIN` transform.

**Refused at define:** Yes (`DeferredAggregate`, family
`float and interval SUM/AVG`, for a float or an `interval`; another type is
refused naming its type). A running float sum drifts from Postgres's, which
adds the group's values in scan order.

**If it appears after define:** The argument column re-typed to a float
(`integer` → `double precision`) pauses the transform on the staging worker's
next pass, naming the column in `status()`'s `capture_failure`; `RESUME`
refuses with define's error until the type is admitted again.

**Remedy:** Sum an integer or `numeric` column. Not supported yet:
[#1119](https://github.com/salesforce-misc/trellis/issues/1119).

#### `array_agg`, `string_agg` or `jsonb_agg`

**Condition:** `array_agg`, `string_agg` or `jsonb_agg` in an aggregate or a
`JOIN` transform.

**Refused at define:** Yes (`DeferredAggregate`, family
`array_agg/string_agg/jsonb_agg`).

**If it appears after define:** n/a.

**Remedy:** Not supported yet:
[#1040](https://github.com/salesforce-misc/trellis/issues/1040).

### Filters and chaining

#### A `WHERE` other than `TRUE`

**Condition:** A `WHERE` other than `TRUE`.

**Refused at define:** Yes (parse error). Partial data is not supported
([#804](https://github.com/salesforce-misc/trellis/issues/804)).

**If it appears after define:** n/a.

**Remedy:** None.

#### Chaining off a transform that is not `live`

**Condition:** Chaining off a transform that is not `live`
(`TransformNotLive`).

**Refused at define:** Yes.

**If it appears after define:** n/a.

**Remedy:** Wait for the upstream to go `live`, then define.

### Target names

#### A target whose name is taken

**Condition:** A target whose name is taken: a relation of any kind already
exists in the target schema under the target's name or the name of a table
Trellis keeps beside it (`<target>__ledger`). That includes another
definition's target, in this instance or in another instance sharing the target
schema.

**Refused at define:** Yes (`TargetTableExists`, a `conflict` error). Trellis
writes only to target tables it creates, or to an empty table it adopts on
request, and `DROP TRANSFORM` drops the target, so it never takes over an
existing relation unasked.

With `ADOPT EXISTING TABLE` after the target
(`TRANSFORM t ADOPT EXISTING TABLE FROM s …`), define builds into the existing
table instead, if it is a plain, empty table shaped exactly as define would
create it: the same columns in the same order, each with the type (no
modifier), collation and nullability define gives it, and the key define
creates (the primary key, or an aggregate's or `JOIN`'s
`UNIQUE NULLS NOT DISTINCT` group key, not deferrable and with no `INCLUDE`
columns).

Define records that key, so a unique constraint the table already has never
keys it instead (the [key row](#the-key-a-transform-keys-each-table-by) above).

It may carry anything a user may add to any target (indexes, unique or not;
constraints; triggers; rules; foreign keys, its own or another table's
referencing it; grants; comments; row security policies, subject to the
[row-security row](#row-level-security-on-a-role-trellis-runs-as) above;
extended statistics; publications; views over it; storage parameters; a replica
identity), with the outcomes [any target
has](#target-tables-are-trellis-owned).

Anything else refuses with `TargetNotAdoptable`, its message naming the
property by code: `missing` (no such table, a `not_found` error),
`not_a_table`, `partitioned`, `partition`, `inheritance`, `typed_table`,
`unlogged`, `extension_member`, `access_method`, `not_empty`, `not_owner` (the
defining role is neither its owner nor a member that inherits the owner's
privileges, so it couldn't alter or drop it later), `privilege` (it lacks
`SELECT`, `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE` on it), `missing_column`,
`extra_column`, `column_order`, `column_type`, `column_collation`,
`column_nullability`, `column_default`, `generated_column`, `identity_column`,
`key`, `owned_sequence`, and `unrecognized` for any constraint or object
depending on the table the check doesn't know, so a new Postgres feature
refuses until Trellis learns it (all `validation` errors).

The emptiness and shape are checked under a lock that keeps writes and DDL out
until define commits. Another definition's target in the same instance refuses
with `TargetTableExists`, empty or not; another instance's empty target can't
be told from a user's table, so don't adopt one (both instances would write
it). The ledger (`<target>__ledger`) and hidden tables are always created by
define: one whose name is taken refuses as without the opt-in.

**If it appears after define:** n/a: an adopted target is an ordinary one after
define; rows written into it by hand afterwards are the [known correctness
gap](known-correctness-gaps.md#rows-written-into-a-target-a-hidden-table-or-trelliss-schema)
any target has.

**Remedy:** Choose another target name, give the instance its own target schema
([instance-identity](instance-identity.md)), or drop or rename the existing
relation. To adopt a table, make it match: the message names the first
difference (empty it, fix the column, `ALTER TABLE … OWNER TO` the defining
role or grant it membership in the owner `WITH INHERIT TRUE`, grant the missing
privilege).

#### A target that would take a read table's name

**Condition:** A target, of any kind, that would take the name of a table
another transform reads as its `FROM` or joined table: one dropped or renamed
away since that transform was defined (while the table is still there, its name
is taken: `TargetTableExists`), or one `ADOPT EXISTING TABLE` would adopt.

**Refused at define:** Yes (`TargetNameHasReaders`), naming every such
transform, whatever its status, a paused one included. The reader would
otherwise go on reading the new target through its change feed and keep the
rows it derived from the table that had the name, since the staging worker's
pass treats a name that is a target's as fed by that target, not by capture.

**If it appears after define:** n/a: once the target exists, a new reader of
its name is a chain, and the target can't be dropped while one reads it
(`DependentsBlockDrop`).

**Remedy:** `DROP TRANSFORM` each reader the message names (pause it first if
it is running), then define the target; or give the target another name.

#### A target in a Trellis catalog schema

**Condition:** A target in a Trellis catalog schema:
`TRANSFORM <schema>.<target>` naming this instance's catalog schema, or a
schema holding another instance's `trellis_instance` table (matched by schema
name exactly as written, and by a table of that name, even one the role can't
read). The target and its ledger would live there, and the other instance's
uninstall (`drop schema … cascade`) would drop them.

**Refused at define:** Yes (`TargetInCatalogSchema`, a `validation` error),
before anything is created. An unqualified target takes
`Config::target_schema`, which an attach already refuses to be either
([instance-identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)).

**If it appears after define:** A target schema that gains a `trellis_instance`
table after define (another instance attached to it) pauses nothing, and the
transform keeps running. `RESUME` re-validates and refuses with the same error,
leaving the transform paused.

**Remedy:** Put the target in an ordinary schema (`TRANSFORM reporting.t`).
After define, `DROP TRANSFORM` it (paused) and define it again in an ordinary
schema: removing the other instance's catalog from the schema would drop the
target with it.

### Partitioned sources

A declaratively partitioned table (`RANGE`, `LIST` or `HASH`, at any depth, with
or without a `DEFAULT`) is a source on PostgreSQL 15 or later, with up to 1,000
leaves: the `FROM` of an aggregate or either table of a `JOIN`. A target is always a plain table. Define accepts it
when the group key holds every partition-key column at every level: an
aggregate's `GROUP BY`, or a `JOIN` table's `GROUP BY` and `ON` columns from
that table. Each group then lives in exactly one
partition, so a change to the tree touches only the groups that route to one
partition, and the catalog says which those are. A fact table partitioned on
the column it joins by needs nothing more in its `GROUP BY`, while one
partitioned on its own id is refused. Everything else is refused at define or
pauses a definition later, as the table above lists
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
change touched, which recomputes the groups that route to it. A `DEFAULT`
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
