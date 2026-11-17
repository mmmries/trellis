# Errors

Every error message starts with its code (`T0042: …`). Find the code here:
each entry says what it means and what to do. A code is never reused or
renumbered ([ADR-0008](decisions/0008-public-api-design.md#3-stable-ffi-safe-error-representation));
the coarse category a host branches on is the error's `code()`
([embedding](embedding.md#the-rust-api)).

The refusal tables in [transforms](transforms.md#refusals) and
[expressions](expressions.md#refusals) list what a definition may not do and
what happens when the condition appears after define. A refusal of a stored
definition, at `RESUME`, `ALTER TRANSFORM` or an upgrade, reports the same
code as at define.

## Definition text

The text of a statement doesn't parse, or uses a form the grammar
([expressions](expressions.md)) leaves out.

### T0001 UnexpectedChar

A character no token starts with, or an operator character no listed
operator uses. Remove it, or use a [listed operator](expressions.md#calculated-fields).

### T0002 UnterminatedString

A `'` string literal never closes. Close it; write `''` for a quote inside.

### T0003 UnexpectedToken

The parser expected something else; the message says what. Fix the text at
the position it names.

### T0004 UnexpectedEof

The text ends before the statement does; the message says what was
expected. Complete the statement.

### T0005 UnsupportedKeySpace

A construct the statement's shape doesn't take, such as a `JOIN` form outside
the [admitted shapes](transforms.md#an-unsupported-join-shape). Rewrite it in
an admitted shape; for `RIGHT JOIN`, swap the tables and use `LEFT JOIN`.

### T0006 UnsupportedOperator

An operator the [lists](expressions.md#calculated-fields) don't name
(`a || b`, `a ^ b`). Use a listed operator.

### T0007 UnsupportedFunction

A function the [lists](expressions.md#calculated-fields) don't name,
`date_part` included. Use a listed function; `EXTRACT` for `date_part`.

### T0008 FunctionArityMismatch

A listed function called with the wrong number of arguments. Pass the number
the message names.

### T0009 NonImmutableConstruct

A function that isn't [immutable](expressions.md#immutability) (`now()`,
`random()`): its value changes without a source row changing. Compute it in
the application, or store it in a source column.

### T0010 UnsupportedPredicate

A `WHERE` other than `TRUE`: partial data is not supported
([#804](https://github.com/salesforce-misc/trellis/issues/804)). There is no
workaround in a definition; filter when reading the target.

### T0011 AtLeastOneArgumentRequired

`COALESCE()` with no arguments. Pass at least one.

### T0012 TooManyQualifiedNameParts

A table name of more than two parts (`a.b.c`). Write `<schema>.<table>`.

### T0013 TooManyColumnNameParts

A column name of more than three parts. Write at most
`<schema>.<table>.<column>`.

### T0014 GroupByKeyNotFromSource

A `GROUP BY` key qualified with a table other than the `FROM` table. Name the
`FROM` table, or read the other table with a `JOIN`.

### T0015 TableAlias

A table alias the grammar refuses: a reserved word, the other `JOIN` table's
name or alias, or any alias on a single-table definition. Drop the alias, or
choose another.

### T0016 AliasedTableName

A column qualified with the name of a `JOIN` table that has an alias. As in
Postgres, qualify it with the alias.

### T0017 UnsupportedCastTarget

A `CAST` to a type the grammar doesn't list, or to a type with a modifier
(`numeric(10,2)`). Cast to a [listed type](expressions.md#a-cast-that-is-not-an-exact-conversion)
without a modifier.

### T0018 PrefixMinusChainTooLong

More than 100 prefix `-` in a row. Shorten the run.

### T0019 ReservedWord

A word Postgres reserves (`order`, `user`, `end`) used as a bare name.
Quote it (`"order"`), or qualify it (`orders.order`).

### T0020 SplitOperator

A two-character comparison split by whitespace (`a < = b`), a syntax error in
Postgres too. Write it without the space.

### T0021 DoubleMinus

`--`, which starts a comment in SQL. Write `- -x`.

### T0022 NumericTrailingJunk

A number directly followed by a letter (`1AND`, `12abc`), which Postgres
refuses. Separate them with a space.

### T0023 UnsupportedNumericForm

A number with an exponent (`1e3`), a `0x`/`0o`/`0b` prefix or `_`
separators. Write plain digits, with an optional fraction.

### T0024 NonAssociativeComparison

Chained comparisons (`a < b < c`), a syntax error in Postgres. Join them with
`AND`, or add parentheses.

### T0025 UnsupportedIsForm

`IS [NOT] TRUE`, `IS [NOT] FALSE` or `IS [NOT] UNKNOWN`. Write
`x IS NOT DISTINCT FROM TRUE` (`IS DISTINCT FROM` for the `NOT` forms), or
`x IS NULL`.

### T0026 NotChainTooLong

More than 100 prefix `NOT` in a row. Shorten the run.

### T0027 SimpleCaseUnsupported

The simple `CASE <operand> WHEN <value> THEN …`. Write
`CASE WHEN <operand> = <value> THEN …`.

### T0029 UnsupportedCastOperator

`<expr>::<type>`. Write `CAST(<expr> AS <type>)`, or a typed literal
(`DATE '2024-01-05'`).

### T0030 ExpressionTooDeep

An expression nested more than 100 levels deep. Split it into calculated
fields.

### T0031 MalformedDefinitionAddress

A `PAUSE`, `RESUME` or `DROP` address of the wrong shape, such as
`<transform>.<column>` under `DROP TRANSFORM`. Write the shape the message
names.

### T0032 UnterminatedQuotedIdentifier

A `"` quoted name never closes. Close it; write `""` for a quote inside.

### T0033 ZeroLengthQuotedIdentifier

`""`, a zero-length name, which Postgres refuses. Write a name.

### T0034 UnicodeEscapeIdentifier

A `U&"…"` name. Write the name in ASCII.

### T0035 UnsupportedQuotedIdentifier

A quoted name the grammar refuses though Postgres takes it
([details](expressions.md#an-unsupported-quoted-name)). Rename the table or
column, or write the name in ASCII characters; leave function and type names
unquoted.

### T0036 NameTooLong

A name over Postgres's 63 bytes, which Postgres would truncate. Shorten it. A
stored definition with such a name keeps running, but its `ALTER TRANSFORM`
refuses: drop it and define it again with a shorter name.

### T0037 ReservedTableName

An unquoted table name Postgres reserves (`FROM user`). Quote it as the
catalog spells it (`FROM "user"`), or qualify it (`public.user`).

### T0038 GrammarVersionTooOld

A stored definition was written under a grammar older than this release reads
([grammar versions](transforms.md#grammar-versions)). `DROP TRANSFORM` and
define it again in the current grammar.

### T0039 GrammarVersionTooNew

A stored definition was written under a grammar newer than this release
reads: a rollback, or an older process beside a newer one. Run the newer
release ([upgrading](upgrading.md#rolling-back)).

## Definition checks

The text parses, but what it computes isn't one Trellis maintains exactly.
Each is a row of a [refusal table](expressions.md#refusals).

### T0028 UnsupportedLiteralType

A typed literal of a type with no admitted literal. Write a literal of a
[listed type](type-support.md#typed-literals-and-casts), or `CAST` one.

### T0040 DuplicateFieldName

Two fields with one name. Rename one.

### T0041 ReservedFieldName

A field name starting with `__`, which Trellis keeps for the columns it adds.
Rename the field.

### T0042 ReservedGroupByColumnName

A `GROUP BY` key whose target column name starts with `__`. Group by another
column, or join a 1-1 target that renames it.

### T0043 FieldNamedAfterPrimaryKey

A 1-1 field named after a source key column (`id AS id`): the target carries
every key column already. Drop the field, or give it another name
(`id AS order_id`).

### T0044 UnresolvedColumn

A name that is neither a source column nor a field. Fix the name; the
message lists what is there.

### T0045 Cycle

Fields that read each other in a cycle. Break the cycle.

### T0046 FieldExpansionTooLarge

Fields that read each other so often or so deeply that inlining them passes
100 levels or 100,000 nodes. Shorten the chain, or read source columns in the
later fields.

### T0047 TableCycle

The target would feed its own source through a
[chain](transforms.md#chaining-and-cycle-detection). Write to another target.

### T0048 TargetEqualsSource

The target is the source table. Write to another target.

### T0050 JoinSelf

A table joined to itself, even under two aliases: one change would feed both
sides. Not supported.

### T0051 JoinTooManyTables

An aggregate `JOIN` of more than two tables. Join two, or chain transforms.

### T0052 JoinUnknownTable

A column qualified with a table the `JOIN` doesn't read. Qualify it with the
`FROM` or joined table.

### T0053 JoinUnqualifiedColumn

An unqualified column in a `JOIN`. Qualify it with its table.

### T0054 JoinUnresolvedColumn

A qualified column its table doesn't have. Fix the name.

### T0055 JoinOnSameSide

An `ON` term comparing two columns of one table. Compare a column of each
side.

### T0056 JoinMixedSideArgument

An aggregate argument reading both tables (`SUM(a.x + b.y)`). Read one
table per argument.

### T0057 JoinOnRepeatedPair

An `ON` term repeating an earlier one. Remove it.

### T0058 JoinOnRepeatedColumn

A column in two `ON` terms, paired with different columns. Name each column
once.

### T0059 IndexKeyTooWide

A key over more than 32 columns (31 for a 1-1 `JOIN`'s `ON`), Postgres's
index limit. Group by or join on fewer columns.

### T0060 TableTooWide

A table Trellis would create of more than 1,600 columns
([details](transforms.md#a-table-of-more-than-1600-columns)). Use fewer
fields or distinct aggregate arguments.

### T0061 JoinKeyTypeMismatch

`ON` columns of different types or collations. Alter one column to match the
other; after define, then `RESUME`.

### T0062 JoinUnsupportedKeyType

An `ON` column of a type off the [join-key list](type-support.md#matrix).
Join on a listed type; after define, retype the column, then `RESUME`.

### T0063 JoinNondeterministicCollation

An `ON` column with a nondeterministic collation, whose `=` Trellis can't
reproduce. Use a deterministic collation ([collation](type-support.md#collation)).

### T0064 JoinFieldShape

A `JOIN` field that is neither a `GROUP BY` key nor an admitted aggregate of
one column ([shapes](transforms.md#an-unsupported-join-shape)). Rewrite it in
an admitted shape.

### T0065 OneToOneJoinAggregate

An aggregate in a 1-1 `JOIN`. Add a `GROUP BY`, or aggregate in a separate
transform.

### T0066 OneToOneJoinKeyNotUnique

The joined table's `ON` columns aren't unique, so a row could match several
([1-1 `JOIN`](transforms.md#a-1-1-join)). Join on a unique key, or create a
unique index over the `ON` columns.

### T0067 OneToOneJoinUnsupported

A construct a 1-1 `JOIN` doesn't take; the message names the fix.

### T0068 StarJoinTooManyTables

A 1-1 `JOIN` of more than 5 tables
([star](transforms.md#1-1-join-of-more-than-two-tables-star)). Join at most 5,
or chain transforms.

### T0069 JoinOnReadsAnotherJoinedTable

An `ON` reading another joined table: each joins to the `FROM` table. Join
each table to the `FROM` table.

### T0070 DeferredAggregate

An aggregate Trellis doesn't keep exact yet: `MIN`/`MAX` in a `JOIN`,
`bool_*`, `bit_*`, float or `interval` `SUM`/`AVG`, `array_agg`, `string_agg`,
`jsonb_agg` ([not supported](type-support.md#aggregates-that-are-not-supported)).
Take `MIN`/`MAX` in a plain aggregate over one table, and `SUM`/`AVG` over an
integer or `numeric` column.

### T0071 UnsupportedAggregateArgument

`SUM` or `AVG` over a type other than an integer or `numeric`. Aggregate an
integer or `numeric` column.

### T0072 UnsupportedExtremeArgument

`MIN` or `MAX` over a type with no order or with a collation
([`MIN` and `MAX`](type-support.md#min-and-max)). Aggregate a number, date or
time column.

### T0073 ExtremeOfExpression

`MIN` or `MAX` of an expression. Aggregate a plain source column; compute
the expression in a 1-1 transform first.

### T0074 ChainedExtreme

`MIN` or `MAX` of another transform's target, which has no index for its
probe. Aggregate the source table instead.

### T0075 ExtremesWithoutSourceIndex

`WITHOUT SOURCE INDEX` on a definition with `MIN` or `MAX`, whose probe
needs an index. Drop `WITHOUT SOURCE INDEX` and create the indexes the
message names.

### T0076 ReservedTargetSuffix

A target name ending in `__ledger`, a name Trellis keeps for a target's
ledger. Choose another name.

### T0077 TargetNameTooLong

A target name over 55 bytes: its ledger's name would pass 63. Choose a
shorter name.

### T0078 TypeMismatch

An operand type an operator doesn't take (`text_col + 1`; `+` never
concatenates). `CAST` it, or use an operand of the operator's type.

### T0079 MixedFloatExactComparison

A float compared with an exact numeric
([details](expressions.md#a-float-compared-with-an-exact-numeric)). Compare
two floats or two exact values.

### T0080 MixedTemporalComparison

Two different temporal types compared, a comparison that can read the session
time zone. `CAST` one side so both are one type.

### T0081 TextOrderingComparison

`<`, `<=`, `>`, `>=` over `text`, whose order depends on the collation. Order
by a non-text value, or by `CHAR_LENGTH`.

### T0082 ComparisonTypeMismatch

A comparison or `NULLIF` between types no family covers (`text` with a
number). Compare values of one family; `IS NULL` takes any type.

### T0083 NullIfFirstArgumentCast

`NULLIF(a, b)` where Postgres casts `a` to compare it. Write
`NULLIF(CAST(a AS numeric), b)`, or put the float first.

### T0084 CaseConditionNotBoolean

A `CASE` condition that isn't `boolean`. Compare it (`WHEN x = 1`).

### T0085 CaseResultTypeMismatch

`CASE` results that don't unify to one type. `CAST` them to one type.

### T0086 UntypedNull

A `NULL` with no type from its context
([details](expressions.md#an-untyped-null-literal)). Write
`CAST(NULL AS <type>)`.

### T0087 ConstantDivisorZero

A literal zero divisor, which fails every row
([details](expressions.md#a-literal-zero-divisor)). Divide by a nonzero
value or a column; `x / NULLIF(col, 0)`.

### T0088 IntegerDivision

`/` between two integers, which Postgres truncates. Write
`CAST(qty AS numeric) / 2`.

### T0089 FloatModulo

`%` with a float operand. Use integer or `numeric` operands.

### T0090 TemporalOperand

A date or time construct over a type it doesn't take
([details](expressions.md#date-and-time-arithmetic)); the message names the
fix. For example `DATE_TRUNC('month', CAST(d AS timestamp))` for a date, and
`tz AT TIME ZONE 'UTC'` before reading a `timestamptz` as a `timestamp`.

### T0091 TemporalLiteral

A `DATE_TRUNC` unit, `EXTRACT` field or `AT TIME ZONE` zone that isn't
admitted ([details](expressions.md#a-date_trunc-unit-or-extract-field-that-is-not-admitted)).
Use a listed unit or field, and the zone `'UTC'`; convert other zones in the
source.

### T0092 UnsupportedCast

A `CAST` that isn't an exact conversion
([details](expressions.md#a-cast-that-is-not-an-exact-conversion)). Widen
the column, or compute in `numeric`. A `timestamptz` to `timestamp` is
[T0090](#t0090-temporaloperand): write `tz AT TIME ZONE 'UTC'`.

### T0093 NumericFunctionArgument

An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` doesn't take
([details](expressions.md#an-argument-abs-ceil-floor-round-or-trunc-does-not-admit)).
Pass the integer itself, or a `numeric` value (`CAST(qty AS numeric)`).

### T0094 ScaleNotIntegerLiteral

A `ROUND` or `TRUNC` scale that isn't an integer literal. Write the scale as
a literal.

### T0095 FunctionArgTypeMismatch

A function argument of the wrong type for its position. Pass the type the
message names, with `CAST` if needed.

### T0096 NonLiteralRegexPattern

A `REGEXP_COUNT` pattern that isn't a string literal. Write the pattern as a
literal.

### T0097 InvalidTypedLiteral

A typed literal not in its type's canonical spelling
([details](expressions.md#a-typed-literal-not-in-its-types-canonical-spelling)).
Write the spelling Postgres prints: `DATE '2024-01-05'`.

### T0099 UnresolvedGroupByColumn

A `GROUP BY` column the source doesn't have. Fix the name.

### T0100 UnsupportedGroupByKeyType

A `GROUP BY` key of a type that can't be a key ([matrix](type-support.md#matrix)).
Group by a column of a key type; after define, retype it, then `RESUME`.

### T0101 GroupByKeysShareTargetColumn

Two `GROUP BY` keys with one target column name (`orders.region` and
`customers.region`); keys can't be aliased. List a key once; for a `JOIN`,
join a 1-1 target that renames one side's column.

### T0102 UngroupedColumnReference

A field reading a column that is neither a `GROUP BY` key nor inside an
aggregate. Aggregate the column, or group by it.

### T0103 AggregateOfAggregateField

An aggregate of another aggregate field (`SUM(val) AS total, SUM(total)`).
Aggregate the source column.

### T0104 AmbiguousColumnReference

A bare name that is both a source column and a field computing something
else. Write `src.val` for the column, or rename the field.

### T0105 QualifiedColumnNotFromSource

A column qualified with a table or schema other than the `FROM` table's.
Name the `FROM` table, or read the other table with a `JOIN`.

### T0106 GroupingColumnFieldMustBePassthrough

A field named after a `GROUP BY` key that isn't that key
(`GROUP BY id SELECT SUM(id) AS id`). Rename the field.

### T0107 CalculatedFieldShadowsSourceColumn

A 1-1 field named after a source column it doesn't pass through. Rename the
field. After define, a source column added under a field's name refuses
`RESUME` until one of them is renamed.

### T0108 NondeterministicKeyCollation

A key column with a nondeterministic collation, whose `=` treats distinct
strings as equal. Use a deterministic collation ([collation](type-support.md#collation)).

### T0109 NondeterministicCollationTextFunction

A column with a nondeterministic collation passed to `STRPOS` or
`REGEXP_COUNT`, which Postgres refuses for every row. Use a deterministic
collation, or a column with one.

### T0110 QualifiedSourceTableNotFound

The schema a qualified `FROM` names has no such table. Fix the name.

### T0111 QualifiedTargetTableNotFound

The schema a qualified target names doesn't exist. Create the schema, or fix
the name.

### T0112 StringAggSeparatorNotLiteral

A `string_agg` separator that isn't a string literal. Not reached while
`string_agg` itself is refused ([T0070](#t0070-deferredaggregate)).

## Tables and keys

What a definition reads or writes can't be maintained as it stands
([refusal table](transforms.md#refusals)). After define, the same condition
pauses the definition ([paused definitions](operations.md#paused-definitions));
fix it, then `RESUME`.

### T0113 NoPrimaryKey

A source table with no primary key, and no `KEY (…)` for it
([the key](transforms.md#the-key-a-transform-keys-each-table-by)). Add a
primary key, or name a unique key with `KEY (…)`. Another instance's
aggregate target has none: chain within the instance
([instance identity](instance-identity.md)), or name its group key with
`KEY (…)`.

### T0114 DeferrablePrimaryKey

A deferrable primary key, which can hold duplicates inside a transaction.
Postgres can't make a key not deferrable in place: recreate it in one
statement (`ALTER TABLE … DROP CONSTRAINT …, ADD PRIMARY KEY (…)`), or name a
non-deferrable unique key with `KEY (…)`.

### T0115 KeyOnTrellisTarget

A `KEY (…)` on another transform's target, which is keyed already. Drop the
`KEY (…)`.

### T0116 KeyNotBacked

No unique index backs a `KEY (…)` exactly
([the key](transforms.md#the-key-a-transform-keys-each-table-by)). Create a
valid, non-partial unique index over exactly those columns, `NULLS NOT
DISTINCT` if one can be `NULL`.

### T0117 KeyDiffersFromReaders

The definition keys a table differently from the transforms that already
read it. Name the same key, the one the message gives.

### T0118 RecordedKeyMissing

The index over the key Trellis recorded for a table is gone. Create a unique
index over exactly those columns again, then `RESUME`.

### T0119 NoPrimaryKeyToRecord

A `RESUME` found the table's key changed, and the table has no primary key
to take as the new one. Add a primary key, then `RESUME`
([a changed key](operations.md#pause-and-resume)).

### T0121 UnsupportedPrimaryKeyType

A source key column of a type off the [key list](type-support.md#matrix).
Key the table by a listed type, or name another key with `KEY (…)`.

### T0126 SourceTableNotFound

A table that isn't on the search path, or a source table dropped since. Fix
the name, qualify it, or create the table.

### T0127 InvalidRegexPattern

A `REGEXP_COUNT` pattern Postgres rejects; the message carries the server's
reason. Write a pattern Postgres's `regexp_count` accepts. After an upgrade
of Postgres that changes the verdict, each row fails and is
[held](operations.md#releasing-held-keys).

### T0128 TableNotAccessible

A table the role Trellis runs as can't use: no `USAGE` on its schema, or no
privilege on the table. `GRANT USAGE ON SCHEMA`, or make the role a member of
the owner ([roles](recommendations.md#one-trellis-role)).

### T0129 KeyRepairBlocked

A `RESUME`'s re-key of a 1-1 target would drop a former key column a view or
index of yours depends on ([a changed key](operations.md#pause-and-resume)).
Drop the objects it names, or `DROP TRANSFORM` and define it again.

### T0130 KeyConstraintBlocked

A `RESUME`'s re-key of a 1-1 target would replace a key constraint a foreign
key or view of yours depends on. Drop the objects it names, or `DROP
TRANSFORM` and define it again.

### T0131 KeyColumnTaken

A `RESUME`'s re-key of a 1-1 target would add a key column whose name a
field or a column of yours takes. Drop or rename your column. For a field,
restore the source's key, resume, rename the field with `ALTER TRANSFORM`,
then change the key again. Or `DROP TRANSFORM` and define it again.

### T0132 SourceKeyChanged

A 1-1 target is keyed differently from its source, and no key constraint is
left to re-key in place. `DROP TRANSFORM` and define it again.

### T0133 VirtualGeneratedColumn

A definition reads a virtual generated column, which capture can't read
([details](transforms.md#a-virtual-generated-column-a-definition-reads)).
Make it `STORED`, or write its expression in the definition where the
grammar covers it (in a 1-1 transform, under a field name no source column
has).

### T0134 SourceIndexMissing

A source lacks the index its `self_check` walk pages by
([details](transforms.md#a-source-without-the-index-its-self_check-walk-pages-by)).
Create the index the message names, or define `WITHOUT SOURCE INDEX`.

### T0135 ExtremeIndexMissing

A `MIN` or `MAX` source lacks the index its probe reads. Create the index the
message names ([recommendations](recommendations.md#index-each-minmax-argument-and-vacuum-retention-sources)).

### T0136 ExtremeSourceNotReadable

The role drains run as, the ring's owner, lacks `SELECT` on a `MIN` or `MAX`
source, which its probe reads
([row-level security](transforms.md#row-level-security-on-a-role-trellis-runs-as)).
`GRANT SELECT` on the source to that role, then `RESUME`.

### T0137 LedgerTableMissing

A 1-1 target's ledger table was dropped by hand. `DROP TRANSFORM` and define
it again.

### T0138 TargetTableMissing

A target table was dropped by hand. `DROP TRANSFORM` and define it again.

### T0139 TargetInCatalogSchema

A target in this instance's catalog schema, or in another instance's
([instance identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)).
Use an ordinary schema; after define, `DROP TRANSFORM` and define it there.

### T0140 TargetTableSuffixCollision

Two of an instance's targets with one bare name in two schemas. Choose
another name.

### T0141 TargetTableExists

A relation already holds the target's name, or its ledger's
([details](transforms.md#a-target-whose-name-is-taken)). Choose another
name, give the instance its own target schema
([instance identity](instance-identity.md)), drop or rename the relation, or
build into it with `ADOPT EXISTING TABLE`.

### T0142 TargetNameHasReaders

Other definitions read a table of the target's name as a source, or did
before it was dropped or renamed: they would read the new target. `DROP
TRANSFORM` each reader named (paused first), or choose another name.

### T0143 TargetNotAdoptable

`ADOPT EXISTING TABLE` found a table it can't build into
([details](transforms.md#a-target-whose-name-is-taken)). The message names
the first difference, checked in this order (column rows one column at a
time):

| Difference | The table | Do |
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

### T0144 SourceNotChangeKeyed

A source capture can't key: a partition, a table in an inheritance
hierarchy, or one with no primary key. Use a plain table; after a change,
`DETACH PARTITION` or `NO INHERIT`, then `RESUME`.

### T0145 PartitionedSource

A partitioned source the definition can't read as written
([partitioned sources](transforms.md#partitioned-sources)); the message names
the column or limit. Add the partition-key column to the `GROUP BY` or join on
it; partition by a plain listed column; merge partitions to 1,000 or fewer
leaves, or upgrade to PostgreSQL 15 or later; for a 1-1 transform, aggregate
over it or use a plain table. After a change, detach the sub-level, then
`RESUME`.

### T0146 RowSecurityApplies

Row-level security applies to a role Trellis reads a source or writes its
target as ([details](transforms.md#row-level-security-on-a-role-trellis-runs-as)).
Give the role `BYPASSRLS`, or run as the owner without `FORCE ROW LEVEL
SECURITY`; then `RESUME`.

### T0147 Subscribed

A logical-replication subscription replicates into a source, and capture
never sees its changes. Remove the table from the publication and `ALTER
SUBSCRIPTION … REFRESH PUBLICATION`, or drop the subscription; then `RESUME`.

## Statements on a transform

`PAUSE`, `RESUME`, `ALTER TRANSFORM`, `DROP TRANSFORM` and the other calls that
address an existing transform ([operations](operations.md#repair-tools)).

### T0148 TransformNotFound

No transform has that name. Fix the name; `trellis status` lists them.

### T0149 NameCaseMismatch

A name found nothing, but one differing only in case exists. An unquoted name
folds to lower case: quote it as the catalog spells it.

### T0150 TransformNotPaused

A `DROP TRANSFORM` of a transform that isn't paused or quarantined; the
message names its status. `PAUSE TRANSFORM` it first, or wait out its build.

### T0151 AboveRunningRelease

A `DROP TRANSFORM` of a definition whose floor is above this release, which
doesn't know everything it owns. Drop it from a release at or above its
floor ([floors](transforms.md#floors)).

### T0152 CannotRun

A statement on a definition this release can't run: its floor or grammar is
newer. Run it from a release at or above the definition's floor
([upgrading](upgrading.md)).

### T0153 DependentsBlockDrop

Other definitions read the target being dropped. Drop the ones the message
names first.

### T0154 TransformNotLive

An `ALTER TRANSFORM` of a transform that isn't `live`, or a define chaining
off one whose build hasn't finished. Retry once it is `live`; `RESUME` a
paused one first.

### T0155 AlterFieldNotFound

`ALTER TRANSFORM … ALTER <field>` of a field the transform doesn't have. Fix
the name, or `ADD` it.

### T0156 AlterFieldAlreadyExists

`ALTER TRANSFORM … ADD` of a field that exists with another expression. Use
`ALTER <field> AS <expr>` to change it.

### T0157 UnsupportedAlter

An `ALTER TRANSFORM` edit this release doesn't make
([changing a definition](transforms.md#changing-a-definition)). To change a
field's column type, `DROP` the field, then `ADD` it again. A field over a
source column retyped since define: `PAUSE` and `RESUME` the transform, then
edit it. Any other shape (an aggregate, a `JOIN`): `PAUSE`, `DROP TRANSFORM`
and define it again with the change.

### T0158 ColumnPauseLockTimeout

A define, `ALTER TRANSFORM`, `DROP TRANSFORM` or `RESUME` waited out
`lock_timeout` for a lock. Nothing changed: retry.

### T0173 ColumnNotPaused

`RESUME TRANSFORM <target>.<column>` of a column that isn't paused. Nothing
to do.

### T0174 DefinitionNotLive

A field `RESUME` while the transform isn't `live`. `RESUME TRANSFORM
<target>` first if it is paused, or wait until it is `live`.

### T0175 ColumnAwaitingCapture

A field `RESUME` while capture is still being widened to the column it reads.
Retry once [`capture_wait`](status-reference.md#capture_wait) clears.

### T0177 TransformNotPaused

A whole-transform `RESUME` of a transform that isn't paused or quarantined.
Nothing to do.

### T0178 KeyNotHeld

A release of a key the transform doesn't hold. Check
[`held_keys`](status-reference.md#held_keys) for the spelling.

### T0179 ReleaseLockTimeout

A key release waited out `lock_timeout` for the drain pages on its table.
Retry, or set a longer `lock_timeout`
([releasing held keys](operations.md#releasing-held-keys)).

### T0210 TableNotCaptured

`request_backfill` of a table no definition reads. Name a source table.

### T0211 AmbiguousSourceTable

`request_backfill` of a bare name that several source tables share. Qualify
it with its schema.

### T0218 ColumnNotFound

`PAUSE TRANSFORM <target>.<column>` of a column that isn't a field; the
message lists the fields. Fix the name. A two-part address reads as
`<transform>.<column>`, not `<schema>.<transform>`.

### T0221 TargetSelect

`target_select_sql` can't render the target's `SELECT`: the target has a
column the definition computes nothing for. Read the columns the definition
writes.

## Builds and drains

A failure every key reproduces. It halts the build, drain or staging pass and
pauses the definitions it reaches, with the code in their
[`capture_failure`](status-reference.md#capture_failure); find the reason in
[paused definitions](operations.md#paused-definitions).

### T0160 Unsupported

A build met a definition shape it can't build, which define refuses: one
edited since. `DROP TRANSFORM` and define it again.

### T0168 JoinOneToOneHalt

A 1-1 `JOIN` page can't finish: a field fails on more rows than can be held
one by one. Fix what the message names, then `RESUME`.

### T0169 HopBoundExceeded

A propagation wave ran past the hop bound through the tables named: a value
cycle across chained transforms, or a runaway wave. Break the cycle, then
`RESUME`.

### T0170 GroupBasisUnsupported

An aggregate met something the group basis doesn't handle yet; the message
names it. Fix what it names, then `RESUME`; or `DROP TRANSFORM` and define
it again.

### T0172 SourceTableDropped

A change names a source table Postgres no longer has. Its staged changes are
purged; put the table back, then `RESUME`.

### T0184 ProducerAlreadyRunning

Another process holds the staging worker's lock. Only one staging worker runs
per instance; this one stands by.

### T0185 RingFull

The staging ring has no free slot: drains are behind
([ring size](staging-and-claiming/02-the-staging-ring.md#ring-size-and-backpressure)).
Capture keeps appending; it clears as drains catch up. If it persists, check
the drain workers are live and size the ring up
([ring sizing](recommendations.md#ring-sizing)).

### T0189 ConvergenceTimeout

A wait for a watermark token didn't converge within its timeout. Poll again
([poll to `live`](embedding.md#poll-to-live-dont-wait)); check
[`drain_failure`](status-reference.md#drain_failure) if it never does.

## Self-check

A [`self_check`](operations.md#self_check) that can't run, or stopped.

### T0192 UnsupportedKeySpace

A `self_check` of a transform whose shape it doesn't audit. Nothing to do.

### T0193 InvalidScope

A `self_check` limit that isn't a positive row count. Pass a positive limit.

### T0194 UnsupportedExpr

A `self_check` of a field it can't render, or of a definition whose recorded
state is missing. Report it as a bug.

### T0195 PageTimedOut

A `self_check` page ran past its budget. Start a new `self_check`, or one
over a smaller scope.

### T0196 UnreadableJob

A `self_check` job a newer release wrote. Read it from that release, or start
a new one.

## Connecting and the handle

Starting Trellis, its handle, and the calls on it
([embedding](embedding.md)).

### T0199 InvalidTableName

A table name not in the `schema.table` shape. Qualify it.

### T0200 DottedIdentifierComponent

A schema or table name with a `.` in it, which Trellis can't tell from the
separator. Rename it.

### T0201 NoIdentityKey

A test helper found a table with no key to stage its rows by. Add a primary
key.

### T0202 HeartbeatNotUnderReclaimTtl

The heartbeat interval is more than half the reclaim TTL, so a live worker's
claims would be reclaimed. Lower the heartbeat interval, or raise the TTL.

### T0203 Spawn

The background thread didn't start. Check the process's thread limit.

### T0204 ThreadExitedBeforeReady

The background thread stopped before it was ready. The log has its panic;
report it as a bug.

### T0205 ThreadPanicked

The background thread panicked during shutdown. The log has its panic;
report it as a bug.

### T0207 MigrationsPending

A read failed because this version's migrations haven't run. A background
process of this version runs them once no other version is live; or run
`migrate` ([upgrading](upgrading.md#upgrading)).

### T0212 BlockingSpawn

The blocking handle's thread didn't start. Check the process's thread limit.

### T0213 BlockingThreadExitedBeforeReady

The blocking handle's thread stopped before it was ready. The log has its
panic; report it as a bug.

### T0214 BlockingThreadGone

The blocking handle's thread is gone. Connect a new handle; report the panic
in the log as a bug.

### T0215 CancelledByShutdown

The handle shut down while the call ran. Retry on a live handle.

### T0216 CalledFromAsyncContext

A blocking call from a thread inside a `tokio` runtime. Call the async API
there.

### T0219 CallTimeout

The call didn't finish within 30 seconds, and changed nothing
([every call returns within 30 seconds](embedding.md#every-call-returns-within-30-seconds)).
Retry; if it keeps timing out, find the lock or load it waited on.

### T0220 OtherVersionLive

Another Trellis version is live on the instance, so `migrate` and
state-changing calls change nothing until it stops
([rolling deploys](embedding.md#rolling-deploys)). Finish the deploy.

### T0222 Config

The configuration is invalid; the message names the setting. Fix it
([configuration](embedding.md#configuration-from-the-environment)).

### T0223 BuildPool

The connection pool can't be built, usually from an unparsable DSN. Fix the
DSN.

### T0224 Pool

No pooled connection was available in time. Check the database is reachable
and the pool large enough.

### T0226 Migrate

A migration failed; the message carries the server's error. Fix its cause,
then run `migrate` again.

### T0227 IncompatibleInstance

The schema belongs to another Trellis instance, or to something else
([instance identity](instance-identity.md#detecting-a-foreign-or-incompatible-schema)).
Configure another schema.

### T0229 BelowCatalogFloor

This version is below the catalog floor
([the catalog floor](upgrading.md#the-catalog-floor)). Run the version the
message names, or later.

### T0230 MigrationHistory

The catalog's migration history doesn't match this binary's: another build of
the same version, or a gap. Run the build that migrated the database.

### T0231 RestoreDefinition

`migrate` couldn't restore a definition from a
[schema dump](embedding.md#schema-dumps); the message says why. Fix the
tables it resolves to, then run `migrate` again; the definitions after it stay
queued.

### T0232 SchemaDump

A schema dump still names this instance's capture functions after the
filter, so it wouldn't load. Report it as a bug, with the dump's Postgres
version.

## Retried by Trellis

A worker lost a race or a claim. Nothing applied twice or was lost, and
Trellis retries: these reach a log, not a caller. Look further only if one
repeats without end.

### T0161 Superseded

A build's claim was superseded by a resume or a reclaim.

### T0163 ClaimLost

A drain page's claim was reclaimed before it committed.

### T0164 PageAlreadyCommitted

A drain page found it had already committed.

### T0165 VersionFenceMiss

A definition changed during a drain.

### T0166 LedgerEntryCollected

A 1-1 ledger entry was collected during a page.

### T0167 JoinKeyMoved

A 1-1 `JOIN` key moved during a page.

### T0186 SealGateBlocked

A seal waited for its predecessor's writers.

### T0187 Raced

Another worker advanced the ring first.

## Internal errors

A state Trellis's own code never produces: a bug, or a hand edit of a
Trellis-owned table. Report it as a bug with the message. Where it names a
definition, `DROP TRANSFORM` and define it again.

### T0049 JoinNeedsBothTables

A `JOIN` reached single-table validation.

### T0120 MalformedCompositeKey

A staged composite key doesn't match the table's key.

### T0122 UnroutedAggregate

An aggregate define refuses reached target DDL.

### T0125 UnknownValueType

A stored definition holds an unknown column type.

### T0171 GroupLockGap

A `JOIN` page staged groups it didn't lock.

### T0182 TruncateWithoutLsn

A staged truncate has no WAL position.

### T0183 InvalidRingSlot

The ring pointer names a slot outside the ring.

### T0188 UnfencedSealedSegment

A sealed segment has no fence: a crash between the seal's two steps. The
drain stalls rather than apply it.

## Database errors

A query or connection failed in the layer the heading names; the message
carries Postgres's error. Most are transient (a dropped connection, a
deadlock, a timeout) and Trellis retries them. A lasting one is the server's
condition: fix what Postgres's message names (a missing privilege, a full
disk, a server that is down).

### T0123 Db

[Database error](#database-errors) in table DDL.

### T0124 Db

[Database error](#database-errors) in define and the statements on a
transform.

### T0159 Db

[Database error](#database-errors) in a build.

### T0162 Db

[Database error](#database-errors) in a drain.

### T0190 Db

[Database error](#database-errors) in the staging worker.

### T0197 Db

[Database error](#database-errors) in a `self_check`.

### T0198 Db

[Database error](#database-errors) in capture.

### T0206 Db

[Database error](#database-errors) in the background client.

### T0208 Db

[Database error](#database-errors) in a call on the handle.

### T0225 Connect

[Database error](#database-errors) connecting to Postgres.

## Retired codes

A retired code is no longer reported; the condition reports another code.

| Code | Now |
|---|---|
| T0098 | [T0028](#t0028-unsupportedliteraltype) |
| T0176 | [T0148](#t0148-transformnotfound) |
| T0180 | [T0158](#t0158-columnpauselocktimeout) |
| T0181 | The refusal's own code |
| T0191 | [T0148](#t0148-transformnotfound) |
| T0209 | [T0126](#t0126-sourcetablenotfound) |
| T0217 | [T0148](#t0148-transformnotfound) |
| T0228 | [T0220](#t0220-otherversionlive) |
