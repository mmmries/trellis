# Errors

Every error message starts with its code (`T0042: …`). Find the code here:
each entry says what it means and what to do. A code is never reused or
renumbered ([ADR-0008](decisions/0008-public-api-design.md#3-stable-ffi-safe-error-representation));
the coarse category a host branches on is the error's `code()`
([embedding](embedding.md#the-rust-api)).

A refusal's entry gives its remedy and the [class](transforms.md#after-define)
of what happens when the condition appears after define. A refusal of a stored
definition, at `RESUME`, `ALTER TRANSFORM` or an upgrade, reports the same
code as at define.

## Definition text

The text of a statement doesn't parse, or uses a form the grammar
([expressions](expressions.md)) leaves out.

### T0001 UnexpectedChar

A character no token starts with, or an operator character no listed operator
uses. Remove it, or use a [listed operator](expressions.md#calculated-fields).
After define: [Text](transforms.md#text).

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
the [admitted shapes](transforms.md#an-unsupported-join-shape). Rewrite it in an
admitted shape; for `RIGHT JOIN`, swap the tables and use `LEFT JOIN`. After
define: [Text](transforms.md#text).

### T0006 UnsupportedOperator

An operator the [lists](expressions.md#calculated-fields) don't name (`a || b`,
`a ^ b`). Use a listed operator. After define: [Text](transforms.md#text).

### T0007 UnsupportedFunction

A function the [lists](expressions.md#calculated-fields) don't name,
`date_part` included. Use a listed function; `EXTRACT` for `date_part`.

After define: [Text](transforms.md#text).

### T0008 FunctionArityMismatch

A listed function called with the wrong number of arguments. Pass the number the
message names. After define: [Text](transforms.md#text).

### T0009 NonImmutableConstruct

A function that isn't [immutable](expressions.md#immutability) (`now()`,
`random()`): its value changes without a source row changing. Compute it in
the application, or store it in a source column.
After define: [Text](transforms.md#text).

### T0010 UnsupportedPredicate

A `WHERE` other than `TRUE` in a [grammar-1](transforms.md#grammar-versions)
definition, which 0.1.x wrote and which reads only `TRUE`. Drop the transform
and define it again, which writes the current grammar.

### T0011 AtLeastOneArgumentRequired

`COALESCE()` with no arguments. Pass at least one.

After define: [Text](transforms.md#text).

### T0012 TooManyQualifiedNameParts

A table name of more than two parts (`a.b.c`). Write `<schema>.<table>`. After
define: [Text](transforms.md#text).

### T0013 TooManyColumnNameParts

A column name of more than three parts. Write at most
`<schema>.<table>.<column>`.

After define: [Text](transforms.md#text).

### T0014 GroupByKeyNotFromSource

A `GROUP BY` key qualified with a table other than the `FROM` table. Name the
`FROM` table, or read the other table with a `JOIN`.

After define: [Text](transforms.md#text).

### T0015 TableAlias

A table alias the grammar refuses: a reserved word, the other `JOIN` table's
name or alias, or any alias on a single-table definition. Drop the alias, or
choose another. After define: [Text](transforms.md#text).

### T0016 AliasedTableName

A column qualified with the name of a `JOIN` table that has an alias. As in
Postgres, qualify it with the alias. After define: [Text](transforms.md#text).

### T0017 UnsupportedCastTarget

A `CAST` to a type the grammar doesn't list, or to a type with a modifier
(`numeric(10,2)`). Cast to a [listed
type](expressions.md#a-cast-that-is-not-an-exact-conversion) without a modifier.
After define: [Retype](transforms.md#retype).

### T0018 PrefixMinusChainTooLong

More than 100 prefix `-` in a row. Shorten the run. After define:
[Text](transforms.md#text).

### T0019 ReservedWord

A word Postgres reserves (`order`, `user`, `end`) used as a bare name
([details](expressions.md#an-unquoted-reserved-word-as-a-name)). Quote it
(`"order"`), or qualify it (`orders.order`). After define:
[Retype](transforms.md#retype): renaming a read column is a rename.

### T0020 SplitOperator

A two-character comparison split by whitespace (`a < = b`), a syntax error in
Postgres too. Write it without the space.

### T0021 DoubleMinus

`--`, which starts a comment in SQL. Write `- -x`.

After define: [Text](transforms.md#text).

### T0022 NumericTrailingJunk

A number directly followed by a letter (`1AND`, `12abc`), which Postgres
refuses. Separate them with a space.

### T0023 UnsupportedNumericForm

A number with an exponent (`1e3`), a `0x`/`0o`/`0b` prefix or `_` separators.
Write plain digits, with an optional fraction. After define:
[Text](transforms.md#text).

### T0024 NonAssociativeComparison

Chained comparisons (`a < b < c`), a syntax error in Postgres. Join them with
`AND`, or add parentheses.

After define: [Text](transforms.md#text).

### T0025 UnsupportedIsForm

`IS [NOT] TRUE`, `IS [NOT] FALSE` or `IS [NOT] UNKNOWN`. Write
`x IS NOT DISTINCT FROM TRUE` (`IS DISTINCT FROM` for the `NOT` forms), or
`x IS NULL`.

After define: [Text](transforms.md#text).

### T0026 NotChainTooLong

More than 100 prefix `NOT` in a row. Shorten the run. After define:
[Text](transforms.md#text).

### T0027 SimpleCaseUnsupported

The simple `CASE <operand> WHEN <value> THEN …`. Write
`CASE WHEN <operand> = <value> THEN …`.

After define: [Text](transforms.md#text).

### T0029 UnsupportedCastOperator

`<expr>::<type>`. Write `CAST(<expr> AS <type>)`, or a typed literal
(`DATE '2024-01-05'`).
After define: [Text](transforms.md#text).

### T0030 ExpressionTooDeep

An expression nested more than 100 levels deep: operands chained by operators,
or nested parentheses or calls (a flat list of many terms is not deep). Split
it into calculated fields. After define: [Text](transforms.md#text).

### T0031 MalformedDefinitionAddress

A `PAUSE`, `RESUME` or `DROP` address of the wrong shape, such as
`<transform>.<column>` under `DROP TRANSFORM`. Write the shape the message
names.

### T0032 UnterminatedQuotedIdentifier

A `"` quoted name never closes. Close it; write `""` for a quote inside.

### T0033 ZeroLengthQuotedIdentifier

`""`, a zero-length name, which Postgres refuses. Write a name.

After define: [Text](transforms.md#text).

### T0034 UnicodeEscapeIdentifier

A `U&"…"` name. Write the name in ASCII. After define:
[Text](transforms.md#text).

### T0035 UnsupportedQuotedIdentifier

A quoted name the grammar refuses though Postgres takes it
([details](expressions.md#an-unsupported-quoted-name)). Rename the table or
column, or write the name in ASCII characters; leave function and type names
unquoted. After define: [Text](transforms.md#text).

### T0036 NameTooLong

A name over Postgres's 63 bytes after folding and unescaping, which Postgres
would truncate. Shorten it. After define: [Text](transforms.md#text). A
definition 0.1.x stored with a longer `AS` label keeps running, but its `ALTER
TRANSFORM` refuses and the upgrade that writes a `KEY (…)` into it pauses it:
drop it and define it again with a shorter name.

### T0037 ReservedTableName

An unquoted table name Postgres reserves (`FROM user`). Quote it as the catalog
spells it (`FROM "user"`), or qualify it (`public.user`). After define:
[Text](transforms.md#text).

### T0038 GrammarVersionTooOld

A stored definition was written under a grammar older than this release reads
([grammar versions](transforms.md#grammar-versions)). `DROP TRANSFORM` and
define it again in the current grammar.

### T0039 GrammarVersionTooNew

A stored definition was written under a grammar newer than this release
reads: a rollback, or an older process beside a newer one. Run the newer
release ([upgrading](upgrading.md#rolling-back)).

### T0240 UnsupportedAggregateModifier

An aggregate call with `DISTINCT`, `ALL`, `ORDER BY` or `FILTER (WHERE …)`.
Postgres accepts each; Trellis supports none. For `COUNT(DISTINCT x)`, [count in
two transforms](transforms.md#counting-distinct-values). Drop `ALL`: it is the
default. For the others, move the condition or order out of the call. After
define: [Text](transforms.md#text).

## Definition checks

The text parses, but what it computes isn't one Trellis maintains exactly
(what an [expression](expressions.md) may contain).

### T0028 UnsupportedLiteralType

A typed literal of a type with no admitted literal. Write a literal of a [listed
type](type-support.md#typed-literals-and-casts), or `CAST` one. After define:
[Text](transforms.md#text).

### T0040 DuplicateFieldName

Two fields with one name. Rename one. After define: [Text](transforms.md#text).

### T0041 ReservedFieldName

A field name starting with `__`, which Trellis keeps for the columns it adds.
Rename the field. After define: [Text](transforms.md#text).

### T0042 ReservedGroupByColumnName

A `GROUP BY` key whose target column name starts with `__`. Group by another
column, or join a 1-1 target that renames it. After define:
[Text](transforms.md#text).

### T0043 FieldNamedAfterPrimaryKey

A 1-1 field named after a source key column, or a 1-1 `JOIN`'s after an `a` key
column (`id AS id`): the target carries every key column already. Drop the
field, or give it another name (`id AS order_id`). After define:
[Text](transforms.md#text).

### T0044 UnresolvedColumn

A name that is neither a source column nor a field. Fix the name; the
message lists what is there.

### T0045 Cycle

Fields that read each other in a cycle. Break the cycle. After define:
[Text](transforms.md#text).

### T0046 FieldExpansionTooLarge

Fields that read each other so often or so deeply that inlining them passes 100
levels or 100,000 nodes. Shorten the chain, or read source columns in the later
fields. After define: [Text](transforms.md#text).

### T0047 TableCycle

The target would feed its own source through a
[chain](transforms.md#chaining-and-cycle-detection). Write to another target.
After define: [Text](transforms.md#text).

### T0048 TargetEqualsSource

The target is the source table. Write to another target. After define:
[Text](transforms.md#text).

### T0050 JoinSelf

A table joined to itself, even under two aliases: one change would feed both
sides. Not supported. After define: [Text](transforms.md#text).

### T0051 JoinTooManyTables

An aggregate `JOIN` of more than two tables. Join two, or chain transforms.
After define: [Text](transforms.md#text).

### T0052 JoinUnknownTable

A column qualified with a table the `JOIN` doesn't read. Qualify it with the
`FROM` or joined table.

After define: [Text](transforms.md#text).

### T0053 JoinUnqualifiedColumn

An unqualified column in a `JOIN`. Qualify it with its table. After define:
[Text](transforms.md#text).

### T0054 JoinUnresolvedColumn

A qualified column its table doesn't have. Fix the name. After define:
[Text](transforms.md#text).

### T0055 JoinOnSameSide

An `ON` term comparing two columns of one table. Compare a column of each side.
After define: [Text](transforms.md#text).

### T0056 JoinMixedSideArgument

An aggregate argument reading both tables (`SUM(a.x + b.y)`). Read one table per
argument. After define: [Text](transforms.md#text).

### T0057 JoinOnRepeatedPair

An `ON` term repeating an earlier one. Remove it. After define:
[Text](transforms.md#text).

### T0058 JoinOnRepeatedColumn

A column in two `ON` terms, paired with different columns. Name each column
once. After define: [Text](transforms.md#text).

### T0059 IndexKeyTooWide

A key over more than 32 columns (31 for a 1-1 `JOIN`'s `ON`), Postgres's index
limit. Group by or join on fewer columns. After define:
[Text](transforms.md#text).

### T0060 TableTooWide

A table Trellis would create of more than 1,600 columns
([details](transforms.md#a-table-of-more-than-1600-columns)). Use fewer fields
or distinct aggregate arguments. After define: [Text](transforms.md#text).

### T0061 JoinKeyTypeMismatch

`ON` columns of different types or collations. Alter one column to match the
other; after define, then `RESUME`.
After define: [Retype](transforms.md#retype); a change between deterministic
collations pauses nothing, but `RESUME` refuses until they match.

### T0062 JoinUnsupportedKeyType

An `ON` column of a type off the [join-key list](type-support.md#matrix). Join
on a listed type; after define, retype the column, then `RESUME`. After define:
[Retype](transforms.md#retype).

### T0063 JoinNondeterministicCollation

An `ON` column with a nondeterministic collation, whose `=` Trellis can't
reproduce. Use a deterministic collation
([collation](type-support.md#collation)). After define:
[Retype](transforms.md#retype).

### T0064 JoinFieldShape

A `JOIN` field that is neither a `GROUP BY` key nor an admitted aggregate of one
column ([shapes](transforms.md#an-unsupported-join-shape)). Rewrite it in an
admitted shape. After define: [Text](transforms.md#text).

### T0065 OneToOneJoinAggregate

An aggregate in a 1-1 `JOIN`. Add a `GROUP BY`, or aggregate in a separate
transform. After define: [Text](transforms.md#text).

### T0066 OneToOneJoinKeyNotUnique

The joined table's `ON` columns aren't unique, so a row could match several
([1-1 `JOIN`](transforms.md#a-1-1-join)). Join on a unique key, or create a
unique index over the `ON` columns. After define: [Key](transforms.md#key),
[Retype](transforms.md#retype) or [Shape](transforms.md#shape), by what changed.

### T0067 OneToOneJoinUnsupported

A construct a 1-1 `JOIN` doesn't take; the message names the fix. After define:
[Key](transforms.md#key), [Retype](transforms.md#retype) or
[Shape](transforms.md#shape), by what changed.

### T0068 StarJoinTooManyTables

A 1-1 `JOIN` of more than 5 tables
([star](transforms.md#1-1-join-of-more-than-two-tables-star)). Join at most 5,
or chain transforms. After define: [Text](transforms.md#text).

### T0069 JoinOnReadsAnotherJoinedTable

An `ON` reading another joined table: each joins to the `FROM` table. Join each
table to the `FROM` table. After define: [Text](transforms.md#text).

### T0070 DeferredAggregate

An aggregate Trellis doesn't keep exact yet: `MIN`/`MAX` in a `JOIN`,
`bool_*`, `bit_*`, `array_agg`, `string_agg`, `jsonb_agg`
([not supported](type-support.md#aggregates-that-are-not-supported)).
Take `MIN`/`MAX` in a plain aggregate over one table.
After define: [Text](transforms.md#text).

### T0071 UnsupportedAggregateArgument

`SUM` or `AVG` over a type other than a number or an `interval`. Aggregate a
number or `interval` column.
After define: [Retype](transforms.md#retype).

### T0072 UnsupportedExtremeArgument

`MIN` or `MAX` over a type with no order or with a collation
([`MIN` and `MAX`](type-support.md#min-and-max)). Aggregate a number, date or
time column.
After define: [Index](transforms.md#index), [Retype](transforms.md#retype).

### T0073 ExtremeOfExpression

`MIN` or `MAX` of an expression. Aggregate a plain source column; compute
the expression in a 1-1 transform first.
After define: [Index](transforms.md#index), [Retype](transforms.md#retype).

### T0074 ChainedExtreme

`MIN` or `MAX` of another transform's target, which has no index for its
probe. Aggregate the source table instead.
After define: [Index](transforms.md#index), [Retype](transforms.md#retype).

### T0075 ExtremesWithoutSourceIndex

`WITHOUT SOURCE INDEX` on a definition with `MIN` or `MAX`, whose probe
needs an index. Drop `WITHOUT SOURCE INDEX` and create the indexes the
message names.
After define: [Index](transforms.md#index), [Retype](transforms.md#retype).

### T0076 ReservedTargetSuffix

A target name ending in `__ledger`, a name Trellis keeps for a target's ledger.
Choose another name. After define: [Text](transforms.md#text).

### T0077 TargetNameTooLong

A target name over 55 bytes: its ledger's name would pass 63. Choose a shorter
name. After define: [Text](transforms.md#text).

### T0078 TypeMismatch

An operand type an operator doesn't take (`text_col + 1`; `+` never
concatenates). `CAST` it, or use an operand of the operator's type. After
define: [Retype](transforms.md#retype).

### T0079 MixedFloatExactComparison

A float compared with an exact numeric
([details](expressions.md#a-float-compared-with-an-exact-numeric)). Compare two
floats or two exact values. After define: [Retype](transforms.md#retype).

### T0080 MixedTemporalComparison

Two different temporal types compared, a comparison that can read the session
time zone. `CAST` one side so both are one type. After define:
[Retype](transforms.md#retype).

### T0081 TextOrderingComparison

`<`, `<=`, `>`, `>=` over `text`, whose order depends on the collation. Order
by a non-text value, or by `CHAR_LENGTH`.
After define: [Retype](transforms.md#retype).

### T0082 ComparisonTypeMismatch

A comparison or `NULLIF` between types no family covers (`text` with a number).
Compare values of one family; `IS NULL` takes any type. After define:
[Retype](transforms.md#retype).

### T0083 NullIfFirstArgumentCast

`NULLIF(a, b)` where Postgres casts `a` to compare it. Write
`NULLIF(CAST(a AS numeric), b)`, or put the float first.

After define: [Retype](transforms.md#retype).

### T0084 CaseConditionNotBoolean

A `CASE` condition that isn't `boolean`. Compare it (`WHEN x = 1`). After
define: [Retype](transforms.md#retype).

### T0085 CaseResultTypeMismatch

`CASE` results that don't unify to one type. `CAST` them to one type.

After define: [Retype](transforms.md#retype).

### T0086 UntypedNull

A `NULL` with no type from its context
([details](expressions.md#an-untyped-null-literal)). Write
`CAST(NULL AS <type>)`.

After define: [Text](transforms.md#text); beside a column (`date_col + NULL`),
[Retype](transforms.md#retype).

### T0087 ConstantDivisorZero

A literal zero divisor, which fails every row
([details](expressions.md#a-literal-zero-divisor)). Divide by a nonzero value or
a column; `x / NULLIF(col, 0)`. After define: [Text](transforms.md#text).

### T0088 IntegerDivision

`/` between two integers, which Postgres truncates. Write
`CAST(qty AS numeric) / 2`.

After define: [Retype](transforms.md#retype).

### T0089 FloatModulo

`%` with a float operand. Use integer or `numeric` operands.

After define: [Retype](transforms.md#retype).

### T0090 TemporalOperand

A date or time construct over a type it doesn't take
([details](expressions.md#date-and-time-arithmetic)); the message names the
fix. For example `DATE_TRUNC('month', CAST(d AS timestamp))` for a date,
`tz AT TIME ZONE 'UTC'` before reading a `timestamptz` as a `timestamp`, and
an `integer` day count or `EXTRACT(EPOCH FROM …)` differences for date
arithmetic.
After define: [Retype](transforms.md#retype).

### T0091 TemporalLiteral

A `DATE_TRUNC` unit, `EXTRACT` field or `AT TIME ZONE` zone that isn't admitted
([details](expressions.md#a-date_trunc-unit-or-extract-field-that-is-not-admitted)).
Use a listed unit or field, and the zone `'UTC'`; convert other zones in the
source. After define: [Text](transforms.md#text) for a unit or field;
[Retype](transforms.md#retype) for an `AT TIME ZONE` zone.

### T0092 UnsupportedCast

A `CAST` that isn't an exact conversion
([details](expressions.md#a-cast-that-is-not-an-exact-conversion)). Widen the
column, or compute in `numeric`. A `timestamptz` to `timestamp` is
[T0090](#t0090-temporaloperand): write `tz AT TIME ZONE 'UTC'`. After define:
[Retype](transforms.md#retype).

### T0093 NumericFunctionArgument

An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` doesn't take
([details](expressions.md#an-argument-abs-ceil-floor-round-or-trunc-does-not-admit)).
Pass the integer itself, or a `numeric` value (`CAST(qty AS numeric)`). After
define: [Retype](transforms.md#retype).

### T0094 ScaleNotIntegerLiteral

A `ROUND` or `TRUNC` scale that isn't an integer literal. Write the scale as a
literal. After define: [Retype](transforms.md#retype).

### T0095 FunctionArgTypeMismatch

A function argument of the wrong type for its position. Pass the type the
message names, with `CAST` if needed. After define:
[Retype](transforms.md#retype).

### T0096 NonLiteralRegexPattern

A `REGEXP_COUNT` pattern that isn't a string literal. Write the pattern as a
literal. After define: [Text](transforms.md#text).

### T0097 InvalidTypedLiteral

A typed literal not in its type's canonical spelling
([details](expressions.md#a-typed-literal-not-in-its-types-canonical-spelling)).
Write the spelling Postgres prints: `DATE '2024-01-05'`. After define:
[Text](transforms.md#text).

### T0099 UnresolvedGroupByColumn

A `GROUP BY` column the source doesn't have. Fix the name.

### T0100 UnsupportedGroupByKeyType

A `GROUP BY` key of a type that can't be a key
([matrix](type-support.md#matrix)). Group by a column of a key type; after
define, retype it, then `RESUME`. After define: [Retype](transforms.md#retype).

### T0101 GroupByKeysShareTargetColumn

Two `GROUP BY` keys with one target column name (`orders.region` and
`customers.region`); keys can't be aliased. List a key once; for a `JOIN`,
join a 1-1 target that renames one side's column.
After define: [Text](transforms.md#text).

### T0102 UngroupedColumnReference

A field reading a column that is neither a `GROUP BY` key nor inside an
aggregate. Aggregate the column, or group by it. After define:
[Text](transforms.md#text).

### T0103 AggregateOfAggregateField

An aggregate of another aggregate field (`SUM(val) AS total, SUM(total)`).
Aggregate the source column. After define: [Text](transforms.md#text).

### T0104 AmbiguousColumnReference

A bare name that is both a source column and a field computing something else.
Write `src.val` for the column, or rename the field. After define: [Resume
only](transforms.md#resume-only): a source column added under a field's name.

### T0105 QualifiedColumnNotFromSource

A column qualified with a table or schema other than the `FROM` table's. Name
the `FROM` table, or read the other table with a `JOIN`. After define:
[Text](transforms.md#text).

### T0106 GroupingColumnFieldMustBePassthrough

A field named after a `GROUP BY` key that isn't that key (`GROUP BY id SELECT
SUM(id) AS id`). Rename the field. After define: [Text](transforms.md#text).

### T0107 CalculatedFieldShadowsSourceColumn

A 1-1 field named after a source column it doesn't pass through. Rename the
field. After define: [Resume only](transforms.md#resume-only): a source column
added under a field's name, until one of them is renamed.

### T0108 NondeterministicKeyCollation

A key column with a nondeterministic collation, whose `=` treats distinct
strings as equal. Use a deterministic collation
([collation](type-support.md#collation)). After define:
[Retype](transforms.md#retype).

### T0109 NondeterministicCollationTextFunction

A column with a nondeterministic collation passed to `STRPOS` or
`REGEXP_COUNT`, which Postgres refuses for every row, or compared as text (`=`,
`<>`, `!=`, `IS [NOT] DISTINCT FROM`, `NULLIF`), where its `=` would treat
distinct strings as equal; directly or through `COALESCE`, a `CASE` result or a
field. Use a deterministic collation, or a column with one. After define: under
`STRPOS` or `REGEXP_COUNT`, [Per row](transforms.md#per-row); on a column only
compared, nothing detects it ([known correctness
gaps](known-correctness-gaps.md#a-read-column-given-a-nondeterministic-collation)).

### T0110 QualifiedSourceTableNotFound

The schema a qualified `FROM` names has no such table. Fix the name.

### T0111 QualifiedTargetTableNotFound

The schema a qualified target names doesn't exist. Create the schema, or fix
the name.

### T0112 StringAggSeparatorNotLiteral

A `string_agg` separator that isn't a string literal. Not reached while
`string_agg` itself is refused ([T0070](#t0070-deferredaggregate)).

### T0233 ConflictingCollations

A comparison, `NULLIF`, `STRPOS` or `REGEXP_COUNT` over text columns of two
different collations, neither the database default, directly or through
`COALESCE`, a `CASE` result or a field. Postgres can't pick one (`42P22`) and
fails every row where both are not `NULL`. Give the two columns one collation
(`ALTER COLUMN … TYPE text COLLATE …`).
After define: a column re-collated later holds each key written ([releasing
held keys](operations.md#releasing-held-keys)), and `RESUME` refuses it.

### T0234 PredicateNotBoolean

A [`WHERE`](expressions.md#where) whose type isn't `boolean`. Write a
comparison: `qty > 0`. After define: [Retype](transforms.md#retype).

### T0235 AggregateInPredicate

A [`WHERE`](expressions.md#where) that calls an aggregate: it filters source
rows, not groups. Compare source columns: `qty > 0`. After define:
[Text](transforms.md#text).

### T0236 PredicateColumnNotFound

A [`WHERE`](expressions.md#where) names a column the source table doesn't
have; the message names it. A `WHERE` reads source columns, never fields.

### T0237 PredicateNotTotal

A [`WHERE` construct that can raise an error](expressions.md#where) for some
value of its operands; the message names it. Compare without it, or filter on a
stored generated column or a
[chained](transforms.md#chaining-and-cycle-detection) 1-1 transform. After
define: [Retype](transforms.md#retype).

### T0239 FilteredExtreme

`MIN` or `MAX` in a plain aggregate with a `WHERE` other than `TRUE`: an
extreme is read from a source index, which holds the rows the `WHERE` drops
too. Take the `MIN`/`MAX` of a stored generated column
`CASE WHEN <predicate> THEN <column> END` in an unfiltered transform.

After define: [Text](transforms.md#text).

### T0241 EnumLiteralNotALabel

An enum column compared with a string literal that is not one of its labels:
Postgres fails every statement that reads it. Use a label of the enum. After a
label was renamed, rename it back, or drop the transform and define it again.
After define: [Retype](transforms.md#retype).

### T0243 JoinPredicateReadsBothSides

A top-level `AND` condition of a [join aggregate](transforms.md#join-aggregate)'s
`WHERE` reads both tables (`a.x > b.y`, or `a.x > 0 OR b.y > 0`); the message
names it. Each condition filters one table's rows, so no table can apply this
one. Split it into conditions that each read one table, or compute the value
in a [chained](transforms.md#chaining-and-cycle-detection) 1-1 `JOIN` and
aggregate that.
After define: [Text](transforms.md#text).

### T0244 JoinPredicateJoinedSideUnderLeft

A `LEFT` [join aggregate](transforms.md#join-aggregate)'s `WHERE` has a
condition on the joined table; the message names it. In SQL it also drops the
`FROM` rows with no match, which the `LEFT` keeps, so the query is an `INNER`
one: write `INNER JOIN`.
After define: [Text](transforms.md#text).

### T0245 JoinPredicateReadsNoColumn

A top-level `AND` condition of a [join aggregate](transforms.md#join-aggregate)'s
`WHERE` reads no column (`WHERE FALSE`, `1 = 1`); the message names it. Each
condition filters one table's rows. Drop it, or, to keep nothing, drop the
transform.
After define: [Text](transforms.md#text).

### T0246 CharKeyPaddingFunction

A field of an aggregate passes a `char(n)` `GROUP BY` key to `OCTET_LENGTH`
as `char(n)`: directly, or through another field or a `COALESCE`, `NULLIF` or
`CASE` that Postgres types `char(n)`. The aggregate keeps the key as `text`
without its trailing blanks
([the key's type](type-support.md#the-types-of-the-columns-trellis-creates)),
so the length would differ from Postgres's. Group by a `text` column.
After define: [Retype](transforms.md#retype).

## Tables and keys

What a definition reads or writes can't be maintained as it stands
(what [sources and targets](transforms.md#supported-sources-and-targets) may be), at define or at `RESUME`. A
condition that appears after define pauses the definition
([paused definitions](operations.md#paused-definitions)); fix it, then
`RESUME`.

### T0113 NoPrimaryKey

A source table with no primary key, and no `KEY (…)` for it ([the
key](transforms.md#the-key-a-transform-keys-each-table-by)). Add a primary key,
or name a unique key with `KEY (…)`. Another instance's aggregate target has
none: chain within the instance ([instance identity](instance-identity.md)),
group over a 1-1 transform of it, or name its group key with `KEY (…)`. After
define: [Key](transforms.md#key) for a source with no primary key; an aggregate
target replacing a source is [Disruption](transforms.md#disruption), and
`RESUME` then refuses.

### T0114 DeferrablePrimaryKey

A deferrable primary key, which can hold duplicates inside a transaction.
Postgres can't make a key not deferrable in place: recreate it in one statement
(`ALTER TABLE … DROP CONSTRAINT …, ADD PRIMARY KEY (…)`), or name a
non-deferrable unique key with `KEY (…)`. After define:
[Key](transforms.md#key).

### T0115 KeyOnTrellisTarget

A `KEY (…)` on another transform's target, which is keyed already. Drop the
`KEY (…)`.

After define: [Key](transforms.md#key).

### T0116 KeyNotBacked

No unique index backs a `KEY (…)` exactly ([the
key](transforms.md#the-key-a-transform-keys-each-table-by)). Create a valid,
non-partial unique index over exactly those columns, `NULLS NOT DISTINCT` if one
can be `NULL`. After define: [Key](transforms.md#key).

### T0117 KeyDiffersFromReaders

The definition keys a table differently from the transforms that already read
it. Name the same key, the one the message gives. After define:
[Key](transforms.md#key).

### T0118 RecordedKeyMissing

The index over the key Trellis recorded for a table is gone. Create a unique
index over exactly those columns again (`NULLS NOT DISTINCT` if one can be
`NULL`), then `RESUME`.

After define: [Key](transforms.md#key).

### T0119 NoPrimaryKeyToRecord

A `RESUME` found the table's key changed, and the table has no primary key
to take as the new one. Add a primary key, then `RESUME`
([a changed key](operations.md#pause-and-resume)).

### T0121 UnsupportedPrimaryKeyType

A source key column of a type off the [key list](type-support.md#matrix). Key
the table by a listed type, or name another key with `KEY (…)`. After define:
[Key](transforms.md#key).

### T0126 SourceTableNotFound

A table that isn't on the search path, or a source table dropped since. Fix
the name, qualify it, or create the table.

### T0127 InvalidRegexPattern

A `REGEXP_COUNT` pattern Postgres rejects (`(?P<n>a)`, `a++`, `\pL`); define,
`ALTER TRANSFORM` and `RESUME` probe it with `regexp_count('', pattern)`, and
the message carries the server's reason. Back-references, lookahead and
`\m`/`\M` are admitted. Write a pattern Postgres's `regexp_count` accepts.
After define: [Text](transforms.md#text); a Postgres upgrade that changes the
verdict is [Per row](transforms.md#per-row).

### T0128 TableNotAccessible

A table the role Trellis runs as can't use: no `USAGE` on its schema, or no
privilege on the table. `GRANT USAGE ON SCHEMA`, or make the role a member of
the owner ([roles](recommendations.md#one-trellis-role)).
After define: [Shape](transforms.md#shape): a revoked table privilege or schema
`USAGE` halts the readers on the next drain, and a revoked schema `USAGE` at
the next staging pass too.

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
([details](transforms.md#a-virtual-generated-column-a-definition-reads)). Make
it `STORED`, or write its expression in the definition where the grammar covers
it (in a 1-1 transform, under a field name no source column has). After define:
[Shape](transforms.md#shape).

### T0134 SourceIndexMissing

A source lacks the index its `self_check` walk pages by
([details](transforms.md#a-source-without-the-index-its-self_check-walk-pages-by)).
Create the index the message names, or define `WITHOUT SOURCE INDEX`. After
define: [Index](transforms.md#index).

### T0135 ExtremeIndexMissing

A `MIN` or `MAX` source lacks the index its probe reads. Create the index the
message names
([recommendations](recommendations.md#index-each-minmax-argument-and-vacuum-retention-sources)).
After define: [Index](transforms.md#index), [Retype](transforms.md#retype).

### T0136 ExtremeSourceNotReadable

The role drains run as, the ring's owner, lacks `SELECT` on a `MIN` or `MAX`
source, which its probe reads
([row-level security](transforms.md#row-level-security-on-a-role-trellis-runs-as)).
`GRANT SELECT` on the source to that role, then `RESUME`.

After define: [Shape](transforms.md#shape).

### T0137 LedgerTableMissing

A 1-1 target's ledger table was dropped by hand. `DROP TRANSFORM` and define
it again.

### T0138 TargetTableMissing

A target table was dropped by hand. `DROP TRANSFORM` and define it again.

### T0139 TargetInCatalogSchema

A target in this instance's catalog schema, or in another instance's ([instance
identity](instance-identity.md#a-catalog-schema-that-belongs-to-another-instance)).
Use an ordinary schema; after define, `DROP TRANSFORM` and define it there.
After define: [Resume only](transforms.md#resume-only).

### T0140 TargetTableSuffixCollision

Two of an instance's targets with one bare name in two schemas. Choose another
name. After define: matters only when the statement runs.

### T0141 TargetTableExists

A relation already holds the target's name, or its ledger's
([details](transforms.md#a-target-whose-name-is-taken)). Choose another name,
give the instance its own target schema ([instance
identity](instance-identity.md)), drop or rename the relation, or build into it
with `ADOPT EXISTING TABLE`. After define: matters only when the statement
runs.

### T0142 TargetNameHasReaders

Other definitions read a table of the target's name as a source (including the
table `ADOPT EXISTING TABLE` would adopt), or did before it was dropped or
renamed: they would read the new target. `DROP TRANSFORM` each reader named
(paused first), or choose another name. After define: matters only when the
statement runs; later, a new reader of the name is a chain, and the target can't
be dropped while one reads it ([T0153
DependentsBlockDrop](errors.md#t0153-dependentsblockdrop)).

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

After define: matters only when the statement runs.

### T0144 SourceNotChangeKeyed

A source capture can't key: a partition, a table in an inheritance
hierarchy, or one with no primary key. Use a plain table; after a change,
`DETACH PARTITION` or `NO INHERIT`, then `RESUME`.

After define: [Shape](transforms.md#shape) (`ATTACH PARTITION`, `INHERIT`, a
child created `INHERITS` it, or a drop and recreate as one).

### T0145 PartitionedSource

A partitioned source the definition can't read as written ([partitioned
sources](transforms.md#partitioned-sources)): a group key that omits a
partition-key column at some level; a key level that is an expression, or whose
column has a type other than `smallint`, `integer`, `bigint`, `numeric`, `text`,
`varchar`, `uuid`, `date`, `timestamp`, `timestamptz` or `boolean`, a
non-default operator class or a nondeterministic collation; more than 1,000
leaves, or PostgreSQL before 15; or the `FROM` of a 1-1 transform. The message
names the column or limit. Add the partition-key column to the `GROUP BY` or
join on it; partition by a plain listed column; merge partitions to 1,000 or
fewer leaves, or upgrade to PostgreSQL 15 or later; for a 1-1 transform,
aggregate over it or use a plain table. After a change, detach the sub-level,
then `RESUME`.

After define: [Shape](transforms.md#shape) for a sub-level added later or a
table attached into the tree; the source dropped and created again partitioned
is [Disruption](transforms.md#disruption), and `RESUME` then refuses.

### T0146 RowSecurityApplies

Row-level security applies to a role Trellis reads a source or writes its target
as ([details](transforms.md#row-level-security-on-a-role-trellis-runs-as)). Give
the role `BYPASSRLS`, or run as the owner without `FORCE ROW LEVEL SECURITY`;
then `RESUME`. After define: [Shape](transforms.md#shape).

### T0147 Subscribed

A logical-replication subscription in the same database replicates into a
source, in any sync state or disabled: its apply worker fires only row-level
triggers, so capture never sees its changes. Publishing a table is fine. Remove
the table from the publication and `ALTER SUBSCRIPTION … REFRESH PUBLICATION`,
or drop the subscription; then `RESUME`. After define:
[Shape](transforms.md#shape).

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

An `ALTER TRANSFORM` of a transform that isn't `live`, or a define chaining off
one whose build hasn't finished. Retry once it is `live`; `RESUME` a paused one
first. After define: matters only when the statement runs.

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

A `PAUSE`, `RESUME`, define, `ALTER TRANSFORM` or `DROP TRANSFORM` waited out
`lock_timeout` for the column-pause lock. Retry: each step is idempotent, so a
call that committed some of its steps first finishes the rest.

### T0173 ColumnNotPaused

`RESUME TRANSFORM <target>.<column>` of a column that isn't paused, or that
names no field. Nothing to do, or fix the name.

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

`target_select_sql` was passed a target column that is neither a field nor a
key column of the definition (a column you added, say). Pass only the
definition's columns.

### T0242 OutsideDependentsBlock

Objects outside Trellis depend on the target: another table's foreign key
into it, or a view over it or over the field's column. `DROP TRANSFORM`,
`ALTER TRANSFORM … DROP` and a 1-1 `RESUME` that empties the target refuse
while they exist: Trellis never drops what it doesn't own, and never cascades.
The message names the dependents. Drop or alter them (`DROP VIEW …`,
`ALTER TABLE … DROP CONSTRAINT …`), then retry; the failed statement changed
nothing. After define: matters only when the statement runs.

## Builds and drains

A failure every key reproduces. It pauses the definitions it reaches, with
the error in their [`capture_failure`](status-reference.md#capture_failure)
or [`backfill_failure`](status-reference.md#backfill_failure); find the
reason in [paused definitions](operations.md#paused-definitions).

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

The call didn't finish within 30 seconds, and changed nothing it hadn't
committed ([every call returns within 30 seconds](embedding.md#every-call-returns-within-30-seconds)).
Retry; if it keeps timing out, find the lock or load it waited on.

### T0189 ConvergenceTimeout

`await_converged` ran out of time before the token's changes drained: the
target is behind, not broken. Call again, or allow longer
([reading your own writes](embedding.md#reading-your-own-writes)). If it never
converges, check [`held_keys`](status-reference.md#held_keys) and
[`drain_failure`](status-reference.md#drain_failure).

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

The catalog's migration history doesn't fit this binary's: a migration with
another name or checksum, a gap, newer migrations with no catalog floor, or a
version that isn't a release; the message says which. Run the release that
migrated the database.

### T0231 RestoreDefinition

`migrate` couldn't restore a definition from a
[schema dump](embedding.md#schema-dumps): its text resolves to other tables
than the dump recorded, or define refused it; the message says which. Fix
that, then run `migrate` again; the definitions after it stay queued.

### T0232 SchemaDump

A schema dump still names this instance's capture functions after the
filter, so it wouldn't load. Report it as a bug, with the dump's Postgres
version.

## Retried by Trellis

A worker lost a race or a claim, or waits on another. Nothing applied twice
or was lost, and Trellis retries: these reach a log, not a caller. Look
further only if one repeats without end.

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

### T0184 ProducerAlreadyRunning

Another process holds the staging worker's lock; this one stands by
([rolling deploys](embedding.md#rolling-deploys)).

### T0185 RingFull

The staging ring has no free slot to seal into: drains are behind
([backpressure](staging-and-claiming/02-the-staging-ring.md#ring-size-and-backpressure)).
Capture keeps appending to the active segment meanwhile. If it persists,
check that drain workers are live
([`has_live_drain_workers`](embedding.md#detecting-it-has_live_drain_workers-and-has_live_staging_worker)),
and watch free disk ([ring sizing](recommendations.md#ring-sizing)).

### T0186 SealGateBlocked

A seal waited for its predecessor's writers.

### T0187 Raced

Another worker advanced the ring first.

### T0188 UnfencedSealedSegment

A sealed segment has no fence yet: a crash between the seal's two steps. The
drain skips it until the staging worker's recovery pass fences it.

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
| T0238 | None: every key space applies a `WHERE` |
