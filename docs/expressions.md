# Expressions

What a transform's calculated fields may contain: the names they read, their
operators, functions and literals, and what define refuses in them. Keys,
sources and targets are in [Defining Transforms](transforms.md).

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
table. The names Trellis refuses (over 63 bytes, an unquoted reserved word, a
few quoted forms) are in [Refusals](#refusals). A name that finds nothing while
one differing from it only in case exists is refused naming the quoted
spelling: `DROP TRANSFORM OrderTotals` beside a target created as
`"OrderTotals"` refuses with `did you mean "OrderTotals"?`, and drops nothing.
Rendered text, such as the definition `ALTER TRANSFORM` stores, quotes every
name that needs it.

## Calculated Fields

A calculated field's formula may reference:

* Source columns feeding the calculated row, following the shape its granularity
  requires (see [Granularity](transforms.md#granularity)).
* Other calculated columns on the same target table, by name
  ([chaining](transforms.md#chaining-and-cycle-detection)).

A bare name means a field when no source column has that name, and the source
column when no field does. A name that is both a source column and a field
whose expression is something else is refused ([Refusals](#refusals)). Two
reads of such a name are not ambiguous:

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
A second table is read with a `JOIN`. A field has no qualified form: to read a
field a source column shadows, rename the field.

### Comparison and logic

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
support](type-support.md#comparison-and-logic)). A comparison, `IS NULL` or
logic field is a `boolean` column, and another field may read it.

Binding, loosest first: `OR`, `AND`, `NOT`, `IS ...`, the comparisons, `+` and
`-`, `*` `/` `%`, prefix `-`; so `NOT a = b` is `NOT (a = b)` and
`a = b IS NULL` is `(a = b) IS NULL`. `NOT` and `CASE` are keywords wherever
an operand follows (a `-` begins one: `NOT -x > 0` is `NOT ((-x) > 0)`, as in
Postgres). Postgres promises no evaluation order inside `AND` and `OR`, so
`y <> 0 AND x / y > 1` can still divide by zero; guard a division with
`CASE WHEN y <> 0 THEN x / y END` or `x / NULLIF(y, 0)`. A failure inside a
`CASE` branch is raised only for the rows that take the branch, except a
constant Postgres folds while planning (a `2147483647 + 1` in a branch fails
every row). In an aggregate, `COUNT(CASE WHEN c THEN 1 END)` and
`SUM(CASE WHEN c THEN qty ELSE 0 END)` over an integer define, and so does
`SUM(CASE WHEN c THEN amount ELSE 0 END)` over a `numeric(10,2)` column, whose
sum shows the scale Postgres shows.

### Arithmetic

A formula's arithmetic is Postgres's: `+`, `-`, `*`, `/`, `%`, prefix `-`,
`ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNC` and the `CAST` of an integer (result
types in [type support](type-support.md#arithmetic)). `*`, `/` and `%` bind
tighter than `+` and `-`, prefix `-` tighter than all of them, and operators of
one tier group left to right: `a - b * c` is `a - (b * c)`, `-a * b` is
`(-a) * b`, `a - b - c` is `(a - b) - c`. A minus before a number literal is
one negative literal, typed by its value as Postgres types it, so
`-2147483648` is an `integer`.

A value an operation cannot compute for one row (an overflow, a division by
zero) raises `22003` or `22012` for that row's record, which is quarantined
and charged to the field ([ADR-0003](decisions/0003-quarantine-storage-and-api.md)).
Define doesn't evaluate constant expressions as Postgres's planner does, so a
constant Postgres fails while planning (`2147483647 + 1`,
`DATE '2024-01-01' + 2147483647`, a `COALESCE(real_col, ...)` whose constant
is past `real`'s range) defines, and every row fails. A 1-1 applying changes
then pauses the field's column and stays `live`
([a paused column](operations.md#when-a-definition-needs-you)); a 1-1 or
`JOIN` build, and a `JOIN` applying changes, quarantine the definition; an
aggregate's build fails whole and pauses it.

### Dates and times

`date + n`, `n + date` and `date - n` add or subtract a count of days (a
`smallint` or `integer`) and give a `date`; `date - date` is the days between,
an `integer`. `DATE_TRUNC('<unit>', ts)` truncates a `timestamp` (`week` to
Monday); `EXTRACT(<field> FROM x)` reads a part of a `date` or `timestamp` as
an unconstrained `numeric`, which an aggregate counts, sums or averages;
`tz AT TIME ZONE 'UTC'` turns a `timestamptz` into the `timestamp` it is in
UTC, and `CAST(d AS timestamp)` turns a `date` into midnight. `AT TIME ZONE`
binds tighter than `*` and looser than prefix `-`. A duration is
`EXTRACT(EPOCH FROM b) - EXTRACT(EPOCH FROM a)`, in exact `numeric` seconds;
there is no `interval`. `infinity` and `-infinity` follow Postgres
(`infinity + 1` is `infinity`, `EXTRACT(year FROM 'infinity'::date)` is
`Infinity`, `EXTRACT(day FROM ...)` of it is `NULL`), and a row whose date
leaves the range (`date + n` past 5874897-12-31, two dates one of which is
infinite, a date past 294276-12-31 cast to `timestamp`) raises `22008` for
that row's record, which is quarantined; a constant that does fails every row.

A bucket for an aggregate is computed in a 1-1 transform and grouped by in
another (`DATE_TRUNC('month', ts) AS bucket`, then `GROUP BY bucket`), since a
`GROUP BY` key is a column. `AT` is a keyword only after an operand and before
`TIME ZONE`, and `EXTRACT` only before `(`, so columns of those names (and
`year`, `time`, `zone`) are still columns ([type
support](type-support.md#dates-and-times)).

### Immutability

Formulas may only use **immutable** functions and operators — those whose output
depends solely on their inputs. Anything depending on database state outside the
row (current time, timezone, collation, session settings, random values) is
disallowed: incremental maintenance requires that re-evaluating a formula on
unchanged inputs always yields the same result.

## Refusals

The columns are those of [transforms' table](transforms.md#refusals); "After
define" names one of [its classes](transforms.md#after-define). A row links to
an entry under [Details](#details) where it needs more room.

| Condition | Error | After define | Remedy |
|---|---|---|---|
| **Constants** | | | |
| [A literal zero divisor](#a-literal-zero-divisor) | `ConstantDivisorZero` | [Text](transforms.md#text) | A nonzero divisor or a column; `x / NULLIF(col, 0)` |
| [A typed literal not in its type's canonical spelling](#a-typed-literal-not-in-its-types-canonical-spelling) | `InvalidTypedLiteral` | [Text](transforms.md#text) | The spelling Postgres prints: `DATE '2024-01-05'` |
| **Comparisons** | | | |
| [A float compared with an exact numeric](#a-float-compared-with-an-exact-numeric) | `MixedFloatExactComparison` | [Retype](transforms.md#retype) | Compare two floats or two exact values |
| A comparison between two different temporal types (`date` and `timestamp`, `timestamp` and `timestamptz`, `time` and `timetz`): a conversion to or from `timestamptz` reads the session time zone | `MixedTemporalComparison` | [Retype](transforms.md#retype) | Cast one side so both are one type |
| `<`, `<=`, `>`, `>=` over `text`, whose order depends on the collation. `=`, `<>`, `!=` and `IS [NOT] DISTINCT FROM` are admitted | `TextOrderingComparison` | [Retype](transforms.md#retype) | Order by a non-text value, or by `CHAR_LENGTH` |
| A comparison or `NULLIF` between types no family covers: `text` with a number, `boolean` with a number, `uuid` with `text`, and every type outside the families (`bytea`, `jsonb`, `interval`, enums, `inet`, …) | `ComparisonTypeMismatch` | [Retype](transforms.md#retype) | Compare values of one family; `IS NULL` takes any type |
| Chained comparisons (`a < b < c`, `a = b = c`, `IS DISTINCT FROM` followed by another `IS` form), which Postgres reads as a syntax error | `NonAssociativeComparison` | [Text](transforms.md#text) | Join them with `AND`, or add parentheses |
| **Grammar forms and typing** | | | |
| `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN`, and the simple `CASE <operand> WHEN <value> THEN …` | `UnsupportedIsForm`, `SimpleCaseUnsupported` | [Text](transforms.md#text) | `x IS NOT DISTINCT FROM TRUE` (`IS DISTINCT FROM` for the `NOT` forms), `x IS NULL`; `CASE WHEN <operand> = <value> THEN …` |
| [An untyped `NULL` literal](#an-untyped-null-literal) | `UntypedNull` | [Text](transforms.md#text) | `CAST(NULL AS <type>)` |
| A `CASE` condition that isn't `boolean`, or `CASE` results that don't unify as `COALESCE`'s do (`THEN 1 ELSE 'x'`). Results unify to a common type: two integers to the wider, an exact numeric with a float to the float, otherwise `numeric` | `CaseConditionNotBoolean`, `CaseResultTypeMismatch` | [Retype](transforms.md#retype) | Make the results one type, with `CAST` |
| `NULLIF(a, b)` where Postgres casts `a` to compare it: an integer `a` with a `numeric` `b`, or an exact `a` with a float `b` | `NullIfFirstArgumentCast` | [Retype](transforms.md#retype) | `NULLIF(CAST(a AS numeric), b)`, or the float first |
| **Expression size** | | | |
| An expression nested more than 100 levels deep (operands chained by operators, or nested parentheses or calls); a flat list of many terms is not deep | `ExpressionTooDeep` | [Text](transforms.md#text) | Split it into calculated fields |
| Calculated fields that read each other so often or so deeply that inlining them passes 100 levels or more than a build can render | `FieldExpansionTooLarge` | [Text](transforms.md#text) | Shorten the chain, or read source columns in the later fields |
| An expression whose fields inline to more than 100,000 nodes (each `WHEN x = 1 THEN 1` branch is four) | `FieldExpansionTooLarge` | [Text](transforms.md#text) | Shorten it, or split it into calculated fields |
| **Arithmetic, functions and casts** | | | |
| `/` between two integers (`qty / 2`): Postgres truncates the quotient. `numeric` and float quotients are admitted | `IntegerDivision` | [Retype](transforms.md#retype) | `CAST(qty AS numeric) / 2` |
| `%` with a float operand: Postgres has no float modulo. `%` takes integers and `numeric` | `FloatModulo` | [Retype](transforms.md#retype) | Use `numeric` operands |
| [An argument `ABS`, `CEIL`, `FLOOR`, `ROUND` or `TRUNC` does not admit](#an-argument-abs-ceil-floor-round-or-trunc-does-not-admit) | `NumericFunctionArgument`, `ScaleNotIntegerLiteral`, `FunctionArgTypeMismatch` | [Retype](transforms.md#retype) | A `numeric` value; a literal scale |
| [A `CAST` that is not an exact conversion](#a-cast-that-is-not-an-exact-conversion) | `UnsupportedCast`, `TemporalOperand`, `UnsupportedCastTarget` | [Retype](transforms.md#retype) | Widen the column, or compute in `numeric`; `tz AT TIME ZONE 'UTC'` |
| A `REGEXP_COUNT` pattern that isn't a string literal, or that Postgres rejects (`(?P<n>a)`, `a++`, `\pL`); define, `ALTER TRANSFORM` and `RESUME` probe it with `regexp_count('', pattern)`. Back-references, lookahead and `\m`/`\M` are admitted | `NonLiteralRegexPattern`, `InvalidRegexPattern` (with the server's message) | [Text](transforms.md#text); a Postgres upgrade that changes the verdict is [Per row](transforms.md#per-row) | Write a pattern Postgres's `regexp_count` accepts |
| **Dates and times** | | | |
| [Date and time arithmetic](#date-and-time-arithmetic) | `TemporalOperand` | [Retype](transforms.md#retype) | An `integer` day count; `EXTRACT(EPOCH FROM …)` differences |
| `DATE_TRUNC` of a `date` or `timestamptz`, `EXTRACT` of any type but `date` and `timestamp`, and `date_part` (an unsupported function) | `TemporalOperand` | [Retype](transforms.md#retype) | `DATE_TRUNC('month', CAST(d AS timestamp))`; `… tz AT TIME ZONE 'UTC'`; `EXTRACT` for `date_part` |
| [A `DATE_TRUNC` unit or `EXTRACT` field that is not admitted](#a-date_trunc-unit-or-extract-field-that-is-not-admitted) | `TemporalLiteral` | [Text](transforms.md#text) | A listed spelling |
| `AT TIME ZONE` with a zone other than the literal `'UTC'`, or over anything but a `timestamptz`: zone offsets change with time zone database releases | `TemporalLiteral` (the zone), `TemporalOperand` (the operand) | [Retype](transforms.md#retype) | Convert in the source; `CAST(d AS timestamp)` for a date |
| **Field, column and table names** | | | |
| A field named after a 1-1 source key column or a 1-1 `JOIN`'s `a` key column (`id AS id`; `id AS order_id` is fine), or a field or `GROUP BY` column whose name starts with `__` | `FieldNamedAfterPrimaryKey`, `ReservedFieldName`, `ReservedGroupByColumnName` | [Text](transforms.md#text) | Rename the field |
| An aggregate whose argument names another aggregate field no source column is named after (`SUM(val) AS total, SUM(total)`) | `AggregateOfAggregateField` | [Text](transforms.md#text) | Aggregate the source column |
| A bare name that is both a source column and a field whose expression isn't just that column, read by another field (`(grp + 1) AS val, SUM(val) AS total`; see [Calculated Fields](#calculated-fields)). A 1-1 transform refuses such a field whether or not anything reads it | `AmbiguousColumnReference`; in a 1-1 transform, `CalculatedFieldShadowsSourceColumn` | [Resume only](transforms.md#resume-only): a source column added under a field's name | Write `src.val` for the column, or rename the field |
| [An unquoted reserved word as a name](#an-unquoted-reserved-word-as-a-name) | `ReservedWord` | [Retype](transforms.md#retype): renaming a read column is a rename | Quote it (`"order"`), or qualify it (`orders.order`) |
| An unquoted target, `FROM` or `JOIN` table name that Postgres reserves (`FROM user`, `FROM order`); `public.order` is fine | `ReservedTableName` | [Text](transforms.md#text) | Quote it as the catalog spells it (`FROM "order"`) |
| A name over 63 bytes after folding and unescaping, in any position: Postgres would truncate it | `NameTooLong` | [Text](transforms.md#text); a definition 0.1.x stored with a longer `AS` label keeps running, its `ALTER TRANSFORM` refuses, and the upgrade that writes a `KEY (…)` into it pauses it | 63 bytes or fewer; drop and define such a stored definition again |
| [An unsupported quoted name](#an-unsupported-quoted-name) | `ZeroLengthQuotedIdentifier`, `UnicodeEscapeIdentifier`, `UnsupportedQuotedIdentifier` | [Text](transforms.md#text) | ASCII names; function and type names unquoted |
| A qualified name whose table isn't the `FROM` table (`product.cost`), or whose schema isn't its schema; a `JOIN`'s are in [transforms](transforms.md#an-unsupported-join-shape) | `QualifiedColumnNotFromSource`, `GroupByKeyNotFromSource` | [Text](transforms.md#text) | Name the `FROM` table, or read the other table with a `JOIN` |

### Details

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
both operand types. A bare number literal against a float is admitted
([type support](type-support.md#numeric-and-floating-point)).

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

What each admits is in [type support](type-support.md#arithmetic). These are
refused:

- An integer argument to any but `ABS`: it is already whole, and Postgres would cast it to
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
its own rules.

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
produces or consumes one.

`DATE_TRUNC('month', d)` resolves to the `timestamptz` overload, which is
`STABLE`. Both `DATE_TRUNC` and `EXTRACT` read the session `TimeZone` on a
`timestamptz`, and `date_part` returns `double precision`.

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
