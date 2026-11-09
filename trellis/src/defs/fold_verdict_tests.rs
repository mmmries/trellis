//! Pins define's verdict (admitted type, or refusal) on constant `CASE`,
//! `COALESCE`, `NULLIF` and arithmetic forms that mix `real`, `double
//! precision`, `numeric`, integer and `date` operands. The constant fold
//! resolves a `CASE` / `COALESCE` to a float type to evaluate the branch it
//! picks (#1802), and the refusals below are exactly what that type decides;
//! a change to how the fold learns the type (#1805) must leave every verdict
//! as it is. Each entry holds the admitted type, or the prefix of the refusal.
//! The last test pins the fold's float typing walk to inference.

use std::collections::HashMap;

use super::ast::ValueType;
use super::parse;
use super::pg_type::PgType;
use super::validate::{float_width_disagreements, infer_field_types, validate};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

fn columns() -> HashMap<String, ValueType> {
    let mut map = HashMap::new();
    for (name, ty) in [
        ("i4", ValueType::Integer(IntWidth::Int4)),
        ("i8", ValueType::Integer(IntWidth::Int8)),
        ("n", ValueType::Numeric),
        ("f4", ValueType::Float(FloatWidth::Float4)),
        ("f8", ValueType::Float(FloatWidth::Float8)),
        ("b", ValueType::Boolean),
        ("d", ValueType::Other(PgType::Date)),
    ] {
        map.insert(name.to_string(), ty);
    }
    map
}

fn verdict(text: &str) -> String {
    let def = match parse(&format!("TRANSFORM t FROM s SELECT {text} AS x")) {
        Ok(def) => def,
        Err(e) => return format!("parse error: {e}"),
    };
    match validate(&def, &columns()) {
        Err(e) => format!("refused: {e}"),
        Ok(_) => match infer_field_types(&def, &columns()) {
            Ok(types) => format!("admitted: {}", types["x"]),
            Err(e) => format!("refused (infer): {e}"),
        },
    }
}

/// (form, prefix of the verdict).
const VERDICTS: &[(&str, &str)] = &[
    (
        "1 / (COALESCE(CAST(NULL AS real) + NULL, 16777217) - 16777216)",
        "refused: calculated field 'x' divides by the constant zero",
    ),
    (
        "1 / (COALESCE(NULLIF(NULL, REAL '0'), 16777217) - 16777216)",
        "refused: calculated field 'x' divides by the constant zero",
    ),
    (
        "1 / (COALESCE(NULLIF(NULL, DOUBLE PRECISION '0'), 9007199254740993) - 9007199254740992)",
        "refused: calculated field 'x' divides by the constant zero",
    ),
    (
        "1 / (COALESCE(NULLIF(NULL, DOUBLE PRECISION '0') * 1, 9007199254740993) - 9007199254740992)",
        "refused: calculated field 'x' divides by the constant zero",
    ),
    (
        "1 / (COALESCE(CAST(NULL AS real) + NULL, 9007199254740993) - 9007199254740992)",
        "refused: calculated field 'x' divides by the constant zero",
    ),
    (
        "(((- NULL) / (NULL / 16777217)) - ((-9223372036854775808 - n) + (f4 - 16777216)))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "(((i8 + CAST(NULL AS integer)) - CASE WHEN 1 = 1 THEN -9223372036854775809 ELSE i4 END) + ((- 1) * (- NULL)))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "((COALESCE(f8, i4) - COALESCE(i4, 0.1)) * (NULLIF(CAST(NULL AS real), i8) * (NULL - DOUBLE PRECISION '9.007199254740992e+15')))",
        "refused: calculated field 'x' compares a real value with a bigint value ",
    ),
    (
        "((n + f4) % f8)",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    (
        "(- ((- (9007199254740993 / 5)) + (99999999999999999999 - NULLIF(NULL, 2147483647))))",
        "refused: calculated field 'x' divides a bigint value by a integer value;",
    ),
    (
        "(- NULL)",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "(- NULLIF(-9223372036854775808, REAL '3.4e+38'))",
        "refused: calculated field 'x': NULLIF of a bigint value and a real value",
    ),
    ("(- REAL '0')", "admitted: real"),
    ("(- f4)", "admitted: real"),
    ("(- f8)", "admitted: double precision"),
    ("(- i8)", "admitted: bigint"),
    (
        "(-9223372036854775808 * 2147483647)",
        "refused: calculated field 'x' has the constant (-9223372036854775808 * 2",
    ),
    (
        "(-9223372036854775809 - NULLIF(f8, ABS(n)))",
        "refused: calculated field 'x' compares a double precision value with a n",
    ),
    ("(0 + 9007199254740993)", "admitted: bigint"),
    (
        "(16777218 + 2147483647)",
        "refused: calculated field 'x' has the constant (16777218 + 2147483647), ",
    ),
    (
        "(2147483647 - (-9223372036854775808 * 1))",
        "refused: calculated field 'x' has the constant (2147483647 - (-922337203",
    ),
    ("(2147483648 - 16777217)", "admitted: bigint"),
    ("(5 - 9007199254740993)", "admitted: bigint"),
    ("(9007199254740993 + 0)", "admitted: bigint"),
    (
        "(ABS(NULL) + (n - 2147483648))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "(ABS(NULL) - ((0 / 9007199254740993) * 0))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "(CEIL(16777218) + (NULL * REAL '1'))",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "(CEIL(9007199254740993) - ABS(NULL))",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "(CEIL(9223372036854775807) - 16777217)",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "(NULLIF(NULLIF(-9223372036854775808, NULL), (REAL '1' - NULL)) - (- (REAL '3.4e+38' - f4)))",
        "refused: calculated field 'x' compares a bigint value with a real value ",
    ),
    (
        "(NULLIF(REAL '1.6777216e+07', n) * (2147483648 - REAL '0'))",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "(NULLIF(n, f4) - DOUBLE PRECISION '9.007199254740992e+15')",
        "refused: calculated field 'x' compares a numeric value with a real value",
    ),
    ("(REAL '0' + i4)", "admitted: double precision"),
    ("(REAL '1' * f8)", "admitted: double precision"),
    ("(f8 / 16777217)", "admitted: double precision"),
    (
        "(n % CEIL(DOUBLE PRECISION '0'))",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    (
        "(n % REAL '1.6777216e+07')",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    ("(n + 16777217)", "admitted: numeric"),
    ("(n + REAL '1')", "admitted: double precision"),
    (
        "-2147483648 - 1",
        "refused: calculated field 'x' has the constant (-2147483648 - 1), which ",
    ),
    (
        "-9223372036854775808 - 1",
        "refused: calculated field 'x' has the constant (-9223372036854775808 - 1",
    ),
    ("-9223372036854775809", "admitted: numeric"),
    ("-REAL '1.5'", "admitted: real"),
    ("0", "admitted: integer"),
    ("0.0", "admitted: numeric"),
    ("1", "admitted: integer"),
    ("1 + 2 + 3 > 4 + 5", "admitted: boolean"),
    (
        "1 + 2147483647",
        "refused: calculated field 'x' has the constant (1 + 2147483647), which i",
    ),
    ("1 + DATE '2023-02-28'", "admitted: date"),
    ("1 + d - 2", "admitted: date"),
    (
        "1 / ((((- (REAL '1.6777216e+07' - 16777218)) - ABS(NULL)) + NULLIF(CEIL(1.5), f4)))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "1 / (((-9223372036854775808 - 2147483647) + NULLIF(DOUBLE PRECISION '9.007199254740992e+15', REAL '0')))",
        "refused: calculated field 'x' has the constant (-9223372036854775808 - 2",
    ),
    (
        "1 / (((0.0 + 1) / (f8 + DOUBLE PRECISION '1e+308')) - ((f4 - 16777217) - ABS(NULL)))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "1 / (((9223372036854775807 + 5) / NULLIF(-9223372036854775809, DOUBLE PRECISION '0')) - (- COALESCE(16777218, -9223372036854775808)))",
        "refused: calculated field 'x' has the constant (9223372036854775807 + 5)",
    ),
    (
        "1 / (((CEIL((DOUBLE PRECISION '9.007199254740992e+15' - 16777217)) - (REAL '1' - NULLIF(CAST(NULL AS real), i8))) * REAL '1'))",
        "refused: calculated field 'x' compares a real value with a bigint value ",
    ),
    (
        "1 / ((- 16777217))",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / ((- CEIL(9007199254740993)))",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "1 / ((- NULL))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "1 / ((- i8))",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "1 / ((-9223372036854775808 * (2147483647 - 16777218)) - (16777217 + COALESCE(16777216, 99999999999999999999, REAL '1')))",
        "refused: calculated field 'x' has the constant (-9223372036854775808 * (",
    ),
    (
        "1 / ((0 / NULLIF(-9223372036854775808, REAL '3.4e+38')))",
        "refused: calculated field 'x': NULLIF of a bigint value and a real value",
    ),
    (
        "1 / ((16777217 - NULLIF(0, 0)))",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / ((CEIL(9007199254740993) % (- f8)))",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "1 / ((NULLIF(i8, CAST(NULL AS double precision)) - 2147483647))",
        "refused: calculated field 'x' compares a bigint value with a double prec",
    ),
    (
        "1 / ((f8 % i8))",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    (
        "1 / ((i4 + -9223372036854775808) - (REAL '1' - ((9223372036854775807 * 16777218) * COALESCE(i4, -9223372036854775809))))",
        "refused: calculated field 'x' has the constant (9223372036854775807 * 16",
    ),
    (
        "1 / ((i8 + 16777217))",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "1 / (0)",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / (0.0)",
        "refused: calculated field 'x' divides by the constant zero in (1 / 0.0);",
    ),
    (
        "1 / (1)",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / (16777217)",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / (5 - CASE WHEN TRUE THEN (REAL '1.6777216e+07' + (9007199254740992 / 5)) ELSE CASE WHEN 1 = 1 THEN (REAL '1.6777216e+07' % 9223372036854775809) END END)",
        "refused: calculated field 'x' divides a bigint value by a integer value;",
    ),
    (
        "1 / (9007199254740993 - 0)",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "1 / (9007199254740993 - CEIL(1))",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "1 / (9007199254740993)",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "1 / (ABS(16777217))",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "1 / (ABS(9007199254740993))",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "1 / (ABS(REAL '0'))",
        "refused: calculated field 'x' divides by the constant zero in (1 / ABS(C",
    ),
    (
        "1 / (CASE WHEN FALSE THEN (CEIL(NULL) * (1 - CAST(NULL AS integer))) WHEN (0.1 - CAST(NULL AS real)) < CEIL(f8) THEN REAL '1' END)",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "1 / (CEIL(16777217))",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "1 / (CEIL(2147483648))",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "1 / (CEIL(NULL))",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "1 / (CEIL(REAL '0'))",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "1 / (CEIL(REAL '1'))",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "1 / (CEIL(f4))",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "1 / (NULL)",
        "refused: calculated field 'x' has a NULL in an operand of / beside a int",
    ),
    (
        "1 / (NULLIF((- DOUBLE PRECISION '9.007199254740992e+15'), (9223372036854775807 + 16777216)))",
        "refused: calculated field 'x' has the constant (9223372036854775807 + 16",
    ),
    (
        "1 / (NULLIF((- n), COALESCE(f8, f4)))",
        "refused: calculated field 'x' compares a numeric value with a double pre",
    ),
    (
        "1 / (NULLIF((NULL - -9223372036854775809), (5 - f8)) - i8)",
        "refused: calculated field 'x' compares a numeric value with a double pre",
    ),
    (
        "1 / (NULLIF(-9223372036854775808, (REAL '0' - REAL '0')))",
        "refused: calculated field 'x': NULLIF of a bigint value and a real value",
    ),
    (
        "1 / (NULLIF(1, REAL '3.4e+38'))",
        "refused: calculated field 'x': NULLIF of a integer value and a real valu",
    ),
    (
        "1 / (NULLIF(16777216, 1.5))",
        "refused: calculated field 'x': NULLIF of a integer value and a numeric v",
    ),
    (
        "1 / (NULLIF(16777217, REAL '3.4e+38'))",
        "refused: calculated field 'x': NULLIF of a integer value and a real valu",
    ),
    (
        "1 / (NULLIF(16777217, f8))",
        "refused: calculated field 'x': NULLIF of a integer value and a double pr",
    ),
    (
        "1 / (NULLIF(ABS(CAST(NULL AS real)), CAST(NULL AS numeric)))",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "1 / (NULLIF(COALESCE(0, 9007199254740993), f8))",
        "refused: calculated field 'x' compares a bigint value with a double prec",
    ),
    (
        "1 / (NULLIF(DOUBLE PRECISION '0', CAST(NULL AS numeric)))",
        "refused: calculated field 'x' compares a double precision value with a n",
    ),
    (
        "1 / (NULLIF(DOUBLE PRECISION '1', n))",
        "refused: calculated field 'x' compares a double precision value with a n",
    ),
    (
        "1 / (NULLIF(REAL '3.4e+38', (COALESCE(ABS(NULL), (REAL '3.4e+38' + n)) / (- ABS(i4)))))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "1 / (NULLIF(n, DOUBLE PRECISION '1e+308'))",
        "refused: calculated field 'x' compares a numeric value with a double pre",
    ),
    (
        "1 / (NULLIF(n, REAL '0'))",
        "refused: calculated field 'x' compares a numeric value with a real value",
    ),
    (
        "1 / (NULLIF(n, f4))",
        "refused: calculated field 'x' compares a numeric value with a real value",
    ),
    (
        "1 / (REAL '0')",
        "refused: calculated field 'x' divides by the constant zero in (1 / CAST(",
    ),
    ("1 / (REAL '1')", "admitted: double precision"),
    (
        "1 / (REAL '1.6777216e+07' - CEIL(0))",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "1 / (REAL '3.4e+38' - NULLIF(REAL '1.6777216e+07', n))",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "1 / (i8)",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    ("1 / (n - 16777217)", "admitted: numeric"),
    ("1 = 1 OR 1.5 / 0 = 1", "admitted: boolean"),
    (
        "1.0 / 0 + na",
        "refused: calculated field 'x' references 'na', which is neither a source",
    ),
    (
        "1.5 / ((((NULLIF(0.1, CAST(NULL AS numeric)) + (2147483647 + 2147483647)) - (- (0.0 - 0.1))) + COALESCE((REAL '0' + (i8 + 2147483647)), n)))",
        "refused: calculated field 'x' has the constant (2147483647 + 2147483647)",
    ),
    (
        "1.5 / ((((REAL '0' + 9223372036854775807) + 1.5) + (- (CAST(NULL AS integer) / NULL))))",
        "refused: calculated field 'x' has a NULL in an operand of / beside a int",
    ),
    (
        "1.5 / (((16777217 + n) - (f4 % 1)))",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    (
        "1.5 / (((f4 + 5) - (REAL '3.4e+38' - CAST(NULL AS double precision))) - (COALESCE(9223372036854775807, CAST(NULL AS integer)) + (-9223372036854775808 - 2147483648)))",
        "refused: calculated field 'x' has the constant (-9223372036854775808 - 2",
    ),
    (
        "1.5 / ((- NULL) - (- DOUBLE PRECISION '9.007199254740992e+15'))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "1.5 / ((- NULL) - (9007199254740993 - 16777218))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "1.5 / ((- REAL '0'))",
        "refused: calculated field 'x' divides by the constant zero in (1.5 / (-C",
    ),
    (
        "1.5 / ((9007199254740993 / i4) - n)",
        "refused: calculated field 'x' divides a bigint value by a integer value;",
    ),
    (
        "1.5 / ((9223372036854775807 * 5) - n)",
        "refused: calculated field 'x' has the constant (9223372036854775807 * 5)",
    ),
    (
        "1.5 / ((COALESCE((-9223372036854775809 + 9007199254740993), NULLIF(CAST(NULL AS integer), i8), REAL '1') - COALESCE((f8 + f4), (9007199254740993 / 16777218))))",
        "refused: calculated field 'x' divides a bigint value by a integer value;",
    ),
    (
        "1.5 / ((DOUBLE PRECISION '1' % i4))",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    (
        "1.5 / ((REAL '1' - 1))",
        "refused: calculated field 'x' divides by the constant zero in (1.5 / (CA",
    ),
    (
        "1.5 / ((n - DOUBLE PRECISION '1') - CEIL(NULL))",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "1.5 / (0)",
        "refused: calculated field 'x' divides by the constant zero in (1.5 / 0);",
    ),
    ("1.5 / (16777217)", "admitted: numeric"),
    (
        "1.5 / (ABS(((NULLIF(i8, REAL '3.4e+38') - 1) + f8)))",
        "refused: calculated field 'x' compares a bigint value with a real value ",
    ),
    (
        "1.5 / (ABS((-9223372036854775808 - 9007199254740993)))",
        "refused: calculated field 'x' has the constant (-9223372036854775808 - 9",
    ),
    (
        "1.5 / (ABS(9223372036854775809) - (- NULL))",
        "refused: calculated field 'x' has a NULL in the operand of prefix -, whe",
    ),
    (
        "1.5 / (ABS(NULL))",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "1.5 / (CASE WHEN f4 < (n - 5) THEN (- (NULL - REAL '0')) END)",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "1.5 / (CASE WHEN i8 = REAL '0' THEN (0 - 2147483648) END - 1.5)",
        "refused: calculated field 'x' compares a bigint value with a real value ",
    ),
    (
        "1.5 / (CEIL(0) - REAL '1.6777216e+07')",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "1.5 / (CEIL(REAL '0'))",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "1.5 / (CEIL(i8) - 9223372036854775809)",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "1.5 / (CEIL(i8))",
        "refused: calculated field 'x': CEIL does not take a bigint value: an int",
    ),
    (
        "1.5 / (COALESCE(((REAL '1' + CAST(NULL AS integer)) * COALESCE(NULL, REAL '1.6777216e+07')), (REAL '1.6777216e+07' - CEIL(NULL))))",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "1.5 / (COALESCE(CEIL(NULL), (0 % i4)))",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "1.5 / (NULLIF((REAL '1' / 9223372036854775809), n))",
        "refused: calculated field 'x' compares a double precision value with a n",
    ),
    (
        "1.5 / (NULLIF(0, DOUBLE PRECISION '0'))",
        "refused: calculated field 'x': NULLIF of a integer value and a double pr",
    ),
    (
        "1.5 / (NULLIF(9007199254740992, f8))",
        "refused: calculated field 'x': NULLIF of a bigint value and a double pre",
    ),
    (
        "1.5 / (NULLIF(9007199254740993, 9223372036854775809))",
        "refused: calculated field 'x': NULLIF of a bigint value and a numeric va",
    ),
    (
        "1.5 / (NULLIF(CAST(NULL AS real), i8))",
        "refused: calculated field 'x' compares a real value with a bigint value ",
    ),
    (
        "1.5 / (NULLIF(NULLIF(f4, 1), ABS(2147483648)) - CEIL(DOUBLE PRECISION '0'))",
        "refused: calculated field 'x' compares a real value with a bigint value ",
    ),
    (
        "1.5 / (NULLIF(REAL '3.4e+38', n))",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "1.5 / (NULLIF(f4, n))",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "1.5 / (NULLIF(f8, (5 - 9007199254740993)))",
        "refused: calculated field 'x' compares a double precision value with a b",
    ),
    (
        "1.5 / (NULLIF(i8, f4))",
        "refused: calculated field 'x' compares a bigint value with a real value ",
    ),
    (
        "1.5 / (REAL '0')",
        "refused: calculated field 'x' divides by the constant zero in (1.5 / CAS",
    ),
    ("16777217", "admitted: integer"),
    (
        "16777217 AS g, 1 / (g - g)",
        "refused: calculated field 'x' divides a integer value by a integer value",
    ),
    (
        "16777217 AS g, 1 / (g - i8)",
        "refused: calculated field 'x' divides a integer value by a bigint value;",
    ),
    (
        "2147483647 * 2",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "2147483647 + 1",
        "refused: calculated field 'x' has the constant (2147483647 + 1), which i",
    ),
    (
        "2147483647 + 2147483647",
        "refused: calculated field 'x' has the constant (2147483647 + 2147483647)",
    ),
    (
        "2147483647 - -1",
        "refused: calculated field 'x' has the constant (2147483647 - -1), which ",
    ),
    (
        "46341 * 46341",
        "refused: calculated field 'x' has the constant (46341 * 46341), which is",
    ),
    ("9007199254740993", "admitted: bigint"),
    (
        "9223372036854775807 * 2",
        "refused: calculated field 'x' has the constant (9223372036854775807 * 2)",
    ),
    ("9223372036854775809", "admitted: numeric"),
    ("ABS((- 16777217))", "admitted: integer"),
    (
        "ABS((n % REAL '0'))",
        "refused: calculated field 'x' applies '%' to a float; Postgres has no mo",
    ),
    ("ABS(16777217)", "admitted: integer"),
    (
        "ABS(NULLIF(5, REAL '1.6777216e+07'))",
        "refused: calculated field 'x': NULLIF of a integer value and a real valu",
    ),
    (
        "ABS(NULLIF(f4, i8))",
        "refused: calculated field 'x' compares a real value with a bigint value ",
    ),
    ("ABS(REAL '1')", "admitted: real"),
    (
        "CASE WHEN ((- COALESCE(NULL, 0, REAL '0')) = 0) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "admitted: date",
    ),
    (
        "CASE WHEN (COALESCE(0, 16777217) = 16777217) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN (COALESCE(DOUBLE PRECISION '0', 0) = 0) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "admitted: date",
    ),
    (
        "CASE WHEN (COALESCE(NULL, 0, REAL '0') = 0) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "admitted: date",
    ),
    (
        "CASE WHEN (COALESCE(NULL, 1.5, REAL '0') = 1.5) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "admitted: date",
    ),
    (
        "CASE WHEN (COALESCE(REAL '0', 0) = 0) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "admitted: date",
    ),
    (
        "CASE WHEN (COALESCE(REAL '0', 1.5) = 1.5) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN (COALESCE(f4, 16777217) = 16777217) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN (COALESCE(f8, 16777217) = 16777217) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN (COALESCE(n, 16777217) = 16777217) THEN DATE '2024-02-29' ELSE (2147483647 * 2) + DATE '2024-02-29' END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN TRUE THEN CASE WHEN DOUBLE PRECISION '1e+308' = f4 THEN (- 0.1) WHEN i4 > 0 THEN COALESCE(-9223372036854775809, 16777216) ELSE (2147483647 + 16777217) END END",
        "refused: calculated field 'x' has the constant (2147483647 + 16777217), ",
    ),
    (
        "CASE WHEN a THEN 1 ELSE 2 END",
        "refused: calculated field 'x' references 'a', which is neither a source ",
    ),
    (
        "CASE WHEN a THEN 1 END",
        "refused: calculated field 'x' references 'a', which is neither a source ",
    ),
    ("CASE WHEN b THEN NULL END", "admitted: text"),
    (
        "CASE WHEN b THEN i2 ELSE i8 END",
        "refused: calculated field 'x' references 'i2', which is neither a source",
    ),
    (
        "CASE WHEN ba THEN 1 END",
        "refused: calculated field 'x' references 'ba', which is neither a source",
    ),
    (
        "CASE WHEN i4 > 0 THEN 2147483647 * 2 ELSE 1 END",
        "refused: calculated field 'x' has the constant (2147483647 * 2), which i",
    ),
    (
        "CASE WHEN i4 > 0 THEN COALESCE(NULL, DOUBLE PRECISION '0') WHEN 16777217 < i4 THEN CASE WHEN TRUE THEN 0.1 ELSE 1 END ELSE CEIL(NULL) END",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "CASE WHEN i4 THEN 1 END",
        "refused: calculated field 'x': a CASE condition must be a boolean, found",
    ),
    (
        "CASE WHEN na / 0 > 0 THEN 1 END",
        "refused: calculated field 'x' references 'na', which is neither a source",
    ),
    (
        "CEIL(0)",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "CEIL(REAL '0')",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "CEIL(REAL '3.4e+38')",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "CEIL(f4)",
        "refused: calculated field 'x': CEIL does not take a real value: only num",
    ),
    (
        "CEIL(i4)",
        "refused: calculated field 'x': CEIL does not take a integer value: an in",
    ),
    (
        "COALESCE(CEIL(NULL), (DOUBLE PRECISION '1e+308' - 9007199254740992))",
        "refused: calculated field 'x' has a NULL in an argument of ceil, where n",
    ),
    (
        "DOUBLE PRECISION 'NaN' = DOUBLE PRECISION 'NaN'",
        "admitted: boolean",
    ),
    (
        "DOUBLE PRECISION 'NaN' > DOUBLE PRECISION 'Infinity'",
        "admitted: boolean",
    ),
    ("NULL", "admitted: text"),
    (
        "NULLIF(((9007199254740993 / i4) - n), (COALESCE(9007199254740993, DOUBLE PRECISION '9.007199254740992e+15') - (- 9007199254740992.0)))",
        "refused: calculated field 'x' divides a bigint value by a integer value;",
    ),
    (
        "NULLIF((99999999999999999999 + NULLIF((REAL '1.6777216e+07' - 2147483647), ABS(NULL))), i8)",
        "refused: calculated field 'x' has a NULL in an argument of abs, where no",
    ),
    (
        "NULLIF(1, f4)",
        "refused: calculated field 'x': NULLIF of a integer value and a real valu",
    ),
    (
        "NULLIF(2147483648, f4)",
        "refused: calculated field 'x': NULLIF of a bigint value and a real value",
    ),
    (
        "NULLIF(9007199254740993, 0.0)",
        "refused: calculated field 'x': NULLIF of a bigint value and a numeric va",
    ),
    (
        "NULLIF(CASE WHEN 9223372036854775809 = 9007199254740993 THEN NULL WHEN TRUE THEN 2147483648 END, (- f8))",
        "refused: calculated field 'x' compares a bigint value with a double prec",
    ),
    (
        "NULLIF(REAL '3.4e+38', n)",
        "refused: calculated field 'x' compares a real value with a numeric value",
    ),
    (
        "PROPTEST_CASES",
        "refused: calculated field 'x' references 'proptest_cases', which is neit",
    ),
    ("REAL '-0' = REAL '0'", "admitted: boolean"),
    ("REAL '0'", "admitted: real"),
    ("REAL '1'", "admitted: real"),
    (
        "REAL '1.5' + f4a",
        "refused: calculated field 'x' references 'f4a', which is neither a sourc",
    ),
    (
        "REAL '1e-30' * REAL '1e-30'",
        "refused: calculated field 'x' has the constant (CAST('1e-30' AS REAL) * ",
    ),
    (
        "REAL 'NaN' >= f4a",
        "refused: calculated field 'x' references 'f4a', which is neither a sourc",
    ),
    (
        "ba AND 1.5 / 0 = 1",
        "refused: calculated field 'x' references 'ba', which is neither a source",
    ),
    (
        "d + 1 - d2",
        "refused: calculated field 'x' references 'd2', which is neither a source",
    ),
    (
        "d - d2 + 1",
        "refused: calculated field 'x' references 'd2', which is neither a source",
    ),
    ("f4", "admitted: real"),
    (
        "f4a / REAL '-0'",
        "refused: calculated field 'x' references 'f4a', which is neither a sourc",
    ),
    (
        "f4a / REAL '0'",
        "refused: calculated field 'x' references 'f4a', which is neither a sourc",
    ),
    (
        "f4a IS NOT DISTINCT FROM REAL 'NaN'",
        "refused: calculated field 'x' references 'f4a', which is neither a sourc",
    ),
    ("f8", "admitted: double precision"),
    (
        "i2 + NULL",
        "refused: calculated field 'x' references 'i2', which is neither a source",
    ),
    (
        "i4 / NULL",
        "refused: calculated field 'x' has a NULL in an operand of / beside a int",
    ),
    ("i8", "admitted: bigint"),
    ("n", "admitted: numeric"),
    (
        "n AS g, 1 / (g - NULLIF((0 + 0.0), REAL '3.4e+38'))",
        "refused: calculated field 'x' compares a numeric value with a real value",
    ),
];

#[test]
fn define_verdicts_on_constant_float_resolving_forms_are_pinned() {
    let mut failures = Vec::new();
    for (text, expected) in VERDICTS {
        let found = verdict(text);
        if !found.starts_with(expected) {
            failures.push(format!(
                "`{text}`\n  expected: {expected}\n  found:    {found}"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A small deterministic generator (xorshift) of constant trees.
struct Rng(u64, &'static [&'static str]);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick(&mut self, items: &[&'static str]) -> &'static str {
        items[self.below(items.len() as u64) as usize]
    }
}

const LEAVES: &[&str] = &[
    "0",
    "1",
    "5",
    "2147483647",
    "16777216",
    "16777217",
    "9007199254740993",
    "9223372036854775807",
    "-9223372036854775809",
    "99999999999999999999",
    "1.5",
    "0.0",
    "REAL '0'",
    "REAL '1'",
    "REAL '1.6777216e+07'",
    "DOUBLE PRECISION '0'",
    "DOUBLE PRECISION '1'",
    "CAST(NULL AS real)",
    "CAST(NULL AS double precision)",
    "CAST(NULL AS integer)",
    "CAST(NULL AS numeric)",
    "NULL",
    "f4",
    "f8",
    "n",
    "i4",
    "i8",
];

fn condition(rng: &mut Rng, depth: u32) -> String {
    match rng.below(6) {
        0 => "FALSE".into(),
        1 => "TRUE".into(),
        2 => "i4 > 0".into(),
        3 => "NULL IS NULL".into(),
        4 => format!("{} = {}", tree(rng, depth), tree(rng, depth)),
        _ => format!("{} < {}", tree(rng, depth), tree(rng, depth)),
    }
}

fn tree(rng: &mut Rng, depth: u32) -> String {
    if depth == 0 || rng.below(5) == 0 {
        if !rng.1.is_empty() && rng.below(3) == 0 {
            let fields = rng.1;
            return rng.pick(fields).to_string();
        }
        return rng.pick(LEAVES).to_string();
    }
    let d = depth - 1;
    match rng.below(13) {
        0 | 1 => format!("({} + {})", tree(rng, d), tree(rng, d)),
        2 | 3 => format!("({} - {})", tree(rng, d), tree(rng, d)),
        4 => format!("({} * {})", tree(rng, d), tree(rng, d)),
        5 => format!("({} / {})", tree(rng, d), tree(rng, d)),
        6 => format!("(- {})", tree(rng, d)),
        7 => format!("ABS({})", tree(rng, d)),
        8 | 9 => {
            let mut text = format!("CASE WHEN {} THEN {}", condition(rng, d), tree(rng, d));
            if rng.below(3) == 0 {
                text.push_str(&format!(
                    " WHEN {} THEN {}",
                    condition(rng, d),
                    tree(rng, d)
                ));
            }
            if rng.below(4) != 0 {
                text.push_str(&format!(" ELSE {}", tree(rng, d)));
            }
            text.push_str(" END");
            text
        }
        10 => {
            let mut text = format!("COALESCE({}, {}", tree(rng, d), tree(rng, d));
            if rng.below(3) == 0 {
                text.push_str(&format!(", {}", tree(rng, d)));
            }
            text.push(')');
            text
        }
        11 => format!("NULLIF({}, {})", tree(rng, d), tree(rng, d)),
        _ => format!("CEIL({})", tree(rng, d)),
    }
}

/// A form with up to two calculated fields the rest of it reads.
fn form(rng: &mut Rng) -> String {
    let depth = 1 + rng.below(4) as u32;
    let lead = if rng.below(2) == 0 { "1" } else { "1.5" };
    match rng.below(5) {
        0 => tree(rng, depth),
        1 => format!("{lead} / ({})", tree(rng, depth)),
        2 => {
            let g = tree(rng, depth);
            rng.1 = &["g"];
            let rest = tree(rng, depth);
            rng.1 = &[];
            format!("{g} AS g, {lead} / (g - {rest})")
        }
        _ => {
            let g = tree(rng, depth);
            rng.1 = &["g"];
            let h = tree(rng, depth);
            rng.1 = &["g", "h"];
            let rest = tree(rng, depth);
            rng.1 = &[];
            format!("{g} AS g, {h} AS h, {lead} / (g - h + {rest})")
        }
    }
}

/// The constant fold types a `CASE` or `COALESCE` with `float_width`, its own
/// walk of the typing rules, and not with inference: inference folds every
/// operand, so a fold that called it would cost exponential time in nesting
/// depth (a release build defining `1 / (COALESCE(NULLIF(... COALESCE(NULLIF(
/// <float CASE>, 1), 2) ..., 1), 2) - K)` took 49 us at 30 levels of nesting
/// and 96 us at 40 with the walk, and 33 ms and 584 ms with inference, which
/// grew about 2.6x per `COALESCE` and `NULLIF` pair). This pins the two
/// together: on every `CASE` and `COALESCE` node of a corpus, the walk and
/// inference agree on the float width (or on there being none), so a
/// float-typed construct added to inference alone fails here, instead of
/// reopening the constant divisor that #1802 closed.
#[test]
fn float_width_agrees_with_inference_on_every_case_and_coalesce() {
    let mut forms: Vec<String> = VERDICTS.iter().map(|(text, _)| text.to_string()).collect();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15, &[]);
    for _ in 0..5000 {
        forms.push(form(&mut rng));
    }
    // A wide exact literal beside a float operand in each wrapper, then
    // subtracted from a constant that equals it only in that float (#1802).
    let wide = [
        "16777217",
        "9007199254740993",
        "-9223372036854775809",
        "1.5",
        "0",
    ];
    let floats = [
        "REAL '0'",
        "DOUBLE PRECISION '0'",
        "f4",
        "f8",
        "CAST(NULL AS real)",
        "ABS(f4)",
        "(- f8)",
        "n",
        "i4",
        "NULLIF(f4, 0)",
        "NULLIF(0, f8)",
        "CEIL(f8)",
    ];
    for x in wide {
        for f in floats {
            for wrap in [
                format!("CASE WHEN FALSE THEN {f} ELSE {x} END"),
                format!("CASE WHEN i4 > 0 THEN {x} WHEN FALSE THEN {f} END"),
                format!("COALESCE(NULL, {x}, {f})"),
                format!("COALESCE({f}, {x})"),
                format!("ABS(CASE WHEN FALSE THEN {f} ELSE {x} END)"),
                format!("(- COALESCE(NULL, {x}, {f}))"),
                format!("NULLIF(CASE WHEN FALSE THEN {f} ELSE {x} END, 5)"),
                format!("COALESCE(CASE WHEN FALSE THEN {f} ELSE {x} END, {x} + {f})"),
            ] {
                forms.push(format!("1 / ({wrap} - 16777216)"));
                forms.push(format!("{wrap} AS g, 1 / (g - 9007199254740992)"));
            }
        }
    }
    let mut checked = 0;
    let mut disagreements = Vec::new();
    for text in &forms {
        let Ok(def) = parse(&format!("TRANSFORM t FROM s SELECT {text} AS x")) else {
            continue;
        };
        checked += 1;
        for found in float_width_disagreements(&def, &columns()) {
            disagreements.push(format!("`{text}`: {found}"));
        }
    }
    assert!(checked > 4000, "the corpus should parse: {checked}");
    assert!(
        disagreements.is_empty(),
        "{}",
        disagreements[..disagreements.len().min(20)].join("\n")
    );
}
