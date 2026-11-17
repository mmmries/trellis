//! Which `WHERE` predicates are total: none of their constructs can raise an
//! error for any value of its operand types (ADR-0004, "Partial-data
//! predicate"). A predicate that raised on some row would fail every
//! statement that reads that row, where a field's failure holds one key.
//!
//! The class is the research's (#804, `T-totality.md` §3), checked on
//! Postgres 15 to 18, in its strict form: column reads, literals,
//! comparisons, `IS [NOT] NULL`, `AND`/`OR`/`NOT`, `CASE`/`COALESCE`/`NULLIF`
//! over those, and the functions and casts that cannot raise. Every other
//! construct is refused by name ([`check`]). The match over [`Expr`] is
//! exhaustive and a function not named here is refused, so a construct the
//! grammar gains later is refused in a predicate until it is classified.

use std::collections::BTreeSet;

use super::ast::{Expr, Operator, ValueType};
use super::validate::{ColumnTypes, infer_expr_type_in};
use crate::float::FloatWidth;

/// A construct of a predicate that can raise: the construct as SQL, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NotTotal {
    pub construct: String,
    pub reason: String,
}

/// The `DATE_TRUNC` units that cannot leave `timestamp`'s range: its lowest
/// value, 4714-11-24 00:00 BC, is a Monday midnight, so truncating to a week
/// or less never goes below it. A month or more can (`22008`).
const TOTAL_DATE_TRUNC_UNITS: &[&str] = &[
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "week",
];

/// `numeric`'s format limits: digits before and after the point. A literal
/// past them is refused by Postgres while planning (`22003`).
const NUMERIC_INTEGER_DIGITS: usize = 131_072;
const NUMERIC_FRACTION_DIGITS: usize = 16_383;

/// The first construct of `predicate`, outermost first, that can raise for
/// some value of its operand types, typed against `columns`. `predicate` is
/// already typed (`boolean`), so every node types except an untyped `NULL`,
/// which takes its context's type.
pub(crate) fn check(predicate: &Expr, columns: &ColumnTypes<'_>) -> Result<(), NotTotal> {
    check_node(predicate, columns)?;
    for child in predicate.children() {
        check(child, columns)?;
    }
    Ok(())
}

fn refuse(expr: &Expr, reason: impl Into<String>) -> Result<(), NotTotal> {
    Err(NotTotal {
        construct: super::ast::render_field_expr(expr),
        reason: reason.into(),
    })
}

fn type_of(expr: &Expr, columns: &ColumnTypes<'_>) -> Option<ValueType> {
    infer_expr_type_in("WHERE", expr, columns).ok()
}

/// The text of a number literal, signed, when `expr` is one (a prefix `-`
/// over a literal is how the parser reads `-5`).
fn number_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::NumberLiteral(text) => Some(text.clone()),
        Expr::Negate(inner) => number_literal(inner).map(|text| format!("-{text}")),
        _ => None,
    }
}

/// Whether the number literal `text` is the lowest `integer` or `bigint`,
/// the type Postgres gives it: `ABS` of it raises while planning.
fn lowest_integer(text: &str) -> bool {
    text.parse::<i128>()
        .is_ok_and(|value| value == i128::from(i32::MIN) || value == i128::from(i64::MIN))
}

/// Whether the number literal `text` converts to a float of `width` the way
/// Postgres's input function does: finite, and not rounded to zero unless
/// it is zero (an underflow raises `22003`).
fn literal_fits_float(text: &str, width: FloatWidth) -> bool {
    let is_zero = text.chars().all(|c| matches!(c, '0' | '.' | '-'));
    let (finite, zero) = match width {
        FloatWidth::Float4 => match text.parse::<f32>() {
            Ok(value) => (value.is_finite(), value == 0.0),
            Err(_) => (false, false),
        },
        FloatWidth::Float8 => match text.parse::<f64>() {
            Ok(value) => (value.is_finite(), value == 0.0),
            Err(_) => (false, false),
        },
    };
    finite && (is_zero || !zero)
}

/// The distinct text columns whose collation reaches `expr`'s value: the
/// columns read through text-typed nodes only (a `CASE` condition or a
/// function's non-text argument contributes none).
fn text_columns(expr: &Expr, columns: &ColumnTypes<'_>, out: &mut BTreeSet<String>) {
    if type_of(expr, columns) != Some(ValueType::Text) {
        return;
    }
    match expr {
        Expr::Column(name) => {
            out.insert(name.clone());
        }
        // A `JOIN`'s tables can share a column name: two tables' columns
        // are two columns, which may carry conflicting collations.
        Expr::SourceColumn { table, column, .. } => {
            out.insert(match columns {
                ColumnTypes::Join { .. } => format!("{table}.{column}"),
                ColumnTypes::Source(_) | ColumnTypes::Live(_) => column.clone(),
            });
        }
        _ => {
            for child in expr.children() {
                text_columns(child, columns, out);
            }
        }
    }
}

/// Side condition C2: a collation-sensitive operation over text reads at
/// most one text column. Two columns may carry conflicting collations,
/// which raises `42P22` for each row where both are non-`NULL`.
fn one_text_column(
    expr: &Expr,
    operands: &[&Expr],
    columns: &ColumnTypes<'_>,
) -> Result<(), NotTotal> {
    let mut read = BTreeSet::new();
    for operand in operands {
        text_columns(operand, columns, &mut read);
    }
    if read.len() > 1 {
        let names: Vec<String> = read.into_iter().map(|c| format!("'{c}'")).collect();
        return refuse(
            expr,
            format!(
                "it compares text columns {}, whose collations can conflict (42P22)",
                names.join(" and ")
            ),
        );
    }
    Ok(())
}

/// The operand types of a binary construct, an untyped `NULL` taking the
/// other side's.
fn pair_types(
    lhs: &Expr,
    rhs: &Expr,
    columns: &ColumnTypes<'_>,
) -> (Option<ValueType>, Option<ValueType>) {
    let (l, r) = (type_of(lhs, columns), type_of(rhs, columns));
    (l.or(r), r.or(l))
}

/// A comparison or `NULLIF`: total but for a number literal beside a float
/// that the float can't hold (C1; compared in `double precision`), and two
/// text columns (C2).
fn comparison(
    expr: &Expr,
    lhs: &Expr,
    rhs: &Expr,
    columns: &ColumnTypes<'_>,
) -> Result<(), NotTotal> {
    let (l, r) = pair_types(lhs, rhs, columns);
    for (float, other) in [(l, rhs), (r, lhs)] {
        if let Some(ValueType::Float(_)) = float
            && let Some(text) = number_literal(other)
            && !literal_fits_float(&text, FloatWidth::Float8)
        {
            return refuse(
                expr,
                format!("the literal {text} is out of double precision's range (22003)"),
            );
        }
    }
    if l == Some(ValueType::Text) && r == Some(ValueType::Text) {
        one_text_column(expr, &[lhs, rhs], columns)?;
    }
    Ok(())
}

/// `CASE` results or `COALESCE` arguments, unified to one type. Unifying a
/// float with `numeric` casts the `numeric` to the float, which raises past
/// its range (C3); a number literal there must fit the float (C1). An
/// integer converts to a float without error.
fn unification(expr: &Expr, results: &[&Expr], columns: &ColumnTypes<'_>) -> Result<(), NotTotal> {
    let types: Vec<Option<ValueType>> = results.iter().map(|r| type_of(r, columns)).collect();
    let floats: Vec<FloatWidth> = types
        .iter()
        .filter_map(|t| match t {
            Some(ValueType::Float(width)) => Some(*width),
            _ => None,
        })
        .collect();
    let Some(width) = floats.iter().copied().reduce(FloatWidth::wider) else {
        return Ok(());
    };
    for (result, value_type) in results.iter().zip(&types) {
        if let Some(text) = number_literal(result) {
            if !literal_fits_float(&text, width) {
                return refuse(
                    expr,
                    format!("the literal {text} is out of the float result's range (22003)"),
                );
            }
        } else if *value_type == Some(ValueType::Numeric) {
            return refuse(
                expr,
                "it unifies numeric with a float, and a numeric past the float's range raises \
                 (22003)",
            );
        }
    }
    Ok(())
}

fn check_node(expr: &Expr, columns: &ColumnTypes<'_>) -> Result<(), NotTotal> {
    match expr {
        Expr::Column(_)
        | Expr::SourceColumn { .. }
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. }
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::Not(_)
        | Expr::IsNull { .. } => Ok(()),
        Expr::NumberLiteral(text) => {
            let (integer, fraction) = text.split_once('.').unwrap_or((text, ""));
            let digits = integer.trim_start_matches('-').trim_start_matches('0');
            if digits.len() > NUMERIC_INTEGER_DIGITS || fraction.len() > NUMERIC_FRACTION_DIGITS {
                return refuse(expr, "the literal is out of numeric's range (22003)");
            }
            Ok(())
        }
        Expr::BinaryOp { op, lhs, rhs } => match op {
            Operator::And | Operator::Or | Operator::AtTimeZone => Ok(()),
            _ if op.is_comparison() => comparison(expr, lhs, rhs, columns),
            Operator::Modulo
                if number_literal(rhs)
                    .is_some_and(|text| text.chars().any(|c| matches!(c, '1'..='9'))) =>
            {
                Ok(())
            }
            Operator::Add | Operator::Subtract => {
                let (l, r) = pair_types(lhs, rhs, columns);
                let date = Some(ValueType::Other(super::pg_type::PgType::Date));
                if l == date || r == date {
                    refuse(expr, "date arithmetic can leave date's range (22008)")
                } else {
                    refuse(expr, "arithmetic can overflow (22003)")
                }
            }
            Operator::Multiply => refuse(expr, "arithmetic can overflow (22003)"),
            Operator::Divide | Operator::Modulo => refuse(
                expr,
                "it can divide by zero (22012); only % by a nonzero number literal is total",
            ),
            _ => refuse(expr, "the operator is not classified as total"),
        },
        Expr::Negate(operand) => {
            if number_literal(operand).is_some() {
                return Ok(());
            }
            match type_of(operand, columns) {
                Some(ValueType::Integer(_)) => {
                    refuse(expr, "negating the lowest integer overflows (22003)")
                }
                _ => Ok(()),
            }
        }
        Expr::Cast {
            expr: operand,
            target,
        } => {
            let date = Some(ValueType::Other(super::pg_type::PgType::Date));
            if type_of(operand, columns) == date
                && *target == ValueType::Other(super::pg_type::PgType::Timestamp)
            {
                return refuse(
                    expr,
                    "a date past 294276-12-31 is out of timestamp's range (22008)",
                );
            }
            Ok(())
        }
        Expr::Case {
            branches,
            otherwise,
        } => {
            let results: Vec<&Expr> = branches
                .iter()
                .map(|(_, result)| result)
                .chain(otherwise.as_deref())
                .collect();
            unification(expr, &results, columns)
        }
        Expr::FunctionCall { name, args } => function(expr, name, args, columns),
    }
}

fn function(
    expr: &Expr,
    name: &str,
    args: &[Expr],
    columns: &ColumnTypes<'_>,
) -> Result<(), NotTotal> {
    let first = args.first().and_then(|arg| type_of(arg, columns));
    match name {
        "CHAR_LENGTH" | "OCTET_LENGTH" | "TRUNC" | "EXTRACT" => Ok(()),
        "ABS" => match first {
            Some(ValueType::Integer(_))
                if number_literal(&args[0]).is_none_or(|text| lowest_integer(&text)) =>
            {
                refuse(expr, "ABS of the lowest integer overflows (22003)")
            }
            _ => Ok(()),
        },
        "CEIL" | "FLOOR" | "ROUND" => match first {
            Some(ValueType::Float(_)) => Ok(()),
            _ => refuse(
                expr,
                "rounding a numeric can carry past numeric's format limit (22003)",
            ),
        },
        "DATE_TRUNC" => match args.first() {
            Some(Expr::StringLiteral(unit))
                if TOTAL_DATE_TRUNC_UNITS.contains(&unit.to_ascii_lowercase().as_str()) =>
            {
                Ok(())
            }
            _ => refuse(
                expr,
                "truncating to a month or more can go below timestamp's lowest value (22008); \
                 units up to week are total",
            ),
        },
        "STRPOS" => one_text_column(expr, &args.iter().collect::<Vec<_>>(), columns),
        "NULLIF" => match args {
            [lhs, rhs] => comparison(expr, lhs, rhs, columns),
            _ => Ok(()),
        },
        "COALESCE" => unification(expr, &args.iter().collect::<Vec<_>>(), columns),
        "REGEXP_COUNT" => refuse(
            expr,
            "a regular expression's evaluation is not shown to be total",
        ),
        _ => refuse(expr, "the function is not classified as total"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::defs::pg_type::PgType;
    use crate::integer::IntWidth;

    fn columns() -> HashMap<String, ValueType> {
        HashMap::from([
            ("i".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("j".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("n".to_string(), ValueType::Numeric),
            ("m".to_string(), ValueType::Numeric),
            ("f".to_string(), ValueType::Float(FloatWidth::Float8)),
            ("r".to_string(), ValueType::Float(FloatWidth::Float4)),
            ("t".to_string(), ValueType::Text),
            ("u".to_string(), ValueType::Text),
            ("b".to_string(), ValueType::Boolean),
            ("d".to_string(), ValueType::Other(PgType::Date)),
            ("ts".to_string(), ValueType::Other(PgType::Timestamp)),
            ("tz".to_string(), ValueType::Other(PgType::TimestampTz)),
        ])
    }

    /// The verdict on `predicate`, parsed as a 1-1 definition's `WHERE`.
    fn verdict(predicate: &str) -> Result<(), NotTotal> {
        let def = crate::defs::parse(&format!(
            "TRANSFORM x FROM s SELECT i AS k WHERE {predicate}"
        ))
        .unwrap_or_else(|err| panic!("{predicate}: {err}"));
        let expr = def.predicate.filter().expect("a filter").clone();
        let columns = columns();
        check(&expr, &ColumnTypes::Source(&columns))
    }

    #[test]
    fn the_total_class_is_admitted() {
        for predicate in [
            "b",
            "NOT b AND (i > 0 OR n IS NULL)",
            "i = 1",
            "j <> -5",
            "n >= 12.5",
            "i = j",
            "i = n",
            "f > 1.5",
            "r < 39999999999999999999999999999999999999",
            "f IS DISTINCT FROM r",
            "t = 'open'",
            "t <> 'draft' OR t IS NULL",
            "t = t",
            "NULLIF(t, 'x') IS NULL",
            "COALESCE(b, FALSE)",
            "COALESCE(i, j, 0) > 3",
            "COALESCE(f, i) > 0",
            "COALESCE(f, 1.5) > 0",
            "CASE WHEN i > 0 THEN n ELSE 0 END > 1",
            "CASE WHEN t = 'a' THEN t ELSE 'b' END = 'a'",
            "CAST(i AS bigint) > 1",
            "CAST(i AS numeric) > 1",
            "-n > 1",
            "ABS(n) > 1",
            "ABS(f) > 1",
            "ABS(-2147483647) > 1",
            "ABS(-2147483649) > 1",
            "ABS(-9223372036854775809) > 1",
            "CEIL(f) > 1",
            "FLOOR(f) > 1",
            "TRUNC(n) > 1",
            "TRUNC(n, 2) > 1",
            "TRUNC(f) > 1",
            "EXTRACT(year FROM d) = 2026",
            "EXTRACT(dow FROM ts) < 6",
            "tz AT TIME ZONE 'UTC' >= TIMESTAMP '2026-01-01 00:00:00'",
            "DATE_TRUNC('week', ts) = TIMESTAMP '2026-01-05 00:00:00'",
            "DATE_TRUNC('day', ts) = ts",
            "CHAR_LENGTH(t) > 0",
            "OCTET_LENGTH(t) <= 8192",
            "STRPOS(t, 'abc') = 1",
            "i % 10 = 3",
            "n % 0.3 = 0",
            "j % -7 = 1",
            "d >= DATE '2026-01-30'",
            "FALSE",
            "NULL",
            "(TRUE)",
            "1 = 1",
        ] {
            assert_eq!(verdict(predicate), Ok(()), "{predicate}");
        }
    }

    #[test]
    fn every_construct_that_can_raise_is_refused_by_name() {
        for (predicate, construct) in [
            ("i + 1 > 0", "\"i\" + 1"),
            ("n - m > 0", "\"n\" - \"m\""),
            ("n * 2 > 0", "\"n\" * 2"),
            ("f * f > 0", "\"f\" * \"f\""),
            ("n / 2 > 0", "\"n\" / 2"),
            ("i % j = 0", "\"i\" % \"j\""),
            ("-i > 0", "-\"i\""),
            ("ABS(i) > 5", "abs(\"i\")"),
            // Postgres types these literals `integer` and `bigint`.
            ("ABS(-2147483648) > 5", "abs(-2147483648)"),
            ("ABS(-9223372036854775808) > 5", "abs(-9223372036854775808)"),
            ("CEIL(n) = 3", "ceil(\"n\")"),
            ("FLOOR(n) = 3", "floor(\"n\")"),
            ("ROUND(n, 2) = 1.25", "round(\"n\", 2)"),
            ("d + 30 >= DATE '2026-03-01'", "\"d\" + 30"),
            ("d - d > 3", "\"d\" - \"d\""),
            ("CAST(d AS timestamp) > ts", "CAST(\"d\" AS timestamp)"),
            (
                "DATE_TRUNC('month', ts) = ts",
                "date_trunc('month', \"ts\")",
            ),
            ("t = u", "\"t\" = \"u\""),
            ("NULLIF(t, u) IS NULL", "nullif(\"t\", \"u\")"),
            ("STRPOS(t, u) = 1", "strpos(\"t\", \"u\")"),
            (
                "CASE WHEN b THEN t ELSE u END = 'x'",
                "CASE WHEN \"b\" THEN \"t\" ELSE \"u\" END = 'x'",
            ),
            ("REGEXP_COUNT(t, 'a') > 0", "regexp_count(\"t\", 'a')"),
            ("COALESCE(r, n) > 0", "COALESCE(\"r\", \"n\")"),
            (
                "CASE WHEN b THEN f ELSE n END > 0",
                "CASE WHEN \"b\" THEN \"f\" ELSE \"n\" END",
            ),
        ] {
            let refused = verdict(predicate).expect_err(predicate);
            // The rendering's parentheses and quoting aren't the point.
            let bare = |text: &str| text.to_lowercase().replace(['(', ')', '"', ' '], "");
            assert_eq!(
                bare(&refused.construct),
                bare(construct),
                "{predicate}: refused {refused:?}"
            );
        }
    }

    #[test]
    fn a_number_literal_beside_a_float_must_fit_it() {
        let big = format!("1{}", "0".repeat(309));
        let tiny = format!("0.{}1", "0".repeat(400));
        assert!(verdict(&format!("f > {big}")).is_err());
        assert!(verdict(&format!("f > {tiny}")).is_err());
        assert!(verdict(&format!("r > {big}")).is_err());
        assert_eq!(verdict("f > 0.000"), Ok(()));
        // Unified to `real`, a literal past `real`'s range raises.
        assert!(verdict("COALESCE(r, 1000000000000000000000000000000000000000) > 0").is_err());
        assert_eq!(
            verdict("COALESCE(f, 1000000000000000000000000000000000000000) > 0"),
            Ok(())
        );
    }

    #[test]
    fn a_literal_past_numerics_format_is_refused() {
        let wide = "9".repeat(NUMERIC_INTEGER_DIGITS + 1);
        assert!(verdict(&format!("n > {wide}")).is_err());
        let fine = "9".repeat(1000);
        assert_eq!(verdict(&format!("n > {fine}")), Ok(()));
        // The sign is no digit.
        let widest = "9".repeat(NUMERIC_INTEGER_DIGITS);
        assert_eq!(verdict(&format!("n > -{widest}")), Ok(()));
    }
}
