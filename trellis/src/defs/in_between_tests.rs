//! `[NOT] IN (...)` and `[NOT] BETWEEN ... AND ...` (#2172): how they parse,
//! how they type, and what define refuses. Every reading pinned here was
//! checked against a scratch Postgres 17 (`EXPLAIN (VERBOSE)` of the same
//! text); the conformance harness (`generative/tests/expr_conformance.rs`)
//! checks the values against a real Postgres too.

use std::collections::HashMap;

use super::ast::{Expr, ValueType, render_field_expr};
use super::error::ParseError;
use super::parse;
use super::pg_type::{PgType, oid};
use super::validate::{JoinColumnInfo, ValidationError, validate, validate_live};
use crate::float::FloatWidth;
use crate::integer::IntWidth;

fn parse_text(text: &str) -> Result<super::ast::TransformDef, ParseError> {
    parse(&format!("TRANSFORM t FROM s SELECT {text} AS x"))
}

fn expr_of(text: &str) -> Expr {
    parse_text(text)
        .unwrap_or_else(|e| panic!("`{text}` should parse: {e}"))
        .fields
        .remove(0)
        .expr
}

fn parse_err(text: &str) -> ParseError {
    parse_text(text).expect_err(&format!("`{text}` should not parse"))
}

/// The tree as an s-expression, to pin groupings at a glance.
fn sexp(expr: &Expr) -> String {
    let list = |items: &[Expr]| items.iter().map(sexp).collect::<Vec<_>>().join(" ");
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{text}'"),
        Expr::BooleanLiteral(value) => value.to_string(),
        Expr::Null => "null".to_string(),
        Expr::BinaryOp { op, lhs, rhs } => format!(
            "({} {} {})",
            super::registry::operator_spec(*op).symbol,
            sexp(lhs),
            sexp(rhs)
        ),
        Expr::Negate(operand) => format!("(neg {})", sexp(operand)),
        Expr::Not(operand) => format!("(not {})", sexp(operand)),
        Expr::IsNull { expr, negated } => format!(
            "({} {})",
            if *negated { "notnull" } else { "isnull" },
            sexp(expr)
        ),
        Expr::InList {
            expr,
            list: elements,
            negated,
        } => format!(
            "({} {} [{}])",
            if *negated { "notin" } else { "in" },
            sexp(expr),
            list(elements)
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "({} {} {} {})",
            if *negated { "notbetween" } else { "between" },
            sexp(expr),
            sexp(low),
            sexp(high)
        ),
        Expr::FunctionCall { name, args } => format!("({} {})", name.to_lowercase(), list(args)),
        other => format!("{other:?}"),
    }
}

#[track_caller]
fn assert_tree(text: &str, expected: &str) {
    assert_eq!(sexp(&expr_of(text)), expected, "`{text}`");
}

/// The operators and their `NOT` forms, each one operator.
#[test]
fn the_four_operators_parse() {
    assert_tree("a IN (1, 2)", "(in a [1 2])");
    assert_tree("a NOT IN (1, NULL)", "(notin a [1 null])");
    assert_tree("a in (1)", "(in a [1])");
    assert_tree("a BETWEEN 1 AND 2", "(between a 1 2)");
    assert_tree("a NOT BETWEEN 1 AND 2", "(notbetween a 1 2)");
    assert_tree("a between -1 and -a", "(between a -1 (neg a))");
    // An element is any expression.
    assert_tree("a IN (b + 1, (2), abs(c))", "(in a [(+ b 1) 2 (abs c)])");
    // `values` before `)` is a column, as in Postgres.
    assert_tree("a IN (values)", "(in a [values])");
}

/// `IN` and `BETWEEN` bind tighter than the comparisons and looser than
/// `+`, and the upper bound ends at the first operator that doesn't bind
/// tighter than `BETWEEN`, as Postgres reads each of these.
#[test]
fn precedence_mirrors_postgres() {
    for (text, tree) in [
        ("a + 1 IN (2)", "(in (+ a 1) [2])"),
        ("-a IN (1)", "(in (neg a) [1])"),
        ("p = a IN (1, 2)", "(= p (in a [1 2]))"),
        ("a IN (1, 2) = p", "(= (in a [1 2]) p)"),
        ("p = a BETWEEN 1 AND 2", "(= p (between a 1 2))"),
        ("a BETWEEN 1 AND 2 = p", "(= (between a 1 2) p)"),
        ("a BETWEEN 1 AND 2 AND p", "(AND (between a 1 2) p)"),
        ("a NOT BETWEEN 1 AND 2 OR p", "(OR (notbetween a 1 2) p)"),
        ("a BETWEEN 1 + 1 AND 2 * 3", "(between a (+ 1 1) (* 2 3))"),
        ("NOT a IN (1)", "(not (in a [1]))"),
        ("NOT a BETWEEN 1 AND 2", "(not (between a 1 2))"),
        ("p AND a IN (1)", "(AND p (in a [1]))"),
        ("a IN (1) IS NULL", "(isnull (in a [1]))"),
        ("a BETWEEN 1 AND 2 IS NULL", "(isnull (between a 1 2))"),
        ("a IS NULL IN (TRUE)", "(in (isnull a) [true])"),
        // The list's `)` closes the operand, so another `IN` or a `BETWEEN`
        // reads onto it.
        ("a IN (1) IN (TRUE)", "(in (in a [1]) [true])"),
        ("a NOT IN (1) NOT IN (TRUE)", "(notin (notin a [1]) [true])"),
        (
            "a IN (1) BETWEEN TRUE AND FALSE",
            "(between (in a [1]) true false)",
        ),
        // The upper bound takes a prefix `NOT` and what binds tighter.
        ("p BETWEEN q AND NOT p", "(between p q (not p))"),
        ("p BETWEEN q AND NOT p = q", "(between p q (not (= p q)))"),
        ("a BETWEEN (b) AND 2", "(between a b 2)"),
        ("a IN ((1), (2))", "(in a [1 2])"),
    ] {
        assert_tree(text, tree);
    }
}

/// Text Postgres refuses as a syntax error is refused here too.
#[test]
fn postgres_syntax_errors_are_refused() {
    for text in [
        "a IN ()",
        "a IN (1,)",
        "a IN 1",
        "a NOT",
        "a BETWEEN 1",
        "a BETWEEN 1 OR 2",
        "a BETWEEN 1 AND 2 BETWEEN 3 AND 4",
        "a BETWEEN 1 AND 2 IN (TRUE)",
        "a BETWEEN b AND c IN (TRUE)",
        "a IN (1) = p = q",
        // The lower bound is Postgres's restricted operand.
        "a BETWEEN NOT p AND q",
        "a BETWEEN 1 + NOT p AND q",
        "ts BETWEEN ts AT TIME ZONE 'UTC' AND ts",
    ] {
        parse_err(text);
    }
    assert_eq!(
        parse_err("a BETWEEN 1 AND 2 IN (TRUE)"),
        ParseError::NonAssociativeComparison {
            operator: "IN".to_string()
        }
    );
}

/// The lower bound is refused where Postgres's reading of it would be one
/// this grammar does not parse to (a comparison there is legal Postgres,
/// `a BETWEEN (1 = 1) AND TRUE`, but rare; the parenthesized spelling is
/// admitted).
#[test]
fn a_comparison_in_the_lower_bound_needs_parentheses() {
    parse_err("p BETWEEN a = 1 AND q");
    assert_tree("p BETWEEN (a = 1) AND q", "(between p (= a 1) q)");
    assert_tree(
        "ts BETWEEN (ts AT TIME ZONE 'UTC') AND ts",
        "(between ts (AT TIME ZONE ts 'UTC') ts)",
    );
}

/// The four named refusals, each with its own code.
#[test]
fn the_named_refusals_have_their_codes() {
    for (text, expected, code) in [
        ("a IN (SELECT 1)", ParseError::SubqueryInList, "T0248"),
        ("a NOT IN (select b)", ParseError::SubqueryInList, "T0248"),
        ("a IN (VALUES (1))", ParseError::SubqueryInList, "T0248"),
        (
            "a IN (WITH q AS (SELECT 1) SELECT 1)",
            ParseError::SubqueryInList,
            "T0248",
        ),
        (
            "a BETWEEN SYMMETRIC 2 AND 1",
            ParseError::BetweenSymmetric {
                modifier: "SYMMETRIC".to_string(),
            },
            "T0249",
        ),
        (
            "a NOT BETWEEN asymmetric 1 AND 2",
            ParseError::BetweenSymmetric {
                modifier: "ASYMMETRIC".to_string(),
            },
            "T0249",
        ),
        (
            "a = ANY (b)",
            ParseError::QuantifiedComparison {
                operator: "=".to_string(),
                quantifier: "ANY".to_string(),
            },
            "T0250",
        ),
        (
            "a <> all (b)",
            ParseError::QuantifiedComparison {
                operator: "<>".to_string(),
                quantifier: "ALL".to_string(),
            },
            "T0250",
        ),
        (
            "a > SOME (b)",
            ParseError::QuantifiedComparison {
                operator: ">".to_string(),
                quantifier: "SOME".to_string(),
            },
            "T0250",
        ),
        ("(a, b) IN ((1, 2))", ParseError::RowConstructor, "T0251"),
        ("a IN ((1, 2))", ParseError::RowConstructor, "T0251"),
        ("(a, b) = (1, 2)", ParseError::RowConstructor, "T0251"),
    ] {
        let found = parse_err(text);
        assert_eq!(found, expected, "`{text}`");
        assert!(found.to_string().starts_with(code), "`{text}`: {found}");
    }
}

/// `between` is a keyword Postgres lets a column take, and Trellis read an
/// unquoted `between` as a column before the operator existed. Every such
/// text reads as it did (the stop condition of #2172): the operator is a
/// keyword only after a complete operand, where the word was a syntax
/// error before.
#[test]
fn a_column_named_between_reads_as_before() {
    for (text, tree) in [
        ("between", "between"),
        ("NOT between", "(not between)"),
        ("between AND p", "(AND between p)"),
        ("p AND between", "(AND p between)"),
        ("between IS NULL", "(isnull between)"),
        ("(between)", "between"),
        ("between = p", "(= between p)"),
        ("p = between", "(= p between)"),
        ("-between", "(neg between)"),
        ("COALESCE(between, p)", "(coalesce between p)"),
        (
            "CASE WHEN between THEN 1 END",
            "Case { branches: [(Column(\"between\"), NumberLiteral(\"1\"))], otherwise: None }",
        ),
        // New text, read as Postgres reads it.
        (
            "between BETWEEN between AND between",
            "(between between between between)",
        ),
        ("between IN (between)", "(in between [between])"),
        (
            "between NOT BETWEEN 1 AND between",
            "(notbetween between 1 between)",
        ),
    ] {
        assert_tree(text, tree);
    }
    let def = parse("TRANSFORM t FROM s SELECT x AS f WHERE NOT between").unwrap();
    assert_eq!(
        def.predicate.filter(),
        Some(&Expr::Not(Box::new(Expr::Column("between".to_string()))))
    );
}

/// The definition text renders each operator as written and reads back to
/// the same tree.
#[test]
fn rendered_text_reparses_to_the_same_tree() {
    for text in [
        "a IN (1, b + 1, NULL)",
        "a NOT IN ('x', 'y')",
        "a BETWEEN -1 AND b * 2",
        "a NOT BETWEEN (b = 1) AND NOT p",
        "a IN (1) IN (TRUE)",
        "p = a BETWEEN 1 AND 2",
    ] {
        let tree = expr_of(text);
        let rendered = render_field_expr(&tree);
        assert_eq!(
            expr_of(&rendered),
            tree,
            "`{text}` rendered as `{rendered}`"
        );
    }
}

/// The write path renders the operator as written; an `IN`'s elements and
/// operand are typed from their neighbour as `=`'s are (#2000), a
/// `BETWEEN`'s bounds as the ordering operators' are.
#[test]
fn sql_renders_the_operator_as_written() {
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("ch NOT IN ('a', NULL, 1)")),
        "(\"ch\" not in ('a', null, 1::integer))"
    );
    assert_eq!(
        super::oracle::render_expr_sql(&expr_of("a BETWEEN 1 AND 'x'")),
        "(\"a\" between 1::integer and 'x'::text)"
    );
}

const I4: ValueType = ValueType::Integer(IntWidth::Int4);
const F4: ValueType = ValueType::Float(FloatWidth::Float4);
const F8: ValueType = ValueType::Float(FloatWidth::Float8);
const T: ValueType = ValueType::Text;

fn columns() -> HashMap<String, ValueType> {
    [
        ("i", I4),
        ("i8", ValueType::Integer(IntWidth::Int8)),
        ("n", ValueType::Numeric),
        ("f4", F4),
        ("f8", F8),
        ("b", ValueType::Boolean),
        ("t", T),
        ("t2", T),
        ("d", ValueType::Other(PgType::Date)),
        ("ts", ValueType::Other(PgType::Timestamp)),
        ("tz", ValueType::Other(PgType::TimestampTz)),
        ("u", ValueType::Uuid),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

fn check(text: &str) -> Result<(), ValidationError> {
    validate(
        &parse_text(text).unwrap_or_else(|e| panic!("`{text}`: {e}")),
        &columns(),
    )
}

#[track_caller]
fn assert_admitted(text: &str) {
    if let Err(e) = check(text) {
        panic!("`{text}` was refused: {e}");
    }
    let def = parse_text(text).unwrap();
    assert_eq!(
        super::validate::infer_field_types(&def, &columns()).unwrap()["x"],
        ValueType::Boolean,
        "`{text}`"
    );
}

#[track_caller]
fn assert_refused(text: &str, needle: &str) {
    match check(text) {
        Ok(()) => panic!("`{text}` was admitted"),
        Err(e) => assert!(
            e.to_string().contains(needle),
            "`{text}` was refused without naming `{needle}`: {e}"
        ),
    }
}

/// Each element and bound is admitted exactly where the comparison it
/// stands for is (rules 3, 4, 9).
#[test]
fn elements_and_bounds_type_as_their_comparisons() {
    for text in [
        "i IN (1, 2, i8, n + 1)",
        "n NOT IN (1, 2.5)",
        "t IN ('a', t2, COALESCE(t2, 'z'))",
        "b IN (TRUE, NULL)",
        "i NOT IN (1, NULL)",
        "NULL IN (1, 2)",
        "d IN (DATE '2024-01-01', d)",
        "u IN (u)",
        "f8 IN (1.5, 2, f4)",
        "f4 IN (REAL '1.5', f4)",
        // One element is one `=`.
        "f4 IN (1.5)",
        "NULL IN (1)",
        "NULL IN (i, 1, 2.5, f8)",
        "i BETWEEN 1 AND n",
        "f8 NOT BETWEEN 0.5 AND f4",
        "d BETWEEN DATE '2024-01-01' AND d",
        "ts BETWEEN NULL AND ts",
        "i IN (1) = (i BETWEEN 1 AND 2)",
    ] {
        assert_admitted(text);
    }
    // Each comparison's own refusal, naming the construct.
    for (text, needle) in [
        (
            "i IN (1, 'a')",
            "'IN' cannot compare a integer value with a text value",
        ),
        ("i NOT IN (t)", "'NOT IN' cannot compare"),
        ("t BETWEEN 'a' AND 'b'", "orders text with 'BETWEEN'"),
        ("i IN (1, f8)", "using 'IN'; a float and an exact numeric"),
        (
            "d NOT BETWEEN d AND ts",
            "using 'NOT BETWEEN'; Postgres converts one side",
        ),
        ("ts IN (tz)", "using 'IN'"),
        ("i IN (b)", "'IN' cannot compare"),
        ("NULL BETWEEN NULL AND NULL", "orders text"),
    ] {
        assert_refused(text, needle);
    }
}

/// Where Postgres's common-type reading of a list (two or more elements
/// that read no column) differs from comparing each element on its own,
/// the list is refused (rule 3): a `real` beside an exact number rounds the
/// number to `real` (`f4 IN (1.1, 2.2)` is `f4 = ANY ('{1.1,2.2}'::real[])`,
/// where `f4 = 1.1` compares as `double precision`), and a `NULL` operand
/// converts a string element to the elements' common type (`NULL IN (1,
/// 'a')` fails in Postgres, where `NULL = 'a'` is `NULL`).
#[test]
fn a_list_whose_common_type_reads_differently_is_refused() {
    for text in [
        "f4 IN (1.1, 2.2)",
        "f4 NOT IN (16777217, 1)",
        "1.1 IN (REAL '1.1', REAL '2')",
        "f4 IN (f4, 1)",
        "f4 IN (f8, 1)",
        "NULL IN (1, 'a')",
        "NULL IN (t, 1)",
    ] {
        let found = check(text).expect_err(text);
        assert!(
            matches!(found, ValidationError::InListCommonType { .. }),
            "`{text}`: {found}"
        );
        assert!(found.to_string().starts_with("T0252"), "{found}");
    }
}

/// A live text column, of type `type_oid`.
fn text_column(type_oid: u32) -> JoinColumnInfo {
    JoinColumnInfo {
        value_type: T,
        pg_type: String::new(),
        type_oid,
        typmod: -1,
        collation_oid: 0,
        collation: None,
        collation_deterministic: true,
        is_enum: false,
        enum_labels: None,
        copy_type: String::new(),
        copy_collation: None,
        trellis_target: false,
    }
}

/// `ch` is `char(4)`, `nm` `name`, `t` `text`, `vc` `varchar`, `st` an enum
/// with labels `open` and `closed`, `i` an `integer`.
fn live(text: &str) -> Result<(), ValidationError> {
    let status = ValueType::Other(PgType::Enum("enum:public.status"));
    let mut columns: HashMap<String, JoinColumnInfo> = [
        ("ch", text_column(oid::BPCHAR)),
        ("nm", text_column(oid::NAME)),
        ("t", text_column(oid::TEXT)),
        ("vc", text_column(oid::VARCHAR)),
    ]
    .into_iter()
    .map(|(name, info)| (name.to_string(), info))
    .collect();
    let mut enum_info = text_column(16_400);
    enum_info.value_type = status;
    enum_info.is_enum = true;
    enum_info.enum_labels = Some(vec!["open".to_string(), "closed".to_string()]);
    columns.insert("st".to_string(), enum_info);
    let mut int_info = text_column(oid::INT4);
    int_info.value_type = I4;
    columns.insert("i".to_string(), int_info);
    validate_live(
        &parse(&format!("TRANSFORM t FROM s SELECT i AS i, {text} AS x"))
            .unwrap_or_else(|e| panic!("`{text}`: {e}")),
        &columns,
    )
}

/// A `char(n)` or `name` operand converts the list's string elements to its
/// own type (`ch IN ('a', COALESCE('b ', 'x'))` is
/// `ch = ANY ('{a,"b "}'::bpchar[])`, true for a `ch` of `'b'`), where
/// `ch = COALESCE('b ', 'x')` compares as `text`, false: refused unless every element is a
/// string literal, a `NULL` or of the operand's own type.
#[test]
fn a_padded_operand_with_another_string_type_is_refused() {
    for text in [
        "ch IN ('a', 'b ')",
        "ch NOT IN ('a', NULL, ch)",
        "nm IN ('a', 'b')",
        "t IN ('a', COALESCE('b ', 'x'), ch)",
        "vc IN (COALESCE('b ', 'x'), 'b')",
        "'a' IN (ch, 'b', 'c')",
        "COALESCE(t, 'x') IN (ch, nm)",
        // One element is one `=`: Postgres has no list to type.
        "ch IN (COALESCE('b ', 'x'))",
        "nm NOT IN (t)",
    ] {
        assert_eq!(live(text), Ok(()), "`{text}`");
    }
    for text in [
        "ch IN ('a', COALESCE('b ', 'x'))",
        "ch IN (t, 'a')",
        "ch NOT IN (vc, 'a')",
        "COALESCE(ch, 'x') IN ('a', CASE WHEN i = 1 THEN 'b ' END)",
        "nm IN ('a', NULLIF(t, 'x'))",
        "nm IN (ch, 'a')",
    ] {
        assert!(
            matches!(live(text), Err(ValidationError::InListCommonType { .. })),
            "`{text}`: {:?}",
            live(text)
        );
    }
}

/// An enum column takes label literals in `IN` and `NOT IN` under `=`'s
/// rule (rule 7): each must be a label. `BETWEEN` over an enum stays
/// refused, like the orderings.
#[test]
fn an_enum_takes_label_literals_in_a_list() {
    for text in [
        "st IN ('open', 'closed')",
        "st NOT IN ('open')",
        "'open' IN (st)",
    ] {
        assert_eq!(live(text), Ok(()), "`{text}`");
    }
    for text in ["st IN ('open', 'Open')", "st NOT IN ('nope')"] {
        assert!(
            matches!(
                live(text),
                Err(ValidationError::EnumLiteralNotALabel { .. })
            ),
            "`{text}`: {:?}",
            live(text)
        );
    }
    // An enum beside an enum, a `NULL` included, is no comparison this
    // grammar admits, in a list as beside `=`.
    assert!(live("st = NULL").is_err());
    assert!(live("st IN ('open', NULL)").is_err());
    assert!(live("st BETWEEN 'open' AND 'closed'").is_err());
    assert!(live("st BETWEEN st AND st").is_err());
}

/// The operators are comparisons, so a `WHERE` takes them wherever it
/// takes the comparisons they stand for (rule 5), and a literal past a
/// float's range is refused there as it is beside `=`.
#[test]
fn a_where_takes_them_where_it_takes_their_comparisons() {
    for predicate in [
        "i IN (1, 2)",
        "t NOT IN ('a')",
        "i NOT BETWEEN 1 AND 10 OR b",
    ] {
        let text = format!("TRANSFORM t FROM s SELECT i AS i WHERE {predicate}");
        assert_eq!(
            validate(&parse(&text).unwrap(), &columns()),
            Ok(()),
            "{text}"
        );
    }
    let huge = format!("1{}", "0".repeat(400));
    let text = format!("TRANSFORM t FROM s SELECT i AS i WHERE f8 BETWEEN 0 AND {huge}");
    assert!(
        matches!(
            validate(&parse(&text).unwrap(), &columns()),
            Err(ValidationError::PredicateNotTotal { .. })
        ),
        "{text}"
    );
}

/// A definition using either operator has the floor of the release they
/// arrive in.
#[test]
fn the_operators_raise_the_floor() {
    use super::floor::{ExprNode, Feature, Release};
    assert_eq!(
        Feature::Expr(ExprNode::InList).since(),
        Some(Release::new(0, 4, 0))
    );
    assert_eq!(
        Feature::Expr(ExprNode::Between).since(),
        Some(Release::new(0, 4, 0))
    );
}
