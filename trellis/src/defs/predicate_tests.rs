//! The `WHERE` predicate's parse, floor, validation and read set (#1991
//! step A): everything up to the key spaces' apply paths, which refuse a
//! filter until their own step lands.

use std::collections::HashMap;

use super::ast::{Expr, Operator, Predicate, ValueType, render_definition_text};
use super::floor::{Floors, Release};
use super::validate::{ValidationError, validate};
use super::{grammar, parse};
use crate::integer::IntWidth;

fn columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("qty".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("amount".to_string(), ValueType::Numeric),
        ("status".to_string(), ValueType::Text),
        ("note".to_string(), ValueType::Text),
        ("active".to_string(), ValueType::Boolean),
        ("g".to_string(), ValueType::Text),
    ])
}

fn validated(text: &str) -> Result<(), ValidationError> {
    validate(&parse(text).expect("parses"), &columns())
}

/// The refusal define gives `text`, past the key-space refusal: the
/// predicate's checks come before it, so a refusal here is the predicate's.
fn predicate_refusal(text: &str) -> Option<ValidationError> {
    match validated(text) {
        Err(ValidationError::FilterNotYetSupported { .. }) | Ok(()) => None,
        Err(other) => Some(other),
    }
}

#[test]
fn only_an_omitted_where_or_the_lone_true_is_the_unfiltered_predicate() {
    for text in [
        "TRANSFORM t FROM s SELECT qty AS q",
        "TRANSFORM t FROM s SELECT qty AS q WHERE TRUE",
        "TRANSFORM t FROM s SELECT qty AS q WHERE true",
    ] {
        assert_eq!(parse(text).unwrap().predicate, Predicate::True, "{text}");
    }
    for (text, expr) in [
        (
            "TRANSFORM t FROM s SELECT qty AS q WHERE (TRUE)",
            Expr::BooleanLiteral(true),
        ),
        ("TRANSFORM t FROM s SELECT qty AS q WHERE NULL", Expr::Null),
        (
            "TRANSFORM t FROM s SELECT qty AS q WHERE qty > 0",
            Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("qty".to_string())),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        ),
        (
            "TRANSFORM t FROM s SELECT qty AS q WHERE TRUE AND active",
            Expr::BinaryOp {
                op: Operator::And,
                lhs: Box::new(Expr::BooleanLiteral(true)),
                rhs: Box::new(Expr::Column("active".to_string())),
            },
        ),
        // A quoted `"TRUE"` names a column, as in Postgres.
        (
            "TRANSFORM t FROM s SELECT qty AS q WHERE \"TRUE\"",
            Expr::Column("TRUE".to_string()),
        ),
    ] {
        assert_eq!(
            parse(text).unwrap().predicate,
            Predicate::Filter(expr),
            "{text}"
        );
    }
}

#[test]
fn a_filter_renders_back_to_the_same_predicate() {
    for text in [
        "TRANSFORM t FROM s SELECT qty AS q WHERE (TRUE)",
        "TRANSFORM t FROM s SELECT qty AS q WHERE qty > 0 AND (status = 'open' OR status IS NULL)",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE NOT active",
        "TRANSFORM t FROM s SELECT qty AS q WHERE NULL",
        "TRANSFORM t FROM s SELECT qty AS q WHERE TRUE",
    ] {
        let def = parse(text).unwrap();
        let rendered = render_definition_text(&def);
        assert_eq!(
            parse(&rendered).unwrap(),
            def,
            "{text} rendered as {rendered}"
        );
    }
}

/// Grammar 1 had no `TRUE` literal and read only the token after `WHERE`,
/// so its text keeps that rule: a filter there is refused, as it was.
#[test]
fn grammar_1_keeps_the_lone_true_rule() {
    let lone = "TRANSFORM t FROM s SELECT qty AS q WHERE TRUE";
    assert_eq!(
        grammar::load_in(lone, 1, 1, 2).unwrap().predicate,
        Predicate::True
    );
    let filtered = "TRANSFORM t FROM s SELECT qty AS q WHERE qty > 0";
    assert!(matches!(
        grammar::load_in(filtered, 1, 1, 2),
        Err(super::error::ParseError::UnsupportedPredicate { .. })
    ));
    // So a filter's text is stamped with the current grammar, which an older
    // binary refuses to read.
    assert_eq!(grammar::stamp_in(filtered, 1, 2), 2);
    assert_eq!(grammar::stamp_in(lone, 1, 2), 1);
}

#[test]
fn a_filter_records_this_releases_floor_and_the_lone_true_does_not() {
    let floors = Floors::of_this_binary();
    let floor = |text: &str| floors.of(&parse(text).unwrap(), false, Some(&[]));
    let filter = Release::new(0, 4, 0);
    assert_eq!(
        floor("TRANSFORM t FROM s SELECT qty AS q"),
        super::floor::BASELINE
    );
    assert_eq!(
        floor("TRANSFORM t FROM s SELECT qty AS q WHERE TRUE"),
        super::floor::BASELINE
    );
    for text in [
        "TRANSFORM t FROM s SELECT qty AS q WHERE (TRUE)",
        "TRANSFORM t FROM s SELECT qty AS q WHERE FALSE",
        "TRANSFORM t FROM s SELECT qty AS q WHERE qty > 0",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE active",
    ] {
        assert_eq!(floor(text), filter, "{text}");
    }
}

#[test]
fn a_predicate_is_typed_and_read_as_postgres_reads_it() {
    // Not boolean.
    assert_eq!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE qty"),
        Some(ValidationError::PredicateNotBoolean {
            found: ValueType::Integer(IntWidth::Int4)
        })
    );
    // An aggregate call.
    assert_eq!(
        predicate_refusal(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE SUM(qty) > 1"
        ),
        Some(ValidationError::AggregateInPredicate {
            function: "SUM".to_string()
        })
    );
    // A field's name is no column of the predicate.
    assert_eq!(
        predicate_refusal("TRANSFORM t FROM s SELECT amount AS amt WHERE amt > 1"),
        Some(ValidationError::PredicateColumnNotFound {
            column: "amt".to_string()
        })
    );
    assert!(matches!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE other.qty > 1"),
        Some(ValidationError::QualifiedColumnNotFromSource { .. })
    ));
    assert!(matches!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE s.nope > 1"),
        Some(ValidationError::PredicateColumnNotFound { .. })
    ));
    // A field named like a source column: the predicate reads the column.
    for text in [
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS status WHERE status = 'open'",
        "TRANSFORM t FROM s SELECT qty AS qty, amount AS amount WHERE s.qty > 0",
        "TRANSFORM t FROM s SELECT qty AS q WHERE FALSE",
        "TRANSFORM t FROM s SELECT qty AS q WHERE NULL",
        "TRANSFORM t FROM s SELECT qty AS q WHERE 1 = 1",
    ] {
        assert_eq!(predicate_refusal(text), None, "{text}");
    }
    // The expression rules hold in a predicate: a zero divisor, a bad typed
    // literal.
    assert!(matches!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE qty % 0 = 1"),
        Some(ValidationError::ConstantDivisorZero { .. })
    ));
    assert!(matches!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE DATE '2024-1-5' IS NULL"),
        Some(ValidationError::InvalidTypedLiteral { .. })
    ));
}

#[test]
fn a_predicate_that_can_raise_is_refused_by_name() {
    match predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE amount / qty > 5") {
        Some(ValidationError::PredicateNotTotal { construct, reason }) => {
            assert_eq!(construct, "(amount / qty)");
            assert!(reason.contains("divide by zero"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        predicate_refusal("TRANSFORM t FROM s SELECT qty AS q WHERE status = note"),
        Some(ValidationError::PredicateNotTotal { .. })
    ));
}

/// Until each key space's step lands (#1991), define refuses its filter by
/// name, after the predicate's own checks. A plain aggregate and a join
/// aggregate apply one.
#[test]
fn every_key_space_refuses_a_filter_until_its_step_lands() {
    assert_eq!(
        validated("TRANSFORM t FROM s SELECT qty AS q WHERE qty > 0"),
        Err(ValidationError::FilterNotYetSupported { key_space: "1-1" }),
    );
    for text in [
        "TRANSFORM t FROM s SELECT qty AS q",
        "TRANSFORM t FROM s SELECT qty AS q WHERE TRUE",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n, SUM(qty) AS total, \
         AVG(amount) AS av WHERE qty > 0",
    ] {
        assert_eq!(validated(text), Ok(()), "{text}");
    }
    // Both `JOIN` key spaces filter (#2041, #2042): their validators check
    // the predicate.
    for text in [
        "TRANSFORM t FROM s JOIN u ON s.k = u.k GROUP BY s.g \
         SELECT s.g AS g, COUNT(*) AS n WHERE s.qty > 0",
        "TRANSFORM t FROM s JOIN u ON s.k = u.k SELECT s.a AS a WHERE u.b > 0",
    ] {
        let def = parse(text).unwrap();
        assert_eq!(
            super::validate::refuse_unsupported_filter(&def),
            Ok(()),
            "{text}"
        );
    }
}

/// A plain aggregate's `MIN`/`MAX` is refused under any `WHERE` other than
/// `TRUE`, `WHERE (TRUE)` included, naming the field; unfiltered, it stays
/// admitted.
#[test]
fn a_filtered_aggregate_refuses_min_and_max() {
    for (text, field, function) in [
        (
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, MIN(qty) AS lo WHERE qty > 0",
            "lo",
            "MIN",
        ),
        (
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n, \
             max(amount) + 1 AS hi WHERE (TRUE)",
            "hi",
            "MAX",
        ),
    ] {
        assert_eq!(
            validated(text),
            Err(ValidationError::FilteredExtreme {
                field: field.to_string(),
                function: function.to_string(),
            }),
            "{text}"
        );
    }
    for text in [
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, MIN(qty) AS lo",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, MIN(qty) AS lo WHERE TRUE",
    ] {
        assert_eq!(validated(text), Ok(()), "{text}");
    }
}

/// The predicate's columns are read whatever the fields read: imaged by
/// capture, cast out of an image, watched for drift and checked for
/// collation, and never charged to a field.
#[test]
fn the_predicates_columns_join_the_read_set() {
    let def = parse(
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS status \
         WHERE status = 'open' AND s.active",
    )
    .unwrap();
    let read = super::oracle::predicate_columns(&def);
    assert_eq!(read.into_iter().collect::<Vec<_>>(), ["active", "status"]);
    let captured = crate::capture::columns::columns_read(&def, None);
    assert!(
        captured.contains("status") && captured.contains("active"),
        "{captured:?}"
    );
    // The fields' own set stays the fields'.
    assert!(!super::oracle::referenced_source_columns(&def).contains("active"));

    // A `JOIN`'s predicate reads each side's columns through its table.
    let join = parse(
        "TRANSFORM t FROM s JOIN u ON s.k = u.k GROUP BY s.g SELECT s.g AS g, COUNT(*) AS n \
         WHERE s.qty > 0 AND u.flag",
    )
    .unwrap();
    let from = super::join::side_columns(&join, super::join::Side::From);
    let joined = super::join::side_columns(&join, super::join::JOINED);
    assert!(from.contains("qty") && !from.contains("flag"), "{from:?}");
    assert!(
        joined.contains("flag") && !joined.contains("qty"),
        "{joined:?}"
    );

    // A text comparison in the predicate is checked for its collation like
    // a field's.
    let def = parse("TRANSFORM t FROM s SELECT qty AS q WHERE status = 'x'").unwrap();
    let reads = super::validate::collation_refusing_function_reads(&def);
    assert!(
        reads
            .iter()
            .any(|r| r.field == "WHERE" && r.column == "status"),
        "{reads:?}"
    );
}

/// The target's `SELECT` carries the predicate, before any `GROUP BY`.
#[test]
fn the_target_select_carries_the_where() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY g SELECT g AS g, COUNT(*) AS n WHERE qty > 0").unwrap();
    let sql = super::oracle::render_aggregate_select_sql(&def);
    assert!(
        sql.contains(" where (\"qty\" > 0") && sql.ends_with("group by \"g\""),
        "{sql}"
    );
}

#[test]
fn a_field_level_refusal_over_the_predicate_names_the_where() {
    let err = validated("TRANSFORM t FROM s SELECT qty AS q WHERE CAST(amount AS integer) > 0")
        .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("the WHERE predicate casts a numeric value")
            && !message.contains("calculated field"),
        "{message}"
    );
}
