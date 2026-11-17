//! Aggregate-call modifiers (#1739): Postgres parses `DISTINCT`/`ALL` before
//! the arguments, `ORDER BY` after them and `FILTER (WHERE ...)` after the
//! `)`. Trellis supports none, so each gets a named refusal. Text Postgres
//! rejects keeps its existing error (ADR-0022).

use super::error::ParseError;
use super::parse;

fn agg(call: &str) -> String {
    format!("TRANSFORM t FROM s GROUP BY g SELECT g AS g, {call} AS n")
}

fn modifier(call: &str) -> (String, &'static str) {
    match parse(&agg(call)) {
        Err(ParseError::UnsupportedAggregateModifier { function, modifier }) => {
            (function, modifier)
        }
        other => panic!("{call}: expected UnsupportedAggregateModifier, got {other:?}"),
    }
}

/// Postgres accepts each of these as syntax (checked on a scratch Postgres).
#[test]
fn modifiers_in_aggregate_calls_are_refused_by_name() {
    for (call, function, expected) in [
        ("COUNT(DISTINCT x)", "COUNT", "DISTINCT"),
        ("count(distinct x)", "COUNT", "DISTINCT"),
        ("Count( Distinct  x )", "COUNT", "DISTINCT"),
        ("COUNT(DISTINCT s.x + 1)", "COUNT", "DISTINCT"),
        ("COUNT(DISTINCT x, y)", "COUNT", "DISTINCT"),
        ("COUNT(ALL x)", "COUNT", "ALL"),
        ("count(all x)", "COUNT", "ALL"),
        ("SUM(DISTINCT x)", "SUM", "DISTINCT"),
        ("MIN(ALL x)", "MIN", "ALL"),
        ("AVG(DISTINCT x)", "AVG", "DISTINCT"),
        ("COUNT(x ORDER BY y)", "COUNT", "ORDER BY"),
        ("SUM(x order by y desc)", "SUM", "ORDER BY"),
        ("COUNT(*) FILTER (WHERE x > 1)", "COUNT", "FILTER"),
        ("COUNT(x) filter (where x > 1)", "COUNT", "FILTER"),
        ("SUM(x) FILTER (WHERE x > 1)", "SUM", "FILTER"),
    ] {
        assert_eq!(modifier(call), (function.to_string(), expected), "{call}");
    }
}

#[test]
fn count_distinct_names_the_two_transform_composition() {
    let err = parse(&agg("COUNT(DISTINCT x)")).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("GROUP BY g, x"), "{text}");
    assert!(text.contains("COUNT(x)"), "{text}");
    let all = parse(&agg("COUNT(ALL x)")).unwrap_err().to_string();
    assert!(all.contains("drop it"), "{all}");
}

#[test]
fn a_join_aggregate_refuses_them_too() {
    let err = parse(
        "TRANSFORM t FROM a JOIN b ON a.k = b.k GROUP BY a.g \
         SELECT a.g AS g, COUNT(DISTINCT b.x) AS n",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ParseError::UnsupportedAggregateModifier {
            modifier: "DISTINCT",
            ..
        }
    ));
}

/// Postgres rejects these (`count(distinct)` is a syntax error), and a quoted
/// `"distinct"` is an ordinary column: neither reads as the modifier.
#[test]
fn text_without_a_modifier_keeps_its_existing_reading() {
    for call in ["COUNT(DISTINCT)", "COUNT(ALL)", "SUM(distinct, x)"] {
        let err = parse(&agg(call)).unwrap_err();
        assert!(
            matches!(err, ParseError::ReservedWord { .. }),
            "{call}: {err:?}"
        );
    }
    for call in ["COUNT(\"distinct\")", "COUNT(\"all\")", "SUM(\"distinct\")"] {
        if let Err(err) = parse(&agg(call)) {
            panic!("{call}: {err}");
        }
    }
    // A reserved word as a plain column is still the reserved-word error.
    let err = parse("TRANSFORM t FROM s SELECT distinct AS x").unwrap_err();
    assert!(matches!(err, ParseError::ReservedWord { .. }), "{err:?}");
    // A non-aggregate function is not an aggregate call.
    let err = parse("TRANSFORM t FROM s SELECT ABS(DISTINCT x) AS y").unwrap_err();
    assert!(matches!(err, ParseError::ReservedWord { .. }), "{err:?}");
}

/// `FILTER` is not a reserved word: as a column or a field name it stays an
/// identifier, as in Postgres. Only `FILTER (` right after a call's `)` is
/// the clause.
#[test]
fn filter_as_a_name_is_an_identifier() {
    for statement in [
        "TRANSFORM t FROM s SELECT filter AS f",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, count(x) AS filter",
        "TRANSFORM t FROM s GROUP BY g SELECT g AS g, count(filter) AS n",
    ] {
        if let Err(err) = parse(statement) {
            panic!("{statement}: {err}");
        }
    }
}
