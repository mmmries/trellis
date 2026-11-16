//! The totality classifier (ADR-0004, `defs::totality`) as a property: every
//! `WHERE` define admits evaluates without an error over every row of a
//! table holding each type's extremes. Predicates are generated at random
//! over the grammar, typed by construction; define's own validation decides
//! which are admitted, and Postgres evaluates each admitted one.
//!
//! A refused predicate needs no check: the class is strict, so it refuses
//! some constructs that never raise here. The controls hold the table to the
//! property: a construct the classifier refuses does raise on it.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::validate::ValidationError;
use trellis::defs::{PgType, ValueType, parse, validate};
use trellis::float::FloatWidth;
use trellis::integer::IntWidth;

use crate::support::pipeline::connect_with;

/// Each type's extremes, one row each, and a row of `NULL`s. `t` and `u`
/// carry two different non-default collations, so comparing them raises
/// `42P22` on a row where both are non-`NULL`.
const SETUP: &str = "\
    create table public.x (id int primary key, i2 smallint, i4 integer, i8 bigint, \
        n numeric, f4 real, f8 double precision, t text collate \"C\", \
        u text collate \"POSIX\", b boolean, d date, ts timestamp, tz timestamptz); \
    insert into public.x values \
      (1, 32767, 2147483647, 9223372036854775807, repeat('9', 131072)::numeric, \
       '3.4028235e38', '1.7976931348623157e308', repeat('z', 100000), 'zz', true, \
       '5874897-12-31', '294276-12-31 23:59:59.999999', '294276-12-30 23:59:59.999999+00'), \
      (2, -32768, -2147483648, -9223372036854775808, ('-' || repeat('9', 131072))::numeric, \
       '-3.4028235e38', '-1.7976931348623157e308', '', '', false, \
       '4714-11-24 BC', '4714-11-24 00:00:00 BC', '4714-11-24 00:00:00+00 BC'), \
      (3, 0, 0, 0, (repeat('9', 131072) || '.5')::numeric, '1.4e-45', '4.9e-324', 'a', 'b', true, \
       '4714-11-30 BC', '4714-12-31 00:00:00 BC', '4001-06-01 00:00:00+00 BC'), \
      (4, 0, 0, 0, ('0.' || repeat('0', 16382) || '1')::numeric, 'NaN', 'NaN', 'abc', 'abd', \
       null, 'infinity', 'infinity', 'infinity'), \
      (5, 1, -1, 1, 'NaN', 'Infinity', '-Infinity', 'x', 'x', false, \
       '-infinity', '-infinity', '-infinity'), \
      (6, null, null, null, 1e50, null, null, null, null, null, null, null, null), \
      (7, null, null, null, '-Infinity', null, null, null, null, null, null, null, null)";

fn columns() -> HashMap<String, ValueType> {
    let mut columns = HashMap::from([
        ("i2", ValueType::Integer(IntWidth::Int2)),
        ("i4", ValueType::Integer(IntWidth::Int4)),
        ("i8", ValueType::Integer(IntWidth::Int8)),
        ("n", ValueType::Numeric),
        ("f4", ValueType::Float(FloatWidth::Float4)),
        ("f8", ValueType::Float(FloatWidth::Float8)),
        ("t", ValueType::Text),
        ("u", ValueType::Text),
        ("b", ValueType::Boolean),
        ("d", ValueType::Other(PgType::Date)),
        ("ts", ValueType::Other(PgType::Timestamp)),
        ("tz", ValueType::Other(PgType::TimestampTz)),
    ]);
    columns.insert("id", ValueType::Integer(IntWidth::Int4));
    columns
        .into_iter()
        .map(|(name, value_type)| (name.to_string(), value_type))
        .collect()
}

/// A fixed-seed xorshift: the run is the same every time.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }

    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[self.below(items.len())]
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Bool,
    Int,
    Numeric,
    Float,
    Text,
    Date,
    Timestamp,
}

/// A random expression of `kind`, in the grammar's text.
fn expr(rng: &mut Rng, kind: Kind, depth: u32) -> String {
    let leaf = depth == 0 || rng.below(3) == 0;
    let sub = |rng: &mut Rng, kind| expr(rng, kind, depth.saturating_sub(1));
    match kind {
        Kind::Int if leaf => rng
            .pick(&["i2", "i4", "i8", "0", "1", "-1", "7", "2147483647", "id"])
            .to_string(),
        Kind::Int => match rng.below(12) {
            0..=2 => {
                let op = rng.pick(&["+", "-", "*"]);
                format!("({} {op} {})", sub(rng, Kind::Int), sub(rng, Kind::Int))
            }
            3 => format!(
                "({} % {})",
                sub(rng, Kind::Int),
                rng.pick(&["10", "-1", "i4"])
            ),
            4 => format!("(-{})", sub(rng, Kind::Int)),
            5 => format!("ABS({})", sub(rng, Kind::Int)),
            6 => format!(
                "{}({})",
                rng.pick(&["CHAR_LENGTH", "OCTET_LENGTH"]),
                sub(rng, Kind::Text)
            ),
            7 => format!("STRPOS({}, {})", sub(rng, Kind::Text), sub(rng, Kind::Text)),
            8 => format!("REGEXP_COUNT({}, 'a')", sub(rng, Kind::Text)),
            9 => format!("CAST({} AS bigint)", sub(rng, Kind::Int)),
            10 => format!("COALESCE({}, {})", sub(rng, Kind::Int), sub(rng, Kind::Int)),
            _ => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                sub(rng, Kind::Bool),
                sub(rng, Kind::Int),
                sub(rng, Kind::Int)
            ),
        },
        Kind::Numeric if leaf => rng
            .pick(&["n", "1.5", "0.001", "99999999999999999999"])
            .to_string(),
        Kind::Numeric => match rng.below(10) {
            0 => {
                let op = rng.pick(&["+", "-", "*", "/"]);
                format!(
                    "({} {op} {})",
                    sub(rng, Kind::Numeric),
                    sub(rng, Kind::Numeric)
                )
            }
            1 => format!(
                "({} % {})",
                sub(rng, Kind::Numeric),
                rng.pick(&["0.3", "n"])
            ),
            2 => format!("(-{})", sub(rng, Kind::Numeric)),
            3 => format!(
                "{}({})",
                rng.pick(&["ABS", "CEIL", "FLOOR", "ROUND", "TRUNC"]),
                sub(rng, Kind::Numeric)
            ),
            4 => format!(
                "{}({}, 2)",
                rng.pick(&["ROUND", "TRUNC"]),
                sub(rng, Kind::Numeric)
            ),
            5 => format!("CAST({} AS numeric)", sub(rng, Kind::Int)),
            6 => format!(
                "EXTRACT({} FROM {})",
                rng.pick(&["year", "epoch", "dow", "julian"]),
                if rng.below(2) == 0 { "d" } else { "ts" }
            ),
            7 => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Numeric),
                sub(rng, Kind::Int)
            ),
            8 => format!("({} + {})", sub(rng, Kind::Int), sub(rng, Kind::Numeric)),
            _ => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                sub(rng, Kind::Bool),
                sub(rng, Kind::Numeric),
                sub(rng, Kind::Int)
            ),
        },
        Kind::Float if leaf => rng.pick(&["f4", "f8", "REAL '1.5'"]).to_string(),
        Kind::Float => match rng.below(7) {
            0 => {
                let op = rng.pick(&["+", "-", "*", "/"]);
                format!("({} {op} {})", sub(rng, Kind::Float), sub(rng, Kind::Float))
            }
            1 => format!("(-{})", sub(rng, Kind::Float)),
            2 => format!(
                "{}({})",
                rng.pick(&["ABS", "CEIL", "FLOOR", "TRUNC"]),
                sub(rng, Kind::Float)
            ),
            3 => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Float),
                sub(rng, Kind::Int)
            ),
            4 => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Float),
                sub(rng, Kind::Numeric)
            ),
            5 => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Float),
                rng.pick(&[
                    "1.5",
                    "1000000000000000000000000000000000000000",
                    "0.000001"
                ])
            ),
            _ => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                sub(rng, Kind::Bool),
                sub(rng, Kind::Float),
                sub(rng, Kind::Float)
            ),
        },
        Kind::Text if leaf => rng.pick(&["t", "u", "'abc'", "''"]).to_string(),
        Kind::Text => match rng.below(3) {
            0 => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Text),
                sub(rng, Kind::Text)
            ),
            1 => format!("NULLIF({}, {})", sub(rng, Kind::Text), sub(rng, Kind::Text)),
            _ => format!(
                "CASE WHEN {} THEN {} ELSE {} END",
                sub(rng, Kind::Bool),
                sub(rng, Kind::Text),
                sub(rng, Kind::Text)
            ),
        },
        Kind::Date if leaf => rng.pick(&["d", "DATE '2024-01-05'"]).to_string(),
        Kind::Date => match rng.below(2) {
            0 => format!("({} + {})", sub(rng, Kind::Date), sub(rng, Kind::Int)),
            _ => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Date),
                sub(rng, Kind::Date)
            ),
        },
        Kind::Timestamp if leaf => rng
            .pick(&[
                "ts",
                "TIMESTAMP '2024-01-05 00:00:00'",
                "tz AT TIME ZONE 'UTC'",
            ])
            .to_string(),
        Kind::Timestamp => match rng.below(3) {
            0 => format!("CAST({} AS timestamp)", sub(rng, Kind::Date)),
            1 => format!(
                "DATE_TRUNC('{}', {})",
                rng.pick(&["second", "day", "week", "month", "year", "millennium"]),
                sub(rng, Kind::Timestamp)
            ),
            _ => format!(
                "COALESCE({}, {})",
                sub(rng, Kind::Timestamp),
                sub(rng, Kind::Timestamp)
            ),
        },
        Kind::Bool if leaf => rng.pick(&["b", "TRUE", "FALSE", "NULL"]).to_string(),
        Kind::Bool => {
            let comparisons = [
                Kind::Int,
                Kind::Numeric,
                Kind::Float,
                Kind::Text,
                Kind::Date,
                Kind::Timestamp,
            ];
            match rng.below(9) {
                0..=3 => {
                    let kind = comparisons[rng.below(comparisons.len())];
                    let op = rng.pick(&["=", "<>", "<", ">=", "IS DISTINCT FROM"]);
                    let rhs = match (kind, rng.below(3)) {
                        (Kind::Float, 0) => rng
                            .pick(&[
                                "1.5",
                                &"9".repeat(310),
                                "0.00000000000000000000000000000000000000000000001",
                            ])
                            .to_string(),
                        (Kind::Int, 0) => sub(rng, Kind::Numeric),
                        _ => sub(rng, kind),
                    };
                    format!("({} {op} {rhs})", sub(rng, kind))
                }
                4 => format!("({} AND {})", sub(rng, Kind::Bool), sub(rng, Kind::Bool)),
                5 => format!("({} OR {})", sub(rng, Kind::Bool), sub(rng, Kind::Bool)),
                6 => format!("(NOT {})", sub(rng, Kind::Bool)),
                7 => {
                    let kind = comparisons[rng.below(comparisons.len())];
                    format!("({} IS NULL)", sub(rng, kind))
                }
                _ => format!(
                    "COALESCE({}, {})",
                    sub(rng, Kind::Bool),
                    sub(rng, Kind::Bool)
                ),
            }
        }
    }
}

enum Verdict {
    Admitted,
    NotTotal,
    /// Refused by another rule (a type mismatch, say), or unparsable.
    Other,
}

fn verdict(predicate: &str, columns: &HashMap<String, ValueType>) -> Verdict {
    let Ok(def) = parse(&format!(
        "TRANSFORM y FROM x SELECT id AS k WHERE {predicate}"
    )) else {
        return Verdict::Other;
    };
    match validate(&def, columns) {
        // The predicate's checks run first; this refusal follows them until
        // the 1-1 key space applies a filter.
        Ok(()) | Err(ValidationError::FilterNotYetSupported { .. }) => Verdict::Admitted,
        Err(ValidationError::PredicateNotTotal { .. }) => Verdict::NotTotal,
        Err(_) => Verdict::Other,
    }
}

#[tokio::test]
async fn every_admitted_predicate_evaluates_over_every_extreme_without_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_with(db.dsn(), "").await;
    client.batch_execute(SETUP).await.expect("setup");
    let columns = columns();

    // The controls: each is refused, and raises on this table.
    for control in [
        "i4 + 1 > 0",
        "-i2 > 0",
        "n + 1 > 0",
        "CEIL(n) > 0",
        "f8 * 2 > 0",
        "1.0 / i4 > 0",
        "CAST(d AS timestamp) > ts",
        "d + 1 > d",
        "DATE_TRUNC('month', ts) > ts",
        "t = u",
        "COALESCE(f4, n) > 0",
    ] {
        assert!(
            matches!(verdict(control, &columns), Verdict::NotTotal),
            "{control} is refused"
        );
        let raised = client
            .query_one(
                &format!("select count(*) from public.x where ({control}) is true"),
                &[],
            )
            .await;
        assert!(raised.is_err(), "{control} raises on the extremes");
    }

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let (mut admitted, mut refused) = (0, 0);
    for _ in 0..3000 {
        let predicate = expr(&mut rng, Kind::Bool, 4);
        match verdict(&predicate, &columns) {
            Verdict::Admitted => {
                admitted += 1;
                if let Err(err) = client
                    .query_one(
                        &format!("select count(*) from public.x where ({predicate}) is true"),
                        &[],
                    )
                    .await
                {
                    panic!("admitted predicate raised: {predicate}: {err}");
                }
            }
            Verdict::NotTotal => refused += 1,
            Verdict::Other => {}
        }
    }
    // Both sides of the class are exercised.
    assert!(admitted > 300, "only {admitted} predicates admitted");
    assert!(
        refused > 300,
        "only {refused} predicates refused as not total"
    );
}
