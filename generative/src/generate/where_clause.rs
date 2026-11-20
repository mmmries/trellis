//! `WHERE` generation (#2043, #1991 step E): a drawn total predicate on the
//! definitions whose key space applies one, and a three-valued evaluator of
//! the drawn shapes, so the coverage report can sort each update into its
//! crossing bucket without a database ([`crate::run::Coverage::where_crossings`]).
//!
//! # Which definitions draw one
//!
//! [`filters`] names the key spaces that draw one: every key space but the
//! plain aggregate with `MIN`/`MAX` (define refuses an extreme under a
//! filter).
//!
//! # Seeds stay what they were
//!
//! The arm is derived from a digest of the drawn program ([`digest`]), never
//! drawn by proptest, so every strategy draws exactly what it drew before the
//! arm existed: a definition the arm leaves off is byte for byte the same
//! program. About half the filtering definitions get a predicate
//! ([`WHERE_SHARE`]).
//!
//! # The predicates
//!
//! Every template is total ([`docs/expressions.md`, "WHERE"]): comparisons
//! with a number or string literal, `[NOT] IN`, `[NOT] BETWEEN`,
//! `IS [NOT] NULL`, `AND`, `OR`, `NOT`,
//! `COALESCE` and `%` by a nonzero literal, over the columns the generators
//! update, so rows cross the predicate both ways. A few are degenerate
//! (`FALSE`, `NULL`, `(TRUE)`), which define accepts (#1991 rule 6). A plain
//! aggregate's and a plain 1-1's read their source's `c1`, `c2`, flag and
//! grain columns: a `MoveGroup` crosses and changes group at once. A 1-1
//! `JOIN`'s read `ja` and one or two joined tables: cross-table comparisons,
//! an anti-join (`jb.id IS NULL`) and a column of an unmatched `LEFT` row, so
//! a `b` change crosses every row on its key. A join aggregate's keep
//! ADR-0019's per-side rule ([`join_aggregate_predicate`]).

use std::collections::HashMap;

use trellis::dev::defs::ast::{
    Expr, JoinKind, KeySpace, Operator, Predicate, TransformDef, ValueType,
};

use crate::model::{Program, Table};

/// How many filtering definitions in 256 draw a predicate.
pub const WHERE_SHARE: u64 = 128;

/// Whether the generators draw a `WHERE` on `def`'s key space (the module
/// doc).
pub fn filters(def: &TransformDef) -> bool {
    match &def.key_space {
        // #2039: `MIN`/`MAX` under a filter is refused (`FilteredExtreme`).
        KeySpace::Aggregate { .. } => !def.fields.iter().any(|f| has_extreme(&f.expr)),
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) | KeySpace::Join(_) => true,
    }
}

fn has_extreme(expr: &Expr) -> bool {
    let mut found = false;
    expr.walk(&mut |e| {
        if let Expr::FunctionCall { name, .. } = e
            && (name == "MIN" || name == "MAX")
        {
            found = true;
        }
    });
    found
}

/// A stable digest of `program` (FNV-1a over its `Debug` text): the same
/// program always digests the same, in every process.
pub fn digest(program: &Program) -> u64 {
    let text = format!("{:?}", (&program.tables, &program.defs, &program.ops));
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// One step of SplitMix64: a well-mixed `u64` from `state`.
fn mix(state: u64) -> u64 {
    let mut z = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// `program` with a predicate on about half its filtering definitions
/// ([`filters`]), each derived from the program's [`digest`] and its index.
/// A definition already filtered, or one whose source the templates can't
/// read, is left as it is.
pub fn with_predicates(mut program: Program) -> Program {
    let digest = digest(&program);
    let tables = program.tables.clone();
    for (i, def) in program.defs.iter_mut().enumerate() {
        if !filters(def) || def.predicate != Predicate::True {
            continue;
        }
        let draw = mix(digest ^ mix(i as u64 + 1));
        if draw % 256 >= WHERE_SHARE {
            continue;
        }
        let draw = mix(draw);
        let expr = match &def.key_space {
            KeySpace::Aggregate { .. } | KeySpace::OneToOne => tables
                .iter()
                .find(|t| t.name == def.source)
                .and_then(|table| aggregate_predicate(draw, table)),
            KeySpace::JoinOneToOne(join) => {
                let joined: Vec<&str> = join.joins.iter().map(|j| j.right.as_str()).collect();
                Some(join_one_to_one_predicate(draw, &def.source, &joined))
            }
            KeySpace::Join(join) => {
                let left = join.joins[0].kind == JoinKind::Left;
                Some(join_aggregate_predicate(
                    draw,
                    &def.source,
                    &join.joins[0].right,
                    left,
                ))
            }
        };
        if let Some(expr) = expr {
            def.predicate = Predicate::Filter(expr);
        }
    }
    program
}

fn column(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

fn qualified(table: &str, column: &str) -> Expr {
    Expr::SourceColumn {
        schema: None,
        table: table.to_string(),
        column: column.to_string(),
    }
}

fn number(n: i64) -> Expr {
    Expr::NumberLiteral(n.to_string())
}

fn binary(op: Operator, lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryOp {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

fn is_null(expr: Expr, negated: bool) -> Expr {
    Expr::IsNull {
        expr: Box::new(expr),
        negated,
    }
}

fn in_list(expr: Expr, list: Vec<Expr>, negated: bool) -> Expr {
    Expr::InList {
        expr: Box::new(expr),
        list,
        negated,
    }
}

fn between(expr: Expr, low: Expr, high: Expr, negated: bool) -> Expr {
    Expr::Between {
        expr: Box::new(expr),
        low: Box::new(low),
        high: Box::new(high),
        negated,
    }
}

fn coalesce(expr: Expr, otherwise: Expr) -> Expr {
    Expr::FunctionCall {
        name: "COALESCE".to_string(),
        args: vec![expr, otherwise],
    }
}

/// A degenerate predicate: `FALSE`, `NULL` or `(TRUE)` (#1991 rule 6).
fn degenerate(draw: u64) -> Expr {
    match draw % 3 {
        0 => Expr::BooleanLiteral(false),
        1 => Expr::Null,
        _ => Expr::BooleanLiteral(true),
    }
}

/// How many templates [`aggregate_predicate`] draws from.
pub const AGGREGATE_TEMPLATES: u64 = 14;

/// A plain aggregate's predicate over `table`, the generators' uniform
/// shape (`generate::build_program_multi_with_shapes`: `c1`, `c2` numeric,
/// then text, boolean, uuid and the grain), or `None` for another shape.
pub fn aggregate_predicate(draw: u64, table: &Table) -> Option<Expr> {
    let types: Vec<ValueType> = table.columns.iter().map(|c| c.value_type).collect();
    let numeric = |t: ValueType| matches!(t, ValueType::Numeric | ValueType::Integer(_));
    if types.len() != 7
        || !numeric(types[1])
        || !numeric(types[2])
        || types[4] != ValueType::Boolean
        || !numeric(types[6])
    {
        return None;
    }
    let name = |i: usize| table.columns[i].name.as_str();
    let (c1, c2, flag, grain) = (name(1), name(2), name(4), name(6));
    let k = 10 + (draw >> 8) as i64 % 81;
    let g = (draw >> 16) as i64 % 3;
    Some(match draw % AGGREGATE_TEMPLATES {
        0 => binary(Operator::GreaterThan, column(c1), number(k)),
        1 => binary(Operator::LessThanOrEqual, column(c2), number(k)),
        2 => column(flag),
        3 => binary(
            Operator::And,
            binary(Operator::GreaterThan, column(c1), number(k)),
            column(flag),
        ),
        4 => Expr::Not(Box::new(binary(Operator::LessThan, column(c2), number(k)))),
        5 => binary(
            Operator::GreaterThan,
            coalesce(column(c1), number(g * 40)),
            number(k),
        ),
        6 => binary(Operator::GreaterThan, column(c1), column(c2)),
        7 => binary(Operator::NotEqual, column(grain), number(g)),
        8 => binary(
            Operator::Equal,
            binary(Operator::Modulo, column(c1), number(2)),
            number(0),
        ),
        9 => binary(
            Operator::Or,
            is_null(column(c1), false),
            binary(Operator::GreaterThan, column(c2), number(k)),
        ),
        10 => in_list(
            column(c1),
            vec![number(k), number(k + 10), number(g * 40)],
            false,
        ),
        11 => between(column(c2), number(k - 20), number(k), g == 1),
        12 => in_list(column(grain), vec![number(0), number(g)], true),
        _ => degenerate(draw >> 24),
    })
}

/// How many templates [`join_one_to_one_predicate`] draws from.
pub const JOIN_ONE_TO_ONE_TEMPLATES: u64 = 14;

/// A 1-1 `JOIN`'s predicate over its root `source` and its `joined` tables
/// (the generators' shape: `x`, `t` on the root; `id`, `y`, `t`, `w` on each
/// joined table), reading one joined table it draws, or two.
pub fn join_one_to_one_predicate(draw: u64, source: &str, joined: &[&str]) -> Expr {
    let b = joined[(draw >> 32) as usize % joined.len()];
    let other = joined[(draw >> 40) as usize % joined.len()];
    let k = [-30, -10, 0, 10, 30][(draw >> 8) as usize % 5];
    let a = |c: &str| qualified(source, c);
    let j = |c: &str| qualified(b, c);
    match draw % JOIN_ONE_TO_ONE_TEMPLATES {
        0 => binary(Operator::GreaterThan, a("x"), number(k)),
        1 => is_null(j("y"), false),
        2 => is_null(j("id"), false),
        3 => binary(Operator::GreaterThan, a("x"), j("y")),
        4 => binary(
            Operator::LessThanOrEqual,
            coalesce(j("y"), number(0)),
            number(k),
        ),
        5 => binary(
            Operator::Or,
            binary(Operator::Equal, a("t"), number(1)),
            binary(Operator::Equal, j("t"), number(1)),
        ),
        6 => binary(
            Operator::NotEqual,
            j("w"),
            Expr::StringLiteral("w0".to_string()),
        ),
        7 => binary(
            Operator::And,
            Expr::Not(Box::new(binary(Operator::LessThan, a("x"), number(k)))),
            is_null(j("y"), true),
        ),
        8 => binary(
            Operator::Or,
            is_null(a("x"), false),
            binary(Operator::GreaterThan, j("y"), number(k)),
        ),
        9 => binary(Operator::GreaterThanOrEqual, j("y"), qualified(other, "y")),
        10 => between(a("x"), number(k), j("y"), false),
        11 => in_list(j("y"), vec![number(k), number(0)], true),
        12 => in_list(
            j("w"),
            vec![
                Expr::StringLiteral("w0".to_string()),
                Expr::StringLiteral("w1".to_string()),
            ],
            false,
        ),
        _ => degenerate(draw >> 24),
    }
}

/// How many conjunct templates [`join_aggregate_predicate`] draws each
/// conjunct from.
pub const JOIN_AGGREGATE_CONJUNCTS: u64 = 8;

/// A join aggregate's predicate over its `FROM` table `source` and its
/// `joined` table (the JOIN tier's shape, `generate::join`: `ga`, `t`, `k`
/// on the `FROM` side, `gb`, `t`, `k` on the joined side, all `integer`;
/// the aggregate arguments `x` and `y` are left out, as their type varies).
/// ADR-0019's per-side rule holds: one or two top-level `AND` conjuncts,
/// each reading one side and at least one column, and under `LEFT` none
/// reading the joined side ([`join_side_conjuncts`]).
pub fn join_aggregate_predicate(draw: u64, source: &str, joined: &str, left: bool) -> Expr {
    let conjunct = |draw: u64, table: &str, group: &str| -> Expr {
        let c = |column: &str| qualified(table, column);
        let g = (draw >> 8) as i64 % 4;
        match draw % JOIN_AGGREGATE_CONJUNCTS {
            0 => binary(Operator::NotEqual, c(group), number(g)),
            1 => binary(Operator::Equal, c("t"), number(1)),
            2 => is_null(c(group), true),
            3 => binary(
                Operator::GreaterThanOrEqual,
                coalesce(c(group), number(0)),
                number(g),
            ),
            4 => binary(
                Operator::Equal,
                binary(Operator::Modulo, c("k"), number(2)),
                number(0),
            ),
            5 => in_list(c(group), vec![number(g), number(g + 1)], false),
            6 => between(c("k"), number(0), number(g), true),
            _ => binary(
                Operator::Or,
                binary(Operator::Equal, c("t"), number(0)),
                binary(Operator::GreaterThan, c(group), number(1)),
            ),
        }
    };
    let side = |draw: u64| -> Expr {
        if left || draw.is_multiple_of(2) {
            conjunct(draw >> 1, source, "ga")
        } else {
            conjunct(draw >> 1, joined, "gb")
        }
    };
    let first = side(draw >> 16);
    // One conjunct in three; else two, of the same side or of both.
    if (draw >> 48).is_multiple_of(3) {
        first
    } else {
        binary(Operator::And, first, side(draw >> 32))
    }
}

/// A join aggregate's `WHERE` split by side (ADR-0019): the `AND` of the
/// top-level conjuncts that read `table`, for each table one does.
pub fn join_side_conjuncts(predicate: &Expr) -> HashMap<String, Expr> {
    fn collect<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr {
            Expr::BinaryOp {
                op: Operator::And,
                lhs,
                rhs,
            } => {
                collect(lhs, out);
                collect(rhs, out);
            }
            _ => out.push(expr),
        }
    }
    let mut conjuncts = Vec::new();
    collect(predicate, &mut conjuncts);
    let mut out: HashMap<String, Expr> = HashMap::new();
    for conjunct in conjuncts {
        let mut table = None;
        conjunct.walk(&mut |e| {
            if let Expr::SourceColumn { table: t, .. } = e {
                table = Some(t.clone());
            }
        });
        let Some(table) = table else { continue };
        let joined = match out.remove(&table) {
            Some(lhs) => binary(Operator::And, lhs, conjunct.clone()),
            None => conjunct.clone(),
        };
        out.insert(table, joined);
    }
    out
}

/// A value in [`eval`]: SQL `NULL`, or a typed value. A column's text is
/// [`Value::Raw`] until a comparison types it from the other side.
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Null,
    Bool(bool),
    Num(f64),
    Text(String),
    Raw(String),
}

impl Value {
    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            Value::Raw(text) => match text.as_str() {
                "true" | "t" => Some(true),
                "false" | "f" => Some(false),
                other => panic!("eval: {other:?} isn't a boolean"),
            },
            Value::Null => None,
            other => panic!("eval: {other:?} isn't a boolean"),
        }
    }

    fn as_num(&self) -> Option<f64> {
        match self {
            Value::Num(n) => Some(*n),
            Value::Raw(text) => Some(
                text.parse()
                    .unwrap_or_else(|_| panic!("eval: {text:?} isn't a number")),
            ),
            Value::Null => None,
            other => panic!("eval: {other:?} isn't a number"),
        }
    }
}

/// Postgres's order of two numbers: `NaN` equals `NaN` and sorts above
/// every other value (a `numeric` column admits it).
fn num_order(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.partial_cmp(&b).expect("not NaN"),
    }
}

/// Evaluates `expr`, one of the shapes this module draws, as Postgres
/// would, reading each column with `read(table, column)` (`table` is `None`
/// for a bare name): `Some(true)` for a member row, `Some(false)` or `None`
/// (`NULL`) for a row the predicate excludes.
///
/// # Panics
///
/// On a shape this module never draws.
pub fn eval(expr: &Expr, read: &dyn Fn(Option<&str>, &str) -> Option<String>) -> Option<bool> {
    value(expr, read).as_bool()
}

fn value(expr: &Expr, read: &dyn Fn(Option<&str>, &str) -> Option<String>) -> Value {
    let raw = |v: Option<String>| v.map_or(Value::Null, Value::Raw);
    match expr {
        Expr::Column(name) => raw(read(None, name)),
        Expr::SourceColumn { table, column, .. } => raw(read(Some(table), column)),
        Expr::NumberLiteral(text) => Value::Num(text.parse().expect("a number literal")),
        Expr::StringLiteral(text) => Value::Text(text.clone()),
        Expr::BooleanLiteral(b) => Value::Bool(*b),
        Expr::Null => Value::Null,
        Expr::Not(operand) => match value(operand, read).as_bool() {
            Some(b) => Value::Bool(!b),
            None => Value::Null,
        },
        Expr::IsNull { expr, negated } => {
            Value::Bool((value(expr, read) == Value::Null) != *negated)
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" => args
            .iter()
            .map(|a| value(a, read))
            .find(|v| *v != Value::Null)
            .unwrap_or(Value::Null),
        // `=` joined by `OR`, or `<>` joined by `AND`.
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let operand = value(expr, read);
            let (op, join) = if *negated {
                (Operator::NotEqual, and3 as fn(Value, Value) -> Value)
            } else {
                (Operator::Equal, or3 as fn(Value, Value) -> Value)
            };
            list.iter()
                .map(|element| compare(op, &operand, &value(element, read)))
                .reduce(join)
                .expect("a list has an element")
        }
        // `>= low AND <= high`, or `< low OR > high`.
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let operand = value(expr, read);
            let (low, high) = (value(low, read), value(high, read));
            if *negated {
                or3(
                    compare(Operator::LessThan, &operand, &low),
                    compare(Operator::GreaterThan, &operand, &high),
                )
            } else {
                and3(
                    compare(Operator::GreaterThanOrEqual, &operand, &low),
                    compare(Operator::LessThanOrEqual, &operand, &high),
                )
            }
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let (l, r) = (value(lhs, read), value(rhs, read));
            match op {
                Operator::And => and3(l, r),
                Operator::Or => or3(l, r),
                Operator::Modulo => match (l.as_num(), r.as_num()) {
                    (Some(a), Some(b)) if a.is_nan() => Value::Num(a + b),
                    (Some(a), Some(b)) => Value::Num(a % b),
                    _ => Value::Null,
                },
                _ => compare(*op, &l, &r),
            }
        }
        other => panic!("eval: the WHERE generator draws no {other:?}"),
    }
}

/// Three-valued `AND`.
fn and3(l: Value, r: Value) -> Value {
    match (l.as_bool(), r.as_bool()) {
        (Some(false), _) | (_, Some(false)) => Value::Bool(false),
        (Some(true), Some(true)) => Value::Bool(true),
        _ => Value::Null,
    }
}

/// Three-valued `OR`.
fn or3(l: Value, r: Value) -> Value {
    match (l.as_bool(), r.as_bool()) {
        (Some(true), _) | (_, Some(true)) => Value::Bool(true),
        (Some(false), Some(false)) => Value::Bool(false),
        _ => Value::Null,
    }
}

fn compare(op: Operator, l: &Value, r: &Value) -> Value {
    use std::cmp::Ordering;
    if *l == Value::Null || *r == Value::Null {
        return Value::Null;
    }
    let order = match (l, r) {
        (Value::Text(a), b) | (b, Value::Text(a)) => {
            let b = match b {
                Value::Text(b) | Value::Raw(b) => b,
                other => panic!("eval: text against {other:?}"),
            };
            // Only `=` and `<>` are drawn against text, which no collation
            // changes.
            if a == b {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        }
        _ => num_order(l.as_num().expect("not NULL"), r.as_num().expect("not NULL")),
    };
    Value::Bool(match op {
        Operator::Equal => order == Ordering::Equal,
        Operator::NotEqual => order != Ordering::Equal,
        Operator::GreaterThan => order == Ordering::Greater,
        Operator::GreaterThanOrEqual => order != Ordering::Less,
        Operator::LessThan => order == Ordering::Less,
        Operator::LessThanOrEqual => order != Ordering::Greater,
        other => panic!("eval: the WHERE generator draws no {other:?}"),
    })
}

/// A crossing bucket: a row's membership before and after a change.
pub fn bucket(before: bool, after: bool) -> &'static str {
    match (before, after) {
        (true, true) => "in_in",
        (true, false) => "in_out",
        (false, true) => "out_in",
        (false, false) => "out_out",
    }
}

/// The four [`bucket`]s, in a fixed order.
pub const BUCKETS: [&str; 4] = ["in_in", "in_out", "out_in", "out_out"];

/// A row of a table as the crossing replay holds it.
pub type Row = HashMap<String, Option<String>>;

#[cfg(test)]
mod tests {
    use super::*;

    fn row(cells: &[(&str, Option<&str>)]) -> Row {
        cells
            .iter()
            .map(|(c, v)| (c.to_string(), v.map(str::to_string)))
            .collect()
    }

    fn eval_on(expr: &Expr, row: &Row) -> Option<bool> {
        eval(expr, &|_, c| row.get(c).cloned().flatten())
    }

    #[test]
    fn three_valued_logic_follows_postgres() {
        let r = row(&[("a", Some("5")), ("b", None), ("f", Some("true"))]);
        let gt = |c: &str, n| binary(Operator::GreaterThan, column(c), number(n));
        assert_eq!(eval_on(&gt("a", 3), &r), Some(true));
        assert_eq!(eval_on(&gt("b", 3), &r), None);
        assert_eq!(
            eval_on(&binary(Operator::And, gt("b", 3), gt("a", 9)), &r),
            Some(false)
        );
        assert_eq!(
            eval_on(&binary(Operator::Or, gt("b", 3), gt("a", 3)), &r),
            Some(true)
        );
        assert_eq!(eval_on(&Expr::Not(Box::new(gt("b", 3))), &r), None);
        assert_eq!(eval_on(&column("f"), &r), Some(true));
        assert_eq!(eval_on(&is_null(column("b"), false), &r), Some(true));
        assert_eq!(
            eval_on(
                &binary(
                    Operator::GreaterThan,
                    coalesce(column("b"), number(7)),
                    number(6)
                ),
                &r
            ),
            Some(true)
        );
        assert_eq!(eval_on(&Expr::Null, &r), None);
    }

    #[test]
    fn a_join_aggregate_predicate_keeps_adr_0019s_per_side_rule() {
        let (mut both, mut joined) = (0, 0);
        for i in 0..2000u64 {
            let draw = mix(i);
            for left in [false, true] {
                let expr = join_aggregate_predicate(draw, "ja", "jb", left);
                let sides = join_side_conjuncts(&expr);
                assert!(!sides.is_empty(), "{expr:?} reads no column");
                for side in sides.values() {
                    let mut tables = std::collections::BTreeSet::new();
                    side.walk(&mut |e| {
                        if let Expr::SourceColumn { table, .. } = e {
                            tables.insert(table.clone());
                        }
                    });
                    assert_eq!(tables.len(), 1, "a side's conjuncts read one table");
                }
                assert!(
                    !(left && sides.contains_key("jb")),
                    "LEFT reads jb: {expr:?}"
                );
                joined += usize::from(sides.contains_key("jb"));
                both += usize::from(sides.len() == 2);
            }
        }
        assert!(joined > 200 && both > 100, "joined {joined}, both {both}");
    }

    #[test]
    fn in_and_between_follow_three_valued_logic() {
        let r = row(&[("a", Some("5")), ("b", None)]);
        let list = |c: &str, items: Vec<Expr>, negated| in_list(column(c), items, negated);
        assert_eq!(
            eval_on(&list("a", vec![number(1), number(5)], false), &r),
            Some(true)
        );
        assert_eq!(
            eval_on(&list("a", vec![number(1), Expr::Null], false), &r),
            None
        );
        assert_eq!(
            eval_on(&list("a", vec![number(5), Expr::Null], false), &r),
            Some(true)
        );
        assert_eq!(
            eval_on(&list("a", vec![number(1), Expr::Null], true), &r),
            None
        );
        assert_eq!(
            eval_on(&list("a", vec![number(5), Expr::Null], true), &r),
            Some(false)
        );
        assert_eq!(eval_on(&list("b", vec![number(1)], true), &r), None);
        let range = |lo, hi, negated| between(column("a"), number(lo), number(hi), negated);
        assert_eq!(eval_on(&range(5, 9, false), &r), Some(true));
        assert_eq!(eval_on(&range(6, 9, false), &r), Some(false));
        assert_eq!(eval_on(&range(6, 9, true), &r), Some(true));
        assert_eq!(
            eval_on(&between(column("a"), number(6), Expr::Null, false), &r),
            Some(false)
        );
        assert_eq!(
            eval_on(&between(column("a"), number(1), Expr::Null, false), &r),
            None
        );
    }

    #[test]
    fn nan_sorts_above_every_number_and_equals_itself() {
        let r = row(&[("a", Some("NaN")), ("b", Some("NaN"))]);
        assert_eq!(
            eval_on(&binary(Operator::GreaterThan, column("a"), number(99)), &r),
            Some(true)
        );
        assert_eq!(
            eval_on(&binary(Operator::Equal, column("a"), column("b")), &r),
            Some(true)
        );
        assert_eq!(
            eval_on(
                &binary(
                    Operator::Equal,
                    binary(Operator::Modulo, column("a"), number(2)),
                    number(0)
                ),
                &r
            ),
            Some(false)
        );
    }
}
