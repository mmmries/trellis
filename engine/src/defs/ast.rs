//! The typed AST a transform definition parses into.
//!
//! Shapes mirror the logical model in `docs/transforms.md`: a definition
//! picks a key-space, a set of calculated fields, and a partial-data
//! predicate. This slice (issue #22) only populates the 1-1 key-space and
//! the `+`-only expression language; [`KeySpace`] and [`Predicate`] have
//! room to grow (aggregate/cross-join variants, general predicates) without
//! changing the shape callers already match on.
//!
//! Issue #63 widens the value model from numeric-only to [`ValueType`]'s
//! three variants; see `docs/transforms.md` and ADR-0004's growth policy.

use std::fmt;

/// A parsed transform definition, ready for the validator (issue #23) and
/// evaluator (issue #24).
#[derive(Debug, Clone, PartialEq)]
pub struct TransformDef {
    pub target: String,
    pub source: String,
    pub key_space: KeySpace,
    pub fields: Vec<FieldDef>,
    pub predicate: Predicate,
}

/// A parsed standalone relationship declaration (ADR-0006):
/// `RELATIONSHIP <name> FROM <from_table>.<fk_col> TO <to_table>.<pk_col>`.
///
/// This slice (issue #24) is grammar + AST only — cardinality validation,
/// catalog storage, and referencing a relationship from a calculated field
/// are all deferred to later issues.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDef {
    pub name: String,
    pub from_table: String,
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
}

/// The target table's primary-key space (see `docs/transforms.md#granularity`).
///
/// [`KeySpace::Aggregate`] (issue #11's groundwork) is a `GROUP BY <cols>`
/// definition, whose target's primary key is the grouping columns rather
/// than an inherited source column. Cross-join key-spaces are still rejected
/// at parse time with a specific error rather than represented here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySpace {
    OneToOne,
    /// `group_by` holds the source column names named after `GROUP BY`, in
    /// the order they were written — that order becomes the target table's
    /// composite primary key column order.
    Aggregate {
        group_by: Vec<String>,
    },
}

/// One `<expr> AS <name>` calculated-field entry.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub name: String,
    pub expr: Expr,
}

/// A calculated-field scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A reference to a column on the (single, 1-1) source row.
    Column(String),
    /// A numeric literal, kept as the source text so the evaluator picks
    /// the numeric type/precision rather than the parser.
    NumberLiteral(String),
    /// A single-quoted string literal (issue #63).
    StringLiteral(String),
    /// A `<rel>.<column>` relationship-path reference (issue #25, ADR-0006).
    /// `rel` is the head's **relationship name**, not a table/alias —
    /// resolving whether it's an actually-declared relationship, and its
    /// cardinality, is deferred to later validation/eval issues; this
    /// variant is grammar + AST only.
    RelationshipPath { rel: String, column: String },
    BinaryOp {
        op: Operator,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
    },
    /// A `name(args)` function call (issue #64) — the first function-call
    /// syntax the grammar accepts. `name` is the uppercased canonical form
    /// looked up in [`super::registry::FUNCTIONS`]; arity and argument types
    /// are checked against that same registry entry, not hardcoded here.
    ///
    /// In an [`KeySpace::Aggregate`] definition, `name` may instead be one of
    /// `SUM`/`MIN`/`MAX`/`AVG` (looked up in
    /// [`super::registry::AGGREGATE_FUNCTION_SPECS`]) with a single Numeric
    /// argument, or `COUNT` (issue #75) with an empty `args` — `COUNT(*)`
    /// row-counting, the only `COUNT` shape this grammar accepts — since
    /// `*` is not itself an expression. There's no separate AST node for
    /// aggregate calls, since they're syntactically identical `name(args)`
    /// calls, just resolved against a different registry depending on
    /// key-space.
    FunctionCall { name: String, args: Vec<Expr> },
}

/// The value type a column or expression carries (issue #63). `Text` and
/// `Boolean` are plumbed through so columns of those types can be declared
/// and passed through a calculated field; issue #64 added `Text`-argument
/// functions, and issue #65 adds the `>` comparison operator, the first
/// operator whose result type differs from its operands' (`Numeric,
/// Numeric -> Boolean`). `Uuid` (issue #79) is narrower still: it's
/// representable, comparable, and passthrough-able (including as an
/// aggregate `GROUP BY` key), but has no arithmetic/regex operations the way
/// `Numeric`/`Text` do — there's no real-world use for `uuid + uuid`, so
/// [`super::registry`] never grants it an operator or function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Numeric,
    Text,
    Boolean,
    Uuid,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueType::Numeric => write!(f, "numeric"),
            ValueType::Text => write!(f, "text"),
            ValueType::Boolean => write!(f, "boolean"),
            ValueType::Uuid => write!(f, "uuid"),
        }
    }
}

/// An operator accepted by the expression grammar; see [`super::registry`]
/// for the list the parser validates against and the evaluator reuses.
/// [`Operator::Add`] is `Numeric, Numeric -> Numeric`; [`Operator::GreaterThan`]
/// (issue #65) is `Numeric, Numeric -> Boolean`, matching Postgres's `>` on
/// `numeric` exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Add,
    GreaterThan,
}

/// The partial-data predicate slot (`docs/transforms.md#partial-data`).
///
/// This slice accepts only a trivially-true predicate — either `WHERE` is
/// omitted, or written as the literal `WHERE TRUE`. General predicate
/// expressions are deferred to a later issue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    True,
}
