//! The function/operator registry ADR-0004 requires: the grammar's accepted
//! operators and functions must equal the evaluator's re-implemented set
//! (issue #24 consumes this list too), so it lives as a plain, reusable
//! table rather than being baked into the parser's control flow.
//!
//! Issue #64 adds the first four functions (`strpos`, `octet_length`,
//! `char_length`, `regexp_count`), each carrying its arity and argument/
//! return [`ValueType`]s so the parser, validator, and evaluator all read
//! the same source of truth rather than each hardcoding a function's shape.

use super::ast::{Operator, ValueType};
use crate::integer::IntWidth;

/// One operator the grammar accepts, and the AST node it parses to.
#[derive(Debug, Clone, Copy)]
pub struct OperatorSpec {
    pub symbol: &'static str,
    pub operator: Operator,
    /// The operator's **canonical** operand types — the signature named in a
    /// [`super::validate::ValidationError::TypeMismatch`].
    ///
    /// Since issue #111 this is *not* the admissibility test: exact integers
    /// have their own [`ValueType`], so each operator covers a family of
    /// Postgres operators rather than one signature, and
    /// [`operator_result_type`] is what decides both whether a pair of
    /// operands is admissible and what the result type is. There is
    /// correspondingly no `return_type` field any more — a `+`'s result
    /// depends on its operands (`int4 + int4` is `integer`, `int4 +
    /// numeric` is `numeric`), so a constant could only have been wrong.
    pub arg_types: (ValueType, ValueType),
    /// Binding strength [`super::parser`]'s precedence-climbing loop uses
    /// to group `a OP1 b OP2 c`: higher binds tighter, so an operator with
    /// a higher `precedence` grabs its operands before a lower-precedence
    /// one does. All operators here are left-associative, so operators at
    /// the *same* level still group left-to-right (`a + b + c` parses as
    /// `(a + b) + c`).
    ///
    /// Levels are spaced out (not packed as 0, 1, 2, ...) so a new operator
    /// can be slotted between two existing levels later without
    /// renumbering everything else. Use the [`precedence`] constants below
    /// rather than a raw number.
    pub precedence: u8,
}

/// Named precedence levels, standard SQL/Postgres order — highest binds
/// tightest. Gaps are left between levels for future operators (e.g. `NOT`
/// or `AND`/`OR`, both lower than comparison) without renumbering.
pub mod precedence {
    /// `=`, `<>`, `<`, `>`, `<=`, `>=` and similar comparisons.
    pub const COMPARISON: u8 = 10;
    /// Binary `+`, `-`.
    pub const ADDITIVE: u8 = 20;
    /// `*`, `/`, `%`. Unused until those operators land — see this module's
    /// doc comment: the gap exists precisely so they can be added without
    /// renumbering the levels around them.
    #[allow(dead_code)]
    pub const MULTIPLICATIVE: u8 = 30;
}

/// The full set of operators this grammar/evaluator pairing supports.
///
/// # Precedence table (issue #67)
///
/// [`super::parser`] parses binary operators with a real precedence-climbing
/// loop keyed off each [`OperatorSpec::precedence`] here, so `a OP1 b OP2 c`
/// groups the way a precedence-aware grammar (e.g. Postgres's) would, not
/// flat-left-to-right. `+` sits at [`precedence::ADDITIVE`] and `>` at
/// [`precedence::COMPARISON`], so e.g. `a > b + c` parses as `a > (b + c)`
/// (`+` binds tighter and grabs `b` and `c` first), matching Postgres.
///
/// This matters beyond just matching Postgres's grouping: before this table
/// existed, correctness relied on an accident of the type lattice — `+` is
/// `(Numeric, Numeric) -> Numeric` and `>` is `(Numeric, Numeric) ->
/// Boolean`, so the *wrong* flat-parse regrouping of `a > b + c` (as
/// `(a > b) + c`) happened to get caught by [`super::validate`]'s type
/// checker rather than silently computing a wrong-but-plausible answer. See
/// `mod::tests::mixed_operators_respect_precedence` for the regression test
/// pinning real precedence (renamed from `flat_parse_of_mixed_operators_is_
/// caught_by_type_checking`, which pinned the old, wrong behavior).
///
/// Without a real precedence table, that safety net would have been only
/// an emergent property of the current operator set: the day a second
/// Numeric-returning operator (e.g. `-`, `*`) or a second Boolean-returning
/// operator (e.g. `<`, `=`) is added at a "compatible" spot in the type
/// lattice, both possible flat-parse regroupings of some `a OP1 b OP2 c`
/// could type-check (just to different results) with no type error to catch
/// the mistake. Assigning every new operator a real [`precedence`] level
/// (rather than leaving it flat) is what keeps that from happening.
pub const OPERATORS: &[OperatorSpec] = &[
    OperatorSpec {
        symbol: "+",
        operator: Operator::Add,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::ADDITIVE,
    },
    OperatorSpec {
        symbol: ">",
        operator: Operator::GreaterThan,
        arg_types: (ValueType::Numeric, ValueType::Numeric),
        precedence: precedence::COMPARISON,
    },
];

/// Looks up an operator's spec by its own [`Operator`] variant (as opposed
/// to [`lookup_operator`], keyed by concrete syntax) — what
/// [`super::parser`] wants for [`OperatorSpec::precedence`], and what
/// [`super::validate`]'s type-checker names in a `TypeMismatch` once
/// [`operator_result_type`] has rejected an operand.
pub fn operator_spec(operator: Operator) -> &'static OperatorSpec {
    OPERATORS
        .iter()
        .find(|spec| spec.operator == operator)
        .expect("every Operator variant has a matching OperatorSpec in OPERATORS")
}

/// The result type of `op` applied to operands of `lhs`/`rhs` type, or
/// `None` if Postgres has no such operator — the **overload-resolution**
/// replacement for the old "compare both operands to [`OperatorSpec::arg_types`]
/// for exact equality" check (issue #111).
///
/// Exact equality was sufficient while every arithmetic/comparison operand
/// was the single [`ValueType::Numeric`] bucket. It stops being sufficient
/// the moment exact integers have their own type: Postgres has a *family* of
/// `+` operators (`int2pl`, `int4pl`, `int24pl`, `numeric_add`, …) plus an
/// implicit `int -> numeric` coercion, so `int4_col + 1` is `integer` while
/// `int4_col + numeric_col` is `numeric`. This function encodes exactly that
/// and nothing more — it is deliberately **not** a general coercion lattice
/// (`super::typed_literal` rejects building one for the same reason), just
/// the closed set of combinations the two registered operators admit:
///
/// | lhs | rhs | `+` | `>` |
/// |---|---|---|---|
/// | `Integer(a)` | `Integer(b)` | `Integer(wider(a,b))` | `Boolean` |
/// | `Integer(_)` | `Numeric` | `Numeric` | `Boolean` |
/// | `Numeric` | `Integer(_)` | `Numeric` | `Boolean` |
/// | `Numeric` | `Numeric` | `Numeric` | `Boolean` |
///
/// Any other combination is `None`: `Text`/`Boolean`/`Uuid`/`Other` have no
/// `+` or `>` in this grammar, exactly as before.
///
/// `Integer + Numeric -> Numeric` mirrors Postgres promoting the integer
/// operand through its implicit cast and then running `numeric_add`, which
/// is *unbounded* — so a mixed-type sum correctly cannot overflow, while an
/// all-integer one correctly can (see [`crate::integer::checked_add`]).
/// [`super::eval`]'s `apply_operator` dispatches on the same shape, and has
/// to: if the two disagreed, the target column's declared type and the value
/// written into it would disagree too.
pub fn operator_result_type(op: Operator, lhs: ValueType, rhs: ValueType) -> Option<ValueType> {
    if !lhs.is_exact_numeric_family() || !rhs.is_exact_numeric_family() {
        return None;
    }
    Some(match op {
        Operator::Add => match (lhs, rhs) {
            (ValueType::Integer(a), ValueType::Integer(b)) => ValueType::Integer(a.wider(b)),
            _ => ValueType::Numeric,
        },
        Operator::GreaterThan => ValueType::Boolean,
    })
}

/// One function the grammar accepts, and the arity/types the parser and
/// validator check a call against (issue #64).
#[derive(Debug, Clone, Copy)]
pub struct FunctionSpec {
    /// Canonical uppercased name, matched against the parser's uppercased
    /// identifier (see [`super::parser`]) and lowercased back to Postgres's
    /// own spelling when rendering a call to SQL (see [`super::oracle`]).
    pub name: &'static str,
    pub arg_types: &'static [ValueType],
    pub return_type: ValueType,
}

/// Function names the grammar accepts as calculated-field functions,
/// each re-implemented in [`super::eval`] with identical Postgres 15+
/// semantics (per ADR-0004: the grammar and evaluator function sets are one
/// list).
///
/// All four return Postgres `integer` (`int4`), not `numeric` — checked
/// against `pg_proc.prorettype` for `strpos`/`octet_length`/`char_length`/
/// `regexp_count`. They were declared `Numeric` only because, before issue
/// #111, [`ValueType`] had no way to say "integer"; saying it now is what
/// makes `char_length(t) + 1` type — and overflow — the way Postgres does,
/// and what declares the derived column `integer` rather than `numeric`.
pub const FUNCTIONS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "STRPOS",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "OCTET_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "CHAR_LENGTH",
        arg_types: &[ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
    FunctionSpec {
        name: "REGEXP_COUNT",
        arg_types: &[ValueType::Text, ValueType::Text],
        return_type: ValueType::Integer(IntWidth::Int4),
    },
];

/// Looks up a function by its canonical uppercased name.
pub fn lookup_function(name: &str) -> Option<&'static FunctionSpec> {
    FUNCTIONS.iter().find(|spec| spec.name == name)
}

/// Aggregate function names, called out specifically so a rejection can
/// explain that they need an aggregate key-space (`GROUP BY`), not that
/// they're simply unknown.
pub const AGGREGATE_FUNCTIONS: &[&str] = &["SUM", "COUNT", "AVG", "MIN", "MAX"];

/// The aggregate functions an [`super::ast::KeySpace::Aggregate`] definition
/// may call in a calculated field (issue #11's groundwork), plus `COUNT`
/// (issue #75). `SUM`/`MIN`/`MAX`/`AVG` are numeric-only and unary; `COUNT`
/// is arity-0 (`COUNT(*)` — row-counting, not `COUNT(<column>)`), which
/// [`super::parser`] special-cases: it accepts the literal `*` in place of an
/// argument list and hands this spec's empty `arg_types` an empty `args`
/// vec, rather than teaching this table's shape a non-expression argument
/// syntax.
pub const AGGREGATE_FUNCTION_SPECS: &[FunctionSpec] = &[
    FunctionSpec {
        name: "SUM",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "MIN",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "MAX",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "AVG",
        arg_types: &[ValueType::Numeric],
        return_type: ValueType::Numeric,
    },
    FunctionSpec {
        name: "COUNT",
        arg_types: &[],
        return_type: ValueType::Numeric,
    },
];

/// Looks up an aggregate function by its canonical uppercased name — the
/// [`AGGREGATE_FUNCTION_SPECS`] counterpart to [`lookup_function`].
pub fn lookup_aggregate_function(name: &str) -> Option<&'static FunctionSpec> {
    AGGREGATE_FUNCTION_SPECS
        .iter()
        .find(|spec| spec.name == name)
}

/// The result type of aggregate `name` over an argument of type `arg` —
/// [`operator_result_type`]'s aggregate twin (issue #111), and for the same
/// reason: once exact integers are their own type, an aggregate's result
/// type is a *function of its argument's* type in Postgres, not a constant.
/// `None` for an argument type the aggregate doesn't accept.
///
/// The rules are Postgres's own (`pg_aggregate` → `pg_proc.prorettype`), and
/// two of them are the kind of thing that looks wrong until you check:
///
/// * `sum(smallint)` and `sum(integer)` return **`bigint`** — Postgres
///   widens, because a sum of many `int4`s routinely leaves `int4`. It does
///   still raise `22003` once the sum leaves `bigint`.
/// * `sum(bigint)` returns **`numeric`**, not `bigint` — the same reasoning
///   one step further up, and it is why a `bigint` sum cannot overflow.
/// * `avg(<any exact integer>)` returns `numeric`; the average of integers
///   is not an integer.
/// * `min`/`max` return their argument's own type exactly.
///
/// `COUNT` is deliberately absent and keeps the constant `Numeric` return
/// type its [`AGGREGATE_FUNCTION_SPECS`] row declares. It is arity-0
/// `COUNT(*)` row-counting in this grammar — type-agnostic by construction,
/// which is why `docs/type-support.md` omits it from every per-type
/// aggregate cell. Postgres types `count(*)` as `bigint`; aligning that is
/// orthogonal to this issue's exact-integer split (it would change a derived
/// column's type for definitions that reference no integer at all) and is
/// left to #120.
pub fn aggregate_result_type(name: &str, arg: ValueType) -> Option<ValueType> {
    if !arg.is_exact_numeric_family() {
        return None;
    }
    Some(match (name, arg) {
        ("SUM", ValueType::Integer(IntWidth::Int2 | IntWidth::Int4)) => {
            ValueType::Integer(IntWidth::Int8)
        }
        ("SUM", ValueType::Integer(IntWidth::Int8) | ValueType::Numeric) => ValueType::Numeric,
        ("AVG", _) => ValueType::Numeric,
        ("MIN" | "MAX", arg) => arg,
        _ => return None,
    })
}

/// Identifiers known to be non-immutable (depend on database/session state
/// rather than solely their inputs), called out per
/// `docs/transforms.md#calculated-fields` so a rejection can name the
/// immutability rule rather than reporting a generic unknown-function error.
pub const NON_IMMUTABLE_NAMES: &[&str] = &[
    "NOW",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CLOCK_TIMESTAMP",
    "RANDOM",
    "STATEMENT_TIMESTAMP",
    "TRANSACTION_TIMESTAMP",
];

/// Looks up an operator by its concrete-syntax symbol.
pub fn lookup_operator(symbol: &str) -> Option<Operator> {
    OPERATORS
        .iter()
        .find(|spec| spec.symbol == symbol)
        .map(|spec| spec.operator)
}
