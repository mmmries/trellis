//! Renders a validated definition's expressions as Postgres SQL.
//!
//! Postgres evaluates every calculated field: the engine renders the
//! definition into the statement that writes it (ADR-0002) and computes no
//! value itself. [`render_expr_sql`] and [`render_qualified_expr_sql`] are the
//! renderers; [`render_target_select_sql`] renders a whole definition as the
//! `SELECT` that computes its target, which [`crate::target_select_sql`]
//! publishes and the tests compare a target with.

use std::collections::{HashMap, HashSet};

use crate::pool::quote_ident;

use super::ast::{Expr, KeySpace, QualifiedColumn, TransformDef, ValueType};
use super::typed_literal;
use crate::integer::IntWidth;

/// The `FROM`-clause identifier the whole-target `SELECT` reads a table
/// through (ADR-0011 grammar clause 4): `"schema"."table"`, each component
/// quoted independently, when the definition wrote the schema; the plain
/// `"table"` quoted identifier for a bare name, which resolves against the
/// executing connection's own `search_path`.
///
/// A bare table doesn't resolve on a Trellis session, whose `search_path` is
/// Trellis's schema alone (`docs/decisions/0011-fully-qualified-names.md`),
/// so the rendered SQL runs on a connection whose `search_path` reaches the
/// table, such as a test's own.
fn quoted_table(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// Why [`render_target_select_sql`] can't render a definition's `SELECT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetSelectError {
    /// A target column the definition computes no value for: neither a field
    /// nor one of its key columns.
    NotComputed { column: String },
    /// The fields can't be inlined into self-contained expressions (a cyclic
    /// alias chain, or an expansion over the size budget), which define
    /// refuses.
    Unsubstitutable(String),
}

impl std::fmt::Display for TargetSelectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetSelectError::NotComputed { column } => write!(
                f,
                "the target has a column {column:?} that the definition computes no value for: \
                 it is neither a field nor a key column"
            ),
            TargetSelectError::Unsubstitutable(why) => f.write_str(why),
        }
    }
}

/// What one output column of the whole-target `SELECT` holds.
enum Output<'a> {
    /// The field of this name.
    Field(&'a str),
    /// The key column of this name, read from the `FROM` table (a 1-1 or a
    /// 1-1 `JOIN`) or a `GROUP BY` key (an aggregate or a join aggregate).
    Key(&'a str),
}

/// Renders `def` as the Postgres `SELECT` that computes its whole target from
/// the tables it reads as they stand, as a fresh build would: one output
/// column per entry of `target_columns`, in that order, so an
/// `INSERT INTO <target> <select>` lands each value in its column.
///
/// A target column is a field of `def`, or one of its key columns: the
/// `FROM` table's key for a 1-1 or a 1-1 `JOIN` (the `KEY (...)` columns
/// when the definition names them, and otherwise any column that isn't a
/// field, which a target only has for its source's key), or a `GROUP BY` key
/// for an aggregate or a join aggregate. Every field is inlined first
/// ([`super::backfill::substituted_field_exprs`]), so a field reading
/// another renders as a self-contained expression.
///
/// Each value has the type Postgres gives its expression, so an `INSERT`
/// lands it by the assignment cast the statements that write the target rely
/// on. A `JOIN`'s columns are qualified by their
/// tables' names, which the `FROM` clause names unaliased.
pub(crate) fn render_target_select_sql(
    def: &TransformDef,
    target_columns: &[&str],
) -> Result<String, TargetSelectError> {
    let outputs = target_columns
        .iter()
        .map(|&column| output_of(def, column))
        .collect::<Result<Vec<_>, _>>()?;
    select_sql(def, &outputs, false)
}

/// The Postgres `SELECT` that computes a transform's whole target from the
/// tables it reads, as they stand: what a fresh build of the target would
/// write, without capture, the drain or any pause. A test that runs with
/// Trellis off fills a target with it (`DELETE FROM <target>`, then
/// `INSERT INTO <target> <select>`), on its own connection and inside its own
/// transaction, as the Ruby client's `Trellis::TestSupport#refresh_trellis_target`
/// does.
///
/// `definition` is the `TRANSFORM` statement as Trellis stores it, written
/// under grammar `grammar_version` (ADR-0018), and `target_columns` the
/// target table's columns in table order. The `SELECT` has one output column
/// per target column, in that order, each with the type Postgres gives its
/// expression, so an `INSERT` lands every value in its column by the
/// assignment cast the statements that write the target rely on.
///
/// It reads no database: the text is parsed with `apply`'s own parser and
/// rendered as the engine renders its writes. A table the definition names
/// without a schema is left unqualified, so it resolves on the `search_path`
/// of the connection that runs the `SELECT`.
///
/// # Errors
///
/// [`TrellisError::Parse`](crate::TrellisError::Parse) if the text doesn't
/// parse under its grammar version, and
/// [`TrellisError::TargetSelect`](crate::TrellisError::TargetSelect) if a
/// target column is neither a field nor a key column of the definition.
pub fn target_select_sql<S: AsRef<str>>(
    definition: &str,
    grammar_version: i32,
    target_columns: &[S],
) -> Result<String, crate::TrellisError> {
    let def = super::grammar::load(definition, grammar_version)?;
    let columns: Vec<&str> = target_columns.iter().map(AsRef::as_ref).collect();
    render_target_select_sql(&def, &columns).map_err(|err| crate::TrellisError::TargetSelect {
        target: def.target.clone(),
        reason: err.to_string(),
    })
}

/// Which [`Output`] the target column `column` of `def` is.
fn output_of<'a>(def: &TransformDef, column: &'a str) -> Result<Output<'a>, TargetSelectError> {
    if def.fields.iter().any(|field| field.name == column) {
        return Ok(Output::Field(column));
    }
    let is_key = match &def.key_space {
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) => def
            .source_key
            .as_ref()
            .is_none_or(|key| key.iter().any(|k| k == column)),
        KeySpace::Aggregate { group_by } => group_by.iter().any(|k| k == column),
        KeySpace::Join(join) => join.group_by.iter().any(|k| k.column == column),
    };
    if is_key {
        Ok(Output::Key(column))
    } else {
        Err(TargetSelectError::NotComputed {
            column: column.to_string(),
        })
    }
}

/// The `SELECT` of `outputs` over `def`'s tables, each output `::text` when
/// `as_text` (the tests' comparison form).
fn select_sql(
    def: &TransformDef,
    outputs: &[Output<'_>],
    as_text: bool,
) -> Result<String, TargetSelectError> {
    let join = match &def.key_space {
        KeySpace::Join(join) | KeySpace::JoinOneToOne(join) => Some(join),
        KeySpace::OneToOne | KeySpace::Aggregate { .. } => None,
    };
    // Each field inlined as the statements that write the target inline it:
    // a `JOIN`'s keeps its columns' tables, and a reference to a `GROUP BY`
    // key's field reads the key.
    let substituted: HashMap<&str, Expr> = match join {
        None => super::backfill::substitute_all_fields(def)
            .map_err(|err| TargetSelectError::Unsubstitutable(err.to_string()))?,
        Some(_) => def
            .fields
            .iter()
            .map(|field| {
                crate::staging::join::substitute(def, &field.expr, 0).ok_or_else(|| {
                    TargetSelectError::Unsubstitutable(
                        "a cyclic calculated-field alias reference".to_string(),
                    )
                })
            })
            .collect::<Result<_, _>>()?,
    }
    .into_iter()
    .zip(&def.fields)
    .map(|(expr, field)| (field.name.as_str(), expr))
    .collect();
    // A `JOIN`'s columns are qualified by their table's bare name; a single
    // table's are bare.
    let qualified = |column: &QualifiedColumn| {
        format!(
            "{}.{}",
            quote_ident(&column.table),
            quote_ident(&column.column)
        )
    };
    let expr_sql = |expr: &Expr| match join {
        Some(join) => render_with(expr, &|table, column| match table {
            Some(table) => format!("{}.{}", quote_ident(table), quote_ident(column)),
            None => match join.group_by.iter().find(|key| key.column == column) {
                Some(key) => qualified(key),
                None => quote_ident(column),
            },
        }),
        None => render_expr_sql(expr),
    };
    let select_list: Vec<String> = outputs
        .iter()
        .map(|output| {
            let (value, name) = match output {
                Output::Field(name) => (expr_sql(&substituted[name]), *name),
                Output::Key(name) => {
                    let value = match &def.key_space {
                        KeySpace::Join(join) => join
                            .group_by
                            .iter()
                            .find(|k| k.column == *name)
                            .map(qualified)
                            .expect("output_of admits only a GROUP BY key's column"),
                        KeySpace::JoinOneToOne(_) => {
                            format!("{}.{}", quote_ident(&def.source), quote_ident(name))
                        }
                        KeySpace::OneToOne | KeySpace::Aggregate { .. } => quote_ident(name),
                    };
                    (value, *name)
                }
            };
            if as_text {
                format!("({value})::text as {}", quote_ident(name))
            } else {
                format!("{value} as {}", quote_ident(name))
            }
        })
        .collect();

    let mut sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quoted_table(def.explicit_source_schema.as_deref(), &def.source)
    );
    if let Some(join) = join {
        for joined in &join.joins {
            let kind = match joined.kind {
                super::ast::JoinKind::Inner => "inner",
                super::ast::JoinKind::Left => "left",
            };
            let on: Vec<String> = joined
                .on
                .iter()
                .map(|term| format!("{} = {}", qualified(&term.lhs), qualified(&term.rhs)))
                .collect();
            sql.push_str(&format!(
                " {kind} join {} on {}",
                quoted_table(joined.explicit_right_schema.as_deref(), &joined.right),
                on.join(" and ")
            ));
        }
    }
    let group_by: Vec<String> = match &def.key_space {
        KeySpace::Aggregate { group_by } => group_by.iter().map(|k| quote_ident(k)).collect(),
        KeySpace::Join(join) => join.group_by.iter().map(qualified).collect(),
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) => Vec::new(),
    };
    if !group_by.is_empty() {
        sql.push_str(&format!(" group by {}", group_by.join(", ")));
    }
    Ok(sql)
}

/// Renders a [`KeySpace::OneToOne`] `def` back to the Postgres `SELECT` that
/// computes its whole target from the source's current contents: the primary
/// key `pk_column` as text first, then each calculated field as text, in
/// definition order. Tests compare a maintained target with the rows this
/// returns. It is [`render_target_select_sql`]'s rendering, each column
/// `::text`.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::OneToOne`], or if substitution fails
/// (see [`render_aggregate_select_sql`]).
#[cfg(any(test, feature = "test-util"))]
pub fn render_one_to_one_select_sql(def: &TransformDef, pk_column: &str) -> String {
    assert!(
        matches!(def.key_space, KeySpace::OneToOne),
        "render_one_to_one_select_sql called on a non-1-1 definition"
    );
    let outputs: Vec<Output<'_>> = std::iter::once(Output::Key(pk_column))
        .chain(def.fields.iter().map(|f| Output::Field(&f.name)))
        .collect();
    select_sql(def, &outputs, true)
        .expect("1-1 oracle rendering requires a substitutable definition")
}

/// Renders an [`KeySpace::Aggregate`] `def` back to the equivalent Postgres
/// `SELECT ... GROUP BY ...` (the correctness oracle): each field in
/// definition order, with the type Postgres gives it. It is
/// [`render_target_select_sql`]'s rendering of the fields alone.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`], or if substitution
/// fails (a cyclic alias chain, or a pathologically large expansion — see
/// [`super::backfill::substituted_field_exprs`]'s doc comment). This helper
/// is a test-only reference-SQL oracle (only ever called from `trellis`'s and
/// `benchmark`'s test/comparison code), so it stays infallible and panics on
/// either misuse.
#[cfg(any(test, feature = "test-util"))]
pub fn render_aggregate_select_sql(def: &TransformDef) -> String {
    assert!(
        matches!(def.key_space, KeySpace::Aggregate { .. }),
        "render_aggregate_select_sql called on a non-aggregate definition"
    );
    let outputs: Vec<Output<'_>> = def.fields.iter().map(|f| Output::Field(&f.name)).collect();
    select_sql(def, &outputs, false)
        .expect("aggregate oracle rendering requires a substitutable definition")
}

/// The set of `def.source` column names any calculated field references —
/// i.e. every [`Expr::Column`] name that isn't itself *another* calculated
/// field's name. `crate::capture::columns` uses it to decide which columns a
/// table's capture trigger images; it leaves out `GROUP BY` keys, which that
/// caller adds.
///
/// A field referencing a source column of its own name (`SELECT c AS c`, an
/// identity passthrough) is a passthrough, not a reference to another
/// calculated field, so its own source column is included — the same
/// `is_self_passthrough` carve-out `validate.rs`'s cycle detection makes (the
/// build's `SELECT a AS a` has it via `ddl::passthrough_source_column`).
///
/// Unlike `validate.rs`'s version of that carve-out, this one doesn't also
/// check that `name` actually names a real source column: it takes no
/// `source_columns` map. That's safe because the caller runs only against
/// persisted definitions, which `validate()` has already accepted, and
/// `validate()` rejects a field naming itself when that name isn't a real
/// source column (`UnresolvedColumn`). It is an invariant enforced by caller
/// discipline: a caller holding a definition that hasn't been through
/// `validate()` must thread `source_columns` through here and add the same
/// guard.
pub(crate) fn referenced_source_columns(def: &TransformDef) -> HashSet<String> {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();

    // A name inside an aggregate's argument that matches an aggregate field is
    // the source column of that name, so it is read even though
    // it isn't the enclosing field's own name.
    let aggregates = super::validate::aggregate_field_names(
        def.fields.iter().map(|f| (f.name.as_str(), &f.expr)),
    );
    let mut columns = HashSet::new();
    for field in &def.fields {
        collect_columns(&field.expr, field.name.as_str(), &field_names, &mut columns);
        let mut ignored = Vec::new();
        let mut aggregate_args = Vec::new();
        super::validate::collect_reads(
            &field.expr,
            &aggregates,
            false,
            &mut ignored,
            &mut aggregate_args,
        );
        columns.extend(aggregate_args);
    }
    columns
}

fn collect_columns(
    expr: &Expr,
    field_name: &str,
    field_names: &HashSet<&str>,
    out: &mut HashSet<String>,
) {
    expr.walk(&mut |e| match e {
        Expr::Column(name) => {
            let is_self_passthrough = name == field_name;
            if is_self_passthrough || !field_names.contains(name.as_str()) {
                out.insert(name.clone());
            }
        }
        Expr::SourceColumn { column, .. } => {
            out.insert(column.clone());
        }
        _ => {}
    });
}

/// Renders a calculated-field expression back to the equivalent Postgres
/// SQL expression text: the statements that write a 1-1 target embed it, so
/// Postgres computes the field (ADR-0002) — the ADR-0004 claim ("our grammar
/// is an immutable subset of Postgres semantics") made literal for every
/// expression. Column references are quoted identifiers, since a source
/// column name could collide with a SQL keyword; literals carry an explicit
/// cast so the rendered text is unambiguous regardless of context.
pub fn render_expr_sql(expr: &Expr) -> String {
    render_with(expr, &|_, name| quote_ident(name))
}

/// [`render_expr_sql`] and [`render_qualified_expr_sql`]: the expression
/// with each column read rendered by `column` (of the table it names, if
/// any, and its name).
fn render_with(expr: &Expr, column: &dyn Fn(Option<&str>, &str) -> String) -> String {
    let render = |e: &Expr| render_with(e, column);
    match expr {
        Expr::Column(name) => column(None, name),
        Expr::SourceColumn {
            table,
            column: name,
            ..
        } => column(Some(table), name),
        Expr::NumberLiteral(text) => number_literal_sql(text),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::BooleanLiteral(value) => value.to_string(),
        // Bare, so Postgres types it from the same context `validate` did.
        Expr::Null => "null".to_string(),
        Expr::TypedLiteral { value_type, text } => typed_literal::render_sql(*value_type, text),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = super::registry::operator_spec(*op).symbol;
            format!("({} {symbol} {})", render(lhs), render(rhs))
        }
        Expr::Negate(operand) => format!("(-({}))", render(operand)),
        Expr::Cast { expr, target } => format!("cast({} as {target})", render(expr)),
        Expr::Not(operand) => format!("(not {})", render(operand)),
        Expr::IsNull { expr, negated } => format!(
            "({} is {}null)",
            render(expr),
            if *negated { "not " } else { "" }
        ),
        Expr::Case {
            branches,
            otherwise,
        } => render_case_sql(branches, otherwise.as_deref(), render),
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)`: the parser's only accepted `COUNT`
            // shape, but its AST has no argument to render — `count()` is
            // not valid Postgres, so this renders the `*` back explicitly
            // rather than falling through to the generic `name(args)` case
            // below.
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => match super::ast::as_extract(name, args) {
            Some((field, operand)) => format!("extract({field} from {})", render(operand)),
            None => {
                let rendered: Vec<String> = args.iter().map(render).collect();
                format!("{}({})", name.to_lowercase(), rendered.join(", "))
            }
        },
    }
}

/// A searched `CASE` as SQL text, each condition and result rendered by
/// `render`. Shared by every renderer so they spell the construct alike.
pub(crate) fn render_case_sql(
    branches: &[(Expr, Expr)],
    otherwise: Option<&Expr>,
    mut render: impl FnMut(&Expr) -> String,
) -> String {
    let mut text = String::from("(case");
    for (condition, result) in branches {
        text.push_str(&format!(
            " when {} then {}",
            render(condition),
            render(result)
        ));
    }
    if let Some(otherwise) = otherwise {
        text.push_str(&format!(" else {}", render(otherwise)));
    }
    text.push_str(" end)");
    text
}

/// An unadorned number literal, cast to the type `validate` infers for it
/// (`integer` if it fits, else `bigint`, else `numeric`), which is the type
/// Postgres's own lexer gives the bare literal. Overflow, and the result
/// type an operator over it has, are then the ones the definition's
/// declared column types were inferred with: `int4_col + 1` is `integer`
/// and overflows where an `integer` does. `self_check`'s audit renders a
/// literal with this too, so a date takes `date + integer` there as here.
pub(crate) fn number_literal_sql(text: &str) -> String {
    // A negative literal is parenthesized: `::` binds tighter than the sign,
    // and `-2147483648::integer` casts 2147483648 first.
    let shown = if text.starts_with('-') {
        format!("({text})")
    } else {
        text.to_string()
    };
    let pg_type = match super::validate::number_literal_type(text) {
        ValueType::Integer(IntWidth::Int4) => "integer",
        ValueType::Integer(IntWidth::Int8) => "bigint",
        _ => "numeric",
    };
    format!("{shown}::{pg_type}")
}

/// [`render_expr_sql`] with every source column qualified by `source_sql`
/// (already-quoted SQL text — a quoted table name, or a query alias such as
/// `s`), for SQL that reads the source under an alias: the group-basis and
/// `JOIN` partial reads.
pub(crate) fn render_qualified_expr_sql(expr: &Expr, source_sql: &str) -> String {
    render_with(expr, &|_, name| {
        format!("{source_sql}.{}", quote_ident(name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{FieldDef, KeySpace, Operator, Predicate};

    fn def() -> TransformDef {
        TransformDef {
            target: "order_totals".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![
                FieldDef {
                    name: "double_price".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("price".to_string())),
                        rhs: Box::new(Expr::Column("price".to_string())),
                    },
                },
                FieldDef {
                    name: "total".to_string(),
                    expr: Expr::BinaryOp {
                        op: Operator::Add,
                        lhs: Box::new(Expr::Column("double_price".to_string())),
                        rhs: Box::new(Expr::Column("tax".to_string())),
                    },
                },
            ],
            predicate: Predicate::True,
            without_source_index: false,
            adopt_target: false,
            source_key: None,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    /// `MIN(b)` reads the source column `b` even though a field
    /// named `b` exists (an aggregate), so the capture trigger must image it.
    #[test]
    fn referenced_source_columns_includes_a_source_column_named_like_an_aggregate_field() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(a) AS b, MIN(b) AS lo",
        )
        .expect("parse");
        let columns = referenced_source_columns(&def);
        assert!(columns.contains("b"), "{columns:?}");
        assert!(columns.contains("a"), "{columns:?}");
    }

    /// `SUM(s.val)` reads the source column `val` though a field
    /// named `val` exists, so the capture trigger images it and the SQL
    /// oracle sums it.
    #[test]
    fn a_qualified_name_in_an_aggregate_reads_the_source_column() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, (grp + 1) AS val, \
             SUM(s.val) AS total",
        )
        .expect("parse");
        let columns = referenced_source_columns(&def);
        assert!(columns.contains("val"), "{columns:?}");
        let sql = render_aggregate_select_sql(&def);
        assert!(sql.contains(r#"sum("val")"#), "{sql}");
        assert!(!sql.contains(r#"sum(("grp""#), "{sql}");
    }

    #[test]
    fn referenced_source_columns_excludes_calculated_field_names() {
        let columns = referenced_source_columns(&def());
        assert_eq!(
            columns,
            HashSet::from(["price".to_string(), "tax".to_string()])
        );
    }

    /// A field naming itself (`SELECT c AS c`, an identity passthrough) must
    /// still include its own source column — `is_self_passthrough`'s carve-out
    /// in `collect_columns` — or the capture trigger would not image it,
    /// though the build (`SELECT a AS a`) special-cases it via
    /// `ddl::passthrough_source_column`.
    #[test]
    fn referenced_source_columns_includes_a_self_named_passthrough_columns_own_source() {
        let def = TransformDef {
            target: "t1".to_string(),
            source: "t0".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "c".to_string(),
                expr: Expr::Column("c".to_string()),
            }],
            predicate: Predicate::True,
            without_source_index: false,
            adopt_target: false,
            source_key: None,
            explicit_source_schema: None,
            explicit_target_schema: None,
        };
        let columns = referenced_source_columns(&def);
        assert_eq!(
            columns,
            HashSet::from(["c".to_string()]),
            "a field's own source column must not be excluded just because it shares the \
             field's name"
        );
    }

    #[test]
    fn render_expr_sql_renders_greater_than_composed_with_a_function_call() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::FunctionCall {
                name: "STRPOS".to_string(),
                args: vec![
                    Expr::Column("name".to_string()),
                    Expr::StringLiteral("foo".to_string()),
                ],
            }),
            rhs: Box::new(Expr::NumberLiteral("0".to_string())),
        };
        assert_eq!(
            render_expr_sql(&expr),
            "(strpos(\"name\", 'foo'::text) > 0::integer)"
        );
    }

    #[test]
    fn render_expr_sql_renders_coalesce_as_a_postgres_function_call() {
        // COALESCE renders through the generic `name(args)` arm to lowercase
        // `coalesce(...)` — valid Postgres.
        let expr = Expr::FunctionCall {
            name: "COALESCE".to_string(),
            args: vec![
                Expr::Column("amount".to_string()),
                Expr::NumberLiteral("0".to_string()),
            ],
        };
        assert_eq!(render_expr_sql(&expr), "coalesce(\"amount\", 0::integer)");
    }

    #[test]
    fn render_expr_sql_escapes_a_single_quote_in_a_function_call_string_literal() {
        let expr = Expr::FunctionCall {
            name: "REGEXP_COUNT".to_string(),
            args: vec![
                Expr::Column("description".to_string()),
                Expr::StringLiteral("o'clock".to_string()),
            ],
        };
        assert_eq!(
            render_expr_sql(&expr),
            "regexp_count(\"description\", 'o''clock'::text)"
        );
    }

    /// Every construct of the grammar, through both write-path renderers:
    /// the same text but for a column, which the qualified renderer reads
    /// through the alias it is given.
    #[test]
    fn both_write_path_renderers_render_every_construct_alike() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM s SELECT CASE WHEN a > 1 AND NOT (b IS NULL) THEN -(a + 2.5) \
             WHEN b IS NOT NULL THEN CAST(b AS numeric) ELSE COALESCE(a, NULL) END AS x, \
             EXTRACT(year FROM d) AS y, DATE '2024-01-01' AS z, 'o''clock' AS w, \
             TRUE AS v, ROUND(m, 2) AS r, s.a AS sa, -2147483648 AS lo",
        )
        .expect("parses");
        let mut exprs: Vec<Expr> = def.fields.into_iter().map(|f| f.expr).collect();
        // `COUNT(*)`, which no 1-1 field holds, renders its `*` back.
        exprs.push(Expr::FunctionCall {
            name: "COUNT".to_string(),
            args: Vec::new(),
        });
        let expected = [
            "(case when ((\"a\" > 1::integer) AND (not (\"b\" is null))) \
             then (-((\"a\" + 2.5::numeric))) when (\"b\" is not null) \
             then cast(\"b\" as numeric) else coalesce(\"a\", null) end)",
            "extract(year from \"d\")",
            "'2024-01-01'::date",
            "'o''clock'::text",
            "true",
            "round(\"m\", 2::integer)",
            "\"a\"",
            "(-2147483648)::integer",
            "count(*)",
        ];
        assert_eq!(exprs.len(), expected.len());
        for (expr, expected) in exprs.iter().zip(expected) {
            assert_eq!(render_expr_sql(expr), expected);
            assert_eq!(
                render_qualified_expr_sql(expr, "q"),
                expected
                    .replace("\"a\"", "q.\"a\"")
                    .replace("\"b\"", "q.\"b\"")
                    .replace("\"d\"", "q.\"d\"")
                    .replace("\"m\"", "q.\"m\"")
            );
        }
    }

    /// The whole-target `SELECT` of each granularity, columns in the order
    /// given (the target's), keys read from where the target's key comes
    /// from, and a `JOIN`'s columns qualified by their tables.
    #[test]
    fn target_select_renders_every_granularity_in_the_target_columns_order() {
        let cases = [
            (
                "TRANSFORM t FROM s SELECT price * 2 AS doubled, doubled + 1 AS more",
                vec!["id", "doubled", "more"],
                r#"select "id" as "id", ("price" * 2::integer) as "doubled", (("price" * 2::integer) + 1::integer) as "more" from "s""#,
            ),
            (
                "TRANSFORM t FROM shop.orders GROUP BY owner_id, kind \
                 SELECT owner_id AS owner_id, COUNT(*) AS n, SUM(price) AS total",
                vec!["owner_id", "kind", "total", "n"],
                r#"select "owner_id" as "owner_id", "kind" as "kind", sum("price") as "total", count(*) as "n" from "shop"."orders" group by "owner_id", "kind""#,
            ),
            (
                "TRANSFORM t FROM orders AS o LEFT JOIN customers c ON o.customer_id = c.id \
                 GROUP BY c.tier SELECT SUM(o.amount) AS total",
                vec!["tier", "total"],
                r#"select "customers"."tier" as "tier", sum("orders"."amount") as "total" from "orders" left join "customers" on "orders"."customer_id" = "customers"."id" group by "customers"."tier""#,
            ),
            (
                "TRANSFORM t FROM orders JOIN customers ON orders.customer_id = customers.id \
                 LEFT JOIN regions ON orders.region_id = regions.id \
                 SELECT customers.name AS customer_name, regions.name AS region",
                vec!["id", "customer_name", "region"],
                r#"select "orders"."id" as "id", "customers"."name" as "customer_name", "regions"."name" as "region" from "orders" inner join "customers" on "orders"."customer_id" = "customers"."id" left join "regions" on "orders"."region_id" = "regions"."id""#,
            ),
        ];
        for (text, columns, expected) in cases {
            assert_eq!(
                target_select_sql(
                    text,
                    super::super::grammar::CURRENT_GRAMMAR_VERSION,
                    &columns
                )
                .expect("renders"),
                expected,
                "{text}"
            );
        }
    }

    /// A column the definition computes nothing for is refused, naming it:
    /// a `GROUP BY` target's non-key, or a column outside a 1-1's `KEY (...)`.
    #[test]
    fn target_select_refuses_a_column_the_definition_does_not_compute() {
        for (text, columns) in [
            (
                "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
                vec!["g", "n", "stray"],
            ),
            (
                "TRANSFORM t FROM s KEY (code) SELECT price AS price",
                vec!["code", "price", "stray"],
            ),
        ] {
            let err = target_select_sql(
                text,
                super::super::grammar::CURRENT_GRAMMAR_VERSION,
                &columns,
            )
            .expect_err("refused");
            assert_eq!(err.code(), crate::ErrorCode::Validation, "{err}");
            assert!(err.to_string().contains("\"stray\""), "{err}");
        }
    }

    /// The text loads under its own grammar version: grammar 1 reads every
    /// name exactly, so an upper-case name stays upper case.
    #[test]
    fn target_select_reads_the_text_under_its_grammar_version() {
        let text = "TRANSFORM t FROM Items SELECT Price AS Price";
        assert_eq!(
            target_select_sql(text, 1, &["id", "Price"]).expect("renders"),
            r#"select "id" as "id", "Price" as "Price" from "Items""#
        );
        assert_eq!(
            target_select_sql(text, 2, &["id", "price"]).expect("renders"),
            r#"select "id" as "id", "price" as "price" from "items""#
        );
        let err = target_select_sql("TRANSFORM oops", 2, &["id"]).expect_err("refused");
        assert_eq!(err.code(), crate::ErrorCode::Parse, "{err}");
    }
}
