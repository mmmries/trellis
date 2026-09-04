//! A from-scratch recompute of a 1-1 target from current source data
//! (issue #25), used to assert any incrementally-maintained target is
//! byte-equal to a recompute (`docs/data-flow.md#correctness`).
//!
//! **This module's [`recompute`] is the evaluator-driven recompute**: it reads
//! each source row's text image and feeds it through the engine's own
//! `evaluate`. Its role is a *secondary cross-check*, not the authority. The
//! primary oracle is Postgres itself (by rendering the definition as a `SELECT`).
//! Comparing the evaluator-driven recompute against the Postgres-SQL oracle
//! ensures the engine mirrors Postgres semantics exactly.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

use crate::pool::{Pool, quote_ident};

use super::ast::{Expr, KeySpace, Operator, RelationshipDef, TransformDef, ValueType};
use super::eval::{EvalError, RegexCache, Row, Value, evaluate, evaluate_aggregate};
use super::registry::lookup_aggregate_function;

/// Why a from-scratch recompute failed.
#[derive(Debug)]
pub enum OracleError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// Evaluating a source row's calculated fields failed (see
    /// [`EvalError`]); a row that fails here would be quarantined by the
    /// real apply path, not silently dropped from the recompute.
    Eval(EvalError),
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleError::Db(err) => {
                write!(f, "oracle recompute database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            OracleError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            OracleError::Eval(err) => write!(f, "oracle recompute evaluation error: {err}"),
        }
    }
}

impl std::error::Error for OracleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OracleError::Db(err) => Some(err),
            OracleError::Pool(err) => Some(err),
            OracleError::Eval(err) => Some(err),
        }
    }
}

impl From<tokio_postgres::Error> for OracleError {
    fn from(err: tokio_postgres::Error) -> Self {
        OracleError::Db(err)
    }
}

impl From<crate::error::Error> for OracleError {
    fn from(err: crate::error::Error) -> Self {
        OracleError::Pool(err)
    }
}

impl From<EvalError> for OracleError {
    fn from(err: EvalError) -> Self {
        OracleError::Eval(err)
    }
}

/// A from-scratch recompute of `def`'s target: every source row's primary
/// key (as text) mapped to its calculated columns.
pub type Recomputed = HashMap<String, HashMap<String, Option<Value>>>;

/// Recomputes `def`'s entire target from `def.source`'s current contents,
/// keyed by `pk_column` (the source table's primary key, e.g. from
/// [`super::ddl::source_primary_key`]). `source_columns` gives each
/// referenced source column's [`ValueType`], the same map `def` was
/// validated against.
///
/// Reads `pk_column` plus every source column any calculated field
/// references, as text, then runs each row through [`evaluate`] — the exact
/// function the incremental path evaluates deltas with. No SQL expression
/// here computes a calculated field's value; only column selection is SQL.
pub async fn recompute(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Recomputed, OracleError> {
    let referenced = referenced_source_columns(def);

    let mut select_list = vec![format!("{}::text", quote_ident(pk_column))];
    select_list.extend(
        referenced
            .iter()
            .map(|c| format!("{}::text", quote_ident(c))),
    );

    let sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quote_ident(&def.source)
    );

    let client = pool.get().await?;
    let rows = client.query(sql.as_str(), &[]).await?;

    let mut result = Recomputed::with_capacity(rows.len());
    // Reused across every row below (issue #68): `def` is fixed for the
    // whole recompute, so any `regexp_count` pattern it references compiles
    // to the same `Regex` on every row.
    let mut regex_cache = RegexCache::new();
    for db_row in rows {
        let pk_value: String = db_row.get(0);
        let mut image: Row = HashMap::with_capacity(referenced.len());
        for (i, column) in referenced.iter().enumerate() {
            let value: Option<String> = db_row.get(i + 1);
            image.insert(column.clone(), value);
        }
        let evaluated = evaluate(def, &image, source_columns, &mut regex_cache)?;
        result.insert(pk_value, evaluated);
    }

    Ok(result)
}

/// Recomputes an [`KeySpace::Aggregate`] `def`'s entire target from
/// `def.source`'s current contents, keyed by the grouping columns' text
/// values joined with `,` (there's no single primary-key column to key
/// [`Recomputed`] by, unlike [`recompute`]'s 1-1 case — the group is the
/// key). Groups rows in Rust (reading every grouping and referenced column
/// as text, then partitioning by the grouping columns' values) rather than
/// issuing a SQL `GROUP BY` itself, so this stays the same kind of
/// evaluator-driven secondary cross-check `recompute` is: the real oracle is
/// Postgres's own `GROUP BY` via [`render_aggregate_select_sql`], not this
/// function.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`].
pub async fn recompute_aggregate(
    pool: &Pool,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Recomputed, OracleError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("recompute_aggregate called on a non-aggregate definition");
    };

    let mut columns = referenced_source_columns(def);
    for column in group_by {
        columns.insert(column.clone());
    }
    let columns: Vec<String> = columns.into_iter().collect();

    let select_list: Vec<String> = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    let sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quote_ident(&def.source)
    );

    let client = pool.get().await?;
    let db_rows = client.query(sql.as_str(), &[]).await?;

    let mut groups: HashMap<String, Vec<Row>> = HashMap::new();
    for db_row in db_rows {
        let mut image: Row = HashMap::with_capacity(columns.len());
        for (i, column) in columns.iter().enumerate() {
            let value: Option<String> = db_row.get(i);
            image.insert(column.clone(), value);
        }
        let key = group_key(&image, group_by);
        groups.entry(key).or_default().push(image);
    }

    let mut result = Recomputed::with_capacity(groups.len());
    let mut regex_cache = RegexCache::new();
    for rows in groups.values() {
        let evaluated = evaluate_aggregate(def, rows, source_columns, &mut regex_cache)?;
        let key = group_key(&rows[0], group_by);
        result.insert(key, evaluated);
    }

    Ok(result)
}

/// The composite grouping-key text used by [`recompute_aggregate`] to key
/// [`Recomputed`] — every row in a group shares these values, so any row's
/// image gives the same key. A grouping column is assumed non-`NULL` (the
/// typical case for a real foreign/primary key); this doesn't attempt to
/// match Postgres's "`NULL` groups with `NULL`" `GROUP BY` semantics.
fn group_key(image: &Row, group_by: &[String]) -> String {
    // Length-prefix each component (`"{len}:{value}"`) rather than joining
    // on a bare separator: a Text grouping column's value can itself
    // contain any separator character (including a comma), which would
    // make two distinct groupings collide, e.g. `(a="x,y", b="z")` vs.
    // `(a="x", b="y,z")`. Prefixing each value with its own byte length
    // makes the encoding unambiguous regardless of what characters the
    // value contains — the length prefix itself is always parsed as a
    // number, not searched for as a delimiter.
    group_by
        .iter()
        .map(|c| image.get(c).cloned().flatten().unwrap_or_default())
        .map(|v| format!("{}:{v}", v.len()))
        .collect::<String>()
}

/// Renders an [`KeySpace::Aggregate`] `def` back to the equivalent Postgres
/// `SELECT ... GROUP BY ...` (issue #11 groundwork's correctness oracle):
/// each calculated field renders via [`render_expr_sql`] exactly as the 1-1
/// case does (a `SUM`/`MIN`/`MAX`/`AVG` call renders like any other function
/// call — `render_expr_sql` already lowercases the name and wraps its args),
/// so no separate aggregate-rendering logic is needed; only the `GROUP BY`
/// clause itself is new.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`].
pub fn render_aggregate_select_sql(def: &TransformDef) -> String {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("render_aggregate_select_sql called on a non-aggregate definition");
    };

    let select_list: Vec<String> = def
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} as {}",
                render_expr_sql(&field.expr),
                quote_ident(&field.name)
            )
        })
        .collect();
    let group_cols: Vec<String> = group_by.iter().map(|c| quote_ident(c)).collect();

    format!(
        "select {} from {} group by {}",
        select_list.join(", "),
        quote_ident(&def.source),
        group_cols.join(", ")
    )
}

/// The set of `def.source` column names any calculated field references —
/// i.e. every [`Expr::Column`] name that isn't itself another calculated
/// field's name. Used to select only the columns a recompute actually needs.
fn referenced_source_columns(def: &TransformDef) -> HashSet<String> {
    let field_names: HashSet<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();

    let mut columns = HashSet::new();
    for field in &def.fields {
        collect_columns(&field.expr, &field_names, &mut columns);
    }
    columns
}

fn collect_columns(expr: &Expr, field_names: &HashSet<&str>, out: &mut HashSet<String>) {
    match expr {
        Expr::Column(name) => {
            if !field_names.contains(name.as_str()) {
                out.insert(name.clone());
            }
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
        // Not a source-column reference by name; the validator (#23)
        // rejects relationship paths outright, so `render_expr_sql` never
        // has to render one (issue #25 is grammar + AST only).
        Expr::RelationshipPath { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_columns(lhs, field_names, out);
            collect_columns(rhs, field_names, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns(arg, field_names, out);
            }
        }
    }
}

/// Renders a calculated-field expression back to the equivalent Postgres
/// SQL expression text (issue #64), so the generative correctness oracle
/// (`docs/generative-test-suite.md`) can cross-check a function call's
/// rendered `SELECT` against this evaluator's output over the same source
/// data — the ADR-0004 claim ("our grammar is an immutable subset of
/// Postgres semantics") made checkable per expression, not just per
/// operator. Column references are quoted identifiers, since a source
/// column name could collide with a SQL keyword; literals carry an explicit
/// cast so the rendered text is unambiguous regardless of context.
pub fn render_expr_sql(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => quote_ident(name),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        Expr::RelationshipPath { rel, column } => panic!(
            "render_expr_sql called on an unresolved relationship path '{rel}.{column}' \
             — the validator (#23) should have rejected this before reaching the oracle \
             (issue #25 is grammar + AST only)"
        ),
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_expr_sql(lhs),
                render_expr_sql(rhs)
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            // `COUNT(*)` (issue #75): the parser's only accepted `COUNT`
            // shape, but its AST has no argument to render — `count()` is
            // not valid Postgres, so this renders the `*` back explicitly
            // rather than falling through to the generic `name(args)` case
            // below.
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args.iter().map(render_expr_sql).collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
}

/// Renders a relationship-enriched 1-1 [`TransformDef`] back to the equivalent
/// Postgres `SELECT` (issue #32), so the generative correctness oracle can
/// assert the engine's incrementally-maintained target is byte-equal to what
/// Postgres itself computes over the same source + related data (ADR-0004).
///
/// `relationships` is keyed by relationship name — the head of a
/// `<rel>.<column>` path — mirroring how the evaluator's
/// [`super::eval::RelationshipContext`] is caller-provided (issue #28/#29): the
/// oracle needs the same relationship endpoints (`from_col`, `to_table`,
/// `to_col`) the eval-side context was built from, so both sides describe the
/// same join. A referenced relationship absent from the map is a caller bug
/// (the validator resolves paths before eval/oracle) and panics, like
/// [`render_expr_sql`]'s unresolved-path guard.
///
/// The two relationship shapes render exactly as the evaluator resolves them,
/// so the SQL and the engine agree row-for-row:
///
/// * A **to-one** enrichment (a bare `<rel>.<column>` path, issue #28) becomes
///   a `LEFT JOIN` from the source to the to-side table on
///   `source.from_col = rel.to_col`, projecting `rel.column`. `LEFT JOIN` keeps
///   a from-row with no match (or a `NULL` join key) alive with a `NULL`
///   enrichment — matching #28's "no-match / NULL fk => NULL, from-row
///   survives".
/// * A **to-many** aggregate (`sum(<rel>.<column>)`, issue #29) becomes a
///   correlated aggregate subquery over the to-side table filtered by the join
///   key. Postgres's aggregate over the empty correlated set gives `count → 0`
///   and `sum`/`min`/`max`/`avg → NULL`, and `count(rel.column)` counts only
///   non-`NULL` values — exactly [`super::eval::eval_to_many_aggregate`] /
///   `reduce_numeric_aggregate`.
///
/// The relationship name doubles as the SQL alias for both the `LEFT JOIN`
/// target and the correlated subquery's table, so a to-side table that shares
/// the source table's name (or is referenced twice) stays unambiguous.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::OneToOne`], or a referenced
/// relationship is missing from `relationships`.
pub fn render_relationship_select_sql(
    def: &TransformDef,
    relationships: &HashMap<String, RelationshipDef>,
) -> String {
    assert!(
        matches!(def.key_space, KeySpace::OneToOne),
        "render_relationship_select_sql called on a non-1-1 definition"
    );

    // Every to-one path (a bare `<rel>.<column>`, anywhere in an expression
    // tree) needs a LEFT JOIN. A path that is the sole argument of an aggregate
    // is a to-many enrichment — rendered as a correlated subquery below — and
    // contributes no JOIN. A `BTreeSet` dedups a relationship referenced by
    // several columns and orders the JOINs deterministically for stable output.
    let mut to_one_rels: BTreeSet<&str> = BTreeSet::new();
    for field in &def.fields {
        collect_to_one_rels(&field.expr, &mut to_one_rels);
    }

    let select_list: Vec<String> = def
        .fields
        .iter()
        .map(|field| {
            format!(
                "{} as {}",
                render_rel_expr_sql(&field.expr, &def.source, relationships),
                quote_ident(&field.name)
            )
        })
        .collect();

    let mut sql = format!(
        "select {} from {}",
        select_list.join(", "),
        quote_ident(&def.source)
    );
    for rel_name in to_one_rels {
        let rel = relationships.get(rel_name).unwrap_or_else(|| {
            panic!("render_relationship_select_sql: unknown relationship '{rel_name}'")
        });
        sql.push_str(&format!(
            " left join {to_table} as {alias} on {source}.{from_col} = {alias}.{to_col}",
            to_table = quote_ident(&rel.to_table),
            alias = quote_ident(rel_name),
            source = quote_ident(&def.source),
            from_col = quote_ident(&rel.from_col),
            to_col = quote_ident(&rel.to_col),
        ));
    }
    sql
}

/// Collects the names of to-one relationships referenced by a bare path (so
/// [`render_relationship_select_sql`] can emit their `LEFT JOIN`s). Mirrors the
/// evaluator's structural test in #29: a `<rel>.<column>` path that is the sole
/// argument of an aggregate call is a to-many enrichment (a correlated
/// subquery, no JOIN), so recursion stops there; every other path is to-one.
fn collect_to_one_rels<'a>(expr: &'a Expr, out: &mut BTreeSet<&'a str>) {
    match expr {
        Expr::RelationshipPath { rel, .. } => {
            out.insert(rel.as_str());
        }
        Expr::FunctionCall { name, args }
            if lookup_aggregate_function(name).is_some()
                && matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            // To-many enrichment: rendered as a correlated subquery, no JOIN.
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_to_one_rels(arg, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_to_one_rels(lhs, out);
            collect_to_one_rels(rhs, out);
        }
        Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
    }
}

/// Renders one calculated-field expression of a relationship-enriched 1-1
/// definition. Like [`render_expr_sql`] but relationship-aware: source columns
/// are qualified with the source table (so they don't collide with a JOINed
/// to-side column of the same name), a bare to-one path reads off its JOIN
/// alias, and an aggregate-wrapped to-many path becomes a correlated subquery.
fn render_rel_expr_sql(
    expr: &Expr,
    source: &str,
    relationships: &HashMap<String, RelationshipDef>,
) -> String {
    match expr {
        Expr::Column(name) => format!("{}.{}", quote_ident(source), quote_ident(name)),
        Expr::NumberLiteral(text) => format!("{text}::numeric"),
        Expr::StringLiteral(text) => format!("'{}'::text", text.replace('\'', "''")),
        // A to-one enrichment reads the referenced column off the to-side table
        // the LEFT JOIN brought in, aliased by the relationship name.
        Expr::RelationshipPath { rel, column } => {
            format!("{}.{}", quote_ident(rel), quote_ident(column))
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render_rel_expr_sql(lhs, source, relationships),
                render_rel_expr_sql(rhs, source, relationships)
            )
        }
        // A to-many aggregate (issue #29): an aggregate whose sole argument is a
        // relationship path — the same structural shape the evaluator matches —
        // renders as a correlated aggregate subquery over the to-side table,
        // filtered by the join key. Postgres's empty-set semantics (`count → 0`,
        // the rest → `NULL`, `count(col)` counting non-`NULL`) are exactly what
        // `eval_to_many_aggregate` reproduces, so the two converge.
        Expr::FunctionCall { name, args }
            if lookup_aggregate_function(name).is_some()
                && matches!(args.as_slice(), [Expr::RelationshipPath { .. }]) =>
        {
            let Expr::RelationshipPath { rel, column } = &args[0] else {
                unreachable!("guarded by the matches! above");
            };
            let reldef = relationships.get(rel.as_str()).unwrap_or_else(|| {
                panic!("render_relationship_select_sql: unknown relationship '{rel}'")
            });
            format!(
                "(select {func}({alias}.{col}) from {to_table} as {alias} \
                 where {alias}.{to_col} = {source}.{from_col})",
                func = name.to_lowercase(),
                alias = quote_ident(rel),
                col = quote_ident(column),
                to_table = quote_ident(&reldef.to_table),
                to_col = quote_ident(&reldef.to_col),
                source = quote_ident(source),
                from_col = quote_ident(&reldef.from_col),
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let rendered_args: Vec<String> = args
                .iter()
                .map(|arg| render_rel_expr_sql(arg, source, relationships))
                .collect();
            format!("{}({})", name.to_lowercase(), rendered_args.join(", "))
        }
    }
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
        }
    }

    #[test]
    fn referenced_source_columns_excludes_calculated_field_names() {
        let columns = referenced_source_columns(&def());
        assert_eq!(
            columns,
            HashSet::from(["price".to_string(), "tax".to_string()])
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
            "(strpos(\"name\", 'foo'::text) > 0::numeric)"
        );
    }

    fn category_rel() -> HashMap<String, RelationshipDef> {
        HashMap::from([(
            "category".to_string(),
            RelationshipDef {
                name: "category".to_string(),
                from_table: "products".to_string(),
                from_col: "category_id".to_string(),
                to_table: "categories".to_string(),
                to_col: "id".to_string(),
            },
        )])
    }

    fn comments_rel() -> HashMap<String, RelationshipDef> {
        HashMap::from([(
            "comments".to_string(),
            RelationshipDef {
                name: "comments".to_string(),
                from_table: "posts".to_string(),
                from_col: "id".to_string(),
                to_table: "comments".to_string(),
                to_col: "post_id".to_string(),
            },
        )])
    }

    #[test]
    fn render_relationship_select_sql_renders_a_to_one_left_join() {
        let def = TransformDef {
            target: "product_view".to_string(),
            source: "products".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![
                FieldDef {
                    name: "id".to_string(),
                    expr: Expr::Column("id".to_string()),
                },
                FieldDef {
                    name: "category_name".to_string(),
                    expr: Expr::RelationshipPath {
                        rel: "category".to_string(),
                        column: "name".to_string(),
                    },
                },
            ],
            predicate: Predicate::True,
        };
        assert_eq!(
            render_relationship_select_sql(&def, &category_rel()),
            "select \"products\".\"id\" as \"id\", \
             \"category\".\"name\" as \"category_name\" \
             from \"products\" \
             left join \"categories\" as \"category\" \
             on \"products\".\"category_id\" = \"category\".\"id\""
        );
    }

    #[test]
    fn render_relationship_select_sql_renders_a_to_many_correlated_aggregate() {
        let def = TransformDef {
            target: "post_stats".to_string(),
            source: "posts".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "total_words".to_string(),
                expr: Expr::FunctionCall {
                    name: "SUM".to_string(),
                    args: vec![Expr::RelationshipPath {
                        rel: "comments".to_string(),
                        column: "word_count".to_string(),
                    }],
                },
            }],
            predicate: Predicate::True,
        };
        assert_eq!(
            render_relationship_select_sql(&def, &comments_rel()),
            "select (select sum(\"comments\".\"word_count\") \
             from \"comments\" as \"comments\" \
             where \"comments\".\"post_id\" = \"posts\".\"id\") as \"total_words\" \
             from \"posts\""
        );
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
}
