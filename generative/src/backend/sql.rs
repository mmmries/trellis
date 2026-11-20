//! Shared DDL/DML/read-back plumbing: every piece of
//! [`ManualBackend`](super::ManualBackend)'s rendering and raw-SQL logic
//! that has nothing to do with *how* the engine itself is run — only with
//! turning a [`Program`]/[`Op`]/[`Snapshot`] into concrete SQL text and back
//! — lives here instead, so [`SubprocessBackend`](super::SubprocessBackend)
//! (the subprocess-supervised backend) can reuse it verbatim rather
//! than re-deriving the same DDL/DML shapes a second time. Split out of
//! `manual.rs` when `SubprocessBackend` was added; every function here was
//! previously a private `ManualBackend` method or free function, unchanged
//! in behavior, just parameterized over `&HashMap<String, Table>`/
//! `&tokio_postgres::Client` instead of closing over `&self`.
//!
//! Deliberately does **not** know anything about starting/stopping/
//! restarting an engine — that's each backend's own, genuinely different
//! concern (an in-process [`trellis::Client`] vs. a real OS subprocess
//! `Command`), and is the only reason two `Backend` impls exist at all. See
//! the `backend` module doc comment's "backend seam" note: this module is
//! part of that seam, reachable only from within `generative::backend`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use trellis::dev::defs::StoredStatus;
use trellis::dev::defs::ast::{
    Expr, FieldDef, JoinKind, KeySpace, Operator, Predicate, QualifiedColumn, TransformDef,
    ValueType,
};
use trellis::dev::staging::{
    StagingError, converged_through, converged_through_except_held, watermark_token,
};

use super::DefinitionState;
use super::ring_wait::{
    Expiry, ProgressDeadline, QuiesceBudget, await_ring, dump_state, expiry_reason,
};

use crate::model::{
    Column, Op, OpOutcome, PartitionAction, PartitionStrategy, Partitioning, Table, TxnWrap,
};

/// Quotes a Postgres identifier for safe interpolation into SQL text,
/// mirroring `trellis::pool`'s own (crate-private) helper of the same name.
pub(super) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The Postgres type name a [`ValueType`] casts to.
///
/// Returns [`std::borrow::Cow`] rather than a bare `&'static str`,
/// mirroring `defs::ddl::pg_type_name`'s own signature:
/// every family but [`trellis::dev::defs::PgType::Enum`] still
/// renders a fixed keyword and stays `Cow::Borrowed` — the generator itself
/// never emits an `Other`-typed column at all (see `generate::Column`'s doc
/// comment), so the owned-`Cow` branch is unreachable here in practice, but
/// this stays parity-complete with the real `defs::ddl::pg_type_name` rather
/// than silently drifting out of sync with it.
pub(super) fn pg_type_name(value_type: ValueType) -> std::borrow::Cow<'static, str> {
    match value_type {
        ValueType::Numeric => "numeric".into(),
        // An exact integer renders as its own Postgres width,
        // mirroring `defs::ddl::pg_type_name`.
        ValueType::Integer(width) => width.pg_name().into(),
        // Likewise `real`/`double precision`.
        ValueType::Float(width) => width.pg_name().into(),
        ValueType::Text => "text".into(),
        ValueType::Boolean => "boolean".into(),
        ValueType::Uuid => "uuid".into(),
        // The generator itself never emits an `Other`-typed
        // column (see `generate::Column`'s doc comment), but this mirrors
        // `defs::ddl::pg_type_name` for exhaustiveness/parity.
        ValueType::Other(pg_type) => pg_type.sql_type_name(),
    }
}

/// The declared scale of every generated [`ValueType::Numeric`] source column.
pub(super) const NUMERIC_COLUMN_TYPE: &str = "numeric(14,2)";

/// The type a generated source column is declared with: [`pg_type_name`],
/// except that a [`ValueType::Numeric`] column is declared
/// [`NUMERIC_COLUMN_TYPE`]. The group basis admits `SUM`/`AVG` only over a
/// `numeric` with a declared scale, and every aggregate this
/// generator draws reads such a column. The drawn values are small integers
/// (`generate::VALUE_MAX`, `GRAIN_MAX`), which `numeric(14,2)` stores
/// exactly; casts and typed literals keep the bare `numeric` of
/// [`pg_type_name`], which the column's assignment coerces.
pub(super) fn column_ddl_type(value_type: ValueType) -> std::borrow::Cow<'static, str> {
    match value_type {
        ValueType::Numeric => NUMERIC_COLUMN_TYPE.into(),
        other => pg_type_name(other),
    }
}

/// Renders `def` back to the concrete `TRANSFORM ... FROM ... SELECT ...`
/// syntax `create_definition`/`install_definition` parses — see
/// `ManualBackend`'s former doc comment on this function (preserved in git
/// history) for the full rationale; unchanged here.
pub(super) fn render_definition(def: &TransformDef) -> String {
    let fields: Vec<String> = def
        .fields
        .iter()
        .map(|field| format!("{} AS {}", render_expr(&field.expr), field.name))
        .collect();
    let key_space_clause = match &def.key_space {
        // The JOIN tier: `FROM a [LEFT|INNER] JOIN b ON … GROUP BY …`.
        KeySpace::Join(join) => {
            let qualified = |c: &QualifiedColumn| format!("{}.{}", c.table, c.column);
            format!(
                " {} {}{} ON {} GROUP BY {}",
                match join.joins[0].kind {
                    JoinKind::Inner => "INNER JOIN",
                    JoinKind::Left => "LEFT JOIN",
                },
                join.joins[0].right,
                render_key(join.joins[0].right_key.as_deref()),
                join.joins[0]
                    .on
                    .iter()
                    .map(|on| format!("{} = {}", qualified(&on.lhs), qualified(&on.rhs)))
                    .collect::<Vec<_>>()
                    .join(" AND "),
                join.group_by
                    .iter()
                    .map(qualified)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        // The 1-1 JOIN tier: the same clause without `GROUP BY`, once per
        // position of a star, in the definition's order.
        KeySpace::JoinOneToOne(join) => {
            let qualified = |c: &QualifiedColumn| format!("{}.{}", c.table, c.column);
            join.joins
                .iter()
                .map(|joined| {
                    format!(
                        " {} {}{} ON {}",
                        match joined.kind {
                            JoinKind::Inner => "INNER JOIN",
                            JoinKind::Left => "LEFT JOIN",
                        },
                        joined.right,
                        render_key(joined.right_key.as_deref()),
                        joined
                            .on
                            .iter()
                            .map(|on| format!("{} = {}", qualified(&on.lhs), qualified(&on.rhs)))
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    )
                })
                .collect::<String>()
        }
        KeySpace::OneToOne => String::new(),
        KeySpace::Aggregate { group_by } => format!(
            " GROUP BY {}",
            group_by
                .iter()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let opt_out = if def.without_source_index {
        " WITHOUT SOURCE INDEX"
    } else {
        ""
    };
    // Parenthesized, so a drawn `(TRUE)` stays a filter rather than reading
    // back as an omitted `WHERE` (#1991 rule 8).
    let filter = match &def.predicate {
        Predicate::True => String::new(),
        Predicate::Filter(expr) => format!(" WHERE ({})", render_expr(expr)),
    };
    let key = render_key(def.source_key.as_deref());
    format!(
        "TRANSFORM {} FROM {}{key}{key_space_clause}{opt_out} SELECT {}{filter}",
        def.target,
        def.source,
        fields.join(", ")
    )
}

/// ` KEY (<columns>)` for a table read through an explicit key (ADR-0026),
/// or nothing.
fn render_key(columns: Option<&[String]>) -> String {
    columns.map_or_else(String::new, |columns| {
        format!(" KEY ({})", columns.join(", "))
    })
}

/// The source indexes `def` needs, as `(table, leading columns)`: an
/// aggregate's `GROUP BY` columns on its source, then for each `MIN`/`MAX`
/// argument (a plain column) the `GROUP BY` columns and the argument (#1117
/// rule 4: the extremes' probe reads one endpoint of it per group), and a
/// `JOIN` side's `ON` columns, in `ON` order, on each side. Asked of every
/// definition, whether or not the engine requires it: the index serves a
/// `self_check` walk either way.
pub(super) fn required_source_indexes(def: &TransformDef) -> Vec<(String, Vec<String>)> {
    match &def.key_space {
        KeySpace::OneToOne | KeySpace::JoinOneToOne(_) => Vec::new(),
        KeySpace::Aggregate { group_by } => {
            let mut indexes = vec![(def.source.clone(), group_by.to_vec())];
            for field in &def.fields {
                if let Expr::FunctionCall { name, args } = &field.expr
                    && (name == "MIN" || name == "MAX")
                    && let [Expr::Column(arg) | Expr::SourceColumn { column: arg, .. }] =
                        args.as_slice()
                {
                    let mut columns = group_by.to_vec();
                    columns.push(arg.clone());
                    if !indexes.iter().any(|(_, c)| *c == columns) {
                        indexes.push((def.source.clone(), columns));
                    }
                }
            }
            indexes
        }
        KeySpace::Join(join) => [def.source.as_str(), join.joins[0].right.as_str()]
            .into_iter()
            .map(|table| {
                let columns = join.joins[0]
                    .on
                    .iter()
                    .filter_map(|term| {
                        [&term.lhs, &term.rhs]
                            .into_iter()
                            .find(|c| c.table == table)
                            .map(|c| c.column.clone())
                    })
                    .collect();
                (table.to_string(), columns)
            })
            .collect(),
    }
}

/// Creates the source indexes `def` needs ([`required_source_indexes`]) on
/// tables in `schema` (`None`: the session's `search_path`), if they don't
/// exist: the engine refuses a `GROUP BY` or `JOIN` definition without them.
pub(super) async fn create_source_indexes(
    raw: &tokio_postgres::Client,
    schema: Option<&str>,
    def: &TransformDef,
) -> Result<(), tokio_postgres::Error> {
    create_source_indexes_where(raw, schema, def, |_| true).await
}

/// [`create_source_indexes`], only on the tables `on` accepts: a table's
/// owner creates its index, so the application indexes its sources and
/// Trellis the targets a chained definition reads.
pub(super) async fn create_source_indexes_where(
    raw: &tokio_postgres::Client,
    schema: Option<&str>,
    def: &TransformDef,
    on: impl Fn(&str) -> bool,
) -> Result<(), tokio_postgres::Error> {
    for (table, columns) in required_source_indexes(def) {
        if !on(&table) {
            continue;
        }
        let qualified = match schema {
            Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table)),
            None => quote_ident(&table),
        };
        let mut name = format!("gen_{table}_{}_idx", columns.join("_"));
        name.truncate(60);
        let cols: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        raw.batch_execute(&format!(
            "create index if not exists {} on {qualified} ({})",
            quote_ident(&name),
            cols.join(", ")
        ))
        .await?;
    }
    Ok(())
}

/// Renders a generator-built [`Expr`] back to source text. Every operand of a
/// `BinaryOp` is unconditionally parenthesized so a rendered expression always
/// re-parses to the exact same tree regardless of operator precedence — see
/// `tests::render_expr_parenthesizes_nested_binary_ops_so_they_round_trip` below.
pub(super) fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => match schema {
            Some(schema) => format!("{schema}.{table}.{column}"),
            None => format!("{table}.{column}"),
        },
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        Expr::BooleanLiteral(value) => value.to_string().to_uppercase(),
        Expr::Null => "NULL".to_string(),
        // A typed literal, rendered in the `<type> '<text>'`
        // spelling rather than `CAST(... AS ...)`. Both re-parse to this
        // same node, so either would round-trip; the typed-literal form is
        // the shorter one and keeps the generated definition text closer to
        // how the docs spell it.
        Expr::TypedLiteral { value_type, text } => format!(
            "{} '{}'",
            pg_type_name(*value_type).to_ascii_uppercase(),
            text.replace('\'', "''")
        ),
        Expr::BinaryOp { op, lhs, rhs } => {
            format!(
                "({}) {} ({})",
                render_expr(lhs),
                render_operator(*op),
                render_expr(rhs)
            )
        }
        Expr::Negate(operand) => format!("-({})", render_expr(operand)),
        Expr::Cast { expr, target } => {
            format!("CAST({} AS {})", render_expr(expr), pg_type_name(*target))
        }
        Expr::Not(operand) => format!("NOT ({})", render_expr(operand)),
        Expr::IsNull { expr, negated } => format!(
            "({}) IS {}NULL",
            render_expr(expr),
            if *negated { "NOT " } else { "" }
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "({}) {}IN ({})",
            render_expr(expr),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(|e| format!("({})", render_expr(e)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "({}) {}BETWEEN ({}) AND ({})",
            render_expr(expr),
            if *negated { "NOT " } else { "" },
            render_expr(low),
            render_expr(high)
        ),
        Expr::Case {
            branches,
            otherwise,
        } => {
            let mut text = String::from("CASE");
            for (condition, result) in branches {
                text.push_str(&format!(
                    " WHEN ({}) THEN ({})",
                    render_expr(condition),
                    render_expr(result)
                ));
            }
            if let Some(otherwise) = otherwise {
                text.push_str(&format!(" ELSE ({})", render_expr(otherwise)));
            }
            text.push_str(" END");
            text
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "COUNT(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            if let Some((field, operand)) = trellis::dev::defs::ast::as_extract(name, args) {
                return format!("EXTRACT({field} FROM {})", render_expr(operand));
            }
            let args: Vec<String> = args.iter().map(render_expr).collect();
            format!("{name}({})", args.join(", "))
        }
    }
}

pub(super) fn render_operator(op: Operator) -> &'static str {
    trellis::dev::defs::registry::operator_spec(op).symbol
}

/// `source_columns` for `table`, as `trellis::dev::defs::install_definition` wants
/// it.
pub(super) fn source_columns(table: &Table) -> HashMap<String, ValueType> {
    table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type))
        .collect()
}

/// One row's placeholder assignment for an `INSERT`/`UPDATE` statement:
/// `column = $n::type` (or `column` for the column list), plus the bound
/// text value at that position.
pub(super) struct Assignment {
    pub(super) fragment: String,
    pub(super) value: Option<String>,
}

/// The Postgres type `column` on `table` was actually declared with in
/// [`create_source_table`] — the single source of truth every
/// `$n::text::<type>` cast renders from.
///
/// This is just [`pg_type_name`] of the column's own [`ValueType`],
/// including for the primary key: `ValueType` can name `bigint` (see
/// `crate::model::PRIMARY_KEY_VALUE_TYPE`).
pub(super) fn column_pg_type(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
) -> std::borrow::Cow<'static, str> {
    pg_type_name(
        tables
            .get(table)
            .and_then(|t| t.columns.iter().find(|c| c.name == column))
            .map(|c| c.value_type)
            .unwrap_or(ValueType::Numeric),
    )
}

pub(super) fn assignment(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
    index: usize,
    value: &Option<String>,
) -> Assignment {
    Assignment {
        fragment: format!(
            "{}=${}::text::{}",
            quote_ident(column),
            index,
            column_pg_type(tables, table, column)
        ),
        value: value.clone(),
    }
}

/// Creates `table` as a source table: one column per [`Column`], the primary
/// key declared from its own [`crate::model::PRIMARY_KEY_VALUE_TYPE`]
/// (`bigint`), a single-column `UNIQUE` constraint
/// on every column named in `table.unique_cols`, and a composite one on
/// every set in `table.unique_sets`.
///
/// `schema` qualifies the table's name (`None` leaves it to the session's
/// `search_path`: `public` for the application's own session, the instance
/// schema for the backend's raw one): a source table that more than
/// one Trellis instance reads is created in `public`, outside every
/// instance's catalog schema.
pub(super) async fn create_source_table(
    raw: &tokio_postgres::Client,
    schema: Option<&str>,
    table: &Table,
) -> Result<(), tokio_postgres::Error> {
    let name = match schema {
        Some(schema) => format!("{}.{}", quote_ident(schema), quote_ident(&table.name)),
        None => quote_ident(&table.name),
    };
    if let Some(partitioning) = &table.partitioning {
        raw.batch_execute(&partitioned_source_ddl(&name, table, partitioning))
            .await?;
        return Ok(());
    }
    let mut sql = format!("create table {name} (");
    for (i, column) in table.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote_ident(&column.name));
        sql.push(' ');
        sql.push_str(&column_ddl_type(column.value_type));
        if column.name == table.pk_col {
            // A table read through a `KEY (...)` without a primary key
            // still names each row by `pk_col`.
            sql.push_str(match &table.source_key {
                Some(key) if !key.primary_key => " not null unique",
                _ => " primary key",
            });
        } else if table.unique_cols.iter().any(|c| c == &column.name) {
            sql.push_str(" unique");
        } else if let Some(key) = &table.source_key
            && key.columns.contains(&column.name)
            && !key.is_nullable(&column.name)
        {
            sql.push_str(" not null");
        }
    }
    for set in &table.unique_sets {
        sql.push_str(", unique (");
        sql.push_str(
            &set.iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push(')');
    }
    sql.push(')');
    if let Some(key) = &table.source_key {
        sql.push_str(&format!(
            "; create unique index on {name} ({}){}",
            key.columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", "),
            if key.nullable.is_empty() {
                ""
            } else {
                " nulls not distinct"
            }
        ));
    }
    raw.batch_execute(&sql).await?;
    Ok(())
}

/// The DDL of a partitioned source table (ADR-0017): the root, keyed by
/// `(pk_col, partition column)` with the partition column `NOT NULL`, and
/// its leaves, each named after the root with a `_p<n>` suffix (`_pd` for a
/// `DEFAULT` leaf).
fn partitioned_source_ddl(name: &str, table: &Table, partitioning: &Partitioning) -> String {
    assert!(
        table.unique_cols.is_empty() && table.unique_sets.is_empty(),
        "a partitioned table can't carry a UNIQUE constraint — a generator bug"
    );
    let key = quote_ident(&partitioning.column);
    let mut columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| {
            let mut sql = format!(
                "{} {}",
                quote_ident(&column.name),
                column_ddl_type(column.value_type)
            );
            if column.name == table.pk_col || column.name == partitioning.column {
                sql.push_str(" not null");
            }
            sql
        })
        .collect();
    columns.push(format!(
        "primary key ({}, {key})",
        quote_ident(&table.pk_col)
    ));
    let (method, leaves): (&str, Vec<String>) = match partitioning.strategy {
        PartitionStrategy::Hash { modulus } => (
            "hash",
            (0..modulus)
                .map(|r| format!("for values with (modulus {modulus}, remainder {r})"))
                .collect(),
        ),
        PartitionStrategy::List { values, default } => (
            "list",
            (0..values)
                .map(|v| format!("for values in ({v})"))
                .chain(default.then(|| "default".to_string()))
                .collect(),
        ),
        PartitionStrategy::Range { bounds } => (
            "range",
            (0..bounds)
                .map(|b| {
                    let from = if b == 0 {
                        "minvalue".to_string()
                    } else {
                        b.to_string()
                    };
                    let to = if b + 1 == bounds {
                        "maxvalue".to_string()
                    } else {
                        (b + 1).to_string()
                    };
                    format!("for values from ({from}) to ({to})")
                })
                .collect(),
        ),
    };
    let mut sql = format!(
        "create table {name} ({}) partition by {method} ({key});",
        columns.join(", ")
    );
    for (i, bound) in leaves.iter().enumerate() {
        let suffix = if bound == "default" {
            "pd".to_string()
        } else {
            format!("p{i}")
        };
        // The leaf sits beside the root: `name` is the root's (maybe
        // schema-qualified, quoted) name, so the leaf's is built from the
        // table's own bare name and the same qualifier.
        let leaf = match name.rsplit_once('.') {
            Some((schema, _)) => format!(
                "{schema}.{}",
                quote_ident(&format!("{}_{suffix}", table.name))
            ),
            None => quote_ident(&format!("{}_{suffix}", table.name)),
        };
        sql.push_str(&format!(
            " create table {leaf} partition of {name} {bound};"
        ));
    }
    sql
}

/// [`apply_op`]'s failure modes: either an unknown table — `apply_op` was
/// given an op naming a table `install` never created, same "generator bug,
/// not a condition to paper over" contract each backend's own
/// `UnknownTable` error variant documented before this was shared — or a
/// real database error. Each backend's own error enum wraps both via
/// `From`.
#[derive(Debug)]
pub(super) enum ApplyOpError {
    UnknownTable(String),
    Db(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for ApplyOpError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApplyOpError::Db(err)
    }
}

/// Applies one [`Op`] as raw source DML against `raw`, given the currently
/// tracked `tables` (for column/PK type resolution) — the shared body behind
/// both `ManualBackend::apply` and `SubprocessBackend::apply`. Returns the
/// statement's affected-row count, exactly like [`super::Backend::apply`]'s
/// own contract.
pub(super) async fn apply_op(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
) -> Result<u64, ApplyOpError> {
    match op {
        Op::Leaf { leaf, op } => {
            assert!(
                !matches!(**op, Op::Leaf { .. } | Op::Partition { .. }),
                "apply_op: an Op::Leaf wraps a write — a generator bug"
            );
            apply_write(raw, tables, op, Some(leaf)).await
        }
        Op::Partition { table, action, .. } => {
            if !tables.contains_key(table) {
                return Err(ApplyOpError::UnknownTable(table.clone()));
            }
            raw.batch_execute(&partition_action_sql(tables, table, action))
                .await?;
            Ok(1)
        }
        Op::Upsert { .. } | Op::Merge { .. } | Op::Cte { .. } => {
            apply_statement_form(raw, tables, op).await
        }
        _ => apply_write(raw, tables, op, None).await,
    }
}

/// The SQL of one [`PartitionAction`] on the root `table`, sent as one
/// simple-query string. A `Create`'s statements run as one implicit
/// transaction, so a refused bound leaves nothing behind (an explicit
/// `BEGIN` would leave the session in an aborted transaction instead).
fn partition_action_sql(
    tables: &HashMap<String, Table>,
    table: &str,
    action: &PartitionAction,
) -> String {
    let root = quote_ident(table);
    match action {
        PartitionAction::Detach { leaf, concurrently } => format!(
            "alter table {root} detach partition {}{}",
            quote_ident(leaf),
            if *concurrently { " concurrently" } else { "" }
        ),
        PartitionAction::Attach { leaf, bound } => {
            format!(
                "alter table {root} attach partition {} {bound}",
                quote_ident(leaf)
            )
        }
        PartitionAction::Drop { leaf } => format!("drop table {}", quote_ident(leaf)),
        PartitionAction::Create {
            leaf,
            bound,
            rows,
            standalone,
        } => {
            let quoted = quote_ident(leaf);
            let mut sql = if *standalone {
                format!("create table {quoted} (like {root}); ")
            } else {
                format!("create table {quoted} partition of {root} {bound}; ")
            };
            push_inserts(&mut sql, tables, table, &quoted, rows);
            if *standalone {
                sql.push_str(&format!(
                    "alter table {root} attach partition {quoted} {bound}; "
                ));
            }
            sql
        }
        PartitionAction::CreateMiddle {
            middle,
            bound,
            leaves,
            rows,
            standalone,
        } => {
            let quoted = quote_ident(middle);
            let key = quote_ident(
                &tables[table]
                    .partitioning
                    .as_ref()
                    .expect("a middle's root is partitioned — a generator bug")
                    .column,
            );
            let mut sql = if *standalone {
                format!("create table {quoted} (like {root}) partition by list ({key}); ")
            } else {
                format!(
                    "create table {quoted} partition of {root} {bound} partition by list ({key}); "
                )
            };
            for (leaf, leaf_bound) in leaves {
                sql.push_str(&format!(
                    "create table {} partition of {quoted} {leaf_bound}; ",
                    quote_ident(leaf)
                ));
            }
            push_inserts(&mut sql, tables, table, &quoted, rows);
            if *standalone {
                sql.push_str(&format!(
                    "alter table {root} attach partition {quoted} {bound}; "
                ));
            }
            sql
        }
    }
}

/// Appends an `INSERT` into the relation `quoted` of each of `rows`, each a
/// full row of the root `table`'s columns.
fn push_inserts(
    sql: &mut String,
    tables: &HashMap<String, Table>,
    table: &str,
    quoted: &str,
    rows: &[Vec<(String, Option<String>)>],
) {
    for row in rows {
        let columns: Vec<String> = row.iter().map(|(c, _)| quote_ident(c)).collect();
        let values: Vec<String> = row
            .iter()
            .map(|(c, v)| match v {
                Some(v) => format!("{}::{}", quote_literal(v), column_pg_type(tables, table, c)),
                None => "null".to_string(),
            })
            .collect();
        sql.push_str(&format!(
            "insert into {quoted} ({}) values ({}); ",
            columns.join(", "),
            values.join(", ")
        ));
    }
}

/// `value` as a single-quoted SQL string literal.
pub(super) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// [`apply_op`] for a single write: issued against `relation` when given (a
/// leaf of the op's partitioned table), else against the op's own table,
/// whose [`Table`] types every value either way.
async fn apply_write(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
    relation: Option<&str>,
) -> Result<u64, ApplyOpError> {
    let relation = |table: &str| quote_ident(relation.unwrap_or(table));
    let affected = match op {
        Op::Insert { table, row, .. } => {
            let columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
            let assignments: Vec<Assignment> = row
                .iter()
                .enumerate()
                .map(|(i, (col, val))| Assignment {
                    fragment: format!("${}::text::{}", i + 1, column_pg_type(tables, table, col)),
                    value: val.clone(),
                })
                .collect();
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = assignments
                .iter()
                .map(|a| a.fragment.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "insert into {} ({column_list}) values ({placeholders})",
                relation(table)
            );
            let params: Vec<Option<String>> = assignments.into_iter().map(|a| a.value).collect();
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Update {
            table, pk, changes, ..
        } => {
            let pk_col = tables
                .get(table)
                .map(|t| t.pk_col.clone())
                .ok_or_else(|| ApplyOpError::UnknownTable(table.clone()))?;
            let mut assignments = Vec::with_capacity(changes.len());
            for (i, (col, val)) in changes.iter().enumerate() {
                assignments.push(assignment(tables, table, col, i + 1, val));
            }
            let set_clause = assignments
                .iter()
                .map(|a| a.fragment.clone())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "update {} set {set_clause} where {}=${}::text::{}",
                relation(table),
                quote_ident(&pk_col),
                changes.len() + 1,
                column_pg_type(tables, table, &pk_col),
            );
            let mut params: Vec<Option<String>> =
                assignments.into_iter().map(|a| a.value).collect();
            params.push(Some(pk.clone()));
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Delete { table, pk, .. } => {
            let pk_col = tables
                .get(table)
                .map(|t| t.pk_col.clone())
                .ok_or_else(|| ApplyOpError::UnknownTable(table.clone()))?;
            let sql = format!(
                "delete from {} where {}=$1::text::{}",
                relation(table),
                quote_ident(&pk_col),
                column_pg_type(tables, table, &pk_col),
            );
            raw.execute(&sql, &[pk]).await?
        }
        Op::Truncate { table, .. } => {
            // The load-bearing gotcha: Postgres's
            // `TRUNCATE` command tag always reports `0` rows affected,
            // regardless of how many rows actually existed — trusting that
            // raw count into `run_convergence`'s `Ok(0) => AffectsNoRows`
            // classifier would misclassify every non-empty truncate as a
            // no-op. So the real row count is synthesized instead: a
            // `SELECT count(*)` in the *same transaction* as the `TRUNCATE`,
            // taken before it runs, so nothing can slip a concurrent write in
            // between the count and the clear.
            let quoted = relation(table);
            let txn = raw.transaction().await?;
            let count_row = txn
                .query_one(&format!("select count(*) from {quoted}"), &[])
                .await?;
            let count: i64 = count_row.get(0);
            txn.batch_execute(&format!("truncate table {quoted}"))
                .await?;
            txn.commit().await?;
            count as u64
        }
        Op::BulkInsert { table, rows, .. } => {
            let Some(first_row) = rows.first() else {
                panic!(
                    "apply_op: Op::BulkInsert against table {table:?} carries no rows — a \
                     generator bug"
                );
            };
            let columns: Vec<&str> = first_row.iter().map(|(c, _)| c.as_str()).collect();
            let column_list = columns
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");

            let mut placeholder_groups = Vec::with_capacity(rows.len());
            let mut params: Vec<Option<String>> = Vec::with_capacity(rows.len() * columns.len());
            for row in rows {
                let row_columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
                assert_eq!(
                    row_columns, columns,
                    "apply_op: every Op::BulkInsert row must carry the same columns in the same \
                     order as the first row — a generator bug (table {table:?})"
                );
                let placeholders: Vec<String> = row
                    .iter()
                    .map(|(col, val)| {
                        params.push(val.clone());
                        format!(
                            "${}::text::{}",
                            params.len(),
                            column_pg_type(tables, table, col)
                        )
                    })
                    .collect();
                placeholder_groups.push(format!("({})", placeholders.join(", ")));
            }

            let sql = format!(
                "insert into {} ({column_list}) values {}",
                relation(table),
                placeholder_groups.join(", ")
            );
            let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                .iter()
                .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
            raw.execute(&sql, &params).await?
        }
        Op::Leaf { .. }
        | Op::Partition { .. }
        | Op::Upsert { .. }
        | Op::Merge { .. }
        | Op::Cte { .. } => {
            unreachable!("apply_op handles these before apply_write")
        }
    };
    Ok(affected)
}

/// The qualified source table the catalog persisted for the definition
/// whose target is `target` (bare), read on `raw`, whose `search_path` lists
/// the instance schema first. A bare `def.source` is no good to the engine's
/// helpers: no Trellis session has the target schema or `public` on its
/// `search_path` to resolve it.
pub(super) async fn persisted_source(
    raw: &tokio_postgres::Client,
    target: &str,
) -> Result<String, tokio_postgres::Error> {
    raw.query_one(
        "select source_table from transform_definitions \
         where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .map(|row| row.get(0))
}

/// Reads `qualified_table` back as text, ordered by `pk_col`, into `pk ->
/// column -> value`.
pub(super) async fn read_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    pk_col: &str,
    columns: &[Column],
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>,
    tokio_postgres::Error,
> {
    let select_list = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select {select_list} from {qualified_table} order by {}",
        quote_ident(pk_col)
    );
    let rows = client.query(&sql, &[]).await?;

    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let mut by_column = std::collections::BTreeMap::new();
        let pk_value: Option<String> = row.get(0);
        let pk_value = pk_value.expect("primary key column is never NULL");
        for (i, column) in columns.iter().enumerate() {
            by_column.insert(column.name.clone(), row.get::<_, Option<String>>(i));
        }
        result.insert(pk_value, by_column);
    }
    Ok(result)
}

/// Reads a 1-1 target over a `KEY (...)` source (ADR-0026) back as text,
/// each row keyed by its `key` columns' [`crate::model::source_key_text`],
/// with `fields` as its columns.
pub(super) async fn read_keyed_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    key: &[String],
    fields: &[FieldDef],
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>,
    tokio_postgres::Error,
> {
    let select_list: Vec<String> = key
        .iter()
        .map(|c| c.as_str())
        .chain(fields.iter().map(|f| f.name.as_str()))
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    let sql = format!("select {} from {qualified_table}", select_list.join(", "));
    let mut result = std::collections::BTreeMap::new();
    for row in client.query(&sql, &[]).await? {
        let parts: Vec<Option<String>> = (0..key.len()).map(|i| row.get(i)).collect();
        let by_column = fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.clone(), row.get(key.len() + i)))
            .collect();
        result.insert(crate::model::source_key_text(&parts), by_column);
    }
    Ok(result)
}

/// Reads an `Aggregate` key-space target table back as text, keyed by the
/// same composite [`crate::model::group_key`] convention the SQL
/// oracles use — see `ManualBackend`'s former doc comment (git history) for
/// the full rationale; unchanged here.
pub(super) async fn read_aggregate_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    group_by: &[String],
    fields: &[FieldDef],
) -> Result<
    std::collections::BTreeMap<String, std::collections::BTreeMap<String, Option<String>>>,
    tokio_postgres::Error,
> {
    let value_fields: Vec<&str> = fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|name| !group_by.iter().any(|g| g == name))
        .collect();

    let mut select_list: Vec<String> = group_by
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect();
    select_list.extend(
        value_fields
            .iter()
            .map(|c| format!("{}::text", quote_ident(c))),
    );
    let sql = format!("select {} from {qualified_table}", select_list.join(", "));
    let rows = client.query(&sql, &[]).await?;

    let mut result = std::collections::BTreeMap::new();
    for row in rows {
        let group_values: Vec<Option<String>> = (0..group_by.len())
            .map(|i| row.get::<_, Option<String>>(i))
            .collect();
        let key = crate::model::group_key(&group_values);
        let mut by_column = std::collections::BTreeMap::new();
        for (i, name) in value_fields.iter().enumerate() {
            by_column.insert(
                (*name).to_string(),
                row.get::<_, Option<String>>(group_by.len() + i),
            );
        }
        result.insert(key, by_column);
    }
    Ok(result)
}

/// Why [`quiesce`] gave up.
#[derive(Debug)]
pub(super) enum QuiesceError {
    /// A status or partition query itself failed — a dropped connection, a
    /// catalog table that isn't there. Propagated, deliberately not panicked
    /// on: the proptest-driven harness above `Backend::quiesce`
    /// (`run::check_program`'s callers) depends on a structured, shrinkable
    /// failure rather than an unwind from inside a wait loop.
    Db(tokio_postgres::Error),
    /// The ring's own wait failed: taking the watermark token, or
    /// [`await_ring`] (a query failure or its named
    /// `ConvergenceTimeout`).
    Staging(StagingError),
    /// `unsettled` names every target table (`def.target`) whose
    /// `transform_definitions.status` hadn't reached a terminal backfill
    /// outcome when the budget ran out after `waited`.
    DefinitionsUnsettled {
        unsettled: Vec<String>,
        waited: Duration,
    },
    /// `unsettled` names every partitioned source (ADR-0017) whose tree the
    /// reconcile pass hadn't recorded yet, or that still had a correction
    /// queued, when the budget ran out after `waited`.
    PartitionsUnsettled {
        unsettled: Vec<String>,
        waited: Duration,
    },
}

impl From<tokio_postgres::Error> for QuiesceError {
    fn from(err: tokio_postgres::Error) -> Self {
        QuiesceError::Db(err)
    }
}

impl From<StagingError> for QuiesceError {
    fn from(err: StagingError) -> Self {
        QuiesceError::Staging(err)
    }
}

/// The target tables of every `defs` entry whose current
/// `transform_definitions.status` is neither `live` nor `quarantined` — see
/// `ManualBackend::unsettled_definitions`'s former doc comment (git history)
/// for the full rationale behind this query shape.
async fn unsettled_definitions(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
) -> Result<Vec<String>, tokio_postgres::Error> {
    let mut unsettled = Vec::new();
    for def in defs {
        let Some(row) = raw
            .query_opt(
                "select d.status, exists (select 1 from resume_requests r \
                                          where r.transform_id = d.id) \
                 from transform_definitions d \
                 where split_part(d.target_table, '.', 2) = $1",
                &[&def.target],
            )
            .await?
        else {
            unsettled.push(def.target.clone());
            continue;
        };
        let status_text: String = row.get(0);
        let status = StoredStatus::from_persisted(&status_text).expect("a stored status");
        // A `RESUME` that waits for an upstream to be `live` leaves its
        // definition paused with a resume request, which the capture pass
        // completes: still on its way back, so not settled.
        let resuming: bool = row.get(1);
        if resuming {
            unsettled.push(def.target.clone());
            continue;
        }
        // `Paused` joins the terminal set for the same reason `Quarantined`
        // is in it: a frozen definition never progresses on its
        // own, so waiting for it to settle any further is waiting forever.
        // A run that leaves one paused fails its own check instead.
        if !matches!(
            status,
            StoredStatus::Live | StoredStatus::Quarantined | StoredStatus::Paused
        ) {
            unsettled.push(def.target.clone());
        }
    }
    Ok(unsettled)
}

/// Each of `defs`' catalog status and, when it has one, why it stopped: what
/// `capture_failures` records for it (its kind and error, and each reason
/// beyond that error) and its poisoned keys' latest error. Read in `defs`'
/// order; a definition with no catalog row reads as `<missing>`.
pub(super) async fn definition_states(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    reported: &HashMap<String, String>,
) -> Result<Vec<DefinitionState>, tokio_postgres::Error> {
    let targets: Vec<String> = defs.iter().map(|d| d.target.clone()).collect();
    let rows = raw
        .query(
            "select split_part(d.target_table, '.', 2), d.status, \
                    nullif(concat_ws('; ', \
                        (select f.kind || ': ' || f.error \
                           || coalesce(' (caused by definition ' || f.caused_by || ')', '') \
                           from capture_failures f where f.transform_id = d.id), \
                        (select string_agg(r.reason, '; ' order by r.position) \
                           from capture_failure_reasons r \
                           join capture_failures f on f.transform_id = r.transform_id \
                          where r.transform_id = d.id and r.reason <> f.error), \
                        (select 'poisoned ' || count(*) || ' key(s), last error: ' \
                                || max(p.last_error) \
                           from poison p where p.transform_id = d.id \
                          having count(*) > 0)), '') \
               from transform_definitions d \
              where split_part(d.target_table, '.', 2) = any($1)",
            &[&targets],
        )
        .await?;
    let mut found: HashMap<String, (String, Option<String>)> = rows
        .iter()
        .map(|row| (row.get(0), (row.get(1), row.get(2))))
        .collect();
    Ok(targets
        .into_iter()
        .map(|target| {
            let (status, reason) = found
                .remove(&target)
                .unwrap_or_else(|| ("<missing>".to_string(), None));
            let reported = reported
                .get(&target)
                .cloned()
                .unwrap_or_else(|| "<missing>".to_string());
            DefinitionState {
                target,
                status,
                reported,
                reason,
            }
        })
        .collect())
}

/// Every definition's status as an operator reads it, by bare target name:
/// `Trellis::definitions` through a facade connected for the call, so a
/// stored `live` reads `catching_up` exactly when the engine says so.
pub(super) async fn reported_statuses(
    config: trellis::Config,
) -> Result<HashMap<String, String>, trellis::TrellisError> {
    let facade = trellis::Trellis::connect(config, trellis::TrellisOptions::default()).await?;
    let listed = facade.definitions().await;
    facade.shutdown().await?;
    Ok(listed?
        .into_iter()
        .map(|d| {
            let bare = d
                .target_table
                .rsplit('.')
                .next()
                .unwrap_or(&d.target_table)
                .to_string();
            (bare, d.status.as_str().to_string())
        })
        .collect())
}

/// Every partitioned source (ADR-0017) whose partition maintenance a target
/// may not reflect yet: its tree as the catalog has it now (each node's oid
/// and `pg_inherits.xmin`, which an attach, detach, drop or new leaf
/// changes) isn't the tree the reconcile pass last recorded, or a correction
/// is still queued for it. The pass records the tree and queues the
/// corrections in one transaction, and a correction leaves the queue once
/// every definition reading the source has applied it, so a source that
/// passes this has no maintenance left to correct. A definition reads
/// `live` throughout the first part (until the pass sees the DDL) and the
/// queued part of the second (until the pass schedules the correction), so
/// definition status alone can't show either.
///
/// It reads the engine's recorded tree as `capture::partitions` writes it
/// (`nodes.nodes[]`, each `oid` and `inherits_xmin` as `xmin::text`). A
/// change to that format leaves every source unsettled, so a quiesce times
/// out naming it (`partition_maintenance_converges_end_to_end` fails); it
/// can't make one look settled early.
async fn unsettled_partitions(
    raw: &tokio_postgres::Client,
) -> Result<Vec<String>, tokio_postgres::Error> {
    let rows = raw
        .query(
            "select s.table_name from partition_sources s \
             where (select array_agg((n->>'oid') || ':' || coalesce(n->>'inherits_xmin', '') \
                                     order by (n->>'oid')::bigint) \
                    from jsonb_array_elements(s.nodes->'nodes') n) \
                   is distinct from \
                   (select array_agg(t.relid::oid::bigint::text || ':' \
                                     || coalesce(i.xmin::text, '') \
                                     order by t.relid::oid::bigint) \
                    from pg_catalog.pg_partition_tree(s.root_oid::oid) t \
                    left join pg_catalog.pg_inherits i on i.inhrelid = t.relid) \
             union \
             select table_name from partition_corrections \
             order by 1",
            &[],
        )
        .await?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

/// Polls `probe` every [`SETTLE_POLL`] until it returns an empty list or
/// `started + timeout` passes, returning the last non-empty list and the time
/// waited on timeout.
async fn poll_until_empty<F, Fut>(
    started: Instant,
    timeout: Duration,
    mut probe: F,
) -> Result<Result<(), (Vec<String>, Duration)>, tokio_postgres::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<String>, tokio_postgres::Error>>,
{
    loop {
        let found = probe().await?;
        if found.is_empty() {
            return Ok(Ok(()));
        }
        let waited = started.elapsed();
        if waited >= timeout {
            return Ok(Err((found, waited)));
        }
        tokio::time::sleep(SETTLE_POLL.min(timeout - waited)).await;
    }
}

/// How often [`quiesce`] re-reads definition status.
/// Each read is one cheap catalog query on the test's own isolated database,
/// so a short fixed interval costs nothing and keeps a settle from
/// overshooting the moment it happens.
const SETTLE_POLL: Duration = Duration::from_millis(20);

/// Waits until the engine has no work left that could still change a
/// target. Shared by both backends' `Backend::quiesce`.
///
/// `budget` bounds the wait two ways ([`ProgressDeadline`]): it fails once
/// the ring has made no progress for `budget.no_progress`, and in any case at
/// `budget.cap`. A slow drain that keeps committing pages is waited out; a
/// stuck one fails a no-progress window after it stopped, with the
/// database's state printed to stderr.
///
/// A two-way check, the same one an operator makes (ADR-0002, "What `live`
/// promises"): every definition reports `live` (or a terminal `paused` /
/// `quarantined`), then the convergence wait on a fresh token returns. Each
/// covers work the other can't see:
///
/// - **Live status.** A definition's build runs its plan job and chunks
///   through `backfill_chunks`, entirely outside the ring (ADR-0002, "A
///   build is Re-derive over chunks"): it sits in `waiting_to_backfill`
///   until the reconcile pass starts it, then `backfilling` until every job
///   is done. A rebuild (`request_backfill`, a capture re-install, the
///   rebuild of an upstream it reads) moves a `live` definition back to
///   `backfilling` the same way. Only `transform_definitions.status` shows
///   any of these. `catching_up` is never stored: a reader of an upstream
///   that is not `live` reports it, and the upstream's own stored status is
///   what this check waits on.
/// - **Caught up to the token.** [`await_converged`](trellis::dev::staging::await_converged) against a token taken
///   after the mutations of interest: the public read-your-writes path, so
///   every quiesce in the suite exercises it.
///
/// - **Partition maintenance corrected** (ADR-0017). An attach, detach,
///   drop or new leaf reaches a target only through the correction the next
///   reconcile pass queues, and a leaf `TRUNCATE` through the one its seal
///   queues; until the pass schedules it, the definition still reads `live`.
///   [`unsettled_partitions`] waits for the pass to record the tree and for
///   the queue to empty, ahead of the status check, and is checked again at
///   the end, since the ring wait is what seals a truncate's marker.
///
/// The order is what makes this sound. Status first, then the ring: a
/// definition reports `live` only once its build's last chunk has
/// committed, and it applies from its build's start, so every change a chunk
/// didn't read is in the ring under an origin at or below a token taken
/// after the flip (a 1-1 build's recomputes of its `NULL` keys carry no
/// origin, so they gate every token). The convergence wait, started only
/// then, covers them.
///
/// After the ring drains, the status check runs once more, and then the ring
/// once more against the same token (a definition can be rebuilt and flipped
/// back `live` in between). The whole sequence repeats if either finds work
/// again rather than returning on stale evidence.
pub(super) async fn quiesce(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    budget: QuiesceBudget,
) -> Result<(), QuiesceError> {
    let mut deadline = ProgressDeadline::new(budget, Instant::now());
    quiesce_within(raw, defs, &mut deadline, HeldKeys::Block).await
}

/// What a quiesce makes of a key a definition holds in quarantine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeldKeys {
    /// It blocks convergence, as it blocks every engine wait: a run that
    /// models no held key never settles past one.
    Block,
    /// It doesn't: the quiesce settles once everything but the held keys'
    /// parked changes has drained. A change still failing, its key not yet
    /// held, keeps the ring pending until the key fuse holds it. For a run
    /// that models held keys (`crate::run::run_convergence_with_held_keys`).
    Settle,
}

/// [`quiesce`] within a caller's `deadline`, which the caller can read
/// afterwards for how long the ring wait took and how often the ring's
/// progress extended it.
///
/// The ring wait ([`await_ring`]) is the only part that extends the
/// deadline: each time the ring's drain state moves, the no-progress window
/// restarts, up to the budget's cap. The status and partition waits get the
/// current window. Any timeout prints the database's state to stderr first.
pub(super) async fn quiesce_within(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    deadline: &mut ProgressDeadline,
    held: HeldKeys,
) -> Result<(), QuiesceError> {
    let result = quiesce_passes(raw, defs, deadline, held).await;
    if let Err(
        QuiesceError::DefinitionsUnsettled { .. } | QuiesceError::PartitionsUnsettled { .. },
    ) = &result
    {
        let expiry = deadline
            .expired(Instant::now())
            .unwrap_or(Expiry::NoProgress);
        dump_state(raw, None, &expiry_reason(expiry, deadline)).await;
    }
    result
}

async fn quiesce_passes(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    deadline: &mut ProgressDeadline,
    held: HeldKeys,
) -> Result<(), QuiesceError> {
    loop {
        poll_until_empty(deadline.window_start(), deadline.window_budget(), || {
            unsettled_partitions(raw)
        })
        .await?
        .map_err(|(unsettled, _)| QuiesceError::PartitionsUnsettled {
            unsettled,
            waited: deadline.started().elapsed(),
        })?;
        poll_until_empty(deadline.window_start(), deadline.window_budget(), || {
            unsettled_definitions(raw, defs)
        })
        .await?
        .map_err(|(unsettled, _)| QuiesceError::DefinitionsUnsettled {
            unsettled,
            waited: deadline.started().elapsed(),
        })?;

        let token = watermark_token(raw).await?;
        await_ring(raw, token, deadline, held).await?;

        let unsettled = unsettled_definitions(raw, defs).await?;
        if unsettled.is_empty() && unsettled_partitions(raw).await?.is_empty() {
            // Rows staged with no origin between the ring wait and the read
            // above (an image-less `recompute`) gate every token, `token`
            // included, so this one check fails only if such rows appeared.
            let converged = match held {
                HeldKeys::Block => converged_through(raw, token).await?,
                HeldKeys::Settle => converged_through_except_held(raw, token).await?,
            };
            if converged {
                return Ok(());
            }
            continue;
        }
        // Every wait above checks at least once even with the budget spent,
        // so without this a state that keeps reappearing would loop forever.
        if deadline.expired(Instant::now()).is_some() {
            return Err(QuiesceError::DefinitionsUnsettled {
                unsettled,
                waited: deadline.started().elapsed(),
            });
        }
    }
}

/// Waits, up to `timeout`, for every one of `defs` to settle (`live`,
/// `quarantined` or `paused`), without waiting on the ring.
pub(super) async fn await_settled(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
    timeout: Duration,
) -> Result<(), QuiesceError> {
    poll_until_empty(Instant::now(), timeout, || unsettled_definitions(raw, defs))
        .await?
        .map_err(|(unsettled, waited)| QuiesceError::DefinitionsUnsettled { unsettled, waited })
}

/// One pass of [`quiesce`]'s two checks, in its order, without waiting on
/// either: every definition settled, then the ring converged
/// through a token taken after that. A caller that has to act between
/// checks (seal by hand, say) polls this and then confirms with [`quiesce`].
pub(super) async fn settled(
    raw: &tokio_postgres::Client,
    defs: &[TransformDef],
) -> Result<bool, QuiesceError> {
    if !unsettled_partitions(raw).await?.is_empty()
        || !unsettled_definitions(raw, defs).await?.is_empty()
    {
        return Ok(false);
    }
    let token = watermark_token(raw).await?;
    Ok(converged_through(raw, token).await?)
}

// ---------------------------------------------------------------------
// Statement forms and transactions (#2181).
// ---------------------------------------------------------------------

/// A statement's SQL text and its bound values, each cast in the text from
/// `text`.
struct Rendered {
    sql: String,
    params: Vec<Option<String>>,
}

impl Rendered {
    /// `$n::text::<type>` for the next bound value, `value`.
    fn bind(&mut self, value: Option<String>, pg_type: &str) -> String {
        self.params.push(value);
        format!("${}::text::{pg_type}", self.params.len())
    }

    fn params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        self.params
            .iter()
            .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect()
    }
}

/// `table`'s primary-key column, or [`ApplyOpError::UnknownTable`].
fn pk_col<'a>(tables: &'a HashMap<String, Table>, table: &str) -> Result<&'a str, ApplyOpError> {
    tables
        .get(table)
        .map(|t| t.pk_col.as_str())
        .ok_or_else(|| ApplyOpError::UnknownTable(table.to_string()))
}

/// A plain [`Op::Insert`], [`Op::Update`] or [`Op::Delete`] as one
/// statement, `RETURNING 1` when `returning`.
fn render_plain(
    tables: &HashMap<String, Table>,
    op: &Op,
    returning: bool,
) -> Result<Rendered, ApplyOpError> {
    let mut r = Rendered {
        sql: String::new(),
        params: Vec::new(),
    };
    match op {
        Op::Insert { table, row, .. } => {
            let columns: Vec<String> = row.iter().map(|(c, _)| quote_ident(c)).collect();
            let values: Vec<String> = row
                .iter()
                .map(|(c, v)| r.bind(v.clone(), &column_pg_type(tables, table, c)))
                .collect();
            r.sql = format!(
                "insert into {} ({}) values ({})",
                quote_ident(table),
                columns.join(", "),
                values.join(", ")
            );
        }
        Op::Update {
            table, pk, changes, ..
        } => {
            let key = pk_col(tables, table)?;
            let set: Vec<String> = changes
                .iter()
                .map(|(c, v)| {
                    let value = r.bind(v.clone(), &column_pg_type(tables, table, c));
                    format!("{}={value}", quote_ident(c))
                })
                .collect();
            let pk_value = r.bind(Some(pk.clone()), &column_pg_type(tables, table, key));
            r.sql = format!(
                "update {} set {} where {}={pk_value}",
                quote_ident(table),
                set.join(", "),
                quote_ident(key)
            );
        }
        Op::Delete { table, pk, .. } => {
            let key = pk_col(tables, table)?;
            let pk_value = r.bind(Some(pk.clone()), &column_pg_type(tables, table, key));
            r.sql = format!(
                "delete from {} where {}={pk_value}",
                quote_ident(table),
                quote_ident(key)
            );
        }
        other => panic!("render_plain: {other:?} is no plain write — a generator bug"),
    }
    if returning {
        r.sql.push_str(" returning 1");
    }
    Ok(r)
}

/// The SQL of an [`Op::Upsert`], [`Op::Merge`] or [`Op::Cte`], and whether
/// it reports its count as a row (`SELECT count(*)`) rather than as its
/// command tag.
fn render_statement_form(
    tables: &HashMap<String, Table>,
    op: &Op,
) -> Result<(Rendered, bool), ApplyOpError> {
    let write = op.write();
    let table = write.table();
    let key = pk_col(tables, table)?;
    let key_type = column_pg_type(tables, table, key);
    let quoted = quote_ident(table);
    let mut r = Rendered {
        sql: String::new(),
        params: Vec::new(),
    };
    match (op, write) {
        (Op::Cte { .. }, _) => {
            let inner = render_plain(tables, write, true)?;
            return Ok((
                Rendered {
                    sql: format!("with w as ({}) select count(*) from w", inner.sql),
                    params: inner.params,
                },
                true,
            ));
        }
        (Op::Upsert { update, .. }, Op::Insert { row, .. }) => {
            let columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
            let values: Vec<String> = row
                .iter()
                .map(|(c, v)| r.bind(v.clone(), &column_pg_type(tables, table, c)))
                .collect();
            r.sql = format!(
                "insert into {quoted} ({}) values ({}) {}",
                columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", "),
                conflict_clause(key, &columns, *update)
            );
        }
        (Op::Upsert { update, .. }, Op::Update { pk, changes, .. }) => {
            assert!(*update, "an upserted update is DO UPDATE — a generator bug");
            let mut columns = vec![key];
            let mut values = vec![r.bind(Some(pk.clone()), &key_type)];
            for (c, v) in changes {
                columns.push(c);
                values.push(r.bind(v.clone(), &column_pg_type(tables, table, c)));
            }
            r.sql = format!(
                "insert into {quoted} ({}) values ({}) {}",
                columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", "),
                conflict_clause(key, &columns, true)
            );
        }
        (Op::Merge { .. }, Op::Insert { row, .. }) => {
            let source: Vec<String> = row
                .iter()
                .map(|(c, v)| {
                    let value = r.bind(v.clone(), &column_pg_type(tables, table, c));
                    format!("{value} as {}", quote_ident(c))
                })
                .collect();
            let columns: Vec<String> = row.iter().map(|(c, _)| quote_ident(c)).collect();
            let values: Vec<String> = row
                .iter()
                .map(|(c, _)| format!("s.{}", quote_ident(c)))
                .collect();
            r.sql = format!(
                "merge into {quoted} t using (select {}) s on t.{k} = s.{k} \
                 when not matched then insert ({}) values ({})",
                source.join(", "),
                columns.join(", "),
                values.join(", "),
                k = quote_ident(key),
            );
        }
        (Op::Merge { .. }, Op::Update { pk, changes, .. }) => {
            let set: Vec<String> = changes
                .iter()
                .map(|(c, v)| {
                    let value = r.bind(v.clone(), &column_pg_type(tables, table, c));
                    format!("{}={value}", quote_ident(c))
                })
                .collect();
            let pk_value = r.bind(Some(pk.clone()), &key_type);
            r.sql = format!(
                "merge into {quoted} t using (select {pk_value} as k) s on t.{} = s.k \
                 when matched then update set {}",
                quote_ident(key),
                set.join(", ")
            );
        }
        (Op::Merge { .. }, Op::Delete { pk, .. }) => {
            let pk_value = r.bind(Some(pk.clone()), &key_type);
            r.sql = format!(
                "merge into {quoted} t using (select {pk_value} as k) s on t.{} = s.k \
                 when matched then delete",
                quote_ident(key)
            );
        }
        (form, write) => {
            panic!("no statement form renders {form:?} over {write:?} — a generator bug")
        }
    }
    Ok((r, false))
}

/// `ON CONFLICT (<key>) DO NOTHING`, or `DO UPDATE SET` every column of
/// `columns` but the key from `EXCLUDED`.
fn conflict_clause(key: &str, columns: &[&str], update: bool) -> String {
    let set: Vec<String> = columns
        .iter()
        .filter(|c| **c != key)
        .map(|c| format!("{q} = excluded.{q}", q = quote_ident(c)))
        .collect();
    if !update || set.is_empty() {
        return format!("on conflict ({}) do nothing", quote_ident(key));
    }
    format!(
        "on conflict ({}) do update set {}",
        quote_ident(key),
        set.join(", ")
    )
}

/// [`apply_op`] for an [`Op::Upsert`], [`Op::Merge`] or [`Op::Cte`].
async fn apply_statement_form(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
) -> Result<u64, ApplyOpError> {
    let (rendered, counted) = render_statement_form(tables, op)?;
    if counted {
        let row = raw.query_one(&rendered.sql, &rendered.params()).await?;
        let count: i64 = row.get(0);
        return Ok(count as u64);
    }
    Ok(raw.execute(&rendered.sql, &rendered.params()).await?)
}

/// How many times [`apply_txn`] runs a transaction again after a
/// serialization failure or deadlock before it gives up.
const MAX_TXN_RETRIES: u64 = 50;

/// Whether `err` is a serialization failure or a deadlock: the errors an
/// application retries its transaction on.
fn retryable(err: &ApplyOpError) -> bool {
    use tokio_postgres::error::SqlState;
    matches!(err, ApplyOpError::Db(e) if e.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE)
        || e.code() == Some(&SqlState::T_R_DEADLOCK_DETECTED))
}

/// How one run of a [`TxnWrap`] ended.
enum Attempt {
    /// Committed, with each op's result.
    Committed(Vec<Result<u64, String>>),
    /// Prepared and left prepared as `gid` ([`PrepareWrap::hold`]).
    Held(Vec<Result<u64, String>>, String),
    /// An op the program expected to succeed failed, the last result: the
    /// transaction is aborted.
    Aborted(Vec<Result<u64, String>>),
    /// A serialization failure or deadlock: run it again.
    Retry(ApplyOpError),
    /// Anything else outside an op.
    Fatal(ApplyOpError),
}

/// Applies `ops`, the ops of `wrap`, as the transaction `wrap` describes,
/// on `raw` (see [`TxnWrap`]): the body behind every backend's
/// [`super::OpApplier::apply_txn`].
///
/// # Panics
///
/// If `ops` isn't as long as `wrap.ops`, or holds an op that can't run
/// inside a transaction block (a `TRUNCATE`, whose count runs its own
/// transaction, or partition maintenance): a generator bug.
pub(super) async fn apply_txn(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    wrap: &TxnWrap,
    ops: &[Op],
) -> Result<super::TxnApplied, ApplyOpError> {
    assert_eq!(
        wrap.ops.len(),
        ops.len(),
        "apply_txn: one op per index — a generator bug"
    );
    assert!(
        ops.iter()
            .all(|op| !matches!(op, Op::Truncate { .. } | Op::Partition { .. })),
        "apply_txn: a TRUNCATE or partition maintenance never runs in a wrap — a generator bug"
    );
    let mut retries = 0;
    loop {
        let attempt = txn_attempt(raw, tables, wrap, ops).await;
        match attempt {
            Attempt::Committed(results) => {
                return Ok(super::TxnApplied {
                    results,
                    retries,
                    held: None,
                });
            }
            Attempt::Held(results, gid) => {
                let commit = wrap.prepare.is_some_and(|p| p.commit);
                return Ok(super::TxnApplied {
                    results,
                    retries,
                    held: Some(super::HeldPrepared { gid, commit }),
                });
            }
            Attempt::Aborted(results) => {
                raw.batch_execute("rollback").await?;
                return Ok(super::TxnApplied {
                    results,
                    retries,
                    held: None,
                });
            }
            Attempt::Retry(err) => {
                raw.batch_execute("rollback").await?;
                retries += 1;
                if retries > MAX_TXN_RETRIES {
                    return Err(err);
                }
                tokio::time::sleep(Duration::from_millis(retries.min(20))).await;
            }
            Attempt::Fatal(err) => {
                // The session may already be out of the transaction.
                let _ = raw.batch_execute("rollback").await;
                return Err(err);
            }
        }
    }
}

/// One run of `wrap`, from `BEGIN` to `COMMIT`.
async fn txn_attempt(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    wrap: &TxnWrap,
    ops: &[Op],
) -> Attempt {
    macro_rules! step {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(err) => {
                    let err = ApplyOpError::from(err);
                    return if retryable(&err) {
                        Attempt::Retry(err)
                    } else {
                        Attempt::Fatal(err)
                    };
                }
            }
        };
    }
    let begin = async |raw: &mut tokio_postgres::Client| -> Result<(), ApplyOpError> {
        raw.batch_execute(&format!("begin isolation level {}", wrap.isolation.sql()))
            .await?;
        if wrap.read_first {
            first_read(raw, tables, &ops[0]).await?;
            think(wrap).await;
        }
        Ok(())
    };
    step!(begin(raw).await);
    // A prepared transaction rolled back at once is a rehearsal that ends in
    // `PREPARE` and `ROLLBACK PREPARED`.
    let rehearse_prepared = wrap.prepare.is_some_and(|p| !p.commit && !p.hold);
    if wrap.rehearse || rehearse_prepared {
        match run_ops(raw, tables, wrap, ops, 0..ops.len()).await {
            Ok(Ok(_)) => {}
            Ok(Err(results)) => return Attempt::Aborted(results),
            Err(err) => return classify(err),
        }
        if rehearse_prepared {
            let gid = next_gid();
            step!(
                raw.batch_execute(&format!("prepare transaction '{gid}'"))
                    .await
            );
            step!(
                raw.batch_execute(&format!("rollback prepared '{gid}'"))
                    .await
            );
        } else {
            step!(raw.batch_execute("rollback").await);
        }
        step!(begin(raw).await);
    }
    let mut results = Vec::with_capacity(ops.len());
    let savepoint = wrap
        .savepoint
        .filter(|sp| sp.start < sp.end && sp.end <= ops.len());
    let mut at = 0;
    while at < ops.len() {
        let (range, sp) = match savepoint {
            Some(sp) if sp.start == at => (sp.start..sp.end, Some(sp)),
            Some(sp) if sp.start > at => (at..sp.start, None),
            _ => (at..ops.len(), None),
        };
        at = range.end;
        if let Some(sp) = sp {
            step!(raw.batch_execute("savepoint trellis_gen_wrap").await);
            if sp.rollback {
                match run_ops(raw, tables, wrap, ops, range.clone()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(results)) => return Attempt::Aborted(results),
                    Err(err) => return classify(err),
                }
                step!(
                    raw.batch_execute("rollback to savepoint trellis_gen_wrap")
                        .await
                );
            }
        }
        match run_ops(raw, tables, wrap, ops, range).await {
            Ok(Ok(done)) => results.extend(done),
            Ok(Err(done)) => {
                results.extend(done);
                return Attempt::Aborted(results);
            }
            Err(err) => return classify(err),
        }
        if sp.is_some() {
            step!(
                raw.batch_execute("release savepoint trellis_gen_wrap")
                    .await
            );
        }
    }
    match wrap.prepare.filter(|p| p.commit || p.hold) {
        None => {
            step!(raw.batch_execute("commit").await);
            Attempt::Committed(results)
        }
        Some(p) => {
            let gid = next_gid();
            step!(
                raw.batch_execute(&format!("prepare transaction '{gid}'"))
                    .await
            );
            if p.hold {
                return Attempt::Held(results, gid);
            }
            step!(raw.batch_execute(&format!("commit prepared '{gid}'")).await);
            Attempt::Committed(results)
        }
    }
}

/// A global transaction id no other prepared transaction of this process
/// shares. The harness fails a case that leaves one prepared at the oracle
/// ([`PREPARED_GID_PREFIX`]).
fn next_gid() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{PREPARED_GID_PREFIX}{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

/// Every prepared transaction the generative suite makes has a gid with this
/// prefix.
pub const PREPARED_GID_PREFIX: &str = "trellis_gen_";

/// Resolves a transaction [`apply_txn`] left prepared, as the program says:
/// `COMMIT PREPARED`, or `ROLLBACK PREPARED` and then `wrap`'s ops again in
/// a transaction that commits (the oracle identity), whose result this
/// returns. With no `ops` it only rolls back.
pub(super) async fn finish_prepared(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    held: &super::HeldPrepared,
    wrap: &TxnWrap,
    ops: &[Op],
) -> Result<super::TxnApplied, ApplyOpError> {
    let gid = &held.gid;
    if held.commit {
        raw.batch_execute(&format!("commit prepared '{gid}'"))
            .await?;
        return Ok(super::TxnApplied::default());
    }
    raw.batch_execute(&format!("rollback prepared '{gid}'"))
        .await?;
    if ops.is_empty() {
        return Ok(super::TxnApplied::default());
    }
    let again = TxnWrap {
        prepare: None,
        rehearse: false,
        ..wrap.clone()
    };
    apply_txn(raw, tables, &again, ops).await
}

#[cfg(test)]
#[path = "sql_txn_tests.rs"]
mod txn_tests;

fn classify(err: ApplyOpError) -> Attempt {
    if retryable(&err) {
        Attempt::Retry(err)
    } else {
        Attempt::Fatal(err)
    }
}

/// `wrap`'s think time, if any.
async fn think(wrap: &TxnWrap) {
    if !wrap.think.is_zero() {
        tokio::time::sleep(wrap.think).await;
    }
}

/// A read of `op`'s row by key, or of any one row of its table for an op
/// with no single key.
async fn first_read(
    raw: &tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    op: &Op,
) -> Result<(), ApplyOpError> {
    let write = op.write();
    let write = match write {
        Op::Leaf { op, .. } => op.as_ref(),
        other => other,
    };
    let table = write.table();
    let key = pk_col(tables, table)?;
    let pk = match write {
        Op::Insert { row, .. } => row
            .iter()
            .find(|(c, _)| c == key)
            .and_then(|(_, v)| v.clone()),
        Op::Update { pk, .. } | Op::Delete { pk, .. } => Some(pk.clone()),
        _ => None,
    };
    match pk {
        Some(pk) => {
            raw.query(
                &format!(
                    "select 1 from {} where {}=$1::text::{}",
                    quote_ident(table),
                    quote_ident(key),
                    column_pg_type(tables, table, key)
                ),
                &[&pk],
            )
            .await?;
        }
        None => {
            raw.query(
                &format!("select 1 from {} limit 1", quote_ident(table)),
                &[],
            )
            .await?;
        }
    }
    Ok(())
}

/// Runs `ops[range]` inside the open transaction. An op that may fail runs
/// in a savepoint of its own. `Err` is a serialization failure or another
/// error outside an op's own statement; `Ok(Err(results))` ends at an op
/// that failed where the program expected it to succeed.
async fn run_ops(
    raw: &mut tokio_postgres::Client,
    tables: &HashMap<String, Table>,
    wrap: &TxnWrap,
    ops: &[Op],
    range: std::ops::Range<usize>,
) -> Result<Result<Vec<Result<u64, String>>, Vec<Result<u64, String>>>, ApplyOpError> {
    let mut results = Vec::with_capacity(range.len());
    for i in range {
        let op = &ops[i];
        let may_fail = op.expect().matches(&OpOutcome::Fails);
        if may_fail {
            raw.batch_execute("savepoint trellis_gen_stmt").await?;
        }
        match apply_op(raw, tables, op).await {
            Ok(count) => {
                if may_fail {
                    raw.batch_execute("release savepoint trellis_gen_stmt")
                        .await?;
                }
                results.push(Ok(count));
            }
            Err(err) if retryable(&err) => return Err(err),
            Err(err) => {
                results.push(Err(format!("{err:?}")));
                if !may_fail {
                    return Ok(Err(results));
                }
                raw.batch_execute(
                    "rollback to savepoint trellis_gen_stmt; release savepoint trellis_gen_stmt",
                )
                .await?;
            }
        }
        if i + 1 < ops.len() {
            think(wrap).await;
        }
    }
    Ok(Ok(results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use trellis::dev::defs::parse;

    /// A partitioned source keys on `(pk, partition column)`, both `NOT
    /// NULL`, and its leaves cover every value each strategy routes.
    #[test]
    fn a_partitioned_source_keys_on_its_partition_column_and_routes_every_value() {
        let table = Table {
            name: "t0".to_string(),
            pk_col: "c0".to_string(),
            columns: vec![
                Column {
                    name: "c0".to_string(),
                    value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
                },
                Column {
                    name: "g".to_string(),
                    value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
                },
            ],
            unique_cols: Vec::new(),
            unique_sets: Vec::new(),
            partitioning: None,
            source_key: None,
        };
        let ddl = |strategy| {
            partitioned_source_ddl(
                "\"public\".\"t0\"",
                &table,
                &Partitioning {
                    column: "g".to_string(),
                    strategy,
                },
            )
        };
        assert_eq!(
            ddl(PartitionStrategy::Range { bounds: 2 }),
            "create table \"public\".\"t0\" (\"c0\" bigint not null, \"g\" bigint not null, \
             primary key (\"c0\", \"g\")) partition by range (\"g\"); \
             create table \"public\".\"t0_p0\" partition of \"public\".\"t0\" \
             for values from (minvalue) to (1); \
             create table \"public\".\"t0_p1\" partition of \"public\".\"t0\" \
             for values from (1) to (maxvalue);"
        );
        assert!(
            ddl(PartitionStrategy::List {
                values: 1,
                default: true
            })
            .ends_with(
                "\"t0_p0\" partition of \"public\".\"t0\" for values in (0); \
                 create table \"public\".\"t0_pd\" partition of \"public\".\"t0\" default;"
            )
        );
        assert!(
            ddl(PartitionStrategy::Hash { modulus: 2 })
                .ends_with("for values with (modulus 2, remainder 1);")
        );
    }

    /// Real operator precedence: a nested, mixed-operator `Expr` must
    /// round-trip through `render_expr` and back through the real parser to
    /// the exact same tree, not a reflowed one.
    #[test]
    fn render_expr_parenthesizes_nested_binary_ops_so_they_round_trip() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("b".to_string())),
                rhs: Box::new(Expr::Column("c".to_string())),
            }),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(
            def.fields[0].expr, expr,
            "round-tripping through render_expr -> parse must reproduce the exact original tree"
        );
    }

    /// A qualified source column inside an aggregate renders
    /// back to the same qualified spelling, so a generated definition that
    /// carries one re-parses to the same tree.
    #[test]
    fn render_expr_round_trips_a_qualified_source_column_in_an_aggregate() {
        for (schema, prefix) in [(None, "s"), (Some("app"), "app.s")] {
            let expr = Expr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![Expr::SourceColumn {
                    schema: schema.map(str::to_string),
                    table: "s".to_string(),
                    column: "v".to_string(),
                }],
            };
            let rendered = render_expr(&expr);
            assert_eq!(rendered, format!("SUM({prefix}.v)"));
            let text = format!("TRANSFORM t FROM s GROUP BY g SELECT g AS g, {rendered} AS total");
            let def = parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            assert_eq!(def.fields[1].expr, expr);
        }
    }

    /// Every `JOIN` shape the JOIN tier draws renders back to text
    /// that parses to the same definition.
    #[test]
    fn render_definition_round_trips_a_join() {
        use crate::generate::join::{JoinDefSpec, JoinField, JoinGroups, JoinKey, join_definition};
        for left in [true, false] {
            for key in [JoinKey::Single, JoinKey::Pair] {
                for groups in [JoinGroups::Joined, JoinGroups::Both, JoinGroups::From] {
                    let def = join_definition(
                        "j",
                        &JoinDefSpec {
                            left,
                            key,
                            groups,
                            fields: JoinField::ALL.to_vec(),
                        },
                    );
                    let text = render_definition(&def);
                    let reparsed = parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
                    assert_eq!(reparsed, def, "{text}");
                }
            }
        }
    }

    /// A joined table's `KEY (...)` (#2171) renders back after the table's
    /// name, beside the `FROM` side's: dropped, the definition would read
    /// the joined table by its primary key, or fail define without one.
    #[test]
    fn render_definition_round_trips_a_joined_tables_key() {
        for text in [
            "TRANSFORM j FROM ja KEY (k0) LEFT JOIN jb KEY (k0, k1) \
             ON ja.k0 = jb.k0 AND ja.k1 = jb.k1 SELECT jb.y AS y",
            "TRANSFORM j FROM ja INNER JOIN jb KEY (k0) ON ja.k0 = jb.k0 SELECT jb.y AS y",
            "TRANSFORM j FROM ja LEFT JOIN jb KEY (k0) ON ja.k0 = jb.k0 \
             GROUP BY ja.t SELECT COUNT(*) AS n",
        ] {
            let def = parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
            let rendered = render_definition(&def);
            assert!(rendered.contains("jb KEY (k0"), "{rendered}");
            let reparsed = parse(&rendered).unwrap_or_else(|e| panic!("{rendered}: {e:?}"));
            assert_eq!(reparsed, def, "{rendered}");
        }
    }

    /// The mirror shape, pinned so a future change to `render_expr` can't
    /// quietly regress this direction while only testing the other one.
    #[test]
    fn render_expr_round_trips_a_greater_than_wrapping_an_add() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            }),
            rhs: Box::new(Expr::Column("c".to_string())),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(def.fields[0].expr, expr);
    }

    // --- `quiesce` ---
    //
    // No engine runs in these tests. Each stands the engine's side up by hand
    // on a second session, in the order the engine itself commits it, so the
    // interleaving that used to let `quiesce` return early is exact rather
    // than left to timing. Replication progress is faked far ahead, so the
    // ring's convergence predicate turns only on what the ring holds.

    const SOURCE: &str = "public.s";

    async fn session(dsn: &str) -> tokio_postgres::Client {
        let config = trellis::Config::from_dsn(dsn.to_string()).expect("config");
        let (client, connection) = tokio_postgres::connect(config.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("set search_path to {}, public", config.schema()))
            .await
            .expect("set search_path");
        client
    }

    /// One definition `t` on `SOURCE`, catalogued with `status`, and the
    /// harness's view of it.
    async fn catalog_definition(client: &tokio_postgres::Client, status: &str) -> TransformDef {
        client
            .batch_execute(&format!(
                "insert into source_table_versions (source_table, version) values ('{SOURCE}', 1); \
                 insert into transform_definitions \
                     (target_table, source_table, source_version, definition_text, status) \
                     values ('public.t', '{SOURCE}', 1, 'TRANSFORM t FROM s SELECT a AS a', '{status}')"
            ))
            .await
            .expect("catalog the definition");
        parse("TRANSFORM t FROM s SELECT a AS a").expect("parse")
    }

    /// Stages what a build stages for a `NULL`-keyed row: an image-less `recompute` row,
    /// NULL `origin_lsn`, in the active slot (`seg_0` on a fresh ring).
    const STAGE_ENUMERATION: &str =
        "insert into seg_0 (src_table, key, op) values ('public.s', '1', 'recompute')";

    /// Stands in for the drain: empties the active slot. Returns the instant
    /// just before it started, so a `quiesce` that waited for it can't have
    /// returned earlier.
    async fn drain_ring(client: &tokio_postgres::Client) -> Instant {
        let drained_at = Instant::now();
        client
            .execute("delete from seg_0", &[])
            .await
            .expect("drain");
        drained_at
    }

    /// A build stages null-origin rows (a 1-1 build's `NULL`-key recomputes)
    /// and only then flips the definition `live`, in a later transaction. The
    /// ring wait must start after the flip, or it passes on an empty ring and
    /// the status wait returns at the flip with those rows still undrained.
    #[tokio::test]
    async fn quiesce_drains_rows_staged_before_their_definition_went_live() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "backfilling").await;

        let engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            engine.execute(STAGE_ENUMERATION, &[]).await.expect("stage");
            engine
                .execute("update transform_definitions set status = 'live'", &[])
                .await
                .expect("go live");
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], TEST_BUDGET).await.expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned before the rows staged ahead of the flip to live drained"
        );
    }

    /// A rebuilding definition reads `backfilling` until its last job
    /// commits, here with null-origin rows staged in the same transaction as
    /// the flip to `live`. `quiesce` waits for the flip and then for those
    /// rows to drain.
    #[tokio::test]
    async fn quiesce_waits_for_a_rebuilding_definition_and_its_staged_rows() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "backfilling").await;

        let mut engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            finish_build(&mut engine).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], TEST_BUDGET).await.expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned before the rebuild finished and its staged rows drained"
        );
    }

    /// A `live` definition drops to `backfilling` while `quiesce` is already
    /// waiting on the ring (a rebuild started after it went live). The ring
    /// draining must not end the wait: the re-check has to find the
    /// definition rebuilding and wait out the rebuild and its staged rows'
    /// drain too.
    #[tokio::test]
    async fn quiesce_waits_for_a_rebuild_started_while_it_waited_on_the_ring() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        // Earlier staged rows, still undrained, so `quiesce` passes the
        // status check at once and settles into the ring wait.
        raw.execute(STAGE_ENUMERATION, &[])
            .await
            .expect("stage the earlier rows");

        let mut engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            engine
                .execute(
                    "update transform_definitions set status = 'backfilling'",
                    &[],
                )
                .await
                .expect("start a rebuild");
            tokio::time::sleep(Duration::from_millis(200)).await;
            drain_ring(&engine).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            finish_build(&mut engine).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            drain_ring(&engine).await
        });

        quiesce(&raw, &[def], TEST_BUDGET).await.expect("quiesce");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(
            returned_at >= drained_at,
            "quiesce returned on the first drain, with a rebuild started during its ring wait \
             still running"
        );
    }

    /// A build that never finishes is a named timeout, not a hang and not a
    /// silent success.
    #[tokio::test]
    async fn quiesce_names_a_definition_still_backfilling_on_timeout() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "backfilling").await;

        let budget = QuiesceBudget {
            no_progress: Duration::from_millis(300),
            cap: Duration::from_millis(300),
        };
        match quiesce(&raw, &[def], budget).await {
            Err(QuiesceError::DefinitionsUnsettled { unsettled, .. }) => {
                assert_eq!(unsettled, vec!["t".to_string()]);
            }
            other => panic!("expected DefinitionsUnsettled, got {other:?}"),
        }
    }

    /// A build's last job: rows it staged and the definition's flip to
    /// `live` commit together.
    async fn finish_build(engine: &mut tokio_postgres::Client) {
        let txn = engine.transaction().await.expect("begin");
        txn.execute(STAGE_ENUMERATION, &[]).await.expect("stage");
        txn.execute("update transform_definitions set status = 'live'", &[])
            .await
            .expect("go live");
        txn.commit().await.expect("commit");
    }

    /// The partition model's `HASH` routing table is the one
    /// Postgres computes, for every modulus and grain it covers.
    #[tokio::test]
    async fn the_hash_remainders_are_postgres_own() {
        use crate::generate::partition::{HASH_REMAINDERS, ROUTED_GRAIN_MAX};
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        raw.batch_execute("create table h (g bigint not null) partition by hash (g)")
            .await
            .expect("create");
        for (m, row) in HASH_REMAINDERS.iter().enumerate() {
            let modulus = m as i32 + 1;
            for (grain, &remainder) in row.iter().enumerate() {
                let found: i32 = raw
                    .query_one(
                        "select r from generate_series(0, $1 - 1) r \
                         where satisfies_hash_partition('h'::regclass, $1, r, $2::bigint)",
                        &[&modulus, &(grain as i64)],
                    )
                    .await
                    .expect("remainder")
                    .get(0);
                assert_eq!(
                    found,
                    i32::from(remainder),
                    "modulus {modulus}, grain {grain} (of 0..={ROUTED_GRAIN_MAX})"
                );
            }
        }
    }

    /// A partitioned source is unsettled while its tree differs
    /// from the one recorded (here: a leaf attached since), and while a
    /// correction is queued for it.
    #[tokio::test]
    async fn a_partitioned_source_is_unsettled_until_recorded_and_corrected() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        raw.batch_execute(
            "create table public.r (g bigint not null) partition by list (g); \
             create table public.r0 partition of public.r for values in (0);",
        )
        .await
        .expect("create");
        let record = "insert into partition_sources \
                          (table_name, root_oid, lifecycle_fp, compliance_fp, nodes, \
                           catalog_oid) \
                      select 'public.r', 'public.r'::regclass::oid::bigint, '', '', \
                             jsonb_build_object('nodes', jsonb_agg(jsonb_build_object( \
                                 'oid', t.relid::oid::bigint, \
                                 'inherits_xmin', i.xmin::text))), \
                             pg_catalog.to_regclass('partition_sources') \
                      from pg_partition_tree('public.r') t \
                      left join pg_inherits i on i.inhrelid = t.relid \
                      on conflict (table_name) do update set nodes = excluded.nodes";
        raw.batch_execute(record).await.expect("record");
        assert!(unsettled_partitions(&raw).await.expect("probe").is_empty());

        raw.batch_execute("create table public.r1 partition of public.r for values in (1)")
            .await
            .expect("attach");
        assert_eq!(
            unsettled_partitions(&raw).await.expect("probe"),
            vec!["public.r".to_string()]
        );
        raw.batch_execute(record).await.expect("record again");
        assert!(unsettled_partitions(&raw).await.expect("probe").is_empty());

        raw.batch_execute(
            "insert into partition_corrections (table_name, kind, node_oid, node_name, cause) \
             values ('public.r', 'new_leaf', 'public.r1'::regclass::oid::bigint, \
                     'public.r1', 'created')",
        )
        .await
        .expect("queue");
        assert_eq!(
            unsettled_partitions(&raw).await.expect("probe"),
            vec!["public.r".to_string()]
        );
    }

    /// A budget no test reaches: these tests' engine side finishes in
    /// well under a second.
    const TEST_BUDGET: QuiesceBudget = QuiesceBudget {
        no_progress: Duration::from_secs(30),
        cap: Duration::from_secs(180),
    };

    /// A short budget for the progress tests, so a window passes quickly.
    const SHORT_WINDOW: QuiesceBudget = QuiesceBudget {
        no_progress: Duration::from_secs(2),
        cap: Duration::from_secs(20),
    };

    /// Stands in for one committed drain page: moves bucket 0's
    /// `drain_cursor` row of the first segment to key `step`, as a page
    /// with more of its bucket left does.
    async fn commit_a_page(client: &tokio_postgres::Client, step: i64) {
        client
            .execute(
                "insert into drain_cursor (seg_seq, bucket, after_route, after_src_table, after_key) \
                 values (1, 0, $1, 'public.s', $2) \
                 on conflict (seg_seq, bucket) do update \
                 set after_route = excluded.after_route, after_key = excluded.after_key",
                &[&step, &step.to_string()],
            )
            .await
            .expect("move the cursor");
    }

    fn assert_ring_timeout(result: Result<(), QuiesceError>) -> Duration {
        let err = result.expect_err("the quiesce must time out");
        let shape = format!("{err:?}");
        assert!(
            shape.starts_with("Staging(ConvergenceTimeout { token: "),
            "a ring timeout keeps its error shape: {shape}"
        );
        match err {
            QuiesceError::Staging(StagingError::ConvergenceTimeout { waited, .. }) => waited,
            other => unreachable!("{other:?}"),
        }
    }

    /// A ring that never drains and whose drain state never moves (a stuck
    /// page, a held key) fails one no-progress window after the quiesce
    /// started, as a fixed budget of that length did.
    #[tokio::test]
    async fn a_ring_that_never_moves_fails_after_one_no_progress_window() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        raw.execute(STAGE_ENUMERATION, &[]).await.expect("stage");

        let started = Instant::now();
        let waited = assert_ring_timeout(quiesce(&raw, &[def], SHORT_WINDOW).await);
        let elapsed = started.elapsed();
        assert!(
            waited >= SHORT_WINDOW.no_progress - Duration::from_millis(500),
            "gave up early: {waited:?}"
        );
        assert!(
            elapsed < SHORT_WINDOW.no_progress + Duration::from_secs(3),
            "a quiesce with no progress must not be extended: {elapsed:?}"
        );
    }

    /// A drain that keeps committing pages, for longer than a window in all,
    /// is waited out: each committed page restarts the window.
    #[tokio::test]
    async fn a_ring_that_keeps_draining_is_waited_out_past_the_window() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        raw.execute(STAGE_ENUMERATION, &[]).await.expect("stage");

        let engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            for step in 0..10 {
                tokio::time::sleep(Duration::from_millis(500)).await;
                commit_a_page(&engine, step).await;
            }
            drain_ring(&engine).await
        });

        let started = Instant::now();
        quiesce(&raw, &[def], SHORT_WINDOW)
            .await
            .expect("a drain that keeps committing pages converges");
        let returned_at = Instant::now();
        let drained_at = engine_side.await.expect("engine side");
        assert!(returned_at >= drained_at);
        assert!(
            started.elapsed() > SHORT_WINDOW.no_progress * 2,
            "the drain ran past the window, so the wait must have too"
        );
    }

    /// However long the ring keeps advancing, the quiesce gives up at the
    /// cap.
    #[tokio::test]
    async fn a_ring_that_keeps_moving_without_draining_fails_at_the_cap() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        raw.execute(STAGE_ENUMERATION, &[]).await.expect("stage");

        let engine = session(db.dsn()).await;
        let engine_side = tokio::spawn(async move {
            for step in 0..=i64::MAX {
                tokio::time::sleep(Duration::from_millis(300)).await;
                commit_a_page(&engine, step).await;
            }
        });

        let budget = QuiesceBudget {
            no_progress: Duration::from_secs(2),
            cap: Duration::from_secs(6),
        };
        let started = Instant::now();
        assert_ring_timeout(quiesce(&raw, &[def], budget).await);
        let elapsed = started.elapsed();
        engine_side.abort();
        assert!(
            elapsed >= budget.cap - Duration::from_millis(500)
                && elapsed < budget.cap + Duration::from_secs(3),
            "ended at {elapsed:?}, not the cap"
        );
    }

    /// A session holding the ring's bookkeeping table locked blocks the
    /// convergence poll and the progress sample alike. Both are bounded, so
    /// the quiesce still fails one window in, and the state dump doesn't
    /// hang on the lock either.
    #[tokio::test]
    async fn a_held_lock_on_the_ring_still_fails_after_one_window() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = session(db.dsn()).await;
        let def = catalog_definition(&raw, "live").await;
        raw.execute(STAGE_ENUMERATION, &[]).await.expect("stage");
        let holder = session(db.dsn()).await;
        holder
            .batch_execute("begin; lock table segments in access exclusive mode")
            .await
            .expect("hold the lock");

        let started = Instant::now();
        assert_ring_timeout(quiesce(&raw, &[def], SHORT_WINDOW).await);
        let elapsed = started.elapsed();
        assert!(
            elapsed < SHORT_WINDOW.no_progress + Duration::from_secs(12),
            "a held lock must not stretch the wait: {elapsed:?}"
        );
        holder.batch_execute("rollback").await.expect("release");
    }
}
