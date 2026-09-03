//! Postgres-backed catalog for transform definitions (issue #23).
//!
//! **Schema choice**: a definition is stored as its original source text
//! (`transform_definitions.definition_text`) rather than a serialized AST.
//! Reading it back re-parses via [`super::parse`], reusing the grammar's
//! own parser instead of standing up a second, independently-drifting
//! serialization format for the same information. The tradeoff is a
//! re-parse on every read; given definitions are small and read
//! infrequently relative to the source-row volume they govern, that's the
//! right side to take the cost on. Flagged in the issue #23 report as the
//! open question to confirm.
//!
//! Every source table has exactly one monotonically increasing version, in
//! `source_table_versions`, that definition creation bumps in the same
//! transaction as the insert. That version lives in a real, lockable row
//! (not a computed value) so stage 05's version fence can `FOR SHARE`/
//! `FOR UPDATE` it directly.
//!
//! v1 definitions are immutable: this module only exposes creation and
//! read, no update/delete.

use std::collections::HashMap;
use std::fmt;

use crate::pool::Pool;

use super::ast::{TransformDef, ValueType};
use super::error::ParseError;
use super::model::{Definition, NodeKind, SchemaNode};
use super::parser::parse;
use super::validate::{ValidationError, validate};

/// Why creating or reading a definition failed.
#[derive(Debug)]
pub enum CatalogError {
    /// The source text failed to parse (issue #22's grammar).
    Parse(ParseError),
    /// The parsed definition failed validation (issue #23).
    Validate(ValidationError),
    /// Acquiring a connection or running a query against Postgres failed.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
    /// A definition's persisted `source_columns` jsonb held a value other
    /// than `"numeric"`/`"text"`/`"boolean"`/`"uuid"` for some column —
    /// meaning the row was written by something other than
    /// [`create_definition`], since that's the only writer and it only ever
    /// encodes [`ValueType`]'s variants.
    UnknownValueType { column: String, text: String },
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::Parse(err) => write!(f, "failed to parse transform definition: {err}"),
            CatalogError::Validate(err) => {
                write!(f, "transform definition failed validation: {err}")
            }
            CatalogError::Db(err) => {
                write!(f, "transform catalog database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            CatalogError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            CatalogError::UnknownValueType { column, text } => write!(
                f,
                "column '{column}' has an unrecognized persisted value type '{text}'"
            ),
        }
    }
}

impl std::error::Error for CatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CatalogError::Parse(err) => Some(err),
            CatalogError::Validate(err) => Some(err),
            CatalogError::Db(err) => Some(err),
            CatalogError::Pool(err) => Some(err),
            CatalogError::UnknownValueType { .. } => None,
        }
    }
}

impl From<ParseError> for CatalogError {
    fn from(err: ParseError) -> Self {
        CatalogError::Parse(err)
    }
}

impl From<ValidationError> for CatalogError {
    fn from(err: ValidationError) -> Self {
        CatalogError::Validate(err)
    }
}

impl From<tokio_postgres::Error> for CatalogError {
    fn from(err: tokio_postgres::Error) -> Self {
        CatalogError::Db(err)
    }
}

impl From<crate::error::Error> for CatalogError {
    fn from(err: crate::error::Error) -> Self {
        CatalogError::Pool(err)
    }
}

/// Parses, validates, and stores a new transform definition, bumping its
/// source table's version in the same transaction. `source_columns` maps the
/// definition's source table's known columns to their [`ValueType`] (see
/// [`super::validate::validate`] — introspecting a live Postgres schema for
/// this is intake's concern, out of scope here).
pub async fn create_definition(
    pool: &Pool,
    source_text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Definition, CatalogError> {
    let def: TransformDef = parse(source_text)?;
    validate(&def, source_columns)?;

    let mut client = pool.get().await?;
    let txn = client.transaction().await?;

    // Issue #20: every definition's source and target resolve to a
    // first-class `SchemaNode`, created on first reference (a source node
    // the moment something first transforms it; a target node the moment
    // its owning definition is created) — a side effect alongside the
    // catalog writes below rather than a change to `TransformDef`'s shape,
    // per the issue's "prefer the smaller change" guidance.
    resolve_node_in_txn(&txn, &def.source, NodeKind::Source).await?;
    resolve_node_in_txn(&txn, &def.target, NodeKind::Target).await?;

    let version: i64 = txn
        .query_one(
            "insert into source_table_versions (source_table, version)
             values ($1, 1)
             on conflict (source_table)
             do update set version = source_table_versions.version + 1
             returning version",
            &[&def.source],
        )
        .await?
        .get(0);

    let (type_keys, type_vals) = encode_type_map(source_columns);

    let id: i64 = txn
        .query_one(
            "insert into transform_definitions
                (target_table, source_table, source_version, definition_text, source_columns)
             values ($1, $2, $3, $4, jsonb_object($5::text[], $6::text[]))
             returning id",
            &[
                &def.target,
                &def.source,
                &version,
                &source_text,
                &type_keys,
                &type_vals,
            ],
        )
        .await?
        .get(0);

    txn.commit().await?;

    Ok(Definition {
        id,
        source_version: version,
        def,
        source_columns: source_columns.clone(),
    })
}

/// Resolves `table_name` to its [`SchemaNode`], creating one if this is the
/// first time Trellis has seen the table and setting its `kind` role flag
/// (`is_source`/`is_target`) to `true`. Idempotent, and additive across
/// roles: resolving the same table under both [`NodeKind::Source`] and
/// [`NodeKind::Target`] over separate calls (chained/multi-hop transforms —
/// see [`super::model::NodeKind`]'s doc comment) merges into one node with
/// both flags set, rather than erroring.
///
/// Runs in its own transaction; [`create_definition`] instead calls
/// [`resolve_node_in_txn`] directly so both of a definition's node
/// resolutions land in the same transaction as the definition write.
pub async fn resolve_node(
    pool: &Pool,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let node = resolve_node_in_txn(&txn, table_name, kind).await?;
    txn.commit().await?;
    Ok(node)
}

/// The transactional core of [`resolve_node`] — see its doc comment.
/// Upserts `table_name`, OR-ing `kind`'s role flag into whatever the row
/// already has (or defaulting the other flag `false` if the row is new).
async fn resolve_node_in_txn(
    txn: &tokio_postgres::Transaction<'_>,
    table_name: &str,
    kind: NodeKind,
) -> Result<SchemaNode, CatalogError> {
    let (is_source, is_target) = match kind {
        NodeKind::Source => (true, false),
        NodeKind::Target => (false, true),
    };

    let row = txn
        .query_one(
            "insert into schema_nodes (table_name, is_source, is_target)
             values ($1, $2, $3)
             on conflict (table_name) do update
                set is_source = schema_nodes.is_source or excluded.is_source,
                    is_target = schema_nodes.is_target or excluded.is_target
             returning id, is_source, is_target",
            &[&table_name, &is_source, &is_target],
        )
        .await?;

    Ok(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    })
}

/// The [`SchemaNode`] already resolved for `table_name`, or `None` if
/// nothing has ever referenced it as a source or a target.
pub async fn node_for_table(
    pool: &Pool,
    table_name: &str,
) -> Result<Option<SchemaNode>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select id, is_source, is_target from schema_nodes where table_name = $1",
            &[&table_name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };

    Ok(Some(SchemaNode {
        id: row.get(0),
        table_name: table_name.to_string(),
        is_source: row.get(1),
        is_target: row.get(2),
    }))
}

/// Splits `source_columns` into the parallel key/value text arrays
/// `jsonb_object`'s two-array form wants (see `create_definition`'s insert
/// and [`transforms_for_source`]'s matching read side).
fn encode_type_map(source_columns: &HashMap<String, ValueType>) -> (Vec<&str>, Vec<&'static str>) {
    let mut keys = Vec::with_capacity(source_columns.len());
    let mut vals = Vec::with_capacity(source_columns.len());
    for (name, value_type) in source_columns {
        keys.push(name.as_str());
        vals.push(match value_type {
            ValueType::Numeric => "numeric",
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
        });
    }
    (keys, vals)
}

/// One definition row's non-`source_columns` fields, accumulated while
/// [`transforms_for_source`] walks its single, lateral-joined query — see
/// that function's doc comment.
struct PendingDefinition {
    source_version: i64,
    text: String,
    source_columns: HashMap<String, ValueType>,
}

/// The transform definitions currently subscribed to `source_table` — the
/// mapping intake (#7/#8) will use to decide what to subscribe to.
///
/// One query, not one-per-definition-row (issue #69): a `left join lateral
/// jsonb_each_text(...)` unnests every subscribed definition's persisted
/// `source_columns` map inline, so this is still "decode JSON via SQL, no
/// serde_json dependency" — matching `staging::apply::decode_image`'s
/// convention — just decoded for every row in one round trip instead of one
/// per definition. The `left join` (rather than an inner join/`cross join
/// lateral`) matters: a definition whose `source_columns` is `{}` must still
/// come back with zero entries, not disappear from the result entirely.
pub async fn transforms_for_source(
    pool: &Pool,
    source_table: &str,
) -> Result<Vec<Definition>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select t.id, t.source_version, t.definition_text, e.key, e.value
             from transform_definitions t
             left join lateral jsonb_each_text(t.source_columns) e on true
             where t.source_table = $1
             order by t.id",
            &[&source_table],
        )
        .await?;

    // `order` preserves the query's `order by t.id` across the group-by
    // done in Rust below (a plain `HashMap` has no ordering of its own).
    let mut order: Vec<i64> = Vec::new();
    let mut by_id: HashMap<i64, PendingDefinition> = HashMap::new();

    for row in rows {
        let id: i64 = row.get(0);
        let key: Option<String> = row.get(3);
        let value: Option<String> = row.get(4);

        let pending = by_id.entry(id).or_insert_with(|| {
            order.push(id);
            PendingDefinition {
                source_version: row.get(1),
                text: row.get(2),
                source_columns: HashMap::new(),
            }
        });

        if let (Some(key), Some(value)) = (key, value) {
            let value_type = match value.as_str() {
                "numeric" => ValueType::Numeric,
                "text" => ValueType::Text,
                "boolean" => ValueType::Boolean,
                "uuid" => ValueType::Uuid,
                other => {
                    return Err(CatalogError::UnknownValueType {
                        column: key,
                        text: other.to_string(),
                    });
                }
            };
            pending.source_columns.insert(key, value_type);
        }
    }

    let mut result = Vec::with_capacity(order.len());
    for id in order {
        let pending = by_id.remove(&id).expect("id was just pushed to order");
        let def = parse(&pending.text)?;
        result.push(Definition {
            id,
            source_version: pending.source_version,
            def,
            source_columns: pending.source_columns,
        });
    }
    Ok(result)
}

/// Every distinct source table with at least one registered transform
/// definition, unqualified (as stored — see [`create_definition`]'s
/// `def.source`). Issue #14: a running [`crate::Client`]'s maintenance loop
/// polls this to notice a transform registered against a source table it
/// hasn't seen before, so it can add that table to the publication and
/// discharge its backfill without waiting for a restart.
pub async fn all_source_tables(pool: &Pool) -> Result<Vec<String>, CatalogError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select distinct source_table from transform_definitions",
            &[],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// The current version of `source_table`, or `None` if no definition has
/// ever been created against it.
pub async fn source_table_version(
    pool: &Pool,
    source_table: &str,
) -> Result<Option<i64>, CatalogError> {
    let client = pool.get().await?;
    let row = client
        .query_opt(
            "select version from source_table_versions where source_table = $1",
            &[&source_table],
        )
        .await?;
    Ok(row.map(|row| row.get(0)))
}
