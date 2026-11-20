//! String literals Postgres types from their neighbour (#2000).
//!
//! Postgres types a string literal that is an operand of `=`, `<>` or
//! `IS [NOT] DISTINCT FROM`, an argument of `COALESCE` or `NULLIF`, or a
//! `CASE` result from the values beside it. Both renderers leave such a
//! literal bare (`super::oracle`, and `self_check`'s audit), so Postgres
//! types it as it types the user's text. A release before 0.4.0 cast it to
//! `text`, which reads differently beside two types: `char(n)`, whose `=`
//! ignores trailing blanks and whose values lose them as `text`, and
//! `name`, which truncates a literal over 63 bytes. An aggregate's field
//! reads a `char(n)` or `name` `GROUP BY` key, kept as `text`, as its source
//! type too (#2053, `crate::staging::group_basis::GroupShape::key_reads`),
//! which changes the same fields. [`affected`] finds a definition that
//! differs, over its tables' live column types.
//!
//! Such a definition carries [`super::floor::Feature::NeighbourTypedLiteral`],
//! so a release before it pauses it on rollback. At attach
//! ([`super::grammar::pause_unloadable_in`]) [`rebuild_in`] rebuilds each
//! one an earlier release stamped (its floor is below the feature's), and
//! stamps it again, so it runs once per definition.

use std::collections::{HashMap, HashSet};

use super::ast::{Expr, FieldDef, KeySpace, TransformDef};
use super::floor::{Feature, Floors, Release};
use super::pg_type::oid;
use super::validate::{
    JoinColumnInfo, ReadColumn, aggregate_field_names, collect_value_columns,
    without_aggregate_fields,
};

/// The longest `name` Postgres keeps (`NAMEDATALEN - 1`); a longer literal
/// typed `name` is truncated.
const NAME_BYTES: usize = 63;

/// Whether `def` has a string literal Postgres types from a neighbour whose
/// value can be a `char(n)`, or a `name` with the literal over 63 bytes,
/// over the live columns of its `FROM` table (`from`) and, for a `JOIN`, of
/// each joined table (`joined`, by position). A neighbour's values are
/// followed as `self_check`'s collation check follows them: through another
/// field, `COALESCE`, `NULLIF`'s first argument and `CASE` results. Domains
/// need no look through: the validator refuses a column of one.
pub(crate) fn affected(
    def: &TransformDef,
    from: &HashMap<String, JoinColumnInfo>,
    joined: Option<&[&HashMap<String, JoinColumnInfo>]>,
) -> bool {
    let join = match &def.key_space {
        KeySpace::Join(join) | KeySpace::JoinOneToOne(join) => Some(join),
        _ => None,
    };
    let resolve = |(qualifier, column): &ReadColumn| -> Option<&JoinColumnInfo> {
        match (qualifier, join, joined) {
            (Some(qualifier), Some(join), Some(joined)) => {
                super::validate::join_resolver(def, join, from, joined)(qualifier)
                    .ok()
                    .map(|(_, info)| info)
            }
            _ => from.get(column),
        }
    };
    let fields: HashMap<&str, &FieldDef> =
        def.fields.iter().map(|f| (f.name.as_str(), f)).collect();
    def.fields.iter().any(|field| {
        let mut found = false;
        walk(&field.expr, &fields, &mut |literal, beside, fields| {
            found = found || reads_differently(literal, beside, field, fields, &resolve);
        });
        found
    })
}

/// Whether `literal`, typed from `beside`, reads differently from `text`:
/// a value of `beside` can be a `char(n)`, or a `name` and the literal is
/// over 63 bytes.
fn reads_differently<'a>(
    literal: &str,
    beside: &[&Expr],
    field: &FieldDef,
    fields: &HashMap<&str, &FieldDef>,
    resolve: &impl Fn(&ReadColumn) -> Option<&'a JoinColumnInfo>,
) -> bool {
    let mut columns = Vec::new();
    let mut visited = HashSet::new();
    for expr in beside {
        collect_value_columns(expr, &field.name, fields, &mut visited, &mut columns);
    }
    columns
        .iter()
        .filter_map(resolve)
        .any(|info| match info.type_oid {
            oid::BPCHAR => true,
            oid::NAME => literal.len() > NAME_BYTES,
            _ => false,
        })
}

/// Calls `found` with each string literal under `expr` that Postgres types
/// from its neighbours, those neighbours, and the fields a name among them
/// can read: an aggregate's argument reads source columns, not aggregate
/// fields.
fn walk<'e>(
    expr: &'e Expr,
    fields: &HashMap<&str, &FieldDef>,
    found: &mut impl FnMut(&str, &[&'e Expr], &HashMap<&str, &FieldDef>),
) {
    let scoped;
    let fields = match expr {
        Expr::FunctionCall { name, .. } if super::registry::is_aggregate_name(name) => {
            let aggregates = aggregate_field_names(fields.iter().map(|(n, f)| (*n, &f.expr)));
            scoped = without_aggregate_fields(fields, &aggregates);
            &scoped
        }
        _ => fields,
    };
    let mut each = |operands: Vec<&'e Expr>| {
        for (i, operand) in operands.iter().enumerate() {
            if let Expr::StringLiteral(text) = operand {
                let others: Vec<&Expr> = operands
                    .iter()
                    .enumerate()
                    .filter(|&(j, _)| j != i)
                    .map(|(_, other)| *other)
                    .collect();
                found(text, &others, fields);
            }
        }
    };
    match expr {
        Expr::BinaryOp { op, lhs, rhs } if op.is_comparison() && !op.is_ordering() => {
            each(vec![lhs, rhs]);
        }
        Expr::InList {
            expr: operand,
            list,
            ..
        } => {
            for element in list {
                each(vec![operand, element]);
            }
        }
        Expr::FunctionCall { name, args } if name == "COALESCE" || name == "NULLIF" => {
            each(args.iter().collect());
        }
        Expr::Case {
            branches,
            otherwise,
        } => each(
            branches
                .iter()
                .map(|(_, result)| result)
                .chain(otherwise.as_deref())
                .collect(),
        ),
        _ => {}
    }
    for child in expr.children() {
        walk(child, fields, found);
    }
}

/// The release that renders such a literal bare.
fn since() -> Release {
    Feature::NeighbourTypedLiteral
        .since()
        .expect("declares its release")
}

/// Rebuilds each stored definition [`affected`] whose floor is below
/// [`Feature::NeighbourTypedLiteral`]'s, in `txn` (see the module doc), and
/// stamps its floor again. One that applies (`live`, or under a build) is
/// rebuilt in place (`crate::staging::build::start_rebuilds`); a frozen one
/// is rebuilt by its resume, and one waiting for its first build by that
/// build. A definition this binary can't run or read is left to the
/// attach's own pause. Nothing runs under a binary older than the feature,
/// which would stamp the same floor again. Returns how many it rebuilt or
/// stamped.
pub(crate) async fn rebuild_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
) -> Result<usize, crate::Error> {
    if floors.above_running(&since().to_string()).is_some() {
        return Ok(0);
    }
    let rows = txn
        .query(
            &format!(
                "select d.id, d.floor_version, d.source_table, {} \
                 from transform_definitions d order by d.id",
                super::catalog::joined_tables_array_sql("d")
            ),
            &[],
        )
        .await?;
    let mut found = Vec::new();
    for row in &rows {
        let (id, floor): (i64, &str) = (row.get(0), row.get(1));
        if Release::parse(floor).is_none_or(|floor| floor >= since()) {
            continue;
        }
        let definition = match super::catalog::definition_by_id_in(txn, floors, id).await {
            Ok(Some(definition)) => definition,
            Ok(None) | Err(super::catalog::CatalogError::Parse(_)) => continue,
            Err(e) => return Err(failed(e)),
        };
        let (source, joined): (String, Vec<String>) = (row.get(2), row.get(3));
        let from = super::live_table::LiveTable::read(txn, &source)
            .await?
            .typed();
        let mut joined_maps = Vec::with_capacity(joined.len());
        for table in &joined {
            joined_maps.push(
                super::live_table::LiveTable::read(txn, table)
                    .await?
                    .typed(),
            );
        }
        let joined_refs: Vec<_> = joined_maps.iter().collect();
        let joined_columns = (!joined_refs.is_empty()).then_some(joined_refs.as_slice());
        if affected(&definition.def, &from, joined_columns) {
            found.push(definition);
        }
    }
    if found.is_empty() {
        return Ok(0);
    }
    let ids: Vec<i64> = found.iter().map(|d| d.id).collect();
    txn.execute(
        "select 1 from transform_definitions where id = any($1) order by id for update",
        &[&ids],
    )
    .await?;
    let rebuilt = crate::staging::build::rebuildable(txn, &ids).await?;
    let started = crate::staging::build::start_rebuilds(txn, rebuilt.clone()).await?;
    for definition in &found {
        super::floor::stamp(txn, floors, definition.id, &definition.def)
            .await
            .map_err(failed)?;
    }
    started.stamp(txn).await?;
    crate::instance_log::info!(
        ids = ?ids,
        rebuilt = ?rebuilt,
        "definitions reading a string literal beside a char(n) or name value \
         as a release before 0.4.0 did; rebuilding the ones that apply"
    );
    Ok(found.len())
}

/// An error of the rebuild, as the attach reports it.
fn failed(e: impl crate::error_code::Message) -> crate::Error {
    crate::Error::IncompatibleInstance(format!(
        "rebuilding definitions with a neighbour-typed string literal: {}",
        e.message()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(type_oid: u32) -> JoinColumnInfo {
        JoinColumnInfo {
            value_type: super::super::ast::ValueType::Text,
            pg_type: String::new(),
            type_oid,
            typmod: -1,
            collation_oid: 100,
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            enum_labels: None,
            copy_type: String::new(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    /// `ch character(4)`, `nm name`, `t text`, `v varchar`.
    fn columns() -> HashMap<String, JoinColumnInfo> {
        [
            ("id", column(oid::INT8)),
            ("ch", column(oid::BPCHAR)),
            ("nm", column(oid::NAME)),
            ("t", column(oid::TEXT)),
            ("v", column(oid::VARCHAR)),
        ]
        .into_iter()
        .map(|(name, info)| (name.to_string(), info))
        .collect()
    }

    fn affected_text(text: &str) -> bool {
        let def = crate::defs::parse(text).expect("parses");
        affected(&def, &columns(), None)
    }

    /// A literal in a neighbour-typed position beside a `char(n)` value, or
    /// a long one beside a `name`, through fields, `COALESCE`, `NULLIF` and
    /// `CASE`, and inside an aggregate's argument.
    #[test]
    fn a_literal_beside_a_char_or_a_long_name_is_affected() {
        let long = "x".repeat(64);
        for text in [
            "TRANSFORM t FROM s SELECT ch = 'ab' AS f".to_string(),
            "TRANSFORM t FROM s SELECT 'ab' <> ch AS f".to_string(),
            "TRANSFORM t FROM s SELECT ch IS NOT DISTINCT FROM 'ab' AS f".to_string(),
            "TRANSFORM t FROM s SELECT NULLIF(ch, 'ab') AS f".to_string(),
            "TRANSFORM t FROM s SELECT COALESCE(ch, 'ab') AS f".to_string(),
            "TRANSFORM t FROM s SELECT CASE WHEN id = 1 THEN 'x' ELSE ch END AS f".to_string(),
            "TRANSFORM t FROM s SELECT COALESCE(t, ch) = 'ab' AS f".to_string(),
            "TRANSFORM t FROM s SELECT ch AS c, c = 'ab' AS f".to_string(),
            "TRANSFORM t FROM s SELECT s.ch = 'ab' AS f".to_string(),
            format!("TRANSFORM t FROM s SELECT nm = '{long}' AS f"),
            "TRANSFORM g FROM s GROUP BY t SELECT COUNT(CASE WHEN ch = 'a' THEN 1 END) AS n"
                .to_string(),
            "TRANSFORM g FROM s GROUP BY ch SELECT ch = 'ab' AS f, COUNT(*) AS n".to_string(),
            format!("TRANSFORM g FROM s GROUP BY nm SELECT NULLIF(nm, '{long}') AS f"),
        ] {
            assert!(affected_text(&text), "{text}");
        }
    }

    /// A literal beside `text`, `varchar` or a short `name`, a literal whose
    /// position Postgres types without its neighbour, and a neighbour whose
    /// own value is computed (`CHAR_LENGTH`), aren't.
    #[test]
    fn other_literals_are_not_affected() {
        let long = "x".repeat(63);
        for text in [
            "TRANSFORM t FROM s SELECT t = 'ab' AS f, v <> 'ab' AS g".to_string(),
            "TRANSFORM t FROM s SELECT COALESCE(t, v, 'ab') AS f".to_string(),
            format!("TRANSFORM t FROM s SELECT nm = '{long}' AS f"),
            "TRANSFORM t FROM s SELECT STRPOS(ch, 'a') AS f".to_string(),
            "TRANSFORM t FROM s SELECT CHAR_LENGTH(ch) = 2 AS f".to_string(),
            "TRANSFORM t FROM s SELECT ch IS NULL AS f".to_string(),
        ] {
            assert!(!affected_text(&text), "{text}");
        }
    }
}
