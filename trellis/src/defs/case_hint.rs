//! The case hint on a missed reference (#1508): an unquoted name folds to
//! lower case, so a name written `OrderTotals` finds nothing when only
//! `"OrderTotals"` exists. When a folded name finds nothing but a name that
//! differs from it only in case exists, the refusal names that one and its
//! quoted spelling ([`super::catalog::CatalogError::NameCaseMismatch`]).
//! An addressing statement never acts on another name than the exact one it
//! wrote: a miss with such a match refuses rather than succeeding as a drop
//! of nothing.

use tokio_postgres::GenericClient;

use super::ast::{KeySpace, TransformDef, quote_name};
use super::catalog::target_match_sql;

/// `schema.table` (or `table`) as a definition writes it, each part quoted
/// where it needs to be.
fn spelled(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_name(schema), quote_name(name)),
        None => quote_name(name).into_owned(),
    }
}

/// The spelling of a registered transform whose name matches `name` (bare
/// or `<schema>.<table>`, as an address writes it) in a case other than
/// `name`'s, when no transform is `name` exactly.
pub(crate) async fn transform(
    client: &impl GenericClient,
    name: &str,
) -> Result<Option<String>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            &format!(
                "select target_table from transform_definitions \
                 where not exists (select 1 from transform_definitions where {}) \
                   and case when position('.' in $1::text) > 0 \
                            then lower(target_table) = lower($1::text) \
                            else lower(split_part(target_table, '.', 2)) = lower($1::text) end \
                 order by target_table limit 1",
                target_match_sql("target_table", 1)
            ),
            &[&name],
        )
        .await?;
    Ok(row.map(|row| {
        let target: String = row.get(0);
        let (schema, table) = target.split_once('.').unwrap_or(("", &target));
        if name.contains('.') {
            spelled(Some(schema), table)
        } else {
            spelled(None, table)
        }
    }))
}

/// The spelling of a table matching `table` in another case: in `schema`
/// when one is given (itself matched in any case), else in one of
/// `schemas`. Only when no table is `schema.table` exactly.
pub(crate) async fn table(
    client: &impl GenericClient,
    schemas: &[String],
    schema: Option<&str>,
    table: &str,
) -> Result<Option<String>, tokio_postgres::Error> {
    let row = client
        .query_opt(
            "select n.nspname, c.relname from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             where c.relkind in ('r', 'p', 'v', 'm', 'f') \
               and lower(c.relname) = lower($3) \
               and case when $2::text is null then n.nspname = any($1) \
                        else lower(n.nspname) = lower($2::text) end \
               and (c.relname <> $3 or n.nspname <> coalesce($2::text, n.nspname)) \
             order by array_position($1, n.nspname::text), n.nspname, c.relname limit 1",
            &[&schemas, &schema, &table],
        )
        .await?;
    Ok(row.map(|row| {
        let (found_schema, found): (String, String) = (row.get(0), row.get(1));
        spelled(schema.map(|_| found_schema.as_str()), &found)
    }))
}

/// The spelling of a column matching `column` in another case on one of
/// the tables `def` reads (`FROM`, then `JOIN`), each resolved as the
/// definition names it: in its schema, or in the first of `schemas` that
/// holds it.
pub(crate) async fn column(
    client: &impl GenericClient,
    schemas: &[String],
    def: &TransformDef,
    column: &str,
) -> Result<Option<String>, tokio_postgres::Error> {
    let mut tables = vec![(def.explicit_source_schema.as_deref(), def.source.as_str())];
    if let KeySpace::Join(join) | KeySpace::JoinOneToOne(join) = &def.key_space {
        for joined in &join.joins {
            tables.push((
                joined.explicit_right_schema.as_deref(),
                joined.right.as_str(),
            ));
        }
    }
    for (schema, table) in tables {
        let row = client
            .query_opt(
                "select a.attname from pg_attribute a \
                 join pg_class c on c.oid = a.attrelid \
                 join pg_namespace n on n.oid = c.relnamespace \
                 where c.relname = $3 \
                   and case when $2::text is null then n.nspname = any($1) \
                            else n.nspname = $2::text end \
                   and a.attnum > 0 and not a.attisdropped \
                   and lower(a.attname) = lower($4) and a.attname <> $4 \
                 order by array_position($1, n.nspname::text), a.attname limit 1",
                &[&schemas, &schema, &table, &column],
            )
            .await?;
        if let Some(row) = row {
            return Ok(Some(quote_name(&row.get::<_, String>(0)).into_owned()));
        }
    }
    Ok(None)
}
