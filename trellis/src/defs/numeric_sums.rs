//! The numeric partials of a `SUM` or `AVG` (`crate::staging::group_basis`'s
//! "Numeric sums") for a definition a release before them stored.
//!
//! Such a release admitted a `numeric` sum only over a plain `numeric(p,s)`
//! column, and kept its sum alone: a hidden table with `__sum_<i>` and no
//! map of exceptions. Every value of the column has scale `s`, so a finite
//! stored sum with a count above 0 has scale `s` too: with `s` as its
//! reference scale, its map is empty. A `NaN` it summed made the stored sum
//! `NaN` for good, which no count recovers.
//!
//! At attach ([`super::grammar::pause_unloadable_in`]) each hidden table that
//! lacks the partials of a `numeric` argument gains them in place
//! ([`convert_table`]): the map column, `NULL` in every row, recording as its
//! reference scale the one the column declares now, and the zero flag and its
//! index over the new column. Only a row whose stored sum has another scale
//! (or every row, for a column that declares none now) is written: its map
//! counts its values at that scale. The definition's floor is stamped again, now that its tables have
//! a layout an older release can't read (ADR-0024). A definition with a group
//! whose stored sum is `NaN` is rebuilt in place, which recomputes every
//! group from its source; one that doesn't apply (paused) rebuilds on its
//! resume.

use crate::defs::ast::KeySpace;
use crate::defs::floor::Floors;
use crate::staging::group_basis::{self, NUMERIC_PARTS, SumPart};

/// An error of the conversion, as the attach reports it.
fn failed(e: impl std::fmt::Display) -> crate::Error {
    crate::Error::IncompatibleInstance(format!("converting numeric sums: {e}"))
}

/// Converts every stored definition's hidden tables, in `txn` (see the module
/// doc). Returns how many definitions it converted.
pub(crate) async fn convert_in(
    txn: &tokio_postgres::Transaction<'_>,
    floors: &Floors,
) -> Result<usize, crate::Error> {
    let schema: String = txn
        .query_one(
            "select n.nspname::text from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where c.oid = 'transform_definitions'::regclass",
            &[],
        )
        .await?
        .get(0);
    let ids: Vec<i64> = txn
        .query("select id from transform_definitions order by id", &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let (mut converted, mut poisoned) = (0, Vec::new());
    for id in ids {
        let Some(definition) = super::catalog::definition_by_id_in(txn, floors, id)
            .await
            .map_err(failed)?
        else {
            continue;
        };
        let mut found = None;
        for table in numeric_tables(txn, &schema, &definition).await? {
            if let Some(nan) = convert_table(txn, &table).await? {
                found = Some(found.unwrap_or(false) || nan);
            }
        }
        let Some(nan) = found else { continue };
        super::floor::stamp(txn, floors, id, &definition.def)
            .await
            .map_err(failed)?;
        converted += 1;
        if nan {
            poisoned.push(id);
        }
    }
    if !poisoned.is_empty() {
        txn.execute(
            "select 1 from transform_definitions where id = any($1) order by id for update",
            &[&poisoned],
        )
        .await?;
        let rebuilt = crate::staging::build::rebuildable(txn, &poisoned).await?;
        crate::staging::build::start_rebuilds(txn, &rebuilt).await?;
        crate::staging::build::stamp_rebuild_seg(txn, &rebuilt).await?;
    }
    Ok(converted)
}

/// A hidden table of a definition, with what its partials should be.
pub(crate) struct NumericTable {
    /// The table, quoted and qualified.
    pub(crate) table: String,
    /// Per aggregate argument (in order): whether its partials are a
    /// `numeric` argument's ([`NUMERIC_PARTS`]), and whether a `SUM` or `AVG`
    /// reads it, so its count is its finite values' count.
    pub(crate) args: Vec<(bool, bool)>,
    /// Per aggregate argument (in order): its reference scale, the one its
    /// column declares now (`group_basis::source_scales`).
    pub(crate) scales: Vec<Option<i32>>,
    /// Its zero flag's column definition over every partial.
    pub(crate) zero: String,
}

/// The hidden tables of `definition` whose partials include a `numeric`
/// argument's: an aggregate's group table, a `JOIN`'s `P`, `M` and `T`.
async fn numeric_tables(
    txn: &tokio_postgres::Transaction<'_>,
    schema: &str,
    definition: &crate::defs::model::Definition,
) -> Result<Vec<NumericTable>, crate::Error> {
    let def = &definition.def;
    let tables = if crate::staging::join::applies(def) {
        let Some(joined) = super::catalog::join_table_of(txn, definition.id)
            .await
            .map_err(failed)?
        else {
            return Ok(Vec::new());
        };
        let sides = super::catalog::JoinSides::read(txn, definition.source_table.clone(), vec![joined])
            .await
            .map_err(failed)?;
        let numeric = |side| {
            super::join::numeric_arguments(def, side, &sides.from_columns, &sides.first_joined().columns)
        };
        let reference = |side| {
            super::join::reference_scales(def, side, &sides.from_columns, &sides.first_joined().columns)
        };
        let (from_scales, joined_scales) = (
            reference(super::join::Side::From),
            reference(super::join::JOINED),
        );
        let summed = crate::staging::join::plan_for(txn, schema, definition)
            .await
            .map_err(failed)?
            .summed_arguments();
        let (from, joined) = (
            numeric(super::join::Side::From),
            numeric(super::join::JOINED),
        );
        let fa = from.len();
        let pair = |numeric: &[bool], summed: &[bool]| -> Vec<(bool, bool)> {
            numeric
                .iter()
                .copied()
                .zip(summed.iter().copied())
                .collect()
        };
        let all = [from.clone(), joined.clone()].concat();
        vec![
            NumericTable {
                table: super::join::HiddenTable::FromPartials.qualified(schema, definition.id),
                args: pair(&from, &summed[..fa]),
                scales: from_scales.clone(),
                zero: super::join::zero_column(&from),
            },
            NumericTable {
                table: super::join::HiddenTable::JoinedPartials.qualified(schema, definition.id),
                args: pair(&joined, &summed[fa..]),
                scales: joined_scales.clone(),
                zero: super::join::zero_column(&joined),
            },
            NumericTable {
                table: super::join::HiddenTable::TargetPartials.qualified(schema, definition.id),
                args: pair(&all, &summed),
                scales: [from_scales, joined_scales].concat(),
                zero: super::join::zero_column(&all),
            },
        ]
    } else if matches!(def.key_space, KeySpace::Aggregate { .. }) {
        let Some(mut shape) = group_basis::route(def, &definition.source_columns) else {
            return Ok(Vec::new());
        };
        shape.declare_scales(txn, &definition.source_table).await?;
        vec![NumericTable {
            table: group_basis::hidden_table_ident(schema, definition.id),
            args: shape
                .arguments()
                .iter()
                .map(|arg| {
                    let numeric = arg.parts() == NUMERIC_PARTS;
                    (numeric, numeric)
                })
                .collect(),
            scales: shape
                .arguments()
                .iter()
                .map(|arg| arg.reference_scale())
                .collect(),
            zero: group_basis::hidden_zero_column(&shape),
        }]
    } else {
        Vec::new()
    };
    Ok(tables
        .into_iter()
        .filter(|t| t.args.iter().any(|(numeric, _)| *numeric))
        .collect())
}

/// Gives `table` the partials of each `numeric` argument it lacks (see the
/// module doc), in place. Returns `None` when it lacked none (or doesn't
/// exist), and otherwise whether a row's stored sum of one of them is `NaN`.
pub(crate) async fn convert_table(
    txn: &tokio_postgres::Transaction<'_>,
    table: &NumericTable,
) -> Result<Option<bool>, crate::Error> {
    let columns: Vec<String> = txn
        .query(
            "select attname::text from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attnum > 0 and not attisdropped",
            &[&table.table],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let lacking: Vec<(usize, bool)> = table
        .args
        .iter()
        .enumerate()
        .filter(|(_, (numeric, _))| *numeric)
        .map(|(i, (_, summed))| (i + 1, *summed))
        .filter(|(i, _)| {
            columns.contains(&SumPart::Sum.hidden(*i))
                && !columns.contains(&SumPart::Scales.hidden(*i))
        })
        .collect();
    if lacking.is_empty() {
        return Ok(None);
    }
    let t = &table.table;
    let nan: Vec<String> = lacking
        .iter()
        .map(|(i, _)| format!("{} = 'NaN'::numeric", SumPart::Sum.hidden(*i)))
        .collect();
    let nan: bool = txn
        .query_one(
            &format!(
                "select exists (select 1 from {t} where {})",
                nan.join(" or ")
            ),
            &[],
        )
        .await?
        .get(0);
    // The zero-group index, which goes with the zero flag it reads.
    let index: Option<String> = txn
        .query_opt(
            "select pg_catalog.pg_get_indexdef(i.indexrelid) from pg_catalog.pg_index i \
             where i.indrelid = pg_catalog.to_regclass($1) and i.indpred is not null \
               and pg_catalog.pg_get_expr(i.indpred, i.indrelid) = '__zero'",
            &[&table.table],
        )
        .await?
        .map(|row| row.get(0));
    let mut alter = vec!["drop column __zero".to_string()];
    let (mut sets, mut written, mut comments) = (Vec::new(), Vec::new(), Vec::new());
    for &(i, summed) in &lacking {
        for part in &NUMERIC_PARTS[1..] {
            alter.push(format!("add column {}", part.ddl(i)));
        }
        let scale = table.scales.get(i - 1).copied().flatten();
        comments.push((i, scale));
        if summed {
            // A row whose finite values aren't at the reference scale: its
            // map counts them at its stored sum's.
            let (count, sum) = (format!("__count_{i}"), SumPart::Sum.hidden(i));
            let off = match scale {
                Some(s) => {
                    format!("{count} > 0 and {sum} <> 'NaN'::numeric and scale({sum}) <> {s}")
                }
                None => format!("{count} > 0 and {sum} <> 'NaN'::numeric"),
            };
            sets.push(format!(
                "{} = case when {off} then jsonb_build_object(scale({sum})::text, {count}) end",
                SumPart::Scales.hidden(i)
            ));
            written.push(format!("({off})"));
        }
    }
    txn.batch_execute(&format!(
        "alter table {t} {};\n{}",
        alter.join(", "),
        group_basis::scale_comments_sql(t, &comments)
    ))
    .await?;
    if !sets.is_empty() {
        txn.execute(
            &format!(
                "update {t} set {} where {}",
                sets.join(", "),
                written.join(" or ")
            ),
            &[],
        )
        .await?;
    }
    txn.batch_execute(&format!("alter table {t} add column {}", table.zero))
        .await?;
    if let Some(index) = index {
        txn.batch_execute(&index).await?;
    }
    Ok(Some(nan))
}
