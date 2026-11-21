//! The drain's check for a `NULL` in a key column the table's recorded key
//! renders raw (#2217).
//!
//! A raw key part is the column's text, so a `NULL` in it renders as an
//! empty part: the key of a real `''`, or no key at all. Postgres admits one
//! where Trellis read the column as `NOT NULL` only through a `NOT NULL`
//! that doesn't hold (Postgres 18 can validate one over a `NOT VALID`,
//! `NULL`-holding partition), or after a `DROP NOT NULL` on a key a record
//! from before #2147's encoding renders raw. Capture never raises (ADR-0002
//! I6): it stages the row, whose image holds the column's JSON `null`.
//!
//! [`pause_readers_of_null_keys`] runs before a drain page computes
//! anything, beside the check of a redefined primary key, and pauses every
//! reader of a table whose page holds such a row. It reads the record only
//! when a record's key has an empty part, so a page whose keys have none
//! costs nothing more. Before it pauses, it reads the column's `NOT NULL`
//! again ([`crate::defs::recorded_keys::raw_now_nullable`]): a column that
//! can hold `NULL` now took a `DROP NOT NULL`, and the reason is that
//! ([`PauseReason::RawKeyNowNullable`], or [`PauseReason::UnbackedKey`]
//! when no unique index backs the key any more), never
//! [`PauseReason::NullInNotNullKey`].

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::apply::ApplyError;
use super::fold::FoldedChange;
use super::pause_message::PauseReason;
use crate::defs::ddl::COMPOSITE_KEY_SEPARATOR;
use crate::pool::Pool;

/// Two key separators in a row: an empty part inside a composite key.
const DOUBLE_SEPARATOR: &str = "\u{1f}\u{1f}";

/// Whether `key` (a ring key) can hold an empty part: the
/// whole key empty at arity 1, else a separator at either end or two in a
/// row. Escaping only changes a part that isn't empty, so every key with an
/// empty part passes; some keys without one pass too, and the exact check
/// ([`raw_null_columns`]) tells them apart.
fn may_hold_an_empty_part(key: &str) -> bool {
    key.is_empty()
        || key.starts_with(COMPOSITE_KEY_SEPARATOR)
        || key.ends_with(COMPOSITE_KEY_SEPARATOR)
        || key.contains(DOUBLE_SEPARATOR)
}

/// One table's record: its key columns and the ones it renders raw.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Record {
    key: Vec<String>,
    raw: Vec<String>,
}

/// Whether `image` (a JSON object, as the ring stores it) holds `null` at
/// `column`.
fn null_in(image: &serde_json::Value, column: &str) -> bool {
    image.get(column).is_some_and(serde_json::Value::is_null)
}

/// The raw key columns of `record` that hold a JSON `null` in one of
/// `change`'s images: its folded images, its prior image and each unfolded
/// row's (`FoldedChange::group_sides`), since a `NULL` a later write of the
/// same key replaced folds out of the endpoints.
fn raw_null_columns(record: &Record, change: &FoldedChange) -> BTreeSet<String> {
    let mut images: Vec<serde_json::Value> =
        [&change.old_image, &change.new_image, &change.prior_image]
            .into_iter()
            .flatten()
            .filter_map(|text| serde_json::from_str(text).ok())
            .collect();
    for side in &change.group_sides {
        if let Ok(side) = serde_json::from_str::<serde_json::Value>(side) {
            for at in ["o", "n"] {
                if let Some(image) = side.get(at).filter(|i| i.is_object()) {
                    images.push(image.clone());
                }
            }
        }
    }
    record
        .raw
        .iter()
        .filter(|column| images.iter().any(|image| null_in(image, column)))
        .cloned()
        .collect()
}

/// The raw key columns holding `NULL` in `folded`, by table, given each
/// candidate table's record.
fn observed(
    records: &HashMap<String, Record>,
    folded: &[&FoldedChange],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for change in folded {
        let Some(record) = records.get(&change.src_table) else {
            continue;
        };
        if record.raw.is_empty() {
            continue;
        }
        let columns = raw_null_columns(record, change);
        if !columns.is_empty() {
            found
                .entry(change.src_table.clone())
                .or_default()
                .extend(columns);
        }
    }
    found
}

/// Pauses every reader of each table a record of `folded` holds a `NULL`
/// in a raw key column of (see the module doc), before the page computes
/// anything. A no-op, with no query, when no record's key can hold an empty
/// part.
pub(crate) async fn pause_readers_of_null_keys(
    pool: &Pool,
    folded: &[FoldedChange],
) -> Result<(), ApplyError> {
    let candidates: Vec<&FoldedChange> = folded
        .iter()
        .filter(|c| may_hold_an_empty_part(&c.key))
        .collect();
    if candidates.is_empty() {
        return Ok(());
    }
    let tables: Vec<String> = candidates
        .iter()
        .map(|c| c.src_table.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut client = pool.get().await?;
    let records: HashMap<String, Record> = client
        .query(
            "select table_name, key_columns, \
                    array(select c from unnest(key_columns) c \
                          where not c = any(encoded_columns)) \
             from definition_keys where table_name = any($1)",
            &[&tables],
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                Record {
                    key: row.get(1),
                    raw: row.get(2),
                },
            )
        })
        .collect();
    let found = observed(&records, &candidates);
    if found.is_empty() {
        return Ok(());
    }
    let catalog = crate::capture::columns::load_catalog_for(&**client, pool.floors()).await?;
    let mut pauses: Vec<(i64, PauseReason)> = Vec::new();
    for (table, columns) in found {
        let reason = reason(&**client, &table, &records[&table], columns).await?;
        crate::instance_log::warn!(table = %table, "a NULL in a key column Trellis renders raw: {reason}");
        for id in super::schema_change::unpaused_readers(&catalog, &table) {
            pauses.push((id, reason.clone()));
        }
    }
    if pauses.is_empty() {
        return Ok(());
    }
    let txn = client.transaction().await?;
    for (id, reason) in crate::defs::lifecycle::pause_many(&txn, pauses).await? {
        crate::instance_log::warn!(transform_id = id, table = %reason.table(), "definition paused: {reason}");
    }
    txn.commit().await?;
    Ok(())
}

/// Why `table`'s readers pause for a `NULL` observed in its raw key
/// `columns`, from its `NOT NULL` read again now: a column that can hold
/// `NULL` now took a `DROP NOT NULL` (T0254, or an unbacked `KEY` when no
/// unique index backs the key any more); one Postgres still reads as `NOT
/// NULL` holds a `NULL` it shouldn't (T0253).
async fn reason(
    client: &impl tokio_postgres::GenericClient,
    table: &str,
    record: &Record,
    columns: BTreeSet<String>,
) -> Result<PauseReason, ApplyError> {
    let nullable = crate::defs::recorded_keys::raw_now_nullable(client, table).await?;
    if nullable.is_empty() {
        return Ok(PauseReason::NullInNotNullKey {
            table: table.to_string(),
            columns: columns.into_iter().collect(),
        });
    }
    if crate::defs::source_key::backing(client, table, &record.key)
        .await?
        .is_none()
    {
        let partitioned = crate::defs::partition_tree::is_partitioned_root(client, table).await?;
        return Ok(PauseReason::UnbackedKey {
            table: table.to_string(),
            key: record.key.clone(),
            partitioned,
            nullable: true,
        });
    }
    Ok(PauseReason::RawKeyNowNullable {
        table: table.to_string(),
        columns: nullable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn change(table: &str, key: &str, old: Option<&str>, new: Option<&str>) -> FoldedChange {
        FoldedChange {
            src_table: table.to_string(),
            key: key.to_string(),
            new_image: new.map(str::to_string),
            old_image: old.map(str::to_string),
            src_changed: None,
            origin_lsn: None,
            lsn: None,
            hop_gen: 0,
            first_seen: std::time::SystemTime::UNIX_EPOCH,
            is_truncate: false,
            prior_image: None,
            row_count: 1,
            has_recompute: false,
            ends_in_delete: false,
            last_change: None,
            group_sides: Vec::new(),
        }
    }

    /// A key with an empty part always passes the cheap filter, at every
    /// arity and position; a key whose parts are all non-empty passes only
    /// at arity ≥ 2 by chance of an escape, never at arity 1.
    #[test]
    fn every_key_with_an_empty_part_is_a_candidate() {
        let sep = COMPOSITE_KEY_SEPARATOR;
        for key in [
            String::new(),
            format!("{sep}b"),
            format!("a{sep}"),
            format!("a{sep}{sep}c"),
            format!("{sep}"),
        ] {
            assert!(may_hold_an_empty_part(&key), "{key:?}");
        }
        for key in ["a", "0", "\u{1}", &format!("a{sep}b")] {
            assert!(!may_hold_an_empty_part(key), "{key:?}");
        }
    }

    /// The exact check: a JSON `null` in a raw key column of any image,
    /// including one a later write of the same key folded out of the
    /// endpoints; never a `NULL` in an encoded or non-key column, nor an
    /// image that lacks the column.
    #[test]
    fn a_null_in_a_raw_key_column_of_any_image_is_observed() {
        let records = HashMap::from([
            (
                "public.t".to_string(),
                Record {
                    key: vec!["k".to_string()],
                    raw: vec!["k".to_string()],
                },
            ),
            (
                "public.e".to_string(),
                Record {
                    key: vec!["k".to_string()],
                    raw: Vec::new(),
                },
            ),
        ]);
        let null_new = change("public.t", "", None, Some(r#"{"k": null, "v": 1}"#));
        let null_old = change("public.t", "", Some(r#"{"k": null, "v": 1}"#), None);
        let real_empty = change("public.t", "", None, Some(r#"{"k": "", "v": null}"#));
        let encoded = change("public.e", "", None, Some(r#"{"k": null}"#));
        let lacks = change("public.t", "", None, Some(r#"{"v": 1}"#));
        let mut folded_out = change("public.t", "", None, Some(r#"{"k": "", "v": 2}"#));
        folded_out.group_sides = vec![
            r#"{"t": "1", "l": "0/1", "op": "insert", "o": null, "n": {"k": null, "v": 1}}"#
                .to_string(),
        ];
        let expect = BTreeMap::from([("public.t".to_string(), BTreeSet::from(["k".to_string()]))]);
        for hit in [&null_new, &null_old, &folded_out] {
            assert_eq!(observed(&records, &[hit]), expect, "{hit:?}");
        }
        for miss in [&real_empty, &encoded, &lacks] {
            assert!(observed(&records, &[miss]).is_empty(), "{miss:?}");
        }
    }
}
