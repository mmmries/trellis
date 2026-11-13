//! A batch of keys matched through the key's index: one arm per pattern of
//! `NULL` key parts, each binding one `text[]` per non-`NULL` column, so the
//! bind count doesn't grow with the batch. [`Keyset`] splits the keys and
//! [`ArmSql`] spells an arm's relation and match; each statement keeps its
//! own join form, settings and lock order.
//!
//! # One arm per `NULL` pattern
//!
//! Within an arm a `NULL` part is matched with `col is null` and the others
//! with `=`, so every arm probes the key's index. Matching a whole batch with
//! `is not distinct from` on a column one of its keys binds a `NULL` for is
//! correct but can't use the index: on a 500k-group aggregate target, a
//! 500-key batch with one `NULL` group took 8.5 s against 0.4 ms.
//!
//! # The single-column bound
//!
//! A single-column key's non-`NULL` arm also restricts the column to its own
//! array (`t.<col> = any(<array>)`, [`ArmSql::bounded_matches`]), which the
//! match already implies. It caps the table's side of whatever join the
//! planner picks at the batch: a table analyzed while small and grown since
//! is otherwise read in full. For 5,000 keys against a source analyzed at
//! 100 rows:
//!
//! - Neither the bound nor `super::ledger::ENTRY_PLAN_SETTINGS` (no
//!   sequential scan): a hash join over a sequential scan, 100 ms at 1.15M
//!   rows on PostgreSQL 16 and 17 (a target at 1M: 102 ms against 31 ms).
//! - The bound alone: still a sequential scan on PostgreSQL 16, which prices
//!   an index scan for thousands of `= any` values far above 17's estimate
//!   (28 ms at 1.15M rows, 41 ms at 2M). PostgreSQL 17 reads the index:
//!   10–15 ms.
//! - The settings alone: a scan of the key's whole index, 31 ms.
//! - Both: 15–19 ms on PostgreSQL 16, 10–15 ms on 17.
//!
//! So each statement that takes the bound runs under the settings, as does
//! the 1-1 apply's single-column lock and delete (`= any($1)` alone, the
//! simplest form, which PostgreSQL 16 also scanned a 400k-row target for).
//!
//! # A composite key is never bounded
//!
//! The planner multiplies one bound per column's selectivities as if they
//! were independent, but each column of a composite key is nearly as
//! selective as the whole key. It expects a row or two, then compares every
//! bounded row with every key, quadratic in the batch, fresh statistics or
//! not: 5,000 keys took 2.3 s to lock (25 ms unbounded) and 1,474 ms to read
//! (13 ms) at 1M rows with a four-column key, and 1.8 s to lock at 20M rows
//! with a two-column key (19 ms). A leading-column bound alone reads a whole
//! tenant when that column has few values (84 ms against 4 ms for 500 keys
//! over 1,000 tenants).
//!
//! Unbounded, a composite key is probed once per key while its statistics
//! are fresh. While they lag, the join hashed the keys against a sequential
//! scan (133–167 ms for a two-column key at 1M rows analyzed at 100, on
//! PostgreSQL 16 and 17), unless the statement runs under the settings
//! (12–16 ms). A composite key is always the full key of a primary key or
//! unique index (`ddl::identity_key_columns`), so the settings always have an
//! index to probe. The 1-1 apply's composite delete runs without them: its
//! `exists` probed the index once per key for 5,000 keys of a two-column key
//! analyzed at 100 rows and grown to 1M or 3M, and of a four-column one grown
//! to 1M, on PostgreSQL 16, 17 and 18.

use std::collections::BTreeMap;

use tokio_postgres::types::ToSql;

use crate::defs::ddl::PrimaryKeyColumn;
use crate::pool::quote_ident;

/// A batch of keys by `NULL` pattern (`pattern[i]`: part `i` is `NULL`), in
/// pattern order, so the keys with no `NULL` part come first.
pub(super) struct Keyset<K, P> {
    pub(super) arms: BTreeMap<Vec<bool>, Arm<K, P>>,
}

/// The keys of one `NULL` pattern.
pub(super) struct Arm<K, P> {
    pub(super) keys: Vec<K>,
    /// One array per column that isn't `NULL` in the pattern, in key order.
    pub(super) parts: Vec<Vec<P>>,
}

impl<K, P, I: IntoIterator<Item = Option<P>>> FromIterator<(K, I)> for Keyset<K, P> {
    fn from_iter<T: IntoIterator<Item = (K, I)>>(keys: T) -> Self {
        let mut arms: BTreeMap<Vec<bool>, Arm<K, P>> = BTreeMap::new();
        for (key, parts) in keys {
            let parts: Vec<Option<P>> = parts.into_iter().collect();
            let arm = arms
                .entry(parts.iter().map(Option::is_none).collect())
                .or_insert_with_key(|pattern| Arm {
                    keys: Vec::new(),
                    parts: pattern
                        .iter()
                        .filter(|null| !**null)
                        .map(|_| Vec::new())
                        .collect(),
                });
            arm.keys.push(key);
            for (column, part) in arm.parts.iter_mut().zip(parts.into_iter().flatten()) {
                column.push(part);
            }
        }
        Self { arms }
    }
}

impl<'a> Keyset<(), &'a str> {
    /// Decoded keys (`super::apply::decode_target_pk_parts`), a `NULL` part
    /// a `None`.
    pub(super) fn of_parts(keys: impl IntoIterator<Item = &'a Vec<Option<String>>>) -> Self {
        keys.into_iter()
            .map(|key| ((), key.iter().map(Option::as_deref)))
            .collect()
    }
}

impl<K, P: ToSql + Sync> Arm<K, P> {
    /// The parts' arrays, as the arm's [`ArmSql`] numbers them.
    pub(super) fn params(&self) -> impl Iterator<Item = &(dyn ToSql + Sync)> {
        self.parts.iter().map(|part| part as _)
    }
}

/// One arm as SQL: its relation `unnest(<arrays>) as <alias>(<columns>)` and
/// the match of a row to it.
pub(super) struct ArmSql {
    alias: &'static str,
    arrays: Vec<String>,
    columns: Vec<String>,
    /// Each key column's quoted name and its relation column, `None` where
    /// the pattern is `NULL`.
    matched: Vec<(String, Option<String>)>,
}

impl ArmSql {
    /// Key column `i` named `c<i>`, the arrays bound from `$start`.
    pub(super) fn new(
        pk: &[PrimaryKeyColumn],
        pattern: &[bool],
        alias: &'static str,
        start: usize,
    ) -> Self {
        Self::named(pk, pattern, alias, start, |i, _| format!("c{i}"))
    }

    /// Key column `i`, the `n`th non-`NULL` one, named `name(i, n)`.
    pub(super) fn named(
        pk: &[PrimaryKeyColumn],
        pattern: &[bool],
        alias: &'static str,
        start: usize,
        name: impl Fn(usize, usize) -> String,
    ) -> Self {
        let (mut arrays, mut columns) = (Vec::new(), Vec::new());
        let matched = pk
            .iter()
            .zip(pattern)
            .enumerate()
            .map(|(i, (column, &null))| {
                let part = (!null).then(|| {
                    let part = name(i, columns.len());
                    let param = start + arrays.len();
                    arrays.push(format!("${param}::text[]::{}[]", column.data_type));
                    columns.push(part.clone());
                    part
                });
                (quote_ident(&column.name), part)
            })
            .collect();
        Self {
            alias,
            arrays,
            columns,
            matched,
        }
    }

    /// Leads the relation with the keys' own `text[]` at `$param`, as its
    /// column `key`.
    pub(super) fn with_keys(mut self, param: usize) -> Self {
        self.arrays.insert(0, format!("${param}::text[]"));
        self.columns.insert(0, "key".to_string());
        self
    }

    /// The relation, `None` when it binds nothing (every part `NULL`).
    pub(super) fn relation(&self) -> Option<String> {
        (!self.arrays.is_empty()).then(|| {
            format!(
                "unnest({}) as {}({})",
                self.arrays.join(", "),
                self.alias,
                self.columns.join(", ")
            )
        })
    }

    /// `<row>.<col> is null` for a `NULL` part and `<row>.<col> =
    /// <alias>.<part>` for the others, and-ed in key order.
    pub(super) fn matches(&self, row: &str) -> String {
        self.matched
            .iter()
            .map(|(ident, part)| match part {
                Some(part) => format!("{row}.{ident} = {}.{part}", self.alias),
                None => format!("{row}.{ident} is null"),
            })
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// [`Self::matches`], and on a single-column key's non-`NULL` arm also
    /// the bound (the module doc's "The single-column bound").
    pub(super) fn bounded_matches(&self, row: &str) -> String {
        match (self.matched.as_slice(), self.arrays.last()) {
            // The part's array is the last, after any keys' array.
            ([(ident, Some(_))], Some(array)) => {
                format!("{} and {row}.{ident} = any({array})", self.matches(row))
            }
            _ => self.matches(row),
        }
    }
}
