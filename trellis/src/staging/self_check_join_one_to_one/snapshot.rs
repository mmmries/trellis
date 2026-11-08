//! A 1-1 `JOIN` definition's hidden state, its target and its sources as its
//! copies have applied them, all read under one snapshot: what the generative
//! suite's in-run invariant (`generative::invariant::join1`, #1709) checks
//! while a case's writes and drains run. Test-only, never API.
//!
//! [`star_snapshot`] reads it position by position ([`StarSnapshot`]): `R`,
//! and at each joined position its `M`, its `J` rows, its table as `M` has
//! applied it and its fan-out markers. A two-table `JOIN` is the
//! one-position case, which [`hidden_snapshot`] reads as a [`HiddenSnapshot`].
//!
//! **One snapshot.** The statements run in one `REPEATABLE READ READ ONLY`
//! transaction, so they share the snapshot its first query takes. Capture
//! writes a change's ring row in the change's own transaction, and a page
//! writes `R`, `M`, the target and its markers, and marks its ring rows
//! applied, in one transaction (ADR-0021). So under one snapshot a page in
//! flight is invisible whole, and each source is what its copy has applied
//! plus the ring rows still unapplied, which [`super::Side::applied`]
//! subtracts as `self_check` does.
//!
//! **Locks before the snapshot.** `TRUNCATE` and `DROP` are not MVCC-safe: a
//! snapshot taken before one commits reads the emptied or recreated table
//! as empty. A ring slot is retired by `TRUNCATE`, a source may be
//! truncated by the workload, and `RESUME` drops and recreates `R`, `M` and
//! `J`. So the transaction first takes `ACCESS SHARE` on every ring slot,
//! every source (the root and each joined table), `R`, each position's `M`,
//! `J` and the target, with `LOCK TABLE`, which
//! takes no snapshot: any such statement either committed before the
//! snapshot, which then sees its effects, or waits until the read ends.
//! Retirement takes its lock `NOWAIT` and skips a slot it can't lock. A lock
//! that isn't granted within [`LOCK_TIMEOUT`] (a `RESUME` holding one table
//! while it waits for another this read holds) skips the read:
//! [`star_snapshot`] returns `None`, as it does when a hidden table is gone.

use std::collections::BTreeSet;

use crate::defs::catalog;
use crate::defs::ddl;
use crate::defs::join::{self as spec, Position, Side as JoinSide};
use crate::defs::join_one_to_one::{HiddenTable, read_columns};
use crate::defs::ledger as schema;
use crate::pool::{Pool, quote_ident};

use super::super::converge::per_ring_table;
use super::super::self_check::{SelfCheckError, paused_columns, walk_key};
use super::{Audit, Side, build};

/// How long the read waits for a lock before it skips (the module doc).
pub const LOCK_TIMEOUT: &str = "200ms";

/// One entry of `R` or `M`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenEntry {
    /// The entry key: its row's primary key text.
    pub key: String,
    /// The join key's columns as text, in `ON` order.
    pub join_key: Vec<Option<String>>,
    /// The columns the ledger copies, as text, in [`SourceView::columns`]'s
    /// order (the join key first).
    pub values: Vec<Option<String>>,
    pub tombstone: bool,
    /// The entry's applied position, as `pg_lsn`'s number.
    pub applied_lsn: Option<u64>,
}

/// One target row: its key text and each field as text, in
/// [`HiddenSnapshot::fields`]'s order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetRow {
    pub key: String,
    pub values: Vec<Option<String>>,
}

/// The target row the definition's fields give a live `R` entry, read with
/// the newest live `M` entry on its join key (the apply's rule: the latest
/// applied position, then the highest key), evaluated by Postgres under the
/// snapshot by `self_check`'s renderer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedRow {
    /// The `R` entry's key.
    pub key: String,
    /// The `M` entry it was read with, or `None` when its key has no live one.
    pub joined: Option<String>,
    pub values: Vec<Option<String>>,
}

/// A source as its copy has applied it (`self_check`'s view).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceView {
    /// The columns the copy holds, the join key first.
    pub columns: Vec<String>,
    /// Each row as applied, by primary key text, its values in
    /// [`Self::columns`]'s order.
    pub rows: Vec<(String, Vec<Option<String>>)>,
    /// Keys the ring can't vouch for: an unapplied change with no usable
    /// image, or a held key whose parked origin is unknown.
    pub uncertain: BTreeSet<String>,
    /// An unapplied change that touches every key (a `TRUNCATE`).
    pub global: bool,
}

/// A pending fan-out marker (ADR-0021): the rows of its join key after
/// `cursor` (in `R`'s entry-key order under `"C"`; every row when `None`)
/// may still join an older `M` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanOutMarker {
    pub join_key: Vec<String>,
    pub cursor: Option<String>,
}

/// A 1-1 `JOIN` definition's state under one snapshot (the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HiddenSnapshot {
    pub target: String,
    /// `transform_definitions.status`.
    pub status: String,
    /// A build chunk of the definition is not done yet.
    pub building: bool,
    /// `LEFT` (else `INNER`).
    pub left: bool,
    /// The target's fields, except paused ones, in order.
    pub fields: Vec<String>,
    /// `R`, every entry, tombstones included.
    pub from_entries: Vec<HiddenEntry>,
    /// `M`, every entry.
    pub joined_entries: Vec<HiddenEntry>,
    /// `J`'s rows: each join key's columns as text.
    pub keys: Vec<Vec<Option<String>>>,
    /// The target's rows.
    pub rows: Vec<TargetRow>,
    /// One per live `R` entry.
    pub joined_rows: Vec<JoinedRow>,
    /// `a` as `R` has applied it.
    pub from_applied: SourceView,
    /// `b` as `M` has applied it.
    pub joined_applied: SourceView,
    pub markers: Vec<FanOutMarker>,
    /// Keys of `a` held in quarantine (`poison_held`).
    pub held: BTreeSet<String>,
}

/// One `R` entry of a star: its key, its join key at each position, the
/// columns `R` copies, and its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarFromEntry {
    pub key: String,
    /// The join key's columns as text at each position (`join_keys[p - 1]`),
    /// in that position's `ON` order.
    pub join_keys: Vec<Vec<Option<String>>>,
    /// The columns `R` copies, as text, in [`SourceView::columns`]'s order
    /// (every position's join key first).
    pub values: Vec<Option<String>>,
    pub tombstone: bool,
    /// The entry's applied position, as `pg_lsn`'s number.
    pub applied_lsn: Option<u64>,
}

/// One position's hidden state: its kind, its `M`, its `J` rows, its table as
/// `M` has applied it, and its pending fan-outs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StarPositionView {
    /// `LEFT` (else `INNER`).
    pub left: bool,
    /// The position's `M`, every entry.
    pub joined_entries: Vec<HiddenEntry>,
    /// `J`'s rows at this position: each join key's columns as text.
    pub keys: Vec<Vec<Option<String>>>,
    /// The position's table as its `M` has applied it.
    pub joined_applied: SourceView,
    /// The position's pending fan-out markers.
    pub markers: Vec<FanOutMarker>,
}

/// The target row a live `R` entry's fields give, read with the newest live
/// entry on its join key at each position's `M`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarJoinedRow {
    /// The `R` entry's key.
    pub key: String,
    /// The entry read at each position (`joined[p - 1]`), or `None` when the
    /// key has no live one there.
    pub joined: Vec<Option<String>>,
    pub values: Vec<Option<String>>,
}

/// A 1-1 `JOIN` definition's state under one snapshot, position by position
/// (the module doc). Position `p` is at `positions[p - 1]`; a two-table
/// `JOIN` has one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarSnapshot {
    pub target: String,
    /// `transform_definitions.status`.
    pub status: String,
    /// A build chunk of the definition is not done yet.
    pub building: bool,
    /// The target's fields, except paused ones, in order.
    pub fields: Vec<String>,
    /// `R`, every entry, tombstones included.
    pub from_entries: Vec<StarFromEntry>,
    pub positions: Vec<StarPositionView>,
    /// The target's rows.
    pub rows: Vec<TargetRow>,
    /// One per live `R` entry.
    pub joined_rows: Vec<StarJoinedRow>,
    /// `a` as `R` has applied it.
    pub from_applied: SourceView,
    /// Keys of `a` held in quarantine (`poison_held`).
    pub held: BTreeSet<String>,
}

impl From<&HiddenSnapshot> for StarSnapshot {
    /// A two-table snapshot as the one-position star it is.
    fn from(s: &HiddenSnapshot) -> Self {
        StarSnapshot {
            target: s.target.clone(),
            status: s.status.clone(),
            building: s.building,
            fields: s.fields.clone(),
            from_entries: s
                .from_entries
                .iter()
                .map(|e| StarFromEntry {
                    key: e.key.clone(),
                    join_keys: vec![e.join_key.clone()],
                    values: e.values.clone(),
                    tombstone: e.tombstone,
                    applied_lsn: e.applied_lsn,
                })
                .collect(),
            positions: vec![StarPositionView {
                left: s.left,
                joined_entries: s.joined_entries.clone(),
                keys: s.keys.clone(),
                joined_applied: s.joined_applied.clone(),
                markers: s.markers.clone(),
            }],
            rows: s.rows.clone(),
            joined_rows: s
                .joined_rows
                .iter()
                .map(|j| StarJoinedRow {
                    key: j.key.clone(),
                    joined: vec![j.joined.clone()],
                    values: j.values.clone(),
                })
                .collect(),
            from_applied: s.from_applied.clone(),
            held: s.held.clone(),
        }
    }
}

impl StarSnapshot {
    /// The snapshot of a two-table `JOIN` (one position) as a
    /// [`HiddenSnapshot`], or `None` for a star.
    pub fn two_table(self) -> Option<HiddenSnapshot> {
        let [position]: [StarPositionView; 1] = self.positions.try_into().ok()?;
        Some(HiddenSnapshot {
            target: self.target,
            status: self.status,
            building: self.building,
            left: position.left,
            fields: self.fields,
            from_entries: self
                .from_entries
                .into_iter()
                .map(|e| HiddenEntry {
                    key: e.key,
                    join_key: e.join_keys.into_iter().flatten().collect(),
                    values: e.values,
                    tombstone: e.tombstone,
                    applied_lsn: e.applied_lsn,
                })
                .collect(),
            joined_entries: position.joined_entries,
            keys: position.keys,
            rows: self.rows,
            joined_rows: self
                .joined_rows
                .into_iter()
                .map(|j| JoinedRow {
                    key: j.key,
                    joined: j.joined.into_iter().next().flatten(),
                    values: j.values,
                })
                .collect(),
            from_applied: self.from_applied,
            joined_applied: position.joined_applied,
            markers: position.markers,
            held: self.held,
        })
    }
}

/// SQLSTATEs of the `LOCK TABLE` that mean "skip this read": a lock not
/// granted in time, a hidden table a `RESUME` dropped and hasn't recreated.
fn skippable(error: &tokio_postgres::Error) -> bool {
    use tokio_postgres::error::SqlState;
    matches!(
        error.code(),
        Some(code) if *code == SqlState::LOCK_NOT_AVAILABLE
            || *code == SqlState::UNDEFINED_TABLE
            || *code == SqlState::T_R_DEADLOCK_DETECTED
    )
}

/// Reads `target_table`'s (a two-table 1-1 `JOIN` target's) [`HiddenSnapshot`],
/// or `None` when the read is skipped (the module doc). A star's target is
/// refused: [`star_snapshot`] reads every position.
pub async fn hidden_snapshot(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<HiddenSnapshot>, SelfCheckError> {
    let Some(star) = star_snapshot(pool, target_table).await? else {
        return Ok(None);
    };
    let positions = star.positions.len();
    star.two_table()
        .map(Some)
        .ok_or_else(|| SelfCheckError::UnsupportedExpr {
            target: target_table.to_string(),
            detail: format!(
                "a star of {positions} joined tables has no two-table snapshot: read its \
                 star_snapshot"
            ),
        })
}

/// Reads `target_table`'s (a 1-1 `JOIN` target's) [`StarSnapshot`], or `None`
/// when the read is skipped (the module doc).
pub async fn star_snapshot(
    pool: &Pool,
    target_table: &str,
) -> Result<Option<StarSnapshot>, SelfCheckError> {
    let def = catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| SelfCheckError::TargetNotFound(target_table.to_string()))?;
    let pk = walk_key(pool, &def).await?;
    let paused = paused_columns(pool, &def.def).await?;
    let audit = build(pool, &def, &pk, &paused).await?;
    let mut client = pool.get().await?;
    let txn = client
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let mut tables = vec![audit.a.ident.clone()];
    tables.extend(audit.joined.iter().map(|j| j.side.ident.clone()));
    tables.push(audit.a.ledger.ident.clone());
    tables.extend(audit.joined.iter().map(|j| j.side.ledger.ident.clone()));
    let locked = txn
        .batch_execute(&format!(
            "set local lock_timeout = '{LOCK_TIMEOUT}'; \
             lock table {rings}, {tables}, {j}, {target} in access share mode",
            rings = per_ring_table(", ", |_, table| table.to_string()),
            tables = tables.join(", "),
            j = HiddenTable::Keys.qualified(pool.schema(), def.id),
            target = ddl::qualified_target_table_ident(&def.target_table),
        ))
        .await;
    match locked {
        Ok(()) => {}
        Err(e) if skippable(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    match read(&txn, &audit, &pk, pool.schema(), target_table).await {
        Ok(snapshot) => {
            txn.commit().await?;
            Ok(Some(snapshot))
        }
        Err(e) => Err(e.into()),
    }
}

/// The values a ledger's statement selects: its `columns` as text.
fn texts(alias: &str, columns: &[String]) -> String {
    let values: Vec<String> = columns
        .iter()
        .map(|c| format!("{alias}.{}::text", quote_ident(c)))
        .collect();
    format!("array[{}]::text[]", values.join(", "))
}

async fn read(
    txn: &tokio_postgres::Transaction<'_>,
    audit: &Audit<'_>,
    pk: &[ddl::PrimaryKeyColumn],
    catalog_schema: &str,
    target_table: &str,
) -> Result<StarSnapshot, tokio_postgres::Error> {
    let id = audit.id();
    let r = &audit.a.ledger.ident;
    let j = HiddenTable::Keys.qualified(catalog_schema, id);
    let target = ddl::qualified_target_table_ident(&audit.def.target_table);
    let status = txn
        .query_one(
            "select status, exists (select 1 from backfill_chunks \
                                    where definition_id = $1 and not done) \
             from transform_definitions where id = $1",
            &[&id],
        )
        .await?;
    let held = txn
        .query(
            "select key from poison_held where transform_id = $1 and src_table = $2",
            &[&id, &audit.a.table],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    let from_columns = read_columns(&audit.def.def, JoinSide::From);
    let from_keys: Vec<Vec<String>> = audit.joined.iter().map(|p| p.from_key.clone()).collect();
    let from_entries = entries(txn, r, &from_keys, &from_columns)
        .await?
        .into_iter()
        .map(|(e, join_keys)| StarFromEntry {
            key: e.key,
            join_keys,
            values: e.values,
            tombstone: e.tombstone,
            applied_lsn: e.applied_lsn,
        })
        .collect();

    let mut positions = Vec::with_capacity(audit.joined.len());
    for joined in &audit.joined {
        positions.push(position_view(txn, audit, joined, &j).await?);
    }

    let field_texts: Vec<String> = audit
        .fields
        .iter()
        .map(|f| format!("{}::text", quote_ident(&f.name)))
        .collect();
    let rows = txn
        .query(
            &format!(
                "select {key}, array[{values}]::text[] from {target}",
                key = ddl::pk_key_sql_expr(pk, None),
                values = field_texts.join(", "),
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| TargetRow {
            key: row.get(0),
            values: row.get(1),
        })
        .collect();

    let key_col = quote_ident(schema::KEY_COLUMN);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let applied = quote_ident(schema::APPLIED_LSN_COLUMN);
    // One lateral per position over its `M`, aliased as the fields read it.
    let laterals: Vec<String> = audit
        .joined
        .iter()
        .map(|joined| {
            let on: Vec<String> = joined
                .from_key
                .iter()
                .zip(&joined.side.key)
                .map(|(a, b)| format!("x.{} = a.{}", quote_ident(b), quote_ident(a)))
                .collect();
            format!(
                "left join lateral (select x.* from {m} x \
                                    where {on} and not x.{tomb} \
                                    order by x.{applied} desc nulls last, \
                                             x.{key_col} collate \"C\" desc \
                                    limit 1) {alias} on true",
                m = joined.side.ledger.ident,
                on = on.join(" and "),
                alias = joined.alias(),
            )
        })
        .collect();
    let picked: Vec<String> = audit
        .joined
        .iter()
        .map(|joined| format!("{}.{key_col}", joined.alias()))
        .collect();
    let joined_values: Vec<String> = audit
        .fields
        .iter()
        .map(|f| format!("({})::text", f.sql))
        .collect();
    let joined_rows = txn
        .query(
            &format!(
                "select a.{key_col}, array[{picked}]::text[], array[{values}]::text[] \
                 from {r} a {laterals} \
                 where not a.{tomb}",
                picked = picked.join(", "),
                values = joined_values.join(", "),
                laterals = laterals.join(" "),
            ),
            &[],
        )
        .await?
        .iter()
        .map(|row| StarJoinedRow {
            key: row.get(0),
            joined: row.get(1),
            values: row.get(2),
        })
        .collect();

    let from_applied = applied_view(txn, audit, &audit.a, from_columns).await?;

    Ok(StarSnapshot {
        target: target_table.to_string(),
        status: status.get(0),
        building: status.get(1),
        fields: audit.fields.iter().map(|f| f.name.clone()).collect(),
        from_entries,
        positions,
        rows,
        joined_rows,
        from_applied,
        held,
    })
}

/// `joined`'s position's hidden state: its `M`, its `J` rows (`j`'s rows at
/// the position, read through its own group of key columns), its table as
/// `M` has applied it, and its fan-out markers.
async fn position_view(
    txn: &tokio_postgres::Transaction<'_>,
    audit: &Audit<'_>,
    joined: &super::Joined,
    j: &str,
) -> Result<StarPositionView, tokio_postgres::Error> {
    let position: Position = joined.position;
    let number = i16::try_from(position.number()).expect("a position fits a smallint");
    let side = JoinSide::Joined(position);
    let columns = read_columns(&audit.def.def, side);
    let joined_entries = entries(
        txn,
        &joined.side.ledger.ident,
        std::slice::from_ref(&joined.side.key),
        &columns,
    )
    .await?
    .into_iter()
    .map(|(entry, _)| entry)
    .collect();
    let j_columns: Vec<String> = spec::from_key_at(&audit.def.def, position)
        .iter()
        .map(|c| crate::defs::join_one_to_one::j_column(position, c))
        .collect();
    let keys = txn
        .query(
            &format!(
                "select {} from {j} j where j.position = $1",
                texts("j", &j_columns),
            ),
            &[&number],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    let markers = txn
        .query(
            "select join_key, cursor from join1_fan_out \
             where transform_id = $1 and position = $2",
            &[&audit.id(), &number],
        )
        .await?
        .iter()
        .map(|row| FanOutMarker {
            join_key: row.get(0),
            cursor: row.get(1),
        })
        .collect();
    let joined_applied = applied_view(txn, audit, &joined.side, columns).await?;
    Ok(StarPositionView {
        left: joined.left,
        joined_entries,
        keys,
        joined_applied,
        markers,
    })
}

/// Every entry of the ledger `ledger`, with its join key at each of `keys`'s
/// positions (an `R` entry's, one per position; an `M` entry's, its own
/// alone). Each [`HiddenEntry::join_key`] is all of them, in position order.
async fn entries(
    txn: &tokio_postgres::Transaction<'_>,
    ledger: &str,
    keys: &[Vec<String>],
    columns: &[String],
) -> Result<Vec<(HiddenEntry, Vec<Vec<Option<String>>>)>, tokio_postgres::Error> {
    let join_keys: Vec<String> = keys.iter().map(|key| texts("l", key)).collect();
    let rows = txn
        .query(
            &format!(
                "select l.{key_col}, {values}, l.{tomb}, \
                        (l.{applied} - '0/0'::pg_lsn)::text{join_keys} \
                 from {ledger} l",
                key_col = quote_ident(schema::KEY_COLUMN),
                values = texts("l", columns),
                tomb = quote_ident(schema::TOMBSTONE_COLUMN),
                applied = quote_ident(schema::APPLIED_LSN_COLUMN),
                join_keys = join_keys
                    .iter()
                    .map(|k| format!(", {k}"))
                    .collect::<String>(),
            ),
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let join_keys: Vec<Vec<Option<String>>> =
                (0..keys.len()).map(|i| row.get(4 + i)).collect();
            let entry = HiddenEntry {
                key: row.get(0),
                join_key: join_keys.iter().flatten().cloned().collect(),
                values: row.get(1),
                tombstone: row.get(2),
                applied_lsn: row
                    .get::<_, Option<String>>(3)
                    .map(|n| n.parse().expect("an lsn difference is a whole number")),
            };
            (entry, join_keys)
        })
        .collect())
}

/// `side`'s source as its ledger has applied it.
async fn applied_view(
    txn: &tokio_postgres::Transaction<'_>,
    audit: &Audit<'_>,
    side: &Side,
    columns: Vec<String>,
) -> Result<SourceView, tokio_postgres::Error> {
    let ring = side.ring(audit.id());
    let sql = format!(
        "with {ctes}__applied as materialized ({applied}) \
         select 1, {key}, {values} from __applied e \
         union all select 3, u.__k, null::text[] from ({unc}) u \
         union all select 4, '', null::text[] where {global}",
        ctes = ring.one_to_one_ctes(&side.ledger),
        applied = side.applied(&ring),
        key = ddl::pk_key_sql_expr(&side.pk, Some("e")),
        values = texts("e", &columns),
        unc = ring.uncertain_keys_sql(),
        global = ring.global_sql(),
    );
    let mut view = SourceView {
        columns,
        ..SourceView::default()
    };
    for row in txn.query(sql.as_str(), &[]).await? {
        match row.get::<_, i32>(0) {
            1 => view.rows.push((row.get(1), row.get(2))),
            3 => {
                view.uncertain.insert(row.get(1));
            }
            _ => view.global = true,
        }
    }
    Ok(view)
}
