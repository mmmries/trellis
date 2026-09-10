//! Publication and slot lifecycle (issue #8) — see "Adjacent invariants
//! that are easy to miss" and "Failure modes" in
//! docs/staging-and-claiming/01-intake-and-lsn-confirmation.md:
//!
//! - [`reconcile_publication`] changes a publication's table set in place
//!   (`ALTER PUBLICATION ... ADD/DROP TABLE`), never by dropping and
//!   recreating it.
//! - [`run_pending_backfills`] discharges the `pending_backfill` markers
//!   [`reconcile_publication`] leaves behind: a newly-added table's
//!   pre-existing rows, staged by enumeration once the marker's transaction
//!   fence has settled.
//! - [`initial_snapshot_handshake`] creates a slot and backfills every
//!   watched table from the exact snapshot the slot's creation exports —
//!   gap-free by construction.
//! - [`require_slot_healthy`] is the loud startup check for slot
//!   invalidation/loss.

use std::collections::BTreeSet;

use tokio_postgres::types::PgLsn;
use tokio_postgres::{GenericClient, Transaction};

use super::error::IntakeError;
use crate::pool::quote_ident;
use crate::staging::append::{self, StagedChange};
use crate::staging::session::ProducerSession;

/// Joins `schema` and `table` into the `"schema.table"` shape
/// [`StagedChange::src_table`] uses throughout this crate — the *only* place
/// that string is built. Enforces by construction the invariant
/// [`split_qualified`] depends on: neither component may itself contain a
/// `.`. A schema or table literally named `a.b` is legal Postgres (quoted),
/// but the joined-string representation has no way to tell that dot apart
/// from the separator, so [`split_qualified`] would silently mis-split it —
/// this turns that into a loud error at the point the ambiguous string would
/// otherwise be built.
pub(crate) fn qualify(schema: &str, table: &str) -> Result<String, IntakeError> {
    if schema.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: schema.to_string(),
        });
    }
    if table.contains('.') {
        return Err(IntakeError::DottedIdentifierComponent {
            component: table.to_string(),
        });
    }
    Ok(format!("{schema}.{table}"))
}

/// Splits a `"schema.table"` name (the shape [`StagedChange::src_table`]
/// uses throughout this crate) into its parts, for building DDL/enumeration
/// SQL that needs them quoted separately. Unambiguous for every name this
/// crate can actually produce: [`qualify`] is the sole construction site for
/// `src_table`, and it rejects any component containing a `.` before
/// joining — so the first `.` here is always the real separator, never one
/// smuggled in through a dotted identifier.
fn split_qualified(name: &str) -> Result<(&str, &str), IntakeError> {
    name.split_once('.')
        .ok_or_else(|| IntakeError::InvalidTableName(name.to_string()))
}

async fn current_publication_tables(
    client: &impl GenericClient,
    publication: &str,
) -> Result<BTreeSet<String>, IntakeError> {
    let rows = client
        .query(
            "select schemaname, tablename from pg_publication_tables where pubname = $1",
            &[&publication],
        )
        .await?;
    rows.into_iter()
        .map(|r| qualify(&r.get::<_, String>(0), &r.get::<_, String>(1)))
        .collect()
}

/// Reconciles `publication`'s table set to exactly `desired_tables`, in
/// place — **never** by dropping and recreating the publication, which would
/// orphan the slot's retention and lose rows written in the gap. Adding a
/// table commits the `ALTER` and a `pending_backfill` marker in one
/// transaction, so the follow-up enumeration ([`run_pending_backfills`]) is
/// exactly as durable as the schema change that requires it. Idempotent:
/// safe to call on every setup pass, and (issue #14) on every periodic
/// re-reconciliation pass a running client's maintenance loop makes.
///
/// Takes a plain `&mut tokio_postgres::Client` rather than a
/// [`crate::staging::session::ProducerSession`]: nothing here needs that
/// session's guards (`synchronous_commit`, the producer singleton advisory
/// lock) — only its `transaction()`/`client()` shape, which a plain
/// `Client` has too. That matters for a running client's maintenance loop,
/// whose own connection is never a `ProducerSession`: the one already-live
/// `ProducerSession` for the whole client lifetime is intake's own (held for
/// as long as [`super::Intake`] runs), and the singleton lock it holds is
/// session-scoped — a second `ProducerSession::connect` call while intake is
/// running would simply fail to acquire it.
pub async fn reconcile_publication(
    client: &mut tokio_postgres::Client,
    publication: &str,
    desired_tables: &[String],
) -> Result<(), IntakeError> {
    let current = current_publication_tables(client, publication).await?;
    let desired: BTreeSet<&String> = desired_tables.iter().collect();

    let to_add: Vec<&String> = desired_tables
        .iter()
        .filter(|t| !current.contains(*t))
        .collect();
    let to_drop: Vec<&String> = current.iter().filter(|t| !desired.contains(t)).collect();

    if to_add.is_empty() && to_drop.is_empty() {
        return Ok(());
    }

    let txn = client.transaction().await?;
    for table in &to_drop {
        let (schema, name) = split_qualified(table)?;
        txn.execute(
            &format!(
                "alter publication {} drop table {}.{}",
                quote_ident(publication),
                quote_ident(schema),
                quote_ident(name)
            ),
            &[],
        )
        .await?;
    }
    for table in &to_add {
        let (schema, name) = split_qualified(table)?;
        txn.execute(
            &format!(
                "alter publication {} add table {}.{}",
                quote_ident(publication),
                quote_ident(schema),
                quote_ident(name)
            ),
            &[],
        )
        .await?;
        // The fence: captured in the *same* transaction as the ADD, so it
        // names exactly the transactions concurrent with this table joining
        // the stream — see the module doc.
        let fence: String = txn
            .query_one("select pg_current_snapshot()::text", &[])
            .await?
            .get(0);
        txn.execute(
            "insert into pending_backfill (table_name, fence_snapshot) \
             values ($1, $2::text::pg_snapshot) on conflict (table_name) do nothing",
            &[table, &fence],
        )
        .await?;
    }
    txn.commit().await?;
    Ok(())
}

/// A parsed `pg_snapshot` text representation (`"xmin:xmax:xip,..."`) — only
/// `xmin`/`xmax` matter for fence settlement, so the in-progress list is
/// parsed for validity and otherwise discarded.
#[derive(Debug)]
struct Snapshot {
    xmin: i64,
    xmax: i64,
}

impl Snapshot {
    fn parse(text: &str) -> Result<Self, IntakeError> {
        let mut parts = text.split(':');
        let xmin = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| IntakeError::InvalidSnapshot(text.to_string()))?;
        let xmax = parts
            .next()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| IntakeError::InvalidSnapshot(text.to_string()))?;
        Ok(Self { xmin, xmax })
    }

    /// Whether every transaction that could have been in flight when `fence`
    /// was captured has since settled (committed or aborted). `xmin` only
    /// advances past a transaction once it completes — so a transaction
    /// concurrent with the fence, still open, holds `xmin` at its own xid
    /// for as long as it stays open. The comparison must be **strict**:
    /// `pg_current_snapshot()`'s `xmax` is "one past the latest *completed*
    /// txid," which says nothing about a still-running transaction whose xid
    /// happens to equal it (the fence's own ADD transaction is exactly such
    /// a case) — `self.xmin > fence.xmax`, not `>=`, is what actually forces
    /// that transaction to complete first.
    fn settled_since(&self, fence: &Snapshot) -> bool {
        self.xmin > fence.xmax
    }
}

async fn current_snapshot(client: &impl GenericClient) -> Result<Snapshot, IntakeError> {
    let text: String = client
        .query_one("select pg_current_snapshot()::text", &[])
        .await?
        .get(0);
    Snapshot::parse(&text)
}

struct PendingBackfill {
    table: String,
    fence: Snapshot,
}

async fn fetch_pending_backfills(
    client: &impl GenericClient,
) -> Result<Vec<PendingBackfill>, IntakeError> {
    let rows = client
        .query(
            "select table_name, fence_snapshot::text from pending_backfill",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|r| {
            let table: String = r.get(0);
            let fence_text: String = r.get(1);
            Snapshot::parse(&fence_text).map(|fence| PendingBackfill { table, fence })
        })
        .collect()
}

async fn primary_key_columns(
    txn: &Transaction<'_>,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, IntakeError> {
    let rows = txn
        .query(
            "select a.attname \
             from pg_index i \
             join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey) \
             where i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass \
               and i.indisprimary \
             order by array_position(i.indkey, a.attnum)",
            &[&schema, &table],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

/// Page size for [`enumerate_and_append`]'s server-side cursor. Bounds one
/// backfill enumeration pass to O(page) memory rather than O(table) — the
/// same class of fix issue #8 gave the CDC stream path via [`super::spill`],
/// applied here to the enumeration path that used to `SELECT` an entire
/// source table into one `Vec`. 10k rows keeps each `FETCH` round-trip cheap
/// while staying a trivial allocation even for a table with wide keys.
const BACKFILL_PAGE_ROWS: i64 = 10_000;

/// The fixed cursor name [`enumerate_and_append`] declares. A literal, not
/// caller input, so it needs no quoting/escaping of its own — only the
/// schema/table it selects from does.
const BACKFILL_CURSOR: &str = "trellis_backfill_cursor";

/// Enumerates `src_table`'s current rows as image-less
/// [`StagedChange::Recompute`] triggers — the shape that asserts nothing
/// about a row's state, which is all a backfill actually knows ("this key
/// exists as of now," not any particular before/after image) — and appends
/// each page directly into the active ring segment, inside `txn`.
///
/// Streams via a server-side `DECLARE ... CURSOR` rather than one `SELECT`
/// of the whole table: the earlier version bought a `Vec<StagedChange>` the
/// same size as the source table, unbounded memory for a backfill of any
/// real size (the same class of bug issue #8 fixed for the stream path via
/// spill). A cursor only lives for the transaction that declares it, which
/// is exactly the scope `txn` already has here — so paging changes nothing
/// about the one-transaction durability guarantee both callers rely on.
///
/// Also reused by definition creation's backfill (issue #23): a definition
/// enumerates its source exactly once here regardless of how many
/// calculated fields it declares, preserving the "N columns, one backfill"
/// property as the definition model becomes first-class.
pub(crate) async fn enumerate_and_append(
    txn: &Transaction<'_>,
    src_table: &str,
) -> Result<(), IntakeError> {
    let (schema, table) = split_qualified(src_table)?;
    let pk_cols = primary_key_columns(txn, schema, table).await?;
    if pk_cols.is_empty() {
        return Err(IntakeError::MissingKeyValue {
            table: src_table.to_string(),
        });
    }
    let select_list = pk_cols
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    txn.batch_execute(&format!(
        "declare {BACKFILL_CURSOR} cursor for select {select_list} from {}.{}",
        quote_ident(schema),
        quote_ident(table)
    ))
    .await?;
    loop {
        let rows = txn
            .query(
                &format!("fetch forward {BACKFILL_PAGE_ROWS} from {BACKFILL_CURSOR}"),
                &[],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        let mut page = Vec::with_capacity(rows.len());
        for row in &rows {
            let key = (0..pk_cols.len())
                .map(|i| row.get::<_, String>(i))
                .collect::<Vec<_>>()
                .join("\u{1f}");
            page.push(StagedChange::Recompute {
                src_table: src_table.to_string(),
                key,
                hop_gen: 0,
                group_key: None,
            });
        }
        append::append(txn, &page).await?;
    }
    txn.batch_execute(&format!("close {BACKFILL_CURSOR}"))
        .await?;
    Ok(())
}

/// Records that `qualified_table` (a `"schema.table"` name, as [`qualify`]
/// builds) has, as of *now*, been fully folded into an already-built target by
/// a direct backfill (issue #79, bug B) — capturing both a fence snapshot and
/// the table's current row count in one statement (so they share one MVCC
/// snapshot and describe the same instant). [`coverage_covers`] later consults
/// this to skip a redundant catch-up enumeration.
///
/// A plain upsert (last write wins) is correct because the caller
/// ([`crate::defs::catalog::install_definition`]) only ever records coverage
/// for a table with exactly one reader, and *clears* it (see
/// [`clear_backfill_coverage`]) the moment a second reader appears — so two
/// live recordings for one table never coexist to be reconciled.
pub async fn record_backfill_coverage(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), IntakeError> {
    let (schema, table) = split_qualified(qualified_table)?;
    let row = client
        .query_one(
            &format!(
                "select count(*)::bigint, pg_current_snapshot()::text from {}.{}",
                quote_ident(schema),
                quote_ident(table)
            ),
            &[],
        )
        .await?;
    let count: i64 = row.get(0);
    let fence: String = row.get(1);
    client
        .execute(
            "insert into backfill_coverage (table_name, fence_snapshot, covered_row_count) \
             values ($1, $2::text::pg_snapshot, $3) \
             on conflict (table_name) do update set \
               fence_snapshot = excluded.fence_snapshot, \
               covered_row_count = excluded.covered_row_count",
            &[&qualified_table, &fence, &count],
        )
        .await?;
    Ok(())
}

/// Drops any coverage record for `qualified_table` — called when a table gains
/// a second reader (so the single-reader assumption [`record_backfill_coverage`]
/// relies on no longer holds) or when any definition is built through the ring
/// path (which never records coverage of its own). Removing the record forces
/// [`coverage_covers`] back to full enumeration — the safe default.
pub async fn clear_backfill_coverage(
    client: &impl GenericClient,
    qualified_table: &str,
) -> Result<(), IntakeError> {
    client
        .execute(
            "delete from backfill_coverage where table_name = $1",
            &[&qualified_table],
        )
        .await?;
    Ok(())
}

/// Whether `table`'s pre-existing rows are already fully reflected in a
/// directly-built target and its catch-up enumeration can therefore be
/// skipped (issue #79, bug B). Returns `false` — meaning "enumerate, the safe
/// default" — whenever anything is uncertain.
///
/// # Why not a plain fence comparison
///
/// The obvious design — "skip if the build's fence is at least as recent as
/// the table's publication-join fence" — cannot work, because the direct
/// build *always* precedes the join in time: a definition is built (reading
/// the current source + relationship tables) and only *afterward* does the
/// periodic reconcile loop notice those tables and add them to the
/// publication. So the build snapshot's xids are always *older* than the join
/// fence's, and any whole-snapshot `settled_since`-style test would either
/// never skip (useless) or always skip (unsafe) — the global xid clock
/// advances between build and join regardless of whether *this* table saw any
/// write, so it cannot answer the only question that matters: did **this
/// table** change in the gap between the build and the join?
///
/// # What we actually check
///
/// The build's coverage record (`backfill_coverage`) names the exact snapshot
/// `fence` at which the table was fully folded into a target, plus the row
/// count at that instant. The table's contribution is unchanged since the
/// fence — and the build therefore still fully covers it — iff **all three**
/// hold:
///
/// 1. No surviving row was inserted or updated after the fence: every live
///    row's `xmin` is visible in `fence` (`pg_visible_in_snapshot`). An insert
///    or in-place update stamps a fresh, fence-invisible `xmin`, so this
///    catches both.
/// 2. The current row count equals the recorded count. This is what catches a
///    *delete*, whose tuple simply vanishes and so leaves no invisible `xmin`
///    behind. (Insert-then-delete churn that nets to the same count is still
///    caught by rule 1 via the inserted row's `xmin` — unless that row was
///    also deleted, in which case the table's contribution genuinely did not
///    change and skipping is correct.)
/// 3. Both the fence and the current snapshot are in xid epoch 0 (their
///    `pg_snapshot` xmax is below 2^32). The `xmin::text::xid8` cast in rule 1
///    reads a 32-bit tuple xid as an epoch-0 `xid8`; once the xid counter has
///    wrapped (epoch > 0) that reconstruction is wrong, so past the first
///    wraparound we conservatively refuse to skip rather than risk comparing
///    across epochs. Every case short of ~2^32 lifetime transactions — all
///    tests, and any realistic build→join window — is epoch 0.
///
/// A concurrent writer is a non-issue: the caller only reaches here once the
/// marker's own fence has settled, and an as-yet-uncommitted row is simply not
/// visible in `fence`, so it lands on the safe side (rule 1 fails → enumerate).
async fn coverage_covers(txn: &Transaction<'_>, table: &str) -> Result<bool, IntakeError> {
    let (schema, name) = split_qualified(table)?;
    let Some(row) = txn
        .query_opt(
            "select fence_snapshot::text, covered_row_count \
             from backfill_coverage where table_name = $1",
            &[&table],
        )
        .await?
    else {
        return Ok(false);
    };
    let fence: String = row.get(0);
    let count: i64 = row.get(1);
    let covered: bool = txn
        .query_one(
            &format!(
                "select \
                   pg_snapshot_xmax($1::text::pg_snapshot) < '4294967296'::xid8 \
                   and pg_snapshot_xmax(pg_current_snapshot()) < '4294967296'::xid8 \
                   and (select count(*)::bigint from {sch}.{tbl}) = $2 \
                   and not exists ( \
                     select 1 from {sch}.{tbl} \
                     where not pg_visible_in_snapshot(xmin::text::xid8, $1::text::pg_snapshot) \
                   )",
                sch = quote_ident(schema),
                tbl = quote_ident(name),
            ),
            &[&fence, &count],
        )
        .await?
        .get(0);
    Ok(covered)
}

/// Runs every pending backfill whose fence has settled, staging its
/// pre-existing rows and deleting the marker in the *same* transaction as
/// that staging commit. A crash between the `ALTER` and this point leaves
/// the marker durable; a fence that hasn't settled yet is left alone for the
/// next setup pass — this function is meant to be retried on every one.
///
/// Issue #79 (bug B): before enumerating a marker's table, consult
/// [`coverage_covers`]. When a direct backfill has already folded the table's
/// current contents into a target and the table provably hasn't changed since,
/// the enumeration would stage millions of `Recompute` markers that only
/// re-derive already-correct values — so the marker is discharged with nothing
/// staged. Any uncertainty falls back to the full enumeration this has always
/// done, so the skip can never drop work.
pub async fn run_pending_backfills(
    client: &mut tokio_postgres::Client,
    wake_channel: &str,
) -> Result<(), IntakeError> {
    let pending = fetch_pending_backfills(client).await?;
    if pending.is_empty() {
        return Ok(());
    }
    let now = current_snapshot(client).await?;

    for marker in pending {
        if !now.settled_since(&marker.fence) {
            continue;
        }
        let txn = client.transaction().await?;
        let staged = if coverage_covers(&txn, &marker.table).await? {
            false
        } else {
            enumerate_and_append(&txn, &marker.table).await?;
            true
        };
        txn.execute(
            "delete from pending_backfill where table_name = $1",
            &[&marker.table],
        )
        .await?;
        if staged {
            txn.execute("select pg_notify($1, '')", &[&wake_channel])
                .await?;
        }
        txn.commit().await?;
    }
    Ok(())
}

/// The initial snapshot handshake: creates `slot` and backfills every one of
/// `tables` from the exact snapshot `pg_create_logical_replication_slot`
/// exports — valid for the rest of this transaction — then commits. Intake
/// then streams from the slot's own consistent point. Gap-free by
/// construction, not by overlap-and-dedup: nothing written after the slot's
/// consistent point is enumerated here, and nothing before it is skipped by
/// the stream.
///
/// **Not one atomic unit.** `pg_create_logical_replication_slot` persists the
/// slot to disk the moment it returns, independent of the surrounding
/// transaction — only the backfill and the `replication_progress` INSERT
/// that follow are undone by a rollback or a crash before `commit()`. A
/// crash in that window leaves the slot on disk with no progress row: the
/// orphaned state [`slot_is_orphaned`] below detects and
/// [`IntakeError::OrphanedSlot`] names, rather than dying on "slot already
/// exists" if this function were just retried.
///
/// Callers on a fresh install run this once, before ever calling
/// [`super::Intake::connect`]; an existing install with a `replication_progress`
/// row for `slot` should never call this again (see [`require_slot_healthy`] —
/// recovery from slot loss is this same function, run explicitly by an
/// operator, never automatically).
///
/// Also seeds `replication_progress` for `slot` at its own consistent point —
/// this is "whatever first uses the slot's name" that `V4__replication_progress.sql`
/// says is responsible for the row's one INSERT. The linchpin
/// (`engine::intake::stage_and_advance`) only ever UPDATEs it; without this
/// seed the row never exists, so [`super::Intake::connect`]'s precondition
/// check (issue #31, finding 2) would reject every fresh slot.
pub async fn initial_snapshot_handshake(
    session: &mut ProducerSession,
    slot: &str,
    tables: &[String],
) -> Result<(), IntakeError> {
    if slot_is_orphaned(session.client(), slot).await? {
        return Err(IntakeError::OrphanedSlot {
            slot: slot.to_string(),
        });
    }
    let txn = session.transaction().await?;
    // Must be the transaction's first statement (Postgres requires `SET
    // TRANSACTION` before any other command), and matters here: only a
    // REPEATABLE READ transaction holds its snapshot steady across the
    // multiple enumeration queries below.
    txn.execute("set transaction isolation level repeatable read", &[])
        .await?;
    let slot_row = txn
        .query_one(
            "select lsn from pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await?;
    let consistent_point: PgLsn = slot_row.get(0);
    // One append call per table-page rather than collecting every table's
    // changes into one giant `Vec` first: still all one transaction, so this
    // is equivalent to the old "collect all then append once" for atomicity
    // — `append::append` has no state that requires being called exactly
    // once (see its own doc comment: it just reads the ring pointer and
    // inserts).
    for table in tables {
        enumerate_and_append(&txn, table).await?;
    }
    txn.execute(
        "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
        &[&slot, &consistent_point],
    )
    .await?;
    txn.commit().await?;
    Ok(())
}

/// Whether `slot` exists in `pg_replication_slots` but has no
/// `replication_progress` row — the orphaned state a crash between
/// `pg_create_logical_replication_slot` (which persists immediately, see
/// [`initial_snapshot_handshake`]'s doc comment) and that same handshake's
/// commit leaves behind. A slot that exists *with* a progress row is a
/// different situation (re-running setup against an already-initialized
/// slot) and is left to the existing "slot already exists" error path.
async fn slot_is_orphaned(client: &impl GenericClient, slot: &str) -> Result<bool, IntakeError> {
    let slot_exists: bool = client
        .query_one(
            "select exists(select 1 from pg_replication_slots where slot_name = $1)",
            &[&slot],
        )
        .await?
        .get(0);
    if !slot_exists {
        return Ok(false);
    }
    let has_progress_row: bool = client
        .query_one(
            "select exists(select 1 from replication_progress where slot_name = $1)",
            &[&slot],
        )
        .await?
        .get(0);
    Ok(!has_progress_row)
}

/// The slot's observed health, per `pg_replication_slots.wal_status`.
enum SlotHealth {
    Healthy,
    Missing,
    Invalidated,
}

async fn slot_health(client: &impl GenericClient, slot: &str) -> Result<SlotHealth, IntakeError> {
    let row = client
        .query_opt(
            "select wal_status from pg_replication_slots where slot_name = $1",
            &[&slot],
        )
        .await?;
    Ok(match row {
        None => SlotHealth::Missing,
        Some(r) => {
            let wal_status: String = r.get(0);
            if wal_status == "lost" {
                SlotHealth::Invalidated
            } else {
                SlotHealth::Healthy
            }
        }
    })
}

/// Checked at [`super::Intake::connect`] whenever `last_confirmed_lsn` shows
/// this instance has confirmed work against `slot` before: the slot must
/// still exist and not be marked `lost`. Invalidation (retention cap
/// exceeded) and loss on failover (pre-PG17 doesn't preserve slots across a
/// promotion) both mean everything between `last_confirmed_lsn` and any new
/// slot's start position is unrecoverable short of a fresh
/// [`initial_snapshot_handshake`] — so this errors loudly at startup rather
/// than silently resuming into a gap.
pub async fn require_slot_healthy(
    client: &impl GenericClient,
    slot: &str,
    last_confirmed_lsn: tokio_postgres::types::PgLsn,
) -> Result<(), IntakeError> {
    match slot_health(client, slot).await? {
        SlotHealth::Healthy => Ok(()),
        SlotHealth::Missing | SlotHealth::Invalidated => Err(IntakeError::SlotLost {
            slot: slot.to_string(),
            last_confirmed_lsn: u64::from(last_confirmed_lsn),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_parses_xmin_and_xmax() {
        let s = Snapshot::parse("10:20:11,15").unwrap();
        assert_eq!(s.xmin, 10);
        assert_eq!(s.xmax, 20);
    }

    #[test]
    fn snapshot_parse_rejects_malformed_text() {
        match Snapshot::parse("not-a-snapshot") {
            Err(IntakeError::InvalidSnapshot(text)) => assert_eq!(text, "not-a-snapshot"),
            other => panic!("expected InvalidSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn fence_settles_only_once_xmin_strictly_passes_the_old_xmax() {
        let fence = Snapshot::parse("10:20:").unwrap();
        assert!(!Snapshot::parse("15:25:").unwrap().settled_since(&fence));
        // Exactly at the fence's xmax must not count as settled — that is
        // precisely the boundary a transaction concurrent with the fence
        // (e.g. the ADD's own transaction) can land on.
        assert!(!Snapshot::parse("20:30:").unwrap().settled_since(&fence));
        assert!(Snapshot::parse("21:31:").unwrap().settled_since(&fence));
    }

    #[test]
    fn split_qualified_rejects_an_unqualified_name() {
        match split_qualified("widgets") {
            Err(IntakeError::InvalidTableName(name)) => assert_eq!(name, "widgets"),
            other => panic!("expected InvalidTableName, got {other:?}"),
        }
    }

    #[test]
    fn qualify_rejects_a_dotted_schema_or_table_component() {
        match qualify("a.b", "widgets") {
            Err(IntakeError::DottedIdentifierComponent { component }) => {
                assert_eq!(component, "a.b")
            }
            other => panic!("expected DottedIdentifierComponent, got {other:?}"),
        }
        match qualify("public", "a.b") {
            Err(IntakeError::DottedIdentifierComponent { component }) => {
                assert_eq!(component, "a.b")
            }
            other => panic!("expected DottedIdentifierComponent, got {other:?}"),
        }
    }

    #[test]
    fn qualify_round_trips_through_split_qualified() {
        let joined = qualify("public", "widgets").unwrap();
        assert_eq!(joined, "public.widgets");
        assert_eq!(split_qualified(&joined).unwrap(), ("public", "widgets"));
    }
}
