//! The drain's halt: what a [`FailureClass::Halting`] failure parks.
//!
//! A halting failure is structural, not one row's fault: a key the drain
//! can't use (`NoPrimaryKey`, `UnsupportedPrimaryKeyType`), a propagation
//! wave past the hop bound (`HopBoundExceeded`), an aggregate target the
//! group basis can't apply (`GroupBasisUnsupported`), a truncate with no ring `lsn`
//! (`TruncateWithoutLsn`), Postgres refusing the drain's role a read or
//! write (`42501`, see below). Every
//! key it touches reproduces it, so
//! retrying the page re-fails it forever, and quarantining a key blames one
//! for nobody's fault. Instead [`halt_closure`] pauses the definitions the
//! failure reaches, and the drain retries the page without them:
//!
//! - every definition that reads the halting table, as its source or its
//!   `JOIN` table (`capture::columns::readers_of`). For a hop
//!   bound, the halting tables are those the wave ran away through, so their
//!   readers are the cycle's definitions. For a truncate with no `lsn`, it
//!   is the truncated table. For an aggregate the group basis can't apply,
//!   it's the definition that writes that target;
//! - and everything downstream of them, so no hop target goes quietly stale.
//!
//! Each is paused like a capture failure (`capture_failures`, with `kind`
//! `halt`). Its rows then drain with the page: a paused definition's share is
//! dropped, and a table with no reader that isn't frozen is skipped
//! (`apply::source_key_for_apply`), so the page commits, its claims
//! are marked drained and its segments retire. Resuming a definition is the
//! rebuild it is for any pause (`quarantine::resume_transform`).
//!
//! One halt is one episode: the pauses and the halting-stop record commit in
//! one transaction, and only a call that newly paused something records the
//! stop. A peer that met the same failure, or a retry that meets it again,
//! finds the closure already frozen and records nothing.
//!
//! # A refused read or write
//!
//! Every Trellis session runs with `row_security = off`
//! ([`crate::pool::ROW_SECURITY_OFF`]), so a statement that row-level
//! security would filter fails with `42501` instead, as does one the role
//! lacks a privilege for. Neither names a key, and Postgres's message names
//! the table bare (and in the server's `lc_messages`), so the seed is read
//! from the catalog instead, on the drain's own pooled connection and so as
//! its login role ([`refusals`]): each table an unfrozen definition reads
//! whose policies apply to that role ([`row_security::Readers::Session`]),
//! or that it can't read at all, seeds that table's readers, and each
//! target the role can't write, or whose policies apply to it
//! ([`row_security::Readers::Target`]), seeds its writer. Each definition's
//! record carries the reason it found and Postgres's own message. A `42501`
//! the catalog pins on no table (one about Trellis's own objects, say)
//! pauses nothing: [`super::apply`]'s halt then retries the page once and
//! surfaces the error, charging no key either way, as a drain holdup
//! ([`super::holdup`]).
//!
//! A Re-derive build's start that fails in a way no retry gets past halts
//! from the definition writing its target ([`halt_failed_start`]), as for
//! an aggregate the group basis can't apply.
//!
//! [`FailureClass::Halting`]: super::quarantine::FailureClass::Halting

use std::collections::{BTreeSet, HashSet};

use tokio_postgres::{GenericClient, Transaction};

use crate::capture::columns::{CaptureCatalog, load_catalog_for, readers_of};
use crate::defs::ddl::DdlError;
use crate::defs::model::StoredStatus;
use crate::defs::row_security;
use crate::pool::Pool;

use super::apply::ApplyError;
use super::quarantine;

/// Where a halting failure starts its closure.
enum Seed {
    /// Every definition reading these tables.
    Tables(Vec<String>),
    /// The definition writing this target.
    Target(String),
}

/// The tables or target `err`'s innermost [`ApplyError`] names, when it's a
/// halting one.
fn seed(err: &ApplyError) -> Option<Seed> {
    match quarantine::innermost_apply_error(err) {
        ApplyError::Ddl(DdlError::NoPrimaryKey { source_table })
        | ApplyError::Ddl(DdlError::UnsupportedPrimaryKeyType { source_table, .. })
        | ApplyError::Ddl(DdlError::RecordedKeyMissing {
            table: source_table,
            ..
        })
        | ApplyError::Ddl(DdlError::DeferrablePrimaryKey {
            table: source_table,
            ..
        }) => Some(Seed::Tables(vec![source_table.clone()])),
        ApplyError::HopBoundExceeded { tables, .. } => Some(Seed::Tables(tables.clone())),
        ApplyError::TruncateWithoutLsn { src_table } => Some(Seed::Tables(vec![src_table.clone()])),
        ApplyError::GroupBasisUnsupported { target, .. }
        | ApplyError::JoinOneToOneHalt { target, .. } => Some(Seed::Target(target.clone())),
        _ => None,
    }
}

/// The message of the first Postgres error on `err`'s source chain, if it
/// has one.
fn refused_message(err: &ApplyError) -> Option<String> {
    let mut link: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(err) = link {
        if let Some(pg) = err.downcast_ref::<tokio_postgres::Error>() {
            return pg.as_db_error().map(|db| db.message().to_string());
        }
        link = err.source();
    }
    None
}

/// Pauses every definition that isn't frozen and that the halting failure
/// `err` reaches (see the module doc), and, if that paused any, records the
/// halting stop, all in one transaction. Returns the bare targets of the
/// definitions this call paused, in id order: empty when every one was
/// already frozen (a peer halted first), or when `err` isn't a halting
/// failure this module seeds a closure from.
pub async fn halt_closure(pool: &Pool, err: &ApplyError) -> Result<Vec<String>, ApplyError> {
    let seed = match seed(err) {
        Some(Seed::Tables(tables)) => {
            let mut qualified = Vec::with_capacity(tables.len());
            for table in tables {
                qualified.push(quarantine::qualified_src_table(pool, &table).await?);
            }
            Some(Seed::Tables(qualified))
        }
        Some(target) => Some(target),
        None if quarantine::is_insufficient_privilege(err) => None,
        None => return Ok(Vec::new()),
    };

    // A refused read or write reads as Postgres's own message, without the
    // wrappers of whichever step met it, since the refusal names the step's
    // role and table.
    let text = match &seed {
        None => refused_message(err).unwrap_or_else(|| err.to_string()),
        Some(_) => err.to_string(),
    };
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let paused = pause_closure(
        &txn,
        pool.schema(),
        pool.floors(),
        vec![(seed, text.clone())],
        "the drain halted",
        &text,
    )
    .await?;
    txn.commit().await?;
    Ok(paused)
}

/// [`halt_closure`] for capture re-reads Postgres refused
/// (`capture::sql`, "A refused re-read"): each of `refused`'s tables, with
/// the refusals its markers recorded, halts its readers and everything
/// downstream of them, kind `halt`. The statements whose re-read was refused
/// staged nothing, so the readers halt whether or not the role can read the
/// table by the time the drain sees the markers. Returns the bare targets it
/// paused.
pub(crate) async fn halt_refused_captures(
    pool: &Pool,
    refused: &std::collections::BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<String>, ApplyError> {
    if refused.is_empty() {
        return Ok(Vec::new());
    }
    let halts: Vec<(Option<Seed>, String)> = refused
        .iter()
        .map(|(table, refusals)| {
            let refusals: Vec<&str> = refusals.iter().map(String::as_str).collect();
            (
                Some(Seed::Tables(vec![table.clone()])),
                format!(
                    "Trellis's capture of {table} was refused a re-read of it as {}, so changes \
                     written to it meanwhile are missing",
                    refusals.join("; ")
                ),
            )
        })
        .collect();
    let stop = halts
        .iter()
        .map(|(_, text)| text.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let paused = pause_closure(
        &txn,
        pool.schema(),
        pool.floors(),
        halts,
        "the drain halted",
        &stop,
    )
    .await?;
    txn.commit().await?;
    Ok(paused)
}

/// [`halt_closure`] for a Re-derive build's start
/// (`staging::build::start_ready_builds`): a start that failed in a way no
/// retry gets past (its ledger table was dropped, say) pauses the
/// definition writing `target` and everything downstream of its target, with
/// kind `halt`, so one definition's failure doesn't stop the pass's other
/// builds. `Trellis::status` reports `err` (the failure's text) as the
/// definition's `capture_failure`, and a resume rebuilds it or refuses with
/// the repair. Records the halting stop if it paused any, in one
/// transaction, and returns the bare targets it paused: empty when every
/// definition it reaches was already frozen.
pub(crate) async fn halt_failed_start(
    pool: &Pool,
    target: &str,
    failure: &ApplyError,
) -> Result<Vec<String>, ApplyError> {
    let halted = format!("the re-derive build of {target} couldn't start");
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    // A refused start names the role that was refused, as a refused drain
    // does: Postgres's message names the table and not the role.
    let err = if quarantine::is_insufficient_privilege(failure) {
        let role: String = txn
            .query_one("select current_user::text", &[])
            .await?
            .get(0);
        format!("{failure} (role {role} lacks a privilege on that table; grant it)")
    } else {
        failure.to_string()
    };
    let paused = pause_closure(
        &txn,
        pool.schema(),
        pool.floors(),
        vec![(Some(Seed::Target(target.to_string())), err.clone())],
        &halted,
        &err,
    )
    .await?;
    txn.commit().await?;
    Ok(paused)
}

/// Pauses, in `txn`, every unfrozen definition each of `halts`' seeds
/// reaches, or, for a halt with no seed (a refused read or write, `42501`),
/// every one the [`refusals`] read as `txn`'s role reaches; and records the
/// halting stop, with `stop`, if that paused any. `halted` opens each
/// record's sentence: what halted, and the halt's failure text follows it.
///
/// Every row it pauses is locked in one sorted statement before the first
/// pause ([`lifecycle::pause_many`], ADR-0028 I5), across every halt and
/// every refused table, since their closures interleave in id. A definition
/// an earlier halt reaches is left to it, as if each halt ran after the
/// last; within one halt, each refused table's reason is recorded on every
/// definition it reaches.
///
/// [`lifecycle::pause_many`]: crate::defs::lifecycle::pause_many
async fn pause_closure(
    txn: &Transaction<'_>,
    schema: &str,
    floors: &crate::defs::floor::Floors,
    halts: Vec<(Option<Seed>, String)>,
    halted: &str,
    stop: &str,
) -> Result<Vec<String>, ApplyError> {
    let catalog = load_catalog_for(txn, floors).await?;
    let unfrozen: HashSet<i64> = txn
        .query(
            "select id from transform_definitions where status = any($1)",
            &[&StoredStatus::dispatchable()],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    let mut pauses = Vec::new();
    let mut reached = HashSet::new();
    for (seed, err) in halts {
        // Each seed, with the table its records name and the sentence they
        // carry.
        let episodes: Vec<(Seed, String, String)> = match seed {
            Some(seed) => {
                let source_table = match &seed {
                    Seed::Tables(tables) => tables.join(", "),
                    Seed::Target(target) => target.clone(),
                };
                let error = format!(
                    "{halted}: {err}; fix the cause, then resume the definition to rebuild it, or \
                     drop the definition"
                );
                vec![(seed, source_table, error)]
            }
            None => refusals(txn, schema, &catalog, &unfrozen)
                .await?
                .into_iter()
                .map(|refusal| {
                    let error = format!(
                        "{halted}: {err}; {}; then resume the definition to rebuild it, or drop \
                         the definition",
                        refusal.reason
                    );
                    (refusal.seed, refusal.table, error)
                })
                .collect(),
        };
        let mut members = HashSet::new();
        for (seed, source_table, error) in &episodes {
            for id in closure(&catalog, seed, &unfrozen) {
                if reached.contains(&id) {
                    continue;
                }
                members.insert(id);
                pauses.push((
                    id,
                    super::pause_message::PauseReason::Halt {
                        table: source_table.clone(),
                        text: error.clone(),
                    },
                ));
            }
        }
        reached.extend(members);
    }

    let paused: Vec<String> = crate::defs::lifecycle::pause_many(txn, pauses)
        .await?
        .into_iter()
        .map(|(id, _)| {
            let reader = catalog.definitions.iter().find(|r| r.id == id);
            reader.map_or_else(|| id.to_string(), |r| r.def.target.clone())
        })
        .collect();
    if !paused.is_empty() {
        quarantine::record_halting_stop(txn, stop).await?;
    }
    Ok(paused)
}

/// A table the drain's role was refused, found by [`refusals`].
struct Refusal {
    /// The readers of the table, or the writer of the target.
    seed: Seed,
    /// The unquoted `schema.table` identity.
    table: String,
    /// What the catalog says the role can't do there, and how to fix it.
    reason: String,
}

/// Where the documentation on row-level security and Trellis's roles lives,
/// for a refusal's reason.
const ROW_SECURITY_DOCS: &str = "see \"Supported sources and targets\" in docs/transforms.md for the roles row-level security must not apply to";

/// Every table an unfrozen definition reads or writes that the session's
/// role (the drain's: `client` is one of its pool's connections) can't use:
/// it lacks the privilege, or row-level security applies to it (see the
/// module doc). Reads are checked for `SELECT`, and for policies
/// ([`row_security::Readers::Session`]); targets for `SELECT`, `INSERT`,
/// `UPDATE` and `DELETE`, and for policies ([`row_security::Readers::Target`]).
/// A privilege is only counted missing when the role holds it on no column
/// at all, so a column grant never pins the failure on the wrong table.
async fn refusals(
    client: &impl GenericClient,
    schema: &str,
    catalog: &CaptureCatalog,
    unfrozen: &HashSet<i64>,
) -> Result<Vec<Refusal>, ApplyError> {
    let live = || {
        catalog
            .definitions
            .iter()
            .filter(|r| unfrozen.contains(&r.id))
    };
    let read: BTreeSet<String> = live()
        .flat_map(|r| std::iter::once(r.source.clone()).chain(r.joined.iter().cloned()))
        .collect();
    let written: BTreeSet<String> = live().map(|r| r.target.clone()).collect();

    let mut refusals = Vec::new();
    for table in read {
        if !readers_of(catalog, &table, &BTreeSet::new(), true)
            .iter()
            .any(|id| unfrozen.contains(id))
        {
            continue;
        }
        let reason = match missing_privileges(client, &table, &["SELECT"]).await? {
            Some(missing) => Some(missing),
            None => row_security::applying(client, schema, &table, row_security::Readers::Session)
                .await?
                .map(|rls| format!("{rls}; {ROW_SECURITY_DOCS}")),
        };
        if let Some(reason) = reason {
            refusals.push(Refusal {
                seed: Seed::Tables(vec![table.clone()]),
                table,
                reason,
            });
        }
    }
    for table in written {
        let reason =
            match missing_privileges(client, &table, &["SELECT", "INSERT", "UPDATE", "DELETE"])
                .await?
            {
                Some(missing) => Some(missing),
                None => {
                    row_security::applying(client, schema, &table, row_security::Readers::Target)
                        .await?
                        .map(|rls| format!("{rls}; {ROW_SECURITY_DOCS}"))
                }
            };
        if let Some(reason) = reason {
            refusals.push(Refusal {
                seed: Seed::Target(table.clone()),
                table,
                reason,
            });
        }
    }
    Ok(refusals)
}

/// The sentence naming which of `privileges` the session's role lacks on
/// `table` (an unquoted `schema.table` identity), counting `USAGE` on its
/// schema too; `None` if it holds them all, or there is no such table.
/// Looked up by name rather than through `regclass`, whose cast itself
/// raises on a schema the role can't use. `SELECT`, `INSERT` and `UPDATE`
/// count as held when held on any column.
async fn missing_privileges(
    client: &impl GenericClient,
    table: &str,
    privileges: &[&str],
) -> Result<Option<String>, ApplyError> {
    let Some((nspname, relname)) = table.split_once('.') else {
        return Ok(None);
    };
    let row = client
        .query_opt(
            "select current_user::text, \
                    pg_catalog.has_schema_privilege(n.oid, 'USAGE'), \
                    array(select p from unnest($3::text[]) p \
                          where not case when p = 'DELETE' \
                                         then pg_catalog.has_table_privilege(c.oid, p) \
                                         else pg_catalog.has_any_column_privilege(c.oid, p) end) \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2",
            &[&nspname, &relname, &privileges],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let role: String = row.get(0);
    let usage: bool = row.get(1);
    let missing: Vec<String> = row.get(2);
    Ok(if !usage {
        Some(format!(
            "role {role} lacks USAGE on schema {nspname}, so it can't reach {table}, which \
             Trellis reads or writes as that role. Grant it"
        ))
    } else if !missing.is_empty() {
        Some(format!(
            "role {role} lacks {} on {table}, which Trellis needs as that role. Grant it",
            missing.join(", ")
        ))
    } else {
        None
    })
}

/// Whether the session's role holds `USAGE` on the schema of each of
/// `tables` (unquoted `schema.table` identities) that exists. Without it any
/// lookup of the table by name raises `42501`, `to_regclass` included, so a
/// read that would only report on the table (`Trellis::status`'s index gaps)
/// checks this first, and the drain halts the table's readers instead.
pub(crate) async fn schemas_usable(
    client: &impl GenericClient,
    tables: &[String],
) -> Result<bool, tokio_postgres::Error> {
    let schemas: Vec<&str> = tables
        .iter()
        .filter_map(|table| table.split_once('.').map(|(schema, _)| schema))
        .collect();
    let row = client
        .query_one(
            "select coalesce(pg_catalog.bool_and( \
                 pg_catalog.has_schema_privilege(n.oid, 'USAGE')), true) \
             from pg_catalog.pg_namespace n where n.nspname = any($1)",
            &[&schemas],
        )
        .await?;
    Ok(row.get(0))
}

/// What a staging pass leaves out because the role can't use a schema
/// ([`halt_unusable_schemas`]).
#[derive(Debug, Default)]
pub(crate) struct Unusable {
    /// Every definition, paused or not, that reads or writes a table in a
    /// schema the role can't use.
    pub(crate) definitions: Vec<i64>,
    /// The tables a definition reads or writes in a schema the role can't
    /// use. Its tables in other schemas stay in the pass, for their other
    /// readers.
    pub(crate) tables: HashSet<String>,
}

/// Pauses, with kind `halt`, every unfrozen definition that reads or writes
/// a table in a schema the pool's role can't use ([`schemas_usable`]), and
/// everything downstream of them, as the drain's halt does; and returns
/// every such definition, for a staging pass to leave out: any lookup of
/// those tables by name raises, and would fail the pass for every table.
pub(crate) async fn halt_unusable_schemas(pool: &Pool) -> Result<Unusable, ApplyError> {
    let mut client = pool.get().await?;
    let txn = client.transaction().await?;
    let catalog = load_catalog_for(&*txn, pool.floors()).await?;
    let tables_of = |r: &crate::capture::columns::CaptureReader| {
        let mut tables = r.joined.clone();
        tables.extend([r.source.clone(), r.target.clone()]);
        tables
    };
    let every: BTreeSet<String> = catalog.definitions.iter().flat_map(tables_of).collect();
    if schemas_usable(&*txn, &Vec::from_iter(every.iter().cloned())).await? {
        return Ok(Unusable::default());
    }
    let mut refused = BTreeSet::new();
    for table in every {
        if !schemas_usable(&*txn, std::slice::from_ref(&table)).await? {
            refused.insert(table);
        }
    }
    let unusable = Unusable {
        definitions: catalog
            .definitions
            .iter()
            .filter(|reader| tables_of(reader).iter().any(|t| refused.contains(t)))
            .map(|reader| reader.id)
            .collect(),
        tables: refused.iter().cloned().collect(),
    };
    // Every refused table's halt pauses under one sorted lock of every row
    // it pauses ([`pause_closure`]), and is one halting stop.
    let mut halts = Vec::new();
    let mut reasons = Vec::new();
    for table in &refused {
        let reason = missing_privileges(&*txn, table, &[])
            .await?
            .unwrap_or_else(|| format!("the role lacks USAGE on the schema of {table}"));
        for seed in [
            Seed::Tables(vec![table.clone()]),
            Seed::Target(table.clone()),
        ] {
            halts.push((Some(seed), reason.clone()));
        }
        reasons.push(reason);
    }
    let paused = pause_closure(
        &txn,
        pool.schema(),
        pool.floors(),
        halts,
        "the staging pass halted",
        &reasons.join("; "),
    )
    .await?;
    txn.commit().await?;
    if !paused.is_empty() {
        crate::instance_log::error!(
            paused = ?paused,
            refused = ?refused,
            "the staging pass halted every definition reading or writing a table in a schema \
             its role lacks USAGE on; paused them until resumed, and leaving those tables out"
        );
    }
    Ok(unusable)
}

/// The ids of the definitions in `unfrozen` that `seed` reaches: its
/// readers (or writer), then every reader of each one's target, to a
/// fixpoint. A frozen definition is left out and not followed, since it
/// applies nothing for its own readers to see.
fn closure(catalog: &CaptureCatalog, seed: &Seed, unfrozen: &HashSet<i64>) -> BTreeSet<i64> {
    let mut members = BTreeSet::new();
    let mut tables: Vec<String> = Vec::new();
    match seed {
        Seed::Tables(seeds) => tables.extend(seeds.iter().cloned()),
        Seed::Target(target) => {
            for reader in &catalog.definitions {
                if (reader.target == *target || reader.def.target == *target)
                    && unfrozen.contains(&reader.id)
                    && members.insert(reader.id)
                {
                    tables.push(reader.target.clone());
                }
            }
        }
    }
    let mut seen = HashSet::new();
    while let Some(table) = tables.pop() {
        if !seen.insert(table.clone()) {
            continue;
        }
        for id in readers_of(catalog, &table, &BTreeSet::new(), true) {
            if unfrozen.contains(&id)
                && members.insert(id)
                && let Some(reader) = catalog.definitions.iter().find(|r| r.id == id)
            {
                tables.push(reader.target.clone());
            }
        }
    }
    members
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::columns::CaptureReader;
    use crate::defs::parse;

    /// `(source, target, text)` per definition, ids from 1.
    fn catalog(definitions: &[(&str, &str, &str)]) -> CaptureCatalog {
        CaptureCatalog {
            definitions: definitions
                .iter()
                .enumerate()
                .map(|(i, (source, target, text))| CaptureReader {
                    id: i as i64 + 1,
                    source: source.to_string(),
                    def: parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}")),
                    capture_failed: false,
                    refresh_stamp: None,
                    target: target.to_string(),
                    joined: Vec::new(),
                })
                .collect(),
        }
    }

    fn all(catalog: &CaptureCatalog) -> HashSet<i64> {
        catalog.definitions.iter().map(|r| r.id).collect()
    }

    fn tables(tables: &[&str]) -> Seed {
        Seed::Tables(tables.iter().map(|t| t.to_string()).collect())
    }

    /// A truncate with no `lsn` halts every definition reading the
    /// truncated table.
    #[test]
    fn a_truncate_without_an_lsn_seeds_its_table() {
        let err = ApplyError::TruncateWithoutLsn {
            src_table: "public.posts".to_string(),
        };
        match seed(&err) {
            Some(Seed::Tables(tables)) => assert_eq!(tables, ["public.posts"]),
            _ => panic!("a truncate without an lsn seeds its table"),
        }
    }

    /// A key seed reaches the table's direct reader and what reads its
    /// target, to any depth, and nothing else.
    #[test]
    fn a_key_seed_reaches_readers_and_their_downstream() {
        let catalog = catalog(&[
            (
                "public.users",
                "public.users_copy",
                "TRANSFORM users_copy FROM users SELECT name AS name",
            ),
            (
                "public.posts",
                "public.posts_plain",
                "TRANSFORM posts_plain FROM posts SELECT author AS author",
            ),
            (
                "public.users_copy",
                "public.copy_2",
                "TRANSFORM copy_2 FROM users_copy SELECT name AS name",
            ),
            (
                "public.copy_2",
                "public.copy_3",
                "TRANSFORM copy_3 FROM copy_2 SELECT name AS name",
            ),
            (
                "public.posts_plain",
                "public.plain_copy",
                "TRANSFORM plain_copy FROM posts_plain SELECT author AS author",
            ),
        ]);
        assert_eq!(
            closure(&catalog, &tables(&["public.users"]), &all(&catalog)),
            BTreeSet::from([1, 3, 4])
        );
    }

    /// A hop bound's seed is the cycle's tables: every definition on the
    /// cycle, and what is downstream of it, whichever table the wave ran
    /// away through.
    #[test]
    fn a_hop_bound_seed_reaches_the_cycle_and_its_downstream() {
        let catalog = catalog(&[
            ("public.a", "public.b", "TRANSFORM b FROM a SELECT v AS v"),
            ("public.b", "public.c", "TRANSFORM c FROM b SELECT v AS v"),
            ("public.c", "public.a", "TRANSFORM a FROM c SELECT v AS v"),
            ("public.c", "public.d", "TRANSFORM d FROM c SELECT v AS v"),
            ("public.x", "public.y", "TRANSFORM y FROM x SELECT v AS v"),
        ]);
        for table in ["public.a", "public.b", "public.c"] {
            assert_eq!(
                closure(&catalog, &tables(&[table]), &all(&catalog)),
                BTreeSet::from([1, 2, 3, 4]),
                "{table}"
            );
        }
    }

    /// A frozen definition is neither a member nor followed: it applies
    /// nothing, so what reads its target reads nothing new.
    #[test]
    fn a_frozen_definition_is_left_out_and_not_followed() {
        let catalog = catalog(&[
            ("public.a", "public.b", "TRANSFORM b FROM a SELECT v AS v"),
            ("public.b", "public.c", "TRANSFORM c FROM b SELECT v AS v"),
            ("public.a", "public.e", "TRANSFORM e FROM a SELECT v AS v"),
        ]);
        let unfrozen = HashSet::from([2, 3]);
        assert_eq!(
            closure(&catalog, &tables(&["public.a"]), &unfrozen),
            BTreeSet::from([3])
        );
    }

    /// What a `42501` halt seeds from, read off the catalog as the
    /// drain's own role. A source it can't select from seeds its readers; a
    /// target it can't write, or whose policies apply to it, seeds its
    /// writer; a table whose schema it can't use is named as such. A column
    /// grant counts as the privilege, and a frozen definition's tables aren't
    /// looked at.
    #[tokio::test]
    async fn refusals_name_each_table_the_role_cant_use_and_why() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let (admin, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        admin
            .batch_execute(
                "create role drainer login; \
                 create schema hidden; \
                 create table public.s (id int primary key, v int); \
                 create table public.t (id int primary key, v int); \
                 create table public.u (id int primary key, v int); \
                 create table public.w (id int primary key, v int); \
                 create table public.cols (id int primary key, v int); \
                 create table hidden.h (id int primary key, v int); \
                 create table public.f (id int primary key, v int); \
                 grant all on public.t, public.w to drainer; \
                 grant select (id, v) on public.cols to drainer; \
                 grant all on public.u to drainer; \
                 alter table public.w enable row level security; \
                 grant all on hidden.h to drainer;",
            )
            .await
            .expect("tables, and a role with some grants on them");
        let dsn = db.dsn().replace("user=postgres", "user=drainer");
        let (drainer, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect as the drain's role");
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let catalog = catalog(&[
            // 1: can't read `s`, can write `t`.
            ("public.s", "public.t", "TRANSFORM t FROM s SELECT v AS v"),
            // 2: reads `u`; `w`'s policies apply to the role.
            ("public.u", "public.w", "TRANSFORM w FROM u SELECT v AS v"),
            // 3: reads `cols` through a column grant; can't use `hidden`.
            (
                "public.cols",
                "hidden.h",
                "TRANSFORM h FROM cols SELECT v AS v",
            ),
            // 4: frozen, so `f` isn't looked at.
            ("public.f", "public.f2", "TRANSFORM f2 FROM f SELECT v AS v"),
        ]);
        let unfrozen = HashSet::from([1, 2, 3]);
        let found = refusals(&drainer, "trellis", &catalog, &unfrozen)
            .await
            .expect("read the catalog");
        let found: Vec<(String, bool, String)> = found
            .into_iter()
            .map(|r| (r.table, matches!(r.seed, Seed::Target(_)), r.reason))
            .collect();
        assert_eq!(found.len(), 3, "{found:#?}");
        assert_eq!((found[0].0.as_str(), found[0].1), ("public.s", false));
        assert!(
            found[0].2.contains("role drainer lacks SELECT on public.s"),
            "{}",
            found[0].2
        );
        assert_eq!((found[1].0.as_str(), found[1].1), ("hidden.h", true));
        assert!(
            found[1]
                .2
                .contains("role drainer lacks USAGE on schema hidden"),
            "{}",
            found[1].2
        );
        assert_eq!((found[2].0.as_str(), found[2].1), ("public.w", true));
        assert!(
            found[2]
                .2
                .contains("row-level security on target public.w applies to role drainer"),
            "{}",
            found[2].2
        );
        assert!(found[2].2.contains("docs/transforms.md"), "{}", found[2].2);
    }

    /// An aggregate the group basis can't apply names its target bare: the
    /// seed is the definition writing it, and what is downstream of it.
    #[test]
    fn a_target_seed_reaches_its_writer_and_its_downstream() {
        let catalog = catalog(&[
            (
                "public.s",
                "public.agg",
                "TRANSFORM agg FROM s GROUP BY g SELECT g AS g, SUM(v) AS total",
            ),
            (
                "public.agg",
                "public.agg_copy",
                "TRANSFORM agg_copy FROM agg SELECT total AS total",
            ),
            ("public.s", "public.t", "TRANSFORM t FROM s SELECT v AS v"),
        ]);
        for target in ["agg", "public.agg"] {
            assert_eq!(
                closure(&catalog, &Seed::Target(target.to_string()), &all(&catalog)),
                BTreeSet::from([1, 2]),
                "{target}"
            );
        }
    }

    /// A refused read's halt pauses every episode's closure under one sorted
    /// lock of their rows, first (ADR-0028 I5, #2094), not episode by
    /// episode: refusals go table by table in name order, so `a`'s reader,
    /// the higher id, comes before `z`'s, the lower one, and pausing it first
    /// would hold its row while waiting on the lower one. Frozen with the
    /// lower id's row held by another transaction, which then takes the
    /// higher id's, as a sorted taker of both would: had the pauses gone
    /// episode by episode, the two would deadlock.
    #[tokio::test]
    async fn a_refusal_halt_locks_every_episodes_closure_in_id_order() {
        use std::collections::HashMap;

        use crate::defs::ast::ValueType;

        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("config");
        let pool = crate::pool::Pool::new(&config).expect("pool");
        let schema = crate::config::DEFAULT_SCHEMA;
        let connect = |dsn: String| async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!("set search_path to {schema}, public"))
                .await
                .expect("search path");
            client
        };
        let admin = connect(db.dsn().to_string()).await;
        admin
            .batch_execute(&format!(
                "create table public.z (id bigint primary key, x numeric); \
                 create table public.a (id bigint primary key, x numeric); \
                 create role drainer login; \
                 grant usage on schema {schema} to drainer; \
                 grant all on all tables in schema {schema} to drainer; \
                 grant all on all sequences in schema {schema} to drainer"
            ))
            .await
            .expect("sources, and a role that can't read them");
        let columns: HashMap<String, ValueType> = ["id", "x"]
            .iter()
            .map(|name| (name.to_string(), ValueType::Numeric))
            .collect();
        let mut ids = Vec::new();
        for text in [
            "TRANSFORM low FROM public.z SELECT x AS v",
            "TRANSFORM high FROM public.a SELECT x AS v",
        ] {
            let def = crate::defs::catalog::create_definition(&pool, text, &columns)
                .await
                .expect(text);
            ids.push(def.id);
        }
        let (low, high) = (ids[0], ids[1]);
        assert!(low < high);
        admin
            .execute(
                "update transform_definitions set status = 'live' where id = any($1)",
                &[&ids],
            )
            .await
            .expect("live");

        let mut taker = connect(db.dsn().to_string()).await;
        let other = taker.transaction().await.expect("txn");
        other
            .execute(
                "select 1 from transform_definitions where id = $1 for update",
                &[&low],
            )
            .await
            .expect("lock low");
        let other_pid: i32 = other
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let mut pauser = connect(db.dsn().replace("user=postgres", "user=drainer")).await;
        let pauser_pid: i32 = pauser
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let floors = pool.floors().clone();
        let pausing = tokio::spawn(async move {
            let txn = pauser.transaction().await.expect("txn");
            let paused = pause_closure(
                &txn,
                schema,
                &floors,
                vec![(None, "denied".to_string())],
                "the drain halted",
                "denied",
            )
            .await;
            if paused.is_ok() {
                txn.commit().await.expect("commit");
            }
            paused
        });
        // Wait for the pauses to queue behind `low`'s row: a lock state,
        // which they reach once, not a convergence.
        loop {
            let blocked: bool = admin
                .query_one(
                    "select $2 = any(pg_blocking_pids($1))",
                    &[&pauser_pid, &other_pid],
                )
                .await
                .expect("blocking pids")
                .get(0);
            if blocked {
                break;
            }
            assert!(
                !pausing.is_finished(),
                "the pauses finished without queuing behind the low reader's row"
            );
            tokio::task::yield_now().await;
        }
        other
            .execute(
                "select 1 from transform_definitions where id = $1 for update",
                &[&high],
            )
            .await
            .expect("the pauses hold no row past the one they wait on");
        other.commit().await.expect("commit");
        let paused = pausing.await.expect("pause task").expect("pauses");
        assert_eq!(paused, ["low", "high"], "both paused, in id order");
    }
}
