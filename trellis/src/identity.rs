//! Instance identity: deciding, before any migration touches a schema,
//! whether attaching to it is safe.
//!
//! Because several independent Trellis instances can share one database
//! cluster — even one database — differing only in `config.schema()` (see
//! `docs/instance-identity.md`), attaching to a schema needs to distinguish
//! three cases:
//!
//! 1. **Fresh (or mid-migration on our own behalf).** The schema doesn't
//!    exist yet, exists but is empty, or exists with Trellis's own
//!    migration ledger (`refinery_schema_history`) but no marker yet — the
//!    last of these is a crash between the runner creating V9's table and
//!    [`seed_marker`] committing, and must be exactly as attachable as a
//!    fresh schema, or a crashed first-ever migrate would permanently lock
//!    itself out. Ours to take/resume.
//! 2. **Same instance.** The schema already carries our marker
//!    (`trellis_instance`) recording this exact schema name and a format
//!    version we understand — a clean, idempotent re-attach.
//! 3. **Someone else's.** The marker records a *different* schema name (a
//!    marker was copied/restored under the wrong name), the marker records
//!    a format version newer than this build understands, or the schema
//!    pre-existed with foreign objects and neither a marker nor a Trellis
//!    migration ledger (not ours, don't clobber it). All three are refused
//!    with [`Error::IncompatibleInstance`].
//!
//! [`prepare_attach`] runs this decision *before* [`crate::migrate::migrate`]
//! hands off to the refinery runner, and [`seed_marker`] runs after the
//! runner *unconditionally* (an idempotent upsert, not gated on a "fresh"
//! flag) — see both functions' docs for why that matters for crash recovery
//! and for two instances racing a first-ever attach.

use crate::config::Config;
use crate::error::Error;
use crate::pool::quote_ident;

/// The on-disk shape/meaning of the `trellis_instance` marker. Bump this
/// when a future change to the marker (or to what attaching means) isn't
/// safely interpretable by old code — a schema whose marker records a
/// version newer than this constant is refused rather than guessed at. Only
/// one version has ever existed, so there's no migration path defined yet;
/// add one alongside the first version bump.
pub const INSTANCE_FORMAT_VERSION: i32 = 1;

/// A resolved instance identity, for reporting (see [`Config`]'s
/// [`std::fmt::Display`] impl for the short form; this is the structured
/// version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The schema this instance operates in.
    pub schema: String,
    /// The instance-identity marker format this build writes/expects. See
    /// [`INSTANCE_FORMAT_VERSION`].
    pub format_version: i32,
}

impl Identity {
    /// The identity this build of Trellis would attach to `config` as.
    /// Doesn't touch the database — see [`prepare_attach`] for the
    /// database-backed check.
    pub fn resolved(config: &Config) -> Self {
        Self {
            schema: config.schema().to_string(),
            format_version: INSTANCE_FORMAT_VERSION,
        }
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema {:?}, instance format version {}",
            self.schema, self.format_version
        )
    }
}

/// Ensures `config.schema()` exists and is safe to attach to, refusing it
/// otherwise (see the module docs for the three cases).
///
/// Deliberately does *not* return a "fresh vs. re-attach" flag: an earlier
/// version of this function did, and gated [`seed_marker`] on it, which
/// meant a process death between the runner creating `trellis_instance` and
/// the (then-conditional) seed insert committing left a schema with our
/// ledger and tables but no marker — a state this same function would then
/// refuse to touch again, permanently. Marker seeding is unconditional now
/// (see [`seed_marker`]), so no flag is needed here.
pub(crate) async fn prepare_attach(
    client: &mut tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let schema = config.schema();
    let existed_before = schema_exists(client, schema).await?;

    // Fully qualified (not relying on `search_path`) because the schema
    // itself may not exist yet on a brand-new database.
    client
        .batch_execute(&format!(
            "create schema if not exists {}",
            quote_ident(schema)
        ))
        .await?;

    match read_marker(client, schema).await? {
        Some(marker) => {
            if marker.schema_name != schema {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} is already claimed by a different Trellis instance \
                     (its marker records schema {:?}); refusing to attach",
                    marker.schema_name
                )));
            }
            if marker.instance_format_version > INSTANCE_FORMAT_VERSION {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} was written by a newer Trellis (instance format \
                     version {}); this build only understands up to version {INSTANCE_FORMAT_VERSION}",
                    marker.instance_format_version
                )));
            }
            // Same schema, a format version we understand: a clean re-attach.
            Ok(())
        }
        None => {
            // No marker yet doesn't necessarily mean "foreign": it's also
            // the state of a schema that's ours but mid-migration (crashed
            // between the runner creating V9's table and `seed_marker`
            // committing — or, before V9 even ran, between an earlier
            // migration committing and the crash). `refinery_schema_history`
            // is refinery's own ledger table, created by the very first
            // migration it applies, so its presence is a reliable signal
            // that this schema is (or was becoming) a Trellis instance
            // rather than something foreign. Only refuse when the schema
            // pre-existed, has at least one table, AND has neither marker
            // nor ledger — genuinely foreign, nothing of ours to resume.
            if existed_before
                && schema_has_any_tables(client, schema).await?
                && !schema_has_refinery_ledger(client, schema).await?
            {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} already exists with objects that aren't a Trellis \
                     instance (no identity marker or migration ledger found); refusing to \
                     take it over"
                )));
            }
            // Brand new, pre-existing but empty, or ours mid-migration:
            // safe to (re)take. `seed_marker` will (re)seed the marker
            // after the runner has ensured `trellis_instance` exists.
            Ok(())
        }
    }
}

/// Seeds the identity marker, unconditionally, after the migration runner
/// has ensured `trellis_instance` exists. `on conflict (singleton) do
/// nothing` rather than a plain insert, for two reasons:
///
/// - **Crash recovery.** If a prior attach crashed after the runner created
///   `trellis_instance` but before this insert committed, the table exists
///   but is empty; this call fills it in, exactly as if this were the first
///   successful attach. Running it on every `migrate()` call (not just
///   "fresh" ones, per the old, now-removed flag) means there's no separate
///   "did we already seed this" state to get wrong.
/// - **Concurrent first attach.** Two processes racing to migrate the same
///   brand-new schema both reach this point; `singleton`'s primary key
///   would turn a second plain insert into a `Error::Connect` (a unique
///   violation), when the right outcome is a silent no-op for the loser —
///   its row already matches (same schema, same version) since they're
///   attaching to the same config.
pub(crate) async fn seed_marker(
    client: &mut tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let schema = config.schema();
    client
        .execute(
            "insert into trellis_instance (schema_name, instance_format_version) \
             values ($1, $2) \
             on conflict (singleton) do nothing",
            &[&schema, &INSTANCE_FORMAT_VERSION],
        )
        .await?;
    Ok(())
}

struct Marker {
    schema_name: String,
    instance_format_version: i32,
}

/// Whether `schema` exists at all, independent of `search_path` (this must
/// work before the schema is created, and information_schema views aren't
/// scoped by `search_path` anyway).
async fn schema_exists(client: &tokio_postgres::Client, schema: &str) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from pg_namespace where nspname = $1)",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Whether `schema` contains any tables at all (used only to help
/// distinguish an empty pre-existing schema, which is safe to take over,
/// from one with foreign objects, which isn't — see
/// [`schema_has_refinery_ledger`] for the other half of that distinction).
async fn schema_has_any_tables(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from information_schema.tables where table_schema = $1)",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Whether `schema` contains refinery's own migration ledger
/// (`refinery_schema_history`). That table is created by the very first
/// migration refinery ever applies in a schema, so finding it — even
/// without a `trellis_instance` marker — means this schema is a Trellis
/// instance that crashed partway through its first-ever migrate, not a
/// foreign schema. Schema-qualified explicitly rather than relying on
/// `search_path`, matching [`read_marker`].
async fn schema_has_refinery_ledger(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from information_schema.tables \
             where table_schema = $1 and table_name = 'refinery_schema_history')",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Reads the identity marker from `schema`, if its `trellis_instance` table
/// exists yet *and has a row*. Both "table doesn't exist" (every fresh
/// attach) and "table exists but is empty" (a crash between the runner
/// creating it and [`seed_marker`] committing) read as `None` — a schema
/// we're safe to (re)seed, not an error — so this uses `query_opt`, not
/// `query_one`, on both the existence probe's absence and the row lookup:
/// a `query_one` against a zero-row `trellis_instance` would surface as an
/// opaque `Error::Connect` rather than the "no marker yet" case it actually
/// is.
async fn read_marker(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<Option<Marker>, Error> {
    let exists_row = client
        .query_one(
            "select exists(select 1 from information_schema.tables \
             where table_schema = $1 and table_name = 'trellis_instance')",
            &[&schema],
        )
        .await?;
    let table_exists: bool = exists_row.get(0);
    if !table_exists {
        return Ok(None);
    }

    // Qualified with the schema explicitly rather than relying on
    // `search_path`: this runs on the pool's shared client, whose
    // `search_path` is pinned to `config.schema()` already in the normal
    // case, but being explicit here keeps this function correct regardless.
    let row = client
        .query_opt(
            &format!(
                "select schema_name, instance_format_version from {}.trellis_instance",
                quote_ident(schema)
            ),
            &[],
        )
        .await?;
    Ok(row.map(|row| Marker {
        schema_name: row.get(0),
        instance_format_version: row.get(1),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_resolved_matches_config_schema() {
        let config = Config::from_dsn("postgresql://example/db").expect("valid default schema");
        let identity = Identity::resolved(&config);
        assert_eq!(identity.schema, config.schema());
        assert_eq!(identity.format_version, INSTANCE_FORMAT_VERSION);
    }

    #[test]
    fn identity_display_is_human_readable() {
        let identity = Identity {
            schema: "trellis".to_string(),
            format_version: 1,
        };
        assert_eq!(
            identity.to_string(),
            "schema \"trellis\", instance format version 1"
        );
    }
}
