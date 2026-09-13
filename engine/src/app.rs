//! The Trellis client facade — the one interface an embedding application is
//! meant to use.
//!
//! Everything an embedder needs to stand up and run a Trellis instance hangs
//! off [`Trellis`]: apply migrations, register relationships and transform
//! definitions, list what's registered, request an ad-hoc backfill, and run
//! the live CDC/apply pipeline. It composes the lower-level building blocks
//! (`defs`, `client`, `intake`, `staging`) into one coherent surface so
//! callers never have to stitch those together themselves — and, critically,
//! never accidentally pick the wrong path: definition registration always
//! goes through [`defs::install_definition`], the fast direct-build entry
//! point that also records backfill coverage, rather than the lower-level
//! `create_definition`/`create_target_table` primitives it's built from.
//!
//! The individual `defs::*`/`intake::*`/`staging::*` items remain public for
//! internal harnesses (the benchmark and generative-test crates use them as
//! an oracle/testbench), but embedders should treat [`Trellis`] as *the*
//! interface and reach past it only when they genuinely need a primitive it
//! doesn't expose.
//!
//! [`TrellisError::code`] reports a stable, coarse [`ErrorCode`] category
//! for any failure this facade can return, on top of the existing `Display`
//! message — settled ahead of issue #87's FFI embedding work so a host
//! language on the other side of that boundary has something stable to
//! match on instead of every internal Rust error variant (`docs/public-api-design.md`,
//! decision 3).
//!
//! **[`define`](Trellis::define) doesn't block on backfill.** Per
//! `docs/public-api-design.md`'s decision 1 and
//! [ADR-0007's amendment](../../docs/decisions/0007-direct-set-based-backfill.md#backgrounding-and-resumability-amendment),
//! a plain (non-relationship) 1-1 transform's initial backfill runs as a
//! durable, claimable queue of chunks that running drain
//! (`application_threads`) workers execute — anywhere in the fleet, not
//! necessarily on the connection that called `define()`. `define()` itself
//! returns once the definition is registered and that chunk work is
//! enumerated/persisted, with [`TransformStatus::Backfilling`]; callers that
//! need the target actually populated poll [`Trellis::status`] until it
//! reports [`TransformStatus::Live`] — which requires *some* client in the
//! fleet to be running with `drain_threads > 0` (a `define`-only connection,
//! with no such client anywhere, leaves the transform queued indefinitely).
//! A relationship-enriched 1-1 transform (like the `count(posts.id)` example
//! below) or an aggregate (`GROUP BY`) transform still builds fully
//! synchronously in-call today — see `engine::defs::backfill`'s module docs
//! for why those two shapes aren't chunked into the durable queue yet.
//!
//! # Lifecycle
//!
//! ```no_run
//! # async fn example() -> Result<(), engine::TrellisError> {
//! use engine::{Config, Trellis, TrellisOptions, TransformStatus};
//!
//! // Define transforms with no runtime attached.
//! let trellis = Trellis::connect(Config::resolve(None)?, TrellisOptions::default()).await?;
//! trellis.migrate().await?;
//! trellis
//!     .define_relationship("RELATIONSHIP posts FROM authors.id TO posts.author")
//!     .await?;
//! trellis
//!     .define("TRANSFORM authors_calc FROM authors SELECT count(posts.id) AS post_count")
//!     .await?;
//!
//! // Separately, run the live pipeline: staging worker + two drain threads.
//! // Drain threads are also what finish any queued backfill chunk work, for
//! // a plain 1-1 transform `define()` returned before fully building.
//! let running = Trellis::connect(
//!     Config::resolve(None)?,
//!     TrellisOptions { staging: true, drain_threads: 2 },
//! )
//! .await?;
//! // Poll until every registered transform is done backfilling.
//! while running.status("authors_calc").await? != Some(TransformStatus::Live) {
//!     // ... sleep, then re-check ...
//! }
//! // ... run until shutdown ...
//! running.shutdown().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::SystemTime;

use crate::client::{Client, ClientError, ClientOptions};
use crate::config::Config;
use crate::defs::{
    self, CatalogError, Definition, ParseError, RelationshipDefinition, TransformStatus, ValueType,
};
use crate::error_code::{self, ErrorCode};
use crate::pool::Pool;

/// Options a client sets when it [`connect`](Trellis::connect)s.
///
/// The two knobs mirror [`ClientOptions`]'s core contract (see its doc
/// comment): whether this connection owns CDC intake + ring maintenance, and
/// how many drain (application) workers it runs. A connection that only
/// defines transforms leaves both at their defaults (nothing background
/// starts); a connection that runs the live pipeline sets `staging` and a
/// non-zero `drain_threads`.
#[derive(Debug, Clone, Default)]
pub struct TrellisOptions {
    /// Whether this connection runs the CDC subscriber and ring maintenance
    /// (the staging worker). Exactly one connection in a fleet should set
    /// this. When set, [`Trellis::connect`] derives the source-table set to
    /// publish from the catalog, so at least one definition must already be
    /// registered.
    pub staging: bool,
    /// How many drain (application) worker threads this connection runs. Zero
    /// (the default) runs none.
    pub drain_threads: usize,
}

/// A connected Trellis instance — see the [module docs](self).
///
/// Holds a connection pool and, when [`TrellisOptions`] asked for it, a
/// running background [`Client`] (staging worker and/or drain workers). Drop
/// or [`shutdown`](Trellis::shutdown) stops that background work.
pub struct Trellis {
    config: Config,
    pool: Pool,
    /// `Some` iff `options.staging || options.drain_threads > 0` — the live
    /// pipeline this connection started.
    client: Option<Client>,
}

impl Trellis {
    /// Connects to the database `config` names and, if `options` asks for any
    /// background work, starts it before returning.
    ///
    /// With the default options nothing background runs — the returned handle
    /// is purely for defining transforms/relationships and inspecting the
    /// catalog. With `staging` set, the source-table set is derived from the
    /// registered definitions and CDC intake + ring maintenance start; with a
    /// non-zero `drain_threads`, that many application workers start.
    pub async fn connect(config: Config, options: TrellisOptions) -> Result<Self, TrellisError> {
        let pool = Pool::new(&config)?;
        let client = if options.staging || options.drain_threads > 0 {
            Some(Self::start_client(&config, &pool, &options).await?)
        } else {
            None
        };
        Ok(Self {
            config,
            pool,
            client,
        })
    }

    /// The connection pool backing this instance. Exposed for callers that
    /// need to run their own queries against target tables; not needed for
    /// anything [`Trellis`]'s own methods already cover.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    /// The resolved configuration (schema names, DSN) this instance connected
    /// with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Applies Trellis's schema migrations. Idempotent — safe to call on
    /// every startup.
    pub async fn migrate(&self) -> Result<(), TrellisError> {
        crate::migrate(&self.pool, &self.config)
            .await
            .map_err(TrellisError::Engine)
    }

    /// Registers a transform definition and creates its target table.
    ///
    /// Introspects the source table's columns for the validator, then routes
    /// through [`defs::install_definition`] — the fast direct-build path.
    ///
    /// **Returns before backfill finishes** for a plain (non-relationship)
    /// 1-1 transform (see this module's doc comment): the returned
    /// [`Definition`] reports [`TransformStatus::Backfilling`], and the
    /// target is populated in the background by whichever drain
    /// (`application_threads`) workers are running in the fleet, not by this
    /// call. Poll [`Trellis::status`] for [`TransformStatus::Live`] once you
    /// need the target's contents. A relationship-enriched 1-1 or an
    /// aggregate (`GROUP BY`) transform still builds synchronously — the
    /// returned [`Definition`] already reports [`TransformStatus::Live`] for
    /// those two shapes.
    pub async fn define(&self, definition_text: &str) -> Result<Definition, TrellisError> {
        let parsed = defs::parse(definition_text)?;
        let source_columns = self.source_columns(&parsed.source).await?;
        defs::install_definition(
            &self.pool,
            definition_text,
            &source_columns,
            self.config.target_schema(),
        )
        .await
        .map_err(TrellisError::Catalog)
    }

    /// Registers a relationship declaration (ADR-0006) — the standalone
    /// `RELATIONSHIP <name> FROM <table>.<col> TO <table>.<col>` form a later
    /// [`define`](Trellis::define) can reference in a calculated field.
    pub async fn define_relationship(
        &self,
        definition_text: &str,
    ) -> Result<RelationshipDefinition, TrellisError> {
        defs::create_relationship(&self.pool, definition_text)
            .await
            .map_err(TrellisError::Catalog)
    }

    /// Every registered transform definition, oldest first.
    pub async fn definitions(&self) -> Result<Vec<DefinitionSummary>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select id, target_table, source_table, source_version, status, created_at \
                 from transform_definitions order by id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let status_text: String = row.get(4);
                let status = TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                    panic!("transform_definitions.status held unrecognized value '{status_text}'")
                });
                DefinitionSummary {
                    id: row.get(0),
                    target_table: row.get(1),
                    source_table: row.get(2),
                    source_version: row.get(3),
                    status,
                    created_at: row.get(5),
                }
            })
            .collect())
    }

    /// One registered transform definition's current [`TransformStatus`]
    /// (issue #55), by target table name — the read a host-language embedder
    /// polls after [`define`](Trellis::define) returns, per
    /// `docs/public-api-design.md`'s decision 1 ("define, then poll status
    /// until live"). A thin convenience over [`definitions`](Trellis::definitions)
    /// for callers that only want one row rather than the full list.
    pub async fn status(
        &self,
        target_table: &str,
    ) -> Result<Option<TransformStatus>, TrellisError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "select status from transform_definitions where target_table = $1",
                &[&target_table],
            )
            .await?;
        Ok(row.map(|row| {
            let status_text: String = row.get(0);
            TransformStatus::from_persisted(&status_text).unwrap_or_else(|| {
                panic!("transform_definitions.status held unrecognized value '{status_text}'")
            })
        }))
    }

    /// Every registered relationship declaration, oldest first.
    pub async fn relationships(&self) -> Result<Vec<RelationshipSummary>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select id, name, from_table, from_col, to_table, to_col, cardinality, created_at \
                 from relationship_definitions order by id",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| RelationshipSummary {
                id: row.get(0),
                name: row.get(1),
                from_table: row.get(2),
                from_col: row.get(3),
                to_table: row.get(4),
                to_col: row.get(5),
                cardinality: row.get(6),
                created_at: row.get(7),
            })
            .collect())
    }

    /// Re-stages `source_table`'s current rows via a `pending_backfill`
    /// marker, so a transform registered *after* the table already joined the
    /// replication publication gets its own backfill. Only valid for a table
    /// that's already a publication member — a never-published table is
    /// backfilled in full on first contact by the running staging worker, so
    /// this is refused there (and the running staging worker must discharge
    /// the marker).
    pub async fn request_backfill(&self, source_table: &str) -> Result<(), TrellisError> {
        let client = self.pool.get().await?;
        let schema_rows = client
            .query(
                "select table_schema from information_schema.tables \
                 where table_name = $1 and table_schema = any(current_schemas(false))",
                &[&source_table],
            )
            .await?;
        let schema: String = schema_rows
            .first()
            .ok_or_else(|| TrellisError::SourceTableNotFound(source_table.to_string()))?
            .get(0);
        let qualified = format!("{schema}.{source_table}");

        let publication = ClientOptions::default().publication;
        let already_published: bool = client
            .query_one(
                "select exists(select 1 from pg_publication_tables \
                 where pubname = $1 and schemaname = $2 and tablename = $3)",
                &[&publication, &schema, &source_table],
            )
            .await?
            .get(0);
        if !already_published {
            return Err(TrellisError::TableNotPublished {
                table: qualified,
                publication,
            });
        }

        client
            .execute(
                "insert into pending_backfill (table_name, fence_snapshot) \
                 values ($1, pg_current_snapshot()) \
                 on conflict (table_name) \
                 do update set fence_snapshot = excluded.fence_snapshot, added_at = now()",
                &[&qualified],
            )
            .await?;
        Ok(())
    }

    /// Poison-quarantine entries recorded since `watermark`, oldest first —
    /// the keys the apply path gave up on. A running client surfaces these so
    /// a whole-table failure (every row poisoned) doesn't sit silently.
    pub async fn poisoned_since(
        &self,
        watermark: SystemTime,
    ) -> Result<Vec<PoisonEntry>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select src_table, key, last_error, poisoned_at \
                 from poison where poisoned_at > $1 order by poisoned_at",
                &[&watermark],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| PoisonEntry {
                src_table: row.get(0),
                key: row.get(1),
                last_error: row.get(2),
                poisoned_at: row.get(3),
            })
            .collect())
    }

    /// Stops any background work this connection started (staging worker and
    /// drain workers) and waits for it to exit cleanly. A no-op for a
    /// connection that started none.
    pub async fn shutdown(self) -> Result<(), TrellisError> {
        if let Some(client) = self.client {
            client.shutdown().await.map_err(TrellisError::Client)?;
        }
        Ok(())
    }

    /// Starts the background [`Client`] for a `staging`/`drain_threads`
    /// connection. When staging, derives the source-table set from the
    /// catalog (a staging worker needs it non-empty).
    async fn start_client(
        config: &Config,
        pool: &Pool,
        options: &TrellisOptions,
    ) -> Result<Client, TrellisError> {
        let source_tables = if options.staging {
            let tables = qualified_source_tables(pool).await?;
            if tables.is_empty() {
                return Err(TrellisError::NoDefinitions);
            }
            tables
        } else {
            Vec::new()
        };

        let client_options = ClientOptions {
            staging_worker: options.staging,
            application_threads: options.drain_threads,
            source_tables,
            ..Default::default()
        };
        Client::start(config.dsn(), client_options).map_err(TrellisError::Client)
    }

    /// Introspects `source_table`'s column names and types for the definition
    /// validator, the same `information_schema` read
    /// [`defs::install_definition`] expects its caller to supply.
    async fn source_columns(
        &self,
        source_table: &str,
    ) -> Result<HashMap<String, ValueType>, TrellisError> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "select column_name, data_type from information_schema.columns \
                 where table_name = $1 and table_schema = any(current_schemas(false))",
                &[&source_table],
            )
            .await?;

        if rows.is_empty() {
            return Err(TrellisError::SourceTableNotFound(source_table.to_string()));
        }

        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let column_name: String = row.get(0);
                let data_type: String = row.get(1);
                pg_value_type(&data_type).map(|value_type| (column_name, value_type))
            })
            .collect())
    }
}

/// The full transitive closure of source tables reachable from every
/// registered definition — each definition's direct anchor table plus every
/// relationship `to_table` reachable from one (see
/// [`defs::all_source_tables`]) — each qualified as `"schema.table"` for
/// [`ClientOptions::source_tables`]. `all_source_tables` returns bare table
/// names, so this resolves each one's schema off `information_schema`.
///
/// `pub` (rather than `pub(crate)`) only so `engine/tests/app.rs` can exercise
/// it directly as `engine::app::qualified_source_tables`; not re-exported
/// from the crate root, so it isn't part of [`Trellis`]'s public surface.
pub async fn qualified_source_tables(pool: &Pool) -> Result<Vec<String>, TrellisError> {
    let client = pool.get().await?;
    let tables = defs::all_source_tables(pool).await?;

    let mut qualified = Vec::with_capacity(tables.len());
    for source_table in tables {
        let schema_rows = client
            .query(
                "select table_schema from information_schema.tables \
                 where table_name = $1 and table_schema = any(current_schemas(false))",
                &[&source_table],
            )
            .await?;
        let schema: String = schema_rows
            .first()
            .ok_or_else(|| TrellisError::SourceTableNotFound(source_table.clone()))?
            .get(0);
        qualified.push(format!("{schema}.{source_table}"));
    }
    Ok(qualified)
}

/// Maps a Postgres `information_schema.columns.data_type` string to the
/// [`ValueType`] the definition validator understands. A column of a type not
/// mapped here is simply omitted from the validator's view (the same behavior
/// the POC's own introspection had).
fn pg_value_type(data_type: &str) -> Option<ValueType> {
    match data_type {
        "smallint" | "integer" | "bigint" | "numeric" | "real" | "double precision" => {
            Some(ValueType::Numeric)
        }
        "text" | "character varying" | "character" | "citext" => Some(ValueType::Text),
        "boolean" => Some(ValueType::Boolean),
        _ => None,
    }
}

/// One registered transform definition, as [`Trellis::definitions`] reports
/// it.
#[derive(Debug, Clone)]
pub struct DefinitionSummary {
    pub id: i64,
    pub target_table: String,
    pub source_table: String,
    pub source_version: i64,
    /// Where this transform is in its lifecycle (issue #55) — see
    /// [`TransformStatus`].
    pub status: TransformStatus,
    pub created_at: SystemTime,
}

/// One registered relationship declaration, as [`Trellis::relationships`]
/// reports it. `cardinality` is the persisted `"to_one"`/`"to_many"` string.
#[derive(Debug, Clone)]
pub struct RelationshipSummary {
    pub id: i64,
    pub name: String,
    pub from_table: String,
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
    pub cardinality: String,
    pub created_at: SystemTime,
}

/// One poison-quarantine entry, as [`Trellis::poisoned_since`] reports it.
#[derive(Debug, Clone)]
pub struct PoisonEntry {
    pub src_table: String,
    pub key: String,
    pub last_error: String,
    pub poisoned_at: SystemTime,
}

/// Why a [`Trellis`] operation failed. Composes the crate's lower-level error
/// types via `From`, matching the hand-rolled-enum convention the rest of the
/// crate uses. [`TrellisError::code`] reports a stable, coarse [`ErrorCode`]
/// category for this error alongside its `Display` message — see
/// `docs/public-api-design.md`, decision 3.
#[derive(Debug)]
pub enum TrellisError {
    /// A definition failed to parse before it could be registered.
    Parse(ParseError),
    /// A catalog operation (define/relationship/install) failed.
    Catalog(CatalogError),
    /// Starting or stopping the background client failed.
    Client(ClientError),
    /// A connection/config/migration-layer failure.
    Engine(crate::error::Error),
    /// A direct Postgres query this facade runs itself (introspection,
    /// listing, backfill request) failed.
    Db(tokio_postgres::Error),
    /// `staging` was requested but no transform definitions are registered,
    /// so there is nothing to publish or stream.
    NoDefinitions,
    /// A named source table doesn't resolve on the connection's search path.
    SourceTableNotFound(String),
    /// [`Trellis::request_backfill`] was asked to backfill a table that isn't
    /// a member of the publication yet.
    TableNotPublished { table: String, publication: String },
}

impl TrellisError {
    /// This error's stable, coarse [`ErrorCode`] category
    /// (`docs/public-api-design.md`, decision 3). Delegates to the wrapped
    /// error's own `code()` wherever one nests here
    /// ([`TrellisError::Parse`], [`TrellisError::Catalog`],
    /// [`TrellisError::Client`], [`TrellisError::Engine`]) rather than
    /// hardcoding one category for a whole variant, so the mapping composes
    /// through nesting instead of re-deriving a category this crate already
    /// has one for.
    pub fn code(&self) -> ErrorCode {
        match self {
            TrellisError::Parse(err) => err.code(),
            TrellisError::Catalog(err) => err.code(),
            TrellisError::Client(err) => err.code(),
            TrellisError::Engine(err) => err.code(),
            TrellisError::Db(err) => error_code::classify_pg_error(err),
            // `staging` requested with nothing registered, or a backfill
            // request against an unpublished table, are both rejected calls
            // given the connection's current state — same category as any
            // other invalid-configuration error.
            TrellisError::NoDefinitions | TrellisError::TableNotPublished { .. } => {
                ErrorCode::Validation
            }
            TrellisError::SourceTableNotFound(_) => ErrorCode::NotFound,
        }
    }
}

impl std::fmt::Display for TrellisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrellisError::Parse(err) => write!(f, "{err}"),
            TrellisError::Catalog(err) => write!(f, "{err}"),
            TrellisError::Client(err) => write!(f, "{err}"),
            TrellisError::Engine(err) => write!(f, "{err}"),
            TrellisError::Db(err) => {
                write!(f, "database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            TrellisError::NoDefinitions => write!(
                f,
                "no transform definitions registered; define one before running with staging \
                 enabled"
            ),
            TrellisError::SourceTableNotFound(table) => {
                write!(f, "source table \"{table}\" not found on the search path")
            }
            TrellisError::TableNotPublished { table, publication } => write!(
                f,
                "\"{table}\" isn't in publication \"{publication}\" yet; run with staging enabled \
                 and it will be backfilled automatically on first contact"
            ),
        }
    }
}

impl std::error::Error for TrellisError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TrellisError::Parse(err) => Some(err),
            TrellisError::Catalog(err) => Some(err),
            TrellisError::Client(err) => Some(err),
            TrellisError::Engine(err) => Some(err),
            TrellisError::Db(err) => Some(err),
            TrellisError::NoDefinitions
            | TrellisError::SourceTableNotFound(_)
            | TrellisError::TableNotPublished { .. } => None,
        }
    }
}

impl From<ParseError> for TrellisError {
    fn from(err: ParseError) -> Self {
        TrellisError::Parse(err)
    }
}

impl From<CatalogError> for TrellisError {
    fn from(err: CatalogError) -> Self {
        TrellisError::Catalog(err)
    }
}

impl From<ClientError> for TrellisError {
    fn from(err: ClientError) -> Self {
        TrellisError::Client(err)
    }
}

impl From<crate::error::Error> for TrellisError {
    fn from(err: crate::error::Error) -> Self {
        TrellisError::Engine(err)
    }
}

impl From<tokio_postgres::Error> for TrellisError {
    fn from(err: tokio_postgres::Error) -> Self {
        TrellisError::Db(err)
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::*;

    #[test]
    fn no_definitions_is_validation() {
        assert_eq!(TrellisError::NoDefinitions.code(), ErrorCode::Validation);
    }

    #[test]
    fn source_table_not_found_is_not_found() {
        assert_eq!(
            TrellisError::SourceTableNotFound("widgets".to_string()).code(),
            ErrorCode::NotFound
        );
    }

    /// [`TrellisError::Catalog`] must delegate to [`CatalogError::code`]
    /// rather than hardcoding a category — the exact composition-through-
    /// nesting case `docs/public-api-design.md`'s decision 3 calls out.
    #[test]
    fn catalog_delegates_to_the_wrapped_catalog_error() {
        let inner = CatalogError::SourceTableNotFound("orders".to_string());
        let expected = inner.code();
        let wrapped = TrellisError::Catalog(inner);

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::NotFound);
    }

    /// Two layers of nesting: [`TrellisError::Client`] wraps
    /// [`ClientError::Config`], which itself wraps [`crate::error::Error`] —
    /// the code must survive both hops unchanged.
    #[test]
    fn client_delegates_through_two_layers_of_nesting() {
        let inner = crate::error::Error::IncompatibleInstance("mismatched marker".to_string());
        let expected = inner.code();
        let wrapped = TrellisError::Client(ClientError::Config(inner));

        assert_eq!(wrapped.code(), expected);
        assert_eq!(wrapped.code(), ErrorCode::Conflict);
    }
}
