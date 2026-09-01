//! Connection pool management, built on `deadpool-postgres`.
//!
//! [`Pool::new`] wires up a `deadpool_postgres::Pool` from a resolved
//! [`Config`] and attaches a per-connection session bootstrap hook (via
//! deadpool's `post_create` hook). That hook pins `search_path` to the
//! configured Trellis schema followed by the configured target schema (see
//! `Config::target_schema`), so migrations and staging objects land in the
//! Trellis schema and transform target tables resolve without every query
//! needing to qualify table names, while [`crate::defs::ddl`] still
//! schema-qualifies target-table DDL explicitly (`search_path` only decides
//! where an *unqualified* `CREATE TABLE` lands, and that must be the
//! Trellis schema, not the target schema, for the rest of this module's
//! unqualified references to Trellis's own objects to keep resolving
//! correctly).
//!
//! This hook is also the seam intake will extend: it's the one place that
//! runs exactly once per physical connection, before it's ever handed to a
//! caller, which is where `synchronous_commit = on` will need to be enforced
//! for the staging ring to make its durability guarantees.
//! Nothing beyond `search_path` is enforced here yet.

use crate::config::Config;
use crate::error::Error;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Pool as DeadpoolPool, RecyclingMethod,
};
use std::str::FromStr;
use tokio_postgres::NoTls;

/// A pooled connection, handed out by [`Pool::get`].
pub type Client = deadpool_postgres::Client;

/// Trellis's connection pool.
///
/// Connections are unencrypted (`NoTls`) for now; TLS is out of scope for
/// this issue and can be layered on by swapping the `NoTls` connector below
/// for a real one once a TLS approach is chosen.
///
/// `Clone` is cheap: `deadpool_postgres::Pool` is itself `Arc`-backed, so a
/// clone shares the same underlying pool of physical connections rather than
/// opening a second one. [`crate::Client`]'s app-worker tasks each get their
/// own clone rather than a `&Pool` reference, since every worker runs in its
/// own spawned task with its own lifetime.
#[derive(Debug, Clone)]
pub struct Pool {
    inner: DeadpoolPool,
}

impl Pool {
    /// Builds a pool from `config`. Fails only if `config.dsn` can't be
    /// parsed as a Postgres connection string or deadpool's builder rejects
    /// the resulting configuration; no network I/O happens here — the pool
    /// connects lazily on first `get()`.
    pub fn new(config: &Config) -> Result<Self, Error> {
        let pg_config = tokio_postgres::Config::from_str(config.dsn())
            .map_err(|err| Error::Config(format!("invalid database connection string: {err}")))?;

        let manager_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let manager = Manager::from_config(pg_config, NoTls, manager_config);

        let schema = config.schema().to_string();
        let target_schema = config.target_schema().to_string();
        let inner = DeadpoolPool::builder(manager)
            .post_create(Hook::async_fn(move |client, _metrics| {
                let schema = schema.clone();
                let target_schema = target_schema.clone();
                Box::pin(async move {
                    session_bootstrap(client, &schema, &target_schema)
                        .await
                        .map_err(HookError::Backend)
                })
            }))
            .build()?;

        Ok(Self { inner })
    }

    /// Acquires a connection, waiting for one to become available if the
    /// pool is at capacity.
    pub async fn get(&self) -> Result<Client, Error> {
        Ok(self.inner.get().await?)
    }
}

/// Runs once per physical connection, right after it's established and
/// before it's returned to any caller.
///
/// Pins `search_path` to `schema` (first, so unqualified references to
/// Trellis's own objects always resolve there) followed by `target_schema`
/// (so unqualified reads/writes against a transform target table resolve
/// even when it lives outside both `schema` and `public`) and `public`
/// (Postgres's own default, kept last as a fallback for anything that
/// depends on it today). Beyond `search_path`, this is the seam intake will
/// use to enforce `synchronous_commit = on`.
async fn session_bootstrap(
    client: &mut tokio_postgres::Client,
    schema: &str,
    target_schema: &str,
) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(&format!(
            "set search_path to {}, {}, public",
            quote_ident(schema),
            quote_ident(target_schema)
        ))
        .await
}

/// Quotes a Postgres identifier for safe interpolation into SQL text.
///
/// `config.schema` is operator-supplied (an environment variable), not
/// end-user input, but it still flows into SQL as text rather than a bind
/// parameter (identifiers can't be bound), so it's quoted defensively.
pub(crate) fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("trellis"), "\"trellis\"");
        assert_eq!(quote_ident("weird\"schema"), "\"weird\"\"schema\"");
    }

    #[test]
    fn unparsable_dsn_is_a_typed_config_error() {
        let config =
            Config::from_dsn("not a valid dsn").expect("schema is valid; only the DSN is bogus");
        match Pool::new(&config) {
            Err(Error::Config(_)) => {}
            other => panic!("expected a typed config error, got {other:?}"),
        }
    }
}
