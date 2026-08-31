//! Trellis's error type for the connectivity and migration layer.
//!
//! Kept as a plain enum implementing [`std::error::Error`] (no `thiserror`/
//! `anyhow`) per the crate's dependency policy: pull in only what's on the
//! approved list for this issue.

use std::fmt;

/// Errors that can occur while resolving configuration, connecting to
/// Postgres, or running migrations.
#[derive(Debug)]
pub enum Error {
    /// The engine's own configuration (DSN, schema name, ...) was invalid.
    Config(String),
    /// Building the connection pool failed (e.g. an unparsable DSN).
    BuildPool(deadpool_postgres::BuildError),
    /// Acquiring a connection from the pool failed.
    Pool(deadpool_postgres::PoolError),
    /// A direct Postgres protocol/connection error.
    Connect(tokio_postgres::Error),
    /// Applying migrations failed.
    Migrate(refinery::Error),
    /// The configured schema already belongs to a different Trellis
    /// instance (a mismatched identity marker), an incompatible one (an
    /// instance format version newer than this build understands), or a
    /// pre-existing, non-Trellis schema with no marker at all. See
    /// `crate::identity`.
    IncompatibleInstance(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config(msg) => write!(f, "invalid trellis configuration: {msg}"),
            Error::BuildPool(err) => write!(f, "failed to build connection pool: {err}"),
            Error::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
            Error::Connect(err) => write!(f, "postgres connection error: {err}"),
            Error::Migrate(err) => write!(f, "failed to apply migrations: {err}"),
            Error::IncompatibleInstance(msg) => write!(f, "refusing to attach: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Config(_) => None,
            Error::BuildPool(err) => Some(err),
            Error::Pool(err) => Some(err),
            Error::Connect(err) => Some(err),
            Error::Migrate(err) => Some(err),
            Error::IncompatibleInstance(_) => None,
        }
    }
}

impl From<deadpool_postgres::BuildError> for Error {
    fn from(err: deadpool_postgres::BuildError) -> Self {
        Error::BuildPool(err)
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(err: deadpool_postgres::PoolError) -> Self {
        Error::Pool(err)
    }
}

impl From<tokio_postgres::Error> for Error {
    fn from(err: tokio_postgres::Error) -> Self {
        Error::Connect(err)
    }
}

impl From<refinery::Error> for Error {
    fn from(err: refinery::Error) -> Self {
        Error::Migrate(err)
    }
}

/// Writes `err`'s message, plus the underlying `DbError` detail if one is
/// attached.
///
/// `tokio_postgres::Error`'s own `Display` only ever prints its bare
/// error-kind text (e.g. `"db error"`) — the actual Postgres message
/// (severity, text, and any `DETAIL`/`HINT`) lives on the `DbError` its
/// `source()` carries, not in its own `Display`. Every `Display` impl in
/// this crate that wraps a `tokio_postgres::Error` needs this, or a caller
/// only ever sees the useless "db error" stand-in instead of what actually
/// went wrong.
pub fn write_pg_error(f: &mut fmt::Formatter<'_>, err: &tokio_postgres::Error) -> fmt::Result {
    write!(f, "{err}")?;
    if let Some(db_err) = err.as_db_error() {
        write!(f, ": {db_err}")?;
    }
    Ok(())
}
