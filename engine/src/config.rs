//! Configuration for connecting to Postgres.
//!
//! Config comes only from CLI args (passed in by the caller) and
//! environment variables — no config crate, no config files. Resolution
//! order for the DSN:
//!
//! 1. An explicit DSN passed by the caller (e.g. a `--database-url` CLI
//!    flag).
//! 2. The `TRELLIS_DATABASE_URL` environment variable, used verbatim.
//! 3. The standard libpq `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`/
//!    `PGDATABASE` environment variables, assembled into a DSN.
//!
//! The Postgres schema Trellis manages its own objects under is likewise
//! resolved from `TRELLIS_SCHEMA`, defaulting to [`DEFAULT_SCHEMA`].

use crate::error::Error;
use std::fmt;

/// The default Postgres schema Trellis's own objects (staging tables, the
/// refinery migration ledger, and eventually the transform catalog) live
/// under. Kept separate from `public` so an install's footprint can be
/// inspected or dropped without touching application schemas that happen to
/// share the database. This is a deliberate but minimal choice for now —
/// later work may need finer-grained namespacing (e.g. per-workspace
/// schemas). See `docs/instance-identity.md`.
pub const DEFAULT_SCHEMA: &str = "trellis";

/// Resolved connection configuration.
///
/// Both fields are private and every constructor validates the schema name
/// via [`validate_schema_name`], so there is no way to hold a `Config` whose
/// schema hasn't been checked — see [`Config::with_schema`].
#[derive(Debug, Clone)]
pub struct Config {
    /// A Postgres connection string, in either URL (`postgresql://...`) or
    /// libpq keyword/value (`host=... user=...`) form.
    dsn: String,
    /// The schema Trellis operates in. See [`DEFAULT_SCHEMA`].
    schema: String,
}

impl Config {
    /// Resolves configuration from an optional explicit DSN (typically a
    /// CLI flag the caller already parsed) and environment variables.
    pub fn resolve(cli_dsn: Option<String>) -> Result<Self, Error> {
        let dsn = match cli_dsn {
            Some(dsn) => dsn,
            None => Self::dsn_from_env(),
        };

        if dsn.trim().is_empty() {
            return Err(Error::Config(
                "no database connection string provided (pass a DSN, set \
                 TRELLIS_DATABASE_URL, or set PGHOST/PGDATABASE/...)"
                    .to_string(),
            ));
        }

        let schema = std::env::var("TRELLIS_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        Self::with_schema(dsn, schema)
    }

    /// Builds a [`Config`] from an explicit DSN, bypassing environment
    /// resolution entirely (the schema is still resolved from
    /// `TRELLIS_SCHEMA`/[`DEFAULT_SCHEMA`] and validated). Useful for tests.
    pub fn from_dsn(dsn: impl Into<String>) -> Result<Self, Error> {
        let schema = std::env::var("TRELLIS_SCHEMA").unwrap_or_else(|_| DEFAULT_SCHEMA.to_string());
        Self::with_schema(dsn, schema)
    }

    /// Builds a [`Config`] from an explicit DSN and schema, validating the
    /// schema via [`validate_schema_name`]. This is the one place a `Config`
    /// is actually constructed — [`Config::resolve`] and
    /// [`Config::from_dsn`] both resolve a schema and hand it to this — so
    /// there is no path to a `Config` carrying an unvalidated schema name.
    pub fn with_schema(dsn: impl Into<String>, schema: impl Into<String>) -> Result<Self, Error> {
        let schema = schema.into();
        validate_schema_name(&schema)?;
        Ok(Self {
            dsn: dsn.into(),
            schema,
        })
    }

    /// The Postgres connection string this instance was configured with.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// The Postgres schema this instance is configured to operate in. See
    /// [`DEFAULT_SCHEMA`] and `docs/instance-identity.md`.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    fn dsn_from_env() -> String {
        if let Ok(dsn) = std::env::var("TRELLIS_DATABASE_URL") {
            return dsn;
        }

        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let dbname = std::env::var("PGDATABASE").unwrap_or_else(|_| user.clone());

        match std::env::var("PGPASSWORD") {
            Ok(password) => format!("postgresql://{user}:{password}@{host}:{port}/{dbname}"),
            Err(_) => format!("postgresql://{user}@{host}:{port}/{dbname}"),
        }
    }
}

impl fmt::Display for Config {
    /// A short, human-readable summary of the resolved instance identity —
    /// the schema this instance operates in — for wherever configuration
    /// gets reported (logs, diagnostics). Deliberately omits the DSN, which
    /// may carry a password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "trellis instance in schema {:?}", self.schema)
    }
}

/// Validates a candidate Trellis schema name.
///
/// Every place this name reaches SQL goes through [`crate::pool::quote_ident`]
/// as a quoted (delimited) identifier, so Postgres will accept almost any
/// character in it — over-restricting the character set here would reject
/// names Postgres itself is happy with. The real hazards a quoted identifier
/// doesn't protect against are:
///
/// - **Empty or all-whitespace.** Not a name at all — almost certainly a
///   misconfigured `TRELLIS_SCHEMA` (e.g. `TRELLIS_SCHEMA=" "`), not an
///   intentional identity.
/// - **Longer than 63 bytes.** Postgres's `NAMEDATALEN` limit means longer
///   identifiers are *silently truncated*, not rejected — two distinct
///   configured names could collide under truncation without anyone
///   noticing, which is exactly the failure mode instance identity exists to
///   prevent.
/// - **An embedded NUL byte.** Postgres (via libpq) treats a C string's NUL
///   as its terminator, so a name containing one would be interpreted as
///   something shorter and different than what was configured.
fn validate_schema_name(name: &str) -> Result<(), Error> {
    if name.trim().is_empty() {
        return Err(Error::Config(
            "TRELLIS_SCHEMA must not be empty or all-whitespace".to_string(),
        ));
    }
    if name.contains('\0') {
        return Err(Error::Config(
            "TRELLIS_SCHEMA must not contain a NUL byte".to_string(),
        ));
    }
    if name.len() > 63 {
        return Err(Error::Config(format!(
            "TRELLIS_SCHEMA {name:?} is {} bytes long, exceeding Postgres's 63-byte \
             identifier limit (NAMEDATALEN); Postgres would silently truncate it, \
             which could collide with another instance's schema name",
            name.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_dsn_wins() {
        let config = Config::resolve(Some("postgresql://example/db".to_string())).unwrap();
        assert_eq!(config.dsn(), "postgresql://example/db");
        assert_eq!(config.schema(), DEFAULT_SCHEMA);
    }

    #[test]
    fn empty_dsn_is_rejected() {
        let err = Config::resolve(Some(String::new())).unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn valid_schema_name_passes() {
        assert!(validate_schema_name("trellis").is_ok());
        assert!(validate_schema_name(DEFAULT_SCHEMA).is_ok());
    }

    #[test]
    fn empty_schema_name_is_rejected() {
        assert!(matches!(validate_schema_name(""), Err(Error::Config(_))));
    }

    #[test]
    fn whitespace_only_schema_name_is_rejected() {
        assert!(matches!(
            validate_schema_name("   \t  "),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn schema_name_over_namedatalen_is_rejected() {
        let too_long = "a".repeat(64);
        assert!(matches!(
            validate_schema_name(&too_long),
            Err(Error::Config(_))
        ));
        // Exactly at the limit is fine.
        let at_limit = "a".repeat(63);
        assert!(validate_schema_name(&at_limit).is_ok());
    }

    #[test]
    fn schema_name_with_embedded_nul_is_rejected() {
        assert!(matches!(
            validate_schema_name("trel\0lis"),
            Err(Error::Config(_))
        ));
    }
}
