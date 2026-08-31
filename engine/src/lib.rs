//! Trellis engine: a declarative API for creating incrementally maintained
//! data transformations in PostgreSQL.
//!
//! [`client::Client`] is the embedder's entry point — one call starts CDC
//! intake, ring maintenance, and N application workers against a live
//! database. Everything else in the crate is machinery `Client` composes:
//!
//! - [`config`] resolves a [`Config`] from CLI args + environment.
//! - [`pool`] manages a `deadpool-postgres` connection pool and exposes the
//!   per-connection session bootstrap seam.
//! - [`migrate`] applies Trellis's embedded SQL migrations.
//! - [`identity`] decides, before migrations run, whether the configured
//!   schema is safe to attach to — see `docs/instance-identity.md`.
//! - [`intake`] streams committed source-table changes off a logical
//!   replication slot into the durable staging ring.
//! - [`staging`] owns the ring, sealing, claim-time fold, and the exactly-once
//!   apply path — see `docs/staging-and-claiming/README.md`.
//! - [`defs`] parses, validates, and catalogs transform definitions (today:
//!   the 1-1, `+`-only grammar — see `docs/decisions/0004-transform-definition-grammar.md`).
//!
//! **Current subset**: 1-1 scalar transforms only end to end (issue #11's
//! 1-1 slice). Aggregate/invertible-delta maintenance is not yet wired up —
//! the apply frame that will carry it (fence, lock order, atomic apply ∪
//! mark-drained) is already in place in [`staging`].
//!
//! The ring has a fixed [`staging::RING_SIZE`] of 4 slots; a `drained`
//! segment's slot is freed for reuse by retirement (stage 06,
//! `docs/staging-and-claiming/06-cleanup-and-reclaim.md`,
//! [`staging::retire_drained_segments`]), run both as
//! [`staging::seal_if_active_nonempty`]'s one-retry-on-`RingFull` step and on
//! every maintenance tick ([`client::ClientOptions::maintenance_interval`]).
//! Quarantine, stage 06's other half, is not yet implemented.

pub mod client;
pub mod config;
pub mod defs;
pub mod error;
pub mod identity;
pub mod intake;
pub mod migrate;
pub mod numeric;
pub mod pool;
pub mod staging;

pub use client::{Client, ClientError, ClientOptions};
pub use config::Config;
pub use error::Error;
pub use identity::Identity;
pub use migrate::migrate;
pub use numeric::Numeric;
pub use pool::Pool;

/// Placeholder entry point exercising the async plumbing the engine will
/// build on. Returns the crate version so callers have something to check.
pub async fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn version_is_reported() {
        assert_eq!(version().await, env!("CARGO_PKG_VERSION"));
    }
}
