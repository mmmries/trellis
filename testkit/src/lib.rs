//! Reusable ephemeral-Postgres integration test harness for Trellis.
//!
//! - [`cluster`] spins up a throwaway Postgres instance and hands out
//!   per-scenario isolated, migrated databases.
//! - [`fixtures`] seeds the common source-table / derived-table shape most
//!   staging/claiming test suites need.
//! - [`crash`] provides the seams durability tests (the generative suite's
//!   kill-9 subprocess engine, the straddler/phase-gap tests) build their actual crash-point scenarios
//!   on.
//! - [`lsn`] gives tests a realistic LSN to stage CDC ring rows at.
//! - [`plan`] reads the work an `explain (analyze)` plan says a statement did.
//! - [`locale`] finds a locale that makes an unpinned `lc_monetary` show.
//! - [`blocking`] reads from the server's lock table whether one session
//!   was stuck behind another's lock, and for how long, and waits for one
//!   to queue behind another.
//! - [`raw`] opens plain `tokio_postgres` connections outside any pool.
//! - [`wait`] waits for a condition with a hard deadline and a failure
//!   that names what was awaited.
//!
//! The crate also builds a `trellis-testkit` binary (`src/bin/`) that holds
//! one [`TestCluster`] for a test suite written in another language (the
//! Elixir and Ruby bindings): it prints the connection details as a JSON
//! line and tears the cluster down on a signal or when its stdin closes.
//!
//! This crate is dev-only scaffolding shared across the workspace (any
//! crate's integration tests can dev-depend on it), not part of the
//! engine's runtime behavior. Setup failures panic rather than returning
//! `Result`s: a broken test fixture should fail loudly and immediately,
//! not be handled gracefully.

pub mod blocking;
pub mod cluster;
pub mod crash;
#[cfg(feature = "engine-access")]
pub mod fixtures;
pub mod locale;
pub mod lsn;
pub mod plan;
pub mod raw;
pub mod wait;

pub use blocking::{BlockedWatch, Blocker, wait_blocked_behind, watch_blocked};
pub use cluster::{ClusterBackup, StopMode, TestCluster, TestDatabase};
pub use lsn::wal_insert_lsn;
#[cfg(feature = "engine-access")]
pub use raw::connect_raw;
pub use raw::connect_with;
pub use wait::{wait_for, wait_until};
