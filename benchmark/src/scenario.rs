//! The benchmark's shared raw-connection helper.
//!
//! [`connect_raw`] connects directly to a DSN (bypassing
//! `trellis::dev::pool::Pool`) with the session `trellis`'s own integration
//! tests use: the scenarios' reference and correctness queries want a plain
//! `tokio_postgres::Client`, which the pool's wrapped client doesn't expose,
//! so they go through a raw connection.

pub(crate) use testkit::connect_raw;
