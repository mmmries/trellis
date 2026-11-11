//! Generative correctness test suite for the `trellis` crate. See
//! `docs/generative-test-suite.md` for the architecture.
//!
//! Five strictly-separated modules (design doc §1):
//!
//! - [`model`] — plain-data description of a generated program. Names no
//!   engine internals beyond the AST types it deliberately reuses.
//! - [`generate`] — generators producing only valid programs. Makes no
//!   engine calls.
//! - [`oracle`] — independent recompute through Postgres, sharing no rendering code with
//!   the engine.
//! - [`backend`] — the ONLY module that drives the engine's pipeline and
//!   reads back derived state.
//! - [`run`] — drives a program through a backend, asserts the properties.
//!
//! Beside them:
//!
//! - [`shrink`] — how long a property's failure may shrink before it reports.
//! - [`expr_gen`] and [`expr_conformance`] — generated typed expressions,
//!   and the properties that check them against a real Postgres.
//! - [`plant_sweep`] — the planted-bug sweep: a tier's seeded cases run once
//!   per planted bug (`trellis::dev::plant`), judged on whether it catches each.
//! - [`baseline_quarantine`] — the sweep's baseline bar and its checked-in
//!   list of known, filed failures.
//! - [`invariant`] — the 1-1 `JOIN` tier's in-run check of hidden state, which
//!   reads that state through `trellis::dev` and judges it in pure code.

// Every module below names `trellis::dev` — ADR-0012's curated, uncommitted
// re-export of the engine items this suite's oracle and backends need — which
// exists only when `trellis/test-util` is on. This crate turns that on through
// its own `engine-access` feature, enabled by the self dev-dependency in
// `Cargo.toml` and by nothing else, so a plain `cargo build --workspace`
// compiles this crate down to an empty lib rather than leaking the engine's
// test-only hooks into `cli`'s production binary. See `Cargo.toml` for the
// feature-unification rule that protects against.
#[cfg(feature = "engine-access")]
pub mod backend;
// Names no engine item, but only the planted-bug sweep reads it, so it is
// gated with the rest and a plain build still compiles this crate empty.
#[cfg(feature = "engine-access")]
pub mod baseline_quarantine;
#[cfg(feature = "engine-access")]
pub mod expr_conformance;
#[cfg(feature = "engine-access")]
pub mod expr_gen;
#[cfg(feature = "engine-access")]
pub mod generate;
#[cfg(feature = "engine-access")]
pub mod invariant;
#[cfg(feature = "engine-access")]
pub mod model;
#[cfg(feature = "engine-access")]
pub mod oracle;
#[cfg(feature = "engine-access")]
pub mod plant_sweep;
#[cfg(feature = "engine-access")]
pub mod run;
#[cfg(feature = "engine-access")]
pub mod shrink;
