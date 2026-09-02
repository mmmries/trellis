//! Generative correctness test suite for the `engine` crate (issue #3,
//! epic #2). See `docs/generative-test-suite.md` for the architecture.
//!
//! Five strictly-separated modules (design doc §1):
//!
//! - [`model`] — plain-data description of a generated program. Names no
//!   engine internals beyond the AST types it deliberately reuses.
//! - [`generate`] — generators producing only valid programs. Makes no
//!   engine calls.
//! - [`oracle`] — independent recompute, sharing no evaluation code with
//!   the engine.
//! - [`backend`] — the ONLY module that drives the engine's pipeline and
//!   reads back derived state.
//! - [`run`] — drives a program through a backend, asserts the properties.
//!
//! `generate`/`oracle`/`run` are skeletons today; [`model`] and [`backend`]
//! are this issue's substance.

pub mod backend;
pub mod generate;
pub mod model;
pub mod oracle;
pub mod run;
