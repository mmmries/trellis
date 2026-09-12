//! The backend seam (design doc §1 "The backend seam"): the ONLY module
//! that drives the engine's maintenance pipeline and reads back derived
//! state. Nothing outside this module may import `engine::client`,
//! `engine::staging`, or `engine::defs::catalog`/`ddl` — the oracle
//! (`crate::oracle`) and generators (`crate::generate`) must stay reachable
//! only through the shared, engine-independent pieces named in the design
//! doc, so a second backend (a concurrent runtime, later a
//! subprocess-supervised one) can be added without touching either.

mod manual;

pub use manual::{ManualBackend, ManualBackendError};

use std::collections::BTreeMap;
use std::future::Future;

use crate::model::{Op, Program};

/// Merged source+derived state, read back deterministically: `table -> pk
/// (rendered text) -> column -> value (rendered text, `None` is SQL
/// `NULL`)`. A `BTreeMap` at every level so two snapshots compare and diff
/// stably regardless of physical row/column order (design doc §1).
pub type Snapshot = BTreeMap<String, BTreeMap<String, BTreeMap<String, Option<String>>>>;

/// A backend that can install a [`Program`]'s schema and definitions, apply
/// its ops as raw source DML, wait for the engine to catch up, and read
/// back merged state. See the module doc comment and design doc §1 for the
/// contract each method must uphold — in particular, [`Backend::apply`]
/// must never go through an application-level notification API, and
/// [`Backend::quiesce`] must be a client-side watermark poll, never a
/// `sleep`.
pub trait Backend {
    type Error: std::fmt::Debug;

    /// Creates every table in `program.tables`, installs every definition
    /// in `program.defs` (and its neighbor target table), and starts
    /// whatever engine machinery this backend needs to keep them
    /// incrementally maintained.
    fn install(
        &mut self,
        program: &Program,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Applies one op as raw source DML. On success, returns the number of
    /// rows the statement affected — `0` for an update/delete that named a
    /// primary key no row has (a source no-op, not an error); an `Err`
    /// return means the statement itself was rejected (e.g. a primary-key
    /// violation). Callers compare this against the op's
    /// [`crate::model::OpOutcome`] expectation (design doc §4 "operation
    /// errors are checked, not swallowed").
    fn apply(&mut self, op: &Op) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Blocks until the engine has caught up with every op applied so far.
    fn quiesce(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Reads back merged source+derived state as a [`Snapshot`].
    fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Self::Error>> + Send;
}
