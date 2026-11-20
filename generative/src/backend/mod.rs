//! The backend seam (design doc §1 "The backend seam"): the ONLY module
//! that drives the engine's maintenance pipeline and reads back derived
//! state. Nothing outside this module may import `trellis::client`,
//! `trellis::staging`, or the engine's own `defs::catalog`/`ddl` — the oracle
//! (`crate::oracle`) and generators (`crate::generate`) must stay reachable
//! only through the shared, engine-independent pieces named in the design
//! doc, so a second backend can be added without touching either. Two
//! backends exist today: [`ManualBackend`] (a concurrent-runtime-capable, but
//! always in-process, [`trellis::Client`]) and [`SubprocessBackend`] (issue
//! A real OS subprocess `testkit::CrashGuard` can `SIGKILL`, for tests
//! that need a genuine crash rather than an in-process simulation). Both
//! share [`sql`]'s DDL/DML/read-back rendering — see that module's doc
//! comment.

mod manual;
mod ring_wait;
mod roles;
mod sql;
mod subprocess;
mod wiring;

pub use manual::{
    HeldKeyReport, ManualApplier, ManualBackend, ManualBackendError, SEAL_ON_DEMAND_INTERVAL,
    SERVER_STOP_RECLAIM_TTL, SHUTDOWN_TIMEOUT, SourceTables, activity_summary, activity_summary_on,
    await_pool_usable,
};
pub use roles::{
    APP_ROLE, CaseDatabase, HEIR_ROLE, OTHER_OWNER_ROLE, Roles, TARGETS_SCHEMA, TRELLIS_ROLE,
    WRITER_ROLE, as_role,
};
pub use sql::HeldKeys;
pub use subprocess::{
    REACH_FILE_ENV, SubprocessBackend, SubprocessBackendError, reach_request, write_reach,
};

use std::collections::BTreeMap;
use std::future::Future;

use crate::model::{BurstAction, Op, Program, RestartMode};

/// Merged source+derived state, read back deterministically: `table -> pk
/// (rendered text) -> column -> value (rendered text, `None` is SQL
/// `NULL`)`. A `BTreeMap` at every level so two snapshots compare and diff
/// stably regardless of physical row/column order (design doc §1).
pub type Snapshot = BTreeMap<String, BTreeMap<String, BTreeMap<String, Option<String>>>>;

/// One definition's catalog state at a check: its `status` as stored
/// (`live`, `paused`, ...), its status as `Trellis::definitions` reports it
/// (`reported`: a stored `live` reads `catching_up` while an upstream isn't
/// `live`) and, when something recorded why it stopped, the `reason` (its
/// `capture_failures` row, or its poisoned keys' error). What a divergence
/// report, the unexpected-pause check and the "status tells the truth"
/// check read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionState {
    pub target: String,
    pub status: String,
    pub reported: String,
    pub reason: Option<String>,
}

impl DefinitionState {
    /// Whether the definition is `paused`.
    pub fn is_paused(&self) -> bool {
        self.status == "paused"
    }

    /// Whether the definition reports `live` with no failure recorded: the
    /// claim that its target is exact (ADR-0002, "What `live` promises").
    pub fn claims_exact(&self) -> bool {
        self.reported == "live" && self.reason.is_none()
    }
}

impl std::fmt::Display for DefinitionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.target, self.status)?;
        if self.reported != self.status {
            write!(f, " (reported {})", self.reported)?;
        }
        if let Some(reason) = &self.reason {
            write!(f, " ({reason})")?;
        }
        Ok(())
    }
}

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

    /// Each of `defs`' [`DefinitionState`], in `defs`' order, read from the
    /// catalog. A divergence report prints them, and a run fails on a
    /// paused one in a tier that never expects a pause.
    fn definition_states(
        &mut self,
        defs: &[trellis::dev::defs::ast::TransformDef],
    ) -> impl Future<Output = Result<Vec<DefinitionState>, Self::Error>> + Send;

    /// Reads back merged source+derived state as a [`Snapshot`].
    fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Self::Error>> + Send;

    /// Simulates an ungraceful crash-and-restart of the backend's primary
    /// engine client: tears down whatever is
    /// currently running it and starts a fresh one against the same target.
    /// The ring is durable Postgres state, not in-memory, so a fresh client
    /// is expected to pick back up exactly where the crashed one left off:
    /// no stuck or lost work, no duplicate processing. Each implementation
    /// picks its own faithful stand-in for "the process died": [`ManualBackend`]
    /// (in-process) replaces its `trellis::Client`, awaiting the outgoing
    /// one's graceful `shutdown()` first rather than just
    /// dropping it — `Drop`'s shutdown signal is best-effort and doesn't
    /// join the background thread, so a bare drop-then-restart could race
    /// the old producer's advisory-lock release against the new producer's
    /// acquire and come up as a standby, taking the lock only at its next poll; no
    /// subprocess/`SIGKILL` machinery is needed there either way.
    /// [`SubprocessBackend`] does the real thing: `SIGKILL`s the
    /// actual OS process, *waits for it to actually exit* and then for
    /// Postgres to free its singleton before spawning a fresh one — the same
    /// "don't just drop, wait for the teardown to really finish" discipline,
    /// one layer down.
    fn restart(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Starts an additional engine client alongside whatever is already
    /// running, application-worker-only (never a second staging worker —
    /// see `trellis::client`'s module doc comment: exactly one staging worker
    /// per fleet), demonstrating multiple clients can coexist draining the
    /// same ring.
    fn scale_out(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Applies ops on a connection of its own, so the concurrent
/// tier can issue several lanes of a burst at once, each from its own task.
/// Same contract as [`Backend::apply`]: raw source DML, the affected row
/// count on success, `Err` when the statement itself was rejected.
pub trait OpApplier: Send + 'static {
    type Error: std::fmt::Debug + Send;

    fn apply(&mut self, op: &Op) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Applies `ops`, the ops `wrap` names, in order, as the one
    /// transaction `wrap` describes (#2181), retrying it from the start on a
    /// serialization failure or deadlock. `Err` only when the transaction
    /// itself couldn't commit; an op's own rejection is its entry in
    /// [`TxnApplied::results`].
    fn apply_txn(
        &mut self,
        wrap: &crate::model::TxnWrap,
        ops: &[Op],
    ) -> impl Future<Output = Result<TxnApplied, Self::Error>> + Send;
}

/// What [`OpApplier::apply_txn`] did (#2181).
#[derive(Debug, Default)]
pub struct TxnApplied {
    /// Each op's affected-row count, or its error's text, from the run that
    /// committed, in order. An op that failed where the program expected it
    /// to succeed ends the list: the transaction rolled back there.
    pub results: Vec<Result<u64, String>>,
    /// How many times the transaction was rolled back and run again after a
    /// serialization failure or deadlock.
    pub retries: u64,
}

/// A [`Backend`] the concurrent tier can drive: it hands out
/// independent [`OpApplier`]s, and it can say how its engine actually sealed
/// and claimed the batches a run produced ([`DrainAudit`]).
pub trait ConcurrentBackend: Backend {
    type Applier: OpApplier;

    /// Puts `wiring` (#2181) on the tables the next [`Backend::install`]
    /// creates, before it installs any definition.
    fn set_wiring(&mut self, wiring: &[crate::model::Wiring]);

    /// A new applier on its own connection, for tables already installed.
    fn applier(&self) -> impl Future<Output = Result<Self::Applier, Self::Error>> + Send;

    /// Starts recording every seal and claim from here on, tagged with the
    /// burst passed to [`ConcurrentBackend::begin_burst`]. Call once, after
    /// [`Backend::install`].
    fn start_drain_audit(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Tags every batch sealed from now on as belonging to burst `burst`.
    fn begin_burst(&mut self, burst: usize)
    -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// What the recording has seen so far.
    fn drain_audit(&mut self) -> impl Future<Output = Result<DrainAudit, Self::Error>> + Send;

    /// Takes one operator action while a burst's lanes run, through the
    /// public API an operator would use: see each
    /// [`BurstAction`] for the call. `program` supplies an
    /// [`BurstAction::Install`]'s definition, which from then on is part of
    /// what [`Backend::quiesce`] waits for and [`Backend::snapshot`] reads.
    fn act(
        &mut self,
        program: &Program,
        action: &BurstAction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// A `self_check` in `Strict` mode of each of `targets`, which the run
    /// calls at every quiescence point once the oracle has agreed, for a
    /// backend that opted in. One line per target that is not clean, with
    /// the report. The default audits nothing.
    fn strict_audit(
        &mut self,
        targets: &[String],
    ) -> impl Future<Output = Result<Vec<String>, Self::Error>> + Send {
        let _ = targets;
        async { Ok(Vec::new()) }
    }
}

/// The fewest rows a sealed batch needs for the engine to split it into
/// buckets that several workers can claim (`staging::claim`). Re-exported
/// here because nothing outside this module may name `trellis::staging` (the
/// module doc comment); `crate::run::Coverage` reads it.
pub const SPLIT_THRESHOLD_ROWS: usize = trellis::dev::staging::MIN_ROWS_TO_SPLIT as usize;

/// How the engine sealed and claimed one run's batches, read
/// from triggers on its own `segments` and `seg_claims` rows. This is the
/// evidence that a run really split batches across workers, as opposed to
/// merely issuing many ops. Plain counts, so runs add up
/// ([`DrainAudit::add`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainAudit {
    /// Batches sealed.
    pub sealed: u64,
    /// Sealed batches split into more than one bucket.
    pub split: u64,
    /// Split batches whose buckets were claimed by two or more workers.
    pub split_across_workers: u64,
    /// The most distinct workers that claimed buckets of one batch.
    pub max_workers_per_batch: u64,
    /// The most rows sealed into one batch.
    pub max_rows_per_batch: u64,
    /// `(burst, table, key)`s whose changes were sealed into two or more
    /// batches during the same burst: one key's changes in flight in several
    /// batches at once.
    pub keys_in_several_batches: u64,
}

impl DrainAudit {
    /// Adds `other`'s counts into this one, keeping the larger maximum.
    pub fn add(&mut self, other: &DrainAudit) {
        self.sealed += other.sealed;
        self.split += other.split;
        self.split_across_workers += other.split_across_workers;
        self.max_workers_per_batch = self.max_workers_per_batch.max(other.max_workers_per_batch);
        self.max_rows_per_batch = self.max_rows_per_batch.max(other.max_rows_per_batch);
        self.keys_in_several_batches += other.keys_in_several_batches;
    }
}

/// Control over the Postgres server itself, as opposed to a database on it:
/// what a [`crate::model::DbAdminAction::RestartPostgres`]
/// needs. A trait rather than a `testkit` type so `run` names no cluster
/// type (its module doc comment), and so a harness that drives some other
/// server can supply its own.
pub trait ClusterControl {
    /// Stops the server in `mode` and starts it again, returning once it
    /// accepts connections. Every open connection is severed.
    fn restart_postgres(&self, mode: RestartMode);

    /// Freezes every database and moves the next transaction id to `lead`
    /// ids short of the next 32-bit epoch boundary, restarting the server
    /// for each jump (`testkit::TestCluster::age_xids_to_epoch_edge`).
    /// `lead` is a multiple of `testkit::XID_PAGE`.
    fn age_xids_to_epoch_edge(&self, lead: u32);

    /// Assigns and commits `count` transaction ids.
    fn burn_xids(&self, count: u32);
}

impl ClusterControl for testkit::TestCluster {
    fn restart_postgres(&self, mode: RestartMode) {
        self.restart(match mode {
            RestartMode::Fast => testkit::StopMode::Fast,
            RestartMode::Immediate => testkit::StopMode::Immediate,
        });
    }

    fn age_xids_to_epoch_edge(&self, lead: u32) {
        testkit::TestCluster::age_xids_to_epoch_edge(self, lead);
    }

    fn burn_xids(&self, count: u32) {
        testkit::TestCluster::burn_xids(self, count);
    }
}

/// Backing a Postgres server up and restoring it into a fresh one: what
/// `crate::run::run_convergence_with_restore` needs. A trait for the same
/// reason as [`ClusterControl`].
pub trait BackupRestore: Sized {
    /// A backup, kept until it has been restored from.
    type Backup;

    /// Stops the server, copies its whole data directory while it's down,
    /// and starts it again, returning once it accepts connections. Every
    /// open connection is severed, as by a restart.
    fn cold_backup(&self) -> Self::Backup;

    /// Starts a new server on a copy of `backup`, returning once it accepts
    /// connections.
    fn restore(backup: &Self::Backup) -> Self;

    /// The connection string for database `name` on this server.
    fn database_dsn(&self, name: &str) -> String;
}

impl BackupRestore for testkit::TestCluster {
    type Backup = testkit::ClusterBackup;

    fn cold_backup(&self) -> Self::Backup {
        testkit::TestCluster::cold_backup(self)
    }

    fn restore(backup: &Self::Backup) -> Self {
        testkit::TestCluster::from_backup(backup)
    }

    fn database_dsn(&self, name: &str) -> String {
        testkit::TestCluster::database_dsn(self, name)
    }
}
