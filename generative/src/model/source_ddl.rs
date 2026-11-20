//! Source DDL in the op stream (`docs/generative-test-suite.md` §7,
//! bucket 3): schema and maintenance changes to a tracked source, drawn
//! between a program's ops with changes in flight, and judged by "never
//! silently wrong" (`crate::run::judge`).

use crate::gaps::Gap;

/// One drawn change to a tracked source table. Each runs as one or two
/// groups of statements: one when the event fires and, for a change undone
/// or completed later ([`SourceDdl::closes`]), one once the event's window
/// of ops has been applied. The statements run as the
/// bootstrap superuser (`execute_admin`).
#[derive(Debug, Clone, PartialEq)]
pub enum SourceDdl {
    /// `VACUUM FULL`: rewrites the table, every file node moves, no value
    /// changes.
    VacuumFull,
    /// `CLUSTER … USING` the primary key's index.
    Cluster,
    /// `REINDEX TABLE`, `CONCURRENTLY` or not.
    Reindex { concurrently: bool },
    /// `ALTER COLUMN column TYPE <its own type> USING column + 1`: a
    /// same-type rewrite of a numeric column, which changes every value and
    /// fires no trigger. The pass re-reads the table.
    RewriteColumn { column: String },
    /// `ALTER COLUMN column SET DEFAULT 0` when the event fires, and the
    /// same rewrite as [`SourceDdl::RewriteColumn`] at its close. Within one
    /// reconcile interval this is the register's
    /// [`Gap::AlterTypeRewrite`] shape (a metadata change beside the
    /// rewrite), unless the server versions attributes and a read column
    /// follows this one; across a pass the pass sees the rewrite alone.
    RewriteBesideDefault { column: String },
    /// Every capture trigger on the table disabled by name when the event
    /// fires and enabled again (`ENABLE ALWAYS`, as Trellis installs them)
    /// at its close, so the window's writes are never captured. The capture
    /// audit sees it, so it must pause the table's readers either way
    /// (#2184's decision 4).
    DisableCaptureTriggers,
}

impl SourceDdl {
    /// Whether the change has a closing group of statements.
    pub fn closes(&self) -> bool {
        matches!(
            self,
            SourceDdl::RewriteBesideDefault { .. } | SourceDdl::DisableCaptureTriggers
        )
    }

    /// A short name for coverage counts and reports.
    pub fn kind(&self) -> &'static str {
        match self {
            SourceDdl::VacuumFull => "vacuum_full",
            SourceDdl::Cluster => "cluster",
            SourceDdl::Reindex {
                concurrently: false,
            } => "reindex",
            SourceDdl::Reindex { concurrently: true } => "reindex_concurrently",
            SourceDdl::RewriteColumn { .. } => "rewrite_column",
            SourceDdl::RewriteBesideDefault { .. } => "rewrite_beside_default",
            SourceDdl::DisableCaptureTriggers => "disable_capture_triggers",
        }
    }
}

/// One [`SourceDdl`] on `table`, fired right after `ops[op]` is applied.
/// The next `window` ops are applied after its opening statements and
/// before its closing ones (or, for a change with none, before the run
/// next waits), so they are in flight around it: `op + window <
/// ops.len()`.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceDdlEvent {
    pub op: usize,
    pub table: String,
    pub action: SourceDdl,
    pub window: usize,
    /// Whether a reconcile pass runs right after the opening statements,
    /// with the window's ops still to come. Otherwise the whole event lands
    /// inside one reconcile interval: the run steps the passes itself
    /// (`crate::run::run_convergence_with_source_ddl`).
    pub pass_inside: bool,
}

impl SourceDdlEvent {
    /// The register entries this event may reproduce: a definition may end
    /// silently wrong after it only as one of these, detected by
    /// `self_check` and made exact by the entry's repair.
    pub fn listed_gaps(&self) -> Vec<Gap> {
        match (&self.action, self.pass_inside) {
            (SourceDdl::RewriteBesideDefault { .. }, false) => vec![Gap::AlterTypeRewrite],
            _ => Vec::new(),
        }
    }

    /// The last op the event spans.
    pub fn last_op(&self) -> usize {
        self.op + self.window
    }
}

/// The source-DDL schedule for one program run: events in op order, none
/// starting before the previous one's window has ended.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceDdlPlan {
    pub events: Vec<SourceDdlEvent>,
}
