//! Source DDL in the op stream (`docs/generative-test-suite.md` §7,
//! bucket 3): schema and maintenance changes to a tracked source, drawn
//! between a program's ops with changes in flight, and judged by "never
//! silently wrong" (`crate::run::judge`).

use crate::gaps::Gap;

/// One drawn change to a tracked source table, or to who may read or write
/// it (#2182, [`SourceDdl::privilege`]). Each runs as one or two groups of
/// statements: one when the event fires and, for a change undone or
/// completed later ([`SourceDdl::closes`]), one once the event's window of
/// ops has been applied. The statements run as the bootstrap superuser
/// (`execute_admin`). The roles named are `crate::backend::roles`'.
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
    /// `OWNER TO` [`HEIR_ROLE`](crate::backend::HEIR_ROLE), which Trellis
    /// and the application both inherit, and back at the close.
    OwnerToHeir,
    /// `OWNER TO` [`OTHER_OWNER_ROLE`](crate::backend::OTHER_OWNER_ROLE),
    /// which Trellis doesn't inherit, and back at the close: Trellis can
    /// neither read the table nor manage its triggers meanwhile.
    OwnerToOther,
    /// `REVOKE SELECT` from the Trellis role and from the owner it inherits
    /// (the owner's own grant), and `GRANT` back at the close: Trellis owns
    /// the table but can't read it.
    RevokeSelect,
    /// The owner role revoked from the Trellis role, and granted back at
    /// the close: how an operator cuts Trellis off under the
    /// recommendations, where it reads the sources only through that
    /// membership. The owner's own privileges are untouched.
    RevokeMembership,
    /// `REVOKE USAGE` on `public`, where the sources are, from `PUBLIC` and
    /// the owner, and `GRANT` back at the close. Targets in `public` lose it
    /// too, so the drain's writes are refused as well.
    RevokeSchemaUsage,
    /// Row-level security enabled and forced on the table, with a policy
    /// letting the writer see and write every row, and, when
    /// `trellis_policy`, one for the Trellis role that lets it see every
    /// row; disabled and the policies dropped at the close. Trellis's
    /// sessions (`row_security = off`) are refused either way. Inside an
    /// application statement the capture function runs with the
    /// application's `row_security`, so the policies apply to its re-read.
    Rls { trellis_policy: bool },
    /// Only the application role changed: its writes run as the writer for
    /// the window.
    WriterChange,
    /// The drain's own refusal: `INSERT`, `UPDATE` and `DELETE` on
    /// `target`, a target reading the table, revoked from the Trellis role
    /// that owns it, and granted back at the close.
    RevokeTargetWrites { target: String },
    /// The drain's own refusal: row-level security enabled and forced on
    /// `target`, with no policy; disabled at the close. The catalog pins it
    /// on the target, so the target's writer halts.
    TargetRls { target: String },
    /// The drain's own refusal through the application's trigger on
    /// `target`: a statement trigger writing a table the Trellis role can't
    /// write, dropped at the close. The catalog can't pin that on a table,
    /// so a drain meeting it holds the page up (`docs/observability.md`,
    /// "A halting failure").
    TargetTriggerRefused { target: String },
}

impl SourceDdl {
    /// Whether the change has a closing group of statements.
    pub fn closes(&self) -> bool {
        matches!(
            self,
            SourceDdl::RewriteBesideDefault { .. } | SourceDdl::DisableCaptureTriggers
        ) || self.privilege()
    }

    /// Whether this is a privilege or ownership change (#2182): every one
    /// is undone at its close.
    pub fn privilege(&self) -> bool {
        matches!(
            self,
            SourceDdl::OwnerToHeir
                | SourceDdl::OwnerToOther
                | SourceDdl::RevokeSelect
                | SourceDdl::RevokeMembership
                | SourceDdl::RevokeSchemaUsage
                | SourceDdl::Rls { .. }
                | SourceDdl::WriterChange
                | SourceDdl::RevokeTargetWrites { .. }
                | SourceDdl::TargetRls { .. }
                | SourceDdl::TargetTriggerRefused { .. }
        )
    }

    /// Whether the change takes away something Trellis's role needs: every
    /// privilege change but an owner it inherits and the role change.
    pub fn refuses_trellis(&self) -> bool {
        self.privilege() && !matches!(self, SourceDdl::OwnerToHeir | SourceDdl::WriterChange)
    }

    /// Whether the application's writes run as the writer role for the
    /// window: the change takes away what the owner's sessions need to
    /// write, or is the role change itself.
    pub fn writes_as_writer(&self) -> bool {
        matches!(
            self,
            SourceDdl::OwnerToOther
                | SourceDdl::RevokeSelect
                | SourceDdl::RevokeSchemaUsage
                | SourceDdl::Rls { .. }
                | SourceDdl::WriterChange
        )
    }

    /// The target the change refuses the drain's writes to, for the
    /// drain's own refusals.
    pub fn refused_target(&self) -> Option<&str> {
        match self {
            SourceDdl::RevokeTargetWrites { target }
            | SourceDdl::TargetRls { target }
            | SourceDdl::TargetTriggerRefused { target } => Some(target),
            _ => None,
        }
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
            SourceDdl::OwnerToHeir => "owner_to_heir",
            SourceDdl::OwnerToOther => "owner_to_other",
            SourceDdl::RevokeSelect => "revoke_select",
            SourceDdl::RevokeMembership => "revoke_membership",
            SourceDdl::RevokeSchemaUsage => "revoke_schema_usage",
            SourceDdl::Rls {
                trellis_policy: false,
            } => "rls",
            SourceDdl::Rls {
                trellis_policy: true,
            } => "rls_trellis_policy",
            SourceDdl::WriterChange => "writer_change",
            SourceDdl::RevokeTargetWrites { .. } => "revoke_target_writes",
            SourceDdl::TargetRls { .. } => "target_rls",
            SourceDdl::TargetTriggerRefused { .. } => "target_trigger_refused",
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
