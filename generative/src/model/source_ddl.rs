//! Source DDL in the op stream (`docs/generative-test-suite.md` §7,
//! bucket 3): schema and maintenance changes to a tracked source, drawn
//! between a program's ops with changes in flight, and judged by "never
//! silently wrong" (`crate::run::judge`).

use trellis::dev::defs::IntWidth;
use trellis::dev::defs::ast::{Expr, JoinedTable, KeySpace, TransformDef, ValueType};

use crate::gaps::Gap;

/// What the server under test does that decides which register shapes an
/// event reproduces ([`SourceDdlEvent::listed_gaps`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Server {
    /// Whether an `ALTER COLUMN ... TYPE` stamps `pg_attver` into the
    /// `attoptions` of the column and of every column after it, as
    /// PostgreSQL 15.20, 16.16, 17.12, 18.7 and later minors do.
    pub versions_attributes: bool,
    /// Whether the server has `ALTER COLUMN ... SET EXPRESSION`
    /// (PostgreSQL 17 and later).
    pub sets_expressions: bool,
}

impl Server {
    /// Probes the server on a temporary table of `client`'s session, which
    /// it drops again.
    pub async fn probe(client: &tokio_postgres::Client) -> Result<Self, String> {
        client
            .batch_execute(
                "create temp table trellis_attver_probe (c int); \
                 alter table trellis_attver_probe alter column c type bigint",
            )
            .await
            .map_err(|e| format!("probe a retype: {e}"))?;
        let versions_attributes: bool = client
            .query_one(
                "select coalesce(array_to_string(attoptions, ','), '') like '%pg_attver=%' \
                 from pg_catalog.pg_attribute \
                 where attrelid = 'pg_temp.trellis_attver_probe'::regclass and attname = 'c'",
                &[],
            )
            .await
            .map_err(|e| format!("read the probe: {e}"))?
            .get(0);
        client
            .batch_execute("drop table pg_temp.trellis_attver_probe")
            .await
            .map_err(|e| format!("drop the probe: {e}"))?;
        let version: i32 = client
            .query_one("select current_setting('server_version_num')::int", &[])
            .await
            .map_err(|e| format!("read the server version: {e}"))?
            .get(0);
        Ok(Self {
            versions_attributes,
            sets_expressions: version >= 170_000,
        })
    }
}

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
    /// `ALTER COLUMN column TYPE to`, with `USING column + 1` (`+ 10` on a
    /// key column) when `using`: a re-type the triggers don't see. `shape`
    /// says what kind of change it is to the column's type before it, which
    /// decides what the register expects ([`SourceDdlEvent::listed_gaps`]).
    /// A key column's `USING` changes the keys later ops address, so the
    /// generator draws one only at the program's last op.
    Retype {
        column: String,
        to: SqlType,
        using: bool,
        shape: RetypeShape,
    },
    /// `column` renamed to `<column>_away`, the program's last DDL: later
    /// ops would name it. Its readers pause, and their `RESUME` refuses
    /// while the column is missing.
    RenameColumn { column: String },
    /// `column` dropped, the program's last DDL, as
    /// [`SourceDdl::RenameColumn`].
    DropColumn { column: String },
    /// `column` dropped and added again under its name and type in one
    /// group, so every row holds `NULL` under a new `attnum`. The pass
    /// re-reads the table for the readers still valid over it.
    DropAndAddColumn { column: String },
    /// `column` added, unread, with a volatile default: rewrites the table
    /// and changes no read column.
    AddColumn { column: String },
    /// `column` and `other`, of one type, swap names through a third name
    /// when the event fires, and swap back at its close: within one
    /// reconcile interval this is the register's [`Gap::ColumnNameSwap`];
    /// a pass in between re-reads the table each time.
    SwapColumnNames { column: String, other: String },
    /// `column` renamed to `<column>_away` and a column of its type added
    /// under its name when the event fires; at the close that column is
    /// dropped and the name given back. The register's other
    /// [`Gap::ColumnNameSwap`] shape.
    ShadowColumn { column: String },
    /// `column` dropped and added again as a stored generated column
    /// (`GENERATED ALWAYS AS (pk * 2) STORED`), the program's last DDL
    /// (later ops would write it); at the close, on a server that has it,
    /// its expression replaced (`SET EXPRESSION AS (pk * 3)`). The pass
    /// re-reads the table for each.
    GeneratedColumn { column: String },
    /// `column`'s `NOT NULL` flipped when the event fires (set where the
    /// column can hold `NULL`, dropped where it can't) and flipped back at
    /// its close. The generator takes a column no op writes `NULL` to.
    /// `SET`/`DROP NOT NULL` moves only the column's catalog row, so every
    /// reader stays exact. On a `KEY (…)` column (`key`) the same holds with
    /// changes in flight, since the key's encoding is recorded with the key
    /// (`docs/staging-and-claiming/capture-drift.md`), except that a key
    /// column that can hold `NULL` under a `NULLS DISTINCT` index no longer
    /// backs the key: a pass that sees it pauses the readers.
    FlipNotNull { column: String, key: bool },
    /// `ALTER COLUMN column SET DEFAULT 7` when the event fires, and the
    /// table rewritten by `VACUUM FULL` beside it when `rewrite`; `DROP
    /// DEFAULT` at its close. No value changes, and a rewrite with the
    /// column's default changed beside it isn't a same-type rewrite, so
    /// every reader stays exact.
    SetDefault { column: String, rewrite: bool },
    /// `column`, a `text` column, given `collation` when the event fires
    /// and the database's default collation again at its close, the
    /// window's changes drained in between
    /// ([`SourceDdl::drains_before_close`]). The type is unchanged and
    /// nothing is rewritten.
    Collate {
        column: String,
        collation: Collation,
    },
    /// Every capture trigger on the table disabled by name when the event
    /// fires and enabled again (`ENABLE ALWAYS`, as Trellis installs them)
    /// at its close, so the window's writes are never captured. The capture
    /// audit sees it, so it must pause the table's readers either way
    /// (#2184's decision 4).
    DisableCaptureTriggers,
    /// The table renamed to `<table>_away` when the event fires, the
    /// window's writes naming it there, and its name given back at the
    /// close. A pass in between finds the table gone from its name and
    /// pauses its readers; so does the drain within one reconcile interval,
    /// whose purge of the table's staged changes pauses them. A partitioned
    /// source's within one interval is the register's
    /// [`Gap::SourceRenameRoundtrip`].
    RenameTable,
    /// The table moved to [`MOVED_SCHEMA`] when the event fires, the
    /// window's writes finding it there through the application's
    /// `search_path`, and moved back at the close. The same register entry
    /// as [`Self::RenameTable`].
    MoveSchema,
    /// The table renamed to `<table>_away` for good, the program's last
    /// DDL: its readers pause, and their `RESUME` refuses while it is
    /// missing.
    RenameTableAway,
    /// The table dropped and created again under its name as a table swap
    /// does (`LIKE … INCLUDING ALL`, its rows copied, the old one dropped
    /// and the new one renamed onto its name), as the application. The
    /// window's writes reach the new table, which carries no capture
    /// trigger: a pass pauses its readers either way.
    RecreateTable,
    /// The valid unique index over exactly `columns` that backs the
    /// table's key, changed as `change` says: a `KEY (…)`'s index, the
    /// primary key's, or the unique key a 1-1 `JOIN`'s `b` is joined on.
    /// The pass checks the index by shape, not by oid
    /// (`docs/staging-and-claiming/capture-drift.md`), so a key left backed
    /// throughout keeps every reader exact; one left unbacked pauses its
    /// readers at a pass (a `KEY (…)`'s), or halts them at a drained change
    /// (T0118).
    KeyIndex {
        columns: Vec<String>,
        change: IndexChange,
    },
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
    /// sessions and its capture functions (`row_security = off`) are
    /// refused either way.
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
            SourceDdl::RewriteBesideDefault { .. }
                | SourceDdl::DisableCaptureTriggers
                | SourceDdl::SwapColumnNames { .. }
                | SourceDdl::ShadowColumn { .. }
                | SourceDdl::GeneratedColumn { .. }
                | SourceDdl::FlipNotNull { .. }
                | SourceDdl::SetDefault { .. }
                | SourceDdl::Collate { .. }
                | SourceDdl::RenameTable
                | SourceDdl::MoveSchema
                | SourceDdl::KeyIndex {
                    change: IndexChange::Drop | IndexChange::RebuildConcurrently,
                    ..
                }
        ) || self.privilege()
    }

    /// Whether the change leaves the table so that a later op's write
    /// would fail (a column gone, or made generated): the generator draws
    /// it only after the program's last op.
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            SourceDdl::RenameColumn { .. }
                | SourceDdl::DropColumn { .. }
                | SourceDdl::GeneratedColumn { .. }
                | SourceDdl::RenameTableAway
        )
    }

    /// The column whose indexes the change drops for good: one dropped and
    /// added again under its name, which carries none of them. Every other
    /// change keeps a column's indexes while it keeps its name, or gives
    /// them back at the close.
    pub fn unindexes(&self) -> Option<&str> {
        match self {
            SourceDdl::DropAndAddColumn { column } | SourceDdl::GeneratedColumn { column } => {
                Some(column)
            }
            _ => None,
        }
    }

    /// Whether the table leaves its name until the close: the generator
    /// keeps the table's partition maintenance out of the window, since it
    /// names the root, and with no pass inside the run drains the window
    /// while the table is away, which purges its staged changes and pauses
    /// its readers.
    pub fn moves_table(&self) -> bool {
        matches!(self, SourceDdl::RenameTable | SourceDdl::MoveSchema)
    }

    /// Whether the run drains the window's changes before the closing
    /// statements, with no pass beyond the window's own: a collation
    /// changes only what a drain computes, so without it the window could
    /// drain after the close.
    /// A key's index dropped drains too, so the window's changes always
    /// drain while it is gone, not only when a drain wins the race with
    /// the close.
    pub fn drains_before_close(&self) -> bool {
        matches!(
            self,
            SourceDdl::Collate { .. }
                | SourceDdl::KeyIndex {
                    change: IndexChange::Drop,
                    ..
                }
        )
    }

    /// Whether the run settles the changes already staged before the
    /// opening statements: a key's index dropped with a pass inside, so the
    /// pass, not a drain racing it, is what first meets the key unbacked.
    /// Only the window's changes are in flight across it.
    pub fn drains_before_open(&self) -> bool {
        matches!(
            self,
            SourceDdl::KeyIndex {
                change: IndexChange::Drop,
                ..
            }
        )
    }

    /// Whether the run stops the engine from the event's opens to its
    /// closes, so the changes in flight stay staged across both: a `KEY (…)`
    /// column's `NOT NULL` flip, whose changes staged under one nullability
    /// then drain under the other (#2147).
    pub fn holds_drain(&self) -> bool {
        matches!(self, SourceDdl::FlipNotNull { key: true, .. })
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

    /// Whether a plan's lanes may issue the event's window
    /// ([`SourceDdlPlan::lanes`]): not when the window's writes need the
    /// application's own session as the event leaves it (a schema on its
    /// `search_path`, the writer role) or the change is a privilege one.
    pub fn issues_from_lanes(&self) -> bool {
        !self.privilege() && !self.writes_as_writer() && *self != SourceDdl::MoveSchema
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
            SourceDdl::RenameTable => "rename_table",
            SourceDdl::MoveSchema => "move_schema",
            SourceDdl::RenameTableAway => "rename_table_away",
            SourceDdl::RecreateTable => "recreate_table",
            SourceDdl::KeyIndex { change, .. } => change.kind(),
            SourceDdl::RenameColumn { .. } => "rename_column",
            SourceDdl::DropColumn { .. } => "drop_column",
            SourceDdl::DropAndAddColumn { .. } => "drop_and_add_column",
            SourceDdl::AddColumn { .. } => "add_column",
            SourceDdl::SwapColumnNames { .. } => "swap_column_names",
            SourceDdl::ShadowColumn { .. } => "shadow_column",
            SourceDdl::GeneratedColumn { .. } => "generated_column",
            SourceDdl::Retype { using, shape, .. } => shape.kind(*using),
            SourceDdl::FlipNotNull { key: false, .. } => "flip_not_null",
            SourceDdl::FlipNotNull { key: true, .. } => "flip_key_not_null",
            SourceDdl::SetDefault { rewrite: false, .. } => "set_default",
            SourceDdl::SetDefault { rewrite: true, .. } => "set_default_beside_vacuum",
            SourceDdl::Collate {
                collation: Collation::C,
                ..
            } => "collate_c",
            SourceDdl::Collate {
                collation: Collation::CaseInsensitive,
                ..
            } => "collate_nondeterministic",
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
    /// The register entries this event may reproduce on `server`: a
    /// definition may end silently wrong after it only as one of these,
    /// detected by `self_check` and made exact by the entry's repair. Where
    /// the server versions attributes, a rewrite also moves the catalog row
    /// of every column after the rewritten one, so a read one shows the pass
    /// the rewrite: the shape is the register's only with no read column
    /// after it.
    pub fn listed_gaps(&self, program: &crate::model::Program, server: Server) -> Vec<Gap> {
        // A read column's name taken by another column and given back with
        // no pass between: the pass that comes after sees only its catalog
        // row's `xmin` moved.
        if matches!(
            (&self.action, self.pass_inside),
            (
                SourceDdl::SwapColumnNames { .. } | SourceDdl::ShadowColumn { .. },
                false
            )
        ) {
            return vec![Gap::ColumnNameSwap];
        }
        // A partitioned source renamed away and back, or moved to another
        // schema and back, with no pass between: its capture stages nothing
        // while its name doesn't resolve to the tree's root. A plain one's
        // drain while it is away purges its staged changes and pauses its
        // readers, which resume exact.
        if self.action.moves_table() && !self.pass_inside && self.partitioned(program) {
            return vec![Gap::SourceRenameRoundtrip];
        }
        // The pass doesn't see a read column's collation change, pass or no
        // pass: the rows computed under it keep its comparisons after the
        // close changes it back.
        if let SourceDdl::Collate {
            collation: Collation::CaseInsensitive,
            ..
        } = &self.action
        {
            return vec![Gap::NondeterministicCollation];
        }
        let column = match (&self.action, self.pass_inside) {
            (SourceDdl::RewriteBesideDefault { column }, false) => column,
            // The register's "widening that changes values": the pass sees a
            // widening and nothing to re-read, so a reader whose copies the
            // widening doesn't outgrow keeps the old values, pass or no pass.
            (
                SourceDdl::Retype {
                    column,
                    using: true,
                    shape: RetypeShape::Widen | RetypeShape::Outgrow,
                    ..
                },
                _,
            ) => column,
            _ => return Vec::new(),
        };
        if server.versions_attributes && self.read_column_after(program, column) {
            return Vec::new();
        }
        vec![Gap::AlterTypeRewrite]
    }

    /// Whether the event's table is partitioned.
    pub fn partitioned(&self, program: &crate::model::Program) -> bool {
        program
            .tables
            .iter()
            .any(|t| t.name == self.table && t.partitioning.is_some())
    }

    /// Whether a definition reads a column of the event's table that comes
    /// after `column` (by `attnum`). The key counts: capture images it.
    fn read_column_after(&self, program: &crate::model::Program, column: &str) -> bool {
        let Some(table) = program.tables.iter().find(|t| t.name == self.table) else {
            return false;
        };
        let read: Vec<String> = program
            .defs
            .iter()
            .filter(|d| reads(d, &table.name))
            .flat_map(|d| columns_read_of(d, &table.name))
            .chain([table.pk_col.clone()])
            .collect();
        table
            .columns
            .iter()
            .skip_while(|c| c.name != column)
            .skip(1)
            .any(|c| read.contains(&c.name))
    }

    /// The column the event takes from the table's names for good (renamed
    /// away or dropped), which a snapshot must no longer read.
    pub fn removed_column(&self) -> Option<&str> {
        match &self.action {
            SourceDdl::RenameColumn { column } | SourceDdl::DropColumn { column } => Some(column),
            _ => None,
        }
    }

    /// Makes `program`'s model of the event's table follow the event: a
    /// re-typed column takes its new type, which the oracle reads, and a
    /// column renamed away or dropped leaves it. Every other action leaves
    /// the model as it is.
    pub fn follow(&self, program: &mut crate::model::Program) {
        if let Some(column) = self.removed_column() {
            if let Some(table) = program.tables.iter_mut().find(|t| t.name == self.table) {
                table.columns.retain(|c| c.name != column);
            }
            return;
        }
        let SourceDdl::Retype { column, to, .. } = &self.action else {
            return;
        };
        let table = program
            .tables
            .iter_mut()
            .find(|t| t.name == self.table)
            .unwrap_or_else(|| panic!("{self:?} names no table of the program"));
        let col = table
            .columns
            .iter_mut()
            .find(|c| &c.name == column)
            .unwrap_or_else(|| panic!("{self:?} names no column of {}", table.name));
        col.value_type = to.value_type();
    }

    /// The last op the event spans.
    pub fn last_op(&self) -> usize {
        self.op + self.window
    }
}

/// The schema a [`SourceDdl::MoveSchema`] moves the table to.
pub const MOVED_SCHEMA: &str = "trellis_moved";

/// The name a [`SourceDdl::RenameTable`] or [`SourceDdl::RenameTableAway`]
/// gives the table.
pub fn table_away(table: &str) -> String {
    format!("{table}_away")
}

/// What a [`SourceDdl::KeyIndex`] does to the index backing a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexChange {
    /// Dropped when the event fires (its constraint, where it is one) and
    /// created again at the close, the window's changes drained in
    /// between ([`SourceDdl::drains_before_close`]): the drained change
    /// halts its readers (T0118), unless a pass in between paused them
    /// first (a `KEY (…)`'s; a pass skips a table with no primary key).
    /// They resume exact once it is back.
    Drop,
    /// Dropped and created again under its name in one transaction: a new
    /// oid of the same shape.
    Replace,
    /// A copy built `CONCURRENTLY` when the event fires, so the window
    /// runs with both; at the close the old one dropped `CONCURRENTLY`
    /// and the copy renamed onto its name, or swapped in as the constraint
    /// (`… USING INDEX`).
    RebuildConcurrently,
}

impl IndexChange {
    fn kind(self) -> &'static str {
        match self {
            IndexChange::Drop => "drop_key_index",
            IndexChange::Replace => "replace_key_index",
            IndexChange::RebuildConcurrently => "rebuild_key_index_concurrently",
        }
    }
}

/// The declared type of a column a [`SourceDdl::Retype`] changes, before or
/// after.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Numeric {
        precision: u8,
        scale: u8,
    },
    /// `numeric` with no modifier.
    NumericFree,
    Int4,
    Int8,
}

impl SqlType {
    /// The type as an `ALTER COLUMN ... TYPE` names it.
    pub fn sql(self) -> String {
        match self {
            SqlType::Numeric { precision, scale } => format!("numeric({precision},{scale})"),
            SqlType::NumericFree => "numeric".to_string(),
            SqlType::Int4 => "integer".to_string(),
            SqlType::Int8 => "bigint".to_string(),
        }
    }

    /// The model's type for a column of this type, which the oracle reads.
    pub fn value_type(self) -> ValueType {
        match self {
            SqlType::Numeric { .. } | SqlType::NumericFree => ValueType::Numeric,
            SqlType::Int4 => ValueType::Integer(IntWidth::Int4),
            SqlType::Int8 => ValueType::Integer(IntWidth::Int8),
        }
    }
}

/// The collation a [`SourceDdl::Collate`] gives a `text` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Collation {
    /// `"C"`: deterministic, and orders by byte where the database's ICU
    /// `en-US` doesn't (`'B' < 'b'`).
    C,
    /// An ICU collation that ignores case (`und-u-ks-level2`,
    /// `deterministic = false`): `'OPEN' = 'open'`.
    CaseInsensitive,
}

impl Collation {
    /// The statements that create the collation where it isn't built in.
    pub fn create(self) -> Option<&'static str> {
        match self {
            Collation::C => None,
            Collation::CaseInsensitive => Some(
                "CREATE COLLATION IF NOT EXISTS public.trellis_ci \
                 (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
            ),
        }
    }

    /// The collation as a `COLLATE` clause names it.
    pub fn sql(self) -> &'static str {
        match self {
            Collation::C => "\"C\"",
            Collation::CaseInsensitive => "public.trellis_ci",
        }
    }
}

/// What a [`SourceDdl::Retype`] does to the column's type
/// (`docs/staging-and-claiming/capture-drift.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetypeShape {
    /// A pure widening every copy Trellis keeps still holds: more `numeric`
    /// precision at the same scale, or the modifier dropped. Without
    /// `USING` no value changes, and nothing needs doing.
    Widen,
    /// Neither a widening nor a re-type of the key: a narrower `numeric`
    /// scale, or `numeric` to `integer`. A changed column: its readers are
    /// re-read, or paused where define would now read them otherwise.
    Narrow,
    /// A widening that outgrows a copy a reader may keep: `integer` to
    /// `bigint` or to `numeric`. Those readers pause, and `RESUME` re-types
    /// and rebuilds them.
    Outgrow,
    /// A re-type of the primary key column.
    Key,
}

impl RetypeShape {
    fn kind(self, using: bool) -> &'static str {
        match (self, using) {
            (RetypeShape::Widen, false) => "retype_widen",
            (RetypeShape::Widen, true) => "retype_widen_using",
            (RetypeShape::Narrow, false) => "retype_narrow",
            (RetypeShape::Narrow, true) => "retype_narrow_using",
            (RetypeShape::Outgrow, false) => "retype_outgrow",
            (RetypeShape::Outgrow, true) => "retype_outgrow_using",
            (RetypeShape::Key, false) => "retype_key",
            (RetypeShape::Key, true) => "retype_key_using",
        }
    }
}

/// Whether `def` reads `table`: as its `FROM`, or as a table it joins.
pub fn reads(def: &TransformDef, table: &str) -> bool {
    def.source == table || joined(def).any(|j| j.right == table)
}

/// The tables `def` joins, none for a definition without a `JOIN`.
fn joined(def: &TransformDef) -> impl Iterator<Item = &JoinedTable> {
    match &def.key_space {
        KeySpace::Join(spec) | KeySpace::JoinOneToOne(spec) => spec.joins.as_slice(),
        _ => &[],
    }
    .iter()
}

/// The columns of `table` that `def` reads. A definition without a `JOIN`
/// reads only its `FROM`, by bare names; a `JOIN` names each column with
/// its table, in its fields, filter, `ON` terms and `GROUP BY`.
fn columns_read_of(def: &TransformDef, table: &str) -> Vec<String> {
    let (KeySpace::Join(spec) | KeySpace::JoinOneToOne(spec)) = &def.key_space else {
        return crate::generate::columns_read(def);
    };
    let mut out = Vec::new();
    let mut add = |expr: &Expr| {
        expr.walk(&mut |e| {
            if let Expr::SourceColumn {
                table: t, column, ..
            } = e
                && t == table
            {
                out.push(column.clone());
            }
        });
    };
    for field in &def.fields {
        add(&field.expr);
    }
    if let Some(predicate) = def.predicate.filter() {
        add(predicate);
    }
    let qualified = spec
        .joins
        .iter()
        .flat_map(|j| j.on.iter().flat_map(|on| [&on.lhs, &on.rhs]))
        .chain(&spec.group_by);
    out.extend(
        qualified
            .filter(|c| c.table == table)
            .map(|c| c.column.clone()),
    );
    out
}

/// The source-DDL schedule for one program run: events in op order, none
/// starting before the previous one's window has ended.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SourceDdlPlan {
    pub events: Vec<SourceDdlEvent>,
    /// How many lanes issue each event's window, each on its own
    /// connection and in program order, racing the others, as the
    /// concurrent tier deals a burst (`crate::generate::deal_to_lanes`).
    /// `0` or `1` issues it in order on the run's own connection, as does
    /// an event whose action doesn't [`SourceDdl::issues_from_lanes`].
    pub lanes: usize,
}
