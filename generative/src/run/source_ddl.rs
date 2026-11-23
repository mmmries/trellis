//! Source DDL in the op stream (#2184, `docs/generative-test-suite.md` §7,
//! bucket 3), judged by "never silently wrong" ([`super::judge`]), and the
//! privilege and ownership changes run the same way (#2182,
//! [`super::privilege`]).
//!
//! The engine runs with a reconcile interval too long to fire during a
//! case, and the run steps the passes itself ([`PassStepper`]): an event's
//! statements and the ops in its window land inside one reconcile interval
//! unless the event draws a pass inside it. Every op, an event's window
//! included, is followed by a judged check, which settles with passes.
//!
//! Like [`super::run_convergence_with_db_admin`] this is concrete over
//! [`ManualBackend`], and the whole program installs up front.

use std::time::Duration;

use trellis::dev::defs::ast::TransformDef;
use trellis::dev::pool::Pool;

use crate::backend::{Backend, ManualBackend};
use crate::gaps::Gap;
use crate::model::{Program, RetypeShape, Server, SourceDdl, SourceDdlEvent, SourceDdlPlan};

use super::judge::{Judged, PassStepper, judge};
use super::privilege::{self, RefusalSeen};
use super::{Outcome, RunError, apply_and_check_outcome};

/// The engine's reconcile interval in this tier: longer than any case, so
/// only the run's own passes run once the engine has started.
const NO_PASS_DURING_A_CASE: Duration = Duration::from_secs(3600);

/// What a source-DDL run judged: each check's verdicts, with the event that
/// closed just before it (`None` for a check after a plain op).
#[derive(Debug, Clone, Default)]
pub struct SourceDdlRun {
    pub checks: Vec<(Option<SourceDdlEvent>, Vec<Judged>)>,
    /// What the drain did on each drain refusal's target while it lasted
    /// ([`SourceDdl::refused_target`]).
    pub refusals: Vec<(SourceDdlEvent, RefusalSeen)>,
    /// The reconcile passes stepped inside a privilege change that failed,
    /// with their errors: the engine's own loop logs one and runs the next.
    pub refused_passes: Vec<(SourceDdlEvent, String)>,
    /// The server the run probed, which decides the events' listed gaps.
    pub server: Server,
}

impl SourceDdlRun {
    /// [`Outcome::Ran`]: a run returns this only after driving every op.
    pub fn outcome(&self) -> Outcome {
        Outcome::Ran
    }
}

/// Runs `program` with `plan`'s source DDL between its ops, judging every
/// definition after each op and each event.
pub async fn run_convergence_with_source_ddl(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    plan: &SourceDdlPlan,
) -> Result<SourceDdlRun, RunError> {
    run_convergence_with_source_ddl_allowing(backend, pool, program, plan, |event, server| {
        event.listed_gaps(program, server)
    })
    .await
}

/// [`run_convergence_with_source_ddl`] with the register entries each event
/// may reproduce on the server given by `allowed` instead of the event's
/// own [`SourceDdlEvent::listed_gaps`]: for the control that shows an
/// untagged gap fails the run.
pub async fn run_convergence_with_source_ddl_allowing(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    plan: &SourceDdlPlan,
    allowed: impl Fn(&SourceDdlEvent, Server) -> Vec<Gap>,
) -> Result<SourceDdlRun, RunError> {
    let mut previous_end = None;
    for event in &plan.events {
        assert!(
            event.last_op() < program.ops.len(),
            "source-DDL event {event:?} runs past the last op ({} ops) — a generator bug",
            program.ops.len()
        );
        assert!(
            previous_end.is_none_or(|end| event.op > end),
            "source-DDL event {event:?} starts inside the previous one's window — a generator bug"
        );
        previous_end = Some(event.last_op());
    }

    backend.set_reconcile_interval(NO_PASS_DURING_A_CASE);
    backend
        .install(program)
        .await
        .map_err(|e| RunError::Install(format!("{e:?}")))?;
    let passes = PassStepper::connect(backend)
        .await
        .map_err(RunError::Install)?;
    let server = passes.server().await.map_err(RunError::Install)?;
    // The first pass installs capture, so an event on the first op finds it.
    passes.step().await.map_err(RunError::Install)?;

    let mut run = SourceDdlRun {
        server,
        ..SourceDdlRun::default()
    };
    // The program as the events have left it: the oracle reads a re-typed
    // column's new type.
    let mut model = program.clone();
    let mut op_index = 0;
    while op_index < program.ops.len() {
        apply_and_check_outcome(backend, op_index, &program.ops[op_index], false).await?;
        let event = plan.events.iter().find(|e| e.op == op_index);
        if let Some(event) = event {
            let fired = |error: String| RunError::SourceDdl {
                op_index,
                event: Box::new(event.clone()),
                error,
            };
            let statements = Statements::resolve(&passes, backend, program, event, server)
                .await
                .map_err(fired)?;
            execute(backend, &statements.opens).await.map_err(fired)?;
            event.follow(&mut model);
            if let Some(column) = event.removed_column() {
                backend.forget_column(&event.table, column);
            }
            let writer = event.action.writes_as_writer();
            if writer {
                let dsn = backend.roles().writer();
                backend
                    .write_as(Some(&dsn))
                    .await
                    .map_err(|e| fired(format!("write as {dsn}: {e:?}")))?;
            }
            if event.pass_inside {
                match passes.step().await {
                    Ok(()) => {}
                    // A pass refused what the change took away fails as
                    // the engine's own loop's would; the judged check's
                    // passes, after the close, must not.
                    Err(error) if event.action.refuses_trellis() && refusal(&error) => {
                        run.refused_passes.push((event.clone(), error));
                    }
                    Err(error) => return Err(fired(error)),
                }
            }
            for window_op in op_index + 1..=event.last_op() {
                apply_and_check_outcome(backend, window_op, &program.ops[window_op], false).await?;
            }
            if let Some(target) = event.action.refused_target() {
                let seen =
                    privilege::observe_refusal(backend, target)
                        .await
                        .map_err(|problem| RunError::NeverSilentlyWrong {
                            op_index: event.last_op(),
                            target: target.to_string(),
                            problem,
                            detail: format!("{event:?}"),
                        })?;
                run.refusals.push((event.clone(), seen));
            }
            // A collation decides only what a drain computes: drain the
            // window under it before it's changed back. With no pass since
            // the last check settled, nothing waits for one; after the
            // window's pass, a build it queued would, so settle beside
            // passes, which don't see a collation.
            if event.action.drains_before_close() {
                if event.pass_inside {
                    passes.settle(backend).await
                } else {
                    backend.quiesce().await.map_err(|e| format!("{e:?}"))
                }
                .map_err(|e| fired(format!("drain the window: {e}")))?;
            }
            execute(backend, &statements.closes).await.map_err(fired)?;
            if writer {
                backend
                    .write_as(None)
                    .await
                    .map_err(|e| fired(format!("write as the application again: {e:?}")))?;
            }
            op_index = event.last_op();
        }
        let gaps = event.map(|e| allowed(e, server)).unwrap_or_default();
        let judged = judge(
            backend,
            &passes,
            pool,
            &model,
            &model.defs,
            op_index,
            |def| allowed_for(event, def, &gaps),
        )
        .await?;
        run.checks.push((event.cloned(), judged));
        op_index += 1;
    }
    Ok(run)
}

/// Whether a pass's error is Postgres refusing Trellis's role something
/// (SQLSTATE `42501`), so an in-window pass failing for any other reason
/// still fails the run.
fn refusal(error: &str) -> bool {
    [
        "permission denied",
        "must be owner",
        "row-level security",
        "42501",
    ]
    .iter()
    .any(|refused| error.contains(refused))
}

/// The entries `def` may end silently wrong as at the check after `event`:
/// `gaps` when it reads the event's table, none otherwise, so a shape's
/// entry never excuses a definition the shape didn't touch.
fn allowed_for(event: Option<&SourceDdlEvent>, def: &TransformDef, gaps: &[Gap]) -> Vec<Gap> {
    match event {
        Some(event) if def.source == event.table => gaps.to_vec(),
        _ => Vec::new(),
    }
}

/// An event's statements, with the names they need read from the catalog.
struct Statements {
    opens: Vec<String>,
    closes: Vec<String>,
}

impl Statements {
    async fn resolve(
        passes: &PassStepper,
        backend: &ManualBackend,
        program: &Program,
        event: &SourceDdlEvent,
        server: Server,
    ) -> Result<Self, String> {
        let table = format!("public.{}", quote(&event.table));
        if event.action.privilege() {
            let target = event
                .action
                .refused_target()
                .map(|target| backend.qualified_target(target));
            let (opens, closes) = privilege::statements(&event.action, &table, target.as_deref());
            return Ok(Self { opens, closes });
        }
        let rewrite = |column: &str, ty: &str| {
            let column = quote(column);
            format!("ALTER TABLE {table} ALTER COLUMN {column} TYPE {ty} USING {column} + 1")
        };
        let (opens, closes) = match &event.action {
            SourceDdl::VacuumFull => (vec![format!("VACUUM FULL {table}")], Vec::new()),
            SourceDdl::Cluster => {
                let rows = passes
                    .query(
                        "select indexrelid::regclass::text from pg_index \
                         where indrelid = $1::text::regclass and indisunique \
                         order by indisprimary desc, indexrelid limit 1",
                        &[&table],
                    )
                    .await?;
                let index: String = rows
                    .first()
                    .ok_or_else(|| format!("{table} has no unique index to cluster on"))?
                    .get(0);
                (vec![format!("CLUSTER {table} USING {index}")], Vec::new())
            }
            SourceDdl::Reindex { concurrently } => (
                vec![format!(
                    "REINDEX TABLE {}{table}",
                    if *concurrently { "CONCURRENTLY " } else { "" }
                )],
                Vec::new(),
            ),
            SourceDdl::RewriteColumn { column } => {
                let ty = column_type(passes, &table, column).await?;
                (vec![rewrite(column, &ty)], Vec::new())
            }
            SourceDdl::RewriteBesideDefault { column } => {
                let ty = column_type(passes, &table, column).await?;
                (
                    vec![format!(
                        "ALTER TABLE {table} ALTER COLUMN {} SET DEFAULT 0",
                        quote(column)
                    )],
                    vec![rewrite(column, &ty)],
                )
            }
            SourceDdl::Retype {
                column,
                to,
                using,
                shape,
            } => {
                let quoted = quote(column);
                let using = match (using, shape) {
                    (false, _) => String::new(),
                    (true, RetypeShape::Key) => format!(" USING {quoted} + 10"),
                    (true, _) => format!(" USING {quoted} + 1"),
                };
                (
                    vec![format!(
                        "ALTER TABLE {table} ALTER COLUMN {quoted} TYPE {}{using}",
                        to.sql()
                    )],
                    Vec::new(),
                )
            }
            SourceDdl::DisableCaptureTriggers => {
                let rows = passes
                    .query(
                        "select tgname::text from pg_trigger \
                         where tgrelid = $1::text::regclass and not tgisinternal order by tgname",
                        &[&table],
                    )
                    .await?;
                let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
                if names.is_empty() {
                    return Err(format!("{table} has no capture triggers to disable"));
                }
                let alter = |how: &str| {
                    names
                        .iter()
                        .map(|name| format!("ALTER TABLE {table} {how} TRIGGER {}", quote(name)))
                        .collect::<Vec<_>>()
                };
                (alter("DISABLE"), alter("ENABLE ALWAYS"))
            }
            SourceDdl::RenameColumn { column } => (
                vec![format!(
                    "ALTER TABLE {table} RENAME COLUMN {} TO {}",
                    quote(column),
                    quote(&away(column))
                )],
                Vec::new(),
            ),
            SourceDdl::DropColumn { column } => (
                vec![format!("ALTER TABLE {table} DROP COLUMN {}", quote(column))],
                Vec::new(),
            ),
            SourceDdl::DropAndAddColumn { column } => {
                let ty = column_type(passes, &table, column).await?;
                let quoted = quote(column);
                (
                    vec![
                        format!("ALTER TABLE {table} DROP COLUMN {quoted}"),
                        format!("ALTER TABLE {table} ADD COLUMN {quoted} {ty}"),
                    ],
                    Vec::new(),
                )
            }
            SourceDdl::AddColumn { column } => (
                vec![format!(
                    "ALTER TABLE {table} ADD COLUMN {} double precision DEFAULT random()",
                    quote(column)
                )],
                Vec::new(),
            ),
            SourceDdl::SwapColumnNames { column, other } => {
                // A swap undoes itself: the close runs the same renames.
                let rename = |from: &str, to: &str| {
                    format!(
                        "ALTER TABLE {table} RENAME COLUMN {} TO {}",
                        quote(from),
                        quote(to)
                    )
                };
                let swap = vec![
                    rename(column, &away(column)),
                    rename(other, column),
                    rename(&away(column), other),
                ];
                (swap.clone(), swap)
            }
            SourceDdl::ShadowColumn { column } => {
                let ty = column_type(passes, &table, column).await?;
                let (quoted, away) = (quote(column), quote(&away(column)));
                (
                    vec![
                        format!("ALTER TABLE {table} RENAME COLUMN {quoted} TO {away}"),
                        format!("ALTER TABLE {table} ADD COLUMN {quoted} {ty}"),
                    ],
                    vec![
                        format!("ALTER TABLE {table} DROP COLUMN {quoted}"),
                        format!("ALTER TABLE {table} RENAME COLUMN {away} TO {quoted}"),
                    ],
                )
            }
            SourceDdl::GeneratedColumn { column } => {
                let ty = column_type(passes, &table, column).await?;
                let quoted = quote(column);
                let pk = program
                    .tables
                    .iter()
                    .find(|t| t.name == event.table)
                    .map(|t| quote(&t.pk_col))
                    .ok_or_else(|| format!("{event:?} names no table of the program"))?;
                (
                    vec![
                        format!("ALTER TABLE {table} DROP COLUMN {quoted}"),
                        format!(
                            "ALTER TABLE {table} ADD COLUMN {quoted} {ty} \
                             GENERATED ALWAYS AS ({pk} * 2) STORED"
                        ),
                    ],
                    if server.sets_expressions {
                        vec![format!(
                            "ALTER TABLE {table} ALTER COLUMN {quoted} SET EXPRESSION AS ({pk} * 3)"
                        )]
                    } else {
                        Vec::new()
                    },
                )
            }
            SourceDdl::FlipNotNull { column, .. } => {
                let rows = passes
                    .query(
                        "select attnotnull from pg_attribute \
                         where attrelid = $1::text::regclass and attname = $2 \
                         and not attisdropped",
                        &[&table, column],
                    )
                    .await?;
                let not_null: bool = rows
                    .first()
                    .ok_or_else(|| format!("{table} has no column {column}"))?
                    .get(0);
                let alter =
                    |how: &str| format!("ALTER TABLE {table} ALTER COLUMN {} {how}", quote(column));
                let (open, close) = if not_null {
                    ("DROP NOT NULL", "SET NOT NULL")
                } else {
                    ("SET NOT NULL", "DROP NOT NULL")
                };
                (vec![alter(open)], vec![alter(close)])
            }
            SourceDdl::SetDefault { column, rewrite } => {
                let alter =
                    |how: &str| format!("ALTER TABLE {table} ALTER COLUMN {} {how}", quote(column));
                let mut opens = vec![alter("SET DEFAULT 7")];
                if *rewrite {
                    opens.push(format!("VACUUM FULL {table}"));
                }
                (opens, vec![alter("DROP DEFAULT")])
            }
            SourceDdl::Collate { column, collation } => {
                let ty = column_type(passes, &table, column).await?;
                let collate = |to: &str| {
                    format!(
                        "ALTER TABLE {table} ALTER COLUMN {} TYPE {ty} COLLATE {to}",
                        quote(column)
                    )
                };
                let opens = collation
                    .create()
                    .map(str::to_string)
                    .into_iter()
                    .chain([collate(collation.sql())])
                    .collect();
                (opens, vec![collate("\"default\"")])
            }
            other => unreachable!("{other:?} is a privilege change"),
        };
        debug_assert!(program.tables.iter().any(|t| t.name == event.table));
        Ok(Self { opens, closes })
    }
}

/// `column`'s type on `table` as Postgres prints it, so a same-type rewrite
/// restates it exactly.
async fn column_type(passes: &PassStepper, table: &str, column: &str) -> Result<String, String> {
    let rows = passes
        .query(
            "select format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = $1::text::regclass and attname = $2 and not attisdropped",
            &[&table, &column],
        )
        .await?;
    rows.first()
        .map(|row| row.get(0))
        .ok_or_else(|| format!("{table} has no column {column}"))
}

async fn execute(backend: &ManualBackend, statements: &[String]) -> Result<(), String> {
    for statement in statements {
        backend
            .execute_admin(statement)
            .await
            .map_err(|e| format!("{statement}: {e:?}"))?;
    }
    Ok(())
}

/// The name a renamed-away column takes.
fn away(column: &str) -> String {
    format!("{column}_away")
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{DefShape, TableSpec, build_program_multi_with_shapes};

    /// Only a refusal, inside a change that takes something from Trellis,
    /// may fail an in-window pass.
    #[test]
    fn only_a_refused_pass_inside_a_refusing_change_is_recorded() {
        assert!(refusal("db error: ERROR: permission denied for table t0"));
        assert!(refusal("must be owner of table t0"));
        assert!(!refusal("db error: ERROR: deadlock detected"));
        assert!(!refusal("connection closed"));
        assert!(SourceDdl::RevokeMembership.refuses_trellis());
        assert!(!SourceDdl::OwnerToHeir.refuses_trellis());
        assert!(!SourceDdl::WriterChange.refuses_trellis());
        assert!(!SourceDdl::VacuumFull.refuses_trellis());
    }

    fn table() -> TableSpec {
        TableSpec {
            seed_values: vec![(Some(1), Some(1))],
            text_values: vec![None],
            bool_values: vec![None],
            uuid_values: vec![None],
            grain_values: vec![Some("0".to_string())],
            mutates: Vec::new(),
        }
    }

    /// A shape's entry excuses only the definitions that read the table it
    /// changed: a wrong reader of another table still fails the run.
    #[test]
    fn a_shapes_gaps_reach_only_the_readers_of_its_table() {
        let program = build_program_multi_with_shapes(
            &[table(), table()],
            &[(0, DefShape::OneToOne), (1, DefShape::OneToOne)],
        );
        let event = SourceDdlEvent {
            op: 0,
            table: program.defs[0].source.clone(),
            action: SourceDdl::RewriteBesideDefault {
                column: "c1".to_string(),
            },
            window: 0,
            pass_inside: false,
        };
        assert_ne!(program.defs[0].source, program.defs[1].source);
        let gaps = event.listed_gaps(&program, Server::default());
        assert_eq!(gaps, vec![Gap::AlterTypeRewrite]);
        assert_eq!(allowed_for(Some(&event), &program.defs[0], &gaps), gaps);
        assert!(allowed_for(Some(&event), &program.defs[1], &gaps).is_empty());
        assert!(allowed_for(None, &program.defs[0], &gaps).is_empty());
    }
}
