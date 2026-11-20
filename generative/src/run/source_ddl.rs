//! Source DDL in the op stream (#2184, `docs/generative-test-suite.md` §7,
//! bucket 3), judged by "never silently wrong" ([`super::judge`]).
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
use crate::model::{Program, SourceDdl, SourceDdlEvent, SourceDdlPlan};

use super::judge::{Judged, PassStepper, judge};
use super::{Outcome, RunError, apply_and_check_outcome};

/// The engine's reconcile interval in this tier: longer than any case, so
/// only the run's own passes run once the engine has started.
const NO_PASS_DURING_A_CASE: Duration = Duration::from_secs(3600);

/// What a source-DDL run judged: each check's verdicts, with the event that
/// closed just before it (`None` for a check after a plain op).
#[derive(Debug, Clone, Default)]
pub struct SourceDdlRun {
    pub checks: Vec<(Option<SourceDdlEvent>, Vec<Judged>)>,
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
    run_convergence_with_source_ddl_allowing(
        backend,
        pool,
        program,
        plan,
        SourceDdlEvent::listed_gaps,
    )
    .await
}

/// [`run_convergence_with_source_ddl`] with the register entries each event
/// may reproduce given by `allowed` instead of the event's own
/// [`SourceDdlEvent::listed_gaps`]: for the control that shows an untagged
/// gap fails the run.
pub async fn run_convergence_with_source_ddl_allowing(
    backend: &mut ManualBackend,
    pool: &Pool,
    program: &Program,
    plan: &SourceDdlPlan,
    allowed: impl Fn(&SourceDdlEvent) -> Vec<Gap>,
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
    // The first pass installs capture, so an event on the first op finds it.
    passes.step().await.map_err(RunError::Install)?;

    let mut run = SourceDdlRun::default();
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
            let statements = Statements::resolve(&passes, program, event)
                .await
                .map_err(fired)?;
            execute(backend, &statements.opens).await.map_err(fired)?;
            if event.pass_inside {
                passes.step().await.map_err(fired)?;
            }
            for window_op in op_index + 1..=event.last_op() {
                apply_and_check_outcome(backend, window_op, &program.ops[window_op], false).await?;
            }
            execute(backend, &statements.closes).await.map_err(fired)?;
            op_index = event.last_op();
        }
        let gaps = event.map(&allowed).unwrap_or_default();
        let judged = judge(
            backend,
            &passes,
            pool,
            program,
            &program.defs,
            op_index,
            |def| allowed_for(event, def, &gaps),
        )
        .await?;
        run.checks.push((event.cloned(), judged));
        op_index += 1;
    }
    Ok(run)
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
        program: &Program,
        event: &SourceDdlEvent,
    ) -> Result<Self, String> {
        let table = format!("public.{}", quote(&event.table));
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

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{DefShape, TableSpec, build_program_multi_with_shapes};

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
        let gaps = event.listed_gaps();
        assert_eq!(gaps, vec![Gap::AlterTypeRewrite]);
        assert_eq!(allowed_for(Some(&event), &program.defs[0], &gaps), gaps);
        assert!(allowed_for(Some(&event), &program.defs[1], &gaps).is_empty());
        assert!(allowed_for(None, &program.defs[0], &gaps).is_empty());
    }
}
