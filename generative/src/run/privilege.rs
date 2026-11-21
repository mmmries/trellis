//! Privilege and ownership changes in the op stream (#2182), run by
//! [`super::run_convergence_with_source_ddl`] as source-DDL events
//! ([`SourceDdl::privilege`]) and judged the same way.
//!
//! Its decision (2026-11-20) sets what each may end as:
//!
//! - **A capture-side refusal** (Trellis's `SELECT` or `USAGE` revoked,
//!   row-level security on its role, an owner it doesn't inherit) ends
//!   paused with a reason, or a halt; the judge holds it to that, and
//!   [`super::RunError::CaptureFailedWriter`] fails a run in which an
//!   application write fails inside a capture function (ADR-0002 I6).
//! - **The drain's own refusal on a target** may also hold its page up
//!   while the cause lasts, as `docs/observability.md` ("A halting
//!   failure") documents: `observe_refusal` checks the holdup is reported
//!   (`drain_failure`, with the refusal's SQLSTATE) before the change is
//!   undone, and the judged check after the close needs the page committed.

use std::time::{Duration, Instant};

use trellis::{Config, Trellis, TrellisOptions};

use crate::backend::{
    APP_ROLE, HEIR_ROLE, ManualBackend, OTHER_OWNER_ROLE, TRELLIS_ROLE, WRITER_ROLE,
};
use crate::model::SourceDdl;

/// The writer's grants on every table the application created, so its
/// writes, and its triggers' writes to other tables, go on.
fn grant_writer() -> String {
    format!(
        "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA public \
         TO {WRITER_ROLE}"
    )
}

/// `action`'s opening and closing statements on the qualified, quoted
/// `table`; `target` is the qualified, quoted target a drain refusal names.
pub(super) fn statements(
    action: &SourceDdl,
    table: &str,
    target: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let owner = |role: &str| format!("ALTER TABLE {table} OWNER TO {role}");
    match action {
        SourceDdl::OwnerToHeir => (vec![owner(HEIR_ROLE)], vec![owner(APP_ROLE)]),
        SourceDdl::OwnerToOther => (
            vec![grant_writer(), owner(OTHER_OWNER_ROLE)],
            vec![owner(APP_ROLE)],
        ),
        SourceDdl::RevokeSelect => (
            vec![
                grant_writer(),
                format!("REVOKE SELECT ON {table} FROM {TRELLIS_ROLE}, {APP_ROLE}"),
            ],
            vec![format!("GRANT SELECT ON {table} TO {APP_ROLE}")],
        ),
        SourceDdl::RevokeMembership => (
            vec![format!("REVOKE {APP_ROLE} FROM {TRELLIS_ROLE}")],
            vec![format!("GRANT {APP_ROLE} TO {TRELLIS_ROLE}")],
        ),
        SourceDdl::RevokeSchemaUsage => (
            vec![
                grant_writer(),
                format!("GRANT USAGE ON SCHEMA public TO {WRITER_ROLE}"),
                format!("REVOKE USAGE ON SCHEMA public FROM PUBLIC, {APP_ROLE}"),
            ],
            vec![format!(
                "GRANT USAGE ON SCHEMA public TO PUBLIC, {APP_ROLE}"
            )],
        ),
        SourceDdl::Rls { trellis_policy } => {
            let mut opens = vec![
                grant_writer(),
                format!(
                    "CREATE POLICY gen_writer ON {table} TO {WRITER_ROLE} \
                     USING (true) WITH CHECK (true)"
                ),
            ];
            if *trellis_policy {
                opens.push(format!(
                    "CREATE POLICY gen_trellis ON {table} FOR SELECT TO {TRELLIS_ROLE} \
                     USING (true)"
                ));
            }
            opens.push(format!(
                "ALTER TABLE {table} ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY"
            ));
            (
                opens,
                vec![
                    format!(
                        "ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY, \
                         DISABLE ROW LEVEL SECURITY"
                    ),
                    format!("DROP POLICY IF EXISTS gen_writer ON {table}"),
                    format!("DROP POLICY IF EXISTS gen_trellis ON {table}"),
                ],
            )
        }
        SourceDdl::WriterChange => (vec![grant_writer()], Vec::new()),
        SourceDdl::RevokeTargetWrites { .. } => {
            let target = target.expect("a drain refusal names its target");
            (
                vec![format!(
                    "REVOKE INSERT, UPDATE, DELETE ON {target} FROM {TRELLIS_ROLE}"
                )],
                vec![format!(
                    "GRANT INSERT, UPDATE, DELETE ON {target} TO {TRELLIS_ROLE}"
                )],
            )
        }
        SourceDdl::TargetRls { .. } => {
            let target = target.expect("a drain refusal names its target");
            (
                vec![format!(
                    "ALTER TABLE {target} ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY"
                )],
                vec![format!(
                    "ALTER TABLE {target} NO FORCE ROW LEVEL SECURITY, DISABLE ROW LEVEL SECURITY"
                )],
            )
        }
        SourceDdl::TargetTriggerRefused { .. } => {
            let target = target.expect("a drain refusal names its target");
            (
                vec![
                    "CREATE TABLE public.gen_refused (n int)".to_string(),
                    "CREATE FUNCTION public.gen_refused_write() RETURNS trigger \
                     LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.gen_refused VALUES (1); \
                     RETURN NULL; END $$"
                        .to_string(),
                    format!(
                        "CREATE TRIGGER gen_refused AFTER INSERT OR UPDATE OR DELETE ON {target} \
                         FOR EACH STATEMENT EXECUTE FUNCTION public.gen_refused_write()"
                    ),
                ],
                vec![
                    format!("DROP TRIGGER gen_refused ON {target}"),
                    "DROP FUNCTION public.gen_refused_write()".to_string(),
                    "DROP TABLE public.gen_refused".to_string(),
                ],
            )
        }
        other => unreachable!("{other:?} is not a privilege change"),
    }
}

/// What the drain did on a refused target while the refusal lasted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalSeen {
    /// The definition writing the target halted: paused with `reason`.
    Halted { reason: String },
    /// The drain held its page up, reported as the definition's
    /// `drain_failure` with the refusal's SQLSTATE.
    HeldUp { error: String },
    /// Neither within [`REFUSAL_WAIT`]: no change reached the target's
    /// drain while it was refused.
    Unseen,
}

impl RefusalSeen {
    /// A short name for coverage counts.
    pub fn kind(&self) -> &'static str {
        match self {
            RefusalSeen::Halted { .. } => "halted",
            RefusalSeen::HeldUp { .. } => "held_up",
            RefusalSeen::Unseen => "unseen",
        }
    }
}

/// How long `observe_refusal` waits for the drain to meet a refusal. Kept
/// short: a holdup's attempts count up meanwhile, and #2185's bounded-retry
/// check counts them at the next quiesce.
pub const REFUSAL_WAIT: Duration = Duration::from_secs(3);

/// Waits, while `target`'s refusal lasts, for its definition to halt or
/// report the drain holding its page up. A holdup that reports no
/// `drain_failure`, or one without the refusal's SQLSTATE (`42501`), is
/// silent, and fails.
pub(super) async fn observe_refusal(
    backend: &ManualBackend,
    target: &str,
) -> Result<RefusalSeen, String> {
    let config = Config::from_dsn(backend.trellis_dsn().to_string())
        .and_then(|c| c.with_target_schema(backend.target_schema().to_string()))
        .map_err(|e| e.to_string())?;
    let facade = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|e| e.to_string())?;
    let deadline = Instant::now() + REFUSAL_WAIT;
    let seen = loop {
        let status = facade
            .status(target)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("{target} has no definition"))?;
        if status.status.as_str() == "paused" {
            let reason = status
                .capture_failure
                .as_ref()
                .map(|f| format!("{:?}: {}", f.kind, f.error))
                .unwrap_or_default();
            if reason.is_empty() {
                break Err(format!(
                    "{target} paused with no reason while its writes were refused"
                ));
            }
            break Ok(RefusalSeen::Halted { reason });
        }
        if let Some(failure) = &status.drain_failure {
            break match failure.sqlstate.as_deref() {
                Some("42501") => Ok(RefusalSeen::HeldUp {
                    error: failure.error.clone(),
                }),
                other => Err(format!(
                    "{target}'s drain failure carries SQLSTATE {other:?}, not the refusal's: {}",
                    failure.error
                )),
            };
        }
        if Instant::now() >= deadline {
            break Ok(RefusalSeen::Unseen);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let _ = facade.shutdown().await;
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every privilege change but the role change alone undoes what it
    /// opened, and the drain refusals touch only their target.
    #[test]
    fn every_change_but_the_role_change_closes() {
        let target = "\"public\".\"d0\"";
        for action in [
            SourceDdl::OwnerToHeir,
            SourceDdl::OwnerToOther,
            SourceDdl::RevokeSelect,
            SourceDdl::RevokeMembership,
            SourceDdl::RevokeSchemaUsage,
            SourceDdl::Rls {
                trellis_policy: false,
            },
            SourceDdl::Rls {
                trellis_policy: true,
            },
            SourceDdl::RevokeTargetWrites {
                target: "d0".to_string(),
            },
            SourceDdl::TargetRls {
                target: "d0".to_string(),
            },
            SourceDdl::TargetTriggerRefused {
                target: "d0".to_string(),
            },
        ] {
            assert!(action.privilege(), "{action:?}");
            let (opens, closes) = statements(&action, "\"public\".\"t\"", Some(target));
            assert!(!opens.is_empty() && !closes.is_empty(), "{action:?}");
            if action.refused_target().is_some() {
                assert!(
                    opens.iter().chain(&closes).any(|s| s.contains(target)),
                    "{action:?}"
                );
            }
        }
        let (opens, closes) = statements(&SourceDdl::WriterChange, "t", None);
        assert_eq!(opens, vec![grant_writer()]);
        assert!(closes.is_empty());
    }
}
