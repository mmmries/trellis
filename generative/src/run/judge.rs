//! "Never silently wrong": the judgement a run makes after a change Trellis
//! must notice (source DDL, #2184; privilege and ownership changes, #2182),
//! where the oracle doesn't model the engine's pause policy.
//!
//! At quiescence each definition must end one of:
//!
//! 1. **exact** against the recompute ([`Verdict::Exact`]);
//! 2. **paused with a reason**, and exact once its `RESUME` settles
//!    ([`Verdict::ResumedExact`]), or its `RESUME` refuses with a code
//!    ([`Verdict::ResumeRefused`]);
//! 3. **a listed gap**: the drawn shape names the register entry
//!    ([`Gap`]) it may reproduce, `self_check` reports the divergence, and
//!    the entry's repair makes it exact ([`Verdict::ListedGap`]).
//!
//! A definition exact against the recompute that the shape may have left
//! wrong is `self_check`ed too: the damage can sit in what Trellis keeps
//! beside the target (a `JOIN`'s hidden tables) until a later write
//! carries it in, and is case 3 when the check reports it.
//!
//! Anything else fails the run ([`RunError::NeverSilentlyWrong`]). Case 3
//! makes `docs/known-correctness-gaps.md` a checked allowlist: a missing
//! entry fails, and so does one whose detection doesn't detect or whose
//! repair doesn't repair. The judge doesn't check that the divergence is
//! the one the entry describes.
//!
//! The decision at each step is a pure function ([`first_look`],
//! [`after_resume`], [`after_repair`]), unit-tested below. [`judge`] drives
//! them over a [`ManualBackend`]. It steps the reconcile passes itself
//! ([`PassStepper`]), so a runner whose engine runs with a long reconcile
//! interval (one that places a change inside one interval or across a
//! pass) still settles.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use trellis::Config;
use trellis::dev::defs::ast::TransformDef;
use trellis::dev::pool::Pool;

use crate::backend::{Backend, ConcurrentBackend, DefinitionState, ManualBackend};
use crate::gaps::{Gap, Repair};
use crate::model::{BurstAction, Program, Server};

use super::{RunError, check_def};

/// How one definition ended at a judged check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Exact against the recompute, as it stood.
    Exact,
    /// Paused with `reason`, and exact once its `RESUME` settled.
    ResumedExact { reason: String },
    /// Paused with `reason`, and its `RESUME` refused with `code`.
    ResumeRefused { reason: String, code: String },
    /// Silently wrong as the named register entry says, reported by
    /// `self_check`, and exact after the entry's repair.
    ListedGap(Gap),
}

impl Verdict {
    /// A short name for coverage counts.
    pub fn kind(&self) -> &'static str {
        match self {
            Verdict::Exact => "exact",
            Verdict::ResumedExact { .. } => "resumed_exact",
            Verdict::ResumeRefused { .. } => "resume_refused",
            Verdict::ListedGap(_) => "listed_gap",
        }
    }
}

/// One definition's [`Verdict`] at a judged check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Judged {
    pub target: String,
    pub verdict: Verdict,
}

/// What to do with a definition after the first settled look.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirstLook {
    Done(Verdict),
    /// Paused with `reason`: `RESUME` it.
    Resume {
        reason: String,
    },
    /// Live and wrong, as `gap` allows: `self_check` it, then repair it.
    Repair(Gap),
    Fail(String),
}

/// The first look at a settled definition: `state` from the catalog, and
/// whether its target differs from the recompute (`None` when the oracle
/// can't recompute it, as for a definition a change left invalid).
/// `allowed` are the register entries the drawn shape may reproduce.
pub fn first_look(state: &DefinitionState, diverged: Option<bool>, allowed: &[Gap]) -> FirstLook {
    if state.is_paused() {
        return match &state.reason {
            Some(reason) => FirstLook::Resume {
                reason: reason.clone(),
            },
            None => FirstLook::Fail("paused with no reason recorded".to_string()),
        };
    }
    if state.status != "live" {
        return FirstLook::Fail(format!(
            "settled as {} (reported {}), neither live nor paused",
            state.status, state.reported
        ));
    }
    match diverged {
        None => FirstLook::Fail("live, but the oracle can't recompute it".to_string()),
        Some(false) => FirstLook::Done(Verdict::Exact),
        Some(true) => match repairable(allowed) {
            Some(gap) => FirstLook::Repair(gap),
            None if allowed.is_empty() => FirstLook::Fail(format!(
                "silently wrong: reports {} and differs from the recompute, and the drawn \
                 shape names no listed gap",
                state.reported
            )),
            None => FirstLook::Fail(format!(
                "silently wrong as {allowed:?}, whose entries document no repair the harness runs"
            )),
        },
    }
}

/// The entry of `allowed` the harness can repair, if any: the one a wrong
/// definition is repaired as, and the one an exact definition is
/// `self_check`ed for, since a shape's damage can sit in what Trellis keeps
/// beside the target (a `JOIN`'s hidden tables) until a later change
/// carries it into the target, after the shape's check.
pub fn repairable(allowed: &[Gap]) -> Option<Gap> {
    allowed
        .iter()
        .find(|gap| gap.repair() != Repair::None)
        .copied()
}

/// The look after a paused definition's `RESUME` (`resumed`: its error
/// text on a refusal) and a settle.
pub fn after_resume(
    reason: &str,
    resumed: Result<(), String>,
    state: &DefinitionState,
    diverged: Option<bool>,
) -> Result<Verdict, String> {
    if let Err(error) = resumed {
        return match error_code(&error) {
            Some(code) => Ok(Verdict::ResumeRefused {
                reason: reason.to_string(),
                code,
            }),
            None => Err(format!(
                "paused ({reason}), and its RESUME failed with no error code: {error}"
            )),
        };
    }
    if state.status != "live" {
        return Err(format!(
            "paused ({reason}); after its RESUME settled it is {state}"
        ));
    }
    match diverged {
        Some(false) => Ok(Verdict::ResumedExact {
            reason: reason.to_string(),
        }),
        Some(true) => Err(format!(
            "paused ({reason}); its RESUME settled live and differs from the recompute"
        )),
        None => Err(format!(
            "paused ({reason}); its RESUME settled live but the oracle can't recompute it"
        )),
    }
}

/// The look after a silently wrong definition was `self_check`ed
/// (`reported`: the check reported it not clean) and repaired as `gap`'s
/// entry says, then settled.
pub fn after_repair(
    gap: Gap,
    reported: bool,
    state: &DefinitionState,
    diverged: Option<bool>,
) -> Result<Verdict, String> {
    if !reported {
        return Err(format!(
            "silently wrong as {gap}, but self_check reported it clean, where the entry \
             says self_check detects it"
        ));
    }
    if state.status != "live" {
        return Err(format!(
            "silently wrong as {gap}; after the entry's repair ({:?}) it is {state}",
            gap.repair()
        ));
    }
    match diverged {
        Some(false) => Ok(Verdict::ListedGap(gap)),
        _ => Err(format!(
            "silently wrong as {gap}, and the entry's repair ({:?}) left it differing from \
             the recompute",
            gap.repair()
        )),
    }
}

/// The first `T` + four digits in `text`: the stable code an error message
/// prints.
pub fn error_code(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    (0..bytes.len().saturating_sub(4)).find_map(|i| {
        let boundary = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
        let digits = &bytes[i + 1..i + 5];
        let after = bytes.get(i + 5).is_none_or(|b| !b.is_ascii_alphanumeric());
        (boundary && bytes[i] == b'T' && digits.iter().all(u8::is_ascii_digit) && after)
            .then(|| text[i..i + 5].to_string())
    })
}

/// Steps the staging worker's reconcile pass by hand, on a session of its
/// own as the Trellis role, for a run whose engine runs with a long
/// reconcile interval.
pub struct PassStepper {
    client: Arc<Mutex<tokio_postgres::Client>>,
    pool: Pool,
    schema: String,
}

/// How often [`PassStepper::settle`] runs a pass while it waits, standing
/// in for the engine's own interval.
pub const SETTLE_PASS_EVERY: Duration = Duration::from_millis(100);

impl PassStepper {
    /// Connects to `backend`'s database as the role the engine runs as.
    pub async fn connect(backend: &ManualBackend) -> Result<Self, String> {
        let config =
            Config::from_dsn(backend.trellis_dsn().to_string()).map_err(|e| e.to_string())?;
        let (client, connection) = tokio_postgres::connect(config.dsn(), tokio_postgres::NoTls)
            .await
            .map_err(|e| e.to_string())?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // As the engine's own sessions resolve its tables.
        client
            .batch_execute(&format!(
                "set search_path to {}, public",
                quote_ident(config.schema())
            ))
            .await
            .map_err(|e| e.to_string())?;
        let pool = Pool::new(&config).map_err(|e| e.to_string())?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            pool,
            schema: config.schema().to_string(),
        })
    }

    /// One reconcile pass, as the maintenance loop runs it.
    pub async fn step(&self) -> Result<(), String> {
        let mut client = self.client.lock().await;
        trellis::dev::staging::reconcile_pass(&mut client, &self.pool, &self.schema).await
    }

    /// A read on the stepper's own session.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, String> {
        let client = self.client.lock().await;
        client.query(sql, params).await.map_err(|e| e.to_string())
    }

    /// Rounds of a pass, then [`Backend::quiesce`] with a pass every
    /// [`SETTLE_PASS_EVERY`] beside it, until a round's passes change
    /// nothing a pass writes ([`Self::pass_state`]). One round isn't
    /// enough: a pass that pauses a table's readers leaves the rest of its
    /// checks to the next pass (a widening that outgrows a copy, then the
    /// rewrite a versioned attribute shows on a later column), and that
    /// pass can land after the quiesce has seen every definition settled.
    pub async fn settle(&self, backend: &mut ManualBackend) -> Result<(), String> {
        let mut before = self.pass_state().await?;
        for _ in 0..SETTLE_ROUNDS {
            self.settle_round(backend).await?;
            let after = self.pass_state().await?;
            if after == before {
                return Ok(());
            }
            before = after;
        }
        Err(format!(
            "the reconcile passes still changed the catalog after {SETTLE_ROUNDS} rounds of \
             passes and quiesce"
        ))
    }

    /// A pass, then [`Backend::quiesce`] with a pass every
    /// [`SETTLE_PASS_EVERY`] beside it: a build the pass has to start, a
    /// re-read or a pause it queues, all land before the wait ends. The
    /// passes beside it stop before this returns.
    async fn settle_round(&self, backend: &mut ManualBackend) -> Result<(), String> {
        self.step().await?;
        let stop = Arc::new(AtomicBool::new(false));
        let ticker = {
            let stop = Arc::clone(&stop);
            let client = Arc::clone(&self.client);
            let pool = self.pool.clone();
            let schema = self.schema.clone();
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(SETTLE_PASS_EVERY).await;
                    let mut client = client.lock().await;
                    trellis::dev::staging::reconcile_pass(&mut client, &pool, &schema).await?;
                }
                Ok::<(), String>(())
            })
        };
        let quiesced = backend.quiesce().await.map_err(|e| format!("{e:?}"));
        stop.store(true, Ordering::Relaxed);
        let ticked = ticker.await.map_err(|e| e.to_string())?;
        quiesced?;
        ticked
    }

    /// What a pass's responses write: each definition's catalog row, the
    /// capture baselines and trigger records, the capture failures, the
    /// per-column states and the partition fingerprints, as one digest. A
    /// pass with nothing to do writes none of them.
    async fn pass_state(&self) -> Result<String, String> {
        let digest = |table: &str| {
            format!(
                "coalesce((select string_agg(t::text, ',' order by t::text) from {table} t), '')"
            )
        };
        let tables = [
            "transform_definitions",
            "capture_baselines",
            "capture_triggers",
            "capture_failures",
            "column_status",
            "partition_sources",
        ];
        let sql = format!(
            "select md5(concat_ws('|', {}))",
            tables.map(digest).join(", ")
        );
        let rows = self.query(&sql, &[]).await?;
        Ok(rows[0].get(0))
    }

    /// What the server does that decides which register shapes an event
    /// reproduces ([`Server`]), probed on a temporary table of the
    /// stepper's own session.
    pub async fn server(&self) -> Result<Server, String> {
        let client = self.client.lock().await;
        Server::probe(&client).await
    }
}

/// The most rounds [`PassStepper::settle`] runs before it gives up on the
/// passes ever going quiet.
const SETTLE_ROUNDS: usize = 10;

/// Settles, reads each of `defs`' state and its target against the
/// recompute (`None` where the oracle can't recompute it).
async fn look(
    backend: &mut ManualBackend,
    passes: &PassStepper,
    pool: &Pool,
    program: &Program,
    defs: &[TransformDef],
) -> Result<Vec<(DefinitionState, Option<bool>, String)>, RunError> {
    passes.settle(backend).await.map_err(RunError::Quiesce)?;
    let states = backend
        .definition_states(defs)
        .await
        .map_err(|e| RunError::Snapshot(format!("definition states: {e:?}")))?;
    let snapshot = backend
        .snapshot()
        .await
        .map_err(|e| RunError::Snapshot(format!("{e:?}")))?;
    let mut looks = Vec::with_capacity(defs.len());
    for (def, state) in defs.iter().zip(states) {
        let (diverged, report) = match check_def(pool, program, defs, def, &snapshot).await {
            Ok(report) => (Some(report.diverged()), report.to_string()),
            Err(error) => (None, format!("the oracle can't recompute it: {error}")),
        };
        looks.push((state, diverged, report));
    }
    Ok(looks)
}

/// Judges every one of `defs` at the check after op `op_index`, letting a
/// definition end silently wrong only as one of `allowed(def)`: the entries
/// the drawn shape may reproduce in that definition (none for one that
/// doesn't read what the shape changed). Takes each step
/// the verdict needs (a `RESUME`, a `self_check` and a repair) through the
/// library calls an operator would make, and leaves every definition exact
/// or refused by its `RESUME`.
pub async fn judge(
    backend: &mut ManualBackend,
    passes: &PassStepper,
    pool: &Pool,
    program: &Program,
    defs: &[TransformDef],
    op_index: usize,
    allowed: impl Fn(&TransformDef) -> Vec<Gap>,
) -> Result<Vec<Judged>, RunError> {
    let fail = |target: &str, problem: String, detail: String| RunError::NeverSilentlyWrong {
        op_index,
        target: target.to_string(),
        problem,
        detail,
    };
    let looks = look(backend, passes, pool, program, defs).await?;
    let mut judged: Vec<Option<Verdict>> = vec![None; defs.len()];
    // The definitions a second look decides.
    let mut to_resume: Vec<(usize, String)> = Vec::new();
    let mut to_repair: Vec<(usize, Gap)> = Vec::new();
    // Exact definitions the shape may have left wrong where the recompute
    // can't see it yet (a `JOIN`'s hidden tables), for `self_check`.
    let mut latent: Vec<(usize, Gap)> = Vec::new();
    for (i, (def, (state, diverged, report))) in defs.iter().zip(&looks).enumerate() {
        match first_look(state, *diverged, &allowed(def)) {
            FirstLook::Done(Verdict::Exact) => {
                judged[i] = Some(Verdict::Exact);
                if let Some(gap) = repairable(&allowed(def)) {
                    latent.push((i, gap));
                }
            }
            FirstLook::Done(verdict) => judged[i] = Some(verdict),
            FirstLook::Fail(problem) => {
                return Err(fail(&def.target, problem, format!("{state}\n{report}")));
            }
            FirstLook::Resume { reason } => to_resume.push((i, reason)),
            FirstLook::Repair(gap) => to_repair.push((i, gap)),
        }
    }
    // Every wrong definition is checked before anything is repaired: a
    // `request_backfill` rebuilds every reader of the table, so a check
    // after it would find the others clean.
    let mut repaired: Vec<(usize, Gap, bool)> = Vec::new();
    if !to_repair.is_empty() || !latent.is_empty() {
        let targets: Vec<String> = to_repair
            .iter()
            .chain(&latent)
            .map(|(i, _)| defs[*i].target.clone())
            .collect();
        backend.set_strict_audit(true);
        let audited = backend.strict_audit(&targets).await;
        backend.set_strict_audit(false);
        let unclean = audited.map_err(|e| RunError::Snapshot(format!("self_check: {e:?}")))?;
        let reported = |i: usize| {
            let prefix = format!("{}:", defs[i].target);
            unclean.iter().any(|line| line.starts_with(&prefix))
        };
        for (i, gap) in &to_repair {
            repaired.push((*i, *gap, reported(*i)));
        }
        // An exact target `self_check` finds wrong underneath is the gap
        // too, repaired before a later change brings it to the target.
        for (i, gap) in &latent {
            if reported(*i) {
                repaired.push((*i, *gap, true));
            }
        }
    }
    let mut resumed: Vec<(usize, String, Result<(), String>)> = Vec::new();
    for (i, reason) in to_resume {
        let result = backend
            .act(
                program,
                &BurstAction::Resume {
                    target: defs[i].target.clone(),
                    column: None,
                },
            )
            .await
            .map_err(|e| match &e {
                crate::backend::ManualBackendError::Facade(inner) => format!("{inner}"),
                other => format!("{other:?}"),
            });
        resumed.push((i, reason, result));
    }
    let mut backfilled: Vec<String> = Vec::new();
    for (i, gap, _) in &repaired {
        let def = &defs[*i];
        let repair = match gap.repair() {
            Repair::RequestBackfill | Repair::UndoThenBackfill
                if backfilled.contains(&def.source) =>
            {
                Vec::new()
            }
            Repair::RequestBackfill | Repair::UndoThenBackfill => {
                backfilled.push(def.source.clone());
                vec![BurstAction::RequestBackfill {
                    table: def.source.clone(),
                }]
            }
            Repair::PauseResume => vec![
                BurstAction::Pause {
                    target: def.target.clone(),
                    column: None,
                },
                BurstAction::Resume {
                    target: def.target.clone(),
                    column: None,
                },
            ],
            Repair::None => unreachable!("first_look repairs only by a repair"),
        };
        for action in &repair {
            backend.act(program, action).await.map_err(|e| {
                let (state, _, report) = &looks[*i];
                fail(
                    &def.target,
                    format!("the repair of {gap} failed: {action:?}: {e:?}"),
                    format!("{state}\n{report}"),
                )
            })?;
        }
    }
    if !resumed.is_empty() || !repaired.is_empty() {
        let again = look(backend, passes, pool, program, defs).await?;
        for (i, reason, result) in resumed {
            let (state, diverged, report) = &again[i];
            let verdict = after_resume(&reason, result, state, *diverged)
                .map_err(|problem| fail(&defs[i].target, problem, format!("{state}\n{report}")))?;
            judged[i] = Some(verdict);
        }
        for (i, gap, reported) in repaired {
            let (state, diverged, report) = &again[i];
            let verdict = after_repair(gap, reported, state, *diverged)
                .map_err(|problem| fail(&defs[i].target, problem, format!("{state}\n{report}")))?;
            judged[i] = Some(verdict);
        }
    }
    Ok(defs
        .iter()
        .zip(judged)
        .map(|(def, verdict)| Judged {
            target: def.target.clone(),
            verdict: verdict.expect("every definition is judged"),
        })
        .collect())
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(status: &str, reason: Option<&str>) -> DefinitionState {
        DefinitionState {
            target: "d0".to_string(),
            status: status.to_string(),
            reported: status.to_string(),
            reason: reason.map(str::to_string),
        }
    }

    #[test]
    fn only_an_entry_with_a_repair_is_checked_for_and_repaired() {
        assert_eq!(repairable(&[]), None);
        assert_eq!(repairable(&[Gap::TransactionPooler]), None);
        assert_eq!(
            repairable(&[Gap::TransactionPooler, Gap::SourceRenameRoundtrip]),
            Some(Gap::SourceRenameRoundtrip)
        );
    }

    #[test]
    fn an_exact_live_definition_is_done() {
        assert_eq!(
            first_look(&state("live", None), Some(false), &[]),
            FirstLook::Done(Verdict::Exact)
        );
    }

    #[test]
    fn a_live_wrong_definition_fails_unless_the_shape_names_a_repairable_gap() {
        assert!(matches!(
            first_look(&state("live", None), Some(true), &[]),
            FirstLook::Fail(p) if p.starts_with("silently wrong")
        ));
        assert_eq!(
            first_look(&state("live", None), Some(true), &[Gap::AlterTypeRewrite]),
            FirstLook::Repair(Gap::AlterTypeRewrite)
        );
        assert!(matches!(
            first_look(&state("live", None), Some(true), &[Gap::TransactionPooler]),
            FirstLook::Fail(_)
        ));
    }

    #[test]
    fn a_pause_needs_a_reason_and_is_resumed() {
        assert!(matches!(
            first_look(&state("paused", None), Some(true), &[]),
            FirstLook::Fail(_)
        ));
        assert_eq!(
            first_look(
                &state("paused", Some("capture: trigger disabled")),
                None,
                &[]
            ),
            FirstLook::Resume {
                reason: "capture: trigger disabled".to_string()
            }
        );
    }

    #[test]
    fn neither_live_nor_paused_fails() {
        for status in ["quarantined", "backfilling", "waiting_to_backfill"] {
            assert!(
                matches!(
                    first_look(&state(status, None), Some(false), &[]),
                    FirstLook::Fail(_)
                ),
                "{status}"
            );
        }
        assert!(matches!(
            first_look(&state("live", None), None, &[]),
            FirstLook::Fail(_)
        ));
    }

    #[test]
    fn a_resume_ends_exact_or_refused_with_a_code() {
        let live = state("live", None);
        assert_eq!(
            after_resume("r", Ok(()), &live, Some(false)),
            Ok(Verdict::ResumedExact {
                reason: "r".to_string()
            })
        );
        assert_eq!(
            after_resume(
                "r",
                Err("T0123: define would refuse it".to_string()),
                &state("paused", Some("r")),
                None
            ),
            Ok(Verdict::ResumeRefused {
                reason: "r".to_string(),
                code: "T0123".to_string()
            })
        );
        assert!(after_resume("r", Err("no code".to_string()), &live, None).is_err());
        assert!(after_resume("r", Ok(()), &live, Some(true)).is_err());
        assert!(after_resume("r", Ok(()), &state("paused", Some("r")), Some(false)).is_err());
    }

    #[test]
    fn a_listed_gap_needs_detection_and_a_repair_that_repairs() {
        let live = state("live", None);
        let gap = Gap::AlterTypeRewrite;
        assert_eq!(
            after_repair(gap, true, &live, Some(false)),
            Ok(Verdict::ListedGap(gap))
        );
        assert!(after_repair(gap, false, &live, Some(false)).is_err());
        assert!(after_repair(gap, true, &live, Some(true)).is_err());
        assert!(after_repair(gap, true, &state("paused", Some("x")), Some(false)).is_err());
    }

    #[test]
    fn the_error_code_is_the_first_bare_t_and_four_digits() {
        assert_eq!(error_code("T0247: paused").as_deref(), Some("T0247"));
        assert_eq!(
            error_code("refused (T1002) because").as_deref(),
            Some("T1002")
        );
        assert_eq!(error_code("AT0247 T12345 T12"), None);
        assert_eq!(error_code("x T9999"), Some("T9999".to_string()));
    }
}
