//! Ring and maintenance steps the staging worker's role is refused
//! (`42501`), and how the maintenance loop reports them.
//!
//! A refused step blocks no definition's correctness, only the ring's
//! housekeeping (retiring a drained segment needs `TRUNCATE`, say), so it
//! pauses nothing. Instead the loop keeps going with its other steps, backs
//! the refused step off, logs it once a minute per step and table
//! ([`LogLimiter`]), and reports it on every `status()` of the handle that
//! runs the loop ([`RefusedStep`]) until a pass of the step succeeds. A
//! definition's own refused build start is not here: it halts and pauses
//! that definition (`staging::halt::halt_failed_start`).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tokio_postgres::GenericClient;

/// How long a refused step waits before it tries again.
pub(crate) const REFUSED_STEP_BACKOFF: Duration = Duration::from_secs(5);

/// How often the same refusal is logged at `warn`.
pub(crate) const REFUSAL_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// A maintenance step the worker's role is refused, as `status()` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RefusedStep {
    /// The step, such as `retire_drained_segments` or `seal`.
    pub step: String,
    /// The role the staging worker runs as, when it could be read.
    pub role: Option<String>,
    /// The table or sequence refused, as Postgres names it.
    pub object: Option<String>,
    /// The privileges the role lacks on `object`, when they could be read.
    pub missing: Vec<String>,
    /// Postgres's own message.
    pub error: String,
}

impl RefusedStep {
    /// A refusal known only by its Postgres message.
    pub(crate) fn from_message(step: &str, error: &str) -> Self {
        Self {
            step: step.to_string(),
            role: None,
            object: parse_refused_object(error).map(|(_, name)| name.to_string()),
            missing: Vec::new(),
            error: error.to_string(),
        }
    }
}

impl fmt::Display for RefusedStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "maintenance step {} was refused", self.step)?;
        if let Some(role) = &self.role {
            write!(f, " to role {role}")?;
        }
        match (&self.object, self.missing.is_empty()) {
            (Some(object), false) => {
                write!(f, ": it lacks {} on {object}", self.missing.join(", "))?
            }
            (Some(object), true) => write!(f, " on {object}")?,
            (None, _) => {}
        }
        write!(
            f,
            " ({}); grant it, and the step recovers by itself",
            self.error
        )
    }
}

/// The refusals the loop has met and not yet seen recover, shared with the
/// [`crate::Client`] that runs it.
#[derive(Debug, Clone, Default)]
pub(crate) struct RefusedSteps(Arc<Mutex<BTreeMap<String, RefusedStep>>>);

impl RefusedSteps {
    pub(crate) fn set(&self, refused: RefusedStep) {
        self.lock().insert(refused.step.clone(), refused);
    }

    /// Clears `step`'s refusal; whether it had one.
    pub(crate) fn clear(&self, step: &str) -> bool {
        self.lock().remove(step).is_some()
    }

    pub(crate) fn snapshot(&self) -> Vec<RefusedStep> {
        self.lock().values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, RefusedStep>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// When each refused step may run again.
#[derive(Debug, Default)]
pub(crate) struct StepBackoff {
    until: HashMap<&'static str, Instant>,
}

impl StepBackoff {
    pub(crate) fn due(&self, step: &str, now: Instant) -> bool {
        self.until.get(step).is_none_or(|until| now >= *until)
    }

    pub(crate) fn refused(&mut self, step: &'static str, now: Instant, wait: Duration) {
        self.until.insert(step, now + wait);
    }

    pub(crate) fn recovered(&mut self, step: &str) {
        self.until.remove(step);
    }
}

/// Lets a log line through at most once per interval per key (a step and a
/// table), so a refusal that repeats every tick logs once a minute.
#[derive(Debug, Default)]
pub(crate) struct LogLimiter {
    last: HashMap<(String, String), Instant>,
}

impl LogLimiter {
    pub(crate) fn allow(
        &mut self,
        step: &str,
        table: &str,
        now: Instant,
        interval: Duration,
    ) -> bool {
        let key = (step.to_string(), table.to_string());
        match self.last.get(&key) {
            Some(last) if now.duration_since(*last) < interval => false,
            _ => {
                self.last.insert(key, now);
                true
            }
        }
    }

    /// Forgets `step`'s lines, so a refusal after a recovery logs at once.
    pub(crate) fn forget(&mut self, step: &str) {
        self.last.retain(|(s, _), _| s != step);
    }
}

/// The kind and name Postgres's `permission denied for <kind> <name>` names.
pub(crate) fn parse_refused_object(message: &str) -> Option<(&str, &str)> {
    let rest = message.split("permission denied for ").nth(1)?;
    let mut words = rest.split_whitespace();
    let kind = words.next()?;
    let name = words.next()?.trim_end_matches([',', '.', ';']);
    Some((kind, name))
}

/// Fills in the role and missing privileges of a refusal of `step` that
/// Postgres reported as `error`, reading them on `client` (the refused
/// session). Whatever can't be read stays empty.
pub(crate) async fn diagnose(
    client: &impl GenericClient,
    schema: &str,
    step: &str,
    error: &str,
) -> RefusedStep {
    let mut refused = RefusedStep::from_message(step, error);
    refused.role = client
        .query_one("select current_user::text", &[])
        .await
        .ok()
        .map(|row| row.get(0));
    let Some((kind, name)) = parse_refused_object(error) else {
        return refused;
    };
    let (probe, privileges): (&str, &[&str]) = match kind {
        "table" => (
            "pg_catalog.has_table_privilege(c.oid, p)",
            &["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"],
        ),
        "sequence" => (
            "pg_catalog.has_sequence_privilege(c.oid, p)",
            &["USAGE", "SELECT", "UPDATE"],
        ),
        _ => return refused,
    };
    let privileges: Vec<String> = privileges.iter().map(ToString::to_string).collect();
    let sql = format!(
        "select array(select p from unnest($3::text[]) p where not {probe}) \
         from pg_catalog.pg_class c \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where c.relname = $2 \
         order by (n.nspname = $1) desc limit 1"
    );
    if let Ok(Some(row)) = client.query_opt(&sql, &[&schema, &name, &privileges]).await {
        refused.missing = row.get(0);
    }
    refused
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_limiter_logs_once_an_interval_per_step_and_table() {
        let mut limiter = LogLimiter::default();
        let start = Instant::now();
        let minute = REFUSAL_LOG_INTERVAL;
        assert!(limiter.allow("retire", "seg_0", start, minute));
        // Every tick in the interval is held back.
        for secs in [1, 30, 59] {
            let now = start + Duration::from_secs(secs);
            assert!(!limiter.allow("retire", "seg_0", now, minute), "{secs}s");
        }
        // Another table, or another step, has its own line.
        assert!(limiter.allow("retire", "seg_1", start, minute));
        assert!(limiter.allow("seal", "seg_0", start, minute));
        // A whole interval later it speaks again, and the clock restarts there.
        let later = start + minute;
        assert!(limiter.allow("retire", "seg_0", later, minute));
        assert!(!limiter.allow("retire", "seg_0", later + Duration::from_secs(1), minute));
        // A recovery lets the next refusal through at once.
        limiter.forget("retire");
        assert!(limiter.allow("retire", "seg_0", later + Duration::from_secs(2), minute));
    }

    #[test]
    fn a_refused_step_waits_out_its_backoff_and_a_recovery_ends_it() {
        let mut backoff = StepBackoff::default();
        let start = Instant::now();
        assert!(backoff.due("seal", start));
        backoff.refused("seal", start, Duration::from_secs(5));
        assert!(!backoff.due("seal", start + Duration::from_secs(4)));
        assert!(backoff.due("seal", start + Duration::from_secs(5)));
        assert!(backoff.due("retire", start), "only the refused step waits");
        backoff.recovered("seal");
        assert!(backoff.due("seal", start));
    }

    #[test]
    fn the_refused_object_is_read_off_postgress_message() {
        assert_eq!(
            parse_refused_object("permission denied for table seg_0"),
            Some(("table", "seg_0"))
        );
        assert_eq!(
            parse_refused_object("db error: ERROR: permission denied for sequence seg_seq"),
            Some(("sequence", "seg_seq"))
        );
        assert_eq!(parse_refused_object("permission denied for schema"), None);
        assert_eq!(parse_refused_object("deadlock detected"), None);
    }

    #[test]
    fn a_refusal_reads_as_what_to_grant() {
        let mut refused = RefusedStep::from_message(
            "retire_drained_segments",
            "permission denied for table seg_0",
        );
        refused.role = Some("worker".to_string());
        refused.missing = vec!["TRUNCATE".to_string()];
        let text = refused.to_string();
        assert!(text.contains("retire_drained_segments"), "{text}");
        assert!(text.contains("role worker"), "{text}");
        assert!(text.contains("lacks TRUNCATE on seg_0"), "{text}");
    }

    #[test]
    fn the_shared_set_holds_a_refusal_until_its_step_recovers() {
        let shared = RefusedSteps::default();
        shared.set(RefusedStep::from_message(
            "seal",
            "permission denied for table a",
        ));
        shared.set(RefusedStep::from_message(
            "seal",
            "permission denied for table b",
        ));
        assert_eq!(shared.snapshot().len(), 1, "one reason per step");
        assert!(shared.clear("seal"));
        assert!(!shared.clear("seal"));
        assert!(shared.snapshot().is_empty());
    }
}
