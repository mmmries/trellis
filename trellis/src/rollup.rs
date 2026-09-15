//! Self-retained metric history (issue #54, epic #49): the write and prune
//! halves of `docs/observability.md`'s "Retention: Postgres rollup tables"
//! design, settled by `docs/decisions/0009-observability-decisions.md`
//! decision 7.
//!
//! This module owns exactly two operations, both plain SQL against
//! `metric_rollup` (`trellis/migrations/V22__metric_rollup.sql`):
//!
//! * [`write_snapshot`] — takes a [`crate::metrics::snapshot`] reading and
//!   inserts one row per metric series.
//! * [`prune`] — deletes rows older than a retention window.
//!
//! Both are wired into [`crate::client`]'s `maintenance_loop` on the same
//! tick (`ClientOptions::rollup_interval`, default [`DEFAULT_ROLLUP_INTERVAL`]),
//! gated the same way every other maintenance operation is — only the
//! staging worker runs it, never every client in a fleet (see that module's
//! doc comment). This module itself has no opinion on *when* it runs; it's
//! plain functions over a connection, not a task or a loop of its own,
//! matching `crate::staging::retire::retire_drained_segments` and
//! `crate::defs::chunk_queue::reclaim_stale_chunks`'s own shape (a function
//! `maintenance_loop` calls on its own cadence, not a self-scheduling
//! subsystem).
//!
//! **Explicitly out of scope for this issue** (see the issue's own "Out of
//! scope" list): a query/read API over the accumulated history. That's the
//! design doc's future "suggest optimizations" use case, not this one —
//! this module only ever writes and prunes.

use std::fmt;
use std::time::{Duration, SystemTime};

use crate::error_code::{self, ErrorCode};
use crate::metrics::{MetricKind, MetricSample};

/// ADR-0009 decision 7's default rollup interval: how often
/// [`write_snapshot`] runs. Configurable via
/// [`crate::client::ClientOptions::rollup_interval`].
pub const DEFAULT_ROLLUP_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// ADR-0009 decision 7's default retention window: how far back
/// [`prune`] keeps rows before trimming them. Configurable via
/// [`crate::client::ClientOptions::rollup_retention`].
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Failure modes for this module's two operations — both are ever only a
/// direct Postgres protocol/query error, so this is a thin, single-variant
/// wrapper rather than a richer enum; kept as a named type (not a bare
/// `tokio_postgres::Error` return) purely so it can carry a [`Self::code`]
/// method matching every other error type in this crate
/// (`docs/decisions/0008-public-api-design.md` decision 3).
#[derive(Debug)]
pub enum RollupError {
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
}

impl RollupError {
    /// This error's stable, coarse [`ErrorCode`] category. Every variant
    /// here is a Postgres error today, so this always delegates to
    /// [`error_code::classify_pg_error`] rather than hand-picking a
    /// category the way a richer enum (e.g. [`crate::staging::StagingError`])
    /// does for its non-`Db` variants.
    pub fn code(&self) -> ErrorCode {
        match self {
            RollupError::Db(err) => error_code::classify_pg_error(err),
        }
    }
}

impl fmt::Display for RollupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RollupError::Db(err) => {
                write!(f, "metric rollup database error: ")?;
                crate::error::write_pg_error(f, err)
            }
        }
    }
}

impl std::error::Error for RollupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RollupError::Db(err) => Some(err),
        }
    }
}

impl From<tokio_postgres::Error> for RollupError {
    fn from(err: tokio_postgres::Error) -> Self {
        RollupError::Db(err)
    }
}

/// Writes one `metric_rollup` row per entry in `samples` (typically
/// [`crate::metrics::snapshot`]'s output), stamped with `rolled_up_at` —
/// taken as an explicit parameter rather than calling `SystemTime::now()`
/// internally, so every row a single tick writes shares exactly one
/// timestamp (not one per row, drifting by however long the loop over
/// `samples` takes) and so tests can pin it to a known instant. Returns the
/// number of rows written (always `samples.len()` on success — `u64` to
/// match [`tokio_postgres::Client::execute`]'s own return type, not because
/// any single insert could plausibly affect more than one row).
///
/// A counter/gauge sample writes `value` and leaves the four
/// histogram-only columns null; a histogram sample does the reverse (see
/// `V22__metric_rollup.sql`'s column doc comment for the full shape). One
/// `execute` per sample rather than a single multi-row statement: the
/// per-tick sample count is small (one series per distinct metric ×
/// label-set this crate currently records — on the order of tens, not
/// thousands), so simplicity wins over the write-amplification concern that
/// motivates batching elsewhere in this crate (e.g.
/// `staging::retire::retire_drained_segments`'s own per-candidate loop).
pub async fn write_snapshot(
    client: &tokio_postgres::Client,
    samples: &[MetricSample],
    rolled_up_at: SystemTime,
) -> Result<u64, RollupError> {
    let mut written = 0u64;
    for sample in samples {
        let labels_json = encode_labels(&sample.labels);
        let kind = sample.kind.as_sql();
        match sample.kind {
            MetricKind::Counter | MetricKind::Gauge => {
                client
                    .execute(
                        "insert into metric_rollup \
                         (rolled_up_at, metric_name, metric_kind, labels, value) \
                         values ($1, $2, $3, $4::text::jsonb, $5)",
                        &[
                            &rolled_up_at,
                            &sample.name,
                            &kind,
                            &labels_json,
                            &sample.value,
                        ],
                    )
                    .await?;
            }
            MetricKind::Histogram => {
                let bounds: Vec<f64> = sample.buckets.iter().map(|(le, _)| *le).collect();
                let counts: Vec<i64> = sample
                    .buckets
                    .iter()
                    .map(|(_, count)| i64::try_from(*count).unwrap_or(i64::MAX))
                    .collect();
                let hist_count = sample.count.map(|c| i64::try_from(c).unwrap_or(i64::MAX));
                client
                    .execute(
                        "insert into metric_rollup \
                         (rolled_up_at, metric_name, metric_kind, labels, \
                          bucket_bounds, bucket_counts, histogram_sum, histogram_count) \
                         values ($1, $2, $3, $4::text::jsonb, $5, $6, $7, $8)",
                        &[
                            &rolled_up_at,
                            &sample.name,
                            &kind,
                            &labels_json,
                            &bounds,
                            &counts,
                            &sample.sum,
                            &hist_count,
                        ],
                    )
                    .await?;
            }
        }
        written += 1;
    }
    Ok(written)
}

/// Deletes every `metric_rollup` row older than `retention`, measured back
/// from `now` (an explicit parameter for the same testability reason
/// [`write_snapshot`]'s `rolled_up_at` is). Returns the number of rows
/// deleted. `now.checked_sub(retention)` saturates to
/// [`SystemTime::UNIX_EPOCH`] rather than panicking in the pathological
/// case of a `retention` longer than the time since the Unix epoch — that
/// just means "nothing is old enough to prune yet", which the resulting
/// `delete ... where rolled_up_at < epoch` correctly expresses as a no-op.
pub async fn prune(
    client: &tokio_postgres::Client,
    now: SystemTime,
    retention: Duration,
) -> Result<u64, RollupError> {
    let cutoff = now.checked_sub(retention).unwrap_or(SystemTime::UNIX_EPOCH);
    let deleted = client
        .execute(
            "delete from metric_rollup where rolled_up_at < $1",
            &[&cutoff],
        )
        .await?;
    Ok(deleted)
}

/// Hand-rolled minimal JSON object encoder for a label set, bound to
/// `metric_rollup.labels` via the same `$n::text::jsonb` cast convention
/// `staging::append`'s CDC-image inserts already use (see that module's doc
/// comment) — this crate has no `serde_json` (or any `serde`) dependency
/// today, and label sets here are small, flat `string -> string` maps, not
/// worth adding one for. Escapes exactly what the JSON grammar requires for
/// a string: `"`, `\`, and the control characters (including the three with
/// short escapes, `\n`/`\r`/`\t`); every other Unicode scalar value passes
/// through as-is (valid UTF-8 in equals valid UTF-8 out, requiring no
/// `\uXXXX` escaping beyond the control-character range).
fn encode_labels(labels: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (i, (key, value)) in labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json_escape_into(&mut out, key);
        out.push(':');
        json_escape_into(&mut out, value);
    }
    out.push('}');
    out
}

fn json_escape_into(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_labels_produces_valid_minimal_json() {
        assert_eq!(encode_labels(&[]), "{}");
        assert_eq!(
            encode_labels(&[("transform".to_string(), "orders".to_string())]),
            r#"{"transform":"orders"}"#
        );
        assert_eq!(
            encode_labels(&[
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]),
            r#"{"a":"1","b":"2"}"#
        );
    }

    #[test]
    fn encode_labels_escapes_quotes_backslashes_and_control_characters() {
        let json = encode_labels(&[(
            "transform".to_string(),
            "weird\"name\\with\ncontrol\tchars".to_string(),
        )]);
        assert_eq!(json, r#"{"transform":"weird\"name\\with\ncontrol\tchars"}"#);
    }
}
