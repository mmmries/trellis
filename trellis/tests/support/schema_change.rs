//! The helpers of the 1-1 `JOIN` schema-change suites
//! (`join_star_schema_changes.rs`, `join_one_to_one_schema_changes.rs`): the
//! staging worker's pass by hand, the resume statements, and the reads of a
//! definition's status, capture failures and column types.
//!
//! A binary using this module also declares `support/drain_driver.rs` as
//! `mod drain_driver` at its root.

#![allow(dead_code)]

use crate::drain_driver::Driver;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::TransformStatus;
use trellis::{Trellis, TrellisError, TrellisOptions};

pub async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// One whole staging-worker pass: capture (the schema-change checks among
/// it), the requested resumes, then the start of ready builds.
pub async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

pub async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

pub async fn status(d: &Driver, target: &str) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// Every capture failure recorded, as `(source_table, columns, error)`.
pub async fn failures(d: &Driver) -> Vec<String> {
    d.rows("select source_table, columns, error from capture_failures order by 1")
        .await
}

/// `table.column`'s type, as `format_type` renders it.
pub async fn column_type(d: &Driver, table: &str, column: &str) -> String {
    d.ctl
        .query_one(
            "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attname = $2",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
        .get(0)
}

/// The reason `target` is paused with, asserting it is paused for a capture
/// failure on `table` about `columns`.
pub async fn paused_for(d: &Driver, target: &str, table: &str, columns: &[&str]) -> String {
    let failure = capture_failure(d, target, table).await;
    assert_eq!(
        failure.columns,
        columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "{target}: {}",
        failure.error
    );
    failure.error
}

/// The reason `target` is paused with, asserting it is paused for a capture
/// failure on `table` whose columns include `column`.
pub async fn paused_naming(d: &Driver, target: &str, table: &str, column: &str) -> String {
    let failure = capture_failure(d, target, table).await;
    assert!(
        failure.columns.iter().any(|c| c == column),
        "{target}: {:?} lacks {column}: {}",
        failure.columns,
        failure.error
    );
    failure.error
}

/// `target`'s capture failure, asserting it is paused for one on `table`.
async fn capture_failure(d: &Driver, target: &str, table: &str) -> trellis::CaptureFailure {
    let bare = target.split_once('.').map_or(target, |(_, bare)| bare);
    let reported = trellis(d)
        .await
        .status(bare)
        .await
        .expect("status")
        .expect(target);
    assert_eq!(
        reported.status,
        TransformStatus::Paused,
        "{target}: {:?}",
        failures(d).await
    );
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, table, "{target}: {}", failure.error);
    failure
}

/// Asserts `target` isn't paused and nothing is recorded against it.
pub async fn assert_unpaused(d: &Driver, target: &str, after: &str) {
    assert_ne!(
        status(d, target).await,
        "paused",
        "{target} after {after}: {:?}",
        failures(d).await
    );
    assert!(failures(d).await.is_empty(), "{target} after {after}");
}

/// `RESUME TRANSFORM target`, which must go through.
pub async fn resume(d: &Driver, target: &str) {
    trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
        .unwrap_or_else(|err| panic!("resume {target}: {err}"));
}

/// `RESUME TRANSFORM target`, which must be refused: returns the message.
pub async fn resume_refused(d: &Driver, target: &str) -> String {
    match trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
    {
        Err(
            err @ TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused { .. }),
        ) => err.to_string(),
        other => panic!("expected the resume of {target} to be refused, got {other:?}"),
    }
}

/// Whether `target`'s resume waits on the staging worker to re-type its
/// copies: it is paused, and its reason says so. Returns the reason.
pub async fn assert_retyping(d: &Driver, target: &str) -> String {
    assert_eq!(status(d, target).await, "paused", "{target}");
    let error: String = d
        .ctl
        .query_one(
            "select f.error from capture_failures f join transform_definitions d \
             on d.id = f.transform_id where d.target_table = $1",
            &[&target],
        )
        .await
        .expect("the reason")
        .get(0);
    assert!(error.starts_with("resuming:"), "{error}");
    error
}
