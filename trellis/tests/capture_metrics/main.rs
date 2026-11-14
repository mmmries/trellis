//! Capture tests that assert on the process-wide metrics registry by source table name, kept apart from the rest of `capture` so no other test's capture pass counts toward their series.
//!
//! Run one module with `cargo test -p trellis --test capture_metrics <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod capture_missed_ddl;
mod capture_set_expression;
