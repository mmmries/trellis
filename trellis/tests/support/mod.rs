//! Helpers shared by the test targets, compiled once per target: each
//! family's `main.rs` declares this module, and a test module names what it
//! uses as `use crate::support::pipeline;`. A family uses only some of it,
//! hence the `allow`s on that declaration.

pub mod attver;
pub mod drain_driver;
pub mod pipeline;
pub mod relabel;
pub mod render_lab;
pub mod resume_passes;
pub mod schema_change;
pub mod sql_recompute;
pub mod star;
pub mod star_cx;
pub mod trigger_pipeline;
pub mod upstream_releases;
pub mod varchar_columns;
pub mod wait_live;
