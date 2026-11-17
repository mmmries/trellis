//! Star 1-1 `JOIN` transforms, second half (split from `join_star_a` only to balance the CI shards).
//!
//! Run one module with `cargo test -p trellis --test join_star_b <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod join_star_joined;
mod join_star_joined_families;
mod join_star_n4;
mod join_star_quarantine;
mod join_star_recollation;
mod join_star_root;
mod join_star_schema_changes;
mod join_star_shared_columns;
mod join_star_where;
