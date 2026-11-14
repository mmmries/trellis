//! Partitioned sources.
//!
//! Run one module with `cargo test -p trellis --test partition <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod partition_group_basis;
mod partition_group_basis_build;
mod partition_join;
mod partition_lifecycle_cells;
mod partition_restore;
mod partition_routing;
mod partition_row_security;
mod partition_sweep;
mod partition_thousand_leaves;
mod partition_truncate;
mod pg_partman;
