//! Trigger capture: install, the capture pass, schema changes and disruptions on captured tables.
//!
//! Run one module with `cargo test -p trellis --test capture <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod capture_disruption;
mod capture_disruption_join;
mod capture_disruption_partitioned;
mod capture_enum_relabel;
mod capture_enum_retype;
mod capture_hierarchy;
mod capture_install;
mod capture_join;
mod capture_key_retype;
mod capture_parity;
mod capture_partition_pass;
mod capture_partitioned;
mod capture_partitioned_staging;
mod capture_reread;
mod capture_retype_floor;
mod capture_schema_change;
mod capture_ssi;
mod capture_transition_images;
mod capture_type_rename;
mod capture_virtual_generated;
mod logical_replication;
mod standby_staging;
mod unchanged_toast_images;
mod user_table_ddl;
