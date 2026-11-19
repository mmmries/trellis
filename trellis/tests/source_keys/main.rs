//! Source keys and source indexes: key rules, upgrades and resume after a key change.
//!
//! Run one module with `cargo test -p trellis --test source_keys <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod deferrable_key;
mod include_key_upgrade;
mod key_backing_index;
mod nullable_key_capture;
mod probe_index_grace;
mod resume_after_source_key_change;
mod resume_drops_deleted_keys;
mod resume_rerecords_source_key;
mod retype_invalidates_reader;
mod source_index;
mod source_key_clause;
mod source_key_upgrade;
mod target_identity_key;
