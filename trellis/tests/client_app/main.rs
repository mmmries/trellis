//! The embedding API and the client: connecting, waits, deadlines, handover, worker registration and status.
//!
//! Run one module with `cargo test -p trellis --test client_app <module>::`.

#[path = "../support/mod.rs"]
#[allow(dead_code, unused_imports)]
mod support;

mod app;
mod app_await_busy_stream;
mod app_await_quiet_stream;
mod app_converge;
mod blocking_trellis;
mod call_deadline;
mod call_fence;
mod claims;
mod client_e2e;
mod connectivity;
mod converge;
mod cross_instance_target_source;
mod identity;
mod instance_metrics;
mod live_means_steady_state;
mod liveness;
mod logical_restore;
mod paused_transform_message;
mod schema_dump_restore;
mod tcp_keepalives;
mod tls_connect;
mod transform_status_lifecycle;
mod upstream_build_releases;
mod version_handover;
mod wake_channel;
mod worker_registry;
