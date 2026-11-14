//! `trellis_instance_up` follows each handle's life,
//! per instance, in the one registry the process renders.
//!
//! Two instances share a process and a database, each with its own catalog
//! schema, and each runs a staging worker. The label values are the
//! instances' `<database>/<schema>` names, as in their log lines. Nothing
//! here waits: `connect` returns with the worker started and `shutdown` with
//! it joined, so the series are read at those points.

use testkit::TestCluster;
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, Trellis, TrellisOptions};

const INSTANCE_B: &str = "instance_b";

/// `trellis_instance_up` of the instance named `instance`, in the process's
/// rendered body.
fn up(instance: &str) -> Option<u8> {
    let line = format!("trellis_instance_up{{trellis_instance=\"{instance}\"}} ");
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .find_map(|rendered| {
            rendered
                .strip_prefix(&line)
                .map(|value| value.parse().unwrap())
        })
}

async fn migrated(config: &Config) {
    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer.migrate().await.expect("migrate");
    definer
        .shutdown()
        .await
        .expect("shutdown the define-only handle");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_instance_reports_up_while_its_handle_runs_and_zero_after() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config_a = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema A");
    let config_b = Config::with_schema(db.dsn().to_string(), INSTANCE_B).expect("schema B");
    let raw = db.pool.get().await.expect("connection");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name_a = format!("{database}/{DEFAULT_SCHEMA}");
    let name_b = format!("{database}/{INSTANCE_B}");

    migrated(&config_a).await;
    migrated(&config_b).await;
    // The define-only handles are gone: both instances are listed, at 0.
    assert_eq!(up(&name_a), Some(0));
    assert_eq!(up(&name_b), Some(0));

    let staging = TrellisOptions {
        staging: true,
        ..TrellisOptions::default()
    };
    let a = Trellis::connect(config_a, staging.clone())
        .await
        .expect("connect A");
    let b = Trellis::connect(config_b, staging)
        .await
        .expect("connect B");
    assert_eq!(up(&name_a), Some(1));
    assert_eq!(up(&name_b), Some(1));

    b.shutdown().await.expect("shutdown B");
    assert_eq!(up(&name_a), Some(1), "A still runs");
    assert_eq!(up(&name_b), Some(0), "B's series stays, at 0");

    drop(a);
    assert_eq!(up(&name_a), Some(0), "a dropped handle is stopped too");
}

/// The series a running instance's workers record carry its name, not
/// `unknown`: the drain workers run on the client's runtime, whose threads
/// carry the instance, and the staging worker's tick sets its gauge there.
///
/// Waits through `await_converged`, the facade's read-your-writes call, whose
/// timeout is only the bound on a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_instances_workers_record_under_its_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema");
    let raw = db.pool.get().await.expect("connection");
    raw.batch_execute("create table public.metric_src (id bigint primary key, price numeric)")
        .await
        .expect("create the source");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name = format!("{database}/{DEFAULT_SCHEMA}");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer.migrate().await.expect("migrate");
    definer
        .apply("TRANSFORM metric_doubled FROM metric_src SELECT price + price AS total")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown the definer");

    let trellis = Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..TrellisOptions::default()
        },
    )
    .await
    .expect("connect");
    raw.batch_execute("insert into public.metric_src values (1, 10)")
        .await
        .expect("insert");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converge");

    let rendered = trellis.metrics().render_prometheus();
    let applied = rendered
        .lines()
        .filter(|line| line.starts_with("trellis_changes_applied_total{"))
        .filter(|line| line.contains("transform=\"metric_doubled\""))
        .collect::<Vec<_>>();
    assert_eq!(applied.len(), 1, "{rendered}");
    assert!(
        applied[0].contains(&format!("trellis_instance=\"{name}\"")),
        "recorded under another instance: {}",
        applied[0]
    );
    trellis.shutdown().await.expect("shutdown");
}

/// One series of the process's rendered body for `instance`.
fn series(instance: &str, metric: &str, labels: &[&str]) -> Option<f64> {
    let wanted = format!("trellis_instance=\"{instance}\"");
    trellis::Metrics::new()
        .render_prometheus()
        .lines()
        .find(|line| {
            line.strip_prefix(metric)
                .is_some_and(|rest| rest.starts_with('{'))
                && line.contains(&wanted)
                && labels.iter().all(|label| line.contains(label))
        })
        .and_then(|line| line.rsplit(' ').next()?.parse().ok())
}

/// A running staging worker reports the status and liveness gauges its
/// housekeeping pass reads: that it leads, the definitions by status, the
/// operator's pause told apart from a failure, and its own drain worker. A
/// stopped one reports 0 for leading.
///
/// The first housekeeping pass runs in the staging worker's first tick,
/// before the tick that seals the write the test waits on, and `shutdown`
/// joins the loop, so both are read at points the worker has passed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_staging_worker_reports_the_status_and_liveness_gauges() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema");
    let raw = db.pool.get().await.expect("connection");
    raw.batch_execute("create table public.gauge_src (id bigint primary key, price numeric)")
        .await
        .expect("create the source");
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name = format!("{database}/{DEFAULT_SCHEMA}");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer.migrate().await.expect("migrate");
    for target in ["gauge_running", "gauge_paused"] {
        definer
            .apply(&format!(
                "TRANSFORM {target} FROM gauge_src SELECT price + price AS total"
            ))
            .await
            .expect("define");
    }
    definer
        .apply("PAUSE TRANSFORM gauge_paused")
        .await
        .expect("pause");
    definer.shutdown().await.expect("shutdown the definer");
    assert_eq!(series(&name, "trellis_staging_leader", &[]), None);

    let trellis = Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..TrellisOptions::default()
        },
    )
    .await
    .expect("connect");
    raw.batch_execute("insert into public.gauge_src values (1, 10)")
        .await
        .expect("insert");
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converge");
    assert_eq!(series(&name, "trellis_staging_leader", &[]), Some(1.0));
    trellis.shutdown().await.expect("shutdown");

    let by = |status: &str| {
        series(
            &name,
            "trellis_definitions",
            &[&format!("status=\"{status}\"")],
        )
    };
    let total: f64 = [
        "waiting_to_backfill",
        "backfilling",
        "catching_up",
        "live",
        "quarantined",
        "paused",
    ]
    .iter()
    .map(|status| by(status).unwrap_or_else(|| panic!("no series for {status}")))
    .sum();
    assert_eq!(
        total, 2.0,
        "both definitions are counted, in one status each"
    );
    assert_eq!(by("paused"), Some(1.0));
    let cause = |cause: &str| {
        series(
            &name,
            "trellis_paused_definitions",
            &[&format!("cause=\"{cause}\"")],
        )
    };
    assert_eq!(cause("operator"), Some(1.0));
    for failure in [
        "capture",
        "disruption",
        "halt",
        "backfill_failure",
        "unknown",
    ] {
        assert_eq!(cause(failure), Some(0.0), "{failure}");
    }
    assert_eq!(series(&name, "trellis_held_keys", &[]), Some(0.0));
    assert_eq!(series(&name, "trellis_quarantined_columns", &[]), Some(0.0));
    assert_eq!(series(&name, "trellis_drain_failures", &[]), Some(0.0));
    assert_eq!(series(&name, "trellis_drain_workers", &[]), Some(1.0));
    assert_eq!(
        series(&name, "trellis_staging_leader", &[]),
        Some(0.0),
        "a stopped worker no longer leads"
    );
}
