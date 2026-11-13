//! The status and liveness gauges: how many definitions are in each status,
//! why the paused ones are, and the held keys, quarantined columns and holdups
//! that don't change a definition's status word (`docs/observability.md`).
//! Read on the staging worker's housekeeping pass, next to the ring's gauges
//! ([`super::ring_gauges`]), so no hot path keeps a count.
//!
//! The pass reads the catalog and nothing else:
//!
//! - **Statuses** are [`crate::defs::catalog::reported_statuses`], the read
//!   `Trellis::definitions` makes, so a gauge and the API agree on a
//!   `catching_up` definition. One query over the definitions, plus a
//!   definitions scan per paused column of a 1-1 target while there is one.
//! - **Why paused** is one query over the paused definitions: the `kind` of
//!   the definition's `capture_failures` record, else a failing build chunk
//!   (`backfill_failure`), else the operator, who leaves no record.
//! - **The counts** are one statement of `count(*)` over tables that hold a
//!   row per held key, paused column, open holdup or live worker, each
//!   small by construction (a fuse evicts a definition's keys past a
//!   threshold, and a holdup is cleared by the page that commits).
//!
//! No label names a definition, a table or a key: the series count is fixed.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio_postgres::GenericClient;

use super::worker_registry::LIVE_DRAIN_WORKER_SQL;
use crate::CaptureFailureKind;
use crate::TransformStatus;
use crate::app::CHUNK_FAILURE_JOIN;
use crate::defs::catalog::{self, CatalogError};
use crate::defs::floor::Floors;
use crate::metrics::StatusGauges;

/// Why a definition is paused, `trellis_paused_definitions`' `cause` label:
/// the kinds of a `capture_failure`, the operator, who leaves no record, a
/// build that kept failing (its error is on `backfill_failure`), and a
/// pause this release can't read the cause of.
const OPERATOR: &str = "operator";
const BACKFILL_FAILURE: &str = "backfill_failure";
const UNKNOWN: &str = "unknown";

/// Every cause, so each series is set on every read.
const CAUSES: [&str; 6] = [
    OPERATOR,
    "capture",
    "disruption",
    "halt",
    BACKFILL_FAILURE,
    UNKNOWN,
];

/// The cause of a paused definition with `kind` as its record's kind, if it
/// has a record, and whether a build chunk of it keeps failing.
fn cause(kind: Option<&str>, build_failing: bool) -> &'static str {
    match kind.map(CaptureFailureKind::from_persisted) {
        Some(CaptureFailureKind::Capture) => "capture",
        Some(CaptureFailureKind::Disruption) => "disruption",
        Some(CaptureFailureKind::Halt) => "halt",
        Some(CaptureFailureKind::Unknown) => UNKNOWN,
        None if build_failing => BACKFILL_FAILURE,
        None => OPERATOR,
    }
}

/// The counts of one housekeeping read, over the instance's catalog.
/// `drain_ttl` is how recently a worker must have been seen to count as
/// live, the window `Trellis::has_live_drain_workers` uses.
pub(crate) async fn read(
    client: &impl GenericClient,
    floors: &Floors,
    drain_ttl: Duration,
) -> Result<StatusGauges, CatalogError> {
    let reported = catalog::reported_statuses(client, floors).await?;
    let paused = client
        .query(
            &format!(
                "select d.id, cf.kind, bcf.attempts is not null \
                 from transform_definitions d \
                 {CHUNK_FAILURE_JOIN} \
                 left join capture_failures cf on cf.transform_id = d.id \
                 where d.status = 'paused'"
            ),
            &[],
        )
        .await?;
    let counts = client
        .query_one(
            &format!(
                "select (select count(*) from poison), \
                        (select count(*) from column_status \
                         where local_fuse and last_error is not null), \
                        (select count(*) from drain_holdups), \
                        (select count(*) from capture_holdups where operation is not null), \
                        (select count(*) from capture_holdups where operation is null), \
                        (select count(*) from worker_registry w {LIVE_DRAIN_WORKER_SQL})"
            ),
            &[&drain_ttl.as_secs_f64()],
        )
        .await?;

    let mut gauges = StatusGauges {
        definitions: TransformStatus::ALL
            .into_iter()
            .map(|status| (status.as_str(), 0))
            .collect(),
        paused_by: CAUSES.into_iter().map(|cause| (cause, 0)).collect(),
        held_keys: counts.get::<_, i64>(0) as u64,
        quarantined_columns: counts.get::<_, i64>(1) as u64,
        drain_failures: counts.get::<_, i64>(2) as u64,
        capture_waits: counts.get::<_, i64>(3) as u64,
        capture_failures: counts.get::<_, i64>(4) as u64,
        drain_workers: counts.get::<_, i64>(5) as u64,
    };
    // A definition reported `paused` that the second read didn't return (one
    // paused between the two reads, or a status a newer release wrote) is
    // `unknown`, so the causes add up to the paused count.
    let causes: BTreeMap<i64, &'static str> = paused
        .iter()
        .map(|row| {
            (
                row.get::<_, i64>(0),
                cause(row.get::<_, Option<&str>>(1), row.get(2)),
            )
        })
        .collect();
    for (id, status) in &reported {
        *gauges.definitions.entry(status.as_str()).or_default() += 1;
        if *status == TransformStatus::Paused {
            *gauges
                .paused_by
                .entry(causes.get(id).copied().unwrap_or(UNKNOWN))
                .or_default() += 1;
        }
    }
    Ok(gauges)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::defs::ast::ValueType;
    use crate::defs::lifecycle;
    use crate::pool::Pool;
    use crate::staging::pause_message::PauseReason;

    const TTL: Duration = Duration::from_secs(30);

    /// Defines `name` over `public.orders`, with the target and ledger a
    /// resume or a drop needs.
    async fn define(pool: &Pool, raw: &tokio_postgres::Client, name: &str) {
        let columns: HashMap<String, ValueType> = ["id", "price", "tax"]
            .iter()
            .map(|column| (column.to_string(), ValueType::Numeric))
            .collect();
        catalog::create_definition(
            pool,
            &format!("TRANSFORM {name} FROM public.orders SELECT price + tax AS total"),
            &columns,
        )
        .await
        .expect("create definition");
        raw.batch_execute(&format!(
            "create table public.{name} (id bigint primary key, total numeric)"
        ))
        .await
        .expect("create the target");
        raw.batch_execute(&crate::defs::ledger::one_to_one_ledger_ddl(&format!(
            "public.{name}__ledger"
        )))
        .await
        .expect("create the ledger");
    }

    /// Pauses `name` for `reason` through the one pause entry point, as the
    /// capture pass and the drain do.
    async fn pause_for(pool: &Pool, raw: &tokio_postgres::Client, name: &str, reason: PauseReason) {
        let id: i64 = raw
            .query_one(
                "select id from transform_definitions where target_table = $1",
                &[&format!("public.{name}")],
            )
            .await
            .expect("definition")
            .get(0);
        let mut client = pool.get().await.expect("connection");
        let txn = client.transaction().await.expect("begin");
        assert!(
            lifecycle::pause(&txn, id, &reason).await.expect("pause"),
            "{name} paused"
        );
        txn.commit().await.expect("commit");
    }

    async fn set_status(raw: &tokio_postgres::Client, name: &str, status: &str) {
        raw.execute(
            "update transform_definitions set status = $2 where target_table = $1",
            &[&format!("public.{name}"), &status],
        )
        .await
        .expect("set status");
    }

    /// One series of the process's registry for `instance`.
    fn series(rendered: &str, metric: &str, instance: &str, labels: &[&str]) -> Option<f64> {
        let wanted = format!("trellis_instance=\"{instance}\"");
        rendered
            .lines()
            .find(|line| {
                line.strip_prefix(metric)
                    .is_some_and(|rest| rest.starts_with('{'))
                    && line.contains(&wanted)
                    && labels.iter().all(|label| line.contains(label))
            })
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
    }

    /// One read of the catalog, published and rendered as an instance of its
    /// own, as the staging worker's pass does it.
    async fn publish(raw: &tokio_postgres::Client, instance: &str) -> (StatusGauges, String) {
        let reading = read(raw, &Floors::of_this_binary(), TTL)
            .await
            .expect("read the status gauges");
        let name: Arc<str> = Arc::from(instance);
        crate::instance_log::run_scoped(&name, || {
            crate::metrics::set_status_gauges(reading.clone())
        });
        (reading, crate::metrics::Metrics::new().render_prometheus())
    }

    /// Every condition the gauges count, set up in a catalog: definitions in
    /// each status, paused by the operator, a disruption, a halt, another
    /// capture failure and a failing build; held keys; columns whose fuse
    /// tripped, beside ones paused otherwise, which don't count; a
    /// drain holdup; a capture wait and a capture failure; a live worker and
    /// a stale one. The reading counts each, and clears each as its condition
    /// does: a resume, a drop, a released key and a worker that went quiet.
    #[tokio::test]
    async fn the_status_gauges_count_each_condition_and_clear_with_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("config");
        let pool = Pool::new(&config).expect("pool");
        let conn = db.pool.get().await.expect("connection");
        let raw: &tokio_postgres::Client = &conn;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, price numeric, tax numeric)",
        )
        .await
        .expect("source");
        for name in [
            "waiting",
            "building",
            "live",
            "broken",
            "by_op",
            "by_disruption",
            "by_halt",
            "by_capture",
            "by_build",
        ] {
            define(&pool, raw, name).await;
        }
        // The catalog-level define leaves a definition `live`; the others take
        // their state.
        set_status(raw, "waiting", "waiting_to_backfill").await;
        set_status(raw, "building", "backfilling").await;
        set_status(raw, "live", "live").await;
        set_status(raw, "broken", "quarantined").await;
        lifecycle::pause_transform(&pool, "by_op")
            .await
            .expect("operator pause");
        pause_for(
            &pool,
            raw,
            "by_disruption",
            PauseReason::Disruption {
                table: "public.orders".into(),
                text: "its capture trigger was disabled".into(),
            },
        )
        .await;
        pause_for(
            &pool,
            raw,
            "by_halt",
            PauseReason::Halt {
                table: "public.orders".into(),
                text: "the drain halted".into(),
            },
        )
        .await;
        pause_for(
            &pool,
            raw,
            "by_capture",
            PauseReason::capture("public.orders", "a column it reads was dropped"),
        )
        .await;
        set_status(raw, "by_build", "paused").await;
        raw.batch_execute(
            "insert into backfill_chunks (definition_id, kind, attempts, last_error, \
                                          next_attempt_at) \
             select id, 'plan', 3, 'a missing table', now() from transform_definitions \
             where target_table = 'public.by_build'; \
             insert into poison (transform_id, src_table, key, last_error) \
             select id, 'public.orders', k, 'refused' from transform_definitions, \
                    unnest(array['1', '2', '3']) k where target_table = 'public.live'; \
             insert into column_status (transform_table, column_name, last_error, local_fuse, \
                                        awaiting_capture) \
             values ('live', 'total', 'division by zero', true, false), \
                    ('broken', 'total', 'division by zero', true, false), \
                    ('waiting', 'total', null, true, false), \
                    ('building', 'total', 'upstream paused', false, false), \
                    ('by_build', 'total', null, false, true); \
             insert into drain_holdups (seg_seq, buckets, tables, error, since, last_seen, \
                                        attempts) \
             select seg_seq, '{0}', '{public.orders}', 'permission denied', now(), now(), 4 \
             from segments limit 1; \
             insert into capture_holdups (table_name, since, operation, lock_mode, observed_at, \
                                          blockers) \
             values ('public.orders', now(), 'install', 'ShareRowExclusiveLock', now(), '{}'); \
             insert into capture_holdups (table_name, since, error, columns) \
             values ('public.other', now(), 'no primary key', '{}'); \
             insert into worker_registry (worker_id, version, staging, drain, standby, last_seen) \
             values ('drain-a', '0.2.0', false, true, false, now()), \
                    ('drain-quiet', '0.2.0', false, true, false, now() - interval '10 minutes'), \
                    ('stand-by', '0.3.0', false, true, true, now()), \
                    ('stager', '0.2.0', true, false, false, now())",
        )
        .await
        .expect("seed the catalog");

        let instance = "status_gauges_db/trellis";
        let (reading, rendered) = publish(raw, instance).await;
        let counts = |map: &BTreeMap<&'static str, u64>| {
            map.iter()
                .filter(|(_, count)| **count > 0)
                .map(|(key, count)| (*key, *count))
                .collect::<BTreeMap<_, _>>()
        };
        assert_eq!(
            counts(&reading.definitions),
            BTreeMap::from([
                ("waiting_to_backfill", 1),
                ("backfilling", 1),
                ("live", 1),
                ("quarantined", 1),
                ("paused", 5),
            ]),
            "{reading:?}"
        );
        assert_eq!(
            counts(&reading.paused_by),
            BTreeMap::from([
                ("operator", 1),
                ("disruption", 1),
                ("halt", 1),
                ("capture", 1),
                ("backfill_failure", 1),
            ]),
            "{reading:?}"
        );
        assert_eq!(
            (
                reading.held_keys,
                reading.quarantined_columns,
                reading.drain_failures,
                reading.capture_waits,
                reading.capture_failures,
                reading.drain_workers,
            ),
            (3, 2, 1, 1, 1, 1),
            "{reading:?}"
        );
        // The rendered series carry the same.
        let get = |metric: &str, labels: &[&str]| series(&rendered, metric, instance, labels);
        assert_eq!(
            get("trellis_definitions", &["status=\"paused\""]),
            Some(5.0)
        );
        assert_eq!(
            get("trellis_definitions", &["status=\"catching_up\""]),
            Some(0.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"operator\""]),
            Some(1.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"disruption\""]),
            Some(1.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"halt\""]),
            Some(1.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"unknown\""]),
            Some(0.0)
        );
        assert_eq!(get("trellis_held_keys", &[]), Some(3.0));
        assert_eq!(get("trellis_quarantined_columns", &[]), Some(2.0));
        assert_eq!(get("trellis_drain_failures", &[]), Some(1.0));
        assert_eq!(
            get("trellis_capture_holdups", &["kind=\"wait\""]),
            Some(1.0)
        );
        assert_eq!(
            get("trellis_capture_holdups", &["kind=\"failure\""]),
            Some(1.0)
        );
        assert_eq!(get("trellis_drain_workers", &[]), Some(1.0));

        // Each condition clears. An operator's resume sends its definition to
        // `waiting_to_backfill`, a drop removes one, a held key is released, a
        // column resumes, the holdups go, and a worker that stops heartbeating
        // is no longer live.
        crate::staging::quarantine::resume_transform(&pool, "by_op")
            .await
            .expect("resume");
        crate::staging::quarantine::resume_transform(&pool, "by_halt")
            .await
            .expect("resume");
        lifecycle::drop_transform(&pool, "by_capture")
            .await
            .expect("drop");
        raw.batch_execute(
            "delete from poison where key = '1'; \
             delete from column_status where transform_table = 'broken'; \
             delete from drain_holdups; \
             delete from capture_holdups where operation is not null; \
             update worker_registry set last_seen = now() - interval '10 minutes' \
             where worker_id = 'drain-a'",
        )
        .await
        .expect("clear");
        let (reading, rendered) = publish(raw, instance).await;
        assert_eq!(
            counts(&reading.definitions),
            BTreeMap::from([
                ("waiting_to_backfill", 3),
                ("backfilling", 1),
                ("live", 1),
                ("quarantined", 1),
                ("paused", 2),
            ]),
            "{reading:?}"
        );
        assert_eq!(
            counts(&reading.paused_by),
            BTreeMap::from([("disruption", 1), ("backfill_failure", 1)]),
            "{reading:?}"
        );
        assert_eq!(
            (
                reading.held_keys,
                reading.quarantined_columns,
                reading.drain_failures,
                reading.capture_waits,
                reading.capture_failures,
                reading.drain_workers,
            ),
            (2, 1, 0, 0, 1, 0),
            "{reading:?}"
        );
        let get = |metric: &str, labels: &[&str]| series(&rendered, metric, instance, labels);
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"operator\""]),
            Some(0.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"halt\""]),
            Some(0.0)
        );
        assert_eq!(
            get("trellis_paused_definitions", &["cause=\"capture\""]),
            Some(0.0)
        );
        assert_eq!(
            get("trellis_definitions", &["status=\"paused\""]),
            Some(2.0)
        );
        assert_eq!(get("trellis_held_keys", &[]), Some(2.0));
        assert_eq!(get("trellis_drain_failures", &[]), Some(0.0));
        assert_eq!(
            get("trellis_capture_holdups", &["kind=\"wait\""]),
            Some(0.0)
        );
        assert_eq!(get("trellis_drain_workers", &[]), Some(0.0));
    }

    /// A `live` definition reading an upstream that is paused, or a paused
    /// column of one, reports `catching_up`, in the gauge as in the API
    /// (`reported_statuses`).
    #[tokio::test]
    async fn a_definition_reading_a_paused_upstream_counts_as_catching_up() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("config");
        let pool = Pool::new(&config).expect("pool");
        let conn = db.pool.get().await.expect("connection");
        let raw: &tokio_postgres::Client = &conn;
        raw.batch_execute(
            "create table public.orders (id bigint primary key, price numeric, tax numeric)",
        )
        .await
        .expect("source");
        define(&pool, raw, "upstream").await;
        let columns: HashMap<String, ValueType> = ["id", "total"]
            .iter()
            .map(|column| (column.to_string(), ValueType::Numeric))
            .collect();
        catalog::create_definition(
            &pool,
            "TRANSFORM downstream FROM public.upstream SELECT total * 2 AS doubled",
            &columns,
        )
        .await
        .expect("create the reader");
        set_status(raw, "upstream", "live").await;
        set_status(raw, "downstream", "live").await;

        let live = read(raw, &Floors::of_this_binary(), TTL)
            .await
            .expect("read");
        assert_eq!(live.definitions["live"], 2, "{live:?}");
        assert_eq!(live.definitions["catching_up"], 0, "{live:?}");

        // A paused column it reads holds it back too.
        raw.batch_execute(
            "insert into column_status (transform_table, column_name, local_fuse) \
             values ('upstream', 'total', true)",
        )
        .await
        .expect("pause the column");
        let column = read(raw, &Floors::of_this_binary(), TTL)
            .await
            .expect("read");
        assert_eq!(column.definitions["catching_up"], 1, "{column:?}");
        raw.batch_execute("delete from column_status")
            .await
            .expect("resume the column");

        lifecycle::pause_transform(&pool, "upstream")
            .await
            .expect("pause");
        let behind = read(raw, &Floors::of_this_binary(), TTL)
            .await
            .expect("read");
        assert_eq!(behind.definitions["paused"], 1, "{behind:?}");
        assert_eq!(behind.definitions["catching_up"], 1, "{behind:?}");
        assert_eq!(behind.definitions["live"], 0, "{behind:?}");
        assert_eq!(behind.paused_by["operator"], 1, "{behind:?}");
    }
}
