//! Version handover (`docs/decisions/0023-version-handover.md`), with the
//! binary version injected (`Config::with_binary_version`) so two versions
//! share one database in one test. Each check is driven directly: a
//! background process's first handover pass runs inside `Trellis::connect`,
//! later passes are stepped with `client::handover_pass_as`, and calls are
//! made after the state they test is set up. Nothing waits for a poll.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::client::handover_pass_as;
use trellis::staging::ProducerSession;
use trellis::{Config, ErrorCode, Trellis, TrellisError, TrellisOptions};

const OTHER: &str = "9.9.9-handover-test";

fn config(db: &testkit::TestDatabase) -> Config {
    Config::from_dsn(db.dsn().to_string()).expect("config")
}

async fn raw(db: &testkit::TestDatabase) -> Client {
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("search_path");
    client
}

/// `(worker_id, version, standby)` of every registered row, by worker id.
async fn registry(raw: &Client) -> Vec<(String, Option<String>, bool)> {
    raw.query(
        "select worker_id, version, standby from worker_registry order by worker_id",
        &[],
    )
    .await
    .expect("read the registry")
    .iter()
    .map(|row| (row.get(0), row.get(1), row.get(2)))
    .collect()
}

fn refused(result: Result<impl std::fmt::Debug, TrellisError>) -> TrellisError {
    let err = result.expect_err("refused while another version is live");
    assert_eq!(err.code(), ErrorCode::VersionHandover, "{err}");
    err
}

/// Rules 2, 4, 6 and 7 across a cross-version deploy: the new version's
/// background process stands by beside the old one, its calls refuse (from a
/// define-only handle too) while read-only calls and `status` work and say
/// why, the old version's calls go on, and once the old version shuts down
/// the new one takes over and its calls go through.
#[tokio::test]
async fn a_second_version_stands_by_then_takes_over_once_the_first_deregisters() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute("create table public.m (id bigint primary key, g bigint not null); create index on public.m (g)")
        .await
        .expect("source");

    let old = Trellis::connect(
        config(&db),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect the old version");
    old.apply("TRANSFORM m_agg FROM m GROUP BY g SELECT g AS g, COUNT(*) AS n")
        .await
        .expect("the old version defines");
    let old_rows = registry(&raw).await;
    assert_eq!(old_rows.len(), 1, "{old_rows:?}");
    let old_worker = old_rows[0].0.clone();

    // The new version's worker: its first pass, in `connect`, stands it by.
    let new_worker = Trellis::connect(
        config(&db).with_binary_version(OTHER),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    let rows = registry(&raw).await;
    let new_row = rows
        .iter()
        .find(|(worker, _, _)| *worker != old_worker)
        .expect("the new worker registered");
    assert_eq!(new_row.1.as_deref(), Some(OTHER));
    assert!(new_row.2, "the new worker stands by: {rows:?}");
    assert!(
        old.has_live_staging_worker().await.expect("read"),
        "the old version keeps the staging lock"
    );

    // Rule 6: every state-changing call refuses from a handle of the new
    // version, a define-only one included; read-only calls keep working.
    let new_definer = Trellis::connect(
        config(&db).with_binary_version(OTHER),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only handle");
    for handle in [&new_definer, &new_worker] {
        let err = refused(handle.apply("TRANSFORM m_copy FROM m SELECT g AS g").await);
        let text = err.to_string();
        assert!(text.contains(OTHER), "{text}");
        assert!(text.contains(env!("CARGO_PKG_VERSION")), "{text}");
        assert!(text.contains(&old_worker), "{text}");
        assert!(text.contains("30 seconds"), "{text}");
        refused(handle.apply("PAUSE TRANSFORM m_agg").await);
        refused(handle.request_backfill("public.m").await);
        refused(handle.release_key("m_agg", "public.m", "1").await);
        refused(handle.detect_capture_disruptions().await);
        refused(
            handle
                .self_check(
                    "m_agg",
                    trellis::SelfCheckMode::Standard,
                    std::time::Duration::from_secs(1),
                )
                .await,
        );
        assert_eq!(handle.definitions().await.expect("listing").len(), 1);
        let status = handle
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined");
        let seen = status.version_handover.expect("status says why");
        assert_eq!(seen.this_version, OTHER);
        assert_eq!(seen.other_versions(), [env!("CARGO_PKG_VERSION")]);
        assert!(
            seen.processes.iter().any(|p| p.worker_id == old_worker),
            "{seen:?}"
        );
    }
    // The old version's own calls go on: a standby holds nothing back.
    old.apply("PAUSE TRANSFORM m_agg")
        .await
        .expect("the old version still changes state");
    assert_eq!(
        old.status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .version_handover,
        None
    );

    // The old version stops cleanly: its row goes, and the new version's
    // next pass takes over.
    old.shutdown().await.expect("shutdown the old version");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        handover_pass_as(&pool, &config(&db).with_binary_version(OTHER), "next-pass")
            .await
            .expect("pass")
            .took_over,
        "nothing of another version is live any more"
    );
    new_definer
        .apply("RESUME TRANSFORM m_agg")
        .await
        .expect("the new version changes state once it has taken over");
    assert_eq!(
        new_definer
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .version_handover,
        None
    );

    new_definer.shutdown().await.expect("shutdown");
    new_worker.shutdown().await.expect("shutdown");
}

/// Rule 2's pre-0.2.0 shapes, through the calls: a live registry row with no
/// version (a drain worker of a release before 0.2.0), and the staging lock
/// held by a session no row names (its staging-only leader). Each refuses a
/// state-changing call of this build and stands a background pass by; with
/// both gone, the pass takes over.
#[tokio::test]
async fn the_pre_0_2_0_shapes_hold_this_version_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");

    raw.execute(
        "insert into worker_registry (worker_id) values ('trellis-old-1-00')",
        &[],
    )
    .await
    .expect("a pre-0.2.0 drain worker's row");
    let err = refused(trellis.request_backfill("public.nothing").await);
    assert!(err.to_string().contains("a release before 0.2.0"), "{err}");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over,
        "a background process stands by"
    );
    raw.execute("delete from worker_registry", &[])
        .await
        .expect("the old drain worker stops");

    let leader = ProducerSession::connect(db.dsn(), trellis::config::DEFAULT_SCHEMA)
        .await
        .expect("a staging leader that registers nothing");
    let err = refused(trellis.apply("PAUSE TRANSFORM nothing").await);
    assert!(err.to_string().contains("registers no version"), "{err}");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over
    );
    leader.release().await.expect("the old leader stops");

    assert!(
        handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over,
        "nothing else is live"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// Puts the database back to before migration V9 (every migration after it
/// undone too): the catalog a release before 0.2.0 left.
async fn rewind_to_v8(raw: &Client) {
    raw.batch_execute(
        "alter table worker_registry drop column version, drop column staging, \
           drop column drain, drop column standby, drop column staging_pid; \
         drop table join1_fan_out; \
         drop table catalog_floor; \
         alter table transform_definitions drop column floor_version; \
         alter table capture_baselines drop column generated_stored, drop column expr_hash; \
         alter table partition_corrections drop constraint partition_corrections_cause_check; \
         drop table definition_keys; \
         update trellis_instance set instance_format_version = 1; \
         delete from refinery_schema_history where version > 8",
    )
    .await
    .expect("a database at V8");
}

/// A live registry row of a release before 0.2.0 (it records no version).
async fn old_worker_live(raw: &Client) {
    raw.execute(
        "insert into worker_registry (worker_id) values ('trellis-old-1-00')",
        &[],
    )
    .await
    .expect("a live v0.1.0 drain worker");
}

/// Whether migration V9 has run.
async fn v9_ran(raw: &Client) -> bool {
    raw.query_one(
        "select exists (select 1 from refinery_schema_history where version = 9)",
        &[],
    )
    .await
    .expect("history")
    .get(0)
}

/// Rules 3 and 4: a migration this binary carries is never run while a
/// Trellis process is live; `migrate` refuses instead, and a background
/// process stands by and migrates nothing. An explicit `migrate` still runs
/// it once nothing is live, and the next pass then finds nothing pending and
/// takes over without migrating.
#[tokio::test]
async fn a_pending_migration_is_not_run_while_a_process_is_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    old_worker_live(&raw).await;

    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    let err = refused(trellis.migrate().await);
    assert!(err.to_string().contains("migrations are pending"), "{err}");
    assert!(!v9_ran(&raw).await, "V9 did not run");
    // A background process waits too, migrates nothing, and registers
    // nothing meanwhile.
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(!pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    assert!(!v9_ran(&raw).await, "V9 did not run");
    let rows: i64 = raw
        .query_one("select count(*) from worker_registry", &[])
        .await
        .expect("registry")
        .get(0);
    assert_eq!(rows, 1, "only the old worker's row");

    raw.execute("delete from worker_registry", &[])
        .await
        .expect("the old worker stops");
    trellis
        .migrate()
        .await
        .expect("migrates once nothing is live");
    assert!(v9_ran(&raw).await, "V9 ran");
    // V11 recorded the floor and set the marker's retired format version to
    // 2 on a catalog it upgraded, which releases before 0.2.0 refuse.
    let marker = raw
        .query_one(
            "select (select version from catalog_floor), instance_format_version \
             from trellis_instance",
            &[],
        )
        .await
        .expect("marker");
    assert_eq!(marker.get::<_, String>(0), "0.2.0");
    assert_eq!(marker.get::<_, i32>(1), 2);
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    trellis.shutdown().await.expect("shutdown");
}

/// Rules 1, 3 and 7 across a deploy with no `migrate` step: while the old
/// release is live, a background process of this one stands by and runs
/// nothing, and its reads say which migrations are pending; once the old
/// release is gone, its next pass runs them, takes over and logs which ran.
#[tokio::test]
async fn a_background_process_migrates_itself_once_the_old_version_is_gone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    old_worker_live(&raw).await;

    // Its first pass, in `connect`, stands it by.
    let worker = Trellis::connect(
        config(&db),
        TrellisOptions {
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    assert!(
        !v9_ran(&raw).await,
        "nothing migrates beside the old worker"
    );
    // The V8 catalog lacks what the migrations add, so a read refuses with
    // the pending migrations rather than a bare SQL error.
    let err = refused(worker.status("anything").await);
    let TrellisError::MigrationsPending { pending, .. } = &err else {
        panic!("{err:?}");
    };
    assert_eq!(
        pending.migrations.first().map(String::as_str),
        Some("V9__worker_versions")
    );
    assert_eq!(pending.cannot_run, None, "{pending:?}");

    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(!pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    // The worker stops before the old one does, so its own handover task
    // can't run the migrations before the pass this test steps.
    worker.shutdown().await.expect("shutdown");

    raw.execute("delete from worker_registry", &[])
        .await
        .expect("the old worker stops");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over, "{pass:?}");
    assert_eq!(
        pass.migrated.first().map(String::as_str),
        Some("V9__worker_versions")
    );
    assert!(
        pass.migrated.len() > 1,
        "every pending migration ran: {pass:?}"
    );
    let log = pass.log.expect("the takeover is logged");
    assert!(
        log.contains("ran this binary's pending migrations (V9__worker_versions, "),
        "{log}"
    );
    assert!(v9_ran(&raw).await);
    // The ordinary `migrate` ran: V11 recorded the floor and the marker.
    let marker = raw
        .query_one(
            "select (select version from catalog_floor), instance_format_version \
             from trellis_instance",
            &[],
        )
        .await
        .expect("marker");
    assert_eq!(marker.get::<_, String>(0), "0.2.0");
    assert_eq!(marker.get::<_, i32>(1), 2);
    let rows = registry(&raw).await;
    assert!(
        rows.iter().any(|(id, version, standby)| id == "probe"
            && version.as_deref() == Some(env!("CARGO_PKG_VERSION"))
            && !standby),
        "the process registered as taken over: {rows:?}"
    );
    // Nothing is pending now, so reads work and report nothing pending.
    let reader = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    assert!(reader.definitions().await.expect("listing").is_empty());
    assert_eq!(reader.status("anything").await.expect("status"), None);
    reader.shutdown().await.expect("shutdown");
}

/// Rule 1: two background processes of this version that find the
/// migrations pending at once migrate one after the other under the locks:
/// one runs them, the other finds nothing left, and both take over.
#[tokio::test]
async fn two_processes_racing_to_migrate_run_the_migrations_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;

    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let cfg = config(&db);
    let (first, second) = tokio::join!(
        handover_pass_as(&pool, &cfg, "first"),
        handover_pass_as(&pool, &cfg, "second"),
    );
    let (first, second) = (first.expect("pass"), second.expect("pass"));
    assert!(first.took_over && second.took_over, "{first:?} {second:?}");
    assert_eq!(
        [&first, &second]
            .iter()
            .filter(|pass| !pass.migrated.is_empty())
            .count(),
        1,
        "exactly one ran the migrations: {first:?} {second:?}"
    );
    let ran: i64 = raw
        .query_one(
            "select count(*) from refinery_schema_history where version = 9",
            &[],
        )
        .await
        .expect("history")
        .get(0);
    assert_eq!(ran, 1);
    assert_eq!(registry(&raw).await.len(), 2, "both registered");
}

/// Rule 2: a process of the old release that goes live after a pass decided
/// to migrate, but before the migration took the handover lock, holds it
/// back: `migrate` decides again under the lock, runs nothing, and the
/// process stands by. The pass is held between its decision and `migrate` by
/// holding the attach lock `migrate` takes first; it is seen waiting on that
/// lock in `pg_locks`.
#[tokio::test]
async fn a_process_that_goes_live_before_the_migration_holds_it_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    let holder = self::raw(&db).await;
    holder
        .execute(
            "select pg_advisory_lock($1)",
            &[&trellis::identity::ATTACH_LOCK_KEY],
        )
        .await
        .expect("hold the attach lock");

    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let cfg = config(&db);
    let pass = tokio::spawn(async move { handover_pass_as(&pool, &cfg, "probe").await });
    // The pass decided (nothing live) and waits in `migrate` for the lock.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let waiting: bool = raw
            .query_one(
                "select exists (select 1 from pg_locks \
                 where locktype = 'advisory' and not granted \
                 and database = (select oid from pg_database \
                                 where datname = current_database()))",
                &[],
            )
            .await
            .expect("pg_locks")
            .get(0);
        if waiting {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the pass never reached migrate"
        );
        tokio::task::yield_now().await;
    }
    old_worker_live(&raw).await;
    holder
        .execute(
            "select pg_advisory_unlock($1)",
            &[&trellis::identity::ATTACH_LOCK_KEY],
        )
        .await
        .expect("release it");

    let pass = pass.await.expect("pass task").expect("pass");
    assert!(!pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    // A version that went live meanwhile is no refusal to back off from: the
    // next pass checks again.
    assert_eq!(pass.retry_in, None, "{pass:?}");
    assert!(
        !v9_ran(&raw).await,
        "nothing migrated beside the old worker"
    );
    let log = pass.log.expect("the standby is logged");
    assert!(log.contains("trellis-old-1-00"), "{log}");
}

/// A role that can read and write Trellis's tables but not create or alter
/// them, as its config, and the grants that give it the tables there now.
async fn dml_only_role(db: &testkit::TestDatabase, raw: &Client) -> (Config, String) {
    let role = format!("handover_dml_{}", std::process::id());
    let grants = format!(
        "grant usage on schema {schema} to {role}; \
         grant select, insert, update, delete on all tables in schema {schema} to {role}; \
         grant usage, select on all sequences in schema {schema} to {role}",
        schema = trellis::config::DEFAULT_SCHEMA
    );
    raw.batch_execute(&format!("create role {role} login; {grants}"))
        .await
        .expect("a role with DML rights only");
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let config = Config::from_dsn(db.dsn().replace("user=postgres", &format!("user={role}")))
        .expect("config");
    (config, grants)
}

/// Rule 3's refusals: a role that can read and write Trellis's tables but
/// not alter them can't run the migrations, so its background process
/// stands by and says why, in its log and in `status()`, and tries again
/// every pass; once another role runs `migrate`, it takes over.
#[tokio::test]
async fn a_role_that_cannot_migrate_stands_by_and_says_so() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    let (as_role, grants) = dml_only_role(&db, &raw).await;

    let worker = Trellis::connect(
        as_role.clone(),
        TrellisOptions {
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    assert!(!v9_ran(&raw).await);
    let err = refused(worker.status("anything").await);
    let TrellisError::MigrationsPending { pending, .. } = &err else {
        panic!("{err:?}");
    };
    let reason = pending
        .cannot_run
        .as_deref()
        .expect("its pass tried and says why");
    assert!(reason.contains("this role cannot run them"), "{reason}");

    let pool = trellis::pool::Pool::new(&as_role).expect("pool");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("a refused migrate is a standby, not an error");
    assert!(!pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    // It waits on an operator, so the process tries again after a minute,
    // not every standby poll.
    assert!(
        pass.retry_in
            .is_some_and(|wait| wait > std::time::Duration::from_secs(30)),
        "{pass:?}"
    );
    let log = pass.log.expect("the standby is logged");
    assert!(log.contains("this role cannot run them"), "{log}");
    assert!(log.contains("V9__worker_versions"), "{log}");

    Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect as the owner")
        .migrate()
        .await
        .expect("the owner migrates");
    raw.batch_execute(&grants)
        .await
        .expect("grants on the new tables");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("pass");
    assert!(pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    worker.shutdown().await.expect("shutdown");
}

/// Rule 3: the automatic migration is the ordinary `migrate`, so it refuses
/// a history that doesn't fit this binary's migrations (a changed
/// migration), and the process stands by and says why until it fits.
#[tokio::test]
async fn a_changed_migration_stands_the_process_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    let checksum: String = raw
        .query_one(
            "select checksum from refinery_schema_history where version = 1",
            &[],
        )
        .await
        .expect("history")
        .get(0);
    raw.execute(
        "update refinery_schema_history set checksum = '1' where version = 1",
        &[],
    )
    .await
    .expect("a changed migration");

    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(!pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    let log = pass.log.expect("the standby is logged");
    assert!(log.contains("migrate refused"), "{log}");
    assert!(pass.retry_in.is_some(), "a changed migration backs off");
    assert!(!v9_ran(&raw).await);

    raw.execute(
        "update refinery_schema_history set checksum = $1 where version = 1",
        &[&checksum],
    )
    .await
    .expect("the history fits again");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over && !pass.migrated.is_empty(), "{pass:?}");
}

/// Rule 5: a define-only handle runs no background work, so nothing
/// migrates on its behalf: connecting leaves the migrations pending.
#[tokio::test]
async fn a_define_only_handle_never_migrates() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    rewind_to_v8(&raw).await;
    let definer = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    refused(definer.status("anything").await);
    assert!(!v9_ran(&raw).await);
    definer.migrate().await.expect("an explicit migrate");
    assert!(v9_ran(&raw).await);
    definer.shutdown().await.expect("shutdown");
}

/// While another version is live and nothing is pending, `migrate` changes
/// nothing and succeeds, so a deploy's migration step that has nothing to
/// migrate doesn't fail. It doesn't record its version as the one that ran
/// the takeover work, and a marker naming another version doesn't refuse it.
#[tokio::test]
async fn migrate_with_nothing_pending_beside_another_version_changes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.execute(
        "insert into worker_registry (worker_id, version) values ('trellis-new-1-00', $1)",
        &[&OTHER],
    )
    .await
    .expect("a live process of another version");
    raw.execute(
        "update trellis_instance set attached_version = $1",
        &[&OTHER],
    )
    .await
    .expect("the other version ran the takeover work");
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    trellis
        .migrate()
        .await
        .expect("nothing to migrate and nothing to change");
    assert_eq!(attached(&raw).await.as_deref(), Some(OTHER));
    refused(trellis.apply("PAUSE TRANSFORM nothing").await);
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 8 with ADR-0024: a binary meets the catalog floor first, so it never
/// attaches to a database a newer binary migrated past what it can read.
/// Below the floor it refuses, alone and beside a live process of the newer
/// version alike, rather than succeeding there and taking over once the newer
/// one stops. At or above the floor, it tolerates the newer migration.
#[tokio::test]
async fn migrate_beside_a_newer_version_still_refuses_below_the_catalog_floor() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.execute(
        "insert into refinery_schema_history (version, name, applied_on, checksum) \
         values (9999, 'from_a_newer_binary', '2026-11-02T00:00:00Z', '1')",
        &[],
    )
    .await
    .expect("a migration this binary lacks");
    // A release at the floor: a development build's own version is below it,
    // and its own migrations' floor doesn't cover one it lacks.
    let trellis = Trellis::connect(
        config(&db).with_binary_version("0.2.0"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    trellis
        .migrate()
        .await
        .expect("at the floor, a newer binary's migration is tolerated");

    raw.execute("update catalog_floor set version = '9.9.9'", &[])
        .await
        .expect("the newer binary raised the floor");
    let alone = trellis.migrate().await.expect_err("below the floor");
    assert_eq!(alone.code(), ErrorCode::VersionFloor, "{alone}");
    assert!(alone.to_string().contains("9.9.9"), "{alone}");

    raw.execute(
        "insert into worker_registry (worker_id, version) values ('trellis-new-1-00', $1)",
        &[&OTHER],
    )
    .await
    .expect("a live process of the newer version");
    let beside = trellis
        .migrate()
        .await
        .expect_err("refused beside the newer version too");
    assert_eq!(beside.code(), ErrorCode::VersionFloor, "{beside}");
    trellis.shutdown().await.expect("shutdown");
}

/// Two processes of one version never hold each other back, whatever their
/// roles: a second worker beside a leading one (a rolling deploy of the same
/// release, or a restart beside the old process's row) takes over, the
/// leader's lock matched to its row, and a call goes through.
#[tokio::test]
async fn processes_of_one_version_never_stand_by() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    let worker = TrellisOptions {
        staging: true,
        drain_threads: 1,
        ..Default::default()
    };
    let leader = Trellis::connect(config(&db), worker.clone())
        .await
        .expect("connect the leader");
    assert!(leader.has_live_staging_worker().await.expect("read"));
    let second = Trellis::connect(config(&db), worker)
        .await
        .expect("connect a second worker");
    let rows = registry(&raw).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|(_, _, standby)| !standby), "{rows:?}");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over,
        "the leader's lock is matched to its row of this version"
    );
    let err = second
        .apply("TRANSFORM m_copy FROM m SELECT g AS g")
        .await
        .expect_err("there is no source table");
    assert_ne!(err.code(), ErrorCode::VersionHandover, "{err}");
    second.shutdown().await.expect("shutdown");
    leader.shutdown().await.expect("shutdown");
}

/// `has_live_drain_workers` counts the live rows whose drain threads run and
/// don't stand by, and a row with no version (a release before 0.2.0, where
/// only drain workers registered), but not a staging-only process or one that
/// stands by.
#[tokio::test]
async fn has_live_drain_workers_counts_running_drain_threads() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    raw.batch_execute(&format!(
        "insert into worker_registry (worker_id, version, staging, drain, standby) values \
           ('staging-only', '{OTHER}', true, false, false), \
           ('standing-by', '{OTHER}', false, true, true)"
    ))
    .await
    .expect("rows that drain nothing");
    assert!(!trellis.has_live_drain_workers().await.expect("read"));
    raw.execute(
        "insert into worker_registry (worker_id) values ('trellis-old-1-00')",
        &[],
    )
    .await
    .expect("a pre-0.2.0 drain worker's row");
    assert!(trellis.has_live_drain_workers().await.expect("read"));
    raw.batch_execute(
        "delete from worker_registry where version is null; \
         update worker_registry set standby = false where worker_id = 'standing-by'",
    )
    .await
    .expect("the drain worker takes over");
    assert!(trellis.has_live_drain_workers().await.expect("read"));
    trellis.shutdown().await.expect("shutdown");
}

/// A background process's pass and `migrate` decide under the instance's
/// handover lock, so two of them never decide at once: while another session
/// holds it, neither finishes, and both do once it is released. (A pass that
/// didn't take the lock would finish at once; one that does can't finish
/// while it is held, so the timeout only bounds the wait, never the result.)
#[tokio::test]
async fn the_pass_and_migrate_wait_for_the_handover_lock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    let trellis = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let key = trellis::handover::handover_lock_key(trellis::config::DEFAULT_SCHEMA);
    raw.execute("select pg_advisory_lock($1)", &[&key])
        .await
        .expect("hold the handover lock");
    let wait = std::time::Duration::from_secs(2);
    assert!(
        tokio::time::timeout(wait, handover_pass_as(&pool, &config(&db), "probe"))
            .await
            .is_err(),
        "the pass waits for the lock"
    );
    assert!(
        tokio::time::timeout(wait, trellis.migrate()).await.is_err(),
        "migrate waits for the lock"
    );
    raw.execute("select pg_advisory_unlock($1)", &[&key])
        .await
        .expect("release it");
    assert!(
        handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over
    );
    trellis.migrate().await.expect("migrate");
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 4 at start: a worker that stands by from its first pass never takes
/// the staging-worker lock, so it installs and seals nothing. The staging
/// setup, which runs before `connect` returns, and the maintenance loop both
/// check; were neither to, the lock would be held when it is read here.
#[tokio::test]
async fn a_worker_standing_by_from_the_start_takes_no_staging_lock() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.execute(
        "insert into worker_registry (worker_id) values ('trellis-old-1-00')",
        &[],
    )
    .await
    .expect("a pre-0.2.0 drain worker's row");
    let worker = Trellis::connect(
        config(&db),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    assert!(!worker.has_live_staging_worker().await.expect("read"));
    let rows = registry(&raw).await;
    assert!(
        rows.iter()
            .any(|(id, version, standby)| id != "trellis-old-1-00"
                && version.is_some()
                && *standby),
        "{rows:?}"
    );
    worker.shutdown().await.expect("shutdown");
}

fn below_the_floor(result: Result<impl std::fmt::Debug, TrellisError>) -> TrellisError {
    let err = result.expect_err("refused below the catalog floor");
    assert_eq!(err.code(), ErrorCode::VersionFloor, "{err}");
    err
}

/// ADR-0024 rule 2 for a handle that only connects: below the catalog floor,
/// a worker connects and stands by without writing anything (no registry
/// row, no staging lock, no capture), every state-changing call of any of
/// this version's handles refuses with `version_floor`, read-only calls and
/// `status` keep working and name the floor, and a version that meets the
/// floor goes on. Once the floor comes down, this version's next pass takes
/// over and its calls go through.
#[tokio::test]
async fn a_handle_below_the_catalog_floor_stands_by_and_refuses_until_it_meets_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.m (id bigint primary key, g bigint not null); \
         create index on public.m (g)",
    )
    .await
    .expect("source");
    let definer = Trellis::connect(config(&db), TrellisOptions::default())
        .await
        .expect("connect a define-only handle");
    definer
        .apply("TRANSFORM m_agg FROM m GROUP BY g SELECT g AS g, COUNT(*) AS n")
        .await
        .expect("define at the floor");
    let floor: String = raw
        .query_one("select version from catalog_floor", &[])
        .await
        .expect("the floor")
        .get(0);
    raw.execute("update catalog_floor set version = '9.9.9'", &[])
        .await
        .expect("a newer release raised the floor");

    let worker = Trellis::connect(
        config(&db),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a worker below the floor connects without error");
    assert!(
        !worker.has_live_staging_worker().await.expect("read"),
        "it takes no staging lock"
    );
    assert_eq!(registry(&raw).await, [], "it registers nothing");
    let triggers: i64 = raw
        .query_one(
            "select count(*) from pg_trigger where tgrelid = 'public.m'::regclass \
             and not tgisinternal",
            &[],
        )
        .await
        .expect("count triggers")
        .get(0);
    assert_eq!(triggers, 0, "it installs no capture");
    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    assert!(
        !handover_pass_as(&pool, &config(&db), "probe")
            .await
            .expect("pass")
            .took_over,
        "every standby poll checks the floor again"
    );

    for handle in [&definer, &worker] {
        let err = below_the_floor(handle.apply("PAUSE TRANSFORM m_agg").await);
        let text = err.to_string();
        assert!(text.contains("9.9.9"), "{text}");
        assert!(text.contains(env!("CARGO_PKG_VERSION")), "{text}");
        below_the_floor(handle.apply("TRANSFORM m_copy FROM m SELECT g AS g").await);
        below_the_floor(handle.request_backfill("public.m").await);
        below_the_floor(handle.release_key("m_agg", "public.m", "1").await);
        below_the_floor(handle.detect_capture_disruptions().await);
        below_the_floor(
            handle
                .self_check(
                    "m_agg",
                    trellis::SelfCheckMode::Standard,
                    std::time::Duration::from_secs(1),
                )
                .await,
        );
        assert_eq!(handle.definitions().await.expect("listing").len(), 1);
        let status = handle
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined");
        let seen = status.below_catalog_floor.expect("status says why");
        assert_eq!(seen.floor, "9.9.9");
        assert_eq!(seen.this_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(
            status.version_handover, None,
            "{:?}",
            status.version_handover
        );
    }

    // A version at the floor goes on: a process standing by below it holds
    // nothing back.
    let newer = Trellis::connect(
        config(&db).with_binary_version("9.9.9"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a version at the floor");
    newer
        .apply("PAUSE TRANSFORM m_agg")
        .await
        .expect("a version at the floor changes state");
    assert_eq!(
        newer
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .below_catalog_floor,
        None
    );
    newer.shutdown().await.expect("shutdown");

    raw.execute("update catalog_floor set version = $1", &[&floor])
        .await
        .expect("the floor comes down");
    assert!(
        handover_pass_as(&pool, &config(&db), "next-pass")
            .await
            .expect("pass")
            .took_over,
        "this version's next pass takes over"
    );
    worker
        .apply("RESUME TRANSFORM m_agg")
        .await
        .expect("calls go through once it meets the floor");
    assert_eq!(
        worker
            .status("m_agg")
            .await
            .expect("status")
            .expect("defined")
            .below_catalog_floor,
        None
    );

    worker.shutdown().await.expect("shutdown");
    definer.shutdown().await.expect("shutdown");
}

/// The version the marker records as the last to run the takeover work.
async fn attached(raw: &Client) -> Option<String> {
    raw.query_one("select attached_version from trellis_instance", &[])
        .await
        .expect("the marker")
        .get(0)
}

/// Defines `t` over `public.s`, then stores text for it that an earlier
/// release accepted and this one refuses, which the takeover work pauses.
async fn define_refused_text(db: &testkit::TestDatabase, raw: &Client) {
    raw.batch_execute("create table public.s (id bigint primary key, a numeric)")
        .await
        .expect("source");
    let definer = Trellis::connect(config(db), TrellisOptions::default())
        .await
        .expect("connect");
    definer
        .apply("TRANSFORM t FROM s SELECT a + 1 AS x")
        .await
        .expect("define");
    definer.shutdown().await.expect("shutdown");
    raw.execute(
        "update transform_definitions set definition_text = 'TRANSFORM t FROM s SELECT a+1AS x'",
        &[],
    )
    .await
    .expect("text this release refuses");
}

/// Whether `t` is paused.
async fn t_paused(raw: &Client) -> bool {
    raw.query_one(
        "select status = 'paused' from transform_definitions where target_table = 'public.t'",
        &[],
    )
    .await
    .expect("t")
    .get(0)
}

/// A schema dump's restore queue with nothing in it; restoring it drops it.
async fn queue_restore(raw: &Client) {
    raw.batch_execute(
        "create table restore_queue (ordinal integer primary key, source_table text not null, \
         target_table text not null, join_table text, definition_text text not null, \
         grammar_version integer not null, floor_version text not null)",
    )
    .await
    .expect("a restore queue");
}

async fn restore_queued(raw: &Client) -> bool {
    raw.query_one("select to_regclass('restore_queue') is not null", &[])
        .await
        .expect("to_regclass")
        .get(0)
}

const TOOK_OVER: &str = "no other Trellis version is live any more; taking over";
const RAN_THE_WORK: &str = "ran this binary's takeover work; taking over";

/// Rules 3 and 7: the takeover work runs once per binary version, and the
/// marker records it in the work's own transaction. A kill after the
/// migrations and before the work leaves the marker unset, so the next pass
/// runs the work (pausing text this release refuses) and takes over; a
/// restart of the same version runs none; a queued restore is due whatever
/// the marker says.
#[tokio::test]
async fn a_pass_runs_the_takeover_work_this_version_has_not_then_takes_over() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    define_refused_text(&db, &raw).await;
    raw.execute("update trellis_instance set attached_version = null", &[])
        .await
        .expect("killed before the takeover work committed");
    assert!(!t_paused(&raw).await);

    let pool = trellis::pool::Pool::new(&config(&db)).expect("pool");
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    assert_eq!(pass.log.as_deref(), Some(RAN_THE_WORK));
    assert!(t_paused(&raw).await, "the work paused the refused text");
    assert_eq!(
        attached(&raw).await.as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );

    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over, "{pass:?}");
    assert_eq!(
        pass.log.as_deref(),
        Some(TOOK_OVER),
        "a restart runs no work"
    );

    queue_restore(&raw).await;
    let pass = handover_pass_as(&pool, &config(&db), "probe")
        .await
        .expect("pass");
    assert!(pass.took_over, "{pass:?}");
    assert_eq!(pass.log.as_deref(), Some(RAN_THE_WORK));
    assert!(!restore_queued(&raw).await, "the queue was restored");
}

/// Rule 3 for a release with no new migration: a process of another version
/// with nothing pending runs the takeover work and records its version before
/// it takes over.
#[tokio::test]
async fn a_version_with_no_new_migration_runs_the_takeover_work() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    define_refused_text(&db, &raw).await;
    let other = config(&db).with_binary_version(OTHER);
    let pool = trellis::pool::Pool::new(&other).expect("pool");
    let pass = handover_pass_as(&pool, &other, "probe")
        .await
        .expect("pass");
    assert!(pass.took_over && pass.migrated.is_empty(), "{pass:?}");
    assert_eq!(pass.log.as_deref(), Some(RAN_THE_WORK));
    assert!(t_paused(&raw).await);
    assert_eq!(attached(&raw).await.as_deref(), Some(OTHER));
}

/// Rule 3 with nothing pending runs no migration runner, so a role that can
/// read and write Trellis's tables but not create them takes over when the
/// marker names its version, and runs the work that writes only rows. Work it
/// can't finish commits nothing, the marker included; work that needs more (a
/// restore) stands it by, saying why in its log and `status()`, backing off.
#[tokio::test]
async fn a_role_that_cannot_migrate_runs_the_takeover_work_it_can() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    define_refused_text(&db, &raw).await;
    let (as_role, grants) = dml_only_role(&db, &raw).await;
    let pool = trellis::pool::Pool::new(&as_role).expect("pool");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("pass");
    assert!(pass.took_over, "{pass:?}");
    assert_eq!(pass.log.as_deref(), Some(TOOK_OVER), "the marker matches");

    let role = format!("handover_dml_{}", std::process::id());
    raw.batch_execute(&format!(
        "update trellis_instance set attached_version = null; \
         revoke update on transform_definitions from {role}"
    ))
    .await
    .expect("work the role can't finish");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("pass");
    assert!(!pass.took_over, "{pass:?}");
    assert!(!t_paused(&raw).await);
    assert_eq!(attached(&raw).await, None, "nothing committed");

    raw.batch_execute(&grants).await.expect("grants");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("pass");
    assert!(pass.took_over, "{pass:?}");
    assert_eq!(pass.log.as_deref(), Some(RAN_THE_WORK));
    assert!(t_paused(&raw).await);
    assert_eq!(
        attached(&raw).await.as_deref(),
        Some(env!("CARGO_PKG_VERSION"))
    );

    queue_restore(&raw).await;
    raw.batch_execute(&grants)
        .await
        .expect("grants on the queue");
    let pass = handover_pass_as(&pool, &as_role, "probe")
        .await
        .expect("a refused restore is a standby, not an error");
    assert!(!pass.took_over, "{pass:?}");
    assert!(
        pass.retry_in
            .is_some_and(|wait| wait > std::time::Duration::from_secs(30)),
        "{pass:?}"
    );
    let log = pass.log.expect("the standby is logged");
    assert!(log.contains("this role cannot run them"), "{log}");
    assert!(restore_queued(&raw).await);
    let worker = Trellis::connect(
        as_role,
        TrellisOptions {
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("a standby connects without error");
    let pending = worker
        .status("t")
        .await
        .expect("status")
        .expect("defined")
        .migrations_pending
        .expect("status says why it stands by");
    assert!(pending.migrations.is_empty(), "{pending:?}");
    let reason = pending.cannot_run.expect("its pass tried");
    assert!(reason.contains("this role cannot run them"), "{reason}");
    // With no migration pending, the catalog is current: a read that fails
    // fails with its own error, not as a version handover.
    raw.batch_execute(&format!(
        "revoke select on transform_definitions from {role}"
    ))
    .await
    .expect("a read the role can't make");
    let err = worker.definitions().await.expect_err("can't read");
    assert_ne!(err.code(), ErrorCode::VersionHandover, "{err}");
    worker.shutdown().await.expect("shutdown");
}
