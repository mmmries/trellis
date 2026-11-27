//! A staging worker whose role holds DML on Trellis's tables and nothing
//! more, beside a privileged role that applies definitions (rules 1-3 of
//! #2025).
//!
//! A step the role is refused is reported, not fatal: a refused ring step
//! shows in `refused()` (what `status()` carries as `maintenance_refusals`),
//! pauses nothing and clears when the privilege is granted; a refused build
//! start halts and pauses that definition only. Every test steps the leader
//! by hand (`LeaderTick`), with its back-off at zero: nothing waits on a
//! timer.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::client::LeaderTick;
use trellis::defs::TransformStatus;
use trellis::{CaptureFailureKind, Trellis};

use crate::support::pipeline::{self, bring_live, connect, definer};

/// The worker role, its connection and its `LeaderTick`, over the database
/// `raw` (as `postgres`) built: DML on every table there now, `TRUNCATE`
/// withheld, and no default privileges for tables created later.
struct Worker {
    role: String,
    client: Client,
    tick: LeaderTick,
}

async fn dml_worker(dsn: &str, raw: &Client) -> Worker {
    let role = format!("dml_worker_{}", std::process::id());
    raw.batch_execute(&format!(
        "create role {role} login; \
         grant usage on schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant usage, select, update on all sequences in schema trellis to {role}; \
         grant usage on schema public to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("a DML-only role");
    let as_role = dsn.replace("user=postgres", &format!("user={role}"));
    let client = connect(&as_role).await;
    let config = trellis::Config::from_dsn(as_role).expect("config");
    let pool = trellis::pool::Pool::new(&config).expect("pool");
    Worker {
        role,
        client,
        tick: LeaderTick::new(pool, "trellis"),
    }
}

async fn status_of(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis.status(target).await.expect("status").expect(target)
}

/// A source with one live definition `t`, captured and built by the
/// privileged role.
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::pool::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.u (id int primary key, a int); \
         insert into public.u values (1, 1)",
    )
    .await
    .expect("seed");
    let admin = definer(dsn).await;
    admin
        .apply("TRANSFORM t FROM public.u SELECT a AS a")
        .await
        .expect("define t");
    bring_live(raw, pool, &["t"]).await;
    admin
}

/// A ring segment the role may not retire: it is refused `TRUNCATE` on the
/// segment, which names the step, the role, the table and the privilege,
/// pauses nothing, doesn't end the tick, and is gone once the privilege is
/// granted and a pass of the step succeeds.
#[tokio::test]
async fn a_refused_ring_step_is_reported_pauses_nothing_and_recovers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    let mut worker = dml_worker(db.dsn(), &raw).await;

    // A drained segment for the worker to retire.
    raw.batch_execute("insert into public.u values (2, 2)")
        .await
        .expect("a write");
    let batch = pipeline::seal(&mut raw).await;
    pipeline::drain(&db.pool, batch, "setup").await;

    let failed = worker.tick.tick(&mut worker.client).await;
    assert!(!failed, "a refusal doesn't drop the session");
    let refused = worker.tick.refused();
    assert_eq!(refused.len(), 1, "{refused:?}");
    let step = &refused[0];
    assert_eq!(step.step, "retire_drained_segments", "{step:?}");
    assert_eq!(step.role.as_deref(), Some(worker.role.as_str()), "{step:?}");
    assert!(
        step.object
            .as_deref()
            .is_some_and(|o| o.starts_with("seg_")),
        "{step:?}"
    );
    assert_eq!(step.missing, vec!["TRUNCATE".to_string()], "{step:?}");
    let text = step.to_string();
    assert!(text.contains("lacks TRUNCATE"), "{text}");

    // Nothing is paused: the definition still reads live.
    let reported = status_of(&admin, "t").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert!(reported.capture_failure.is_none(), "{reported:?}");

    // The rest of the tick ran: a definition made meanwhile is started in
    // the same tick that was refused its retirement.
    admin
        .apply("TRANSFORM t2 FROM public.u SELECT a AS a")
        .await
        .expect("define t2");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {0}; \
         grant select, insert, update, delete on all tables in schema public to {0}",
        worker.role
    ))
    .await
    .expect("grants on t2's tables");
    assert!(!worker.tick.tick(&mut worker.client).await);
    assert_eq!(
        status_of(&admin, "t2").await.status,
        TransformStatus::Backfilling,
        "reconcile ran after the refused step, and started the build"
    );
    assert_eq!(worker.tick.refused().len(), 1, "still refused TRUNCATE");

    // After the grant the next pass succeeds and the report clears itself.
    raw.batch_execute(&format!(
        "grant truncate on all tables in schema trellis to {}",
        worker.role
    ))
    .await
    .expect("grant truncate");
    assert!(!worker.tick.tick(&mut worker.client).await);
    assert!(
        worker.tick.refused().is_empty(),
        "{:?}",
        worker.tick.refused()
    );
}

/// A definition created after the worker's grants, whose ledger and hidden
/// tables the role can't write, halts and pauses with the build's halt
/// reason naming the role and the table; a definition granted before keeps
/// running.
#[tokio::test]
async fn a_build_start_refused_halts_that_definition_only() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    let mut worker = dml_worker(db.dsn(), &raw).await;

    admin
        .apply("TRANSFORM t2 FROM public.u SELECT a AS a")
        .await
        .expect("define t2");
    assert!(!worker.tick.tick(&mut worker.client).await);

    let halted = status_of(&admin, "t2").await;
    assert_eq!(halted.status, TransformStatus::Paused);
    let failure = halted.capture_failure.expect("t2 reports why");
    assert_eq!(failure.kind, CaptureFailureKind::Halt, "{failure:?}");
    assert!(
        failure
            .error
            .contains("the re-derive build of public.t2 couldn't start"),
        "{failure:?}"
    );
    assert!(failure.error.contains("t2__ledger"), "{failure:?}");
    assert!(failure.error.contains(&worker.role), "{failure:?}");

    let kept = status_of(&admin, "t").await;
    assert_eq!(kept.status, TransformStatus::Live);
    assert!(kept.capture_failure.is_none(), "{kept:?}");
    // Tombstone collection reads the same ledger, so the ring step is
    // refused on it too: reported, and nothing paused for it.
    let refused = worker.tick.refused();
    assert!(
        refused
            .iter()
            .all(|step| step.object.as_deref() == Some("t2__ledger")),
        "{refused:?}"
    );
}

/// Strips `row_security = off` from every capture function of `public.u`,
/// as postgres: what a capture function an older release created looks like
/// to this one (#2227 added the setting), so the next pass finds it stale.
async fn age_capture_functions(raw: &Client) {
    raw.batch_execute(
        "do $$ declare f regprocedure; begin \
           for f in select p.oid::regprocedure from pg_catalog.pg_proc p \
             where p.pronamespace = 'trellis'::regnamespace and p.proname like 'cap\\_%\\_u\\_%' \
               and p.proconfig::text like '%row_security%' \
           loop execute format('alter function %s reset row_security', f); end loop; end $$",
    )
    .await
    .expect("age the capture functions");
}

/// #2291: after an upgrade, a DML-only worker can't replace the older
/// release's capture functions. A definition made after the upgrade, whose
/// columns those functions already image, still starts its build, and the
/// refused refresh is reported (T0255), pausing nothing. Once the source
/// owner runs a pass, the functions are current and the report clears.
#[tokio::test]
async fn a_stale_capture_function_the_worker_cannot_replace_does_not_hold_a_new_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    age_capture_functions(&raw).await;
    let worker = dml_worker(db.dsn(), &raw).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = worker;

    admin
        .apply("TRANSFORM t2 FROM public.u SELECT a AS a")
        .await
        .expect("define t2");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t2's tables");
    assert!(!tick.tick(&mut client).await);
    let started = status_of(&admin, "t2").await;
    assert_eq!(started.status, TransformStatus::Backfilling, "{started:?}");
    assert!(started.capture_failure.is_none(), "{started:?}");
    assert!(started.capture_wait.is_none(), "{started:?}");
    assert_eq!(status_of(&admin, "t").await.status, TransformStatus::Live);

    let refused = tick.refused();
    assert_eq!(refused.len(), 1, "{refused:?}");
    let step = &refused[0];
    assert_eq!(step.step, "refresh_capture_functions", "{step:?}");
    assert_eq!(step.role.as_deref(), Some(role.as_str()), "{step:?}");
    assert_eq!(step.object.as_deref(), Some("public.u"), "{step:?}");
    assert!(step.error.starts_with("T0255: "), "{step:?}");
    assert!(step.error.contains("permission denied"), "{step:?}");
    assert!(
        step.error.contains("as the role that owns the sources"),
        "{step:?}"
    );
    let stale = trellis::capture::install::installed(&raw, "trellis", "public.u")
        .await
        .expect("installed");
    assert!(
        matches!(
            stale,
            trellis::capture::install::Installed::Complete { current: false, .. }
        ),
        "{stale:?}"
    );

    // The source owner's pass re-creates the functions; the worker's next
    // pass finds them current and the report clears itself.
    pipeline::reconcile_pass(&mut raw, &db.pool).await;
    let current = trellis::capture::install::installed(&raw, "trellis", "public.u")
        .await
        .expect("installed");
    assert!(
        matches!(
            current,
            trellis::capture::install::Installed::Complete { current: true, .. }
        ),
        "{current:?}"
    );
    assert!(!tick.tick(&mut client).await);
    assert!(tick.refused().is_empty(), "{:?}", tick.refused());
}

/// An `ALTER TRANSFORM` field reading a column another definition already
/// reads, on a source whose functions an older release created: the field
/// is released and built on the functions as they are, as a new definition
/// starts on them, instead of waiting for a refresh the worker is refused.
#[tokio::test]
async fn a_stale_capture_function_the_worker_cannot_replace_does_not_hold_an_altered_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.u (id int primary key, a int, b int); \
         insert into public.u values (1, 1, 2)",
    )
    .await
    .expect("seed");
    let admin = definer(db.dsn()).await;
    for text in [
        "TRANSFORM t FROM public.u SELECT a AS a",
        "TRANSFORM tb FROM public.u SELECT b AS b",
    ] {
        admin.apply(text).await.expect("define");
    }
    bring_live(&mut raw, &db.pool, &["t", "tb"]).await;
    age_capture_functions(&raw).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = dml_worker(db.dsn(), &raw).await;
    admin
        .apply("ALTER TRANSFORM t ADD b AS b2")
        .await
        .expect("alter t");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t's tables");
    assert!(!tick.tick(&mut client).await);
    assert!(
        tick.refused()
            .iter()
            .any(|step| step.step == "refresh_capture_functions"),
        "{:?}",
        tick.refused()
    );
    pipeline::start_and_settle_builds(&db.pool).await;
    let awaiting = pipeline::rows(
        &raw,
        "select column_name from trellis.column_status where awaiting_capture",
    )
    .await;
    assert!(awaiting.is_empty(), "{awaiting:?}");
    assert_eq!(status_of(&admin, "t").await.status, TransformStatus::Live);
    assert_eq!(
        pipeline::rows(&raw, "select b2 from public.t").await,
        vec!["(2)".to_string()]
    );
}

/// Stale functions that lack a column a new definition reads need a widen
/// that adds capture, not a refresh: the definition still waits for the
/// source owner, as on a fresh source, and nothing reports a refresh.
#[tokio::test]
async fn a_stale_capture_function_missing_a_new_column_still_holds_its_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    age_capture_functions(&raw).await;
    raw.batch_execute("alter table public.u add column b int")
        .await
        .expect("a new column");
    let worker = dml_worker(db.dsn(), &raw).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = worker;
    admin
        .apply("TRANSFORM t2 FROM public.u SELECT b AS b")
        .await
        .expect("define t2");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t2's tables");
    assert!(!tick.tick(&mut client).await);
    let waiting = status_of(&admin, "t2").await;
    assert_eq!(waiting.status, TransformStatus::WaitingToBackfill);
    let failure = waiting.capture_failure.expect("t2 reports why it waits");
    assert!(failure.error.contains("permission denied"), "{failure:?}");
    assert!(
        tick.refused()
            .iter()
            .all(|step| step.step != "refresh_capture_functions"),
        "{:?}",
        tick.refused()
    );
}

/// A fresh deployment: a definition on a table nothing captures yet waits
/// for a role that owns it to install capture (docs/operations.md, "Grants
/// for a worker role"), and says so.
#[tokio::test]
async fn a_dml_only_worker_cannot_install_capture_on_a_new_source() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute("create table public.v (id int primary key, b int)")
        .await
        .expect("a second source");
    let worker = dml_worker(db.dsn(), &raw).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = worker;
    admin
        .apply("TRANSFORM t3 FROM public.v SELECT b AS b")
        .await
        .expect("define t3");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t3's tables");
    assert!(!tick.tick(&mut client).await);
    let waiting = status_of(&admin, "t3").await;
    assert_eq!(waiting.status, TransformStatus::WaitingToBackfill);
    let failure = waiting.capture_failure.expect("t3 reports why it waits");
    assert_eq!(failure.source_table, "public.v", "{failure:?}");
    assert!(failure.error.contains("permission denied"), "{failure:?}");
}

/// Each capture function of `table` (bare, in `public`), as `oid:xmin`:
/// whether a step re-created or touched it.
async fn function_versions(raw: &Client, table: &str) -> Vec<String> {
    pipeline::rows(
        raw,
        &format!(
            "select p.oid::text || ':' || p.xmin::text from pg_catalog.pg_proc p \
             where p.pronamespace = 'trellis'::regnamespace \
               and p.proname like 'cap\\_%\\_{table}\\_%' order by p.proname"
        ),
    )
    .await
}

/// How many of `table`'s capture functions run with `row_security = off`.
async fn row_security_off(raw: &Client, table: &str) -> usize {
    pipeline::rows(
        raw,
        &format!(
            "select 1 from pg_catalog.pg_proc p \
             where p.pronamespace = 'trellis'::regnamespace \
               and p.proname like 'cap\\_%\\_{table}\\_%' \
               and p.proconfig::text like '%row_security=off%'"
        ),
    )
    .await
    .len()
}

async fn installed(raw: &Client, table: &str) -> trellis::capture::install::Installed {
    trellis::capture::install::installed(raw, "trellis", table)
        .await
        .expect("installed")
}

fn is_current(installed: &trellis::capture::install::Installed) -> bool {
    matches!(
        installed,
        trellis::capture::install::Installed::Complete { current: true, .. }
    )
}

/// A `Trellis` handle connected as `role`.
async fn handle_as(dsn: &str, role: &str) -> Trellis {
    pipeline::definer(&dsn.replace("user=postgres", &format!("user={role}"))).await
}

/// ADR-0023: `migrate` as the source owner re-creates the out-of-date
/// capture functions (they get #2227's `row_security = off`) with no staging
/// worker, and leaves current ones alone.
#[tokio::test]
async fn migrate_as_the_source_owner_refreshes_out_of_date_capture_functions() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute("create table public.v (id int primary key, b int)")
        .await
        .expect("a second source");
    admin
        .apply("TRANSFORM tv FROM public.v SELECT b AS b")
        .await
        .expect("define tv");
    bring_live(&mut raw, &db.pool, &["t", "tv"]).await;
    age_capture_functions(&raw).await;
    assert!(!is_current(&installed(&raw, "public.u").await));
    assert_eq!(row_security_off(&raw, "u").await, 0);
    let untouched = function_versions(&raw, "v").await;
    assert!(!untouched.is_empty());

    let migrated = admin.migrate_with_report().await.expect("migrate");
    assert!(migrated.stale_capture.is_empty(), "{migrated:?}");
    assert!(is_current(&installed(&raw, "public.u").await));
    assert!(row_security_off(&raw, "u").await > 0);
    assert_eq!(function_versions(&raw, "v").await, untouched);

    // A second `migrate` finds nothing to do.
    let refreshed = function_versions(&raw, "u").await;
    admin.migrate().await.expect("migrate again");
    assert_eq!(function_versions(&raw, "u").await, refreshed);
    assert_eq!(function_versions(&raw, "v").await, untouched);
}

/// `migrate` as a role that doesn't own the sources can't re-create their
/// out-of-date capture functions: it succeeds, reporting the table with
/// T0255, and changes nothing.
#[tokio::test]
async fn migrate_as_a_dml_only_role_reports_the_out_of_date_capture_and_succeeds() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _admin = setup(db.dsn(), &mut raw, &db.pool).await;
    age_capture_functions(&raw).await;
    let stale = function_versions(&raw, "u").await;
    let worker = dml_worker(db.dsn(), &raw).await;
    let handle = handle_as(db.dsn(), &worker.role).await;

    let migrated = handle
        .migrate_with_report()
        .await
        .expect("migrate succeeds");
    assert_eq!(migrated.stale_capture.len(), 1, "{migrated:?}");
    let (table, message) = &migrated.stale_capture[0];
    assert_eq!(table, "public.u");
    assert!(message.starts_with("T0255: "), "{message}");
    assert!(message.contains("permission denied"), "{message}");
    assert!(message.contains("`trellis migrate`"), "{message}");
    assert_eq!(function_versions(&raw, "u").await, stale);
}

/// Beside a staging leader: `migrate` takes the table lock a widen takes,
/// so while another session holds it (a leader's install or widen, or a
/// writer), it waits at most its budget, changes nothing and reports the
/// table. Once the lock is free, the next `migrate` refreshes it.
#[tokio::test]
async fn migrate_leaves_a_table_whose_lock_is_held_and_reports_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    age_capture_functions(&raw).await;
    let stale = function_versions(&raw, "u").await;

    let mut holder = connect(db.dsn()).await;
    let held = holder.transaction().await.expect("begin");
    held.batch_execute("lock table public.u in share row exclusive mode")
        .await
        .expect("hold the lock a widen takes");
    let migrated = admin.migrate_with_report().await.expect("migrate");
    assert_eq!(migrated.stale_capture.len(), 1, "{migrated:?}");
    let (table, message) = &migrated.stale_capture[0];
    assert_eq!(table, "public.u");
    assert!(message.contains("lock wasn't free"), "{message}");
    assert_eq!(function_versions(&raw, "u").await, stale);
    held.rollback().await.expect("release");

    let migrated = admin.migrate_with_report().await.expect("migrate");
    assert!(migrated.stale_capture.is_empty(), "{migrated:?}");
    assert!(is_current(&installed(&raw, "public.u").await));
}

/// Out-of-date functions that image more than the readers need now (a
/// definition reading `b` was dropped, and no pass narrowed them): a
/// refresh to the narrower spec, for `migrate` as for the worker.
async fn setup_narrowed(dsn: &str, raw: &mut Client, pool: &trellis::pool::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.u (id int primary key, a int, b int); \
         insert into public.u values (1, 1, 2)",
    )
    .await
    .expect("seed");
    let admin = definer(dsn).await;
    for text in [
        "TRANSFORM t FROM public.u SELECT a AS a",
        "TRANSFORM tb FROM public.u SELECT b AS b",
    ] {
        admin.apply(text).await.expect("define");
    }
    bring_live(raw, pool, &["t", "tb"]).await;
    for text in ["PAUSE TRANSFORM tb", "DROP TRANSFORM tb"] {
        admin.apply(text).await.expect("drop tb");
    }
    age_capture_functions(raw).await;
    admin
}

fn imaged(installed: &trellis::capture::install::Installed) -> Vec<String> {
    match installed {
        trellis::capture::install::Installed::Complete { spec, .. } => spec.columns().to_vec(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn migrate_refreshes_out_of_date_capture_functions_to_a_narrower_spec() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup_narrowed(db.dsn(), &mut raw, &db.pool).await;
    let before = installed(&raw, "public.u").await;
    assert!(imaged(&before).contains(&"b".to_string()), "{before:?}");

    let migrated = admin.migrate_with_report().await.expect("migrate");
    assert!(migrated.stale_capture.is_empty(), "{migrated:?}");
    let after = installed(&raw, "public.u").await;
    assert!(is_current(&after), "{after:?}");
    assert!(!imaged(&after).contains(&"b".to_string()), "{after:?}");
    assert!(imaged(&after).contains(&"a".to_string()), "{after:?}");
}

#[tokio::test]
async fn a_narrower_refresh_the_worker_cannot_do_does_not_hold_a_new_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup_narrowed(db.dsn(), &mut raw, &db.pool).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = dml_worker(db.dsn(), &raw).await;
    admin
        .apply("TRANSFORM t2 FROM public.u SELECT a AS a")
        .await
        .expect("define t2");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t2's tables");
    assert!(!tick.tick(&mut client).await);
    let started = status_of(&admin, "t2").await;
    assert_eq!(started.status, TransformStatus::Backfilling, "{started:?}");
    let refused = tick.refused();
    assert!(
        refused
            .iter()
            .any(|step| step.step == "refresh_capture_functions"
                && step.object.as_deref() == Some("public.u")),
        "{refused:?}"
    );
}

/// #2296: a refresh the worker's role can't do is reported from the catalog
/// before the table lock: a writer holding the table in an open transaction
/// doesn't make the pass wait for it, and the new definition still starts.
#[tokio::test]
async fn a_refused_refresh_takes_no_lock_on_the_source() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let admin = setup(db.dsn(), &mut raw, &db.pool).await;
    age_capture_functions(&raw).await;
    let Worker {
        role,
        mut client,
        mut tick,
    } = dml_worker(db.dsn(), &raw).await;
    admin
        .apply("TRANSFORM t2 FROM public.u SELECT a AS a")
        .await
        .expect("define t2");
    raw.batch_execute(&format!(
        "grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema public to {role}"
    ))
    .await
    .expect("grants on t2's tables");

    let mut writer = connect(db.dsn()).await;
    let open = writer.transaction().await.expect("begin");
    open.batch_execute("lock table public.u in row exclusive mode")
        .await
        .expect("a writer's lock");
    assert!(!tick.tick(&mut client).await);
    let started = status_of(&admin, "t2").await;
    assert_eq!(started.status, TransformStatus::Backfilling, "{started:?}");
    assert!(started.capture_wait.is_none(), "{started:?}");
    assert!(
        tick.refused()
            .iter()
            .any(|step| step.step == "refresh_capture_functions"),
        "{:?}",
        tick.refused()
    );
    open.rollback().await.expect("release");
}
