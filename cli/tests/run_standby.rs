//! `trellis run` stands by like an embedded handle when `migrate` is refused
//! (ADR-0023, rule 3): the binary spawned under a role that can read and write
//! Trellis's tables but not alter them, against a database with migrations
//! pending.
//!
//! - it stays up and logs why, then starts its pipeline once a role that can
//!   migrate has run `migrate`, and a signal stops it cleanly
//!   (`run_stands_by_while_its_role_cannot_migrate_then_takes_over`)
//! - on a fresh database (no Trellis tables at all) it migrates before its
//!   staging worker reads the catalog, and runs
//!   (`run_migrates_a_fresh_database_before_it_stages`)
//!
//! The standby poll, and the wait after a refused `migrate`, are driven down
//! to 100 ms with `TRELLIS_TEST_STANDBY_POLL_MS` instead of waiting out 5 and
//! 60 seconds; the test waits on the process's log lines, not on a timer.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::{Config, Trellis, TrellisOptions};

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Puts the database back to before migration V9 (every later migration
/// undone too): the catalog a release before 0.2.0 left, so this binary's
/// migrations are pending.
async fn rewind_to_v8(raw: &Client) {
    raw.batch_execute(
        "alter table trellis.worker_registry drop column version, drop column staging, \
           drop column drain, drop column standby, drop column staging_pid; \
         drop table trellis.join1_fan_out; \
         drop table trellis.catalog_floor; \
         alter table trellis.transform_definitions drop column floor_version; \
         alter table trellis.capture_baselines drop column generated_stored, drop column expr_hash; \
         alter table trellis.partition_corrections drop constraint partition_corrections_cause_check; \
         drop table trellis.definition_keys; \
         update trellis.trellis_instance set instance_format_version = 1; \
         delete from trellis.refinery_schema_history where version > 8",
    )
    .await
    .expect("a database at V8");
}

async fn v9_ran(raw: &Client) -> bool {
    raw.query_one(
        "select exists (select 1 from trellis.refinery_schema_history where version = 9)",
        &[],
    )
    .await
    .expect("history")
    .get(0)
}

/// Waits for a line of the process's stderr containing `needle`; `Err` with
/// the lines read when it ends or `timeout` passes first.
fn wait_for_log(
    lines: &Receiver<String>,
    needle: &str,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    let mut seen = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) if line.contains(needle) => return Ok(line),
            Ok(line) => seen.push(line),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                return Err(format!("no `{needle}` in:\n{}", seen.join("\n")));
            }
        }
    }
}

fn stderr_lines(child: &mut Child) -> Receiver<String> {
    lines_of(child.stderr.take().expect("piped stderr"))
}

fn lines_of(stream: impl std::io::Read + Send + 'static) -> Receiver<String> {
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

#[tokio::test]
async fn run_stands_by_while_its_role_cannot_migrate_then_takes_over() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    rewind_to_v8(&raw).await;

    let role = format!("run_standby_{}", std::process::id());
    let grants = format!(
        "grant usage on schema trellis to {role}; \
         grant select, insert, update, delete on all tables in schema trellis to {role}; \
         grant usage, select on all sequences in schema trellis to {role}"
    );
    raw.batch_execute(&format!("create role {role} login; {grants}"))
        .await
        .expect("a role with DML rights only");
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let role_dsn = db.dsn().replace("user=postgres", &format!("user={role}"));

    let mut run = Command::new(env!("CARGO_BIN_EXE_trellis"))
        .args(["run", "--drain-threads", "1", "--database-url", &role_dsn])
        .env("TRELLIS_TEST_STANDBY_POLL_MS", "100")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn trellis run");
    let log = stderr_lines(&mut run);

    let timeout = Duration::from_secs(60);
    let standby = wait_for_log(&log, "standing by", timeout).unwrap_or_else(|e| panic!("{e}"));
    assert!(standby.contains("this role cannot run them"), "{standby}");
    assert!(
        run.try_wait().expect("try_wait").is_none(),
        "it stands by rather than exiting"
    );
    assert!(!v9_ran(&raw).await, "nothing migrated under the role");

    // A role that can migrate runs `migrate`; the process notices on a poll
    // and starts its pipeline.
    let admin = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect as the owner");
    admin.migrate().await.expect("migrate");
    admin.shutdown().await.expect("shutdown");
    assert!(v9_ran(&raw).await);
    // The tables the migrations created need the role's grants too.
    raw.batch_execute(&grants).await.expect("grants");

    let took_over = wait_for_log(&log, "taking over", timeout).unwrap_or_else(|e| panic!("{e}"));
    assert!(
        took_over.contains("no other Trellis version"),
        "{took_over}"
    );
    assert!(run.try_wait().expect("try_wait").is_none());

    let signalled = Command::new("kill")
        .args(["-INT", &run.id().to_string()])
        .status()
        .expect("kill");
    assert!(signalled.success());
    let status = run.wait().expect("wait");
    assert!(status.success(), "a signal stops it cleanly: {status}");
}

#[tokio::test]
async fn run_migrates_a_fresh_database_before_it_stages() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let raw = connect(db.dsn()).await;

    let mut run = Command::new(env!("CARGO_BIN_EXE_trellis"))
        .args(["run", "--drain-threads", "1", "--database-url", db.dsn()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn trellis run");
    let stdout = lines_of(run.stdout.take().expect("piped stdout"));
    let log = stderr_lines(&mut run);

    if let Err(e) = wait_for_log(&stdout, "trellis running", Duration::from_secs(60)) {
        let _ = run.kill();
        let stderr: Vec<String> = log.try_iter().collect();
        panic!("{e}\nstderr:\n{}", stderr.join("\n"));
    }
    assert!(run.try_wait().expect("try_wait").is_none());
    assert!(v9_ran(&raw).await, "migrated");
    // The pipeline connects once the handle's first pass has decided, so its
    // registry row is there, and active.
    let active: i64 = raw
        .query_one(
            "select count(*) from trellis.worker_registry where staging and not standby",
            &[],
        )
        .await
        .expect("registry")
        .get(0);
    assert_eq!(active, 1);

    let signalled = Command::new("kill")
        .args(["-INT", &run.id().to_string()])
        .status()
        .expect("kill");
    assert!(signalled.success());
    let status = run.wait().expect("wait");
    assert!(status.success(), "a signal stops it cleanly: {status}");
}
