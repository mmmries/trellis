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
