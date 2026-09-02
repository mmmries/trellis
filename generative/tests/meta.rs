//! Meta-tests for the harness itself (issue #4, design doc §6): a green
//! generative run must prove it ran. These are deliberately not about any
//! generated program or oracle comparison — they check that the harness's
//! own plumbing (standing up a cluster, classifying a backend that can't
//! stand up) behaves, independent of any classifier logic under test.

use generative::backend::ManualBackend;
use generative::run::{Outcome, classify_stand_up};
use testkit::TestCluster;

/// Fails fast if the harness cannot stand up a `testkit` cluster it should
/// have been able to — a sanity check on the harness, not on any property.
/// Deliberately independent of [`Outcome`]/`classify_stand_up`: this only
/// proves the cluster itself comes up and accepts a trivial query.
#[tokio::test(flavor = "multi_thread")]
async fn the_harness_can_stand_up_a_testkit_cluster() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let row = db
        .pool
        .get()
        .await
        .expect("pool connection")
        .query_one("select 1", &[])
        .await
        .expect("trivial query against a freshly-provisioned cluster");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

/// The other half of design doc §6: a backend that is provisioned (a real,
/// healthy `testkit` cluster) but cannot stand up must be reported as
/// `BackendUnusable`, never silently skipped or treated as green. Forces the
/// failure by connecting to a database name the cluster never created,
/// which fails at the raw `tokio_postgres::connect` step inside
/// `ManualBackend::connect`.
#[tokio::test(flavor = "multi_thread")]
async fn a_backend_that_cannot_stand_up_is_reported_unusable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let broken_dsn = db.dsn().replace(db.name(), "does_not_exist_at_all");
    assert_ne!(
        broken_dsn,
        db.dsn(),
        "test setup bug: db.dsn() must contain db.name() for this corruption to take effect"
    );

    let outcome = match classify_stand_up(ManualBackend::connect(broken_dsn).await) {
        Ok(_) => panic!("a backend pointed at a nonexistent database must not stand up"),
        Err(outcome) => outcome,
    };

    match &outcome {
        Outcome::BackendUnusable { error } => {
            assert!(
                !error.is_empty(),
                "BackendUnusable must carry the engine's error text"
            );
        }
        other => panic!("expected BackendUnusable, got {other}"),
    }
    assert!(
        !outcome.as_pass(),
        "BackendUnusable must never be a pass: {outcome}"
    );
}
