//! `trellis migrate`, spawned against an empty database on a
//! [`TestCluster`]: it creates Trellis's schema, says so, and a second run
//! changes nothing.

use std::process::Command;

use testkit::TestCluster;

#[tokio::test]
async fn migrate_creates_the_schema_and_is_idempotent() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    for _ in 0..2 {
        let output = Command::new(env!("CARGO_BIN_EXE_trellis"))
            .args(["migrate", "--database-url", db.dsn()])
            .output()
            .expect("spawn trellis");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout), "migrated\n");
    }
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    let history: bool = client
        .query_one(
            "select pg_catalog.to_regclass('trellis.refinery_schema_history') is not null",
            &[],
        )
        .await
        .expect("query")
        .get(0);
    assert!(history);
}
