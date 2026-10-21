//! PROTOTYPE / MEASUREMENT for #1226 (not part of the shipped suite).
//! Prints per-phase wall times of one fixture. Run:
//!   cargo test -p testkit --test prototype_1226_phases -- --ignored --nocapture
use std::process::Command;
use std::time::Instant;
use testkit::TestCluster;

#[tokio::test]
#[ignore]
async fn phases() {
    let n: usize = std::env::var("N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    for _ in 0..n {
        // initdb alone, same flags as testkit
        let d = std::env::temp_dir().join(format!("p1226-initdb-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let t = Instant::now();
        let ok = Command::new("initdb")
            .args(["-D"])
            .arg(&d)
            .args([
                "-U",
                "postgres",
                "--auth=trust",
                "--no-sync",
                "--encoding=UTF8",
                "--locale=C.UTF-8",
                "--locale-provider=icu",
                "--icu-locale=en-US",
            ])
            .output()
            .unwrap()
            .status
            .success();
        assert!(ok);
        let initdb = t.elapsed();
        let _ = std::fs::remove_dir_all(&d);

        let t = Instant::now();
        let c = TestCluster::start();
        let start_total = t.elapsed();
        let t = Instant::now();
        let db = c.create_empty_database().await;
        let createdb = t.elapsed();
        let t = Instant::now();
        let db2 = c.create_isolated_database().await;
        let createdb_migrate = t.elapsed();
        let t = Instant::now();
        drop(db);
        drop(db2);
        let dropdb = t.elapsed();
        let t = Instant::now();
        drop(c);
        let teardown = t.elapsed();
        println!(
            "PHASES initdb_ms={} start_total_ms={} createdb_ms={} createdb+migrate_ms={} dropdb2_ms={} teardown_ms={}",
            initdb.as_millis(),
            start_total.as_millis(),
            createdb.as_millis(),
            createdb_migrate.as_millis(),
            dropdb.as_millis(),
            teardown.as_millis()
        );
    }
}
