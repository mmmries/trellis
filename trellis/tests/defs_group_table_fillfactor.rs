//! Issue #779: an aggregate's group table is created with a `fillfactor` below
//! 100, so the update-only churn of paged drains can be HOT and pruned in-page
//! instead of bloating the table. Only the group table: never the user's
//! source, a 1-1 target, or the ledger and group-delta tables Trellis keeps
//! beside the aggregate target.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{ValueType, install_definition};

#[tokio::test]
async fn only_an_aggregate_group_table_is_created_with_the_group_fillfactor() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let (raw, conn) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    raw.batch_execute(
        "create table public.src (id bigint primary key, g bigint not null, v numeric)",
    )
    .await
    .expect("create source");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Numeric),
        ("g".to_string(), ValueType::Numeric),
        ("v".to_string(), ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM totals FROM public.src GROUP BY g SELECT g AS g, SUM(v) AS v",
        &columns,
        "public",
    )
    .await
    .expect("install aggregate");
    install_definition(
        &db.pool,
        "TRANSFORM doubled FROM public.src SELECT v + v AS v2",
        &columns,
        "public",
    )
    .await
    .expect("install 1-1");

    // Every table the two defines created, with its reloptions.
    let rows = raw
        .query(
            "select c.relname::text, coalesce(c.reloptions::text, '') from pg_class c \
             where c.relnamespace = 'public'::regnamespace and c.relkind = 'r' \
             order by 1",
            &[],
        )
        .await
        .expect("reloptions");
    let options: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    let of = |table: &str| {
        options
            .iter()
            .find(|(name, _)| name == table)
            .unwrap_or_else(|| panic!("no table {table} in {options:?}"))
            .1
            .clone()
    };
    assert_eq!(of("totals"), "{fillfactor=20}", "the group table");
    assert_eq!(of("totals__ledger"), "", "the aggregate's ledger");
    assert_eq!(of("doubled"), "", "a 1-1 target");
    assert_eq!(of("src"), "", "the user's source");
    let with_options: Vec<&str> = options
        .iter()
        .filter(|(_, o)| !o.is_empty())
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(
        with_options,
        ["totals"],
        "only the group table: {options:?}"
    );
}
