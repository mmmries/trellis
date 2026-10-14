//! Issue #779: an aggregate's group table is created with a `fillfactor` below
//! 100, so the update-only churn of paged drains can be HOT and pruned in-page
//! instead of bloating the table. Only the group table, never the source.

use std::collections::HashMap;

use testkit::TestCluster;
use trellis::defs::{ValueType, install_definition};

#[tokio::test]
async fn aggregate_group_table_is_created_with_the_group_fillfactor() {
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
    install_definition(
        &db.pool,
        "TRANSFORM totals FROM public.src GROUP BY g SELECT g AS g, SUM(v) AS v",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("g".to_string(), ValueType::Numeric),
            ("v".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install aggregate");

    let options = |table: &'static str| {
        let raw = &raw;
        async move {
            raw.query_one(
                "select coalesce(reloptions::text, '') from pg_class \
                 where oid = ('public.' || $1)::regclass",
                &[&table],
            )
            .await
            .expect("reloptions")
            .get::<_, String>(0)
        }
    };
    assert_eq!(options("totals").await, "{fillfactor=20}");
    assert_eq!(options("src").await, "", "the user's source is untouched");
}
