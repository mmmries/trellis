//! `COUNT(DISTINCT x)` and the other aggregate-call modifiers get a named
//! refusal at define time, not a reserved-word error about a column (#1739).

use std::collections::HashMap;

use testkit::{TestCluster, connect_with};
use trellis::defs::{CatalogError, ParseError, ValueType, install_definition};
use trellis::integer::IntWidth;

#[tokio::test]
async fn count_distinct_is_refused_by_name_and_points_at_the_two_transform_recipe() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_with(db.dsn(), "").await;
    client
        .batch_execute("create table public.cd_src (id integer primary key, g integer, x integer)")
        .await
        .expect("create cd_src");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
    ]);

    for call in [
        "COUNT(DISTINCT x)",
        "COUNT(ALL x)",
        "SUM(DISTINCT x)",
        "COUNT(x ORDER BY id)",
        "COUNT(*) FILTER (WHERE x > 1)",
    ] {
        let text = format!(
            "TRANSFORM cd_out FROM cd_src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, {call} AS n"
        );
        let err = install_definition(&db.pool, &text, &columns, "public")
            .await
            .expect_err(call);
        assert!(
            matches!(
                err,
                CatalogError::Parse(ParseError::UnsupportedAggregateModifier { .. })
            ),
            "{call}: {err:?}"
        );
        assert!(err.to_string().starts_with("T0233"), "{call}: {err}");
        assert!(!err.to_string().contains("reserved word"), "{call}: {err}");
    }

    // The documented recipe (docs/transforms.md, "Counting distinct values")
    // defines.
    install_definition(
        &db.pool,
        "TRANSFORM cd_gx FROM cd_src GROUP BY g, x WITHOUT SOURCE INDEX \
         SELECT g AS g, x AS x, COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("the first transform of the recipe");
    trellis::intake::harness::start_registered_builds(&db.pool)
        .await
        .expect("dispatch the build");
    trellis::staging::build::settle_builds(&db.pool).await;
    let target = HashMap::from([
        ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("x".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("n".to_string(), ValueType::Integer(IntWidth::Int8)),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM cd_distinct FROM cd_gx GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, COUNT(x) AS distinct_x",
        &target,
        "public",
    )
    .await
    .expect("the second transform of the recipe");
}
