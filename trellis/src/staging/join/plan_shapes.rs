//! The plans of a `JOIN` build's statements over every cell or group
//! (#1876), read by `EXPLAIN` in the build's own transaction after
//! [`build`] ran, so its working tables hold the build's rows and
//! statistics.

use std::collections::HashMap;

use super::*;
use crate::integer::IntWidth;
use crate::pool::Pool;

async fn explain(txn: &Transaction<'_>, sql: &str) -> String {
    (txn.query(&format!("explain (costs off) {sql}"), &[]).await)
        .expect("explain")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The part of `plan` from the first line holding `from` to the next
/// holding `to` (or the end).
fn section<'a>(plan: &'a str, from: &str, to: &str) -> &'a str {
    let start = plan
        .find(from)
        .unwrap_or_else(|| panic!("no {from}:\n{plan}"));
    let rest = &plan[start..];
    rest.find(to).map_or(rest, |end| &rest[..end])
}

#[tokio::test]
async fn a_join_builds_statements_over_every_cell_keep_their_plans() {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let config = crate::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
    let pool = Pool::new(&config).expect("pool");
    let mut client = pool.get().await.expect("client");
    // Enough posts that `J` is too big to scan for a few keys, as at scale.
    client
        .batch_execute(
            "create table public.posts (id bigint primary key, author integer, words integer); \
             insert into public.posts select g, g % 3000, g % 50 from generate_series(1, 150000) g; \
             create table public.post_tags (post bigint, tag text, primary key (post, tag)); \
             insert into public.post_tags select p, 'tag' || ((p * 7 + s) % 20) \
                 from generate_series(1, 30000) p, generate_series(1, 2) s; \
             analyze public.posts, public.post_tags",
        )
        .await
        .expect("seed");
    let columns = HashMap::from([
        ("post".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("tag".to_string(), ValueType::Text),
    ]);
    let definition = crate::defs::install_definition(
        &pool,
        "TRANSFORM totals FROM post_tags JOIN posts ON posts.id = post_tags.post \
         GROUP BY post_tags.tag, posts.author SELECT COUNT(*) AS n, SUM(posts.words) AS w",
        &columns,
        "public",
    )
    .await
    .expect("define");
    let txn = tokio_postgres::Client::transaction(&mut client)
        .await
        .expect("begin");
    build(&txn, pool.schema(), &definition, &BuildWork::Single)
        .await
        .expect("build");
    let mut plan = plan_for(&txn, pool.schema(), &definition)
        .await
        .expect("plan");
    for side in [&mut plan.from, &mut plan.joined] {
        side.recompute = true;
        side.node = Some(side.plan.source_row().to_string());
    }
    let id = definition.id;
    txn.batch_execute(group_basis::BULK_PLAN_SETTINGS)
        .await
        .expect("settings");

    // `P`'s new cells are inserted in cell order.
    let side = &plan.from;
    let [_, write] = (side.plan).join_build_sql(&side.grouped, &side.matched, &side.deltas);
    let cells = explain(&txn, &write).await;
    let insert = section(
        &cells,
        &format!("Insert on {}", HiddenTable::FromPartials.name(id)),
        "CTE d",
    );
    assert!(insert.contains("Sort Key: r_1.post, r_1.tag"), "{cells}");

    txn.rollback().await.expect("rollback");
}
