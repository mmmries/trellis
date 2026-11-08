//! A 0.1.0 definition's `numeric(p,s)` `SUM`/`AVG` on upgrade (#1120 rule 5,
//! `trellis::defs::numeric_sums`): its hidden table held the sum alone,
//! possibly `NaN` for good after a `NaN` came and went. The attach gives it
//! the scale map and the special-value counts in place, from each group's
//! count and its stored sum's scale, re-stamps the definition's floor, and
//! rebuilds a definition holding a `NaN` sum. The 0.1.0 layout is made here
//! by hand from a definition this release wrote: the columns this release
//! added dropped, and the zero flag as 0.1.0 computed it.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::config::DEFAULT_SCHEMA;
use trellis::{Config, TransformStatus, Trellis, TrellisOptions};

async fn raw(db: &testkit::TestDatabase) -> Client {
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("search_path");
    client
}

async fn running(db: &testkit::TestDatabase) -> Trellis {
    Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect a running handle")
}

async fn wait_for(trellis: &Trellis, targets: &[&str], status: TransformStatus) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    for target in targets {
        loop {
            let now = trellis
                .status(target)
                .await
                .expect("status")
                .expect("defined")
                .status;
            if now == status {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{target} never reached {status:?}: {now:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
}

async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("token");
    trellis
        .await_converged(token, std::time::Duration::from_secs(60))
        .await
        .expect("converged");
}

async fn self_check_clean(trellis: &Trellis, targets: &[&str]) {
    use trellis::staging::self_check::CheckPageExt;
    for target in targets {
        let report = trellis
            .self_check_to_end(
                target,
                trellis::SelfCheckMode::Strict,
                std::time::Duration::from_secs(30),
            )
            .await
            .unwrap_or_else(|err| panic!("self_check {target}: {err}"));
        assert_eq!(
            report.outcome,
            trellis::SelfCheckOutcome::Converged,
            "{target}"
        );
    }
}

/// The rows of `actual` and `expected` (text) that the other lacks.
async fn difference(raw: &Client, actual: &str, expected: &str) -> Vec<String> {
    raw.query(
        &format!(
            "select format('%s %s', side, r) from ( \
                 (select 'target' as side, r from (({actual}) except all ({expected})) x(r)) \
                 union all \
                 (select 'postgres', r from (({expected}) except all ({actual})) y(r))) z \
             order by 1"
        ),
        &[],
    )
    .await
    .expect("compare")
    .iter()
    .map(|row| row.get(0))
    .collect()
}

async fn id_of(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where target_table = $1",
        &[&format!("public.{target}")],
    )
    .await
    .expect("id")
    .get(0)
}

/// Turns the hidden table `table`, over one `numeric(p,s)` sum, back into
/// 0.1.0's: no scale map or special-value counts, and the zero flag (and its
/// index) over the count, the argument's count and its sum.
async fn table_to_0_1_0(raw: &Client, table: &str) {
    let index: String = raw
        .query_one(
            "select pg_get_indexdef(indexrelid) from pg_index \
             where indrelid = to_regclass($1) and indpred is not null",
            &[&table],
        )
        .await
        .expect("the zero-group index")
        .get(0);
    raw.batch_execute(&format!(
        "alter table {table} drop column __zero, drop column __scales_1, \
             drop column __nan_1, drop column __pinf_1, drop column __ninf_1; \
         alter table {table} add column __zero boolean not null generated always as \
             (__n = 0 and __count_1 = 0 and __sum_1 = 0) stored; \
         {index}"
    ))
    .await
    .expect("the 0.1.0 layout");
}

/// Definition `id` as 0.1.0 stored it: its hidden `tables` in 0.1.0's
/// layout ([`table_to_0_1_0`]), and 0.1.0's floor.
async fn to_0_1_0(raw: &Client, id: i64, tables: &[String]) {
    for table in tables {
        table_to_0_1_0(raw, table).await;
    }
    raw.execute(
        "update transform_definitions set floor_version = '0.1.0' where id = $1",
        &[&id],
    )
    .await
    .expect("the 0.1.0 floor");
}

const EXACT: &str = "select format('%s %s %s %s', g, t, a, c) from public.{}";
const POSTGRES: &str =
    "select format('%s %s %s %s', g, sum(v), avg(v), count(v)) from public.{} group by g";

fn exact(target: &str, source: &str) -> (String, String) {
    if target == "jn" {
        return (
            "select format('%s %s %s %s', r, t, a, c) from public.jn".to_string(),
            "select format('%s %s %s %s', d.r, sum(s.v), avg(s.v), count(s.v)) \
             from public.s join public.d on s.g = d.g group by d.r"
                .to_string(),
        );
    }
    (EXACT.replace("{}", target), POSTGRES.replace("{}", source))
}

/// Every target of the test, with its source.
const TARGETS: [(&str, &str); 3] = [("fin", "s"), ("poi", "p"), ("jn", "s")];

/// A finite 0.1.0 sum gains `{s: count}` and no special values, and stays
/// exact; a 0.1.0 sum stuck at `NaN` after its `NaN` left is rebuilt from
/// its source. Both are stamped with the new layout's floor, and their
/// pages after the upgrade follow Postgres's text, `NaN` in and out.
#[tokio::test]
async fn a_0_1_0_numeric_sum_is_converted_in_place_and_a_nan_one_rebuilt() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, g integer, v numeric(10,2)); \
         create table public.p (id integer primary key, g integer, v numeric(10,2)); \
         insert into public.s values (1, 1, 1.25), (2, 1, 2.50), (3, 2, -4), (4, 3, null); \
         insert into public.p values (1, 1, 1.25), (2, 2, 3); \
         create table public.d (id integer primary key, g integer, r text); \
         insert into public.d values (1, 1, 'a'), (2, 2, 'a'), (3, 1, 'b')",
    )
    .await
    .expect("sources");
    let trellis = running(&db).await;
    for (target, source) in [("fin", "s"), ("poi", "p")] {
        trellis
            .apply(&format!(
                "TRANSFORM {target} FROM public.{source} GROUP BY g WITHOUT SOURCE INDEX \
                 SELECT g AS g, SUM(v) AS t, AVG(v) AS a, COUNT(v) AS c"
            ))
            .await
            .expect("define");
    }
    trellis
        .apply(
            "TRANSFORM jn FROM public.s JOIN public.d ON s.g = d.g GROUP BY d.r \
             WITHOUT SOURCE INDEX SELECT d.r AS r, SUM(s.v) AS t, AVG(s.v) AS a, \
             COUNT(s.v) AS c",
        )
        .await
        .expect("define the JOIN");
    wait_for(&trellis, &["fin", "poi", "jn"], TransformStatus::Live).await;
    converge(&trellis).await;
    trellis.shutdown().await.expect("shutdown");

    let (fin, poi, jn) = (
        id_of(&raw, "fin").await,
        id_of(&raw, "poi").await,
        id_of(&raw, "jn").await,
    );
    to_0_1_0(&raw, fin, &[format!("{DEFAULT_SCHEMA}.agg_{fin}")]).await;
    to_0_1_0(&raw, poi, &[format!("{DEFAULT_SCHEMA}.agg_{poi}")]).await;
    // A `JOIN`'s `P` and `T` carry the sum; its `M` has no argument.
    to_0_1_0(
        &raw,
        jn,
        &["p", "t"].map(|t| format!("{DEFAULT_SCHEMA}.join_{jn}_{t}")),
    )
    .await;
    // 0.1.0's `NaN` bug: a `NaN` in group 2 came and went, and left the
    // stored sum, and the target's, at `NaN`.
    raw.batch_execute(&format!(
        "update {DEFAULT_SCHEMA}.agg_{poi} set __sum_1 = 'NaN' where g = 2; \
         update public.poi set t = 'NaN', a = 'NaN' where g = 2"
    ))
    .await
    .expect("a NaN sum");

    // The upgrade's attach, as a release starting over 0.1.0's catalog runs
    // it, before any worker reads a definition.
    let upgrade = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    upgrade.migrate().await.expect("the upgrade's attach");
    upgrade.shutdown().await.expect("shutdown");
    let trellis = running(&db).await;
    let maps: Vec<String> = raw
        .query(
            &format!(
                "select format('%s %s %s %s %s %s', g, __scales_1, __nan_1, __pinf_1, \
                        __ninf_1, __zero) \
                 from {DEFAULT_SCHEMA}.agg_{fin} order by g"
            ),
            &[],
        )
        .await
        .expect("the converted partials")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        maps,
        [
            "1 {\"2\": 2} 0 0 0 f",
            "2 {\"2\": 1} 0 0 0 f",
            "3 {} 0 0 0 f"
        ]
    );
    let joined: Vec<String> = raw
        .query(
            &format!(
                "select format('%s %s', r, __scales_1) from {DEFAULT_SCHEMA}.join_{jn}_t \
                 order by r"
            ),
            &[],
        )
        .await
        .expect("the converted JOIN partials")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(joined, ["a {\"2\": 3}", "b {\"2\": 2}"]);
    for target in ["fin", "poi", "jn"] {
        let floor: String = raw
            .query_one(
                "select floor_version from transform_definitions where target_table = $1",
                &[&format!("public.{target}")],
            )
            .await
            .expect("floor")
            .get(0);
        assert_eq!(floor, "0.2.0", "{target}");
    }
    wait_for(&trellis, &["fin", "poi", "jn"], TransformStatus::Live).await;
    converge(&trellis).await;
    for (target, source) in TARGETS {
        let (actual, expected) = exact(target, source);
        assert_eq!(
            difference(&raw, &actual, &expected).await,
            Vec::<String>::new(),
            "{target} after the upgrade"
        );
    }
    self_check_clean(&trellis, &["fin", "poi", "jn"]).await;

    for step in [
        "insert into public.s values (5, 1, 'NaN'), (6, 2, 0.01); \
         insert into public.p values (3, 2, 'NaN')",
        "delete from public.s where id = 5; update public.p set v = 7.5 where id = 3",
        "delete from public.s where id in (1, 2); delete from public.p where g = 2",
    ] {
        raw.batch_execute(step).await.expect(step);
        converge(&trellis).await;
        for (target, source) in TARGETS {
            let (actual, expected) = exact(target, source);
            assert_eq!(
                difference(&raw, &actual, &expected).await,
                Vec::<String>::new(),
                "{target} after {step}"
            );
        }
    }
    self_check_clean(&trellis, &["fin", "poi", "jn"]).await;
    trellis.shutdown().await.expect("shutdown");
}
