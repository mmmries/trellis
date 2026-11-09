//! A 0.1.0 definition's `numeric(p,s)` `SUM`/`AVG` on upgrade (#1120 rule 5,
//! `trellis::defs::numeric_sums`): its hidden table held the sum alone,
//! possibly `NaN` for good after a `NaN` came and went. The attach gives it
//! an empty map of exceptions in place, under its column's scale as the
//! reference, re-stamps the definition's floor, and
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
/// 0.1.0's: no map of exceptions, and the zero flag (and its index) over
/// the count, the argument's count and its sum.
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
        "alter table {table} drop column __zero, drop column __scales_1; \
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

/// A finite 0.1.0 sum gains an empty (`NULL`) map of exceptions under its
/// column's scale as the reference, and stays exact; a 0.1.0 sum stuck at `NaN` after its `NaN` left is rebuilt from
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
                "select format('%s %s %s %s', g, coalesce(__scales_1::text, 'null'), __zero, \
                        col_description(attrelid, attnum)) \
                 from {DEFAULT_SCHEMA}.agg_{fin}, pg_attribute \
                 where attrelid = '{DEFAULT_SCHEMA}.agg_{fin}'::regclass \
                   and attname = '__scales_1' order by g"
            ),
            &[],
        )
        .await
        .expect("the converted partials")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(maps, ["1 null f 2", "2 null f 2", "3 null f 2"]);
    let joined: Vec<String> = raw
        .query(
            &format!(
                "select format('%s %s', r, coalesce(__scales_1::text, 'null')) \
                 from {DEFAULT_SCHEMA}.join_{jn}_t order by r"
            ),
            &[],
        )
        .await
        .expect("the converted JOIN partials")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(joined, ["a null", "b null"]);
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

/// Gives the hidden `table` 0.1.1's encoding of a `NaN` before it takes
/// 0.1.0's layout ([`table_to_0_1_0`]): 0.1.1 counted each `NaN` its sum
/// summed by adding `10^2100` to `__sum_1` (a `JOIN` product times its
/// matches), where this release counts it in the map of exceptions.
async fn table_to_0_1_1(raw: &Client, table: &str) {
    raw.batch_execute(&format!(
        "update {table} set __sum_1 = __sum_1 + 1e2100 * (__scales_1->>'NaN')::numeric \
         where __scales_1 ? 'NaN'"
    ))
    .await
    .expect("0.1.1's NaN weight");
    table_to_0_1_0(raw, table).await;
}

/// A 0.1.1 definition's `numeric(p,s)` sum holding a `NaN` (#1827): 0.1.1
/// added `10^2100` per `NaN` to the stored sum and read it as `NaN` from
/// half of that. This release counts a `NaN` in the map of exceptions
/// instead, so the attach rebuilds such a definition as it does a 0.1.0
/// one stuck at `NaN`; left as stored, the sum would read as a 2,100-digit
/// number once its group changed, and stay off by `10^2100` after the
/// `NaN` left. Every page after the upgrade follows Postgres's text.
#[tokio::test]
async fn a_0_1_1_nan_weighted_numeric_sum_is_rebuilt() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.w (id integer primary key, g integer, v numeric(10,2)); \
         insert into public.w values (1, 1, 1.25), (2, 1, 'NaN'), (3, 2, 2.50), \
             (4, 3, 3.75), (5, 3, 'NaN'), (6, 3, 'NaN'), (7, 4, null); \
         create table public.d (id integer primary key, g integer, r text); \
         insert into public.d values (1, 1, 'a'), (2, 3, 'a'), (3, 3, 'b'), (4, 2, 'b')",
    )
    .await
    .expect("sources");
    let trellis = running(&db).await;
    trellis
        .apply(
            "TRANSFORM wt FROM public.w GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(v) AS t, AVG(v) AS a, COUNT(v) AS c",
        )
        .await
        .expect("define");
    trellis
        .apply(
            "TRANSFORM wj FROM public.w JOIN public.d ON w.g = d.g GROUP BY d.r \
             WITHOUT SOURCE INDEX SELECT d.r AS r, SUM(w.v) AS t, AVG(w.v) AS a, \
             COUNT(w.v) AS c",
        )
        .await
        .expect("define the JOIN");
    wait_for(&trellis, &["wt", "wj"], TransformStatus::Live).await;
    converge(&trellis).await;
    trellis.shutdown().await.expect("shutdown");

    let (wt, wj) = (id_of(&raw, "wt").await, id_of(&raw, "wj").await);
    let tables = [
        format!("{DEFAULT_SCHEMA}.agg_{wt}"),
        format!("{DEFAULT_SCHEMA}.join_{wj}_p"),
        format!("{DEFAULT_SCHEMA}.join_{wj}_t"),
    ];
    for table in &tables {
        table_to_0_1_1(&raw, table).await;
    }
    for id in [wt, wj] {
        raw.execute(
            "update transform_definitions set floor_version = '0.1.0' where id = $1",
            &[&id],
        )
        .await
        .expect("the 0.1.x floor");
    }
    // The fixture is 0.1.1's: group 3's sum is `3.75 + 2 × 10^2100`.
    let weighted: String = raw
        .query_one(
            &format!("select __sum_1::text from {DEFAULT_SCHEMA}.agg_{wt} where g = 3"),
            &[],
        )
        .await
        .expect("the weighted sum")
        .get(0);
    assert!(
        weighted.starts_with('2') && weighted.ends_with("3.75") && weighted.len() > 2100,
        "{}",
        &weighted[..20]
    );

    let upgrade = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    upgrade.migrate().await.expect("the upgrade's attach");
    upgrade.shutdown().await.expect("shutdown");
    let trellis = running(&db).await;
    wait_for(&trellis, &["wt", "wj"], TransformStatus::Live).await;
    converge(&trellis).await;

    let pages = [
        (
            "select format('%s %s %s %s', g, t, a, c) from public.wt",
            "select format('%s %s %s %s', g, sum(v), avg(v), count(v)) from public.w group by g",
        ),
        (
            "select format('%s %s %s %s', r, t, a, c) from public.wj",
            "select format('%s %s %s %s', d.r, sum(w.v), avg(w.v), count(w.v)) \
             from public.w join public.d on w.g = d.g group by d.r",
        ),
    ];
    let check = async |when: &str| {
        for (actual, expected) in pages {
            assert_eq!(
                difference(&raw, actual, expected).await,
                Vec::<String>::new(),
                "{actual} {when}"
            );
        }
    };
    check("after the upgrade").await;
    self_check_clean(&trellis, &["wt", "wj"]).await;
    for step in [
        "delete from public.w where id = 2",
        "delete from public.w where id = 5",
        "update public.w set v = 1.00 where id = 6",
        "insert into public.w values (8, 2, 'NaN')",
        "delete from public.w where id = 8; update public.w set v = 0.05 where id = 3",
    ] {
        raw.batch_execute(step).await.expect(step);
        converge(&trellis).await;
        check(step).await;
    }
    self_check_clean(&trellis, &["wt", "wj"]).await;
    trellis.shutdown().await.expect("shutdown");
}

/// Turns every `numeric(p,s)` sum of the hidden `table` back into 0.1.0's
/// layout and encoding: a sum counting a `NaN` is `NaN` itself, the map of
/// exceptions is gone, and the zero flag reads the count and the sum of
/// each argument.
async fn table_to_0_1_0_encoded(raw: &Client, table: &str) {
    let args: Vec<i32> = raw
        .query(
            "select substr(attname, 10)::int from pg_attribute \
             where attrelid = to_regclass($1) and attname like '\\_\\_scales\\_%' \
               and not attisdropped order by 1",
            &[&table],
        )
        .await
        .expect("the arguments")
        .iter()
        .map(|row| row.get(0))
        .collect();
    let index: String = raw
        .query_one(
            "select pg_get_indexdef(indexrelid) from pg_index \
             where indrelid = to_regclass($1) and indpred is not null",
            &[&table],
        )
        .await
        .expect("the zero-group index")
        .get(0);
    let mut sql = Vec::new();
    for i in &args {
        sql.push(format!(
            "update {table} set __sum_{i} = 'NaN' where __scales_{i} ? 'NaN'"
        ));
    }
    let drops: Vec<String> = args
        .iter()
        .map(|i| format!("drop column __scales_{i}"))
        .collect();
    let zero: Vec<String> = args
        .iter()
        .map(|i| format!("__count_{i} = 0 and __sum_{i} = 0"))
        .collect();
    sql.push(format!(
        "alter table {table} drop column __zero, {}",
        drops.join(", ")
    ));
    sql.push(format!(
        "alter table {table} add column __zero boolean not null generated always as \
             (__n = 0 and {}) stored",
        zero.join(" and ")
    ));
    sql.push(index);
    raw.batch_execute(&sql.join(";\n"))
        .await
        .expect("the 0.1.0 layout");
}

/// A 0.1.0 `JOIN` aggregate stuck at `NaN` (#1830): 0.1.0 left a `JOIN`
/// group's sum at `NaN` for good once a `NaN` came and went, in the `P` or
/// `M` cell that summed it and in `T`. The takeover's rebuild re-derives
/// `P` and `M`, but carries each cell's change into `T` as a difference,
/// which a stored `NaN` in `T` absorbs. So the attach rebuilds such a
/// definition as a first build would: its hidden tables afresh, then every
/// group from its source. Covered: `INNER` and `LEFT`, a stuck sum on each
/// side (`P`'s argument and `M`'s, at different scales), a group whose `NaN`
/// row remains, `AVG`, and a plain aggregate and a `JOIN` holding no `NaN`
/// alongside, which the attach converts in place and leaves `live`.
#[tokio::test]
async fn a_0_1_0_join_stuck_at_nan_is_rebuilt_afresh() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, g integer, v numeric(10,2)); \
         insert into public.s values (1, 1, 1.25), (2, 1, 2.50), (3, 2, 3.75), \
             (4, 3, 0.10), (5, 4, 'NaN'), (6, 4, 1.00), (7, 5, 7.77), (8, 4, 'NaN'); \
         create table public.d (id integer primary key, g integer, r text, x numeric(8,3)); \
         insert into public.d values (1, 1, 'a', 1.000), (2, 2, 'a', 2.5), \
             (3, 3, 'b', 0.125), (4, 4, 'c', 0.5); \
         create table public.f (id integer primary key, g integer, v numeric(10,2)); \
         insert into public.f values (1, 1, 1.25), (2, 1, 2.50)",
    )
    .await
    .expect("sources");
    let trellis = running(&db).await;
    let select = "SELECT d.r AS r, SUM(s.v) AS t, AVG(s.v) AS a, COUNT(s.v) AS c, \
                  SUM(d.x) AS tx, AVG(d.x) AS ax";
    for (target, join) in [("ij", "JOIN"), ("lj", "LEFT JOIN")] {
        trellis
            .apply(&format!(
                "TRANSFORM {target} FROM public.s {join} public.d ON s.g = d.g GROUP BY d.r \
                 WITHOUT SOURCE INDEX {select}"
            ))
            .await
            .expect("define the JOIN");
    }
    // Untouched by the fix: no `NaN` in either.
    trellis
        .apply(
            "TRANSFORM fj FROM public.f JOIN public.d ON f.g = d.g GROUP BY d.r \
             WITHOUT SOURCE INDEX SELECT d.r AS r, SUM(f.v) AS t",
        )
        .await
        .expect("define the finite JOIN");
    trellis
        .apply(
            "TRANSFORM fa FROM public.f GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, SUM(v) AS t, AVG(v) AS a",
        )
        .await
        .expect("define the finite aggregate");
    let all = ["ij", "lj", "fj", "fa"];
    wait_for(&trellis, &all, TransformStatus::Live).await;
    converge(&trellis).await;
    trellis.shutdown().await.expect("shutdown");

    let mut ids = Vec::new();
    for target in all {
        let id = id_of(&raw, target).await;
        ids.push(id);
        let tables: Vec<String> = if target == "fa" {
            vec![format!("{DEFAULT_SCHEMA}.agg_{id}")]
        } else {
            ["p", "m", "t"]
                .map(|t| format!("{DEFAULT_SCHEMA}.join_{id}_{t}"))
                .into_iter()
                .filter(|t| target != "fj" || !t.ends_with("_m"))
                .collect()
        };
        for table in &tables {
            table_to_0_1_0_encoded(&raw, table).await;
        }
        raw.execute(
            "update transform_definitions set floor_version = '0.1.0' where id = $1",
            &[&id],
        )
        .await
        .expect("the 0.1.0 floor");
    }
    // 0.1.0's bug: a `NaN` on `s` at `g = 1` came and went, and one on `d`
    // at `g = 3`, leaving `P`'s cell, `M`'s cell, `T` and the target at
    // `NaN` for groups `a` (`s.v`) and `b` (`d.x`).
    for (target, id) in [("ij", ids[0]), ("lj", ids[1])] {
        raw.batch_execute(&format!(
            "update {DEFAULT_SCHEMA}.join_{id}_p set __sum_1 = 'NaN' where g = 1; \
             update {DEFAULT_SCHEMA}.join_{id}_m set __sum_1 = 'NaN' where g = 3; \
             update {DEFAULT_SCHEMA}.join_{id}_t set __sum_1 = 'NaN' where r = 'a'; \
             update {DEFAULT_SCHEMA}.join_{id}_t set __sum_2 = 'NaN' where r = 'b'; \
             update public.{target} set t = 'NaN', a = 'NaN' where r = 'a'; \
             update public.{target} set tx = 'NaN', ax = 'NaN' where r = 'b'"
        ))
        .await
        .expect("a stuck NaN");
    }

    let upgrade = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    upgrade.migrate().await.expect("the upgrade's attach");
    upgrade.shutdown().await.expect("shutdown");
    // The definitions holding no `NaN` were converted in place, not rebuilt.
    let statuses: Vec<String> = raw
        .query(
            "select status from transform_definitions where id = any($1) order by id",
            &[&ids[2..].to_vec()],
        )
        .await
        .expect("statuses")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(statuses, ["live", "live"]);
    let trellis = running(&db).await;
    wait_for(&trellis, &all, TransformStatus::Live).await;
    converge(&trellis).await;

    let pages: Vec<(String, String)> = [("ij", "join"), ("lj", "left join")]
        .into_iter()
        .map(|(target, join)| {
            (
                format!(
                    "select format('%s %s %s %s %s %s', r, t, a, c, tx, ax) from public.{target}"
                ),
                format!(
                    "select format('%s %s %s %s %s %s', d.r, sum(s.v), avg(s.v), count(s.v), \
                            sum(d.x), avg(d.x)) \
                     from public.s {join} public.d on s.g = d.g group by d.r"
                ),
            )
        })
        .chain([
            (
                "select format('%s %s', r, t) from public.fj".to_string(),
                "select format('%s %s', d.r, sum(f.v)) from public.f join public.d \
                 on f.g = d.g group by d.r"
                    .to_string(),
            ),
            (
                "select format('%s %s %s', g, t, a) from public.fa".to_string(),
                "select format('%s %s %s', g, sum(v), avg(v)) from public.f group by g".to_string(),
            ),
        ])
        .collect();
    let check = async |when: &str| {
        for (actual, expected) in &pages {
            assert_eq!(
                difference(&raw, actual, expected).await,
                Vec::<String>::new(),
                "{actual} {when}"
            );
        }
    };
    check("after the upgrade").await;
    self_check_clean(&trellis, &all).await;
    for step in [
        // Group `c`'s `NaN` rows leave one at a time.
        "delete from public.s where id = 5",
        "delete from public.s where id = 8",
        // A `NaN` comes back to `a`, and on `d` to `b`, and leaves again.
        "insert into public.s values (9, 2, 'NaN'); update public.d set x = 'NaN' where id = 3",
        "delete from public.s where id = 9; update public.d set x = 0.250 where id = 3",
        "update public.s set v = 9.99 where id = 1; insert into public.f values (3, 2, 0.01)",
    ] {
        raw.batch_execute(step).await.expect(step);
        converge(&trellis).await;
        check(step).await;
    }
    self_check_clean(&trellis, &all).await;
    trellis.shutdown().await.expect("shutdown");
}
