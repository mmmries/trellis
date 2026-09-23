//! Issue #330 spike probe: drives PAUSE / gap writes / RESUME through the live
//! pipeline and samples the targets at high frequency, to measure how each
//! candidate fix behaves between the resume and a correct steady state.
//!
//! `#[ignore]`d: it is a measurement, not a test. Run with
//! `SPIKE_LABEL=<name> cargo test -p trellis --test spike_330_probe -- --ignored --nocapture`.
//! Knobs: `SPIKE_ROWS` (source rows, default 50000), `SPIKE_GROUPS` (groups,
//! default 1000), `SPIKE_OUT` (csv dir, default /tmp/spike330).

use std::io::Write;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::{Config, Trellis, TrellisOptions};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn one<T: for<'a> tokio_postgres::types::FromSql<'a>>(c: &Client, sql: &str) -> T {
    c.query_one(sql, &[]).await.expect(sql).get(0)
}

fn env_or(name: &str, default: i64) -> i64 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Clone, PartialEq, Debug)]
struct Sample {
    ms: u128,
    statuses: String,
    rollup_n: i64,
    rollup_wrong: i64,
    doubles_n: i64,
    doubles_wrong: i64,
    echo_n: i64,
    echo_wrong: i64,
}

async fn sample(c: &Client, started: Instant) -> Sample {
    let t = DEFAULT_TARGET_SCHEMA;
    let row = c
        .query_one(
            &format!(
                "select \
                 (select string_agg(split_part(target_table,'.',2) || '=' || status, ',' order by target_table) from transform_definitions), \
                 (select count(*) from {t}.order_rollup), \
                 (select count(*) from exp_rollup e full join {t}.order_rollup r on r.g = e.g where r.total is distinct from e.total), \
                 (select count(*) from {t}.order_doubles), \
                 (select count(*) from exp_doubles e full join {t}.order_doubles d on d.id = e.id where d.x is distinct from e.x), \
                 (select count(*) from {t}.rollup_echo), \
                 (select count(*) from exp_rollup e full join {t}.rollup_echo r on r.g = e.g where r.t is distinct from e.total)"
            ),
            &[],
        )
        .await
        .expect("sample");
    Sample {
        ms: started.elapsed().as_millis(),
        statuses: row.get(0),
        rollup_n: row.get(1),
        rollup_wrong: row.get(2),
        doubles_n: row.get(3),
        doubles_wrong: row.get(4),
        echo_n: row.get(5),
        echo_wrong: row.get(6),
    }
}

async fn poll_until<F>(timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out after {timeout:?}: {message}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[ignore = "measurement probe for the issue #330 spike, not a test"]
async fn resume_visibility_window_probe() {
    let label = std::env::var("SPIKE_LABEL").unwrap_or_else(|_| "unlabeled".to_string());
    let rows = env_or("SPIKE_ROWS", 50_000);
    let groups = env_or("SPIKE_GROUPS", 1_000);
    let out_dir = std::env::var("SPIKE_OUT").unwrap_or_else(|_| "/tmp/spike330".to_string());
    let t = DEFAULT_TARGET_SCHEMA;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(&format!(
        "create table orders (id bigint primary key, g bigint, a numeric); \
         alter table orders replica identity full; \
         insert into orders (id, g, a) select s, s % {groups}, s from generate_series(1, {rows}) s;"
    ))
    .await
    .expect("seed");

    let definer = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("definer");
    for stmt in [
        "TRANSFORM order_rollup FROM orders GROUP BY g SELECT sum(a) AS total",
        "TRANSFORM order_doubles FROM orders SELECT a + a AS x",
    ] {
        definer.apply(stmt).await.expect(stmt);
    }
    raw.batch_execute(&format!("alter table {t}.order_rollup replica identity full"))
        .await
        .expect("replica identity on the chained source");
    definer
        .apply("TRANSFORM rollup_echo FROM order_rollup GROUP BY g SELECT sum(total) AS t")
        .await
        .expect("chained definition");
    definer.shutdown().await.expect("definer shutdown");

    let running = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("pipeline");

    // Initial convergence.
    raw.batch_execute(
        "create table exp_rollup as select g, sum(a) as total from orders group by g; \
         create table exp_doubles as select id, a + a as x from orders;",
    )
    .await
    .expect("expectations");
    let started = Instant::now();
    poll_until(Duration::from_secs(300), "initial convergence", async || {
        let s = sample(&raw, started).await;
        s.rollup_wrong == 0 && s.doubles_wrong == 0 && s.echo_wrong == 0
            && s.statuses.split(',').all(|p| p.ends_with("=live"))
    })
    .await;
    let build_ms = started.elapsed().as_millis();

    running.apply("PAUSE TRANSFORM order_rollup").await.expect("pause rollup");
    running.apply("PAUSE TRANSFORM order_doubles").await.expect("pause doubles");

    // The gap: every row of the first 10% of groups goes away, every 10th
    // remaining row goes away, and a handful of new rows arrive.
    let dead_groups = groups / 10;
    raw.batch_execute(&format!(
        "delete from orders where g < {dead_groups}; \
         delete from orders where id % 10 = 0; \
         insert into orders (id, g, a) select s, {dead_groups} + (s % 7), s from generate_series({rows} + 1, {rows} + 100) s; \
         drop table exp_rollup; drop table exp_doubles; \
         create table exp_rollup as select g, sum(a) as total from orders group by g; \
         create table exp_doubles as select id, a + a as x from orders;"
    ))
    .await
    .expect("gap writes");
    // Let intake drain the gap's CDC (nothing applies to the paused targets).
    poll_until(Duration::from_secs(60), "ring quiescent after the gap", async || {
        let sealed: i64 = one(&raw, "select count(*) from segments where state in ('sealed','draining')").await;
        sealed == 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = sample(&raw, started).await;

    if std::env::var("SPIKE_MANUAL_DELETE").is_ok() {
        // Does a direct delete on the (published) aggregate target reach the
        // chained reader at all? Delete one live group by hand while paused.
        let victim = groups - 1;
        raw.batch_execute(&format!("delete from {t}.order_rollup where g = {victim}"))
            .await
            .expect("manual delete");
        let started = Instant::now();
        let mut reacted = None;
        let mut max_staged: i64 = 0;
        let mut staged_ops = String::new();
        while started.elapsed() < Duration::from_secs(8) {
            let n: i64 = one(&raw, &format!("select count(*) from {t}.rollup_echo where g = {victim}")).await;
            let row = raw.query_one("select count(*), coalesce(string_agg(distinct op || ':' || left(key, 12), ' '), '') from (select * from seg_0 union all select * from seg_1 union all select * from seg_2 union all select * from seg_3) r where src_table like '%order_rollup%'", &[]).await.expect("ring");
            let staged: i64 = row.get(0);
            if staged > max_staged { max_staged = staged; staged_ops = row.get(1); }
            if n == 0 && reacted.is_none() {
                reacted = Some(started.elapsed().as_millis());
            }
            if reacted.is_some() && started.elapsed() > Duration::from_secs(2) { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Is intake still alive after that? Write to the real source and
        // watch for its ring row.
        let probe_id = rows + 1000;
        raw.batch_execute(&format!("insert into orders (id, g, a) values ({probe_id}, {victim}, 1)")).await.expect("liveness insert");
        let started = Instant::now();
        let mut intake_alive = None;
        while started.elapsed() < Duration::from_secs(8) {
            let n: i64 = one(&raw, &format!("select count(*) from (select key, src_table from seg_0 union all select key, src_table from seg_1 union all select key, src_table from seg_2 union all select key, src_table from seg_3) r where src_table like '%orders' and key = '{probe_id}'")).await;
            if n > 0 { intake_alive = Some(started.elapsed().as_millis()); break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let progress: String = one(&raw, "select coalesce(string_agg(confirmed_flush_lsn::text, ','), '') from pg_replication_slots").await;
        println!("--- intake liveness after the manual delete: source insert staged after {intake_alive:?} ms; slot confirmed_flush={progress}");
        let published: String = one(&raw, "select coalesce(string_agg(schemaname || '.' || tablename, ',' order by 1), '') from pg_publication_tables").await;
        println!("--- manual delete of rollup group {victim}: echo dropped it after {reacted:?} ms; max ring rows on order_rollup seen={max_staged} [{staged_ops}]; published=[{published}]");
    }
    // RESUME, then settle the xmin fence so the discharge isn't waiting on us.
    let t0 = Instant::now();
    running.apply("RESUME TRANSFORM order_rollup").await.expect("resume rollup");
    running.apply("RESUME TRANSFORM order_doubles").await.expect("resume doubles");
    raw.batch_execute("select txid_current()").await.expect("xid");

    let mut samples: Vec<Sample> = Vec::new();
    let mut converged_since: Option<Instant> = None;
    let deadline = t0 + Duration::from_secs(env_or("SPIKE_TIMEOUT_S", 60) as u64);
    loop {
        let s = sample(&raw, t0).await;
        let good = s.rollup_wrong == 0
            && s.doubles_wrong == 0
            && s.echo_wrong == 0
            && s.statuses.split(',').all(|p| p.ends_with("=live"));
        samples.push(s);
        if good {
            let since = *converged_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= Duration::from_secs(2) {
                break;
            }
        } else {
            converged_since = None;
        }
        if Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Is intake still alive after the resume? Write to the real source and
    // watch for its ring row (or, if already drained, its target row).
    let probe_id = rows + 5000;
    raw.batch_execute(&format!("insert into orders (id, g, a) values ({probe_id}, {}, 1)", groups - 1)).await.expect("liveness insert");
    let started = Instant::now();
    let mut intake_alive = None;
    while started.elapsed() < Duration::from_secs(8) {
        let n: i64 = one(&raw, &format!("select (select count(*) from (select key, src_table from seg_0 union all select key, src_table from seg_1 union all select key, src_table from seg_2 union all select key, src_table from seg_3) r where src_table like '%orders' and key = '{probe_id}') + (select count(*) from {t}.order_doubles where id = {probe_id})")).await;
        if n > 0 { intake_alive = Some(started.elapsed().as_millis()); break; }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("intake alive after resume: {}", match intake_alive { Some(ms) => format!("yes (source insert staged after {ms} ms)"), None => "NO (source insert never staged within 8 s)".to_string() });
    if std::env::var("SPIKE_DEBUG").is_ok() {
        let rows = raw
            .query(
                &format!(
                    "select e.g::text, e.total::text, r.total::text, x.t::text \
                     from exp_rollup e \
                     full join {t}.rollup_echo x on x.g = e.g \
                     left join {t}.order_rollup r on r.g = e.g \
                     where x.t is distinct from e.total order by 1 limit 8"
                ),
                &[],
            )
            .await
            .expect("debug rows");
        println!("--- debug: wrong echo groups (g, expected total, rollup.total, echo.t) ---");
        for r in rows {
            let g: Option<String> = r.get(0);
            let e: Option<String> = r.get(1);
            let rt: Option<String> = r.get(2);
            let xt: Option<String> = r.get(3);
            println!("{g:?} {e:?} {rt:?} {xt:?}");
        }
        let segs: String = one(&raw, "select coalesce(string_agg(seg_seq || ':' || state, ','), '') from segments").await;
        let pend: i64 = one(&raw, "select count(*) from pending_backfill").await;
        let quarantined: i64 = one(&raw, "select count(*) from poison").await;
        println!("--- debug: segments [{segs}] pending_backfill={pend} poison={quarantined}");
    }
    running.shutdown().await.expect("shutdown");

    std::fs::create_dir_all(&out_dir).expect("out dir");
    let mut f = std::fs::File::create(format!("{out_dir}/{label}.csv")).expect("csv");
    writeln!(f, "ms,statuses,rollup_n,rollup_wrong,doubles_n,doubles_wrong,echo_n,echo_wrong").unwrap();
    for s in &samples {
        writeln!(f, "{},{},{},{},{},{},{},{}", s.ms, s.statuses.replace(',', ";"), s.rollup_n, s.rollup_wrong, s.doubles_n, s.doubles_wrong, s.echo_n, s.echo_wrong).unwrap();
    }

    // Summary.
    let exp_rollup_n: i64 = one(&raw, "select count(*) from exp_rollup").await;
    let exp_doubles_n: i64 = one(&raw, "select count(*) from exp_doubles").await;
    let first = |pred: &dyn Fn(&Sample) -> bool| samples.iter().find(|s| pred(s)).map(|s| s.ms);
    let converged = |wrong: &dyn Fn(&Sample) -> i64| -> Option<u128> {
        // first sample after which `wrong` stays 0
        let mut last_bad = None;
        for s in &samples { if wrong(s) != 0 { last_bad = Some(s.ms); } }
        match last_bad {
            None => samples.first().map(|s| s.ms),
            Some(lb) => samples.iter().find(|s| s.ms > lb).map(|s| s.ms),
        }
    };
    let min_by = |n: &dyn Fn(&Sample) -> i64| samples.iter().map(|s| (n(s), s.ms)).min();
    let last = samples.last().unwrap();
    println!("=== spike #330 probe [{label}] rows={rows} groups={groups} samples={} initial_build_ms={build_ms}", samples.len());
    println!("before resume: rollup n={} wrong={} | doubles n={} wrong={} | echo n={} wrong={}", before.rollup_n, before.rollup_wrong, before.doubles_n, before.doubles_wrong, before.echo_n, before.echo_wrong);
    println!("expected after: rollup n={exp_rollup_n} doubles n={exp_doubles_n} echo n={exp_rollup_n}");
    println!("discharge (status leaves waiting_to_backfill): {:?} ms", first(&|s| !s.statuses.contains("waiting_to_backfill")));
    println!("all live: {:?} ms", first(&|s| s.statuses.split(',').all(|p| p.ends_with("=live"))));
    println!("rollup : min n={:?} (n,ms); first n<expected at {:?} ms; converged at {:?} ms; final n={} wrong={}", min_by(&|s| s.rollup_n), first(&|s| s.rollup_n < exp_rollup_n), converged(&|s| s.rollup_wrong), last.rollup_n, last.rollup_wrong);
    println!("doubles: min n={:?} (n,ms); first n<expected at {:?} ms; converged at {:?} ms; final n={} wrong={}", min_by(&|s| s.doubles_n), first(&|s| s.doubles_n < exp_doubles_n), converged(&|s| s.doubles_wrong), last.doubles_n, last.doubles_wrong);
    println!("echo   : min n={:?} (n,ms); first n<expected at {:?} ms; converged at {:?} ms; final n={} wrong={}", min_by(&|s| s.echo_n), first(&|s| s.echo_n < exp_rollup_n), converged(&|s| s.echo_wrong), last.echo_n, last.echo_wrong);
    println!("--- transitions (first 40) ---");
    let mut prev: Option<&Sample> = None;
    let mut shown = 0;
    for s in &samples {
        let changed = prev.map_or(true, |p| p.statuses != s.statuses || p.rollup_n != s.rollup_n || p.rollup_wrong != s.rollup_wrong || p.doubles_n != s.doubles_n || p.doubles_wrong != s.doubles_wrong || p.echo_n != s.echo_n || p.echo_wrong != s.echo_wrong);
        if changed && shown < 40 {
            println!("{:>7} ms | {} | rollup {}/{} | doubles {}/{} | echo {}/{}", s.ms, s.statuses, s.rollup_n, s.rollup_wrong, s.doubles_n, s.doubles_wrong, s.echo_n, s.echo_wrong);
            shown += 1;
        }
        prev = Some(s);
    }
}
