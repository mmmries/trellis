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
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, PartialEq, Debug)]
struct TargetSample {
    n: i64,
    /// expected rows absent from the target
    missing: i64,
    /// target rows that should not exist any more
    extra: i64,
    /// rows present on both sides with a different value
    wrong: i64,
}

impl TargetSample {
    fn bad(&self) -> i64 {
        self.missing + self.extra + self.wrong
    }
}

#[derive(Clone, PartialEq, Debug)]
struct Sample {
    ms: u128,
    statuses: String,
    rollup: TargetSample,
    doubles: TargetSample,
    echo: TargetSample,
}

fn target_sql(target: &str, expected: &str, key: &str, tcol: &str, ecol: &str) -> String {
    format!(
        "(select count(*) from {target}), \
         (select count(*) from {expected} e left join {target} t on t.{key} = e.{key} where t.{key} is null), \
         (select count(*) from {target} t left join {expected} e on t.{key} = e.{key} where e.{key} is null), \
         (select count(*) from {target} t join {expected} e on t.{key} = e.{key} where t.{tcol} is distinct from e.{ecol})"
    )
}

fn read_target(row: &tokio_postgres::Row, at: usize) -> TargetSample {
    TargetSample {
        n: row.get(at),
        missing: row.get(at + 1),
        extra: row.get(at + 2),
        wrong: row.get(at + 3),
    }
}

async fn sample(c: &Client, started: Instant) -> Sample {
    let t = DEFAULT_TARGET_SCHEMA;
    let row = c
        .query_one(
            &format!(
                "select \
                 (select string_agg(split_part(target_table,'.',2) || '=' || status, ',' order by target_table) from transform_definitions), \
                 {}, {}, {}",
                target_sql(&format!("{t}.order_rollup"), "exp_rollup", "g", "total", "total"),
                target_sql(&format!("{t}.order_doubles"), "exp_doubles", "id", "x", "x"),
                target_sql(&format!("{t}.rollup_echo"), "exp_rollup", "g", "t", "total"),
            ),
            &[],
        )
        .await
        .expect("sample");
    Sample {
        ms: started.elapsed().as_millis(),
        statuses: row.get(0),
        rollup: read_target(&row, 1),
        doubles: read_target(&row, 5),
        echo: read_target(&row, 9),
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
        assert!(
            Instant::now() < deadline,
            "timed out after {timeout:?}: {message}"
        );
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
    raw.batch_execute(&format!(
        "alter table {t}.order_rollup replica identity full"
    ))
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
    poll_until(
        Duration::from_secs(300),
        "initial convergence",
        async || {
            let s = sample(&raw, started).await;
            s.rollup.bad() == 0
                && s.doubles.bad() == 0
                && s.echo.bad() == 0
                && s.statuses.split(',').all(|p| p.ends_with("=live"))
        },
    )
    .await;
    let build_ms = started.elapsed().as_millis();

    running
        .apply("PAUSE TRANSFORM order_rollup")
        .await
        .expect("pause rollup");
    running
        .apply("PAUSE TRANSFORM order_doubles")
        .await
        .expect("pause doubles");

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
    poll_until(
        Duration::from_secs(60),
        "ring quiescent after the gap",
        async || {
            let sealed: i64 = one(
                &raw,
                "select count(*) from segments where state in ('sealed','draining')",
            )
            .await;
            sealed == 0
        },
    )
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
            let n: i64 = one(
                &raw,
                &format!("select count(*) from {t}.rollup_echo where g = {victim}"),
            )
            .await;
            let row = raw.query_one("select count(*), coalesce(string_agg(distinct op || ':' || left(key, 12), ' '), '') from (select * from seg_0 union all select * from seg_1 union all select * from seg_2 union all select * from seg_3) r where src_table like '%order_rollup%'", &[]).await.expect("ring");
            let staged: i64 = row.get(0);
            if staged > max_staged {
                max_staged = staged;
                staged_ops = row.get(1);
            }
            if n == 0 && reacted.is_none() {
                reacted = Some(started.elapsed().as_millis());
            }
            if reacted.is_some() && started.elapsed() > Duration::from_secs(2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Is intake still alive after that? Write to the real source and
        // watch for its ring row.
        let probe_id = rows + 1000;
        raw.batch_execute(&format!(
            "insert into orders (id, g, a) values ({probe_id}, {victim}, 1)"
        ))
        .await
        .expect("liveness insert");
        let started = Instant::now();
        let mut intake_alive = None;
        while started.elapsed() < Duration::from_secs(8) {
            let n: i64 = one(&raw, &format!("select count(*) from (select key, src_table from seg_0 union all select key, src_table from seg_1 union all select key, src_table from seg_2 union all select key, src_table from seg_3) r where src_table like '%orders' and key = '{probe_id}'")).await;
            if n > 0 {
                intake_alive = Some(started.elapsed().as_millis());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let progress: String = one(&raw, "select coalesce(string_agg(confirmed_flush_lsn::text, ','), '') from pg_replication_slots").await;
        println!(
            "--- intake liveness after the manual delete: source insert staged after {intake_alive:?} ms; slot confirmed_flush={progress}"
        );
        let published: String = one(&raw, "select coalesce(string_agg(schemaname || '.' || tablename, ',' order by 1), '') from pg_publication_tables").await;
        println!(
            "--- manual delete of rollup group {victim}: echo dropped it after {reacted:?} ms; max ring rows on order_rollup seen={max_staged} [{staged_ops}]; published=[{published}]"
        );
    }
    // RESUME, then settle the xmin fence so the discharge isn't waiting on us.
    let t0 = Instant::now();
    running
        .apply("RESUME TRANSFORM order_rollup")
        .await
        .expect("resume rollup");
    running
        .apply("RESUME TRANSFORM order_doubles")
        .await
        .expect("resume doubles");
    raw.batch_execute("select txid_current()")
        .await
        .expect("xid");

    let mut samples: Vec<Sample> = Vec::new();
    let mut converged_since: Option<Instant> = None;
    let deadline = t0 + Duration::from_secs(env_or("SPIKE_TIMEOUT_S", 60) as u64);
    loop {
        let s = sample(&raw, t0).await;
        let good = s.rollup.bad() == 0
            && s.doubles.bad() == 0
            && s.echo.bad() == 0
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
    raw.batch_execute(&format!(
        "insert into orders (id, g, a) values ({probe_id}, {}, 1)",
        groups - 1
    ))
    .await
    .expect("liveness insert");
    let started = Instant::now();
    let mut intake_alive = None;
    while started.elapsed() < Duration::from_secs(8) {
        let n: i64 = one(&raw, &format!("select (select count(*) from (select key, src_table from seg_0 union all select key, src_table from seg_1 union all select key, src_table from seg_2 union all select key, src_table from seg_3) r where src_table like '%orders' and key = '{probe_id}') + (select count(*) from {t}.order_doubles where id = {probe_id})")).await;
        if n > 0 {
            intake_alive = Some(started.elapsed().as_millis());
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!(
        "intake alive after resume: {}",
        match intake_alive {
            Some(ms) => format!("yes (source insert staged after {ms} ms)"),
            None => "NO (source insert never staged within 8 s)".to_string(),
        }
    );
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
        let segs: String = one(
            &raw,
            "select coalesce(string_agg(seg_seq || ':' || state, ','), '') from segments",
        )
        .await;
        let pend: i64 = one(&raw, "select count(*) from pending_backfill").await;
        let quarantined: i64 = one(&raw, "select count(*) from poison").await;
        println!("--- debug: segments [{segs}] pending_backfill={pend} poison={quarantined}");
    }
    running.shutdown().await.expect("shutdown");

    std::fs::create_dir_all(&out_dir).expect("out dir");
    let mut f = std::fs::File::create(format!("{out_dir}/{label}.csv")).expect("csv");
    writeln!(f, "ms,statuses,rollup_n,rollup_missing,rollup_extra,rollup_wrong,doubles_n,doubles_missing,doubles_extra,doubles_wrong,echo_n,echo_missing,echo_extra,echo_wrong").unwrap();
    for s in &samples {
        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            s.ms,
            s.statuses.replace(',', ";"),
            s.rollup.n,
            s.rollup.missing,
            s.rollup.extra,
            s.rollup.wrong,
            s.doubles.n,
            s.doubles.missing,
            s.doubles.extra,
            s.doubles.wrong,
            s.echo.n,
            s.echo.missing,
            s.echo.extra,
            s.echo.wrong
        )
        .unwrap();
    }

    // Summary.
    let last = samples.last().unwrap();
    let live_at = samples
        .iter()
        .find(|s| s.statuses.split(',').all(|p| p.ends_with("=live")))
        .map(|s| s.ms);
    println!(
        "=== spike #330 probe [{label}] rows={rows} groups={groups} samples={} initial_build_ms={build_ms}",
        samples.len()
    );
    println!(
        "before resume: rollup {:?} | doubles {:?} | echo {:?}",
        before.rollup, before.doubles, before.echo
    );
    println!(
        "discharge (status leaves waiting_to_backfill): {:?} ms; all live: {live_at:?} ms",
        samples
            .iter()
            .find(|s| !s.statuses.contains("waiting_to_backfill"))
            .map(|s| s.ms)
    );
    let report = |name: &str, pick: &dyn Fn(&Sample) -> &TargetSample| {
        let peak_missing = samples.iter().map(|s| pick(s).missing).max().unwrap_or(0);
        let first_missing = samples.iter().find(|s| pick(s).missing > 0).map(|s| s.ms);
        let last_missing = samples
            .iter()
            .filter(|s| pick(s).missing > 0)
            .map(|s| s.ms)
            .last();
        let last_extra = samples
            .iter()
            .filter(|s| pick(s).extra > 0)
            .map(|s| s.ms)
            .last();
        let last_wrong = samples
            .iter()
            .filter(|s| pick(s).wrong > 0)
            .map(|s| s.ms)
            .last();
        let last_bad = samples
            .iter()
            .filter(|s| pick(s).bad() > 0)
            .map(|s| s.ms)
            .last();
        let converged = match last_bad {
            None => samples.first().map(|s| s.ms),
            Some(lb) => samples.iter().find(|s| s.ms > lb).map(|s| s.ms),
        };
        // ms-weighted integral of "bad rows" after live: how much wrongness readers were exposed to
        let mut exposure: f64 = 0.0;
        for w in samples.windows(2) {
            if let Some(l) = live_at {
                if w[0].ms >= l {
                    exposure += pick(&w[0]).bad() as f64 * (w[1].ms - w[0].ms) as f64 / 1000.0;
                }
            }
        }
        println!(
            "{name:<7}: missing peak={peak_missing} from {first_missing:?} to {last_missing:?} ms | extra (stale, should be gone) until {last_extra:?} ms | wrong-value until {last_wrong:?} ms | converged {converged:?} ms | final {:?} | bad-row-seconds after live={exposure:.1}",
            pick(last)
        );
    };
    report("rollup", &|s| &s.rollup);
    report("doubles", &|s| &s.doubles);
    report("echo", &|s| &s.echo);
    println!("--- transitions (first 40) ---");
    let mut prev: Option<&Sample> = None;
    let mut shown = 0;
    for s in &samples {
        let changed = prev.map_or(true, |p| {
            p.statuses != s.statuses
                || p.rollup != s.rollup
                || p.doubles != s.doubles
                || p.echo != s.echo
        });
        if changed && shown < 40 {
            let f =
                |t: &TargetSample| format!("n={} m={} x={} w={}", t.n, t.missing, t.extra, t.wrong);
            println!(
                "{:>7} ms | {} | rollup {} | doubles {} | echo {}",
                s.ms,
                s.statuses.replace("order_", "").replace("rollup_", ""),
                f(&s.rollup),
                f(&s.doubles),
                f(&s.echo)
            );
            shown += 1;
        }
        prev = Some(s);
    }
}
