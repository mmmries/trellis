//! `one-to-one-join-fan-out` and `one-to-one-join-poison`: what a change of a
//! 1-1 `JOIN`'s `b` row costs when it rewrites many rows of `a` (ADR-0021,
//! #1547). Both are on a captured pair of tables:
//!
//! ```text
//! asrc (id integer pk, k integer, x integer)   -- F rows on k = 1, plus background keys
//! bsrc (id integer pk, w integer, tag text)    -- the b row id = 1 is the hot key
//! TRANSFORM jt FROM public.asrc LEFT JOIN public.bsrc ON asrc.k = bsrc.id
//!     SELECT asrc.x + COALESCE(bsrc.w, 0) AS total, ...
//! ```
//!
//! ## `--joined N`: a star of `N + 1` tables
//!
//! `--joined N` (1 to 4, default 1) joins `N` tables to the one root `asrc`,
//! each on its own key column of `asrc` (a star, #1460): position 1 is
//! `asrc.k = bsrc.id`, position `p > 1` is `asrc.k{p} = bsrc{p}.id`, every
//! joined table `(id integer pk, w integer, tag text)` of 100 rows. The
//! `TRANSFORM` is the left-to-right `LEFT JOIN` chain, its `total` the root's
//! `x` plus every position's `COALESCE(w, 0)`, and the oracle and the control
//! join the same tables. `--joined 1` is the two-table shape of every earlier
//! run, table for table.
//!
//! `--fan-out-position P` (1 to N, default 1) is the joined table the fan-out
//! scenario updates: the hot rows of `asrc` hold key 1 in column `k{P}` (and
//! spread keys at the other positions), and the rep updates `bsrc{P}` row 1.
//!
//! ## `one-to-one-join-fan-out`
//!
//! For each fan-out size `F` (`--fan-outs`), a fresh cluster holds `F` rows of
//! `asrc` on key 1. The definition is built (the build is timed too), then
//! `--reps` times one `update bsrc set w = w + 1 where id = 1` commits and the
//! run waits for its page to land and for the chunks of the fan-out the page
//! left pending (`join1_fan_out`) to finish. Reported per size: the page's
//! transaction duration ([`PAGE_TRANSACTION_METRIC`]; mean, min, max), its
//! cost per row, the wall time from the `bsrc` commit to the last chunk,
//! WAL bytes and Postgres CPU per row, the engine process's peak RSS, and the
//! **control**: the same rewrite as one hand-written set-based `UPDATE` of a
//! copy of the target (`create table ctl as select * from jt`), run on the
//! same cluster with the engine stopped, under the same checkpoint-first
//! rule. The engine's cost over the control is what the ledger (`R`, `M`,
//! `J`), the rendered upsert and the page's own bookkeeping add to a plain
//! `UPDATE ... FROM`.
//!
//! `--probe-reps` more reps each issue a change of an `a` row on the key 30 ms
//! after the `bsrc` commit and time it to landing on the target
//! (`a_probe_ms`), against the same probe on an idle key (`a_probe_idle_ms`):
//! what a writer on a hot key waits.
//!
//! ## `one-to-one-join-root`
//!
//! Throughput of changes to the root alone, none of which fans out. A fresh
//! cluster holds `--root-rows` rows of `asrc` (default 200,000) with keys spread
//! over every position's 100 `b` rows. The definition is built, then `--reps`
//! passes each update every row of `asrc` in commits of `--commit-rows` (1,000)
//! rows from one connection and wait for the target to hold the pass's values.
//! Reported per pass: rows/s from the first update to the target landing, the
//! writer's own time, WAL bytes and Postgres CPU per row.
//!
//! ## `one-to-one-join-poison`
//!
//! A fan-out in which `P` rows of `a` fail their fields. The poisoned rows hold
//! `x = 2147483646`, so `asrc.x + COALESCE(bsrc.w, 0)` is in range for `w = 1` and an
//! `integer` overflow for `w = 2`, which the one `bsrc` update sets. The page
//! rolls the fan-out back and bisects its rows under savepoints down to the
//! failing ones (at most 256 attempts), holds each in quarantine and writes the
//! rest. Reported per `P`: the page's duration against the same fan-out with
//! `P = 0`, the held rows, whether the target matches its query for every row
//! but the held ones, and, when the bisection runs out of attempts, the halt.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::Client as RawClient;
use trellis::dev::defs::{IntWidth, ValueType, install_definition};

use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::disk_tier;
use crate::streaming::idle_cost;
use crate::streaming::one_to_one_apply::PageHistogram;
use crate::streaming::process_memory::RssSampler;
use crate::streaming::scrape::{CHANGES_APPLIED_METRIC, counter_value, scrape};
use crate::streaming::server_cost;
use crate::streaming::tuning::EngineTuning;

const TARGET: &str = "jt";

/// The fields a run's definition selects, by name.
pub const SHAPES: &[&str] = &["light", "wide"];

/// Position `p`'s key column of `asrc`: `k`, then `k2`, `k3`, ...
fn key_col(p: usize) -> String {
    if p == 1 { "k".into() } else { format!("k{p}") }
}

/// Position `p`'s joined table: `bsrc`, then `bsrc2`, `bsrc3`, ...
fn b_table(p: usize) -> String {
    if p == 1 {
        "bsrc".into()
    } else {
        format!("bsrc{p}")
    }
}

/// Position `p`'s alias in the oracle and control SQL: `b`, then `b2`, ...
fn b_alias(p: usize) -> String {
    if p == 1 { "b".into() } else { format!("b{p}") }
}

/// The oracle's and the control's joins of `asrc` (alias `a`) to every position.
fn oracle_joins(joined: usize) -> String {
    (1..=joined)
        .map(|p| {
            format!(
                "left join public.{} {} on a.{} = {}.id",
                b_table(p),
                b_alias(p),
                key_col(p),
                b_alias(p)
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// `(select list, target column types, oracle select list over a and b)`.
///
/// `light` is one `integer` sum of the root's `x` and every position's `w`.
/// `wide` adds a `text` pass-through of each position's tag, a product and a
/// comparison: the shape of a typical derived row. With `joined = 1` they are
/// the two-table shapes of every earlier run.
fn shape(name: &str, joined: usize) -> (String, Vec<(String, ValueType)>, String) {
    let int4 = ValueType::Integer(IntWidth::Int4);
    let sum: String = (1..=joined)
        .map(|p| format!(" + COALESCE({}.w, 0)", b_table(p)))
        .collect();
    let osum: String = (1..=joined)
        .map(|p| format!(" + coalesce({}.w, 0)", b_alias(p)))
        .collect();
    let tag_name = |p: usize| {
        if p == 1 {
            "tag".to_string()
        } else {
            format!("tag{p}")
        }
    };
    match name {
        "light" => (
            format!("asrc.x{sum} AS total"),
            vec![("total".to_string(), int4)],
            format!("a.x{osum}"),
        ),
        "wide" => {
            let tags: String = (1..=joined)
                .map(|p| format!(", {}.tag AS {}", b_table(p), tag_name(p)))
                .collect();
            let otags: String = (1..=joined)
                .map(|p| format!(", {}.tag", b_alias(p)))
                .collect();
            let mut columns = vec![("total".to_string(), int4)];
            columns.extend((1..=joined).map(|p| (tag_name(p), ValueType::Text)));
            columns.push(("dbl".to_string(), int4));
            columns.push(("big".to_string(), ValueType::Boolean));
            (
                format!(
                    "asrc.x{sum} AS total{tags}, asrc.x * 2{sum} AS dbl, \
                     (asrc.x{sum}) > 500 AS big"
                ),
                columns,
                format!("a.x{osum}{otags}, a.x * 2{osum}, (a.x{osum}) > 500"),
            )
        }
        other => panic!("unknown --shape {other:?}; expected one of {SHAPES:?}"),
    }
}

/// Rows of `a` on keys other than the hot one, by default: enough for `J` to
/// hold more than a key.
pub const BACKGROUND_ROWS: i64 = 1_000;

/// The run's knobs, for both scenarios.
#[derive(Debug, Clone)]
pub struct OneToOneJoin {
    /// Fan-out sizes, in rows of `a` on the hot key.
    pub fan_outs: Vec<i64>,
    pub reps: usize,
    pub probe_reps: usize,
    pub shape: String,
    /// `one-to-one-join-poison`: failing rows per run.
    pub poisoned: Vec<i64>,
    /// `one-to-one-join-poison`: the fan-out size every run shares.
    pub poison_rows: i64,
    pub timeout: Duration,
    /// `--statements`: after the reps, print the busiest statements by total
    /// time (`pg_stat_statements`; see `idle-cost --statements` for the cluster
    /// settings). Use `--reps 1 --probe-reps 0` so only the fan-out is in it.
    pub statements: bool,
    /// `--background-rows`: rows of `a` on other keys (spread over 98 keys), which
    /// make `R` larger than the fan-out.
    pub background_rows: i64,
    /// `--analyze-hidden`: `ANALYZE` the definition's hidden tables once it is
    /// live, before the reps.
    pub analyze_hidden: bool,
    /// `--joined`: joined tables beside the root, 1 to 4 (the module doc).
    pub joined: usize,
    /// `--fan-out-position`: the joined table the fan-out updates, 1 to `joined`.
    pub position: usize,
    /// `one-to-one-join-root`: rows of `a`.
    pub root_rows: i64,
    /// `one-to-one-join-root`: rows per commit of the updating writer.
    pub commit_rows: i64,
}

impl OneToOneJoin {
    /// The most joined tables a run takes: 5 tables (#1459's cap).
    pub const MAX_JOINED: usize = 4;

    /// Whether the knobs name a shape that exists.
    pub fn validate(&self) {
        assert!(
            (1..=Self::MAX_JOINED).contains(&self.joined),
            "--joined must be 1 to {}",
            Self::MAX_JOINED
        );
        assert!(
            (1..=self.joined).contains(&self.position),
            "--fan-out-position must be 1 to --joined ({})",
            self.joined
        );
        assert!(
            self.root_rows >= 1 && self.commit_rows >= 1,
            "--root-rows and --commit-rows must be at least 1"
        );
    }

    /// The statement that changes the fan-out's joined row.
    fn update_b(&self) -> String {
        format!(
            "update public.{} set w = w + 1 where id = 1",
            b_table(self.position)
        )
    }
}

/// A page's numbers, from `bsrc`'s commit to the page landing.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rep {
    pub page_secs: f64,
    pub wall_secs: f64,
    pub wal_bytes: i64,
    pub pg_cpu_secs: f64,
}

/// What `a` is filled with.
#[derive(Clone, Copy)]
enum Rows {
    /// `fan_out` rows on the hot key of the fan-out position, `poison` of them
    /// failing, plus the background rows.
    FanOut { fan_out: i64, poison: i64 },
    /// `n` rows spread over every position's keys.
    Root(i64),
}

/// Creates the tables and fills them. `poison` rows of the hot key hold
/// `x = 2147483646`, spread evenly over the key's ids.
async fn load(raw: &RawClient, cfg: &OneToOneJoin, rows: Rows) {
    let joined = cfg.joined;
    let keys_ddl: String = (1..=joined)
        .map(|p| format!("{} integer, ", key_col(p)))
        .collect();
    let mut ddl = format!("create table public.asrc (id integer primary key, {keys_ddl}x integer)");
    for p in 1..=joined {
        ddl.push_str(&format!(
            "; create table public.{} (id integer primary key, w integer, tag text); \
             insert into public.{} select g, 1, 'b' || g from generate_series(1, 100) g",
            b_table(p),
            b_table(p)
        ));
    }
    raw.batch_execute(&ddl).await.expect("create the sources");
    // A position's key off the hot one: spread over the joined table's 100 rows.
    let spread = |_p: usize| "1 + (g % 100)".to_string();
    match rows {
        Rows::FanOut {
            fan_out,
            poison: poison_rows,
        } => {
            let hot: String = (1..=joined)
                .map(|p| {
                    if p == cfg.position {
                        "1".to_string()
                    } else {
                        spread(p)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            let stride = if poison_rows > 0 {
                fan_out / poison_rows
            } else {
                0
            };
            raw.execute(
                &format!(
                    "insert into public.asrc \
                     select g, {hot}, case when $2::bigint > 0 and g % $2::bigint = 0 \
                                           then 2147483646 else (g % 1000)::integer end \
                     from generate_series(1, $1::bigint) g"
                ),
                &[&fan_out, &stride],
            )
            .await
            .expect("load the hot key");
            let background: String = (1..=joined)
                .map(|p| {
                    if p == cfg.position {
                        "2 + (g % 98)".to_string()
                    } else {
                        spread(p)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            raw.execute(
                &format!(
                    "insert into public.asrc \
                     select $1::bigint + g, {background}, g \
                     from generate_series(1, $2::bigint) g"
                ),
                &[&fan_out, &cfg.background_rows],
            )
            .await
            .expect("load the background keys");
        }
        Rows::Root(n) => {
            let keys: String = (1..=joined).map(spread).collect::<Vec<_>>().join(", ");
            raw.execute(
                &format!(
                    "insert into public.asrc select g, {keys}, (g % 1000)::integer \
                     from generate_series(1, $1::bigint) g"
                ),
                &[&n],
            )
            .await
            .expect("load the root rows");
        }
    }
    let mut analyze = String::from("analyze public.asrc");
    for p in 1..=joined {
        analyze.push_str(&format!("; analyze public.{}", b_table(p)));
    }
    raw.batch_execute(&analyze).await.expect("analyze");
}

/// What a fresh cluster with the sources, the engine and a live definition
/// holds.
struct Fixture {
    cluster: TestCluster,
    db: testkit::TestDatabase,
    raw: RawClient,
    client: trellis::Client,
    build_secs: f64,
}

async fn fixture(cfg: &OneToOneJoin, tuning: &EngineTuning, rows: Rows) -> Fixture {
    cfg.validate();
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    load(&raw, cfg, rows).await;
    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");
    let (fields, columns, _) = shape(&cfg.shape, cfg.joined);
    let columns: HashMap<String, ValueType> = columns.into_iter().collect();
    let joins: String = (1..=cfg.joined)
        .map(|p| {
            format!(
                " LEFT JOIN public.{b} ON asrc.{k} = {b}.id",
                b = b_table(p),
                k = key_col(p)
            )
        })
        .collect();
    let text = format!("TRANSFORM {TARGET} FROM public.asrc{joins} SELECT {fields}");
    let started = Instant::now();
    install_definition(&db.pool, &text, &columns, "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    wait_for_live(&raw, TARGET, Instant::now() + cfg.timeout).await;
    let build_secs = started.elapsed().as_secs_f64();
    if cfg.analyze_hidden {
        let tables: Vec<String> = raw
            .query(
                "select format('%I.%I', n.nspname, c.relname) from pg_class c \
                 join pg_namespace n on n.oid = c.relnamespace \
                 where c.relname ~ '^join1_[0-9]+_[rmj]$'",
                &[],
            )
            .await
            .expect("list the hidden tables")
            .iter()
            .map(|r| r.get(0))
            .collect();
        for table in tables {
            raw.batch_execute(&format!("analyze {table}"))
                .await
                .expect("analyze a hidden table");
        }
    }
    Fixture {
        cluster,
        db,
        raw,
        client,
        build_secs,
    }
}

/// The definition's status text.
async fn status(raw: &RawClient) -> String {
    raw.query_one(
        "select status::text from transform_definitions where target_table = $1",
        &[&format!("public.{TARGET}")],
    )
    .await
    .expect("read status")
    .get(0)
}

/// Rows of the target that differ from the join written over the sources.
async fn mismatches(raw: &RawClient, shape_name: &str, joined: usize) -> i64 {
    let (_, columns, oracle) = shape(shape_name, joined);
    let names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
    let joins = oracle_joins(joined);
    raw.query_one(
        &format!(
            "select count(*) from \
               (select a.id, row({oracle})::text as v from public.asrc a {joins}) s \
             full join (select id, row({})::text as v from public.{TARGET}) t using (id) \
             where s.v is distinct from t.v",
            names.join(", ")
        ),
        &[],
    )
    .await
    .expect("oracle")
    .get(0)
}

/// Rows of the target whose `total` differs from the `light` join written in
/// `bigint`, which a poisoned row cannot overflow.
async fn total_mismatches(raw: &RawClient) -> i64 {
    raw.query_one(
        &format!(
            "select count(*) from public.asrc a \
             left join public.bsrc b on a.k = b.id \
             full join public.{TARGET} t on t.id = a.id \
             where t.total::bigint is distinct from a.x::bigint + coalesce(b.w, 0)"
        ),
        &[],
    )
    .await
    .expect("bigint oracle")
    .get(0)
}

/// What a page landing needs: one page more than `before`, or the definition
/// leaving `live`.
enum Landed {
    Page,
    Left(String),
    TimedOut,
}

async fn wait_page(
    raw: &RawClient,
    before: &PageHistogram,
    applied: u64,
    timeout: Duration,
) -> Landed {
    let deadline = Instant::now() + timeout;
    let mut last_status_check = Instant::now();
    loop {
        // A page that failed (a halt rolls the page back) records no applied change.
        let rendered = scrape();
        if PageHistogram::capture(&rendered).since(before).count >= 1
            && counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET) > applied
        {
            return Landed::Page;
        }
        if last_status_check.elapsed() > Duration::from_millis(100) {
            last_status_check = Instant::now();
            let s = status(raw).await;
            if s != "live" {
                return Landed::Left(s);
            }
        }
        if Instant::now() > deadline {
            return Landed::TimedOut;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// Waits until no 1-1 `JOIN` fan-out is pending: the chunks after a page
/// that left its key to them (`join1_fan_out`) have all committed, so every
/// target row on the key joins the new `b` row.
async fn wait_fan_outs_done(raw: &RawClient, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let sql = format!(
        "select exists (select 1 from {}.join1_fan_out)",
        trellis::dev::config::DEFAULT_SCHEMA
    );
    loop {
        let pending: bool = raw
            .query_one(&sql, &[])
            .await
            .expect("read the pending fan-outs")
            .get(0);
        if !pending {
            return;
        }
        assert!(Instant::now() < deadline, "the fan-out never finished");
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// `checkpoint`, so every rep starts with the same full-page-write state.
async fn checkpoint(raw: &RawClient) {
    raw.batch_execute("checkpoint").await.expect("checkpoint");
}

/// One `update bsrc` and the wait for its page.
async fn one_rep(f: &Fixture, cfg: &OneToOneJoin) -> (Rep, Landed) {
    let timeout = cfg.timeout;
    checkpoint(&f.raw).await;
    let rendered = scrape();
    let before = PageHistogram::capture(&rendered);
    let applied = counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET);
    let disk = disk_tier::sample(&f.raw).await;
    let cost = server_cost::start(&f.cluster);
    let started = Instant::now();
    f.raw
        .batch_execute(&cfg.update_b())
        .await
        .expect("update b");
    let landed = wait_page(&f.raw, &before, applied, timeout).await;
    if matches!(landed, Landed::Page) {
        wait_fan_outs_done(&f.raw, timeout).await;
    }
    let wall_secs = started.elapsed().as_secs_f64();
    let pages = PageHistogram::capture(&scrape()).since(&before);
    let cost = cost.finish(&f.raw).await;
    let disk = disk_tier::since(&f.raw, &disk).await;
    (
        Rep {
            page_secs: pages.sum,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        },
        landed,
    )
}

/// The wall time of one change of an `a` row, committed to landing on the
/// target, polling the one target row.
async fn a_probe(raw: &RawClient, joined: usize, id: i32, timeout: Duration) -> f64 {
    let sum: String = (1..=joined)
        .map(|p| {
            format!(
                " + coalesce((select w from public.{} where id = a.{}), 0)",
                b_table(p),
                key_col(p)
            )
        })
        .collect();
    let expected: i32 = raw
        .query_one(
            &format!("update public.asrc a set x = x + 1 where id = $1 returning a.x{sum}"),
            &[&id],
        )
        .await
        .expect("update a")
        .get(0);
    let started = Instant::now();
    loop {
        let total: Option<i32> = raw
            .query_opt(
                &format!("select total from public.{TARGET} where id = $1"),
                &[&id],
            )
            .await
            .expect("read the target row")
            .map(|r| r.get(0));
        if total == Some(expected) {
            return started.elapsed().as_secs_f64();
        }
        assert!(
            started.elapsed() < timeout,
            "the probe change of a row never landed"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// The ten busiest statements since the last reset, one JSON line: calls,
/// total and mean milliseconds, rows, WAL bytes and the first 300 characters.
async fn top_statements(raw: &RawClient, fan_out: i64) -> String {
    raw.query_one(
        "select jsonb_build_object('scenario', 'one-to-one-join-fan-out-statements', \
                'fan_out', $1::bigint, 'rows', coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb))::text \
         from (select calls, round(total_exec_time::numeric, 1)::float8 as total_ms, \
                      round(mean_exec_time::numeric, 2)::float8 as mean_ms, rows, \
                      wal_bytes::bigint as wal_bytes, left(regexp_replace(query, '\\s+', ' ', 'g'), 300) as query \
               from pg_stat_statements where query not like '%pg_stat_statements%' \
               order by total_exec_time desc limit 10) t",
        &[&fan_out],
    )
    .await
    .expect("read pg_stat_statements")
    .get(0)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    if v.is_empty() {
        f64::NAN
    } else {
        v[v.len() / 2]
    }
}

/// The set-based control: `reps` timed `UPDATE ... FROM` of a copy of the
/// target's hot-key rows, the engine stopped.
async fn control(
    cluster: &TestCluster,
    raw: &RawClient,
    fan_out: i64,
    reps: usize,
    cfg: &OneToOneJoin,
) -> Vec<Rep> {
    let (_, columns, _) = shape(&cfg.shape, cfg.joined);
    let osum: String = (1..=cfg.joined)
        .map(|p| format!(" + coalesce({}.w, 0)", b_alias(p)))
        .collect();
    let tag_name = |p: usize| {
        if p == 1 {
            "tag".to_string()
        } else {
            format!("tag{p}")
        }
    };
    let sets: Vec<String> = match cfg.shape.as_str() {
        "light" => vec![format!("total = a.x{osum}")],
        _ => {
            let mut sets = vec![format!("total = a.x{osum}")];
            sets.extend((1..=cfg.joined).map(|p| format!("{} = {}.tag", tag_name(p), b_alias(p))));
            sets.push(format!("dbl = a.x * 2{osum}"));
            sets.push(format!("big = (a.x{osum}) > 500"));
            sets
        }
    };
    assert_eq!(sets.len(), columns.len());
    let joins = oracle_joins(cfg.joined);
    let hot = format!("a.{} = 1", key_col(cfg.position));
    raw.batch_execute(&format!(
        "create table public.ctl as select * from public.{TARGET}; \
             alter table public.ctl add primary key (id); analyze public.ctl"
    ))
    .await
    .expect("copy the target");
    let mut out = Vec::new();
    for _ in 0..reps {
        checkpoint(raw).await;
        let disk = disk_tier::sample(raw).await;
        let cost = server_cost::start(cluster);
        let started = Instant::now();
        let n = raw
            .execute(
                &format!(
                    "update public.ctl c set {} from public.asrc a {joins} \
                     where a.id = c.id and {hot}",
                    sets.join(", ")
                ),
                &[],
            )
            .await
            .expect("control update");
        assert_eq!(n as i64, fan_out, "the control rewrites the fan-out");
        let wall_secs = started.elapsed().as_secs_f64();
        let cost = cost.finish(raw).await;
        let disk = disk_tier::since(raw, &disk).await;
        out.push(Rep {
            page_secs: wall_secs,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        });
    }
    out
}

fn mean(v: &[f64]) -> f64 {
    v.iter().sum::<f64>() / v.len().max(1) as f64
}

fn min(v: &[f64]) -> f64 {
    v.iter().cloned().fold(f64::INFINITY, f64::min)
}

fn max(v: &[f64]) -> f64 {
    v.iter().cloned().fold(0.0, f64::max)
}

/// One fan-out size's numbers.
#[derive(Debug, Clone)]
pub struct FanOutResult {
    pub fan_out: i64,
    pub shape: String,
    pub joined: usize,
    pub position: usize,
    pub build_secs: f64,
    pub reps: Vec<Rep>,
    pub control: Vec<Rep>,
    pub a_probe_secs: Vec<f64>,
    pub a_probe_idle_secs: Vec<f64>,
    pub peak_rss_mb: f64,
    pub oracle_mismatches: i64,
    pub landed: bool,
    pub background_rows: i64,
    pub analyze_hidden: bool,
}

impl FanOutResult {
    pub fn to_json(&self) -> String {
        let f = self.fan_out as f64;
        let col = |reps: &[Rep], g: fn(&Rep) -> f64| reps.iter().map(g).collect::<Vec<_>>();
        let page = col(&self.reps, |r| r.page_secs);
        let wall = col(&self.reps, |r| r.wall_secs);
        let wal = col(&self.reps, |r| r.wal_bytes as f64);
        let cpu = col(&self.reps, |r| r.pg_cpu_secs);
        let c_page = col(&self.control, |r| r.page_secs);
        let c_wal = col(&self.control, |r| r.wal_bytes as f64);
        let c_cpu = col(&self.control, |r| r.pg_cpu_secs);
        format!(
            "{{\"scenario\":\"one-to-one-join-fan-out\",\"shape\":\"{}\",\"joined\":{},\"position\":{},\"fan_out\":{},\"background_rows\":{},\"analyze_hidden\":{},\
             \"build_secs\":{:.2},\"build_us_per_row\":{:.2},\"reps\":{},\
             \"page_secs_mean\":{:.4},\"page_secs_min\":{:.4},\"page_secs_max\":{:.4},\
             \"page_us_per_row\":{:.3},\"wall_secs_mean\":{:.4},\
             \"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.3},\
             \"control_secs_mean\":{:.4},\"control_us_per_row\":{:.3},\
             \"control_wal_bytes_per_row\":{:.1},\"control_pg_cpu_us_per_row\":{:.3},\
             \"engine_over_control\":{:.2},\
             \"a_probe_secs_median\":{:.4},\"a_probe_idle_secs_median\":{:.4},\
             \"peak_rss_mb\":{:.1},\"landed\":{},\"oracle_mismatches\":{},\
             \"page_secs_reps\":{},\"wall_secs_reps\":{},\"wal_bytes_reps\":{}}}",
            self.shape,
            self.joined,
            self.position,
            self.fan_out,
            self.background_rows,
            self.analyze_hidden,
            self.build_secs,
            self.build_secs * 1e6 / (f + self.background_rows as f64),
            self.reps.len(),
            mean(&page),
            min(&page),
            max(&page),
            mean(&page) * 1e6 / f,
            mean(&wall),
            mean(&wal) / f,
            mean(&cpu) * 1e6 / f,
            mean(&c_page),
            mean(&c_page) * 1e6 / f,
            mean(&c_wal) / f,
            mean(&c_cpu) * 1e6 / f,
            mean(&page) / mean(&c_page).max(f64::EPSILON),
            median(self.a_probe_secs.clone()),
            median(self.a_probe_idle_secs.clone()),
            self.peak_rss_mb,
            self.landed,
            self.oracle_mismatches,
            json_list(&page, 4),
            json_list(&wall, 4),
            json_list(&wal, 0),
        )
    }
}

/// `values` as a JSON array of numbers with `places` decimals.
fn json_list(values: &[f64], places: usize) -> String {
    let items: Vec<String> = values.iter().map(|v| format!("{v:.places$}")).collect();
    format!("[{}]", items.join(","))
}

/// One fan-out size on a fresh cluster.
pub async fn run_fan_out(cfg: &OneToOneJoin, tuning: &EngineTuning, fan_out: i64) -> FanOutResult {
    let f = fixture(cfg, tuning, Rows::FanOut { fan_out, poison: 0 }).await;
    let rss = RssSampler::start(Duration::from_millis(100), None);
    let idle: Vec<f64> = {
        let mut v = Vec::new();
        for _ in 0..cfg.probe_reps.max(3) {
            // A row off the hot key: no fan-out is anywhere near it.
            v.push(a_probe(&f.raw, cfg.joined, (fan_out + 1) as i32, cfg.timeout).await);
        }
        v
    };
    if cfg.statements {
        idle_cost::reset_statements(&f.raw).await;
    }
    // The last probe's page records its duration just after its row is visible.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut reps = Vec::new();
    let mut landed = true;
    for _ in 0..cfg.reps {
        let (rep, l) = one_rep(&f, cfg).await;
        landed &= matches!(l, Landed::Page);
        reps.push(rep);
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // The a-writer's wait: a change of a row on the hot key 30 ms into a fan-out.
    let mut probes = Vec::new();
    for _ in 0..cfg.probe_reps {
        checkpoint(&f.raw).await;
        let before = PageHistogram::capture(&scrape());
        f.raw
            .batch_execute(&cfg.update_b())
            .await
            .expect("update b");
        tokio::time::sleep(Duration::from_millis(30)).await;
        probes.push(a_probe(&f.raw, cfg.joined, 2, cfg.timeout).await);
        // Both pages (the fan-out and the probe) are done before the next rep.
        let deadline = Instant::now() + cfg.timeout;
        while PageHistogram::capture(&scrape()).since(&before).count < 2 {
            assert!(
                Instant::now() < deadline,
                "the probe reps' pages never landed"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        wait_fan_outs_done(&f.raw, cfg.timeout).await;
    }
    if cfg.statements {
        println!("{}", top_statements(&f.raw, fan_out).await);
    }
    let peak_rss_mb = rss.finish().peak_bytes as f64 / 1e6;
    let oracle_mismatches = mismatches(&f.raw, &cfg.shape, cfg.joined).await;
    let Fixture {
        cluster,
        db: _db,
        raw,
        client,
        build_secs,
    } = f;
    client.shutdown().await.expect("client shutdown");
    let control = control(&cluster, &raw, fan_out, cfg.reps, cfg).await;
    FanOutResult {
        fan_out,
        shape: cfg.shape.clone(),
        joined: cfg.joined,
        position: cfg.position,
        build_secs,
        reps,
        control,
        a_probe_secs: probes,
        a_probe_idle_secs: idle,
        peak_rss_mb,
        oracle_mismatches,
        landed,
        background_rows: cfg.background_rows,
        analyze_hidden: cfg.analyze_hidden,
    }
}

/// Runs every fan-out size in `cfg`, one fresh cluster each.
pub async fn run_fan_outs(cfg: &OneToOneJoin, tuning: &EngineTuning) -> Vec<FanOutResult> {
    let mut results = Vec::new();
    for &fan_out in &cfg.fan_outs {
        let r = run_fan_out(cfg, tuning, fan_out).await;
        println!("{}", r.to_json());
        results.push(r);
    }
    results
}

/// One pass of the root scenario: every row of `a` updated once.
#[derive(Debug, Clone, Copy, Default)]
pub struct RootPass {
    /// From the first update to the target holding the pass's values.
    pub wall_secs: f64,
    /// From the first update to the writer's last commit.
    pub write_secs: f64,
    pub wal_bytes: i64,
    pub pg_cpu_secs: f64,
}

/// The root scenario's numbers.
#[derive(Debug, Clone)]
pub struct RootResult {
    pub shape: String,
    pub joined: usize,
    pub rows: i64,
    pub commit_rows: i64,
    pub build_secs: f64,
    pub passes: Vec<RootPass>,
    pub peak_rss_mb: f64,
    pub landed: bool,
    pub oracle_mismatches: i64,
}

impl RootResult {
    pub fn to_json(&self) -> String {
        let rows = self.rows as f64;
        let col = |g: fn(&RootPass) -> f64| self.passes.iter().map(g).collect::<Vec<_>>();
        let wall = col(|p| p.wall_secs);
        let write = col(|p| p.write_secs);
        let wal = col(|p| p.wal_bytes as f64);
        let cpu = col(|p| p.pg_cpu_secs);
        let rate: Vec<f64> = wall.iter().map(|w| rows / w).collect();
        format!(
            "{{\"scenario\":\"one-to-one-join-root\",\"shape\":\"{}\",\"joined\":{},\
             \"rows\":{},\"commit_rows\":{},\"build_secs\":{:.2},\"passes\":{},\
             \"rows_per_sec_median\":{:.1},\"rows_per_sec_min\":{:.1},\"rows_per_sec_max\":{:.1},\
             \"write_secs_median\":{:.3},\"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.3},\
             \"rows_per_sec_passes\":{},\"peak_rss_mb\":{:.1},\"landed\":{},\"oracle_mismatches\":{}}}",
            self.shape,
            self.joined,
            self.rows,
            self.commit_rows,
            self.build_secs,
            self.passes.len(),
            median(rate.clone()),
            min(&rate),
            max(&rate),
            median(write),
            mean(&wal) / rows,
            mean(&cpu) * 1e6 / rows,
            json_list(&rate, 1),
            self.peak_rss_mb,
            self.landed,
            self.oracle_mismatches,
        )
    }
}

/// Whether any target row is below `marker`: a pass has not landed yet.
async fn root_pending(raw: &RawClient, marker: i32) -> bool {
    raw.query_one(
        &format!("select exists (select 1 from public.{TARGET} where total < $1)"),
        &[&marker],
    )
    .await
    .expect("read the target")
    .get(0)
}

/// `cfg.reps` passes over `cfg.root_rows` rows of `a` on one fresh cluster.
/// Pass `n` sets `x` to `n * 10_000_000 + id % 1000`, which every target `total`
/// then exceeds (the joined `w` are small), so a pass has landed when no `total`
/// is under its marker.
pub async fn run_root(cfg: &OneToOneJoin, tuning: &EngineTuning) -> RootResult {
    assert!(cfg.reps <= 200, "--reps over 200 overflows the markers");
    let f = fixture(cfg, tuning, Rows::Root(cfg.root_rows)).await;
    let rss = RssSampler::start(Duration::from_millis(100), None);
    let mut passes = Vec::new();
    let mut landed = true;
    for pass in 1..=cfg.reps {
        let marker = pass as i32 * 10_000_000;
        checkpoint(&f.raw).await;
        let disk = disk_tier::sample(&f.raw).await;
        let cost = server_cost::start(&f.cluster);
        let started = Instant::now();
        let mut lo = 1;
        while lo <= cfg.root_rows {
            let hi = (lo + cfg.commit_rows - 1).min(cfg.root_rows);
            f.raw
                .batch_execute(&format!(
                    "update public.asrc set x = {marker} + id % 1000 where id between {lo} and {hi}"
                ))
                .await
                .expect("update a");
            lo = hi + 1;
        }
        let write_secs = started.elapsed().as_secs_f64();
        let deadline = Instant::now() + cfg.timeout;
        let mut last_status_check = Instant::now();
        let mut this_landed = true;
        while root_pending(&f.raw, marker).await {
            if last_status_check.elapsed() > Duration::from_millis(500) {
                last_status_check = Instant::now();
                if status(&f.raw).await != "live" {
                    this_landed = false;
                    break;
                }
            }
            if Instant::now() > deadline {
                this_landed = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let wall_secs = started.elapsed().as_secs_f64();
        let cost = cost.finish(&f.raw).await;
        let disk = disk_tier::since(&f.raw, &disk).await;
        landed &= this_landed;
        passes.push(RootPass {
            wall_secs,
            write_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        });
        if !this_landed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let peak_rss_mb = rss.finish().peak_bytes as f64 / 1e6;
    let oracle_mismatches = mismatches(&f.raw, &cfg.shape, cfg.joined).await;
    let build_secs = f.build_secs;
    let _ = f.client.shutdown().await;
    RootResult {
        shape: cfg.shape.clone(),
        joined: cfg.joined,
        rows: cfg.root_rows,
        commit_rows: cfg.commit_rows,
        build_secs,
        passes,
        peak_rss_mb,
        landed,
        oracle_mismatches,
    }
}

/// Runs the root scenario once and prints its line.
pub async fn run_roots(cfg: &OneToOneJoin, tuning: &EngineTuning) -> Vec<RootResult> {
    let r = run_root(cfg, tuning).await;
    println!("{}", r.to_json());
    vec![r]
}

/// One poisoned fan-out's numbers.
#[derive(Debug, Clone)]
pub struct PoisonResult {
    pub rows: i64,
    pub poisoned: i64,
    pub shape: String,
    pub rep: Rep,
    pub outcome: String,
    /// Rows of `a` held in quarantine.
    pub held: i64,
    /// Of those, the rows that hold the failing `x`.
    pub held_poisoned: i64,
    /// Rows of the target that differ from the join.
    pub mismatches: i64,
    pub build_secs: f64,
}

/// [`PoisonResult::outcome`] for a fan-out the bisection gave up on.
pub const HALTED: &str = "left live: paused";

impl PoisonResult {
    /// Whether the run's end state is the one its outcome allows. A committed
    /// page holds exactly the failing rows, which are then the only rows of
    /// the target left behind; a halt rolls the whole page back, so it holds
    /// nothing and leaves every row of the fan-out behind. A run that timed out
    /// or ended any other way fails.
    pub fn oracle_holds(&self) -> bool {
        match self.outcome.as_str() {
            "committed" => {
                self.held == self.poisoned
                    && self.held_poisoned == self.poisoned
                    && self.mismatches == self.poisoned
            }
            HALTED => self.held == 0 && self.mismatches == self.rows,
            _ => false,
        }
    }

    pub fn to_json(&self) -> String {
        // A page that did not commit has no duration worth reporting.
        let committed = self.outcome == "committed";
        let or_null = |v: f64, places: usize| {
            if committed {
                format!("{v:.places$}")
            } else {
                "null".to_string()
            }
        };
        format!(
            "{{\"scenario\":\"one-to-one-join-poison\",\"shape\":\"{}\",\"fan_out\":{},\
             \"poisoned\":{},\"outcome\":\"{}\",\"page_secs\":{},\"wall_secs\":{:.4},\
             \"page_us_per_row\":{},\"wal_bytes_per_row\":{:.1},\"pg_cpu_us_per_row\":{:.3},\
             \"held_rows\":{},\"held_poisoned_rows\":{},\"target_rows_off\":{},\
             \"oracle_holds\":{},\"build_secs\":{:.2}}}",
            self.shape,
            self.rows,
            self.poisoned,
            self.outcome,
            or_null(self.rep.page_secs, 4),
            self.rep.wall_secs,
            or_null(self.rep.page_secs * 1e6 / self.rows as f64, 3),
            self.rep.wal_bytes as f64 / self.rows as f64,
            self.rep.pg_cpu_secs * 1e6 / self.rows as f64,
            self.held,
            self.held_poisoned,
            self.mismatches,
            self.oracle_holds(),
            self.build_secs,
        )
    }
}

/// One poisoned fan-out on a fresh cluster: `poisoned` rows of `cfg.poison_rows`
/// fail when `w` goes from 1 to 2.
pub async fn run_poison(cfg: &OneToOneJoin, tuning: &EngineTuning, poisoned: i64) -> PoisonResult {
    let rows = cfg.poison_rows;
    // `wide`'s `a.x * 2` overflows on a poisoned row at the build.
    let cfg = &OneToOneJoin {
        shape: "light".to_string(),
        joined: 1,
        position: 1,
        ..cfg.clone()
    };
    let f = fixture(
        cfg,
        tuning,
        Rows::FanOut {
            fan_out: rows,
            poison: poisoned,
        },
    )
    .await;
    checkpoint(&f.raw).await;
    let rendered = scrape();
    let before = PageHistogram::capture(&rendered);
    let applied = counter_value(&rendered, CHANGES_APPLIED_METRIC, TARGET);
    let disk = disk_tier::sample(&f.raw).await;
    let cost = server_cost::start(&f.cluster);
    let started = Instant::now();
    f.raw
        .batch_execute("update public.bsrc set w = 2 where id = 1")
        .await
        .expect("update b");
    let landed = wait_page(&f.raw, &before, applied, cfg.timeout).await;
    let wall_secs = started.elapsed().as_secs_f64();
    let pages = PageHistogram::capture(&scrape()).since(&before);
    let cost = cost.finish(&f.raw).await;
    let disk = disk_tier::since(&f.raw, &disk).await;
    let outcome = match landed {
        Landed::Page => "committed".to_string(),
        Landed::Left(s) => format!("left live: {s}"),
        Landed::TimedOut => "timed out".to_string(),
    };
    // The held rows keep the total they had; every other row has the new one.
    let held: i64 = f
        .raw
        .query_one("select count(*) from poison", &[])
        .await
        .map_or(-1, |r| r.get(0));
    let held_poisoned: i64 = f
        .raw
        .query_one(
            "select count(*) from poison p join public.asrc a on a.id::text = p.key \
             where a.x = 2147483646",
            &[],
        )
        .await
        .map_or(-1, |r| r.get(0));
    let off = total_mismatches(&f.raw).await;
    let build_secs = f.build_secs;
    let _ = f.client.shutdown().await;
    PoisonResult {
        rows,
        poisoned,
        shape: cfg.shape.clone(),
        rep: Rep {
            page_secs: pages.sum,
            wall_secs,
            wal_bytes: disk.wal_bytes,
            pg_cpu_secs: cost.pg_cpu_secs,
        },
        outcome,
        held,
        held_poisoned,
        mismatches: off,
        build_secs,
    }
}

/// Runs every poison count in `cfg`, one fresh cluster each.
pub async fn run_poisons(cfg: &OneToOneJoin, tuning: &EngineTuning) -> Vec<PoisonResult> {
    let mut results = Vec::new();
    for &p in &cfg.poisoned {
        let r = run_poison(cfg, tuning, p).await;
        println!("{}", r.to_json());
        results.push(r);
    }
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(outcome: &str, held: i64, held_poisoned: i64, mismatches: i64) -> PoisonResult {
        PoisonResult {
            rows: 100,
            poisoned: 4,
            shape: "light".to_string(),
            rep: Rep::default(),
            outcome: outcome.to_string(),
            held,
            held_poisoned,
            mismatches,
            build_secs: 0.0,
        }
    }

    #[test]
    fn the_two_table_shapes_are_the_ones_every_earlier_run_used() {
        let (fields, columns, oracle) = shape("light", 1);
        assert_eq!(fields, "asrc.x + COALESCE(bsrc.w, 0) AS total");
        assert_eq!(columns.len(), 1);
        assert_eq!(oracle, "a.x + coalesce(b.w, 0)");
        let (fields, columns, oracle) = shape("wide", 1);
        assert_eq!(
            fields,
            "asrc.x + COALESCE(bsrc.w, 0) AS total, bsrc.tag AS tag, \
             asrc.x * 2 + COALESCE(bsrc.w, 0) AS dbl, (asrc.x + COALESCE(bsrc.w, 0)) > 500 AS big"
        );
        assert_eq!(columns.len(), 4);
        assert_eq!(
            oracle,
            "a.x + coalesce(b.w, 0), b.tag, a.x * 2 + coalesce(b.w, 0), \
             (a.x + coalesce(b.w, 0)) > 500"
        );
        assert_eq!(oracle_joins(1), "left join public.bsrc b on a.k = b.id");
    }

    #[test]
    fn a_star_sums_every_position_and_joins_each_on_its_own_key() {
        let (fields, columns, oracle) = shape("light", 3);
        assert_eq!(
            fields,
            "asrc.x + COALESCE(bsrc.w, 0) + COALESCE(bsrc2.w, 0) + COALESCE(bsrc3.w, 0) AS total"
        );
        assert_eq!(columns.len(), 1);
        assert_eq!(
            oracle,
            "a.x + coalesce(b.w, 0) + coalesce(b2.w, 0) + coalesce(b3.w, 0)"
        );
        assert_eq!(
            oracle_joins(3),
            "left join public.bsrc b on a.k = b.id \
             left join public.bsrc2 b2 on a.k2 = b2.id \
             left join public.bsrc3 b3 on a.k3 = b3.id"
        );
        let (_, wide, _) = shape("wide", 3);
        let names: Vec<&str> = wide.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["total", "tag", "tag2", "tag3", "dbl", "big"]);
    }

    #[test]
    #[should_panic(expected = "--fan-out-position must be 1 to --joined (2)")]
    fn a_fan_out_position_past_the_last_joined_table_is_refused() {
        OneToOneJoin {
            fan_outs: vec![10],
            reps: 1,
            probe_reps: 0,
            shape: "light".into(),
            poisoned: vec![],
            poison_rows: 10,
            timeout: Duration::from_secs(1),
            statements: false,
            background_rows: 1,
            analyze_hidden: false,
            joined: 2,
            position: 3,
            root_rows: 10,
            commit_rows: 10,
        }
        .validate();
    }

    #[test]
    fn a_committed_poisoned_page_must_hold_exactly_the_failing_rows() {
        assert!(result("committed", 4, 4, 4).oracle_holds());
        // A good row held in place of a failing one, with the counts unchanged.
        assert!(!result("committed", 4, 3, 4).oracle_holds());
        assert!(!result("committed", 5, 4, 4).oracle_holds());
        assert!(!result("committed", 4, 4, 5).oracle_holds());
    }

    #[test]
    fn a_halt_must_roll_the_whole_fan_out_back_and_a_timeout_fails() {
        assert!(result(HALTED, 0, 0, 100).oracle_holds());
        assert!(!result(HALTED, 0, 0, 99).oracle_holds());
        assert!(!result(HALTED, 1, 1, 100).oracle_holds());
        assert!(!result("timed out", 4, 4, 4).oracle_holds());
        assert!(!result("left live: failed", 0, 0, 100).oracle_holds());
    }
}
