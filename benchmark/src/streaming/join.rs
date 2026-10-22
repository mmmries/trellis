//! `join` (#1113 J8): #1091 J2's scenario through the real engine. What a
//! `JOIN … GROUP BY` transform costs to keep, next to a same-session
//! control that keeps one row per fact instead.
//!
//! # The data (J2's "Data", scaled by `--facts`)
//!
//! `a (id bigint pk, day int /*365 values*/, k int, x bigint /*1% null*/,
//! pad text /*40 chars*/)` holds the facts and `b (id bigint pk, k int
//! unique, gb int /*500 values*/, y bigint /*1% null*/)` the dimension rows.
//! Two shapes ([`Dataset`]):
//!
//! - **D1**: about 2,000 facts per key, plus 4% orphan keys (facts with no
//!   `b` row, unmatched under `LEFT`).
//! - **D2**: about 10 facts per key (the high-cardinality key), plus 5%
//!   orphan keys.
//!
//! Every value is a hash of the row's id, so each database below loads the
//! same rows.
//!
//! # The variants ([`Variant`])
//!
//! Each runs in a database of its own, with a client of its own, in one
//! cluster and one process:
//!
//! - **S1**: `FROM a LEFT JOIN b ON a.k = b.k GROUP BY b.gb SELECT COUNT(*),
//!   COUNT(a.x), SUM(a.x), COUNT(b.y), SUM(b.y)`.
//! - **S2**: the same, `GROUP BY a.day, b.gb`.
//! - **B0**, the control: J2's per-fact baseline, kept by the application.
//!   Next to `a`, `a0` holds every fact again with `gb` and `y` copied onto
//!   it. Every write to `a` makes the same change to `a0` in its
//!   transaction, reading `gb` and `y` from `b`, and a dimension change
//!   rewrites every fact of its key in `a0`: J2's B0 apply, done by the
//!   writer. Trellis keeps two aggregates over `a0`, one per `GROUP BY`
//!   (S1's and S2's), as J2's B0 maintained both targets. Nothing reads `a`
//!   there, so it isn't captured.
//!
//! A `JOIN` builds only over two empty tables until #1109, and every
//! aggregate runs on the group basis (the dev knob, which `join` turns on):
//! the definitions are installed over empty tables, and the data is loaded
//! through capture, in `--load-chunk`-row transactions.
//!
//! # The pages (J2's "Pages")
//!
//! Every page is one transaction, written by the harness on its own
//! connection, so the engine starts on it only once it commits. A page's
//! cost is its write time (`write_s`) plus the time from its commit until
//! a watermark token taken right after it has converged (`apply_s`):
//! `page_s`. B0's dimension pages are mostly write time (the application's
//! rewrite of every fact of the key); a `JOIN`'s are mostly apply time.
//!
//! - **a-side page** (`a_page`): `--a-page` fact changes (20,000), 30%
//!   inserts, 40% `UPDATE x`, 15% `UPDATE k` (to a random key), 15%
//!   `UPDATE day`, in groups of 200 changes with one statement per kind (J2's
//!   500 source transactions of 200 rows, inside one commit). One warm-up page,
//!   then `--a-pages` (5) measured.
//! - **b-side pages** (`b_update`, `b_insert`, `b_delete`): `--b-page`
//!   dimension rows (2% of D1's keys by default, the same number for D2, as
//!   J2 used 1,000 for both): `UPDATE gb`, `INSERT` of an orphan key
//!   (unmatched to matched) and `DELETE` (matched to unmatched). `--b-pages`
//!   (3) update pages, one fewer insert and delete pages.
//!
//! Every page is applied to every variant, in an order that rotates with the
//! page, so drift across the session lands on all of them alike.
//!
//! # Hot keys (J2's measurement 5, D1 only)
//!
//! `--hot-writers` (8) connections write `--hot-pages` (160) pages of 1,000
//! changes onto the first `--hot-keys` (10) keys: 30% inserts, 40% `UPDATE
//! x`, 15% `UPDATE day`, 15% `UPDATE k` among the hot keys, each writer on
//! facts of its own. Twice per variant: alone, and with one more connection
//! moving `gb` of the hot keys' dimension rows between 0 and 1, page after
//! page, until the fact writers finish (then putting it back). Only the
//! variant measured runs its client. Reported:
//! fact changes per second from the first write until the variant
//! converged, how long it took to converge after the last write, the
//! dimension pages that got through, and deadlocks.
//!
//! # Checks
//!
//! After each dataset, every `JOIN` target equals its `JOIN … GROUP BY`
//! query, every B0 target its `GROUP BY` over `a0`, `a0` agrees with `b`, and
//! each `JOIN` target equals the B0 target with the same `GROUP BY` in the
//! other database (the same pages reached both). A failed check fails the
//! run. A summary line says whether D1 kept J2's ordering, S1 < S2 < B0 by
//! median `page_s`, for every page kind; a different ordering is a
//! measurement, not a failure.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::Client as RawClient;
use tokio_postgres::types::ToSql;
use trellis::config::DEFAULT_SCHEMA;
use trellis::dev::defs::ast::ValueType;

use crate::scenario::connect_raw;
use crate::streaming::chain::wait_for_live;
use crate::streaming::contention;
use crate::streaming::disk_tier::json_escape;
use crate::streaming::idle_cost::{wal_bytes_since, wal_lsn};
use crate::streaming::scrape::{self, SumCountSnapshot};
use crate::streaming::tuning::EngineTuning;

/// J2's two data shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dataset {
    D1,
    D2,
}

impl Dataset {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "d1" => Dataset::D1,
            "d2" => Dataset::D2,
            other => panic!("unknown dataset {other:?} (d1, d2)"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Dataset::D1 => "d1",
            Dataset::D2 => "d2",
        }
    }

    fn facts_per_key(self) -> i64 {
        match self {
            Dataset::D1 => 2_000,
            Dataset::D2 => 10,
        }
    }

    /// Orphan keys, in percent of the matched keys.
    fn orphan_percent(self) -> i64 {
        match self {
            Dataset::D1 => 4,
            Dataset::D2 => 5,
        }
    }
}

/// What keeps the aggregate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    S1,
    S2,
    B0,
    /// #1155: S2 with a co-resident 1-1 transform (`c11`) and group-basis
    /// aggregate (`cagg`) on `a`.
    C,
}

impl Variant {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "s1" => Variant::S1,
            "s2" => Variant::S2,
            "b0" => Variant::B0,
            "c" => Variant::C,
            other => panic!("unknown variant {other:?} (s1, s2, b0, c)"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Variant::S1 => "s1",
            Variant::S2 => "s2",
            Variant::B0 => "b0",
            Variant::C => "c",
        }
    }
}

/// #1155's engine arms: a drain-worker count and a scratch prototype.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arm {
    pub threads: usize,
    /// 0 main, 2 `share` (#1230: `FOR SHARE` on `J` for `FROM`-side pages).
    pub proto: u8,
}

impl Arm {
    /// `8`, `8:main`, `8:share`.
    pub fn parse(raw: &str) -> Self {
        let mut parts = raw.trim().split(':');
        let threads = parts
            .next()
            .and_then(|t| t.parse().ok())
            .unwrap_or_else(|| panic!("bad arm {raw:?}"));
        let proto = match parts.next().map(str::to_ascii_lowercase).as_deref() {
            None | Some("main") => 0,
            Some("e3" | "share") => 2,
            Some(other) => panic!("unknown prototype {other:?} (main, share)"),
        };
        Arm { threads, proto }
    }

    pub fn label(self) -> String {
        let proto = ["main", "", "share"][self.proto as usize];
        format!("{}:{proto}", self.threads)
    }

    fn tuning(self, base: &EngineTuning) -> EngineTuning {
        EngineTuning {
            application_threads: self.threads,
            ..base.clone()
        }
    }
}

pub const DEFAULT_VARIANTS: [Variant; 3] = [Variant::S1, Variant::S2, Variant::B0];
pub const DEFAULT_DATASETS: [Dataset; 2] = [Dataset::D1, Dataset::D2];

/// The scenario's knobs; see the module doc.
#[derive(Debug, Clone)]
pub struct JoinBench {
    pub datasets: Vec<Dataset>,
    pub variants: Vec<Variant>,
    pub facts: i64,
    pub a_page: usize,
    pub a_pages: usize,
    /// `None`: 2% of D1's keys.
    pub b_page: Option<usize>,
    pub b_pages: usize,
    pub hot_pages: usize,
    pub hot_writers: usize,
    pub hot_keys: usize,
    pub load_chunk: i64,
    pub seed: u64,
    /// The longest one page, or the load, may take to converge.
    pub timeout: Duration,
    /// #1155: the engine arms every page runs under, interleaved. Empty:
    /// one arm, the tuning's thread count on main.
    pub arms: Vec<Arm>,
    /// The arms the hot-key runs use; `None`: every arm.
    pub hot_arms: Option<Vec<Arm>>,
    /// Which hot-key runs: without and/or with the dimension writer.
    pub hot_dims: Vec<bool>,
    /// Abort when `MemAvailable` falls below this many GiB.
    pub min_free_gib: f64,
}

impl Default for JoinBench {
    fn default() -> Self {
        Self {
            datasets: DEFAULT_DATASETS.to_vec(),
            variants: DEFAULT_VARIANTS.to_vec(),
            facts: 2_000_000,
            a_page: 20_000,
            a_pages: 5,
            b_page: None,
            b_pages: 3,
            hot_pages: 160,
            hot_writers: 8,
            hot_keys: 10,
            load_chunk: 100_000,
            seed: 1113,
            timeout: Duration::from_secs(1_800),
            arms: Vec::new(),
            hot_arms: None,
            hot_dims: vec![false, true],
            min_free_gib: 3.0,
        }
    }
}

/// The changes of one a-side page group (J2's source transaction): one
/// statement per kind.
#[derive(Debug, Clone, Default)]
struct AGroup {
    ins_id: Vec<i64>,
    ins_day: Vec<i32>,
    ins_k: Vec<i32>,
    ins_x: Vec<Option<i64>>,
    x_id: Vec<i64>,
    x: Vec<Option<i64>>,
    k_id: Vec<i64>,
    k: Vec<i32>,
    day_id: Vec<i64>,
    day: Vec<i32>,
}

impl AGroup {
    fn changes(&self) -> usize {
        self.ins_id.len() + self.x_id.len() + self.k_id.len() + self.day_id.len()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BKind {
    Update,
    Insert,
    Delete,
}

impl BKind {
    fn name(self) -> &'static str {
        match self {
            BKind::Update => "b_update",
            BKind::Insert => "b_insert",
            BKind::Delete => "b_delete",
        }
    }
}

/// One b-side page: `keys` with their new `gb` and `y` (an update sets only
/// `gb`; a delete uses neither).
#[derive(Debug, Clone)]
struct BPage {
    kind: BKind,
    keys: Vec<i32>,
    gb: Vec<i32>,
    y: Vec<Option<i64>>,
}

/// Where the generator stands: the shared state every variant's database
/// is in, between pages.
struct Generator {
    rng: StdRng,
    /// The next fact id; every id below it exists.
    next_id: i64,
    /// Keys with facts: the matched ones, then the orphans, below this.
    all_keys: i32,
    /// Matched keys not deleted yet.
    matched: Vec<i32>,
    /// Orphan keys not inserted yet.
    orphans: Vec<i32>,
}

impl Generator {
    fn x(&mut self) -> Option<i64> {
        (self.rng.random_range(0..100) != 0).then(|| self.rng.random_range(0..1_000))
    }

    fn distinct_ids(&mut self, n: usize, taken: &mut HashSet<i64>) -> Vec<i64> {
        let mut ids = Vec::with_capacity(n);
        while ids.len() < n {
            let id = self.rng.random_range(1..self.next_id);
            if taken.insert(id) {
                ids.push(id);
            }
        }
        ids
    }

    /// An a-side page of `changes` changes in groups of [`A_GROUP`].
    fn a_page(&mut self, changes: usize) -> Vec<AGroup> {
        let mut groups = Vec::new();
        let mut left = changes;
        while left > 0 {
            let size = left.min(A_GROUP);
            left -= size;
            let (ins, x, k) = (size * 3 / 10, size * 4 / 10, size * 15 / 100);
            let day = size - ins - x - k;
            let mut taken = HashSet::new();
            let mut g = AGroup {
                x_id: self.distinct_ids(x, &mut taken),
                k_id: self.distinct_ids(k, &mut taken),
                day_id: self.distinct_ids(day, &mut taken),
                ..Default::default()
            };
            g.x = (0..x).map(|_| self.x()).collect();
            g.k = (0..k)
                .map(|_| self.rng.random_range(0..self.all_keys))
                .collect();
            g.day = (0..day).map(|_| self.rng.random_range(0..365)).collect();
            for _ in 0..ins {
                g.ins_id.push(self.next_id);
                self.next_id += 1;
                g.ins_day.push(self.rng.random_range(0..365));
                g.ins_k.push(self.rng.random_range(0..self.all_keys));
                let x = self.x();
                g.ins_x.push(x);
            }
            groups.push(g);
        }
        groups
    }

    /// A b-side page of `rows` rows; matched keys below `protect` (the hot
    /// keys) are never deleted.
    fn b_page(&mut self, kind: BKind, rows: usize, protect: i32) -> BPage {
        let keys: Vec<i32> = match kind {
            BKind::Update => {
                let mut picked = HashSet::new();
                let rows = rows.min(self.matched.len());
                while picked.len() < rows {
                    picked.insert(self.matched[self.rng.random_range(0..self.matched.len())]);
                }
                picked.into_iter().collect()
            }
            BKind::Insert => {
                let n = rows.min(self.orphans.len());
                let keys: Vec<i32> = self.orphans.drain(..n).collect();
                self.matched.extend(&keys);
                keys
            }
            BKind::Delete => {
                let mut keys = Vec::new();
                while keys.len() < rows && self.matched.iter().any(|&k| k >= protect) {
                    let i = self.rng.random_range(0..self.matched.len());
                    if self.matched[i] >= protect {
                        keys.push(self.matched.swap_remove(i));
                    }
                }
                keys
            }
        };
        let mut keys = keys;
        keys.sort_unstable();
        let gb = keys.iter().map(|_| self.rng.random_range(0..500)).collect();
        let y = keys.iter().map(|_| self.x()).collect();
        BPage { kind, keys, gb, y }
    }
}

/// Changes per a-side group: J2's 200-row source transactions.
const A_GROUP: usize = 200;
/// Changes per hot-key page.
const HOT_PAGE: usize = 1_000;
const PAD: &str = "pppppppppppppppppppppppppppppppppppppppp";
const POLL: Duration = Duration::from_millis(2);

/// One variant's database.
struct VariantDb {
    variant: Variant,
    db: TestDatabase,
    /// Reads, the watermark token and the convergence poll.
    raw: RawClient,
    /// Writes pages.
    writer: RawClient,
    client: Option<trellis::Client>,
    targets: Vec<String>,
    /// #1155: co-resident definitions' targets (variant C), not checked.
    extra_targets: Vec<String>,
    /// #1155: the contention sampler's connection.
    sampler: RawClient,
}

/// One timed page.
#[derive(Debug, Clone)]
struct PageTime {
    write_s: f64,
    apply_s: f64,
    wal_bytes: i64,
    changes: usize,
    probe: Probe,
}

/// #1155: what one page's apply window looked like.
#[derive(Debug, Clone, Default)]
struct Probe {
    /// Mean engine backends busy / in a row-lock wait, by table.
    busy_by_table: Vec<(String, f64)>,
    row_lock_by_table: Vec<(String, f64)>,
    busy_mean: f64,
    row_lock_mean: f64,
    running_mean: f64,
    samples: u64,
    /// `trellis_join_lock_wait_seconds`' sum and count over the page.
    j_lock_wait_s: f64,
    j_lock_count: u64,
    /// CPU seconds: this process (the engine and the harness), and the
    /// cluster's backends.
    proc_cpu_s: f64,
    pg_cpu_s: f64,
    deadlocks: i64,
    rollbacks: i64,
    /// Mean `trellis_transform_latency_seconds` per target, ms.
    latency_ms: Vec<(String, Option<f64>)>,
    /// The most-sampled `(wait class, statement)` pairs.
    top: Vec<(String, String, f64)>,
}

impl Probe {
    fn table(v: &[(String, f64)], t: &str) -> f64 {
        v.iter().find(|(k, _)| k == t).map_or(0.0, |(_, m)| *m)
    }

    fn json(&self) -> String {
        let map = |v: &[(String, f64)]| {
            v.iter()
                .map(|(k, m)| format!("\"{k}\":{m:.3}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            "\"samples\":{},\"busy_mean\":{:.3},\"row_lock_mean\":{:.3},\"running_mean\":{:.3},\
             \"busy_by_table\":{{{}}},\"row_lock_by_table\":{{{}}},\"j_lock_wait_s\":{:.4},\
             \"j_lock_count\":{},\"proc_cpu_s\":{:.3},\"pg_cpu_s\":{:.3},\"deadlocks\":{},\
             \"rollbacks\":{},\"latency_ms\":{{{}}},\"top\":[{}]",
            self.samples,
            self.busy_mean,
            self.row_lock_mean,
            self.running_mean,
            map(&self.busy_by_table),
            map(&self.row_lock_by_table),
            self.j_lock_wait_s,
            self.j_lock_count,
            self.proc_cpu_s,
            self.pg_cpu_s,
            self.deadlocks,
            self.rollbacks,
            self.latency_ms
                .iter()
                .map(|(t, m)| format!(
                    "\"{t}\":{}",
                    m.map_or("null".to_string(), |m| format!("{m:.1}"))
                ))
                .collect::<Vec<_>>()
                .join(","),
            self.top
                .iter()
                .map(|(c, q, m)| format!("[\"{c}\",\"{}\",{m:.3}]", json_escape(q)))
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

/// CPU seconds (user + system) of process `pid`, `0` if gone.
fn cpu_s(pid: u32) -> f64 {
    let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return 0.0;
    };
    let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // After `comm`: state is field 3, utime 14 and stime 15 (1-based).
    let ticks: f64 = [11, 12]
        .iter()
        .filter_map(|&i| fields.get(i)?.parse::<f64>().ok())
        .sum();
    ticks / 100.0
}

/// CPU seconds of the postmaster `postmaster`'s children, summed.
fn pg_cpu_s(postmaster: u32) -> f64 {
    let mut total = 0.0;
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return 0.0;
    };
    for entry in dir.flatten() {
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|p| p.parse::<u32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) else {
            continue;
        };
        let fields: Vec<&str> = rest.split_whitespace().collect();
        if fields.get(1).and_then(|p| p.parse::<u32>().ok()) == Some(postmaster) {
            total += cpu_s(pid);
        }
    }
    total
}

/// `MemAvailable` in GiB.
fn mem_available_gib() -> f64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|m| {
            m.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        })
        .map_or(f64::INFINITY, |kb| kb / 1024.0 / 1024.0)
}

fn check_memory(cfg: &JoinBench) {
    let free = mem_available_gib();
    assert!(
        free >= cfg.min_free_gib,
        "ABORT: MemAvailable {free:.1} GiB is below {} GiB",
        cfg.min_free_gib
    );
}

impl PageTime {
    fn page_s(&self) -> f64 {
        self.write_s + self.apply_s
    }
}

fn definitions(variant: Variant) -> Vec<(&'static str, String)> {
    const FIELDS: &str = "COUNT(*) AS n, COUNT(a.x) AS cx, SUM(a.x) AS sx, COUNT(b.y) AS cy, \
                          SUM(b.y) AS sy";
    const B0_FIELDS: &str = "COUNT(*) AS n, COUNT(x) AS cx, SUM(x) AS sx, COUNT(y) AS cy, \
                             SUM(y) AS sy";
    match variant {
        Variant::S1 => vec![(
            "s1",
            format!(
                "TRANSFORM s1 FROM public.a LEFT JOIN public.b ON a.k = b.k GROUP BY b.gb \
                 SELECT b.gb AS gb, {FIELDS}"
            ),
        )],
        Variant::C => {
            let mut defs = definitions(Variant::S2);
            defs.push((
                "c11",
                "TRANSFORM c11 FROM public.a SELECT day AS day, x AS x".to_string(),
            ));
            defs.push((
                "cagg",
                "TRANSFORM cagg FROM public.a GROUP BY day SELECT day AS day, COUNT(*) AS n, \
                 SUM(x) AS sx"
                    .to_string(),
            ));
            defs
        }
        Variant::S2 => vec![(
            "s2",
            format!(
                "TRANSFORM s2 FROM public.a LEFT JOIN public.b ON a.k = b.k \
                 GROUP BY a.day, b.gb SELECT a.day AS day, b.gb AS gb, {FIELDS}"
            ),
        )],
        Variant::B0 => vec![
            (
                "b0_s1",
                format!("TRANSFORM b0_s1 FROM public.a0 GROUP BY gb SELECT gb AS gb, {B0_FIELDS}"),
            ),
            (
                "b0_s2",
                format!(
                    "TRANSFORM b0_s2 FROM public.a0 GROUP BY day, gb \
                     SELECT day AS day, gb AS gb, {B0_FIELDS}"
                ),
            ),
        ],
    }
}

/// The SQL rows of the facts for ids `lo..=hi`: every value a hash of the id.
fn fact_rows(lo: i64, hi: i64, all_keys: i64) -> String {
    const MASK: &str = "9223372036854775807";
    format!(
        "select g as id, mod(hashint8extended(g, 1) & {MASK}, 365)::int as day, \
                mod(hashint8extended(g, 2) & {MASK}, {all_keys})::int as k, \
                case when mod(g, 100) = 0 then null \
                     else mod(hashint8extended(g, 3) & {MASK}, 1000) end as x \
         from generate_series({lo}::bigint, {hi}::bigint) g"
    )
}

async fn timed_converge(raw: &RawClient, timeout: Duration, what: &str) {
    let token = trellis::dev::staging::watermark_token(raw)
        .await
        .expect("take a watermark token");
    let deadline = Instant::now() + timeout;
    loop {
        if trellis::dev::staging::converged_through(raw, token)
            .await
            .expect("check convergence")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} did not converge within {timeout:?}"
        );
        tokio::time::sleep(POLL).await;
    }
}

impl VariantDb {
    async fn start(cluster: &TestCluster, variant: Variant, tuning: &EngineTuning) -> VariantDb {
        let db = cluster.create_isolated_database().await;
        let raw = connect_raw(db.dsn()).await;
        let writer = connect_raw(db.dsn()).await;
        raw.batch_execute(
            "create table public.b (id bigint primary key, k int not null unique, gb int, \
                 y bigint); \
             create table public.a (id bigint primary key, day int not null, \
                 k int not null, x bigint, pad text); \
             create index on public.a (k)",
        )
        .await
        .expect("create the tables");
        if variant == Variant::B0 {
            // J2's B0 table, with its index on `k`: a dimension change finds
            // a key's facts through it.
            raw.batch_execute(
                "create table public.a0 (id bigint primary key, day int not null, \
                     k int not null, x bigint, pad text, gb int, y bigint); \
                 create index on public.a0 (k); \
                 create index on public.a0 (gb); \
                 create index on public.a0 (day, gb)",
            )
            .await
            .expect("create a0");
        }
        let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client");
        let int4 = ValueType::Integer(trellis::IntWidth::Int4);
        let int8 = ValueType::Integer(trellis::IntWidth::Int8);
        let mut columns: std::collections::HashMap<String, ValueType> = [
            ("id", int8),
            ("day", int4),
            ("k", int4),
            ("x", int8),
            ("pad", ValueType::Text),
        ]
        .into_iter()
        .map(|(c, t)| (c.to_string(), t))
        .collect();
        if variant == Variant::B0 {
            columns.insert("gb".to_string(), int4);
            columns.insert("y".to_string(), int8);
        }
        let mut targets = Vec::new();
        let mut extra_targets = Vec::new();
        for (target, text) in definitions(variant) {
            trellis::dev::defs::install_definition(&db.pool, &text, &columns, "public")
                .await
                .unwrap_or_else(|e| panic!("define {target}: {e:?}"));
            wait_for_live(&raw, target, Instant::now() + Duration::from_secs(120)).await;
            if target.starts_with('c') {
                extra_targets.push(target.to_string());
            } else {
                targets.push(target.to_string());
            }
        }
        let sampler = connect_raw(db.dsn()).await;
        VariantDb {
            variant,
            db,
            raw,
            writer,
            client: Some(client),
            targets,
            extra_targets,
            sampler,
        }
    }

    /// Loads `b` then the facts, through capture, and waits for the targets.
    async fn load(&mut self, cfg: &JoinBench, keys: i64, all_keys: i64) -> f64 {
        let started = Instant::now();
        self.writer
            .batch_execute(&format!(
                "insert into public.b (id, k, gb, y) \
                 select k, k, mod(hashint8extended(k, 4) & 9223372036854775807, 500)::int, \
                        case when mod(k, 100) = 0 then null \
                             else mod(hashint8extended(k, 5) & 9223372036854775807, 1000) end \
                 from generate_series(0::bigint, {}::bigint) k",
                keys - 1
            ))
            .await
            .expect("load b");
        let mut lo = 1;
        while lo <= cfg.facts {
            let hi = (lo + cfg.load_chunk - 1).min(cfg.facts);
            let rows = fact_rows(lo, hi, all_keys);
            let mut sql = format!(
                "insert into public.a (id, day, k, x, pad) \
                 select id, day, k, x, '{PAD}' from ({rows}) s"
            );
            if self.variant == Variant::B0 {
                sql.push_str(&format!(
                    "; insert into public.a0 (id, day, k, x, pad, gb, y) \
                     select f.id, f.day, f.k, f.x, f.pad, b.gb, b.y \
                     from public.a f left join public.b on b.k = f.k \
                     where f.id between {lo} and {hi}"
                ));
            }
            self.writer.batch_execute(&sql).await.expect("load facts");
            lo = hi + 1;
        }
        timed_converge(&self.raw, cfg.timeout, "the load").await;
        self.raw
            .batch_execute("vacuum analyze")
            .await
            .expect("vacuum analyze");
        started.elapsed().as_secs_f64()
    }

    /// Writes one a-side page as one transaction and times it.
    async fn a_page(&mut self, page: &[AGroup], timeout: Duration, postmaster: u32) -> PageTime {
        let b0 = self.variant == Variant::B0;
        let wal0 = wal_lsn(&self.raw).await;
        let started = Instant::now();
        let tx = self.writer.transaction().await.expect("begin");
        let mut changes = 0;
        for g in page {
            changes += g.changes();
            write_a_group(&tx, b0, g)
                .await
                .expect("write an a-side group");
        }
        tx.commit().await.expect("commit the page");
        let write_s = started.elapsed().as_secs_f64();
        let (ended, probe) = self
            .converge_probed(timeout, "an a-side page", started, postmaster)
            .await;
        let apply_s = ended - write_s;
        PageTime {
            write_s,
            apply_s,
            wal_bytes: wal_bytes_since(&self.raw, &wal0).await,
            changes,
            probe,
        }
    }

    /// Writes one b-side page as one transaction and times it. B0 also
    /// rewrites every fact of each key.
    async fn b_page(&mut self, page: &BPage, timeout: Duration, postmaster: u32) -> PageTime {
        let b0 = self.variant == Variant::B0;
        let wal0 = wal_lsn(&self.raw).await;
        let started = Instant::now();
        let tx = self.writer.transaction().await.expect("begin");
        match page.kind {
            BKind::Update => {
                tx.execute(
                    "update public.b set gb = v.gb from unnest($1::int[], $2::int[]) v(k, gb) \
                     where b.k = v.k",
                    &[&page.keys, &page.gb],
                )
                .await
                .expect("update gb");
                if b0 {
                    tx.execute(
                        "update public.a0 set gb = v.gb \
                         from unnest($1::int[], $2::int[]) v(k, gb) where a0.k = v.k",
                        &[&page.keys, &page.gb],
                    )
                    .await
                    .expect("rewrite the facts' gb");
                }
            }
            BKind::Insert => {
                tx.execute(
                    "insert into public.b (id, k, gb, y) \
                     select v.k, v.k, v.gb, v.y \
                     from unnest($1::int[], $2::int[], $3::bigint[]) v(k, gb, y)",
                    &[&page.keys, &page.gb, &page.y],
                )
                .await
                .expect("insert dimension rows");
                if b0 {
                    tx.execute(
                        "update public.a0 set gb = v.gb, y = v.y \
                         from unnest($1::int[], $2::int[], $3::bigint[]) v(k, gb, y) \
                         where a0.k = v.k",
                        &[&page.keys, &page.gb, &page.y],
                    )
                    .await
                    .expect("rewrite the facts' gb and y");
                }
            }
            BKind::Delete => {
                tx.execute(
                    "delete from public.b where k = any($1::int[])",
                    &[&page.keys],
                )
                .await
                .expect("delete dimension rows");
                if b0 {
                    tx.execute(
                        "update public.a0 set gb = null, y = null where k = any($1::int[])",
                        &[&page.keys],
                    )
                    .await
                    .expect("clear the facts' gb and y");
                }
            }
        }
        tx.commit().await.expect("commit the page");
        let write_s = started.elapsed().as_secs_f64();
        let (ended, probe) = self
            .converge_probed(timeout, "a b-side page", started, postmaster)
            .await;
        let apply_s = ended - write_s;
        PageTime {
            write_s,
            apply_s,
            wal_bytes: wal_bytes_since(&self.raw, &wal0).await,
            changes: page.keys.len(),
            probe,
        }
    }

    /// #1155: waits for the page to converge while sampling the engine's
    /// backends; returns when it converged (seconds since `started`) and
    /// the window's probe.
    async fn converge_probed(
        &self,
        timeout: Duration,
        what: &str,
        started: Instant,
        postmaster: u32,
    ) -> (f64, Probe) {
        const LATENCY: &str = "trellis_transform_latency_seconds";
        const J_WAIT: &str = "trellis_join_lock_wait_seconds";
        let all: Vec<String> = self
            .targets
            .iter()
            .chain(&self.extra_targets)
            .cloned()
            .collect();
        let before = scrape::scrape();
        let lat0: Vec<SumCountSnapshot> = all
            .iter()
            .map(|t| SumCountSnapshot::capture(&before, LATENCY, t))
            .collect();
        // The J lock metric is labelled with the qualified target.
        let j_label = format!("public.{}", self.targets[0]);
        let j0 = SumCountSnapshot::capture(&before, J_WAIT, &j_label);
        let (dl0, rb0) = contention::deadlocks_and_rollbacks(&self.sampler).await;
        let cpu0 = cpu_s(std::process::id());
        let pg0 = pg_cpu_s(postmaster);
        let done = std::sync::atomic::AtomicBool::new(false);
        let converge = async {
            timed_converge(&self.raw, timeout, what).await;
            let ended = started.elapsed().as_secs_f64();
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            ended
        };
        let sample =
            contention::sample_while_with(&self.sampler, "a", "", &all, Instant::now(), || {
                !done.load(std::sync::atomic::Ordering::Relaxed)
            });
        let (ended, summary) = tokio::join!(converge, sample);
        let cpu1 = cpu_s(std::process::id());
        let pg1 = pg_cpu_s(postmaster);
        let (dl1, rb1) = contention::deadlocks_and_rollbacks(&self.sampler).await;
        let after = scrape::scrape();
        let j = SumCountSnapshot::capture(&after, J_WAIT, &j_label).since(&j0);
        let latency_ms = all
            .iter()
            .zip(&lat0)
            .map(|(t, base)| {
                (
                    t.clone(),
                    SumCountSnapshot::capture(&after, LATENCY, t)
                        .since(base)
                        .mean_ms(),
                )
            })
            .collect();
        let probe = Probe {
            busy_by_table: summary.busy_by_table.clone(),
            row_lock_by_table: summary.row_lock_by_table.clone(),
            busy_mean: summary.engine_busy_mean,
            row_lock_mean: summary.engine_row_lock_mean,
            running_mean: summary.engine_running_mean,
            samples: summary.samples,
            j_lock_wait_s: j.sum,
            j_lock_count: j.count,
            proc_cpu_s: cpu1 - cpu0,
            pg_cpu_s: pg1 - pg0,
            deadlocks: dl1 - dl0,
            rollbacks: rb1 - rb0,
            latency_ms,
            top: summary
                .top_statements
                .iter()
                .map(|(c, q, m)| (c.label().to_string(), q.clone(), *m))
                .collect(),
        };
        (ended, probe)
    }

    /// #1155: starts the client under `arm` and waits until it has nothing
    /// left to apply.
    async fn enter(&mut self, arm: Arm, tuning: &EngineTuning, timeout: Duration) {
        trellis::dev::staging::set_a_side_share(arm.proto == 2);
        self.restart(&arm.tuning(tuning));
        timed_converge(&self.raw, timeout, "the restart").await;
    }

    /// The bytes of what this variant keeps: the `JOIN`'s hidden tables and
    /// target, or B0's per-fact table, its aggregates' hidden tables and
    /// targets.
    async fn state_bytes(&self) -> i64 {
        let mut relations: Vec<String> =
            self.targets.iter().map(|t| format!("public.{t}")).collect();
        if self.variant == Variant::B0 {
            relations.push("public.a0".to_string());
        }
        let ids: Vec<i64> = self
            .raw
            .query("select id from transform_definitions order by id", &[])
            .await
            .expect("read definition ids")
            .iter()
            .map(|r| r.get(0))
            .collect();
        let hidden = if self.variant == Variant::B0 {
            ids.iter()
                .map(|id| format!("^agg_{id}$"))
                .collect::<Vec<_>>()
        } else {
            ids.iter().map(|id| format!("^join_{id}_")).collect()
        };
        let row = self
            .raw
            .query_one(
                "select coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint \
                 from pg_class c join pg_namespace n on n.oid = c.relnamespace \
                 where c.relkind = 'r' and ( \
                     (n.nspname = $1 and c.relname ~ any($2)) \
                     or (n.nspname || '.' || c.relname) = any($3))",
                &[&DEFAULT_SCHEMA, &hidden, &relations],
            )
            .await
            .expect("read state size");
        row.get(0)
    }

    /// Every target against its oracle; returns the mismatches.
    async fn check(&self) -> Vec<String> {
        let mut failures = Vec::new();
        for target in &self.targets {
            let (groups, oracle) = oracle_sql(target);
            let actual = rows(
                &self.raw,
                &format!(
                    "select {groups}, n::text, cx::text, sx::text, cy::text, sy::text \
                     from public.{target}"
                ),
            )
            .await;
            let expected = rows(&self.raw, &oracle).await;
            if actual != expected {
                failures.push(format!(
                    "{}: {target} differs from its oracle ({} rows vs {})",
                    self.variant.name(),
                    actual.len(),
                    expected.len()
                ));
            }
        }
        if self.variant == Variant::B0 {
            let stale = rows(
                &self.raw,
                "select a0.id::text from public.a0 left join public.b on b.k = a0.k \
                 where a0.gb is distinct from b.gb or a0.y is distinct from b.y limit 5",
            )
            .await;
            if !stale.is_empty() {
                failures.push(format!("b0: a0 disagrees with b on facts {stale:?}"));
            }
            let drifted = rows(
                &self.raw,
                "select coalesce(a.id, a0.id)::text from public.a full join public.a0 \
                     on a0.id = a.id \
                 where (a.day, a.k, a.x) is distinct from (a0.day, a0.k, a0.x) limit 5",
            )
            .await;
            if !drifted.is_empty() {
                failures.push(format!("b0: a0 disagrees with a on facts {drifted:?}"));
            }
        }
        failures
    }

    async fn target_rows(&self, target: &str) -> Vec<String> {
        let (groups, _) = oracle_sql(target);
        rows(
            &self.raw,
            &format!(
                "select {groups}, n::text, cx::text, sx::text, cy::text, sy::text \
                 from public.{target}"
            ),
        )
        .await
    }

    async fn shutdown(&mut self) {
        if let Some(client) = self.client.take() {
            client.shutdown().await.expect("client shutdown");
        }
    }

    /// Starts the client again after [`Self::shutdown`].
    fn restart(&mut self, tuning: &EngineTuning) {
        assert!(self.client.is_none(), "the client is already running");
        self.client =
            Some(trellis::Client::start(self.db.dsn(), tuning.client_options()).expect("client"));
    }
}

/// A target's group columns as text, and its oracle query.
fn oracle_sql(target: &str) -> (&'static str, String) {
    const AGGS: &str = "count(*)::text, count(x)::text, sum(x)::text, count(y)::text, \
                        sum(y)::text";
    match target {
        "s1" => (
            "gb::text",
            "select b.gb::text, count(*)::text, count(a.x)::text, sum(a.x)::text, \
                    count(b.y)::text, sum(b.y)::text \
             from public.a left join public.b on a.k = b.k group by b.gb"
                .to_string(),
        ),
        "s2" => (
            "day::text, gb::text",
            "select a.day::text, b.gb::text, count(*)::text, count(a.x)::text, \
                    sum(a.x)::text, count(b.y)::text, sum(b.y)::text \
             from public.a left join public.b on a.k = b.k group by a.day, b.gb"
                .to_string(),
        ),
        "b0_s1" => (
            "gb::text",
            format!("select gb::text, {AGGS} from public.a0 group by gb"),
        ),
        "b0_s2" => (
            "day::text, gb::text",
            format!("select day::text, gb::text, {AGGS} from public.a0 group by day, gb"),
        ),
        other => unreachable!("no target {other}"),
    }
}

/// `sql`'s rows, each rendered as one string, sorted.
async fn rows(raw: &RawClient, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = raw
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    row.get::<_, Option<String>>(i)
                        .unwrap_or_else(|| "NULL".into())
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    out.sort();
    out
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n == 0 {
        f64::NAN
    } else if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// One (dataset, variant, page kind)'s measured pages.
#[derive(Debug, Clone)]
pub struct PageResult {
    pub dataset: Dataset,
    pub variant: Variant,
    pub kind: &'static str,
    times: Vec<PageTime>,
    pub arm: Arm,
}

impl PageResult {
    pub fn median_page_s(&self) -> f64 {
        median(self.times.iter().map(PageTime::page_s).collect())
    }

    fn to_json(&self, common: &str) -> String {
        let page: Vec<f64> = self.times.iter().map(PageTime::page_s).collect();
        let min = page.iter().copied().fold(f64::INFINITY, f64::min);
        let max = page.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let changes = self.times.first().map_or(0, |t| t.changes);
        let apply: Vec<f64> = self.times.iter().map(|t| t.apply_s).collect();
        let amin = apply.iter().copied().fold(f64::INFINITY, f64::min);
        let amax = apply.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let med =
            |f: &dyn Fn(&Probe) -> f64| median(self.times.iter().map(|t| f(&t.probe)).collect());
        let mut tables = Vec::new();
        for t in ["j", "p", "m", "t", "l", "target", "claim", "other"] {
            tables.push(format!(
                "\"row_lock_{t}_median\":{:.3},\"busy_{t}_median\":{:.3}",
                med(&|p| Probe::table(&p.row_lock_by_table, t)),
                med(&|p| Probe::table(&p.busy_by_table, t)),
            ));
        }
        let mut latency = Vec::new();
        if let Some(first) = self.times.first() {
            for (i, (t, _)) in first.probe.latency_ms.iter().enumerate() {
                let v: Vec<f64> = self
                    .times
                    .iter()
                    .filter_map(|p| p.probe.latency_ms.get(i)?.1)
                    .collect();
                latency.push(format!("\"latency_{t}_ms_median\":{:.1}", median(v)));
            }
        }
        format!(
            "{{\"scenario\":\"join\",\"dataset\":\"{}\",\"variant\":\"{}\",\"kind\":\"{}\",\
             \"arm\":\"{}\",\"pages\":{},\"changes_per_page\":{changes},\"page_s_median\":{:.4},\
             \"page_s_min\":{min:.4},\"page_s_max\":{max:.4},\"write_s_median\":{:.4},\
             \"apply_s_median\":{:.4},\"apply_s_min\":{amin:.4},\"apply_s_max\":{amax:.4},\
             \"busy_mean_median\":{:.3},\"row_lock_mean_median\":{:.3},\"running_mean_median\":{:.3},\
             {},\"j_lock_wait_s_median\":{:.4},\"proc_cpu_s_median\":{:.3},\
             \"pg_cpu_s_median\":{:.3},\"deadlocks_total\":{},\"rollbacks_total\":{},{}{}\
             \"wal_bytes_per_change_median\":{:.0},{common}}}",
            self.dataset.name(),
            self.variant.name(),
            self.kind,
            self.arm.label(),
            self.times.len(),
            self.median_page_s(),
            median(self.times.iter().map(|t| t.write_s).collect()),
            median(self.times.iter().map(|t| t.apply_s).collect()),
            med(&|p| p.busy_mean),
            med(&|p| p.row_lock_mean),
            med(&|p| p.running_mean),
            tables.join(","),
            med(&|p| p.j_lock_wait_s),
            med(&|p| p.proc_cpu_s),
            med(&|p| p.pg_cpu_s),
            self.times.iter().map(|t| t.probe.deadlocks).sum::<i64>(),
            self.times.iter().map(|t| t.probe.rollbacks).sum::<i64>(),
            latency.join(","),
            if latency.is_empty() { "" } else { "," },
            median(
                self.times
                    .iter()
                    .map(|t| t.wal_bytes as f64 / t.changes.max(1) as f64)
                    .collect()
            ),
        )
    }
}

/// One hot-key run.
#[derive(Debug, Clone)]
struct HotResult {
    variant: Variant,
    dimension_writer: bool,
    changes: usize,
    write_window_s: f64,
    converge_after_s: f64,
    changes_per_s: f64,
    dimension_pages: usize,
    retries: u64,
    deadlocks: i64,
    /// #1230: WAL bytes, multixact ids and multixact member slots the run used
    /// (a checkpoint's control data before and after).
    wal_bytes: i64,
    mxids: i64,
    mx_members: i64,
    /// #1230: `J` lock waits of the run's pages, by the mode the page locked.
    share_waits: WaitStats,
    update_waits: WaitStats,
}

/// A distribution of waits, in milliseconds.
#[derive(Debug, Default, Clone, Copy)]
struct WaitStats {
    n: usize,
    sum_ms: f64,
    p50_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

impl WaitStats {
    fn of(mut secs: Vec<f64>) -> Self {
        if secs.is_empty() {
            return Self::default();
        }
        secs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        let at = |q: f64| secs[((secs.len() - 1) as f64 * q).round() as usize] * 1000.0;
        WaitStats {
            n: secs.len(),
            sum_ms: secs.iter().sum::<f64>() * 1000.0,
            p50_ms: at(0.5),
            p99_ms: at(0.99),
            max_ms: at(1.0),
        }
    }

    fn json(&self, name: &str) -> String {
        format!(
            "\"{name}_n\":{},\"{name}_sum_ms\":{:.1},\"{name}_p50_ms\":{:.2},\"{name}_p99_ms\":{:.2},\"{name}_max_ms\":{:.2}",
            self.n, self.sum_ms, self.p50_ms, self.p99_ms, self.max_ms
        )
    }
}

/// #1230: the WAL position and multixact counters, after a checkpoint (the
/// control data's next multixact id and offset are as of the last one).
async fn pg_cost_counters(raw: &RawClient) -> (i64, i64, i64) {
    raw.batch_execute("checkpoint").await.expect("checkpoint");
    let row = raw
        .query_one(
            "select pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint, \
                    (select next_multixact_id::text::bigint from pg_control_checkpoint()), \
                    (select next_multi_offset::text::bigint from pg_control_checkpoint())",
            &[],
        )
        .await
        .expect("read the cost counters");
    (row.get(0), row.get(1), row.get(2))
}

/// What [`run`] found: every measurement line it printed, and whether
/// every check passed.
#[derive(Debug, Default)]
pub struct JoinOutcome {
    pub pages: Vec<PageResult>,
    pub failures: Vec<String>,
    /// Source rows written, summed over every database: the loads, then
    /// each page's changes (B0's rewrites of the facts not counted).
    pub rows_written: u64,
}

impl JoinOutcome {
    /// Whether D1 kept J2's ordering, S1 < S2 < B0 by median `page_s`, per
    /// page kind. `None` for a kind not measured on all three.
    pub fn d1_ordering(&self) -> BTreeMap<&'static str, Option<bool>> {
        let mut out = BTreeMap::new();
        for kind in ["a_page", "b_update", "b_insert", "b_delete"] {
            let of = |v: Variant| {
                self.pages
                    .iter()
                    .find(|p| p.dataset == Dataset::D1 && p.variant == v && p.kind == kind)
                    .map(PageResult::median_page_s)
            };
            let kept = match (of(Variant::S1), of(Variant::S2), of(Variant::B0)) {
                (Some(s1), Some(s2), Some(b0)) => Some(s1 < s2 && s2 < b0),
                _ => None,
            };
            out.insert(kind, kept);
        }
        out
    }
}

/// Runs the scenario; prints one JSON line per measurement and a summary.
pub async fn run(cfg: &JoinBench, tuning: &EngineTuning) -> JoinOutcome {
    assert!(
        trellis::dev::staging::group_basis_enabled(),
        "join runs behind the group-basis knob; the CLI turns it on"
    );
    let cluster = TestCluster::start();
    let postmaster: u32 = std::fs::read_to_string(cluster.data_dir().join("postmaster.pid"))
        .ok()
        .and_then(|p| p.lines().next()?.trim().parse().ok())
        .unwrap_or(0);
    let arms: Vec<Arm> = if cfg.arms.is_empty() {
        vec![Arm {
            threads: tuning.application_threads,
            proto: 0,
        }]
    } else {
        cfg.arms.clone()
    };
    let d1_keys = (cfg.facts / Dataset::D1.facts_per_key()).max(10);
    let b_rows = cfg.b_page.unwrap_or_else(|| (d1_keys as usize / 50).max(1));
    let mut outcome = JoinOutcome::default();
    let common = format!(
        "\"facts\":{},\"application_threads\":{},\"maintenance_interval_ms\":{}",
        cfg.facts,
        tuning.application_threads,
        tuning.maintenance_interval.as_millis()
    );
    for &dataset in &cfg.datasets {
        let keys = (cfg.facts / dataset.facts_per_key()).max(10);
        let b_pages = cfg.b_pages;
        let edits = b_pages.saturating_sub(1).max(1);
        // Enough orphans for every insert page: each arm's delete page
        // (before its insert page) hands its keys to the orphans.
        let orphans = (keys * dataset.orphan_percent() / 100).max((edits * b_rows) as i64);
        let all_keys = keys + orphans;
        eprintln!(
            "join: {} — {} facts, {keys} keys + {orphans} orphan keys, b pages of {b_rows} rows, \
             arms {:?}",
            dataset.name(),
            cfg.facts,
            arms.iter().map(|a| a.label()).collect::<Vec<_>>()
        );
        let mut dbs = Vec::new();
        trellis::dev::staging::set_a_side_share(false);
        for &variant in &cfg.variants {
            check_memory(cfg);
            let mut db = VariantDb::start(&cluster, variant, tuning).await;
            let load_s = db.load(cfg, keys, all_keys).await;
            outcome.rows_written += (cfg.facts + keys) as u64;
            let state = db.state_bytes().await;
            db.shutdown().await;
            println!(
                "{{\"scenario\":\"join\",\"dataset\":\"{}\",\"variant\":\"{}\",\"kind\":\"load\",\
                 \"load_s\":{load_s:.3},\"keys\":{keys},\"orphan_keys\":{orphans},\
                 \"state_bytes\":{state},{common}}}",
                dataset.name(),
                variant.name()
            );
            dbs.push(db);
        }

        let mut generator = Generator {
            rng: StdRng::seed_from_u64(cfg.seed ^ dataset.facts_per_key() as u64),
            next_id: cfg.facts + 1,
            all_keys: all_keys as i32,
            matched: (0..keys as i32).collect(),
            orphans: (keys as i32..all_keys as i32).collect(),
        };
        let protect = if dataset == Dataset::D1 {
            cfg.hot_keys as i32
        } else {
            0
        };
        let mut results: BTreeMap<(usize, usize, &'static str), Vec<PageTime>> = BTreeMap::new();
        let mut rotation = 0usize;
        // Rotates an order with every page.
        let order = |rotation: usize, n: usize| -> Vec<usize> {
            (0..n).map(|i| (i + rotation) % n).collect()
        };
        let page_line = |db: &VariantDb, arm: Arm, kind: &str, n: usize, t: &PageTime| {
            println!(
                "{{\"scenario\":\"join\",\"kind\":\"page\",\"dataset\":\"{}\",\"variant\":\"{}\",\
                 \"arm\":\"{}\",\"page_kind\":\"{kind}\",\"page\":{n},\"page_s\":{:.4},\
                 \"write_s\":{:.4},\"apply_s\":{:.4},\"changes\":{},{}}}",
                dataset.name(),
                db.variant.name(),
                arm.label(),
                t.page_s(),
                t.write_s,
                t.apply_s,
                t.changes,
                t.probe.json()
            );
        };
        for page in 0..=cfg.a_pages {
            for ai in order(rotation, arms.len()) {
                let groups = generator.a_page(cfg.a_page);
                for i in order(rotation + ai, dbs.len()) {
                    check_memory(cfg);
                    dbs[i].enter(arms[ai], tuning, cfg.timeout).await;
                    let time = dbs[i].a_page(&groups, cfg.timeout, postmaster).await;
                    dbs[i].shutdown().await;
                    outcome.rows_written += time.changes as u64;
                    page_line(&dbs[i], arms[ai], "a_page", page, &time);
                    if page > 0 {
                        results.entry((i, ai, "a_page")).or_default().push(time);
                    }
                }
            }
            rotation += 1;
        }
        let plan = std::iter::repeat_n(BKind::Update, b_pages)
            .chain(std::iter::repeat_n([BKind::Delete, BKind::Insert], edits).flatten());
        for (n, kind) in plan.enumerate() {
            for ai in order(rotation, arms.len()) {
                let page = generator.b_page(kind, b_rows, protect);
                if kind == BKind::Delete {
                    // Deleted keys are orphans now: a later insert page may
                    // take them.
                    generator.orphans.extend(&page.keys);
                }
                for i in order(rotation + ai, dbs.len()) {
                    check_memory(cfg);
                    dbs[i].enter(arms[ai], tuning, cfg.timeout).await;
                    let time = dbs[i].b_page(&page, cfg.timeout, postmaster).await;
                    dbs[i].shutdown().await;
                    outcome.rows_written += time.changes as u64;
                    page_line(&dbs[i], arms[ai], kind.name(), n, &time);
                    results.entry((i, ai, kind.name())).or_default().push(time);
                }
            }
            rotation += 1;
        }
        for ((i, ai, kind), times) in results {
            let result = PageResult {
                dataset,
                variant: dbs[i].variant,
                kind,
                times,
                arm: arms[ai],
            };
            println!("{}", result.to_json(&common));
            outcome.pages.push(result);
        }

        if dataset == Dataset::D1 && cfg.hot_pages > 0 {
            // Only the variant measured runs a client: every variant's drain
            // workers and the hot writers together are more connections
            // than the cluster takes. Variant C sits the hot-key runs out.
            let hot_arms = cfg.hot_arms.clone().unwrap_or_else(|| arms.clone());
            let reader = dbs
                .iter()
                .position(|d| d.variant != Variant::C)
                .unwrap_or(0);
            for &dimension_writer in &cfg.hot_dims {
                for ai in order(rotation, hot_arms.len()) {
                    let arm = hot_arms[ai];
                    let pages = hot_pages(cfg, &mut generator, &dbs[reader]).await;
                    for i in order(rotation + ai, dbs.len()) {
                        if dbs[i].variant == Variant::C {
                            continue;
                        }
                        check_memory(cfg);
                        dbs[i].enter(arm, tuning, cfg.timeout).await;
                        let hot = run_hot(&mut dbs[i], cfg, &pages, dimension_writer).await;
                        dbs[i].shutdown().await;
                        outcome.rows_written +=
                            (hot.changes + hot.dimension_pages * cfg.hot_keys) as u64;
                        println!(
                            "{{\"scenario\":\"join\",\"dataset\":\"d1\",\"variant\":\"{}\",\
                             \"kind\":\"hot_keys\",\"arm\":\"{}\",\"dimension_writer\":{},\"hot_keys\":{},\
                             \"writers\":{},\"changes\":{},\"write_window_s\":{:.3},\
                             \"converge_after_s\":{:.3},\"changes_per_s\":{:.0},\
                             \"dimension_pages\":{},\"retries\":{},\"deadlocks\":{},\
                             \"wal_bytes\":{},\"mxids\":{},\"mx_members\":{},{},{},{common}}}",
                            hot.variant.name(),
                            arm.label(),
                            hot.dimension_writer,
                            cfg.hot_keys,
                            cfg.hot_writers,
                            hot.changes,
                            hot.write_window_s,
                            hot.converge_after_s,
                            hot.changes_per_s,
                            hot.dimension_pages,
                            hot.retries,
                            hot.deadlocks,
                            hot.wal_bytes,
                            hot.mxids,
                            hot.mx_members,
                            hot.share_waits.json("jshare"),
                            hot.update_waits.json("jupdate")
                        );
                    }
                }
                rotation += 1;
            }
        }
        trellis::dev::staging::set_a_side_share(false);
        for db in &mut dbs {
            db.restart(tuning);
            timed_converge(&db.raw, cfg.timeout, "the final restart").await;
        }

        for db in &dbs {
            outcome.failures.extend(db.check().await);
            let state = db.state_bytes().await;
            println!(
                "{{\"scenario\":\"join\",\"dataset\":\"{}\",\"variant\":\"{}\",\"kind\":\"end\",\
                 \"state_bytes\":{state},{common}}}",
                dataset.name(),
                db.variant.name()
            );
        }
        // The same pages reached every database, so a `JOIN` target equals
        // the B0 target with its `GROUP BY`.
        if let Some(b0) = dbs.iter().find(|d| d.variant == Variant::B0) {
            for db in dbs
                .iter()
                .filter(|d| d.variant != Variant::B0 && d.variant != Variant::C)
            {
                let target = &db.targets[0];
                if db.target_rows(target).await != b0.target_rows(&format!("b0_{target}")).await {
                    outcome.failures.push(format!(
                        "{}: {target} differs from b0_{target}",
                        dataset.name()
                    ));
                }
            }
        }
        for db in &mut dbs {
            db.shutdown().await;
        }
    }
    let ordering = outcome.d1_ordering();
    let ordering_json: Vec<String> = ordering
        .iter()
        .map(|(kind, kept)| {
            format!(
                "\"{kind}\":{}",
                kept.map_or("null".to_string(), |k| k.to_string())
            )
        })
        .collect();
    let all_kept = ordering.values().all(|k| *k == Some(true));
    println!(
        "{{\"scenario\":\"join\",\"kind\":\"summary\",\"d1_ordering\":{{{}}},\
         \"d1_ordering_kept\":{all_kept},\"checks_ok\":{},\"failures\":[{}],\
         \"rows_written\":{},{common}}}",
        ordering_json.join(","),
        outcome.failures.is_empty(),
        outcome
            .failures
            .iter()
            .map(|f| format!("\"{}\"", json_escape(f)))
            .collect::<Vec<_>>()
            .join(","),
        outcome.rows_written
    );
    outcome
}

/// One writer's hot pages, in order.
type WriterPages = Vec<Vec<AGroup>>;

/// The hot-key pages: `cfg.hot_pages` pages of [`HOT_PAGE`] changes on the
/// hot keys, dealt round-robin to `cfg.hot_writers` writers, each writer
/// updating only facts of its own (the hot facts with `id % writers` its
/// index, and the ones it inserted), so writers never wait on each other's
/// rows.
async fn hot_pages(cfg: &JoinBench, generator: &mut Generator, db: &VariantDb) -> Vec<WriterPages> {
    let writers = cfg.hot_writers.max(1);
    let hot = cfg.hot_keys as i32;
    let ids: Vec<i64> = db
        .raw
        .query("select id from public.a where k < $1 order by id", &[&hot])
        .await
        .expect("read the hot facts")
        .iter()
        .map(|r| r.get(0))
        .collect();
    let mut owned: Vec<Vec<i64>> = vec![Vec::new(); writers];
    for id in ids {
        owned[(id as usize) % writers].push(id);
    }
    let mut pages: Vec<WriterPages> = vec![Vec::new(); writers];
    for p in 0..cfg.hot_pages {
        let w = p % writers;
        let (ins, x, day) = (HOT_PAGE * 3 / 10, HOT_PAGE * 4 / 10, HOT_PAGE * 15 / 100);
        let k = HOT_PAGE - ins - x - day;
        let mut g = AGroup::default();
        let mut taken = HashSet::new();
        let pick = |n: usize, rng: &mut StdRng, taken: &mut HashSet<i64>| -> Vec<i64> {
            // The three picks of a page share `taken`, so they cannot take
            // more ids than the writer owns between them.
            let n = n.min(owned[w].len() - taken.len());
            let mut out = Vec::with_capacity(n);
            while out.len() < n {
                let id = owned[w][rng.random_range(0..owned[w].len())];
                if taken.insert(id) {
                    out.push(id);
                }
            }
            out
        };
        g.x_id = pick(x, &mut generator.rng, &mut taken);
        g.day_id = pick(day, &mut generator.rng, &mut taken);
        g.k_id = pick(k, &mut generator.rng, &mut taken);
        g.x = g.x_id.iter().map(|_| generator.x()).collect();
        g.day = g
            .day_id
            .iter()
            .map(|_| generator.rng.random_range(0..365))
            .collect();
        g.k = g
            .k_id
            .iter()
            .map(|_| generator.rng.random_range(0..hot))
            .collect();
        for _ in 0..ins {
            let id = generator.next_id;
            generator.next_id += 1;
            g.ins_id.push(id);
            g.ins_day.push(generator.rng.random_range(0..365));
            g.ins_k.push(generator.rng.random_range(0..hot));
            let x = generator.x();
            g.ins_x.push(x);
            owned[w].push(id);
        }
        pages[w].push(vec![g]);
    }
    pages
}

/// Whether `e` is a deadlock or serialization failure, which the writer
/// retries.
fn retryable(e: &tokio_postgres::Error) -> bool {
    e.code().is_some_and(|c| {
        *c == tokio_postgres::error::SqlState::T_R_DEADLOCK_DETECTED
            || *c == tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE
    })
}

/// Writes `pages` from one connection per writer, and with
/// `dimension_writer` one more connection flipping the hot keys' `gb`
/// until they finish; then waits for the variant to converge.
async fn run_hot(
    db: &mut VariantDb,
    cfg: &JoinBench,
    pages: &[WriterPages],
    dimension_writer: bool,
) -> HotResult {
    let variant = db.variant;
    let dsn = db.db.dsn().to_string();
    let hot = cfg.hot_keys as i32;
    let original: Vec<(i32, Option<i32>)> = db
        .raw
        .query(
            "select k, gb from public.b where k < $1 order by k",
            &[&hot],
        )
        .await
        .expect("read the hot dimension rows")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let (deadlocks0, _) = crate::streaming::contention::deadlocks_and_rollbacks(&db.raw).await;
    let cost0 = pg_cost_counters(&db.raw).await;
    trellis::dev::staging::record_join_lock_waits(true);
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let retries = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let started = Instant::now();
    let mut writers = tokio::task::JoinSet::new();
    for writer_pages in pages {
        let writer_pages = writer_pages.clone();
        let dsn = dsn.clone();
        let retries = Arc::clone(&retries);
        writers.spawn(async move {
            let mut conn = connect_raw(&dsn).await;
            let mut changes = 0;
            for page in &writer_pages {
                loop {
                    match write_hot_page(&mut conn, variant, page).await {
                        Ok(()) => break,
                        Err(e) if retryable(&e) => {
                            retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => panic!("hot page: {e}"),
                    }
                }
                changes += page.iter().map(AGroup::changes).sum::<usize>();
            }
            changes
        });
    }
    let flipper = dimension_writer.then(|| {
        let dsn = dsn.clone();
        let done = Arc::clone(&done);
        let retries = Arc::clone(&retries);
        let keys: Vec<i32> = original.iter().map(|(k, _)| *k).collect();
        tokio::spawn(async move {
            let mut conn = connect_raw(&dsn).await;
            let mut pages = 0usize;
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                let gb: Vec<i32> = keys.iter().map(|_| (pages % 2) as i32).collect();
                match write_dimension_page(&mut conn, variant, &keys, &gb).await {
                    Ok(()) => pages += 1,
                    Err(e) if retryable(&e) => {
                        retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => panic!("dimension page: {e}"),
                }
            }
            pages
        })
    });
    let mut changes = 0;
    while let Some(joined) = writers.join_next().await {
        changes += joined.expect("a hot writer panicked");
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    let dimension_pages = match flipper {
        Some(task) => task.await.expect("the dimension writer panicked"),
        None => 0,
    };
    let write_window_s = started.elapsed().as_secs_f64();
    timed_converge(&db.raw, cfg.timeout, "the hot-key run").await;
    let total = started.elapsed().as_secs_f64();
    let (deadlocks1, _) = crate::streaming::contention::deadlocks_and_rollbacks(&db.raw).await;
    let waits = trellis::dev::staging::take_join_lock_waits();
    trellis::dev::staging::record_join_lock_waits(false);
    let cost1 = pg_cost_counters(&db.raw).await;
    let by_mode = |share: bool| {
        WaitStats::of(
            waits
                .iter()
                .filter(|(s, _)| *s == share)
                .map(|(_, w)| *w)
                .collect(),
        )
    };
    if dimension_writer {
        // Put `gb` back, so every variant's database holds the same rows
        // whatever number of dimension pages got through.
        let keys: Vec<i32> = original.iter().map(|(k, _)| *k).collect();
        let gb: Vec<Option<i32>> = original.iter().map(|(_, g)| *g).collect();
        let gb: Vec<i32> = gb.iter().map(|g| g.unwrap_or(0)).collect();
        write_dimension_page(&mut db.writer, variant, &keys, &gb)
            .await
            .expect("restore the hot dimension rows");
        timed_converge(&db.raw, cfg.timeout, "the restore").await;
    }
    HotResult {
        variant,
        dimension_writer,
        changes,
        write_window_s,
        converge_after_s: total - write_window_s,
        changes_per_s: changes as f64 / total,
        dimension_pages,
        retries: retries.load(std::sync::atomic::Ordering::Relaxed),
        deadlocks: deadlocks1 - deadlocks0,
        wal_bytes: cost1.0 - cost0.0,
        mxids: cost1.1 - cost0.1,
        mx_members: cost1.2 - cost0.2,
        share_waits: by_mode(true),
        update_waits: by_mode(false),
    }
}

async fn write_hot_page(
    conn: &mut RawClient,
    variant: Variant,
    page: &[AGroup],
) -> Result<(), tokio_postgres::Error> {
    let tx = conn.transaction().await?;
    for g in page {
        write_a_group(&tx, variant == Variant::B0, g).await?;
    }
    tx.commit().await
}

/// One a-side group's statements against `a`, and for B0 the same changes
/// to its per-fact copy `a0`, `gb` and `y` read from `b` as the copy keeps
/// them (J2's B0 apply, done by the application in the writing
/// transaction).
async fn write_a_group(
    tx: &tokio_postgres::Transaction<'_>,
    b0: bool,
    g: &AGroup,
) -> Result<(), tokio_postgres::Error> {
    let pad = PAD.to_string();
    let insert: [&(dyn ToSql + Sync); 5] = [&g.ins_id, &g.ins_day, &g.ins_k, &g.ins_x, &pad];
    let mut statements = vec![(
        "insert into public.a (id, day, k, x, pad) select v.id, v.day, v.k, v.x, $5 \
         from unnest($1::bigint[], $2::int[], $3::int[], $4::bigint[]) v(id, day, k, x)",
        "update public.a f set k = v.k from unnest($1::bigint[], $2::int[]) v(id, k) \
         where f.id = v.id",
        "a",
    )];
    if b0 {
        statements.push((
            "insert into public.a0 (id, day, k, x, pad, gb, y) \
             select v.id, v.day, v.k, v.x, $5, b.gb, b.y \
             from unnest($1::bigint[], $2::int[], $3::int[], $4::bigint[]) v(id, day, k, x) \
             left join public.b on b.k = v.k",
            "update public.a0 f set k = s.k, gb = s.gb, y = s.y \
             from (select v.id, v.k, b.gb, b.y from unnest($1::bigint[], $2::int[]) v(id, k) \
                   left join public.b on b.k = v.k) s \
             where f.id = s.id",
            "a0",
        ));
    }
    for (ins, upd_k, table) in statements {
        tx.execute(ins, &insert).await?;
        tx.execute(
            &format!(
                "update public.{table} f set x = v.x \
                 from unnest($1::bigint[], $2::bigint[]) v(id, x) where f.id = v.id"
            ),
            &[&g.x_id, &g.x],
        )
        .await?;
        tx.execute(upd_k, &[&g.k_id, &g.k]).await?;
        tx.execute(
            &format!(
                "update public.{table} f set day = v.day \
                 from unnest($1::bigint[], $2::int[]) v(id, day) where f.id = v.id"
            ),
            &[&g.day_id, &g.day],
        )
        .await?;
    }
    Ok(())
}

async fn write_dimension_page(
    conn: &mut RawClient,
    variant: Variant,
    keys: &[i32],
    gb: &[i32],
) -> Result<(), tokio_postgres::Error> {
    let tx = conn.transaction().await?;
    tx.execute(
        "update public.b set gb = v.gb from unnest($1::int[], $2::int[]) v(k, gb) \
         where b.k = v.k",
        &[&keys, &gb],
    )
    .await?;
    if variant == Variant::B0 {
        tx.execute(
            "update public.a0 set gb = v.gb from unnest($1::int[], $2::int[]) v(k, gb) \
             where a0.k = v.k",
            &[&keys, &gb],
        )
        .await?;
    }
    tx.commit().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generator() -> Generator {
        Generator {
            rng: StdRng::seed_from_u64(7),
            next_id: 1_001,
            all_keys: 25,
            matched: (0..20).collect(),
            orphans: (20..25).collect(),
        }
    }

    /// An a-side page has J2's mix, in groups of 200, every update naming
    /// a fact that exists by then at most once per group, inserts taking
    /// fresh ids.
    #[test]
    fn an_a_side_page_has_j2s_mix() {
        let mut g = generator();
        let page = g.a_page(1_000);
        assert_eq!(page.len(), 5);
        for group in &page {
            assert_eq!(
                (
                    group.ins_id.len(),
                    group.x_id.len(),
                    group.k_id.len(),
                    group.day_id.len()
                ),
                (60, 80, 30, 30)
            );
            let mut ids: Vec<i64> = [&group.x_id, &group.k_id, &group.day_id]
                .into_iter()
                .flatten()
                .copied()
                .collect();
            // Earlier groups' inserts exist by the time a later group runs.
            assert!(ids.iter().all(|&id| (1..1_301).contains(&id)));
            let n = ids.len();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(ids.len(), n);
            assert!(group.k.iter().all(|&k| (0..25).contains(&k)));
        }
        assert_eq!(g.next_id, 1_001 + 300);
    }

    /// Inserts take orphan keys, deletes spare the protected (hot) keys,
    /// and an update names live matched keys only.
    #[test]
    fn b_side_pages_keep_to_their_keys() {
        let mut g = generator();
        let ins = g.b_page(BKind::Insert, 3, 5);
        assert_eq!(ins.keys, [20, 21, 22]);
        let del = g.b_page(BKind::Delete, 10, 5);
        assert!(del.keys.iter().all(|&k| k >= 5));
        let upd = g.b_page(BKind::Update, 12, 5);
        assert!(upd.keys.iter().all(|k| g.matched.contains(k)));
        assert!(upd.keys.iter().all(|k| !del.keys.contains(k)));
    }

    fn result(variant: Variant, kind: &'static str, s: f64) -> PageResult {
        PageResult {
            dataset: Dataset::D1,
            variant,
            kind,
            times: vec![PageTime {
                write_s: s,
                apply_s: 0.0,
                wal_bytes: 0,
                changes: 1,
                probe: Probe::default(),
            }],
            arm: Arm {
                threads: 8,
                proto: 0,
            },
        }
    }

    #[test]
    fn the_d1_ordering_is_s1_then_s2_then_b0_per_kind() {
        let outcome = JoinOutcome {
            pages: vec![
                result(Variant::S1, "a_page", 1.0),
                result(Variant::S2, "a_page", 2.0),
                result(Variant::B0, "a_page", 3.0),
                result(Variant::S1, "b_update", 1.0),
                result(Variant::S2, "b_update", 4.0),
                result(Variant::B0, "b_update", 3.0),
            ],
            failures: Vec::new(),
            rows_written: 0,
        };
        let ordering = outcome.d1_ordering();
        assert_eq!(ordering["a_page"], Some(true));
        assert_eq!(ordering["b_update"], Some(false));
        assert_eq!(ordering["b_insert"], None);
    }
}
