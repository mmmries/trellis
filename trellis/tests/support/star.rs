//! The shared fixture for star 1-1 `JOIN` tests (#1460, #1464 step 2a): a
//! root `a` joined to `b1..bP`, one joined table per position, each on its
//! own key column of `a`.
//!
//! A test names a [`Shape`]: how many joined positions the star has, and the
//! position under test (`at`). Bodies are written once against "the joined
//! table at `at`" with [`Shape::sql`]'s placeholders, and the fixture lays out
//! the other positions present, matched and idle:
//!
//! - `a(id, k1..kP, x, y)`, with an index on each `k{p}`;
//! - `b{p}(id, k, w, tag)`, with a unique index on `k`;
//! - every `k{q}` other than `k{at}` defaults to the next value of its own
//!   sequence, from 1001, and `b{q}` holds a row on each of the keys 1001 to
//!   1200. So a row a body inserts with no `k{q}` is matched at position `q`,
//!   on a key no other row has, and no writer of the test touches it.
//!
//! The two-table 1-1 `JOIN` is `Shape { positions: 1, at: 1 }`: `a` and `b1`.
//! `n` is the number of tables, `positions + 1`.
//!
//! Every scenario ends with [`assert_oracle`]: the target equals the full
//! multi-join query, `EXCEPT ALL` both ways; `R` equals `a` and each `M_p`
//! equals `b{p}`; the engine's test-only oracle
//! (`one_to_one_join_diverging_keys`) is clean; and so is a walk of the
//! engine's `self_check` ([`self_check`]), every part at every position.
//!
//! A binary using this module also declares `support/drain_driver.rs` as
//! `mod drain_driver` at its root.

#![allow(dead_code)]

use crate::drain_driver::{self, Driver};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::ApplyError;
use trellis::staging::build::{self, Step, WorkerOptions};
use trellis::staging::interleave::PausePoint;
use trellis::staging::self_check::{
    Divergence, SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page,
};

/// The most joined positions a star may have: 5 tables (#1459's cap).
pub const MAX_POSITIONS: usize = 4;

/// The first key of the sequences the idle positions' keys are drawn from.
const IDLE_KEY_START: i32 = 1001;

/// How many rows each idle position's table holds, on keys from
/// [`IDLE_KEY_START`]: more than any scenario inserts.
const IDLE_ROWS: i32 = 200;

/// One position's join kind: `LEFT` keeps an `a` row with no match there,
/// `INNER` drops it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Left,
    Inner,
}

use Kind::{Inner as I, Left as L};

impl Kind {
    fn letter(self) -> char {
        match self {
            Kind::Left => 'l',
            Kind::Inner => 'i',
        }
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Left => "LEFT JOIN",
            Kind::Inner => "INNER JOIN",
        }
    }

    fn sql_word(self) -> &'static str {
        match self {
            Kind::Left => "left join",
            Kind::Inner => "join",
        }
    }
}

/// The kind tuples a test runs at each number of positions: both kinds at
/// one, the 4 pairs at two (n = 3), and #1459's strength-2 cover at three
/// (n = 4). Four positions (n = 5) take the same construction, all `LEFT`
/// and then one `LEFT` per position among `INNER`s: 5 tuples, the fewest that
/// cover every pair.
pub const KINDS_1: [&[Kind]; 2] = [&[L], &[I]];
pub const KINDS_2: [&[Kind]; 4] = [&[L, L], &[L, I], &[I, L], &[I, I]];
pub const KINDS_3: [&[Kind]; 4] = [&[L, L, L], &[L, I, I], &[I, L, I], &[I, I, L]];
pub const KINDS_4: [&[Kind]; 5] = [
    &[L, L, L, L],
    &[L, I, I, I],
    &[I, L, I, I],
    &[I, I, L, I],
    &[I, I, I, L],
];

/// The shapes a per-position pin runs at: the two-table `JOIN`, and the last
/// position of a three-table star, with position 1 present, matched and
/// idle.
pub const SHAPES: [Shape; 2] = [
    Shape::TWO,
    Shape {
        positions: 2,
        at: 2,
    },
];

/// A star's number of joined positions, and the position under test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    pub positions: usize,
    pub at: usize,
}

impl Shape {
    /// The two-table 1-1 `JOIN`.
    pub const TWO: Shape = Shape {
        positions: 1,
        at: 1,
    };

    pub fn new(positions: usize, at: usize) -> Self {
        assert!(
            (1..=MAX_POSITIONS).contains(&positions) && (1..=positions).contains(&at),
            "no shape has position {at} of {positions}"
        );
        Shape { positions, at }
    }

    /// The other positions, present, matched and idle.
    pub fn idle(self) -> impl Iterator<Item = usize> {
        (1..=self.positions).filter(move |p| *p != self.at)
    }

    pub fn all(self) -> std::ops::RangeInclusive<usize> {
        1..=self.positions
    }

    /// The kind tuples this shape runs.
    pub fn tuples(self) -> &'static [&'static [Kind]] {
        match self.positions {
            1 => &KINDS_1,
            2 => &KINDS_2,
            3 => &KINDS_3,
            4 => &KINDS_4,
            _ => unreachable!("Shape::new bounds positions"),
        }
    }

    /// One star of this shape per kind tuple.
    pub fn stars(self) -> Vec<Star> {
        self.tuples()
            .iter()
            .map(|kinds| Star { shape: self, kinds })
            .collect()
    }

    /// The joined table at position `p`.
    pub fn b_at(self, p: usize) -> String {
        format!("public.b{p}")
    }

    /// The joined table at the position under test.
    pub fn b(self) -> String {
        self.b_at(self.at)
    }

    /// `a`'s key column for the position under test.
    pub fn k(self) -> String {
        format!("k{}", self.at)
    }

    /// `sql` with its placeholders filled for the position under test:
    ///
    /// - `{b}`: the joined table at `at`, `public.b{at}`;
    /// - `{k}`: `a`'s key column for it, `k{at}`;
    /// - `{a_ins}`: `public.a (id, k{at}, x, y)`, so an insert's rows are
    ///   written `(id, k, x, y)` and the idle positions take their defaults.
    pub fn sql(self, sql: &str) -> String {
        sql.replace("{a_ins}", &format!("public.a (id, {}, x, y)", self.k()))
            .replace("{b}", &self.b())
            .replace("{k}", &self.k())
    }

    /// The source tables' DDL, with no rows.
    pub fn ddl(self) -> String {
        let mut ddl = String::new();
        for q in self.idle() {
            ddl.push_str(&format!(
                "create sequence public.a_k{q}_seq start {IDLE_KEY_START}; "
            ));
        }
        let keys: Vec<String> = self
            .all()
            .map(|p| {
                if p == self.at {
                    format!("k{p} integer")
                } else {
                    format!("k{p} integer default nextval('public.a_k{p}_seq')")
                }
            })
            .collect();
        ddl.push_str(&format!(
            "create table public.a (id integer primary key, {}, x integer, y text)",
            keys.join(", ")
        ));
        for p in self.all() {
            ddl.push_str(&format!(
                "; create table public.b{p} (id integer primary key, k integer, w integer, \
                 tag text); \
                 create unique index on public.b{p} (k); \
                 create index on public.a (k{p})"
            ));
        }
        ddl
    }

    /// The seed: `a_rows` `(id, k, x, y)` into `a` with `k` at `at`,
    /// `b_rows` `(id, k, w, tag)` into the table at `at`, and the idle
    /// positions' tables filled.
    pub fn seed(self, a_rows: &str, b_rows: &str) -> String {
        let mut seed = Vec::new();
        for q in self.idle() {
            seed.push(format!(
                "insert into public.b{q} select i, {start} + i - 1, {q} * 1000, 'b{q}_' || i \
                 from generate_series(1, {IDLE_ROWS}) i",
                start = IDLE_KEY_START
            ));
        }
        if !a_rows.is_empty() {
            seed.push(self.sql(&format!("insert into {{a_ins}} values {a_rows}")));
        }
        if !b_rows.is_empty() {
            seed.push(format!("insert into {} values {b_rows}", self.b()));
        }
        seed.join("; ")
    }

    /// What the idle positions add to a matched row's `total`: each idle
    /// table's `w` is its position times 1000.
    pub fn idle_total(self) -> i32 {
        self.idle().map(|q| q as i32 * 1000).sum()
    }

    /// `a`'s columns, as `Driver::start` takes them.
    pub fn columns(self) -> Vec<(String, ValueType)> {
        let int4 = ValueType::Integer(IntWidth::Int4);
        let mut columns = vec![("id".to_string(), int4)];
        columns.extend(self.all().map(|p| (format!("k{p}"), int4)));
        columns.push(("x".to_string(), int4));
        columns.push(("y".to_string(), ValueType::Text));
        columns
    }

    /// Every source table, captured.
    pub fn tables(self) -> Vec<String> {
        let mut tables = vec!["public.a".to_string()];
        tables.extend(self.all().map(|p| self.b_at(p)));
        tables
    }
}

/// One defined star: a shape and a kind per position (`kinds[p - 1]`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Star {
    pub shape: Shape,
    pub kinds: &'static [Kind],
}

impl Star {
    /// The transform's name: `j` and a letter per position, `jl`, `jli`.
    pub fn name(self) -> String {
        std::iter::once('j')
            .chain(self.kinds.iter().map(|k| k.letter()))
            .collect()
    }

    pub fn target(self) -> String {
        format!("public.{}", self.name())
    }

    /// The kind at position `p`.
    pub fn kind(self, p: usize) -> Kind {
        self.kinds[p - 1]
    }

    /// The target's columns, in order: `id, total, tag1..tagP, y`.
    fn target_columns(self) -> String {
        let tags: Vec<String> = self.shape.all().map(|p| format!("tag{p}")).collect();
        format!("id, total, {}, y", tags.join(", "))
    }

    /// The definition text.
    pub fn definition(self) -> String {
        assert_eq!(self.kinds.len(), self.shape.positions, "{self:?}");
        let joins: Vec<String> = self
            .shape
            .all()
            .map(|p| format!("{} public.b{p} ON a.k{p} = b{p}.k", self.kind(p).word()))
            .collect();
        let total: Vec<String> = self
            .shape
            .all()
            .map(|p| format!(" + COALESCE(b{p}.w, 0)"))
            .collect();
        let tags: Vec<String> = self
            .shape
            .all()
            .map(|p| format!(", b{p}.tag AS tag{p}"))
            .collect();
        format!(
            "TRANSFORM {} FROM public.a {} SELECT a.x{} AS total{}, a.y AS y",
            self.name(),
            joins.join(" "),
            total.concat(),
            tags.concat()
        )
    }

    /// The target's rows as the live multi-join computes them, in the
    /// target's column order.
    pub fn expected(self) -> String {
        let joins: Vec<String> = self
            .shape
            .all()
            .map(|p| {
                format!(
                    "{} public.b{p} b{p} on a.k{p} = b{p}.k",
                    self.kind(p).sql_word()
                )
            })
            .collect();
        let total: Vec<String> = self
            .shape
            .all()
            .map(|p| format!(" + coalesce(b{p}.w, 0)"))
            .collect();
        let tags: Vec<String> = self.shape.all().map(|p| format!(", b{p}.tag")).collect();
        format!(
            "select a.id, a.x{} as total{}, a.y from public.a a {}",
            total.concat(),
            tags.concat(),
            joins.join(" ")
        )
    }

    /// The target's rows.
    pub fn actual(self) -> String {
        format!("select {} from {}", self.target_columns(), self.target())
    }
}

/// A database with `a` seeded with `a_rows` and the table at `shape.at`
/// with `b_rows` ([`Shape::seed`]), each of `stars` defined and built, and
/// every table captured.
pub async fn start(stars: &[Star], a_rows: &str, b_rows: &str) -> Driver {
    let (ddl, definitions, columns, tables) = setup(stars, a_rows, b_rows);
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    Driver::start(&ddl, &columns, &definitions, &tables).await
}

/// [`start`] with the builds started and left for the test to run.
pub async fn start_unbuilt(stars: &[Star], a_rows: &str, b_rows: &str) -> Driver {
    let (ddl, definitions, columns, tables) = setup(stars, a_rows, b_rows);
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    Driver::start_unbuilt(&[], &ddl, &columns, &definitions, &tables).await
}

/// A database with the tables of `stars`' shape seeded by `seed` alone (no
/// idle rows: the test writes every table's rows), each of `stars` defined
/// and built, and every table captured. For cross-position pins, whose
/// writers touch more than one position.
pub async fn start_seeded(stars: &[Star], seed: &str) -> Driver {
    let shape = stars.first().expect("a star to define").shape;
    let ddl = format!("{}; {seed}", shape.ddl());
    let definitions: Vec<String> = stars.iter().map(|s| s.definition()).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let columns = shape.columns();
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let tables = shape.tables();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    Driver::start(&ddl, &columns, &definitions, &tables).await
}

/// Runs `sql` on `table` with its capture triggers disabled, in one
/// transaction: a change no entry will ever hear about. (Capture triggers
/// are `ENABLE ALWAYS`, so `session_replication_role = replica` doesn't skip
/// them.)
pub async fn write_uncaptured(d: &Driver, table: &str, sql: &str) {
    let toggle = |action: &str| {
        format!(
            "do $$ declare t text; begin \
                 for t in select tgname from pg_trigger \
                          where tgrelid = '{table}'::regclass and not tgisinternal loop \
                     execute format('alter table {table} {action} trigger %I', t); \
                 end loop; \
             end $$;"
        )
    };
    d.ctl
        .batch_execute(&format!(
            "begin; {} {sql}; {} commit;",
            toggle("disable"),
            toggle("enable always")
        ))
        .await
        .expect("write past capture");
}

/// One writer of a pin: a captured change, a Re-derive of a table's keys,
/// or both in one page.
#[derive(Debug, Clone, Copy)]
pub enum Writer {
    Change(&'static str),
    Rederive(&'static str, &'static [&'static str]),
    Both(&'static str, &'static [&'static str], &'static str),
}

impl Writer {
    /// Writes this writer's changes and seals them into a batch.
    pub async fn seal(self, d: &mut Driver) -> i64 {
        match self {
            Writer::Change(sql) => write(d, sql).await,
            Writer::Rederive(table, keys) => d.stage_recomputes(table, keys).await,
            Writer::Both(table, keys, sql) => {
                write(d, sql).await;
                d.stage_recomputes(table, keys).await;
            }
        }
        d.seal().await
    }
}

/// A two-writer family on every star of `shape` over `seed`
/// ([`start_seeded`]), after the uncaptured `pre` writes `(table, sql)`:
/// each writer in its own batch, either one's page frozen at its commit
/// while the other queues ([`race`]).
pub async fn family_seeded(
    shape: Shape,
    seed: &str,
    pre: &[(&str, &str)],
    first: Writer,
    second: Writer,
) {
    for star in shape.stars() {
        for flip in [false, true] {
            let mut d = start_seeded(&[star], seed).await;
            for (table, sql) in pre {
                write_uncaptured(&d, table, sql).await;
            }
            let one = first.seal(&mut d).await;
            let two = second.seal(&mut d).await;
            let (x, y) = if flip { (two, one) } else { (one, two) };
            race(&mut d, star, x, y).await;
        }
    }
}

/// [`family_seeded`] over [`start`]'s seed: `a_rows` and `b_rows` at
/// `shape.at`, the idle positions filled.
pub async fn family_seeded_at(
    shape: Shape,
    a_rows: &str,
    b_rows: &str,
    first: Writer,
    second: Writer,
) {
    for star in shape.stars() {
        for flip in [false, true] {
            let mut d = start(&[star], a_rows, b_rows).await;
            let one = first.seal(&mut d).await;
            let two = second.seal(&mut d).await;
            let (x, y) = if flip { (two, one) } else { (one, two) };
            race(&mut d, star, x, y).await;
        }
    }
}

type Setup = (String, Vec<String>, Vec<(String, ValueType)>, Vec<String>);

fn setup(stars: &[Star], a_rows: &str, b_rows: &str) -> Setup {
    let shape = stars.first().expect("a star to define").shape;
    assert!(
        stars.iter().all(|s| s.shape == shape),
        "one shape per database: {stars:?}"
    );
    let ddl = format!("{}; {}", shape.ddl(), shape.seed(a_rows, b_rows));
    let definitions = stars.iter().map(|s| s.definition()).collect();
    (ddl, definitions, shape.columns(), shape.tables())
}

pub async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// Writes `sql` and drains it as one sealed batch.
pub async fn step(d: &mut Driver, sql: &str) {
    write(d, sql).await;
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

pub async fn definition_id(d: &Driver, star: Star) -> i64 {
    d.ctl
        .query_one(
            "select id from transform_definitions where target_table = $1",
            &[&star.target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// `star`'s `R`, qualified.
pub async fn r_table(d: &Driver, star: Star) -> String {
    let id = definition_id(d, star).await;
    format!("{DEFAULT_SCHEMA}.join1_{id}_r")
}

/// `star`'s `M` for position `p`, qualified: `_m` at position 1, `_m{p}`
/// after it (the hidden layout per position owns these names).
pub async fn m_table(d: &Driver, star: Star, p: usize) -> String {
    let id = definition_id(d, star).await;
    if p == 1 {
        format!("{DEFAULT_SCHEMA}.join1_{id}_m")
    } else {
        format!("{DEFAULT_SCHEMA}.join1_{id}_m{p}")
    }
}

/// The label a page's `R` pause points carry for `star`'s definition.
pub async fn r_label(d: &Driver, star: Star) -> String {
    r_table(d, star).await
}

/// The label a page's `M` pause points carry for position `p` of `star`.
pub async fn m_label(d: &Driver, star: Star, p: usize) -> String {
    m_table(d, star, p).await
}

/// Asserts `star`'s target equals the live multi-join (`EXCEPT ALL` both
/// ways), `R` holds exactly `a`'s live rows with their keys, and each `M_p`
/// exactly `b{p}`'s. The engine's test-only oracle and a walk of its
/// `self_check` find nothing too.
pub async fn assert_star(d: &Driver, star: Star) {
    let (actual, expected) = (star.actual(), star.expected());
    let extra = d.rows(&format!("({actual}) except all ({expected})")).await;
    let missing = d.rows(&format!("({expected}) except all ({actual})")).await;
    assert!(
        extra.is_empty() && missing.is_empty(),
        "{} target differs from the live join: rows only in the target {extra:?}, \
         rows only in the join {missing:?}",
        star.name()
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &star.target())
        .await
        .expect("the oracle");
    assert!(
        diverging.is_empty(),
        "{} diverges at {diverging:?}",
        star.name()
    );
    let found = self_check(d, &star.target(), SELF_CHECK_PAGE).await;
    assert!(
        found.is_empty(),
        "{} self_check finds {found:?}",
        star.name()
    );
    let keys: Vec<String> = star.shape.all().map(|p| format!("k{p}")).collect();
    let keys = keys.join(", ");
    let r = r_table(d, star).await;
    let held = d
        .rows(&format!(
            "select __from_key, {keys}, x, y from {r} where not __tombstone order by 1"
        ))
        .await;
    let live = d
        .rows(&format!(
            "select id::text, {keys}, x, y from public.a order by id::text"
        ))
        .await;
    assert_eq!(
        held,
        live,
        "{} R (left) differs from a (right)",
        star.name()
    );
    for p in star.shape.all() {
        let m = m_table(d, star, p).await;
        let held = d
            .rows(&format!(
                "select __from_key, k, w, tag from {m} where not __tombstone order by 1"
            ))
            .await;
        let live = d
            .rows(&format!(
                "select id::text, k, w, tag from public.b{p} order by id::text"
            ))
            .await;
        assert_eq!(
            held,
            live,
            "{} M at position {p} (left) differs from b{p} (right)",
            star.name()
        );
    }
}

/// The page size [`assert_star`]'s `self_check` walk takes: every part of a
/// fixture's tables in a page or two.
const SELF_CHECK_PAGE: i64 = 1000;

/// Every divergence a walk of the engine's `self_check` of `target` finds,
/// `limit` keys a page, in `Standard` mode (a key a ring can't vouch for is
/// re-checked after a fresh wait), sorted.
pub async fn self_check(d: &Driver, target: &str, limit: i64) -> Vec<Divergence> {
    let mut after = None;
    let mut found = Vec::new();
    loop {
        let page = check_page(
            d.pool(),
            target,
            SelfCheckScope { after, limit },
            SelfCheckMode::Standard,
            std::time::Duration::from_secs(30),
        )
        .await
        .expect("a page of the self_check");
        match page.outcome {
            SelfCheckOutcome::Converged => {}
            SelfCheckOutcome::Diverged(mut divergences) => found.append(&mut divergences),
            other => panic!("{target}: unexpected self_check outcome {other:?}"),
        }
        after = page.next_after;
        if after.is_none() {
            found.sort_by_key(|d| format!("{d:?}"));
            return found;
        }
    }
}

/// Settles the pipeline and asserts each of `stars`.
pub async fn assert_oracle(d: &mut Driver, stars: &[Star]) {
    d.settle().await;
    for star in stars {
        assert_star(d, *star).await;
    }
}

/// Runs the builds a `TRUNCATE` of a joined table started, then settles and
/// asserts each of `stars`, and that nothing deadlocked.
pub async fn assert_rebuilt(d: &mut Driver, stars: &[Star]) {
    build::settle_builds_with(d.pool(), 3).await;
    assert_oracle(d, stars).await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// A build step's options, with chunks of `chunk_rows`.
pub fn build_options(chunk_rows: i64) -> WorkerOptions {
    WorkerOptions {
        chunk_rows,
        drain_batch_cap: 10_000,
        heartbeat_interval: std::time::Duration::from_secs(1),
        reclaim_ttl: std::time::Duration::from_secs(60),
    }
}

/// One drain worker's build step, as `work_once` takes it.
pub async fn build_step(pool: &trellis::pool::Pool, worker: &str, chunk_rows: i64) -> Step {
    build::work_once(pool, worker, &build_options(chunk_rows))
        .await
        .expect("a build step")
}

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `page` finishes without one having queued (`false`): a page that should
/// queue but runs through then fails on what it wrote, or on its error,
/// rather than on a timeout. Polls as `Driver::wait_blocked_behind` does; a
/// page that queues can't finish first, since `pid` holds its lock.
pub async fn queued_or_finished<T>(d: &Driver, pid: i32, page: &drain_driver::Running<T>) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let blocked: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)))",
                &[&pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if blocked {
            return true;
        }
        if page.is_finished() {
            return false;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the page neither queued behind pid {pid} nor finished within 60 s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The end of a race whose second page didn't queue: it fails with what the
/// page did, the target it left or the error it surfaced.
async fn ran_through<T>(
    d: &mut Driver,
    star: Star,
    result: Result<T, ApplyError>,
    moves: u64,
) -> ! {
    let moved = d.join_key_moves() - moves;
    let name = star.name();
    if let Err(err) = result {
        panic!(
            "{name}: the page ran without queueing on the other's lock and failed \
             ({moved} key-moved rollbacks): {err}"
        );
    }
    assert_oracle(d, &[star]).await;
    panic!(
        "{name}: the page ran without queueing on the other's lock ({moved} key-moved rollbacks)"
    );
}

/// Drains `first` frozen at its commit, then `second`, which must queue on a
/// lock the first holds. Lets both finish, asserts nothing deadlocked, and
/// checks the target.
pub async fn race(d: &mut Driver, star: Star, first: i64, second: i64) {
    let target = star.target();
    let mut held = d
        .drain_frozen(first, "first", &[(PausePoint::BeforeCommit, &target)])
        .await;
    let reached = held.reached(PausePoint::BeforeCommit).await;
    let moves = d.join_key_moves();
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, PausePoint::BeforeCommit).await;
    held.finish().await;
    let result = queued.finish_result().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    if !blocked {
        ran_through(d, star, result, moves).await;
    }
    result.expect("the second page");
    assert_oracle(d, &[star]).await;
}

/// [`race`] for batches that may hold a `TRUNCATE` of a joined table: the
/// second page must queue on the first's lock row, and the builds the
/// truncate started run before the oracle.
pub async fn race_rebuilt(d: &mut Driver, star: Star, name: &str, first: i64, second: i64) {
    let target = star.target();
    let mut held = d
        .drain_frozen(first, "first", &[(PausePoint::BeforeCommit, &target)])
        .await;
    let reached = held.reached(PausePoint::BeforeCommit).await;
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, PausePoint::BeforeCommit).await;
    held.finish().await;
    let result = queued.finish_result().await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
    assert!(
        blocked,
        "{} {name}: the second page ran without queueing on the first's lock row: {result:?}",
        star.name()
    );
    result.expect("the second page");
    assert_rebuilt(d, &[star]).await;
}

/// Which of a family's two writers drains first, frozen at its commit.
#[derive(Debug, Clone, Copy)]
pub enum First {
    A,
    B,
}

/// One #1371 family at `shape`: an `a` writer against a writer of the table
/// at `shape.at`, each a [`Shape::sql`] template, each in its own batch. For
/// every kind tuple, the `a` page frozen at its commit while the other
/// queues, then the other way round.
pub async fn family(shape: Shape, a_rows: &str, b_rows: &str, a_sql: &str, b_sql: &str) {
    let (a_sql, b_sql) = (shape.sql(a_sql), shape.sql(b_sql));
    for star in shape.stars() {
        for first in [First::A, First::B] {
            let mut d = start(&[star], a_rows, b_rows).await;
            write(&d, &a_sql).await;
            let a = d.seal().await;
            write(&d, &b_sql).await;
            let b = d.seal().await;
            match first {
                First::A => race(&mut d, star, a, b).await,
                First::B => race(&mut d, star, b, a).await,
            }
        }
    }
}
