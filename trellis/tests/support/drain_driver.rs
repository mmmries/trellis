//! A hand-driven drain for the interleaving tests.
//!
//! Source writes go through trigger capture, installed by hand with
//! `capture::install`, since the client doesn't install it yet
//! (C5). The driver seals both phases itself, and drains one batch at a time
//! with `drain_once` on a worker name the test picks, optionally frozen at a
//! [`PausePoint`] (`trellis::staging::interleave`). Nothing here waits for
//! convergence. The waits are [`Driver::wait_blocked_behind`], for a backend
//! to queue on a lock a frozen worker holds, which the test has forced, and
//! [`Driver::seal`], for the seal gate's or a full ring's backpressure to
//! clear; neither is a race.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio::task::JoinHandle;
use tokio_postgres::Client;
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::install::{self, Progress};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::harness;

use trellis::staging::interleave::{PausePoint, PauseScope, Reached, with_scope};
use trellis::staging::{
    ApplyError, ApplyOutcome, StagedChange, append, apply, collect_tombstones,
    retire_drained_segments,
};

#[path = "pipeline.rs"]
pub mod pipeline;

#[allow(unused_imports)]
pub use pipeline::{WAKE, int4};

/// Splits `ddl` into its schema statements and its `insert into …` seed rows.
/// A group-basis aggregate over a partitioned source is defined while the
/// source is empty, and seeded once capture has recorded the tree.
pub fn split_seed(ddl: &str) -> (String, String) {
    let (mut schema, mut seed) = (String::new(), String::new());
    for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
        let target = if statement.trim_start().starts_with("insert into") {
            &mut seed
        } else {
            &mut schema
        };
        target.push_str(statement);
        target.push(';');
    }
    (schema, seed)
}

/// Opens a plain connection to `dsn` with the engine's schema on the path.
pub use pipeline::connect;

/// Fields drop in declaration order: the clients first, then the database,
/// then the cluster, so `TestDatabase::drop`'s `dropdb` runs against a live
/// server instead of retrying against a stopped one and leaking.
pub struct Driver {
    /// Seals, stages recomputes and reads the oracle. Never frozen and never
    /// holds a pause lock.
    pub ctl: Client,
    /// Holds the session advisory locks frozen workers wait on, and nothing
    /// else, so it can never be part of a deadlock cycle.
    gate: Client,
    next_lock: AtomicI64,
    /// How long each phase of [`Self::start`] took: the source DDL, the
    /// definitions' install (define, build and backfill), the capture
    /// install, and the settle to quiescence. For measurements.
    pub phases: Vec<(&'static str, Duration)>,
    pub db: TestDatabase,
    pub cluster: TestCluster,
}

impl Driver {
    /// A fresh database with `source_ddl` run, `definitions` installed and
    /// live, trigger capture installed for `captured`, and the pipeline
    /// drained to quiescence.
    pub async fn start(
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_with_database_defaults(&[], source_ddl, columns, definitions, captured).await
    }

    /// [`Self::start`] on a database whose defaults
    /// (`alter database ... set name to value`) include `defaults`. The
    /// engine's pool only opens connections once they are set, so every
    /// session Trellis opens starts from them. `ctl` and `gate` connect
    /// before them and keep the server's defaults: they stand in for the
    /// test, not for the engine. A [`Self::user`] connection starts from
    /// them too, as an application's would.
    pub async fn start_with_database_defaults(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_inner(defaults, source_ddl, columns, definitions, captured, true).await
    }

    /// [`Self::start`] for definitions over tables that hold rows: the
    /// builds are started (`backfilling`, their jobs unclaimed) and left for
    /// the test to run, frozen where the scenario needs it, instead of run to
    /// `live`.
    pub async fn start_unbuilt(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_inner(defaults, source_ddl, columns, definitions, captured, false).await
    }

    async fn start_inner(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
        built: bool,
    ) -> Self {
        let cluster = TestCluster::start();
        let mut db = cluster.create_isolated_database().await;
        let mut ctl = connect(db.dsn()).await;
        let gate = connect(db.dsn()).await;
        if !defaults.is_empty() {
            for (name, value) in defaults {
                ctl.batch_execute(&format!(
                    "alter database \"{}\" set {name} to '{value}'",
                    db.name().replace('"', "\"\"")
                ))
                .await
                .expect("set a database default");
            }
            // Migrating the database opened the pool's connections before
            // the defaults existed; a fresh pool opens its own after them.
            let config =
                trellis::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
            db.pool = trellis::pool::Pool::new(&config).expect("a fresh pool");
        }
        let mut phases = Vec::new();
        let started = Instant::now();
        ctl.batch_execute(source_ddl).await.expect("source DDL");
        phases.push(("source_ddl", started.elapsed()));
        let columns: HashMap<String, ValueType> = columns
            .iter()
            .map(|(name, ty)| (name.to_string(), *ty))
            .collect();
        let started = Instant::now();
        for definition in definitions {
            install_definition(&db.pool, definition, &columns, "public")
                .await
                .expect("install definition");
        }
        phases.push(("define", started.elapsed()));
        let started = Instant::now();
        let catalog = load_catalog(&ctl).await.expect("load the catalog");
        for table in captured {
            let spec = capture_spec(&ctl, &catalog, table)
                .await
                .expect("capture spec");
            match install::reconcile(&mut ctl, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("install capture")
            {
                Progress::Done(_) => {}
                Progress::Waiting(wait) => panic!("an unbounded install waited: {wait}"),
            }
        }
        phases.push(("capture_install", started.elapsed()));
        let started = Instant::now();
        if built {
            harness::settle_builds(&db.pool).await;
        } else {
            harness::start_registered_builds(&db.pool)
                .await
                .expect("start the builds");
        }
        let mut driver = Driver {
            cluster,
            db,
            ctl,
            gate,
            next_lock: AtomicI64::new(623_000),
            phases,
        };
        if !built {
            return driver;
        }
        driver.settle().await;
        let settled = started.elapsed();
        driver.phases.push(("settle", settled));
        let not_live: i64 = driver
            .ctl
            .query_one(
                "select count(*) from transform_definitions where status <> 'live'",
                &[],
            )
            .await
            .expect("read statuses")
            .get(0);
        if not_live != 0 {
            let why = driver
                .rows(
                    "select d.target_table, d.status, f.error, \
                            (select string_agg(b.last_error, '; ') from backfill_chunks b \
                             where b.definition_id = d.id) \
                     from transform_definitions d \
                     left join capture_failures f on f.transform_id = d.id \
                     where d.status <> 'live'",
                )
                .await;
            panic!("every definition is live before the scenario: {why:?}");
        }
        driver
    }

    pub fn pool(&self) -> &trellis::pool::Pool {
        &self.db.pool
    }

    /// How many `JoinKeyMoved` this database's 1-1 `JOIN` pages have raised
    /// (`trellis::dev::interleave::join_key_moves`). It counts this test's
    /// database alone, so tests in one binary run in parallel.
    pub fn join_key_moves(&self) -> u64 {
        trellis::dev::interleave::join_key_moves(self.db.name())
    }

    /// Makes the next `n` held-key checks on this database raise
    /// `JoinKeyMoved` (`trellis::dev::interleave::force_join_key_moves`).
    pub fn force_join_key_moves(&self, n: u64) {
        trellis::dev::interleave::force_join_key_moves(self.db.name(), n);
    }

    /// Sets the 1-1 `JOIN` fan-out chunk on this database; `0` restores the
    /// default.
    pub fn set_fan_out_chunk_rows(&self, rows: usize) {
        trellis::dev::defs::set_one_to_one_join_fan_out_chunk_rows_in(self.db.name(), rows);
    }

    /// Has a 1-1 `JOIN` fan-out chunk on this database wait up to `timeout`
    /// for a lock instead of the engine's one second
    /// (`trellis::dev::build::CHUNK_LOCK_TIMEOUT`): for a test that queues
    /// a chunk behind a worker it froze, so a release that comes late on a
    /// loaded machine doesn't make the chunk give up.
    pub fn set_fan_out_chunk_lock_timeout(&self, timeout: Duration) {
        trellis::dev::defs::set_one_to_one_join_fan_out_chunk_lock_timeout_in(
            self.db.name(),
            Some(timeout),
        );
    }

    /// Sets the 1-1 `JOIN` fan-out warning threshold on this database; `0`
    /// restores the default.
    pub fn set_fan_out_warning_rows(&self, rows: i64) {
        trellis::dev::defs::set_one_to_one_join_fan_out_warning_rows_in(self.db.name(), rows);
    }

    /// A new application connection.
    pub async fn user(&self) -> Client {
        connect(self.db.dsn()).await
    }

    /// Runs `sql` on a new application connection.
    pub async fn write(&self, sql: &str) {
        self.user().await.batch_execute(sql).await.expect(sql);
    }

    /// [`Self::write`]s `sql` and drains it as one sealed batch.
    pub async fn step(&mut self, sql: &str) {
        self.write(sql).await;
        let batch = self.seal().await;
        self.drain(batch, "step").await;
    }

    /// [`Self::write`]s `sql` and [`Self::settle`]s it, and everything it
    /// propagates.
    pub async fn step_settle(&mut self, sql: &str) {
        self.write(sql).await;
        self.settle().await;
    }

    /// [`pipeline::seal`]: seals both phases and returns the sealed batch,
    /// waiting out the seal gate's or a full ring's backpressure.
    pub async fn seal(&mut self) -> i64 {
        pipeline::seal(&mut self.ctl).await
    }

    /// [`pipeline::seal_within`].
    pub async fn seal_within(&mut self, wait: Duration) -> Result<i64, String> {
        pipeline::seal_within(&mut self.ctl, wait).await
    }

    /// [`pipeline::try_seal`].
    pub async fn try_seal(&mut self) -> Option<i64> {
        pipeline::try_seal(&mut self.ctl).await
    }

    /// Stages an image-less `Recompute` (today's Re-derive request) for each
    /// of `keys` of `src_table` into the active segment.
    pub async fn stage_recomputes(&self, src_table: &str, keys: &[&str]) {
        let keys: Vec<(&str, Option<String>)> = keys.iter().map(|key| (*key, None)).collect();
        self.stage_recomputes_with_prior(src_table, &keys).await;
    }

    /// As [`Self::stage_recomputes`], each carrying a prior image (a CDC
    /// text-format row image).
    pub async fn stage_recomputes_with_prior(
        &self,
        src_table: &str,
        keys: &[(&str, Option<String>)],
    ) {
        let changes: Vec<StagedChange> = keys
            .iter()
            .map(|(key, prior_image)| StagedChange::Recompute {
                src_table: src_table.to_string(),
                key: key.to_string(),
                hop_gen: 0,
                src_changed: None,
                prior_image: prior_image.clone(),
                origin_lsn: None,
            })
            .collect();
        let mut client = self.db.pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        append(&txn, &changes).await.expect("stage recomputes");
        txn.commit().await.expect("commit recomputes");
    }

    /// Drains `batch` to completion on worker `worker`, with nothing armed.
    pub async fn drain(&self, batch: i64, worker: &str) -> Option<ApplyOutcome> {
        self.drain_frozen(batch, worker, &[]).await.finish().await
    }

    /// Starts draining `batch` on worker `worker` in its own task, frozen at
    /// each of `points` (a `(point, qualified target)` pair) once reached.
    /// The caller holds each point's lock until [`RunningDrain::release`].
    pub async fn drain_frozen(
        &self,
        batch: i64,
        worker: &str,
        points: &[(PausePoint, &str)],
    ) -> RunningDrain {
        self.spawn_drain(batch, worker, 1, points).await
    }

    /// Starts draining this worker's share of `batch` in its own task, as one
    /// of `live_workers` workers (a batch of at least `MIN_ROWS_TO_SPLIT`
    /// rows is split into buckets, and each worker claims its share).
    pub async fn drain_share(&self, batch: i64, worker: &str, live_workers: i64) -> RunningDrain {
        self.spawn_drain(batch, worker, live_workers, &[]).await
    }

    /// A [`PauseScope`] with each of `points` armed, each on a pause lock the
    /// gate takes now.
    async fn arm(&self, points: &[(PausePoint, &str)]) -> (Arc<PauseScope>, Vec<Frozen>) {
        let scope = PauseScope::new();
        let mut frozen = Vec::with_capacity(points.len());
        for &(point, target) in points {
            let lock_key = self.next_lock.fetch_add(1, Ordering::Relaxed);
            self.gate
                .execute("select pg_advisory_lock($1)", &[&lock_key])
                .await
                .expect("take the pause lock");
            frozen.push(Frozen {
                point,
                lock_key,
                reached: Some(scope.arm(point, target, lock_key)),
            });
        }
        (scope, frozen)
    }

    async fn spawn_drain(
        &self,
        batch: i64,
        worker: &str,
        live_workers: i64,
        points: &[(PausePoint, &str)],
    ) -> RunningDrain {
        let (scope, frozen) = self.arm(points).await;
        // Counted as a drain thread's checkouts are: a drain that holds more
        // than `MAX_BACKGROUND_HELD` connections at once aborts the test.
        let pool = self.db.pool.counting_as_background();
        let worker = worker.to_string();
        // Claims again until the claim wins nothing, so a worker whose peers
        // finished first picks up any bucket left over.
        let handle = tokio::spawn(with_scope(
            scope,
            trellis::pool::count_background_connections_in(async move {
                let mut last = None;
                while let Some(outcome) =
                    apply::drain_once(&pool, batch, &worker, live_workers, WAKE).await?
                {
                    last = Some(outcome);
                }
                Ok(last)
            }),
        ));
        RunningDrain {
            handle: Some(handle),
            frozen,
            ended: None,
        }
    }

    /// Starts `work` (given the pool) in its own task, frozen at each of
    /// `points` once reached, as [`Self::drain_frozen`] does for a drain: for
    /// a writer outside the drain, a column resume or an `ALTER`'s backfill.
    pub async fn run_frozen<T, F, Fut>(&self, points: &[(PausePoint, &str)], work: F) -> Running<T>
    where
        T: Send + 'static,
        F: FnOnce(trellis::pool::Pool) -> Fut,
        Fut: Future<Output = Result<T, ApplyError>> + Send + 'static,
    {
        let (scope, frozen) = self.arm(points).await;
        let handle = tokio::spawn(with_scope(scope, work(self.db.pool.clone())));
        Running {
            handle: Some(handle),
            frozen,
            ended: None,
        }
    }

    /// Releases a frozen worker's pause lock.
    async fn unlock(&self, lock_key: i64) {
        let released: bool = self
            .gate
            .query_one("select pg_advisory_unlock($1)", &[&lock_key])
            .await
            .expect("release the pause lock")
            .get(0);
        assert!(released, "the gate held pause lock {lock_key}");
    }

    /// Waits until some backend is queued on a lock `pid` holds. Used after
    /// starting a second drain whose page must block behind a frozen one, so
    /// the test knows it has reached that lock before letting the first go.
    pub async fn wait_blocked_behind(&self, pid: i32) {
        testkit::wait_blocked_behind(&self.ctl, pid, Duration::from_secs(60), || false).await;
    }

    /// Waits until at least `n` backends are queued on a lock some other
    /// backend holds. Like [`Self::wait_blocked_behind`], for when the test
    /// has forced `n` waiters but not which holder each queues behind.
    pub async fn wait_blocked(&self, n: i64) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let blocked: i64 = self
                .ctl
                .query_one(
                    "select count(*) from pg_stat_activity \
                     where cardinality(pg_blocking_pids(pid)) > 0",
                    &[],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if blocked >= n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{blocked} of {n} backends queued on a lock within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// [`pipeline::settle`]: seals and drains until nothing is pending
    /// anywhere in the ring, running every 1-1 `JOIN`'s pending fan-out to
    /// its end. A bounded deterministic loop, not a wait for convergence.
    pub async fn settle(&mut self) {
        pipeline::settle(&self.db.pool, &mut self.ctl).await;
    }

    /// Runs every 1-1 `JOIN`'s pending fan-out to its end, a chunk at a time
    /// (`trellis::staging::one_to_one_join_fan_out_step`), returning how many
    /// chunks ran.
    pub async fn run_fan_outs(&self) -> usize {
        trellis::staging::one_to_one_join_run_pending_fan_outs(&self.db.pool)
            .await
            .expect("run the pending fan-outs")
    }

    /// Frees the ring slots of drained segments.
    pub async fn retire(&mut self) {
        retire_drained_segments(&mut self.ctl)
            .await
            .expect("retire drained segments");
    }

    /// Collects the tombstones the drained prefix no longer needs,
    /// returning how many went.
    pub async fn collect_tombstones(&mut self) -> u64 {
        collect_tombstones(&mut self.ctl)
            .await
            .expect("collect tombstones")
    }

    /// [`pipeline::rows`]: `sql`'s rows, each rendered as a row literal,
    /// `(1,25,2)`.
    pub async fn rows(&self, sql: &str) -> Vec<String> {
        pipeline::rows(&self.ctl, sql).await
    }

    /// Every `deadlock detected` error the server has logged so far, each
    /// with the lines after it (the `DETAIL` naming the processes and their
    /// statements, the `CONTEXT`, the `STATEMENT`), so a failing assertion
    /// shows which statements formed the cycle.
    pub fn deadlocks_logged(&self) -> Vec<String> {
        const REPORT_LINES: usize = 10;
        let log =
            std::fs::read_to_string(self.cluster.root().join("postgres.log")).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.contains("deadlock detected"))
            .map(|(at, _)| lines[at..(at + REPORT_LINES).min(lines.len())].join("\n"))
            .collect()
    }

    /// Releases `drain`'s pause at `point` once it has been reached.
    pub async fn release<T>(&self, drain: &mut Running<T>, point: PausePoint) {
        let at = drain
            .frozen
            .iter()
            .position(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let frozen = drain.frozen.remove(at);
        self.unlock(frozen.lock_key).await;
    }
}

struct Frozen {
    point: PausePoint,
    lock_key: i64,
    reached: Option<tokio::sync::oneshot::Receiver<Reached>>,
}

/// A drain running in its own task. See [`Driver::drain_frozen`].
pub type RunningDrain = Running<Option<ApplyOutcome>>;

/// A drain or a build chunk running in its own task.
pub struct Running<T> {
    handle: Option<JoinHandle<Result<T, ApplyError>>>,
    frozen: Vec<Frozen>,
    /// What the task returned, once [`Running::reached_or_finished`] saw it
    /// end.
    ended: Option<Result<T, ApplyError>>,
}

impl<T> Running<T> {
    /// Waits until the worker is frozen at `point`. Panics if the drain
    /// finishes (or fails) without reaching it.
    pub async fn reached(&mut self, point: PausePoint) -> Reached {
        let frozen = self
            .frozen
            .iter_mut()
            .find(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let rx = frozen.reached.take().expect("reached() once per point");
        let handle = self.handle.as_mut().expect("drain still running");
        tokio::select! {
            reached = rx => match reached {
                Ok(reached) => reached,
                // The worker dropped its pause scope: it finished, or failed,
                // without reaching `point`. Say how.
                Err(_) => panic!(
                    "the task finished without reaching {point:?}: {:?}",
                    handle.await.expect("task").map(|_| ())
                ),
            },
            finished = &mut *handle => panic!(
                "the task finished without reaching {point:?}: {:?}",
                finished.expect("task").map(|_| ())
            ),
        }
    }

    /// Waits until the worker is frozen at `point` (`Some`), or until it
    /// finishes, or fails, without reaching it (`None`): a worker that rolls
    /// back and gives up before `point`. [`Self::finish_result`] then returns
    /// what it returned.
    #[allow(dead_code)]
    pub async fn reached_or_finished(&mut self, point: PausePoint) -> Option<Reached> {
        let frozen = self
            .frozen
            .iter_mut()
            .find(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let rx = frozen.reached.take().expect("reached() once per point");
        let handle = self.handle.as_mut().expect("drain still running");
        let finished = tokio::select! {
            reached = rx => match reached {
                Ok(reached) => return Some(reached),
                Err(_) => handle.await,
            },
            finished = &mut *handle => finished,
        };
        self.handle = None;
        self.ended = Some(finished.expect("task"));
        None
    }

    /// Whether the task has finished, without waiting for it.
    pub fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(|handle| handle.is_finished())
    }

    /// Kills the task where it is, as a worker that dies: nothing it had not
    /// committed survives, and its claims are left for the test to release.
    /// The armed points stay held, so nothing runs on past one.
    pub async fn kill(mut self) {
        let handle = self.handle.take().expect("task");
        handle.abort();
        let _ = handle.await;
    }

    /// Waits for the task to finish, after every armed point was released.
    pub async fn finish(self) -> T {
        self.finish_result().await.expect("the task failed")
    }

    /// [`Self::finish`], returning the task's error instead of panicking.
    pub async fn finish_result(mut self) -> Result<T, ApplyError> {
        assert!(
            self.frozen.is_empty(),
            "release every armed point before finishing the task"
        );
        if let Some(ended) = self.ended.take() {
            return ended;
        }
        self.handle.take().expect("task").await.expect("task")
    }
}
