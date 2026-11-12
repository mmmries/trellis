//! A re-type of a 1-1 target's column that invalidates a definition reading
//! it: a `RESUME` that re-types the column pauses the reader define
//! would now refuse. `ALTER TRANSFORM` re-types no column (a copy has no type
//! modifier, ADR-0020), so it refuses no reader here.
//!
//! The re-type is the one a source's `integer` column moved to an unscaled
//! `numeric` makes. Trellis's copy of it becomes an unconstrained `numeric`,
//! which a `date + p` define refuses (`date + numeric` is no operator). A
//! `SUM` over it is admitted: its aggregate is paused for its outgrown
//! `bigint` total and resumed once the copy is re-typed, and its sums stay
//! Postgres's. A source `numeric(p,s)` column re-typed to an unconstrained
//! `numeric` keeps every value's text, so its `SUM` readers neither pause nor
//! rebuild, and go on summing its new scales exactly. The readers are
//! group-basis aggregates. Every step is a bounded, explicit pass or drain;
//! nothing polls for convergence. The one wait, in the lock-order test, is
//! for a backend to block on a lock.

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::{TransformStatus, ValueType};
use trellis::integer::IntWidth;
use trellis::{Trellis, TrellisError};

use drain_driver::pipeline::{bring_live, connect, definer, reconcile_pass, settle, status};
use trellis::staging::build::settle_builds;

#[path = "support/drain_driver.rs"]
mod drain_driver;

/// `public.src (id, g, price int, d date)`, with `t` a passthrough of
/// `price` as `p` and of `d`, and these readers of `t.p`, all live:
///
/// - `roll`: `COUNT(d + p)` per `g` (valid over an integer, refused over a
///   `numeric`: `date + numeric` is no operator);
/// - `sums`: `SUM(p)` and `AVG(p)` per `g` (valid over both);
/// - `cnt`: `COUNT(p)` per `g` (valid over any type);
/// - `c`: a 1-1 passthrough of `p` (valid).
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::pool::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price int, d date); \
         insert into public.src values (1, 1, 3, '2026-01-01'), (2, 1, 4, '2026-01-02')",
    )
    .await
    .expect("tables");
    let trellis = definer(dsn).await;
    trellis
        .apply("TRANSFORM t FROM public.src SELECT g AS g, price AS p, d AS d")
        .await
        .expect("define t");
    bring_live(raw, pool, &["t"]).await;
    for text in [
        "TRANSFORM roll FROM public.t GROUP BY g WITHOUT SOURCE INDEX \
         SELECT COUNT(d + p) AS dates, COUNT(*) AS n",
        "TRANSFORM sums FROM public.t GROUP BY g WITHOUT SOURCE INDEX \
         SELECT SUM(p) AS total, AVG(p) AS mean",
        "TRANSFORM cnt FROM public.t GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(p) AS c",
        "TRANSFORM c FROM public.t SELECT p AS q",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["t", "roll", "sums", "cnt", "c"]).await;
    trellis
}

async fn column_type(raw: &Client, table: &str, column: &str) -> String {
    raw.query_one(
        "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
         where attrelid = pg_catalog.to_regclass($1) and attname = $2",
        &[&table, &column],
    )
    .await
    .unwrap_or_else(|err| panic!("{table}.{column}: {err}"))
    .get(0)
}

/// Every row of `sql`, each column as text.
async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
        .iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// `sums`' rows beside Postgres's own `sum()` and `avg()` over `table`'s
/// `column`, as text.
async fn assert_sums_exact(raw: &Client, target: &str, table: &str, column: &str) {
    assert_eq!(
        rows(
            raw,
            &format!("select g::text, total::text, mean::text from public.{target} order by g")
        )
        .await,
        rows(
            raw,
            &format!(
                "select g::text, sum({column})::text, avg({column})::text from {table} \
                 group by g order by g"
            )
        )
        .await,
        "{target} over {table}.{column}"
    );
}

/// A `RESUME` that re-types `t.p` pauses `roll`, with
/// define's refusal as the reason naming the re-type, and its own
/// `RESUME` refuses. `sums` goes on, its sums Postgres's over the
/// re-typed column, and `cnt` and `c` stay valid and are not paused by it.
#[tokio::test]
async fn a_resume_that_retypes_a_column_pauses_the_readers_define_would_refuse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    reconcile_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    // `t` is rebuilt and live again before a reader's resume is judged.
    bring_live(&mut raw, &db.pool, &["t"]).await;

    assert_eq!(column_type(&raw, "public.t", "p").await, "numeric");
    for reader in ["roll"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert_eq!(reported.status, TransformStatus::Paused, "{reader}");
        let error = reported.capture_failure.expect("a reason").error;
        assert!(
            error.contains("from integer to numeric"),
            "{reader} names the upstream re-type: {error}"
        );
        match trellis.apply(&format!("RESUME TRANSFORM {reader}")).await {
            Err(TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused {
                ..
            })) => {}
            other => panic!("the resume of {reader} should be refused: {other:?}"),
        }
    }
    // `sums` follows the copy to `numeric` and sums its new scales.
    raw.batch_execute("insert into public.src values (3, 1, 0.125, '2026-01-03')")
        .await
        .expect("a value at a new scale");
    bring_live(&mut raw, &db.pool, &["t", "sums", "cnt", "c"]).await;
    assert_sums_exact(&raw, "sums", "public.t", "p").await;
    assert_eq!(column_type(&raw, "public.sums", "total").await, "numeric");
    for reader in ["sums", "cnt", "c"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert_eq!(
            reported.status,
            TransformStatus::Live,
            "{reader}: {reported:?}"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

/// Waits (bounded) until some backend waits on a lock `pid` holds: the
/// interleaving test's one wait for a lock-wait state, not for convergence.
async fn wait_blocked_behind(raw: &Client, pid: i32) {
    testkit::blocking::wait_blocked_behind(raw, pid, Duration::from_secs(30), || false).await;
}

/// Lock order: the re-type takes the readers' definition rows
/// before the target's `ACCESS EXCLUSIVE` lock. A transaction holding the
/// reader's row (`roll`'s, as its build or an `ALTER TRANSFORM roll` would)
/// and then reading the target `t` is not deadlocked by the resume's
/// re-type: the re-type waits on the row holding nothing on `t`, the
/// holder reads `t` and commits, and the same pass then re-types `t.p` and
/// pauses `roll`. Taking the rows after the re-type (holding `t`) deadlocks
/// here, and Postgres aborts one side.
#[tokio::test]
async fn a_resume_retype_takes_reader_rows_before_the_target_lock() {
    resume_retype_against_a_reader_row_holder(Duration::ZERO, false).await;
}

/// The same interleaving with the holder stalled past the re-type's
/// `lock_timeout` while the pass waits on `roll`'s row (what a box under load
/// does to the test above): the pass gives the re-type up for its next
/// one, and that one re-types `t.p`, pausing `roll`.
#[tokio::test]
async fn a_resume_retype_whose_row_wait_times_out_is_retried_by_the_next_pass() {
    // `staging::quarantine::RETYPE_LOCK_TIMEOUT` is 2 s. The test checks
    // that the first pass did time out, so a longer timeout fails it here
    // rather than leaving it to pass without a retry.
    resume_retype_against_a_reader_row_holder(Duration::from_millis(2500), true).await;
}

/// The last re-type failure recorded on `t` (empty if none).
async fn t_failure(raw: &Client) -> String {
    raw.query_one(
        "select coalesce(string_agg(f.error, ' | '), '') \
         from capture_failures f join transform_definitions d on d.id = f.transform_id \
         where split_part(d.target_table, '.', 2) = 't'",
        &[],
    )
    .await
    .expect("read t's capture_failure")
    .get(0)
}

async fn resume_retype_against_a_reader_row_holder(stall: Duration, expect_retry: bool) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    reconcile_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");

    let holder = connect(db.dsn()).await;
    let holder_pid: i32 = holder
        .query_one("select pg_catalog.pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    holder
        .batch_execute(
            "begin; set local lock_timeout = '10s'; \
             select 1 from transform_definitions \
             where split_part(target_table, '.', 2) = 'roll' for update",
        )
        .await
        .expect("hold roll's definition row");

    let pool = db.pool.clone();
    let dsn = db.dsn().to_string();
    let pass = tokio::spawn(async move {
        let mut pass_client = connect(&dsn).await;
        reconcile_pass(&mut pass_client, &pool).await;
    });
    // The pass's re-type of `t.p` now waits on `roll`'s row.
    wait_blocked_behind(&raw, holder_pid).await;
    tokio::time::sleep(stall).await;
    holder
        .batch_execute("select count(*) from public.t")
        .await
        .expect("the holder reads the target without a deadlock");
    holder.batch_execute("commit").await.expect("commit");
    pass.await.expect("the pass");
    if expect_retry {
        assert_eq!(
            column_type(&raw, "public.t", "p").await,
            "integer",
            "the stalled holder outlasted the re-type's lock_timeout: the first pass left it"
        );
        let failure = t_failure(&raw).await;
        assert!(failure.contains("lock timeout"), "{failure}");
    }

    // The re-type's wait on `roll`'s row is capped by its 2 s lock_timeout
    // (`RETYPE_LOCK_TIMEOUT`); a box so loaded that this task stalls past it
    // between the pass blocking and the holder committing makes the pass
    // leave the re-type for its next one, which is the engine's
    // contract, not the ordering bug. What the ordering bug produces is a
    // deadlock, which Postgres breaks by aborting a side: the holder (its
    // read above would have failed) or the pass, whose recorded last error
    // then says so. So: a bounded number of explicit further passes, each
    // only after a failure that was the lock timeout (a deadlock, or any
    // other failure, fails the test).
    for _ in 0..4 {
        if column_type(&raw, "public.t", "p").await == "numeric" {
            break;
        }
        let failure = t_failure(&raw).await;
        assert!(
            failure.contains("lock timeout") && !failure.contains("deadlock"),
            "the re-type failed other than by its lock_timeout (a deadlock is the ordering \
             bug): {failure}"
        );
        reconcile_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(
        column_type(&raw, "public.t", "p").await,
        "numeric",
        "t.p was re-typed within a few passes"
    );
    assert_eq!(status(&raw, "roll").await, TransformStatus::Paused);
    trellis.shutdown().await.expect("shutdown");
}

/// The same down a chain `t -> c -> roll2`: `t`'s resume re-types `t.p`,
/// which pauses `c` (its copy of `p` is outgrown); `c`'s chained
/// resume re-types `c.q`, which pauses `roll2`, a `COUNT(d + q)` define
/// would now refuse. `c` goes live again, and so does `sums2`, a `SUM(q)`,
/// its sums Postgres's over the re-typed `c.q`.
#[tokio::test]
async fn a_chained_resume_retype_pauses_the_reader_define_would_refuse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price int, d date); \
         insert into public.src values (1, 1, 3, '2026-01-01')",
    )
    .await
    .expect("tables");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM t FROM public.src SELECT g AS g, price AS p, d AS d")
        .await
        .expect("define t");
    bring_live(&mut raw, &db.pool, &["t"]).await;
    trellis
        .apply("TRANSFORM c FROM public.t SELECT g AS g, p AS q, d AS d")
        .await
        .expect("define c");
    bring_live(&mut raw, &db.pool, &["t", "c"]).await;
    for text in [
        "TRANSFORM roll2 FROM public.c GROUP BY g WITHOUT SOURCE INDEX \
         SELECT COUNT(d + q) AS dates",
        "TRANSFORM sums2 FROM public.c GROUP BY g WITHOUT SOURCE INDEX \
         SELECT SUM(q) AS total, AVG(q) AS mean",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["t", "c", "roll2", "sums2"]).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    reconcile_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    bring_live(&mut raw, &db.pool, &["t", "c"]).await;

    assert_eq!(column_type(&raw, "public.c", "q").await, "numeric");
    let reported = trellis
        .status("roll2")
        .await
        .expect("status")
        .expect("roll2");
    assert_eq!(reported.status, TransformStatus::Paused);
    let error = reported.capture_failure.expect("a reason").error;
    assert!(
        error.contains("public.c.q from integer to numeric"),
        "{error}"
    );
    raw.batch_execute("insert into public.src values (2, 1, 2.50, '2026-01-02')")
        .await
        .expect("a value at a new scale");
    bring_live(&mut raw, &db.pool, &["t", "c", "sums2"]).await;
    assert_sums_exact(&raw, "sums2", "public.c", "q").await;
    trellis.shutdown().await.expect("shutdown");
}

/// The captured-source path, and the Decision's retype: a source
/// `numeric(10,2)` column a group-basis `SUM`/`AVG` and a `JOIN`'s `SUM`
/// read is re-typed to an unconstrained `numeric`. Every value keeps its
/// text and the partials already count the scale `2` (one layout for every
/// `numeric`), so the capture pass neither pauses nor rebuilds either; then
/// a value at scale 5 enters and leaves, and the sums follow Postgres's text
/// both ways.
#[tokio::test]
async fn a_source_retype_to_unscaled_numeric_keeps_its_sum_readers_exact() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price numeric(10,2)); \
         create table public.o (id int primary key, g int); \
         insert into public.src values (1, 1, 1.25), (2, 1, 2.50); \
         insert into public.o values (1, 1)",
    )
    .await
    .expect("tables");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(price) AS total, AVG(price) AS mean",
        )
        .await
        .expect("define agg");
    trellis
        .apply(
            "TRANSFORM j FROM public.src JOIN public.o ON src.g = o.g GROUP BY src.g \
             WITHOUT SOURCE INDEX SELECT src.g AS g, SUM(src.price) AS total, \
             AVG(src.price) AS mean",
        )
        .await
        .expect("define j");
    bring_live(&mut raw, &db.pool, &["agg", "j"]).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    reconcile_pass(&mut raw, &db.pool).await;
    for reader in ["agg", "j"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert_eq!(
            reported.status,
            TransformStatus::Live,
            "{reader} neither pauses nor rebuilds: {reported:?}"
        );
        assert_sums_exact(&raw, reader, "public.src", "price").await;
    }
    raw.batch_execute("insert into public.src values (3, 1, 1.12345)")
        .await
        .expect("a value at scale 5");
    settle(&db.pool, &mut raw).await;
    for reader in ["agg", "j"] {
        assert_sums_exact(&raw, reader, "public.src", "price").await;
    }
    raw.batch_execute("delete from public.src where id = 3")
        .await
        .expect("the value leaves");
    settle(&db.pool, &mut raw).await;
    for reader in ["agg", "j"] {
        assert_eq!(
            status(&raw, reader).await,
            TransformStatus::Live,
            "{reader}"
        );
        assert_sums_exact(&raw, reader, "public.src", "price").await;
    }
    trellis.shutdown().await.expect("shutdown");
}

/// The narrowed R1 for a change between two declared scales: a `SUM`
/// over `numeric(10,2)` re-typed to `numeric(12,4)` pauses nothing, and the
/// capture pass rebuilds the aggregate on its own, whose total and average
/// are Postgres's text over the re-scaled values (`3.7500`, equal to the
/// `3.75` the target held).
#[tokio::test]
async fn a_scale_change_between_two_declared_scales_rebuilds_the_sum_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price numeric(10,2)); \
         insert into public.src values (1, 1, 1.25), (2, 1, 2.50)",
    )
    .await
    .expect("table");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(price) AS total, AVG(price) AS mean",
        )
        .await
        .expect("define agg");
    bring_live(&mut raw, &db.pool, &["agg"]).await;

    raw.batch_execute("alter table public.src alter column price type numeric(12,4)")
        .await
        .expect("re-scale the source");
    bring_live(&mut raw, &db.pool, &["agg"]).await;
    assert_eq!(status(&raw, "agg").await, TransformStatus::Live);
    assert_sums_exact(&raw, "agg", "public.src", "price").await;
    trellis.shutdown().await.expect("shutdown");
}

/// After define: a `SUM` argument re-typed from `integer`
/// to `double precision` pauses its aggregate on the capture pass, and the
/// resume refuses with define's family refusal until the column is
/// an admitted type again.
#[tokio::test]
async fn a_source_retype_to_a_float_pauses_its_sum_reader_and_the_resume_refuses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute("create table public.src (id int primary key, g int, q int)")
        .await
        .expect("table");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(q) AS s")
        .await
        .expect("define agg");
    bring_live(&mut raw, &db.pool, &["agg"]).await;

    raw.batch_execute("alter table public.src alter column q type double precision")
        .await
        .expect("retype q");
    reconcile_pass(&mut raw, &db.pool).await;
    let reported = trellis.status("agg").await.expect("status").expect("agg");
    assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");

    match trellis.apply("RESUME TRANSFORM agg").await {
        Err(err) => assert!(
            err.to_string().contains("float and interval SUM/AVG"),
            "{err}"
        ),
        Ok(applied) => panic!("the resume should be refused: {applied:?}"),
    }
    trellis.shutdown().await.expect("shutdown");
}

/// A `COALESCE` constant outside `real`'s range defines over a `double
/// precision` column, where it is in range, and an `ALTER TRANSFORM` adding
/// one over a `real` column is accepted too: define evaluates no constant.
/// Re-typing the first column to `real` pauses the transform for the
/// re-type, as any re-type of a read column does; the resume re-types and
/// rebuilds, and the rebuilt row, which Postgres now fails while planning,
/// is quarantined with Postgres's error, like any per-row error.
#[tokio::test]
async fn a_coalesce_constant_out_of_range_after_a_retype_to_real_quarantines_its_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id int primary key, x double precision, r real); \
         insert into public.src values (1, 1.5, 2.5)",
    )
    .await
    .expect("table");
    let trellis = definer(db.dsn()).await;
    let beyond_real = format!("1{}", "0".repeat(39));
    trellis
        .apply(&format!(
            "TRANSFORM one FROM public.src SELECT COALESCE(x, {beyond_real}) AS v"
        ))
        .await
        .expect("define one");
    bring_live(&mut raw, &db.pool, &["one"]).await;
    trellis
        .apply(&format!(
            "ALTER TRANSFORM one ADD COALESCE(r, {beyond_real}) AS w"
        ))
        .await
        .expect("an ALTER evaluates no constant either");
    trellis
        .apply("ALTER TRANSFORM one DROP w")
        .await
        .expect("drop the added field");
    bring_live(&mut raw, &db.pool, &["one"]).await;

    raw.batch_execute("alter table public.src alter column x type real")
        .await
        .expect("retype x");
    reconcile_pass(&mut raw, &db.pool).await;
    let reported = trellis.status("one").await.expect("status").expect("one");
    assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");

    trellis
        .apply("RESUME TRANSFORM one")
        .await
        .expect("the resume re-validates and accepts");
    reconcile_pass(&mut raw, &db.pool).await;
    settle_builds(&db.pool).await;
    let held: Vec<(String, String, Option<String>)> = raw
        .query("select key, last_error, sqlstate from poison", &[])
        .await
        .expect("read poison")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    assert_eq!(held.len(), 1, "{held:?}");
    let (key, error, sqlstate) = &held[0];
    assert_eq!(key, "1");
    assert_eq!(sqlstate.as_deref(), Some("22003"));
    assert!(error.contains("is out of range for type real"), "{error}");
    trellis.shutdown().await.expect("shutdown");
}

/// An exact constant past `double precision` beside a `numeric` column is
/// computed as `numeric`, so it defines. Re-typing the column to `double
/// precision` pauses the transform for the re-type, and the resume accepts
/// it (define evaluates no constant). The resume's re-type of the target's
/// column then fails on the value the target already holds, which `double
/// precision` can't represent, and the transform stays paused with
/// Postgres's error.
#[tokio::test]
async fn a_constant_beside_a_column_retyped_to_a_float_pauses_on_the_targets_re_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.src (id int primary key, n numeric); \
         insert into public.src values (1, 1.5)",
    )
    .await
    .expect("table");
    let trellis = definer(db.dsn()).await;
    let beyond_double = "9".repeat(310);
    trellis
        .apply(&format!(
            "TRANSFORM one FROM public.src SELECT n + {beyond_double} AS v"
        ))
        .await
        .expect("define one");
    bring_live(&mut raw, &db.pool, &["one"]).await;

    raw.batch_execute("alter table public.src alter column n type double precision")
        .await
        .expect("retype n");
    reconcile_pass(&mut raw, &db.pool).await;
    let reported = trellis.status("one").await.expect("status").expect("one");
    assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");

    trellis
        .apply("RESUME TRANSFORM one")
        .await
        .expect("the resume re-validates and accepts");
    reconcile_pass(&mut raw, &db.pool).await;
    let reported = trellis.status("one").await.expect("status").expect("one");
    assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");
    let failure = reported
        .capture_failure
        .expect("the pause names its reason");
    assert!(
        failure.error.contains("couldn't re-type")
            && failure
                .error
                .contains("is out of range for type double precision"),
        "{}",
        failure.error
    );
    trellis.shutdown().await.expect("shutdown");
}

/// An alter that changes a column's type within the string
/// family leaves a `JOIN` key pair `t.k = o.k` valid (Trellis's copy of a
/// `varchar(n)` column is unbounded, and `varchar` and `text` pair),
/// so the alter is accepted and the `JOIN` is untouched.
#[tokio::test]
async fn an_alter_that_keeps_a_join_key_pair_valid_is_accepted() {
    let int4 = ValueType::Integer(IntWidth::Int4);
    let d = Driver::start(
        "create table public.src (id int primary key, g int, name varchar(10)); \
         create table public.o (id int primary key, k varchar)",
        &[("id", int4), ("g", int4), ("name", ValueType::Text)],
        &["TRANSFORM t FROM public.src SELECT g AS g, name AS k"],
        &["public.src", "public.o"],
    )
    .await;
    let columns: HashMap<String, ValueType> = [
        ("id".to_string(), int4),
        ("g".to_string(), int4),
        ("k".to_string(), ValueType::Text),
    ]
    .into();
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM j FROM public.t JOIN public.o ON t.k = o.k GROUP BY t.g \
         WITHOUT SOURCE INDEX SELECT t.g AS g, COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("define the JOIN");
    let trellis::defs::Statement::AlterTransform(alter) =
        trellis::defs::parse_statement("ALTER TRANSFORM t ALTER k AS COALESCE(name, 'x')")
            .expect("parses")
    else {
        panic!("not an ALTER");
    };
    trellis::defs::alter_transform(d.pool(), &alter)
        .await
        .expect("the alter keeps the key pair valid");
    assert_eq!(
        d.rows("select status from transform_definitions where target_table = 'public.j'")
            .await,
        ["(waiting_to_backfill)"],
        "the JOIN is untouched"
    );
}
