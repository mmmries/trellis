//! Resume after a source key change, on the group basis and for a `JOIN`: a source, or a `JOIN`'s
//! joined table, dropped and created again with another primary key while
//! its readers were paused. The resume recreates the hidden tables these
//! keep and rebuilds from the tables now at the names; a change staged
//! under the old key before the drop is never decoded against the new one.
//! And a `JOIN` whose `GROUP BY` key moved from `varchar` to `text` while paused.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{
    RECREATED, SOURCE, assert_staged, bring_live, capture_pass, rows, settle, setup, status,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SUMS: &str =
    "TRANSFORM c_sums FROM public.c GROUP BY pid WITHOUT SOURCE INDEX SELECT SUM(amount) AS total";
const SUMS_ACTUAL: &str = "select pid::text, total::text from public.c_sums order by 1";
const SUMS_ORACLE: &str =
    "select pid::text, sum(amount)::text from public.c group by pid order by 1";

/// The aggregate on the group basis, with a change staged
/// under the old key and not yet drained.
#[tokio::test]
async fn a_group_basis_aggregate_resumes_onto_a_source_recreated_with_a_wider_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute("insert into public.c values (4, 8, 1); delete from public.c where id = 1")
        .await
        .unwrap();
    settle(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// A plain source converted to a partitioned one, whose key must hold the
/// partition key, on the group basis.
#[tokio::test]
async fn a_group_basis_aggregate_resumes_onto_its_source_converted_to_a_partitioned_table() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)) partition by list (pid); \
         create table public.c_7 partition of public.c for values in (7); \
         create table public.c_8 partition of public.c for values in (8); \
         create index on public.c (pid); \
         insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (4, 8, 1), (5, 7, 2); delete from public.c where id = 1",
    )
    .await
    .unwrap();
    settle(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

const JOINED: &str = "TRANSFORM jl FROM public.c LEFT JOIN public.p ON c.pid = p.pid \
     GROUP BY c.pid WITHOUT SOURCE INDEX SELECT c.pid AS pid, COUNT(*) AS n, SUM(p.w) AS sw";
const JOINED_ACTUAL: &str = "select pid::text, n::text, sw::text from public.jl order by 1";
const JOINED_ORACLE: &str = "select c.pid::text, count(*)::text, sum(p.w)::text \
     from public.c left join public.p on c.pid = p.pid group by c.pid order by 1";

/// A `JOIN` whose joined table is dropped and created again with a wider
/// key, with a change to it staged under the old key, and then its `FROM`
/// table the same way.
#[tokio::test]
async fn a_join_resumes_onto_tables_recreated_with_wider_keys() {
    let cluster = TestCluster::start();
    let source = format!(
        "{SOURCE}; \
         create table public.p (id int primary key, pid int not null, w int); \
         create index on public.p (pid); \
         insert into public.p select i, i % 3, 10 * i from generate_series(1, 4) i"
    );
    let (db, mut raw, trellis) = setup(&cluster, &source, &[JOINED]).await;
    raw.batch_execute("insert into public.p values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.p; \
         create table public.p (id int not null, pid int not null, w int, \
             primary key (pid, id)); \
         create index on public.p (pid); \
         insert into public.p values (1, 1, 7), (2, 2, 8), (3, 2, 9)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(status(&trellis, "jl").await.status, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM jl").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["jl"]).await;
    assert_eq!(
        rows(&raw, JOINED_ACTUAL).await,
        rows(&raw, JOINED_ORACLE).await
    );

    raw.batch_execute("insert into public.c values (100, 2, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)); \
         create index on public.c (pid); \
         insert into public.c values (1, 1, 40), (2, 2, 60), (3, 7, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM jl").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["jl"]).await;
    raw.batch_execute(
        "insert into public.c values (4, 2, 1); delete from public.p where id = 1; \
         insert into public.p values (5, 7, 3)",
    )
    .await
    .unwrap();
    settle(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, JOINED_ACTUAL).await,
        rows(&raw, JOINED_ORACLE).await
    );
}

/// A `JOIN` grouped by a `varchar(5)` column that becomes `text` while it is
/// paused. The resume creates `T` again at `text` and keeps the target's
/// `varchar` (one string family), so the build compares two tables whose
/// group columns differ in type: it matches every held row, rewrites none it
/// needn't, and deletes the row of a group the paused writes emptied.
#[tokio::test]
async fn a_join_resumes_after_its_varchar_group_key_becomes_text() {
    let cluster = TestCluster::start();
    let source = "create table public.a (id int primary key, g varchar(5), k int, x int); \
         create table public.b (id int primary key, k int, y int); \
         create index on public.a (k); create index on public.b (k); \
         insert into public.a select i, case when i % 4 = 0 then null else 'g' || (i % 3) end, \
             i % 5, i from generate_series(1, 40) i; \
         insert into public.b select i, i % 5, i from generate_series(1, 10) i";
    let definition = "TRANSFORM by_g FROM public.a LEFT JOIN public.b ON a.k = b.k \
         GROUP BY a.g WITHOUT SOURCE INDEX SELECT a.g AS g, COUNT(*) AS n, SUM(b.y) AS sy";
    let actual = "select g::text, n::text, sy::text from public.by_g order by 1";
    let oracle = "select a.g::text, count(*)::text, sum(b.y)::text \
         from public.a a left join public.b b on a.k = b.k group by a.g order by 1";
    let (db, mut raw, trellis) = setup(&cluster, source, &[definition]).await;
    trellis.apply("PAUSE TRANSFORM by_g").await.unwrap();
    raw.batch_execute(
        "alter table public.a alter column g type text; \
         delete from public.a where g = 'g2'; update public.a set x = 0 where g = 'g1'",
    )
    .await
    .unwrap();
    trellis.apply("RESUME TRANSFORM by_g").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["by_g"]).await;
    assert_eq!(rows(&raw, actual).await, rows(&raw, oracle).await);
    raw.batch_execute("insert into public.a values (41, 'a-longer-group', 1, 1)")
        .await
        .unwrap();
    settle(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, actual).await, rows(&raw, oracle).await);
}
