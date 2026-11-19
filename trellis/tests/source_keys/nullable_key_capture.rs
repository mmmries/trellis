//! A nullable `KEY (...)` column on a `NULLS NOT DISTINCT` index (ADR-0026,
//! #2126): capture keys a `NULL` key part exactly as the build does
//! (`chr(1)`, a real `chr(1)` doubled), so the `NULL`-keyed row is a row of
//! its own, apart from the `''` row, through inserts, updates and deletes,
//! for a 1-1, an aggregate and a 1-1 `JOIN`'s joined table. Capture functions
//! a 0.3.0 install left behind for such a table are regenerated on attach.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for
//! convergence.

use std::time::Duration;

use crate::support::resume_passes;

use resume_passes::{reconcile_pass, rows, settle, setup, status};
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::defs::TransformStatus;
use trellis::staging::self_check::CheckPageExt;
use trellis::{SelfCheckMode, SelfCheckOutcome, Trellis};

/// `target` is live, equals its oracle, and `self_check` finds nothing.
async fn exact(trellis: &Trellis, raw: &Client, target: &str, actual: &str, oracle: &str) {
    let reported = status(trellis, target).await;
    assert_eq!(
        reported.status,
        TransformStatus::Live,
        "{target}: {reported:?}"
    );
    assert_eq!(reported.drain_failure, None, "{target}");
    assert_eq!(reported.held_keys, None, "{target}");
    assert_eq!(rows(raw, actual).await, rows(raw, oracle).await, "{target}");
    let report = trellis
        .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(30))
        .await
        .expect("self_check");
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "{target}: {:?}",
        report.outcome
    );
}

const SINGLE: &str = "create table public.nk (code text, g int not null, v int not null); \
     create unique index on public.nk (code) nulls not distinct; \
     insert into public.nk values ('', 0, 2), ('a', 1, 3), (chr(1), 0, 4), \
         ('x' || chr(1) || chr(1), 1, 5)";

const NK_ONE: &str = "TRANSFORM nk1 FROM public.nk KEY (code) SELECT v AS v";
const NK_ONE_ACTUAL: &str =
    "select (code is null)::text, code, v::text from public.nk1 order by 1, code collate \"C\"";
const NK_ONE_ORACLE: &str =
    "select (code is null)::text, code, v::text from public.nk order by 1, code collate \"C\"";

const NK_AGG: &str = "TRANSFORM nk_sums FROM public.nk KEY (code) GROUP BY g WITHOUT SOURCE INDEX \
     SELECT COUNT(*) AS n, SUM(v) AS total";
const NK_AGG_ACTUAL: &str = "select g::text, n::text, total::text from public.nk_sums order by g";
const NK_AGG_ORACLE: &str =
    "select g::text, count(*)::text, sum(v)::text from public.nk group by g order by g";

async fn single_exact(trellis: &Trellis, raw: &Client) {
    exact(trellis, raw, "nk1", NK_ONE_ACTUAL, NK_ONE_ORACLE).await;
    exact(trellis, raw, "nk_sums", NK_AGG_ACTUAL, NK_AGG_ORACLE).await;
}

/// The issue's repro: insert, update and delete of the `NULL`-keyed row next
/// to a `''` row and rows holding `chr(1)`, for a 1-1 and an aggregate.
#[tokio::test]
async fn a_null_key_is_a_row_of_its_own_through_every_write() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SINGLE, &[NK_ONE, NK_AGG]).await;
    single_exact(&trellis, &raw).await;

    for write in [
        "insert into public.nk values (null, 1, 1)",
        "update public.nk set v = 10 where code is null",
        "update public.nk set v = 20 where code = chr(1)",
        "update public.nk set g = 0 where code is null",
        // A key move onto and off the NULL key.
        "update public.nk set code = 'b' where code is null",
        "update public.nk set code = null where code = 'b'",
        "delete from public.nk where code is null",
        "insert into public.nk values (null, 0, 7)",
        "update public.nk set v = v + 1",
        "delete from public.nk where code = chr(1)",
        // Two captures in one statement: the second re-reads the live row.
        "insert into public.nk values (null, 1, 8), ('', 1, 9) \
         on conflict (code) do update set v = excluded.v",
        "with d as (delete from public.nk where code is null returning *) \
         insert into public.nk select null, g, v + 1 from d",
        "with u as (update public.nk set v = v + 1 where code is null returning *) \
         update public.nk set v = v + 100 where code = ''",
    ] {
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        single_exact(&trellis, &raw).await;
    }
}

const COMPOSITE: &str = "create table public.nk2 (a text, b text, g int not null, v int not null); \
     create unique index on public.nk2 (a, b) nulls not distinct; \
     insert into public.nk2 values (null, 'x', 0, 1), ('', 'x', 1, 2), ('a', null, 0, 3), \
         ('a', '', 1, 4), (null, null, 0, 5), ('', '', 1, 6), (chr(1), null, 0, 7), \
         (chr(1) || chr(1), chr(1), 1, 8); \
     create table public.nk3 (a int not null, b text, v int not null); \
     create unique index on public.nk3 (a, b) nulls not distinct; \
     insert into public.nk3 values (1, null, 1), (1, '', 2), (2, null, 3)";

const NK2_ONE: &str = "TRANSFORM nk2_one FROM public.nk2 KEY (a, b) SELECT v AS v";
const NK2_ONE_ACTUAL: &str = "select (a is null)::text, a, (b is null)::text, b, v::text from public.nk2_one \
     order by 1, 2, 3, 4";
const NK2_ONE_ORACLE: &str = "select (a is null)::text, a, (b is null)::text, b, v::text from public.nk2 \
     order by 1, 2, 3, 4";

const NK2_AGG: &str = "TRANSFORM nk2_sums FROM public.nk2 KEY (a, b) GROUP BY g \
     WITHOUT SOURCE INDEX SELECT COUNT(*) AS n, SUM(v) AS total";
const NK2_AGG_ACTUAL: &str = "select g::text, n::text, total::text from public.nk2_sums order by g";
const NK2_AGG_ORACLE: &str =
    "select g::text, count(*)::text, sum(v)::text from public.nk2 group by g order by g";

/// A key with a `NOT NULL` column beside a nullable one.
const NK3_ONE: &str = "TRANSFORM nk3_one FROM public.nk3 KEY (a, b) SELECT v AS v";
const NK3_ONE_ACTUAL: &str =
    "select a::text, (b is null)::text, b, v::text from public.nk3_one order by 1, 2, 3";
const NK3_ONE_ORACLE: &str =
    "select a::text, (b is null)::text, b, v::text from public.nk3 order by 1, 2, 3";

async fn composite_exact(trellis: &Trellis, raw: &Client) {
    exact(trellis, raw, "nk2_one", NK2_ONE_ACTUAL, NK2_ONE_ORACLE).await;
    exact(trellis, raw, "nk3_one", NK3_ONE_ACTUAL, NK3_ONE_ORACLE).await;
    exact(trellis, raw, "nk2_sums", NK2_AGG_ACTUAL, NK2_AGG_ORACLE).await;
}

/// The issue's composite repro, and every mix of `NULL` and `''` parts.
#[tokio::test]
async fn a_composite_keys_null_parts_are_apart_from_empty_ones() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, COMPOSITE, &[NK2_ONE, NK2_AGG, NK3_ONE]).await;
    composite_exact(&trellis, &raw).await;

    for write in [
        "update public.nk2 set v = 10 where a is null and b = 'x'",
        "update public.nk2 set v = 30 where b is null",
        "update public.nk2 set v = 50 where a is null and b is null",
        "insert into public.nk2 values (null, '', 0, 7), ('', null, 1, 8)",
        "update public.nk2 set g = 1 - g where a is null or b is null",
        "update public.nk2 set b = 'y' where a is null and b is null",
        "delete from public.nk2 where a is null",
        "delete from public.nk2 where b is null",
        "insert into public.nk2 values (null, null, 1, 9)",
        "update public.nk2 set v = v + 1 where a like chr(1) || '%'",
        "delete from public.nk2 where a = chr(1)",
        "insert into public.nk2 values (null, null, 0, 10), (null, 'x', 1, 11), ('', null, 0, 12) \
         on conflict (a, b) do update set v = excluded.v",
        "with d as (delete from public.nk2 where a is null returning *) \
         insert into public.nk2 select a, b, g, v + 1 from d",
        "update public.nk3 set v = 10 where b is null",
        "insert into public.nk3 values (3, null, 4), (1, null, 5) \
         on conflict (a, b) do update set v = excluded.v",
        "delete from public.nk3 where a = 1 and b is null",
        "with d as (delete from public.nk3 where b is null returning *) \
         insert into public.nk3 select a, b, v + 1 from d",
    ] {
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        composite_exact(&trellis, &raw).await;
    }
}

const JOINED: &str = "create table public.a (id int primary key, k text, v int); \
     create table public.b (code text, label text not null); \
     create unique index on public.b (code) nulls not distinct; \
     create index on public.a (k); \
     insert into public.a values (1, 'x', 1), (2, '', 2), (3, null, 3), (4, chr(1), 4); \
     insert into public.b values ('x', 'X'), ('', 'EMPTY'), (null, 'NULL'), (chr(1), 'ONE')";

const J1: &str = "TRANSFORM j1 FROM public.a LEFT JOIN public.b KEY (code) ON a.k = b.code \
     SELECT a.v AS v, b.label AS label";
const J1_ACTUAL: &str = "select id::text, v::text, label from public.j1 order by id";
const J1_ORACLE: &str = "select a.id::text, a.v::text, b.label from public.a \
     left join public.b on a.k = b.code order by a.id";

const JG: &str = "TRANSFORM jg FROM public.a JOIN public.b KEY (code) ON a.k = b.code \
     GROUP BY b.label SELECT COUNT(*) AS n, SUM(a.v) AS total";
const JG_ACTUAL: &str = "select label, n::text, total::text from public.jg order by label";
const JG_ORACLE: &str = "select b.label, count(*)::text, sum(a.v)::text from public.a \
     join public.b on a.k = b.code group by b.label order by b.label";

/// The issue comment's repro: deleting the `NULL`-keyed joined row unjoins
/// nothing, and the `''` row's `a` rows keep their label, in a 1-1 `JOIN`
/// and a `JOIN` aggregate.
#[tokio::test]
async fn a_joined_tables_null_keyed_row_never_stands_for_the_empty_one() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, JOINED, &[J1, JG]).await;
    exact(&trellis, &raw, "j1", J1_ACTUAL, J1_ORACLE).await;
    exact(&trellis, &raw, "jg", JG_ACTUAL, JG_ORACLE).await;

    for write in [
        "delete from public.b where code is null",
        "update public.a set v = 20 where id = 2",
        "insert into public.b values (null, 'NULL2')",
        "update public.b set label = 'NULL3' where code is null",
        "update public.b set label = 'EMPTY2' where code = ''",
        "update public.a set v = 30",
        "delete from public.b where code = chr(1)",
        "insert into public.b values (chr(1), 'ONE2')",
    ] {
        raw.batch_execute(write).await.expect(write);
        settle(&db.pool, &mut raw).await;
        exact(&trellis, &raw, "j1", J1_ACTUAL, J1_ORACLE).await;
        exact(&trellis, &raw, "jg", JG_ACTUAL, JG_ORACLE).await;
    }
}

const HELD: &str = "TRANSFORM nk_q FROM public.nk KEY (code) SELECT 10.0 / v AS q";
const HELD_ACTUAL: &str =
    "select (code is null)::text, code, q::text from public.nk_q order by 1, code collate \"C\"";
const HELD_ORACLE: &str = "select (code is null)::text, code, (10.0 / v)::text from public.nk \
     order by 1, code collate \"C\"";

/// A `NULL`-keyed row whose field fails is held under its own key, never
/// the `''` row's, and releasing that key applies its row.
#[tokio::test]
async fn a_held_null_key_is_its_own_and_releases() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SINGLE, &[HELD]).await;
    raw.batch_execute("insert into public.nk values (null, 0, 0)")
        .await
        .unwrap();
    // Each drain charges the failing record once, until the threshold holds
    // its key and the rest applies.
    let batch = crate::support::pipeline::seal(&mut raw).await;
    for _ in 0..=trellis::staging::quarantine::DEFAULT_DEATH_THRESHOLD {
        let _ = trellis::staging::apply::drain_once(&db.pool, batch, "held", 1, "held_wake").await;
    }
    assert_eq!(
        rows(&raw, "select key from trellis.poison").await,
        [[Some("\u{1}".to_string())]]
    );
    raw.batch_execute(
        "update public.nk set v = 2 where code is null; \
         update public.nk set v = 5 where code = ''",
    )
    .await
    .unwrap();
    // A held key's change stays parked, so nothing settles until release.
    let batch = crate::support::pipeline::seal(&mut raw).await;
    crate::support::pipeline::drain(&db.pool, batch, "held").await;
    assert_eq!(
        rows(&raw, "select q::text from public.nk_q where code = ''").await,
        [[Some("2.0000000000000000".to_string())]],
        "the '' row applies while the NULL key is held"
    );
    trellis
        .release_key("nk_q", "public.nk", "\u{1}")
        .await
        .expect("release the NULL key");
    settle(&db.pool, &mut raw).await;
    exact(&trellis, &raw, "nk_q", HELD_ACTUAL, HELD_ORACLE).await;
}

/// The installed capture spec for `table`, and whether its functions are
/// what this build generates for it.
async fn installed(raw: &Client, table: &str) -> (trellis::capture::sql::CaptureSpec, bool) {
    match trellis::capture::install::installed(raw, "trellis", table)
        .await
        .expect("read the installed capture")
    {
        trellis::capture::install::Installed::Complete { spec, current } => (spec, current),
        other => panic!("{table}'s capture isn't installed: {other:?}"),
    }
}

/// Capture functions installed by 0.3.0 for a nullable `KEY` encode a
/// `NULL` key part as `''`, and their comment records no nullable column.
/// Attaching regenerates them: the reconcile pass sees them as stale, so
/// later writes are keyed as the build keys them.
#[tokio::test]
async fn capture_functions_a_0_3_0_install_left_are_regenerated_on_attach() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SINGLE, &[NK_ONE]).await;
    let (current, _) = installed(&raw, "public.nk").await;
    assert_eq!(current.nullable_key(), ["code".to_string()]);

    // The functions as 0.3.0 generated them: the same spec, no nullable
    // key column, so the `''` encoding and the key-column presence test.
    let old = trellis::capture::sql::CaptureSpec::new(
        "public.nk",
        current.key().to_vec(),
        current.columns().iter().cloned(),
    )
    .unwrap()
    .with_declared_key(true);
    let txn = raw.transaction().await.unwrap();
    for event in trellis::capture::sql::CaptureEvent::ALL {
        txn.batch_execute(&trellis::capture::sql::function_ddl("trellis", &old, event).unwrap())
            .await
            .unwrap();
        txn.batch_execute(&trellis::capture::sql::comment_ddl("trellis", &old, event).unwrap())
            .await
            .unwrap();
    }
    txn.commit().await.unwrap();
    let (stale, _) = installed(&raw, "public.nk").await;
    assert!(stale.nullable_key().is_empty(), "{stale:?}");

    reconcile_pass(&mut raw, &db.pool).await;
    assert_eq!(installed(&raw, "public.nk").await, (current, true));

    raw.batch_execute(
        "insert into public.nk values (null, 1, 1); \
         update public.nk set v = 10 where code is null",
    )
    .await
    .unwrap();
    settle(&db.pool, &mut raw).await;
    exact(&trellis, &raw, "nk1", NK_ONE_ACTUAL, NK_ONE_ORACLE).await;
}
