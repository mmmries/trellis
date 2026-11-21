//! A table's key encoding is a record kept with its recorded key (#2147,
//! #2217): a `KEY (...)` on a `NULLS NOT DISTINCT` index encodes every key
//! part (`NULL` as `chr(1)`, a real `chr(1)` doubled), whatever the
//! columns' `NOT NULL`, and a `SET NOT NULL` or `DROP NOT NULL` with changes
//! staged never changes how they decode. A record from before the encoding
//! was recorded renders such a key raw; a `NULL` reaching it pauses with
//! T0254, and `RESUME` records it again, encoded.
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

const ROWS: &str = "insert into public.nk values ('', 0, 2), ('a', 1, 3), (chr(1), 0, 4), \
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

async fn both_exact(trellis: &Trellis, raw: &Client) {
    exact(trellis, raw, "nk1", NK_ONE_ACTUAL, NK_ONE_ORACLE).await;
    exact(trellis, raw, "nk_sums", NK_AGG_ACTUAL, NK_AGG_ORACLE).await;
}

/// The key columns recorded as encoded for `table`.
async fn encoded(raw: &Client, table: &str) -> Vec<String> {
    raw.query_one(
        "select encoded_columns from trellis.definition_keys where table_name = $1",
        &[&table],
    )
    .await
    .expect("the record")
    .get(0)
}

/// #2147's repro, `SET NOT NULL` direction: updates of `chr(1)`-holding
/// keys staged, then `SET NOT NULL` on the key column before they drain.
/// They decode as they were staged, and nothing pauses.
#[tokio::test]
async fn set_not_null_with_changes_staged_keeps_the_encoding() {
    let cluster = TestCluster::start();
    let ddl = format!(
        "create table public.nk (code text, g int not null, v int not null); \
         create unique index on public.nk (code) nulls not distinct; {ROWS}"
    );
    let (db, mut raw, trellis) = setup(&cluster, &ddl, &[NK_ONE, NK_AGG]).await;
    both_exact(&trellis, &raw).await;
    assert_eq!(encoded(&raw, "public.nk").await, ["code"]);

    raw.batch_execute(
        "update public.nk set v = 50 where code = 'x' || chr(1) || chr(1); \
         update public.nk set v = 60 where code = chr(1); \
         alter table public.nk alter column code set not null",
    )
    .await
    .expect("the staged updates, then SET NOT NULL");
    settle(&db.pool, &mut raw).await;
    reconcile_pass(&mut raw, &db.pool).await;
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
    assert_eq!(encoded(&raw, "public.nk").await, ["code"], "sticky");

    // A definition defined after the flip reads the same record.
    trellis
        .apply("TRANSFORM nk2 FROM public.nk KEY (code) SELECT g AS g")
        .await
        .expect("a new reader");
    resume_passes::bring_live(&mut raw, &db.pool, &trellis, &["nk2"]).await;
    raw.batch_execute("update public.nk set g = 9 where code = chr(1)")
        .await
        .expect("an update");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
    exact(
        &trellis,
        &raw,
        "nk2",
        "select code, g::text from public.nk2 order by code collate \"C\"",
        "select code, g::text from public.nk order by code collate \"C\"",
    )
    .await;
}

/// #2147's repro, `DROP NOT NULL` direction: a key on a `NULLS NOT
/// DISTINCT` index over a `NOT NULL` column is recorded encoded, so an
/// update staged before the drop decodes right, and a `NULL` inserted after
/// it, before the capture pass runs, is a key of its own. Nothing pauses.
#[tokio::test]
async fn drop_not_null_with_changes_staged_keeps_the_encoding() {
    let cluster = TestCluster::start();
    let ddl = format!(
        "create table public.nk (code text not null, g int not null, v int not null); \
         create unique index on public.nk (code) nulls not distinct; {ROWS}"
    );
    let (db, mut raw, trellis) = setup(&cluster, &ddl, &[NK_ONE, NK_AGG]).await;
    both_exact(&trellis, &raw).await;
    assert_eq!(encoded(&raw, "public.nk").await, ["code"]);

    raw.batch_execute(
        "update public.nk set v = 50 where code = 'x' || chr(1) || chr(1); \
         alter table public.nk alter column code drop not null; \
         insert into public.nk values (null, 1, 70)",
    )
    .await
    .expect("a staged update, DROP NOT NULL, then a NULL key");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
    reconcile_pass(&mut raw, &db.pool).await;
    raw.batch_execute("update public.nk set v = 71 where code is null")
        .await
        .expect("an update of the NULL key");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
}

/// The paused definition's `capture_failure.error`.
async fn pause_error(trellis: &Trellis, target: &str) -> String {
    let reported = status(trellis, target).await;
    assert_eq!(
        reported.status,
        TransformStatus::Paused,
        "{target}: {reported:?}"
    );
    reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target} has a capture failure"))
        .error
}

/// What a definition from before the encoding was recorded leaves: the
/// source's record and its 1-1 target's raw, and the target keyed by a
/// primary key (the migration records today's reading, and rewrites no
/// stored key). Capture is regenerated raw on the next pass.
const RAW_RECORD: &str = "update trellis.definition_keys set encoded_columns = '{}' \
         where table_name in ('public.nk', 'public.nk1'); \
     do $$ declare c text; begin \
         select conname into c from pg_constraint \
          where conrelid = 'public.nk1'::regclass and contype in ('u', 'p'); \
         execute format('alter table public.nk1 drop constraint %I, \
                         alter column code set not null, add primary key (code)', c); \
     end $$";

/// D2: a definition recorded raw on a `NULLS NOT DISTINCT` key over a `NOT
/// NULL` column keeps running raw. After `DROP NOT NULL`, a `NULL` reaching
/// the drain before the capture pass pauses its readers with T0254, never
/// T0253, and `RESUME` records the key again, encoded, re-keys the 1-1
/// target to `NULLS NOT DISTINCT` and rebuilds.
#[tokio::test]
async fn a_raw_record_pauses_on_a_null_after_drop_not_null_and_resume_encodes_it() {
    let cluster = TestCluster::start();
    let ddl = "create table public.nk (code text not null, g int not null, v int not null); \
         create unique index on public.nk (code) nulls not distinct; \
         insert into public.nk values ('', 0, 2), ('a', 1, 3), ('b', 0, 4)";
    let (db, mut raw, trellis) = setup(&cluster, ddl, &[NK_ONE, NK_AGG]).await;
    raw.batch_execute(RAW_RECORD).await.expect("the raw record");
    reconcile_pass(&mut raw, &db.pool).await;
    raw.batch_execute("update public.nk set v = 20 where code = ''")
        .await
        .expect("an update");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;

    raw.batch_execute(
        "alter table public.nk alter column code drop not null; \
         insert into public.nk values (null, 1, 70)",
    )
    .await
    .expect("DROP NOT NULL, then a NULL key");
    settle(&db.pool, &mut raw).await;
    for target in ["nk1", "nk_sums"] {
        let error = pause_error(&trellis, target).await;
        assert!(error.starts_with("T0254"), "{target}: {error}");
        assert!(!error.contains("T0253"), "{target}: {error}");
    }
    // A new reader is refused until the record encodes the key.
    let refused = trellis
        .apply("TRANSFORM nk2 FROM public.nk KEY (code) SELECT g AS g")
        .await
        .expect_err("a new reader of a raw key that can hold NULL");
    assert!(refused.to_string().starts_with("T0254"), "{refused}");

    trellis.apply("RESUME TRANSFORM nk1").await.expect("resume");
    assert_eq!(encoded(&raw, "public.nk").await, ["code"]);
    assert_eq!(encoded(&raw, "public.nk1").await, ["code"]);
    trellis
        .apply("RESUME TRANSFORM nk_sums")
        .await
        .expect("resume");
    resume_passes::bring_live(&mut raw, &db.pool, &trellis, &["nk1", "nk_sums"]).await;
    both_exact(&trellis, &raw).await;
    raw.batch_execute(
        "update public.nk set v = 71 where code is null; \
         insert into public.nk values (chr(1), 0, 9)",
    )
    .await
    .expect("writes under the encoded key");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
}

/// A `KEY` on a plain unique index over a `NOT NULL` column renders raw; a
/// `DROP NOT NULL` unbacks it, and a `NULL` that reaches the drain before
/// the capture pass pauses its readers as unbacked, never with T0253.
#[tokio::test]
async fn a_null_after_drop_not_null_on_a_plain_index_is_an_unbacked_key() {
    let cluster = TestCluster::start();
    let ddl = "create table public.nk (code text not null, g int not null, v int not null); \
         create unique index on public.nk (code); \
         insert into public.nk values ('', 0, 2), ('a', 1, 3)";
    let (db, mut raw, trellis) = setup(&cluster, ddl, &[NK_ONE, NK_AGG]).await;
    assert!(encoded(&raw, "public.nk").await.is_empty());
    raw.batch_execute(
        "alter table public.nk alter column code drop not null; \
         insert into public.nk values (null, 1, 70)",
    )
    .await
    .expect("DROP NOT NULL, then a NULL key");
    settle(&db.pool, &mut raw).await;
    for target in ["nk1", "nk_sums"] {
        let error = pause_error(&trellis, target).await;
        assert!(
            error.starts_with("no valid unique index backs KEY"),
            "{target}: {error}"
        );
    }
}

/// The encoding stays put through a swap of the backing index for an
/// equivalent one, a `SET NOT NULL`, and a `PAUSE`/`RESUME` that keeps the
/// record: only re-recording the key changes it.
#[tokio::test]
async fn the_encoding_stays_through_an_index_swap_and_a_resume() {
    let cluster = TestCluster::start();
    let ddl = format!(
        "create table public.nk (code text, g int not null, v int not null); \
         create unique index nk_old on public.nk (code) nulls not distinct; {ROWS}"
    );
    let (db, mut raw, trellis) = setup(&cluster, &ddl, &[NK_ONE, NK_AGG]).await;
    raw.batch_execute(
        "create unique index nk_new on public.nk (code) nulls not distinct; \
         drop index public.nk_old; \
         alter table public.nk alter column code set not null",
    )
    .await
    .expect("an equivalent index, then SET NOT NULL");
    reconcile_pass(&mut raw, &db.pool).await;
    for target in ["nk1", "nk_sums"] {
        trellis
            .apply(&format!("PAUSE TRANSFORM {target}"))
            .await
            .expect("pause");
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect("resume");
    }
    resume_passes::bring_live(&mut raw, &db.pool, &trellis, &["nk1", "nk_sums"]).await;
    assert_eq!(encoded(&raw, "public.nk").await, ["code"]);
    raw.batch_execute("update public.nk set v = 8 where code = chr(1)")
        .await
        .expect("an update");
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
}

/// Postgres 18's `NOT NULL ... NOT VALID` then `VALIDATE CONSTRAINT` (study
/// row 8): the column reads nullable at define, so the key is encoded, and
/// `VALIDATE` with `chr(1)` keys staged changes nothing. Postgres 15–17
/// have no `NOT VALID` not-null constraint: the case returns early there.
#[tokio::test]
async fn validate_constraint_with_changes_staged_keeps_the_encoding() {
    let cluster = TestCluster::start();
    let ddl = format!(
        "create table public.nk (code text, g int not null, v int not null); \
         create unique index on public.nk (code) nulls not distinct; {ROWS}"
    );
    let (db, mut raw, trellis) = setup(&cluster, &ddl, &[]).await;
    let version: i32 = raw
        .query_one("select current_setting('server_version_num')::int", &[])
        .await
        .unwrap()
        .get(0);
    if version < 180_000 {
        return;
    }
    raw.batch_execute("alter table public.nk add constraint nk_nn not null code not valid")
        .await
        .expect("NOT VALID");
    for text in [NK_ONE, NK_AGG] {
        trellis.apply(text).await.expect(text);
    }
    resume_passes::bring_live(&mut raw, &db.pool, &trellis, &["nk1", "nk_sums"]).await;
    raw.batch_execute(
        "update public.nk set v = 50 where code = 'x' || chr(1) || chr(1); \
         update public.nk set v = 60 where code = chr(1); \
         alter table public.nk validate constraint nk_nn",
    )
    .await
    .expect("staged updates, then VALIDATE");
    settle(&db.pool, &mut raw).await;
    reconcile_pass(&mut raw, &db.pool).await;
    settle(&db.pool, &mut raw).await;
    both_exact(&trellis, &raw).await;
    assert_eq!(encoded(&raw, "public.nk").await, ["code"]);
}
