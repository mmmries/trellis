//! A star 1-1 `JOIN` (more than one joined table, ADR-0021, #1460 sub-step
//! 3.13) across a schema change of a joined table at a later position: the
//! per-table cases of `join_one_to_one_schema_changes.rs`, ported to
//! position 2 of n = 3. Each pauses the whole transform with a
//! `capture_failure` naming the table at that position and its column, as at
//! position 1:
//!
//! - its unique key on that position's `ON` columns dropped, or left
//!   `INVALID` (#1536's check, at every position, against that position's
//!   own `ON` columns), and `RESUME` refused until the key is back; the
//!   swaps and maintenance that keep a key in place pause nothing, and the
//!   pass's memo never hides a drop;
//! - a column the star reads there renamed or dropped;
//! - a column a field reads there widened, and `RESUME` re-types the target;
//! - its `ON` column retyped on one side, `RESUME` refused until the pair
//!   matches again;
//! - the table renamed away, or attached as a partition;
//! - its primary key redefined, or the root's, and `RESUME` re-records or
//!   re-keys;
//! - a value of an enum read only there relabelled.
//!
//! The checks read the catalog and the pause state only, so every test
//! defines the star, leaves its build unstarted, and drives the staging
//! worker's pass by hand: the checks read the catalog and the pause state
//! only. Nothing polls or sleeps. The development knob is process-wide, so
//! every test takes the harness's lock and sets it.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use star::Shape;
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::{TransformStatus, ValueType};
use trellis::integer::IntWidth;
use trellis::{Trellis, TrellisError, TrellisOptions};

/// Position 2 of n = 3, the position under test.
const SHAPE: Shape = Shape {
    positions: 2,
    at: 2,
};

/// Takes the binary's lock and turns the knob on, for the guard's life.
struct Knob {
    _serial: tokio::sync::MutexGuard<'static, ()>,
    was: bool,
}

impl Drop for Knob {
    fn drop(&mut self) {
        trellis::dev::defs::set_star_join(self.was);
    }
}

async fn knob() -> Knob {
    let serial = star::serial().await;
    let was = star::star_join_enabled();
    trellis::dev::defs::set_star_join(true);
    Knob {
        _serial: serial,
        was,
    }
}

/// `a` rows `(id, k, x, y)` with `k` at position 2: two match `b2`, one
/// doesn't.
const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 99, 3, 'three')";
/// `b2` rows `(id, k, w, tag)`.
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";

/// The first star of [`SHAPE`] (all `LEFT`), defined over the seed, its
/// build not run.
async fn start() -> (Driver, star::Star) {
    let star = SHAPE.stars()[0];
    (star::start_unbuilt(&[star], A_ROWS, B_ROWS).await, star)
}

async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// One whole staging-worker pass: capture (the schema-change checks among
/// it), the requested resumes, then the start of ready builds.
async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

async fn status(d: &Driver, target: &str) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&target],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// Every capture failure recorded, as `(source_table, columns, error)`.
async fn failures(d: &Driver) -> Vec<String> {
    d.rows("select source_table, columns, error from capture_failures order by 1")
        .await
}

/// `table.column`'s type, as `format_type` renders it.
async fn column_type(d: &Driver, table: &str, column: &str) -> String {
    d.ctl
        .query_one(
            "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attname = $2",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
        .get(0)
}

/// The reason `target` is paused with, asserting it is paused for a capture
/// failure on `table` about `columns`.
async fn paused_for(d: &Driver, target: &str, table: &str, columns: &[&str]) -> String {
    let bare = target.split_once('.').map_or(target, |(_, bare)| bare);
    let reported = trellis(d)
        .await
        .status(bare)
        .await
        .expect("status")
        .expect(target);
    assert_eq!(
        reported.status,
        TransformStatus::Paused,
        "{target}: {:?}",
        failures(d).await
    );
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, table, "{target}: {}", failure.error);
    assert_eq!(
        failure.columns,
        columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "{target}: {}",
        failure.error
    );
    failure.error
}

/// Asserts `target` isn't paused and nothing is recorded against it.
async fn assert_unpaused(d: &Driver, target: &str, after: &str) {
    assert_ne!(
        status(d, target).await,
        "paused",
        "{target} after {after}: {:?}",
        failures(d).await
    );
    assert!(failures(d).await.is_empty(), "{target} after {after}");
}

/// `RESUME TRANSFORM target`, which must go through.
async fn resume(d: &Driver, target: &str) {
    trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
        .unwrap_or_else(|err| panic!("resume {target}: {err}"));
}

/// `RESUME TRANSFORM target`, which must be refused: returns the message.
async fn resume_refused(d: &Driver, target: &str) -> String {
    match trellis(d)
        .await
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
    {
        Err(
            err @ TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused { .. }),
        ) => err.to_string(),
        other => panic!("expected the resume of {target} to be refused, got {other:?}"),
    }
}

// ------------------------------------------- a position's unique key (#1536)

/// A star whose `ON` columns differ by position: position 1 joins `b1` on
/// `k`, position 2 joins `b2` on `code`, each with its own unique index.
const KEYED_DDL: &str = "create table public.a (id integer primary key, k1 integer, \
                             k2 integer, x integer); \
                         create table public.b1 (id integer primary key, k integer, w integer); \
                         create unique index b1_k_unique on public.b1 (k); \
                         create table public.b2 (id integer primary key, code integer, \
                             w integer); \
                         create unique index b2_code_unique on public.b2 (code); \
                         create index on public.a (k1); create index on public.a (k2); \
                         insert into public.b1 values (1, 10, 1), (2, 20, 2); \
                         insert into public.b2 values (1, 100, 10), (2, 200, 20); \
                         insert into public.a values (1, 10, 100, 5), (2, 20, 300, 6)";

const KEYED: &str = "TRANSFORM v FROM public.a \
                     LEFT JOIN public.b1 ON a.k1 = b1.k \
                     LEFT JOIN public.b2 ON a.k2 = b2.code \
                     SELECT a.x + COALESCE(b1.w, 0) + COALESCE(b2.w, 0) AS total";

async fn start_keyed() -> Driver {
    let int4 = ValueType::Integer(IntWidth::Int4);
    Driver::start_unbuilt(
        &[],
        KEYED_DDL,
        &[("id", int4), ("k1", int4), ("k2", int4), ("x", int4)],
        &[KEYED],
        &["public.a", "public.b1", "public.b2"],
    )
    .await
}

/// Each position's key is checked against that position's own `ON` columns:
/// with both keys in place, passes pause nothing (`b2` has no column `k`, so
/// a check of position 1's `ON` columns against it would find no key).
/// Position 2's unique index dropped, the next pass pauses the whole
/// transform naming `b2` and `code`, and `RESUME` is refused with define's
/// refusal until the index is back. Position 1's dropped pauses it naming
/// `b1` and `k`.
#[tokio::test]
async fn a_lost_unique_key_at_position_2_pauses_naming_it() {
    let _knob = knob().await;
    let mut d = start_keyed().await;
    let target = "public.v";
    pass(&mut d).await;
    pass(&mut d).await;
    assert_unpaused(&d, target, "two passes over intact keys").await;

    write(&d, "drop index public.b2_code_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, target, "public.b2", &["code"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (code) is gone")
            && error.contains("resume the definition to rebuild it"),
        "{error}"
    );
    let refused = resume_refused(&d, "v").await;
    assert!(
        refused.contains("a 1-1 JOIN needs the ON columns of 'public.b2' (code) to be unique"),
        "{refused}"
    );
    assert_eq!(status(&d, target).await, "paused");

    write(&d, "create unique index b2_code_unique on public.b2 (code)").await;
    resume(&d, "v").await;
    pass(&mut d).await;
    assert_unpaused(&d, target, "the resume once the key is back").await;

    write(&d, "drop index public.b1_k_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, target, "public.b1", &["k"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b1 (k) is gone"),
        "{error}"
    );
}

/// Replacing position 2's unique key the way that keeps one in place at
/// every moment pauses nothing, with a pass between every two statements, as
/// at position 1: a create-first swap, a swap in one transaction, the index
/// promoted to a constraint, and the maintenance that rewrites it in place
/// (`REINDEX` in both forms, `VACUUM FULL`, `CLUSTER`): #1536's study's
/// routine rows, at position 2.
#[tokio::test]
async fn swaps_that_keep_position_2_s_key_in_place_pause_nothing() {
    let _knob = knob().await;
    let mut d = start_keyed().await;
    for statement in [
        "create unique index concurrently b2_code_new on public.b2 (code)",
        "drop index concurrently public.b2_code_unique",
        "alter index public.b2_code_new rename to b2_code_unique",
        "begin; drop index public.b2_code_unique; \
         create unique index b2_code_unique on public.b2 (code); commit",
        "alter table public.b2 add constraint b2_code_c unique using index b2_code_unique",
        "reindex index public.b2_code_c",
        "reindex table concurrently public.b2",
        "vacuum full public.b2",
        "cluster public.b2 using b2_code_c",
    ] {
        write(&d, statement).await;
        pass(&mut d).await;
        assert_unpaused(&d, "public.v", statement).await;
    }
}

/// An `INVALID` unique index left by a failed `CREATE UNIQUE INDEX
/// CONCURRENTLY` (over a duplicate written while the old one was gone) as
/// position 2's only key is no key: the pass pauses the transform naming
/// `b2` and `code`.
#[tokio::test]
async fn an_invalid_unique_index_as_position_2_s_only_key_pauses_the_transform() {
    let _knob = knob().await;
    let mut d = start_keyed().await;
    write(
        &d,
        "drop index public.b2_code_unique; insert into public.b2 values (3, 100, 30)",
    )
    .await;
    d.user()
        .await
        .batch_execute("create unique index concurrently b2_code_unique on public.b2 (code)")
        .await
        .expect_err("code = 100 is duplicated");
    pass(&mut d).await;
    paused_for(&d, "public.v", "public.b2", &["code"]).await;
}

/// The pass skips position 2's key query while `b2`'s fingerprint is the one
/// its last check found the key at, and a drop with no other change between
/// two passes still forces the check, which pauses the transform.
#[tokio::test]
async fn a_drop_at_position_2_between_two_passes_with_no_other_change_is_detected() {
    let _knob = knob().await;
    let mut d = start_keyed().await;
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.v'",
            &[],
        )
        .await
        .expect("the definition")
        .get(0);
    let database: String = d
        .ctl
        .query_one("select current_database()::text", &[])
        .await
        .expect("the database")
        .get(0);
    let checks = || trellis::staging::unique_key_checks(&database, DEFAULT_SCHEMA, id);
    pass(&mut d).await;
    let checked = checks();
    assert!(checked >= 2, "the first pass checks both positions' keys");
    pass(&mut d).await;
    pass(&mut d).await;
    assert_eq!(checks(), checked, "skipped while b1 and b2 are unchanged");

    write(&d, "drop index public.b2_code_unique").await;
    pass(&mut d).await;
    assert_eq!(checks(), checked + 1, "the drop forces b2's check alone");
    paused_for(&d, "public.v", "public.b2", &["code"]).await;
}

/// One table at different positions of two stars, on different `ON` columns
/// (`b2` at position 2 of `v1` on `code`, at position 3 of `v2` on `alt`),
/// and the root of a third transform (`v3`): each definition is checked at
/// its own position against its own `ON` columns. `b2`'s key on `alt`
/// dropped pauses `v2` alone; its key on `code` replaced by a partial unique
/// index (built first, so a unique index on `code` exists at every moment)
/// pauses `v1` alone, as define refuses a partial index as the key; the
/// transform `b2` is the root of pauses neither time.
#[tokio::test]
async fn one_table_at_two_positions_of_two_stars_is_checked_per_definition() {
    let _knob = knob().await;
    let int4 = ValueType::Integer(IntWidth::Int4);
    let mut d = Driver::start_unbuilt(
        &[],
        "create table public.a (id integer primary key, k1 integer, k2 integer, x integer); \
         create table public.c (id integer primary key, k1 integer, k2 integer, k3 integer, \
             x integer); \
         create table public.b1 (id integer primary key, k integer, w integer); \
         create unique index b1_k_unique on public.b1 (k); \
         create table public.b3 (id integer primary key, k integer, w integer); \
         create unique index b3_k_unique on public.b3 (k); \
         create table public.b2 (id integer primary key, code integer, alt integer, \
             w integer); \
         create unique index b2_code_unique on public.b2 (code); \
         create unique index b2_alt_unique on public.b2 (alt); \
         create index on public.a (k1); create index on public.a (k2); \
         create index on public.c (k1); create index on public.c (k2); \
         create index on public.c (k3); \
         insert into public.b1 values (1, 10, 1); insert into public.b3 values (1, 30, 3); \
         insert into public.b2 values (1, 100, 1000, 30); \
         insert into public.a values (1, 10, 100, 5); \
         insert into public.c values (1, 10, 1000, 30, 6)",
        &[
            ("id", int4),
            ("k1", int4),
            ("k2", int4),
            ("k3", int4),
            ("x", int4),
            ("code", int4),
            ("alt", int4),
            ("w", int4),
        ],
        &[
            "TRANSFORM v1 FROM public.a \
             LEFT JOIN public.b1 ON a.k1 = b1.k \
             LEFT JOIN public.b2 ON a.k2 = b2.code \
             SELECT a.x + COALESCE(b1.w, 0) + COALESCE(b2.w, 0) AS total",
            "TRANSFORM v2 FROM public.c \
             LEFT JOIN public.b1 ON c.k1 = b1.k \
             LEFT JOIN public.b3 ON c.k3 = b3.k \
             LEFT JOIN public.b2 ON c.k2 = b2.alt \
             SELECT c.x + COALESCE(b2.w, 0) AS total",
            "TRANSFORM v3 FROM public.b2 LEFT JOIN public.b3 ON b2.w = b3.k \
             SELECT b2.code + COALESCE(b3.w, 0) AS total",
        ],
        &[
            "public.a",
            "public.c",
            "public.b1",
            "public.b2",
            "public.b3",
        ],
    )
    .await;
    pass(&mut d).await;
    pass(&mut d).await;
    for target in ["public.v1", "public.v2", "public.v3"] {
        assert_unpaused(&d, target, "two passes over intact keys").await;
    }

    write(&d, "drop index public.b2_alt_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, "public.v2", "public.b2", &["alt"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (alt) is gone"),
        "{error}"
    );
    for target in ["public.v1", "public.v3"] {
        assert_ne!(
            status(&d, target).await,
            "paused",
            "{target} after alt's drop"
        );
    }

    write(
        &d,
        "create unique index b2_code_partial on public.b2 (code) where code > 0",
    )
    .await;
    pass(&mut d).await;
    assert_ne!(
        status(&d, "public.v1").await,
        "paused",
        "a partial index added"
    );
    write(&d, "drop index public.b2_code_unique").await;
    pass(&mut d).await;
    let error = paused_for(&d, "public.v1", "public.b2", &["code"]).await;
    assert!(
        error.starts_with("the unique key on the ON columns of public.b2 (code) is gone"),
        "{error}"
    );
    assert_ne!(
        status(&d, "public.v3").await,
        "paused",
        "the root's transform"
    );
}

// --------------------------------------------- columns of position 2's table

/// A column the star reads at position 2 renamed (`b2.w`) or dropped
/// (`b2.tag`) pauses the transform naming `b2` and that column. Once it is
/// back, `RESUME` goes through.
#[tokio::test]
async fn a_column_read_at_position_2_renamed_or_dropped_pauses_naming_it() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the first pass").await;

    write(&d, &format!("alter table {b} rename column w to w2")).await;
    pass(&mut d).await;
    let error = paused_for(&d, &target, &b, &["w"]).await;
    assert!(error.contains("renamed or dropped"), "{error}");
    let refused = resume_refused(&d, &star.name()).await;
    assert!(refused.contains("w"), "{refused}");
    write(&d, &format!("alter table {b} rename column w2 to w")).await;
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume after the rename").await;

    write(&d, &format!("alter table {b} drop column tag")).await;
    pass(&mut d).await;
    let error = paused_for(&d, &target, &b, &["tag"]).await;
    assert!(error.contains("renamed or dropped"), "{error}");
    write(
        &d,
        &format!("alter table {b} add column tag text; update {b} set tag = 'b' || id"),
    )
    .await;
    pass(&mut d).await;
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume after the drop").await;
}

/// A column a field reads at position 2 widened (`b2.w`, read by `total`, an
/// `integer`, to `bigint`): the pass pauses the transform naming `b2` and
/// `w`. `RESUME` waits for the staging worker to re-type `total`, which the
/// next pass does, and `M2`'s copy of `w` with it.
#[tokio::test]
async fn a_widened_column_read_at_position_2_pauses_and_resume_re_types_the_target() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;
    assert_eq!(column_type(&d, &target, "total").await, "integer");

    write(&d, &format!("alter table {b} alter column w type bigint")).await;
    pass(&mut d).await;
    let error = paused_for(&d, &target, &b, &["w"]).await;
    assert!(error.contains("total"), "{error}");
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_eq!(column_type(&d, &target, "total").await, "bigint");
    let m2 = star::m_table(&d, star, 2).await;
    assert_eq!(column_type(&d, &m2, "w").await, "bigint", "{m2}");
    assert_unpaused(&d, &target, "the resume").await;
}

/// Position 2's `ON` column retyped on `b2`'s side only (`b2.k` to `bigint`,
/// `a.k2` still `integer`): the pass pauses the transform naming `b2` and
/// `k`, and `RESUME` is refused while the pair's types differ. With `a.k2`
/// widened too, `RESUME` goes through and creates `J`'s column for the
/// position at the pair's new type.
#[tokio::test]
async fn an_on_column_retyped_at_position_2_pauses_until_the_pair_matches_again() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;

    write(&d, &format!("alter table {b} alter column k type bigint")).await;
    pass(&mut d).await;
    paused_for(&d, &target, &b, &["k"]).await;
    let refused = resume_refused(&d, &star.name()).await;
    assert!(refused.contains("k"), "{refused}");

    write(&d, "alter table public.a alter column k2 type bigint").await;
    pass(&mut d).await;
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume once the pair matches").await;
    let id = star::definition_id(&d, star).await;
    let j = format!("{DEFAULT_SCHEMA}.join1_{id}_j");
    let columns = d
        .rows(&format!(
            "select attname::text, pg_catalog.format_type(atttypid, atttypmod) \
             from pg_catalog.pg_attribute \
             where attrelid = '{j}'::regclass and attnum > 0 and not attisdropped \
               and attname like 'p2\\_%' order by attnum"
        ))
        .await;
    assert_eq!(columns, ["(p2_k2,bigint)"], "{j}");
}

// ------------------------------------------------------ position 2's table

/// The table at position 2 renamed away pauses the transform with a
/// disruption naming it; put back, `RESUME` goes through. Attached as a
/// partition of another table, it pauses naming it too, and `RESUME` is
/// refused until it is detached.
#[tokio::test]
async fn position_2_s_table_renamed_away_or_attached_as_a_partition_pauses_the_transform() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;

    write(&d, &format!("alter table {b} rename to b2_x")).await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    let failure = d
        .rows("select source_table, kind from capture_failures")
        .await;
    assert_eq!(failure, [format!("({b},disruption)")]);
    write(&d, "alter table public.b2_x rename to b2").await;
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume after the rename").await;

    write(
        &d,
        &format!(
            "create table public.bp (id integer not null, k integer, w integer, tag text) \
                 partition by range (id); \
             alter table public.bp attach partition {b} \
                 for values from (minvalue) to (maxvalue)"
        ),
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    let failure = d.rows("select source_table from capture_failures").await;
    assert_eq!(failure, [format!("({b})")]);
    let refused = resume_refused(&d, &star.name()).await;
    assert!(refused.contains(&b), "{refused}");
    write(&d, &format!("alter table public.bp detach partition {b}")).await;
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume once detached").await;
}

// ------------------------------------------------------------- keys redefined

/// The primary key of the table at position 2 redefined (`(id)` to
/// `(id, tag)`) pauses the transform naming `b2`. `RESUME` records `b2`'s
/// new key as the one it keys `b2` by, and goes through.
#[tokio::test]
async fn position_2_s_primary_key_redefined_pauses_and_resume_records_the_new_key() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    let b = SHAPE.b();
    pass(&mut d).await;

    write(
        &d,
        &format!("alter table {b} drop constraint b2_pkey, add primary key (id, tag)"),
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    let failure = d.rows("select source_table from capture_failures").await;
    assert_eq!(failure, [format!("({b})")]);
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume").await;
    let id = star::definition_id(&d, star).await;
    let recorded = d
        .rows(&format!(
            "select column_name from definition_key_types \
             where transform_id = {id} and table_name = '{b}' order by 1"
        ))
        .await;
    assert_eq!(recorded, ["(id)", "(k)", "(tag)"], "{b}'s key types");
}

/// The root's primary key redefined (`(id)` to `(id, x)`) pauses the star,
/// and `RESUME` re-keys its target to the new key (#1746) and goes through.
#[tokio::test]
async fn the_root_s_primary_key_redefined_and_resume_re_keys_the_target() {
    let _knob = knob().await;
    let (mut d, star) = start().await;
    let target = star.target();
    pass(&mut d).await;

    write(
        &d,
        "alter table public.a drop constraint a_pkey, add primary key (id, x)",
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, &target).await, "paused");
    resume(&d, &star.name()).await;
    pass(&mut d).await;
    assert_unpaused(&d, &target, "the resume").await;
    let key = d
        .rows(&format!(
            "select a.attname::text from pg_catalog.pg_index i \
             cross join lateral unnest(i.indkey::int2[]) with ordinality k(n, o) \
             join pg_catalog.pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.n \
             where i.indrelid = '{target}'::regclass and i.indisprimary order by k.o"
        ))
        .await;
    assert_eq!(key, ["(id)", "(x)"], "{target}'s key");
}

// ------------------------------------------------------- an enum at position 2

/// A value of an enum the star reads only at position 2 (`b2.tone`)
/// relabelled: the pass pauses the transform with the relabel's reason, as
/// for the joined table at position 1 (the column's token sits in
/// `definition_join_tables`, not `join_columns`).
#[tokio::test]
async fn an_enum_value_read_at_position_2_relabelled_pauses_the_transform() {
    let _knob = knob().await;
    let mut d = start_enum().await;
    pass(&mut d).await;
    assert_unpaused(&d, "public.v", "the first pass").await;

    write(&d, "alter type public.tone rename value 'calm' to 'cool'").await;
    pass(&mut d).await;
    assert_eq!(status(&d, "public.v").await, "paused");
    let failures = failures(&d).await;
    assert!(
        failures.len() == 1
            && failures[0].contains("enum value 'calm' of type public.tone was renamed to 'cool'"),
        "{failures:?}"
    );
}

/// The column of an enum the star reads only at position 2 (`b2.tone`)
/// re-typed to another enum: the pass pauses the transform with the re-type's
/// reason, found through the column's token in `definition_join_tables`, and
/// records the new type there, as for the joined table at position 1.
#[tokio::test]
async fn an_enum_column_read_at_position_2_re_typed_to_another_enum_pauses_the_transform() {
    let _knob = knob().await;
    let mut d = start_enum().await;
    pass(&mut d).await;
    assert_unpaused(&d, "public.v", "the first pass").await;

    write(
        &d,
        "create type public.mood as enum ('fast', 'slow'); \
         alter table public.b2 alter column tone type public.mood using \
         (case tone::text when 'brisk' then 'fast' else 'slow' end)::public.mood",
    )
    .await;
    pass(&mut d).await;
    assert_eq!(status(&d, "public.v").await, "paused");
    let failures = failures(&d).await;
    assert!(
        failures.len() == 1
            && failures[0].contains(
                "of public.b2, which this definition reads, changed type from enum public.tone \
                 to enum public.mood"
            ),
        "{failures:?}"
    );
    assert_eq!(
        d.rows(
            "select columns->>'tone' from definition_join_tables where table_name = 'public.b2'"
        )
        .await,
        ["(enum:public.mood)"]
    );
}

/// A star whose position 2 (`b2`) is the only table with an enum column the
/// star reads (`b2.tone`), defined, its build not run.
async fn start_enum() -> Driver {
    let int4 = ValueType::Integer(IntWidth::Int4);
    Driver::start_unbuilt(
        &[],
        "create type public.tone as enum ('calm', 'brisk'); \
         create table public.a (id integer primary key, k1 integer, k2 integer, x integer); \
         create table public.b1 (id integer primary key, k integer, w integer); \
         create unique index on public.b1 (k); \
         create table public.b2 (id integer primary key, code integer, tone public.tone); \
         create unique index on public.b2 (code); \
         insert into public.b1 values (1, 10, 1); \
         insert into public.b2 values (1, 100, 'calm'), (2, 200, 'brisk'); \
         insert into public.a values (1, 10, 100, 5), (2, 20, 200, 6)",
        &[("id", int4), ("k1", int4), ("k2", int4), ("x", int4)],
        &["TRANSFORM v FROM public.a \
           LEFT JOIN public.b1 ON a.k1 = b1.k \
           LEFT JOIN public.b2 ON a.k2 = b2.code \
           SELECT a.x + COALESCE(b1.w, 0) AS total, b2.tone AS tone"],
        &["public.a", "public.b1", "public.b2"],
    )
    .await
}
