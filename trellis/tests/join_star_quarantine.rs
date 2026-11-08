//! A star 1-1 `JOIN`'s quarantine, readers, chained positions and lease
//! (#1780, #1460 sub-step 3.14; #1464's held/release, fence, reader,
//! chained and lease pins). Every pin runs at the middle position of three
//! joined tables (`Shape { positions: 3, at: 2 }`, n = 4), so a rule that
//! covers only the first joined table, or only the first and last, fails it.
//!
//! - **Held and released.** A key of `b2` held in quarantine leaves `M_2`'s
//!   entry as it last applied, and its release re-derives it and fans out.
//!   A row of `a` a `b2` fan-out holds is parked with the `b2` change's
//!   origin (`held_origin` merges every position's changes).
//! - **Fences over every joined table.** A column pause bumps the version
//!   fence of every table the star reads, so it waits for a page of `b2`; a
//!   `b2` fan-out that holds a row takes the definition's row before its lock
//!   row, so a rebuild's start queues behind it rather than deadlocking.
//! - **Readers of a star target**, an aggregate on the group basis, a 1-1
//!   copy and a two-table 1-1 `JOIN` over its primary key, converge through
//!   the target-mutation seam's image feed.
//! - **A target at a joined position**: a star whose middle table is another
//!   transform's 1-1 target, joined on its primary key, hears that target's
//!   changes through the seam.
//! - **The lease**: a `b2` fan-out whose worker loses its claim while it
//!   holds the key's locks commits nothing, and the next instance applies
//!   the change once.
//!
//! Pages are driven by hand (`support/drain_driver.rs`); nothing sleeps or
//! polls for convergence but the lock a pause or a start must queue on.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use star::{Shape, Star};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;
use trellis::staging::{ApplyError, liveness, quarantine};

/// The middle position of three joined tables.
const SHAPE: Shape = Shape {
    positions: 3,
    at: 2,
};

/// `a` rows `(id, k2, x, y)` and `b2` rows `(id, k, w, tag)`.
const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 20, 2, 'two'), (3, 30, 3, 'three')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";

/// An `x` whose total fits with `b2.w = 100` and overflows with `1000`: the
/// idle positions add 4000.
const BIG_X: i32 = 2_147_479_000;

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn knob() {
    trellis::dev::defs::set_star_join(true);
}

/// The all-`LEFT` star of [`SHAPE`].
fn left() -> Star {
    SHAPE.stars()[0]
}

async fn total(d: &Driver, star: Star, id: i32) -> Option<i32> {
    d.ctl
        .query_opt(
            &format!("select total from {} where id = $1", star.target()),
            &[&id],
        )
        .await
        .expect("read the target")
        .map(|row| row.get(0))
}

async fn status(d: &Driver, star: Star) -> String {
    d.ctl
        .query_one(
            "select status::text from transform_definitions where target_table = $1",
            &[&star.target()],
        )
        .await
        .expect("the definition")
        .get(0)
}

/// `a` rows 5 to 40 on key 10 at the middle position, those in `big` with
/// [`BIG_X`].
async fn seed_key_10(d: &mut Driver, big: &[i32]) {
    let rows: Vec<String> = (5..=40)
        .map(|id| {
            let x = if big.contains(&id) { BIG_X } else { id };
            format!("({id}, 10, {x}, 'r')")
        })
        .collect();
    star::step(
        d,
        &SHAPE.sql(&format!("insert into {{a_ins}} values {}", rows.join(", "))),
    )
    .await;
}

/// Releases `key` of `table` for `star` and drains the Re-derive it stages.
async fn release(d: &mut Driver, star: Star, table: &str, key: &str) {
    let held = quarantine::release_key(d.pool(), &star.name(), table, key)
        .await
        .expect("release the key");
    assert_eq!(held, 1, "{key} had its parked work");
    let batch = d.seal().await;
    d.drain(batch, "release").await;
}

/// The oracle, and nothing held.
async fn assert_exact(d: &mut Driver, star: Star, step: &str) {
    star::assert_oracle(d, &[star]).await;
    let name = star.name();
    assert!(
        d.rows("select key from poison").await.is_empty(),
        "{name} after {step}"
    );
    assert!(
        d.rows("select key from poison_held").await.is_empty(),
        "{name} after {step}"
    );
    assert_eq!(status(d, star).await, "live", "{name} after {step}");
}

// ------------------------------------------------------------ held, released

/// Port of `a_held_key_of_b_is_released_by_a_rederive_and_its_fan_out` at the
/// middle position: a check on the target refuses a row of a `b2` change's
/// fan-out, so the drain's isolation charges the change until it holds the
/// `b2` key. `M_2`'s entry stays as it last applied, and an `a` row moving
/// onto the key joins it. The release re-derives the `b2` row and fans out
/// to every row on its key.
#[tokio::test]
async fn a_held_key_at_the_middle_position_is_released_by_a_rederive_and_its_fan_out() {
    let _serial = star::serial().await;
    knob();
    for star in SHAPE.stars() {
        let name = star.name();
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        d.ctl
            .batch_execute(&format!(
                "alter table {} add constraint small check (total < 10000)",
                star.target()
            ))
            .await
            .expect("add the check");
        star::write(
            &d,
            "update public.b2 set w = 9999, tag = 'big' where id = 1",
        )
        .await;
        let batch = d.seal().await;
        let mut drained = false;
        for _ in 0..12 {
            if trellis::staging::apply::drain_once(d.pool(), batch, "page", 1, "j1_wake")
                .await
                .is_ok()
            {
                drained = true;
                break;
            }
        }
        assert!(drained, "{name}: the page drains once the b2 key is held");
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            ["(public.b2,1)"],
            "{name}"
        );
        assert_eq!(total(&d, star, 1).await, Some(1 + 100 + 4000), "{name}");
        star::step(&mut d, "update public.a set k2 = 10 where id = 2").await;
        assert_eq!(
            total(&d, star, 2).await,
            Some(2 + 100 + 4000),
            "{name}: a row moving onto the held key joins M_2's entry as it last applied"
        );
        d.ctl
            .batch_execute(&format!(
                "alter table {} drop constraint small",
                star.target()
            ))
            .await
            .expect("drop the check");
        release(&mut d, star, "public.b2", "1").await;
        assert_exact(&mut d, star, "the release of b2").await;
        assert_eq!(total(&d, star, 2).await, Some(2 + 9999 + 4000), "{name}");
    }
}

/// A field failing on one row of a `b2` change's fan-out holds that row of
/// `a`, and the change applies to every other row. The hold names the
/// position and join key that caused it (#1457 review finding 10), and the
/// row is parked with the `b2` change's origin, so it holds back the
/// watermark tokens that change does; its release re-derives it.
///
/// Pins the `star_held_origin_first_only` plant (the parked row's origin is
/// unknown).
#[tokio::test]
async fn a_row_a_middle_fan_out_holds_is_parked_with_the_changes_origin() {
    let _serial = star::serial().await;
    knob();
    for star in SHAPE.stars() {
        let name = star.name();
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        seed_key_10(&mut d, &[23]).await;
        star::step(&mut d, "update public.b2 set w = 1000 where id = 1").await;
        assert_eq!(status(&d, star).await, "live", "{name}");
        assert_eq!(
            d.rows("select src_table, key from poison").await,
            ["(public.a,23)"],
            "{name}"
        );
        assert_eq!(
            d.rows("select src_table, key, origin_lsn is not null from poison_held")
                .await,
            ["(public.a,23,t)"],
            "{name}: the held row is parked with the b2 change's origin"
        );
        let error = d.rows("select last_error from poison").await;
        assert!(
            error.len() == 1
                && error[0].contains(
                    "held by the fan-out of a change of public.b2, the joined table at \
                     position 2, on join key (10)"
                ),
            "{name}: the hold names the position and key that caused it: {error:?}"
        );
        assert_eq!(
            total(&d, star, 23).await,
            Some(BIG_X + 100 + 4000),
            "{name}: the held row stays as it was"
        );
        assert_eq!(total(&d, star, 24).await, Some(24 + 1000 + 4000), "{name}");
        star::step(&mut d, "update public.a set x = 23 where id = 23").await;
        release(&mut d, star, "public.a", "23").await;
        assert_exact(&mut d, star, "the release").await;
        assert_eq!(total(&d, star, 23).await, Some(23 + 1000 + 4000), "{name}");
    }
}

// -------------------------------------------------------------------- fences

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `work` finishes first (`false`).
async fn queued_or_finished<T>(d: &Driver, pid: i32, work: &drain_driver::Running<T>) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
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
        if blocked || work.is_finished() {
            return blocked;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "neither queued nor finished within 60 s"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Port of `a_column_pause_waits_for_a_page_of_the_joined_table` at the
/// middle position: a pause of a star's column bumps the version fence of
/// every table the star reads, since a page of any of them writes its
/// fields. A `b2` page that planned the column as live and holds `b2`'s
/// fence makes the pause wait for its commit, and the column is frozen from
/// the pause's return.
///
/// Pins the `star_fence_first_only` plant (the pause runs beside the page).
#[tokio::test]
async fn a_column_pause_waits_for_a_page_of_the_middle_joined_table() {
    let _serial = star::serial().await;
    knob();
    let star = left();
    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
    star::write(&d, "update public.b2 set w = 500 where id = 1").await;
    let batch = d.seal().await;
    let target = star.target();
    let mut page = d
        .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &target)])
        .await;
    let reached = page.reached(PausePoint::BeforeCommit).await;
    let name = star.name();
    let pause = d
        .run_frozen(&[], |pool| async move {
            quarantine::pause_column(&pool, &name, "total").await
        })
        .await;
    let queued = queued_or_finished(&d, reached.backend_pid, &pause).await;
    d.release(&mut page, PausePoint::BeforeCommit).await;
    page.finish().await;
    pause.finish().await;
    assert!(
        queued,
        "the pause ran beside a page of b2, which writes the column"
    );
    assert_eq!(total(&d, star, 1).await, Some(1 + 500 + 4000));
    star::step(&mut d, "update public.b2 set w = 600 where id = 1").await;
    assert_eq!(
        total(&d, star, 1).await,
        Some(1 + 500 + 4000),
        "the paused column is frozen"
    );
}

/// Port of `a_fan_out_that_holds_a_row_and_a_rebuild_start_never_deadlock`
/// at the middle position: a `b2` page whose fan-out holds a row writes
/// `poison`, whose foreign key takes the definition's row `FOR KEY SHARE`,
/// so the page takes that lock before its lock row. A rebuild's start (the
/// definition's row `FOR UPDATE`, then the lock row), played by hand, queues
/// behind the page.
///
/// Pins the `star_def_lock_first_only` plant (a deadlock).
#[tokio::test]
async fn a_middle_fan_out_that_holds_a_row_and_a_rebuild_start_never_deadlock() {
    let _serial = star::serial().await;
    knob();
    let star = left();
    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
    seed_key_10(&mut d, &[23]).await;
    let id = star::definition_id(&d, star).await;
    star::write(&d, "update public.b2 set w = 1000 where id = 1").await;
    let batch = d.seal().await;
    let target = star.target();
    let mut page = d
        .drain_frozen(
            batch,
            "page",
            &[(PausePoint::BeforeJoin1Recompute, &target)],
        )
        .await;
    let reached = page.reached(PausePoint::BeforeJoin1Recompute).await;
    let start = d.user().await;
    let lock = format!("select 1 from {DEFAULT_SCHEMA}.join1_{id}_l for update");
    let start_lock = tokio::spawn(async move {
        start.batch_execute("begin").await?;
        start
            .execute(
                "select 1 from transform_definitions where id = $1 for update",
                &[&id],
            )
            .await?;
        start.execute(&lock, &[]).await?;
        start.batch_execute("commit").await
    });
    d.wait_blocked_behind(reached.backend_pid).await;
    d.release(&mut page, PausePoint::BeforeJoin1Recompute).await;
    let page_result = page.finish_result().await;
    let start_result = start_lock.await.expect("the start task");
    let deadlocks = d.deadlocks_logged();
    assert!(
        deadlocks.is_empty(),
        "page {:?}, start {start_result:?}: {deadlocks:?}",
        page_result.as_ref().map(|_| ())
    );
    page_result.expect("the page");
    start_result.expect("the start");
    assert_eq!(d.rows("select key from poison").await, ["(23)"]);
}

// ------------------------------------------------------------------- readers

/// The live rows of `reader`'s query and its target, each sorted.
async fn assert_reader(d: &Driver, actual: &str, expected: &str, when: &str) {
    assert_eq!(
        d.rows(actual).await,
        d.rows(expected).await,
        "{actual} (left) differs from its source (right) after {when}"
    );
}

/// Readers of a star target converge: an aggregate on the group basis (which
/// applies each change's images as a delta, so a row moving groups needs its
/// old image), a 1-1 copy, and a two-table 1-1 `JOIN` of `a` with the
/// target on its primary key. Every write is at the middle position or
/// moves a root row there.
///
/// Fails under the `seam_imageless_row` plant (the aggregate keeps a row in
/// the group it left).
#[tokio::test]
async fn readers_of_a_star_target_converge() {
    let _serial = star::serial().await;
    knob();
    for star in SHAPE.stars() {
        let name = star.name();
        let target = star.target();
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        d.ctl
            .batch_execute(&format!("create index on {target} (tag2)"))
            .await
            .expect("index the target's tag2");
        let mut columns: HashMap<String, ValueType> = SHAPE
            .columns()
            .into_iter()
            .chain([("total".to_string(), int4())])
            .collect();
        for p in SHAPE.all() {
            columns.insert(format!("tag{p}"), ValueType::Text);
        }
        for reader in [
            format!(
                "TRANSFORM {name}_sums FROM {target} GROUP BY tag2 \
                 SELECT tag2 AS tag2, SUM(total) AS s, COUNT(*) AS n"
            ),
            format!("TRANSFORM {name}_copy FROM {target} SELECT total - 1 AS less, tag2 AS tag2"),
            format!(
                "TRANSFORM {name}_over FROM public.a LEFT JOIN {target} ON a.id = {name}.id \
                 SELECT {name}.total AS t, a.y AS y"
            ),
        ] {
            trellis::defs::install_definition(d.pool(), &reader, &columns, "public")
                .await
                .unwrap_or_else(|err| panic!("define a reader: {reader}: {err}"));
            trellis::intake::harness::settle_builds(d.pool()).await;
        }
        d.settle().await;
        let checks = [
            (
                format!("select tag2, s, n from public.{name}_sums order by tag2 nulls first"),
                format!(
                    "select tag2, sum(total), count(*) from {target} group by tag2 \
                     order by tag2 nulls first"
                ),
            ),
            (
                format!("select id, less, tag2 from public.{name}_copy order by id"),
                format!("select id, total - 1, tag2 from {target} order by id"),
            ),
            (
                format!("select id, t, y from public.{name}_over order by id"),
                format!(
                    "select a.id, t.total, a.y from public.a a left join {target} t \
                     on a.id = t.id order by a.id"
                ),
            ),
        ];
        for (when, sql) in [
            ("the seed", ""),
            (
                "a b2 row changing tag",
                "update public.b2 set tag = 'moved' where id = 1",
            ),
            (
                "a root row moving key",
                "update public.a set k2 = 10 where id = 2",
            ),
            ("a b2 row deleted", "delete from public.b2 where id = 2"),
            (
                "a b2 row inserted",
                "insert into public.b2 values (3, 30, 300, 'thirty')",
            ),
            (
                "a b2 row moving key",
                "update public.b2 set k = 20 where id = 1",
            ),
            ("a root row deleted", "delete from public.a where id = 3"),
        ] {
            if !sql.is_empty() {
                star::step(&mut d, sql).await;
            }
            for _ in 0..3 {
                let batch = d.seal().await;
                d.drain(batch, "round").await;
            }
            star::assert_oracle(&mut d, &[star]).await;
            for (actual, expected) in &checks {
                assert_reader(&d, actual, expected, &format!("{name}: {when}")).await;
            }
        }
    }
}

// ------------------------------------------------- a target at a position

/// A star whose middle table is a 1-1 target, `t2` (of `src`), joined on its
/// primary key: `src`'s changes reach `t2` and, through the seam's image
/// feed, the star. Each writer touches `src` or moves a root row at the
/// middle position.
///
/// Pins the `star_reads_first_only` plant (the seam finds no reader of `t2`
/// and stages nothing, so the star keeps `t2`'s old rows).
#[tokio::test]
async fn a_star_with_a_one_to_one_target_at_the_middle_position_converges() {
    let _serial = star::serial().await;
    knob();
    for word in ["LEFT", "INNER"] {
        let ddl = "create table public.a (id integer primary key, k1 integer, k2 integer, \
                       k3 integer, x integer); \
                   create table public.b1 (id integer primary key, k integer, w integer); \
                   create unique index on public.b1 (k); \
                   create table public.src (id integer primary key, w integer, tag text); \
                   create table public.b3 (id integer primary key, k integer, w integer); \
                   create unique index on public.b3 (k); \
                   create index on public.a (k1); create index on public.a (k2); \
                   create index on public.a (k3); \
                   insert into public.b1 values (1, 1, 1000), (2, 2, 1000); \
                   insert into public.b3 values (1, 1, 3000), (2, 2, 3000); \
                   insert into public.src values (10, 100, 'ten'), (20, 200, 'twenty'); \
                   insert into public.a values (1, 1, 10, 1, 1), (2, 2, 20, 2, 2), \
                       (3, 1, 30, 2, 3), (4, 2, 10, 1, 4)";
        let mut d = Driver::start(
            ddl,
            &[
                ("id", int4()),
                ("k1", int4()),
                ("k2", int4()),
                ("k3", int4()),
                ("x", int4()),
                ("w", int4()),
                ("tag", ValueType::Text),
            ],
            &["TRANSFORM t2 FROM public.src SELECT w AS w, tag AS tag"],
            &["public.src"],
        )
        .await;
        let star = format!(
            "TRANSFORM jt FROM public.a LEFT JOIN public.b1 ON a.k1 = b1.k \
             {word} JOIN public.t2 ON a.k2 = t2.id LEFT JOIN public.b3 ON a.k3 = b3.k \
             SELECT a.x + COALESCE(b1.w, 0) + COALESCE(t2.w, 0) + COALESCE(b3.w, 0) AS total, \
             t2.tag AS tag2"
        );
        let columns: HashMap<String, ValueType> = [
            ("total".to_string(), int4()),
            ("tag2".to_string(), ValueType::Text),
        ]
        .into_iter()
        .collect();
        trellis::defs::install_definition(d.pool(), &star, &columns, "public")
            .await
            .unwrap_or_else(|err| panic!("define the star over t2: {err}"));
        for table in ["public.a", "public.b1", "public.b3"] {
            capture(&mut d, table).await;
        }
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        let join = if word == "LEFT" { "left join" } else { "join" };
        let expected = format!(
            "select a.id, a.x + coalesce(b1.w, 0) + coalesce(s.w, 0) + coalesce(b3.w, 0), \
                    s.tag \
             from public.a a left join public.b1 b1 on a.k1 = b1.k \
             {join} public.src s on a.k2 = s.id left join public.b3 b3 on a.k3 = b3.k \
             order by a.id"
        );
        let actual = "select id, total, tag2 from public.jt order by id";
        for (when, sql) in [
            ("the build", ""),
            (
                "a src row changing value",
                "update public.src set w = 150, tag = 'ten!' where id = 10",
            ),
            (
                "a src row inserted",
                "insert into public.src values (30, 300, 'thirty')",
            ),
            ("a src row deleted", "delete from public.src where id = 20"),
            (
                "a src row's key moving",
                "update public.src set id = 20 where id = 30",
            ),
            (
                "a root row moving key",
                "update public.a set k2 = 20 where id = 1",
            ),
        ] {
            if !sql.is_empty() {
                star::step(&mut d, sql).await;
            }
            for _ in 0..4 {
                let batch = d.seal().await;
                d.drain(batch, "round").await;
            }
            d.settle().await;
            assert_eq!(
                d.rows(actual).await,
                d.rows(&expected).await,
                "{word} at the middle position: the star (left) differs from the live join \
                 over src (right) after {when}"
            );
        }
        let statuses = d
            .rows("select status::text from transform_definitions order by id")
            .await;
        assert_eq!(statuses, ["(live)", "(live)"], "{word}");
    }
}

/// Captures `table`, once a definition reads it.
async fn capture(d: &mut Driver, table: &str) {
    let catalog = trellis::capture::columns::load_catalog(&d.ctl)
        .await
        .expect("load the catalog");
    let spec = trellis::capture::columns::capture_spec(&d.ctl, &catalog, table)
        .await
        .expect("capture spec");
    match trellis::capture::install::reconcile(&mut d.ctl, DEFAULT_SCHEMA, &spec, None)
        .await
        .expect("capture the table")
    {
        trellis::capture::install::Progress::Done(_) => {}
        trellis::capture::install::Progress::Waiting(wait) => panic!("capture waited: {wait}"),
    }
}

// --------------------------------------------------------------------- lease

/// The rows on key 10 at the middle position: one fan-out's worth.
const CAP: i32 = 25;

/// Ages `seg_seq`'s claims past the TTL and sweeps them, as the maintenance
/// loop does for a worker whose heartbeat stalled.
async fn expire_claims(d: &Driver, seg_seq: i64) -> u64 {
    d.ctl
        .execute(
            "update seg_claims set claimed_at = now() - interval '1 hour' where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("age the claims");
    liveness::reclaim_stale(&d.ctl, Duration::from_secs(30))
        .await
        .expect("reclaim stale claims")
}

/// Port of `a_fan_out_that_outlives_its_claim_is_applied_once_by_the_next_instance`
/// at the middle position: a `b2` fan-out whose worker loses its claim to the
/// TTL sweep while the page is frozen before the fan-out, holding the key's
/// locks. The second instance claims the freed bucket and queues behind the
/// page's locks; the first then fails its claim check and rolls back
/// (`ClaimLost`), and the second applies the change once.
#[tokio::test]
async fn a_middle_fan_out_that_outlives_its_claim_is_applied_once_by_the_next_instance() {
    let _serial = star::serial().await;
    knob();
    for star in SHAPE.stars() {
        let name = star.name();
        let rows: Vec<String> = (1..=CAP)
            .map(|id| format!("({id}, 10, {id}, 'r')"))
            .collect();
        let a_rows = format!("{}, (100, 20, 100, 'r')", rows.join(", "));
        let mut d = star::start(&[star], &a_rows, B_ROWS).await;
        star::write(&d, "update public.b2 set w = 500 where id = 1").await;
        let batch = d.seal().await;
        let target = star.target();
        let mut first = d
            .drain_frozen(batch, "w1", &[(PausePoint::BeforeJoin1Recompute, &target)])
            .await;
        let reached = first.reached(PausePoint::BeforeJoin1Recompute).await;
        let first_xid: String = d
            .ctl
            .query_one(
                "select backend_xid::text from pg_stat_activity where pid = $1",
                &[&reached.backend_pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get::<_, Option<String>>(0)
            .expect("the frozen page has written, so it holds an xid");
        assert!(
            expire_claims(&d, batch).await > 0,
            "{name}: a claim not held under a row lock is swept once it is stale"
        );
        let second = d.drain_frozen(batch, "w2", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        d.release(&mut first, PausePoint::BeforeJoin1Recompute)
            .await;
        let lost = first.finish_result().await;
        assert!(
            matches!(lost, Err(ApplyError::ClaimLost)),
            "{name}: the stale worker's page must fail its claim check: {lost:?}"
        );
        second.finish().await;
        let from_the_first = d
            .rows(&format!(
                "select count(*) from {target} where xmin::text = '{first_xid}'"
            ))
            .await;
        assert_eq!(
            from_the_first,
            ["(0)"],
            "{name}: nothing the first page wrote committed"
        );
        star::assert_oracle(&mut d, &[star]).await;
        let deadlocks = d.deadlocks_logged();
        assert!(deadlocks.is_empty(), "{deadlocks:?}");
        assert_eq!(
            d.rows(&format!(
                "select count(*) from {target} where id <= {CAP} and total = id + 500 + 4000"
            ))
            .await,
            [format!("({CAP})")],
            "{name}: each row has x + 500"
        );
    }
}
