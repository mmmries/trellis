//! A star 1-1 `JOIN`'s build, rebuild and `RESUME` (#1777, #1460 sub-step
//! 3.11; #1464's PP-C, PS6 and PX1 to PX3 pins): the plan job's `M` pass runs
//! at every position, a build chunk locks `J` at every position for its
//! range's live keys and the keys `R` holds and retries on a late key at any
//! position, and a `RESUME` recreates every position's `M`.
//!
//! The star is `star.rs`'s fixture at n = 3 (`a` joined to `b1` and `b2`),
//! the position under test the last unless a pin says otherwise; the idle
//! position holds 200 rows of its own, so every `M` pass walks rows at every
//! position. Every pin runs under every kind tuple and ends with the oracle:
//! the target is the live multi-join, `R` is `a` and each `M_p` is `b{p}`.
//!
//! - **PP-C** (#1375's C families, at position 2): a chunk frozen with its
//!   rows written holds its `J` rows, so a page on one of its keys queues
//!   behind it; a chunk over a page's frozen row gives up and retries; and
//!   C10 for each of the chunk's three checks (its insert-from-read, its
//!   held keys, its Re-derive read).
//! - **PS6a / PS6b** (F8e / F8d): C10's insert-from-read at the last / first
//!   position.
//! - **PX1** (H1): a `RESUME` removes the rows of `a` rows deleted while
//!   paused. **PX2** (H3): a row re-inserted after the orphan pass's read is
//!   kept. **PX3**: the `M` pass of a rebuild in place tombstones position 2's
//!   entries of rows gone past capture.
//! - One end-to-end n = 4 build with rows at every position: a build in small
//!   chunks, a `RESUME` after changes of every table while paused, and a
//!   rebuild in place after changes of every table past capture.
//! - A producer (#1401): an aggregate's target at position 2, keyed by its
//!   nullable `GROUP BY` column, whose group keyed `NULL` the `M` pass reads.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use std::collections::HashMap;

use drain_driver::Driver;
use star::{Shape, Star};
use trellis::config::{Config, DEFAULT_SCHEMA};
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build::{self, Step};
use trellis::staging::interleave::PausePoint;
use trellis::{Trellis, TrellisOptions};

/// The star of three tables, its last position under test.
const N3: Shape = Shape {
    positions: 2,
    at: 2,
};

/// The star of three tables, its first position under test.
const N3_FIRST: Shape = Shape {
    positions: 2,
    at: 1,
};

/// `a` rows `(id, k, x, y)` on the position under test's keys 10 to 30, one
/// on `NULL`; its table's rows `(id, k, w, tag)` on 10 to 30.
const A_ROWS: &str = "(1, 10, 1, 'a1'), (2, 20, 2, 'a2'), (3, 30, 3, 'a3'), (4, null, 4, 'a4'), \
                      (5, 10, 5, 'a5'), (6, 40, 6, 'a6')";
const B_ROWS: &str = "(1, 10, 100, 'b10'), (2, 20, 200, 'b20'), (3, 30, 300, 'b30')";

/// Turns the development knob on: every pin here defines a star.
fn knob() {
    trellis::dev::defs::set_star_join(true);
}

async fn trellis(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
}

/// Settles every build and the pipeline, then asserts each of `stars` and
/// that nothing deadlocked.
async fn assert_oracle(d: &mut Driver, stars: &[Star]) {
    build::settle_builds_with(d.pool(), 3).await;
    star::assert_oracle(d, stars).await;
    let deadlocks = d.deadlocks_logged();
    assert!(deadlocks.is_empty(), "{deadlocks:?}");
}

/// Asserts `star`'s target equals the live multi-join.
async fn assert_target(d: &Driver, star: Star, step: &str) {
    let (actual, expected) = (star.actual(), star.expected());
    let extra = d.rows(&format!("({actual}) except all ({expected})")).await;
    let missing = d.rows(&format!("({expected}) except all ({actual})")).await;
    assert!(
        extra.is_empty() && missing.is_empty(),
        "{} after {step}: rows only in the target {extra:?}, rows only in the join {missing:?}",
        star.name()
    );
}

/// Asserts `star`'s `M` at position `p` holds `b{p}`'s live rows, and
/// nothing else live.
async fn assert_m(d: &Driver, star: Star, p: usize, step: &str) {
    let m = star::m_table(d, star, p).await;
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
        "{} M at position {p} (left) differs from b{p} after {step}",
        star.name()
    );
}

/// How many of the builds' jobs of `kinds` are still to run.
async fn pending(d: &Driver, kinds: &[&str]) -> i64 {
    let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
    d.ctl
        .query_one(
            "select count(*) from backfill_chunks where not done and kind = any($1)",
            &[&kinds],
        )
        .await
        .expect("count the build's jobs")
        .get(0)
}

/// Runs build steps until no plan job or chunk is left, so the next step
/// claims the sweep.
async fn run_until_the_sweep(d: &Driver) {
    for _ in 0..1000 {
        if pending(d, &["plan", "rederive"]).await == 0 {
            return;
        }
        star::build_step(d.pool(), "builder", 3).await;
    }
    panic!("the plan and its chunks never finished");
}

/// Starts a rebuild in place of a live star and runs its plan job, so `R`
/// holds every row on the keys it had and the chunks are enqueued.
async fn rebuild_planned(d: &mut Driver) {
    // No segment retires while a build runs, so the drained ones go first.
    d.retire().await;
    trellis(d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    assert_eq!(
        star::build_step(d.pool(), "planner", 100).await,
        Step::Planned
    );
}

/// Pauses `stars`, runs `sql` and drains it while they're paused (so no page
/// will ever apply it to them), then resumes them.
async fn change_while_paused(d: &mut Driver, stars: &[Star], sql: &str) {
    let trellis = trellis(d).await;
    for star in stars {
        trellis
            .apply(&format!("PAUSE TRANSFORM {}", star.name()))
            .await
            .expect("pause");
    }
    star::step(d, sql).await;
    for star in stars {
        trellis
            .apply(&format!("RESUME TRANSFORM {}", star.name()))
            .await
            .expect("resume");
    }
    trellis::intake::harness::start_registered_builds(d.pool())
        .await
        .expect("start the resumed builds");
}

/// Waits until some backend queues on a lock `pid` holds (`true`), or until
/// `page` finishes without one having queued (`false`).
async fn queued_or_finished<T>(d: &Driver, pid: i32, page: &drain_driver::Running<T>) -> bool {
    star::queued_or_finished(d, pid, page).await
}

/// Runs one build step that gives up on a lock of `page`, a page of an `a`
/// change frozen at its commit (`reached`), and returns it once the page has
/// committed (`join_one_to_one_build`'s helper of the same name: the step's
/// record of its failure queues behind the page on the definition's row).
async fn step_behind_frozen_page(
    d: &Driver,
    page: &mut drain_driver::RunningDrain,
    reached: &trellis::staging::interleave::Reached,
    worker: &'static str,
    chunk_rows: i64,
) -> Step {
    let pool = d.pool().clone();
    let started = std::time::Instant::now();
    let step = tokio::spawn(async move { star::build_step(&pool, worker, chunk_rows).await });
    let deadline = started + std::time::Duration::from_secs(60);
    let gave_up = loop {
        let queued: bool = d
            .ctl
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)) \
                   and query like '%transform_definitions%for update%')",
                &[&reached.backend_pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if queued {
            break started.elapsed();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the step's failure never queued on the definition's row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    d.release(page, PausePoint::BeforeCommit).await;
    let step = step.await.expect("the build step's task");
    assert!(
        gave_up < trellis::locks::LOCK_TIMEOUT,
        "the step gave up on the page's lock under its short lock timeout, \
         not the session's: it took {gave_up:?}"
    );
    step
}

// ----------------------------------------------------------- PP-C: the chunks

/// PP-C (C1 to C9, a chunk first): a chunk frozen with everything written
/// holds its `J` rows, at position 2 too, so a page of `b2` on one of its
/// keys queues behind it and reads `R` and the target only once the chunk
/// commits.
#[tokio::test]
async fn ppc_a_page_at_position_2_on_a_chunks_key_queues_behind_the_chunk() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
        assert_eq!(
            star::build_step(d.pool(), "planner", 100).await,
            Step::Planned
        );
        let target = star.target();
        let mut chunk = d
            .run_frozen(
                &[(PausePoint::BeforeRebuildCommit, &target)],
                |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
            )
            .await;
        let reached = chunk.reached(PausePoint::BeforeRebuildCommit).await;
        star::write(&d, "update public.b2 set w = 777 where id = 1").await;
        let batch = d.seal().await;
        let page = d.drain_frozen(batch, "page", &[]).await;
        d.wait_blocked_behind(reached.backend_pid).await;
        assert!(!page.is_finished(), "the page queued on the chunk's J row");
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish().await;
        page.finish().await;
        assert_oracle(&mut d, &[star]).await;
    }
}

/// PP-C (C1 to C9, a page first): a page frozen at its commit holds its `J`
/// row at position 2; a chunk over the page's row waits for it at most its
/// short lock timeout, gives up and retries.
#[tokio::test]
async fn ppc_a_chunk_on_a_pages_key_at_position_2_gives_up_and_retries() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
        assert_eq!(
            star::build_step(d.pool(), "planner", 100).await,
            Step::Planned
        );
        star::write(&d, "update public.a set x = 55, k2 = 20 where id = 1").await;
        let batch = d.seal().await;
        let mut page = d
            .drain_frozen(batch, "page", &[(PausePoint::BeforeCommit, &star.target())])
            .await;
        let reached = page.reached(PausePoint::BeforeCommit).await;
        assert_eq!(
            step_behind_frozen_page(&d, &mut page, &reached, "chunk", 100).await,
            Step::Chunk
        );
        page.finish().await;
        let failed: Option<String> = d
            .ctl
            .query_one(
                "select last_error from backfill_chunks where kind = 'rederive'",
                &[],
            )
            .await
            .expect("the chunk")
            .get(0);
        assert!(
            failed.as_deref().is_some_and(|e| e.contains("lock")),
            "the chunk gave up on the page's lock: {failed:?}"
        );
        assert_oracle(&mut d, &[star]).await;
    }
}

/// C10 on the chunk's insert-from-read at `shape.at`: `a` row 1 moves from
/// key 10 onto 70 there after the chunk's unlocked read of its range, so its
/// insert-from-read finds key 70, which it holds no `J` row for. It rolls
/// back and its retry locks 70. A page inserting the row on 70 into the
/// table at that position, sealed before the chunk, drains while the chunk
/// is frozen: planted, it fans out around the chunk's uncommitted entry, and
/// the chunk's row keeps no match (the move's own Apply is refused, its
/// change being in the chunk's basis).
async fn c10_insert_from_read(shape: Shape) {
    for star in shape.stars() {
        let mut d = star::start_unbuilt(&[star], A_ROWS, B_ROWS).await;
        // No row on 70 at that position yet: the `M` pass reads none.
        assert_eq!(
            star::build_step(d.pool(), "planner", 100).await,
            Step::Planned
        );
        star::write(&d, &shape.sql("insert into {b} values (7, 70, 700, 'k7')")).await;
        let insert = d.seal().await;
        let target = star.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
            )
            .await;
        chunk.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, &shape.sql("update public.a set {k} = 70 where id = 1")).await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        // Frozen with its rows written (planted), or rolled back.
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        d.drain(insert, "the insert on 70").await;
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "{}: the chunk rolled back on the moved key",
                star.name()
            );
        }
        let moved = d.seal().await;
        d.drain(moved, "the move").await;
        assert_oracle(&mut d, &[star]).await;
    }
}

/// PS6a (F8e) and PP-C's C10 on the insert-from-read at position 2: the
/// chunk's late key is at the last position. Planted `star_chunk_skip_j_last`
/// (and `join1_chunk_no_key`, at any position), the target keeps no match.
#[tokio::test]
async fn ps6a_a_chunk_whose_row_moved_onto_a_key_it_did_not_lock_at_the_last_position_retries() {
    let _serial = star::serial().await;
    knob();
    c10_insert_from_read(N3).await;
}

/// PS6b (F8d): the same at the first position. Planted
/// `star_chunk_skip_j_first`.
#[tokio::test]
async fn ps6b_a_chunk_whose_row_moved_onto_a_key_it_did_not_lock_at_the_first_position_retries() {
    let _serial = star::serial().await;
    knob();
    c10_insert_from_read(N3_FIRST).await;
}

/// PP-C's C10 on the chunk's Re-derive read at position 2: on a rebuild, row
/// 1 has its `R` entry on key 10 there, so the chunk re-derives it. The row
/// moves onto 70 after the chunk's unlocked read; the chunk's read of `a`
/// finds 70, which it holds no `J` row for, so it rolls back and its retry
/// locks 70. Planted `join1_rd_no_key`, the entry moves onto 70 unlocked and
/// the `b2` insert's page on 70 fans out around it.
#[tokio::test]
async fn ppc_c10_a_chunk_whose_rederived_row_moved_at_position_2_retries() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        rebuild_planned(&mut d).await;
        // Sealed before the chunk opens (the seal gate refuses while the
        // chunk's transaction is open).
        star::write(&d, "insert into public.b2 values (7, 70, 700, 'k7')").await;
        let insert = d.seal().await;
        let target = star.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move { Ok(star::build_step(&pool, "chunk", 100).await) },
            )
            .await;
        chunk.reached(PausePoint::BeforeJoin1Lock).await;
        star::write(&d, "update public.a set k2 = 70 where id = 1").await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        d.drain(insert, "the insert on 70").await;
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "{}: the chunk rolled back on the moved key",
                star.name()
            );
        }
        let moved = d.seal().await;
        d.drain(moved, "the move").await;
        assert_oracle(&mut d, &[star]).await;
    }
}

/// PP-C's C10 on the chunk's held keys at position 2: on a rebuild, row 1
/// moves onto 70 and back onto 10 there, neither drained, so the chunk reads
/// the row on 10 and its `R` entry on 10, unlocked. While it waits before its
/// `J` lock, the first move's page moves the entry onto 70. The chunk's read
/// of `a` finds 10, in its key set, but the entry it locks holds 70, so it
/// rolls back and its retry locks 70. (Planted `join1_no_recheck` is masked
/// here as in the two-table pin: the `b2` insert's page locks the `R`
/// entries on 70, the R-entry rule, and finds the entry moved.)
#[tokio::test]
async fn ppc_c10_a_chunk_whose_entry_moved_off_its_key_set_at_position_2_retries() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        rebuild_planned(&mut d).await;
        // The build's segment, sealed and drained, so the moves below are in
        // later segments: their pages apply them rather than re-derive.
        star::step(&mut d, "update public.a set y = y where id = 6").await;
        star::write(&d, "update public.a set k2 = 70 where id = 1").await;
        let forth = d.seal().await;
        star::write(&d, "insert into public.b2 values (7, 70, 700, 'k7')").await;
        let insert = d.seal().await;
        star::write(&d, "update public.a set k2 = 10 where id = 1").await;
        let target = star.target();
        let moves = trellis::dev::interleave::join_key_moves();
        let mut chunk = d
            .run_frozen(
                &[
                    (PausePoint::BeforeJoin1Lock, &target),
                    (PausePoint::BeforeRebuildCommit, &target),
                ],
                |pool| async move {
                    // Claimed by hand: with two batches sealed and not
                    // drained, a worker's build step wouldn't claim it.
                    let claimed = {
                        let client = pool.get().await.expect("pool");
                        trellis::defs::chunk_queue::claim_chunks_of(
                            &**client,
                            "chunk",
                            1,
                            &[trellis::defs::chunk_queue::KIND_REDERIVE],
                        )
                        .await
                        .expect("claim the chunk")
                    };
                    let chunk = claimed.into_iter().next().expect("a chunk");
                    build::run_claimed(&pool, &chunk, "chunk", &star::build_options(100)).await;
                    Ok(())
                },
            )
            .await;
        let reached = chunk.reached(PausePoint::BeforeJoin1Lock).await;
        d.drain(forth, "the move onto 70").await;
        d.release(&mut chunk, PausePoint::BeforeJoin1Lock).await;
        let frozen = chunk
            .reached_or_finished(PausePoint::BeforeRebuildCommit)
            .await;
        let page = d.drain_frozen(insert, "the insert on 70", &[]).await;
        if frozen.is_some() {
            queued_or_finished(&d, reached.backend_pid, &page).await;
        }
        d.release(&mut chunk, PausePoint::BeforeRebuildCommit).await;
        chunk.finish_result().await.expect("the chunk's build step");
        page.finish().await;
        if frozen.is_none() {
            assert!(
                trellis::dev::interleave::join_key_moves() > moves,
                "{}: the chunk rolled back on the moved entry",
                star.name()
            );
        }
        let back = d.seal().await;
        d.drain(back, "the move back").await;
        assert_oracle(&mut d, &[star]).await;
    }
}

// ------------------------------------------------ PX: RESUME and the rebuild

/// PX1 (H1): a `RESUME` recreates `R` and every `M` empty and keeps the
/// target, so the target rows of `a` rows deleted while paused are backed by
/// nothing. The sweep's orphan pass re-derives them as gone: rows deleted
/// from the middle, the tail, and all of `a`. Planted `join1_no_orphan_pass`
/// leaves them in the target.
#[tokio::test]
async fn px1_a_resume_removes_the_rows_of_a_rows_deleted_while_paused() {
    let _serial = star::serial().await;
    knob();
    for (case, sql) in [
        (
            "mixed",
            "delete from public.a where id in (2, 5); update public.a set x = 1000 where id = 3",
        ),
        ("tail", "delete from public.a where id > 4"),
        ("emptied", "delete from public.a"),
    ] {
        let stars = N3.stars();
        let mut d = star::start(&stars, A_ROWS, B_ROWS).await;
        change_while_paused(&mut d, &stars, sql).await;
        assert!(
            pending(&d, &["sweep"]).await >= 4,
            "{case}: each start over a non-empty target enqueues the sweep"
        );
        assert_oracle(&mut d, &stars).await;
    }
}

/// PX2 (H3): the orphan pass reads row 3's target row as an orphan (row 3
/// was deleted while paused), then row 3 is inserted again and its page
/// applies before the pass re-derives it. The re-derive locks the entry the
/// page wrote and re-reads `a`, so the row stays. Planted
/// `join1_orphan_delete` deletes the orphans' rows by statement, and the
/// re-inserted row is lost.
#[tokio::test]
async fn px2_a_row_reinserted_after_the_orphan_read_is_kept() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        change_while_paused(&mut d, &[star], "delete from public.a where id = 3").await;
        run_until_the_sweep(&d).await;
        let target = star.target();
        let mut sweep = d
            .run_frozen(
                &[(PausePoint::AfterJoin1OrphanRead, &target)],
                |pool| async move { Ok(star::build_step(&pool, "sweep", 3).await) },
            )
            .await;
        sweep.reached(PausePoint::AfterJoin1OrphanRead).await;
        assert_eq!(
            d.rows(&format!("select id from {target} where id = 3"))
                .await,
            ["(3)"],
            "the orphan is still in the target"
        );
        star::step(
            &mut d,
            "insert into public.a (id, k1, k2, x, y) values (3, 1001, 30, 3, 'a3')",
        )
        .await;
        d.release(&mut sweep, PausePoint::AfterJoin1OrphanRead)
            .await;
        sweep.finish().await;
        assert_oracle(&mut d, &[star]).await;
    }
}

/// PX3: the `M` pass over `b_p` ∪ `M_p` at position 2, on a rebuild in place
/// of a live star whose `b2` row 2 (key 20) was deleted, row 1 changed and
/// row 8 inserted (on key 0), and whose `a` row 1 moved onto 30 there, all
/// past capture. The plan job's `M` pass walks position 1, then position 2,
/// and tombstones row 2's entry, whose row is gone. So every `M` equals its
/// table before any chunk runs, and once the chunks are done the target is
/// the live join with the sweep still to run. Planted `join1_m_pass_b_only`
/// skips the walk of `M`, and row 2's entry stays live.
#[tokio::test]
async fn px3_the_m_pass_tombstones_the_m_entries_at_position_2_of_rows_gone_before_a_rebuild() {
    let _serial = star::serial().await;
    knob();
    for star in N3.stars() {
        let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
        star::write_uncaptured(
            &d,
            "public.b2",
            "delete from public.b2 where id = 2; update public.b2 set w = 150 where id = 1; \
             insert into public.b2 values (8, 0, 800, 'k8')",
        )
        .await;
        star::write_uncaptured(&d, "public.a", "update public.a set k2 = 30 where id = 1").await;
        trellis(&d)
            .await
            .request_backfill("public.b2")
            .await
            .expect("request a rebuild");
        assert_eq!(
            d.rows("select kind from backfill_chunks where not done order by kind")
                .await,
            ["(plan)", "(sweep)"],
            "a star's rebuild gets a plan job and a sweep"
        );
        assert_eq!(
            star::build_step(d.pool(), "planner", 3).await,
            Step::Planned
        );
        for p in N3.all() {
            assert_m(&d, star, p, "the M pass").await;
        }
        run_until_the_sweep(&d).await;
        assert_eq!(
            pending(&d, &["sweep"]).await,
            1,
            "the sweep is still to run"
        );
        assert_target(&d, star, "the chunks").await;
        assert_oracle(&mut d, &[star]).await;
    }
}

// ------------------------------------------------------- n = 4, end to end

/// `a` rows on keys at all three positions, some on `NULL` and some on a key
/// with no row, and each joined table's rows.
const N4_SEED: &str = "insert into public.a (id, k1, k2, k3, x, y) \
                           select i, case when i % 4 = 0 then null else i % 5 end, \
                                  (i * 2) % 7, case when i % 5 = 0 then null else (i * 3) % 6 end, \
                                  i, 'a' || i \
                           from generate_series(1, 24) i; \
                       insert into public.b1 select i, i, i * 10, 'b1_' || i \
                           from generate_series(1, 4) i; \
                       insert into public.b2 select i, i, i * 100, 'b2_' || i \
                           from generate_series(1, 5) i; \
                       insert into public.b3 select i, i, i * 1000, 'b3_' || i \
                           from generate_series(0, 5) i";

/// Changes of every table: `a` rows moving keys at each position, deleted
/// and inserted, and each joined table's rows changed, moved and deleted.
const N4_CHANGES: &str = "update public.a set k1 = 2, k2 = 3, k3 = 4 where id = 1; \
                          update public.a set k2 = null where id = 2; \
                          update public.a set k3 = 1 where id in (3, 4); \
                          delete from public.a where id in (5, 6); \
                          insert into public.a (id, k1, k2, k3, x, y) values (30, 1, 2, 3, 30, 'new'); \
                          update public.b1 set w = 11 where id = 1; \
                          update public.b2 set k = 6 where id = 2; \
                          delete from public.b3 where id = 3; \
                          insert into public.b3 values (6, 3, 3333, 'b3_again')";

/// One end-to-end n = 4 star with rows at every position, under every kind
/// tuple: a build in chunks of 3 (each position's `M` pass in several
/// batches), a `RESUME` after changes of every table while paused (which
/// recreates `R` and the `M` of every position), a rebuild in place after
/// changes of every table past capture, and changes applied after it, each
/// checked against the oracle.
#[tokio::test]
async fn an_n4_star_builds_resumes_and_rebuilds_with_rows_at_every_position() {
    let _serial = star::serial().await;
    knob();
    let shape = Shape::new(3, 1);
    let stars = shape.stars();
    let ddl = format!("{}; {N4_SEED}", shape.ddl());
    let definitions: Vec<String> = stars.iter().map(|s| s.definition()).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let columns = shape.columns();
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let tables = shape.tables();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    let mut d = Driver::start_unbuilt(&[], &ddl, &columns, &definitions, &tables).await;
    assert_oracle(&mut d, &stars).await;

    change_while_paused(&mut d, &stars, N4_CHANGES).await;
    assert_oracle(&mut d, &stars).await;

    for (table, sql) in [
        (
            "public.a",
            "update public.a set k1 = 3, k3 = 2 where id = 7; delete from public.a where id = 8",
        ),
        ("public.b1", "update public.b1 set k = 9 where id = 4"),
        ("public.b2", "delete from public.b2 where id = 1"),
        ("public.b3", "update public.b3 set w = 7 where id = 2"),
    ] {
        star::write_uncaptured(&d, table, sql).await;
    }
    d.retire().await;
    trellis(&d)
        .await
        .request_backfill("public.a")
        .await
        .expect("request a rebuild");
    assert_oracle(&mut d, &stars).await;

    for sql in [
        "update public.a set k2 = 4 where id = 9",
        "insert into public.b2 values (9, 1, 900, 'b2_nine')",
        "update public.b3 set k = null where id = 4",
        "delete from public.b1 where id = 2",
    ] {
        star::step(&mut d, sql).await;
        assert_oracle(&mut d, &stars).await;
    }
}

// --------------------------------------------- a producer at position 2

/// An aggregate's target at position 2, keyed by its nullable `GROUP BY`
/// column (#1401): `agg` counts `src` rows per `g`, two of them in the group
/// keyed `NULL`, and position 1 is a plain table `c`.
const PRODUCER_DDL: &str = "create table public.src (id integer primary key, g integer, v integer); \
                            create table public.c (id integer primary key, k integer, w integer); \
                            create unique index on public.c (k); \
                            create table public.a (id integer primary key, kc integer, k integer, \
                                x integer); \
                            create index on public.a (kc); create index on public.a (k); \
                            insert into public.src values \
                                (1, 1, 10), (2, null, 20), (3, 2, 5), (4, null, 7); \
                            insert into public.c values (1, 1, 1000), (2, 2, 2000); \
                            insert into public.a values (1, 1, 1, 1), (2, 2, 2, 2), \
                                (3, 1, null, 3), (4, null, 9, 4)";

/// The target equals the live multi-join, and position 2's `M` holds an
/// entry for every `agg` row, the group keyed `NULL` among them.
async fn assert_producer(d: &Driver, kind: &str, step: &str) {
    let actual = d
        .rows("select id::text, total::text from public.jn order by 1")
        .await;
    let expected = d
        .rows(&format!(
            "select a.id::text, (a.x + coalesce(c.w, 0) + coalesce(agg.n, 0))::text \
             from public.a a left join public.c c on a.kc = c.k \
             {kind} join public.agg agg on a.k = agg.g order by 1"
        ))
        .await;
    assert_eq!(actual, expected, "{kind} target after {step}");
    let id: i64 = d
        .ctl
        .query_one(
            "select id from transform_definitions where target_table = 'public.jn'",
            &[],
        )
        .await
        .expect("the definition")
        .get(0);
    let held: i64 = d
        .ctl
        .query_one(
            &format!("select count(*) from {DEFAULT_SCHEMA}.join1_{id}_m2 where not __tombstone"),
            &[],
        )
        .await
        .expect("count M at position 2")
        .get(0);
    let live: i64 = d
        .ctl
        .query_one("select count(*) from public.agg", &[])
        .await
        .expect("count agg")
        .get(0);
    assert_eq!(held, live, "{kind} M at position 2 after {step}");
    assert_eq!(live, 3, "the aggregate has a group keyed NULL");
}

/// A producer at position 2: the `M` pass of that position reads the group
/// keyed `NULL` with its own batch (`m_null_batch` at the position), which
/// no batch of the table's key order does, so position 2's `M` holds an
/// entry for every `agg` row. No `a` row joins it, and pages that change the
/// group leave the target as it was.
#[tokio::test]
async fn a_row_keyed_by_null_at_position_2_is_read_by_its_m_pass() {
    let _serial = star::serial().await;
    knob();
    let int4 = ValueType::Integer(IntWidth::Int4);
    for (kind, word) in [("left", "LEFT"), ("inner", "INNER")] {
        let mut d = Driver::start(
            PRODUCER_DDL,
            &[("g", int4), ("v", int4)],
            &["TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT count(*) AS n"],
            &["public.src"],
        )
        .await;
        trellis::defs::install_definition(
            d.pool(),
            &format!(
                "TRANSFORM jn FROM public.a LEFT JOIN public.c ON a.kc = c.k \
                 {word} JOIN public.agg ON a.k = agg.g \
                 SELECT a.x + COALESCE(c.w, 0) + COALESCE(agg.n, 0) AS total"
            ),
            &HashMap::from([("total".to_string(), ValueType::Integer(IntWidth::Int8))]),
            "public",
        )
        .await
        .expect("define the star");
        for table in ["public.a", "public.c"] {
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
                trellis::capture::install::Progress::Waiting(wait) => {
                    panic!("capture waited: {wait}")
                }
            }
        }
        trellis::intake::harness::settle_builds(d.pool()).await;
        d.settle().await;
        assert_producer(&d, kind, "the build").await;
        star::step(
            &mut d,
            "insert into public.src values (5, null, 1), (6, 1, 1)",
        )
        .await;
        d.settle().await;
        assert_producer(&d, kind, "the NULL group grows").await;
    }
}
