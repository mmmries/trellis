//! Which row-lock mode a `JOIN` page takes on its `J` rows (#1230, ADR-0019's
//! JR1), observed from a second session while a page is frozen right after its
//! `J` lock statement: a probe with `NOWAIT` for each mode says what the page
//! holds.
//!
//! Every test turns the group-basis knob and the share knob on before its
//! driver starts. They share one process, so none turns either off.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::interleave::PausePoint;

const DDL: &str = "create table public.a (id integer primary key, ga integer, k integer, x integer); \
                   create table public.b (id integer primary key, k integer, gb integer, y integer); \
                   create index on public.a (k); create index on public.b (k)";

const DEF: &str = "TRANSFORM l FROM public.a LEFT JOIN public.b ON a.k = b.k \
                   GROUP BY a.ga, b.gb SELECT a.ga AS ga, b.gb AS gb, COUNT(*) AS n, SUM(a.x) AS sx, SUM(b.y) AS sy";

struct Lab {
    d: Driver,
    j: String,
    target: String,
}

async fn lab() -> Lab {
    trellis::dev::staging::set_group_basis(true);
    trellis::dev::staging::set_a_side_share(true);
    let int4 = || ValueType::Integer(IntWidth::Int4);
    let d = Driver::start_with_database_defaults(
        &[],
        DDL,
        &[("id", int4()), ("ga", int4()), ("k", int4()), ("x", int4())],
        &[],
        &[DEF],
        &["public.a", "public.b"],
    )
    .await;
    let id = d
        .rows("select id from transform_definitions where target_table = 'public.l'")
        .await[0]
        .trim_matches(|c| c == '(' || c == ')')
        .to_string();
    Lab {
        d,
        j: format!("{DEFAULT_SCHEMA}.join_{id}_j"),
        target: "public.l".to_string(),
    }
}

impl Lab {
    async fn write(&self, sql: &str) {
        self.d.user().await.batch_execute(sql).await.expect(sql);
    }

    async fn step(&mut self, sql: &str) {
        self.write(sql).await;
        let batch = self.d.seal().await;
        self.d.drain(batch, "step").await;
    }

    /// Whether a second session can take key 1's `J` row in `mode` without
    /// waiting.
    async fn can_lock(&self, mode: &str) -> bool {
        let c = self.d.user().await;
        c.batch_execute("begin").await.unwrap();
        let r = c
            .batch_execute(&format!(
                "select 1 from {} where k = 1 for {mode} nowait",
                self.j
            ))
            .await;
        c.batch_execute("rollback").await.unwrap();
        match r {
            Ok(()) => true,
            Err(e) => {
                assert_eq!(
                    e.code().map(|c| c.code()),
                    Some("55P03"),
                    "unexpected error {e:?}"
                );
                false
            }
        }
    }

    /// Drains `sql` as one page frozen after its `J` lock, runs `probe`, and
    /// lets the page go.
    async fn frozen_page(&mut self, sql: &str) -> drain_driver::RunningDrain {
        self.write(sql).await;
        let batch = self.d.seal().await;
        let mut frozen = self
            .d
            .drain_frozen(
                batch,
                "x",
                &[(PausePoint::AfterJoinLock, self.target.as_str())],
            )
            .await;
        frozen.reached(PausePoint::AfterJoinLock).await;
        frozen
    }
}

/// A page of fact changes only locks the key's `J` row for share (a second
/// session can share it, not update it); a page with a dimension change locks
/// it for update; a page of both does too, whole (it never upgrades).
#[tokio::test]
async fn a_from_side_only_page_locks_j_for_share_and_every_other_page_for_update() {
    let mut lab = lab().await;
    lab.step("insert into public.a values (1, 1, 1, 5); insert into public.b values (1, 1, 7, 9)")
        .await;
    for (name, sql, shares) in [
        (
            "fact insert",
            "insert into public.a values (2, 2, 1, 6)",
            true,
        ),
        (
            "fact move to another key",
            "update public.a set k = 2 where id = 1",
            true,
        ),
        ("fact delete", "delete from public.a where id = 2", true),
        (
            "dimension update",
            "update public.b set gb = 8 where id = 1",
            false,
        ),
        (
            "dimension y change",
            "update public.b set y = 10 where id = 1",
            false,
        ),
        (
            "fact and dimension in one page",
            "insert into public.a values (3, 3, 1, 1); update public.b set gb = 9 where id = 1",
            false,
        ),
    ] {
        let mut frozen = lab.frozen_page(sql).await;
        assert!(
            lab.can_lock("share").await == shares,
            "{name}: a second FOR SHARE locker {}",
            if shares { "must not wait" } else { "must wait" }
        );
        assert!(
            !lab.can_lock("update").await,
            "{name}: FOR UPDATE must wait"
        );
        assert!(
            !lab.can_lock("no key update").await,
            "{name}: NO KEY UPDATE must wait"
        );
        lab.d.release(&mut frozen, PausePoint::AfterJoinLock).await;
        frozen.finish().await;
    }
}

impl Lab {
    /// Whether `other` ran to its end (`true`) or queued behind backend `pid`
    /// (`false`).
    async fn finished_or_queued(&self, other: &drain_driver::RunningDrain, pid: i32) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if other.is_finished() {
                return true;
            }
            let blocked: bool = self
                .d
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
                return false;
            }
            if std::time::Instant::now() >= deadline {
                let rows = self.d.rows("select pid, wait_event_type, wait_event, pg_blocking_pids(pid)::text, left(query, 100) from pg_stat_activity where state <> 'idle' and pid <> pg_backend_pid()").await;
                panic!("neither queued nor finished; frozen pid {pid}: {rows:#?}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// The target equals its `JOIN … GROUP BY`, and no deadlock was logged.
    async fn check(&mut self, context: &str) {
        self.check_target(context).await;
        assert_eq!(self.d.deadlocks_logged(), Vec::<String>::new(), "{context}");
    }

    /// The target equals its `JOIN … GROUP BY`.
    async fn check_target(&mut self, context: &str) {
        self.d.settle().await;
        assert_eq!(
            self.d
                .rows("select ga, gb, n, sx, sy from public.l order by ga, gb")
                .await,
            self.d
                .rows(
                    "select a.ga, b.gb, count(*), sum(a.x), sum(b.y) from public.a a \
                     left join public.b b on a.k = b.k group by a.ga, b.gb order by a.ga, b.gb"
                )
                .await,
            "{context}"
        );
    }
}

/// A fact page of a key holds its `J` row for share, and at every point after
/// that lock another page of the key either runs to its end (a second fact
/// page) or queues behind it (a dimension page). All exact against the `JOIN`,
/// with the key matched or not (`LEFT`'s unmatched term moves with the first
/// dimension row).
async fn fact_pages_overlap(matched: bool) {
    let mut lab = lab().await;
    if matched {
        lab.step("insert into public.b values (1, 1, 7, 9)").await;
    }
    lab.step("insert into public.a values (1, 1, 1, 5)").await;
    let mut n = 10;
    for dimension in [false, true] {
        if dimension && !matched {
            continue;
        }
        for point in [
            PausePoint::AfterJoinLock,
            PausePoint::AfterEntryLock,
            PausePoint::AfterJoinTargetLock,
            PausePoint::AfterGroupUpsert,
            PausePoint::BeforeCommit,
        ] {
            n += 1;
            let context = format!("matched {matched}, dimension {dimension}, frozen at {point:?}");
            // Both pages are sealed before the first freezes: a seal waits
            // for a page frozen late in its transaction.
            lab.write(&format!("insert into public.a values ({n}, 2, 1, 6)"))
                .await;
            let one = lab.d.seal().await;
            lab.write(&if dimension {
                "update public.b set gb = gb + 1 where k = 1".to_string()
            } else {
                format!("insert into public.a values ({}, {}, 1, 7)", n + 100, 3 + n)
            })
            .await;
            let two = lab.d.seal().await;
            let mut frozen = lab
                .d
                .drain_frozen(one, "x", &[(point, lab.target.as_str())])
                .await;
            let reached = frozen.reached(point).await;
            let second = lab.d.drain_frozen(two, "y", &[]).await;
            let ran = lab.finished_or_queued(&second, reached.backend_pid).await;
            if dimension {
                assert!(
                    !ran,
                    "{context}: the dimension page did not queue behind the fact page"
                );
            } else {
                assert!(
                    ran,
                    "{context}: the second fact page queued behind the first"
                );
            }
            lab.d.release(&mut frozen, point).await;
            frozen.finish().await;
            second.finish().await;
            lab.check(&context).await;
        }
    }
}

#[tokio::test]
async fn fact_pages_of_one_unmatched_key_overlap() {
    fact_pages_overlap(false).await;
}

#[tokio::test]
async fn fact_pages_of_one_matched_key_overlap_and_a_dimension_page_queues_behind_them() {
    fact_pages_overlap(true).await;
}

impl Lab {
    /// Cancels the page frozen at `point` (backend `pid`) and holds the
    /// definition's lock row `FOR UPDATE` until the page's retry queues
    /// behind it, so the caller sees what the rolled-back attempt left.
    async fn cancel_and_hold(
        &self,
        frozen: &mut drain_driver::RunningDrain,
        point: PausePoint,
        pid: i32,
    ) -> tokio_postgres::Client {
        let lock = self.j.replace("_j", "_l");
        let holder = self.d.user().await;
        let holder_pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await
            .expect("pid")
            .get(0);
        let holding = tokio::spawn(async move {
            holder
                .batch_execute(&format!("begin; select 1 from {lock} for update"))
                .await
                .expect("hold the lock row");
            holder
        });
        self.d.wait_blocked_behind(pid).await;
        self.d
            .ctl
            .execute("select pg_cancel_backend($1)", &[&pid])
            .await
            .expect("cancel the page");
        let holder = holding.await.expect("holder");
        self.d.release(frozen, point).await;
        self.d.wait_blocked_behind(holder_pid).await;
        holder
    }
}

/// A fact page cancelled while it holds a `J` row for share (the retry path:
/// a cancelled statement, as a `40P01` victim or a killed worker would
/// leave): the rolled-back attempt leaves no lock and no change, and the
/// retry applies the page exactly once. Frozen at each point from the `J`
/// lock on.
#[tokio::test]
async fn a_cancelled_fact_page_releases_its_share_lock_and_its_retry_applies_once() {
    let mut lab = lab().await;
    lab.step("insert into public.b values (1, 1, 7, 9); insert into public.a values (1, 1, 1, 5)")
        .await;
    for (n, point) in [
        PausePoint::AfterJoinLock,
        PausePoint::AfterEntryLock,
        PausePoint::AfterJoinTargetLock,
        PausePoint::AfterGroupUpsert,
        PausePoint::BeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let context = format!("cancelled at {point:?}");
        let before = lab.d.rows("select * from public.l order by ga, gb").await;
        lab.write(&format!(
            "insert into public.a values ({}, {}, 1, 6)",
            10 + n,
            2 + n
        ))
        .await;
        let batch = lab.d.seal().await;
        let mut frozen = lab
            .d
            .drain_frozen(batch, "x", &[(point, lab.target.as_str())])
            .await;
        let reached = frozen.reached(point).await;
        assert!(lab.can_lock("share").await, "{context}: holds share");
        assert!(!lab.can_lock("update").await, "{context}: holds a lock");
        let holder = lab
            .cancel_and_hold(&mut frozen, point, reached.backend_pid)
            .await;
        // The retry is queued on the lock row and holds nothing yet; the
        // cancelled attempt's rows are gone.
        assert!(
            lab.can_lock("update").await,
            "{context}: the attempt left a J lock"
        );
        assert_eq!(
            lab.d.rows("select * from public.l order by ga, gb").await,
            before,
            "{context}: the cancelled attempt's change is visible"
        );
        holder.batch_execute("commit").await.expect("commit");
        frozen.finish().await;
        lab.check(&context).await;
    }
}

/// A forced `40P01`: an outside transaction holds the key's `T` rows and asks
/// for its `J` row `FOR UPDATE`, behind a fact page's share lock; the page,
/// released, asks for the `T` rows. Postgres aborts one of the two: the one
/// that waited first when `page_waits_first` (the page), the other otherwise.
/// Whichever it is, the final state is exact: the page's retry (when it was
/// the victim) applies the page once; the outside transaction never touched
/// the target.
async fn forced_deadlock(page_waits_first: bool) {
    let mut lab = lab().await;
    lab.step("insert into public.b values (1, 1, 7, 9); insert into public.a values (1, 1, 1, 5)")
        .await;
    lab.write("insert into public.a values (2, 1, 1, 6)").await;
    let batch = lab.d.seal().await;
    let point = PausePoint::AfterJoinLock;
    let mut frozen = lab
        .d
        .drain_frozen(batch, "x", &[(point, lab.target.as_str())])
        .await;
    let reached = frozen.reached(point).await;
    let t = lab.j.replace("_j", "_t");
    let j = lab.j.clone();
    let outside = lab.d.user().await;
    let outside_pid: i32 = outside
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    outside
        .batch_execute(&format!("begin; select 1 from {t} for update"))
        .await
        .expect("hold the T rows");
    let ask = move |outside: tokio_postgres::Client| async move {
        let r = outside
            .batch_execute(&format!("select 1 from {j} where k = 1 for update"))
            .await;
        let _ = outside.batch_execute("rollback").await;
        r
    };
    let asking;
    if page_waits_first {
        // The page goes first to the `T` rows and queues behind the outside
        // transaction; only then does the outside ask for the `J` row.
        lab.d.release(&mut frozen, point).await;
        lab.d.wait_blocked_behind(outside_pid).await;
        asking = tokio::spawn(ask(outside));
    } else {
        asking = tokio::spawn(ask(outside));
        lab.d.wait_blocked_behind(reached.backend_pid).await;
        lab.d.release(&mut frozen, point).await;
    }
    let outside_result = asking.await.expect("outside task");
    let page_result = frozen.finish_result().await;
    if page_waits_first {
        // The page was the victim: the drain retried it in place (its retry
        // took the rows once the outside transaction rolled back).
        outside_result
            .as_ref()
            .expect("the outside transaction was not the victim");
        page_result
            .as_ref()
            .expect("the drain retries a 40P01 victim itself");
    } else {
        let e = outside_result
            .as_ref()
            .expect_err("the outside transaction was the victim");
        assert_eq!(e.code().map(|c| c.code()), Some("40P01"), "{e:?}");
        page_result.as_ref().expect("the page was not the victim");
    }
    lab.d.settle().await;
    lab.check_target("after the forced deadlock").await;
    assert_eq!(
        lab.d.deadlocks_logged().len(),
        1,
        "one deadlock, detected and resolved"
    );
    assert_eq!(
        lab.d.rows("select n from public.l order by ga, gb").await,
        ["(2)"],
        "the target holds both facts exactly once"
    );
}

#[tokio::test]
async fn a_deadlock_that_aborts_the_outside_transaction_leaves_the_target_exact() {
    forced_deadlock(false).await;
}

#[tokio::test]
async fn a_deadlock_that_aborts_the_fact_page_is_retried_exactly_once() {
    forced_deadlock(true).await;
}
