//! Experiment 2: do the two operations survive the known interleavings?
//!
//! Scratch tables, plpgsql implementations of Re-derive / Apply / reverse (see exp2.sql),
//! two or more connections, advisory locks to freeze a step. Every scenario asserts the
//! group table against a from-scratch GROUP BY of the sources.
//!
//! Modes for Apply's skip rule:
//!   literal      skip iff C visible in the entry's basis (the note's Apply, verbatim)
//!   literal-snap as literal, but Apply also stamps basis := its own snapshot
//!   lsn          literal + skip iff C's commit position <= the last applied one
//! Enumerations for the reverse path: ledger | ledger+source | ledger+deplock.

use crate::pg::connect;
use anyhow::{Result, anyhow};
use std::time::Duration;
use tokio_postgres::Client;

const SQL: &str = include_str!("exp2.sql");

#[derive(Clone)]
struct Env {
    mode: String,
    enumeration: String,
}

/// A staged change as intake would hand it to the drain: the source commit's xid8, its
/// WAL position, and the carried image.
#[derive(Clone, Debug)]
struct Change {
    key: i32,
    xid8: String,
    lsn: String,
    kind: &'static str,
    parent: i32,
    amt: i64,
}

struct Db {
    user: Client, // the application writing the sources
    a: Client,    // drain worker A
    b: Client,    // drain worker B
    ctl: Client,  // driver: holds advisory locks, runs the oracle
}

async fn fresh(env: &Env) -> Result<Db> {
    let ctl = connect("exp2").await?;
    ctl.batch_execute(SQL).await?;
    let _ = env;
    Ok(Db {
        user: connect("exp2").await?,
        a: connect("exp2").await?,
        b: connect("exp2").await?,
        ctl,
    })
}

/// Run `stmts` as one source transaction and return the staged change for `key`
/// (xid8 from inside the transaction; the WAL position after its writes, which orders
/// same-row commits exactly because the second writer waits for the first's row lock).
async fn source_tx(
    user: &Client,
    stmts: &str,
    key: i32,
    kind: &'static str,
    parent: i32,
    amt: i64,
) -> Result<Change> {
    user.batch_execute("begin").await?;
    user.batch_execute(stmts).await?;
    let r = user
        .query_one(
            "select pg_current_xact_id()::text, pg_current_wal_insert_lsn()::text",
            &[],
        )
        .await?;
    let c = Change {
        key,
        xid8: r.get(0),
        lsn: r.get(1),
        kind,
        parent,
        amt,
    };
    user.batch_execute("commit").await?;
    Ok(c)
}

async fn apply(
    c: &Client,
    env: &Env,
    ch: &Change,
    hold: Option<i64>,
    dep_lock: bool,
) -> Result<String> {
    Ok(c.query_one(
        "select apply($1, $2::text::xid8, $3::text::pg_lsn, $4, $5, $6, $7, $8, $9)",
        &[
            &ch.key, &ch.xid8, &ch.lsn, &ch.kind, &ch.parent, &ch.amt, &env.mode, &hold, &dep_lock,
        ],
    )
    .await?
    .get(0))
}

/// The fold: last image per key plus the commits it telescopes. literal modes: all visible ->
/// skip, none -> apply the last image, mixed -> Re-derive. lsn mode: Apply with the last commit.
async fn apply_folded(c: &Client, env: &Env, changes: &[Change]) -> Result<String> {
    let last = changes.last().unwrap();
    if env.mode == "lsn" {
        return apply(c, env, last, None, false).await;
    }
    let mut vis = Vec::new();
    for ch in changes {
        let v: bool = c
            .query_one("select coalesce(pg_visible_in_snapshot($1::text::xid8, basis), false) from ledger where from_key = $2", &[&ch.xid8, &ch.key])
            .await?
            .get(0);
        vis.push(v);
    }
    if vis.iter().all(|v| *v) {
        return Ok("skip:all-visible".into());
    }
    if vis.iter().any(|v| *v) {
        return Ok(format!("mixed->{}", rederive(c, last.key, None).await?));
    }
    apply(c, env, last, None, false).await
}

async fn rederive(c: &Client, key: i32, hold: Option<i64>) -> Result<String> {
    Ok(c.query_one("select rederive($1, $2)", &[&key, &hold])
        .await?
        .get(0))
}

async fn reverse(c: &Client, env: &Env, parent: i32, xid8: Option<&str>) -> Result<String> {
    Ok(c.query_one(
        "select reverse($1, $2, $3::text::xid8)",
        &[&parent, &env.enumeration, &xid8],
    )
    .await?
    .get(0))
}

/// Compare the group table with a from-scratch GROUP BY. Ok(()) or the two listings.
async fn oracle(c: &Client) -> Result<()> {
    let rows = c
        .query(
            "with o as (select p.name g, sum(amt)::bigint t, count(*) n from src s join parent p on p.id = s.parent_id group by p.name),
                  t as (select group_key g, total t, member_count n from groups where member_count <> 0)
             select 'oracle' side, g, t, n from o except select 'oracle', g, t, n from t
             union all
             (select 'target', g, t, n from t except select 'target', g, t, n from o)
             order by 1, 2",
            &[],
        )
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let s: Vec<String> = rows
        .iter()
        .map(|r| {
            format!(
                "{} {}={}({})",
                r.get::<_, String>(0),
                r.get::<_, String>(1),
                r.get::<_, i64>(2),
                r.get::<_, i64>(3)
            )
        })
        .collect();
    Err(anyhow!("target != oracle: {}", s.join(", ")))
}

async fn wait_for(c: &Client, sql: &str, what: &str) -> Result<()> {
    for _ in 0..200 {
        let ok: bool = c.query_one(sql, &[]).await?.get(0);
        if ok {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(anyhow!("timed out waiting for {what}"))
}

/// Wait until some backend blocks on the advisory lock `h` (the held step has reached its hold point).
async fn wait_held(ctl: &Client, h: i64) -> Result<()> {
    let _ = h;
    wait_for(ctl, "select exists (select 1 from pg_stat_activity where wait_event_type = 'Lock' and wait_event = 'advisory')", "hold point").await
}

/// Wait until some backend blocks on a row/transaction lock (a worker is waiting on the ledger entry, I1).
async fn wait_blocked_on_row(ctl: &Client) -> Result<()> {
    wait_for(ctl, "select exists (select 1 from pg_stat_activity where wait_event_type = 'Lock' and wait_event in ('transactionid', 'tuple'))", "row-lock wait").await
}

async fn seed(db: &Db, parents: &[(i32, &str)], rows: &[(i32, i32, i64)]) -> Result<()> {
    for (id, name) in parents {
        db.user
            .execute("insert into parent values ($1, $2)", &[id, name])
            .await?;
    }
    for (id, p, amt) in rows {
        db.user
            .execute("insert into src values ($1, $2, $3)", &[id, p, amt])
            .await?;
    }
    // the build: Re-derive over the key space
    for (id, _, _) in rows {
        rederive(&db.ctl, *id, None).await?;
    }
    oracle(&db.ctl).await
}

type Outcome = Result<String>;

/// tokio-postgres displays a server error as just "db error"; pull out the message.
fn errmsg(e: &tokio_postgres::Error) -> String {
    match e.as_db_error() {
        Some(d) => format!("{}: {}", d.code().code(), d.message()),
        None => e.to_string(),
    }
}

fn describe(e: &anyhow::Error) -> String {
    match e.downcast_ref::<tokio_postgres::Error>() {
        Some(pe) => errmsg(pe),
        None => e.to_string(),
    }
}

// ---------------------------------------------------------------- scenarios

/// #344: an enumeration's read stalls, a newer CDC batch for the same key drains, the
/// enumeration writes last. Under I1 the CDC apply cannot even start until the enumeration commits.
async fn s2_stalled_enumeration(env: &Env, c_after_snapshot: bool) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "a")], &[(1, 1, 10), (2, 1, 5)]).await?;
    const H: i64 = 344;
    let ch;
    if c_after_snapshot {
        db.ctl.execute("select pg_advisory_lock($1)", &[&H]).await?;
        let a = tokio::spawn(async move { rederive(&db.a, 1, Some(H)).await.map(|r| (r, db.a)) });
        wait_held(&db.ctl, H).await?;
        ch = source_tx(
            &db.user,
            "update src set amt = 20 where id = 1",
            1,
            "update",
            1,
            20,
        )
        .await?;
        let env2 = env.clone();
        let b = tokio::spawn(async move {
            apply(&db.b, &env2, &ch, None, false)
                .await
                .map(|r| (r, db.b))
        });
        wait_blocked_on_row(&db.ctl).await?;
        if b.is_finished() {
            return Err(anyhow!("apply did not block on the held re-derive"));
        }
        db.ctl
            .execute("select pg_advisory_unlock($1)", &[&H])
            .await?;
        let (ra, _) = a.await??;
        let (rb, _) = b.await??;
        oracle(&db.ctl).await?;
        Ok(format!(
            "rederive={ra} then apply={rb} (blocked on I1 lock)"
        ))
    } else {
        ch = source_tx(
            &db.user,
            "update src set amt = 20 where id = 1",
            1,
            "update",
            1,
            20,
        )
        .await?;
        db.ctl.execute("select pg_advisory_lock($1)", &[&H]).await?;
        let a = tokio::spawn(async move { rederive(&db.a, 1, Some(H)).await.map(|r| (r, db.a)) });
        wait_held(&db.ctl, H).await?;
        let env2 = env.clone();
        let b = tokio::spawn(async move {
            apply(&db.b, &env2, &ch, None, false)
                .await
                .map(|r| (r, db.b))
        });
        wait_blocked_on_row(&db.ctl).await?;
        if b.is_finished() {
            return Err(anyhow!("apply did not block on the held re-derive"));
        }
        db.ctl
            .execute("select pg_advisory_unlock($1)", &[&H])
            .await?;
        let (ra, _) = a.await??;
        let (rb, _) = b.await??;
        oracle(&db.ctl).await?;
        if rb != "skip:visible" {
            return Err(anyhow!(
                "expected the CDC apply to be skipped as visible, got {rb}"
            ));
        }
        Ok(format!("rederive={ra} then apply={rb}"))
    }
}

/// #321: a source commit lands between a group's re-derive and the drain of its own CDC.
async fn s3_commit_before_rederive(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(
        &db,
        &[(1, "a"), (2, "b")],
        &[(1, 1, 10), (2, 1, 5), (3, 2, 7)],
    )
    .await?;
    let ch = source_tx(
        &db.user,
        "update src set amt = 20, parent_id = 2 where id = 1",
        1,
        "update",
        2,
        20,
    )
    .await?;
    // a forced recompute of the group re-derives its members and sees the commit
    for k in [1, 2] {
        rederive(&db.a, k, None).await?;
    }
    oracle(&db.ctl).await?;
    let r = apply(&db.b, env, &ch, None, false).await?;
    oracle(&db.ctl).await?;
    if r != "skip:visible" {
        return Err(anyhow!("expected skip:visible, got {r}"));
    }
    Ok(format!("apply after re-derive: {r}"))
}

/// #389/#539: many workers create the same new groups at once. Batches lock ledger entries in
/// key order and increment groups in one sorted statement (I5). The unsorted per-row variant is
/// the control: it should deadlock.
async fn s4_concurrent_group_creation(env: &Env, sorted: bool) -> Outcome {
    let db = fresh(env).await?;
    let parents: Vec<(i32, String)> = (1..=20).map(|i| (i, format!("g{i}"))).collect();
    for (id, name) in &parents {
        db.user
            .execute("insert into parent values ($1, $2)", &[id, name])
            .await?;
    }
    // 8 workers; every batch inserts 40 rows into random new/existing groups. The sorted variant
    // runs 30 rounds each; the unsorted control runs until a 20s budget expires and just counts.
    let deadlocks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut tasks = Vec::new();
    for w in 0..8 {
        let env = env.clone();
        let deadlocks = deadlocks.clone();
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let c = connect("exp2").await?;
            let mut seedv: u64 = 12345 + w as u64;
            let mut round = 0;
            while if sorted {
                round < 30
            } else {
                !stop.load(std::sync::atomic::Ordering::Relaxed)
            } {
                let mut keys = Vec::new();
                let mut ps = Vec::new();
                let mut amts = Vec::new();
                for i in 0..40 {
                    seedv = seedv
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    keys.push((w * 100_000 + round * 100 + i) as i32);
                    ps.push(((seedv >> 33) % 20) as i32 + 1);
                    amts.push(((seedv >> 20) % 100) as i64);
                }
                round += 1;
                // stage the rows in the source first (so the oracle can see them)
                c.execute(
                    "insert into src select * from unnest($1::int[], $2::int[], $3::bigint[])",
                    &[&keys, &ps, &amts],
                )
                .await?;
                if sorted {
                    let mut idx: Vec<usize> = (0..keys.len()).collect();
                    idx.sort_by_key(|i| keys[*i]);
                    keys = idx.iter().map(|i| keys[*i]).collect();
                    ps = idx.iter().map(|i| ps[*i]).collect();
                    amts = idx.iter().map(|i| amts[*i]).collect();
                }
                loop {
                    let r = c
                        .execute(
                            "select apply_batch($1, $2, $3, $4, $5)",
                            &[&keys, &ps, &amts, &env.mode, &sorted],
                        )
                        .await;
                    match r {
                        Ok(_) => break,
                        Err(e) if errmsg(&e).contains("deadlock") => {
                            deadlocks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    if !sorted {
        // budget over: cut the workers off mid-retry; only the deadlock count matters here
        tokio::time::sleep(Duration::from_secs(20)).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        db.ctl.execute("select pg_terminate_backend(pid) from pg_stat_activity where datname = 'exp2' and query like 'select apply_batch%'", &[]).await?;
        for t in tasks {
            let _ = t.await?;
        }
        let deadlocks = deadlocks.load(std::sync::atomic::Ordering::Relaxed);
        if deadlocks == 0 {
            return Err(anyhow!(
                "control produced no deadlocks; the test cannot fail"
            ));
        }
        return Ok(format!(
            "{deadlocks} deadlocks in 20s with per-row interleaved locking (control: I5 is necessary)"
        ));
    }
    for t in tasks {
        t.await??;
    }
    let deadlocks = deadlocks.load(std::sync::atomic::Ordering::Relaxed);
    oracle(&db.ctl).await?;
    if sorted && deadlocks > 0 {
        return Err(anyhow!("{deadlocks} deadlocks with sorted batches"));
    }
    Ok(format!("{deadlocks} deadlocks (retried), oracle matches"))
}

/// #494: a key goes a -> z -> b inside one batch while z is being rebuilt.
async fn s5_a_z_b(env: &Env, split: bool) -> Outcome {
    let db = fresh(env).await?;
    seed(
        &db,
        &[(1, "a"), (2, "z"), (3, "b")],
        &[(1, 1, 10), (5, 2, 50), (6, 3, 60)],
    )
    .await?;
    let c1 = source_tx(
        &db.user,
        "update src set parent_id = 2 where id = 1",
        1,
        "update",
        2,
        10,
    )
    .await?;
    let c2 = source_tx(
        &db.user,
        "update src set parent_id = 3 where id = 1",
        1,
        "update",
        3,
        10,
    )
    .await?;
    const H: i64 = 494;
    if !split {
        // z's rebuild (re-derive of its members) is held after its read while the folded record drains
        db.ctl.execute("select pg_advisory_lock($1)", &[&H]).await?;
        let a = tokio::spawn(async move { rederive(&db.a, 5, Some(H)).await.map(|r| (r, db.a)) });
        wait_held(&db.ctl, H).await?;
        let r = apply_folded(&db.b, env, &[c1.clone(), c2.clone()]).await?;
        db.ctl
            .execute("select pg_advisory_unlock($1)", &[&H])
            .await?;
        a.await??;
        oracle(&db.ctl).await?;
        return Ok(format!("folded a->z->b: {r}; z untouched"));
    }
    // split across batches, drained out of order: z->b first, then a->z
    let r2 = apply(&db.b, env, &c2, None, false).await?;
    let r1 = apply(&db.a, env, &c1, None, false).await?;
    oracle(&db.ctl).await?;
    Ok(format!("out-of-order split: C2={r2} then C1={r1}"))
}

/// #516/#520/#528: a to-side rename while from-side CDC for the same rows is pending.
async fn s6_rename_with_pending_cdc(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one"), (3, "three")], &[(1, 1, 10), (2, 3, 20)]).await?;
    let c1 = source_tx(
        &db.user,
        "update src set amt = 11 where id = 1",
        1,
        "update",
        1,
        11,
    )
    .await?;
    let c2 = source_tx(
        &db.user,
        "update parent set name = 'uno' where id = 1",
        0,
        "to-side",
        1,
        0,
    )
    .await?;
    let rr = reverse(&db.a, env, 1, Some(&c2.xid8)).await?;
    oracle(&db.ctl).await?;
    let r1 = apply(&db.b, env, &c1, None, false).await?;
    oracle(&db.ctl).await?;
    Ok(format!("reverse: {rr}; pending from-side apply: {r1}"))
}

/// 6b: the race the ledger index alone cannot see. A from-side move r2 -> p1 is mid-Apply (it has
/// read p1's name live, and holds r2's ledger lock with its new join key uncommitted) when p1 is
/// renamed; the rename's reverse path enumerates p1's children.
async fn s6b_inflight_move_vs_rename(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one"), (3, "three")], &[(1, 1, 10), (2, 3, 20)]).await?;
    let c3 = source_tx(
        &db.user,
        "update src set parent_id = 1 where id = 2",
        2,
        "update",
        1,
        20,
    )
    .await?;
    const H: i64 = 528;
    db.ctl.execute("select pg_advisory_lock($1)", &[&H]).await?;
    let env2 = env.clone();
    let dep = env.enumeration == "ledger+deplock";
    let b = tokio::spawn(async move {
        apply(&db.b, &env2, &c3, Some(H), dep)
            .await
            .map(|r| (r, db.b))
    });
    wait_held(&db.ctl, H).await?;
    let c4 = source_tx(
        &db.user,
        "update parent set name = 'uno' where id = 1",
        0,
        "to-side",
        1,
        0,
    )
    .await?;
    let env3 = env.clone();
    let a = tokio::spawn(async move {
        reverse(&db.a, &env3, 1, Some(&c4.xid8))
            .await
            .map(|r| (r, db.a))
    });
    // give the reverse path time to enumerate (with the ledger index it finishes without r2;
    // with the source union or the dependency lock it waits on B)
    tokio::time::sleep(Duration::from_millis(300)).await;
    let reverse_finished_early = a.is_finished();
    db.ctl
        .execute("select pg_advisory_unlock($1)", &[&H])
        .await?;
    let (rb, _) = b.await??;
    let (ra, _) = a.await??;
    oracle(&db.ctl).await?;
    Ok(format!(
        "apply={rb}; reverse={ra}{}",
        if reverse_finished_early {
            " (enumerated before the move committed)"
        } else {
            " (waited for the move)"
        }
    ))
}

/// 6c: to-side TRUNCATE with from-side CDC pending.
async fn s6c_truncate(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one"), (3, "three")], &[(1, 1, 10), (2, 3, 20)]).await?;
    let c1 = source_tx(
        &db.user,
        "update src set amt = 11 where id = 1",
        1,
        "update",
        1,
        11,
    )
    .await?;
    source_tx(&db.user, "truncate parent", 0, "to-side", 0, 0).await?;
    // every child whose ledger join key points at the table
    let keys: Vec<i32> = db
        .ctl
        .query(
            "select from_key from ledger where parent_id is not null order by 1",
            &[],
        )
        .await?
        .iter()
        .map(|r| r.get(0))
        .collect();
    for k in &keys {
        rederive(&db.a, *k, None).await?;
    }
    oracle(&db.ctl).await?;
    let r1 = apply(&db.b, env, &c1, None, false).await?;
    oracle(&db.ctl).await?;
    Ok(format!(
        "re-derived {} children after truncate; pending apply: {r1}",
        keys.len()
    ))
}

/// #549: a forced recompute joins the live to-side, then the reverse record drains.
async fn s7_recompute_then_reverse(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one")], &[(1, 1, 10), (2, 1, 20)]).await?;
    let c2 = source_tx(
        &db.user,
        "update parent set name = 'uno' where id = 1",
        0,
        "to-side",
        1,
        0,
    )
    .await?;
    rederive(&db.a, 1, None).await?; // forced recompute of one child, before the reverse record
    let rr = reverse(&db.b, env, 1, Some(&c2.xid8)).await?;
    oracle(&db.ctl).await?;
    if !rr.contains("skipped 1") {
        return Err(anyhow!(
            "expected the recomputed child to be skipped as visible: {rr}"
        ));
    }
    Ok(format!("reverse: {rr}"))
}

/// #531/#529: a to-side change lost to a dropped slot, older CDC still pending; catch-up
/// re-derives; a deleted to-side key is reached through the ledger's join-key index.
async fn s8_lost_slot(env: &Env, delete_parent: bool) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one"), (3, "three")], &[(1, 1, 10), (2, 3, 20)]).await?;
    let c1 = source_tx(
        &db.user,
        "update src set amt = 11 where id = 1",
        1,
        "update",
        1,
        11,
    )
    .await?;
    if delete_parent {
        source_tx(
            &db.user,
            "delete from parent where id = 1",
            0,
            "to-side",
            1,
            0,
        )
        .await?;
        // the reverse path for a gone key: children found through the ledger, not the parent row
        let rr = reverse(&db.a, env, 1, None).await?;
        oracle(&db.ctl).await?;
        let r1 = apply(&db.b, env, &c1, None, false).await?;
        oracle(&db.ctl).await?;
        return Ok(format!(
            "reverse for deleted parent: {rr}; pending apply: {r1}"
        ));
    }
    source_tx(
        &db.user,
        "update parent set name = 'uno' where id = 1",
        0,
        "to-side",
        1,
        0,
    )
    .await?;
    // the rename's reverse record is lost; catch-up re-derives the key space
    for k in [1, 2] {
        rederive(&db.a, k, None).await?;
    }
    oracle(&db.ctl).await?;
    let r1 = apply(&db.b, env, &c1, None, false).await?;
    oracle(&db.ctl).await?;
    if r1 != "skip:visible" {
        return Err(anyhow!("expected skip:visible, got {r1}"));
    }
    Ok(format!("catch-up re-derive then pending apply: {r1}"))
}

/// Same-key changes draining in either order, with and without a delete.
async fn s9_same_key_order(env: &Env, variant: &str) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one")], &[(1, 1, 10), (2, 1, 20)]).await?;
    let (first, second, label) = match variant {
        "delete-then-older-update" => {
            let c1 = source_tx(
                &db.user,
                "update src set amt = 15 where id = 1",
                1,
                "update",
                1,
                15,
            )
            .await?;
            let c2 = source_tx(&db.user, "delete from src where id = 1", 1, "delete", 0, 0).await?;
            (c2, c1, "delete drains, then the older update")
        }
        "delete-then-reinsert" => {
            let c2 = source_tx(&db.user, "delete from src where id = 1", 1, "delete", 0, 0).await?;
            let c3 = source_tx(
                &db.user,
                "insert into src values (1, 1, 30)",
                1,
                "insert",
                1,
                30,
            )
            .await?;
            (c2, c3, "delete drains, then the newer re-insert")
        }
        "updates-out-of-order" => {
            let c1 = source_tx(
                &db.user,
                "update src set amt = 15 where id = 1",
                1,
                "update",
                1,
                15,
            )
            .await?;
            let c2 = source_tx(
                &db.user,
                "update src set amt = 17 where id = 1",
                1,
                "update",
                1,
                17,
            )
            .await?;
            (c2, c1, "newer update drains, then the older")
        }
        _ => {
            let c1 = source_tx(
                &db.user,
                "update src set amt = 15 where id = 1",
                1,
                "update",
                1,
                15,
            )
            .await?;
            let c2 = source_tx(
                &db.user,
                "update src set amt = 17 where id = 1",
                1,
                "update",
                1,
                17,
            )
            .await?;
            (c1, c2, "updates drain in order")
        }
    };
    let r1 = apply(&db.a, env, &first, None, false).await?;
    let r2 = apply(&db.b, env, &second, None, false).await?;
    oracle(&db.ctl).await?;
    Ok(format!("{label}: {r1}, {r2}"))
}

/// The in-progress case: the change's transaction was in flight when the basis was taken.
/// With the in-progress list stored, it is decidable (not visible -> apply), no re-derive needed.
async fn s10_in_progress_basis(env: &Env) -> Outcome {
    let db = fresh(env).await?;
    seed(&db, &[(1, "one")], &[(1, 1, 10), (2, 1, 20)]).await?;
    db.user
        .batch_execute("begin; update src set amt = 15 where id = 1")
        .await?;
    let r = db
        .user
        .query_one(
            "select pg_current_xact_id()::text, pg_current_wal_insert_lsn()::text",
            &[],
        )
        .await?;
    let ch = Change {
        key: 1,
        xid8: r.get(0),
        lsn: r.get(1),
        kind: "update",
        parent: 1,
        amt: 15,
    };
    // a later transaction completes first, so the in-flight xid sits below the snapshot's xmax
    // (an in-flight xid above xmax is simply "future"; it is never listed in xip)
    db.ctl
        .batch_execute("insert into parent values (99, 'filler')")
        .await?;
    rederive(&db.a, 1, None).await?; // basis has the user's xid in its in-progress list
    let in_xip: bool = db.ctl.query_one("select $1 = any(string_to_array(split_part(basis::text, ':', 3), ',')) from ledger where from_key = 1", &[&ch.xid8]).await?.get(0);
    db.user.batch_execute("commit").await?;
    let ra = apply(&db.b, env, &ch, None, false).await?;
    oracle(&db.ctl).await?;
    if !in_xip || ra != "applied" {
        let basis: String = db
            .ctl
            .query_one("select basis::text from ledger where from_key = 1", &[])
            .await?
            .get(0);
        return Err(anyhow!(
            "in_xip={in_xip} apply={ra} basis={basis} xid={}",
            ch.xid8
        ));
    }
    Ok(format!("xid in basis xip: {in_xip}; apply: {ra}"))
}

pub async fn run(args: &[String]) -> Result<()> {
    let mode = crate::pg::arg(args, "--mode", "lsn".to_string());
    let enumeration = crate::pg::arg(args, "--enumeration", "ledger".to_string());
    let only: Vec<&String> = args
        .iter()
        .filter(|a| {
            !a.starts_with("--")
                && ![
                    "lsn",
                    "literal",
                    "literal-snap",
                    "ledger",
                    "ledger+source",
                    "ledger+deplock",
                ]
                .contains(&a.as_str())
        })
        .collect();
    let admin = connect("postgres").await?;
    admin.batch_execute("select pg_terminate_backend(pid) from pg_stat_activity where datname = 'exp2' and pid <> pg_backend_pid()").await?;
    admin.batch_execute("drop database if exists exp2").await?;
    admin.batch_execute("create database exp2").await?;
    let env = Env {
        mode: mode.clone(),
        enumeration: enumeration.clone(),
    };
    println!("exp2: mode={mode} enumeration={enumeration}");

    let scenarios: Vec<(&str, std::pin::Pin<Box<dyn Future<Output = Outcome>>>)> = vec![
        (
            "2  #344 stalled enumeration, C before its snapshot",
            Box::pin(s2_stalled_enumeration(&env, false)),
        ),
        (
            "2b #344 stalled enumeration, C after its snapshot",
            Box::pin(s2_stalled_enumeration(&env, true)),
        ),
        (
            "3  #321 commit lands before the re-derive",
            Box::pin(s3_commit_before_rederive(&env)),
        ),
        (
            "4  #389 concurrent group creation, sorted batches",
            Box::pin(s4_concurrent_group_creation(&env, true)),
        ),
        (
            "4c #389 control: unsorted per-row locking",
            Box::pin(s4_concurrent_group_creation(&env, false)),
        ),
        (
            "5  #494 a->z->b folded while z rebuilds",
            Box::pin(s5_a_z_b(&env, false)),
        ),
        (
            "5b #494 a->z, z->b split, drained out of order",
            Box::pin(s5_a_z_b(&env, true)),
        ),
        (
            "6  #516 rename with from-side CDC pending",
            Box::pin(s6_rename_with_pending_cdc(&env)),
        ),
        (
            "6b #528 in-flight move vs rename enumeration",
            Box::pin(s6b_inflight_move_vs_rename(&env)),
        ),
        (
            "6c #520 to-side truncate with CDC pending",
            Box::pin(s6c_truncate(&env)),
        ),
        (
            "7  #549 forced recompute then reverse record",
            Box::pin(s7_recompute_then_reverse(&env)),
        ),
        (
            "8  #531 lost reverse record, catch-up re-derive",
            Box::pin(s8_lost_slot(&env, false)),
        ),
        (
            "8b #529 deleted parent reached via ledger",
            Box::pin(s8_lost_slot(&env, true)),
        ),
        (
            "9  tombstone: delete then older update",
            Box::pin(s9_same_key_order(&env, "delete-then-older-update")),
        ),
        (
            "9b delete then newer re-insert",
            Box::pin(s9_same_key_order(&env, "delete-then-reinsert")),
        ),
        (
            "9c same-key updates out of order",
            Box::pin(s9_same_key_order(&env, "updates-out-of-order")),
        ),
        (
            "9d same-key updates in order",
            Box::pin(s9_same_key_order(&env, "in-order")),
        ),
        (
            "10 change in flight at the basis snapshot",
            Box::pin(s10_in_progress_basis(&env)),
        ),
    ];
    let mut failed = 0;
    for (name, fut) in scenarios {
        if !only.is_empty() && !only.iter().any(|o| name.starts_with(o.as_str())) {
            continue;
        }
        let res = tokio::time::timeout(Duration::from_secs(300), fut).await;
        match res {
            Ok(Ok(note)) => println!("  PASS  {name:<52} {note}"),
            Ok(Err(e)) => {
                failed += 1;
                println!("  FAIL  {name:<52} {}", describe(&e))
            }
            Err(_) => {
                failed += 1;
                println!("  FAIL  {name:<52} timed out (deadlock or a step never released)")
            }
        }
        // clear any advisory locks / stuck backends between scenarios
        admin.batch_execute("select pg_terminate_backend(pid) from pg_stat_activity where datname = 'exp2' and pid <> pg_backend_pid()").await?;
    }
    println!("exp2 mode={mode} enumeration={enumeration}: {failed} failed");
    Ok(())
}
