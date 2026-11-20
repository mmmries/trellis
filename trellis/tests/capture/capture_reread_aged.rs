//! The live re-read on an aged cluster (#2205): a frozen version more than
//! `2^31` ids old keeps its raw `xmin`, so its `age` wraps negative and
//! passes the re-read's `age(t.xmin) <= 0` filter. The probe takes the
//! version nearest the writer first.
//!
//! - **Position sweep.** The stale version's `age` placed just below 0,
//!   mid-band (`-2^30`), just above `-2^31` and just outside the band
//!   (positive). At every point the writer's image is its own, at each
//!   isolation level, for an insert, an update and a key move, with the
//!   nested write as an application trigger and as an FK cascade.
//! - **Residual pins.** The two shapes the gaps register keeps
//!   (`docs/known-correctness-gaps.md`, "An old key deleted and re-created
//!   under `REPEATABLE READ`"): the wrong image happens, `self_check`
//!   reports it, and `request_backfill` makes the target exact.
//!
//! The ids are aged with `testkit`'s freeze-and-`pg_resetwal` jump, then
//! burned one by one to the exact point.

use testkit::{TestCluster, XID_PAGE};
use tokio_postgres::{Client, IsolationLevel};

use super::capture_reread::{install_spec, row, spec, staged_by, txid};
use crate::support::pipeline::connect;
use crate::support::relabel::H;

const EPOCH: u64 = 1 << 32;
/// How far from a band edge the sweep puts the stale version: room for the
/// ids the scenarios themselves assign.
const MARGIN: u32 = 2 * XID_PAGE;

/// Moves the next transaction id to the first full id past the current one
/// whose low 32 bits are `low`: a jump to the page at or below it, then
/// single-id transactions the rest of the way.
fn age_next_xid_to(cluster: &TestCluster, low: u32) {
    let next = cluster.next_xid();
    let mut target = next / EPOCH * EPOCH + u64::from(low);
    if target <= next {
        target += EPOCH;
    }
    let edge = (next / EPOCH + 1) * EPOCH;
    let page = u64::from(XID_PAGE);
    let lead = if target < edge {
        (edge - target).div_ceil(page) * page
    } else {
        page
    };
    if edge - lead > next {
        cluster.age_xids_to_epoch_edge(u32::try_from(lead).expect("a lead within one epoch"));
    }
    loop {
        let next = cluster.next_xid();
        if next >= target {
            return;
        }
        assert!(target - next < 10_000_000, "{} ids to burn", target - next);
        cluster.burn_xids((target - next) as u32);
    }
}

async fn age_of(client: &Client, sql: &str) -> i32 {
    client.query_one(sql, &[]).await.expect(sql).get(0)
}

#[derive(Debug, Clone, Copy)]
enum Event {
    Insert,
    Update,
    KeyMove,
}

#[derive(Debug, Clone, Copy)]
enum Nested {
    Trigger,
    Cascade,
}

/// `public.t` captured imaging `a` and `pid`, with sentinel row 0 and two
/// application triggers: one bumps the sentinel for a row written with
/// `nest`, the other moves the `public.p` key named by `shift`, whose FK
/// cascade updates `public.t`'s referencing row. Combo `i` owns ids
/// `10i+1..=10i+3`: the stale key `10i+1` (`a = 7`), a key move's source
/// `10i+2`, and a row `10i+3` referencing `p (10i)`. Row 5 is never written
/// again: its `age` is the stale versions'.
const SWEEP_DDL: &str = "create table public.p (id int primary key); \
     create table public.t (id int primary key, a int, \
         nest bool not null default false, shift int, \
         pid int references public.p (id) on update cascade); \
     create function public.bump() returns trigger language plpgsql as $$ \
     begin update public.t set a = a + 1 where id = 0; return null; end $$; \
     create trigger bump after insert or update on public.t \
         for each row when (new.nest) execute function public.bump(); \
     create function public.shift() returns trigger language plpgsql as $$ \
     begin update public.p set id = id + 1000 where id = new.shift; return null; end $$; \
     create trigger shift after insert or update on public.t \
         for each row when (new.shift is not null) execute function public.shift(); \
     insert into public.t (id, a) values (0, 0)";

const LEVELS: [IsolationLevel; 3] = [
    IsolationLevel::ReadCommitted,
    IsolationLevel::RepeatableRead,
    IsolationLevel::Serializable,
];

fn combos() -> Vec<(i32, IsolationLevel, Event, Nested)> {
    let mut out = Vec::new();
    for level in LEVELS {
        for event in [Event::Insert, Event::Update, Event::KeyMove] {
            for nested in [Nested::Trigger, Nested::Cascade] {
                out.push((10 * (out.len() as i32 + 1), level, event, nested));
            }
        }
    }
    out
}

/// The writer's statement for one combo, writing `a = 50` to key `k`, with
/// the nested write set off by `nest` (the trigger's own write) or `shift`
/// (the FK cascade of the trigger's write to `public.p`). A cascade lands in
/// the span of the statement whose trigger caused it; a cascade of the
/// statement's own write fires after its capture, so it can't make that
/// capture re-read. An update is the optimistic-lock shape (`and a = 40`):
/// under `REPEATABLE READ` a plain `where id = k` reaches the stale version
/// too and fails with a serialization error.
fn statement(base: i32, event: Event, nested: Nested) -> String {
    let k = base + 1;
    let from = base + 2;
    let set = match nested {
        Nested::Trigger => "nest".to_string(),
        Nested::Cascade => "shift".to_string(),
    };
    let value = match nested {
        Nested::Trigger => "true".to_string(),
        Nested::Cascade => base.to_string(),
    };
    match event {
        Event::Insert => format!("insert into public.t (id, a, {set}) values ({k}, 50, {value})"),
        Event::Update => {
            format!("update public.t set a = 50, {set} = {value} where id = {k} and a = 40")
        }
        Event::KeyMove => {
            format!("update public.t set id = {k}, a = 50, {set} = {value} where id = {from}")
        }
    }
}

/// One sweep point: the stale versions' `age` lands at `offset` past their
/// raw `xmin` (mod `2^32`), checked by `band`.
async fn sweep_at(offset: u32, band: impl Fn(i32) -> bool) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(SWEEP_DDL)
        .await
        .expect("create the tables");
    let seed: Vec<String> = combos()
        .iter()
        .map(|(b, ..)| {
            format!(
                "insert into public.p values ({b}); \
                 insert into public.t (id, a, pid) values ({}, 7, null), ({}, 8, null), ({}, 9, {b})",
                b + 1,
                b + 2,
                b + 3,
            )
        })
        .collect();
    raw.batch_execute(&format!(
        "insert into public.t (id, a) values (5, 7); {}",
        seed.join("; ")
    ))
    .await
    .expect("seed every combo in one transaction");
    install_spec(&mut raw, &spec("public.t", &["a", "pid"])).await;
    let xmin: i64 = raw
        .query_one("select xmin::text::bigint from public.t where id = 5", &[])
        .await
        .expect("the seed's xmin")
        .get(0);
    drop(raw);
    age_next_xid_to(&cluster, (xmin as u32).wrapping_add(offset));
    let raw = connect(db.dsn()).await;
    let stale_age = "select age(xmin) from public.t where id = 5";
    let age = age_of(&raw, stale_age).await;
    assert!(band(age), "the stale versions' age before: {age}");

    for (base, level, event, nested) in combos() {
        let k = base + 1;
        let label = format!("age {age}, {level:?}, {event:?}, {nested:?}");
        let mut writer = connect(db.dsn()).await;
        let txn = writer
            .build_transaction()
            .isolation_level(level)
            .start()
            .await
            .expect("begin");
        txn.query_one("select count(*) from public.t", &[])
            .await
            .expect("take the snapshot");
        raw.batch_execute(&format!("delete from public.t where id = {k}"))
            .await
            .expect("another session deletes the stale key");
        if let Event::Update = event {
            txn.batch_execute(&format!("insert into public.t (id, a) values ({k}, 40)"))
                .await
                .expect("re-create the key");
        }
        txn.batch_execute(&statement(base, event, nested))
            .await
            .unwrap_or_else(|e| panic!("{label}: {e:?}"));
        let id = txid(txn.client()).await;
        txn.commit().await.expect("commit");

        let staged = staged_by(&raw, "public.t", &id).await;
        let key = k.to_string();
        let new = format!(r#"{{"a": "50", "id": "{k}", "pid": null}}"#);
        let expected = match event {
            Event::Update => row(
                &key,
                "update",
                Some(&format!(r#"{{"a": "40", "id": "{k}", "pid": null}}"#)),
                Some(&new),
            ),
            Event::Insert | Event::KeyMove => row(&key, "insert", None, Some(&new)),
        };
        assert_eq!(
            staged.iter().rfind(|r| r.0 == key),
            Some(&expected),
            "{label}: {staged:?}"
        );
        assert!(
            !staged
                .iter()
                .any(|r| r.0 == key && r.3.as_deref().is_some_and(|i| i.contains(r#""a": "7""#))),
            "{label}: the stale version is never imaged: {staged:?}"
        );
    }
    let after = age_of(&raw, stale_age).await;
    assert!(band(after), "the stale versions' age after: {after}");
}

#[tokio::test]
async fn the_writers_own_version_wins_just_below_zero() {
    sweep_at(0u32.wrapping_sub(MARGIN), |age| {
        (-(2 * MARGIN as i32)..0).contains(&age)
    })
    .await;
}

#[tokio::test]
async fn the_writers_own_version_wins_mid_band() {
    sweep_at(3 << 30, |age| {
        (-(1 << 30) - 1_000_000..-(1 << 30) + 1_000_000).contains(&age)
    })
    .await;
}

#[tokio::test]
async fn the_writers_own_version_wins_just_above_the_band_floor() {
    sweep_at((1 << 31) + MARGIN, |age| {
        (i32::MIN..i32::MIN + 2 * MARGIN as i32).contains(&age)
    })
    .await;
}

#[tokio::test]
async fn the_writers_own_version_wins_just_outside_the_band() {
    sweep_at((1 << 31) - MARGIN, |age| {
        (i32::MAX - 2 * MARGIN as i32..=i32::MAX).contains(&age)
    })
    .await;
}

// ------------------------------------------------------------ residual pins

/// `public.t` with sentinel row 0 and stale key 1 (`a = 7`): an application
/// trigger deletes a row written with `a = 0` and bumps the sentinel for one
/// written with `nest`.
const RESIDUAL_DDL: &str = "create table public.t (id int primary key, a int, \
         nest bool not null default false); \
     insert into public.t (id, a) values (0, 0), (1, 7); \
     create function public.nested() returns trigger language plpgsql as $$ \
     begin \
         if new.a = 0 and new.id <> 0 then delete from public.t where id = new.id and a = 0; \
         elsif new.nest then update public.t set a = a + 1 where id = 0; end if; \
         return null; \
     end $$; \
     create trigger nested after insert or update on public.t \
         for each row execute function public.nested()";

const TARGET: &str = "select id, a from public.tt";
const ORACLE: &str = "select id, a from public.t";

async fn aged_harness() -> H {
    let h = H::new_aged(
        RESIDUAL_DDL,
        &["TRANSFORM tt FROM public.t SELECT a AS a"],
        Some(XID_PAGE),
    )
    .await;
    h.assert_exact("tt", TARGET, ORACLE).await;
    let age = age_of(&h.raw, "select age(xmin) from public.t where id = 1").await;
    assert!(age < 0, "key 1 is more than 2^31 ids old: age {age}");
    h
}

/// `self_check` reports the stale key, and `request_backfill` repairs it.
async fn assert_detected_and_repaired(mut h: H, wrong: &[&str]) {
    h.converge("after the writer").await;
    assert_eq!(
        h.rows("select id, a from public.tt where id = 1").await,
        wrong,
        "the stale version reached the target"
    );
    assert_ne!(h.rows(TARGET).await, h.rows(ORACLE).await);
    let verdict = h.self_check("tt").await;
    assert!(verdict.starts_with("Diverged"), "{verdict}");
    h.trellis
        .request_backfill("public.t")
        .await
        .expect("request_backfill");
    h.converge("after request_backfill").await;
    h.assert_exact("tt", TARGET, ORACLE).await;
}

/// Residual shape 1: the writer re-creates key 1 and its own trigger
/// deletes it again in the same statement, so the writer has no version of
/// the key and the stale one is the only candidate.
#[tokio::test]
async fn a_key_recreated_and_deleted_in_one_span_images_the_stale_version() {
    let h = aged_harness().await;
    let mut writer = connect(h.db.dsn()).await;
    let txn = writer
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await
        .expect("begin");
    txn.query_one("select count(*) from public.t", &[])
        .await
        .expect("take the snapshot");
    h.write("delete from public.t where id = 1").await;
    txn.batch_execute("insert into public.t (id, a) values (1, 0)")
        .await
        .expect("re-create key 1; the trigger deletes it");
    txn.commit().await.expect("commit");
    assert_detected_and_repaired(h, &["(1,7)"]).await;
}

/// Residual shape 2: the stale version's raw `xmin` lies between the
/// writer's top-level xid and its own version's xmin (written in a
/// subtransaction), so the stale version is nearer the writer.
#[tokio::test]
async fn a_stale_xmin_between_the_writers_xids_images_the_stale_version() {
    let h = aged_harness().await;
    let xmin: i64 = h
        .raw
        .query_one("select xmin::text::bigint from public.t where id = 1", &[])
        .await
        .expect("key 1's xmin")
        .get(0);
    let xmin = u32::try_from(xmin).expect("a 32-bit xmin");
    // The writer's top-level xid lands a few ids short of the stale xmin.
    age_next_xid_to(&h.cluster, xmin - 8);

    let mut writer = connect(h.db.dsn()).await;
    let txn = writer
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await
        .expect("begin");
    txn.query_one("select count(*) from public.t", &[])
        .await
        .expect("take the snapshot");
    let top: i64 = txn
        .query_one(
            "select pg_current_xact_id()::text::bigint % 4294967296",
            &[],
        )
        .await
        .expect("assign the top-level xid")
        .get(0);
    assert!(top < i64::from(xmin), "top-level xid {top} below {xmin}");
    h.write("delete from public.t where id = 1").await;
    // Its own version's subtransaction xid lands past it.
    age_next_xid_to(&h.cluster, xmin + 4);
    txn.batch_execute(
        "savepoint s; insert into public.t (id, a, nest) values (1, 50, true); release s",
    )
    .await
    .expect("re-create key 1 in a subtransaction, with a nested write");
    let ages: Vec<(i32, i32)> = txn
        .query(
            "select a, age(xmin) from public.t where id = 1 order by a",
            &[],
        )
        .await
        .expect("both versions of key 1")
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert!(
        matches!(ages[..], [(7, stale), (50, own)] if own < stale && stale <= 0),
        "the stale version is nearer the writer: {ages:?}"
    );
    txn.commit().await.expect("commit");
    assert_detected_and_repaired(h, &["(1,7)"]).await;
}
