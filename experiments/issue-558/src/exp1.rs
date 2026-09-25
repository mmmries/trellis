//! Experiment 1: can the basis be exact?
//!
//! `epoch`: stage 32-bit xids the way intake would (from pgoutput BEGIN
//! messages) and widen them to `xid8` against an anchor taken at staging time,
//! across a forced epoch boundary (`cluster.sh jump-epoch`).
//!
//! `snapshot`: under a 16-connection write load, take bases the way Re-derive
//! would (lock ledger row, then snapshot + read in one statement), check
//! `pg_visible_in_snapshot` decides every id, and count how often the
//! in-progress list is non-empty / actually matters for the locked row.

use crate::pg::{arg, connect, xid8};
use anyhow::{Result, bail};
use rand::{Rng, SeedableRng};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Widen a 32-bit xid staged from the decoder to `xid8`, given an anchor
/// (`pg_snapshot_xmax(pg_current_snapshot())` as u64) read at staging time.
/// The staged id was assigned before the anchor's snapshot, so it lies in
/// `(anchor - 2^32, anchor)`; pick the unique candidate with the right low bits.
pub fn widen(x32: u32, anchor: u64) -> u64 {
    let back = (anchor as u32).wrapping_sub(x32) as u64;
    anchor - back
}

/// The naive widening the design note's wording could be read as: same epoch as the anchor.
fn widen_naive(x32: u32, anchor: u64) -> u64 {
    (anchor & !0xFFFF_FFFF) | x32 as u64
}

pub async fn epoch(_args: &[String]) -> Result<()> {
    let c = connect("postgres").await?;
    // Logical decoding only reads flushed WAL; the cluster runs synchronous_commit=off,
    // so make this session's commits flush or the slot stops short of the last ones.
    c.batch_execute("set synchronous_commit = on").await?;
    c.batch_execute(
        "drop table if exists t; create table t(id serial primary key, v int);
         drop publication if exists pub; create publication pub for table t;
         select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = 'exp';",
    )
    .await?;
    c.batch_execute("select pg_create_logical_replication_slot('exp', 'pgoutput')")
        .await?;

    let start = xid8(&c, "pg_snapshot_xmax(pg_current_snapshot())").await?;
    let low = start & 0xFFFF_FFFF;
    let to_burn = (0x1_0000_0000u64 - low) as usize + 40;
    println!(
        "next xid8 at start: {start} (epoch {}, low {low:#x}); committing {to_burn} transactions to cross the boundary",
        start >> 32
    );
    if to_burn > 200_000 {
        bail!(
            "cluster is not near an epoch boundary; run `cluster.sh jump-epoch <dir> 7 FFFF8000` first"
        );
    }

    // Commit sequentially on one connection; each statement is its own transaction.
    // `pg_current_xact_id()` inside it is the ground truth xid8.
    let stmt = c
        .prepare("insert into t(v) values (1) returning pg_current_xact_id()::text")
        .await?;
    let mut truth: Vec<u64> = Vec::with_capacity(to_burn);
    let t0 = Instant::now();
    for _ in 0..to_burn {
        let row = c.query_one(&stmt, &[]).await?;
        truth.push(row.get::<_, String>(0).parse()?);
    }
    println!(
        "committed {} txns in {:?}; first {} last {}",
        truth.len(),
        t0.elapsed(),
        truth[0],
        truth[truth.len() - 1]
    );
    let crossed = truth.iter().filter(|x| **x >> 32 != start >> 32).count();
    println!(
        "{crossed} of them are in the new epoch; the first new-epoch xid8 is {:?}",
        truth.iter().find(|x| **x >> 32 != start >> 32)
    );

    // Staging time: after the boundary. Anchor = xmax of the current snapshot, no xid assigned.
    let anchor = xid8(&c, "pg_snapshot_xmax(pg_current_snapshot())").await?;
    println!("anchor at staging time: {anchor} (epoch {})", anchor >> 32);

    // Read the slot the way intake does (binary pgoutput), take the BEGIN xid.
    let rows = c
        .query(
            "select lsn::text, xid::text, data from pg_logical_slot_get_binary_changes('exp', null, null, 'proto_version', '1', 'publication_names', 'pub')",
            &[],
        )
        .await?;
    let mut staged: Vec<(u32, u32)> = Vec::new(); // (BEGIN payload xid, SRF xid column)
    for r in &rows {
        let data: Vec<u8> = r.get(2);
        if data.first() == Some(&b'B') {
            let payload = u32::from_be_bytes(data[17..21].try_into().unwrap());
            let col: u32 = r.get::<_, String>(1).parse()?;
            staged.push((payload, col));
        }
    }
    println!(
        "decoded {} BEGIN messages from the slot ({} messages total)",
        staged.len(),
        rows.len()
    );
    if staged.len() != truth.len() {
        bail!("expected {} BEGINs, got {}", truth.len(), staged.len());
    }

    let mut exact = 0usize;
    let mut naive_wrong = 0usize;
    let mut col_mismatch = 0usize;
    let mut worst_back = 0u64;
    for (i, &(payload, col)) in staged.iter().enumerate() {
        if payload != col {
            col_mismatch += 1;
        }
        let w = widen(payload, anchor);
        if w == truth[i] {
            exact += 1;
        } else if exact == i {
            println!(
                "FIRST MISMATCH at #{i}: staged {payload:#x} widened {w} truth {}",
                truth[i]
            );
        }
        if widen_naive(payload, anchor) != truth[i] {
            naive_wrong += 1;
        }
        worst_back = worst_back.max(anchor - truth[i]);
    }
    println!(
        "widen(): {exact}/{} exact; naive same-epoch widening wrong for {naive_wrong}; BEGIN xid vs SRF xid column mismatches: {col_mismatch}; max staging lag {worst_back} ids",
        truth.len()
    );

    // A basis snapshot taken before the boundary decides new-epoch ids as invisible,
    // and one taken after decides old-epoch ids as visible.
    let pre = truth[0];
    let post = truth[truth.len() - 1];
    let vis: Vec<(bool, bool)> = c
        .query(
            "with s as (select pg_current_snapshot() as snap)
             select pg_visible_in_snapshot($1::text::xid8, snap), pg_visible_in_snapshot($2::text::xid8, snap) from s",
            &[&pre.to_string(), &post.to_string()],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    println!(
        "post-boundary snapshot: pre-boundary id visible={} post-boundary id visible={}",
        vis[0].0, vis[0].1
    );
    // Stored snapshot text round-trips with the epoch intact.
    let snap_text: String = c
        .query_one("select pg_current_snapshot()::text", &[])
        .await?
        .get(0);
    println!("pg_current_snapshot() text after the boundary: {snap_text}");

    let ok = exact == truth.len() && col_mismatch == 0 && vis[0].0 && vis[0].1;
    println!(
        "\nEXPERIMENT 1a (epoch widening): {}",
        if ok { "PASS" } else { "FAIL" }
    );
    Ok(())
}

pub async fn snapshot(args: &[String]) -> Result<()> {
    let rows: i32 = arg(args, "--rows", 100_000);
    let writers: usize = arg(args, "--writers", 16);
    let samplers: usize = arg(args, "--samplers", 4);
    let secs: u64 = arg(args, "--secs", 20);
    let think_ms: u64 = arg(args, "--think-ms", 0);
    println!(
        "exp1-snapshot: rows={rows} writers={writers} samplers={samplers} secs={secs} think_ms={think_ms}"
    );

    let c = connect("postgres").await?;
    c.batch_execute(&format!(
        "drop table if exists rows_, ledger, commits, bases, split_check cascade;
         create table rows_(id int primary key, v int not null default 0);
         insert into rows_(id) select g from generate_series(1, {rows}) g;
         create table ledger(id int primary key);
         insert into ledger select g from generate_series(1, {rows}) g;
         create table commits(seq bigserial, xid8 xid8 not null, row_id int not null);
         create table bases(seq bigserial, row_id int not null, snap pg_snapshot not null, xip_len int not null, read_v int not null);
         create table split_check(seq bigserial, same bool not null);"
    ))
    .await?;

    let stop = Arc::new(AtomicBool::new(false));
    let commits = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..writers {
        let stop = stop.clone();
        let commits = commits.clone();
        tasks.push(tokio::spawn(async move {
            let c = connect("postgres").await?;
            // One transaction: the update and the ground-truth record of its xid8 and row.
            let sql = if think_ms > 0 {
                format!("begin; update rows_ set v = v + 1 where id = $1; select pg_sleep({}); insert into commits(xid8, row_id) values (pg_current_xact_id(), $1); commit", think_ms as f64 / 1000.0)
            } else {
                String::new()
            };
            let one = c.prepare("with u as (update rows_ set v = v + 1 where id = $1) insert into commits(xid8, row_id) values (pg_current_xact_id(), $1)").await?;
            let upd = c.prepare("update rows_ set v = v + 1 where id = $1").await?;
            let rec = c.prepare("insert into commits(xid8, row_id) values (pg_current_xact_id(), $1)").await?;
            let mut rng = rand::rngs::StdRng::from_os_rng();
            while !stop.load(Ordering::Relaxed) {
                let id: i32 = rng.random_range(1..=rows);
                if sql.is_empty() {
                    c.execute(&one, &[&id]).await?;
                } else {
                    c.batch_execute("begin").await?;
                    c.execute(&upd, &[&id]).await?;
                    tokio::time::sleep(Duration::from_millis(think_ms)).await;
                    c.execute(&rec, &[&id]).await?;
                    c.batch_execute("commit").await?;
                }
                commits.fetch_add(1, Ordering::Relaxed);
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    for s in 0..samplers {
        let stop = stop.clone();
        tasks.push(tokio::spawn(async move {
            let c = connect("postgres").await?;
            let lock = c.prepare("select id from ledger where id = $1 for update").await?;
            // Snapshot and read in ONE statement: that is the basis of this read.
            let read = c
                .prepare(
                    "insert into bases(row_id, snap, xip_len, read_v)
                     select $1, s, coalesce(array_length(string_to_array(nullif(split_part(s::text, ':', 3), ''), ','), 1), 0), r.v
                     from pg_current_snapshot() s, rows_ r where r.id = $1",
                )
                .await?;
            // Control: snapshot in one statement, read in the next (READ COMMITTED gives each its own).
            let split = c
                .prepare("insert into split_check(same) select $1 = pg_current_snapshot()::text")
                .await?;
            let mut rng = rand::rngs::StdRng::from_os_rng();
            while !stop.load(Ordering::Relaxed) {
                let id: i32 = rng.random_range(1..=rows);
                c.batch_execute("begin").await?;
                c.execute(&lock, &[&id]).await?;
                c.execute(&read, &[&id]).await?;
                if s == 0 {
                    let snap: String = c.query_one("select pg_current_snapshot()::text", &[]).await?.get(0);
                    c.execute(&split, &[&snap]).await?;
                }
                c.batch_execute("commit").await?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    let t0 = Instant::now();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        t.await??;
    }
    let elapsed = t0.elapsed().as_secs_f64();
    println!(
        "load: {:.0} commits/s over {elapsed:.1}s",
        commits.load(Ordering::Relaxed) as f64 / elapsed
    );

    // ---- analysis
    // xid8 has no arithmetic, so mirror it as a bigint for the range joins below.
    c.batch_execute("alter table commits add column xn bigint; update commits set xn = xid8::text::bigint; create index on commits(row_id, xn); create index on commits(xn); analyze commits; analyze bases").await?;
    let r = c.query_one("select count(*), count(*) filter (where xip_len > 0), avg(xip_len)::float8, max(xip_len), percentile_cont(0.5) within group (order by xip_len) from bases", &[]).await?;
    let n: i64 = r.get(0);
    let nonempty: i64 = r.get(1);
    println!(
        "bases: {n}; xip non-empty: {nonempty} ({:.1}%); mean xip len {:.2}; p50 {:.0}; max {}",
        100.0 * nonempty as f64 / n as f64,
        r.get::<_, f64>(2),
        r.get::<_, f64>(4),
        r.get::<_, i32>(3)
    );
    let r = c
        .query_one(
            "select count(*), count(*) filter (where not same) from split_check",
            &[],
        )
        .await?;
    println!(
        "READ COMMITTED control: {} of {} snapshot-then-read statement pairs saw a different snapshot",
        r.get::<_, i64>(1),
        r.get::<_, i64>(0)
    );

    // Decidability: every commit against every base (sampled to keep it tractable), classified by
    // xmin/xmax/xip position, must agree with pg_visible_in_snapshot: below xmin -> visible; in xip
    // -> not visible; between and not in xip -> visible (all our commits committed); >= xmax -> not.
    c.batch_execute(
        "create temp view base_x as select seq, row_id, snap, read_v, pg_snapshot_xmin(snap) xmin, pg_snapshot_xmax(snap) xmax,
           pg_snapshot_xmin(snap)::text::bigint xmin_n, pg_snapshot_xmax(snap)::text::bigint xmax_n,
           coalesce(string_to_array(nullif(split_part(snap::text, ':', 3), ''), ',')::xid8[], '{}') xip from bases;",
    )
    .await?;
    let r = c
        .query_one(
            "with pairs as (
               select b.seq, c.xid8, b.snap,
                 case when c.xid8 < b.xmin then 'below' when c.xid8 >= b.xmax then 'above' when c.xid8 = any(b.xip) then 'xip' else 'between' end as pos,
                 pg_visible_in_snapshot(c.xid8, b.snap) vis
               from (select * from base_x order by seq limit 300) b
               join commits c on (c.xn >= b.xmin_n - 2000 and c.xn < b.xmax_n + 2000) or c.seq <= 200)
             select count(*), count(*) filter (where pos = 'below' and not vis), count(*) filter (where pos = 'above' and vis),
                    count(*) filter (where pos = 'xip' and vis), count(*) filter (where pos = 'between' and not vis),
                    count(*) filter (where pos = 'between'), count(*) filter (where pos = 'xip')
             from pairs",
            &[],
        )
        .await?;
    let bad: i64 =
        r.get::<_, i64>(1) + r.get::<_, i64>(2) + r.get::<_, i64>(3) + r.get::<_, i64>(4);
    println!(
        "decidability over {} (base, commit) pairs: {} between xmin and xmax not in xip (visible), {} in xip (not visible), {bad} disagreements",
        r.get::<_, i64>(0),
        r.get::<_, i64>(5),
        r.get::<_, i64>(6)
    );

    // What matters for the design: of the changes that will drain against this base for THIS row,
    // how many were in flight when the base was taken?
    let r = c
        .query_one(
            "with per_base as (
               select b.seq,
                 (select count(*) from commits c where c.row_id = b.row_id and c.xn >= b.xmin_n and c.xn < b.xmax_n and c.xid8 = any(b.xip)) as ambiguous
               from base_x b),
             sample as (
               select b.seq,
                 (select count(*) from commits c where c.row_id = b.row_id and c.xn >= b.xmax_n) as future
               from (select * from base_x order by seq limit 2000) b)
             select (select count(*) from per_base), (select count(*) filter (where ambiguous > 0) from per_base), (select sum(ambiguous)::bigint from per_base),
                    (select sum(future)::bigint from sample), (select sum(ambiguous)::bigint from per_base p join sample s using (seq))",
            &[],
        )
        .await?;
    let n: i64 = r.get(0);
    let amb_bases: i64 = r.get(1);
    let amb: i64 = r.get::<_, i64>(2);
    let fut: i64 = r.get::<_, i64>(3);
    let amb_s: i64 = r.get::<_, i64>(4);
    println!(
        "per locked row: {amb_bases} of {n} bases ({:.3}%) had >=1 in-flight change for that row ({amb} ambiguous changes in all); in the first 2000 bases, {amb_s} ambiguous vs {fut} plainly-future changes for the row ({:.4}% of the changes a base will be checked against)",
        100.0 * amb_bases as f64 / n as f64,
        100.0 * amb_s as f64 / (amb_s + fut).max(1) as f64
    );
    // Cross-check that the in-one-statement basis really is the read's snapshot: a base's read_v must
    // equal the number of commits for that row visible in its snapshot (v counts every committed update).
    let r = c
        .query_one(
            "select count(*) filter (where b.read_v <> (select count(*) from commits c where c.row_id = b.row_id and pg_visible_in_snapshot(c.xid8, b.snap))) , count(*)
             from (select * from base_x order by seq limit 200) b",
            &[],
        )
        .await?;
    println!(
        "basis exactness: {} of {} bases read a value that disagrees with the commits their snapshot says are visible",
        r.get::<_, i64>(0),
        r.get::<_, i64>(1)
    );
    Ok(())
}
