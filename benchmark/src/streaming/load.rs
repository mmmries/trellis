//! Load generators against a chain's source table (issue #266's B0
//! prerequisite (a)): a controlled-offered-rate generator for the
//! latency-focused scenarios (B1, where the offered rate is an independent
//! variable, not the thing being measured) and a max-rate generator for the
//! throughput-focused ones (B2/B3's ramp, E3's intake ceiling), which push
//! as fast as one connection allows instead of holding a fixed rate.

use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;

#[derive(Debug, Clone, Copy)]
pub struct LoadConfig {
    pub commits_per_sec: f64,
    pub rows_per_commit: usize,
    pub duration: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct LoadSummary {
    pub commits_issued: u64,
    pub rows_issued: u64,
    pub elapsed: Duration,
}

/// Builds and runs one `INSERT` of `rows_per_commit` values against
/// `public.<source_table>` (`id bigint primary key, val numeric`), starting
/// at `*next_id` and advancing it past the ids just used. Postgres commits a
/// single-statement `execute` as its own implicit transaction, giving
/// exactly the "N rows per commit" shape B4 wants with no explicit
/// `BEGIN`/`COMMIT` round trip.
async fn insert_batch(
    raw: &RawClient,
    source_table: &str,
    next_id: &mut i64,
    rows_per_commit: usize,
) {
    let mut values = Vec::with_capacity(rows_per_commit);
    for _ in 0..rows_per_commit {
        values.push(format!("({next_id}, {next_id})"));
        *next_id += 1;
    }
    let sql = format!(
        "insert into public.{source_table} (id, val) values {}",
        values.join(",")
    );
    raw.execute(sql.as_str(), &[])
        .await
        .expect("insert load batch");
}

/// Runs `cfg` against `public.<source_table>`, starting ids at `first_id` —
/// callers reserve low/negative ids (e.g. `chain::warm_up`'s `-1`) for their
/// own out-of-band rows so they never collide with, or get miscounted
/// among, this generator's own. One `INSERT` per tick; ids advance
/// `rows_per_commit` at a time (see [`insert_batch`]).
///
/// Deliberately not trying to push the offered rate as high as the
/// connection allows — that's [`run_max_rate_load`]'s job. This holds a
/// *precise, controlled* rate steady, self-throttling (not bursting to
/// catch up) if a single insert ever takes longer than one tick period —
/// see `tokio::time::MissedTickBehavior::Delay`.
pub async fn run_controlled_load(
    raw: &RawClient,
    source_table: &str,
    first_id: i64,
    cfg: &LoadConfig,
) -> LoadSummary {
    assert!(
        cfg.commits_per_sec > 0.0,
        "commits_per_sec must be positive"
    );
    assert!(
        cfg.rows_per_commit >= 1,
        "rows_per_commit must be at least 1"
    );

    let period = Duration::from_secs_f64(1.0 / cfg.commits_per_sec);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let start = Instant::now();
    let deadline = start + cfg.duration;
    let mut commits = 0u64;
    let mut next_id = first_id;

    loop {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        insert_batch(raw, source_table, &mut next_id, cfg.rows_per_commit).await;
        commits += 1;
    }

    LoadSummary {
        commits_issued: commits,
        rows_issued: commits * cfg.rows_per_commit as u64,
        elapsed: start.elapsed(),
    }
}

/// Runs back-to-back `INSERT`s of `rows_per_commit` values each, with no
/// pacing at all, for `duration` — as many commits as one connection can
/// physically push through. This is what B2/B3's throughput ramp and E3's
/// intake-ceiling probe offer as "load": the achieved rate this reports
/// (`rows_issued / elapsed`) is therefore a *floor* on the engine's own
/// ceiling, not a clean measurement of it — a single generator connection
/// issuing sequential, awaited `execute` calls may itself be the bottleneck
/// at high enough rates, well before intake or apply are stressed at all.
/// Every caller of this function must report the achieved rate next to
/// whatever it concludes, not assume the offered load equalled the target.
pub async fn run_max_rate_load(
    raw: &RawClient,
    source_table: &str,
    first_id: i64,
    rows_per_commit: usize,
    duration: Duration,
) -> LoadSummary {
    assert!(rows_per_commit >= 1, "rows_per_commit must be at least 1");

    let start = Instant::now();
    let deadline = start + duration;
    let mut commits = 0u64;
    let mut next_id = first_id;

    while Instant::now() < deadline {
        insert_batch(raw, source_table, &mut next_id, rows_per_commit).await;
        commits += 1;
    }

    LoadSummary {
        commits_issued: commits,
        rows_issued: commits * rows_per_commit as u64,
        elapsed: start.elapsed(),
    }
}
