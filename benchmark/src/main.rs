//! Throughput/latency harness for the `trellis` crate.
//!
//! Issue #63's milestone 0: a deterministic, repeatable backfill benchmark
//! (`posts` -> `posts_calc` -> `posts_totals`) that becomes the yardstick
//! M2/M3/M4 are checked against. See `scenario` for the actual pipeline and
//! measurements, `generate` for the deterministic data shape.
//!
//! ## Running
//!
//! This binary carries `required-features = ["engine-access"]` (ADR-0012; see
//! `Cargo.toml`), so every invocation must pass `--features engine-access` —
//! without it Cargo skips the target and `cargo run` fails outright:
//!
//! ```text
//! cargo run -p benchmark --features engine-access --release -- high-cardinality
//! cargo run -p benchmark --features engine-access --release -- low-cardinality
//! cargo run -p benchmark --features engine-access --release -- both
//! cargo run -p benchmark --features engine-access --release -- custom --n 200000 --g 500 --ceiling-secs 60
//! cargo run -p benchmark --features engine-access --release -- relationship-aggregate
//! cargo run -p benchmark --features engine-access --release -- hop-ladder
//! cargo run -p benchmark --features engine-access --release -- hop-latency --depth 10 --rate 10 --duration-secs 60
//! cargo run -p benchmark --features engine-access --release -- hop-latency --depth 10 --rate 10 --maintenance-interval-ms 10 --poll-interval-ms 20 --duration-secs 30
//! cargo run -p benchmark --features engine-access --release -- throughput-ramp --rates 1000,5000,10000 --duration-secs 20 --grace-secs 30
//! cargo run -p benchmark --features engine-access --release -- intake-ceiling --rows-per-commit 1000 --duration-secs 20
//! cargo run -p benchmark --features engine-access --release -- seal-cadence-sweep --intervals-ms 300,100,30,10 --depths 1,2,3,5,10
//! cargo run -p benchmark --features engine-access --release -- transaction-shape --shapes 1,100,10000 --target-rate 20000
//! ```
//!
//! `hop-ladder`/`hop-latency`/`throughput-ramp`/`intake-ceiling`/
//! `seal-cadence-sweep`/`transaction-shape` (issue #266's B0/B1/B2/B4/E2/E3)
//! are a different shape
//! from every other scenario here: they drive the real streaming path (a
//! live `trellis::Client`, CDC intake -> ring -> seal -> claim -> fold ->
//! apply — see `streaming`) rather than the direct, ring-bypassing backfill
//! build the scenarios above time.
//!
//! - `hop-ladder` runs the full depth-1/2/3/5/10 latency sweep;
//!   `hop-latency --depth N` runs one depth alone (for a future
//!   regression-guard lane once a baseline exists, and for issue #268's X1
//!   wake-edge-diagnostic sweeps). Both accept `--rate <commits/sec>` and
//!   `--duration-secs <secs>` (defaults: 10/s, 60s); `hop-latency` also
//!   accepts `--maintenance-interval-ms <ms>` (default 300) and
//!   `--poll-interval-ms <ms>` (default 200, `ClientOptions::poll_interval`'s
//!   own default) — X1a sweeps the latter, X1b sweeps `--rate`, both at a
//!   fixed 10ms `--maintenance-interval-ms` and `--depth 10`. Every result
//!   line also reports `per_hop_mean_ms`: exact mean latency for *every* hop
//!   in the chain (`trellis_transform_latency_seconds_sum`/`_count`), not
//!   just the terminal hop's T1 bucket fractions — promoted from #266's E1
//!   scratch decomposition into a committed capability.
//! - `throughput-ramp` (B2) ramps single-hop 1-1 throughput until a
//!   candidate rate's backlog fails to drain within `--grace-secs`
//!   (default 30s) after a `--duration-secs` (default 20s) offered window;
//!   `--rates` overrides the default candidate list (comma-separated
//!   rows/sec, ascending). See `streaming::b2_throughput_ramp`'s module doc
//!   for why this is a *candidate* saturation rate, not a full T2
//!   confirmation.
//! - `intake-ceiling` (E3, tests H2) measures CDC decode + ring append
//!   alone (`application_threads: 0`, no transforms) — the hard ceiling
//!   `throughput-ramp` sits under.
//! - `seal-cadence-sweep` (E2, tests H1) reruns `hop-ladder`'s depths at
//!   each `--intervals-ms` candidate (default 300/100/30/10, the issue's
//!   own numbers) — does a shorter `maintenance_interval` actually buy back
//!   the latency B1 misses, and does it scale the way H1 predicts?
//! - `transaction-shape` (B4) holds `--target-rate` (default 20k rows/sec)
//!   fixed and sweeps `--shapes` (default 1,100,10000 rows/commit) —
//!   separates per-commit cost from per-row cost.
//!
//! `--release` matters: this pushes 1M rows through a real Postgres
//! instance and a real CDC pipeline, and the debug-build overhead is large
//! enough to distort the numbers. Each scenario prints one line of JSON to
//! stdout (see [`scenario::BenchResult::to_json`]) and the process exits
//! non-zero if the aggregate-backfill phase exceeds its regression
//! ceiling — wire this into CI as `cargo run -p benchmark --features
//! engine-access --release -- high-cardinality`.

mod generate;
mod scenario;
mod scenario_relationship;
mod streaming;

use std::time::Duration;

/// Post-M3 this shape's aggregate phase measures ~0.7-0.8s on this
/// harness/box across repeated runs — the direct, single-pass-then-chunked
/// build ([`trellis::dev::defs::backfill_definition`], issue #63) replaced the ring
/// drain that took ~55-58s here (and ~1m50s on the issue's poc cluster). The
/// M3-review fix (aggregate the source once into a staging table, then chunk
/// the writes from that small table instead of re-scanning the source per
/// chunk) took this from ~1.25s to ~0.75s. 10s keeps >10x headroom over this
/// box's measurement for CI/dev-machine jitter and cold caches while still
/// firing long before any regression back toward the old tens-of-seconds
/// mechanism. Revisit once a CI-hardware baseline exists.
const HIGH_CARDINALITY_CEILING: Duration = Duration::from_secs(10);

/// Post-M3 this shape's aggregate phase measures ~35ms on this harness/box:
/// with only 100 groups the direct build issues a single group-key chunk, so
/// it's far faster than the 100k-group high-cardinality shape (which they no
/// longer track — the direct build's cost scales with group count, so the two
/// diverge sharply, as M3 intended). 5s keeps generous headroom while still
/// catching a regression that would make small-group builds pathological.
const LOW_CARDINALITY_CEILING: Duration = Duration::from_secs(5);

struct Scenario {
    name: &'static str,
    n: i64,
    g: i64,
    ceiling: Duration,
}

const HIGH_CARDINALITY: Scenario = Scenario {
    name: "high-cardinality",
    n: 1_000_000,
    g: 100_000,
    ceiling: HIGH_CARDINALITY_CEILING,
};

const LOW_CARDINALITY: Scenario = Scenario {
    name: "low-cardinality",
    n: 1_000_000,
    g: 100,
    ceiling: LOW_CARDINALITY_CEILING,
};

/// Row counts for the relationship-aggregate scenario (issue #63, C3),
/// matching the real-world ratios reported in the poc that motivated
/// `backfill_relationship_one_to_one`: 100k authors, 1M posts, 4.5M comments.
const RELATIONSHIP_AUTHORS: i64 = 100_000;
const RELATIONSHIP_POSTS: i64 = 1_000_000;
const RELATIONSHIP_COMMENTS: i64 = 4_500_000;

/// Measured ~660-670ms for `install_definition` end to end (target-table
/// creation + the direct relationship-aware build,
/// `backfill_relationship_one_to_one`) on this harness/box across repeated
/// runs against the full 100k/1M/4.5M row counts above — down from the ~1
/// minute the ring path took on the real-world shape that motivated this
/// benchmark (issue #63 C2's handoff doc). 10s keeps >10x headroom for
/// CI/dev-machine jitter and cold caches while still firing long before any
/// regression back toward the ring's tens-of-seconds mechanism. Revisit once
/// a CI-hardware baseline exists.
const RELATIONSHIP_AGGREGATE_CEILING: Duration = Duration::from_secs(10);

/// B1's default offered rate (issue #266: "low offered rate (e.g. 10
/// commits/s)") and per-depth measurement window. 60s at 10 commits/sec
/// gives ~600 end-to-end observations per depth — enough for the p99
/// bucket-fraction check to mean something (a single miss out of 600 is
/// already under the 1% p99 tolerance).
const HOP_LADDER_DEFAULT_RATE: f64 = 10.0;
const HOP_LADDER_DEFAULT_DURATION: Duration = Duration::from_secs(60);

/// B2's default candidate rates (rows/sec) — spans well below and above
/// T2's 100k rows/sec target so the ramp's knee (if any, below 200k) is
/// bracketed rather than only ever tested against a single guess.
const THROUGHPUT_RAMP_DEFAULT_RATES: &[f64] = &[
    1_000.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0, 100_000.0, 200_000.0,
];
const THROUGHPUT_RAMP_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const THROUGHPUT_RAMP_DEFAULT_GRACE: Duration = Duration::from_secs(30);

const INTAKE_CEILING_DEFAULT_ROWS_PER_COMMIT: usize = 1000;
const INTAKE_CEILING_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const INTAKE_CEILING_DEFAULT_GRACE: Duration = Duration::from_secs(30);

/// E2's default sweep, straight from the issue's own text ("Sweep
/// `maintenance_interval` at 300 / 100 / 30 / 10 ms").
const SEAL_CADENCE_SWEEP_DEFAULT_INTERVALS_MS: &[f64] = &[300.0, 100.0, 30.0, 10.0];
const SEAL_CADENCE_SWEEP_DEFAULT_DEPTHS: &[f64] = &[1.0, 2.0, 3.0, 5.0, 10.0];
const SEAL_CADENCE_SWEEP_DEFAULT_RATE: f64 = 10.0;
const SEAL_CADENCE_SWEEP_DEFAULT_DURATION: Duration = Duration::from_secs(15);

/// B4's default target rate: comfortably under B2's measured ~220-250k
/// rows/sec single-hop ceiling on this box, so a `sustained: false` result
/// here means the transaction shape hurt, not that the rate alone would
/// already have saturated regardless of shape.
const TRANSACTION_SHAPE_DEFAULT_TARGET_RATE: f64 = 20_000.0;
const TRANSACTION_SHAPE_DEFAULT_DURATION: Duration = Duration::from_secs(20);
const TRANSACTION_SHAPE_DEFAULT_GRACE: Duration = Duration::from_secs(30);

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let name = args.first().map(String::as_str).unwrap_or("both");

    if name == "hop-ladder" || name == "hop-latency" {
        let rate = parse_flag(&args, "--rate")
            .map(|v| v as f64)
            .unwrap_or(HOP_LADDER_DEFAULT_RATE);
        let duration = parse_flag(&args, "--duration-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(HOP_LADDER_DEFAULT_DURATION);
        // Issue #268's X1: both fields already existed on `ClientOptions`
        // (`maintenance_interval` since E2, `poll_interval` since the
        // engine's wake path itself) — X1 only needed these two small CLI
        // additions to `hop-latency`, not a new scenario, to run the
        // wake-edge diagnostic sweeps.
        let maintenance_interval_ms = parse_flag(&args, "--maintenance-interval-ms")
            .map(|v| v as u64)
            .unwrap_or_else(|_| {
                streaming::b1_hop_ladder::DEFAULT_MAINTENANCE_INTERVAL.as_millis() as u64
            });
        let poll_interval_ms = parse_flag(&args, "--poll-interval-ms")
            .map(|v| v as u64)
            .unwrap_or_else(|_| streaming::b1_hop_ladder::DEFAULT_POLL_INTERVAL.as_millis() as u64);

        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let results = if name == "hop-latency" {
            let depth =
                parse_flag(&args, "--depth").expect("hop-latency requires --depth <hops>") as usize;
            vec![runtime.block_on(streaming::b1_hop_ladder::run_depth_with_poll_interval(
                depth,
                rate,
                duration,
                Duration::from_millis(maintenance_interval_ms),
                Duration::from_millis(poll_interval_ms),
            ))]
        } else {
            runtime.block_on(streaming::b1_hop_ladder::run_ladder(rate, duration))
        };

        let mut any_missed_t1 = false;
        let mut any_zero_samples = false;
        for result in &results {
            println!("{}", result.to_json());
            if result.e2e_count == 0 {
                eprintln!(
                    "WARNING: depth {} observed zero end-to-end samples — commits_issued={}, \
                     changes_applied_terminal={}; the metric sanity check the issue asks for \
                     (commits offered vs. changes applied) has already failed, so this depth's \
                     T1 result is meaningless, not just failing",
                    result.depth, result.commits_issued, result.changes_applied_terminal
                );
                any_zero_samples = true;
            } else if !result.t1_all_pass {
                any_missed_t1 = true;
            }
        }
        if any_missed_t1 {
            eprintln!("one or more depths missed a T1 target — see the bucket fractions above");
        }
        // Deliberately not `process::exit(1)` on a T1 miss alone: the issue
        // states plainly it expects the latency target to fail today (H1).
        // This is a measurement harness, not a regression gate yet —
        // exiting non-zero only for a hard failure (zero samples, meaning
        // the metric sanity check itself failed) keeps `echo $?` meaningful
        // without conflating "the number we expected to be red is red" with
        // "the harness itself is broken".
        if any_zero_samples {
            std::process::exit(1);
        }
        return;
    }

    if name == "throughput-ramp" {
        let duration = parse_flag(&args, "--duration-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(THROUGHPUT_RAMP_DEFAULT_DURATION);
        let grace = parse_flag(&args, "--grace-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(THROUGHPUT_RAMP_DEFAULT_GRACE);
        let rates = parse_rate_list(&args, "--rates")
            .unwrap_or_else(|| THROUGHPUT_RAMP_DEFAULT_RATES.to_vec());

        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let probes = runtime.block_on(streaming::b2_throughput_ramp::run_ramp(
            &rates, duration, grace,
        ));

        for probe in &probes {
            println!("{}", probe.to_json("throughput-ramp"));
        }
        match probes.iter().rev().find(|p| p.sustained) {
            Some(last_sustained) => eprintln!(
                "candidate saturation rate: {} rows/sec sustained (achieved {:.0} rows/sec, \
                 backlog {} after grace)",
                last_sustained.target_rows_per_sec,
                last_sustained.achieved_rows_per_sec,
                last_sustained.backlog_after_grace
            ),
            None => eprintln!(
                "no candidate rate sustained — even the lowest tested rate ({} rows/sec) left a \
                 backlog after the grace period",
                rates.first().copied().unwrap_or(0.0)
            ),
        }
        return;
    }

    if name == "intake-ceiling" {
        let rows_per_commit = parse_flag(&args, "--rows-per-commit")
            .map(|v| v as usize)
            .unwrap_or(INTAKE_CEILING_DEFAULT_ROWS_PER_COMMIT);
        let duration = parse_flag(&args, "--duration-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(INTAKE_CEILING_DEFAULT_DURATION);
        let grace = parse_flag(&args, "--grace-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(INTAKE_CEILING_DEFAULT_GRACE);

        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let result = runtime.block_on(streaming::e3_intake_ceiling::run(
            rows_per_commit,
            duration,
            grace,
        ));
        println!("{}", result.to_json());
        if result.append_backlog > 0 {
            eprintln!(
                "intake did not fully catch up within the grace period: {} rows offered, {} \
                 appended to the ring ({} behind) — the achieved append rate ({:.0} rows/sec) is \
                 a floor, not the true ceiling; rerun with a lower --rows-per-commit or a longer \
                 --grace-secs to pin it down more precisely",
                result.rows_offered,
                result.ring_rows_appended,
                result.append_backlog,
                result.append_achieved_rows_per_sec
            );
        }
        return;
    }

    if name == "seal-cadence-sweep" {
        let intervals_ms: Vec<u64> = parse_rate_list(&args, "--intervals-ms")
            .unwrap_or_else(|| SEAL_CADENCE_SWEEP_DEFAULT_INTERVALS_MS.to_vec())
            .into_iter()
            .map(|v| v as u64)
            .collect();
        let depths: Vec<usize> = parse_rate_list(&args, "--depths")
            .unwrap_or_else(|| SEAL_CADENCE_SWEEP_DEFAULT_DEPTHS.to_vec())
            .into_iter()
            .map(|v| v as usize)
            .collect();
        let rate = parse_flag(&args, "--rate")
            .map(|v| v as f64)
            .unwrap_or(SEAL_CADENCE_SWEEP_DEFAULT_RATE);
        let duration = parse_flag(&args, "--duration-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(SEAL_CADENCE_SWEEP_DEFAULT_DURATION);

        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let results = runtime.block_on(streaming::e2_seal_cadence_sweep::run_sweep(
            &intervals_ms,
            &depths,
            rate,
            duration,
        ));

        for result in &results {
            println!("{}", result.to_json());
        }
        return;
    }

    if name == "transaction-shape" {
        let shapes: Vec<usize> = parse_rate_list(&args, "--shapes")
            .unwrap_or_else(|| {
                streaming::b4_transaction_shape::DEFAULT_SHAPES
                    .iter()
                    .map(|&v| v as f64)
                    .collect()
            })
            .into_iter()
            .map(|v| v as usize)
            .collect();
        let target_rate = parse_flag(&args, "--target-rate")
            .map(|v| v as f64)
            .unwrap_or(TRANSACTION_SHAPE_DEFAULT_TARGET_RATE);
        let duration = parse_flag(&args, "--duration-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(TRANSACTION_SHAPE_DEFAULT_DURATION);
        let grace = parse_flag(&args, "--grace-secs")
            .map(|v| Duration::from_secs(v as u64))
            .unwrap_or(TRANSACTION_SHAPE_DEFAULT_GRACE);

        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let probes = runtime.block_on(streaming::b4_transaction_shape::run_sweep(
            &shapes,
            target_rate,
            duration,
            grace,
        ));
        for probe in &probes {
            println!("{}", probe.to_json("transaction-shape"));
        }
        return;
    }

    if name == "relationship-aggregate" {
        let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
        let result = runtime.block_on(scenario_relationship::run(
            "relationship-aggregate",
            RELATIONSHIP_AUTHORS,
            RELATIONSHIP_POSTS,
            RELATIONSHIP_COMMENTS,
            RELATIONSHIP_AGGREGATE_CEILING,
        ));
        println!("{}", result.to_json());
        let mut failed = false;
        if !result.within_ceiling {
            failed = true;
            eprintln!(
                "REGRESSION: {} install_definition took {}ms, over its {}ms ceiling",
                result.scenario, result.backfill_ms, result.ceiling_ms
            );
        }
        if !result.correctness_ok {
            failed = true;
            eprintln!(
                "CORRECTNESS FAILURE: {}'s backfilled author_totals did not match the oracle",
                result.scenario
            );
        }
        if failed {
            std::process::exit(1);
        }
        return;
    }

    let scenarios = match parse_args(&args, name) {
        Ok(scenarios) => scenarios,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };

    let runtime = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let mut any_over_ceiling = false;
    let mut any_incorrect = false;

    for scenario in scenarios {
        let result = runtime.block_on(scenario::run(
            scenario.name,
            scenario.n,
            scenario.g,
            scenario.ceiling,
        ));
        println!("{}", result.to_json());
        if !result.within_ceiling {
            any_over_ceiling = true;
            eprintln!(
                "REGRESSION: {} aggregate backfill took {}ms, over its {}ms ceiling",
                result.scenario, result.aggregate_backfill_ms, result.ceiling_ms
            );
        }
        if !result.correctness_ok {
            any_incorrect = true;
            eprintln!(
                "CORRECTNESS FAILURE: {}'s backfilled posts_totals did not match the oracle",
                result.scenario
            );
        }
    }

    if any_over_ceiling || any_incorrect {
        std::process::exit(1);
    }
}

/// Parses argv into the list of scenarios to run. `custom` reads
/// `--n`/`--g`/`--ceiling-secs` (all required); every other name is one of
/// the two fixed scenarios above, or `both` for both of them in sequence.
/// (`relationship-aggregate` is handled separately in `main` — it doesn't fit
/// this `n`/`g` shape.)
fn parse_args(args: &[String], name: &str) -> Result<Vec<Scenario>, String> {
    match name {
        "high-cardinality" => Ok(vec![HIGH_CARDINALITY]),
        "low-cardinality" => Ok(vec![LOW_CARDINALITY]),
        "both" => Ok(vec![HIGH_CARDINALITY, LOW_CARDINALITY]),
        "custom" => {
            let n = parse_flag(args, "--n")?;
            let g = parse_flag(args, "--g")?;
            let ceiling_secs = parse_flag(args, "--ceiling-secs")?;
            Ok(vec![Scenario {
                name: "custom",
                n,
                g,
                ceiling: Duration::from_secs(ceiling_secs as u64),
            }])
        }
        other => Err(format!(
            "unknown scenario {other:?} — expected one of: high-cardinality, low-cardinality, \
             both, relationship-aggregate, custom, hop-ladder, hop-latency, throughput-ramp, \
             intake-ceiling, seal-cadence-sweep, transaction-shape"
        )),
    }
}

fn parse_flag(args: &[String], flag: &str) -> Result<i64, String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .ok_or_else(|| format!("custom scenario requires {flag} <value>"))?
        .parse::<i64>()
        .map_err(|e| format!("{flag} must be an integer: {e}"))
}

/// Parses `--rates 1000,5000,10000` into an ascending list of `f64`s, or
/// `None` if `flag` wasn't passed at all (caller falls back to a default
/// list) — panics on a malformed value rather than silently dropping it.
fn parse_rate_list(args: &[String], flag: &str) -> Option<Vec<f64>> {
    let raw = args
        .iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))?;
    Some(
        raw.split(',')
            .map(|s| {
                s.trim()
                    .parse::<f64>()
                    .unwrap_or_else(|e| panic!("{flag} value {s:?} must be a number: {e}"))
            })
            .collect(),
    )
}
