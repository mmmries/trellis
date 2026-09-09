//! Throughput/latency harness for the `engine` crate.
//!
//! Issue #63's milestone 0: a deterministic, repeatable backfill benchmark
//! (`posts` -> `posts_calc` -> `posts_totals`) that becomes the yardstick
//! M2/M3/M4 are checked against. See `scenario` for the actual pipeline and
//! measurements, `generate` for the deterministic data shape.
//!
//! ## Running
//!
//! ```text
//! cargo run -p benchmark --release -- high-cardinality
//! cargo run -p benchmark --release -- low-cardinality
//! cargo run -p benchmark --release -- both
//! cargo run -p benchmark --release -- custom --n 200000 --g 500 --ceiling-secs 60
//! ```
//!
//! `--release` matters: this pushes 1M rows through a real Postgres
//! instance and a real CDC pipeline, and the debug-build overhead is large
//! enough to distort the numbers. Each scenario prints one line of JSON to
//! stdout (see [`scenario::BenchResult::to_json`]) and the process exits
//! non-zero if the aggregate-backfill phase exceeds its regression
//! ceiling — wire this into CI as `cargo run -p benchmark --release --
//! high-cardinality`.

mod generate;
mod scenario;

use std::time::Duration;

/// The issue's own poc baseline (measured on a different box, "poc cluster
/// :5430") reports ~1m50s for this shape post-M1; on this harness/box the
/// same post-M1 code measures ~25s (see the M0 PR description for the
/// full number) — box speed, not a mechanism difference, since this run's
/// `correctness_ok` matches the oracle exactly and takes the identical
/// bulk-recompute path the issue describes. 120s leaves ~4-5x headroom
/// over this box's measurement for CI/dev-machine jitter without hiding a
/// real regression; tighten once a CI-hardware baseline exists and once
/// M2/M3/M4 land.
const HIGH_CARDINALITY_CEILING: Duration = Duration::from_secs(120);

/// Measured on this harness/box at ~25s (see above) — within noise of the
/// high-cardinality shape today, because a from-scratch `create_definition`
/// backfill enumerates and stages its whole source table in one commit
/// (`intake::publication::enumerate_and_append`), which always lands in
/// exactly one ring segment regardless of `(n, g)`. #62's segment-repetition
/// multiplier this scenario is meant to stress needs *multiple* segments to
/// bite — that starts mattering once M3's key-range-chunked direct-build
/// (or any other multi-transaction backfill path) lands, at which point
/// this shape should start diverging sharply from high-cardinality's. Same
/// 120s ceiling for now; revisit once M2/M3 change the mechanism.
const LOW_CARDINALITY_CEILING: Duration = Duration::from_secs(120);

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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let scenarios = match parse_args(&args) {
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
fn parse_args(args: &[String]) -> Result<Vec<Scenario>, String> {
    let name = args.first().map(String::as_str).unwrap_or("both");
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
             both, custom"
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
