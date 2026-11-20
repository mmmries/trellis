//! The planted-bug sweep: runs a tier's seeded
//! cases once with no plant, as the baseline, then once per planted bug
//! (`trellis::dev::plant::Plant`), and judges whether the tier catches each.
//! Shared by the concurrent tier (`tests/concurrent_convergence.rs`,
//! `planted_bugs_are_caught`) and the `JOIN` tier (`tests/join_convergence.rs`,
//! `join_planted_bugs_are_caught`); `docs/generative-test-suite.md`, "The
//! baseline bar", says how a sweep is judged.
//!
//! A plant is armed per process by `TRELLIS_TEST_PLANT`, so [`sweep`] is
//! called by a test that re-runs its own binary, on itself alone, once per
//! plant: the parent judges, each child runs every case and prints one
//! [`SWEEP_LINE`] per case.

use crate::baseline_quarantine;
use trellis::dev::defs::ast::KeySpace;
use trellis::dev::plant::{PLANT_ENV, Plant};

/// Names the plants a sweep runs: `all`, `none` (the baseline alone), or a
/// comma-separated list of `trellis::dev::plant::Plant` names. Unset, a
/// sweep returns at once.
pub const PLANTS_ENV: &str = "GENERATIVE_PLANTS";
/// How many seeds a sweep draws its cases from (default [`PLANT_SEEDS`]).
pub const PLANT_SEEDS_ENV: &str = "GENERATIVE_PLANT_SEEDS";
/// How many cases a sweep draws from each seed (default [`PLANT_CASES`]).
pub const PLANT_CASES_ENV: &str = "GENERATIVE_PLANT_CASES";
/// Which of the concurrent tier's case shapes a sweep draws:
/// `cooling_key` (the default), `hot_key`, `mid_burst`, `steady_load` or
/// `cooling_backfill`. The `JOIN` tier has one.
pub const PLANT_TIER_ENV: &str = "GENERATIVE_PLANT_TIER";
/// Runs one case only, as `<seed>:<case>` (1-based), to look at a failure
/// the sweep reported. Each failing case's report heads are printed to
/// stderr.
pub const PLANT_ONLY_ENV: &str = "GENERATIVE_PLANT_ONLY";
/// Set by the sweep on each process it spawns: run the cases and print one
/// [`SWEEP_LINE`] per case, rather than spawn more processes. Such a process
/// exits 0 whatever its cases do: only the parent judges them, against the
/// baseline bar, so a repro loop that runs it directly reads its rows.
pub const PLANT_CHILD_ENV: &str = "GENERATIVE_PLANT_CHILD";
/// Prefixes each case's result on a sweep process's stdout.
pub const SWEEP_LINE: &str = "plant-sweep\t";
/// The default seed count.
pub const PLANT_SEEDS: u64 = 4;
/// The default case count per seed.
pub const PLANT_CASES: usize = 12;

/// `name`'s value, parsed, or `default` when it is unset.
///
/// # Panics
///
/// When it is set and doesn't parse.
pub fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|_| panic!("{name}={v:?} doesn't parse")),
        Err(_) => default,
    }
}

/// How a sweep judges a tier.
pub struct SweepTier {
    /// The tier's name, as the quarantine list names it.
    pub name: &'static str,
    /// Plants the tier is known not to catch yet. The sweep runs and reports
    /// them, but doesn't fail when they go uncaught; it says so when one is
    /// caught, so the entry can go.
    pub known_misses: &'static [&'static str],
    /// Plants the tier doesn't gate, each with why: another tier (or a
    /// deterministic pin) runs the shape it needs on purpose, and this one
    /// reaches it rarely or never. `GENERATIVE_PLANTS=all` leaves them out;
    /// named, they run and are reported, and the sweep never fails on them.
    pub not_gated: Vec<(&'static str, &'static str)>,
}

/// Which apply path a plant breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlantPath {
    /// A 1-1 target.
    OneToOne,
    /// A group-basis aggregate's page or build.
    GroupBasis,
    /// A group-basis aggregate, or a `JOIN`'s `P` and `M` cells: the
    /// group-basis writer's statements (`staging::group_basis`) that both
    /// share.
    GroupBasisWriter,
    /// A `JOIN`.
    Join,
    /// A 1-1 `JOIN` (ADR-0021): the 1-1 `JOIN` tier
    /// (`join_one_to_one_convergence.rs`) draws these, and no other tier does.
    /// `trellis`'s `join_star_families`, `join_star_joined_families` and
    /// `join_star_truncate` (and the build, quarantine and over-targets
    /// tests) pin them deterministically.
    OneToOneJoin,
    /// The target-mutation seam's feed to a definition reading another's
    /// target: the chained-reader tier (`join_chained_convergence.rs`)
    /// draws these, and no other tier does. `trellis`'s
    /// `join_one_to_one_over_targets`, `join_over_targets` and
    /// `group_basis_chained` pin them deterministically.
    Chain,
    /// A group-basis aggregate's `MIN`/`MAX` extremes (#1117: their probe,
    /// their basis, their GC and the absolute writes of them): only a
    /// definition with `MIN` or `MAX` reaches it, which only the concurrent
    /// tier's `min_max` shape draws.
    Extremes,
    /// Capture's ring key for a source read through a `KEY (...)` with a
    /// nullable column (ADR-0026): only the concurrent tier's `source_key`
    /// shape draws one, and it reaches every target over that source.
    SourceKey,
    /// `self_check`'s comparison of a group-basis aggregate's extremes with
    /// the source (its side 7). Only a tier that audits a `MIN`/`MAX`
    /// definition by `self_check` reaches it.
    SelfCheckExtremes,
    /// Capture of a source write into the ring, ahead of every definition's
    /// apply path: a target of any key space can go wrong. Only a drawn
    /// transaction (#2181) holds the snapshot it needs.
    Capture,
    /// The drain's isolation of a failing page (`staging::quarantine`): a
    /// page that no key fails alone, but one definition's share of it does.
    /// Only the held-key tier's isolation draw makes such a page. A plant
    /// here can stop any reader of the page, whatever its key space.
    Isolation,
    /// The drain's transaction-id comparisons past the first 32-bit epoch.
    /// Only the `db_admin` property's xid-aging action
    /// (`crate::model::DbAdminAction::WrapXids`) crosses the boundary, and
    /// it is no planted-bug sweep tier, so no tier gates these: every
    /// tier lists them in its `not_gated` with [`NO_XID_EPOCH`]. `trellis`'s sealing pin
    /// and `db_admin.rs`'s wrap pin gate them.
    XidEpoch,
}

/// Why no sweep tier gates a [`PlantPath::XidEpoch`] plant.
pub const NO_XID_EPOCH: &str = "no sweep tier crosses the 32-bit xid epoch; the db_admin \
     property's wrap action does, and staging's \
     sealing::a_batch_fenced_past_the_epoch_boundary_reads_its_rows and db_admin.rs's \
     a_wrap_across_the_epoch_boundary_with_work_in_flight_converges pin it";

/// The path `plant` breaks.
pub fn plant_path(plant: Plant) -> PlantPath {
    match plant {
        // The tombstone GC collects a 1-1 target's ledger, and a
        // plain 1-1 target takes the Re-derive build's chunks.
        Plant::StaleOneToOneWrite
        | Plant::EarlyTombstoneGc
        | Plant::ChunkWithoutEntryLock
        | Plant::HeldKeyUnlisted
        | Plant::HeldKeyDeleteApplies
        | Plant::O2oWhereApplyUnfiltered
        | Plant::O2oWhereNullAsTrue
        | Plant::O2oWhereBuildUnfiltered
        | Plant::O2oWhereNoTombstone
        | Plant::O2oWhereColumnNotImaged
        | Plant::FieldReleaseBeforeWiden => PlantPath::OneToOne,
        Plant::GbSkipGroupLock
        | Plant::GbBuildSnapshotBeforeLocks
        | Plant::GbGcIgnoresBuild
        | Plant::GbBuildNoHiddenBranch
        | Plant::GbBuildNoTargetBranch
        | Plant::GbBuildNoRingBranch
        | Plant::GbBuildNullKeyByEquality
        | Plant::GbBuildStragglerUnlocked
        | Plant::GbBuildLeafUnrouted
        | Plant::GbWhereOldImage
        | Plant::GbWhereNullAsTrue
        | Plant::GbWhereBuildUnfiltered
        | Plant::GbWhereFilteredNaming
        | Plant::GbWhereColumnNotImaged => PlantPath::GroupBasis,
        Plant::GbLsnNotVisibility | Plant::GbFoldSidesPerKey | Plant::GbEarlyZeroGc => {
            PlantPath::GroupBasisWriter
        }
        Plant::JoinNoJLock
        | Plant::JoinOtherSideBeforeJ
        | Plant::JoinDropFlip
        | Plant::JoinTSkipLock
        | Plant::JoinWhereMUnfiltered => PlantPath::Join,
        Plant::Join1NoJLock
        | Plant::Join1NoRecheck
        | Plant::Join1LsnOnly
        | Plant::Join1VisOnly
        | Plant::Join1NoEntryLock
        | Plant::Join1OldKeyUnlocked
        | Plant::Join1SnapshotAfterRead
        | Plant::Join1RdNoKey
        | Plant::Join1BrdNoKey
        | Plant::Join1NoPlaceholder
        | Plant::Join1MBeforeJ
        | Plant::Join1ChunkNoKey
        | Plant::Join1NoOrphanPass
        | Plant::Join1OrphanDelete
        | Plant::Join1MPassBOnly
        | Plant::Join1SweepStartBound
        | Plant::Join1TruncateNoFloor
        | Plant::Join1TruncateShareLock
        | Plant::Join1TruncateKeepsR
        | Plant::Join1TruncateKeepsM
        | Plant::Join1TruncateNoRebuild
        | Plant::Join1TruncateDefAfterLock
        | Plant::Join1FanOutHeldRow
        | Plant::Join1ReleaseNoRederive
        | Plant::Join1WhereFanOutNoDelete
        | Plant::Join1WhereDeletePresenceOnly
        | Plant::Join1WhereNullAsTrue
        | Plant::Join1WhereBuildUnfiltered
        | Plant::StarJUnsorted
        | Plant::StarRootSkipJLast
        | Plant::StarRootSkipJFirst
        | Plant::StarRootSkipJMiddle
        | Plant::StarLateSkipLast
        | Plant::StarLateSkipFirst
        | Plant::StarOwnJOnly
        | Plant::StarRAfterM
        | Plant::StarLastNoR
        | Plant::StarRSplit
        | Plant::StarRecheckJ
        | Plant::StarRecheckSkipLast
        | Plant::StarChunkNoR
        | Plant::StarTruncateKeepsOtherMarkers
        | Plant::StarChunkSkipJLast
        | Plant::StarChunkSkipJFirst => PlantPath::OneToOneJoin,
        Plant::SeamTokenEarly | Plant::SeamImagelessRow => PlantPath::Chain,
        Plant::GbxProbeNoXbasis
        | Plant::GbxSharedBasis
        | Plant::GbxProbeBeforeLock
        | Plant::GbxNullExtremeNoProbe
        | Plant::GbxNullKeyByEquality
        | Plant::GbxGcIgnoresXstamp
        | Plant::GbxAbsoluteWriteKeepsExtremes => PlantPath::Extremes,
        Plant::ScExtremesUnderBasis
        | Plant::ScExtremesUncompared
        | Plant::ScExtremeGroupsUncompared => PlantPath::SelfCheckExtremes,
        Plant::StarFenceFirstOnly
        | Plant::StarHeldOriginFirstOnly
        | Plant::StarDefLockFirstOnly
        | Plant::StarReadsFirstOnly => PlantPath::OneToOneJoin,
        Plant::CaptureSlotFromSnapshot => PlantPath::Capture,
        Plant::IsolationPausesEveryReader => PlantPath::Isolation,
        Plant::FenceXid32 => PlantPath::XidEpoch,
        Plant::NullKeyAsEmpty => PlantPath::SourceKey,
    }
}

/// Whether `plant` can make a target with `key_space` diverge: each plant
/// breaks one apply path, so a divergence in a target it never writes is an
/// unplanted failure, not a catch. The baseline reaches everything.
pub fn plant_reaches(plant: &str, key_space: &KeySpace) -> bool {
    let Some(plant) = Plant::from_name(plant) else {
        return true;
    };
    let aggregate = matches!(key_space, KeySpace::Aggregate { .. });
    let join = matches!(key_space, KeySpace::Join(_));
    match plant_path(plant) {
        PlantPath::OneToOne => !aggregate && !join,
        PlantPath::GroupBasis | PlantPath::Extremes | PlantPath::SelfCheckExtremes => aggregate,
        PlantPath::GroupBasisWriter => aggregate || join,
        PlantPath::Join => join,
        PlantPath::OneToOneJoin => matches!(key_space, KeySpace::JoinOneToOne(_)),
        // A reader of a target, a capture's, a failing page's or a keyed source can be
        // of any kind.
        PlantPath::Chain | PlantPath::Capture | PlantPath::Isolation | PlantPath::SourceKey => true,
        // Every target drains through the fenced read.
        PlantPath::XidEpoch => true,
    }
}

/// Whether `plant` can have caused a failure that diverged in targets with
/// these key spaces (`None` for a target the program doesn't define, which is
/// given the benefit of the doubt): any one of them it writes is enough.
/// A failure with no diverging target at all (an error or a quiesce
/// timeout) is reachable too.
pub fn divergence_reachable<'a>(
    plant: &str,
    key_spaces: impl IntoIterator<Item = Option<&'a KeySpace>>,
) -> bool {
    let mut key_spaces = key_spaces.into_iter().peekable();
    key_spaces.peek().is_none()
        || key_spaces.any(|key_space| key_space.is_none_or(|ks| plant_reaches(plant, ks)))
}

/// Seed `seed`'s first `cases` cases of `strategy`. The same seed draws the
/// same cases in every process, which is what lets a sweep judge each plant
/// against an unplanted baseline over exactly the cases the plant ran.
#[cfg(feature = "proptest")]
pub fn seeded_cases<T: std::fmt::Debug>(
    strategy: &proptest::strategy::BoxedStrategy<T>,
    seed: u64,
    cases: usize,
) -> Vec<T> {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&seed.to_le_bytes());
    }
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::from_seed(RngAlgorithm::ChaCha, &bytes),
    );
    (0..cases)
        .map(|_| {
            strategy
                .new_tree(&mut runner)
                .expect("the tier's strategy draws a case")
                .current()
        })
        .collect()
}

/// One case's result in a sweep, as a sweep process prints it.
#[derive(Debug)]
pub struct SweepCase {
    pub plant: String,
    pub seed: u64,
    pub case: usize,
    pub failed: bool,
    /// How many times the plant changed the engine's behavior in this case.
    pub fired: u64,
    /// Whether the plant can have caused the failure: any of the targets the
    /// case diverged in is one the plant writes ([`plant_reaches`]), or the
    /// case failed without diverging (an error or a quiesce timeout). `true`
    /// for a pass.
    pub reachable: bool,
    /// Whether the case failed by a quiesce or convergence timeout
    /// ([`is_quiesce_timeout`]). Such a case counts as caught when the plant
    /// fired, so a plant whose effect is a hang is credited; the report's
    /// "timed out" column says how much of "caught" that is.
    pub timed_out: bool,
    pub secs: f64,
    /// How many times the case reached each site it reached
    /// (`trellis::dev::plant::reached`), armed or not.
    pub reached: Vec<(String, u64)>,
    pub reason: String,
}

/// Whether a failure's report is a quiesce or convergence timeout: the
/// engine never settled, which says the plant hung it, not what it wrote.
pub fn is_quiesce_timeout(message: &str) -> bool {
    [
        "ConvergenceTimeout",
        "DefinitionsUnsettled",
        "PartitionsUnsettled",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

impl SweepCase {
    /// Whether this case caught its plant: it failed, the plant changed the
    /// engine's behavior in it, and the failure is one the plant can cause
    /// (`reachable`). A planted case that failed any other way hit an
    /// unplanted divergence, and says nothing about the plant. For the
    /// baseline, whether it failed.
    ///
    /// The baseline passing the same case doesn't rule out an unplanted
    /// failure in the planted run, since the schedule isn't replayed, only
    /// the case. The target check narrows that, and a plant can still be
    /// credited with an unplanted failure in a target it writes, at the
    /// baseline's rate. A quiesce timeout counts: it is a failure with no
    /// diverging target, so it is reachable, and a plant that hangs the
    /// engine is caught by the hang (`timed_out` counts them).
    pub fn caught(&self) -> bool {
        self.failed && (self.plant == "baseline" || (self.fired > 0 && self.reachable))
    }

    pub fn to_line(&self) -> String {
        format!(
            "{SWEEP_LINE}{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.1}\t{}\t{}",
            self.plant,
            self.seed,
            self.case,
            if self.failed { "FAIL" } else { "pass" },
            self.fired,
            self.reachable,
            self.timed_out,
            self.secs,
            if self.reached.is_empty() {
                "-".to_string()
            } else {
                self.reached
                    .iter()
                    .map(|(site, n)| format!("{site}={n}"))
                    .collect::<Vec<_>>()
                    .join(",")
            },
            self.reason
        )
    }

    pub fn from_line(line: &str) -> Option<SweepCase> {
        let mut fields = line.strip_prefix(SWEEP_LINE)?.splitn(10, '\t');
        let mut next = || fields.next().expect("a sweep line has 10 fields");
        Some(SweepCase {
            plant: next().to_string(),
            seed: next().parse().ok()?,
            case: next().parse().ok()?,
            failed: next() == "FAIL",
            fired: next().parse().ok()?,
            reachable: next().parse().ok()?,
            timed_out: next().parse().ok()?,
            secs: next().parse().ok()?,
            reached: match next() {
                "-" => Vec::new(),
                sites => sites
                    .split(',')
                    .map(|site| {
                        let (name, n) = site.split_once('=')?;
                        Some((name.to_string(), n.parse().ok()?))
                    })
                    .collect::<Option<_>>()?,
            },
            reason: next().to_string(),
        })
    }
}

/// How one case failed: the key space of every target that diverged (`None`
/// for a target the program doesn't define; empty when the case failed
/// without diverging), and the report.
pub struct CaseFailure {
    pub key_spaces: Vec<Option<KeySpace>>,
    pub message: String,
}

/// The name a sweep reports this process's plant under.
pub fn armed_name() -> &'static str {
    trellis::dev::plant::armed().map_or("baseline", |p| p.name())
}

/// Runs a tier's planted-bug sweep (see the module doc), from the test named
/// `test`, which must call this and nothing else first. `draw` gives a
/// seed's first cases; `run` runs one. In a sweep's child process it runs
/// every case and prints its rows; otherwise, with [`PLANTS_ENV`] set, it
/// spawns the children and judges them against the baseline bar and the
/// tier's gate, and panics on a miss.
pub fn sweep<C>(
    test: &str,
    tier: &SweepTier,
    draw: impl Fn(u64, usize) -> Vec<C>,
    run: impl FnMut(&C) -> Result<(), CaseFailure>,
) {
    if std::env::var_os(PLANT_CHILD_ENV).is_some() {
        run_cases(tier.name, draw, run);
        return;
    }
    let Ok(requested) = std::env::var(PLANTS_ENV) else {
        eprintln!("{test}: skipped, {PLANTS_ENV} is not set");
        return;
    };
    judge(test, tier, &requested);
}

/// A sweep process: runs every seed's cases against this process's plant
/// (or none, for the baseline), printing one [`SWEEP_LINE`] per case.
fn run_cases<C>(
    tier: &str,
    draw: impl Fn(u64, usize) -> Vec<C>,
    mut run: impl FnMut(&C) -> Result<(), CaseFailure>,
) {
    let seeds: u64 = env_or(PLANT_SEEDS_ENV, PLANT_SEEDS);
    let cases: usize = env_or(PLANT_CASES_ENV, PLANT_CASES);
    let plant = armed_name();
    let only: Option<(u64, usize)> = std::env::var(PLANT_ONLY_ENV).ok().map(|v| {
        let (seed, case) = v
            .split_once(':')
            .unwrap_or_else(|| panic!("{PLANT_ONLY_ENV}={v:?}: expected <seed>:<case>"));
        (
            seed.parse().expect("a seed number"),
            case.parse().expect("a case number"),
        )
    });
    for seed in 1..=seeds {
        for (index, case) in draw(seed, cases).iter().enumerate() {
            if only.is_some_and(|only| only != (seed, index + 1)) {
                continue;
            }
            let fired_before = trellis::dev::plant::fired();
            let reached_before = trellis::dev::plant::reached();
            let started = std::time::Instant::now();
            let result = run(case);
            let reachable = match &result {
                Err(failure) => {
                    divergence_reachable(plant, failure.key_spaces.iter().map(Option::as_ref))
                }
                Ok(()) => true,
            };
            let reason = match &result {
                Ok(()) => String::new(),
                Err(failure) => {
                    // The report's head names the wrong rows; the rest is
                    // the whole program.
                    let text = &failure.message;
                    let head: Vec<&str> = text.lines().take(24).collect();
                    eprintln!(
                        "({tier}) {plant} seed {seed} case {} failed:\n{}",
                        index + 1,
                        head.join("\n")
                    );
                    text.replace(['\t', '\n'], " ").chars().take(240).collect()
                }
            };
            let line = SweepCase {
                plant: plant.to_string(),
                seed,
                case: index + 1,
                failed: result.is_err(),
                fired: trellis::dev::plant::fired() - fired_before,
                reachable,
                timed_out: result
                    .as_ref()
                    .is_err_and(|f| is_quiesce_timeout(&f.message)),
                secs: started.elapsed().as_secs_f64(),
                reached: trellis::dev::plant::reached()
                    .into_iter()
                    .zip(reached_before)
                    .filter(|((_, after), (_, before))| after > before)
                    .map(|((site, after), (_, before))| (site.to_string(), after - before))
                    .collect(),
                reason,
            }
            .to_line();
            println!("{line}");
            eprintln!("({tier}) {line}");
        }
    }
}

/// The sweep's parent: the baseline and each requested plant in a process of
/// its own, then the verdict.
fn judge(test: &str, tier: &SweepTier, requested: &str) {
    let quarantine = baseline_quarantine::checked_in();
    let closed =
        baseline_quarantine::closed_entries(&quarantine, baseline_quarantine::github_issue_state)
            .unwrap_or_else(|e| {
                panic!("can't confirm generative/baseline-quarantine.txt is current: {e}")
            });
    assert!(
        closed.is_empty(),
        "generative/baseline-quarantine.txt lists cases whose issue is closed; remove each \
         entry, or if the case still fails, reopen or file its issue:\n{}",
        closed
            .iter()
            .map(|e| format!("  {e}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
    let plants: Vec<Plant> = if requested.trim() == "none" {
        Vec::new()
    } else if requested.trim() == "all" {
        for (name, why) in &tier.not_gated {
            eprintln!(
                "{test}: {name} not run, the {} tier doesn't gate it: {why}",
                tier.name
            );
        }
        Plant::ALL
            .iter()
            .copied()
            .filter(|p| !tier.not_gated.iter().any(|(name, _)| *name == p.name()))
            .collect()
    } else {
        requested
            .split(',')
            .map(|name| {
                Plant::from_name(name.trim())
                    .unwrap_or_else(|| panic!("{PLANTS_ENV}: {name:?} names no plant"))
            })
            .collect()
    };

    let exe = std::env::current_exe().expect("this test binary's path");
    let mut results: Vec<SweepCase> = Vec::new();
    for plant in std::iter::once(None).chain(plants.iter().copied().map(Some)) {
        let mut command = std::process::Command::new(&exe);
        command
            .args([
                test,
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PLANT_CHILD_ENV, "1")
            .stderr(std::process::Stdio::inherit());
        match plant {
            Some(plant) => command.env(PLANT_ENV, plant.name()),
            None => command.env_remove(PLANT_ENV),
        };
        let output = command.output().expect("spawn a sweep process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<SweepCase> = stdout.lines().filter_map(SweepCase::from_line).collect();
        assert!(
            output.status.success() && !lines.is_empty(),
            "the sweep process for {} failed ({}):\n{stdout}",
            plant.map_or("baseline", |p| p.name()),
            output.status
        );
        results.extend(lines);
    }

    // The baseline meets the bar only with no failure outside the list. A
    // case the baseline failed, or that the list names, says nothing about
    // any plant, so it is left out of every plant's count (the report says
    // which).
    let baseline: Vec<baseline_quarantine::BaselineCase> = results
        .iter()
        .filter(|r| r.plant == "baseline")
        .map(|r| baseline_quarantine::BaselineCase {
            seed: r.seed,
            case: r.case,
            failed: r.failed,
        })
        .collect();
    let verdict = baseline_quarantine::judge(&quarantine, tier.name, &baseline);
    let mut excluded: Vec<(u64, usize)> = verdict
        .unlisted
        .iter()
        .chain(&verdict.quarantined)
        .copied()
        .chain(
            quarantine
                .iter()
                .filter(|e| e.tier == tier.name)
                .map(|e| (e.seed, e.case)),
        )
        .collect();
    excluded.sort_unstable();
    excluded.dedup();
    let report = report(tier, &results, &excluded);
    eprintln!("{report}");
    for entry in &verdict.now_passing {
        eprintln!(
            "{test}: quarantined case {entry} passed every baseline run; once #{} is fixed, \
             remove its entry",
            entry.issue
        );
    }
    assert!(
        verdict.passes(),
        "the unplanted baseline failed {} case(s) the quarantine list doesn't name: {}. No \
         unknown failure is accepted: triage each into a filed bug (then list it in \
         generative/baseline-quarantine.txt) or a fixed harness defect (see the report \
         above; rerun one with {PLANT_ONLY_ENV}=<seed>:<case>)",
        verdict.unlisted.len(),
        verdict
            .unlisted
            .iter()
            .map(|(seed, case)| format!("{seed}:{case}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    eprintln!("{}", reach_report(tier, &results));
    let seeds: u64 = env_or(PLANT_SEEDS_ENV, PLANT_SEEDS);
    let cases: usize = env_or(PLANT_CASES_ENV, PLANT_CASES);
    if crate::reach_floor::judges(seeds, cases, std::env::var_os(PLANT_ONLY_ENV).is_none()) {
        let totals = baseline_reach(&results)
            .into_iter()
            .map(|(site, (_, total))| (site, total))
            .collect();
        let missed = crate::reach_floor::misses(tier, &totals);
        assert!(
            missed.is_empty(),
            "{}",
            crate::reach_floor::failure(tier, &missed)
        );
    } else {
        eprintln!(
            "{test}: reachability floor not judged: it holds on the default baseline \
             ({PLANT_SEEDS} seeds x {PLANT_CASES} cases) or more, every case run"
        );
    }
    let caught = |name: &str| {
        results
            .iter()
            .any(|r| r.plant == name && r.caught() && !excluded.contains(&(r.seed, r.case)))
    };
    for name in tier.known_misses.iter().filter(|name| caught(name)) {
        eprintln!("{test}: known miss {name} was caught; consider gating it");
    }
    let missed: Vec<&str> = plants
        .iter()
        .map(|p| p.name())
        .filter(|name| !tier.known_misses.contains(name) && !caught(name))
        .filter(|name| !tier.not_gated.iter().any(|(not, _)| not == name))
        .collect();
    assert!(
        missed.is_empty(),
        "the {} tier never caught {missed:?} (see the report above)",
        tier.name
    );
}

/// The sweep's summary: per plant, how many cases it was caught in, how many
/// of those failed by timeout, how many cases it fired in, and each seed's cases-to-first-catch.
/// `excluded` are the cases the baseline failed or the quarantine list
/// names, left out of every plant's row.
fn report(tier: &SweepTier, results: &[SweepCase], excluded: &[(u64, usize)]) -> String {
    use std::collections::HashMap;
    use std::fmt::Write;
    let mut plants: Vec<&str> = Vec::new();
    for r in results {
        if !plants.contains(&r.plant.as_str()) {
            plants.push(&r.plant);
        }
    }
    let mut out = format!(
        "planted-bug sweep ({} tier; plant rows leave out the {} case(s) the baseline \
         failed or the quarantine list names):\n\
         plant | caught | timed out | fired in | failed, not attributable | cases to first catch, per seed | \
         mean case secs\n",
        tier.name,
        excluded.len()
    );
    for plant in plants {
        let rows: Vec<&SweepCase> = results
            .iter()
            .filter(|r| {
                r.plant == plant && (plant == "baseline" || !excluded.contains(&(r.seed, r.case)))
            })
            .collect();
        let caught = rows.iter().filter(|r| r.caught()).count();
        // The caught cases that failed by timeout: liveness, not a wrong row.
        let timed_out = rows.iter().filter(|r| r.caught() && r.timed_out).count();
        let fired = rows.iter().filter(|r| r.fired > 0).count();
        // Failures a plant can't have caused: see `SweepCase::caught`.
        let unattributable = rows.iter().filter(|r| r.failed && !r.caught()).count();
        let mut seeds: Vec<u64> = rows.iter().map(|r| r.seed).collect();
        seeds.dedup();
        let firsts: Vec<String> = seeds
            .iter()
            .map(|&seed| {
                let of_seed: Vec<&&SweepCase> = rows.iter().filter(|r| r.seed == seed).collect();
                of_seed
                    .iter()
                    .find(|r| r.caught())
                    .map_or(format!(">{}", of_seed.len()), |r| r.case.to_string())
            })
            .collect();
        let secs = rows.iter().map(|r| r.secs).sum::<f64>() / rows.len().max(1) as f64;
        let known = if tier.known_misses.contains(&plant) {
            " (known miss)"
        } else if tier.not_gated.iter().any(|(name, _)| *name == plant) {
            " (not gated here)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "{plant}{known} | {caught}/{} | {timed_out} | {fired}/{} | {unattributable} | {} | {secs:.1}",
            rows.len(),
            rows.len(),
            firsts.join(", ")
        );
    }
    // The first few failures per plant, to say what the tier saw.
    let mut listed: HashMap<&str, usize> = HashMap::new();
    for r in results.iter().filter(|r| r.failed) {
        let count = listed.entry(r.plant.as_str()).or_default();
        *count += 1;
        if *count > 3 {
            continue;
        }
        let _ = writeln!(
            out,
            "  {} seed {} case {} ({}): {}",
            r.plant,
            r.seed,
            r.case,
            if r.caught() {
                "caught"
            } else if r.fired == 0 {
                "plant never fired"
            } else {
                "in a target the plant doesn't write"
            },
            r.reason
        );
    }
    out
}

/// Each site's reach over the baseline's cases: how many cases reached it
/// and how often in all, keyed by site name.
pub fn baseline_reach(results: &[SweepCase]) -> std::collections::BTreeMap<String, (usize, u64)> {
    let mut reach = std::collections::BTreeMap::new();
    for r in results.iter().filter(|r| r.plant == "baseline") {
        for (site, n) in &r.reached {
            let (cases, total) = reach.entry(site.clone()).or_insert((0, 0));
            *cases += 1;
            *total += n;
        }
    }
    reach
}

/// The reachability report: every site (`trellis::dev::plant::sites`),
/// with the baseline's cases that reached it and its reaches in all, and
/// what the floor makes of it (`crate::reach_floor`).
pub fn reach_report(tier: &SweepTier, results: &[SweepCase]) -> String {
    use std::fmt::Write;
    let reach = baseline_reach(results);
    let cases = results.iter().filter(|r| r.plant == "baseline").count();
    let mut out = format!(
        "reachability ({} tier, {cases} baseline cases):\n\
         site | cases reached | reaches | floor\n",
        tier.name
    );
    for site in trellis::dev::plant::sites() {
        let (reached, total) = reach.get(site).copied().unwrap_or((0, 0));
        let ungated = tier.not_gated.iter().any(|(name, _)| *name == site);
        let floor = if ungated && !crate::reach_floor::reached_ungated(tier.name, site) {
            "not gated here"
        } else if crate::reach_floor::exemption(tier.name, site).is_some() {
            if reached == 0 {
                "exempt"
            } else {
                "exempt, but reached"
            }
        } else if reached == 0 {
            "MISSED"
        } else if ungated {
            "held, not gated"
        } else {
            "held"
        };
        // A pass count shows only that the path ran.
        let passes = if trellis::dev::plant::counts_passes(site) {
            " (passes)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "{site} | {reached}/{cases} | {total} | {floor}{passes}"
        );
    }
    let suggested = crate::reach_floor::suggested_reached_ungated(
        tier,
        cases,
        &reach
            .iter()
            .map(|(site, (cases, _))| (site.clone(), *cases))
            .collect(),
    );
    let listed: Vec<&str> = crate::reach_floor::REACHED_UNGATED
        .iter()
        .find(|(name, _)| *name == tier.name)
        .map_or(Vec::new(), |(_, sites)| sites.to_vec());
    let mut sorted = suggested.clone();
    sorted.sort_unstable();
    let mut now = listed;
    now.sort_unstable();
    if sorted != now {
        let _ = writeln!(
            out,
            "REACHED_UNGATED's entry for this run would be (ungated sites reached in a \
             quarter of the cases; it differs from the list, which is updated only on \
             purpose): (\"{}\", &{suggested:?}),",
            tier.name
        );
    }
    out
}

/// Why a tier other than the held-key tier leaves out a
/// [`PlantPath::Isolation`] plant.
pub const NO_ISOLATION: &str = "it breaks the drain's isolation of a page that fails only \
     in combination, which only the held-key tier's isolation draw makes (held_keys.rs, \
     held_key_planted_bugs_are_caught)";

/// Why a tier whose programs define no `MIN`/`MAX` doesn't run the
/// extremes' plants ([`PlantPath::Extremes`], [`PlantPath::SelfCheckExtremes`]).
pub const NO_EXTREMES: &str = "it breaks a MIN/MAX extreme of a group-basis aggregate, which \
     only the concurrent tier's min_max shape (concurrent_convergence.rs, \
     GENERATIVE_PLANT_TIER=min_max) and the tampering tier (self_check_cross_check.rs, \
     extremes_tamper_planted_bugs_are_caught) draw";

/// Why a tier other than the hot-key-based concurrent tiers doesn't gate a
/// capture plant.
pub const NO_CAPTURE: &str = "it breaks capture under a drawn transaction (#2181), which \
     every concurrent tier draws; the cooling-key tier (concurrent_convergence.rs, \
     planted_bugs_are_caught) gates it";

/// Why a tier that draws no table read through a nullable `KEY (...)`, or
/// draws one only now and then, doesn't run its plants
/// ([`PlantPath::SourceKey`]).
pub const NO_SOURCE_KEY: &str = "it breaks capture's key for a table read through a nullable \
     KEY (...), which the concurrent tier's source_key shape (concurrent_convergence.rs, \
     GENERATIVE_PLANT_TIER=source_key) and the 1-1 JOIN tier's joined_key sweep \
     (join_one_to_one_convergence.rs, joined_key_planted_bugs_are_caught) draw in every case";

/// The plants whose path a tier's programs never take, each with `why`:
/// for a tier's `not_gated`.
pub fn not_on_path(
    paths: &[PlantPath],
    why: &'static str,
) -> impl Iterator<Item = (&'static str, &'static str)> {
    let paths = paths.to_vec();
    Plant::ALL
        .iter()
        .filter(move |p| paths.contains(&plant_path(**p)))
        .map(move |p| (p.name(), why))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(plant: &str, case: usize, failed: bool, timed_out: bool) -> SweepCase {
        SweepCase {
            plant: plant.to_string(),
            seed: 1,
            case,
            failed,
            fired: 1,
            reachable: true,
            timed_out,
            secs: 1.0,
            reached: Vec::new(),
            reason: String::new(),
        }
    }

    #[test]
    fn a_timed_out_case_counts_in_caught_and_in_timed_out() {
        let tier = SweepTier {
            name: "test",
            known_misses: &[],
            not_gated: Vec::new(),
        };
        let results = [
            case("p", 1, true, true),
            case("p", 2, true, false),
            case("p", 3, false, false),
        ];
        assert!(results[0].caught() && results[1].caught());
        let out = report(&tier, &results, &[]);
        assert!(out.contains("timed out"), "{out}");
        assert!(out.contains("p | 2/3 | 1 | 3/3 | 0 |"), "{out}");
        let line = results[0].to_line();
        assert!(SweepCase::from_line(&line).unwrap().timed_out);
    }

    /// A row's reach survives its line, and the report counts the
    /// baseline's reaches per site with what the floor makes of each.
    #[test]
    fn reach_round_trips_and_is_reported_per_site() {
        let tier = SweepTier {
            name: "cooling_key",
            known_misses: &[],
            not_gated: vec![("early_tombstone_gc", "not gated")],
        };
        let mut reached = case("baseline", 1, false, false);
        reached.reached = vec![("stale_one_to_one_write".into(), 3)];
        reached.reason = "a reason".into();
        let back = SweepCase::from_line(&reached.to_line()).unwrap();
        assert_eq!(back.reached, reached.reached);
        assert_eq!(back.reason, "a reason");
        let none = case("baseline", 2, false, false);
        assert!(
            SweepCase::from_line(&none.to_line())
                .unwrap()
                .reached
                .is_empty()
        );

        let mut planted = case("stale_one_to_one_write", 1, false, false);
        planted.reached = vec![("chunk_without_entry_lock".into(), 5)];
        let out = reach_report(&tier, &[reached, none, planted]);
        assert!(
            out.contains("stale_one_to_one_write | 1/2 | 3 | held"),
            "{out}"
        );
        assert!(
            out.contains("early_tombstone_gc | 0/2 | 0 | not gated here"),
            "{out}"
        );
        assert!(
            out.contains("chunk_without_entry_lock | 0/2 | 0 | MISSED"),
            "{out}"
        );
        assert!(out.contains("capture_widen | 0/2 | 0 | exempt"), "{out}");
    }

    #[test]
    fn quiesce_timeouts_are_told_from_other_failures() {
        assert!(is_quiesce_timeout(
            "Quiesce(Staging(ConvergenceTimeout { .. }))"
        ));
        assert!(!is_quiesce_timeout("concurrent case diverged in the burst"));
    }

    #[test]
    fn a_plants_divergence_is_credited_behind_an_unplanted_one() {
        let group_by = KeySpace::Aggregate {
            group_by: Vec::new(),
        };
        let one_to_one = KeySpace::OneToOne;
        let plant = "stale_one_to_one_write";
        // An unplanted `GROUP BY` divergence first in definition order must
        // not hide the plant's own 1-1 divergence behind it.
        assert!(divergence_reachable(
            plant,
            [Some(&group_by), Some(&one_to_one)]
        ));
        assert!(!divergence_reachable(plant, [Some(&group_by)]));
        assert!(divergence_reachable(plant, std::iter::empty()));
        assert!(divergence_reachable(plant, [None]));
    }

    #[test]
    fn group_basis_plants_reach_aggregates_and_join_plants_reach_joins() {
        let group_by = KeySpace::Aggregate {
            group_by: Vec::new(),
        };
        let one_to_one = KeySpace::OneToOne;
        for plant in Plant::ALL {
            let name = plant.name();
            match plant_path(*plant) {
                PlantPath::GroupBasis
                | PlantPath::GroupBasisWriter
                | PlantPath::Extremes
                | PlantPath::SelfCheckExtremes => {
                    assert!(plant_reaches(name, &group_by), "{name}");
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                }
                PlantPath::Join => {
                    assert!(!plant_reaches(name, &group_by), "{name}");
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                }
                PlantPath::OneToOne => {
                    assert!(plant_reaches(name, &one_to_one), "{name}");
                    assert!(!plant_reaches(name, &group_by), "{name}");
                }
                PlantPath::OneToOneJoin => {
                    assert!(!plant_reaches(name, &one_to_one), "{name}");
                    assert!(!plant_reaches(name, &group_by), "{name}");
                }
                PlantPath::Chain
                | PlantPath::Capture
                | PlantPath::Isolation
                | PlantPath::XidEpoch
                | PlantPath::SourceKey => {
                    assert!(plant_reaches(name, &one_to_one), "{name}");
                    assert!(plant_reaches(name, &group_by), "{name}");
                }
            }
        }
    }
}
