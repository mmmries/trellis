//! The held-key tier (`docs/generative-test-suite.md` §10, "The oracle and
//! erroring rows"): programs that draw erroring rows on purpose
//! ([`held_key_program`], [`held_key_fuse_program`]), driven one op at a time through
//! [`run_convergence_with_held_keys`], whose oracle models what the engine
//! does with a held key and checks what `status()` and `sample_quarantined`
//! report. Shares one cluster per test thread, like `convergence.rs`.
//!
//! Its isolation family ([`isolation_program`], #2185) adds a definition
//! made to fail only in combination, beside its sibling over the same table,
//! and checks the #2130 invariant through
//! [`run_convergence_with_isolation`]: the failing definition pauses with
//! T0247, every other one keeps converging, and the ring retires.

use generative::backend::ManualBackend;
use generative::generate::held_keys::{DivisorSlot, raising_shape};
use generative::generate::isolation::{
    FailKind, IsoWrite, IsolationCase, IsolationParams, isolation_program, with_isolation,
};
use generative::generate::{
    Mutate, TableSpec, build_program_multi_with_derived, held_key_fuse_program, held_key_program,
};
use generative::model::Program;
use generative::plant_sweep::{self, SweepTier};
use generative::run::{
    HeldKeySummary, RunError, TRANSFORM_FUSE, run_convergence_with_held_keys,
    run_convergence_with_isolation,
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, FileFailurePersistence, TestCaseError};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::defs::ast::{Expr, FieldDef};
use trellis::dev::pool::Pool;

struct Harness {
    runtime: tokio::runtime::Runtime,
    cluster: TestCluster,
    /// Cases run, cases that held a key, keys held, definitions quarantined,
    /// definitions paused for failing alone (T0247).
    reach: std::cell::Cell<(usize, usize, usize, usize, usize)>,
}

impl Drop for Harness {
    /// Prints what the thread's cases reached. Print-only: this can run
    /// during a failing case's unwind.
    fn drop(&mut self) {
        let (cases, holding, held, quarantined, paused_alone) = self.reach.get();
        eprintln!(
            "generative: held-key reach: {cases} case(s), {holding} held a key, \
             {held} key(s) held, {quarantined} definition(s) quarantined, \
             {paused_alone} paused failing alone"
        );
    }
}

thread_local! {
    static HARNESS: Harness = Harness {
        runtime: tokio::runtime::Runtime::new().expect("build tokio runtime"),
        cluster: TestCluster::start().long_lived(),
        reach: std::cell::Cell::new((0, 0, 0, 0, 0)),
    };
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    ProptestConfig {
        cases,
        max_shrink_time: generative::shrink::max_shrink_time_ms(),
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        ..ProptestConfig::default()
    }
}

/// A held-key case: a plain or fuse program, or an isolation case.
#[derive(Debug, Clone)]
enum HeldCase {
    Program(Program),
    Isolation(IsolationCase),
}

impl HeldCase {
    fn program(&self) -> &Program {
        match self {
            HeldCase::Program(program) => program,
            HeldCase::Isolation(case) => &case.program,
        }
    }
}

/// Drives `program` against a fresh isolated database of `cluster`.
async fn run(cluster: &TestCluster, program: &Program) -> Result<HeldKeySummary, String> {
    run_case(cluster, &HeldCase::Program(program.clone())).await
}

/// Drives `case` against a fresh isolated database of `cluster`.
async fn run_case(cluster: &TestCluster, case: &HeldCase) -> Result<HeldKeySummary, String> {
    let db = cluster.create_isolated_database().await;
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let result = match case {
        HeldCase::Program(program) => {
            run_convergence_with_held_keys(&mut backend, &pool, program).await
        }
        HeldCase::Isolation(case) => {
            run_convergence_with_isolation(&mut backend, &pool, case).await
        }
    };
    match result {
        Ok(summary) => Ok(summary),
        Err(RunError::Diverged(d)) => Err(format!(
            "held-key run diverged after op {} (target {}):\n{}",
            d.op_index, d.def_target, d.report
        )),
        Err(RunError::HeldKeys {
            op_index,
            target,
            expected,
            reported,
            program,
        }) => Err(format!(
            "after op {op_index}, {target} reported {reported}; the model expects {expected}\n\
             program:\n{program}"
        )),
        Err(RunError::Isolation {
            op_index,
            target,
            problem,
            program,
        }) => Err(format!(
            "isolation broke after op {op_index} ({target}): {problem}\nprogram:\n{program}"
        )),
        Err(RunError::StatusLies { op_index, state }) => Err(format!(
            "status lies after op {op_index}: {state} reports live, but its target isn't \
             exact\nprogram:\n{:#?}",
            case.program()
        )),
        Err(other) => Err(format!("run error: {other:?}")),
    }
}

fn run_one(program: &Program) -> Result<HeldKeySummary, TestCaseError> {
    run_held(&HeldCase::Program(program.clone()))
}

fn run_held(case: &HeldCase) -> Result<HeldKeySummary, TestCaseError> {
    HARNESS.with(|h| {
        let summary = h
            .runtime
            .block_on(run_case(&h.cluster, case))
            .map_err(TestCaseError::fail)?;
        let (cases, holding, held, quarantined, paused_alone) = h.reach.get();
        h.reach.set((
            cases + 1,
            holding + usize::from(summary.held > 0),
            held + summary.held,
            quarantined + summary.quarantined,
            paused_alone + summary.paused_alone,
        ));
        Ok(summary)
    })
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_held_keys_follow_the_model(program in held_key_program()) {
        run_one(&program)?;
    }

    /// Every case trips the whole-transform fuse: the generator draws
    /// [`TRANSFORM_FUSE`] raising seeds or more.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_the_fuse_quarantines_as_modelled(program in held_key_fuse_program()) {
        let summary = run_one(&program)?;
        prop_assert_eq!(summary.quarantined, 1, "the fuse never tripped");
    }

    /// One definition never stalls another (#2130, #2185): every case's
    /// failing definition pauses with T0247, its sibling over the same rows
    /// and every other definition keep converging, and the ring retires.
    #[test]
    #[ignore = "deep-lane property: run with `cargo test -p generative -- --ignored`"]
    fn property_a_definition_failing_in_combination_stalls_no_other(case in isolation_program()) {
        let summary = run_held(&HeldCase::Isolation(case))?;
        prop_assert_eq!(summary.paused_alone, 1, "the pair paused nothing");
    }
}

/// The held-key tier's sweep: it gates the plants that need a held key
/// ([`generative::plant_sweep::PlantPath::OneToOne`]'s `held_key_*`) and
/// the isolation plant ([`generative::plant_sweep::PlantPath::Isolation`]),
/// and leaves out every other plant, which the tiers whose programs take
/// its path gate. Its cases are [`held_key_program`]'s and
/// [`held_key_fuse_program`]'s, alternately, then half as many
/// [`isolation_program`] cases.
fn held_key_sweep_tier() -> SweepTier {
    use trellis::dev::plant::Plant;
    SweepTier {
        name: generative::baseline_quarantine::HELD_KEYS_TIER,
        known_misses: HELD_KEY_KNOWN_MISSES,
        not_gated: Plant::ALL
            .iter()
            .filter(|plant| !HELD_KEY_PLANTS.contains(&plant.name()))
            .map(|plant| {
                (
                    plant.name(),
                    "it breaks no held key's apply or report; the tier whose programs take \
                     its path gates it",
                )
            })
            .collect(),
    }
}

/// The held-key plants the tier runs but rarely catches: none. Of the 48
/// cases of seeds 1 to 4 (logbook `20261116T201534-e-sweep`),
/// `held_key_unlisted` was caught in 46 and `held_key_delete_applies` in 11,
/// each within its seed's first five cases.
const HELD_KEY_KNOWN_MISSES: &[&str] = &[];

/// The plants the held-key tier gates.
const HELD_KEY_PLANTS: &[&str] = &[
    "held_key_unlisted",
    "held_key_delete_applies",
    "isolation_pauses_every_reader",
];

/// Shows the held-key tier catches each held-key plant, and how often: the
/// planted-bug sweep (`generative::plant_sweep`) over seeded cases of both
/// families. Returns at once without `GENERATIVE_PLANTS`. Run it with:
///
/// ```text
/// GENERATIVE_PLANTS=all cargo test -p generative --test held_keys \
///     held_key_planted_bugs_are_caught -- --ignored --nocapture
/// ```
#[test]
#[ignore = "planted-bug sweep: run with GENERATIVE_PLANTS=all and `--ignored`"]
fn held_key_planted_bugs_are_caught() {
    let plain = held_key_program().boxed();
    let fuse = held_key_fuse_program().boxed();
    let isolation = isolation_program().boxed();
    plant_sweep::sweep(
        "held_key_planted_bugs_are_caught",
        &held_key_sweep_tier(),
        |seed, cases| {
            let plain = plant_sweep::seeded_cases(&plain, seed, cases.div_ceil(2));
            let fuse = plant_sweep::seeded_cases(&fuse, seed, cases / 2);
            let isolation = plant_sweep::seeded_cases(&isolation, seed, cases.div_ceil(2));
            let mut fuse = fuse.into_iter();
            plain
                .into_iter()
                .flat_map(|case| std::iter::once(case).chain(fuse.next()))
                .map(HeldCase::Program)
                .chain(isolation.into_iter().map(HeldCase::Isolation))
                .collect()
        },
        |case: &HeldCase| {
            run_held(case)
                .map(|_| ())
                .map_err(|err| plant_sweep::CaseFailure {
                    key_spaces: case
                        .program()
                        .defs
                        .iter()
                        .map(|def| Some(def.key_space.clone()))
                        .collect(),
                    message: err.to_string(),
                })
        },
    );
}

/// The tier runs exactly the held-key plants, each on the 1-1 path, and
/// the isolation plants.
#[test]
fn the_held_key_tier_runs_every_held_key_plant() {
    use generative::plant_sweep::{PlantPath, plant_path};
    use trellis::dev::plant::Plant;
    let tier = held_key_sweep_tier();
    for name in HELD_KEY_PLANTS {
        let plant = Plant::from_name(name).unwrap_or_else(|| panic!("{name} names no plant"));
        let path = plant_path(plant);
        assert_eq!(
            path,
            if name.starts_with("isolation_") {
                PlantPath::Isolation
            } else {
                PlantPath::OneToOne
            },
            "{name}"
        );
    }
    for plant in Plant::ALL {
        let name = plant.name();
        let gated = !tier.not_gated.iter().any(|(not, _)| *not == name);
        assert_eq!(gated, HELD_KEY_PLANTS.contains(&name), "{name}");
    }
    for name in tier.known_misses {
        assert!(HELD_KEY_PLANTS.contains(name), "{name}");
    }
}

/// One table `t0` with `seeds` (`c1`, `c2`) and `mutates`, read by one plain
/// 1-1 definition whose derived field is `c1 / (c2 - 7)`.
fn one_raising_definition(seeds: &[(i64, i64)], mutates: Vec<Mutate>) -> Program {
    let count = seeds.len();
    build_program_multi_with_derived(
        &[TableSpec {
            seed_values: seeds.iter().map(|(a, b)| (Some(*a), Some(*b))).collect(),
            text_values: vec![Some("x".to_string()); count],
            bool_values: vec![Some("true".to_string()); count],
            uuid_values: vec![Some("123e4567-e89b-42d3-a456-426614174000".to_string()); count],
            grain_values: vec![Some("0".to_string()); count],
            mutates,
        }],
        &[0],
        &[raising_shape(
            Expr::Column("$c1".to_string()),
            DivisorSlot::C2,
            7,
        )],
    )
}

/// A row that raises on insert is held with no target row; one that raises
/// on update keeps its old row through a fix at the source, a delete and a
/// revival. A truncate clears the target, held keys' rows with it, and the
/// keys stay held: a held key inserted again stays out, another applies.
#[tokio::test(flavor = "multi_thread")]
async fn erroring_inserts_and_updates_follow_the_held_key_model() {
    let cluster = TestCluster::start();
    let program = one_raising_definition(
        &[(1, 7), (2, 3), (3, 4)],
        vec![
            Mutate::Update {
                pk: 2,
                c1: Some(5),
                c2: Some(7),
            },
            Mutate::Update {
                pk: 1,
                c1: Some(1),
                c2: Some(1),
            },
            Mutate::Delete { pk: 2 },
            Mutate::DuplicateInsert {
                pk: 2,
                c1: Some(2),
                c2: Some(2),
            },
            Mutate::Truncate,
            Mutate::DuplicateInsert {
                pk: 2,
                c1: Some(3),
                c2: Some(3),
            },
            Mutate::DuplicateInsert {
                pk: 3,
                c1: Some(3),
                c2: Some(3),
            },
        ],
    );
    let summary = run(&cluster, &program)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(summary.held, 2);
    assert_eq!(summary.quarantined, 0);
}

/// Five raising rows across two fields, at most four on either, trip the
/// whole-transform fuse: the definition goes `quarantined` with five held
/// keys, and its target holds what it had through a later update and a
/// truncate.
#[tokio::test(flavor = "multi_thread")]
async fn five_held_keys_quarantine_the_definition() {
    let cluster = TestCluster::start();
    let mut program = one_raising_definition(
        &[(1, 7), (2, 7), (3, 7), (7, 1), (7, 2), (4, 4), (5, 5)],
        vec![
            Mutate::Update {
                pk: 6,
                c1: Some(9),
                c2: Some(9),
            },
            Mutate::Truncate,
        ],
    );
    let columns: Vec<String> = program.tables[0].columns[1..=4]
        .iter()
        .map(|c| c.name.clone())
        .collect();
    program.defs[0].fields.push(FieldDef {
        name: "derived_b".to_string(),
        expr: raising_shape(Expr::Column("$c2".to_string()), DivisorSlot::C1, 7).build_expr(
            &columns[0],
            &columns[1],
            &columns[2],
            &columns[3],
        ),
    });
    let summary = run(&cluster, &program)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(summary.held, TRANSFORM_FUSE);
    assert_eq!(summary.quarantined, 1);
}

/// [`one_raising_definition`] (`c1 / (c2 - 7)` over `t0`, holding key 1)
/// with the isolation table, its `kind` of pair after two seeds and an
/// update, and one write after it.
fn isolation_case(kind: FailKind, sibling_aggregate: bool) -> IsolationCase {
    let program = one_raising_definition(
        &[(1, 7), (2, 3)],
        vec![Mutate::Update {
            pk: 2,
            c1: Some(5),
            c2: Some(4),
        }],
    );
    with_isolation(
        program,
        &IsolationParams {
            kind,
            sibling_aggregate,
            seeds: 2,
            before: vec![IsoWrite::Update(0)],
            after: vec![IsoWrite::Insert, IsoWrite::Update(1)],
            pair_group: 1,
            picks: vec![0, 1, 2, 3, 4],
            slots: vec![0, 1, 2, 2, 3, 3],
        },
    )
}

/// Runs `kind`'s pinned case: its failing definition pauses with T0247,
/// the held key stays held, and everything else converges.
async fn pin_isolation(kind: FailKind, sibling_aggregate: bool) {
    let cluster = TestCluster::start();
    let case = isolation_case(kind, sibling_aggregate);
    let summary = run_case(&cluster, &HeldCase::Isolation(case))
        .await
        .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(summary.paused_alone, 1);
    assert_eq!(summary.held, 1, "t0's key 1 raises alone and is held");
}

/// #2130's repro, as the isolation run checks it: two `1e308` rows of one
/// group, in one statement, overflow `iso_d`'s `SUM` only together. It
/// pauses with T0247 naming both keys, its target keeps what it had, and
/// `iso_s`, a `COUNT(*)` over the same rows, follows every write.
#[tokio::test(flavor = "multi_thread")]
async fn a_sum_overflowing_in_one_page_pauses_only_its_definition() {
    pin_isolation(FailKind::FloatSum, true).await;
}

/// The `interval` variant: `'infinity'` and `'-infinity'` in one group
/// (Postgres 17+; on an older server the case draws the float pair).
#[tokio::test(flavor = "multi_thread")]
async fn an_interval_sum_out_of_range_in_one_page_pauses_only_its_definition() {
    pin_isolation(FailKind::IntervalInfinities, false).await;
}

/// A unique index on `iso_d`'s target that the pair's two rows share a
/// value of (`23505`).
#[tokio::test(flavor = "multi_thread")]
async fn a_unique_target_refusing_two_rows_pauses_only_its_definition() {
    pin_isolation(FailKind::UniqueTarget, false).await;
}

/// A trigger on `iso_d`'s target that raises on a second row of one value
/// (`P0001`).
#[tokio::test(flavor = "multi_thread")]
async fn a_target_trigger_refusing_two_rows_pauses_only_its_definition() {
    pin_isolation(FailKind::TriggerTarget, true).await;
}
