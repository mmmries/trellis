//! The reachability floor: every plant site, and every reach marker
//! (`trellis::dev::plant::sites`), must be reached in a tier's baseline
//! unless the tier doesn't gate it (`SweepTier::not_gated`, which says why)
//! or [`EXEMPT`] names it for the tier, with why. The planted-bug sweep
//! (`crate::plant_sweep`) judges it on the baseline it already runs; see
//! `docs/generative-test-suite.md`, "The reachability floor".
//!
//! A site's coverage floor in `tests/coverage.rs` proves the generators
//! draw a shape; this proves the shape reaches the engine path.

use std::collections::BTreeMap;

use crate::plant_sweep::{PLANT_CASES, PLANT_SEEDS, SweepTier};

/// An [`Exemption`]'s tier that covers every tier.
pub const EVERY_TIER: &str = "*";

/// A site a tier's baseline isn't held to reaching.
#[derive(Debug)]
pub struct Exemption {
    /// A sweep tier (`crate::baseline_quarantine::TIERS`), or [`EVERY_TIER`].
    pub tier: &'static str,
    /// A plant's or a marker's name.
    pub site: &'static str,
    /// Why the tier doesn't reach it, and, for a site no tier reaches, the
    /// deterministic test that does.
    pub why: &'static str,
}

/// Sites a tier's baseline isn't held to reaching, beyond its `not_gated`.
/// A site no tier reaches is listed for [`EVERY_TIER`] and names its
/// deterministic pin.
pub const EXEMPT: &[Exemption] = &[
    Exemption {
        tier: EVERY_TIER,
        site: "gb_build_leaf_unrouted",
        why: "it breaks a fresh build over a partitioned source's leaf, and no sweep tier \
              draws a partitioned source until #2109; partition_group_basis_build's \
              a_leaf_build_leaves_another_leafs_groups_alone pins it",
    },
    Exemption {
        tier: EVERY_TIER,
        site: "capture_nullable_key",
        why: "no tier draws a source read through a KEY with a nullable column (#2171 adds \
              it); source_keys's nullable_key_capture tests reach it",
    },
    Exemption {
        tier: EVERY_TIER,
        site: "capture_widen",
        why: "no tier changes a running program's definitions (#2183 adds it); \
              capture_install's a_row_staged_while_the_widen_waits_for_its_lock_is_below_the_gate \
              reaches it",
    },
    Exemption {
        tier: EVERY_TIER,
        site: "privilege_halts",
        why: "every tier runs as a superuser (#2182 adds another role); row_security's \
              a_drain_as_an_unchecked_role_pauses_the_writer_instead_of_skipping_rows reaches it",
    },
];

/// Why `site` is exempt for `tier`, if it is.
pub fn exemption(tier: &str, site: &str) -> Option<&'static Exemption> {
    EXEMPT
        .iter()
        .find(|e| e.site == site && (e.tier == tier || e.tier == EVERY_TIER))
}

/// Whether a sweep of `seeds` seeds of `cases` cases each, every case run,
/// covers the floor's baseline: the default seeds and cases, or more. A
/// seed draws the same first cases however many it draws, so a bigger run
/// is a superset. A smaller run (or one case alone) isn't held to it.
pub fn judges(seeds: u64, cases: usize, every_case: bool) -> bool {
    every_case && seeds >= PLANT_SEEDS && cases >= PLANT_CASES
}

/// The sites `tier`'s baseline must reach and didn't: every site not in
/// its `not_gated`, not exempt, with no count in `reached` (a site's total
/// over the baseline's cases).
pub fn misses(tier: &SweepTier, reached: &BTreeMap<String, u64>) -> Vec<&'static str> {
    trellis::dev::plant::sites()
        .filter(|site| !tier.not_gated.iter().any(|(name, _)| name == site))
        .filter(|site| exemption(tier.name, site).is_none())
        .filter(|site| reached.get(*site).copied().unwrap_or(0) == 0)
        .collect()
}

/// What's wrong with [`EXEMPT`]: an entry naming no site or no tier, with
/// no reason, or repeating another.
pub fn list_problems() -> Vec<String> {
    let sites: Vec<&str> = trellis::dev::plant::sites().collect();
    let mut problems = Vec::new();
    for (i, e) in EXEMPT.iter().enumerate() {
        if !sites.contains(&e.site) {
            problems.push(format!("{e:?} names no site"));
        }
        if e.tier != EVERY_TIER && !crate::baseline_quarantine::TIERS.contains(&e.tier) {
            problems.push(format!("{e:?} names no sweep tier"));
        }
        if e.why.trim().is_empty() {
            problems.push(format!("{e:?} gives no reason"));
        }
        if EXEMPT[..i]
            .iter()
            .any(|f| f.site == e.site && (f.tier == e.tier || f.tier == EVERY_TIER))
        {
            problems.push(format!("{e:?} repeats an earlier entry"));
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tier(not_gated: Vec<(&'static str, &'static str)>) -> SweepTier {
        SweepTier {
            name: "cooling_key",
            known_misses: &[],
            not_gated,
        }
    }

    /// A site counts against the floor unless it was reached, the tier
    /// doesn't gate it, or it is exempt.
    #[test]
    fn misses_leaves_out_reached_ungated_and_exempt_sites() {
        let all: Vec<&str> = trellis::dev::plant::sites().collect();
        let mut reached: BTreeMap<String, u64> =
            all.iter().map(|site| (site.to_string(), 1)).collect();
        assert_eq!(misses(&tier(Vec::new()), &reached), Vec::<&str>::new());

        reached.insert("stale_one_to_one_write".into(), 0);
        reached.remove("early_tombstone_gc");
        reached.insert("capture_widen".into(), 0);
        assert_eq!(
            misses(&tier(Vec::new()), &reached),
            vec!["stale_one_to_one_write", "early_tombstone_gc"],
            "an exempt site isn't a miss"
        );
        assert_eq!(
            misses(&tier(vec![("early_tombstone_gc", "not gated")]), &reached),
            vec!["stale_one_to_one_write"]
        );
    }

    #[test]
    fn only_the_default_baseline_or_more_is_judged() {
        assert!(judges(PLANT_SEEDS, PLANT_CASES, true));
        assert!(judges(PLANT_SEEDS + 1, PLANT_CASES * 2, true));
        assert!(!judges(PLANT_SEEDS - 1, PLANT_CASES, true));
        assert!(!judges(PLANT_SEEDS, PLANT_CASES - 1, true));
        assert!(!judges(PLANT_SEEDS, PLANT_CASES, false));
    }
}
