//! The reachability floor: every plant site, and every reach marker
//! (`trellis::dev::plant::sites`), must be reached in a tier's baseline
//! unless the tier doesn't gate it (`SweepTier::not_gated`, which says why)
//! or [`EXEMPT`] names it for the tier, with why. A site the tier doesn't
//! gate is still held when [`REACHED_UNGATED`] lists it. The planted-bug sweep
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
        tier: "join_star",
        site: "star_late_skip_last",
        why: "a race window: a re-derived row must move at one position between the page's \
              unlocked read and its lock, which the paced writes do only now and then \
              (reached in 1 of 48 baseline cases, logbook 20261120T014255-d-sweep); \
              join_star_root's and join_star_joined's pins reach it",
    },
    Exemption {
        tier: "join_star",
        site: "star_late_skip_first",
        why: "a race window: a re-derived row must move at one position between the page's \
              unlocked read and its lock, which the paced writes do only now and then \
              (reached in 2 of 48 baseline cases, logbook 20261120T014255-d-sweep); \
              join_star_root's and join_star_joined's pins reach it",
    },
    Exemption {
        tier: "join_star",
        site: "star_recheck_j",
        why: "a race window: a re-derived row must move at one position between the page's \
              unlocked read and its lock, which the paced writes do only now and then \
              (reached in 4 of 48 baseline cases, logbook 20261120T014255-d-sweep); \
              join_star_root's and join_star_joined's pins reach it",
    },
    Exemption {
        tier: "join_star",
        site: "star_recheck_skip_last",
        why: "a race window: a re-derived row must move at one position between the page's \
              unlocked read and its lock, which the paced writes do only now and then \
              (reached in 2 of 48 baseline cases, logbook 20261120T014255-d-sweep); \
              join_star_root's and join_star_joined's pins reach it",
    },
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
        why: "a tier widens capture only when a definition installed after another over its \
              source needs a column or group key the installed capture doesn't image, which \
              the drawn programs do in 0 to 22 of 48 \
              baseline cases by tier (join_chained 22, join_one_to_one 15, join_star 12, \
              logbooks 20261120T014255-d-sweep and 20261120T023839-d-sweep); #2183 changes \
              a running program's definitions; capture_install's \
              a_row_staged_while_the_widen_waits_for_its_lock_is_below_the_gate reaches it",
    },
    Exemption {
        tier: EVERY_TIER,
        site: "privilege_halts",
        why: "every tier runs as a superuser (#2182 adds another role); row_security's \
              a_drain_as_an_unchecked_role_pauses_the_writer_instead_of_skipping_rows reaches it",
    },
];

/// Sites each tier doesn't gate but its baseline reaches in a quarter or
/// more of its cases, so the floor holds them too: a reach lost there is
/// exploration lost, though no catch rate shows it. A quarter keeps a
/// timing-dependent reach from dropping to none by chance. Read off each
/// tier's reachability report, which prints the list its run would give
/// ([`suggested_reached_ungated`]); a site the tier gates later leaves its
/// list. A site whose reach counts passes of its path rather than its
/// precondition (`trellis::dev::plant::counts_passes`) isn't listed: its
/// count says less.
pub const REACHED_UNGATED: &[(&str, &[&str])] = &[
    ("join", &["gb_where_old_image"]),
    (
        "mid_burst",
        &[
            "chunk_without_entry_lock",
            "gb_build_snapshot_before_locks",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_ring_branch",
            "gb_build_straggler_unlocked",
            "o2o_where_no_tombstone",
        ],
    ),
    (
        "steady_load",
        &["chunk_without_entry_lock", "o2o_where_no_tombstone"],
    ),
    (
        "join_star",
        &[
            "stale_one_to_one_write",
            "join1_m_pass_b_only",
            "join1_sweep_start_bound",
            "join1_truncate_no_floor",
            "star_j_unsorted",
            "star_r_split",
            "star_held_origin_first_only",
            "star_def_lock_first_only",
        ],
    ),
    (
        "join_chained",
        &[
            "stale_one_to_one_write",
            "chunk_without_entry_lock",
            "gb_fold_sides_per_key",
            "join_no_j_lock",
            "join_t_skip_lock",
            "join1_no_j_lock",
            "join1_no_recheck",
            "join1_lsn_only",
            "join1_vis_only",
            "join1_no_entry_lock",
            "join1_old_key_unlocked",
            "join1_snapshot_after_read",
            "join1_rd_no_key",
            "join1_brd_no_key",
            "join1_no_placeholder",
            "join1_m_before_j",
            "join1_chunk_no_key",
            "join1_no_orphan_pass",
            "join1_orphan_delete",
            "join1_m_pass_b_only",
            "join1_sweep_start_bound",
            "star_j_unsorted",
        ],
    ),
    (
        "cooling_key",
        &[
            "chunk_without_entry_lock",
            "gb_fold_sides_per_key",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
            "o2o_where_build_unfiltered",
            "o2o_where_no_tombstone",
        ],
    ),
    (
        "hot_key",
        &[
            "gb_fold_sides_per_key",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
        ],
    ),
    (
        "cooling_backfill",
        &[
            "gb_build_snapshot_before_locks",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
        ],
    ),
    (
        "min_max",
        &[
            "early_tombstone_gc",
            "gb_skip_group_lock",
            "gb_fold_sides_per_key",
            "gb_build_snapshot_before_locks",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
            "sc_extremes_under_basis",
            "sc_extremes_uncompared",
            "sc_extreme_groups_uncompared",
        ],
    ),
    (
        "join_one_to_one",
        &[
            "stale_one_to_one_write",
            "join1_m_pass_b_only",
            "join1_sweep_start_bound",
            "join1_truncate_no_floor",
            "star_j_unsorted",
        ],
    ),
    (
        "extremes_tamper",
        &[
            "gb_skip_group_lock",
            "gb_fold_sides_per_key",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
            "gbx_probe_no_xbasis",
            "gbx_shared_basis",
            "gbx_probe_before_lock",
            "gbx_null_extreme_no_probe",
            "gbx_null_key_by_equality",
            "gbx_gc_ignores_xstamp",
            "gbx_absolute_write_keeps_extremes",
            "sc_extremes_under_basis",
            "sc_extreme_groups_uncompared",
        ],
    ),
    (
        "held_keys",
        &[
            "stale_one_to_one_write",
            "chunk_without_entry_lock",
            "gb_skip_group_lock",
            "gb_gc_ignores_build",
            "gb_build_no_hidden_branch",
            "gb_build_no_target_branch",
            "gb_build_no_ring_branch",
            "gb_build_null_key_by_equality",
            "gb_build_straggler_unlocked",
        ],
    ),
];

/// Whether [`REACHED_UNGATED`] holds `tier` to reaching `site`.
pub fn reached_ungated(tier: &str, site: &str) -> bool {
    REACHED_UNGATED
        .iter()
        .any(|(name, sites)| *name == tier && sites.contains(&site))
}

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
/// its `not_gated` (or in [`REACHED_UNGATED`]), not exempt, with no count
/// in `reached` (a site's total over the baseline's cases).
pub fn misses(tier: &SweepTier, reached: &BTreeMap<String, u64>) -> Vec<&'static str> {
    trellis::dev::plant::sites()
        .filter(|site| {
            !tier.not_gated.iter().any(|(name, _)| name == site) || reached_ungated(tier.name, site)
        })
        .filter(|site| exemption(tier.name, site).is_none())
        .filter(|site| reached.get(*site).copied().unwrap_or(0) == 0)
        .collect()
}

/// The sites [`REACHED_UNGATED`] would list for `tier` from a baseline of
/// `cases` cases whose per-site reach is `reached` (cases that reached it,
/// keyed by site): each site the tier doesn't gate, isn't exempt from and
/// whose count isn't of passes, reached in a quarter or more of the cases.
pub fn suggested_reached_ungated(
    tier: &SweepTier,
    cases: usize,
    reached: &BTreeMap<String, usize>,
) -> Vec<&'static str> {
    trellis::dev::plant::sites()
        .filter(|site| tier.not_gated.iter().any(|(name, _)| name == site))
        .filter(|site| exemption(tier.name, site).is_none())
        .filter(|site| !trellis::dev::plant::counts_passes(site))
        .filter(|site| cases > 0 && 4 * reached.get(*site).copied().unwrap_or(0) >= cases)
        .collect()
}

/// The sweep's failure for `tier`'s baseline missing `missed`
/// ([`misses`]), with what to change for each kind of site.
pub fn failure(tier: &SweepTier, missed: &[&str]) -> String {
    let (held_ungated, gated): (Vec<&str>, Vec<&str>) = missed
        .iter()
        .partition(|site| tier.not_gated.iter().any(|(name, _)| name == *site));
    let mut out = format!(
        "the {} tier's baseline never reached {missed:?} (see the reachability report above).",
        tier.name
    );
    if !gated.is_empty() {
        out.push_str(&format!(
            " {gated:?}: the tier gates each, so draw a shape that reaches it, or add it to              generative/src/reach_floor.rs's EXEMPT for this tier with why (and its              deterministic pin if no tier reaches it). A plant whose hook computes its              precondition only when armed counts no reach: compute it armed or not, or guard              it with plant::visit (trellis/src/plant.rs, \"Adding a plant\")."
        ));
    }
    if !held_ungated.is_empty() {
        out.push_str(&format!(
            " {held_ungated:?}: the tier doesn't gate these, so only REACHED_UNGATED holds              them; if the tier's draws changed on purpose, replace its entry there with the              list the report above suggests."
        ));
    }
    out
}

/// What's wrong with [`EXEMPT`] and [`REACHED_UNGATED`]: an entry naming
/// no site or no tier, an exemption with no reason, or an entry repeating
/// another.
pub fn list_problems() -> Vec<String> {
    let sites: Vec<&str> = trellis::dev::plant::sites().collect();
    let mut problems = Vec::new();
    for (i, (tier, listed)) in REACHED_UNGATED.iter().enumerate() {
        if !crate::baseline_quarantine::TIERS.contains(tier) {
            problems.push(format!("REACHED_UNGATED's {tier} names no sweep tier"));
        }
        if REACHED_UNGATED[..i].iter().any(|(t, _)| t == tier) {
            problems.push(format!("REACHED_UNGATED lists {tier} twice"));
        }
        for (j, site) in listed.iter().enumerate() {
            if !sites.contains(site) {
                problems.push(format!("REACHED_UNGATED's {tier} names no site {site}"));
            }
            if listed[..j].contains(site) {
                problems.push(format!("REACHED_UNGATED's {tier} lists {site} twice"));
            }
            if exemption(tier, site).is_some() {
                problems.push(format!(
                    "REACHED_UNGATED's {tier} lists {site}, which is exempt"
                ));
            }
            if trellis::dev::plant::counts_passes(site) {
                problems.push(format!(
                    "REACHED_UNGATED's {tier} lists {site}, whose reach counts passes"
                ));
            }
        }
    }
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
        assert!(reached_ungated("cooling_key", "chunk_without_entry_lock"));
        reached.insert("chunk_without_entry_lock".into(), 0);
        assert_eq!(
            misses(
                &tier(vec![
                    ("early_tombstone_gc", "not gated"),
                    ("chunk_without_entry_lock", "not gated")
                ]),
                &reached
            ),
            vec!["stale_one_to_one_write", "chunk_without_entry_lock"],
            "an ungated site REACHED_UNGATED lists is still held"
        );
    }

    /// The suggested list holds the tier's ungated sites reached in a
    /// quarter of the cases, leaving out exempt and pass-counted sites; the
    /// failure names the remedy for gated and ungated misses apart.
    #[test]
    fn the_suggested_list_and_the_failure_follow_the_rules() {
        let tier = tier(vec![
            ("chunk_without_entry_lock", "not gated"),
            ("o2o_where_no_tombstone", "not gated"),
            ("gb_early_zero_gc", "not gated"),
            ("capture_widen", "not gated"),
        ]);
        let reached: BTreeMap<String, usize> = [
            ("chunk_without_entry_lock", 12),
            ("o2o_where_no_tombstone", 11),
            ("gb_early_zero_gc", 48),
            ("capture_widen", 48),
            ("stale_one_to_one_write", 48),
        ]
        .into_iter()
        .map(|(site, n)| (site.to_string(), n))
        .collect();
        assert_eq!(
            suggested_reached_ungated(&tier, 48, &reached),
            vec!["chunk_without_entry_lock"],
            "12 of 48 is a quarter; a gated, an exempt and a pass-counted site aren't listed"
        );
        let text = failure(
            &tier,
            &["stale_one_to_one_write", "chunk_without_entry_lock"],
        );
        assert!(
            text.contains("[\"stale_one_to_one_write\"]: the tier gates each"),
            "{text}"
        );
        assert!(
            text.contains("[\"chunk_without_entry_lock\"]: the tier doesn't gate these"),
            "{text}"
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
