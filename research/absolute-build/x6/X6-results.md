# X6 results: planted violations for candidate A, and the generative tier (#1917)

Research only. Logbook `20261112T181812-r-sweep`, outcome **miss** (the sweep did not catch most plants at this
budget; see "Reading"). Plants and pins are in commit `02589c72`.

## Plants (each caught by a deterministic test)

| plant | rule it breaks | deterministic pin |
|---|---|---|
| `ab_gc_ignores_build` | the zero-group GC skips a definition under a build | `group_basis_write_once_gc` |
| `ab_enum_no_hidden` | step 1 enumerates the hidden rows | `a_vacated_group_drained_before_step_one_is_zeroed` |
| `ab_enum_no_target` | step 1 enumerates the target's groups (resume) | `a_resume_removes_a_target_row_only_the_target_names` |
| `ab_enum_no_ring` | step 1 enumerates the ring's old images | `a_group_vacated_by_an_undrained_change_is_zeroed`, leaf vacated test |
| `ab_stragglers_from_s1` | step 2 reads from a new snapshot | the three `a_page_creates_a_group_inside_step_one_*` tests, leaf test |
| `ab_leaf_unrouted` | a leaf's enumeration is routed | both leaf interleaving tests |

`gb_build_snapshot_before_locks` is still caught by its pin on A's path, leaves included.

## Sweep

`planted_bugs_are_caught` (`concurrent_convergence`), tiers `mid_burst` and `steady_load`, 2 seeds x 6 cases per plant
per tier (a small budget: about 20 minutes of box time in all). "fired" = cases in which the planted code ran.

| plant | mid_burst caught / fired / cases | steady_load caught / fired / cases |
|---|---|---|
| baseline (unplanted prototype) | 0 failures / - / 12 | 0 failures / - / 12 |
| `ab_enum_no_target` | **5** / 12 / 12 | 0 / 12 / 12 |
| `ab_enum_no_ring` | 0 / 12 / 12 | **1** / 12 / 12 |
| `ab_enum_no_hidden` | 0 / 12 / 12 | 0 / 12 / 12 |
| `ab_gc_ignores_build` | 0 / 12 / 12 | 0 / 12 / 12 |
| `ab_stragglers_from_s1` | 0 / 0 / 12 | 0 / 5 / 12 |
| `gb_build_snapshot_before_locks` (existing) | 0 / 12 / 12 | **3** / 12 / 12 |
| `gb_early_zero_gc` (existing) | 0 / 1 / 12 | 0 / 1 / 12 |

Partitioned properties at `PROPTEST_CASES=16`: `property_partitioned_hot_keys_converge_under_concurrent_drains` and
`property_a_partitioned_source_converges_under_the_concurrent_backend` pass unplanted, and also pass with
`ab_leaf_unrouted` (not caught).

## Reading

- **The unplanted prototype passed every generative case run** (24 concurrent cases, 2 partitioned properties x 16).
- **At this budget the tier cannot judge A's rules.** It caught 2 of the 5 unpartitioned A plants, and it also missed
  one existing plant entirely (`gb_early_zero_gc`) and caught the other in 3 of 24 cases, both of which the nightly
  budget is known to catch. The misses say the budget was too small, not that the rules are untestable; a nightly-size
  run was not done.
- `ab_stragglers_from_s1` rarely fires: these tiers seldom produce a straggler (0 of 12 cases in `mid_burst`). A tier
  meant to cover A needs pages applying inside step 1.
- `ab_gc_ignores_build` fires in every case and is never caught: the counterexample needs a zero row collected between
  the build's snapshot and its insert, a narrow window.
- `ab_leaf_unrouted` is not caught by the convergence properties, as predicted: the final state heals when stragglers
  are read through the root.
- So for now each A rule rests on its deterministic pin and on the X1/X1b model, not on the generative tier.

## For a production implementation

Add the plants to the nightly sweep at its normal budget, add a tier (or a hook) that makes pages apply during step 1,
and check a partitioned definition between leaf transactions, not only at the end.
