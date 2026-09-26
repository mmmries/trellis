# #565 research harness (trigger capture)

Python + pgbench scripts behind the phase-1 and phase-2 comments on #565. Not built or run by
`verify`. Paths are hard-coded to the research box (`~/tc-565`, `target/release/trellis` in this
worktree); treat them as a record of what was measured, not a portable tool.

- `capture_sql.py` generates the per-table capture triggers (row/statement, image encodings,
  pointer reads, phase-2 shapes: `new_only`, `skip_noop`, `reread`).
- `e1.py` is the write-path harness (E1/E2/E3 cost/E9, disk tiers via `TC565_BASE`).
- `e3_parity.py`, `e4_*.py`, `e6_*.py`, `e7_join_lock.py`, `e8_idle.py`: the other phase-1 experiments.
- `v_e2e.py` runs trigger capture through Trellis's real seal/drain using this branch's
  `TRELLIS_SPIKE_TRIGGER_CAPTURE` switch: correctness under mixed load, drain backpressure,
  Trellis-down growth. `a_probes.py` / `v_nested_e2e.sh` are the phase-2 correctness probes.
- `queue-phase2.sh` is the queued phase-2 run list.
