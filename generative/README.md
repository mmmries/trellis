# `generative`

The generative correctness test suite (design doc:
`docs/generative-test-suite.md`, epic #2). See that doc for architecture,
oracle design, and the property list; this file is the crate-local
operational notes.

## A real full run is minutes, not seconds

If `cargo test -p generative` reports green in about a second, it executed
nothing — check `PROPTEST_CASES` and that the properties actually ran, not
just the meta-tests. Each proptest case round-trips through a real Postgres
cluster (`testkit`), so a healthy run at the design doc §9 case-count band
(12–24 per property) takes real wall-clock time. A suite that reports green
too fast to have done that work is a bug in the harness, not a fast pass —
see `docs/generative-test-suite.md` §6 and `generative/tests/meta.rs`.
