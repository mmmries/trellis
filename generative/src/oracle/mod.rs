//! The independent recompute oracle (design doc §2): renders a
//! [`engine::defs::ast::TransformDef`]'s formulas back to a `SELECT` against
//! the source and treats Postgres itself as the correctness authority. Uses
//! only the shared components design doc §2 names (the parser AST and the
//! AST→SQL printer) — never the engine's own evaluator or maintenance
//! pipeline, which is [`crate::backend`]'s job alone.
//!
//! Not built yet: this issue (#3) lays down the module skeleton only.
