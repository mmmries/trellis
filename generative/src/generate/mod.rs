//! Program generation (design doc §1/§3): generators producing only valid
//! [`crate::model::Program`]s from a config. Makes no engine calls — the
//! module boundary this crate enforces is that only [`crate::backend`]
//! drives the engine's pipeline.
//!
//! Not built yet: this issue (#3) lays down the module skeleton and the
//! [`crate::model::Program`]/[`crate::backend`] seam it will generate
//! against and drive through. The actual generators (seed-before-mutate,
//! bounded value domains, coverage meta-tests — design doc §3) are a later
//! child of the epic.
