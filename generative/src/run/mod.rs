//! Drives a [`crate::model::Program`] through a [`crate::backend::Backend`]
//! and asserts the properties (design doc §4): install, apply each op,
//! quiesce, compare against [`crate::oracle`].
//!
//! Not built yet: this issue (#3) lays down the module skeleton and the
//! backend seam it will drive; the properties themselves light up as
//! engine stages land (design doc §4).
