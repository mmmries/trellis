//! A wall clock the test server runs on and a test can step: libfaketime,
//! preloaded into the Postgres server (`TestCluster::start_with_fake_clock`).
//!
//! The server reads the clock through libc, so every `now()`, every
//! `clock_timestamp()` and every timestamp it stores moves with it; the
//! monotonic clocks do not (`FAKETIME_DONT_FAKE_MONOTONIC`), as a real NTP
//! step leaves them alone. A step is a file write, not a restart:
//! `FAKETIME_TIMESTAMP_FILE` names a file holding the offset from real time
//! (`+7200s`) and `FAKETIME_NO_CACHE` makes every process re-read it on each
//! call, so the backends already running see the step at once.
//!
//! The library is optional on a developer's box. `docs/generative-test-suite.md`
//! ("Wall-clock steps") says how to install it and how to skip these tests;
//! [`require`] is what a test calls to find it, or to skip loudly.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Names the library file, ahead of the known paths.
pub const LIB_ENV: &str = "TRELLIS_LIBFAKETIME";
/// Set (to anything but `0`) to skip the clock tests on purpose.
pub const SKIP_ENV: &str = "TRELLIS_SKIP_FAKETIME";
/// Set (to anything but `0`) where a missing library must fail the run
/// rather than skip it. CI sets it.
pub const REQUIRE_ENV: &str = "TRELLIS_REQUIRE_FAKETIME";

/// Where distributions put the library (Fedora; Debian and Ubuntu; Arch).
const KNOWN_PATHS: &[&str] = &[
    "/usr/lib64/libfaketime.so.1",
    "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1",
    "/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1",
    "/usr/lib/faketime/libfaketime.so.1",
];

/// What looking for the library came to.
#[derive(Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The library to preload.
    Found(PathBuf),
    /// The tests skip, for this reason.
    Skip(String),
    /// The run fails, for this reason.
    Fail(String),
}

/// [`resolve`] over the process's environment and file system.
pub fn locate() -> Resolution {
    resolve(&|name| std::env::var(name).ok(), &|path| path.is_file())
}

/// Finds the library: [`LIB_ENV`] first (it must name a file), then the known
/// paths. [`SKIP_ENV`] skips on purpose and [`REQUIRE_ENV`] turns a library
/// that is missing into a failure; setting both is a failure.
pub fn resolve(
    env: &dyn Fn(&str) -> Option<String>,
    is_file: &dyn Fn(&Path) -> bool,
) -> Resolution {
    let set = |name: &str| env(name).is_some_and(|v| !v.is_empty() && v != "0");
    let (skip, require) = (set(SKIP_ENV), set(REQUIRE_ENV));
    if skip && require {
        return Resolution::Fail(format!(
            "{SKIP_ENV} and {REQUIRE_ENV} are both set: a run cannot both skip the clock tests and require them"
        ));
    }
    if skip {
        return Resolution::Skip(format!("{SKIP_ENV} is set"));
    }
    let missing = if let Some(path) = env(LIB_ENV).filter(|v| !v.is_empty()) {
        if is_file(Path::new(&path)) {
            return Resolution::Found(PathBuf::from(path));
        }
        // An explicit pointer that is wrong is a mistake, not an absence.
        return Resolution::Fail(format!("{LIB_ENV}={path} is not a file"));
    } else if let Some(path) = KNOWN_PATHS.iter().find(|p| is_file(Path::new(p))) {
        return Resolution::Found(PathBuf::from(path));
    } else {
        format!(
            "libfaketime not found (looked in {}; set {LIB_ENV} to its path)",
            KNOWN_PATHS.join(", ")
        )
    };
    if require {
        Resolution::Fail(format!("{missing}, and {REQUIRE_ENV} is set"))
    } else {
        Resolution::Skip(missing)
    }
}

/// The library for a clock test, or `None` after printing a loud message
/// saying the test is skipped and why. Panics where the run must not skip.
pub fn require(test: &str) -> Option<PathBuf> {
    match locate() {
        Resolution::Found(lib) => Some(lib),
        Resolution::Skip(why) => {
            eprintln!(
                "\n!!!! SKIPPED: {test} did NOT run: {why}.\n\
                 !!!! Wall-clock steps are untested in this run. To run them, install libfaketime \
                 (see \"Wall-clock steps\" in docs/generative-test-suite.md).\n"
            );
            None
        }
        Resolution::Fail(why) => panic!("{test} cannot run: {why}"),
    }
}

/// A libfaketime-driven clock for one server: the library, and the file that
/// holds the current offset.
#[derive(Debug, Clone)]
pub struct FakeClock {
    lib: PathBuf,
    file: PathBuf,
}

impl FakeClock {
    /// A clock at offset zero, its file under `dir`.
    pub(crate) fn new(lib: PathBuf, dir: &Path) -> Self {
        let clock = Self {
            lib,
            file: dir.join("faketime.offset"),
        };
        clock.set_offset(0);
        clock
    }

    /// Puts the clock `seconds` ahead of real time (behind, when negative),
    /// replacing any earlier step: the file is swapped in whole, so a reader
    /// never sees half an offset.
    pub fn set_offset(&self, seconds: i64) {
        let partial = self.file.with_extension("tmp");
        fs::write(&partial, format!("{seconds:+}s\n")).expect("write the clock offset");
        fs::rename(&partial, &self.file).expect("swap in the clock offset");
    }

    /// Makes `command` run on this clock.
    pub(crate) fn apply(&self, command: &mut Command) {
        let preload = match std::env::var("LD_PRELOAD") {
            Ok(existing) if !existing.is_empty() => {
                format!("{} {existing}", self.lib.display())
            }
            _ => self.lib.display().to_string(),
        };
        command
            .env("LD_PRELOAD", preload)
            .env("FAKETIME_TIMESTAMP_FILE", &self.file)
            .env("FAKETIME_NO_CACHE", "1")
            .env("FAKETIME_DONT_FAKE_MONOTONIC", "1");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(env: &[(&str, &str)], files: &[&str]) -> Resolution {
        resolve(
            &|name| {
                env.iter()
                    .find(|(k, _)| *k == name)
                    .map(|(_, v)| v.to_string())
            },
            &|path| files.iter().any(|f| Path::new(f) == path),
        )
    }

    #[test]
    fn the_env_var_wins_over_known_paths() {
        let got = run(
            &[(LIB_ENV, "/opt/lib.so")],
            &["/opt/lib.so", KNOWN_PATHS[0]],
        );
        assert_eq!(got, Resolution::Found(PathBuf::from("/opt/lib.so")));
    }

    #[test]
    fn a_wrong_env_var_fails_rather_than_falling_back() {
        assert!(matches!(
            run(&[(LIB_ENV, "/nope.so")], &[KNOWN_PATHS[0]]),
            Resolution::Fail(_)
        ));
    }

    #[test]
    fn known_paths_are_searched_in_order() {
        let got = run(&[], &[KNOWN_PATHS[1], KNOWN_PATHS[3]]);
        assert_eq!(got, Resolution::Found(PathBuf::from(KNOWN_PATHS[1])));
    }

    #[test]
    fn a_missing_library_skips_unless_required() {
        assert!(matches!(run(&[], &[]), Resolution::Skip(_)));
        assert!(matches!(
            run(&[(REQUIRE_ENV, "1")], &[]),
            Resolution::Fail(_)
        ));
        assert!(matches!(
            run(&[(REQUIRE_ENV, "0")], &[]),
            Resolution::Skip(_)
        ));
    }

    #[test]
    fn the_opt_out_skips_even_with_the_library_present() {
        assert!(matches!(
            run(&[(SKIP_ENV, "1")], &[KNOWN_PATHS[0]]),
            Resolution::Skip(_)
        ));
    }

    #[test]
    fn skip_and_require_together_fail() {
        assert!(matches!(
            run(&[(SKIP_ENV, "1"), (REQUIRE_ENV, "1")], &[KNOWN_PATHS[0]]),
            Resolution::Fail(_)
        ));
    }
}
