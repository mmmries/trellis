//! Golden files of emitted SQL text, for tests that pin a statement
//! generator's output byte for byte (#1870): a refactor of how a statement
//! is assembled must leave the text Postgres plans unchanged, and these
//! tests prove it. Each file is a list of `-- <label>` headers, each
//! followed by one statement's text (most are one line, so a diff names
//! the statement).
//!
//! Run with `TRELLIS_BLESS_GOLDEN=1` to write the files from the current
//! code instead of comparing; review the diff before committing it.

use std::fmt::Write as _;
use std::path::PathBuf;

/// The statements of one golden file, in order.
#[derive(Default)]
pub(crate) struct Golden {
    text: String,
}

impl Golden {
    /// Adds `sql` under `label`.
    pub(crate) fn add(&mut self, label: &str, sql: &str) {
        writeln!(self.text, "-- {label}\n{sql}\n").expect("write to a String");
    }

    /// Compares the statements with `trellis/tests/golden/sql/<name>.sql`,
    /// or writes them there under `TRELLIS_BLESS_GOLDEN=1`. A mismatch names
    /// the first statement that differs.
    pub(crate) fn check(self, name: &str) {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/golden/sql")
            .join(format!("{name}.sql"));
        if std::env::var_os("TRELLIS_BLESS_GOLDEN").is_some() {
            std::fs::create_dir_all(path.parent().expect("a parent")).expect("create the dir");
            std::fs::write(&path, &self.text).expect("write the golden file");
            return;
        }
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "{}: {e}; run with TRELLIS_BLESS_GOLDEN=1 to create it",
                path.display()
            )
        });
        if expected == self.text {
            return;
        }
        let mut label = "";
        for (want, got) in expected.lines().zip(self.text.lines()) {
            if let Some(l) = want.strip_prefix("-- ") {
                label = l;
            }
            if want != got {
                panic!(
                    "{}: `{label}` differs\n want: {want}\n  got: {got}",
                    path.display()
                );
            }
        }
        panic!(
            "{}: the statement lists differ in length ({} vs {} lines)",
            path.display(),
            expected.lines().count(),
            self.text.lines().count()
        );
    }
}
