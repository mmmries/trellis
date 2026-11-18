//! `docs/errors.md` has one `### T0042 Variant` heading for every code
//! `error_code::assigned` records and has not retired, and lists each retired
//! code in its "Retired codes" table. Every heading names an assigned code.

const ASSIGNED: &str = include_str!("../error_code/assigned.rs");
const ERRORS: &str = include_str!("../../../docs/errors.md");

/// An assigned code: `T0042`, and the variant's bare name (`None` when
/// retired).
struct Code {
    code: String,
    name: Option<String>,
}

/// Every code `ASSIGNED` records, read from its `"T0042 Enum::Variant",`
/// entries.
fn assigned() -> Vec<Code> {
    ASSIGNED
        .lines()
        .filter_map(|line| line.trim().strip_prefix("\"T"))
        .filter_map(|rest| rest.strip_suffix("\","))
        .map(|rest| {
            let (number, entry) = rest.split_once(' ').expect("`Tnnnn <entry>`");
            let name = (!entry.starts_with("retired:"))
                .then(|| entry.rsplit("::").next().expect("a name").to_string());
            Code {
                code: format!("T{number}"),
                name,
            }
        })
        .collect()
}

/// The `(code, name)` of each `### Tnnnn Name` heading in errors.md.
fn headings() -> Vec<(String, String)> {
    ERRORS
        .lines()
        .filter_map(|line| line.strip_prefix("### T"))
        .map(|rest| {
            let (number, name) = rest.split_once(' ').unwrap_or((rest, ""));
            (format!("T{number}"), name.to_string())
        })
        .collect()
}

/// The codes in errors.md's "Retired codes" table.
fn retired_listed() -> Vec<String> {
    let start = ERRORS
        .find("\n## Retired codes\n")
        .expect("errors.md has a `## Retired codes` section");
    ERRORS[start..]
        .lines()
        .filter_map(|line| line.strip_prefix("| T"))
        .map(|rest| format!("T{}", rest.split(' ').next().unwrap_or_default()))
        .collect()
}

#[test]
fn every_code_has_one_errors_md_heading() {
    let codes = assigned();
    assert!(codes.len() > 200, "read only {} codes", codes.len());
    let headings = headings();
    let retired = retired_listed();
    let mut failures = Vec::new();
    for code in &codes {
        let written: Vec<&str> = headings
            .iter()
            .filter(|(c, _)| *c == code.code)
            .map(|(_, name)| name.as_str())
            .collect();
        match &code.name {
            Some(name) if written != [name.as_str()] => failures.push(format!(
                "docs/errors.md has {written:?} for {}: give it exactly one `### {} {name}` \
                 heading, saying what it means and the remedy",
                code.code, code.code
            )),
            Some(_) => {}
            None if !written.is_empty() => failures.push(format!(
                "docs/errors.md has a heading for {}, which is retired",
                code.code
            )),
            None if !retired.contains(&code.code) => failures.push(format!(
                "{} is retired: list it in docs/errors.md's \"Retired codes\" table",
                code.code
            )),
            None => {}
        }
    }
    for (code, _) in &headings {
        if !codes.iter().any(|c| c.code == *code) {
            failures.push(format!(
                "docs/errors.md has a heading for {code}, which is no assigned code"
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
