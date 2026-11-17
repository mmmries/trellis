//! `docs/errors.md` has one `### T0042 Variant` heading for every code
//! `error_code::assigned` records and has not retired, and lists each retired
//! code in its "Retired codes" table. The refusal tables in
//! `docs/transforms.md` and `docs/expressions.md` have no Remedy column (the
//! remedy is the errors.md entry's), and their Error column names each error
//! as a link to its heading.

const ASSIGNED: &str = include_str!("../error_code/assigned.rs");
const ERRORS: &str = include_str!("../../../docs/errors.md");
const TABLES: [(&str, &str); 2] = [
    ("transforms.md", include_str!("../../../docs/transforms.md")),
    (
        "expressions.md",
        include_str!("../../../docs/expressions.md"),
    ),
];

/// A refusal table's header row.
const HEADER: &str = "| Condition | Error | After define |";

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

/// The GitHub anchor of a `### T0042 Variant` heading.
fn anchor(code: &str, name: &str) -> String {
    format!("{}-{}", code.to_lowercase(), name.to_lowercase())
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

/// The Error cells of every refusal table in `doc`. A table whose header
/// starts like one but differs (a Remedy column) is a failure.
fn error_cells<'a>(file: &str, doc: &'a str, failures: &mut Vec<String>) -> Vec<&'a str> {
    let mut cells = Vec::new();
    let mut in_table = false;
    for line in doc.lines() {
        if line.starts_with("| Condition | Error |") {
            if line != HEADER {
                failures.push(format!(
                    "docs/{file}: a refusal table's header is `{line}`, not `{HEADER}`: the \
                     remedy belongs in docs/errors.md"
                ));
            }
            in_table = true;
            continue;
        }
        in_table = in_table && line.starts_with('|');
        if in_table
            && !line.starts_with("|---")
            // A group's heading row (`| **Source keys** | | |`) has no cells.
            && !line.ends_with(" | | |")
            && let Some(cell) = line.split(" | ").nth(1)
        {
            cells.push(cell);
        }
    }
    cells
}

#[test]
fn refusal_tables_link_each_error_to_its_heading() {
    let codes = assigned();
    let names: Vec<&str> = codes.iter().filter_map(|c| c.name.as_deref()).collect();
    let mut failures = Vec::new();
    let mut links = 0;
    for (file, doc) in TABLES {
        for cell in error_cells(file, doc, &mut failures) {
            // Every link to errors.md has the text `T0042 Variant` and the anchor
            // `t0042-variant`. What is left of the cell names no variant.
            let mut rest = String::new();
            let mut cell_links = 0;
            let mut tail = cell;
            while let Some((before, after)) = tail.split_once('[') {
                rest.push_str(before);
                let Some((text, after)) = after.split_once("](") else {
                    tail = after;
                    continue;
                };
                let (target, after) = after.split_once(')').unwrap_or((after, ""));
                tail = after;
                if !target.starts_with("errors.md") {
                    rest.push_str(text);
                    continue;
                }
                cell_links += 1;
                let (code, name) = text.split_once(' ').unwrap_or((text, ""));
                let known = codes
                    .iter()
                    .any(|c| c.code == code && c.name.as_deref() == Some(name));
                if !known {
                    failures.push(format!(
                        "docs/{file}: `{text}` is no assigned, unretired code and variant"
                    ));
                } else if target != format!("errors.md#{}", anchor(code, name)) {
                    failures.push(format!("docs/{file}: `{text}` links to `{target}`"));
                }
            }
            rest.push_str(tail);
            links += cell_links;
            if cell_links == 0 && rest.trim() != "n/a" {
                failures.push(format!(
                    "docs/{file}: the Error cell `{cell}` links no docs/errors.md heading: link \
                     each error, or write `n/a`"
                ));
            }
            let words = rest.split(|c: char| !c.is_ascii_alphanumeric());
            for word in words.filter(|word| names.contains(word)) {
                failures.push(format!(
                    "docs/{file}: the Error column names `{word}` bare: link it to its \
                     docs/errors.md heading, with its code as the link text"
                ));
            }
        }
    }
    assert!(links > 100, "read only {links} error links");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
