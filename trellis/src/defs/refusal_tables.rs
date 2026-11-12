//! Every refusal variant is named in the Error column of a refusal table, in
//! `docs/transforms.md` or `docs/expressions.md`. The variants are read from
//! the enums' source text, so a new variant fails here until a row names it or
//! [`EXEMPT`] lists it with a reason.

const ENUMS: [(&str, &str); 4] = [
    ("ParseError", include_str!("error.rs")),
    ("ValidationError", include_str!("validate.rs")),
    ("DdlError", include_str!("ddl.rs")),
    ("CatalogError", include_str!("catalog.rs")),
];

const TABLES: [&str; 2] = [
    include_str!("../../../docs/transforms.md"),
    include_str!("../../../docs/expressions.md"),
];

/// Variants no refusal table names, as `Enum::Variant`.
const EXEMPT: &[&str] = &[
    // Syntax Postgres refuses too: the message is the whole story.
    "ParseError::UnexpectedChar",
    "ParseError::UnexpectedToken",
    "ParseError::UnexpectedEof",
    "ParseError::UnterminatedString",
    "ParseError::UnterminatedQuotedIdentifier",
    "ParseError::SplitOperator",
    "ParseError::NumericTrailingJunk",
    // A name that resolves to nothing; the message names it.
    "ValidationError::UnresolvedColumn",
    "ValidationError::UnresolvedGroupByColumn",
    "ValidationError::QualifiedSourceTableNotFound",
    "ValidationError::QualifiedTargetTableNotFound",
    "CatalogError::SourceTableNotFound",
    "CatalogError::NameCaseMismatch",
    "CatalogError::TransformNotFound",
    "CatalogError::AlterFieldNotFound",
    // Wrappers and infrastructure failures, not refusals.
    "CatalogError::Parse",
    "CatalogError::Validate",
    "CatalogError::Ddl",
    "CatalogError::Backfill",
    "CatalogError::DirectBackfill",
    "CatalogError::Db",
    "CatalogError::Pool",
    "CatalogError::ColumnPauseLockTimeout",
    "DdlError::Db",
    "DdlError::Pool",
    // Internal: a definition that skipped validation, or corrupt stored state.
    "ValidationError::JoinNeedsBothTables",
    "ValidationError::UnsupportedLiteralType",
    "DdlError::MalformedCompositeKey",
    "DdlError::InvalidDefinition",
    "DdlError::AliasSubstitution",
    "DdlError::UnroutedAggregate",
    "CatalogError::UnknownValueType",
    // Never constructed: an unknown operator symbol is `UnexpectedChar`.
    "ParseError::UnsupportedOperator",
    // Unreachable while `DeferredAggregate` refuses `STRING_AGG` (#1040).
    "ValidationError::StringAggSeparatorNotLiteral",
    // Statements on an existing transform (transforms.md, "Changing a
    // definition"; operations.md), not what a definition may read.
    "ParseError::MalformedDefinitionAddress",
    "ParseError::GrammarVersionTooOld",
    "ParseError::GrammarVersionTooNew",
    "CatalogError::TransformNotPaused",
    "CatalogError::AboveRunningRelease",
    "CatalogError::CannotRun",
    "CatalogError::DependentsBlockDrop",
    "CatalogError::AlterFieldAlreadyExists",
    "CatalogError::UnsupportedAlter",
    "CatalogError::LedgerTableMissing",
    "CatalogError::TargetTableMissing",
    // A `RESUME`'s re-keying (operations.md, "PAUSE and RESUME").
    "DdlError::NoPrimaryKeyToRecord",
    "CatalogError::KeyRepairBlocked",
    "CatalogError::KeyConstraintBlocked",
    "CatalogError::KeyColumnTaken",
    "CatalogError::SourceKeyChanged",
];

/// Capitalized words an Error column names that are SQL, not variants.
const KEYWORDS: &[&str] = &["JOIN"];

/// The variant names of `pub enum <name>` in `source`: each line indented
/// once that starts with a capital.
fn variants(name: &str, source: &str) -> Vec<String> {
    let start = source
        .find(&format!("pub enum {name} {{"))
        .unwrap_or_else(|| panic!("no `pub enum {name}`"));
    source[start..]
        .lines()
        .skip(1)
        .take_while(|line| *line != "}")
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|rest| rest.starts_with(|c: char| c.is_ascii_uppercase()))
        .map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_alphanumeric)
                .collect()
        })
        .collect()
}

/// The backticked capitalized names in the Error column of every
/// `| Condition | Error | ...` table in `doc`.
fn named(doc: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut in_table = false;
    for line in doc.lines() {
        in_table = line.starts_with("| Condition | Error |") || (in_table && line.starts_with('|'));
        let Some(cell) = line.split('|').nth(2).filter(|_| in_table) else {
            continue;
        };
        names.extend(
            cell.split('`')
                .skip(1)
                .step_by(2)
                .filter(|name| {
                    name.starts_with(|c: char| c.is_ascii_uppercase())
                        && name.chars().all(|c| c.is_ascii_alphanumeric())
                })
                .map(str::to_string),
        );
    }
    names
}

#[test]
fn every_refusal_variant_is_in_a_refusal_table() {
    let named: Vec<String> = TABLES.iter().flat_map(|doc| named(doc)).collect();
    let all: Vec<(&str, String)> = ENUMS
        .iter()
        .flat_map(|(name, source)| variants(name, source).into_iter().map(move |v| (*name, v)))
        .collect();
    assert!(all.len() > 150, "read only {} variants", all.len());
    let qualified: Vec<String> = all.iter().map(|(name, v)| format!("{name}::{v}")).collect();
    let mut failures = Vec::new();
    for (q, (_, variant)) in qualified.iter().zip(&all) {
        if !EXEMPT.contains(&q.as_str()) && !named.contains(variant) {
            failures.push(format!(
                "{q} is in no refusal table: add a row naming it in the Error column of \
                 docs/expressions.md#refusals if the expression's text decides it, else of \
                 docs/transforms.md#refusals; or add it to EXEMPT with the reason"
            ));
        }
    }
    let stray = EXEMPT.iter().filter(|e| !qualified.iter().any(|q| q == *e));
    failures.extend(stray.map(|e| format!("EXEMPT lists {e}, which is no variant")));
    let stray = named.iter().filter(|n| !KEYWORDS.contains(&n.as_str()));
    let stray = stray.filter(|n| !all.iter().any(|(_, v)| v == *n));
    failures.extend(stray.map(|n| format!("a refusal table names `{n}`, which is no variant")));
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
