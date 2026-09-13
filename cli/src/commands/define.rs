//! `trellis define <GRAMMAR>` — registers a transform or relationship
//! definition against the configured database.
//!
//! The engine facade ([`engine::Trellis`]) keeps `define` (TRANSFORM text)
//! and `define_relationship` (RELATIONSHIP text) as two separate methods,
//! but from an operator's shell there's one grammar and one command — see
//! docs/public-api-design.md's "same grammar" intent. This module is the
//! thin dispatch that picks the right facade method by sniffing the
//! grammar's first keyword.

use engine::{Config, Trellis, TrellisOptions};

/// Help text for `trellis define -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis define [--database-url <URL>] <GRAMMAR>

Registers a TRANSFORM or RELATIONSHIP definition. <GRAMMAR> is the full
grammar text as a single, quoted shell argument, e.g.:

  trellis define 'TRANSFORM post_totals FROM posts SELECT count(*) AS n'
  trellis define 'RELATIONSHIP posts FROM authors.id TO posts.author_id'

Applies pending migrations first, so this works against a freshly created
database with no separate migrate step.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// A parsed `define` invocation: just the grammar text, once
/// `--database-url`/`-d` has already been pulled out of argv by the caller
/// (see `connection::extract_database_url`).
#[derive(Debug)]
pub struct Args {
    pub grammar: String,
}

/// Parses the remaining positional args after `--database-url`/`-d` (and the
/// `define` subcommand name itself) have been stripped. Deliberately refuses
/// to reassemble multiple argv words into one grammar string — a caller who
/// forgot to quote their grammar would otherwise get it silently
/// space-mangled rather than a clear error.
pub fn parse(args: &[String]) -> Result<Args, String> {
    match args {
        [grammar] => Ok(Args {
            grammar: grammar.clone(),
        }),
        [] => Err(format!(
            "{USAGE}\nerror: missing required <GRAMMAR> argument"
        )),
        _ => Err(format!(
            "{USAGE}\nerror: expected exactly one <GRAMMAR> argument (quote it as a single shell \
             argument), got {}: {:?}",
            args.len(),
            args
        )),
    }
}

/// Whether `grammar`'s first whitespace-delimited token is `RELATIONSHIP`,
/// case-insensitively — anything else (in particular `TRANSFORM`, the common
/// case) routes to [`Trellis::define`].
fn is_relationship(grammar: &str) -> bool {
    grammar
        .split_whitespace()
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("RELATIONSHIP"))
}

/// Connects, migrates, registers `args.grammar`, and disconnects, returning
/// a human-readable confirmation on success.
///
/// `shutdown` is called even if migration or registration failed, per
/// [`Trellis::shutdown`]'s "correct lifecycle call" contract — but the
/// earlier error (the one an operator actually needs to see) takes priority
/// over a shutdown failure when both occur.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;

    let outcome = register(&trellis, &args.grammar).await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());

    let message = outcome?;
    shutdown_outcome?;
    Ok(message)
}

/// Applies migrations, then registers `grammar` as a relationship or
/// transform depending on its leading keyword.
async fn register(trellis: &Trellis, grammar: &str) -> Result<String, String> {
    trellis.migrate().await.map_err(|err| err.to_string())?;

    if is_relationship(grammar) {
        let def = trellis
            .define_relationship(grammar)
            .await
            .map_err(|err| err.to_string())?;
        Ok(format!(
            "registered relationship {:?} (id {}, {} cardinality): {}.{} -> {}.{}",
            def.def.name,
            def.id,
            def.cardinality.as_str(),
            def.def.from_table,
            def.def.from_col,
            def.def.to_table,
            def.def.to_col,
        ))
    } else {
        let def = trellis
            .define(grammar)
            .await
            .map_err(|err| err.to_string())?;
        Ok(format!(
            "registered transform (id {}): {} -> {}",
            def.id, def.def.source, def.def.target,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_grammar_is_an_error() {
        let err = parse(&[]).unwrap_err();
        assert!(err.contains("missing required <GRAMMAR>"));
        assert!(err.contains("Usage: trellis define"));
    }

    #[test]
    fn too_many_args_is_an_error() {
        let args = vec!["TRANSFORM".to_string(), "x FROM y".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("expected exactly one <GRAMMAR> argument"));
    }

    #[test]
    fn single_grammar_arg_parses() {
        let args = vec!["TRANSFORM x FROM y SELECT 1".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.grammar, "TRANSFORM x FROM y SELECT 1");
    }

    #[test]
    fn relationship_keyword_is_detected_case_insensitively() {
        assert!(is_relationship(
            "RELATIONSHIP posts FROM authors.id TO posts.author_id"
        ));
        assert!(is_relationship(
            "relationship posts FROM authors.id TO posts.author_id"
        ));
        assert!(is_relationship("RelaTionShip x FROM a.b TO c.d"));
    }

    #[test]
    fn transform_keyword_is_not_treated_as_relationship() {
        assert!(!is_relationship("TRANSFORM totals FROM orders SELECT 1"));
        assert!(!is_relationship("transform totals FROM orders SELECT 1"));
    }

    #[test]
    fn empty_grammar_is_not_a_relationship() {
        assert!(!is_relationship(""));
        assert!(!is_relationship("   "));
    }
}
