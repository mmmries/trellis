//! `trellis run [--staging|--no-staging] [--drain-threads N]` — runs the live
//! CDC/apply pipeline until interrupted.
//!
//! Unlike `define`, this command can't just call `.migrate()` right after
//! connecting: [`engine::Trellis::connect`] with `staging: true` derives the
//! source-table set from the catalog *during* connect, before this module
//! ever gets a chance to run anything, and it errors immediately
//! (`TrellisError::NoDefinitions`) if no definitions are registered yet.
//! Migrating after that connect would be too late even if it created
//! definitions — which it doesn't; migrations only create/update schema, not
//! data. So there is no ordering of "connect once, then migrate" that helps
//! here: an operator must have already run `trellis define` (against a
//! migrated database) before `run` will do anything useful, and that's a
//! real prerequisite, not an oversight.
//!
//! What `.migrate()` *is* still useful for here is the schema itself: on a
//! genuinely fresh database (no Trellis tables at all), the catalog query
//! `connect` runs to derive source tables would fail with a raw "relation
//! does not exist" error rather than the engine's own clear
//! `NoDefinitions` message. So this module opens a short-lived, no-options
//! connection first, migrates on it, and shuts it down — that guarantees the
//! catalog tables exist by the time the real `staging`/`drain_threads`
//! connect runs, so a no-definitions-yet database surfaces the engine's own
//! `NoDefinitions` message instead of a confusing SQL error, while a
//! genuinely fresh database doesn't require a separate `migrate` step any
//! more than `define` does.

use engine::{Config, Trellis, TrellisOptions};

/// Help text for `trellis run -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis run [--staging|--no-staging] [--drain-threads N] [--database-url <URL>]

Runs the live CDC/apply pipeline (staging worker and/or drain workers) until
interrupted with Ctrl-C. Requires at least one TRANSFORM or RELATIONSHIP
definition to already be registered (via `trellis define`) against a
migrated database.

Options:
  --staging                  Run the CDC/staging worker. Default.
  --no-staging               Don't run the CDC/staging worker. Mutually
                             exclusive with --staging.
  --drain-threads <N>        Number of drain (application) worker threads to
                             run. Must be a non-negative integer. Default: 2.
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// A parsed `run` invocation, once `--database-url`/`-d` has already been
/// pulled out of argv by the caller (see `connection::extract_database_url`).
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub staging: bool,
    pub drain_threads: usize,
}

impl Default for Args {
    /// Staging on, two drain threads — a single-process, all-in-one setup
    /// that does something useful with no flags at all.
    fn default() -> Self {
        Self {
            staging: true,
            drain_threads: 2,
        }
    }
}

/// Parses `--staging`/`--no-staging`/`--drain-threads <N>`. Order-independent
/// and each flag may appear at most once; `--staging` and `--no-staging`
/// together are a usage error (rather than "last one wins") since silently
/// picking one would surprise whichever the operator meant.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let mut staging: Option<bool> = None;
    let mut drain_threads: Option<usize> = None;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx].as_str() {
            "--staging" => {
                if staging == Some(false) {
                    return Err(format!(
                        "{USAGE}\nerror: --staging and --no-staging are mutually exclusive"
                    ));
                }
                staging = Some(true);
                idx += 1;
            }
            "--no-staging" => {
                if staging == Some(true) {
                    return Err(format!(
                        "{USAGE}\nerror: --staging and --no-staging are mutually exclusive"
                    ));
                }
                staging = Some(false);
                idx += 1;
            }
            "--drain-threads" => {
                let Some(value) = args.get(idx + 1) else {
                    return Err(format!(
                        "{USAGE}\nerror: --drain-threads requires a value, e.g. --drain-threads 2"
                    ));
                };
                if drain_threads.is_some() {
                    return Err(format!(
                        "{USAGE}\nerror: --drain-threads may only be specified once"
                    ));
                }
                let parsed: usize = value.parse().map_err(|_| {
                    format!(
                        "{USAGE}\nerror: --drain-threads expects a non-negative integer, got {value:?}"
                    )
                })?;
                drain_threads = Some(parsed);
                idx += 2;
            }
            other => {
                return Err(format!("{USAGE}\nerror: unrecognized argument {other:?}"));
            }
        }
    }

    let defaults = Args::default();
    Ok(Args {
        staging: staging.unwrap_or(defaults.staging),
        drain_threads: drain_threads.unwrap_or(defaults.drain_threads),
    })
}

/// Connects (migrating a throwaway connection first — see the module doc
/// comment for why), prints a startup message, blocks until Ctrl-C, then
/// shuts down cleanly. Returns the shutdown message on success.
pub async fn run(args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;

    // Migrate on a plain, no-background-work connection first. This isn't
    // "run migrations before doing anything" for its own sake — it exists so
    // that, on a genuinely fresh database, the staging connect below fails
    // with the engine's clear `NoDefinitions` message rather than a raw
    // "relation does not exist" from the catalog query `connect` runs
    // internally when `staging` is set. See the module doc comment.
    let migrator = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    let migration_outcome = migrator.migrate().await.map_err(|err| err.to_string());
    let migrator_shutdown = migrator.shutdown().await.map_err(|err| err.to_string());
    migration_outcome?;
    migrator_shutdown?;

    let options = TrellisOptions {
        staging: args.staging,
        drain_threads: args.drain_threads,
    };
    let trellis = Trellis::connect(config, options)
        .await
        .map_err(|err| err.to_string())?;

    println!(
        "trellis running: staging={}, drain_threads={}",
        if args.staging { "on" } else { "off" },
        args.drain_threads
    );
    println!("press Ctrl-C to stop");

    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            // Failing to even install the signal handler is unusual, but we
            // must still release the connection/background workers we
            // started rather than leaking them.
            let shutdown_outcome = trellis.shutdown().await;
            return Err(format!(
                "failed to listen for Ctrl-C: {err}{}",
                match shutdown_outcome {
                    Ok(()) => String::new(),
                    Err(shutdown_err) => format!("; additionally, shutdown failed: {shutdown_err}"),
                }
            ));
        }
    }

    println!("shutting down...");
    trellis.shutdown().await.map_err(|err| err.to_string())?;
    Ok("shut down cleanly".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_args_uses_defaults() {
        let parsed = parse(&[]).unwrap();
        assert_eq!(
            parsed,
            Args {
                staging: true,
                drain_threads: 2
            }
        );
    }

    #[test]
    fn explicit_staging_flag() {
        let args = vec!["--staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(parsed.staging);
    }

    #[test]
    fn no_staging_flag() {
        let args = vec!["--no-staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(!parsed.staging);
        // drain_threads still defaults even though staging was overridden.
        assert_eq!(parsed.drain_threads, 2);
    }

    #[test]
    fn staging_and_no_staging_together_is_an_error() {
        let args = vec!["--staging".to_string(), "--no-staging".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("mutually exclusive"));

        let args = vec!["--no-staging".to_string(), "--staging".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("mutually exclusive"));
    }

    #[test]
    fn repeated_staging_flag_is_fine() {
        let args = vec!["--staging".to_string(), "--staging".to_string()];
        let parsed = parse(&args).unwrap();
        assert!(parsed.staging);
    }

    #[test]
    fn drain_threads_parses() {
        let args = vec!["--drain-threads".to_string(), "5".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.drain_threads, 5);
    }

    #[test]
    fn drain_threads_zero_is_valid() {
        let args = vec!["--drain-threads".to_string(), "0".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.drain_threads, 0);
    }

    #[test]
    fn drain_threads_missing_value_is_an_error() {
        let args = vec!["--drain-threads".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn drain_threads_negative_is_an_error() {
        let args = vec!["--drain-threads".to_string(), "-1".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("non-negative integer"));
    }

    #[test]
    fn drain_threads_non_numeric_is_an_error() {
        let args = vec!["--drain-threads".to_string(), "banana".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("non-negative integer"));
    }

    #[test]
    fn drain_threads_specified_twice_is_an_error() {
        let args = vec![
            "--drain-threads".to_string(),
            "1".to_string(),
            "--drain-threads".to_string(),
            "2".to_string(),
        ];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("only be specified once"));
    }

    #[test]
    fn unrecognized_argument_is_an_error() {
        let args = vec!["--bogus".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("unrecognized argument"));
    }

    #[test]
    fn combined_flags_parse_in_either_order() {
        let args = vec![
            "--drain-threads".to_string(),
            "3".to_string(),
            "--no-staging".to_string(),
        ];
        let parsed = parse(&args).unwrap();
        assert_eq!(
            parsed,
            Args {
                staging: false,
                drain_threads: 3
            }
        );
    }
}
