//! `trellis migrate` — applies Trellis's migrations and re-creates any
//! out-of-date capture functions the role can ([`Trellis::migrate_with_report`]),
//! then exits. `apply` and `run` migrate first too; this is the step on its
//! own, for an upgrade whose staging worker runs as a role that doesn't own
//! the sources (docs/upgrading.md).

use trellis::{Config, Trellis, TrellisOptions};

/// Help text for `trellis migrate -h`/`--help`.
pub const USAGE: &str = "\
Usage: trellis migrate [--database-url <URL>]

Applies Trellis's schema migrations, then re-creates every capture function
that is out of date but still captures what its transforms read, and exits.
It installs, widens and drops no capture: the staging worker (`trellis run`)
does that.

Run it as the role that owns the source tables when the staging worker runs
as one that doesn't: only the owner can re-create the functions. A table it
can't bring current is printed, with why, and the command still succeeds.

Options:
  -d, --database-url <URL>  Postgres connection string. May be given before
                             or after the subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// Parsed `trellis migrate` arguments: there are none.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {}

/// Parses `trellis migrate`'s arguments, which must be empty.
pub fn parse(args: &[String]) -> Result<Args, String> {
    if args.is_empty() {
        Ok(Args {})
    } else {
        Err(format!(
            "{USAGE}\nerror: trellis migrate takes no arguments, got {}: {:?}",
            args.len(),
            args
        ))
    }
}

/// Connects, migrates and disconnects, returning what to print: a
/// confirmation, then each table whose capture functions are still out of
/// date. `shutdown` runs even when the migration failed, whose error comes
/// first.
pub async fn run(_args: Args, database_url: Option<String>) -> Result<String, String> {
    let config = Config::resolve(database_url).map_err(|err| err.to_string())?;
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .map_err(|err| err.to_string())?;
    let outcome = trellis.migrate_with_report().await;
    let shutdown_outcome = trellis.shutdown().await.map_err(|err| err.to_string());
    let migrated = outcome.map_err(|err| err.to_string())?;
    shutdown_outcome?;
    Ok(describe(&migrated))
}

/// The lines `migrate` prints for `migrated`.
pub fn describe(migrated: &trellis::Migrated) -> String {
    let mut out = String::from("migrated");
    for (_, message) in &migrated.stale_capture {
        out.push('\n');
        out.push_str(&stale_line(message));
    }
    out
}

/// One table `migrate` left out of date, as a line for the terminal.
pub fn stale_line(message: &str) -> String {
    format!("warning: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrate_takes_no_arguments() {
        assert_eq!(parse(&[]), Ok(Args {}));
        assert!(parse(&["x".to_string()]).is_err());
    }

    #[test]
    fn each_table_left_out_of_date_is_a_warning_line() {
        let mut migrated = trellis::Migrated::default();
        assert_eq!(describe(&migrated), "migrated");
        migrated.stale_capture.push((
            "public.u".to_string(),
            "T0255: the capture functions".to_string(),
        ));
        assert_eq!(
            describe(&migrated),
            "migrated\nwarning: T0255: the capture functions"
        );
    }
}
