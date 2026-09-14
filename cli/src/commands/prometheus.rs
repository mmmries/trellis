//! `trellis prometheus [--bind <ADDR>]` — a Prometheus-scrapeable HTTP
//! listener for the engine's metrics.
//!
//! There is no metrics registry in `engine` yet (tracked as issue #51, "in-
//! process registry"), and no Prometheus text-exposition support either
//! (issue #53, "Prometheus exposition via mountable render_prometheus()").
//! So this command can't actually serve metrics today. What it *can* do is
//! exist: bind a socket, accept connections, and answer every request with a
//! plain, honest "not implemented yet" instead of leaving operators/scrape
//! configs pointed at a connection refusal or (worse) a missing binary
//! subcommand. When #51/#53 land, wiring this up for real should be a small
//! diff against this module — swap the fixed response body for a rendered
//! registry snapshot — not a server built from scratch.
//!
//! Accordingly, `-d`/`--database-url` is accepted (for symmetry with the
//! other subcommands, and because real metrics will eventually need to read
//! from the database) but otherwise unused: this command never calls
//! `Config::resolve`/`Trellis::connect`. It's still validated as a
//! well-formed flag by `connection::extract_database_url` before this
//! module's `parse` ever sees argv, same as every other subcommand — a
//! malformed `-d` with no value is rejected there, not silently swallowed
//! here.
//!
//! There's deliberately no real HTTP parsing. A Prometheus scraper (or
//! `curl`) sends a bare `GET / HTTP/1.1` with a handful of headers and
//! nothing else worth reading; this drains bytes off the socket only far
//! enough to know the client is done sending its request headers (or to give
//! up under a size/time cap, so a slow or silent client can't wedge a
//! connection open indefinitely) and then writes back a fixed, valid
//! HTTP/1.1 response. Pulling in an HTTP-server crate for that would violate
//! this workspace's dependency-minimalism convention for a job this small.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Help text for `trellis prometheus -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis prometheus [--bind <ADDR>] [--database-url <URL>]

Binds an HTTP listener and answers every request with a fixed 501 response,
since the engine has no metrics registry or Prometheus exposition yet (see
issues #51 and #53). This command exists so operators/scrape configs pointed
at it get a clear answer instead of a connection refusal, and so wiring in
real metrics later doesn't require building the listener from scratch.

Options:
  --bind <ADDR>              host:port to listen on. Default:
                             127.0.0.1:9464 (the OpenTelemetry
                             Prometheus-exporter convention port).
  -d, --database-url <URL>  Accepted for symmetry with the other subcommands
                             and forward-compatibility, but currently
                             unused: this command does not connect to
                             Postgres. May be given before or after the
                             subcommand name. Falls back to
                             TRELLIS_DATABASE_URL, then PGHOST/PGPORT/PGUSER/
                             PGPASSWORD/PGDATABASE, if omitted.
  -h, --help                 Print this help and exit.
";

/// The default bind address: `127.0.0.1:9464`, the port convention used by
/// the OpenTelemetry Prometheus exporter.
const DEFAULT_BIND: &str = "127.0.0.1:9464";

/// A parsed `prometheus` invocation, once `--database-url`/`-d` has already
/// been pulled out of argv by the caller (see
/// `connection::extract_database_url`).
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub bind: SocketAddr,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND
                .parse()
                .expect("DEFAULT_BIND is a valid socket address"),
        }
    }
}

/// Parses `--bind <ADDR>`. `<ADDR>` must parse as a `SocketAddr`
/// (`host:port`, e.g. `127.0.0.1:9464` or `0.0.0.0:9464`) — a bad value is a
/// usage error, not something deferred to a confusing bind failure later.
pub fn parse(args: &[String]) -> Result<Args, String> {
    let mut bind: Option<SocketAddr> = None;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx].as_str() {
            "--bind" => {
                let Some(value) = args.get(idx + 1) else {
                    return Err(format!(
                        "{USAGE}\nerror: --bind requires a value, e.g. --bind 127.0.0.1:9464"
                    ));
                };
                if bind.is_some() {
                    return Err(format!("{USAGE}\nerror: --bind may only be specified once"));
                }
                let parsed: SocketAddr = value.parse().map_err(|_| {
                    format!(
                        "{USAGE}\nerror: --bind expects a host:port socket address, got {value:?}"
                    )
                })?;
                bind = Some(parsed);
                idx += 2;
            }
            other => {
                return Err(format!("{USAGE}\nerror: unrecognized argument {other:?}"));
            }
        }
    }

    Ok(Args {
        bind: bind.unwrap_or_else(|| Args::default().bind),
    })
}

/// The largest number of request bytes a single connection is allowed to
/// send before this gives up reading and just responds anyway. Real requests
/// (a scraper's bare `GET / HTTP/1.1` plus a few headers) are a few hundred
/// bytes at most; this cap just bounds memory/time spent on a client that
/// never sends a terminator, without needing a real HTTP parser to know when
/// the headers are "done".
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How long a single connection is allowed to spend before this gives up
/// reading its request and responds anyway — bounds a slow/silent client's
/// hold on a spawned task (though not on `accept`, since each connection
/// runs in its own task).
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The fixed response body, explaining what isn't implemented yet and
/// pointing at the tracking issues.
const RESPONSE_BODY: &str = "\
Prometheus metrics exposition is not implemented yet.

The engine has no in-process metrics registry (issue #51) or Prometheus text
exposition (issue #53) yet. This endpoint exists as a placeholder so scrape
configs and operators get a clear answer instead of a connection refusal.
";

/// Binds `args.bind`, prints a startup message, then accepts connections
/// (each handled in its own spawned task) until interrupted with Ctrl-C.
///
/// `_database_url` is intentionally unused — see the module doc comment for
/// why this command doesn't connect to Postgres.
pub async fn run(args: Args, _database_url: Option<String>) -> Result<String, String> {
    let listener = TcpListener::bind(args.bind)
        .await
        .map_err(|err| format!("failed to bind {}: {err}", args.bind))?;
    let bound_addr = listener
        .local_addr()
        .map_err(|err| format!("failed to read bound address: {err}"))?;

    println!("trellis prometheus listening on http://{bound_addr} (not implemented yet: #51/#53)");
    println!("press Ctrl-C to stop");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer_addr)) => {
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(stream).await {
                                eprintln!("trellis prometheus: connection error: {err}");
                            }
                        });
                    }
                    // A single failed accept (e.g. the peer reset before we
                    // finished accepting it) shouldn't take the whole
                    // listener down — log it and keep serving.
                    Err(err) => {
                        eprintln!("trellis prometheus: accept error: {err}");
                    }
                }
            }
            ctrl_c = tokio::signal::ctrl_c() => {
                ctrl_c.map_err(|err| format!("failed to listen for Ctrl-C: {err}"))?;
                break;
            }
        }
    }

    println!("shutting down...");
    Ok("shut down cleanly".to_string())
}

/// Drains (a bounded amount of) the request off `stream`, then writes back
/// the fixed 501 response and closes the connection. Any I/O error here is
/// this single connection's problem, not the server's — the caller logs and
/// moves on rather than propagating anything that would affect other
/// connections.
async fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    read_request(&mut stream).await?;
    let response = format!(
        "HTTP/1.1 501 Not Implemented\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {RESPONSE_BODY}",
        RESPONSE_BODY.len(),
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| format!("failed to write response: {err}"))?;
    stream
        .shutdown()
        .await
        .map_err(|err| format!("failed to close connection: {err}"))
}

/// Reads off `stream` until the request headers look complete (a
/// `\r\n\r\n` terminator has appeared), the client closes its write side, or
/// [`MAX_REQUEST_BYTES`]/[`READ_TIMEOUT`] is hit — whichever comes first.
/// There's no real HTTP parsing here (see the module doc comment): this only
/// needs to avoid hanging on, or being wedged open by, a client that never
/// finishes sending.
async fn read_request(stream: &mut TcpStream) -> Result<(), String> {
    let drain = async {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 512];
        while buf.len() < MAX_REQUEST_BYTES {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|err| format!("failed to read request: {err}"))?;
            if n == 0 {
                // Client closed its write side (or sent nothing) — nothing
                // more to drain.
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<(), String>(())
    };

    match tokio::time::timeout(READ_TIMEOUT, drain).await {
        Ok(result) => result,
        // A client that never finishes sending headers within the timeout
        // still gets the fixed response — we just stop waiting on it.
        Err(_) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_args_uses_the_default_bind_address() {
        let parsed = parse(&[]).unwrap();
        assert_eq!(parsed.bind, "127.0.0.1:9464".parse().unwrap());
    }

    #[test]
    fn explicit_bind_address_parses() {
        let args = vec!["--bind".to_string(), "0.0.0.0:9999".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.bind, "0.0.0.0:9999".parse().unwrap());
    }

    #[test]
    fn ephemeral_port_zero_parses() {
        let args = vec!["--bind".to_string(), "127.0.0.1:0".to_string()];
        let parsed = parse(&args).unwrap();
        assert_eq!(parsed.bind, "127.0.0.1:0".parse().unwrap());
    }

    #[test]
    fn bind_missing_value_is_an_error() {
        let args = vec!["--bind".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn bind_malformed_value_is_an_error() {
        for bad in ["banana", "127.0.0.1", "not-a-port:abc", ":9464"] {
            let args = vec!["--bind".to_string(), bad.to_string()];
            let err = parse(&args).unwrap_err();
            assert!(
                err.contains("host:port socket address"),
                "expected a clear error for {bad:?}, got: {err}"
            );
        }
    }

    #[test]
    fn bind_specified_twice_is_an_error() {
        let args = vec![
            "--bind".to_string(),
            "127.0.0.1:9464".to_string(),
            "--bind".to_string(),
            "127.0.0.1:9465".to_string(),
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

    // `--database-url`/`-d` is stripped out of argv by
    // `connection::extract_database_url` before this module's `parse` ever
    // runs (see main.rs's dispatch and cli/src/connection.rs) — the same as
    // every other subcommand. So a malformed `-d` with no value is already
    // covered by connection.rs's own tests; there is nothing left for this
    // module's `parse` to reject or accept for that flag. This test just
    // documents that `prometheus` takes no *other* positional arguments.
    #[test]
    fn stray_positional_argument_is_an_error() {
        let args = vec!["bogus".to_string()];
        let err = parse(&args).unwrap_err();
        assert!(err.contains("unrecognized argument"));
    }
}
