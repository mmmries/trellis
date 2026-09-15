//! `trellis prometheus [--bind <ADDR>]` — a Prometheus-scrapeable HTTP
//! listener for the engine's metrics.
//!
//! Issue #51 added the in-process metrics registry and issue #53 added its
//! public, embedder-facing exposition —
//! [`trellis::metrics::Metrics::render_prometheus`] — so this command now
//! renders and serves the real thing: every request gets a `200 OK` with the
//! registry's current contents in Prometheus text exposition format, read
//! fresh at request time.
//!
//! **This process's own registry, nothing more.** `render_prometheus()` (see
//! its doc comment, and ADR-0009 decision 1) reads whichever in-process
//! registry this `trellis` binary's own recorder calls have populated — it
//! is not a network read against some other, already-running engine
//! process. Run standalone (this command's only mode today), that registry
//! stays empty forever: nothing else in this process calls
//! `trellis::staging`/`trellis::client` to actually record observations, so
//! a scrape here returns a valid but metric-less body. That's an accurate
//! answer for this command's current shape, not a placeholder — Prometheus's
//! text format is well-defined for an empty registry (an empty body). The
//! design point of [`trellis::metrics::Metrics::render_prometheus`] (`docs/observability.md`'s
//! "mountable handler, not a bound port") is that a *real* deployment mounts
//! this same render call from inside the process that's actually running the
//! engine (a [`trellis::Trellis`]/[`trellis::Client`] with `staging`/
//! `drain_threads` set) — e.g. behind an axum/actix/hyper `/metrics` route in
//! that same binary — rather than scraping a separate `trellis prometheus`
//! process. This standalone listener remains useful as a drop-in scrape
//! target when that's the topology wanted (or for smoke-testing the
//! exposition format itself), and demonstrates the few lines that wiring
//! takes.
//!
//! Accordingly, `-d`/`--database-url` is accepted (for symmetry with the
//! other subcommands) but otherwise unused: this command never calls
//! `Config::resolve`/`Trellis::connect` — the metrics registry it reads is
//! purely in-process and needs no database connection. It's still validated
//! as a well-formed flag by `connection::extract_database_url` before this
//! module's `parse` ever sees argv, same as every other subcommand — a
//! malformed `-d` with no value is rejected there, not silently swallowed
//! here.
//!
//! There's deliberately no real HTTP parsing. A Prometheus scraper (or
//! `curl`) sends a bare `GET / HTTP/1.1` with a handful of headers and
//! nothing else worth reading; this drains bytes off the socket only far
//! enough to know the client is done sending its request headers (or to give
//! up under a size/time cap, so a slow or silent client can't wedge a
//! connection open indefinitely) and then writes back a fixed-status,
//! rendered-body HTTP/1.1 response. Pulling in an HTTP-server crate for that
//! would violate this workspace's dependency-minimalism convention for a job
//! this small — and per ADR-0009 decision 1, `trellis` itself deliberately
//! never links an HTTP server (`metrics-exporter-prometheus`'s Hyper-listener
//! feature is off); this hand-rolled listener lives here in the CLI, not in
//! the library.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use trellis::metrics::Metrics;

/// Help text for `trellis prometheus -h`/`--help`, and prefixed to any
/// argument-parsing error so a mistake also shows correct usage.
pub const USAGE: &str = "\
Usage: trellis prometheus [--bind <ADDR>] [--database-url <URL>]

Binds an HTTP listener and answers every request with this process's own
in-process metrics registry, rendered in Prometheus text exposition format
(issues #51/#53). Since this command runs standalone rather than alongside a
running engine, the registry it renders is normally empty — see this
module's doc comment for the intended topology (mounting render_prometheus()
inside the process actually running the engine, rather than scraping this
command).

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

/// The `Content-Type` Prometheus's own text exposition format expects
/// (https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md).
const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

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

    println!("trellis prometheus listening on http://{bound_addr}");
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
/// the registry's current contents — rendered fresh for this request via
/// [`Metrics::render_prometheus`] — as a `200 OK` and closes the connection.
/// Any I/O error here is this single connection's problem, not the server's
/// — the caller logs and moves on rather than propagating anything that
/// would affect other connections.
async fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    read_request(&mut stream).await?;
    let body = Metrics::new().render_prometheus();
    let response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {PROMETHEUS_CONTENT_TYPE}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len(),
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
        // still gets a response — we just stop waiting on it.
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

    /// End-to-end (within this process) check that this command really does
    /// serve the registry now, rather than the old fixed 501 body: records
    /// one observation directly against `trellis::metrics` (the same global
    /// registry `Metrics::render_prometheus` reads — no `Trellis`/Postgres
    /// connection needed for this), drives `handle_connection` over a real
    /// loopback socket, and checks the response is a `200 OK` with the
    /// Prometheus content type whose body contains that observation.
    #[tokio::test]
    async fn serves_the_rendered_registry_as_a_200_with_the_prometheus_content_type() {
        trellis::metrics::increment_changes_applied("prometheus_cli_test_target");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener.local_addr().expect("listener has a local address");
        tokio::spawn(async move {
            let (stream, _peer_addr) = listener.accept().await.expect("accept one connection");
            handle_connection(stream)
                .await
                .expect("handle_connection succeeds");
        });

        let mut stream = TcpStream::connect(addr)
            .await
            .expect("connect to the listener");
        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .expect("write the request");
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .await
            .expect("read the whole response before the server closes the connection");
        let response = String::from_utf8(buf).expect("response is valid utf-8");

        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "expected a 200, got: {response}"
        );
        assert!(
            response.contains(&format!("Content-Type: {PROMETHEUS_CONTENT_TYPE}")),
            "missing the Prometheus content type header: {response}"
        );
        assert!(
            response.contains("trellis_changes_applied_total"),
            "response body missing the recorded metric: {response}"
        );
        assert!(
            response.contains("prometheus_cli_test_target"),
            "response body missing the recorded label: {response}"
        );
    }
}
