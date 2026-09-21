//! Programmatic N-hop 1-1 transform chain builder (issue #266's B0
//! prerequisite (f)): `<prefix>_src -> <prefix>_h1 -> ... -> <prefix>_hN`,
//! each hop a plain 1-1 passthrough (`SELECT val AS val`) so the chain's own
//! shape contributes as little compute cost as possible — B1's ladder is
//! measuring hop *count*, not per-hop transform complexity.
//!
//! Every table here is created explicitly qualified into `public` (the
//! benchmark crate's raw connections pin `search_path` to `trellis, public`,
//! matching `trellis`'s own integration tests — see `apply.rs`/
//! `intake_core.rs`'s `connect_raw` convention — under which an *unqualified*
//! `CREATE TABLE` lands in `trellis`, the first schema in that search path,
//! not `public`; explicit qualification here sidesteps that entirely rather
//! than relying on it).
//!
//! Deliberately goes through [`trellis::dev::defs::install_definition`] —
//! the real, product-facing front door — not the direct, ring-bypassing
//! `backfill_definition` `benchmark/src/scenario.rs` uses. That's the whole
//! point of this harness: exercising CDC intake -> ring append -> seal ->
//! claim -> fold -> apply, which the direct build skips entirely (ADR-0007).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio_postgres::Client as RawClient;
use trellis::Pool;
use trellis::dev::defs::{ValueType, install_definition};

pub fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// One programmatically-built N-hop chain: the source table's bare name and
/// each hop's bare target-table name, in hop order. Every table physically
/// lives in `public` (see this module's doc comment).
pub struct Chain {
    pub source: String,
    pub hops: Vec<String>,
}

impl Chain {
    /// The terminal hop — the only transform in the chain
    /// `trellis_end_to_end_latency_seconds` ever fires for (ADR-0009:
    /// end-to-end latency is keyed by terminal transform only).
    pub fn terminal(&self) -> &str {
        self.hops
            .last()
            .expect("a chain always has at least one hop")
    }
}

/// Creates `<prefix>_src (id bigint primary key, val numeric)`, bare — no
/// transforms yet. Split out from [`install_chain_hops`] because
/// `ClientOptions::source_tables` (and the `ALTER PUBLICATION ADD TABLE` a
/// `staging_worker: true` `Client::start` issues against it) needs this
/// table to physically exist *before* the client starts, while the chain's
/// transforms can only be installed *after* — see [`install_chain_hops`]'s
/// doc comment.
pub async fn create_chain_source_table(raw: &RawClient, prefix: &str) -> String {
    let source = format!("{prefix}_src");
    raw.batch_execute(&format!(
        "create table public.{source} (id bigint primary key, val numeric)"
    ))
    .await
    .unwrap_or_else(|e| panic!("create chain source table public.{source}: {e}"));
    source
}

/// Installs `depth` chained 1-1 transforms on top of `source` (already
/// created via [`create_chain_source_table`]).
///
/// Must be called with a `Client` (`staging_worker: true`, at least one
/// `application_thread`) already running against `pool`'s database: with no
/// source rows yet, each hop's backfill enumerates zero chunks, but flipping
/// `Backfilling` -> `Live` (`docs/decisions/0007`'s "Backgrounding and
/// resumability" amendment) still needs a running drain worker to claim and
/// finish that (empty) chunk queue — there's no synchronous fallback path.
pub async fn install_chain_hops(pool: &Pool, source: &str, depth: usize) -> Chain {
    assert!(depth >= 1, "a chain needs at least one hop");
    let prefix = source
        .strip_suffix("_src")
        .expect("source table name must be `<prefix>_src`, from create_chain_source_table");

    let columns = numeric_columns(&["id", "val"]);
    let mut hops = Vec::with_capacity(depth);
    let mut current_source = format!("public.{source}");

    for i in 1..=depth {
        let target = format!("{prefix}_h{i}");
        let source_text = format!("TRANSFORM {target} FROM {current_source} SELECT val AS val");
        let def = install_definition(pool, &source_text, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install_definition({source_text:?}) failed: {e}"));
        assert_eq!(
            def.def.target, target,
            "install_definition must keep the bare target name"
        );
        hops.push(target.clone());
        current_source = format!("public.{target}");
    }

    Chain {
        source: source.to_string(),
        hops,
    }
}

/// Polls `transform_definitions.status` for every hop in `chain` until each
/// reaches `'live'`, or panics after `timeout`. Must run against a database
/// with a `Client` (`application_threads > 0`) already running, or this
/// blocks forever (see [`install_chain_hops`]'s doc comment).
pub async fn wait_for_chain_live(raw: &RawClient, chain: &Chain, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    for target in &chain.hops {
        loop {
            let status: Option<String> = raw
                .query_opt(
                    "select status from transform_definitions where target_table = $1",
                    &[&format!("public.{target}")],
                )
                .await
                .expect("read transform_definitions.status")
                .map(|row| row.get(0));
            if status.as_deref() == Some("live") {
                break;
            }
            if Instant::now() >= deadline {
                panic!(
                    "chain hop {target} never reached 'live' status within {timeout:?} \
                     (last observed status: {status:?})"
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Sends one reserved-id row (`id = -1`) through the whole chain and waits
/// for it to land in the terminal hop, proving CDC intake -> ring -> seal ->
/// claim -> fold -> apply is flowing end to end (in particular, that every
/// intermediate hop has actually joined the publication via the periodic
/// `reconcile_source_tables` — see `ClientOptions::reconcile_interval`'s doc
/// comment) before a benchmark starts its timed measurement window. `-1` is
/// never reused by [`crate::streaming::load::run_controlled_load`], whose
/// ids start at 1, so this row never collides with — or gets counted
/// among — the load generator's own rows.
pub async fn warm_up(raw: &RawClient, chain: &Chain, timeout: Duration) {
    const WARM_UP_ID: i64 = -1;
    raw.execute(
        &format!(
            "insert into public.{} (id, val) values ($1::bigint, $1::numeric)",
            chain.source
        ),
        &[&WARM_UP_ID],
    )
    .await
    .expect("insert warm-up row");

    let terminal = chain.terminal();
    let deadline = Instant::now() + timeout;
    loop {
        let seen = raw
            .query_opt(
                &format!("select 1 from public.{terminal} where id = $1"),
                &[&WARM_UP_ID],
            )
            .await
            .expect("poll warm-up row")
            .is_some();
        if seen {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "warm-up row never reached the terminal hop public.{terminal} within {timeout:?} \
                 — the chain isn't flowing end to end (check reconcile_interval/publication \
                 membership before trusting any measurement from this run)"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
