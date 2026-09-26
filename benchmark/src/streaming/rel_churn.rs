//! Issue #558 experiment 4: to-side churn against a relationship aggregate.
//!
//! `children(id, grp, parent, val)` references `parents(id, weight)` through
//! `RELATIONSHIP parent`; the target is `GROUP BY grp SELECT SUM(parent.weight),
//! SUM(val), COUNT(*)`. Every parent update changes the contribution of each
//! of its children, so it is the reverse path's cost that this probe measures:
//! paced single-row parent updates (`--parent-rate`) and, at the same time,
//! paced single-row child updates (`--child-rate`, 0 to disable), for the
//! offer window, from `offer.connections` writers each. The verdict is how
//! long the target takes to equal a from-scratch oracle once the window
//! closes.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::{Rng, SeedableRng};
use testkit::TestCluster;
use tokio_postgres::Client as RawClient;

use crate::scenario::connect_raw;
use crate::streaming::chain::{numeric_columns, wait_for_live};
use crate::streaming::contention;
use crate::streaming::idle_cost;
use crate::streaming::throughput::Offer;
use crate::streaming::tuning::EngineTuning;

const GROUPS: i64 = 1000;
const SETUP_TIMEOUT: Duration = Duration::from_secs(600);
const ORACLE_POLL: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub struct RelChurnResult {
    pub children_per_parent: usize,
    pub parents: usize,
    pub total_children: usize,
    pub parent_rate: f64,
    pub child_rate: f64,
    pub application_threads: usize,
    pub connections: usize,
    pub offered_duration_secs: f64,
    pub parent_updates_issued: u64,
    pub child_updates_issued: u64,
    /// Seconds from the offer window opening until the target equalled the
    /// oracle, or `None` if it never did inside the grace period.
    pub converged_secs: Option<f64>,
    /// `converged_secs - offered_duration_secs`, clamped at zero: the backlog
    /// the reverse path still had to clear once load stopped.
    pub tail_secs: Option<f64>,
    pub oracle_ok: Option<bool>,
    pub oracle_mismatched_groups: Option<i64>,
    pub wal_bytes: i64,
    pub deadlocks: i64,
    pub xact_rollbacks: i64,
    pub ledger_mode: String,
}

impl RelChurnResult {
    pub fn to_json(&self, scenario: &str) -> String {
        fn opt_f(v: Option<f64>) -> String {
            v.map(|x| format!("{x:.3}"))
                .unwrap_or_else(|| "null".into())
        }
        fn opt_b(v: Option<bool>) -> String {
            v.map(|x| x.to_string()).unwrap_or_else(|| "null".into())
        }
        fn opt_i(v: Option<i64>) -> String {
            v.map(|x| x.to_string()).unwrap_or_else(|| "null".into())
        }
        format!(
            "{{\"scenario\":\"{}\",\"children_per_parent\":{},\"parents\":{},\"total_children\":{},\
             \"parent_rate\":{},\"child_rate\":{},\"application_threads\":{},\"connections\":{},\
             \"offered_duration_secs\":{:.3},\"parent_updates_issued\":{},\"child_updates_issued\":{},\
             \"converged_secs\":{},\"tail_secs\":{},\"oracle_ok\":{},\"oracle_mismatched_groups\":{},\
             \"wal_bytes\":{},\"deadlocks\":{},\"xact_rollbacks\":{},\"ledger_mode\":\"{}\"}}",
            scenario,
            self.children_per_parent,
            self.parents,
            self.total_children,
            self.parent_rate,
            self.child_rate,
            self.application_threads,
            self.connections,
            self.offered_duration_secs,
            self.parent_updates_issued,
            self.child_updates_issued,
            opt_f(self.converged_secs),
            opt_f(self.tail_secs),
            opt_b(self.oracle_ok),
            opt_i(self.oracle_mismatched_groups),
            self.wal_bytes,
            self.deadlocks,
            self.xact_rollbacks,
            self.ledger_mode,
        )
    }
}

/// Paced single-row updates: commit `k` is due at `k / rate` seconds into
/// the window, taken by whichever of `connections` writers is free; each
/// picks a uniformly random id below `ids`. Returns how many were issued.
async fn paced_updates(
    dsn: &str,
    sql: &str,
    ids: i64,
    rate: f64,
    connections: usize,
    start: Instant,
    duration: Duration,
) -> u64 {
    if rate <= 0.0 || ids <= 0 {
        return 0;
    }
    let next = Arc::new(AtomicU64::new(0));
    let issued = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(connections);
    for w in 0..connections {
        let dsn = dsn.to_string();
        let sql = sql.to_string();
        let next = next.clone();
        let issued = issued.clone();
        tasks.push(tokio::spawn(async move {
            let client = connect_raw(&dsn).await;
            let stmt = client.prepare(&sql).await.expect("prepare paced update");
            let mut rng = rand::rngs::StdRng::seed_from_u64(0x558 + w as u64);
            loop {
                let k = next.fetch_add(1, Ordering::Relaxed);
                let due = start + Duration::from_secs_f64(k as f64 / rate);
                if due >= start + duration {
                    break;
                }
                tokio::time::sleep_until(tokio::time::Instant::from_std(due)).await;
                let id: i64 = rng.random_range(0..ids);
                client.execute(&stmt, &[&id]).await.expect("paced update");
                issued.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for t in tasks {
        t.await.expect("paced update worker");
    }
    issued.load(Ordering::Relaxed)
}

/// Groups where the target disagrees with the oracle (`None` = equal).
async fn mismatched_groups(raw: &RawClient, terminal: &str, oracle_sql: &str) -> i64 {
    raw.query_one(
        &format!(
            "select count(*) from ({oracle_sql}) o \
             full outer join public.{terminal} t on t.grp = o.grp \
             where t.grp is null or o.grp is null \
                or t.weight_total is distinct from o.weight_total \
                or t.val is distinct from o.val \
                or t.row_count is distinct from o.row_count"
        ),
        &[],
    )
    .await
    .expect("compare relationship aggregate against oracle")
    .get(0)
}

async fn wait_for_oracle(
    raw: &RawClient,
    terminal: &str,
    oracle_sql: &str,
    deadline: Instant,
) -> bool {
    while Instant::now() < deadline {
        if mismatched_groups(raw, terminal, oracle_sql).await == 0 {
            return true;
        }
        tokio::time::sleep(ORACLE_POLL).await;
    }
    false
}

pub async fn run_probe(
    children_per_parent: usize,
    total_children: usize,
    parent_rate: f64,
    child_rate: f64,
    offer: Offer,
    tuning: &EngineTuning,
) -> RelChurnResult {
    assert!(children_per_parent >= 1);
    let parents = (total_children / children_per_parent).max(1);
    let total_children = parents * children_per_parent;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let sampler = connect_raw(db.dsn()).await;

    raw.batch_execute(&format!(
        "create table public.parents (id bigint primary key, weight numeric not null); \
         create table public.children (id bigint primary key, grp bigint not null, \
             parent bigint not null, val numeric); \
         create index children_parent on public.children (parent); \
         alter table public.parents replica identity full; \
         alter table public.children replica identity full; \
         insert into public.parents select g, g from generate_series(0, {} ) g; \
         insert into public.children select g, g % {GROUPS}, g % {parents}, g \
             from generate_series(0, {}) g; \
         analyze public.parents; analyze public.children;",
        parents as i64 - 1,
        total_children as i64 - 1,
    ))
    .await
    .expect("create and load parent/child tables");

    let client = trellis::Client::start(db.dsn(), tuning.client_options()).expect("client start");

    trellis::dev::defs::create_relationship(
        &db.pool,
        "RELATIONSHIP parent FROM children.parent TO parents.id",
    )
    .await
    .expect("create relationship");
    let columns: HashMap<_, _> = numeric_columns(&["id", "grp", "parent", "val"]);
    let source_text = "TRANSFORM child_totals FROM public.children GROUP BY grp \
         SELECT grp AS grp, SUM(parent.weight) AS weight_total, SUM(val) AS val, COUNT(*) AS row_count";
    let def = trellis::dev::defs::install_definition(&db.pool, source_text, &columns, "public")
        .await
        .expect("install relationship aggregate definition");
    let terminal = def.def.target.clone();
    // The oracle needs the relationship resolved by name, as the tests do.
    let rels = HashMap::from([(
        "parent".to_string(),
        trellis::dev::defs::ast::RelationshipDef {
            name: "parent".to_string(),
            from_table: "children".to_string(),
            from_col: "parent".to_string(),
            to_table: "parents".to_string(),
            to_col: "id".to_string(),
        },
    )]);
    let oracle_sql =
        trellis::dev::defs::oracle::render_aggregate_relationship_select_sql(&def.def, &rels);

    wait_for_live(&raw, &terminal, Instant::now() + SETUP_TIMEOUT).await;
    assert!(
        wait_for_oracle(&raw, &terminal, &oracle_sql, Instant::now() + SETUP_TIMEOUT).await,
        "the backfilled target never matched the oracle"
    );

    let (deadlocks_before, rollbacks_before) = contention::deadlocks_and_rollbacks(&sampler).await;
    let wal_start = idle_cost::wal_lsn(&sampler).await;
    let offer_start = Instant::now();
    let (parent_updates_issued, child_updates_issued) = tokio::join!(
        paced_updates(
            db.dsn(),
            "update public.parents set weight = weight + 1 where id = $1",
            parents as i64,
            parent_rate,
            offer.connections,
            offer_start,
            offer.duration,
        ),
        paced_updates(
            db.dsn(),
            "update public.children set val = val + 1 where id = $1",
            total_children as i64,
            child_rate,
            offer.connections,
            offer_start,
            offer.duration,
        ),
    );
    let converged =
        wait_for_oracle(&raw, &terminal, &oracle_sql, Instant::now() + offer.grace).await;
    let converged_secs = converged.then(|| offer_start.elapsed().as_secs_f64());
    let wal_bytes = idle_cost::wal_bytes_since(&sampler, &wal_start).await;
    let (deadlocks_after, rollbacks_after) = contention::deadlocks_and_rollbacks(&sampler).await;
    let mismatched = mismatched_groups(&raw, &terminal, &oracle_sql).await;

    client.shutdown().await.expect("client shutdown");

    RelChurnResult {
        children_per_parent,
        parents,
        total_children,
        parent_rate,
        child_rate,
        application_threads: tuning.application_threads,
        connections: offer.connections,
        offered_duration_secs: offer.duration.as_secs_f64(),
        parent_updates_issued,
        child_updates_issued,
        converged_secs,
        tail_secs: converged_secs.map(|c| (c - offer.duration.as_secs_f64()).max(0.0)),
        oracle_ok: Some(mismatched == 0),
        oracle_mismatched_groups: Some(mismatched),
        wal_bytes,
        deadlocks: deadlocks_after - deadlocks_before,
        xact_rollbacks: rollbacks_after - rollbacks_before,
        ledger_mode: std::env::var("TRELLIS_EXP558_LEDGER").unwrap_or_else(|_| "off".into()),
    }
}
