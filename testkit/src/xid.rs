//! Synthetic transaction-id age for a [`TestCluster`].
//!
//! Burning ids one transaction at a time runs at about a million a second
//! here, so reaching the 32-bit epoch boundary that way takes over an hour.
//! [`TestCluster::age_xids_to_epoch_edge`] gets there in seconds instead: it
//! freezes every database, stops the server, moves the next transaction id
//! forward with `pg_resetwal -x`, and starts it again. Every row keeps its
//! raw `xmin`, frozen, as on a real instance that has run for years.
//!
//! A jump stays inside Postgres's wraparound protection: it never puts the
//! oldest `datfrozenxid` more than `2^31 - WRAP_MARGIN` ids behind the next
//! id. A target further away takes more than one freeze-and-jump round.
//!
//! The next id always lands on a commit-log page boundary ([`XID_PAGE`]).
//! Postgres zeroes a commit-log page when it assigns the page's first id;
//! landing mid-page on a page that was never written leaves the page
//! missing, and the first commit fails.

use std::path::Path;
use std::process::Command;

use crate::cluster::{StopMode, TestCluster};

/// Transaction ids per commit-log page (`BLCKSZ` 8192 at four ids a byte).
pub const XID_PAGE: u32 = 32_768;
/// Full transaction ids per 32-bit epoch.
const EPOCH: u64 = 1 << 32;
/// How far short of the wraparound stop limit (`2^31` ids past the oldest
/// `datfrozenxid`, less a few million) a jump stops. Also keeps clear of
/// the 40-million-id warning band.
const WRAP_MARGIN: u64 = 100_000_000;

/// What [`TestCluster::age_xids_to_epoch_edge`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct XidAging {
    /// The next full transaction id before.
    pub from: u64,
    /// The next full transaction id after.
    pub to: u64,
    /// How many freeze-and-jump rounds it took (restarts of the server).
    pub rounds: u32,
}

impl TestCluster {
    /// `VACUUM FREEZE` in every database, `template0` included (it is
    /// opened to connections for the vacuum and closed again). A database
    /// dropped meanwhile is skipped. Tuples a running transaction can still
    /// see stay unfrozen: their database's `datfrozenxid` says so, and
    /// [`TestCluster::age_xids_to_epoch_edge`] reads it.
    pub fn freeze_every_database(&self) {
        let rows = self.psql(
            "postgres",
            "select datname || chr(9) || datallowconn::text from pg_database",
        );
        for row in rows.lines().filter(|l| !l.is_empty()) {
            let (name, allowed) = row.split_once('\t').expect("datname and datallowconn");
            let ident = format!("\"{}\"", name.replace('"', "\"\""));
            let closed = allowed != "true";
            if closed {
                self.psql(
                    "postgres",
                    &format!("alter database {ident} allow_connections true"),
                );
            }
            let vacuumed = self.try_psql(name, "vacuum freeze");
            if closed {
                self.psql(
                    "postgres",
                    &format!("alter database {ident} allow_connections false"),
                );
            }
            if let Err(stderr) = vacuumed
                && !stderr.contains("does not exist")
            {
                panic!("vacuum freeze of {name} failed:\n{stderr}");
            }
        }
    }

    /// The next full transaction id the server will assign
    /// (`pg_snapshot_xmax(pg_current_snapshot())`; assigns none itself).
    pub fn next_xid(&self) -> u64 {
        self.psql(
            "postgres",
            "select pg_snapshot_xmax(pg_current_snapshot())::text",
        )
        .trim()
        .parse()
        .expect("next xid")
    }

    /// Moves the next transaction id forward to `lead` ids short of the next
    /// 32-bit epoch boundary (the following one when it is already within
    /// `lead`), freezing every database before each jump. Each round
    /// restarts the server, severing every connection, as
    /// [`TestCluster::restart`] does.
    ///
    /// After it, autovacuum sees every database as overdue for an
    /// anti-wraparound vacuum (`pg_resetwal` records an old `oldestXid`) and
    /// runs one until it reads the real `datfrozenxid`s.
    ///
    /// # Panics
    ///
    /// When `lead` is not a positive multiple of [`XID_PAGE`], or a jump
    /// finds no headroom (a transaction held open across the freeze).
    pub fn age_xids_to_epoch_edge(&self, lead: u32) -> XidAging {
        assert!(
            lead > 0 && lead.is_multiple_of(XID_PAGE),
            "lead {lead} must be a positive multiple of {XID_PAGE}"
        );
        let from = self.next_xid();
        let mut target = (from / EPOCH + 1) * EPOCH - u64::from(lead);
        if target <= from {
            target += EPOCH;
        }
        let mut rounds = 0;
        loop {
            self.freeze_every_database();
            let next = self.next_xid();
            let oldest: u64 = self
                .psql("postgres", "select max(age(datfrozenxid)) from pg_database")
                .trim()
                .parse()
                .expect("oldest datfrozenxid age");
            let headroom = (EPOCH / 2).saturating_sub(WRAP_MARGIN + oldest);
            let reach = (next + headroom) / u64::from(XID_PAGE) * u64::from(XID_PAGE);
            let step = target.min(reach);
            assert!(
                step > next,
                "no room to move the next xid past {next}: the oldest datfrozenxid is {oldest} \
                 ids old"
            );
            self.reset_next_xid(step);
            rounds += 1;
            if step == target {
                return XidAging {
                    from,
                    to: self.next_xid(),
                    rounds,
                };
            }
        }
    }

    /// Assigns and commits `count` transaction ids, each in a transaction of
    /// its own, from one `DO` block.
    pub fn burn_xids(&self, count: u32) {
        if count == 0 {
            return;
        }
        self.psql(
            "postgres",
            &format!(
                "do $$ begin for i in 1..{count} loop \
                     perform pg_current_xact_id(); commit; \
                 end loop; end $$"
            ),
        );
    }

    /// Stops the server (fast), sets its next full transaction id to `next`
    /// with `pg_resetwal`, and starts it again. `next` must be on a
    /// commit-log page boundary.
    fn reset_next_xid(&self, next: u64) {
        assert!(
            next.is_multiple_of(u64::from(XID_PAGE)),
            "{next} is not on a page boundary"
        );
        let (epoch, xid) = (next / EPOCH, next % EPOCH);
        let data_dir = self.data_dir().to_path_buf();
        self.while_stopped(StopMode::Fast, || reset_wal(&data_dir, epoch, xid));
    }

    /// Runs `sql` as the bootstrap superuser on database `db`, panicking on
    /// failure; returns its unaligned, tuples-only output.
    fn psql(&self, db: &str, sql: &str) -> String {
        self.try_psql(db, sql)
            .unwrap_or_else(|stderr| panic!("psql on {db} failed: {sql}\n{stderr}"))
    }

    fn try_psql(&self, db: &str, sql: &str) -> Result<String, String> {
        let output = Command::new("psql")
            .arg("-h")
            .arg(self.socket_dir())
            .arg("-p")
            .arg(self.port().to_string())
            .args(["-U", "postgres", "-d", db, "-XAtq", "-v", "ON_ERROR_STOP=1"])
            .arg("-c")
            .arg(sql)
            .output()
            .unwrap_or_else(|e| panic!("failed to run psql: {e}"));
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(String::from_utf8_lossy(&output.stderr).into_owned())
        }
    }
}

fn reset_wal(data_dir: &Path, epoch: u64, xid: u64) {
    let output = Command::new("pg_resetwal")
        .arg("-e")
        .arg(epoch.to_string())
        .arg("-x")
        .arg(xid.to_string())
        .arg("-D")
        .arg(data_dir)
        .output()
        .unwrap_or_else(|e| panic!("failed to run pg_resetwal: {e}"));
    assert!(
        output.status.success(),
        "pg_resetwal failed: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// From a fresh cluster the edge is more than one jump away. A frozen
    /// row keeps its raw `xmin`, so after the jumps its `age` has wrapped
    /// negative; the cluster then crosses into epoch 1 by burning ids.
    #[test]
    fn a_fresh_cluster_reaches_the_epoch_edge_and_crosses_it() {
        let cluster = TestCluster::start();
        cluster.psql(
            "postgres",
            "create table t (k int primary key); insert into t values (1)",
        );
        let aging = cluster.age_xids_to_epoch_edge(XID_PAGE);
        assert_eq!(aging.to, EPOCH - u64::from(XID_PAGE), "{aging:?}");
        assert!(aging.rounds >= 2, "{aging:?}");
        let age: i64 = cluster
            .psql("postgres", "select age(xmin) from t")
            .trim()
            .parse()
            .unwrap();
        assert!(age < 0, "a frozen row more than 2^31 ids old: age {age}");

        cluster.burn_xids(XID_PAGE);
        cluster.psql("postgres", "insert into t values (2)");
        assert!(cluster.next_xid() > EPOCH);
        assert_eq!(
            cluster.psql("postgres", "select count(*) from t").trim(),
            "2"
        );
    }
}
