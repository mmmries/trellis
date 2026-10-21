//! PROTOTYPE for #1226 ideas A and B. NOT FOR MERGE: measurement scaffolding
//! only. Selected by `TRELLIS_PROTO`:
//!
//! * unset / `base`: today's `TestCluster::start()` + `create_isolated_database`.
//! * `A`: a migrated *template data directory* (built once per process from a
//!   throwaway cluster via the same stop / copy / start `cold_backup` does) is
//!   copied per test (`cp -a --reflink=auto`; `TRELLIS_PROTO_CP=plain` uses
//!   `cp -Rp`, which is what ext4 CI would get), and each test starts its own
//!   postmaster on the copy. The isolated database is `CREATE DATABASE ...
//!   TEMPLATE` from the migrated template database inside the copy.
//! * `B`: a small pool of long-lived clusters (at most `MAX_LIVE_CLUSTERS`),
//!   each leased to one test at a time, each holding a migrated template
//!   database; a test gets a fresh database from it and the cluster goes back
//!   to the pool when the test ends. Concurrent tests never share a cluster
//!   (so the cluster-wide xid snapshot the seal gate reads, `pg_stat_activity`
//!   scans and roles stay as private as today for the test's duration);
//!   sequential tests do. The pool is torn down by an `atexit` hook.
//!
//! `TRELLIS_PROTO_STRATEGY=file_copy` selects `STRATEGY FILE_COPY` for the
//! template clone (default `WAL_LOG`).

use super::*;
use std::ops::Deref;
use std::sync::OnceLock;

const TEMPLATE_DB: &str = "trellis_template";

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    Base,
    A,
    B,
}

pub fn mode() -> Mode {
    match std::env::var("TRELLIS_PROTO").as_deref() {
        Ok("A") => Mode::A,
        Ok("B") => Mode::B,
        _ => Mode::Base,
    }
}

fn migrate_template(cluster: &TestCluster) {
    cluster.create_database(TEMPLATE_DB);
    let dsn = cluster.database_dsn(TEMPLATE_DB);
    // Own thread + runtime: the caller may already be inside a tokio runtime.
    std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let config = Config::from_dsn(dsn.clone()).expect("valid dsn");
                let pool = Pool::new(&config).expect("pool");
                migrate(&pool, &config).await.expect("migrate template");
                drop(pool);
            });
        });
    });
}

fn clone_template(cluster: &TestCluster, name: &str) {
    let strategy = std::env::var("TRELLIS_PROTO_STRATEGY").unwrap_or_default();
    for attempt in 0..100 {
        let mut cmd = Command::new("createdb");
        cmd.arg("-h")
            .arg(&cluster.socket_dir)
            .arg("-p")
            .arg(cluster.port.to_string())
            .arg("-U")
            .arg("postgres")
            .arg("-T")
            .arg(TEMPLATE_DB);
        if strategy == "file_copy" {
            cmd.arg("-S").arg("file_copy");
        }
        cmd.arg(name);
        let out = cmd.output().expect("createdb");
        if out.status.success() {
            return;
        }
        // The template's migrating connections close asynchronously.
        assert!(attempt < 99, "createdb -T failed: {out:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------- idea A

static A_TEMPLATE: OnceLock<ClusterBackup> = OnceLock::new();

fn cp_data_dir(from: &Path, to: &Path) {
    if std::env::var("TRELLIS_PROTO_CP").as_deref() == Ok("plain") {
        copy_data_dir(from, to);
    } else {
        run_to_completion(
            Command::new("cp")
                .arg("-a")
                .arg("--reflink=auto")
                .arg(from)
                .arg(to),
            "reflink copy data directory",
        );
    }
}

fn a_template() -> &'static ClusterBackup {
    A_TEMPLATE.get_or_init(|| {
        reap_orphans_once();
        let tmp = TestCluster::start_listening_in(&std::env::temp_dir(), false);
        migrate_template(&tmp);
        let backup = tmp.cold_backup();
        register_exit_hook();
        backup
    })
}

pub(super) fn start_from_template() -> TestCluster {
    reap_orphans_once();
    let template = a_template();
    let permit = CLUSTERS.acquire();
    let root = std::env::temp_dir().join(format!("trellis-testkit-{}", unique_suffix()));
    let data_dir = root.join("data");
    let socket_dir = root.join("sock");
    fs::create_dir_all(&socket_dir).expect("create socket dir");
    let t = Instant::now();
    cp_data_dir(&template.data_dir(), &data_dir);
    timing("a_copy", t);
    fs::write(root.join("from_template"), b"").expect("marker");
    let log_path = root.join("postgres.log");
    let t = Instant::now();
    let server = spawn_server(&data_dir, &socket_dir, 5432, false, &log_path);
    let cluster = TestCluster {
        root,
        data_dir,
        socket_dir,
        port: 5432,
        tcp: false,
        server: Mutex::new(server),
        _permit: permit,
    };
    cluster.wait_ready(&log_path);
    timing("a_postmaster_ready", t);
    cluster
}

// ---------------------------------------------------------------- idea B

struct Pooled {
    idle: Vec<TestCluster>,
    live: usize,
}
static B_POOL: Mutex<Pooled> = Mutex::new(Pooled {
    idle: Vec::new(),
    live: 0,
});
static B_FREED: Condvar = Condvar::new();

/// A cluster on loan from the pool; goes back on drop.
pub struct Lease {
    cluster: Option<TestCluster>,
}

impl Deref for Lease {
    type Target = TestCluster;
    fn deref(&self) -> &TestCluster {
        self.cluster.as_ref().expect("leased")
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(c) = self.cluster.take() {
            B_POOL
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .idle
                .push(c);
            B_FREED.notify_one();
        }
    }
}

extern "C" fn b_exit_hook() {
    let mut pool = B_POOL.lock().unwrap_or_else(PoisonError::into_inner);
    pool.idle.clear(); // drops each TestCluster: stops postgres, removes the dir
    if let Some(b) = A_TEMPLATE.get() {
        let _ = fs::remove_dir_all(b.root());
    }
}

fn register_exit_hook() {
    static ONCE: Once = Once::new();
    unsafe extern "C" {
        fn atexit(cb: extern "C" fn()) -> i32;
    }
    ONCE.call_once(|| unsafe {
        atexit(b_exit_hook);
    });
}

fn lease() -> Lease {
    let t_wait = Instant::now();
    let mut pool = B_POOL.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        if let Some(c) = pool.idle.pop() {
            drop(pool);
            timing("b_lease_wait", t_wait);
            return Lease { cluster: Some(c) };
        }
        if pool.live < proto_max_clusters().unwrap_or(MAX_LIVE_CLUSTERS) {
            pool.live += 1;
            drop(pool);
            register_exit_hook();
            let c = TestCluster::start();
            let t = Instant::now();
            migrate_template(&c);
            timing("b_template_build", t);
            timing("b_lease_wait", t_wait);
            return Lease { cluster: Some(c) };
        }
        pool = B_FREED.wait(pool).unwrap_or_else(PoisonError::into_inner);
    }
}

// ------------------------------------------------------- unified handle

/// What a prototype-aware fixture holds instead of a bare `TestCluster`.
pub enum ProtoCluster {
    Own(TestCluster),
    Templated(TestCluster),
    Leased(Lease),
}

impl Deref for ProtoCluster {
    type Target = TestCluster;
    fn deref(&self) -> &TestCluster {
        match self {
            ProtoCluster::Own(c) | ProtoCluster::Templated(c) => c,
            ProtoCluster::Leased(l) => l,
        }
    }
}

impl ProtoCluster {
    pub fn start() -> Self {
        match mode() {
            Mode::Base => ProtoCluster::Own(TestCluster::start()),
            Mode::A => ProtoCluster::Templated(start_from_template()),
            Mode::B => ProtoCluster::Leased(lease()),
        }
    }

    pub async fn create_isolated_database(&self) -> TestDatabase {
        match self {
            ProtoCluster::Own(c) => c.create_isolated_database().await,
            ProtoCluster::Templated(c) => clone_db(c),
            ProtoCluster::Leased(l) => clone_db(l),
        }
    }
}

pub(super) fn clone_db(c: &TestCluster) -> TestDatabase {
    let name = format!("trellis_test_{}", unique_suffix());
    let t = Instant::now();
    clone_template(c, &name);
    timing("clone_template", t);
    c.test_database(name)
}

/// Clusters started from the template carry the `trellis_template` database.
pub(super) fn is_template_cluster(c: &TestCluster) -> bool {
    c.root.join("from_template").exists()
}
