//! Statement forms and drawn transactions (#2181) against a real database,
//! with no engine: each form has its plain write's effect and outcome, and
//! each transaction shape commits its ops exactly once.

use std::collections::HashMap;
use std::time::Duration;

use super::*;
use crate::model::{Isolation, SavepointWrap};

const TABLE: &str = "t0";

fn table() -> Table {
    Table {
        name: TABLE.to_string(),
        pk_col: "id".to_string(),
        columns: vec![
            Column {
                name: "id".to_string(),
                value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
            },
            Column {
                name: "c1".to_string(),
                value_type: ValueType::Numeric,
            },
            Column {
                name: "c2".to_string(),
                value_type: ValueType::Text,
            },
        ],
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
    }
}

fn tables() -> HashMap<String, Table> {
    HashMap::from([(TABLE.to_string(), table())])
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A fresh database holding [`table`].
async fn database(cluster: &testkit::TestCluster) -> testkit::TestDatabase {
    let db = cluster.create_empty_database().await;
    let raw = connect(db.dsn()).await;
    create_source_table(&raw, None, &table())
        .await
        .expect("create the table");
    db
}

fn insert(id: i64, c1: &str, expect: OpOutcome) -> Op {
    Op::Insert {
        table: TABLE.to_string(),
        row: vec![
            ("id".to_string(), Some(id.to_string())),
            ("c1".to_string(), Some(c1.to_string())),
            ("c2".to_string(), Some(format!("r{id}"))),
        ],
        expect,
    }
}

fn update(id: i64, c1: &str, expect: OpOutcome) -> Op {
    Op::Update {
        table: TABLE.to_string(),
        pk: id.to_string(),
        changes: vec![("c1".to_string(), Some(c1.to_string()))],
        expect,
    }
}

fn delete(id: i64, expect: OpOutcome) -> Op {
    Op::Delete {
        table: TABLE.to_string(),
        pk: id.to_string(),
        expect,
    }
}

/// Every row, as `(id, c1, c2)` text.
async fn rows(raw: &tokio_postgres::Client) -> Vec<(String, String, String)> {
    raw.query("select id::text, c1::text, c2 from t0 order by id", &[])
        .await
        .expect("read")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

fn row(id: i64, c1: &str) -> (String, String, String) {
    (id.to_string(), c1.to_string(), format!("r{id}"))
}

/// The outcome `result` is, as the run classifies it.
fn outcome(result: &Result<u64, impl std::fmt::Debug>) -> OpOutcome {
    match result {
        Err(_) => OpOutcome::Fails,
        Ok(0) => OpOutcome::AffectsNoRows,
        Ok(_) => OpOutcome::Succeeds,
    }
}

/// Each statement form changes the source as its plain write does, and
/// reports the outcome the generator expects of it.
#[tokio::test]
async fn every_statement_form_has_its_plain_writes_effect_and_outcome() {
    use OpOutcome::{AffectsNoRows, Fails, Succeeds};
    let cluster = testkit::TestCluster::start();
    let db = database(&cluster).await;
    let mut raw = connect(db.dsn()).await;
    let tables = tables();
    let upsert = |op, update, expect| Op::Upsert {
        op: Box::new(op),
        update,
        expect,
    };
    let merge = |op, expect| Op::Merge {
        op: Box::new(op),
        expect,
    };
    let cte = |op, expect| Op::Cte {
        op: Box::new(op),
        expect,
    };
    let ops = [
        upsert(insert(1, "1.00", Succeeds), false, Succeeds),
        // The row exists: skipped, not overwritten.
        upsert(insert(1, "9.00", Fails), false, AffectsNoRows),
        upsert(insert(2, "2.00", Succeeds), true, Succeeds),
        upsert(update(2, "2.50", Succeeds), true, Succeeds),
        merge(insert(3, "3.00", Succeeds), Succeeds),
        // Matched: nothing.
        merge(insert(3, "9.00", Fails), AffectsNoRows),
        merge(update(3, "3.50", Succeeds), Succeeds),
        merge(update(9, "9.00", AffectsNoRows), AffectsNoRows),
        merge(delete(1, Succeeds), Succeeds),
        merge(delete(1, AffectsNoRows), AffectsNoRows),
        cte(insert(4, "4.00", Succeeds), Succeeds),
        cte(insert(4, "9.00", Fails), Fails),
        cte(update(4, "4.50", Succeeds), Succeeds),
        cte(update(9, "9.00", AffectsNoRows), AffectsNoRows),
        cte(insert(5, "5.00", Succeeds), Succeeds),
        cte(delete(5, Succeeds), Succeeds),
        cte(delete(5, AffectsNoRows), AffectsNoRows),
    ];
    for op in &ops {
        let applied = apply_op(&mut raw, &tables, op).await;
        assert_eq!(&outcome(&applied), op.expect(), "{op:?}: {applied:?}");
    }
    assert_eq!(
        rows(&raw).await,
        [row(2, "2.50"), row(3, "3.50"), row(4, "4.50")]
    );
}

fn wrap(ops: usize, isolation: Isolation) -> TxnWrap {
    TxnWrap {
        ops: (0..ops).collect(),
        isolation,
        read_first: true,
        think: Duration::ZERO,
        rehearse: false,
        savepoint: None,
    }
}

/// At every isolation level, a transaction rehearsed and rolled back, with
/// a savepoint rolled back to and a statement that fails in its own
/// savepoint, commits each op once and reports each op's own outcome.
#[tokio::test]
async fn a_wrapped_transaction_commits_each_op_once_at_every_isolation_level() {
    use OpOutcome::{AffectsNoRows, Fails, Succeeds};
    let cluster = testkit::TestCluster::start();
    let tables = tables();
    for isolation in Isolation::ALL {
        for (rehearse, rollback) in [(false, false), (true, false), (false, true), (true, true)] {
            let db = database(&cluster).await;
            let mut raw = connect(db.dsn()).await;
            let ops = [
                insert(1, "1.00", Succeeds),
                // A duplicate, which fails in its own savepoint.
                insert(1, "9.00", Fails),
                update(1, "1.50", Succeeds),
                insert(2, "2.00", Succeeds),
                delete(3, AffectsNoRows),
            ];
            let wrap = TxnWrap {
                rehearse,
                savepoint: Some(SavepointWrap {
                    start: 1,
                    end: 4,
                    rollback,
                }),
                ..wrap(ops.len(), isolation)
            };
            let applied = apply_txn(&mut raw, &tables, &wrap, &ops)
                .await
                .expect("commit");
            let outcomes: Vec<OpOutcome> = applied.results.iter().map(outcome).collect();
            let expected: Vec<OpOutcome> = ops.iter().map(|op| op.expect().clone()).collect();
            assert_eq!(outcomes, expected, "{wrap:?}: {:?}", applied.results);
            assert_eq!(
                rows(&raw).await,
                [row(1, "1.50"), row(2, "2.00")],
                "{wrap:?}"
            );
            // The session is out of the transaction.
            raw.batch_execute("begin; rollback").await.expect("idle");
        }
    }
}

/// An op the program expected to succeed that fails ends the results
/// there, and the transaction rolls back.
#[tokio::test]
async fn an_unexpected_failure_rolls_the_transaction_back() {
    use OpOutcome::Succeeds;
    let cluster = testkit::TestCluster::start();
    let db = database(&cluster).await;
    let mut raw = connect(db.dsn()).await;
    let ops = [
        insert(1, "1.00", Succeeds),
        insert(1, "1.00", Succeeds),
        insert(2, "2.00", Succeeds),
    ];
    let applied = apply_txn(
        &mut raw,
        &tables(),
        &wrap(ops.len(), Isolation::ReadCommitted),
        &ops,
    )
    .await
    .expect("rolled back, not an error");
    assert_eq!(applied.results.len(), 2, "{:?}", applied.results);
    assert!(applied.results[1].is_err(), "{:?}", applied.results);
    assert!(rows(&raw).await.is_empty());
}

/// A `REPEATABLE READ` transaction whose snapshot predates another
/// session's committed update of its row gets a serialization failure,
/// and runs again from the start, then commits.
#[tokio::test]
async fn a_serialization_failure_is_retried_from_the_start() {
    retried_after_a_concurrent_update(OpOutcome::Succeeds).await;
}

/// The same failure in an op that may fail, which runs in its own
/// savepoint, reruns the whole transaction too: it isn't the op's failure.
#[tokio::test]
async fn a_serialization_failure_in_an_op_that_may_fail_is_retried_from_the_start() {
    retried_after_a_concurrent_update(OpOutcome::AnyOf(vec![
        OpOutcome::Succeeds,
        OpOutcome::Fails,
    ]))
    .await;
}

/// [`apply_txn`] of one `REPEATABLE READ` update of row 1, expecting
/// `expect`, whose snapshot predates another session's update of the row:
/// it is retried once, then commits.
async fn retried_after_a_concurrent_update(expect: OpOutcome) {
    use OpOutcome::Succeeds;
    let cluster = testkit::TestCluster::start();
    let db = database(&cluster).await;
    let mut raw = connect(db.dsn()).await;
    let tables = tables();
    apply_op(&mut raw, &tables, &insert(1, "1.00", Succeeds))
        .await
        .expect("seed");

    // The other session holds the row's lock with an update, so the
    // wrapped transaction takes its snapshot, then waits on the lock.
    let other = connect(db.dsn()).await;
    other
        .batch_execute("begin; update t0 set c1 = 7 where id = 1")
        .await
        .expect("lock the row");
    let dsn = db.dsn().to_string();
    let wrapped = tokio::spawn(async move {
        let mut raw = connect(&dsn).await;
        let ops = [update(1, "2.00", expect)];
        apply_txn(&mut raw, &tables, &wrap(1, Isolation::RepeatableRead), &ops).await
    });
    let watcher = connect(db.dsn()).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "the wrapped transaction never waited on the row lock"
        );
        let waiting: i64 = watcher
            .query_one("select count(*) from pg_locks where not granted", &[])
            .await
            .expect("read locks")
            .get(0);
        if waiting > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    other.batch_execute("commit").await.expect("commit");

    let applied = wrapped.await.expect("task").expect("commit");
    assert_eq!(applied.results.len(), 1);
    assert!(applied.results[0].is_ok(), "{:?}", applied.results);
    assert_eq!(applied.retries, 1);
    assert_eq!(rows(&raw).await, [row(1, "2.00")]);
}
