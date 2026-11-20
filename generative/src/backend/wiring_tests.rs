//! Every wiring shape (#2181) against a real database, with no engine: the
//! drawn ops keep their expected outcomes, plain, as statement forms and
//! in a transaction, and the shadows, the tally and the rewrite end as
//! [`crate::model::Shadow`] and [`crate::model::Rewrite`] say.

use std::collections::BTreeMap;
use std::time::Duration;

use trellis::dev::defs::ast::ValueType;

use super::super::sql::{apply_op, apply_txn, create_source_table};
use super::*;
use crate::model::{Column, Isolation, Op, OpOutcome, Rewrite, SavepointWrap, TALLY_BASE, TxnWrap};

fn table(name: &str) -> Table {
    Table {
        name: name.to_string(),
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

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn insert(table: &str, id: i64, c1: &str, expect: OpOutcome) -> Op {
    Op::Insert {
        table: table.to_string(),
        row: vec![
            ("id".to_string(), Some(id.to_string())),
            ("c1".to_string(), Some(c1.to_string())),
            ("c2".to_string(), Some(format!("r{id}"))),
        ],
        expect,
    }
}

fn update(table: &str, id: i64, c1: &str, expect: OpOutcome) -> Op {
    Op::Update {
        table: table.to_string(),
        pk: id.to_string(),
        changes: vec![("c1".to_string(), Some(c1.to_string()))],
        expect,
    }
}

fn delete(table: &str, id: i64, expect: OpOutcome) -> Op {
    Op::Delete {
        table: table.to_string(),
        pk: id.to_string(),
        expect,
    }
}

fn outcome(result: &Result<u64, impl std::fmt::Debug>) -> OpOutcome {
    match result {
        Err(_) => OpOutcome::Fails,
        Ok(0) => OpOutcome::AffectsNoRows,
        Ok(_) => OpOutcome::Succeeds,
    }
}

/// The ops on `t0`, and on `other` when it's another table: plain, every
/// statement form, and each of `t0`'s keys 1 to 5 inserted, updated and
/// deleted. `t0`'s keys left are 2, 5 and 6, once [`txn_ops`] has run.
fn ops(other: Option<&str>) -> Vec<Op> {
    use OpOutcome::{AffectsNoRows, Fails, Succeeds};
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
    let mut ops = vec![
        insert("t0", 1, "1.00", Succeeds),
        insert("t0", 2, "2.00", Succeeds),
        insert("t0", 1, "9.00", Fails),
        update("t0", 1, "1.50", Succeeds),
        update("t0", 9, "9.00", AffectsNoRows),
        upsert(insert("t0", 3, "3.00", Succeeds), true, Succeeds),
        upsert(insert("t0", 3, "9.00", Fails), false, AffectsNoRows),
        upsert(update("t0", 2, "2.50", Succeeds), true, Succeeds),
        merge(insert("t0", 4, "4.00", Succeeds), Succeeds),
        merge(update("t0", 4, "4.50", Succeeds), Succeeds),
        merge(delete("t0", 3, Succeeds), Succeeds),
        cte(insert("t0", 5, "5.00", Succeeds), Succeeds),
        cte(update("t0", 5, "5.50", Succeeds), Succeeds),
        cte(delete("t0", 1, Succeeds), Succeeds),
        delete("t0", 9, AffectsNoRows),
        delete("t0", 2, Succeeds),
        insert("t0", 2, "2.00", Succeeds),
    ];
    if let Some(other) = other {
        ops.extend([
            insert(other, 1, "1.00", Succeeds),
            insert(other, 2, "2.00", Succeeds),
            update(other, 1, "1.25", Succeeds),
            delete(other, 2, Succeeds),
            insert(other, 1, "9.00", Fails),
        ]);
    }
    ops
}

/// A transaction inserting key 6 and deleting key 4.
fn txn_ops() -> Vec<Op> {
    use OpOutcome::{Fails, Succeeds};
    vec![
        insert("t0", 6, "6.00", Succeeds),
        update("t0", 6, "6.50", Succeeds),
        insert("t0", 6, "9.00", Fails),
        delete("t0", 4, Succeeds),
    ]
}

/// The shadow table's rows past [`SHADOW_OFFSET`] and below [`TALLY_BASE`],
/// key less the offset to `lnk`, and how many tally rows it has.
async fn shadows(raw: &tokio_postgres::Client, table: &str) -> (BTreeMap<i64, Option<i64>>, i64) {
    let shadows = raw
        .query(
            &format!(
                "select id - {SHADOW_OFFSET}, lnk from {table} \
                 where id >= {SHADOW_OFFSET} and id < {TALLY_BASE}"
            ),
            &[],
        )
        .await
        .expect("read the shadows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    let tally: i64 = raw
        .query_one(
            &format!("select count(*) from {table} where id >= {TALLY_BASE}"),
            &[],
        )
        .await
        .expect("read the tally")
        .get(0);
    (shadows, tally)
}

/// `t0`'s own rows, key to `c1`.
async fn rows(raw: &tokio_postgres::Client) -> BTreeMap<i64, String> {
    raw.query(
        &format!("select id, c1::text from t0 where id < {SHADOW_OFFSET}"),
        &[],
    )
    .await
    .expect("read t0")
    .into_iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect()
}

/// Every shadow shape, in `t0` itself and in another table, with each
/// rewrite: every op keeps its outcome; each live key of `t0` has its
/// shadow, linked when an `AFTER` trigger keeps it; a gone key's shadow is
/// gone, unless a nulling key unlinked it; a statement trigger keeps one
/// tally row for the one connection; and the rewrite flips the even keys.
#[tokio::test]
async fn every_wiring_keeps_each_ops_outcome_and_its_shadows() {
    let cluster = testkit::TestCluster::start();
    let mut shapes = Vec::new();
    for statement in [false, true] {
        for (timing, fk) in [
            (Timing::Before, None),
            (Timing::After, None),
            (Timing::After, Some(FkAction::Cascade)),
            (Timing::After, Some(FkAction::SetNull)),
        ] {
            for same in [true, false] {
                shapes.push(Some((statement, timing, fk, same)));
            }
        }
    }
    shapes.extend([None, None]);
    for (index, shape) in shapes.into_iter().enumerate() {
        let db = cluster.create_empty_database().await;
        let mut raw = connect(db.dsn()).await;
        let tables: HashMap<String, Table> = ["t0", "t1"]
            .into_iter()
            .map(|t| (t.to_string(), table(t)))
            .collect();
        for t in tables.values() {
            create_source_table(&raw, None, t).await.expect("create");
        }
        let shadow = shape.map(|(statement, timing, fk, same)| Shadow {
            table: if same { "t0" } else { "t1" }.to_string(),
            statement,
            timing,
            before_capture: same,
            fk,
            template: vec![
                ("c1".to_string(), Some("0.00".to_string())),
                ("c2".to_string(), None),
            ],
            toggle: (
                "c1".to_string(),
                Some("0.00".to_string()),
                Some("1.00".to_string()),
            ),
        });
        let rewrite = match (shadow.is_some(), index % 3) {
            (true, 0) => None,
            (_, 1) => Some(Timing::Before),
            _ => Some(Timing::After),
        };
        let wiring = Wiring {
            table: "t0".to_string(),
            shadow: shadow.clone(),
            rewrite: rewrite.map(|timing| Rewrite {
                column: "c1".to_string(),
                timing,
                values: (Some("7.00".to_string()), Some("8.00".to_string())),
            }),
        };
        let ddl = wiring_ddl(&tables, &wiring);
        raw.batch_execute(&ddl)
            .await
            .unwrap_or_else(|e| panic!("{wiring:?}: {e:?}\n{ddl}"));
        let other = shadow
            .as_ref()
            .map(|s| s.table.as_str())
            .filter(|t| *t != "t0");
        for op in ops(other) {
            let applied = apply_op(&mut raw, &tables, &op).await;
            assert_eq!(
                &outcome(&applied),
                op.expect(),
                "{wiring:?} {op:?}: {applied:?}"
            );
        }
        let ops = txn_ops();
        let wrap = TxnWrap {
            ops: (0..ops.len()).collect(),
            isolation: Isolation::ALL[index % 3],
            read_first: true,
            think: Duration::ZERO,
            rehearse: true,
            savepoint: Some(SavepointWrap {
                start: 0,
                end: 2,
                rollback: true,
            }),
        };
        let applied = apply_txn(&mut raw, &tables, &wrap, &ops)
            .await
            .expect("commit");
        let outcomes: Vec<OpOutcome> = applied.results.iter().map(outcome).collect();
        let expected: Vec<OpOutcome> = ops.iter().map(|op| op.expect().clone()).collect();
        assert_eq!(outcomes, expected, "{wiring:?}: {:?}", applied.results);

        let live = rows(&raw).await;
        let values: Vec<&str> = if wiring.rewrite.is_some() {
            vec!["7.00", "5.50", "7.00"]
        } else {
            vec!["2.00", "5.50", "6.50"]
        };
        assert_eq!(
            live.iter()
                .map(|(k, v)| (*k, v.as_str()))
                .collect::<Vec<_>>(),
            [2, 5, 6].into_iter().zip(values).collect::<Vec<_>>(),
            "{wiring:?}: the rewrite flips the even keys"
        );
        let Some(shadow) = shadow else { continue };
        let (shadows, tally) = shadows(&raw, &shadow.table).await;
        let mut expected = BTreeMap::new();
        if !(shadow.statement && shadow.timing == Timing::Before) {
            for k in 1..=6 {
                let link = (shadow.timing == Timing::After).then_some(k);
                if live.contains_key(&k) {
                    expected.insert(k, link);
                } else if shadow.fk == Some(FkAction::SetNull) {
                    expected.insert(k, None);
                }
            }
        }
        assert_eq!(shadows, expected, "{wiring:?}");
        assert_eq!(tally, i64::from(shadow.statement), "{wiring:?}");
    }
}
