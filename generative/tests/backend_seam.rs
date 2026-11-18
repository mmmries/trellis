//! Proves the backend seam (`generative::backend`) actually works
//! end-to-end: install a trivial 1-1 numeric-`+` definition, apply raw
//! source DML, quiesce, and read back via `snapshot()` — the property
//! declarations, hand-built pins, and meta-tests design doc §1 calls
//! `tests/` live in this crate's `tests/` directory, matching every other
//! workspace crate's convention (`trellis/tests/*`).

use generative::backend::{Backend, ManualBackend, SEAL_ON_DEMAND_INTERVAL};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use testkit::TestCluster;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};

#[tokio::test]
async fn installs_a_trivial_def_and_converges_dml_to_the_expected_snapshot() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let b = source.columns[2].name.clone();
    let target_name = pool.next_table_name();

    let def = TransformDef {
        target: target_name.clone(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(b.clone())),
            },
        }],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let program = Program {
        tables: vec![source.clone()],
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: vec![
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("1".to_string())),
                    (a.clone(), Some("10.00".to_string())),
                    (b.clone(), Some("1.50".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("2".to_string())),
                    (a.clone(), Some("20.00".to_string())),
                    (b.clone(), Some("2.00".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
        ],
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    backend.install(&program).await.expect("install program");

    for op in &program.ops {
        backend.apply(op).await.expect("apply op");
    }
    backend.quiesce().await.expect("quiesce");

    let snapshot = backend.snapshot().await.expect("snapshot");

    let target = snapshot.get(&target_name).unwrap_or_else(|| {
        panic!("target table {target_name:?} missing from snapshot: {snapshot:?}")
    });
    assert_eq!(
        target.len(),
        2,
        "both inserted rows must have converged: {target:?}"
    );
    assert_eq!(
        target["1"]["total"],
        Some("11.50".to_string()),
        "row 1's total must be 10.00 + 1.50"
    );
    assert_eq!(
        target["2"]["total"],
        Some("22.00".to_string()),
        "row 2's total must be 20.00 + 2.00"
    );

    // Update row 1 and delete row 2; both must converge in the target too.
    backend
        .apply(&Op::Update {
            table: source.name.clone(),
            pk: "1".to_string(),
            changes: vec![(a.clone(), Some("100.00".to_string()))],
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("apply update");
    backend
        .apply(&Op::Delete {
            table: source.name.clone(),
            pk: "2".to_string(),
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("apply delete");
    backend
        .quiesce()
        .await
        .expect("quiesce after update+delete");

    let snapshot = backend.snapshot().await.expect("snapshot after mutation");
    let target = &snapshot[&target_name];
    assert_eq!(target.len(), 1, "row 2 must have been deleted: {target:?}");
    assert_eq!(
        target["1"]["total"],
        Some("101.50".to_string()),
        "row 1's total must reflect the update (100.00 + 1.50)"
    );

    let source_snapshot = &snapshot[&source.name];
    assert_eq!(
        source_snapshot.len(),
        1,
        "the source table itself must reflect the same delete: {source_snapshot:?}"
    );
}

/// `ManualBackend::force_seal_active_segment` has to keep working past one
/// lap of the staging ring (4 slots, `trellis::staging::append::RING_SIZE`)
/// even when no maintenance tick runs in between to retire drained slots.
/// Seal-boundary pins that force their own seals used to fail
/// intermittently with `RingFull { ring_slot: 2 }`: a forced seal hit a
/// slot whose drained occupant was retirable, but only the engine's
/// maintenance tick ever retired anything, and whether a tick had landed in
/// that window was down to timing. With the maintenance interval pushed far
/// out, no tick runs after startup, so before the fix this failed on the
/// first lap every time.
#[tokio::test(flavor = "multi_thread")]
async fn forced_seals_lap_the_ring_without_a_maintenance_tick() {
    const RING_SIZE: usize = 4;

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let def = TransformDef {
        target: pool.next_table_name(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "copy".to_string(),
            expr: Expr::Column(a),
        }],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let program = Program {
        tables: vec![source],
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: Vec::new(),
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    let mut backend =
        ManualBackend::connect_with_options(db.dsn(), 1, Some(SEAL_ON_DEMAND_INTERVAL))
            .await
            .expect("connect manual backend");
    backend.install(&program).await.expect("install program");

    let mut previous = 0;
    for lap_seal in 0..3 * RING_SIZE {
        let sealed = backend
            .force_seal_active_segment()
            .await
            .unwrap_or_else(|err| panic!("forced seal #{lap_seal} must not fail: {err:?}"));
        assert!(
            sealed > previous,
            "seal #{lap_seal} sealed segment {sealed}, not after {previous}"
        );
        previous = sealed;
    }
}

/// A straggler row in a drained segment's slot gates convergence until the
/// segment is retired (`converged_through`'s condition 3), and with the
/// engine's housekeeping off nothing retires it while the ring isn't full: an
/// idle seal reaches back one segment only. With the straggler's segment two
/// behind an empty active segment, a plain quiesce never converges (#2135);
/// one that retires drained segments does.
#[tokio::test(flavor = "multi_thread")]
async fn a_straggler_in_a_long_drained_segment_needs_retirement_to_converge() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let def = TransformDef {
        target: pool.next_table_name(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "copy".to_string(),
            expr: Expr::Column(a),
        }],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let program = Program {
        tables: vec![source],
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: Vec::new(),
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    let mut backend =
        ManualBackend::connect_with_options(db.dsn(), 1, Some(SEAL_ON_DEMAND_INTERVAL))
            .await
            .expect("connect manual backend");
    backend.install(&program).await.expect("install program");
    backend
        .quiesce_forcing_seals()
        .await
        .expect("settle the install");

    // Three empty seals leave the active segment empty, three past the first.
    let first = backend.force_seal_active_segment().await.expect("seal");
    backend.force_seal_active_segment().await.expect("seal");
    backend.force_seal_active_segment().await.expect("seal");

    let (side, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("side connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    side.batch_execute("set search_path = trellis")
        .await
        .expect("search path");
    let drained = async {
        for _ in 0..200 {
            let undrained: i64 = side
                .query_one(
                    "select count(*) from segments where seg_seq <= $1::bigint + 2 and state <> 'drained'",
                    &[&first],
                )
                .await
                .expect("count undrained")
                .get(0);
            if undrained == 0 {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("the empty segments never drained");
    };
    drained.await;

    // A row lands in the first segment's slot after its fence: invisible to
    // it, so nothing applies it.
    let slot: i16 = side
        .query_one(
            "select ring_slot from segments where seg_seq = $1",
            &[&first],
        )
        .await
        .expect("slot of the first segment")
        .get(0);
    side.execute(
        &format!(
            "insert into seg_{slot} (src_table, key, op, hop_gen) \
             values ('stray', 'k', 'recompute', 0)"
        ),
        &[],
    )
    .await
    .expect("insert the straggler");

    // The old quiesce can't converge: nothing retires the slot.
    let plain = tokio::time::timeout(std::time::Duration::from_secs(3), backend.quiesce()).await;
    assert!(
        plain.is_err(),
        "a plain quiesce must hang on the straggler: {plain:?}"
    );

    backend.set_retire_during_quiesce(true);
    tokio::time::timeout(std::time::Duration::from_secs(20), backend.quiesce())
        .await
        .expect("the retiring quiesce must converge, not time out")
        .expect("quiesce");
}
