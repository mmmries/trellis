//! Proves the manual backend now builds a definition's target through the
//! direct, set-based backfill path (issue #63 M3) rather than the ring
//! enumeration `create_definition` used to stage.
//!
//! The backend seam always creates a source table empty and installs its
//! definitions in the same `install` call, so a definition's backfill normally
//! runs over an empty source. To exercise the direct build over *populated*
//! data — the whole point of M3 — this drives `install` in two phases against
//! one backend: first install the source table alone and seed rows into it,
//! then install the definition, whose backfill must find and build those
//! already-present rows directly. A final live update proves the ring still
//! folds a post-build CDC delta onto the directly-built row (the build/CDC
//! fence).
//!
//! **Why the mid-test `quiesce` is load-bearing.** Phase 1's `install` starts
//! the engine client (its source-table set is non-empty), which creates the
//! replication slot; the seed inserts that follow are therefore captured by
//! CDC and staged into the ring as `Recompute` markers. Without draining them
//! first, those markers stay *pending* — and the moment phase 2 persists the
//! definition, the applier folds them onto the freshly-created target,
//! computing the very same values the direct build would. That masks M3
//! completely: the test would still pass even if `install_definition`'s
//! `backfill_definition` call were a no-op (the CDC fold, not the direct
//! build, would be doing the work). Quiescing *before* the definition exists
//! drains those seed markers while no definition references the source, so
//! they fold into nothing and leave the ring empty. After that, the direct
//! build is the *only* thing that can populate the target — which is exactly
//! the real M3 production scenario (a new definition installed over a source
//! already live under CDC with pre-existing rows), and what makes this test
//! actually discriminate: no-op the direct build and it fails.

use engine::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use generative::backend::{Backend, ManualBackend};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use testkit::TestCluster;

#[tokio::test]
async fn direct_backfill_builds_the_target_from_preexisting_source_rows() {
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
    };

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");

    // Phase 1: install only the source table (no definition yet), then seed two
    // rows into it. These rows exist in the source *before* the definition is
    // created, so the definition's backfill — not CDC — is what must build them
    // into the target.
    let source_only = Program {
        tables: vec![source.clone()],
        defs: vec![],
        def_install_after_op: vec![],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&source_only)
        .await
        .expect("install source table");

    for (pk, av, bv) in [("1", "10.00", "1.50"), ("2", "20.00", "2.00")] {
        backend
            .apply(&Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some(pk.to_string())),
                    (a.clone(), Some(av.to_string())),
                    (b.clone(), Some(bv.to_string())),
                ],
                expect: OpOutcome::Succeeds,
            })
            .await
            .expect("seed source row");
    }

    // Drain the CDC markers the seed inserts staged, *before* any definition
    // exists to fold them onto. This is what forces the target to be built by
    // the direct backfill alone in phase 2 rather than by a still-pending CDC
    // fold — see this module's doc comment. Remove it and the test silently
    // stops testing M3 (it would pass even against a no-op direct build).
    backend
        .quiesce()
        .await
        .expect("quiesce to drain seed CDC before the definition exists");

    // Phase 2: install the definition against the now-populated source. With
    // the seed markers already drained, its target can only be built by the
    // direct backfill.
    let def_only = Program {
        tables: vec![],
        defs: vec![def],
        def_install_after_op: vec![0],
        ops: vec![],
        restart_after_ops: vec![],
        scale_out_after_ops: vec![],
    };
    backend
        .install(&def_only)
        .await
        .expect("install definition (runs the direct backfill)");
    backend.quiesce().await.expect("quiesce after backfill");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot.get(&target_name).unwrap_or_else(|| {
        panic!("target table {target_name:?} missing from snapshot: {snapshot:?}")
    });
    assert_eq!(
        target.len(),
        2,
        "the direct backfill must have built both pre-existing source rows: {target:?}"
    );
    assert_eq!(
        target["1"]["total"],
        Some("11.50".to_string()),
        "row 1's backfilled total must be 10.00 + 1.50"
    );
    assert_eq!(
        target["2"]["total"],
        Some("22.00".to_string()),
        "row 2's backfilled total must be 20.00 + 2.00"
    );

    // A live update after the build must fold onto the directly-built row via
    // the ring — the build/CDC fence the direct path relies on.
    backend
        .apply(&Op::Update {
            table: source.name.clone(),
            pk: "1".to_string(),
            changes: vec![(a.clone(), Some("100.00".to_string()))],
            expect: OpOutcome::Succeeds,
        })
        .await
        .expect("apply post-backfill update");
    backend.quiesce().await.expect("quiesce after update");

    let snapshot = backend.snapshot().await.expect("snapshot after update");
    let target = &snapshot[&target_name];
    assert_eq!(
        target["1"]["total"],
        Some("101.50".to_string()),
        "a post-backfill CDC update must converge onto the directly-built row (100.00 + 1.50)"
    );
}
