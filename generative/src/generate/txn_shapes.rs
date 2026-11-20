//! Transaction shapes and statement forms (#2181, epic #2180 step 2): the
//! concurrent tiers' lanes draw transactions around runs of their ops
//! ([`with_transactions`]), and some single-row writes are issued as
//! `INSERT … ON CONFLICT`, `MERGE` or a writable CTE
//! ([`with_statement_forms`]).
//!
//! # Seeds stay what they were
//!
//! Both are derived from a digest of the drawn program
//! ([`where_clause::digest`]), never drawn by proptest, as the `WHERE` arm
//! is: every strategy draws the program it drew before, and only how its
//! ops are issued changes.
//!
//! # The oracle is unchanged
//!
//! Neither changes what the source holds at quiescence. A statement form
//! has its plain write's effect ([`crate::model::Op::write`]), and a
//! transaction's ops are on its lane's keys, which no other lane writes. A
//! rolled-back transaction or savepoint runs its ops again before the
//! commit ([`crate::model::TxnWrap`]), so it is an oracle identity, and
//! every later op's outcome is what the program expects.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use super::where_clause;
use crate::model::{
    ConcurrentPlan, Isolation, Op, OpOutcome, Program, SavepointWrap, Table, TxnWrap,
};

/// How many eligible ops in 256 get a statement form.
pub const FORM_SHARE: u64 = 40;

/// How many lane positions in 256 start a transaction.
pub const TXN_SHARE: u64 = 24;

/// The most ops one transaction wraps.
pub const MAX_TXN_OPS: usize = 5;

/// How many transactions in 256 wait between their statements, and the
/// longest wait. A wait spans seals, so a `REPEATABLE READ` snapshot taken
/// by the first read is several seal flips old by the first write.
pub const THINK_SHARE: u64 = 32;
pub const MAX_THINK: Duration = Duration::from_millis(40);

/// [`with_statement_forms`], then [`with_transactions`]: what every
/// concurrent tier's case strategy calls last.
pub fn widen(program: Program, plan: ConcurrentPlan) -> (Program, ConcurrentPlan) {
    let program = with_statement_forms(program);
    let plan = with_transactions(plan, &program);
    (program, plan)
}

/// [`widen`] on a concurrent case.
pub fn widen_case(mut case: super::ConcurrentCase) -> super::ConcurrentCase {
    (case.program, case.plan) = widen(case.program, case.plan);
    case
}

/// [`widen`] on a `JOIN` tier's case.
pub fn widen_join(mut case: super::join::JoinCase) -> super::join::JoinCase {
    (case.program, case.plan) = widen(case.program, case.plan);
    case
}

/// [`widen`] on a star case.
pub fn widen_star(mut star: super::join_one_to_one::StarCase) -> super::join_one_to_one::StarCase {
    star.case = widen_join(star.case);
    star
}

/// One step of SplitMix64.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The statement forms an op can take, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Form {
    UpsertNothing,
    UpsertUpdate,
    Merge,
    Cte,
}

/// Whether `table` takes statement forms: a plain table keyed by its
/// primary key alone. `ON CONFLICT` names that key, and `MERGE` moving a row
/// between partitions stays pinned out (#942).
fn takes_forms(table: &Table) -> bool {
    table.partitioning.is_none() && table.unique_cols.is_empty() && table.unique_sets.is_empty()
}

/// Rewrites some of `program`'s single-row writes into a statement form
/// with the same source effect (see [`Op::Upsert`], [`Op::Merge`] and
/// [`Op::Cte`]), [`FORM_SHARE`] in 256 of those that can take one. Which
/// forms an op can take depends on whether its row exists, replayed from
/// the program in order: an insert that the program expects to fail on an
/// existing row becomes a `DO NOTHING` or a `MERGE` that matches and does
/// nothing, expected to affect no rows; an update of a row that doesn't
/// exist is never upserted, which would insert it.
pub fn with_statement_forms(mut program: Program) -> Program {
    let digest = where_clause::digest(&program);
    let tables: HashMap<String, Table> = program
        .tables
        .iter()
        .map(|t| (t.name.clone(), t.clone()))
        .collect();
    let mut present: HashMap<String, HashSet<String>> = HashMap::new();
    for (index, op) in program.ops.iter_mut().enumerate() {
        let table = op.table().to_string();
        let spec = &tables[&table];
        let rows = present.entry(table.clone()).or_default();
        let existed = row_key(op, spec).is_some_and(|pk| rows.contains(&pk));
        replay(op, spec, rows);
        if !takes_forms(spec) {
            continue;
        }
        let draw = mix(digest ^ mix(index as u64 + 0x2181));
        if draw % 256 >= FORM_SHARE {
            continue;
        }
        let forms = forms_for(op, spec, existed);
        if forms.is_empty() {
            continue;
        }
        let (form, expect) = forms[(mix(draw) % forms.len() as u64) as usize].clone();
        let inner = Box::new(op.clone());
        *op = match form {
            Form::UpsertNothing => Op::Upsert {
                op: inner,
                update: false,
                expect,
            },
            Form::UpsertUpdate => Op::Upsert {
                op: inner,
                update: true,
                expect,
            },
            Form::Merge => Op::Merge { op: inner, expect },
            Form::Cte => Op::Cte { op: inner, expect },
        };
    }
    program
}

/// The primary-key value a plain single-row write names.
fn row_key(op: &Op, table: &Table) -> Option<String> {
    match op {
        Op::Insert { row, .. } => row
            .iter()
            .find(|(c, _)| *c == table.pk_col)
            .and_then(|(_, v)| v.clone()),
        Op::Update { pk, .. } | Op::Delete { pk, .. } => Some(pk.clone()),
        _ => None,
    }
}

/// Replays `op`'s effect on which rows of its table exist, as its expected
/// outcome says it goes.
fn replay(op: &Op, table: &Table, rows: &mut HashSet<String>) {
    let succeeds = matches!(op.expect(), OpOutcome::Succeeds);
    match op.write() {
        Op::Insert { .. } if succeeds => {
            rows.extend(row_key(op.write(), table));
        }
        Op::BulkInsert { rows: new, .. } if succeeds => {
            for row in new {
                rows.extend(
                    row.iter()
                        .find(|(c, _)| *c == table.pk_col)
                        .and_then(|(_, v)| v.clone()),
                );
            }
        }
        Op::Update { pk, changes, .. } if succeeds => {
            if let Some((_, Some(new))) = changes.iter().find(|(c, _)| *c == table.pk_col) {
                rows.remove(pk);
                rows.insert(new.clone());
            }
        }
        Op::Delete { pk, .. } if succeeds => {
            rows.remove(pk);
        }
        Op::Truncate { .. } => rows.clear(),
        Op::Leaf { op, .. } => replay(op, table, rows),
        _ => {}
    }
}

/// The forms `op` can take, each with the outcome it then expects.
fn forms_for(op: &Op, table: &Table, existed: bool) -> Vec<(Form, OpOutcome)> {
    use OpOutcome::{AffectsNoRows, Fails, Succeeds};
    match (op, op.expect()) {
        (Op::Insert { .. }, Succeeds) => vec![
            (Form::UpsertNothing, Succeeds),
            (Form::UpsertUpdate, Succeeds),
            (Form::Merge, Succeeds),
            (Form::Cte, Succeeds),
        ],
        (Op::Insert { .. }, Fails) if existed => vec![
            (Form::UpsertNothing, AffectsNoRows),
            (Form::Merge, AffectsNoRows),
            (Form::Cte, Fails),
        ],
        (Op::Update { changes, .. }, expect)
            if !changes.is_empty() && changes.iter().all(|(c, _)| *c != table.pk_col) =>
        {
            match expect {
                Succeeds => vec![
                    (Form::UpsertUpdate, Succeeds),
                    (Form::Merge, Succeeds),
                    (Form::Cte, Succeeds),
                ],
                AffectsNoRows => vec![(Form::Merge, AffectsNoRows), (Form::Cte, AffectsNoRows)],
                Fails => vec![(Form::Cte, Fails)],
                OpOutcome::AnyOf(_) => Vec::new(),
            }
        }
        (Op::Delete { .. }, Succeeds | AffectsNoRows) => vec![
            (Form::Merge, op.expect().clone()),
            (Form::Cte, op.expect().clone()),
        ],
        (Op::Insert { .. } | Op::Update { .. } | Op::Delete { .. }, Fails) => {
            vec![(Form::Cte, Fails)]
        }
        _ => Vec::new(),
    }
}

/// Whether `op` can run inside a drawn transaction: anything but a
/// `TRUNCATE` (whose row count runs a transaction of its own) and partition
/// maintenance (`DETACH … CONCURRENTLY` refuses a transaction block).
fn wrappable(op: &Op) -> bool {
    !matches!(op, Op::Truncate { .. } | Op::Partition { .. })
}

/// Adds transactions to `plan` ([`ConcurrentPlan::txns`]): at
/// [`TXN_SHARE`] in 256 of each lane's positions, a transaction over the
/// next one to [`MAX_TXN_OPS`] wrappable ops of the lane, in the burst, at
/// a drawn isolation level. Most read their first op's row first; some
/// wait [`MAX_THINK`] at most between statements ([`THINK_SHARE`]); one in
/// eight runs once and rolls back first; one in three has a savepoint, half
/// of those rolled back to and run again.
pub fn with_transactions(mut plan: ConcurrentPlan, program: &Program) -> ConcurrentPlan {
    let digest = where_clause::digest(program) ^ 0x0021_812a;
    let mut txns = Vec::new();
    for lane in plan.bursts.iter().flat_map(|b| &b.lanes) {
        let mut at = 0;
        while at < lane.len() {
            let draw = mix(digest ^ mix(lane[at] as u64 + 1));
            if draw % 256 >= TXN_SHARE || !wrappable(&program.ops[lane[at]]) {
                at += 1;
                continue;
            }
            let want = 1 + (mix(draw) % MAX_TXN_OPS as u64) as usize;
            let len = lane[at..]
                .iter()
                .take(want)
                .take_while(|&&i| wrappable(&program.ops[i]))
                .count();
            let ops = lane[at..at + len].to_vec();
            at += len;
            let d = mix(draw ^ 0x5eed);
            let isolation = Isolation::ALL[(d % 3) as usize];
            let read_first = match isolation {
                Isolation::ReadCommitted => (d >> 8).is_multiple_of(2),
                _ => !(d >> 8).is_multiple_of(4),
            };
            let think = if (d >> 16) % 256 < THINK_SHARE {
                let millis = 1 + (d >> 24) % MAX_THINK.as_millis() as u64;
                Duration::from_millis(millis)
            } else {
                Duration::ZERO
            };
            let rehearse = (d >> 32).is_multiple_of(8);
            let savepoint = (d >> 36).is_multiple_of(3).then(|| {
                let start = ((d >> 40) as usize) % len;
                let end = start + 1 + ((d >> 44) as usize) % (len - start);
                SavepointWrap {
                    start,
                    end,
                    rollback: (d >> 50).is_multiple_of(2),
                }
            });
            txns.push(TxnWrap {
                ops,
                isolation,
                read_first,
                think,
                rehearse,
                savepoint,
            });
        }
    }
    plan.txns = txns;
    plan
}

/// The transaction and statement-form shapes `program` and `plan` draw, by
/// name, each paired with the key space of every definition reading the
/// table it writes ([`crate::run::Coverage::txn_shapes`]).
pub fn shapes(program: &Program, plan: &ConcurrentPlan) -> HashSet<(&'static str, &'static str)> {
    let mut key_spaces: HashMap<&str, Vec<&'static str>> = HashMap::new();
    for def in &program.defs {
        key_spaces
            .entry(def.source.as_str())
            .or_default()
            .push(crate::run::key_space_name(&def.key_space));
        if let trellis::dev::defs::ast::KeySpace::Join(join) = &def.key_space {
            for j in &join.joins {
                key_spaces.entry(j.right.as_str()).or_default().push("Join");
            }
        }
        if let trellis::dev::defs::ast::KeySpace::JoinOneToOne(join) = &def.key_space {
            for j in &join.joins {
                key_spaces
                    .entry(j.right.as_str())
                    .or_default()
                    .push("JoinOneToOne");
            }
        }
    }
    let mut out = HashSet::new();
    let mut add = |op: &Op, shape: &'static str| {
        for space in key_spaces.get(op.table()).into_iter().flatten() {
            out.insert((shape, *space));
        }
    };
    for op in &program.ops {
        if let Some(name) = form_name(op) {
            add(op, name);
        }
    }
    for wrap in &plan.txns {
        let first = &program.ops[wrap.ops[0]];
        let mut names = vec![match wrap.isolation {
            Isolation::ReadCommitted => "read_committed",
            Isolation::RepeatableRead => "repeatable_read",
            Isolation::Serializable => "serializable",
        }];
        if wrap.rehearse {
            names.push("rolled_back");
        }
        match wrap.savepoint {
            Some(SavepointWrap { rollback: true, .. }) => names.push("savepoint_rollback"),
            Some(SavepointWrap {
                rollback: false, ..
            }) => names.push("savepoint_release"),
            None => {}
        }
        if wrap.read_first && wrap.isolation != Isolation::ReadCommitted && !wrap.think.is_zero() {
            names.push("stale_snapshot_write");
        }
        if wrap
            .ops
            .iter()
            .any(|&i| program.ops[i].expect().matches(&OpOutcome::Fails))
        {
            names.push("failing_statement");
        }
        for name in names {
            add(first, name);
        }
    }
    out
}

/// The name of `op`'s statement form, if it has one.
pub fn form_name(op: &Op) -> Option<&'static str> {
    let skipped = op.expect() == &OpOutcome::AffectsNoRows;
    Some(match (op, op.write()) {
        (Op::Upsert { update: false, .. }, _) if skipped => "upsert_nothing_conflict",
        (Op::Upsert { update: false, .. }, _) => "upsert_nothing",
        (Op::Upsert { update: true, .. }, Op::Insert { .. }) => "upsert_update_insert",
        (Op::Upsert { update: true, .. }, _) => "upsert_update",
        (Op::Merge { .. }, Op::Insert { .. }) if skipped => "merge_insert_matched",
        (Op::Merge { .. }, Op::Insert { .. }) => "merge_insert",
        (Op::Merge { .. }, Op::Update { .. }) => "merge_update",
        (Op::Merge { .. }, _) => "merge_delete",
        (Op::Cte { .. }, Op::Insert { .. }) => "cte_insert",
        (Op::Cte { .. }, Op::Update { .. }) => "cte_update",
        (Op::Cte { .. }, _) => "cte_delete",
        _ => return None,
    })
}

/// Every name [`shapes`] reports.
pub const SHAPES: &[&str] = &[
    "read_committed",
    "repeatable_read",
    "serializable",
    "rolled_back",
    "savepoint_rollback",
    "savepoint_release",
    "stale_snapshot_write",
    "failing_statement",
    "upsert_nothing",
    "upsert_nothing_conflict",
    "upsert_update_insert",
    "upsert_update",
    "merge_insert",
    "merge_insert_matched",
    "merge_update",
    "merge_delete",
    "cte_insert",
    "cte_update",
    "cte_delete",
];
