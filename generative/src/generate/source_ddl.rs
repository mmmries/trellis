//! Source-DDL plans for a program (#2184), and privilege-change plans
//! (#2182): which [`SourceDdl`] fires after which op, on which table, with
//! how many ops in flight around it.

use proptest::prelude::*;
use trellis::dev::defs::IntWidth;
use trellis::dev::defs::ast::{KeySpace, TransformDef, ValueType};

use super::wiring::with_wiring;
use crate::model::{
    Collation, ConcurrentPlan, IndexChange, Op, OpOutcome, Program, RetypeShape, SourceDdl,
    SourceDdlEvent, SourceDdlPlan, SqlType, Table, reads,
};

/// The most events one plan draws.
pub const MAX_SOURCE_DDL_EVENTS: usize = 2;

/// The most ops one event's window spans.
pub const MAX_SOURCE_DDL_WINDOW: usize = 3;

/// One drawn event before it is fitted to the program: where it falls as a
/// share of the ops, which action, the table and column picks as indexes
/// taken modulo what the program has, the window and the pass.
type Draw = (u16, u8, usize, usize, usize, bool);

/// A plan for `program`: up to [`MAX_SOURCE_DDL_EVENTS`] events, in op
/// order and never overlapping, each on one of the program's tables that a
/// definition reads. A
/// column rewrite takes one of the table's numeric columns other than its
/// primary key. Draws that don't fit (an event past the last op, or one
/// inside the previous one's window) are dropped, so a plan can be empty.
pub fn source_ddl_plan_for(program: &Program) -> impl Strategy<Value = SourceDdlPlan> + use<> {
    plan_for(program, Menu::Maintenance)
}

/// [`source_ddl_plan_for`]'s privilege and ownership changes
/// ([`SourceDdl::privilege`]) in place of its schema and maintenance ones.
/// A drain refusal names a target reading the event's table.
pub fn privilege_plan_for(program: &Program) -> impl Strategy<Value = SourceDdlPlan> + use<> {
    plan_for(program, Menu::Privilege)
}

/// Which actions a plan draws from.
#[derive(Debug, Clone, Copy)]
enum Menu {
    Maintenance,
    Privilege,
}

impl Menu {
    fn actions(self) -> u8 {
        match self {
            Menu::Maintenance => 26,
            Menu::Privilege => 11,
        }
    }
}

fn plan_for(program: &Program, menu: Menu) -> impl Strategy<Value = SourceDdlPlan> + use<> {
    let program = program.clone();
    let draw = (
        any::<u16>(),
        0u8..menu.actions(),
        any::<usize>(),
        any::<usize>(),
        0..=MAX_SOURCE_DDL_WINDOW,
        any::<bool>(),
    );
    // A privilege change's window is the application's own session's.
    let lanes = match menu {
        Menu::Maintenance => prop_oneof![Just(1), 2..=super::MAX_LANES].boxed(),
        Menu::Privilege => Just(1).boxed(),
    };
    (
        prop::collection::vec(draw, 0..=MAX_SOURCE_DDL_EVENTS),
        lanes,
    )
        .prop_map(move |(draws, lanes)| SourceDdlPlan {
            lanes,
            ..fit(&program, &draws, menu)
        })
}

fn fit(program: &Program, draws: &[Draw], menu: Menu) -> SourceDdlPlan {
    let ops = program.ops.len();
    let mut events: Vec<SourceDdlEvent> = Vec::new();
    let mut placed: Vec<(usize, &Draw)> = draws
        .iter()
        .map(|draw| {
            (
                (usize::from(draw.0) * ops) / (usize::from(u16::MAX) + 1),
                draw,
            )
        })
        .collect();
    placed.sort_by_key(|(op, _)| *op);
    // The tables a wiring's foreign key references (#2181): the wiring is
    // a function of the program.
    let wiring = match menu {
        Menu::Privilege => with_wiring(ConcurrentPlan::default(), program).wiring,
        Menu::Maintenance => Vec::new(),
    };
    let referenced: Vec<&str> = wiring
        .iter()
        .filter(|w| w.shadow.as_ref().is_some_and(|s| s.fk.is_some()))
        .map(|w| w.table.as_str())
        .collect();
    // Each re-typed column's type as the plan's earlier events left it.
    let mut retyped: Vec<((String, String), SqlType)> = Vec::new();
    for (op, &(_, action, table, pick, window, pass_inside)) in placed {
        // Only a table a definition reads is a tracked source: the others
        // have no capture to disrupt.
        let tracked: Vec<_> = program
            .tables
            .iter()
            .filter(|t| program.defs.iter().any(|d| reads(d, &t.name)))
            .collect();
        if tracked.is_empty() || op >= ops {
            continue;
        }
        // A column an earlier event made an integer, where the plan has
        // one, is the one a column re-type takes, and the event right after
        // the narrowing always re-types it: re-typing it again is how a
        // widening outgrows a copy a `RESUME` made of it, and drawn
        // independently the two would hardly ever meet.
        let narrowed = events.last().is_some_and(|last| {
            matches!(
                last.action,
                SourceDdl::Retype {
                    to: SqlType::Int4,
                    shape: RetypeShape::Narrow,
                    ..
                }
            )
        });
        let action = match menu {
            Menu::Maintenance if narrowed => 7,
            _ => action,
        };
        let integer = match (menu, action) {
            (Menu::Maintenance, 7) => retyped
                .iter()
                .rev()
                .find(|(_, ty)| matches!(ty, SqlType::Int4 | SqlType::Int8))
                .map(|((t, c), _)| (t.clone(), c.clone())),
            _ => None,
        };
        let table = match &integer {
            Some((name, _)) => *tracked
                .iter()
                .find(|t| &t.name == name)
                .expect("a re-typed table is tracked"),
            None => tracked[table % tracked.len()],
        };
        let numeric: Vec<&str> = table
            .columns
            .iter()
            .filter(|c| c.name != table.pk_col && c.value_type == ValueType::Numeric)
            .map(|c| c.name.as_str())
            .collect();
        let column = (!numeric.is_empty()).then(|| numeric[pick % numeric.len()].to_string());
        let readers: Vec<&str> = program
            .defs
            .iter()
            .filter(|d| reads(d, &table.name))
            .map(|d| d.target.as_str())
            .collect();
        // A table a `JOIN` reads draws only changes to the whole table, its
        // key's index and its rewrites: a column or attribute change there
        // is left to a later slice of the tier, and draws a maintenance
        // action in its place.
        let joined = program
            .defs
            .iter()
            .any(|d| reads(d, &table.name) && is_join(d));
        let action = match action {
            7..=18 if joined && matches!(menu, Menu::Maintenance) => action % 7,
            action => action,
        };
        let target = readers[pick % readers.len()].to_string();
        let declared = |column: &str| {
            retyped
                .iter()
                .rev()
                .find(|((t, c), _)| t == &table.name && c == column)
                .map(|(_, ty)| *ty)
                .or_else(|| declared_type(table, column))
        };
        let action = match (menu, action) {
            (Menu::Maintenance, 7 | 8) => {
                let prefer = integer.as_ref().map(|(_, c)| c.as_str());
                retype(
                    program,
                    table,
                    &declared,
                    prefer,
                    action == 8,
                    op + 1 == ops,
                    pick,
                )
                .unwrap_or(SourceDdl::VacuumFull)
            }
            (Menu::Maintenance, 9..=15) => columns(action, table, &numeric, &declared, op, pick)
                .unwrap_or(SourceDdl::VacuumFull),
            (Menu::Maintenance, 16..=18) => {
                attributes(action, program, table, &numeric, pick).unwrap_or(SourceDdl::VacuumFull)
            }
            (Menu::Maintenance, 19..=22) => whole_table(action, table),
            (Menu::Maintenance, 23..) => key_index(action, program, table, pick),
            (Menu::Maintenance, _) => maintenance(action, column, pick),
            (Menu::Privilege, _) => privilege(action, target),
        };
        let action = match action {
            // A foreign key's check reads the referenced table as its owner,
            // so taking the owner's `SELECT` or `USAGE` fails the
            // application's write in Postgres itself. Revoking the owner
            // from Trellis takes Trellis's read alone.
            SourceDdl::RevokeSelect if referenced.contains(&table.name.as_str()) => {
                SourceDdl::RevokeMembership
            }
            SourceDdl::RevokeSchemaUsage if !referenced.is_empty() => SourceDdl::RevokeMembership,
            action => action,
        };
        // A change later ops couldn't write through fires after the last.
        let (op, window) = if action.terminal() {
            (ops - 1, 0)
        } else {
            (op, window.min(ops - 1 - op))
        };
        // Partition maintenance names the root, which a table moved away
        // doesn't answer to: the window ends before the table's first.
        let window = if action.moves_table() {
            program.ops[op + 1..=op + window]
                .iter()
                .position(|o| matches!(o, Op::Partition { table: t, .. } if t == &table.name))
                .unwrap_or(window)
        } else {
            window
        };
        // A write the key's index refuses would go through while it is
        // dropped: the window ends before the table's first.
        let window = if matches!(
            action,
            SourceDdl::KeyIndex {
                change: IndexChange::Drop,
                ..
            }
        ) {
            program.ops[op + 1..=op + window]
                .iter()
                .position(|o| o.table() == table.name && *o.expect() == OpOutcome::Fails)
                .unwrap_or(window)
        } else {
            window
        };
        let event = SourceDdlEvent {
            op,
            table: table.name.clone(),
            action,
            window,
            pass_inside,
        };
        if events.last().is_some_and(|last| event.op <= last.last_op()) {
            continue;
        }
        if let SourceDdl::Retype { column, to, .. } = &event.action {
            retyped.push(((event.table.clone(), column.clone()), *to));
        }
        events.push(event);
    }
    // A narrowing to `integer` no draw followed is followed all the same,
    // where an op is left, as the loop's own override says.
    if let Some(last) = events.last()
        && let SourceDdl::Retype {
            column,
            to: SqlType::Int4,
            shape: RetypeShape::Narrow,
            ..
        } = &last.action
        && last.last_op() + 1 < ops
    {
        let op = last.last_op() + 1;
        events.push(SourceDdlEvent {
            op,
            table: last.table.clone(),
            action: SourceDdl::Retype {
                column: column.clone(),
                to: if (op / 2) % 2 == 0 {
                    SqlType::Int8
                } else {
                    SqlType::Numeric {
                        precision: 16,
                        scale: 2,
                    }
                },
                using: last.op % 2 == 1,
                shape: RetypeShape::Outgrow,
            },
            window: 0,
            pass_inside: !last.pass_inside,
        });
    }
    SourceDdlPlan { events, lanes: 1 }
}

/// The maintenance action `action` names, on `column` where it takes one.
fn maintenance(action: u8, column: Option<String>, pick: usize) -> SourceDdl {
    match (action, column) {
        (0, _) => SourceDdl::VacuumFull,
        (1, _) => SourceDdl::Cluster,
        // The column pick's parity: drawn apart from the pass.
        (2, _) => SourceDdl::Reindex {
            concurrently: pick % 2 == 1,
        },
        (3, Some(column)) => SourceDdl::RewriteColumn { column },
        (4, Some(column)) => SourceDdl::RewriteBesideDefault { column },
        (3 | 4, None) => SourceDdl::VacuumFull,
        _ => SourceDdl::DisableCaptureTriggers,
    }
}

/// The column change `action` names (#2184 slice 4) on one of `numeric`,
/// `table`'s numeric columns other than its key, `declared` giving each
/// column's type now. A swap takes a second column of the same type, or
/// shadows the first where there is none. `None` where the table has no
/// such column, or is partitioned or read through a `KEY (…)`, as
/// [`retype`] has it.
fn columns(
    action: u8,
    table: &Table,
    numeric: &[&str],
    declared: &dyn Fn(&str) -> Option<SqlType>,
    op: usize,
    pick: usize,
) -> Option<SourceDdl> {
    if table.partitioning.is_some() || table.source_key.is_some() || numeric.is_empty() {
        return None;
    }
    let column = numeric[pick % numeric.len()].to_string();
    Some(match action {
        9 => SourceDdl::RenameColumn { column },
        10 => SourceDdl::DropColumn { column },
        11 => SourceDdl::DropAndAddColumn { column },
        12 => SourceDdl::AddColumn {
            column: format!("added_{op}"),
        },
        13 => match numeric
            .iter()
            .find(|other| **other != column && declared(other) == declared(&column))
        {
            Some(other) => SourceDdl::SwapColumnNames {
                column,
                other: other.to_string(),
            },
            None => SourceDdl::ShadowColumn { column },
        },
        14 => SourceDdl::ShadowColumn { column },
        _ => SourceDdl::GeneratedColumn { column },
    })
}

/// The `NOT NULL`, default or collation change `action` names (#2184
/// slice 5): a `NOT NULL` flip of one of `numeric`, `table`'s numeric
/// columns other than its key, that no op writes `NULL` to (a default set
/// on one where none is free of them), a default set on one, beside a
/// `VACUUM FULL` or not, or the table's `text` column given `"C"` or a
/// case-insensitive collation. `None` where the table has no such column,
/// or is partitioned or read through a `KEY (…)`, as [`retype`] has it.
fn attributes(
    action: u8,
    program: &Program,
    table: &Table,
    numeric: &[&str],
    pick: usize,
) -> Option<SourceDdl> {
    if table.partitioning.is_some() || table.source_key.is_some() || numeric.is_empty() {
        return None;
    }
    let set_default = || SourceDdl::SetDefault {
        column: numeric[pick % numeric.len()].to_string(),
        rewrite: (pick / numeric.len()) % 2 == 1,
    };
    Some(match action {
        16 => {
            let never_null: Vec<&&str> = numeric
                .iter()
                .filter(|c| !writes_null(program, &table.name, c))
                .collect();
            if never_null.is_empty() {
                set_default()
            } else {
                SourceDdl::FlipNotNull {
                    column: never_null[pick % never_null.len()].to_string(),
                    key: false,
                }
            }
        }
        17 => set_default(),
        _ => SourceDdl::Collate {
            column: table
                .columns
                .iter()
                .find(|c| c.name != table.pk_col && c.value_type == ValueType::Text)?
                .name
                .clone(),
            collation: if pick.is_multiple_of(2)
                || calls_collation_refusing_function(program, table)
            {
                Collation::C
            } else {
                Collation::CaseInsensitive
            },
        },
    })
}

/// The change to the whole table `action` names (#2184 slice 6): renamed
/// away and back, moved to another schema and back, renamed away for good,
/// or dropped and created again with its rows. Only a plain table is
/// created again (a tree's leaves would have to be too); a partitioned one
/// draws `VACUUM FULL` instead.
fn whole_table(action: u8, table: &Table) -> SourceDdl {
    match action {
        19 => SourceDdl::RenameTable,
        20 => SourceDdl::MoveSchema,
        21 => SourceDdl::RenameTableAway,
        _ if table.partitioning.is_some() => SourceDdl::VacuumFull,
        _ => SourceDdl::RecreateTable,
    }
}

/// The change to the index backing `table`'s key `action` names (#2184
/// slices 7 and 8): its `KEY (…)`'s, or, by `pick`'s parity, its unique
/// key within a 1-1 `JOIN`'s `ON` columns where it is joined at a position
/// (`docs/transforms.md`, "Key"), or its primary key's. A partitioned
/// table's key index is a tree of indexes no `CONCURRENTLY` build makes, so
/// it draws `REINDEX TABLE CONCURRENTLY` instead.
fn key_index(action: u8, program: &Program, table: &Table, pick: usize) -> SourceDdl {
    if table.partitioning.is_some() {
        return SourceDdl::Reindex { concurrently: true };
    }
    let columns = match (&table.source_key, joined_unique_key(program, table)) {
        (Some(key), _) => key.columns.clone(),
        (None, Some(columns)) if pick % 2 == 1 => columns,
        _ => vec![table.pk_col.clone()],
    };
    let change = match action {
        23 => IndexChange::Drop,
        24 => IndexChange::Replace,
        _ => IndexChange::RebuildConcurrently,
    };
    SourceDdl::KeyIndex { columns, change }
}

/// The columns of `table`'s unique key within the `ON` columns of a 1-1
/// `JOIN` joining it, where it has one: a unique column, or set of
/// columns, all of which the `ON` names on `table`'s side.
fn joined_unique_key(program: &Program, table: &Table) -> Option<Vec<String>> {
    let on: Vec<&str> = program
        .defs
        .iter()
        .filter_map(|d| match &d.key_space {
            KeySpace::JoinOneToOne(spec) => Some(spec),
            _ => None,
        })
        .flat_map(|spec| &spec.joins)
        .filter(|j| j.right == table.name)
        .flat_map(|j| j.on.iter().flat_map(|on| [&on.lhs, &on.rhs]))
        .filter(|c| c.table == table.name)
        .map(|c| c.column.as_str())
        .collect();
    let single = table
        .unique_cols
        .iter()
        .find(|c| on.contains(&c.as_str()))
        .map(|c| vec![c.clone()]);
    single.or_else(|| {
        table
            .unique_sets
            .iter()
            .find(|set| set.iter().all(|c| on.contains(&c.as_str())))
            .cloned()
    })
}

/// Whether `def` joins tables.
fn is_join(def: &TransformDef) -> bool {
    matches!(def.key_space, KeySpace::Join(_) | KeySpace::JoinOneToOne(_))
}

/// The functions Postgres refuses over a nondeterministic collation for
/// every row (`docs/errors.md`, T0109).
pub const COLLATION_REFUSING_FUNCTIONS: [&str; 2] = ["STRPOS", "REGEXP_COUNT"];

/// Whether a definition reading `table` calls one of
/// [`COLLATION_REFUSING_FUNCTIONS`] anywhere. Under a case-insensitive
/// collation each changed row's key is held (`docs/transforms.md`, "Per
/// row"): reported, not a register entry, and a drain that never converges
/// past the held keys, so such a table only draws `"C"`.
pub fn calls_collation_refusing_function(program: &Program, table: &Table) -> bool {
    program
        .defs
        .iter()
        .filter(|d| d.source == table.name)
        .flat_map(|d| d.fields.iter().map(|f| &f.expr).chain(d.predicate.filter()))
        .any(|expr| {
            let mut calls = false;
            expr.walk(&mut |e| {
                calls |= matches!(
                    e,
                    trellis::dev::defs::ast::Expr::FunctionCall { name, .. }
                        if COLLATION_REFUSING_FUNCTIONS.contains(&name.as_str())
                );
            });
            calls
        })
}

/// `column`'s declared type as the generator creates it, where a
/// [`SourceDdl::Retype`] can change it.
fn declared_type(table: &Table, column: &str) -> Option<SqlType> {
    match table.column_type(column)? {
        ValueType::Numeric => Some(SqlType::Numeric {
            precision: 14,
            scale: 2,
        }),
        ValueType::Integer(IntWidth::Int4) => Some(SqlType::Int4),
        ValueType::Integer(IntWidth::Int8) => Some(SqlType::Int8),
        _ => None,
    }
}

/// A re-type of one of `table`'s columns: of its primary key when `key`,
/// else of another numeric-family column, `declared` giving each column's
/// type now, and `prefer` the column to take where it is one. A key re-type changes values (`USING`) only when `last`, the
/// event firing after the program's last op, since later ops address the
/// keys. `None` where the table has no such column: a partitioned table (a
/// partition key can't be re-typed, and a leaf detached meanwhile wouldn't
/// attach again) or one read through a `KEY (…)` isn't re-typed at all.
fn retype(
    program: &Program,
    table: &Table,
    declared: &dyn Fn(&str) -> Option<SqlType>,
    prefer: Option<&str>,
    key: bool,
    last: bool,
    pick: usize,
) -> Option<SourceDdl> {
    if table.partitioning.is_some() || table.source_key.is_some() {
        return None;
    }
    let columns: Vec<(&str, SqlType)> = table
        .columns
        .iter()
        .filter(|c| (c.name == table.pk_col) == key)
        .filter_map(|c| Some((c.name.as_str(), declared(&c.name)?)))
        .collect();
    if columns.is_empty() {
        return None;
    }
    let (column, from) = columns
        .iter()
        .find(|(c, _)| Some(*c) == prefer)
        .copied()
        .unwrap_or(columns[pick % columns.len()]);
    let rest = pick / columns.len();
    let using = (rest / 4) % 2 == 1;
    // `integer` holds no `NaN`: the re-type, or a later write, would fail.
    let integer = |shape| (!writes_nan(program, &table.name, column)).then_some(shape);
    let wider = |precision: u8| SqlType::Numeric {
        precision: precision + 2,
        scale: 2,
    };
    let (shape, to, using) = if key {
        let to = match (from, rest % 3) {
            (_, 2) if last => from,
            // A key define refuses, made a `bigint` again.
            (SqlType::NumericFree, _) => SqlType::Int8,
            (_, 1) => SqlType::NumericFree,
            (SqlType::Int8, _) => SqlType::Int4,
            _ => SqlType::Int8,
        };
        // A key `USING` changes the keys later ops address, so only at the
        // last op; a same-type re-type without it would change nothing.
        let using = to == from || (last && using);
        (RetypeShape::Key, to, using)
    } else {
        let (shape, to) = match (from, rest % 4) {
            (SqlType::Numeric { precision, .. }, 0) => (RetypeShape::Widen, wider(precision)),
            (SqlType::Numeric { .. }, 1) => (RetypeShape::Widen, SqlType::NumericFree),
            (SqlType::Numeric { precision, scale }, 2) if scale > 0 => (
                RetypeShape::Narrow,
                SqlType::Numeric {
                    precision,
                    scale: scale - 1,
                },
            ),
            (SqlType::Numeric { precision, .. }, _) => {
                integer((RetypeShape::Narrow, SqlType::Int4))
                    .unwrap_or((RetypeShape::Widen, wider(precision)))
            }
            (SqlType::NumericFree, 0 | 1) => (RetypeShape::Narrow, wider(14)),
            (SqlType::NumericFree, _) => integer((RetypeShape::Narrow, SqlType::Int4))
                .unwrap_or((RetypeShape::Narrow, wider(14))),
            (SqlType::Int4, 0 | 1) => (RetypeShape::Outgrow, SqlType::Int8),
            (SqlType::Int4, _) => (RetypeShape::Outgrow, wider(14)),
            (SqlType::Int8, 0 | 1) => (RetypeShape::Outgrow, wider(16)),
            (SqlType::Int8, _) => (RetypeShape::Narrow, SqlType::Int4),
        };
        (shape, to, using)
    };
    Some(SourceDdl::Retype {
        column: column.to_string(),
        to,
        using,
        shape,
    })
}

/// The column values one write names.
type Values<'a> = &'a [(String, Option<String>)];

/// The values each write of `op` names, and whether it inserts (a column
/// it leaves out takes its default).
fn written_values(op: &Op) -> Vec<(bool, Values<'_>)> {
    match op.write() {
        Op::Insert { row, .. } => vec![(true, row.as_slice())],
        Op::Update { changes, .. } => vec![(false, changes.as_slice())],
        Op::BulkInsert { rows, .. } => rows.iter().map(|row| (true, row.as_slice())).collect(),
        Op::Leaf { op, .. } => written_values(op),
        _ => Vec::new(),
    }
}

/// Whether any of `program`'s ops writes `NaN` to `column` of `table`.
fn writes_nan(program: &Program, table: &str, column: &str) -> bool {
    program
        .ops
        .iter()
        .filter(|op| op.table() == table)
        .flat_map(written_values)
        .flat_map(|(_, values)| values)
        .any(|(c, v)| c == column && v.as_deref() == Some("NaN"))
}

/// Whether any of `program`'s ops writes `NULL` to `column` of `table`,
/// or inserts a row without it.
fn writes_null(program: &Program, table: &str, column: &str) -> bool {
    program
        .ops
        .iter()
        .filter(|op| op.table() == table)
        .flat_map(written_values)
        .any(
            |(insert, values)| match values.iter().find(|(c, _)| c == column) {
                Some((_, value)) => value.is_none(),
                None => insert,
            },
        )
}

/// The privilege change `action` names, a drain refusal on `target`.
fn privilege(action: u8, target: String) -> SourceDdl {
    match action {
        0 => SourceDdl::OwnerToHeir,
        1 => SourceDdl::OwnerToOther,
        2 => SourceDdl::RevokeSelect,
        3 => SourceDdl::RevokeSchemaUsage,
        4 => SourceDdl::Rls {
            trellis_policy: false,
        },
        5 => SourceDdl::Rls {
            trellis_policy: true,
        },
        6 => SourceDdl::WriterChange,
        7 => SourceDdl::RevokeTargetWrites { target },
        8 => SourceDdl::TargetRls { target },
        9 => SourceDdl::TargetTriggerRefused { target },
        _ => SourceDdl::RevokeMembership,
    }
}
