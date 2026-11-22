//! Source-DDL plans for a program (#2184), and privilege-change plans
//! (#2182): which [`SourceDdl`] fires after which op, on which table, with
//! how many ops in flight around it.

use proptest::prelude::*;
use trellis::dev::defs::ast::ValueType;

use super::wiring::with_wiring;
use crate::model::{ConcurrentPlan, Program, SourceDdl, SourceDdlEvent, SourceDdlPlan};

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
            Menu::Maintenance => 7,
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
    prop::collection::vec(draw, 0..=MAX_SOURCE_DDL_EVENTS)
        .prop_map(move |draws| fit(&program, &draws, menu))
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
    for (op, &(_, action, table, pick, window, pass_inside)) in placed {
        // Only a table a definition reads is a tracked source: the others
        // have no capture to disrupt.
        let tracked: Vec<_> = program
            .tables
            .iter()
            .filter(|t| program.defs.iter().any(|d| d.source == t.name))
            .collect();
        if tracked.is_empty() || op >= ops {
            continue;
        }
        let table = tracked[table % tracked.len()];
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
            .filter(|d| d.source == table.name)
            .map(|d| d.target.as_str())
            .collect();
        let target = readers[pick % readers.len()].to_string();
        let action = match menu {
            Menu::Maintenance => maintenance(action, column, pick),
            Menu::Privilege => privilege(action, target),
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
        let window = window.min(ops - 1 - op);
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
        events.push(event);
    }
    SourceDdlPlan { events }
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
