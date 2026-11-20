//! The DDL of a [`Wiring`] (#2181): the application triggers and foreign
//! key [`crate::generate::wiring`] draws, installed with the source tables,
//! before any definition.

use std::collections::HashMap;

use super::sql::{column_pg_type, quote_ident, quote_literal};
use crate::model::{FkAction, SHADOW_OFFSET, Shadow, TALLY_BASE, Table, Timing, Wiring};

/// The column a shadow's link to its row lives in.
pub(super) const LINK_COLUMN: &str = "lnk";

/// Every statement that installs `wiring` on `tables`, as one script.
///
/// # Panics
///
/// If `wiring` names a table `tables` doesn't hold: a generator bug.
pub(super) fn wiring_ddl(tables: &HashMap<String, Table>, wiring: &Wiring) -> String {
    let table = &tables[&wiring.table];
    let mut sql = String::new();
    if let Some(shadow) = &wiring.shadow {
        sql.push_str(&shadow_ddl(tables, table, shadow));
    }
    if let Some(rewrite) = &wiring.rewrite {
        let t = quote_ident(&table.name);
        let pk = quote_ident(&table.pk_col);
        let col = quote_ident(&rewrite.column);
        let flip = |current: &str| {
            flip_expr(
                tables,
                &table.name,
                &rewrite.column,
                current,
                &rewrite.values.0,
                &rewrite.values.1,
            )
        };
        let mine = format!("abs(new.{pk}) < {SHADOW_OFFSET} and new.{pk} % 2 = 0");
        let (when, body) = match rewrite.timing {
            Timing::Before => (
                "before",
                format!(
                    "if {mine} then new.{col} := {}; end if; return new;",
                    flip(&format!("new.{col}"))
                ),
            ),
            // Only at the top level: the update fires this trigger again.
            Timing::After => (
                "after",
                format!(
                    "if pg_catalog.pg_trigger_depth() = 1 and {mine} then \
                     update {t} set {col} = {} where {pk} = new.{pk}; end if; return null;",
                    flip(&col)
                ),
            ),
        };
        let function = quote_ident(&format!("app_{}_rewrite", table.name));
        sql.push_str(&format!(
            "create function {function}() returns trigger language plpgsql \
             set search_path from current as $$ begin {body} end $$;\n\
             create trigger {} {when} insert or update on {t} \
             for each row execute function {function}();\n",
            quote_ident(&format!("a_app_{}_rewrite", table.name)),
        ));
    }
    sql
}

/// `CASE`: `b` where `current` is `a`, else `a`, each a literal of
/// `table.column`'s type.
fn flip_expr(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
    current: &str,
    a: &Option<String>,
    b: &Option<String>,
) -> String {
    let a = literal(tables, table, column, a);
    let b = literal(tables, table, column, b);
    format!("case when {current} is not distinct from {a} then {b} else {a} end")
}

/// `value` as a literal of `table.column`'s type.
fn literal(
    tables: &HashMap<String, Table>,
    table: &str,
    column: &str,
    value: &Option<String>,
) -> String {
    let ty = column_pg_type(tables, table, column);
    match value {
        Some(value) => format!("{}::text::{ty}", quote_literal(value)),
        None => format!("null::{ty}"),
    }
}

/// The DDL of `shadow`, kept for the rows of `table` (see [`Shadow`]).
fn shadow_ddl(tables: &HashMap<String, Table>, table: &Table, shadow: &Shadow) -> String {
    let target = &tables[&shadow.table];
    let t = quote_ident(&table.name);
    let pk = quote_ident(&table.pk_col);
    let c = quote_ident(&target.name);
    let cpk = quote_ident(&target.pk_col);
    let lnk = quote_ident(LINK_COLUMN);
    let (toggle, a, b) = &shadow.toggle;
    let flip = |current: &str| flip_expr(tables, &target.name, toggle, current, a, b);
    let names: Vec<String> = shadow
        .template
        .iter()
        .map(|(c, _)| quote_ident(c))
        .collect();
    let values: Vec<String> = shadow
        .template
        .iter()
        .map(|(column, value)| literal(tables, &target.name, column, value))
        .collect();
    let columns = names.iter().map(|n| format!(", {n}")).collect::<String>();
    let template = values.iter().map(|v| format!(", {v}")).collect::<String>();
    let reset = std::iter::once(format!("{lnk} = excluded.{lnk}"))
        .chain(names.iter().map(|n| format!("{n} = excluded.{n}")))
        .collect::<Vec<_>>()
        .join(", ");
    let linked = shadow.timing == Timing::After;
    // A cascading key deletes the shadow itself; a nulling one unlinks it,
    // and a re-insert of its row links it again.
    let deletes = shadow.fk.is_none();
    let when = match shadow.timing {
        Timing::Before => "before",
        Timing::After => "after",
    };
    let prefix = if shadow.before_capture || !shadow.statement {
        "a_app"
    } else {
        "zz_app"
    };
    let name = |what: &str| format!("{}_{}_{what}", prefix, table.name);

    let mut sql = format!(
        "alter table {c} add column {lnk} bigint;\n\
         create index on {c} ({lnk});\n"
    );
    if let Some(action) = shadow.fk {
        let action = match action {
            FkAction::Cascade => "cascade",
            FkAction::SetNull => "set null",
        };
        sql.push_str(&format!(
            "alter table {c} add foreign key ({lnk}) references {t} ({pk}) \
             on delete {action} on update cascade;\n"
        ));
    }
    let tally = format!(
        "insert into {c} as s ({cpk}, {lnk}{columns}) \
         values ({TALLY_BASE} + pg_catalog.pg_backend_pid(), null{template}) \
         on conflict ({cpk}) do update set {} = {};",
        quote_ident(toggle),
        flip(&format!("s.{}", quote_ident(toggle))),
    );
    // A statement trigger's own writes to its table would fire it again.
    let top = "if pg_catalog.pg_trigger_depth() = 1 then";
    let function = |what: &str, body: &str| {
        let function = quote_ident(&format!("app_{}_{what}", table.name));
        format!(
            "create function {function}() returns trigger language plpgsql \
             set search_path from current as $$ begin {body} end $$;\n"
        )
    };
    if !shadow.statement {
        let link = if linked {
            format!("new.{pk}")
        } else {
            "null".to_string()
        };
        let delete = if deletes {
            format!("delete from {c} where {cpk} = old.{pk} + {SHADOW_OFFSET};")
        } else {
            String::new()
        };
        let body = format!(
            "if tg_op = 'DELETE' then \
                 if abs(old.{pk}) < {SHADOW_OFFSET} then {delete} null; end if; \
                 if tg_when = 'BEFORE' then return old; end if; return null; \
             end if; \
             if abs(new.{pk}) < {SHADOW_OFFSET} then \
                 if tg_op = 'INSERT' then \
                     insert into {c} ({cpk}, {lnk}{columns}) \
                     values (new.{pk} + {SHADOW_OFFSET}, {link}{template}) \
                     on conflict ({cpk}) do update set {reset}; \
                 else \
                     update {c} set {} = {} where {cpk} = new.{pk} + {SHADOW_OFFSET}; \
                 end if; \
             end if; \
             if tg_when = 'BEFORE' then return new; end if; return null;",
            quote_ident(toggle),
            flip(&quote_ident(toggle)),
        );
        sql.push_str(&function("shadow", &body));
        sql.push_str(&format!(
            "create trigger {} {when} insert or update or delete on {t} \
             for each row execute function {}();\n",
            quote_ident(&name("shadow")),
            quote_ident(&format!("app_{}_shadow", table.name)),
        ));
        return sql;
    }
    if shadow.timing == Timing::Before {
        // No transition tables before the statement: the tally alone.
        sql.push_str(&function(
            "tally",
            &format!("{top} {tally} end if; return null;"),
        ));
        sql.push_str(&format!(
            "create trigger {} before insert or update or delete on {t} \
             for each statement execute function {}();\n",
            quote_ident(&name("tally")),
            quote_ident(&format!("app_{}_tally", table.name)),
        ));
        return sql;
    }
    let mine = |rows: &str| format!("abs({rows}.{pk}) < {SHADOW_OFFSET}");
    let events = [
        (
            "insert",
            "new table as n",
            format!(
                "insert into {c} ({cpk}, {lnk}{columns}) \
                 select n.{pk} + {SHADOW_OFFSET}, n.{pk}{template} from n \
                 where {} and exists (select 1 from {t} x where x.{pk} = n.{pk}) \
                 on conflict ({cpk}) do update set {reset};",
                mine("n")
            ),
        ),
        (
            "update",
            "new table as n",
            format!(
                "update {c} set {} = {} where {cpk} in \
                 (select n.{pk} + {SHADOW_OFFSET} from n where {});",
                quote_ident(toggle),
                flip(&quote_ident(toggle)),
                mine("n")
            ),
        ),
        (
            "delete",
            "old table as o",
            if deletes {
                format!(
                    "delete from {c} where {cpk} in \
                     (select o.{pk} + {SHADOW_OFFSET} from o where {});",
                    mine("o")
                )
            } else {
                String::new()
            },
        ),
    ];
    for (event, referencing, write) in events {
        let what = format!("shadow_{event}");
        sql.push_str(&function(
            &what,
            &format!("{top} {write} {tally} end if; return null;"),
        ));
        sql.push_str(&format!(
            "create trigger {} after {event} on {t} referencing {referencing} \
             for each statement execute function {}();\n",
            quote_ident(&name(&what)),
            quote_ident(&format!("app_{}_{what}", table.name)),
        ));
    }
    sql
}

#[cfg(test)]
#[path = "wiring_tests.rs"]
mod tests;
