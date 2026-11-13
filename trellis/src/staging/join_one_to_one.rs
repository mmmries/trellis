//! A 1-1 `JOIN` transform's apply step (ADR-0021): a page of either side's
//! changes.
//!
//! A 1-1 `JOIN` keeps one target row per `a` row, joined to at most one `b`
//! row, over four hidden tables ([`crate::defs::join_one_to_one`]): `R`, an
//! entry per `a` row on the 1-1 ledger's ordering state, holding the join key
//! and the `a` columns the fields read; `M`, the same per `b` row; `J`, a lock
//! row per join key; and the pair's truncate lock row. Neither side reads the
//! other's source, only its copy under `J`.
//!
//! A star (more than one joined table) is the same path indexed by
//! position: an `M` per joined table, `J`'s rows per position, and `a`'s key
//! one per position. The two-table JOIN is its one-position case. Each
//! joined table's change is of its own position; a key, and K below, is per
//! position, and a step over `M` runs for each position in position order.
//!
//! # One page ([`apply_page`])
//!
//! A page carries changes and Re-derives of either side, in one transaction
//! under `READ COMMITTED`, in the model's order (#1375). Each step is a rule
//! an ablation showed necessary; the plant that removes it is in brackets.
//!
//! 1. **Fold.** Only the newest change per id of each side goes through I2;
//!    a Re-derive of an id replaces that id's changes. A page whose segment
//!    may predate the build re-derives every id
//!    ([`super::ledger::page_may_predate_build`]).
//! 2. **Lock row and key set.** The pair's lock row `FOR SHARE`. Then the key
//!    set K: for each side, every change's new key, the key its entry (`R`
//!    or `M`) holds for each id, and the live key of each Re-derive row (read
//!    unlocked from its table, which keeps the retry below rare); for the `b`
//!    side also each change's old key from its first change's pre-image
//!    [`join1_old_key_unlocked`]. An `a` row's keys are one per position, so
//!    a root writer's K holds its new key and the key `R` holds at every
//!    position [`star_root_skip_j_*`]. A key with a `NULL` part is left out
//!    at its position: it matches nothing there and takes no `J` row.
//! 3. **`J`.** K, every side's keys together, inserted sorted `on conflict
//!    do nothing`, then locked `FOR UPDATE` in one statement sorted by
//!    `(position, key)` [`join1_no_j_lock`, `star_j_unsorted`]. Nothing reads `M`, or `R` for a fan-out, before
//!    this [`join1_m_before_j`].
//! 4. **Entries.** The `R` placeholders and entry lock, then `M`'s
//!    ([`super::one_to_one_ledger::lock_entries`], I5)
//!    [`join1_no_placeholder`, `join1_no_entry_lock`]. The `R` statement
//!    also locks the entries of every row the page's joined changes fan
//!    out to: the R-entry rule (#1459). A joined change's fan-out keys are
//!    its keys in K (new, old and the key `M` holds), and of their rows
//!    the page locks those it recomputes itself, within the fan-out budget
//!    (one per page, summed over positions, [`fan_out::budget`]). Root
//!    entries and fan-out rows of every position are locked in **one**
//!    sorted statement: split per position, two pages fanning out at
//!    crossing positions deadlock [`star_r_split`]. Nothing reads `M`
//!    before it [`star_own_j_only`, `star_r_after_m`, `star_last_no_r`].
//!    After each lock, every held key is checked: a root entry's at every
//!    position against K, a joined entry's against the page's fan-out keys
//!    at its position, not K: a key K holds only for a root item
//!    would let the page recompute rows on it without their `R` entries
//!    [`star_recheck_j`, `star_recheck_skip_last`]. One outside its set
//!    moved after K was read, so the page rolls back with
//!    [`ApplyError::JoinKeyMoved`] and retries [`join1_no_recheck`,
//!    `star_late_skip_*`]. The entry lock also orders the writers of a
//!    `NULL`-keyed row, which has no `J` row.
//! 5. **Re-derive read.** For each side, one statement reads its Re-derive
//!    rows live and `pg_current_snapshot()`, which becomes those entries'
//!    basis [`join1_snapshot_after_read`]. A key it reads outside its set
//!    (K, or a joined row's fan-out keys; the row moved after the unlocked
//!    read) rolls the page back the same way
//!    [`join1_rd_no_key`, `join1_brd_no_key`]: taking that key's `J` late
//!    would break the lock order. A `b` Re-derive's entry fans out like a
//!    `b` change's.
//! 6. **I2.** The entries are updated by ADR-0002's I2 (an Apply must not be
//!    in the entry's basis, must be past its applied position and above the
//!    floor) [`join1_vis_only`, `join1_lsn_only`]; a Re-derive sets the basis
//!    and leaves the applied position alone. Then each side's key and read
//!    columns are written for the entries that changed; a row that is gone
//!    has its key cleared, so it matches nothing.
//! 7. **Recompute and write.** Each changed `a` row's target row is rendered
//!    from `R` and, at each position, the newest live `M` entry on its key
//!    there (a `LEFT JOIN LATERAL` per position); a row that no longer
//!    exists, or one with no match at an `INNER` position, is deleted
//!    ([`write_a_changes`]). Then the fan-out of each position in position
//!    order ([`fan_out`]): every other live `R` entry on a key a changed `M` entry
//!    held before the page or holds after it is recomputed the same way, in
//!    sorted keyset batches of 10k, one statement each, a position's
//!    batches apart from the next's (#1464 Q2: the `R` entries order every
//!    writer of a row), for the keys within the budget; every other key is
//!    left to the chunks of its pending marker ([`fan_out`]'s module), which
//!    the page writes. Fields are rendered SQL, never evaluated in Rust. A
//!    field that fails on a row of either write holds that row of `a` in
//!    quarantine on this first failure, once a bisection under savepoints
//!    has named the rows and fields, and the rest of the page commits
//!    ([`held`]); a row held already is left out of the fan-out.
//!
//! The lock order is the lock row, `J` sorted, `R` entries (the root's and
//! the fan-out rows) sorted in one statement, `M` entries sorted, then
//! target rows, sorted within each statement. A page
//! that may hold rows of `a` in quarantine (one with `a` records, or whose
//! fan-out may hold rows) takes its definition's row `FOR KEY SHARE` before
//! the lock row ([`lock_definitions`]), as a rebuild's start takes it before
//! the lock row. The
//! target rows are not locked ahead across statements, and need not be: each
//! one is written only by a page that holds `J` for the key its `R` entry
//! has, or that row's `R` entry. An `a` page holds `J` for every key its
//! entries hold at every position (the held-key check), and a `b` page reads
//! the entries it fans out to after locking their key, which no writer of an
//! entry can leave or reach without the same lock, and locks those entries.
//! Two writers of one row share a lock that orders them: two root writers
//! `J` and `R`, a root writer and a joined one `J` at that position, and
//! writers at two positions the row's `R` entry. So they queue before
//! either locks the target row, whatever order their statements take, and a
//! chunk of a pending fan-out takes the same `J` row (at one position, where
//! `J` alone orders it).
//!
//! **Readers.** Every write of the target is recorded in the seam
//! (`super::target_mutations`) when something reads the target: a page's
//! writes of its `a` changes' rows and of its inline fan-out, each fan-out
//! chunk's, and the build's (its chunks', its sweep's and a quarantine
//! release's Re-derive, [`build`]). Each row is locked and its prior image
//! read before the write ([`write_target`]), and the downstream rows are
//! staged in the writer's own transaction. An `a` row's carries its changes'
//! origin merged with the page's `b` changes', a fan-out row's the `b`
//! changes' (a chunk's: its marker's), and a build's an unknown origin. A reader
//! applies them as each page and chunk commits, so between two chunks it may
//! see a key's rows joined partly to the old `b` row and partly to the new:
//! the read contract, exact after `await_converged`, which the marker holds
//! back.
//!
//! # A `TRUNCATE` ([`truncate_sides`], [`lock_definitions`], [`start_rebuilds`])
//!
//! A page that carries a `TRUNCATE` of any of the JOIN's tables takes the
//! lock row `FOR UPDATE` instead, so it waits for the pages and chunks in
//! flight and every other page waits for it, and settles the truncate before
//! its changes: each truncated table's ledger (`R`, or its position's `M`) is
//! emptied and its floor raised, a `TRUNCATE` of `a` empties the target, and
//! any deletes the definition's pending fan-outs at every position.
//! `J` keeps its rows. The ledger is emptied by `DELETE`,
//! not `TRUNCATE`: a sweep or `M` pass batch reads `R` or `M` before it takes
//! the lock row, and a `TRUNCATE`'s `ACCESS EXCLUSIVE` lock would wait for
//! that read while the batch waited for the lock row. A `TRUNCATE` of `b`
//! changes every target row, which one transaction would not hold, so the
//! page starts a rebuild of the target and the build's chunks rewrite the
//! rows (a `LEFT` row keeps its old `b` columns until the chunk reaches it).
//! However many joined tables one page truncates, it starts one rebuild.
//! It takes the definitions' rows of every target whose `b` it truncates, in
//! id order, before its first lock row, as a rebuild's start does
//! (`super::build::start_rebuilds`), and starts the rebuilds after it has
//! marked its segments drained, as seal phase 1 takes a segment's row before
//! the segment pointer. The drain's barrier orders the truncate's batch
//! against every other.
//!
//! # A build ([`build`])
//!
//! A fresh build, a `RESUME` or a rebuild in place (`request_backfill`, a
//! catch-up) is made of pages: Re-derive pages of every `b` row without the
//! fan-out, then chunks of `a`'s primary key, then a rebuild sweep of the
//! stale entries and of the target rows no `R` entry backs. Each runs the
//! steps above, so it orders against the drain's pages as a page does.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tokio_postgres::Transaction;
use tokio_postgres::types::{PgLsn, ToSql};

pub(crate) mod build;
pub(crate) mod fan_out;
pub(crate) mod held;
#[cfg(test)]
mod j_tests;

use crate::defs::ast::{Expr, JoinKind, TransformDef};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, Position, Side};
use crate::defs::join_one_to_one::HiddenTable;
use crate::defs::ledger as schema;
use crate::defs::model::Definition;
use crate::defs::oracle::render_qualified_expr_sql;
use crate::pool::{Pool, quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::FoldedChange;
use super::join::{LockRowMode, Truncate, lock_row_sql};
use super::keyset::{ArmSql, Keyset};
use super::one_to_one_ledger::{self as ledger, EntryChange, LedgerRef};
use super::one_to_one_render::{ImageColumn, OneToOneRender};
use super::target_mutations::TargetMutations;

/// The most `R` entries one target-write statement renders.
const WRITE_BATCH: usize = 10_000;

/// A definition's static shape: the hidden tables, the join key, the columns
/// each hidden ledger holds and the fields rendered over a row of `R` and the
/// newest live row of each position's `M`.
#[derive(Debug, Clone)]
pub(crate) struct JoinShape {
    /// The definition's id: the key of its `transform_definitions` row.
    id: i64,
    /// The target, qualified and unquoted: the pause label of its points.
    pub(crate) target: String,
    /// The `FROM` table, qualified.
    source_table: String,
    /// Each joined table, in position order: one for a two-table JOIN.
    positions: Vec<JoinedPosition>,
    pk: Vec<PrimaryKeyColumn>,
    r: LedgerRef,
    j: String,
    lock: String,
    /// `a`'s join key columns, every position's in position order, at the
    /// collation `R` and `J` hold them under ([`under_hidden_collations`]).
    key: Vec<ImageColumn>,
    /// The columns `R` holds besides its ledger: the key, then the `a`
    /// columns the fields read, at `R`'s collations.
    r_columns: Vec<ImageColumn>,
    /// Each written field's name and its SQL over the lateral row `s`.
    fields: Vec<(String, String)>,
    /// The columns of `s`: the SQL over `r` and `b`, and the name `fields`
    /// read it by.
    s_columns: Vec<(String, String)>,
}

/// One joined table of a [`JoinShape`]: its `M`, and its group of `J`.
#[derive(Debug, Clone)]
pub(crate) struct JoinedPosition {
    pub(crate) position: Position,
    /// The joined table, qualified.
    table: String,
    left: bool,
    m: LedgerRef,
    /// `a`'s join key columns at this position, in `ON` order, at `R`'s
    /// collation: the types `J`'s group holds them as.
    key: Vec<ImageColumn>,
    /// `J`'s columns of this position's group, for `key`
    /// ([`crate::defs::join_one_to_one::j_column`]).
    j_key: Vec<String>,
    /// The joined table's join key column names in `ON` order.
    m_key: Vec<String>,
    /// The columns `M` holds besides its ledger, at the joined table's own
    /// collations: `M` only assigns them (`write_entries`).
    m_columns: Vec<ImageColumn>,
}

impl JoinedPosition {
    /// The joined table, qualified.
    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    /// This position's `M`, qualified and quoted.
    pub(crate) fn m_ident(&self) -> &str {
        &self.m.ident
    }
}

/// One position's keys in a page's `J` statements ([`j_statements`]).
pub(crate) struct PositionKeys<'a> {
    pub(crate) position: &'a JoinedPosition,
    pub(crate) keys: &'a BTreeSet<KeyText>,
}

/// The alias of the newest live `M` entry of the joined table at
/// `position` in a recompute ([`JoinShape::joined_sql`]): `b` at position
/// 1, `b2` and on after it, as `M`'s own names run.
fn b_alias(position: Position) -> String {
    if position == Position::FIRST {
        "b".to_string()
    } else {
        format!("b{}", position.number())
    }
}

/// What a field reads of one side, and the SQL name `s` gives it.
fn s_name(side: Side, column: &str) -> String {
    let alias = match side {
        Side::From => "a".to_string(),
        Side::Joined(position) => b_alias(position),
    };
    format!("{alias}.{column}")
}

/// `expr` with each qualified column renamed to its name in `s`
/// ([`s_name`]), and the columns read inserted into `reads`. A bare column
/// can't reach here: define refuses it.
fn qualify(
    def: &TransformDef,
    target: &str,
    expr: &Expr,
    reads: &mut BTreeSet<(Side, String)>,
) -> Result<Expr, ApplyError> {
    let mut sub = |e: &Expr| qualify(def, target, e, reads);
    Ok(match expr {
        Expr::SourceColumn { table, column, .. } => {
            let side = spec::side_of_table(def, table).ok_or_else(|| {
                ApplyError::GroupBasisUnsupported {
                    target: target.to_string(),
                    what: "a field reading a table that is neither side of its 1-1 JOIN",
                }
            })?;
            reads.insert((side, column.clone()));
            Expr::Column(s_name(side, column))
        }
        Expr::Column(_) => {
            return Err(ApplyError::GroupBasisUnsupported {
                target: target.to_string(),
                what: "an unqualified column in a 1-1 JOIN field",
            });
        }
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::TypedLiteral { .. } => expr.clone(),
        Expr::BinaryOp { op, lhs, rhs } => Expr::BinaryOp {
            op: *op,
            lhs: Box::new(sub(lhs)?),
            rhs: Box::new(sub(rhs)?),
        },
        Expr::Negate(operand) => Expr::Negate(Box::new(sub(operand)?)),
        Expr::Cast { expr, target: ty } => Expr::Cast {
            expr: Box::new(sub(expr)?),
            target: *ty,
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(&mut sub).collect::<Result<_, _>>()?,
        },
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
            match expr.map_logic_children(&mut sub) {
                Some(rebuilt) => rebuilt?,
                None => expr.clone(),
            }
        }
    })
}

/// The cast of the SQL `expr` to `column`'s type, with its collation.
fn cast_to(column: &ImageColumn, expr: &str) -> String {
    let collate = column
        .collation
        .as_ref()
        .map(|name| format!(" collate {name}"))
        .unwrap_or_default();
    format!("({expr})::{}{collate}", column.type_sql)
}

/// `columns`, the source table's, each with the collation of the same-named
/// column of the hidden table `hidden` (qualified, unquoted) when it has one.
/// Its type stays the source's.
///
/// A source column can be re-collated after define, between deterministic
/// collations, which pauses nothing, while `R`, `M` and `J` keep the
/// collation they were created with. A cast of a key text under the source's
/// collation, compared with a hidden column inside a subquery, then met two
/// implicit collations and Postgres chose neither (`42P22`); the hidden
/// table's own is the one every statement over it, and the sort of its lock
/// order, runs under (#1567).
async fn under_hidden_collations(
    client: &impl tokio_postgres::GenericClient,
    columns: Vec<ImageColumn>,
    hidden: &str,
) -> Result<Vec<ImageColumn>, ApplyError> {
    let held = super::one_to_one_render::image_columns(client, hidden).await?;
    Ok(columns
        .into_iter()
        .map(|mut c| {
            if let Some(h) = held.iter().find(|h| h.name == c.name) {
                c.collation = h.collation.clone();
            }
            c
        })
        .collect())
}

impl JoinShape {
    /// The shape of `definition`, a 1-1 `JOIN`, with the `paused` fields left
    /// out; `pk` is `a`'s primary key.
    pub(crate) async fn load(
        pool: &Pool,
        definition: &Definition,
        pk: Vec<PrimaryKeyColumn>,
        paused: &HashSet<String>,
    ) -> Result<Self, ApplyError> {
        let def = &definition.def;
        let target = definition.target_table.clone();
        let join = spec::spec(def).ok_or_else(|| ApplyError::GroupBasisUnsupported {
            target: target.clone(),
            what: "a 1-1 JOIN with no joined table",
        })?;
        let (tables, a_columns, b_columns) = {
            let client = pool.get().await?;
            let tables = crate::defs::catalog::joined_tables_of(&**client, definition.id).await?;
            if tables.len() != join.joins.len() {
                return Err(ApplyError::GroupBasisUnsupported {
                    target,
                    what: "a 1-1 JOIN whose catalog doesn't record every joined table",
                });
            }
            let a = super::one_to_one_render::image_columns(&**client, &definition.source_table)
                .await?;
            // `R` and `J` keep the collations their columns were created
            // with, and a source column can be re-collated since (between
            // deterministic collations, which pauses nothing). Every cast of
            // `a`'s key text compared with them runs under the hidden
            // column's own collation, so two collations never meet.
            //
            // A joined table needs no such alignment. `M`'s columns are only
            // ever assigned from a cast (`write_entries`), which compares
            // no collation, and the keys of `M` meet `R` and `J` through
            // `a`'s column types (`JoinedPosition::key`), never through
            // `M`'s own.
            let schema_name = pool.schema();
            let hidden =
                |table: HiddenTable| format!("{schema_name}.{}", table.name(definition.id));
            let a =
                under_hidden_collations(&**client, a, &hidden(HiddenTable::FromEntries)).await?;
            let mut b = Vec::with_capacity(tables.len());
            for table in &tables {
                b.push(super::one_to_one_render::image_columns(&**client, table).await?);
            }
            (tables, a, b)
        };
        let by_name = |columns: &[ImageColumn], names: &[String]| -> Vec<ImageColumn> {
            names
                .iter()
                .filter_map(|n| columns.iter().find(|c| &c.name == n).cloned())
                .collect()
        };
        let key = by_name(&a_columns, &spec::join_key(def, Side::From));
        let read = |side: Side, columns: &[ImageColumn]| -> Vec<ImageColumn> {
            // The key first, then the rest in column order, each once.
            let wanted = spec::side_columns(def, side);
            let key_of_side = spec::join_key(def, side);
            let mut out: Vec<ImageColumn> = Vec::new();
            for column in by_name(columns, &key_of_side) {
                if !out.iter().any(|o| o.name == column.name) {
                    out.push(column);
                }
            }
            out.extend(
                columns
                    .iter()
                    .filter(|c| wanted.contains(&c.name) && !key_of_side.contains(&c.name))
                    .cloned(),
            );
            out
        };
        let r_columns = read(Side::From, &a_columns);
        let schema_name = pool.schema();
        let positions = spec::positions(join)
            .map(|(position, joined_table)| {
                let side = Side::Joined(position);
                let root_key = spec::from_key_at(def, position);
                JoinedPosition {
                    position,
                    table: tables[position.index()].clone(),
                    left: joined_table.kind == JoinKind::Left,
                    m: LedgerRef::hidden(
                        schema_name,
                        definition.id,
                        HiddenTable::JoinedEntries(position),
                        &target,
                    ),
                    key: by_name(&a_columns, &root_key),
                    j_key: root_key
                        .iter()
                        .map(|c| crate::defs::join_one_to_one::j_column(position, c))
                        .collect(),
                    m_key: spec::join_key(def, side),
                    m_columns: read(side, &b_columns[position.index()]),
                }
            })
            .collect();

        let mut reads: BTreeSet<(Side, String)> = BTreeSet::new();
        let mut fields = Vec::with_capacity(def.fields.len());
        for field in &def.fields {
            if paused.contains(&field.name) {
                continue;
            }
            let expr = qualify(def, &target, &field.expr, &mut reads)?;
            fields.push((field.name.clone(), render_qualified_expr_sql(&expr, "s")));
        }
        let s_columns = reads
            .iter()
            .map(|(side, column)| {
                let alias = match side {
                    Side::From => "r".to_string(),
                    Side::Joined(position) => b_alias(*position),
                };
                (
                    s_name(*side, column),
                    format!("{alias}.{}", quote_ident(column)),
                )
            })
            .collect();
        Ok(JoinShape {
            id: definition.id,
            source_table: definition.source_table.clone(),
            positions,
            pk,
            r: LedgerRef::hidden(
                schema_name,
                definition.id,
                HiddenTable::FromEntries,
                &target,
            ),
            j: HiddenTable::Keys.qualified(schema_name, definition.id),
            lock: HiddenTable::TruncateLock.qualified(schema_name, definition.id),
            key,
            r_columns,
            fields,
            s_columns,
            target,
        })
    }

    /// The `FROM` table, qualified.
    pub(crate) fn source_table(&self) -> &str {
        &self.source_table
    }

    /// Every joined table, in position order.
    pub(crate) fn positions(&self) -> &[JoinedPosition] {
        &self.positions
    }

    /// Whether the shape is a star: more than one joined table.
    pub(crate) fn is_star(&self) -> bool {
        self.positions.len() > 1
    }

    /// The joined table at `position`, if the shape has one there.
    pub(crate) fn at(&self, position: Position) -> Option<&JoinedPosition> {
        self.positions.get(position.index())
    }

    /// The joined table at `position`, which the shape has.
    pub(crate) fn joined_at(&self, position: Position) -> &JoinedPosition {
        self.joined(position)
    }

    /// `R`, qualified and quoted.
    pub(crate) fn r_ident(&self) -> &str {
        &self.r.ident
    }

    /// `side`'s table, qualified.
    fn table(&self, side: Side) -> &str {
        match side {
            Side::From => &self.source_table,
            Side::Joined(position) => &self.joined(position).table,
        }
    }

    /// The joined table at `position`, which every side a page names has.
    fn joined(&self, position: Position) -> &JoinedPosition {
        &self.positions[position.index()]
    }

    /// The side of the qualified `table`: the `FROM` table's, or a joined
    /// table's.
    pub(crate) fn side_of(&self, table: &str) -> Option<Side> {
        if table == self.source_table {
            return Some(Side::From);
        }
        self.positions
            .iter()
            .find(|p| p.table == table)
            .map(|p| Side::Joined(p.position))
    }

    /// `side`'s hidden ledger: `R` or the position's `M`.
    fn ledger(&self, side: Side) -> &LedgerRef {
        match side {
            Side::From => &self.r,
            Side::Joined(position) => &self.joined(position).m,
        }
    }

    /// `side`'s join key column names, in `ON` order: the `FROM` side's
    /// every position's, in position order.
    fn side_key(&self, side: Side) -> Vec<String> {
        match side {
            Side::From => self.key.iter().map(|c| c.name.clone()).collect(),
            Side::Joined(position) => self.joined(position).m_key.clone(),
        }
    }

    /// The `J` types of `side`'s join key columns ([`Self::side_key`]): `a`'s
    /// key columns, at `R`'s collation.
    fn side_key_types(&self, side: Side) -> &[ImageColumn] {
        match side {
            Side::From => &self.key,
            Side::Joined(position) => &self.joined(position).key,
        }
    }

    /// The columns `side`'s ledger holds besides its ordering state.
    fn side_columns(&self, side: Side) -> &[ImageColumn] {
        match side {
            Side::From => &self.r_columns,
            Side::Joined(position) => &self.joined(position).m_columns,
        }
    }

    /// The text of each join key column of `side`'s ledger row `alias`, cast
    /// to `J`'s type first, so both sides' keys compare as `K`'s texts do.
    fn key_texts(&self, side: Side, alias: &str) -> Vec<String> {
        self.side_key_types(side)
            .iter()
            .zip(self.side_key(side))
            .map(|(c, name)| {
                format!(
                    "({})::text",
                    cast_to(c, &format!("{alias}.{}", quote_ident(&name)))
                )
            })
            .collect()
    }

    /// Splits the key texts of a `side` row ([`Self::side_key`] order, a part
    /// `None` for `NULL`) into the position each key is of: the `FROM` side's
    /// into one per position, a joined side's into its own. A key with a
    /// `NULL` part matches nothing and is left out.
    fn split_key(&self, side: Side, parts: &[Option<String>]) -> Vec<(Position, KeyText)> {
        let whole = |p: Position, parts: &[Option<String>]| -> Option<(Position, KeyText)> {
            let key: Option<KeyText> = parts.iter().cloned().collect();
            key.map(|key| (p, key))
        };
        match side {
            Side::Joined(position) => whole(position, parts).into_iter().collect(),
            Side::From => {
                let mut out = Vec::with_capacity(self.positions.len());
                let mut at = 0;
                for p in &self.positions {
                    let n = p.key.len();
                    out.extend(whole(p.position, &parts[at..at + n]));
                    at += n;
                }
                out
            }
        }
    }

    /// The `on` of `r`'s key at `position` with that position's `M` as `m`.
    fn m_on(&self, position: Position) -> String {
        let p = self.joined(position);
        p.m_key
            .iter()
            .zip(&p.key)
            .map(|(m, r)| format!("m.{} = r.{}", quote_ident(m), quote_ident(&r.name)))
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// The row of `R`'s entry `r` and, at each position, the newest live `M`
    /// entry on its key there (a `LEFT JOIN LATERAL` per position, aliased
    /// [`b_alias`]), and the lateral row `s` the fields read. `from` is the
    /// relation that supplies `r`. Newest is by applied position, an entry no
    /// Apply has written (a build's or a Re-derive's) last, then by `b`'s
    /// key, as the #1375 model reads it. Two live entries share a key only
    /// while a hand-off between `b` rows is applied out of order, and the
    /// later of its two changes recomputes the key.
    fn joined_sql(&self, from: &str) -> String {
        let s = if self.s_columns.is_empty() {
            "(select 1 as __none) s".to_string()
        } else {
            let cols: Vec<String> = self
                .s_columns
                .iter()
                .map(|(name, sql)| format!("{sql} as {}", quote_ident(name)))
                .collect();
            format!("(select {}) s", cols.join(", "))
        };
        let laterals: Vec<String> = self
            .positions
            .iter()
            .map(|p| {
                format!(
                    "left join lateral (select m.* from {m} m where {on} and not m.{tomb} \
                                        order by m.{applied} desc nulls last, \
                                                 m.{key} collate \"C\" desc \
                                        limit 1) {b} on true",
                    m = p.m.ident,
                    on = self.m_on(p.position),
                    tomb = quote_ident(schema::TOMBSTONE_COLUMN),
                    applied = quote_ident(schema::APPLIED_LSN_COLUMN),
                    key = quote_ident(schema::KEY_COLUMN),
                    b = b_alias(p.position),
                )
            })
            .collect();
        format!("{from} {} cross join lateral {s}", laterals.join(" "))
    }

    /// Whether the `r` row has a target row: not a tombstone, and for each
    /// `INNER` position a live `M` entry on its key there.
    fn has_target_row_sql(&self) -> String {
        let mut terms = vec![format!("not r.{}", quote_ident(schema::TOMBSTONE_COLUMN))];
        for p in self.positions.iter().filter(|p| !p.left) {
            terms.push(format!(
                "exists (select 1 from {m} m where {on} and not m.{tomb})",
                m = p.m.ident,
                on = self.m_on(p.position),
                tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            ));
        }
        terms.join(" and ")
    }

    /// An empty key set, one empty set per position.
    fn empty_keys(&self) -> PageKeys {
        vec![BTreeSet::new(); self.positions.len()]
    }

    /// Whether some position is `INNER`, so a live `R` entry may have no
    /// target row.
    fn any_inner(&self) -> bool {
        self.positions.iter().any(|p| !p.left)
    }
}

/// One change of a page, of either side: an Apply of its last change,
/// written from its image (`None` for a delete), or a Re-derive (`apply`
/// `None`), written from the row the page reads under the entry lock.
/// `old_image` is its first change's pre-image, whose key a `b` change locks.
#[derive(Debug, Clone)]
pub(crate) struct Record {
    key: String,
    apply: Option<(PgLsn, String, Option<String>)>,
    old_image: Option<String>,
}

impl Record {
    /// This record as a Re-derive of its row.
    fn rederive(&self) -> Self {
        Record {
            key: self.key.clone(),
            apply: None,
            old_image: self.old_image.clone(),
        }
    }
}

/// One page's work on one 1-1 `JOIN` target: its changes of each side.
#[derive(Debug, Clone)]
pub(crate) struct JoinOneToOnePlan {
    pub(crate) shape: std::sync::Arc<JoinShape>,
    a: Vec<Record>,
    /// The changes of each joined table, by position (index 0 is position 1).
    b: Vec<Vec<Record>>,
    /// The page's `TRUNCATE` of any of the JOIN's tables, which it settles
    /// before its changes (the module doc).
    truncate: Option<Truncate>,
    /// Whether a changed `M` entry fans out to the `R` entries on its keys.
    /// Only the build's `M` pass ([`build::m_batch`],
    /// [`build::m_null_batch`], [`build::m_gone_batch`]) turns it off: the
    /// build's chunks write every target row after it.
    fan_out: bool,
    /// What the rows a fan-out holds are parked with: the page's `b`
    /// changes' origin ([`held::HeldOrigin`]). An `a` row the page holds
    /// parks with it too, merged with its own changes' ([`write_a_changes`]).
    held_origin: held::HeldOrigin,
    /// Whether a field that fails on the row of one of the page's `a`
    /// changes holds that row in the page ([`write_a_changes`]): a drain's
    /// page. A build's pages and a release's Re-derive ([`build::rederive`])
    /// leave the failure to their callers.
    holds_a: bool,
    /// The origin of the page's changes of each `a` row, by its key, which
    /// the row parks with if the page holds it.
    a_origins: HashMap<String, held::HeldOrigin>,
    /// The rows the page held, for the fuses once it commits
    /// ([`held::charge_after_commit`]).
    held: held::Held,
}

impl JoinOneToOnePlan {
    pub(crate) fn new(shape: JoinShape) -> Self {
        let positions = shape.positions.len();
        Self {
            shape: std::sync::Arc::new(shape),
            a: Vec::new(),
            b: vec![Vec::new(); positions],
            truncate: None,
            fan_out: true,
            held_origin: held::HeldOrigin::default(),
            holds_a: true,
            a_origins: HashMap::new(),
            held: held::Held::default(),
        }
    }

    /// Whether the page truncates a joined table, at any position, so it
    /// starts a rebuild of the target.
    fn truncates_b(&self) -> bool {
        self.truncate
            .as_ref()
            .is_some_and(Truncate::truncates_joined)
    }

    /// Adds a `TRUNCATE` of the table on `side`, `change`, whose ring `lsn`
    /// is `lsn`, the floor it raises.
    pub(crate) fn truncate(&mut self, side: Side, lsn: PgLsn, change: &FoldedChange) {
        Truncate::add(&mut self.truncate, side, lsn, change);
    }

    /// Adds `change` of the table on `side`.
    pub(crate) fn push(&mut self, side: Side, change: &FoldedChange) -> Result<(), ApplyError> {
        let apply = match &change.last_change {
            Some(last) if !change.has_recompute => {
                Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
            }
            _ => None,
        };
        let record = Record {
            key: change.key.clone(),
            apply,
            old_image: change.old_image.clone(),
        };
        match side {
            Side::From => {
                self.a_origins
                    .entry(change.key.clone())
                    .or_default()
                    .add(change);
                self.a.push(record);
            }
            Side::Joined(position) => {
                // Planted bug: only the first position's changes give the
                // origin a fan-out's held rows park with. See `crate::plant`.
                #[cfg(any(test, feature = "test-util"))]
                let skip = crate::plant::fires(
                    crate::plant::Plant::StarHeldOriginFirstOnly,
                    position != Position::FIRST,
                );
                #[cfg(not(any(test, feature = "test-util")))]
                let skip = false;
                if !skip {
                    self.held_origin.add(change);
                }
                self.b[position.index()].push(record);
            }
        }
        Ok(())
    }
}

/// Loads the plan for `definition` (a 1-1 `JOIN`) on a page whose first
/// change for it is of the table on `side`, `pk` being that table's primary
/// key. The target is keyed by the `FROM` table's, which a page that reaches
/// the target through the joined table first reads here.
pub(crate) async fn plan_for(
    pool: &Pool,
    definition: &Definition,
    side: Side,
    pk: Vec<PrimaryKeyColumn>,
) -> Result<JoinOneToOnePlan, ApplyError> {
    let pk = match side {
        Side::From => pk,
        Side::Joined(_) => ddl::source_primary_key(pool, &definition.source_table).await?,
    };
    let paused = super::quarantine::paused_columns_for(pool, &definition.def).await?;
    Ok(JoinOneToOnePlan::new(
        JoinShape::load(pool, definition, pk, &paused).await?,
    ))
}

/// The unquoted names of the key tuple `row`, as text: a key with any `NULL`
/// part is `None`.
type KeyText = Vec<String>;

/// The key texts of `rows`' columns from `first`, `None` for a row with a
/// `NULL` part.
fn key_of(row: &tokio_postgres::Row, first: usize, n: usize) -> Option<KeyText> {
    key_parts(row, first, n).into_iter().collect()
}

/// The key texts of `rows`' columns from `first`, each `None` for `NULL`.
fn key_parts(row: &tokio_postgres::Row, first: usize, n: usize) -> Vec<Option<String>> {
    (first..first + n)
        .map(|i| row.get::<_, Option<String>>(i))
        .collect()
}

/// Step 1, the fold: the record each key keeps. A Re-derive of a key
/// replaces its changes, and of two Applies the later one is kept.
fn fold(records: &[Record]) -> BTreeMap<&str, &Record> {
    let mut by_key: BTreeMap<&str, &Record> = BTreeMap::new();
    for record in records {
        by_key
            .entry(&record.key)
            .and_modify(|kept| {
                let supersedes = match (&record.apply, &kept.apply) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some((lsn, ..)), Some((kept_lsn, ..))) => lsn > kept_lsn,
                };
                if supersedes {
                    *kept = record;
                }
            })
            .or_insert(record);
    }
    by_key
}

/// A page's key set K, one set per position (index 0 is position 1): each
/// position's keys the page locks in `J`.
pub(crate) type PageKeys = Vec<BTreeSet<KeyText>>;

/// Adds `key` of `side`'s row ([`JoinShape::split_key`]) to `k`.
fn add_keys(shape: &JoinShape, side: Side, parts: &[Option<String>], k: &mut PageKeys) {
    for (position, key) in shape.split_key(side, parts) {
        k[position.index()].insert(key);
    }
}

/// Whether a key of `side`'s row ([`JoinShape::split_key`]) isn't in `k`:
/// one that moved after `k` was read, so the page rolls back and retries
/// with it (the late-key retry, at every position).
fn moved_key(shape: &JoinShape, side: Side, parts: &[Option<String>], k: &PageKeys) -> bool {
    let missing: Vec<Position> = shape
        .split_key(side, parts)
        .into_iter()
        .filter(|(position, key)| !k[position.index()].contains(key))
        .map(|(position, _)| position)
        .collect();
    if missing.is_empty() {
        return false;
    }
    // Planted bugs: the late-key retry skips one position of a star's root
    // row, the last (`star_late_skip_last`) or the first
    // (`star_late_skip_first`). See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if side == Side::From && shape.is_star() {
        let at = match crate::plant::armed() {
            Some(plant @ crate::plant::Plant::StarLateSkipLast) => {
                shape.positions.last().map(|p| (plant, p.position))
            }
            Some(plant @ crate::plant::Plant::StarLateSkipFirst) => Some((plant, Position::FIRST)),
            _ => None,
        };
        if let Some((plant, at)) = at
            && crate::plant::fires(plant, missing == [at])
        {
            return false;
        }
    }
    true
}

/// Whether a key of `side`'s row is outside the set the page holds it
/// under, so the page rolls back and retries: a `FROM` row's against K
/// (`k`, [`moved_key`]); a joined table's against the page's
/// fan-out keys at its position (`fan_k`), the keys whose `R` entries the
/// page locked, not K. A key K holds only for a root item of the page
/// would let the page recompute rows on it without their `R` entries.
fn key_moved(
    shape: &JoinShape,
    side: Side,
    parts: &[Option<String>],
    k: &PageKeys,
    fan_k: &PageKeys,
) -> bool {
    let Side::Joined(position) = side else {
        return moved_key(shape, side, parts, k);
    };
    if !moved_key(shape, side, parts, fan_k) {
        return false;
    }
    // Planted bugs: a star's joined-table re-check is against the page's
    // whole `J` set (`star_recheck_j`), or is at its last position
    // (`star_recheck_skip_last`). See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if shape.is_star() {
        let planted = match crate::plant::armed() {
            Some(plant @ crate::plant::Plant::StarRecheckJ) => Some(plant),
            Some(plant @ crate::plant::Plant::StarRecheckSkipLast)
                if shape.positions.last().map(|p| p.position) == Some(position) =>
            {
                Some(plant)
            }
            _ => None,
        };
        if let Some(plant) = planted
            && crate::plant::fires(plant, !moved_key(shape, side, parts, k))
        {
            return false;
        }
    }
    let _ = position;
    true
}

/// Settles one page of `plan` in `txn` (the module doc). Returns how many
/// target rows it wrote and deleted.
pub(crate) async fn apply_page(
    txn: &Transaction<'_>,
    plan: &JoinOneToOnePlan,
    first_seg: i64,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let shape = &*plan.shape;
    // 1. Fold.
    // Test-only: a share of pages re-derives. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    let forced = super::interleave::take_rederived_page();
    #[cfg(not(any(test, feature = "test-util")))]
    let forced = false;
    let predates = plan
        .a
        .iter()
        .chain(plan.b.iter().flatten())
        .any(|r| r.apply.is_some())
        && (forced || super::ledger::page_may_predate_build(txn, &shape.r, first_seg).await?);
    let rederived: (Vec<Record>, Vec<Vec<Record>>) = if predates {
        (
            plan.a.iter().map(Record::rederive).collect(),
            plan.b
                .iter()
                .map(|b| b.iter().map(Record::rederive).collect())
                .collect(),
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let (a_records, b_records): (&[Record], &[Vec<Record>]) = if predates {
        (&rederived.0, &rederived.1)
    } else {
        (&plan.a, &plan.b)
    };
    let a = fold(a_records);
    let b: Vec<BTreeMap<&str, &Record>> = b_records.iter().map(|b| fold(b)).collect();
    let truncate = plan.truncate.as_ref();
    if a.is_empty() && b.iter().all(BTreeMap::is_empty) && truncate.is_none() {
        return Ok((0, 0));
    }
    // Planted bug: apply every change without ADR-0002's I2. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let predicate = !crate::plant::fires(
        crate::plant::Plant::StaleOneToOneWrite,
        a.values()
            .chain(b.iter().flat_map(BTreeMap::values))
            .any(|r| r.apply.is_some()),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let predicate = true;

    let rederive_keys = |records: &BTreeMap<&str, &Record>| -> Vec<String> {
        records
            .values()
            .filter(|r| r.apply.is_none())
            .map(|r| r.key.clone())
            .collect()
    };
    let a_rederive = rederive_keys(&a);
    let b_rederive: Vec<Vec<String>> = b.iter().map(rederive_keys).collect();
    // Each joined table's primary key, which its Re-derives read its rows
    // by.
    let mut m_pk: Vec<Vec<PrimaryKeyColumn>> = Vec::with_capacity(b.len());
    for (p, keys) in shape.positions.iter().zip(&b_rederive) {
        m_pk.push(if keys.is_empty() {
            Vec::new()
        } else {
            ddl::source_primary_key_in_txn(txn, &p.table).await?
        });
    }
    let joined_sides = || {
        shape
            .positions
            .iter()
            .map(|p| (Side::Joined(p.position), p.position.index()))
            .filter(|(_, i)| !b[*i].is_empty())
    };

    // 2. The lock row, a truncate's clear, then K over every side's keys. A
    // page that truncates `b` holds its definition's row already: the caller
    // takes it before any lock row ([`lock_definitions`]).
    // Planted bug: the definition's row comes after the lock row. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let def_late = crate::plant::fires(
        crate::plant::Plant::Join1TruncateDefAfterLock,
        plan.truncates_b(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let def_late = false;
    // Planted bug: a truncate's page takes the lock row `FOR SHARE`, so a
    // page of the pair runs beside it. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let share = crate::plant::fires(
        crate::plant::Plant::Join1TruncateShareLock,
        truncate.is_some(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let share = false;
    let mode = if truncate.is_some() && !share {
        LockRowMode::Update
    } else {
        LockRowMode::Share
    };
    txn.execute(&lock_row_sql(&shape.lock, mode), &[]).await?;
    if def_late {
        txn.execute(
            "select 1 from transform_definitions where id = $1 for update",
            &[&shape.id],
        )
        .await?;
    }
    let cleared = match truncate {
        Some(truncate) => truncate_sides(txn, shape, truncate, mutations).await?,
        None => 0,
    };
    // The root's keys and the joined tables' apart, so a planted bug can
    // leave out the root's alone ([`lock_keys`]).
    let mut root_k = shape.empty_keys();
    if !a.is_empty() {
        let live = live_keys(txn, shape, Side::From, &shape.pk, &a_rederive).await?;
        key_set(txn, shape, Side::From, &a, &live, &mut root_k).await?;
    }
    let mut joined_k = shape.empty_keys();
    for (side, i) in joined_sides() {
        let live = live_keys(txn, shape, side, &m_pk[i], &b_rederive[i]).await?;
        key_set(txn, shape, side, &b[i], &live, &mut joined_k).await?;
    }
    let k: PageKeys = root_k
        .iter()
        .zip(&joined_k)
        .map(|(r, j)| r.union(j).cloned().collect())
        .collect();
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Lock,
        &shape.target,
    )
    .await?;
    // Test-only: a re-derived page holds its unlocked read open. See
    // `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if forced {
        super::interleave::rederive_stall().await;
    }

    // 3. `J`, one sorted statement over every position's keys.
    let locked = root_skip_j(shape, &k, &root_k, &joined_k);
    // Planted bug: the page reads `M` and `R` before it locks `J`. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let late_j = crate::plant::fires(
        crate::plant::Plant::Join1MBeforeJ,
        locked.iter().any(|k| !k.is_empty()),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let late_j = false;
    if !late_j {
        lock_keys(txn, shape, &locked).await?;
    }

    // 4. The `R` entries, the root's and every position's fan-out rows in
    // one statement (the R-entry rule), then each position's `M` entries in
    // position order. A joined change's fan-out keys are its keys in K, and
    // the budget picks those the page recomputes itself, whose rows it locks.
    let fan_k: PageKeys = joined_k;
    let inline = if plan.fan_out {
        fan_out::budget(txn, shape, &fan_k).await?
    } else {
        shape.empty_keys()
    };
    let (r_lock, late_r) = fan_lock_groups(shape, &inline, !a.is_empty());
    let (a_inserted, _) = lock_side(
        txn,
        shape,
        Side::From,
        &a,
        seg_seq,
        predicate,
        (&k, &fan_k),
        &r_lock,
    )
    .await?;
    let mut b_locked = Vec::with_capacity(b.len());
    for (side, i) in shape
        .positions
        .iter()
        .map(|p| (Side::Joined(p.position), p.position.index()))
    {
        b_locked.push(
            lock_side(
                txn,
                shape,
                side,
                &b[i],
                seg_seq,
                predicate,
                (&k, &fan_k),
                &[],
            )
            .await?,
        );
    }

    // 5. The Re-derive reads, one statement per side.
    let a_read = rederive_read(
        txn,
        shape,
        Side::From,
        &shape.pk,
        &a_rederive,
        (&k, &fan_k),
        seg_seq,
    )
    .await?;
    let mut b_read = Vec::with_capacity(b.len());
    for (side, i) in shape
        .positions
        .iter()
        .map(|p| (Side::Joined(p.position), p.position.index()))
    {
        b_read.push(
            rederive_read(
                txn,
                shape,
                side,
                &m_pk[i],
                &b_rederive[i],
                (&k, &fan_k),
                seg_seq,
            )
            .await?,
        );
    }

    // 6. I2, then each side's copied columns.
    let a_changed =
        update_side(txn, shape, Side::From, &a, &a_inserted, &a_read, predicate).await?;
    let mut b_changed = Vec::with_capacity(b.len());
    for (side, i) in shape
        .positions
        .iter()
        .map(|p| (Side::Joined(p.position), p.position.index()))
    {
        b_changed.push(
            update_side(
                txn,
                shape,
                side,
                &b[i],
                &b_locked[i].0,
                &b_read[i],
                predicate,
            )
            .await?,
        );
    }

    // 7. Recompute and write: the `a` changes' rows, then the fan-out of
    // each position in position order.
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::BeforeJoin1Recompute,
        &shape.target,
    )
    .await?;
    let mut changed_keys: Vec<&str> = a_changed.iter().map(String::as_str).collect();
    changed_keys.sort_unstable();
    let (mut written, mut deleted) =
        write_a_changes(txn, plan, &changed_keys, seg_seq, mutations).await?;
    let mut fanned_out: HashSet<String> = a_changed.clone();
    for p in &shape.positions {
        let i = p.position.index();
        let fan_keys = if plan.fan_out {
            fan_out_keys(txn, shape, p, &b_changed[i], &b_locked[i].1).await?
        } else {
            BTreeSet::new()
        };
        let (fanned, fan_deleted, entries) = fan_out(
            txn,
            plan,
            p,
            &fan_keys,
            &inline[i],
            &fanned_out,
            seg_seq,
            mutations,
        )
        .await?;
        fanned_out.extend(entries);
        written += fanned;
        deleted += fan_deleted;
        // Test-only pause point. See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        if !fan_keys.is_empty() {
            super::interleave::pause_at(
                txn,
                super::interleave::PausePoint::AfterJoin1PositionFanOut,
                &shape.target,
            )
            .await?;
        }
    }
    lock_fan_rows(txn, shape, &late_r).await?;
    if late_j {
        lock_keys(txn, shape, &locked).await?;
    }
    Ok((written, deleted + cleared))
}

/// The keys a page locks in `J`: `k`, but under a planted bug, a star's
/// root keys at one position that no joined change of the page holds too
/// (`root_k` and `joined_k`, the two parts of `k`). See `crate::plant`.
fn root_skip_j(
    shape: &JoinShape,
    k: &PageKeys,
    root_k: &PageKeys,
    joined_k: &PageKeys,
) -> PageKeys {
    #[cfg(any(test, feature = "test-util"))]
    if shape.is_star() {
        let at = match crate::plant::armed() {
            Some(plant @ crate::plant::Plant::StarRootSkipJLast) => {
                Some((plant, shape.positions.len() - 1))
            }
            Some(plant @ crate::plant::Plant::StarRootSkipJFirst) => Some((plant, 0)),
            // The middle position of a star of three or more joined tables.
            Some(plant @ crate::plant::Plant::StarRootSkipJMiddle) if shape.positions.len() > 2 => {
                Some((plant, shape.positions.len() / 2))
            }
            _ => None,
        };
        if let Some((plant, i)) = at {
            let root_only: BTreeSet<KeyText> =
                root_k[i].difference(&joined_k[i]).cloned().collect();
            if crate::plant::fires(plant, !root_only.is_empty()) {
                let mut locked = k.clone();
                locked[i] = joined_k[i].clone();
                return locked;
            }
        }
    }
    let _ = (shape, root_k, joined_k);
    k.clone()
}

/// The R-entry rule's lock of the fan-out rows on `keys` (each position's,
/// index 0 is position 1): a query of the entry keys of `R`'s rows on one of
/// them, one indexed `union all` arm per position, its parameters bound from
/// `$first`, and those parameters. `None` when no position has a key. The
/// lock statement matches its rows by entry key against it, never by an
/// `or` of the positions' keys, which would scan the whole of `R`.
fn fan_lock<'a>(
    shape: &JoinShape,
    keys: &'a PageKeys,
    first: usize,
) -> Option<(String, Vec<Vec<&'a str>>)> {
    let mut terms = Vec::new();
    let mut params: Vec<Vec<&str>> = Vec::new();
    for (at, keys) in shape.positions.iter().zip(keys) {
        if keys.is_empty() {
            continue;
        }
        let n = at.key.len();
        let from = first + params.len();
        let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", from + i)).collect();
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let alias = format!("u{}", at.position.number());
        let casts: Vec<String> = at
            .key
            .iter()
            .enumerate()
            .map(|(i, c)| cast_to(c, &format!("{alias}.c{i}")))
            .collect();
        let columns: Vec<String> = at
            .key
            .iter()
            .map(|c| format!("x.{}", quote_ident(&c.name)))
            .collect();
        terms.push(format!(
            "select x.{key} from {r} x where ({}) in (select {} from unnest({}) as {alias}({}))",
            columns.join(", "),
            casts.join(", "),
            arrays.join(", "),
            names.join(", "),
            key = quote_ident(schema::KEY_COLUMN),
            r = shape.r.ident,
        ));
        params.extend(key_arrays(n, keys));
    }
    (!terms.is_empty()).then(|| (terms.join(" union all "), params))
}

/// Locks the `R` entries on `keys` ([`fan_lock`]) `FOR UPDATE`, sorted, in
/// one statement of their own: under a planted bug only, since the R-entry
/// rule locks them with the page's root entries ([`lock_side`]).
async fn lock_fan_rows(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &PageKeys,
) -> Result<(), ApplyError> {
    let Some((rows, params)) = fan_lock(shape, keys, 1) else {
        return Ok(());
    };
    let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|c| c as _).collect();
    super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select 1 from {r} l where l.{key} in ({rows}) order by l.{key} for update of l",
            r = shape.r.ident,
            key = quote_ident(schema::KEY_COLUMN),
        ),
        &params,
    )
    .await?;
    Ok(())
}

/// The R-entry rule's lock statements of a page's fan-out rows on `inline`
/// (#1459 Decision item 1): one set, locked with the page's root entries
/// (`root`: whether it has any) in one statement. Planted bugs change it;
/// the second part is the rows a planted bug locks only after the page's
/// writes. See `crate::plant`.
fn fan_lock_groups(shape: &JoinShape, inline: &PageKeys, root: bool) -> (Vec<PageKeys>, PageKeys) {
    #[cfg(any(test, feature = "test-util"))]
    if shape.is_star() {
        use crate::plant::{Plant, armed, fires};
        let any = inline.iter().any(|keys| !keys.is_empty());
        let last = inline.len() - 1;
        match armed() {
            // A joined-table writer locks its own `J` but not the `R`
            // entries it recomputes.
            Some(plant @ Plant::StarOwnJOnly) if fires(plant, any) => {
                return (vec![shape.empty_keys()], shape.empty_keys());
            }
            // The `R` entries are locked only after the page read `M`.
            Some(plant @ Plant::StarRAfterM) if fires(plant, any) => {
                return (vec![shape.empty_keys()], inline.clone());
            }
            // The last position's writer doesn't lock them.
            Some(plant @ Plant::StarLastNoR) if fires(plant, !inline[last].is_empty()) => {
                let mut keys = inline.clone();
                keys[last].clear();
                return (vec![keys], shape.empty_keys());
            }
            // One statement per position, or at three or more positions
            // two: positions 1 and 2, then the rest.
            Some(plant @ Plant::StarRSplit) => {
                let bounds: Vec<std::ops::Range<usize>> = if inline.len() >= 3 {
                    vec![0..2, 2..inline.len()]
                } else {
                    (0..inline.len()).map(|i| i..i + 1).collect()
                };
                let groups: Vec<PageKeys> = bounds
                    .iter()
                    .map(|range| {
                        let mut keys = shape.empty_keys();
                        for i in range.clone() {
                            keys[i] = inline[i].clone();
                        }
                        keys
                    })
                    .collect();
                let statements = groups
                    .iter()
                    .enumerate()
                    .filter(|(i, keys)| (*i == 0 && root) || keys.iter().any(|k| !k.is_empty()))
                    .count();
                if fires(plant, statements > 1) {
                    return (groups, shape.empty_keys());
                }
            }
            _ => {}
        }
    }
    let _ = root;
    (vec![inline.clone()], shape.empty_keys())
}

/// Locks the definitions' rows of `plans` (one page's) before the page's
/// first 1-1 `JOIN` lock row, in id order, and returns the ids of those whose
/// page truncates `b` for [`start_rebuilds`]. A rebuild's start takes every
/// definition's row it rebuilds `FOR UPDATE`, in id order, before it takes
/// their lock rows (`crate::intake::catch_up`,
/// `super::build::BuildStarts::stamp`): a page that held one target's lock row
/// while it queued on a definition's row would close a cycle with a start.
///
/// - A page that truncates `b` takes its definition's row `FOR UPDATE`.
/// - A page that may hold a row of `a` takes it `FOR KEY SHARE`, the lock
///   `poison`'s foreign key takes when [`held::hold`] writes it, so the hold,
///   after the lock row, never waits for it: a page with `a` records (a
///   drain's, [`write_a_changes`]), and one whose fan-out may hold a row (it
///   has `b` records and fans out).
pub(crate) async fn lock_definitions<'a>(
    txn: &Transaction<'_>,
    plans: impl IntoIterator<Item = &'a JoinOneToOnePlan>,
) -> Result<Vec<i64>, ApplyError> {
    // Each id with whether the page truncates its `b`; a truncate's
    // `FOR UPDATE` covers a hold's `FOR KEY SHARE`.
    let mut locks: BTreeMap<i64, bool> = BTreeMap::new();
    for plan in plans {
        let truncates = plan.truncates_b();
        // Planted bug: only the first position's records count as a fan-out
        // that may hold a row. See `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let first_only = crate::plant::fires(
            crate::plant::Plant::StarDefLockFirstOnly,
            plan.b.iter().skip(1).any(|b| !b.is_empty()),
        );
        #[cfg(not(any(test, feature = "test-util")))]
        let first_only = false;
        let positions = if first_only { 1 } else { plan.b.len() };
        let may_hold = (plan.holds_a && !plan.a.is_empty())
            || (plan.fan_out && plan.b.iter().take(positions).any(|b| !b.is_empty()));
        if truncates || may_hold {
            *locks.entry(plan.shape.id).or_default() |= truncates;
        }
    }
    let ids: Vec<i64> = locks
        .iter()
        .filter(|(_, truncates)| **truncates)
        .map(|(id, _)| *id)
        .collect();
    // Planted bug: each definition's row is taken after its lock row, in
    // `apply_page`. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let late = crate::plant::fires(
        crate::plant::Plant::Join1TruncateDefAfterLock,
        !ids.is_empty(),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let late = false;
    for (id, truncates) in &locks {
        if *truncates && late {
            continue;
        }
        let mode = if *truncates { "update" } else { "key share" };
        txn.execute(
            &format!("select 1 from transform_definitions where id = $1 for {mode}"),
            &[id],
        )
        .await?;
    }
    Ok(ids)
}

/// A page's `TRUNCATE` of any of its tables, under the lock row held `FOR
/// UPDATE`, before the page's changes: each truncated table's ledger (`R`,
/// or the `M` of its position) is emptied and its floor raised to the truncate's `lsn`, so a change from
/// before it that still reaches a page is refused (ADR-0002 I2); and a
/// truncated `a` empties the target, through the seam. `J` keeps its rows,
/// as it keeps every key any table has held. A truncated `b`, at any
/// position, leaves the target to the one rebuild [`start_rebuilds`]
/// starts, however many positions the page truncates. Returns how many target
/// rows it deleted.
async fn truncate_sides(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    truncate: &Truncate,
    mutations: &mut TargetMutations,
) -> Result<usize, ApplyError> {
    let sides = truncate
        .from
        .map(|lsn| (Side::From, lsn))
        .into_iter()
        .chain(
            truncate
                .joined
                .iter()
                .map(|(position, lsn)| (Side::Joined(*position), *lsn)),
        );
    for (side, lsn) in sides {
        let ledger = shape.ledger(side);
        // Planted bugs: the entries stay, or the floor doesn't rise. See
        // `crate::plant`.
        #[cfg(any(test, feature = "test-util"))]
        let (keep, no_floor) = (
            crate::plant::fires(
                match side {
                    Side::From => crate::plant::Plant::Join1TruncateKeepsR,
                    Side::Joined(_) => crate::plant::Plant::Join1TruncateKeepsM,
                },
                true,
            ),
            crate::plant::fires(crate::plant::Plant::Join1TruncateNoFloor, true),
        );
        #[cfg(not(any(test, feature = "test-util")))]
        let (keep, no_floor) = (false, false);
        // A `DELETE`, not a `TRUNCATE`: a sweep or `M` pass batch reads the
        // ledger before it takes the lock row this page holds, and would wait
        // for the page while a `TRUNCATE` waited for its read (the module
        // doc).
        if !keep {
            txn.batch_execute(&format!("delete from {}", ledger.ident))
                .await?;
        }
        if !no_floor {
            ledger::raise_floor(txn, ledger, lsn).await?;
        }
    }
    // Every pending fan-out goes: a truncated `b` starts a rebuild, which
    // recomputes every row, and a truncated `a` leaves no row to recompute.
    let truncated: Vec<Position> = truncate.joined.keys().copied().collect();
    fan_out::clear_all(txn, shape, &truncated).await?;
    if truncate.from.is_none() {
        return Ok(0);
    }
    super::apply::clear_target(
        txn,
        &shape.target,
        &shape.pk,
        truncate.hop_gen,
        truncate.src_changed,
        truncate.origin_lsn,
        mutations,
    )
    .await
}

/// Starts the rebuilds of the targets `ids` ([`lock_definitions`])
/// after a `TRUNCATE` of their `b`, in the page's transaction (ADR-0021):
/// every target row may hold the `b` side of a row that is gone, so the
/// build's chunks, not the page, rewrite them. Each definition reads
/// `backfilling` from the commit, and keeps applying; a `LEFT` row keeps its
/// old `b` columns, and an `INNER` row its place, until the rebuild reaches
/// it. A definition that doesn't apply any more (paused since the page read
/// the catalog) is left to its `RESUME`, whose build recreates `R` and `M`
/// and re-derives every row. The caller holds the definitions' rows and
/// their lock rows `FOR UPDATE`, and calls this after it has locked its
/// segments' rows to mark them drained: the start's segment stamp takes the
/// segment pointer `FOR SHARE`, and seal phase 1 takes the sealed segment's
/// predecessor's row before it updates the pointer.
pub(crate) async fn start_rebuilds(txn: &Transaction<'_>, ids: &[i64]) -> Result<(), ApplyError> {
    // Planted bug: the truncate starts no rebuild. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::fires(crate::plant::Plant::Join1TruncateNoRebuild, !ids.is_empty()) {
        return Ok(());
    }
    if ids.is_empty() {
        return Ok(());
    }
    let ids = super::build::rebuildable(txn, ids).await?;
    let started = super::build::start_rebuilds(txn, ids).await?;
    Ok(started.stamp(txn).await?)
}

/// The live join key of each of `keys`' rows of `side`'s table (whose
/// primary key is `pk`), read unlocked: the Re-derives' part of K. A key is
/// its columns' text, `None` for a `NULL` part; no row contributes none.
async fn live_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    pk: &[PrimaryKeyColumn],
    keys: &[String],
) -> Result<Vec<Vec<Option<String>>>, ApplyError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let names = shape.side_key(side);
    let query = super::apply::live_rows_query(shape.table(side), pk, &names, &keys)?;
    let cols: Vec<String> = names
        .iter()
        .map(|n| format!("m.doc ->> {}", quote_literal(n)))
        .collect();
    let sql = format!("select {} from ({}) m", cols.join(", "), query.docs);
    let rows = super::ledger::query_by_entry_key(txn, &sql, &query.params()).await?;
    Ok(rows
        .iter()
        .map(|row| key_parts(row, 0, names.len()))
        .collect())
}

/// `side`'s part of K, added to `k`: every image's new key, for a `b` change
/// also its old key from its first change's pre-image, the key the side's
/// entry holds for each of `records`, and the `live` keys, as the sorted set
/// of their texts at each position ([`JoinShape::split_key`]). Each key is
/// cast to `J`'s types. A key with a `NULL` part is left out.
async fn key_set(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    live: &[Vec<Option<String>>],
    k: &mut PageKeys,
) -> Result<(), ApplyError> {
    let types = shape.side_key_types(side);
    let n = types.len();
    let names = shape.side_key(side);
    // Planted bug: only the changes' new keys are locked. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let only_new = crate::plant::fires(crate::plant::Plant::Join1OldKeyUnlocked, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let only_new = false;
    let keys: Vec<&str> = records.keys().copied().collect();
    let images: Vec<Option<&str>> = records
        .values()
        .map(|r| match &r.apply {
            Some((_, _, Some(image))) => Some(image.as_str()),
            _ => None,
        })
        .collect();
    let old_images: Vec<Option<&str>> = records
        .values()
        .map(|r| match side {
            Side::Joined(_) if !only_new => r.old_image.as_deref(),
            _ => None,
        })
        .collect();
    let live_columns: Vec<Vec<Option<&str>>> = (0..n)
        .map(|i| live.iter().map(|k| k[i].as_deref()).collect())
        .collect();
    let aliases =
        |prefix: &str| -> Vec<String> { (0..n).map(|i| format!("{prefix}{i}")).collect() };
    let typed = |expr: &dyn Fn(usize, &str) -> String| -> Vec<String> {
        types
            .iter()
            .zip(&names)
            .enumerate()
            .map(|(i, (c, name))| format!("{} as k{i}", cast_to(c, &expr(i, name))))
            .collect()
    };
    let from_image = typed(&|_, name| format!("ch.img ->> {}", quote_literal(name)));
    let from_held = typed(&|_, name| format!("l.{}", quote_ident(name)));
    let from_live = typed(&|i, _| format!("lv.c{i}"));
    let live_arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", 4 + i)).collect();
    let texts: Vec<String> = aliases("k").iter().map(|k| format!("{k}::text")).collect();
    // A key with a `NULL` part is left out at its position: a `FROM` row's
    // keys at the other positions still count ([`JoinShape::split_key`]).
    let present: Vec<String> = aliases("k")
        .iter()
        .map(|k| format!("{k} is not null"))
        .collect();
    let held = if only_new {
        "$1::text[] = '{}'::text[] and"
    } else {
        ""
    };
    let sql = format!(
        "with ch as (select u.img::jsonb as img from unnest($2::text[]) as u(img) \
                     where u.img is not null \
                     union all \
                     select u.img::jsonb from unnest($3::text[]) as u(img) \
                     where u.img is not null), \
         cand as ( \
             select {from_image} from ch \
             union \
             select {from_held} from {ledger} l where {held} l.{key} = any($1::text[]) \
             union \
             select {from_live} from unnest({live_arrays}) as lv({live_names}) \
         ) \
         select {texts} from cand where {present}",
        from_image = from_image.join(", "),
        from_held = from_held.join(", "),
        from_live = from_live.join(", "),
        live_arrays = live_arrays.join(", "),
        live_names = aliases("c").join(", "),
        texts = texts.join(", "),
        present = present.join(" or "),
        ledger = shape.ledger(side).ident,
        key = quote_ident(schema::KEY_COLUMN),
    );
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![&keys, &images, &old_images];
    for column in &live_columns {
        params.push(column);
    }
    let rows = super::ledger::query_by_entry_key(txn, &sql, &params).await?;
    for row in &rows {
        add_keys(shape, side, &key_parts(row, 0, n), k);
    }
    Ok(())
}

/// `keys` as one text array per key column, for [`key_unnest`].
fn key_arrays(n: usize, keys: &BTreeSet<KeyText>) -> Vec<Vec<&str>> {
    (0..n)
        .map(|i| keys.iter().map(|key| key[i].as_str()).collect())
        .collect()
}

/// The `unnest` of [`key_arrays`] bound from parameter `$1` as `u(c0, ..)`,
/// and each column cast to its `J` type, `key`'s.
fn key_unnest(key: &[ImageColumn]) -> (String, Vec<String>) {
    let n = key.len();
    let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", i + 1)).collect();
    let unnest = format!(
        "unnest({}) as u({})",
        arrays.join(", "),
        (0..n)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let casts = key
        .iter()
        .enumerate()
        .map(|(i, c)| cast_to(c, &format!("u.c{i}")))
        .collect();
    (unnest, casts)
}

/// A page's two `J` statements over the keys of some positions, and their
/// parameters ([`j_statements`]).
#[derive(Debug)]
pub(crate) struct JStatements<'a> {
    /// The insert of every key: one statement for the page.
    pub(crate) insert: String,
    /// The lock of every key's row: one statement for the page.
    pub(crate) lock: String,
    /// One text array per key column, position by position: `$1` and on.
    pub(crate) params: Vec<Vec<&'a str>>,
}

/// `J`'s insert-then-lock over `groups`, each a position's keys in position
/// order (ADR-0021, #1765): one `INSERT … ON CONFLICT DO NOTHING` with no
/// conflict target, so every position's partial unique index is an arbiter,
/// sorted by `(position, key)`; then one `FOR UPDATE` over every position's
/// rows sorted the same way. Each key text is cast to its column's type at
/// `R`'s collation, so keys equal by value meet on one row. In the lock, each
/// position's rows are found by `position = i` and its group's columns,
/// through that position's partial index, and the rows of every position are
/// then locked in one sorted pass. A group with no keys takes no part, and
/// `None` is returned when no group has any. Insert-then-lock is exact
/// because `J` rows are never deleted.
pub(crate) fn j_statements<'a>(j: &str, groups: &[PositionKeys<'a>]) -> Option<JStatements<'a>> {
    let groups: Vec<&PositionKeys<'a>> = groups.iter().filter(|g| !g.keys.is_empty()).collect();
    if groups.is_empty() {
        return None;
    }
    let mut params: Vec<Vec<&'a str>> = Vec::new();
    // Each group's `unnest` of its parameters as `u(c0, ..)`, and its key
    // texts cast to `J`'s types.
    let mut unnests = Vec::with_capacity(groups.len());
    for group in &groups {
        let n = group.position.key.len();
        let first = params.len() + 1;
        params.extend(key_arrays(n, group.keys));
        let arrays: Vec<String> = (0..n).map(|i| format!("${}::text[]", first + i)).collect();
        let names: Vec<String> = (0..n).map(|i| format!("c{i}")).collect();
        let casts: Vec<String> = group
            .position
            .key
            .iter()
            .enumerate()
            .map(|(i, c)| cast_to(c, &format!("u.c{i}")))
            .collect();
        unnests.push((
            format!("unnest({}) as u({})", arrays.join(", "), names.join(", ")),
            casts,
        ));
    }
    let group_columns = |group: &PositionKeys<'_>| -> Vec<String> {
        group
            .position
            .j_key
            .iter()
            .map(|c| quote_ident(c))
            .collect()
    };
    let mut columns = vec!["position".to_string()];
    for group in &groups {
        columns.extend(group_columns(group));
    }
    // Planted bug: the `J` statements run unsorted. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let unsorted = crate::plant::fires(
        crate::plant::Plant::StarJUnsorted,
        groups.len() > 1 || params.first().is_some_and(|keys| keys.len() > 1),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let unsorted = false;

    // The insert: one arm per group, its own columns cast and every other
    // group's `NULL` at its type.
    let arms: Vec<String> = groups
        .iter()
        .zip(&unnests)
        .map(|(group, (unnest, casts))| {
            let mut values = vec![format!("{}::smallint", group.position.position.number())];
            for other in &groups {
                if other.position.position == group.position.position {
                    values.extend(casts.iter().cloned());
                } else {
                    values.extend(other.position.key.iter().map(|c| cast_to(c, "null")));
                }
            }
            format!("select {} from {unnest}", values.join(", "))
        })
        .collect();
    let ordinals: Vec<String> = (1..=columns.len()).map(|i| i.to_string()).collect();
    let insert_order = if unsorted {
        String::new()
    } else {
        format!(" order by {}", ordinals.join(", "))
    };
    let insert = format!(
        "insert into {j} ({cols}) select k.* from ({arms}) k{insert_order} on conflict do nothing",
        cols = columns.join(", "),
        arms = arms.join(" union all "),
    );

    // The lock: each group's rows through its partial index, then every
    // row in one sorted pass.
    let finds: Vec<String> = groups
        .iter()
        .zip(&unnests)
        .map(|(group, (unnest, casts))| {
            let qualified: Vec<String> = group_columns(group)
                .iter()
                .map(|c| format!("g.{c}"))
                .collect();
            format!(
                "select g.ctid from {j} g where g.position = {} and ({}) in (select {} from {unnest})",
                group.position.position.number(),
                qualified.join(", "),
                casts.join(", "),
            )
        })
        .collect();
    let lock_order = if unsorted {
        String::new()
    } else {
        let qualified: Vec<String> = columns.iter().map(|c| format!("j.{c}")).collect();
        format!(" order by {}", qualified.join(", "))
    };
    let lock = format!(
        "select 1 from {j} j where j.ctid = any(array({finds})){lock_order} for update of j",
        finds = finds.join(" union all "),
    );
    Some(JStatements {
        insert,
        lock,
        params,
    })
}

/// Inserts K into `J` and locks it, every position's keys in one sorted
/// statement each ([`j_statements`]).
async fn lock_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    k: &PageKeys,
) -> Result<(), ApplyError> {
    let groups: Vec<PositionKeys<'_>> = shape
        .positions
        .iter()
        .zip(k)
        .map(|(position, keys)| PositionKeys { position, keys })
        .collect();
    let statements = j_statements(&shape.j, &groups);
    let params: Vec<&(dyn ToSql + Sync)> = statements
        .iter()
        .flat_map(|s| s.params.iter().map(|c| c as &(dyn ToSql + Sync)))
        .collect();
    if let Some(statements) = &statements {
        txn.execute(&statements.insert, &params).await?;
    }
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterJoin1KeyInsert,
        &shape.target,
    )
    .await?;
    // Planted bug: the page doesn't lock `J`. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip = crate::plant::fires(crate::plant::Plant::Join1NoJLock, statements.is_some());
    #[cfg(not(any(test, feature = "test-util")))]
    let skip = false;
    if let Some(statements) = &statements
        && !skip
    {
        txn.execute(&statements.lock, &params).await?;
    }
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterJoinLock,
        &shape.target,
    )
    .await?;
    Ok(())
}

/// Step 4 for one side: `records`' placeholders and sorted entry lock
/// ([`super::one_to_one_ledger::lock_entries`], I5), then the check that each
/// held key is in the page's set ([`check_held_keys`]). Returns the keys
/// whose entry it inserted, and the key each entry held under the lock.
///
/// On the `FROM` side, `fan` are the R-entry rule's fan-out rows
/// ([`fan_lock`]): the first set's are locked in the one statement with
/// `records`' entries, and only a planted bug splits off the others, each
/// locked in a statement of its own after it.
#[allow(clippy::too_many_arguments)]
async fn lock_side(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    seg_seq: i64,
    predicate: bool,
    (k, fan_k): (&PageKeys, &PageKeys),
    fan: &[PageKeys],
) -> Result<(ledger::Inserted, HashMap<String, KeyText>), ApplyError> {
    let first = fan.first().and_then(|keys| fan_lock(shape, keys, 2));
    if records.is_empty() && fan.iter().all(|keys| keys.iter().all(BTreeSet::is_empty)) {
        return Ok((ledger::Inserted::default(), HashMap::new()));
    }
    let to_lock: Vec<EntryChange<'_>> = records
        .values()
        .map(|r| EntryChange {
            key: &r.key,
            apply: r.apply.as_ref().map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present: r.apply.as_ref().is_none_or(|(_, _, image)| image.is_some()),
        })
        .collect();
    // Planted bug: the page doesn't lock the entries it reads. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::Join1NoEntryLock, !records.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    let ledger = shape.ledger(side);
    let also = first.as_ref().map(|(rows, params)| ledger::AlsoLock {
        keys: rows.clone(),
        params: params.iter().map(|c| c as &(dyn ToSql + Sync)).collect(),
    });
    let inserted = if to_lock.is_empty() && also.is_none() {
        ledger::Inserted::default()
    } else {
        ledger::lock_entries(
            txn,
            ledger,
            &to_lock,
            seg_seq,
            predicate,
            skip_lock,
            also.as_ref(),
        )
        .await?
    };
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if side == Side::From {
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::AfterJoin1RLock,
            &shape.target,
        )
        .await?;
    }
    for keys in fan.iter().skip(1) {
        lock_fan_rows(txn, shape, keys).await?;
    }
    let keys: Vec<&str> = records.keys().copied().collect();
    let held = if keys.is_empty() {
        HashMap::new()
    } else {
        check_held_keys(txn, shape, side, &keys, (k, fan_k)).await?
    };
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        match side {
            Side::From => super::interleave::PausePoint::AfterJoin1REntryLock,
            Side::Joined(_) => super::interleave::PausePoint::AfterJoin1MEntryLock,
        },
        &ledger.label,
    )
    .await?;
    Ok((inserted, held))
}

/// Checks that the key each `keys` entry of `side`'s ledger holds at every
/// position, now that the page has locked them, is in the page's set
/// ([`key_moved`]: K, `k`, for `FROM` entries, the fan-out keys `fan_k` for
/// a joined table's): one outside it moved after the set was read. Returns
/// each entry's held key, for a joined side (none for a `NULL` part; a
/// `FROM` entry's keys are only checked).
async fn check_held_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    keys: &[&str],
    (k, fan_k): (&PageKeys, &PageKeys),
) -> Result<HashMap<String, KeyText>, ApplyError> {
    let n = shape.side_key(side).len();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select l.{key}, {texts} from {ledger} l where l.{key} = any($1::text[])",
            texts = shape.key_texts(side, "l").join(", "),
            ledger = shape.ledger(side).ident,
            key = quote_ident(schema::KEY_COLUMN)
        ),
        &[&keys],
    )
    .await?;
    let parts: Vec<Vec<Option<String>>> = rows.iter().map(|row| key_parts(row, 1, n)).collect();
    let held: HashMap<String, KeyText> = match side {
        Side::From => HashMap::new(),
        Side::Joined(_) => rows
            .iter()
            .filter_map(|row| Some((row.get::<_, String>(0), key_of(row, 1, n)?)))
            .collect(),
    };
    // Test-only: a key that moves on every attempt. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    if side == Side::From && super::interleave::take_forced_join_key_move(txn).await? {
        super::interleave::note_join_key_moved(txn).await;
        return Err(ApplyError::JoinKeyMoved {
            target: shape.target.clone(),
        });
    }
    // Planted bug: the page doesn't check its held keys. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip = crate::plant::fires(crate::plant::Plant::Join1NoRecheck, !rows.is_empty());
    #[cfg(not(any(test, feature = "test-util")))]
    let skip = false;
    if !skip
        && parts
            .iter()
            .any(|key| key_moved(shape, side, key, k, fan_k))
    {
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::note_join_key_moved(txn).await;
        return Err(ApplyError::JoinKeyMoved {
            target: shape.target.clone(),
        });
    }
    Ok(held)
}

/// What one side's Re-derive read found ([`rederive_read`]): each Re-derive
/// key's live row (`None` for a row that is gone), the snapshot that is
/// their basis, and the segment their entries are stamped with.
#[derive(Default)]
struct Rederived {
    rows: HashMap<String, Option<String>>,
    snapshot: Option<String>,
    seg: i64,
}

/// Step 5 for one side: one statement reads the live rows of `keys` (the
/// side's Re-derives) from its table, whose primary key is `pk`, and
/// `pg_current_snapshot()` ([`super::one_to_one_ledger::read_rows`]). A key
/// a row has that isn't in `k` moved after the unlocked read, so the page
/// rolls back with [`ApplyError::JoinKeyMoved`] and retries. No `keys` read
/// nothing, and the entries are stamped with the page's `seg_seq`.
async fn rederive_read(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    pk: &[PrimaryKeyColumn],
    keys: &[String],
    k: (&PageKeys, &PageKeys),
    seg_seq: i64,
) -> Result<Rederived, ApplyError> {
    if keys.is_empty() {
        return Ok(Rederived {
            seg: seg_seq,
            ..Rederived::default()
        });
    }
    let names: Vec<String> = shape
        .side_columns(side)
        .iter()
        .map(|c| c.name.clone())
        .collect();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    // The `a` side's pause points carry the target's label, the `b` side's
    // `M`'s.
    let label = match side {
        Side::From => shape.target.as_str(),
        Side::Joined(position) => shape.joined(position).m.label.as_str(),
    };
    let read = ledger::read_rows(txn, label, shape.table(side), pk, &names, &refs).await?;
    // Planted bugs: the page doesn't check the keys its read of one side
    // found. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let unchecked = crate::plant::fires(
        match side {
            Side::From => crate::plant::Plant::Join1RdNoKey,
            Side::Joined(_) => crate::plant::Plant::Join1BrdNoKey,
        },
        true,
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let unchecked = false;
    if !unchecked {
        check_read_keys(txn, shape, side, &read.rows, k).await?;
    }
    // Planted bug: the basis is a snapshot taken one statement after the
    // read. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let read = if crate::plant::fires(crate::plant::Plant::Join1SnapshotAfterRead, true) {
        let later: String = txn
            .query_one("select pg_catalog.pg_current_snapshot()::text", &[])
            .await?
            .get(0);
        ledger::ReadRows {
            snapshot: later,
            ..read
        }
    } else {
        read
    };
    // Test-only pause point. See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(txn, super::interleave::PausePoint::AfterJoin1Read, label).await?;
    let mut found = read.rows;
    Ok(Rederived {
        rows: keys
            .iter()
            .map(|key| (key.clone(), found.remove(key)))
            .collect(),
        snapshot: Some(read.snapshot),
        seg: seg_seq.max(read.seg),
    })
}

/// Checks that the join key of each live row of `side` a Re-derive read is
/// in `k`. Define gives each `ON` pair one type and collation, so a `b`
/// column's text is the text its `J` key has.
async fn check_read_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    rows: &HashMap<String, String>,
    (k, fan_k): (&PageKeys, &PageKeys),
) -> Result<(), ApplyError> {
    // Read only to count a move, under test (`super::interleave`).
    #[cfg(not(any(test, feature = "test-util")))]
    let _ = txn;
    let names = shape.side_key(side);
    for doc in rows.values() {
        let value: serde_json::Value =
            serde_json::from_str(doc).map_err(|_| ApplyError::JoinKeyMoved {
                target: shape.target.clone(),
            })?;
        let key: Vec<Option<String>> = names
            .iter()
            .map(|name| value.get(name).and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        if key_moved(shape, side, &key, k, fan_k) {
            #[cfg(any(test, feature = "test-util"))]
            super::interleave::note_join_key_moved(txn).await;
            return Err(ApplyError::JoinKeyMoved {
                target: shape.target.clone(),
            });
        }
    }
    Ok(())
}

/// Step 6 for one side: `records`' entries updated by I2 (a Re-derive sets
/// the basis from its read's snapshot), the placeholders nothing wrote
/// dropped, then the side's key and read columns written for the entries
/// that changed: a present row from its image or the Re-derive's read
/// (`read`), and a gone one with its key cleared, so it matches nothing.
/// Returns the keys that changed.
async fn update_side(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    records: &BTreeMap<&str, &Record>,
    inserted: &ledger::Inserted,
    read: &Rederived,
    predicate: bool,
) -> Result<HashSet<String>, ApplyError> {
    if records.is_empty() {
        return Ok(HashSet::new());
    }
    let (rederived, snapshot, entry_seg) = (&read.rows, read.snapshot.as_deref(), read.seg);
    let ledger = shape.ledger(side);
    let mut changes: Vec<EntryChange<'_>> = Vec::with_capacity(records.len());
    for record in records.values() {
        if record.apply.is_some() && inserted.keys.contains(&record.key) {
            continue;
        }
        let present = match &record.apply {
            Some((_, _, image)) => image.is_some(),
            None => match rederived.get(record.key.as_str()) {
                Some(row) => row.is_some(),
                None => continue,
            },
        };
        changes.push(EntryChange {
            key: &record.key,
            apply: record
                .apply
                .as_ref()
                .map(|(lsn, txid, _)| (*lsn, txid.as_str())),
            present,
        });
    }
    let mut changed =
        ledger::update_entries(txn, ledger, &changes, snapshot, entry_seg, predicate).await?;
    changed.extend(inserted.applied.iter().cloned());
    let unwritten: Vec<&str> = inserted
        .keys
        .iter()
        .filter(|key| !changed.contains(*key))
        .map(String::as_str)
        .collect();
    ledger::drop_placeholders(txn, ledger, &unwritten).await?;

    let mut present_keys: Vec<&str> = Vec::new();
    let mut images: Vec<&str> = Vec::new();
    let mut gone: Vec<&str> = Vec::new();
    for record in records.values() {
        if !changed.contains(&record.key) {
            continue;
        }
        let image = match &record.apply {
            Some((_, _, image)) => image.as_deref(),
            None => rederived
                .get(record.key.as_str())
                .and_then(|row| row.as_deref()),
        };
        match image {
            Some(image) => {
                present_keys.push(&record.key);
                images.push(image);
            }
            None => gone.push(&record.key),
        }
    }
    write_entries(txn, shape, side, &present_keys, &images, &gone).await?;
    Ok(changed)
}

/// Writes `side`'s key and read columns for the entries that changed:
/// `present` ones from their `images`, and the key cleared for the `gone`.
async fn write_entries(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    present: &[&str],
    images: &[&str],
    gone: &[&str],
) -> Result<(), ApplyError> {
    let key = quote_ident(schema::KEY_COLUMN);
    let ledger = &shape.ledger(side).ident;
    if !present.is_empty() {
        let columns = shape.side_columns(side);
        let sets: Vec<String> = columns
            .iter()
            .map(|c| format!("{n} = s.{n}", n = quote_ident(&c.name)))
            .collect();
        let render = OneToOneRender::of_columns(columns.to_vec());
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {ledger} l set {sets} \
                 from unnest($1::text[], $2::text[]::jsonb[]) as v(__key, __img) \
                      cross join {row} \
                 where l.{key} = any($1::text[]) and l.{key} = v.__key",
                sets = sets.join(", "),
                row = render.row_sql("v.__img"),
            ),
            &[&present, &images],
        )
        .await?;
    }
    if !gone.is_empty() {
        // A star's positions may join on one root column, which `R`'s key
        // then holds once per position: clear each column once.
        let mut names = shape.side_key(side);
        let mut seen = std::collections::HashSet::new();
        names.retain(|c| seen.insert(c.clone()));
        let sets: Vec<String> = names
            .iter()
            .map(|c| format!("{} = null", quote_ident(c)))
            .collect();
        super::ledger::query_by_entry_key(
            txn,
            &format!(
                "update {ledger} l set {sets} where l.{key} = any($1::text[])",
                sets = sets.join(", "),
            ),
            &[&gone],
        )
        .await?;
    }
    Ok(())
}

/// The keys at position `at` a page's changes of its joined table fan out
/// on: each changed `M` entry's key before the page (`held`, read under its
/// entry lock) and after it (read back from `M`). Every one is in K, so the
/// page holds its `J` lock.
async fn fan_out_keys(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    at: &JoinedPosition,
    changed: &HashSet<String>,
    held: &HashMap<String, KeyText>,
) -> Result<BTreeSet<KeyText>, ApplyError> {
    if changed.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut keys: BTreeSet<KeyText> = changed
        .iter()
        .filter_map(|k| held.get(k))
        .cloned()
        .collect();
    let changed: Vec<&str> = changed.iter().map(String::as_str).collect();
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select {texts} from {m} l where l.{key} = any($1::text[]) and not l.{tomb}",
            texts = shape.key_texts(Side::Joined(at.position), "l").join(", "),
            m = at.m.ident,
            key = quote_ident(schema::KEY_COLUMN),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
        ),
        &[&changed],
    )
    .await?;
    keys.extend(rows.iter().filter_map(|row| key_of(row, 0, at.key.len())));
    Ok(keys)
}

/// The most attempts [`fan_out_failure`]'s bisection makes, as
/// `super::quarantine::MAX_ISOLATION_PROBES` bounds a page's.
const MAX_FAN_OUT_PROBES: usize = 256;

/// The most failing rows a halt of [`fan_out_failure`] names.
const MAX_NAMED_FAN_OUT_ROWS: usize = 8;

/// Step 7's write of the `a` changes' rows `keys` ([`write_target`]), under
/// a savepoint in a drain's page ([`JoinOneToOnePlan::holds_a`]), as the
/// fan-out writes under one. A data exception rolls the write back to it and
/// bisects the rows down to those whose fields fail ([`fan_out_failure`]):
/// the page holds each in quarantine on this first failure ([`held::hold`]),
/// parked with the earliest origin of the page's changes of that row and its
/// `b` changes, and writes every other row. Step 6 has written the held row's change into `R`; its target
/// row stays as it was, since nothing writes a held row's target from its
/// `R` entry, until its release re-derives both from the live row ([`held`]).
/// A bisection that can't name every failing row within
/// [`MAX_FAN_OUT_PROBES`] attempts halts the transform. A build's page writes
/// without the savepoint and leaves a failure to its caller.
async fn write_a_changes(
    txn: &Transaction<'_>,
    plan: &JoinOneToOnePlan,
    keys: &[&str],
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let shape = &*plan.shape;
    let origin = WriteOrigin::Each(&plan.a_origins, &plan.held_origin);
    if !plan.holds_a || keys.is_empty() {
        return write_target(txn, shape, keys, Entries::Changed, origin, mutations).await;
    }
    txn.batch_execute("savepoint join1_a_write").await?;
    match write_target(txn, shape, keys, Entries::Changed, origin, mutations).await {
        Ok(counts) => {
            txn.batch_execute("release savepoint join1_a_write").await?;
            Ok(counts)
        }
        Err(err) if super::one_to_one_render::is_data_exception(&err) => {
            txn.batch_execute("rollback to savepoint join1_a_write")
                .await?;
            let (written, deleted, rows) =
                fan_out_failure(txn, shape, Side::From, keys, origin, mutations).await?;
            // The row's target row reflects neither its own changes nor the
            // page's `b` changes, which `M` now holds: it parks with the
            // earliest origin of both, so a token after a `b` change its row
            // lacks doesn't converge.
            let origins: Vec<held::HeldOrigin> = rows
                .iter()
                .map(|row| {
                    plan.a_origins
                        .get(&row.key)
                        .cloned()
                        .unwrap_or_else(held::HeldOrigin::unknown)
                        .merged(&plan.held_origin)
                })
                .collect();
            let held: Vec<_> = rows.iter().zip(&origins).collect();
            held::hold(txn, shape, seg_seq, Side::From, &held).await?;
            plan.held.record(rows);
            txn.batch_execute("release savepoint join1_a_write").await?;
            Ok((written, deleted))
        }
        Err(err) => Err(err),
    }
}

/// Step 7's fan-out (the module doc) of the changes of the joined table at
/// `at`: the target row of every live `R` entry on `keys` there is
/// recomputed from `R` and the newest live `M` entry at each position, but
/// the `written` ones, which the page's `a` changes wrote already, and
/// those of the `a` rows the definition holds in quarantine, which stay as
/// they are until their release re-derives them ([`held`]). The page holds
/// `J` for each of `keys` (they are in K), and every writer of an `R` entry
/// on a key, or of an `M` entry on it, holds that key's `J` row too, so the
/// entries read here are the ones the key has until the page commits.
///
/// The page recomputes the keys whose rows fit one chunk's budget itself, and
/// leaves every other key to its chunks under a pending marker it writes
/// ([`fan_out::split`]); a key it recomputes whole loses the marker an
/// earlier page left it. Returns the rows written and deleted, and the `R`
/// entries it wrote.
#[allow(clippy::too_many_arguments)]
async fn fan_out(
    txn: &Transaction<'_>,
    plan: &JoinOneToOnePlan,
    at: &JoinedPosition,
    keys: &BTreeSet<KeyText>,
    inline: &BTreeSet<KeyText>,
    written: &HashSet<String>,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize, Vec<String>), ApplyError> {
    if keys.is_empty() {
        return Ok((0, 0, Vec::new()));
    }
    let shape = &*plan.shape;
    let inline = fan_out::split(txn, plan, at, keys, inline, seg_seq).await?;
    let entries: Vec<String> = live_entries_on(txn, shape, at, &inline)
        .await?
        .into_iter()
        .filter(|key| !written.contains(key))
        .collect();
    let (w, d) = write_fan_out(
        txn,
        shape,
        at,
        &entries,
        &plan.held_origin,
        seg_seq,
        mutations,
        &plan.held,
    )
    .await?;
    Ok((w, d, entries))
}

/// The keys of the live `R` entries on `keys` at position `at`, sorted, but
/// those of the `a` rows the definition holds in quarantine.
async fn live_entries_on(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    at: &JoinedPosition,
    keys: &BTreeSet<KeyText>,
) -> Result<Vec<String>, ApplyError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let columns = key_arrays(at.key.len(), keys);
    let mut params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
    let (unnest, casts) = key_unnest(&at.key);
    let qualified: Vec<String> = at
        .key
        .iter()
        .map(|c| format!("r.{}", quote_ident(&c.name)))
        .collect();
    let n = params.len();
    params.push(&shape.id);
    params.push(&shape.source_table);
    let not_held = not_held_filter(txn, shape, n + 1, n + 2).await?;
    let rows = super::ledger::query_by_entry_key(
        txn,
        &format!(
            "select r.{key} from {r} r \
             where ({qualified}) in (select {casts} from {unnest}) and not r.{tomb}{not_held}",
            key = quote_ident(schema::KEY_COLUMN),
            r = shape.r.ident,
            qualified = qualified.join(", "),
            casts = casts.join(", "),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
        ),
        &params,
    )
    .await?;
    let mut entries: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    entries.sort_unstable();
    Ok(entries)
}

/// ` and ` the predicate that leaves out the rows of `a` held in quarantine
/// ([`held::not_held_sql`]), over `R` as `r`, the definition's id and `a`'s
/// name being parameters `$id_param` and `$table_param`.
async fn not_held_filter(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    id_param: usize,
    table_param: usize,
) -> Result<String, ApplyError> {
    // Planted bug: the fan-out writes the rows of `a` held in quarantine.
    // See `crate::plant`. Both parameters stay bound, so the statement's
    // parameters don't change with the plant.
    #[cfg(any(test, feature = "test-util"))]
    if crate::plant::armed() == Some(crate::plant::Plant::Join1FanOutHeldRow)
        && crate::plant::fires(
            crate::plant::Plant::Join1FanOutHeldRow,
            held::holds_any(txn, shape).await?,
        )
    {
        return Ok(format!(
            " and ${id_param}::bigint is not null and ${table_param}::text is not null"
        ));
    }
    #[cfg(not(any(test, feature = "test-util")))]
    let _ = (txn, shape);
    Ok(format!(
        " and {}",
        held::not_held_sql("r", id_param, table_param)
    ))
}

/// Writes the target rows of a fan-out's `entries` (sorted `R` keys): a
/// page's ([`fan_out`]) or a chunk's ([`fan_out::step`]). They are written in
/// sorted keyset batches of [`WRITE_BATCH`], one statement each
/// ([`write_target`]), under one savepoint. A data exception rolls the
/// fan-out back to it and bisects the batches down to the rows whose fields
/// fail ([`fan_out_failure`]), which are held in quarantine ([`held::hold`]),
/// parked with `origin` and `seg_seq` and recorded in `held` for the fuses,
/// while every other row is written, so the `b` change commits and `M`
/// advances. A bisection that can't name every failing row within
/// [`MAX_FAN_OUT_PROBES`] attempts halts the transform.
#[allow(clippy::too_many_arguments)]
async fn write_fan_out(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    at: &JoinedPosition,
    entries: &[String],
    origin: &held::HeldOrigin,
    seg_seq: i64,
    mutations: &mut TargetMutations,
    held: &held::Held,
) -> Result<(usize, usize), ApplyError> {
    if entries.is_empty() {
        return Ok((0, 0));
    }
    let entries: Vec<&str> = entries.iter().map(String::as_str).collect();
    txn.batch_execute("savepoint join1_fan_out").await?;
    let (mut written, mut deleted) = (0, 0);
    for batch in entries.chunks(WRITE_BATCH) {
        match write_target(
            txn,
            shape,
            batch,
            Entries::FanOut,
            WriteOrigin::All(origin),
            mutations,
        )
        .await
        {
            Ok((w, d)) => {
                written += w;
                deleted += d;
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute("rollback to savepoint join1_fan_out")
                    .await?;
                let (written, deleted, rows) = fan_out_failure(
                    txn,
                    shape,
                    Side::Joined(at.position),
                    &entries,
                    WriteOrigin::All(origin),
                    mutations,
                )
                .await?;
                let parked: Vec<_> = rows.iter().map(|row| (row, origin)).collect();
                held::hold(txn, shape, seg_seq, Side::Joined(at.position), &parked).await?;
                held.record(rows);
                txn.batch_execute("release savepoint join1_fan_out").await?;
                return Ok((written, deleted));
            }
            Err(err) => return Err(err),
        }
    }
    txn.batch_execute("release savepoint join1_fan_out").await?;
    Ok((written, deleted))
}

/// A write of the target rows of `entries` failed with a data exception: the
/// fan-out of the page's changes of `side`'s table (`b`), or the write of its
/// `a` changes' rows ([`write_a_changes`]). Bisects them, each attempt under
/// a savepoint of its own, down to the rows whose fields fail, within
/// [`MAX_FAN_OUT_PROBES`] attempts, and names each one's failing fields with
/// the per-field probe ([`failing_fields`]). An attempt that succeeds keeps
/// its writes, so once every failing row is named every other row is
/// written. Returns the rows and deletes written, and the failing rows for
/// the page to hold ([`held::hold`]). A bisection that runs out of attempts
/// first is the halt that names the rows it found, at most
/// [`MAX_NAMED_FAN_OUT_ROWS`]. Any other failure propagates.
async fn fan_out_failure(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    side: Side,
    entries: &[&str],
    origin: WriteOrigin<'_>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize, Vec<held::HeldRow>), ApplyError> {
    let written_entries = match side {
        Side::From => Entries::Changed,
        Side::Joined(_) => Entries::FanOut,
    };
    let mut failing: Vec<(&str, String, Option<String>)> = Vec::new();
    let (mut written, mut deleted) = (0, 0);
    let mut probes = 0;
    let mut runs = vec![(0, entries.len())];
    while let Some((lo, hi)) = runs.pop() {
        if probes >= MAX_FAN_OUT_PROBES {
            runs.push((lo, hi));
            break;
        }
        probes += 1;
        txn.batch_execute("savepoint join1_fan_out_probe").await?;
        match write_target(
            txn,
            shape,
            &entries[lo..hi],
            written_entries,
            origin,
            mutations,
        )
        .await
        {
            Ok((w, d)) => {
                written += w;
                deleted += d;
                txn.batch_execute("release savepoint join1_fan_out_probe")
                    .await?
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute(
                    "rollback to savepoint join1_fan_out_probe; \
                     release savepoint join1_fan_out_probe",
                )
                .await?;
                if hi - lo == 1 {
                    let sqlstate = super::quarantine::sqlstate_of(&err);
                    failing.push((entries[lo], err.to_string(), sqlstate));
                } else {
                    let mid = lo + (hi - lo) / 2;
                    runs.push((mid, hi));
                    runs.push((lo, mid));
                }
            }
            Err(err) => return Err(err),
        }
    }
    let mut rows: Vec<held::HeldRow> = Vec::with_capacity(failing.len());
    for (key, error, sqlstate) in failing {
        let fields = failing_fields(txn, shape, key).await?;
        rows.push(held::HeldRow {
            key: key.to_string(),
            error,
            sqlstate,
            fields,
        });
    }
    if runs.is_empty() {
        return Ok((written, deleted, rows));
    }
    let named: Vec<String> = rows
        .iter()
        .take(MAX_NAMED_FAN_OUT_ROWS)
        .map(|row| {
            let fields: Vec<String> = row
                .fields
                .iter()
                .map(|(field, error)| format!("{field}: {error}"))
                .collect();
            format!("{} ({})", row.key, fields.join("; "))
        })
        .collect();
    let named = if named.is_empty() {
        "none named".to_string()
    } else {
        named.join(", ")
    };
    let reason = match side {
        Side::Joined(_) => format!(
            "a change of {} whose fan-out fails on more rows of {} than {MAX_FAN_OUT_PROBES} \
             attempts to isolate them name: {named}, and maybe more",
            shape.table(side),
            shape.source_table,
        ),
        Side::From => format!(
            "changes of {} that fail on more of its rows than {MAX_FAN_OUT_PROBES} attempts \
             to isolate them name: {named}, and maybe more",
            shape.source_table,
        ),
    };
    Err(ApplyError::JoinOneToOneHalt {
        target: shape.target.clone(),
        reason,
    })
}

/// The per-field probe of one fan-out row, the `R` entry `key`: each field
/// alone over the row and the newest live `M` entry on its key, cast to its
/// target column's type as the upsert's assignment casts it, each under a
/// savepoint. Returns the fields that fail with a data exception, each with
/// its error, in definition order.
async fn failing_fields(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    key: &str,
) -> Result<Vec<(String, String)>, ApplyError> {
    let target_columns = super::one_to_one_render::image_columns(txn, &shape.target).await?;
    let from = format!("{} r", shape.r.ident);
    let mut failing = Vec::new();
    for (name, sql) in &shape.fields {
        let cast = target_columns
            .iter()
            .find(|c| &c.name == name)
            .map_or_else(String::new, |c| format!("::{}", c.type_sql));
        let probe = format!(
            "select (({sql}){cast})::text from {joined} where r.{key_col} = $1",
            joined = shape.joined_sql(&from),
            key_col = quote_ident(schema::KEY_COLUMN),
        );
        txn.batch_execute("savepoint join1_field_probe").await?;
        match txn.query(&probe, &[&key]).await {
            Ok(_) => {
                txn.batch_execute("release savepoint join1_field_probe")
                    .await?
            }
            Err(err) if super::one_to_one_render::is_data_exception(&err) => {
                txn.batch_execute(
                    "rollback to savepoint join1_field_probe; \
                     release savepoint join1_field_probe",
                )
                .await?;
                let message = err
                    .as_db_error()
                    .map_or_else(|| err.to_string(), |db| db.message().to_string());
                failing.push((name.clone(), message));
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(failing)
}

/// Which `R` entries a [`write_target`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Entries {
    /// Entries a page or a build changed, of rows that may be gone.
    Changed,
    /// A batch of a fan-out ([`fan_out`]): live entries on the keys of the
    /// page's `b` changes, read under those keys' `J` locks.
    FanOut,
}

impl Entries {
    /// Whether some of these entries may have no target row, for
    /// [`write_target`] to look for one to delete. A fan-out's entries are
    /// live, so under a `LEFT` join each has its target row; under an `INNER`
    /// join an entry has one only while a live `M` entry is on its key, and a
    /// fan-out of a `b` delete is what removes them.
    fn may_delete(self, shape: &JoinShape) -> bool {
        self == Entries::Changed || shape.any_inner()
    }
}

/// The origin a target write records each row it changes with for the seam
/// ([`write_target`]): what a reader's staged row carries, the earliest
/// source change behind the row.
#[derive(Debug, Clone, Copy)]
enum WriteOrigin<'a> {
    /// A page's `a` changes: each row's own changes' origin (by its key),
    /// merged with the page's `b` changes', which its row joins too.
    Each(&'a HashMap<String, held::HeldOrigin>, &'a held::HeldOrigin),
    /// One origin for every row: a page's fan-out (its `b` changes') or a
    /// chunk's (its marker's).
    All(&'a held::HeldOrigin),
}

impl WriteOrigin<'_> {
    /// The origin of the row whose `R` entry is `key`, as
    /// `(origin_lsn, src_changed, hop_gen)`.
    fn of(&self, key: &str) -> (Option<PgLsn>, Option<std::time::SystemTime>, i32) {
        match self {
            WriteOrigin::Each(rows, b) => rows
                .get(key)
                .cloned()
                .unwrap_or_else(held::HeldOrigin::unknown)
                .merged(b)
                .parts(),
            WriteOrigin::All(origin) => origin.parts(),
        }
    }
}

/// Writes the target rows of `keys` (`R` entries a page changed, or a batch
/// of its fan-out; `entries` says which): each live one with a target row is
/// upserted from `R` and the newest live `M` entry on its key, and the rest
/// are deleted. The fields are rendered SQL. The upsert takes its rows sorted
/// by primary key. Returns how many rows it wrote and deleted.
///
/// When something reads the target, the rows of `keys` are first locked in
/// primary-key order and their images read, and each row the write changes
/// is recorded in `mutations` with its prior image and `origin`, for the
/// caller to stage in its transaction (`super::target_mutations`). The lock
/// comes where the write's own would, after `J` and the entries. A write that
/// fails records nothing, so a savepoint the caller rolls back leaves no key
/// recorded but those an earlier write recorded, which the bisection writes
/// again; a key recorded so keeps its first prior image, the one the
/// rollback restored.
async fn write_target(
    txn: &Transaction<'_>,
    shape: &JoinShape,
    keys: &[&str],
    entries: Entries,
    origin: WriteOrigin<'_>,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if keys.is_empty() {
        return Ok((0, 0));
    }
    let image_sql = mutations.image_sql(txn, &shape.target, "t").await?;
    let arity = shape.pk.len();
    let target = ddl::qualified_target_table_ident(&shape.target);
    // A key with a `NULL` part is a row of an `a` that is keyed by a nullable
    // unique index (an aggregate's target), held `unique nulls not distinct`:
    // the insert's conflict target finds it, and the delete matches it by its
    // `NULL` pattern, as the 1-1 apply does.
    let mut decoded: Vec<Vec<Option<String>>> = Vec::with_capacity(keys.len());
    for key in keys {
        decoded.push(super::apply::decode_target_pk_parts(
            &shape.pk,
            &shape.target,
            key,
        )?);
    }

    let key_expr = ddl::pk_key_sql_expr(&shape.pk, Some("t"));
    let matched = Keyset::of_parts(&decoded);
    // How a statement picks `t`'s rows of one pattern after `from <target>
    // as t `: the keys with no `NULL` part by an `exists` over their keyset,
    // the others by joining theirs with `join` (or by `is null` alone).
    let pick = |pattern: &[bool], join: &str| {
        let arm = ArmSql::new(&shape.pk, pattern, "k", 1);
        let matches = arm.matches("t");
        match arm.relation() {
            Some(keys) if !pattern.contains(&true) => {
                format!("where exists (select 1 from {keys} where {matches})")
            }
            Some(keys) => format!("{join}{keys} where {matches}"),
            None => format!(" where {matches}"),
        }
    };
    let mut prior_images: HashMap<String, String> = HashMap::new();
    if let Some(image_sql) = &image_sql {
        let order: Vec<String> = shape
            .pk
            .iter()
            .map(|c| format!("t.{}", quote_ident(&c.name)))
            .collect();
        let mut rows = Vec::new();
        for (pattern, arm) in &matched.arms {
            let params: Vec<_> = arm.params().collect();
            let sql = format!(
                "select {key_expr}, ({image_sql})::text from {target} as t {} \
                 order by {} for update of t",
                pick(pattern, ", "),
                order.join(", "),
            );
            rows.extend(txn.query(&sql, &params).await?);
        }
        prior_images = rows
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
            .collect();
    }

    let mut deleted = Vec::new();
    // Rows that have no target row: delete them. The key text compares only
    // for rows the index finds by primary key.
    if entries.may_delete(shape) {
        let unbacked = format!(
            "not exists (select 1 from {r} r where r.{key} = ({key_expr}) collate \"C\" and {has})",
            r = shape.r.ident,
            key = quote_ident(schema::KEY_COLUMN),
            has = shape.has_target_row_sql(),
        );
        for (pattern, arm) in &matched.arms {
            let params: Vec<_> = arm.params().collect();
            let sql = format!(
                "delete from {target} as t {} and {unbacked} returning {key_expr}",
                pick(pattern, "using "),
            );
            let rows = txn.query(&sql, &params).await?;
            deleted.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
        }
    }

    let mut written = 0;
    let mut written_keys: Vec<String> = Vec::new();
    let mut sql = upsert_sql(shape);
    if image_sql.is_some() {
        sql.push_str(&format!(
            " returning {}",
            ddl::pk_key_sql_expr(&shape.pk, None)
        ));
    }
    for start in (0..keys.len()).step_by(WRITE_BATCH) {
        let end = (start + WRITE_BATCH).min(keys.len());
        let columns = upsert_params(arity, &decoded[start..end], &keys[start..end]);
        let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
        if image_sql.is_some() {
            let rows = txn.query(&sql, &params).await?;
            written += rows.len();
            written_keys.extend(rows.into_iter().map(|row| row.get::<_, String>(0)));
        } else {
            // The command's row count is the rows the upsert inserted or
            // updated, as `returning` would list them.
            written += txn.execute(&sql, &params).await? as usize;
        }
    }
    if image_sql.is_some() {
        // A target row's key is its `R` entry's key: the same
        // `ddl::pk_key_sql_expr` text.
        for key in written_keys.iter().chain(&deleted) {
            let (origin_lsn, src_changed, hop_gen) = origin.of(key);
            mutations.record(
                &shape.target,
                key.clone(),
                prior_images.remove(key),
                hop_gen,
                src_changed,
                origin_lsn,
            );
        }
    }
    Ok((written, deleted.len()))
}

/// [`write_target`]'s upsert of a batch of `R` entries: parameters `$1..` are
/// the target primary key's parts (one text array each) and the last is the
/// entries' keys ([`upsert_params`]).
///
/// The batch drives the statement: each entry's `R` row is a probe of `R`'s
/// primary key, in a `LATERAL` subquery that `offset 0` keeps from being
/// pulled up into a join the planner may reorder. As a plain join, the
/// planner costs thousands of probes above one pass over `R` and hashes all
/// of `R` (#1580), once per batch: a fan-out then costs in proportion to
/// `R`'s size, not its own.
fn upsert_sql(shape: &JoinShape) -> String {
    let arity = shape.pk.len();
    let target = ddl::qualified_target_table_ident(&shape.target);
    let pk_cols: Vec<String> = shape.pk.iter().map(|c| quote_ident(&c.name)).collect();
    let field_cols: Vec<String> = shape.fields.iter().map(|(n, _)| quote_ident(n)).collect();
    let on_conflict = super::one_to_one_render::on_conflict_by_text(&target, &pk_cols, &field_cols);
    let select_cols: Vec<String> = shape
        .pk
        .iter()
        .enumerate()
        .map(|(j, c)| format!("v.__p{j}::{}", c.data_type))
        .chain(shape.fields.iter().map(|(_, sql)| sql.clone()))
        .collect();
    let unnest_args: Vec<String> = (1..=arity + 1).map(|j| format!("${j}::text[]")).collect();
    let unnest_cols: Vec<String> = (0..arity)
        .map(|j| format!("__p{j}"))
        .chain(std::iter::once("__key".to_string()))
        .collect();
    let ordinals: Vec<String> = (1..=arity).map(|i| i.to_string()).collect();
    let col_list: Vec<String> = pk_cols.iter().chain(field_cols.iter()).cloned().collect();
    let from = format!(
        "unnest({}) as v({}) cross join lateral \
         (select r.* from {r} r where r.{key} = v.__key and {has} offset 0) r",
        unnest_args.join(", "),
        unnest_cols.join(", "),
        r = shape.r.ident,
        key = quote_ident(schema::KEY_COLUMN),
        has = shape.has_target_row_sql(),
    );
    format!(
        "insert into {target} ({cols}) select {select} from {joined} \
         order by {ordinals} {on_conflict}",
        cols = col_list.join(", "),
        select = select_cols.join(", "),
        joined = shape.joined_sql(&from),
        ordinals = ordinals.join(", "),
    )
}

/// [`upsert_sql`]'s parameters for the entries `keys`, whose primary keys'
/// parts are `decoded`.
fn upsert_params(
    arity: usize,
    decoded: &[Vec<Option<String>>],
    keys: &[&str],
) -> Vec<Vec<Option<String>>> {
    let mut columns = super::apply::transpose_pk_parts(arity, decoded);
    columns.push(keys.iter().map(|k| Some(k.to_string())).collect());
    columns
}

/// The basic `self_check` of a 1-1 `JOIN` target, for tests: the primary keys
/// of every target row that differs from the live join of the two tables, or
/// that is missing or extra. The live join is `a [LEFT] JOIN b` evaluated as
/// the definition's fields say, each field cast to its target column's type
/// and compared by text, as every other 1-1 comparison is. It reads the live
/// tables once, in one statement, and takes no lock, so it is exact only on a
/// quiescent instance. The audit that pages, retries and reports is
/// [`super::self_check_join_one_to_one`]'s.
#[cfg(any(test, feature = "internals"))]
pub async fn diverging_keys(pool: &Pool, target_table: &str) -> Result<Vec<String>, ApplyError> {
    let (definition, shape) =
        shape_of(pool, target_table, "a self_check that is not of a 1-1 JOIN").await?;
    let client = pool.get().await?;
    let target_types =
        super::one_to_one_render::image_columns(&**client, &definition.target_table).await?;
    // One join per position, each aliased as the recompute's lateral row
    // ([`b_alias`]), so the fields' `s` reads them as it reads those.
    let joins: Vec<String> = shape
        .positions
        .iter()
        .map(|p| {
            let alias = b_alias(p.position);
            let on: Vec<String> = p
                .key
                .iter()
                .zip(&p.m_key)
                .map(|(a, b)| format!("r.{} = {alias}.{}", quote_ident(&a.name), quote_ident(b)))
                .collect();
            format!(
                "{kind} join {table} {alias} on {on}",
                kind = if p.left { "left" } else { "inner" },
                table = ddl::qualified_source_table(&p.table),
                on = on.join(" and "),
            )
        })
        .collect();
    let expected: Vec<String> = shape
        .fields
        .iter()
        .map(|(name, sql)| {
            let cast = target_types
                .iter()
                .find(|c| &c.name == name)
                .map_or_else(String::new, |c| format!("::{}", c.type_sql));
            format!("(({sql}){cast}) as {}", quote_ident(name))
        })
        .collect();
    let s = if shape.s_columns.is_empty() {
        "(select 1 as __none) s".to_string()
    } else {
        let cols: Vec<String> = shape
            .s_columns
            .iter()
            .map(|(name, sql)| format!("{sql} as {}", quote_ident(name)))
            .collect();
        format!("(select {}) s", cols.join(", "))
    };
    let names: Vec<String> = shape.fields.iter().map(|(n, _)| quote_ident(n)).collect();
    let row = |alias: &str| -> String {
        let cols: Vec<String> = names.iter().map(|n| format!("{alias}.{n}")).collect();
        format!("row({})::text", cols.join(", "))
    };
    let row_or_empty = |alias: &str| {
        if names.is_empty() {
            "''".to_string()
        } else {
            row(alias)
        }
    };
    let sql = format!(
        "with expected as ( \
             select {a_key} as k{comma}{fields} \
             from {a} r {joins} cross join lateral {s} \
         ), actual as ( \
             select {t_key} as k{t_comma}{t_fields} from {target} t \
         ) \
         select coalesce(e.k, t.k) from expected e full join actual t on e.k = t.k \
         where e.k is null or t.k is null or {e_row} is distinct from {t_row} \
         order by 1",
        a_key = ddl::pk_key_sql_expr(&shape.pk, Some("r")),
        comma = if expected.is_empty() { "" } else { ", " },
        fields = expected.join(", "),
        a = ddl::qualified_source_table(&shape.source_table),
        joins = joins.join(" "),
        t_key = ddl::pk_key_sql_expr(&shape.pk, Some("t")),
        t_comma = if names.is_empty() { "" } else { ", " },
        t_fields = names
            .iter()
            .map(|n| format!("t.{n}"))
            .collect::<Vec<_>>()
            .join(", "),
        target = ddl::qualified_target_table_ident(&shape.target),
        e_row = row_or_empty("e"),
        t_row = row_or_empty("t"),
    );
    Ok(client
        .query(&sql, &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

/// The definition of the 1-1 `JOIN` `target_table` and its shape, for the
/// test helpers; `what` is the refusal of a target of another kind.
#[cfg(any(test, feature = "internals"))]
async fn shape_of(
    pool: &Pool,
    target_table: &str,
    what: &'static str,
) -> Result<(Definition, JoinShape), ApplyError> {
    let definition = crate::defs::catalog::definition_by_target(pool, target_table)
        .await?
        .ok_or_else(|| ApplyError::TransformNotFound {
            transform: target_table.to_string(),
        })?;
    if !spec::is_one_to_one(&definition.def) {
        return Err(ApplyError::GroupBasisUnsupported {
            target: target_table.to_string(),
            what,
        });
    }
    let pk = ddl::source_primary_key(pool, &definition.source_table).await?;
    let paused = super::quarantine::paused_columns_for(pool, &definition.def).await?;
    let shape = JoinShape::load(pool, &definition, pk, &paused).await?;
    Ok((definition, shape))
}

/// The plan (`explain`, not run) of [`write_target`]'s upsert of the target
/// rows of the `R` entries `keys` of the 1-1 `JOIN` `target_table`, for tests
/// of its shape.
#[cfg(any(test, feature = "internals"))]
pub async fn explain_target_write(
    pool: &Pool,
    target_table: &str,
    keys: &[&str],
) -> Result<String, ApplyError> {
    let (_, shape) = shape_of(
        pool,
        target_table,
        "a target write that is not of a 1-1 JOIN",
    )
    .await?;
    let decoded = keys
        .iter()
        .map(|key| super::apply::decode_target_pk_parts(&shape.pk, &shape.target, key))
        .collect::<Result<Vec<_>, _>>()?;
    let columns = upsert_params(shape.pk.len(), &decoded, keys);
    let params: Vec<&(dyn ToSql + Sync)> = columns.iter().map(|c| c as _).collect();
    let client = pool.get().await?;
    let rows = client
        .query(&format!("explain {}", upsert_sql(&shape)), &params)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}
