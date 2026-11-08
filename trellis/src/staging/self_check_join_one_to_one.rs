//! `self_check` of a 1-1 `JOIN` target (ADR-0013 "Scope", ADR-0021).
//!
//! The root `a` is joined to one table per position, `b` at position 1 (the
//! only one a two-table `JOIN` has) and `b<p>` at each position `p` after it
//! (a star). A 1-1 `JOIN` target is audited in parts, walked in this order,
//! each a keyset walk of its own keys:
//!
//! 1. **The target** (`t`), a page of `a`'s primary key (the target's), against
//!    the live multi-join: each field as the definition writes it over the `a`
//!    row and, at each position, the row of that position's table on its key,
//!    and the target's row. A key one side has and the other lacks is a
//!    missing or extra row, a field that differs a cell.
//! 2. **`R` against `a`** (`r`), a page of `a`'s primary key: `a`'s rows and
//!    their join key at every position and read columns, against `R`'s live
//!    entries. A row with no live entry, or an entry with other values, is a
//!    cell of `R`.
//! 3. **`R`'s entries with no `a` row** (`ro`), a page of `R`'s live entries by
//!    their key text: an entry whose `a` row is gone.
//!
//! Then, at each position in order, three parts over that position's `M` and
//! table, tagged with the position's number from position 2 on (`m2`, `mo2`,
//! `d2`):
//!
//! 4. **`M` against `b`** (`m`) and 5. **`M`'s entries with no `b` row**
//!    (`mo`), the same for the joined side.
//! 6. **Duplicates** (`d`), a page of `M`'s live entries in join-key order,
//!    each key among them counted over all of `M`: a key that holds more than
//!    one live entry. `b`'s `ON` columns are unique, and the target reads the
//!    newest live entry of a key, which masks a duplicate: this is where
//!    `b` losing its uniqueness shows, before a revalidation pauses the
//!    transform.
//!
//! A divergence of a hidden table is a [`Divergence::Cell`] whose column is
//! `join1_<id>_<r|m|m<p>>.<column>`, and whose key is the entry's: the primary
//! key text of its row. An entry on one side only is a cell whose column is
//! `__entry` (`live` against nothing), so a row whose every value is `NULL` is
//! reported as well. A key with duplicates is a cell `join1_<id>_m.live_entries`
//! (`_m<p>` at position `p`) with the count of live entries against `1`, keyed
//! by the join key's text.
//!
//! The walk's cursor ([`super::self_check::SelfCheckScope::after`]) carries the
//! part: `t:<key>`, `r:<key>`, `ro:<key>`, `m:<key>`, `mo:<key>`, `d:<key>`,
//! `m2:<key>` and so on continues a part after a key, and a bare tag starts
//! one. The first page of a walk (no cursor) starts the target's part, and the
//! last position's duplicates part's end ends the walk.
//!
//! **What each part compares against.** The target is a function of `R` and
//! every position's `M`: every writer of a row holds its join key's `J` row at
//! every position it reads and recomputes the rows it affects (ADR-0021). `R`
//! and each `M` are the 1-1 ledger's copies of `a` and of the position's
//! table. So each part reads a source as its copy has applied it
//! ([`super::self_check_ring`]): a key with an unapplied change is its earliest
//! such change's old image, any other key its live row. The target part joins
//! `a` as `R` has applied it to each position's table as its `M` has, one
//! lateral join per position: each `a` row to the row the newest live entry of
//! its key at that position names (the apply's rule: the latest applied
//! position, then the highest key), so a target that follows `R` and the `M`s
//! agrees while changes are in flight, in one pass.
//!
//! A divergence on a key the ring can't vouch for exactly (an unapplied row
//! with no usable image, an op that touches every key) is re-checked after a
//! fresh wait in `Standard` mode, like every comparison that reads a ring. So
//! is every divergence of the target part while the definition has a pending
//! fan-out at any position (`super::join_one_to_one::fan_out`): the rows of
//! its key past the cursor still join the row that position's `M` held
//! before, and the fresh wait waits for the fan-out to finish. The
//! duplicate part can't subtract: a hand-off of a join key between two `b` rows
//! holds two live entries for as long as the two changes are applied out of
//! order, so it re-checks whenever `b`'s ring holds an unapplied row.
//!
//! The SQL is rendered from the definition separately from the apply path's
//! (`super::join_one_to_one`): the fields by [`super::self_check_joins`]'s
//! renderer, which shares nothing with it, so a rendering bug in the apply
//! disagrees with the audit instead of agreeing with itself.
//!
//! Cost: `a`'s and `b`'s parts are range scans of the source's primary key
//! index, probing `R` or `M` by their primary key; a page of the target part
//! probes each position's table by its join key, its `M` by its primary key. A part driven by the
//! hidden table (`ro`, `mo`) reads a page of its keys, then looks each up in the
//! source by primary key, from the key's decoded columns. The duplicates part
//! reads a page of `M`'s entries by its join-key index, and counts each key's
//! entries over the same index.

use std::collections::{BTreeSet, HashMap, HashSet};

use tokio_postgres::types::PgLsn;

use crate::defs::ast::JoinKind;
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, Position, Side as JoinSide};
use crate::defs::join_one_to_one::HiddenTable;
use crate::defs::ledger as schema;
use crate::defs::model::Definition;
use crate::pool::{Pool, quote_ident};

use super::one_to_one_ledger::LedgerRef;
use super::self_check::{ComparePass, Divergence, Page, SelfCheckError, SelfCheckScope, diff_page};
use super::self_check_groups::Uncertain;
use super::self_check_joins::{joined_alias, render};
use super::self_check_keyset::{Keyset, recollated_pairs};
use super::self_check_ring::Ring;

#[cfg(any(test, feature = "test-util"))]
pub mod snapshot;

/// One part of the walk (the module doc), in walk order: the target's, `R`'s
/// two, then three per position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    /// The target against the live join.
    Target,
    /// `R` against `a`.
    From,
    /// `R`'s live entries against `a`.
    FromOrphans,
    /// A position's `M` against its table.
    Joined(Position),
    /// A position's `M`'s live entries against its table.
    JoinedOrphans(Position),
    /// Keys with more than one live entry in a position's `M`.
    Duplicates(Position),
}

impl Part {
    /// Every part of the walk of a `JOIN` of `positions` joined tables, in
    /// walk order.
    fn walk(positions: usize) -> Vec<Part> {
        let mut parts = vec![Part::Target, Part::From, Part::FromOrphans];
        for position in (0..positions).map(Position::of_index) {
            parts.extend([
                Part::Joined(position),
                Part::JoinedOrphans(position),
                Part::Duplicates(position),
            ]);
        }
        parts
    }

    fn tag(self) -> String {
        let at = |base: &str, position: Position| joined_alias(base, position);
        match self {
            Part::Target => "t".to_string(),
            Part::From => "r".to_string(),
            Part::FromOrphans => "ro".to_string(),
            Part::Joined(position) => at("m", position),
            Part::JoinedOrphans(position) => at("mo", position),
            Part::Duplicates(position) => at("d", position),
        }
    }

    /// The part `tag` names, at any position a `JOIN` may have.
    fn of_tag(tag: &str) -> Option<Part> {
        match tag {
            "t" => return Some(Part::Target),
            "r" => return Some(Part::From),
            "ro" => return Some(Part::FromOrphans),
            _ => {}
        }
        // `mo` before `m`, which it starts with.
        let parts: [(&str, fn(Position) -> Part); 3] = [
            ("mo", Part::JoinedOrphans),
            ("m", Part::Joined),
            ("d", Part::Duplicates),
        ];
        let (rest, part) = parts
            .iter()
            .find_map(|(base, part)| tag.strip_prefix(base).map(|rest| (rest, part)))?;
        let position = if rest.is_empty() {
            Position::FIRST
        } else {
            // Position 1 is tagged bare, never `m1`, and a number is digits.
            if !rest.bytes().all(|b| b.is_ascii_digit()) || rest.starts_with('0') {
                return None;
            }
            Position::from_number(rest.parse().ok()?).filter(|p| *p != Position::FIRST)?
        };
        Some(part(position))
    }

    /// The part after this one in the walk of a `JOIN` of `positions` joined
    /// tables, or `None` after the last.
    fn next(self, positions: usize) -> Option<Part> {
        let walk = Self::walk(positions);
        let at = walk.iter().position(|part| *part == self)?;
        walk.get(at + 1).copied()
    }
}

/// The part and the key within it a cursor continues after (the module doc).
/// A cursor of no part's form continues the target's part after it.
fn parse_cursor(after: Option<&str>) -> (Part, Option<String>) {
    let Some(after) = after else {
        return (Part::Target, None);
    };
    match after.split_once(':') {
        Some((tag, key)) => match Part::of_tag(tag) {
            Some(part) => (part, Some(key.to_string())),
            None => (Part::Target, Some(after.to_string())),
        },
        None => match Part::of_tag(after) {
            Some(part) => (part, None),
            None => (Part::Target, Some(after.to_string())),
        },
    }
}

/// The cursor that follows a page of `part` that ended at `page_end`
/// (`None`: the part's end), in the walk of a `JOIN` of `positions` joined
/// tables.
fn next_cursor(part: Part, page_end: Option<String>, positions: usize) -> Option<String> {
    match page_end {
        Some(key) => Some(format!("{}:{key}", part.tag())),
        None => part.next(positions).map(Part::tag),
    }
}

/// The column a divergence of a hidden table's entry that exists on one side
/// only names.
const ENTRY: &str = "__entry";

/// What an entry's values start with on each side that has it, so a row whose
/// values are all `NULL` still differs from no row.
const LIVE: &str = "live";

/// Refuses a 1-1 `JOIN` definition with a field the comparison can't render.
pub(super) fn check_renders(def: &Definition) -> Result<(), SelfCheckError> {
    for field in &def.def.fields {
        render(&def.def, &field.expr, "a", "b").map_err(|detail| {
            SelfCheckError::UnsupportedExpr {
                target: def.def.target.clone(),
                detail,
            }
        })?;
    }
    Ok(())
}

/// One side of the join: its source, the hidden ledger that copies it and the
/// columns the ledger holds.
struct Side {
    /// `r` or `m`, the ledger's part of its name.
    ledger_name: String,
    /// The tag of this side's ring CTEs: `a` for the root, the side's alias
    /// for a joined table ([`joined_alias`] of `b`).
    tag: String,
    /// The source, qualified and unquoted: what the ring names it.
    table: String,
    /// The source, qualified and quoted.
    ident: String,
    /// The source's primary key, which the ledger's keys encode.
    pk: Vec<PrimaryKeyColumn>,
    ledger: LedgerRef,
    /// The columns the ledger copies, in the order a page compares them.
    columns: Vec<String>,
    /// The side's join key columns, in `ON` order.
    key: Vec<String>,
}

impl Side {
    fn ring(&self, id: i64) -> Ring<'_> {
        Ring {
            src_table: &self.table,
            source_row: self.ident.clone(),
            source_pk: &self.pk,
            transform_id: id,
            tag: &self.tag,
        }
    }

    /// The source's rows as its ledger has applied them: the live row of a key
    /// with no unapplied change, the old image of its earliest unapplied change
    /// otherwise (no row for an insert), as a derived table's body.
    fn applied(&self, ring: &Ring<'_>) -> String {
        let pend = ring.pend_cte();
        format!(
            "(select s.* from {ident} s \
              where not exists (select 1 from {pend} p where p.__k = {skey}) \
              union all \
              select r.* from {pend} p \
              cross join lateral jsonb_populate_record(null::{ident}, p.__i) r \
              where jsonb_typeof(p.__i) = 'object')",
            ident = self.ident,
            skey = ddl::pk_key_sql_expr(&self.pk, Some("s")),
        )
    }

    /// The name of a divergence's column in this side's ledger.
    fn divergence_column(&self, column: &str) -> String {
        format!("{}.{column}", self.ledger_name)
    }
}

/// One field of the target.
struct Field {
    name: String,
    /// The field over the aliases `a` and `b`, cast to its target column's
    /// type.
    sql: String,
}

/// One joined table of the audit, at its position.
struct Joined {
    position: Position,
    /// A `LEFT JOIN` (else `INNER`).
    left: bool,
    /// The `FROM` table's join key columns at this position, in `ON` order.
    from_key: Vec<String>,
    side: Side,
}

impl Joined {
    /// The alias the target part's lateral row of this position is read
    /// through, which the fields read it by: `b`, `b2`, ...
    fn alias(&self) -> String {
        joined_alias("b", self.position)
    }
}

/// What the audit reads of the definition, read once per page.
struct Audit<'a> {
    def: &'a Definition,
    a: Side,
    /// Each joined table, in position order.
    joined: Vec<Joined>,
    /// The fields the page compares: not the paused ones.
    fields: Vec<Field>,
}

impl Audit<'_> {
    fn id(&self) -> i64 {
        self.def.id
    }

    /// The joined table at `position`, or `None` past the definition's last.
    fn at(&self, position: Position) -> Option<&Joined> {
        self.joined.get(position.index())
    }

    /// The `ON` terms of `joined`'s position, over the aliases `a` and `x`:
    /// `a`'s column read under the collation `on_collations` names for its
    /// term (the `ON` pairs' [`recollated_pairs`]), `x`'s as it is, so an
    /// index on the joined table's join key serves the probe.
    fn on(joined: &Joined, on_collations: &[Option<String>]) -> String {
        joined
            .from_key
            .iter()
            .zip(&joined.side.key)
            .zip(on_collations)
            .map(|((a, b), collation)| match collation {
                Some(collation) => format!(
                    "a.{} collate {collation} = x.{}",
                    quote_ident(a),
                    quote_ident(b)
                ),
                None => format!("a.{} = x.{}", quote_ident(a), quote_ident(b)),
            })
            .collect::<Vec<_>>()
            .join(" and ")
    }

    /// [`recollated_pairs`] of each position's `ON` terms, in position order.
    async fn on_collations(
        &self,
        client: &tokio_postgres::Client,
    ) -> Result<Vec<Vec<Option<String>>>, SelfCheckError> {
        let mut collations = Vec::with_capacity(self.joined.len());
        for joined in &self.joined {
            collations.push(
                recollated_pairs(
                    client,
                    (&self.a.ident, &joined.from_key),
                    (&joined.side.ident, &joined.side.key),
                )
                .await?,
            );
        }
        Ok(collations)
    }

    /// The expected target: each `a` row as `R` has applied it, with the fields
    /// of the row on its key at each position, as that position's `M` has
    /// applied it, the newest live entry's when a key has several (a derived
    /// table named `e`, its columns `a`'s primary key's and `__f<i>` for each
    /// field). `on_collations` is [`Self::on_collations`].
    fn expected_target(
        &self,
        pk: &[PrimaryKeyColumn],
        on_collations: &[Vec<Option<String>>],
    ) -> String {
        let a_ring = self.a.ring(self.id());
        let mut columns: Vec<String> = pk
            .iter()
            .map(|c| format!("a.{n} as {n}", n = quote_ident(&c.name)))
            .collect();
        columns.extend(
            self.fields
                .iter()
                .enumerate()
                .map(|(i, f)| format!("({}) as __f{i}", f.sql)),
        );
        let laterals: Vec<String> = self
            .joined
            .iter()
            .zip(on_collations)
            .map(|(joined, collations)| {
                let side = &joined.side;
                let ring = side.ring(self.id());
                let key = ddl::pk_key_sql_expr(&side.pk, Some("x"));
                format!(
                    "{join} (select x.* from {applied} x \
                             left join {m} mm on mm.{key_col} = ({key}) collate \"C\" \
                                 and not mm.{tomb} \
                             where {on} \
                             order by mm.{applied_lsn} desc nulls last, {key} collate \"C\" desc \
                             limit 1) {alias} on true",
                    join = if joined.left {
                        "left join lateral"
                    } else {
                        "join lateral"
                    },
                    applied = side.applied(&ring),
                    m = side.ledger.ident,
                    key_col = quote_ident(schema::KEY_COLUMN),
                    tomb = quote_ident(schema::TOMBSTONE_COLUMN),
                    applied_lsn = quote_ident(schema::APPLIED_LSN_COLUMN),
                    on = Self::on(joined, collations),
                    alias = joined.alias(),
                )
            })
            .collect();
        format!(
            "(select {columns} from {a_applied} a {laterals}) e",
            columns = columns.join(", "),
            a_applied = self.a.applied(&a_ring),
            laterals = laterals.join(" "),
        )
    }

    /// The statement of a page of the target part. `$1` is the page's limit.
    /// Each row is `(side, page_end, key, values)`: side 1 the expected target,
    /// 2 the persisted one, 3 a key the page can't vouch for, 4 every key.
    fn target_sql(&self, keyset: &Keyset, on_collations: &[Vec<Option<String>>]) -> String {
        let refs = keyset.refs(None);
        let carried = keyset.select_carried(&refs);
        let order = keyset.order_by(&refs);
        let key = ddl::pk_key_sql_expr(keyset.columns(), None);
        let expected = self.expected_target(keyset.columns(), on_collations);
        let recomputed_values: Vec<String> = (0..self.fields.len())
            .map(|i| format!("__f{i}::text"))
            .collect();
        let persisted_values: Vec<String> = self
            .fields
            .iter()
            .map(|f| format!("{}::text", quote_ident(&f.name)))
            .collect();
        let target = ddl::qualified_target_table_ident(&self.def.target_table);
        let side = |from: &str, values: &[String]| {
            keyset.side(&refs, |cond| {
                format!(
                    "select {key} as __k{carried}, array[{}]::text[] as __v from {from} \
                     where {cond} order by {order} limit $1",
                    values.join(", ")
                )
            })
        };
        let a_ring = self.a.ring(self.id());
        let b_rings: Vec<(Ring<'_>, &Side)> = self
            .joined
            .iter()
            .map(|joined| (joined.side.ring(self.id()), &joined.side))
            .collect();
        let b_ctes: String = b_rings
            .iter()
            .map(|(ring, side)| ring.one_to_one_ctes(&side.ledger))
            .collect();
        // A key uncertain at any position's ring touches every `a` row on it:
        // every key, as a two-table JOIN's `b` ring does.
        let b_uncertain: String = b_rings
            .iter()
            .map(|(ring, _)| {
                format!(
                    " or {} or exists ({})",
                    ring.global_sql(),
                    ring.uncertain_keys_sql()
                )
            })
            .collect();
        keyset.ranked(
            &format!(
                "{a_ctes}{b_ctes}recomputed as materialized ({recomputed}), \
                 persisted as materialized ({persisted}), ",
                a_ctes = a_ring.one_to_one_ctes(&self.a.ledger),
                recomputed = side(&expected, &recomputed_values),
                persisted = side(&target, &persisted_values),
            ),
            &[(1, "recomputed"), (2, "persisted")],
            &[1, 2],
            &format!(
                " union all select 3, page.page_end, u.__k, null::text[] \
                    from page, ({a_unc}) u \
                  union all select 4, page.page_end, '', null::text[] from page \
                   where {a_global}{b_uncertain} \
                      or exists (select 1 from join1_fan_out where transform_id = {id})",
                a_unc = a_ring.uncertain_keys_sql(),
                a_global = a_ring.global_sql(),
                id = self.id(),
            ),
        )
    }

    /// The statement of a page of a ledger's part against its source, `side`'s.
    /// `$1` is the limit. Each row is `(side, page_end, key, values)`: side 1
    /// the source's rows as the ledger has applied them, 2 the ledger's live
    /// entries on those keys, 3 a key the page can't vouch for, 4 every key.
    /// The values lead with [`LIVE`], then the columns the ledger copies.
    fn ledger_sql(&self, side: &Side, keyset: &Keyset) -> String {
        let refs = keyset.refs(None);
        let carried = keyset.select_carried(&refs);
        let order = keyset.order_by(&refs);
        let key = ddl::pk_key_sql_expr(keyset.columns(), None);
        let ring = side.ring(self.id());
        let expected_values: Vec<String> = side
            .columns
            .iter()
            .map(|c| format!("{}::text", quote_ident(c)))
            .collect();
        let persisted_values: Vec<String> = side
            .columns
            .iter()
            .map(|c| format!("r.{}::text", quote_ident(c)))
            .collect();
        let expected = keyset.side(&refs, |cond| {
            format!(
                "select {key} as __k{carried}, array['{LIVE}', {}]::text[] as __v \
                 from {applied} e where {cond} order by {order} limit $1",
                expected_values.join(", "),
                applied = side.applied(&ring),
            )
        });
        let carried_e: String = (0..keyset.columns().len())
            .map(|i| format!(", e.__c{i} as __c{i}"))
            .collect();
        keyset.ranked(
            &format!(
                "{ctes}expected as materialized ({expected}), \
                 persisted as materialized ( \
                     select e.__k{carried_e}, array['{LIVE}', {persisted}]::text[] as __v \
                     from expected e \
                     join {ledger} r on r.{key_col} = e.__k collate \"C\" and not r.{tomb}), ",
                ctes = ring.one_to_one_ctes(&side.ledger),
                persisted = persisted_values.join(", "),
                ledger = side.ledger.ident,
                key_col = quote_ident(schema::KEY_COLUMN),
                tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            ),
            &[(1, "expected"), (2, "persisted")],
            &[1],
            &format!(
                " union all select 3, page.page_end, u.__k, null::text[] \
                    from page, ({unc}) u \
                  union all select 4, page.page_end, '', null::text[] from page \
                   where {global}",
                unc = ring.uncertain_keys_sql(),
                global = ring.global_sql(),
            ),
        )
    }

    /// The statement that finds, among the keys `$1` (their text) and `$2` (the
    /// same keys as JSON objects of their primary key's columns), the live
    /// entries of `side`'s ledger with no row in the source as the ledger has
    /// applied it. Each row is `(kind, key)`: kind 0 an entry with no row, 3 a
    /// key the page can't vouch for, 4 every key.
    fn orphans_sql(&self, side: &Side) -> String {
        let ring = side.ring(self.id());
        let matched: Vec<String> = side
            .pk
            .iter()
            .map(|c| {
                let n = quote_ident(&c.name);
                if c.nullable {
                    format!("e.{n} is not distinct from kk.{n}")
                } else {
                    format!("e.{n} = kk.{n}")
                }
            })
            .collect();
        format!(
            "with {ctes}\
             keys as materialized ( \
                 select u.__k, k.* \
                 from unnest($1::text[], $2::text[]) as u(__k, __j) \
                 cross join lateral jsonb_populate_record(null::{ident}, u.__j::jsonb) k), \
             orphans as ( \
                 select kk.__k from keys kk \
                 where exists (select 1 from {ledger} r \
                               where r.{key_col} = kk.__k and not r.{tomb}) \
                   and not exists (select 1 from {applied} e where {matched})) \
             select 0, __k from orphans \
             union all select 3, u.__k from ({unc}) u where u.__k in (select __k from keys) \
             union all select 4, '' where {global}",
            ctes = ring.one_to_one_ctes(&side.ledger),
            ident = side.ident,
            ledger = side.ledger.ident,
            key_col = quote_ident(schema::KEY_COLUMN),
            tomb = quote_ident(schema::TOMBSTONE_COLUMN),
            applied = side.applied(&ring),
            matched = matched.join(" and "),
            unc = ring.uncertain_keys_sql(),
            global = ring.global_sql(),
        )
    }
}

/// One part's page: [`ComparePass`] minus the token.
struct Pass {
    rows_compared: i64,
    /// Where the part's next page starts, or `None` when it ended.
    page_end: Option<String>,
    divergences: Vec<Divergence>,
    uncertain: Uncertain,
}

/// One keyset page of the audit of `def`, the 1-1 `JOIN` definition whose
/// target is keyed by `pk` (`a`'s primary key), from the part and key `scope`
/// names (the module doc).
pub(super) async fn compare(
    pool: &Pool,
    def: &Definition,
    pk: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    let (part, after) = parse_cursor(scope.after.as_deref());
    let audit = build(pool, def, pk, paused).await?;
    let after = after.as_deref();
    let limit = scope.limit;
    // The side a joined part reads, or `None` for a position the definition
    // doesn't have: a cursor no walk of it hands out, which ends the walk.
    let joined = |position: Position| audit.at(position).map(|joined| &joined.side);
    let pass = match part {
        Part::Target => Some(target_page(pool, &audit, pk, after, limit).await?),
        Part::From => Some(ledger_page(pool, &audit, &audit.a, after, limit).await?),
        Part::FromOrphans => Some(orphan_page(pool, &audit, &audit.a, after, limit).await?),
        Part::Joined(position) => match joined(position) {
            Some(side) => Some(ledger_page(pool, &audit, side, after, limit).await?),
            None => None,
        },
        Part::JoinedOrphans(position) => match joined(position) {
            Some(side) => Some(orphan_page(pool, &audit, side, after, limit).await?),
            None => None,
        },
        Part::Duplicates(position) => match joined(position) {
            Some(side) => Some(duplicate_page(pool, &audit, side, after, limit).await?),
            None => None,
        },
    };
    let Some(pass) = pass else {
        return Ok(ComparePass {
            checked_through,
            rows_compared: 0,
            next_after: None,
            divergences: Vec::new(),
            uncertain: Uncertain::default(),
        });
    };
    Ok(ComparePass {
        checked_through,
        rows_compared: pass.rows_compared,
        next_after: next_cursor(part, pass.page_end, audit.joined.len()),
        divergences: pass.divergences,
        uncertain: pass.uncertain,
    })
}

/// The type of each column of `table` as a cast names it: the apply's own
/// rendering of a column's type (`type_sql`, ADR-0020), with no modifier.
async fn column_types(pool: &Pool, table: &str) -> Result<HashMap<String, String>, SelfCheckError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            &format!(
                "select a.attname::text, {} \
                 from pg_catalog.pg_attribute a \
                 where a.attrelid = pg_catalog.to_regclass($1) and a.attnum > 0 \
                   and not a.attisdropped",
                crate::defs::copy_type::type_sql("a.atttypid")
            ),
            &[&ddl::regclass_arg(table)],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

/// Reads what the audit needs of `def`: the root and each joined table,
/// their ledgers, and the fields except the paused ones.
async fn build<'a>(
    pool: &Pool,
    def: &'a Definition,
    pk: &[PrimaryKeyColumn],
    paused: &HashSet<String>,
) -> Result<Audit<'a>, SelfCheckError> {
    let unsupported = |detail: String| SelfCheckError::UnsupportedExpr {
        target: def.def.target.clone(),
        detail,
    };
    let join = spec::spec(&def.def).expect("a 1-1 JOIN definition has a join");
    let tables = {
        let client = pool.get().await?;
        catalog::joined_tables_of(&**client, def.id).await?
    };
    if tables.len() != join.joins.len() {
        return Err(unsupported(format!(
            "a 1-1 JOIN of {} joined tables with {} recorded",
            join.joins.len(),
            tables.len()
        )));
    }
    let schema_name = pool.schema();
    let side = |name: JoinSide,
                table: &str,
                pk: Vec<PrimaryKeyColumn>,
                hidden: HiddenTable,
                tag: String| Side {
        ledger_name: hidden.name(def.id),
        tag,
        table: table.to_string(),
        ident: ddl::qualified_source_table(table),
        pk,
        ledger: LedgerRef::hidden(schema_name, def.id, hidden, &def.target_table),
        columns: spec::side_columns(&def.def, name).into_iter().collect(),
        key: spec::join_key(&def.def, name),
    };
    let a = side(
        JoinSide::From,
        &def.source_table,
        pk.to_vec(),
        HiddenTable::FromEntries,
        "a".to_string(),
    );
    let mut joined = Vec::with_capacity(tables.len());
    for ((position, spec_joined), table) in spec::positions(join).zip(&tables) {
        let table_pk = ddl::source_primary_key(pool, table).await?;
        joined.push(Joined {
            position,
            left: spec_joined.kind == JoinKind::Left,
            from_key: spec::from_key_at(&def.def, position),
            side: side(
                JoinSide::Joined(position),
                table,
                table_pk,
                HiddenTable::JoinedEntries(position),
                joined_alias("b", position),
            ),
        });
    }
    let types = column_types(pool, &def.target_table).await?;
    let mut fields = Vec::new();
    for f in def.def.fields.iter().filter(|f| !paused.contains(&f.name)) {
        let sql = render(&def.def, &f.expr, "a", "b").map_err(unsupported)?;
        let sql = match types.get(&f.name) {
            Some(ty) => format!("({sql})::{ty}"),
            None => sql,
        };
        fields.push(Field {
            name: f.name.clone(),
            sql,
        });
    }
    Ok(Audit {
        def,
        a,
        joined,
        fields,
    })
}

/// A page of the statements' rows `(side, page_end, key, values)` as the two
/// sides' pages, the keys the page can't vouch for, and where it ends.
fn read_pages(rows: &[tokio_postgres::Row]) -> (Page, Page, Uncertain, Option<String>) {
    let (mut one, mut two) = (Page::new(), Page::new());
    let mut uncertain = Uncertain::default();
    let mut page_end = None;
    for row in rows {
        page_end = row.get(1);
        let key: String = row.get(2);
        match row.get::<_, i32>(0) {
            3 => {
                uncertain.keys.insert(key);
            }
            4 => uncertain.all = true,
            side => {
                let values: Vec<Option<String>> = row.get(3);
                if side == 1 {
                    one.insert(key, values);
                } else {
                    two.insert(key, values);
                }
            }
        }
    }
    (one, two, uncertain, page_end)
}

/// A page of the target part.
async fn target_page(
    pool: &Pool,
    audit: &Audit<'_>,
    pk: &[PrimaryKeyColumn],
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, pk).await?;
    keyset.set_cursor(after)?;
    let sql = audit.target_sql(&keyset, &audit.on_collations(&client).await?);
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let (recomputed, persisted, uncertain, page_end) = read_pages(&rows);
    let columns: Vec<&str> = audit.fields.iter().map(|f| f.name.as_str()).collect();
    let diff = diff_page(&columns, recomputed, persisted, page_end);
    Ok(Pass {
        rows_compared: diff.rows_compared,
        page_end: diff.next_after,
        divergences: diff.divergences,
        uncertain,
    })
}

/// A cell of `side`'s ledger on `key`.
fn ledger_cell(
    side: &Side,
    key: &str,
    column: &str,
    persisted: Option<String>,
    recomputed: Option<String>,
) -> Divergence {
    Divergence::Cell {
        key: key.to_string(),
        column: side.divergence_column(column),
        persisted,
        recomputed,
    }
}

/// A page of `side`'s ledger's part against its source (`Part::From`,
/// `Part::Joined`).
async fn ledger_page(
    pool: &Pool,
    audit: &Audit<'_>,
    side: &Side,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let client = pool.get().await?;
    let mut keyset = Keyset::resolve(&client, &side.pk).await?;
    keyset.set_cursor(after)?;
    let sql = audit.ledger_sql(side, &keyset);
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let (expected, persisted, uncertain, page_end) = read_pages(&rows);
    let mut divergences = Vec::new();
    for (key, values) in &expected {
        let Some(held) = persisted.get(key) else {
            divergences.push(ledger_cell(side, key, ENTRY, None, Some(LIVE.to_string())));
            continue;
        };
        // The values lead with `LIVE`, which both sides hold.
        for (column, (want, got)) in side
            .columns
            .iter()
            .zip(values.iter().skip(1).zip(held.iter().skip(1)))
        {
            if want != got {
                divergences.push(ledger_cell(side, key, column, got.clone(), want.clone()));
            }
        }
    }
    Ok(Pass {
        rows_compared: expected.len() as i64,
        page_end,
        divergences,
        uncertain,
    })
}

/// A page of `side`'s ledger's live entries, looked up in the source
/// (`Part::FromOrphans`, `Part::JoinedOrphans`).
async fn orphan_page(
    pool: &Pool,
    audit: &Audit<'_>,
    side: &Side,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let client = pool.get().await?;
    let ledger = &side.ledger.ident;
    let key_col = quote_ident(schema::KEY_COLUMN);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let keys: Vec<String> = match after {
        Some(after) => {
            client
                .query(
                    &format!(
                        "select {key_col} from {ledger} where not {tomb} and {key_col} > $1 \
                         order by {key_col} limit $2"
                    ),
                    &[&after, &limit],
                )
                .await?
        }
        None => {
            client
                .query(
                    &format!(
                        "select {key_col} from {ledger} where not {tomb} \
                         order by {key_col} limit $1"
                    ),
                    &[&limit],
                )
                .await?
        }
    }
    .iter()
    .map(|row| row.get(0))
    .collect();
    let page_end = (keys.len() as i64 == limit)
        .then(|| keys.last().cloned())
        .flatten();
    if keys.is_empty() {
        return Ok(Pass {
            rows_compared: 0,
            page_end: None,
            divergences: Vec::new(),
            uncertain: Uncertain::default(),
        });
    }
    // Each key as its primary key's columns, so the source is probed by its
    // index.
    let mut objects = Vec::with_capacity(keys.len());
    for key in &keys {
        let parts = ddl::split_pk_key(&side.pk, &side.table, key)?;
        let object: serde_json::Map<String, serde_json::Value> = side
            .pk
            .iter()
            .zip(parts)
            .map(|(column, part)| {
                (
                    column.name.clone(),
                    part.map_or(serde_json::Value::Null, |p| {
                        serde_json::Value::String(p.into_owned())
                    }),
                )
            })
            .collect();
        objects.push(serde_json::Value::Object(object).to_string());
    }
    let rows = client
        .query(audit.orphans_sql(side).as_str(), &[&keys, &objects])
        .await?;
    let mut divergences = Vec::new();
    let mut uncertain = Uncertain::default();
    for row in &rows {
        let key: String = row.get(1);
        match row.get::<_, i32>(0) {
            3 => {
                uncertain.keys.insert(key);
            }
            4 => uncertain.all = true,
            _ => divergences.push(ledger_cell(side, &key, ENTRY, Some(LIVE.to_string()), None)),
        }
    }
    divergences.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    Ok(Pass {
        rows_compared: keys.len() as i64,
        page_end,
        divergences,
        uncertain,
    })
}

/// A page of the join keys that hold more than one live entry in `side`'s
/// `M` (a joined table's): the next
/// `limit` live entries with a key in the key's order, and each key among
/// them counted over all of `M`, so a key whose entries straddle the page's
/// end is counted whole on the page it starts on. A page reads `limit`
/// entries whether or not any key is duplicated.
async fn duplicate_page(
    pool: &Pool,
    audit: &Audit<'_>,
    side: &Side,
    after: Option<&str>,
    limit: i64,
) -> Result<Pass, SelfCheckError> {
    let client = pool.get().await?;
    let m = &side.ledger.ident;
    let mut keyset = Keyset::resolve_columns(&client, m, &side.key).await?;
    keyset.set_cursor(after)?;
    let refs = keyset.refs(None);
    let carried = keyset.select_carried(&refs);
    let order = keyset.order_by(&refs);
    let key = ddl::pk_key_sql_expr(keyset.columns(), None);
    let tomb = quote_ident(schema::TOMBSTONE_COLUMN);
    let not_null: Vec<String> = refs.iter().map(|r| format!("{r} is not null")).collect();
    let entries = keyset.side(&refs, |cond| {
        format!(
            "select {key} as __k{carried} from {m} \
             where {cond} and not {tomb} and {not_null} order by {order} limit $1",
            not_null = not_null.join(" and "),
        )
    });
    let carried_columns = keyset.carried(None);
    let same: Vec<String> = refs
        .iter()
        .zip(keyset.carried(Some("g")))
        .map(|(r, c)| format!("m.{r} = {c}"))
        .collect();
    let sql = format!(
        "with entries as materialized ( \
             select e.*, row_number() over (order by {carried_order}) as __rn \
             from ({entries}) e), \
         groups as ( \
             select min(__k) as __k, min(__rn) as __rn, {carried_list} \
             from entries group by {carried_list}) \
         select 0::int, null::text, count(*)::text, \
                (select __k from entries order by __rn desc limit 1), 0::bigint \
         from entries \
         union all \
         select 1, g.__k, c.n::text, null, g.__rn from groups g \
         cross join lateral (select count(*) as n from {m} m \
                             where {same} and not m.{tomb}) c \
         where c.n > 1",
        carried_order = keyset.order_by(&carried_columns),
        carried_list = carried_columns.join(", "),
        same = same.join(" and "),
    );
    let rows = client.query(sql.as_str(), &[&limit]).await?;
    let mut entries_read = 0;
    let mut last = None;
    let mut found = Vec::new();
    for row in &rows {
        match row.get::<_, i32>(0) {
            0 => {
                entries_read = row.get::<_, String>(2).parse::<i64>().unwrap_or(0);
                last = row.get::<_, Option<String>>(3);
            }
            _ => found.push((
                row.get::<_, i64>(4),
                row.get::<_, String>(1),
                row.get::<_, String>(2),
            )),
        }
    }
    found.sort();
    let divergences: Vec<Divergence> = found
        .into_iter()
        .map(|(_, key, count)| Divergence::Cell {
            key,
            column: side.divergence_column("live_entries"),
            persisted: Some(count),
            recomputed: Some("1".to_string()),
        })
        .collect();
    let page_end = (entries_read == limit).then_some(last).flatten();
    // A hand-off of a join key between two `b` rows holds two entries on it
    // while its two changes are applied out of order: what is found is the
    // ring's, not `M`'s, while `b`'s ring holds an unapplied row.
    let unapplied = if divergences.is_empty() {
        false
    } else {
        let ring = side.ring(audit.id());
        let ctes = ring.one_to_one_ctes(&side.ledger);
        client
            .query_one(
                &format!(
                    "with {} select {}",
                    ctes.trim_end().trim_end_matches(','),
                    ring.unapplied_sql()
                ),
                &[],
            )
            .await?
            .get(0)
    };
    Ok(Pass {
        rows_compared: entries_read,
        page_end,
        divergences,
        uncertain: Uncertain {
            all: unapplied,
            keys: BTreeSet::new(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_names_its_part_and_key() {
        for positions in 1..=spec::MAX_POSITIONS {
            for part in Part::walk(positions) {
                let bare = parse_cursor(Some(&part.tag()));
                assert_eq!(bare, (part, None));
                let keyed = parse_cursor(Some(&format!("{}:a:b", part.tag())));
                assert_eq!(keyed, (part, Some("a:b".to_string())));
                assert_eq!(
                    next_cursor(part, Some("k".to_string()), positions),
                    Some(format!("{}:k", part.tag()))
                );
            }
        }
        assert_eq!(parse_cursor(None), (Part::Target, None));
        assert_eq!(
            parse_cursor(Some("17")),
            (Part::Target, Some("17".to_string()))
        );
        assert_eq!(next_cursor(Part::Target, None, 1), Some("r".to_string()));
        let last = Part::Duplicates(Position::FIRST);
        assert_eq!(next_cursor(last, None, 1), None);
        assert_eq!(next_cursor(last, None, 2), Some("m2".to_string()));
    }

    #[test]
    fn a_position_is_tagged_from_position_2_on() {
        let second = Position::from_number(2).unwrap();
        assert_eq!(Part::Joined(Position::FIRST).tag(), "m");
        assert_eq!(Part::Joined(second).tag(), "m2");
        assert_eq!(Part::JoinedOrphans(second).tag(), "mo2");
        assert_eq!(Part::Duplicates(second).tag(), "d2");
        // No position 1 by number, none past the cap, no zero-padding: each is
        // a key of the target's part.
        for tag in ["m1", "mo0", "d5", "m02", "mx"] {
            assert_eq!(Part::of_tag(tag), None, "{tag}");
            assert_eq!(
                parse_cursor(Some(tag)),
                (Part::Target, Some(tag.to_string()))
            );
        }
    }

    #[test]
    fn the_walk_visits_every_part_once_in_order() {
        for positions in 1..=spec::MAX_POSITIONS {
            let mut part = Part::Target;
            let mut seen = vec![part];
            while let Some(next) = part.next(positions) {
                seen.push(next);
                part = next;
            }
            assert_eq!(seen, Part::walk(positions));
            assert_eq!(seen.len(), 3 + 3 * positions);
        }
        let tags: Vec<String> = Part::walk(2).into_iter().map(Part::tag).collect();
        assert_eq!(tags, ["t", "r", "ro", "m", "mo", "d", "m2", "mo2", "d2"]);
    }
}
