//! ADR-0017: the partitioned tier's model of a partitioned source, and the
//! partition maintenance, partition-key updates and leaf-named writes it
//! draws.
//!
//! [`super::partition_table`] makes a program's table a partitioned source
//! keyed by `(pk, grain)` and routed by the grain. Once rows can move between
//! partitions and leaves can leave and rejoin the tree, whether an op
//! succeeds depends on which leaf each row lives in, so the expectations
//! [`super::render_mutate`] set from plain pk liveness no longer hold. A
//! [`PartitionSim`] replays the table's ops the way Postgres applies them
//! (rows keyed by `(pk, grain)`, routed to the leaf whose bound holds the
//! grain, or to the `DEFAULT`, or nowhere; within a middle, on to its own
//! leaf the same way) and sets every op's [`Op::expect`] from that.
//!
//! The model routes grains `0..=`[`ROUTED_GRAIN_MAX`]. `HASH` routing is
//! Postgres's own hash of a `bigint`, which [`HASH_REMAINDERS`] tabulates;
//! a database test checks the table against `satisfies_hash_partition`.

use std::collections::BTreeSet;

use crate::model::{Op, OpOutcome, PartitionAction, PartitionStrategy, Program, Table};

/// The largest grain a partitioned source's ops write: [`super::GRAIN_MAX`]
/// for seeds, one more for a partition-key update (a value that routes to no
/// `LIST` leaf), and up to this for a new leaf's rows, so every `HASH`
/// remainder has a value.
pub const PARTITION_GRAIN_MAX: i64 = 7;

/// The largest grain the model routes: [`PARTITION_GRAIN_MAX`], or the top
/// of the hot-key shape's sliding group window
/// ([`super::HOT_WINDOW_GRAIN_MAX`]) for a partitioned hot table
/// (`partitioned_hot_key_case`), whichever is higher.
pub const ROUTED_GRAIN_MAX: i64 = if super::HOT_WINDOW_GRAIN_MAX > PARTITION_GRAIN_MAX {
    super::HOT_WINDOW_GRAIN_MAX
} else {
    PARTITION_GRAIN_MAX
};

/// The reconcile interval a run over a partitioned source sets
/// (`ManualBackend::set_reconcile_interval`): each check after partition
/// maintenance waits for the pass that records it, which the engine's
/// default interval would make the bulk of the run.
pub const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// `HASH_REMAINDERS[modulus - 1][grain]`: the remainder of the `HASH` leaf a
/// `bigint` grain routes to, for each modulus [`super::strategy`] draws
/// (`1..=4`). Postgres's partition hash is seeded with a constant, so it is
/// the same on every server and version.
pub const HASH_REMAINDERS: [[u8; ROUTED_GRAIN_MAX as usize + 1]; 4] = [
    [
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ],
    [
        0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 1,
        0, 1, 0, 1, 0, 1, 1, 1,
    ],
    [
        2, 2, 0, 1, 0, 2, 0, 1, 0, 2, 1, 2, 2, 1, 1, 0, 0, 2, 0, 0, 0, 1, 2, 2, 2, 1, 1, 0, 2, 0,
        2, 2, 2, 0, 0, 2, 1, 2,
    ],
    [
        0, 0, 2, 1, 3, 1, 3, 3, 1, 1, 3, 1, 0, 2, 0, 3, 0, 0, 2, 1, 1, 1, 3, 2, 3, 2, 0, 2, 0, 3,
        0, 1, 0, 3, 0, 1, 1, 1,
    ],
];

/// One partition-maintenance draw, resolved against the modelled tree when
/// it is placed (see [`add_partition_maintenance`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceKind {
    /// Detach a leaf of the tree; the table keeps its rows. `CONCURRENTLY`
    /// only while the tree has no `DEFAULT` (Postgres refuses it then).
    Detach { concurrently: bool },
    /// Attach a detached table again under its own bound, with its rows.
    Attach,
    /// Drop a leaf of the tree.
    Drop,
    /// `TRUNCATE` a leaf of the tree, naming the leaf.
    TruncateLeaf,
    /// A new leaf for a bound no leaf holds, with `rows` rows written into
    /// it directly; when `standalone`, created outside the tree and
    /// attached with them. When every bound is held, a leaf is dropped
    /// first and the new one takes its bound.
    Create { rows: u8, standalone: bool },
    /// Attach a detached table under a free bound other than its own (with
    /// none detached, a leaf is detached first, `CONCURRENTLY` when
    /// `concurrently` and Postgres allows it). Its rows are first moved to
    /// the new bound's smallest grain by partition-key updates named on it,
    /// which a `CHECK` constraint a concurrent detach left refuses. With no
    /// such bound free, another leaf is dropped first.
    Rebound { concurrently: bool },
    /// `LIST` only: a non-`DEFAULT` table detached (as for `Rebound`), then
    /// attached again with its value list widened by a value no leaf holds;
    /// with none free, another leaf is dropped first.
    Widen { concurrently: bool },
    /// `HASH` only: a leaf over more than one slot detached and replaced by
    /// new leaves over its remainders at a multiple of its modulus, each
    /// with `rows` rows.
    Resplit { rows: u8 },
    /// A new middle for a bound no leaf holds, never the `DEFAULT` (#2106)
    /// (for `LIST`, a free value and up to two more free values above it),
    /// partitioned `BY LIST` of the
    /// grain: a leaf for each of `leaves` of the grains its bound holds, and
    /// a `DEFAULT` when `default` or `leaves` is zero. It holds `rows` rows
    /// (primary keys `1..=rows`) spread over the grains its leaves take;
    /// when `standalone`, it is built outside the tree and attached with
    /// them. When every bound is held, a leaf is dropped first. Once
    /// created, `then` acts on it at once; later, it is detached, attached,
    /// dropped, truncated and rebound like any other leaf of the tree, and
    /// its own leaves are written and truncated by name.
    Middle {
        rows: u8,
        leaves: u8,
        default: bool,
        standalone: bool,
        then: MiddleThen,
    },
}

/// What a [`MaintenanceKind::Middle`] does to the middle right after
/// creating it, so a middle's detach, attach and drop are drawn as often
/// as the middle itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddleThen {
    /// Nothing.
    Stay,
    /// Detach it, `CONCURRENTLY` when asked and Postgres allows it.
    Detach { concurrently: bool },
    /// Detach it as for `Detach`, then attach it again under its bound.
    Reattach { concurrently: bool },
    /// Drop it.
    Drop,
}

/// One draw [`add_partition_maintenance`] layers on a program's partitioned
/// table. Plain numbers, so a counterexample shrinks; each is resolved
/// against the modelled tree where it lands, and one that has nothing to act
/// on there (a detach with no leaf left, say) is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionDraw {
    /// Partition maintenance before the program's op `at` (modulo one more
    /// than the op count, so it can also come last). `pick` chooses among
    /// the leaves or bounds the kind can act on.
    Maintenance {
        at: usize,
        pick: usize,
        kind: MaintenanceKind,
    },
    /// Before op `at`, an `UPDATE` of row `pk`'s partition key to `grain`,
    /// which moves it to another partition, named on the leaf it lives in
    /// when `via_leaf` (where a value outside the leaf's bound is refused).
    MoveRow {
        at: usize,
        pk: i64,
        grain: i64,
        via_leaf: bool,
    },
    /// Issue op `at`, if it writes the partitioned table, naming the leaf
    /// its row lives in (or routes to) instead of the root; with `detached`,
    /// a detached table holding the row instead, if there is one.
    ViaLeaf { at: usize, detached: bool },
}

/// How a [`PartitionStrategy`] lays out its bounds: a bound is a set of
/// slots, the finest regions the tree can be cut into. `LIST` and `RANGE`
/// have a slot per value or range of the initial tree, the `DEFAULT` last;
/// `HASH` has a slot per remainder of the finest modulus a re-split reaches
/// ([`hash_slots`]), and a leaf of modulus `m` and remainder `r` holds every
/// slot `s` with `s % m == r`. The `i`-th leaf `partitioned_source_ddl`
/// creates is `<root>_p<i>` (`<root>_pd` for the `DEFAULT`).
#[derive(Debug, Clone, Copy)]
struct Layout(PartitionStrategy);

/// The finest `HASH` modulus a tree of `modulus` is cut into: every modulus
/// [`HASH_REMAINDERS`] tabulates that the initial one divides or is divided
/// by, so any mix of them is one Postgres accepts (each a factor of the next
/// larger).
fn hash_slots(modulus: u8) -> usize {
    if modulus == 3 { 3 } else { 4 }
}

impl Layout {
    fn slots(self) -> usize {
        match self.0 {
            PartitionStrategy::Hash { modulus } => hash_slots(modulus),
            PartitionStrategy::List { values, default } => values as usize + usize::from(default),
            PartitionStrategy::Range { bounds } => bounds as usize,
        }
    }

    fn default_slot(self) -> Option<usize> {
        match self.0 {
            PartitionStrategy::List {
                values,
                default: true,
            } => Some(values as usize),
            _ => None,
        }
    }

    /// How many leaves `partitioned_source_ddl` creates.
    fn initial_leaves(self) -> usize {
        match self.0 {
            PartitionStrategy::Hash { modulus } => modulus as usize,
            _ => self.slots(),
        }
    }

    /// The slots of the `i`-th leaf `partitioned_source_ddl` creates.
    fn initial(self, i: usize) -> BTreeSet<usize> {
        match self.0 {
            PartitionStrategy::Hash { modulus } => (0..self.slots())
                .filter(|s| s % modulus as usize == i)
                .collect(),
            _ => BTreeSet::from([i]),
        }
    }

    /// Every bound a new leaf may take, coarsest first: for `HASH`, each
    /// remainder of each modulus dividing the finest; otherwise each slot.
    fn bounds(self) -> Vec<BTreeSet<usize>> {
        match self.0 {
            PartitionStrategy::Hash { .. } => {
                let slots = self.slots();
                (1..=slots)
                    .filter(|m| slots.is_multiple_of(*m))
                    .flat_map(|m| (0..m).map(move |r| (0..slots).filter(|s| s % m == r).collect()))
                    .collect()
            }
            _ => (0..self.slots()).map(|s| BTreeSet::from([s])).collect(),
        }
    }

    /// The non-`DEFAULT` slot whose bound holds `grain`, if any.
    fn slot_of(self, grain: i64) -> Option<usize> {
        match self.0 {
            PartitionStrategy::Hash { modulus } => {
                let grain = usize::try_from(grain)
                    .ok()
                    .filter(|&g| g <= ROUTED_GRAIN_MAX as usize)
                    .unwrap_or_else(|| {
                        panic!(
                            "the partition model routes grains 0..={ROUTED_GRAIN_MAX}, not {grain}"
                        )
                    });
                Some(HASH_REMAINDERS[hash_slots(modulus) - 1][grain] as usize)
            }
            PartitionStrategy::List { values, .. } => (0..i64::from(values))
                .contains(&grain)
                .then_some(grain as usize),
            PartitionStrategy::Range { bounds } => {
                Some(grain.clamp(0, i64::from(bounds) - 1) as usize)
            }
        }
    }

    /// The `HASH` modulus of a bound over `slots`.
    fn hash_modulus(self, slots: &BTreeSet<usize>) -> usize {
        self.slots() / slots.len()
    }

    /// The bound clause over `slots`, as `partitioned_source_ddl` writes a
    /// one-slot bound.
    fn bound(self, slots: &BTreeSet<usize>) -> String {
        let first = *slots
            .first()
            .expect("a bound holds a slot — a generator bug");
        match self.0 {
            PartitionStrategy::Hash { .. } => {
                let modulus = self.hash_modulus(slots);
                assert!(
                    self.slots().is_multiple_of(slots.len())
                        && slots.iter().all(|s| s % modulus == first % modulus),
                    "{slots:?} is no HASH bound — a generator bug"
                );
                format!(
                    "for values with (modulus {modulus}, remainder {})",
                    first % modulus
                )
            }
            PartitionStrategy::List { .. }
                if self.default_slot().is_some_and(|d| slots.contains(&d)) =>
            {
                assert_eq!(slots.len(), 1, "a DEFAULT with values — a generator bug");
                "default".to_string()
            }
            PartitionStrategy::List { .. } => {
                let values: Vec<String> = slots.iter().map(usize::to_string).collect();
                format!("for values in ({})", values.join(", "))
            }
            PartitionStrategy::Range { bounds } => {
                assert_eq!(
                    slots.len(),
                    1,
                    "a RANGE bound over several slots — a generator bug"
                );
                let from = if first == 0 {
                    "minvalue".to_string()
                } else {
                    first.to_string()
                };
                let to = if first + 1 == bounds as usize {
                    "maxvalue".to_string()
                } else {
                    (first + 1).to_string()
                };
                format!("for values from ({from}) to ({to})")
            }
        }
    }

    /// The slots of a bound clause [`Layout::bound`] wrote.
    fn slots_of_bound(self, bound: &str) -> BTreeSet<usize> {
        let found = match self.0 {
            PartitionStrategy::List { .. } => bound
                .strip_prefix("for values in (")
                .and_then(|b| b.strip_suffix(')'))
                .map(|values| {
                    values
                        .split(", ")
                        .map(|v| v.parse().expect("a LIST value is a slot"))
                        .collect()
                })
                .or_else(|| {
                    (bound == "default").then(|| self.default_slot().into_iter().collect())
                }),
            _ => self.bounds().into_iter().find(|s| self.bound(s) == bound),
        };
        found.unwrap_or_else(|| panic!("no slots have the bound {bound:?} — a generator bug"))
    }
}

/// A leaf of a middle: a `LIST` leaf over `values`, or its `DEFAULT` when
/// `None`.
#[derive(Debug, Clone)]
struct SubLeaf {
    name: String,
    values: Option<BTreeSet<i64>>,
}

/// The leaf of `leaves` a row with `grain` routes to: the one whose value
/// list holds it, else the `DEFAULT`, else none.
fn sub_route(leaves: &[SubLeaf], grain: i64) -> Option<usize> {
    leaves
        .iter()
        .position(|l| l.values.as_ref().is_some_and(|v| v.contains(&grain)))
        .or_else(|| leaves.iter().position(|l| l.values.is_none()))
}

/// The bound clause of a middle's leaf over `values` (`None`: its
/// `DEFAULT`), and back.
fn sub_bound(values: Option<&BTreeSet<i64>>) -> String {
    match values {
        Some(values) => {
            let values: Vec<String> = values.iter().map(i64::to_string).collect();
            format!("for values in ({})", values.join(", "))
        }
        None => "default".to_string(),
    }
}

fn sub_values(bound: &str) -> Option<BTreeSet<i64>> {
    if bound == "default" {
        return None;
    }
    let values = bound
        .strip_prefix("for values in (")
        .and_then(|b| b.strip_suffix(')'))
        .unwrap_or_else(|| panic!("no middle's leaf has the bound {bound:?} — a generator bug"));
    Some(
        values
            .split(", ")
            .map(|v| v.parse().expect("a middle's leaf value is a grain"))
            .collect(),
    )
}

/// Where a write named on a table lands: table `table` of
/// [`PartitionSim::tables`], or, for a middle, its leaf `leaf` alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Node {
    table: usize,
    leaf: Option<usize>,
}

/// One table that is, or was, a leaf: its bound's slots, whether it is in
/// the tree, and its rows' `(pk, grain)` keys. A middle's rows are its
/// leaves', which [`SimTable::sub`] routes.
#[derive(Debug, Clone)]
struct SimTable {
    name: String,
    slots: BTreeSet<usize>,
    attached: bool,
    /// The bounds the table carries as `CHECK` constraints: a `DETACH ...
    /// CONCURRENTLY` of a `LIST` or `RANGE` leaf adds one for the bound it
    /// had, which stays when the table is attached again, under any bound,
    /// and refuses every row outside it.
    checks: Vec<BTreeSet<usize>>,
    rows: BTreeSet<(i64, i64)>,
    /// A middle's leaves, partitioned by the grain; empty for a leaf.
    sub: Vec<SubLeaf>,
}

/// A partitioned source as Postgres holds it, for working out each op's
/// outcome. Only the primary key `(pk, grain)` matters to whether an op
/// succeeds, so that is all a row keeps.
#[derive(Debug, Clone)]
pub struct PartitionSim {
    layout: Layout,
    root: String,
    pk_col: String,
    grain_col: String,
    /// Every table that is a leaf or was detached from the tree (a dropped
    /// one is gone).
    tables: Vec<SimTable>,
    /// How many new leaves have been named.
    created: usize,
}

impl PartitionSim {
    /// The partitioned `table` as `partitioned_source_ddl` creates it: a leaf
    /// per bound, all empty.
    ///
    /// # Panics
    ///
    /// If `table` isn't partitioned.
    pub fn new(table: &Table) -> Self {
        let partitioning = table
            .partitioning
            .as_ref()
            .expect("PartitionSim models a partitioned table");
        let layout = Layout(partitioning.strategy);
        let tables = (0..layout.initial_leaves())
            .map(|leaf| SimTable {
                name: if Some(leaf) == layout.default_slot() {
                    format!("{}_pd", table.name)
                } else {
                    format!("{}_p{leaf}", table.name)
                },
                slots: layout.initial(leaf),
                attached: true,
                checks: Vec::new(),
                rows: BTreeSet::new(),
                sub: Vec::new(),
            })
            .collect();
        PartitionSim {
            layout,
            root: table.name.clone(),
            pk_col: table.pk_col.clone(),
            grain_col: partitioning.column.clone(),
            tables,
            created: 0,
        }
    }

    fn occupant(&self, slot: usize) -> Option<usize> {
        self.tables
            .iter()
            .position(|t| t.attached && t.slots.contains(&slot))
    }

    /// Whether no leaf of the tree holds any of `slots`.
    fn free(&self, slots: &BTreeSet<usize>) -> bool {
        slots.iter().all(|&s| self.occupant(s).is_none())
    }

    /// Every bound a leaf may take in the tree as it is: one no leaf
    /// overlaps.
    fn free_bounds(&self) -> Vec<BTreeSet<usize>> {
        self.layout
            .bounds()
            .into_iter()
            .filter(|b| self.free(b))
            .collect()
    }

    /// The free bounds a middle may take: any but the `DEFAULT`. A leaf
    /// under a partitioned `DEFAULT` keeps a stale partition constraint in
    /// a backend that loaded it once a sibling of the `DEFAULT` is detached
    /// or dropped, and refuses rows the `DEFAULT` now takes (a Postgres bug,
    /// #2106), which no model of the tree can predict.
    fn middle_bounds(&self) -> Vec<BTreeSet<usize>> {
        self.free_bounds()
            .into_iter()
            .filter(|b| !self.layout.default_slot().is_some_and(|d| b.contains(&d)))
            .collect()
    }

    /// Whether a row with `grain` passes table `t`'s `CHECK` constraints.
    fn passes_checks(&self, t: usize, grain: i64) -> bool {
        let slot = self.layout.slot_of(grain);
        self.tables[t]
            .checks
            .iter()
            .all(|check| slot.is_some_and(|s| check.contains(&s)))
    }

    /// The smallest grain a leaf over `slots` holds in the tree as it is (for
    /// a `DEFAULT`, one no other leaf takes), if any.
    fn grain_for(&self, slots: &BTreeSet<usize>) -> Option<i64> {
        self.grains_for(slots).first().copied()
    }

    /// Every grain up to [`PARTITION_GRAIN_MAX`] a leaf over `slots` holds
    /// in the tree as it is, smallest first.
    fn grains_for(&self, slots: &BTreeSet<usize>) -> Vec<i64> {
        let default = self
            .layout
            .default_slot()
            .is_some_and(|d| slots.contains(&d));
        (0..=PARTITION_GRAIN_MAX)
            .filter(|&g| match self.layout.slot_of(g) {
                Some(s) if !default => slots.contains(&s),
                Some(s) => self.occupant(s).is_none(),
                None => default,
            })
            .collect()
    }

    /// The leaf a row with `grain` routes to: its bound's, else the
    /// `DEFAULT`, else none.
    fn route(&self, grain: i64) -> Option<usize> {
        self.layout
            .slot_of(grain)
            .and_then(|s| self.occupant(s))
            .or_else(|| self.layout.default_slot().and_then(|s| self.occupant(s)))
    }

    /// Whether a row with `grain` may live in table `t`: one its `CHECK`
    /// constraints pass, that, in the tree, routes there and, for a middle,
    /// that one of its leaves takes.
    fn holds(&self, t: usize, grain: i64) -> bool {
        (!self.tables[t].attached || self.route(grain) == Some(t))
            && self.passes_checks(t, grain)
            && (self.tables[t].sub.is_empty() || sub_route(&self.tables[t].sub, grain).is_some())
    }

    /// Whether a row with `grain` of `node`'s table lands in `node`.
    fn in_node(&self, node: Node, grain: i64) -> bool {
        node.leaf
            .is_none_or(|l| sub_route(&self.tables[node.table].sub, grain) == Some(l))
    }

    /// The name of the table a row of table `t` with `grain` lives in: for a
    /// middle, the leaf of its own that takes it, if any.
    fn name_in(&self, t: usize, grain: i64) -> String {
        let table = &self.tables[t];
        match sub_route(&table.sub, grain) {
            Some(l) => table.sub[l].name.clone(),
            None => table.name.clone(),
        }
    }

    /// The `(table, row)` of every row a write named on `within` (the root
    /// when `None`) reaches.
    fn scope_rows(&self, within: Option<Node>) -> Vec<(usize, (i64, i64))> {
        (0..self.tables.len())
            .filter(|&t| within.map_or(self.tables[t].attached, |w| w.table == t))
            .flat_map(|t| {
                self.tables[t]
                    .rows
                    .iter()
                    .filter(move |&&(_, g)| within.is_none_or(|w| self.in_node(w, g)))
                    .map(move |&row| (t, row))
            })
            .collect()
    }

    /// Whether `rows` all fit the bound over `slots` in the tree as it is,
    /// and the `DEFAULT` holds none that would (the checks Postgres makes to
    /// attach or create a leaf there).
    fn may_take_slots(&self, slots: &BTreeSet<usize>, rows: &BTreeSet<(i64, i64)>) -> bool {
        let fits = |grain: i64| match self.layout.slot_of(grain) {
            Some(s) => slots.contains(&s),
            None => false,
        };
        if self
            .layout
            .default_slot()
            .is_some_and(|d| slots.contains(&d))
        {
            return rows.iter().all(|&(_, g)| {
                self.layout
                    .slot_of(g)
                    .is_none_or(|s| self.occupant(s).is_none())
            });
        }
        let default_clear = self
            .layout
            .default_slot()
            .and_then(|s| self.occupant(s))
            .is_none_or(|d| self.tables[d].rows.iter().all(|&(_, g)| !fits(g)));
        default_clear && rows.iter().all(|&(_, g)| fits(g))
    }

    fn table_named(&self, name: &str) -> usize {
        self.tables
            .iter()
            .position(|t| t.name == name)
            .unwrap_or_else(|| panic!("no modelled table is named {name:?} — a generator bug"))
    }

    /// The table, or a middle's leaf, named `name`.
    fn node_named(&self, name: &str) -> Node {
        if let Some(table) = self.tables.iter().position(|t| t.name == name) {
            return Node { table, leaf: None };
        }
        self.tables
            .iter()
            .enumerate()
            .find_map(|(table, t)| {
                let leaf = t.sub.iter().position(|l| l.name == name)?;
                Some(Node {
                    table,
                    leaf: Some(leaf),
                })
            })
            .unwrap_or_else(|| panic!("no modelled table is named {name:?} — a generator bug"))
    }

    /// The `(pk, grain)` of a row an insert writes.
    fn key(&self, row: &[(String, Option<String>)]) -> (i64, i64) {
        let get = |column: &str| -> i64 {
            row.iter()
                .find(|(c, _)| c == column)
                .and_then(|(_, v)| v.as_deref())
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| {
                    panic!(
                        "an insert into {} carries no integer {column} — a generator bug",
                        self.root
                    )
                })
        };
        (get(&self.pk_col), get(&self.grain_col))
    }

    /// Inserts `row` into `within` (any leaf when `None`: the one it
    /// routes to), as Postgres would.
    fn insert(&mut self, within: Option<Node>, row: &[(String, Option<String>)]) -> OpOutcome {
        let (pk, grain) = self.key(row);
        let Some(node) =
            within.or_else(|| self.route(grain).map(|table| Node { table, leaf: None }))
        else {
            return OpOutcome::Fails;
        };
        let t = node.table;
        if !self.holds(t, grain) || !self.in_node(node, grain) {
            return OpOutcome::Fails;
        }
        if self.tables[t].rows.insert((pk, grain)) {
            OpOutcome::Succeeds
        } else {
            OpOutcome::Fails
        }
    }

    /// Updates the rows with primary-key column `pk` in `within` (every leaf
    /// when `None`, as through the root), setting the grain to `grain` when
    /// given.
    fn update(&mut self, within: Option<Node>, pk: i64, grain: Option<i64>) -> OpOutcome {
        let matched: Vec<(usize, i64)> = self
            .scope_rows(within)
            .into_iter()
            .filter(|&(_, (p, _))| p == pk)
            .map(|(t, (_, g))| (t, g))
            .collect();
        let Some(new) = grain else {
            return if matched.is_empty() {
                OpOutcome::AffectsNoRows
            } else {
                OpOutcome::Succeeds
            };
        };
        match matched.as_slice() {
            [] => OpOutcome::AffectsNoRows,
            // Every matched row takes the same key: a unique violation.
            [_, _, ..] => OpOutcome::Fails,
            [(_, old)] if *old == new => OpOutcome::Succeeds,
            &[(t, old)] => {
                // Through the root, a row whose new key routes elsewhere
                // moves there; named on a leaf, it must stay.
                let dest = match within {
                    None => match self.route(new) {
                        Some(dest) if self.holds(dest, new) => dest,
                        _ => return OpOutcome::Fails,
                    },
                    Some(node) if self.holds(t, new) && self.in_node(node, new) => t,
                    Some(_) => return OpOutcome::Fails,
                };
                if self.tables[dest].rows.contains(&(pk, new)) {
                    return OpOutcome::Fails;
                }
                self.tables[t].rows.remove(&(pk, old));
                self.tables[dest].rows.insert((pk, new));
                OpOutcome::Succeeds
            }
        }
    }

    fn delete(&mut self, within: Option<Node>, pk: i64) -> OpOutcome {
        self.remove(within, |p| p == pk)
    }

    fn truncate(&mut self, within: Option<Node>) -> OpOutcome {
        self.remove(within, |_| true)
    }

    /// Removes the rows in `within` whose primary-key column `doomed`
    /// accepts.
    fn remove(&mut self, within: Option<Node>, doomed: impl Fn(i64) -> bool) -> OpOutcome {
        let rows: Vec<(usize, (i64, i64))> = self
            .scope_rows(within)
            .into_iter()
            .filter(|&(_, (p, _))| doomed(p))
            .collect();
        for (t, row) in &rows {
            self.tables[*t].rows.remove(row);
        }
        if rows.is_empty() {
            OpOutcome::AffectsNoRows
        } else {
            OpOutcome::Succeeds
        }
    }

    /// Applies a write on the root, or on `within`, and returns its
    /// outcome.
    fn write(&mut self, within: Option<Node>, op: &Op) -> OpOutcome {
        match op {
            Op::Insert { row, .. } => self.insert(within, row),
            Op::BulkInsert { rows, .. } => {
                let before = self.clone();
                for row in rows {
                    if self.insert(within, row) == OpOutcome::Fails {
                        *self = before;
                        return OpOutcome::Fails;
                    }
                }
                OpOutcome::Succeeds
            }
            Op::Update { pk, changes, .. } => {
                let grain = changes
                    .iter()
                    .find(|(c, _)| *c == self.grain_col)
                    .map(|(_, v)| {
                        v.as_deref()
                            .and_then(|v| v.parse().ok())
                            .expect("a partition-key update writes an integer — a generator bug")
                    });
                self.update(within, parse_pk(pk), grain)
            }
            Op::Delete { pk, .. } => self.delete(within, parse_pk(pk)),
            Op::Truncate { .. } => self.truncate(within),
            Op::Leaf { .. } | Op::Partition { .. } => {
                panic!("PartitionSim::write takes a single write — a generator bug")
            }
        }
    }

    /// Applies `op`, which must name this table, and returns the outcome
    /// Postgres gives it.
    pub fn apply(&mut self, op: &Op) -> OpOutcome {
        assert_eq!(
            op.table(),
            self.root,
            "PartitionSim::apply: an op on another table"
        );
        match op {
            Op::Leaf { leaf, op } => {
                let node = self.node_named(leaf);
                self.write(Some(node), op)
            }
            Op::Partition { action, .. } => self.maintain(action),
            _ => self.write(None, op),
        }
    }

    fn maintain(&mut self, action: &PartitionAction) -> OpOutcome {
        match action {
            PartitionAction::Detach { leaf, concurrently } => {
                let t = self.table_named(leaf);
                assert!(
                    self.tables[t].attached,
                    "detach of a table outside the tree"
                );
                self.tables[t].attached = false;
                // Postgres adds no such constraint for a `HASH` bound.
                if *concurrently && !matches!(self.layout.0, PartitionStrategy::Hash { .. }) {
                    let bound = self.tables[t].slots.clone();
                    self.tables[t].checks.push(bound);
                }
                OpOutcome::Succeeds
            }
            PartitionAction::Attach { leaf, bound } => {
                let t = self.table_named(leaf);
                let slots = self.layout.slots_of_bound(bound);
                assert!(
                    !self.tables[t].attached && self.free(&slots),
                    "attach of {leaf} to a bound a leaf holds — a generator bug"
                );
                if !self.may_take_slots(&slots, &self.tables[t].rows) {
                    return OpOutcome::Fails;
                }
                self.tables[t].attached = true;
                self.tables[t].slots = slots;
                OpOutcome::Succeeds
            }
            PartitionAction::Drop { leaf } => {
                let t = self.table_named(leaf);
                self.tables.remove(t);
                OpOutcome::Succeeds
            }
            PartitionAction::Create {
                leaf, bound, rows, ..
            } => {
                let slots = self.layout.slots_of_bound(bound);
                assert!(
                    self.free(&slots),
                    "a new leaf for a bound a leaf holds — a generator bug"
                );
                let mut keys = BTreeSet::new();
                for row in rows {
                    if !keys.insert(self.key(row)) {
                        return OpOutcome::Fails;
                    }
                }
                if !self.may_take_slots(&slots, &keys) {
                    return OpOutcome::Fails;
                }
                self.tables.push(SimTable {
                    name: leaf.clone(),
                    slots,
                    attached: true,
                    checks: Vec::new(),
                    rows: keys,
                    sub: Vec::new(),
                });
                OpOutcome::Succeeds
            }
            PartitionAction::CreateMiddle {
                middle,
                bound,
                leaves,
                rows,
                ..
            } => {
                let slots = self.layout.slots_of_bound(bound);
                assert!(
                    self.free(&slots),
                    "a new middle for a bound a leaf holds — a generator bug"
                );
                let sub: Vec<SubLeaf> = leaves
                    .iter()
                    .map(|(name, bound)| SubLeaf {
                        name: name.clone(),
                        values: sub_values(bound),
                    })
                    .collect();
                let mut keys = BTreeSet::new();
                for row in rows {
                    let key = self.key(row);
                    if !keys.insert(key) || sub_route(&sub, key.1).is_none() {
                        return OpOutcome::Fails;
                    }
                }
                if !self.may_take_slots(&slots, &keys) {
                    return OpOutcome::Fails;
                }
                self.tables.push(SimTable {
                    name: middle.clone(),
                    slots,
                    attached: true,
                    checks: Vec::new(),
                    rows: keys,
                    sub,
                });
                OpOutcome::Succeeds
            }
        }
    }
}

fn parse_pk(pk: &str) -> i64 {
    pk.parse()
        .unwrap_or_else(|_| panic!("a partitioned table's pk is an integer, not {pk:?}"))
}

/// Sets the expectation of every op on `program.tables[table]`, a
/// partitioned source, from a [`PartitionSim`] replay of them in order.
pub fn resimulate(program: &mut Program, table: usize) {
    let mut sim = PartitionSim::new(&program.tables[table]);
    let name = program.tables[table].name.clone();
    for op in program.ops.iter_mut().filter(|op| op.table() == name) {
        let outcome = sim.apply(op);
        op.set_expect(outcome);
    }
}

/// The coverage names of the ops on `program`'s partitioned tables that
/// change a bound or act on a middle, each one Postgres accepts:
/// `attach_rebound` (a table attached under a bound other than the one it
/// last had), `attach_widened` (a `LIST` table attached with values added
/// to its own), `create_resplit` (a new `HASH` leaf at a modulus other than
/// the tree's initial one), `middle_default` (a new middle with a
/// `DEFAULT`), `attach_middle`, `detach_middle` and `drop_middle` (of a
/// middle), and `middle_leaf_write` (a write named on a middle's leaf).
pub fn tree_change_names(program: &Program) -> Vec<&'static str> {
    let mut names = Vec::new();
    for table in program.tables.iter().filter(|t| t.partitioning.is_some()) {
        let mut sim = PartitionSim::new(table);
        for op in program.ops.iter().filter(|op| op.table() == table.name) {
            let is_middle = |leaf: &str| !sim.tables[sim.table_named(leaf)].sub.is_empty();
            let middle = match op {
                Op::Partition {
                    action: PartitionAction::Attach { leaf, .. },
                    ..
                } if is_middle(leaf) => Some("attach_middle"),
                Op::Partition {
                    action: PartitionAction::Detach { leaf, .. },
                    ..
                } if is_middle(leaf) => Some("detach_middle"),
                Op::Partition {
                    action: PartitionAction::Drop { leaf },
                    ..
                } if is_middle(leaf) => Some("drop_middle"),
                Op::Partition {
                    action: PartitionAction::CreateMiddle { leaves, .. },
                    ..
                } if leaves.iter().any(|(_, bound)| bound == "default") => Some("middle_default"),
                Op::Leaf { leaf, .. } if sim.node_named(leaf).leaf.is_some() => {
                    Some("middle_leaf_write")
                }
                _ => None,
            };
            let name = match op {
                Op::Partition {
                    action: PartitionAction::Attach { leaf, bound },
                    ..
                } => {
                    let old = &sim.tables[sim.table_named(leaf)].slots;
                    let new = sim.layout.slots_of_bound(bound);
                    if new == *old {
                        None
                    } else if matches!(sim.layout.0, PartitionStrategy::List { .. })
                        && new.is_superset(old)
                    {
                        // A `HASH` table attached at a coarser modulus is a
                        // rebound, not a widened value list.
                        Some("attach_widened")
                    } else {
                        Some("attach_rebound")
                    }
                }
                Op::Partition {
                    action: PartitionAction::Create { bound, .. },
                    ..
                } => match sim.layout.0 {
                    PartitionStrategy::Hash { modulus }
                        if sim.layout.hash_modulus(&sim.layout.slots_of_bound(bound))
                            != modulus as usize =>
                    {
                        Some("create_resplit")
                    }
                    _ => None,
                },
                _ => None,
            };
            if sim.apply(op) == OpOutcome::Succeeds {
                names.extend(name);
                names.extend(middle);
            }
        }
    }
    names
}

/// Layers `draws` on `program`, whose table `table` is a partitioned source
/// ([`super::partition_table`]): partition maintenance and partition-key
/// updates inserted into its op stream, and some of its writes named on a
/// leaf. Each draw is resolved against a [`PartitionSim`] replay at the
/// point it lands, and every op on the table gets its expectation from the
/// same replay.
///
/// A new leaf's rows have primary keys `1..=rows` (so a key can live in
/// more than one partition at once, under different grains) and the
/// smallest grain its bound holds.
pub fn add_partition_maintenance(
    mut program: Program,
    table: usize,
    draws: &[PartitionDraw],
) -> Program {
    let spec = program.tables[table].clone();
    let root = spec.name.clone();
    let grain_col = spec
        .partitioning
        .as_ref()
        .expect("add_partition_maintenance: the table is partitioned")
        .column
        .clone();
    let ops = std::mem::take(&mut program.ops);
    let count = ops.len();
    let slots = count + 1;
    let mut sim = PartitionSim::new(&spec);
    let mut out = Vec::with_capacity(count + draws.len());
    for (index, op) in ops.into_iter().map(Some).chain([None]).enumerate() {
        for draw in draws {
            match *draw {
                PartitionDraw::Maintenance { at, pick, kind } if at % slots == index => {
                    maintenance(&mut sim, &spec, pick, kind, &mut out);
                }
                PartitionDraw::MoveRow {
                    at,
                    pk,
                    grain,
                    via_leaf,
                } if at % slots == index => {
                    let update = Op::Update {
                        table: root.clone(),
                        pk: pk.to_string(),
                        changes: vec![(grain_col.clone(), Some(grain.to_string()))],
                        expect: OpOutcome::Succeeds,
                    };
                    let op = if via_leaf {
                        on_leaf(&sim, update, false)
                    } else {
                        update
                    };
                    emit(&mut sim, op, &mut out);
                }
                _ => {}
            }
        }
        let Some(op) = op else { break };
        if op.table() != root {
            out.push(op);
            continue;
        }
        let via = draws.iter().find_map(|d| match *d {
            PartitionDraw::ViaLeaf { at, detached } if at % count.max(1) == index => Some(detached),
            _ => None,
        });
        let op = match via {
            Some(detached) => on_leaf(&sim, op, detached),
            None => op,
        };
        emit(&mut sim, op, &mut out);
    }
    program.ops = out;
    program
}

/// Applies `op` to `sim`, sets its expectation from it and appends it.
fn emit(sim: &mut PartitionSim, mut op: Op, out: &mut Vec<Op>) {
    let outcome = sim.apply(&op);
    op.set_expect(outcome);
    out.push(op);
}

/// Appends the ops a [`PartitionDraw::Maintenance`] becomes in the tree
/// `sim` models (none if the kind has nothing to act on there), applying
/// each to `sim`.
fn maintenance(
    sim: &mut PartitionSim,
    table: &Table,
    pick: usize,
    kind: MaintenanceKind,
    out: &mut Vec<Op>,
) {
    let choose = |candidates: Vec<usize>| -> Option<usize> {
        (!candidates.is_empty()).then(|| candidates[pick % candidates.len()])
    };
    let partition = |action: PartitionAction| Op::Partition {
        table: table.name.clone(),
        action,
        expect: OpOutcome::Succeeds,
    };
    let is_default = |sim: &PartitionSim, t: usize| {
        sim.layout
            .default_slot()
            .is_some_and(|d| sim.tables[t].slots.contains(&d))
    };
    // A detached table `keep` accepts, or with none, a leaf it accepts,
    // detached now: its name.
    let detached_or_detach =
        |sim: &mut PartitionSim,
         out: &mut Vec<Op>,
         concurrently: bool,
         keep: &dyn Fn(&PartitionSim, usize) -> bool| {
            let outside: Vec<usize> = (0..sim.tables.len())
                .filter(|&t| !sim.tables[t].attached && keep(sim, t))
                .collect();
            if let Some(t) = choose(outside) {
                return Some(sim.tables[t].name.clone());
            }
            let t = choose(
                (0..sim.tables.len())
                    .filter(|&t| sim.tables[t].attached && keep(sim, t))
                    .collect(),
            )?;
            let name = sim.tables[t].name.clone();
            emit(sim, partition(detach_action(sim, &name, concurrently)), out);
            Some(name)
        };
    // Drops a leaf of the tree other than `spare` (and, when given, only one
    // `keep` accepts).
    let drop_other = |sim: &mut PartitionSim,
                      out: &mut Vec<Op>,
                      spare: &str,
                      keep: &dyn Fn(&PartitionSim, usize) -> bool| {
        if let Some(t) = choose(
            (0..sim.tables.len())
                .filter(|&t| sim.tables[t].attached && sim.tables[t].name != spare && keep(sim, t))
                .collect(),
        ) {
            let leaf = sim.tables[t].name.clone();
            emit(sim, partition(PartitionAction::Drop { leaf }), out);
        }
    };
    match kind {
        MaintenanceKind::Create { .. } | MaintenanceKind::Middle { .. } => {
            // A new leaf needs a bound no leaf holds: with none free, it
            // replaces a leaf, dropped first.
            let free = match kind {
                MaintenanceKind::Middle { .. } => sim.middle_bounds(),
                _ => sim.free_bounds(),
            };
            if free.is_empty()
                && let Some(op) = maintenance_op(sim, table, pick, MaintenanceKind::Drop)
            {
                emit(sim, op, out);
            }
            let Some(op) = maintenance_op(sim, table, pick, kind) else {
                return;
            };
            let middle = match &op {
                Op::Partition {
                    action: PartitionAction::CreateMiddle { middle, .. },
                    ..
                } => Some(middle.clone()),
                _ => None,
            };
            emit(sim, op, out);
            // A middle's next step, if Postgres created it.
            let (Some(middle), MaintenanceKind::Middle { then, .. }) = (middle, kind) else {
                return;
            };
            let Some(t) = sim.tables.iter().position(|t| t.name == middle) else {
                return;
            };
            match then {
                MiddleThen::Stay => {}
                MiddleThen::Drop => {
                    emit(sim, partition(PartitionAction::Drop { leaf: middle }), out)
                }
                MiddleThen::Detach { concurrently } | MiddleThen::Reattach { concurrently } => {
                    let bound = sim.layout.bound(&sim.tables[t].slots);
                    emit(
                        sim,
                        partition(detach_action(sim, &middle, concurrently)),
                        out,
                    );
                    if matches!(then, MiddleThen::Reattach { .. }) {
                        emit(
                            sim,
                            partition(PartitionAction::Attach {
                                leaf: middle,
                                bound,
                            }),
                            out,
                        );
                    }
                }
            }
        }
        MaintenanceKind::Rebound { concurrently } => {
            let Some(leaf) = detached_or_detach(sim, out, concurrently, &|_, _| true) else {
                return;
            };
            let others = |sim: &PartitionSim| -> Vec<BTreeSet<usize>> {
                let t = sim.table_named(&leaf);
                let free = if sim.tables[t].sub.is_empty() {
                    sim.free_bounds()
                } else {
                    sim.middle_bounds()
                };
                let own = &sim.tables[t].slots;
                free.into_iter().filter(|b| b != own).collect()
            };
            if others(sim).is_empty() {
                drop_other(sim, out, &leaf, &|_, _| true);
            }
            let free = others(sim);
            if free.is_empty() {
                return;
            }
            let bound = free[pick % free.len()].clone();
            let Some(grain) = sim.grain_for(&bound) else {
                return;
            };
            let t = sim.table_named(&leaf);
            let pks: BTreeSet<i64> = sim.tables[t].rows.iter().map(|&(pk, _)| pk).collect();
            let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
            for pk in pks {
                let update = Op::Update {
                    table: table.name.clone(),
                    pk: pk.to_string(),
                    changes: vec![(grain_col.clone(), Some(grain.to_string()))],
                    expect: OpOutcome::Succeeds,
                };
                emit(
                    sim,
                    Op::Leaf {
                        leaf: leaf.clone(),
                        op: Box::new(update),
                    },
                    out,
                );
            }
            let bound = sim.layout.bound(&bound);
            emit(sim, partition(PartitionAction::Attach { leaf, bound }), out);
        }
        MaintenanceKind::Widen { concurrently } => {
            if !matches!(sim.layout.0, PartitionStrategy::List { .. }) {
                return;
            }
            let Some(leaf) =
                detached_or_detach(sim, out, concurrently, &|sim, t| !is_default(sim, t))
            else {
                return;
            };
            let values = |sim: &PartitionSim| -> Vec<usize> {
                let own = &sim.tables[sim.table_named(&leaf)].slots;
                (0..sim.layout.slots())
                    .filter(|&s| {
                        Some(s) != sim.layout.default_slot()
                            && !own.contains(&s)
                            && sim.occupant(s).is_none()
                    })
                    .collect()
            };
            if values(sim).is_empty() {
                drop_other(sim, out, &leaf, &|sim, t| !is_default(sim, t));
            }
            let free = values(sim);
            if free.is_empty() {
                return;
            }
            let t = sim.table_named(&leaf);
            let mut bound = sim.tables[t].slots.clone();
            bound.insert(free[pick % free.len()]);
            let bound = sim.layout.bound(&bound);
            emit(sim, partition(PartitionAction::Attach { leaf, bound }), out);
        }
        MaintenanceKind::Resplit { rows } => {
            if !matches!(sim.layout.0, PartitionStrategy::Hash { .. }) {
                return;
            }
            let Some(t) = choose(
                (0..sim.tables.len())
                    .filter(|&t| sim.tables[t].attached && sim.tables[t].slots.len() > 1)
                    .collect(),
            ) else {
                return;
            };
            let slots = sim.tables[t].slots.clone();
            let leaf = sim.tables[t].name.clone();
            let finest = sim.layout.slots();
            let modulus = sim.layout.hash_modulus(&slots);
            let split = if finest.is_multiple_of(2 * modulus) {
                2 * modulus
            } else {
                finest
            };
            emit(sim, partition(detach_action(sim, &leaf, false)), out);
            let remainder = slots.first().expect("a bound holds a slot") % modulus;
            for r in (0..split).filter(|r| r % modulus == remainder) {
                let part: BTreeSet<usize> =
                    slots.iter().copied().filter(|s| s % split == r).collect();
                if let Some(op) = new_leaf(sim, table, &part, rows, false) {
                    emit(sim, op, out);
                }
            }
        }
        _ => {
            if let Some(op) = maintenance_op(sim, table, pick, kind) {
                emit(sim, op, out);
            }
        }
    }
}

/// A detach of `leaf`, `CONCURRENTLY` when asked and the tree has no
/// `DEFAULT` (Postgres refuses it then).
fn detach_action(sim: &PartitionSim, leaf: &str, concurrently: bool) -> PartitionAction {
    let has_default = sim
        .layout
        .default_slot()
        .and_then(|s| sim.occupant(s))
        .is_some();
    PartitionAction::Detach {
        leaf: leaf.to_string(),
        concurrently: concurrently && !has_default,
    }
}

/// A new leaf over `slots` holding `rows` rows (primary keys `1..=rows`, at
/// the bound's smallest grain), or `None` if no grain fits it.
fn new_leaf(
    sim: &mut PartitionSim,
    table: &Table,
    slots: &BTreeSet<usize>,
    rows: u8,
    standalone: bool,
) -> Option<Op> {
    let grain = sim.grain_for(slots)?;
    sim.created += 1;
    let leaf = format!("{}_n{}", table.name, sim.created);
    let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
    let rows = (1..=i64::from(rows))
        .map(|pk| {
            vec![
                (table.pk_col.clone(), Some(pk.to_string())),
                (table.columns[1].name.clone(), Some(pk.to_string())),
                (grain_col.clone(), Some(grain.to_string())),
            ]
        })
        .collect();
    Some(Op::Partition {
        table: table.name.clone(),
        action: PartitionAction::Create {
            leaf,
            bound: sim.layout.bound(slots),
            rows,
            standalone,
        },
        expect: OpOutcome::Succeeds,
    })
}

/// `op`, a write through the root, named on the table its row lives in (a
/// detached one, if `detached` and one holds it), or for an insert, the leaf
/// it routes to; in a middle, on the middle's leaf that takes the row.
/// Unchanged when there is no such table (an insert that routes nowhere),
/// or the op is a whole-table one.
fn on_leaf(sim: &PartitionSim, op: Op, detached: bool) -> Op {
    let leaf = match &op {
        Op::Insert { row, .. } => {
            let (pk, grain) = sim.key(row);
            let outside = detached
                .then(|| {
                    sim.tables.iter().position(|t| {
                        !t.attached && !t.rows.iter().any(|&(p, g)| p == pk && g == grain)
                    })
                })
                .flatten();
            outside
                .or_else(|| sim.route(grain))
                .map(|t| sim.name_in(t, grain))
        }
        Op::Update { pk, .. } | Op::Delete { pk, .. } => {
            let pk = parse_pk(pk);
            let grain = |t: &SimTable| t.rows.iter().find(|&&(p, _)| p == pk).map(|&(_, g)| g);
            let outside = detached
                .then(|| {
                    sim.tables
                        .iter()
                        .position(|t| !t.attached && grain(t).is_some())
                })
                .flatten();
            outside
                .or_else(|| {
                    sim.tables
                        .iter()
                        .position(|t| t.attached && grain(t).is_some())
                })
                .map(|t| sim.name_in(t, grain(&sim.tables[t]).expect("the table holds the pk")))
                .or_else(|| {
                    sim.tables
                        .iter()
                        .position(|t| t.attached)
                        .map(|t| sim.tables[t].name.clone())
                })
        }
        _ => None,
    };
    match leaf {
        Some(leaf) => Op::Leaf {
            leaf,
            op: Box::new(op),
        },
        None => op,
    }
}

/// A new middle ([`MaintenanceKind::Middle`]) for a free bound, or `None`
/// if none is free or no grain fits it.
fn new_middle(
    sim: &mut PartitionSim,
    table: &Table,
    pick: usize,
    rows: u8,
    leaves: u8,
    default: bool,
    standalone: bool,
) -> Option<Op> {
    let free = sim.middle_bounds();
    let mut slots = (!free.is_empty()).then(|| free[pick % free.len()].clone())?;
    if matches!(sim.layout.0, PartitionStrategy::List { .. }) {
        // A `LIST` bound is one value: up to two more free values above it
        // give the middle's leaves values to split.
        let first = *slots.first().expect("a bound holds a slot");
        for bound in &free {
            let s = *bound.first().expect("a bound holds a slot");
            if slots.len() < 3 && s > first {
                slots.insert(s);
            }
        }
    }
    let grains = sim.grains_for(&slots);
    if grains.is_empty() {
        return None;
    }
    let count = usize::from(leaves).min(grains.len());
    let mut values: Vec<i64> = (0..count)
        .map(|i| grains[(pick + i) % grains.len()])
        .collect();
    values.sort_unstable();
    let default = default || count == 0;
    sim.created += 1;
    let middle = format!("{}_m{}", table.name, sim.created);
    let mut sub: Vec<(String, String)> = values
        .iter()
        .map(|&g| {
            (
                format!("{middle}_{g}"),
                sub_bound(Some(&BTreeSet::from([g]))),
            )
        })
        .collect();
    if default {
        sub.push((format!("{middle}_d"), sub_bound(None)));
    }
    let routed = if default { &grains } else { &values };
    let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
    let rows = (1..=i64::from(rows))
        .map(|pk| {
            let grain = routed[(pk as usize - 1) % routed.len()];
            vec![
                (table.pk_col.clone(), Some(pk.to_string())),
                (table.columns[1].name.clone(), Some(pk.to_string())),
                (grain_col.clone(), Some(grain.to_string())),
            ]
        })
        .collect();
    Some(Op::Partition {
        table: table.name.clone(),
        action: PartitionAction::CreateMiddle {
            middle,
            bound: sim.layout.bound(&slots),
            leaves: sub,
            rows,
            standalone,
        },
        expect: OpOutcome::Succeeds,
    })
}

/// The op a single-op [`PartitionDraw::Maintenance`] becomes in the tree
/// `sim` models, or `None` if the kind has nothing to act on.
fn maintenance_op(
    sim: &mut PartitionSim,
    table: &Table,
    pick: usize,
    kind: MaintenanceKind,
) -> Option<Op> {
    let choose = |candidates: Vec<usize>| -> Option<usize> {
        (!candidates.is_empty()).then(|| candidates[pick % candidates.len()])
    };
    let attached: Vec<usize> = (0..sim.tables.len())
        .filter(|&t| sim.tables[t].attached)
        .collect();
    let partition = |action: PartitionAction| Op::Partition {
        table: table.name.clone(),
        action,
        expect: OpOutcome::Succeeds,
    };
    let op = match kind {
        MaintenanceKind::Detach { concurrently } => {
            let t = choose(attached)?;
            partition(detach_action(sim, &sim.tables[t].name, concurrently))
        }
        MaintenanceKind::Drop => {
            let t = choose(attached)?;
            partition(PartitionAction::Drop {
                leaf: sim.tables[t].name.clone(),
            })
        }
        MaintenanceKind::TruncateLeaf => {
            // A leaf or middle of the tree, or a middle's leaf.
            let names: Vec<String> = attached
                .iter()
                .map(|&t| sim.tables[t].name.clone())
                .chain(
                    attached
                        .iter()
                        .flat_map(|&t| sim.tables[t].sub.iter().map(|l| l.name.clone())),
                )
                .collect();
            let i = choose((0..names.len()).collect())?;
            Op::Leaf {
                leaf: names[i].clone(),
                op: Box::new(Op::Truncate {
                    table: table.name.clone(),
                    expect: OpOutcome::Succeeds,
                }),
            }
        }
        MaintenanceKind::Attach => {
            let t = choose(
                (0..sim.tables.len())
                    .filter(|&t| !sim.tables[t].attached && sim.free(&sim.tables[t].slots))
                    .collect(),
            )?;
            partition(PartitionAction::Attach {
                leaf: sim.tables[t].name.clone(),
                bound: sim.layout.bound(&sim.tables[t].slots),
            })
        }
        MaintenanceKind::Create { rows, standalone } => {
            let free = sim.free_bounds();
            let slots = (!free.is_empty()).then(|| free[pick % free.len()].clone())?;
            return new_leaf(sim, table, &slots, rows, standalone);
        }
        MaintenanceKind::Middle {
            rows,
            leaves,
            default,
            standalone,
            ..
        } => {
            return new_middle(sim, table, pick, rows, leaves, default, standalone);
        }
        MaintenanceKind::Rebound { .. }
        | MaintenanceKind::Widen { .. }
        | MaintenanceKind::Resplit { .. } => {
            unreachable!("maintenance() resolves the compound kinds")
        }
    };
    Some(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{
        AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes, partition_table,
    };

    /// A one-table program partitioned by `strategy`, seeding `grains` (pk
    /// `i + 1` at `grains[i]`), then `mutates`.
    fn program(strategy: PartitionStrategy, grains: &[i64], mutates: Vec<Mutate>) -> Program {
        let mut spec = TableSpec::numeric_only(vec![(Some(1), None); grains.len()], mutates);
        spec.grain_values = grains.iter().map(|g| Some(g.to_string())).collect();
        partition_table(
            build_program_multi_with_shapes(
                &[spec],
                &[(
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Count],
                    },
                )],
            ),
            0,
            strategy,
        )
    }

    fn expects(program: &Program) -> Vec<OpOutcome> {
        program.ops.iter().map(|op| op.expect().clone()).collect()
    }

    fn describe(op: &Op) -> String {
        match op {
            Op::Leaf { leaf, op } => format!("{} on {leaf}", describe(op)),
            Op::Partition { action, .. } => format!("{action:?}"),
            Op::Insert { .. } => "insert".into(),
            Op::Update { .. } => "update".into(),
            Op::Delete { .. } => "delete".into(),
            Op::Truncate { .. } => "truncate".into(),
            Op::BulkInsert { .. } => "bulk insert".into(),
        }
    }

    /// A revival of a row moved to another partition no longer
    /// collides with it (the primary key holds the grain), and a second
    /// partition-key update of that pk then hits both rows and fails.
    #[test]
    fn a_revival_after_a_partition_key_update_follows_the_rows_current_grain() {
        let program = program(
            PartitionStrategy::List {
                values: 3,
                default: false,
            },
            &[0],
            vec![
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(2),
                },
                Mutate::DuplicateInsert {
                    pk: 1,
                    c1: None,
                    c2: None,
                },
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(1),
                },
                Mutate::Delete { pk: 1 },
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(1),
                },
            ],
        );
        assert_eq!(
            expects(&program),
            vec![
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                // Two rows of pk 1 (grains 2 and 0), both set to 1.
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::AffectsNoRows,
            ]
        );
    }

    /// A partition-key update to a value no leaf holds fails without a
    /// `DEFAULT`, and lands in the `DEFAULT` with one.
    #[test]
    fn a_partition_key_update_to_an_unrouted_value_fails_without_a_default() {
        let moves = vec![Mutate::MoveGroup {
            pk: 1,
            grain: Some(3),
        }];
        let without = program(
            PartitionStrategy::List {
                values: 3,
                default: false,
            },
            &[0],
            moves.clone(),
        );
        assert_eq!(expects(&without)[1], OpOutcome::Fails);
        let with = program(
            PartitionStrategy::List {
                values: 2,
                default: true,
            },
            &[0],
            moves,
        );
        assert_eq!(expects(&with)[1], OpOutcome::Succeeds);
    }

    /// Detaching a leaf takes its rows out of the root (a delete through the
    /// root then misses them, and a write that routes to its bound fails),
    /// attaching it again brings them back, and a leaf-named write reaches
    /// only that leaf.
    #[test]
    fn maintenance_follows_the_rows_each_leaf_holds() {
        let base = program(
            PartitionStrategy::Range { bounds: 2 },
            &[0, 1],
            vec![
                Mutate::Delete { pk: 1 },
                Mutate::DuplicateInsert {
                    pk: 1,
                    c1: None,
                    c2: None,
                },
                Mutate::Delete { pk: 2 },
            ],
        );
        // ops: insert pk1 (g0), insert pk2 (g1), delete 1, revive 1, delete 2.
        let program = add_partition_maintenance(
            base,
            0,
            &[
                // Detach the leaf holding grain 0 before `delete 1`.
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 0,
                    kind: MaintenanceKind::Detach { concurrently: true },
                },
                // Attach it again before `delete 2`.
                PartitionDraw::Maintenance {
                    at: 4,
                    pick: 0,
                    kind: MaintenanceKind::Attach,
                },
                // `delete 2` named on its leaf.
                PartitionDraw::ViaLeaf {
                    at: 4,
                    detached: false,
                },
                // Truncate the leaf of grain 0 at the end.
                PartitionDraw::Maintenance {
                    at: 5,
                    pick: 0,
                    kind: MaintenanceKind::TruncateLeaf,
                },
            ],
        );
        let shape: Vec<(String, OpOutcome)> = program
            .ops
            .iter()
            .map(|op| (describe(op), op.expect().clone()))
            .collect();
        let detach = format!(
            "{:?}",
            PartitionAction::Detach {
                leaf: "t0_p0".into(),
                concurrently: true
            }
        );
        let attach = format!(
            "{:?}",
            PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values from (minvalue) to (1)".into()
            }
        );
        assert_eq!(
            shape,
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (detach, OpOutcome::Succeeds),
                // pk 1 left with the detached leaf.
                ("delete".into(), OpOutcome::AffectsNoRows),
                // Grain 0 routes to no leaf.
                ("insert".into(), OpOutcome::Fails),
                (attach, OpOutcome::Succeeds),
                ("delete on t0_p1".into(), OpOutcome::Succeeds),
                ("truncate on t0_p0".into(), OpOutcome::Succeeds),
            ]
        );
    }

    /// A new leaf beside a `DEFAULT` holding rows of its bound is refused,
    /// and a leaf-named partition-key update to a value outside the leaf's
    /// bound fails.
    #[test]
    fn a_default_holding_a_bounds_rows_refuses_a_leaf_for_it() {
        let base = program(
            PartitionStrategy::List {
                values: 2,
                default: true,
            },
            &[1, 0],
            vec![Mutate::Update {
                pk: 1,
                c1: None,
                c2: None,
            }],
        );
        // ops: insert pk1 (g1), insert pk2 (g0), update 1.
        let program = add_partition_maintenance(
            base,
            0,
            &[
                // Drop `t0_p1` (pick 1 of p0, p1, pd), with pk 1.
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 1,
                    kind: MaintenanceKind::Drop,
                },
                // Grain 1 now routes to the DEFAULT.
                PartitionDraw::MoveRow {
                    at: 2,
                    pk: 2,
                    grain: 1,
                    via_leaf: false,
                },
                // A new leaf for grain 1 (the only free bound).
                PartitionDraw::Maintenance {
                    at: 3,
                    pick: 0,
                    kind: MaintenanceKind::Create {
                        rows: 1,
                        standalone: false,
                    },
                },
                // Named on the DEFAULT, where grain 0 doesn't belong.
                PartitionDraw::MoveRow {
                    at: 3,
                    pk: 2,
                    grain: 0,
                    via_leaf: true,
                },
            ],
        );
        let shape: Vec<(String, OpOutcome)> = program
            .ops
            .iter()
            .map(|op| (describe(op), op.expect().clone()))
            .collect();
        let drop = format!(
            "{:?}",
            PartitionAction::Drop {
                leaf: "t0_p1".into()
            }
        );
        let create = format!(
            "{:?}",
            PartitionAction::Create {
                leaf: "t0_n1".into(),
                bound: "for values in (1)".into(),
                rows: vec![vec![
                    ("c0".into(), Some("1".into())),
                    ("c1".into(), Some("1".into())),
                    ("c6".into(), Some("1".into())),
                ]],
                standalone: false,
            }
        );
        assert_eq!(
            shape,
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop, OpOutcome::Succeeds),
                ("update".into(), OpOutcome::Succeeds),
                ("update".into(), OpOutcome::AffectsNoRows),
                (create, OpOutcome::Fails),
                ("update on t0_pd".into(), OpOutcome::Fails),
            ]
        );
    }

    fn shape(program: &Program) -> Vec<(String, OpOutcome)> {
        program
            .ops
            .iter()
            .map(|op| (describe(op), op.expect().clone()))
            .collect()
    }

    /// A detached leaf attached under another free bound takes its rows
    /// along once they are moved into it; after a concurrent detach its
    /// `CHECK` constraint refuses the move, so the attach fails, and an
    /// empty one attaches but refuses the new bound's rows.
    #[test]
    fn a_leaf_attached_under_another_bound_keeps_its_check() {
        let list = PartitionStrategy::List {
            values: 3,
            default: false,
        };
        let draws = |concurrently| {
            vec![
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 2,
                    kind: MaintenanceKind::Drop,
                },
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 0,
                    kind: MaintenanceKind::Rebound { concurrently },
                },
                PartitionDraw::MoveRow {
                    at: 2,
                    pk: 2,
                    grain: 2,
                    via_leaf: false,
                },
            ]
        };
        let drop = format!(
            "{:?}",
            PartitionAction::Drop {
                leaf: "t0_p2".into()
            }
        );
        let detach = |concurrently| {
            format!(
                "{:?}",
                PartitionAction::Detach {
                    leaf: "t0_p0".into(),
                    concurrently
                }
            )
        };
        let attach = format!(
            "{:?}",
            PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values in (2)".into()
            }
        );
        // ops: insert pk1 (g0), insert pk2 (g1).
        let plain = add_partition_maintenance(program(list, &[0, 1], vec![]), 0, &draws(false));
        assert_eq!(
            shape(&plain),
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop.clone(), OpOutcome::Succeeds),
                (detach(false), OpOutcome::Succeeds),
                ("update on t0_p0".into(), OpOutcome::Succeeds),
                (attach.clone(), OpOutcome::Succeeds),
                // Grain 2 now routes to t0_p0.
                ("update".into(), OpOutcome::Succeeds),
            ]
        );
        let checked = add_partition_maintenance(program(list, &[0, 1], vec![]), 0, &draws(true));
        assert_eq!(
            shape(&checked),
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop.clone(), OpOutcome::Succeeds),
                (detach(true), OpOutcome::Succeeds),
                // The CHECK holds the row at grain 0, outside the new bound.
                ("update on t0_p0".into(), OpOutcome::Fails),
                (attach.clone(), OpOutcome::Fails),
                ("update".into(), OpOutcome::Fails),
            ]
        );
        // ops: insert pk1 (g1), insert pk2 (g1); t0_p0 is empty.
        let empty = add_partition_maintenance(program(list, &[1, 1], vec![]), 0, &draws(true));
        assert_eq!(
            shape(&empty),
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop, OpOutcome::Succeeds),
                (detach(true), OpOutcome::Succeeds),
                (attach, OpOutcome::Succeeds),
                // Grain 2 routes to t0_p0, whose CHECK refuses it.
                ("update".into(), OpOutcome::Fails),
            ]
        );
    }

    /// A `LIST` leaf detached and attached again with a value list widened
    /// by a free value takes that value's rows.
    #[test]
    fn a_widened_list_leaf_takes_the_new_values_rows() {
        let base = program(
            PartitionStrategy::List {
                values: 3,
                default: true,
            },
            &[0, 1],
            vec![],
        );
        let program = add_partition_maintenance(
            base,
            0,
            &[
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 2,
                    kind: MaintenanceKind::Drop,
                },
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 0,
                    kind: MaintenanceKind::Widen { concurrently: true },
                },
                PartitionDraw::MoveRow {
                    at: 2,
                    pk: 2,
                    grain: 2,
                    via_leaf: false,
                },
            ],
        );
        let detach = format!(
            "{:?}",
            PartitionAction::Detach {
                leaf: "t0_p0".into(),
                // Refused beside a DEFAULT, so drawn plain.
                concurrently: false,
            }
        );
        let attach = format!(
            "{:?}",
            PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values in (0, 2)".into()
            }
        );
        let drop = format!(
            "{:?}",
            PartitionAction::Drop {
                leaf: "t0_p2".into()
            }
        );
        assert_eq!(
            shape(&program),
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop, OpOutcome::Succeeds),
                (detach, OpOutcome::Succeeds),
                (attach, OpOutcome::Succeeds),
                ("update".into(), OpOutcome::Succeeds),
            ]
        );
        assert_eq!(tree_change_names(&program), vec!["attach_widened"]);
        assert_eq!(
            Layout(PartitionStrategy::List {
                values: 3,
                default: true
            })
            .slots_of_bound("for values in (0, 2)"),
            BTreeSet::from([0, 2])
        );
    }

    /// Widening a `LIST` leaf's values by one whose rows the `DEFAULT` holds
    /// is refused, as Postgres refuses it ("updated partition constraint for
    /// default partition ... would be violated").
    #[test]
    fn a_widening_the_default_holds_rows_of_is_refused() {
        let base = program(
            PartitionStrategy::List {
                values: 3,
                default: true,
            },
            &[0, 1],
            vec![],
        );
        let program = add_partition_maintenance(
            base,
            0,
            &[
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 2,
                    kind: MaintenanceKind::Drop,
                },
                // Grain 2 now routes to the DEFAULT.
                PartitionDraw::MoveRow {
                    at: 2,
                    pk: 2,
                    grain: 2,
                    via_leaf: false,
                },
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 0,
                    kind: MaintenanceKind::Widen {
                        concurrently: false,
                    },
                },
            ],
        );
        let attach = format!(
            "{:?}",
            PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values in (0, 2)".into()
            }
        );
        assert_eq!(shape(&program).last(), Some(&(attach, OpOutcome::Fails)));
        assert!(tree_change_names(&program).is_empty());
    }

    /// A `HASH` table attached at a coarser modulus than its own counts as a
    /// rebound, and only a `LIST` value list grown in place as widened.
    #[test]
    fn a_coarser_hash_attach_counts_as_a_rebound_not_a_widening() {
        let base = program(PartitionStrategy::Hash { modulus: 4 }, &[], vec![]);
        let program = add_partition_maintenance(
            base,
            0,
            &[
                // Detach `t0_p0` (4, 0), then drop `t0_p2` (4, 2).
                PartitionDraw::Maintenance {
                    at: 0,
                    pick: 0,
                    kind: MaintenanceKind::Detach {
                        concurrently: false,
                    },
                },
                PartitionDraw::Maintenance {
                    at: 0,
                    pick: 1,
                    kind: MaintenanceKind::Drop,
                },
                // The coarsest free bound other than its own: (2, 0).
                PartitionDraw::Maintenance {
                    at: 0,
                    pick: 0,
                    kind: MaintenanceKind::Rebound {
                        concurrently: false,
                    },
                },
            ],
        );
        assert!(
            program.ops.iter().any(|op| matches!(
                op,
                Op::Partition {
                    action: PartitionAction::Attach { leaf, bound },
                    expect: OpOutcome::Succeeds,
                    ..
                } if leaf == "t0_p0" && bound == "for values with (modulus 2, remainder 0)"
            )),
            "{:#?}",
            program.ops
        );
        assert_eq!(tree_change_names(&program), vec!["attach_rebound"]);
    }

    /// A `HASH` leaf re-split to a multiple modulus is replaced by a leaf per
    /// remainder it covered, and rows route to them.
    #[test]
    fn a_hash_resplit_replaces_a_leaf_by_its_remainders_at_a_multiple_modulus() {
        let base = program(PartitionStrategy::Hash { modulus: 2 }, &[0], vec![]);
        let program = add_partition_maintenance(
            base,
            0,
            &[PartitionDraw::Maintenance {
                at: 1,
                pick: 0,
                kind: MaintenanceKind::Resplit { rows: 1 },
            }],
        );
        let bounds: Vec<String> = program
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Partition {
                    action: PartitionAction::Create { bound, .. },
                    expect: OpOutcome::Succeeds,
                    ..
                } => Some(bound.clone()),
                _ => None,
            })
            .collect();
        // Grain 0 hashes to remainder 0 of modulus 2: leaf t0_p0.
        assert_eq!(
            bounds,
            vec![
                "for values with (modulus 4, remainder 0)".to_string(),
                "for values with (modulus 4, remainder 2)".to_string(),
            ]
        );
        assert!(matches!(
            &program.ops[1],
            Op::Partition {
                action: PartitionAction::Detach { leaf, .. },
                ..
            } if leaf == "t0_p0"
        ));
    }

    /// A full row of `table` with primary-key column `pk` at `grain`.
    fn row(table: &Table, pk: i64, grain: i64) -> Vec<(String, Option<String>)> {
        let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
        vec![
            (table.pk_col.clone(), Some(pk.to_string())),
            (table.columns[1].name.clone(), Some(pk.to_string())),
            (grain_col.clone(), Some(grain.to_string())),
        ]
    }

    /// The outcomes `ops` get, applied in order to a fresh model of `table`.
    fn outcomes(table: &Table, ops: &[Op]) -> Vec<OpOutcome> {
        let mut sim = PartitionSim::new(table);
        ops.iter().map(|op| sim.apply(op)).collect()
    }

    fn ddl(table: &Table, action: PartitionAction) -> Op {
        Op::Partition {
            table: table.name.clone(),
            action,
            expect: OpOutcome::Succeeds,
        }
    }

    fn drop_leaf(table: &Table, leaf: &str) -> Op {
        ddl(table, PartitionAction::Drop { leaf: leaf.into() })
    }

    fn insert(table: &Table, pk: i64, grain: i64) -> Op {
        Op::Insert {
            table: table.name.clone(),
            row: row(table, pk, grain),
            expect: OpOutcome::Succeeds,
        }
    }

    fn move_row(table: &Table, pk: i64, grain: i64) -> Op {
        let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
        Op::Update {
            table: table.name.clone(),
            pk: pk.to_string(),
            changes: vec![(grain_col.clone(), Some(grain.to_string()))],
            expect: OpOutcome::Succeeds,
        }
    }

    fn named(leaf: &str, op: Op) -> Op {
        Op::Leaf {
            leaf: leaf.into(),
            op: Box::new(op),
        }
    }

    /// A middle routes its rows to its own leaves, as Postgres 17 does
    /// against a scratch database: a row its value lists miss lands in its
    /// `DEFAULT`, a write named on one of its leaves must stay in that
    /// leaf, a row moved through the root moves between its leaves, and a
    /// concurrent detach leaves a `CHECK` on it (and its leaves) that
    /// refuses a row outside its bound.
    #[test]
    fn a_middle_routes_its_rows_to_its_own_leaves() {
        let program = program(
            PartitionStrategy::List {
                values: 5,
                default: false,
            },
            &[],
            vec![],
        );
        let t = &program.tables[0];
        let ops = vec![
            drop_leaf(t, "t0_p1"),
            drop_leaf(t, "t0_p2"),
            ddl(
                t,
                PartitionAction::CreateMiddle {
                    middle: "t0_m1".into(),
                    bound: "for values in (1, 2)".into(),
                    leaves: vec![
                        ("t0_m1_1".into(), "for values in (1)".into()),
                        ("t0_m1_d".into(), "default".into()),
                    ],
                    rows: vec![row(t, 1, 1), row(t, 2, 2)],
                    standalone: true,
                },
            ),
            insert(t, 3, 2),
            named("t0_m1_1", insert(t, 4, 2)),
            move_row(t, 1, 2),
            named("t0_m1_d", move_row(t, 3, 0)),
            named("t0_m1", move_row(t, 3, 1)),
            ddl(
                t,
                PartitionAction::Detach {
                    leaf: "t0_m1".into(),
                    concurrently: true,
                },
            ),
            named("t0_m1_d", insert(t, 5, 3)),
            drop_leaf(t, "t0_p3"),
            // The middle's rows are at grains 1 and 2.
            ddl(
                t,
                PartitionAction::Attach {
                    leaf: "t0_m1".into(),
                    bound: "for values in (3)".into(),
                },
            ),
            ddl(
                t,
                PartitionAction::Attach {
                    leaf: "t0_m1".into(),
                    bound: "for values in (1, 2)".into(),
                },
            ),
            named(
                "t0_m1_1",
                Op::Truncate {
                    table: t.name.clone(),
                    expect: OpOutcome::Succeeds,
                },
            ),
            // Pk 3 left `t0_m1_1` with its truncate.
            move_row(t, 3, 2),
            move_row(t, 2, 1),
        ];
        assert_eq!(
            outcomes(t, &ops),
            vec![
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::AffectsNoRows,
                OpOutcome::Succeeds,
            ]
        );
    }

    /// A middle with no `DEFAULT` refuses a row none of its leaves takes,
    /// through the root or named on it, and refuses to be created with one.
    #[test]
    fn a_middle_without_a_default_refuses_a_grain_none_of_its_leaves_takes() {
        let program = program(PartitionStrategy::Hash { modulus: 1 }, &[], vec![]);
        let t = &program.tables[0];
        let middle = |rows| {
            ddl(
                t,
                PartitionAction::CreateMiddle {
                    middle: "t0_m1".into(),
                    bound: "for values with (modulus 1, remainder 0)".into(),
                    leaves: vec![("t0_m1_0".into(), "for values in (0)".into())],
                    rows,
                    standalone: false,
                },
            )
        };
        let ops = vec![
            drop_leaf(t, "t0_p0"),
            middle(vec![row(t, 1, 0), row(t, 2, 1)]),
            middle(vec![row(t, 1, 0)]),
            insert(t, 2, 0),
            insert(t, 3, 1),
            named("t0_m1", insert(t, 3, 1)),
            move_row(t, 2, 1),
        ];
        assert_eq!(
            outcomes(t, &ops),
            vec![
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Fails,
                OpOutcome::Fails,
            ]
        );
    }

    /// A partitioned `DEFAULT` holding a bound's rows in one of its own
    /// leaves refuses a new leaf for that bound, as a plain one does.
    #[test]
    fn a_partitioned_default_holding_a_bounds_rows_refuses_a_leaf_for_it() {
        let program = program(
            PartitionStrategy::List {
                values: 2,
                default: true,
            },
            &[],
            vec![],
        );
        let t = &program.tables[0];
        let leaf = |name: &str| {
            ddl(
                t,
                PartitionAction::Create {
                    leaf: name.into(),
                    bound: "for values in (1)".into(),
                    rows: vec![],
                    standalone: false,
                },
            )
        };
        let ops = vec![
            drop_leaf(t, "t0_pd"),
            drop_leaf(t, "t0_p1"),
            ddl(
                t,
                PartitionAction::CreateMiddle {
                    middle: "t0_m1".into(),
                    bound: "default".into(),
                    leaves: vec![
                        ("t0_m1_5".into(), "for values in (5)".into()),
                        ("t0_m1_d".into(), "default".into()),
                    ],
                    rows: vec![row(t, 1, 5), row(t, 2, 6)],
                    standalone: false,
                },
            ),
            insert(t, 3, 1),
            leaf("t0_n1"),
            named(
                "t0_m1_d",
                Op::Delete {
                    table: t.name.clone(),
                    pk: "3".into(),
                    expect: OpOutcome::Succeeds,
                },
            ),
            leaf("t0_n2"),
            insert(t, 3, 1),
        ];
        assert_eq!(
            outcomes(t, &ops),
            vec![
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
            ]
        );
    }

    /// A [`MaintenanceKind::Middle`] draw over a tree with no free bound
    /// drops a leaf, creates a middle with a leaf per drawn grain and its
    /// rows spread over them, and takes its next step at once; a write
    /// named on a leaf then names the middle's leaf its row routes to.
    #[test]
    fn a_middle_draw_creates_a_middle_and_takes_its_next_step() {
        let range = PartitionStrategy::Range { bounds: 1 };
        // ops: insert pk1 (g0), insert pk2 (g1).
        let program = add_partition_maintenance(
            program(range, &[0, 1], vec![]),
            0,
            &[
                PartitionDraw::Maintenance {
                    at: 1,
                    pick: 0,
                    kind: MaintenanceKind::Middle {
                        rows: 1,
                        leaves: 2,
                        default: false,
                        standalone: false,
                        then: MiddleThen::Reattach { concurrently: true },
                    },
                },
                PartitionDraw::ViaLeaf {
                    at: 1,
                    detached: false,
                },
            ],
        );
        let bound = "for values from (minvalue) to (maxvalue)".to_string();
        assert_eq!(
            shape(&program),
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                (
                    format!(
                        "{:?}",
                        PartitionAction::Drop {
                            leaf: "t0_p0".into()
                        }
                    ),
                    OpOutcome::Succeeds
                ),
                (
                    format!(
                        "{:?}",
                        PartitionAction::CreateMiddle {
                            middle: "t0_m1".into(),
                            bound: bound.clone(),
                            leaves: vec![
                                ("t0_m1_0".into(), "for values in (0)".into()),
                                ("t0_m1_1".into(), "for values in (1)".into()),
                            ],
                            rows: vec![row(&program.tables[0], 1, 0)],
                            standalone: false,
                        }
                    ),
                    OpOutcome::Succeeds
                ),
                (
                    format!(
                        "{:?}",
                        PartitionAction::Detach {
                            leaf: "t0_m1".into(),
                            concurrently: true,
                        }
                    ),
                    OpOutcome::Succeeds
                ),
                (
                    format!(
                        "{:?}",
                        PartitionAction::Attach {
                            leaf: "t0_m1".into(),
                            bound,
                        }
                    ),
                    OpOutcome::Succeeds
                ),
                ("insert on t0_m1_1".into(), OpOutcome::Succeeds),
            ]
        );
        assert_eq!(
            tree_change_names(&program),
            vec!["detach_middle", "attach_middle", "middle_leaf_write"]
        );
    }

    /// A middle never takes the root's `DEFAULT` slot (#2106): with only
    /// the `DEFAULT` free, a `Middle` draw drops a leaf and takes its bound.
    #[test]
    fn a_middle_never_takes_the_default() {
        let list = PartitionStrategy::List {
            values: 2,
            default: true,
        };
        let middle = |then| PartitionDraw::Maintenance {
            at: 0,
            pick: 0,
            kind: MaintenanceKind::Middle {
                rows: 1,
                leaves: 1,
                default: true,
                standalone: false,
                then,
            },
        };
        let program = add_partition_maintenance(
            program(list, &[], vec![]),
            0,
            &[
                PartitionDraw::Maintenance {
                    at: 0,
                    pick: 2,
                    kind: MaintenanceKind::Drop,
                },
                middle(MiddleThen::Detach {
                    concurrently: false,
                }),
                // Only the `DEFAULT` and t0_m1's bound are free: the rebound
                // of the detached middle drops a leaf for another bound.
                PartitionDraw::Maintenance {
                    at: 0,
                    pick: 0,
                    kind: MaintenanceKind::Rebound {
                        concurrently: false,
                    },
                },
            ],
        );
        let bounds: Vec<&str> = program
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Partition {
                    action:
                        PartitionAction::CreateMiddle { bound, .. }
                        | PartitionAction::Attach { bound, .. },
                    ..
                } => Some(bound.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            bounds,
            vec!["for values in (0)", "for values in (1)"],
            "{:#?}",
            program.ops
        );
    }
}
