//! The isolation draw (#2185, step 6a of #2180): a definition made to fail
//! only in combination, beside definitions that must keep converging.
//!
//! The held-key tier's programs ([`super::held_key_program`]) already draw
//! rows that fail alone (a plain 1-1 field that divides by zero). This adds
//! a table `iso` read by two definitions: `iso_d`, which one drawn op makes
//! fail, and `iso_s`, its sibling, which reads the same rows and never
//! fails. That op is a two-row `INSERT` (one statement, so one page), and
//! each of its rows applies alone:
//!
//! - [`FailKind::FloatSum`]: two `1e308` rows of one group of
//!   `SUM(double precision)`, whose sum overflows (`22003`);
//! - [`FailKind::IntervalInfinities`]: `'infinity'` and `'-infinity'` in one
//!   group of `SUM(interval)` (`22008`), on Postgres 17 and later;
//! - [`FailKind::UniqueTarget`]: a plain 1-1 copy whose target has a unique
//!   index the two rows share a value of (`23505`);
//! - [`FailKind::TriggerTarget`]: the same copy with a trigger on its target
//!   that refuses a second row of a value (`P0001`).
//!
//! The engine pauses `iso_d` with T0247 and commits the page for everyone
//! else (`docs/staging-and-claiming/06-cleanup-and-reclaim.md`, "Isolate
//! before blaming"). Every other value the table takes is small and every
//! copied value fresh, so nothing else fails: no row of the table fails
//! alone, before the pair or after it.

use trellis::dev::defs::FloatWidth;
use trellis::dev::defs::PgType;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use crate::model::{Column, Op, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table};

/// The isolation table.
pub const TABLE: &str = "iso";
/// The definition made to fail.
pub const FAILING: &str = "iso_d";
/// Its sibling over the same table, which never fails.
pub const SIBLING: &str = "iso_s";

/// How the drawn pair fails `iso_d`'s apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailKind {
    FloatSum,
    IntervalInfinities,
    UniqueTarget,
    TriggerTarget,
}

impl FailKind {
    pub const ALL: [FailKind; 4] = [
        FailKind::FloatSum,
        FailKind::IntervalInfinities,
        FailKind::UniqueTarget,
        FailKind::TriggerTarget,
    ];

    /// The SQLSTATE the pair's page fails with.
    pub fn sqlstate(self) -> &'static str {
        match self {
            FailKind::FloatSum => "22003",
            FailKind::IntervalInfinities => "22008",
            FailKind::UniqueTarget => "23505",
            FailKind::TriggerTarget => "P0001",
        }
    }

    /// What sets up the target-side refusal, run once the definitions are
    /// installed, before the first op, given `iso_d`'s qualified target:
    /// the unique index or the trigger. Nothing for an aggregate kind.
    pub fn target_ddl(self, target: &str) -> Vec<String> {
        match self {
            FailKind::FloatSum | FailKind::IntervalInfinities => Vec::new(),
            FailKind::UniqueTarget => {
                vec![format!(
                    "create unique index iso_d_n_unique on {target} (n)"
                )]
            }
            FailKind::TriggerTarget => vec![
                format!(
                    "create function iso_d_refuse() returns trigger language plpgsql as $$ \
                     begin \
                       if (select count(*) from {target} where n = new.n) > 1 then \
                         raise exception 'iso_d refuses a second row of n = %', new.n; \
                       end if; \
                       return null; \
                     end $$"
                ),
                format!(
                    "create trigger iso_d_refuse after insert or update on {target} \
                     for each row execute function iso_d_refuse()"
                ),
            ],
        }
    }

    /// Whether `iso_d` is an aggregate (its target groups by `iso_g`).
    fn aggregate(self) -> bool {
        matches!(self, FailKind::FloatSum | FailKind::IntervalInfinities)
    }
}

/// One write to `iso` before or after the pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsoWrite {
    /// A new row.
    Insert,
    /// An update of the `n`th live row (wrapping), to new small values.
    Update(usize),
    /// A delete of the `n`th live row (wrapping).
    Delete(usize),
}

/// What the strategy draws for the isolation table; [`with_isolation`]
/// builds it into a program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsolationParams {
    pub kind: FailKind,
    /// Whether the sibling is an aggregate (`COUNT(*)` by `iso_g`) rather
    /// than a plain 1-1 copy of `iso_g`.
    pub sibling_aggregate: bool,
    /// Rows seeded before the pair, at least one.
    pub seeds: usize,
    pub before: Vec<IsoWrite>,
    pub after: Vec<IsoWrite>,
    /// The group the pair writes (`1..=3`).
    pub pair_group: u8,
    /// Small-value picks, consumed in order and wrapped.
    pub picks: Vec<u8>,
    /// Where the `iso` ops go among the program's own: the `i`th one goes
    /// before the program op at `slots[i]` (clamped), in order.
    pub slots: Vec<usize>,
}

/// The drawn isolation: which op is the pair and how it fails.
#[derive(Debug, Clone, PartialEq)]
pub struct IsolationDraw {
    pub kind: FailKind,
    /// Index into `program.ops` of the pair's two-row `INSERT`.
    pub pair_op: usize,
    /// The pair's primary keys, which the T0247 reason names.
    pub pair_keys: [i64; 2],
}

/// A held-key program with the isolation table.
#[derive(Debug, Clone, PartialEq)]
pub struct IsolationCase {
    pub program: Program,
    pub draw: IsolationDraw,
}

const FLOATS: [&str; 5] = ["1.5", "-2.25", "100", "0", "0.1"];
const INTERVALS: [&str; 5] = ["1 day", "-2 hours", "3 mons", "0", "1 year -5 mins"];

/// Builds `params` into `program`: the `iso` table, `iso_d` and `iso_s`
/// appended after its definitions, and the table's ops spliced among its
/// own. [`FailKind::IntervalInfinities`] falls back to
/// [`FailKind::FloatSum`] where Postgres has no infinite `interval`
/// ([`super::exact_sums::interval_infinities`]).
pub fn with_isolation(mut program: Program, params: &IsolationParams) -> IsolationCase {
    let kind = match params.kind {
        FailKind::IntervalInfinities if !super::exact_sums::interval_infinities() => {
            FailKind::FloatSum
        }
        kind => kind,
    };
    let column = |name: &str, value_type| Column {
        name: name.to_string(),
        value_type,
    };
    program.tables.push(Table {
        name: TABLE.to_string(),
        pk_col: "iso_id".to_string(),
        columns: vec![
            column("iso_id", PRIMARY_KEY_VALUE_TYPE),
            column("iso_g", ValueType::Numeric),
            column("iso_f", ValueType::Float(FloatWidth::Float8)),
            column("iso_iv", ValueType::Other(PgType::Interval)),
            column("iso_n", ValueType::Numeric),
        ],
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
    });
    let failing = match kind {
        FailKind::FloatSum => {
            format!(
                "TRANSFORM {FAILING} FROM {TABLE} GROUP BY iso_g SELECT iso_g AS iso_g, SUM(iso_f) AS sf"
            )
        }
        FailKind::IntervalInfinities => format!(
            "TRANSFORM {FAILING} FROM {TABLE} GROUP BY iso_g SELECT iso_g AS iso_g, SUM(iso_iv) AS siv"
        ),
        FailKind::UniqueTarget | FailKind::TriggerTarget => {
            format!("TRANSFORM {FAILING} FROM {TABLE} SELECT iso_n AS n")
        }
    };
    let sibling = if params.sibling_aggregate {
        format!(
            "TRANSFORM {SIBLING} FROM {TABLE} GROUP BY iso_g SELECT iso_g AS iso_g, COUNT(*) AS cnt"
        )
    } else {
        format!("TRANSFORM {SIBLING} FROM {TABLE} SELECT iso_g AS iso_g, iso_f AS f")
    };
    for text in [failing, sibling] {
        program.defs.push(parse(&text));
    }
    if program.def_install_after_op.len() + 2 == program.defs.len() {
        program.def_install_after_op.extend([0, 0]);
    }

    let mut writer = Writer {
        picks: &params.picks,
        next_pick: 0,
        next_pk: 1,
        next_n: 1_000,
        live: Vec::new(),
        ops: Vec::new(),
    };
    for _ in 0..params.seeds.max(1) {
        writer.write(IsoWrite::Insert);
    }
    for write in &params.before {
        writer.write(*write);
    }
    let pair_keys = [writer.next_pk, writer.next_pk + 1];
    writer.next_pk += 2;
    let group = params.pair_group.clamp(1, 3).to_string();
    let shared_n = writer.fresh_n();
    let pair_rows: Vec<Vec<(String, Option<String>)>> = (0..2)
        .map(|i| {
            let (f, iv) = match (kind, i) {
                (FailKind::FloatSum, _) => ("1e308".to_string(), writer.interval()),
                (FailKind::IntervalInfinities, 0) => (writer.float(), "infinity".to_string()),
                (FailKind::IntervalInfinities, _) => (writer.float(), "-infinity".to_string()),
                _ => (writer.float(), writer.interval()),
            };
            let n = if kind.aggregate() {
                writer.fresh_n()
            } else {
                shared_n.clone()
            };
            vec![
                ("iso_id".to_string(), Some(pair_keys[i].to_string())),
                ("iso_g".to_string(), Some(group.clone())),
                ("iso_f".to_string(), Some(f)),
                ("iso_iv".to_string(), Some(iv)),
                ("iso_n".to_string(), Some(n)),
            ]
        })
        .collect();
    let pair_index_in_iso = writer.ops.len();
    writer.ops.push(Op::BulkInsert {
        table: TABLE.to_string(),
        rows: pair_rows,
        expect: OpOutcome::Succeeds,
    });
    for write in &params.after {
        writer.write(*write);
    }

    // Splice: the `i`th iso op goes before program op `slots[i]`, in order.
    let iso_ops = writer.ops;
    let mut slots: Vec<usize> = (0..iso_ops.len())
        .map(|i| params.slots.get(i).copied().unwrap_or(usize::MAX))
        .collect();
    slots.sort_unstable();
    let own = std::mem::take(&mut program.ops);
    let mut merged = Vec::with_capacity(own.len() + iso_ops.len());
    let mut iso = iso_ops.into_iter().zip(slots).enumerate().peekable();
    let mut pair_op = 0;
    for (index, op) in own.into_iter().enumerate() {
        while let Some((i, (iso_op, _))) = iso.next_if(|(_, (_, slot))| *slot <= index) {
            if i == pair_index_in_iso {
                pair_op = merged.len();
            }
            merged.push(iso_op);
        }
        merged.push(op);
    }
    for (i, (iso_op, _)) in iso {
        if i == pair_index_in_iso {
            pair_op = merged.len();
        }
        merged.push(iso_op);
    }
    program.ops = merged;

    IsolationCase {
        program,
        draw: IsolationDraw {
            kind,
            pair_op,
            pair_keys,
        },
    }
}

fn parse(text: &str) -> TransformDef {
    trellis::dev::defs::parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

/// Writes `iso`'s rows: every value small, every `iso_n` fresh, so no row
/// fails alone and none collides with another's copied value.
struct Writer<'a> {
    picks: &'a [u8],
    next_pick: usize,
    next_pk: i64,
    next_n: i64,
    live: Vec<i64>,
    ops: Vec<Op>,
}

impl Writer<'_> {
    fn pick(&mut self) -> usize {
        let pick = if self.picks.is_empty() {
            self.next_pick
        } else {
            self.picks[self.next_pick % self.picks.len()] as usize
        };
        self.next_pick += 1;
        pick
    }

    fn float(&mut self) -> String {
        FLOATS[self.pick() % FLOATS.len()].to_string()
    }

    fn interval(&mut self) -> String {
        INTERVALS[self.pick() % INTERVALS.len()].to_string()
    }

    fn group(&mut self) -> String {
        (self.pick() % 3 + 1).to_string()
    }

    fn fresh_n(&mut self) -> String {
        self.next_n += 1;
        self.next_n.to_string()
    }

    fn values(&mut self) -> Vec<(String, Option<String>)> {
        vec![
            ("iso_g".to_string(), Some(self.group())),
            ("iso_f".to_string(), Some(self.float())),
            ("iso_iv".to_string(), Some(self.interval())),
            ("iso_n".to_string(), Some(self.fresh_n())),
        ]
    }

    fn write(&mut self, write: IsoWrite) {
        match write {
            IsoWrite::Update(n) | IsoWrite::Delete(n) if !self.live.is_empty() => {
                let pk = self.live[n % self.live.len()];
                if let IsoWrite::Delete(_) = write {
                    self.live.retain(|live| *live != pk);
                    self.ops.push(Op::Delete {
                        table: TABLE.to_string(),
                        pk: pk.to_string(),
                        expect: OpOutcome::Succeeds,
                    });
                } else {
                    let changes = self.values();
                    self.ops.push(Op::Update {
                        table: TABLE.to_string(),
                        pk: pk.to_string(),
                        changes,
                        expect: OpOutcome::Succeeds,
                    });
                }
            }
            _ => {
                let pk = self.next_pk;
                self.next_pk += 1;
                self.live.push(pk);
                let mut row = vec![("iso_id".to_string(), Some(pk.to_string()))];
                row.extend(self.values());
                self.ops.push(Op::Insert {
                    table: TABLE.to_string(),
                    row,
                    expect: OpOutcome::Succeeds,
                });
            }
        }
    }
}

#[cfg(feature = "proptest")]
pub use strategy::isolation_program;

#[cfg(feature = "proptest")]
mod strategy {
    use proptest::prelude::*;

    use super::{FailKind, IsoWrite, IsolationCase, IsolationParams, with_isolation};

    fn iso_write() -> impl Strategy<Value = IsoWrite> {
        prop_oneof![
            2 => Just(IsoWrite::Insert),
            2 => (0..8usize).prop_map(IsoWrite::Update),
            1 => (0..8usize).prop_map(IsoWrite::Delete),
        ]
    }

    /// The held-key tier's isolation family: a [`super::super::held_key_program`]
    /// with the isolation table (module doc), every kind drawn equally.
    pub fn isolation_program() -> impl Strategy<Value = IsolationCase> {
        (
            super::super::held_key_program(),
            prop::sample::select(FailKind::ALL.to_vec()),
            any::<bool>(),
            1..=4usize,
            prop::collection::vec(iso_write(), 0..=3),
            prop::collection::vec(iso_write(), 0..=2),
            1..=3u8,
            prop::collection::vec(any::<u8>(), 8),
            prop::collection::vec(0..64usize, 10),
        )
            .prop_map(
                |(
                    program,
                    kind,
                    sibling_aggregate,
                    seeds,
                    before,
                    after,
                    pair_group,
                    picks,
                    slots,
                )| {
                    with_isolation(
                        program,
                        &IsolationParams {
                            kind,
                            sibling_aggregate,
                            seeds,
                            before,
                            after,
                            pair_group,
                            picks,
                            slots,
                        },
                    )
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty() -> Program {
        Program {
            tables: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: (0..4)
                .map(|i| Op::Delete {
                    table: "t0".to_string(),
                    pk: i.to_string(),
                    expect: OpOutcome::AffectsNoRows,
                })
                .collect(),
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        }
    }

    fn params(kind: FailKind) -> IsolationParams {
        IsolationParams {
            kind,
            sibling_aggregate: false,
            seeds: 2,
            before: vec![IsoWrite::Update(0), IsoWrite::Delete(1)],
            after: vec![IsoWrite::Insert],
            pair_group: 2,
            picks: vec![0, 1, 2, 3],
            slots: vec![0, 1, 3, 3, 9, 9],
        }
    }

    /// The pair is one two-row `INSERT` at `pair_op`, its rows' keys fresh,
    /// spliced among the program's ops in order; every other `iso` write
    /// is small and copies a fresh `iso_n`.
    #[test]
    fn the_pair_is_one_two_row_insert_spliced_in_order() {
        let case = with_isolation(empty(), &params(FailKind::FloatSum));
        let ops = &case.program.ops;
        assert_eq!(ops.len(), 4 + 6);
        let Op::BulkInsert { table, rows, .. } = &ops[case.draw.pair_op] else {
            panic!(
                "pair_op {} is {:?}",
                case.draw.pair_op, ops[case.draw.pair_op]
            );
        };
        assert_eq!(table, TABLE);
        assert_eq!(rows.len(), 2);
        for (row, key) in rows.iter().zip(case.draw.pair_keys) {
            assert!(row.contains(&("iso_id".to_string(), Some(key.to_string()))));
            assert!(row.contains(&("iso_f".to_string(), Some("1e308".to_string()))));
            assert!(row.contains(&("iso_g".to_string(), Some("2".to_string()))));
        }
        // The program's own ops keep their order.
        let own: Vec<&str> = ops
            .iter()
            .filter_map(|op| match op {
                Op::Delete { table, pk, .. } if table == "t0" => Some(pk.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(own, ["0", "1", "2", "3"]);
        assert_eq!(case.program.defs.len(), 2);
        assert_eq!(case.program.defs[0].target, FAILING);
        assert_eq!(case.program.defs[1].target, SIBLING);
    }

    /// A 1-1 kind's pair shares one fresh `iso_n`; no other write repeats
    /// one, so only the pair collides.
    #[test]
    fn only_the_pair_shares_a_copied_value() {
        for kind in [FailKind::UniqueTarget, FailKind::TriggerTarget] {
            let case = with_isolation(empty(), &params(kind));
            let mut ns: Vec<String> = Vec::new();
            for op in &case.program.ops {
                let rows: Vec<&Vec<(String, Option<String>)>> = match op {
                    Op::Insert { row, .. } => vec![row],
                    Op::Update { changes, .. } => vec![changes],
                    Op::BulkInsert { rows, .. } => rows.iter().collect(),
                    _ => Vec::new(),
                };
                for row in rows {
                    if let Some((_, Some(n))) = row.iter().find(|(c, _)| c == "iso_n") {
                        ns.push(n.clone());
                    }
                }
            }
            let mut unique = ns.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), ns.len() - 1, "{kind:?}: {ns:?}");
            assert!(!case.draw.kind.target_ddl("public.iso_d").is_empty());
        }
    }
}
