//! The source-key dimension (#2171, ADR-0026): a drawn program's first
//! definition's source read through an explicit `KEY (...)` over a unique
//! index instead of its primary key.
//!
//! [`key_source`] rewrites a built program: the table gains one or two
//! `Text` key columns under a unique index (`NULLS NOT DISTINCT` when any is
//! nullable), keeps or drops its primary key, and every definition over it
//! names the key. Each insert on the table writes a drawn key, and each
//! update may also move its row to another key. The key values are the ones
//! the key encoding turns on ([`KEY_PART_VALUES`]): `NULL`, `''`, `chr(1)`,
//! a doubled `chr(1)` and the composite separator `chr(31)`.
//!
//! Two rows can now collide on the key, so [`resimulate`] recomputes the
//! table's expectations: an insert or key move onto a live row's key fails
//! with a unique violation. The concurrent tier keeps a keyed table's writes
//! in one lane ([`super::concurrent_plan`]): which of two racing writes to
//! one key fails would otherwise depend on commit order.

use std::collections::{BTreeSet, HashMap};

use trellis::dev::defs::ast::{KeySpace, ValueType};

use crate::model::{Column, Op, OpOutcome, Program, SourceKey};

/// The text values a key part draws, beside `NULL` where the column is
/// nullable: the empty string, `chr(1)` (the build's `NULL` stand-in, which
/// it doubles), a doubled `chr(1)`, one inside a longer string, `chr(31)`
/// (the composite key's separator) and two plain values.
pub const KEY_PART_VALUES: &[&str] =
    &["", "\u{1}", "\u{1}\u{1}", "a\u{1}\u{1}", "\u{1f}", "a", "b"];

/// The class of a drawn key part, as the coverage floors count them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyValueClass {
    Null,
    Empty,
    /// Exactly `chr(1)`.
    Chr1,
    /// Holds two `chr(1)` in a row.
    DoubledChr1,
    /// Holds `chr(31)`.
    Separator,
    Plain,
}

impl KeyValueClass {
    pub const ALL: &[KeyValueClass] = &[
        KeyValueClass::Null,
        KeyValueClass::Empty,
        KeyValueClass::Chr1,
        KeyValueClass::DoubledChr1,
        KeyValueClass::Separator,
        KeyValueClass::Plain,
    ];

    pub fn of(value: Option<&str>) -> KeyValueClass {
        match value {
            None => KeyValueClass::Null,
            Some("") => KeyValueClass::Empty,
            Some("\u{1}") => KeyValueClass::Chr1,
            Some(v) if v.contains("\u{1}\u{1}") => KeyValueClass::DoubledChr1,
            Some(v) if v.contains('\u{1f}') => KeyValueClass::Separator,
            Some(_) => KeyValueClass::Plain,
        }
    }
}

/// The drawn shape of a keyed source.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyShape {
    /// One entry per key column (one or two): whether it is nullable.
    pub nullable: Vec<bool>,
    /// Whether the table keeps its primary key, which no definition reads.
    pub primary_key: bool,
}

/// One drawn key: a value per key column (`None` is SQL `NULL`).
pub type Key = Vec<Option<String>>;

/// Everything [`key_source`] adds to a built program.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyDraw {
    pub shape: KeyShape,
    /// The key each row inserted on the table writes, in op order, reused
    /// from the start when the table inserts more rows. Never empty.
    pub inserts: Vec<Key>,
    /// For each update on the table, in op order (reused likewise):
    /// `Some(key)` also moves its row to `key`. Never empty.
    pub moves: Vec<Option<Key>>,
}

/// The key columns' names: `k0`, `k1`, apart from every generated column's
/// `c<n>`.
pub fn key_column(index: usize) -> String {
    format!("k{index}")
}

/// `program` with the source of its first definition read through a
/// `KEY (...)` as `draw` says (the module doc comment): key columns, the
/// unique index, every definition over the table naming the key, a key on
/// every insert, key moves on updates, and the table's expectations
/// recomputed ([`resimulate`]).
///
/// # Panics
///
/// If `program` has no definition, or its first definition's source isn't
/// one of its tables, or the table is partitioned.
pub fn key_source(mut program: Program, draw: &KeyDraw) -> Program {
    assert!(!draw.inserts.is_empty() && !draw.moves.is_empty());
    let name = program.defs[0].source.clone();
    let table = program
        .tables
        .iter_mut()
        .find(|t| t.name == name)
        .expect("the first definition reads one of the program's tables");
    assert!(
        table.partitioning.is_none(),
        "a partitioned source keeps its primary key"
    );
    let columns: Vec<String> = (0..draw.shape.nullable.len()).map(key_column).collect();
    for column in &columns {
        table.columns.push(Column {
            name: column.clone(),
            value_type: ValueType::Text,
        });
    }
    table.source_key = Some(SourceKey {
        columns: columns.clone(),
        nullable: columns
            .iter()
            .zip(&draw.shape.nullable)
            .filter(|(_, nullable)| **nullable)
            .map(|(c, _)| c.clone())
            .collect(),
        primary_key: draw.shape.primary_key,
    });
    for def in program.defs.iter_mut().filter(|d| d.source == name) {
        def.source_key = Some(columns.clone());
    }

    let with_key = |key: &Key| -> Vec<(String, Option<String>)> {
        columns.iter().cloned().zip(key.iter().cloned()).collect()
    };
    let (mut inserted, mut updated) = (0, 0);
    for op in program.ops.iter_mut().filter(|op| op.table() == name) {
        match op {
            Op::Insert { row, .. } => {
                row.extend(with_key(&draw.inserts[inserted % draw.inserts.len()]));
                inserted += 1;
            }
            Op::BulkInsert { rows, .. } => {
                for row in rows {
                    row.extend(with_key(&draw.inserts[inserted % draw.inserts.len()]));
                    inserted += 1;
                }
            }
            Op::Update { changes, .. } => {
                if let Some(key) = &draw.moves[updated % draw.moves.len()] {
                    changes.extend(with_key(key));
                }
                updated += 1;
            }
            Op::Delete { .. } | Op::Truncate { .. } => {}
            Op::Leaf { .. } | Op::Partition { .. } => {
                unreachable!("a keyed source isn't partitioned")
            }
        }
    }
    resimulate(&mut program, &name);
    program
}

/// The key `values` (an insert's row or an update's changes) write on a
/// keyed table, if they write one.
fn written_key(key: &SourceKey, values: &[(String, Option<String>)]) -> Option<Key> {
    key.columns
        .iter()
        .map(|c| {
            values
                .iter()
                .find(|(name, _)| name == c)
                .map(|(_, v)| v.clone())
        })
        .collect()
}

/// Recomputes the expectation of every op on the keyed table `name`: a row
/// is live by `pk_col`, a write of a live row's `pk_col` or of another live
/// row's key fails (the index is `NULLS NOT DISTINCT`, so `NULL` parts are
/// equal), and an update or delete of a row that isn't live affects none.
///
/// # Panics
///
/// If `name` isn't a keyed table of `program`.
pub fn resimulate(program: &mut Program, name: &str) {
    let table = program
        .tables
        .iter()
        .find(|t| t.name == name)
        .expect("a table of the program");
    let key = table.source_key.clone().expect("a keyed table");
    let pk_col = table.pk_col.clone();
    let mut live: HashMap<String, Key> = HashMap::new();
    let taken = |live: &HashMap<String, Key>, key: &Key, except: Option<&str>| {
        live.iter()
            .any(|(pk, k)| k == key && Some(pk.as_str()) != except)
    };
    for op in program.ops.iter_mut().filter(|op| op.table() == name) {
        let outcome = match op {
            Op::Insert { row, .. } => {
                let pk = pk_of(row, &pk_col);
                let k = written_key(&key, row).expect("an insert writes the key");
                if live.contains_key(&pk) || taken(&live, &k, None) {
                    OpOutcome::Fails
                } else {
                    live.insert(pk, k);
                    OpOutcome::Succeeds
                }
            }
            Op::BulkInsert { rows, .. } => {
                let mut next = live.clone();
                let mut fails = false;
                for row in rows.iter() {
                    let pk = pk_of(row, &pk_col);
                    let k = written_key(&key, row).expect("an insert writes the key");
                    fails |= next.contains_key(&pk) || taken(&next, &k, None);
                    next.insert(pk, k);
                }
                if fails {
                    OpOutcome::Fails
                } else {
                    live = next;
                    OpOutcome::Succeeds
                }
            }
            Op::Update { pk, changes, .. } => {
                // An update may also move its row to another `pk_col` (the
                // 1-1 JOIN tier's rekey).
                let moved_to = changes
                    .iter()
                    .find(|(c, _)| *c == pk_col)
                    .and_then(|(_, v)| v.clone())
                    .filter(|to| to != pk);
                match live.get(pk.as_str()).cloned() {
                    None => OpOutcome::AffectsNoRows,
                    Some(old) => {
                        let k = written_key(&key, changes).unwrap_or(old);
                        if moved_to.as_ref().is_some_and(|to| live.contains_key(to))
                            || taken(&live, &k, Some(pk))
                        {
                            OpOutcome::Fails
                        } else {
                            live.remove(pk.as_str());
                            live.insert(moved_to.unwrap_or_else(|| pk.clone()), k);
                            OpOutcome::Succeeds
                        }
                    }
                }
            }
            Op::Delete { pk, .. } => {
                if live.remove(pk.as_str()).is_some() {
                    OpOutcome::Succeeds
                } else {
                    OpOutcome::AffectsNoRows
                }
            }
            Op::Truncate { .. } => {
                let outcome = if live.is_empty() {
                    OpOutcome::AffectsNoRows
                } else {
                    OpOutcome::Succeeds
                };
                live.clear();
                outcome
            }
            Op::Leaf { .. } | Op::Partition { .. } => {
                unreachable!("a keyed source isn't partitioned")
            }
        };
        op.set_expect(outcome);
    }
}

fn pk_of(row: &[(String, Option<String>)], pk_col: &str) -> String {
    row.iter()
        .find(|(name, _)| name == pk_col)
        .and_then(|(_, v)| v.clone())
        .expect("every insert writes a non-NULL pk_col")
}

/// What a keyed program draws, by name, for the coverage floors: its
/// shape (`single`/`composite`, `not_null`/`nullable`,
/// `primary_key`/`no_primary_key`), the key spaces over it (`one_to_one`,
/// `aggregate`, and `joined` for a 1-1 `JOIN`'s joined table), each [`KeyValueClass`] a key it writes
/// holds (`value_<class>`), `twins` when two keys it inserts differ only in
/// one part being `NULL` in one and `''` in the other, and the key moves its
/// updates make that run (`move`, `move_to_null`, `move_from_null`) and
/// that collide (`collision`). Empty for a program with no keyed table.
pub fn drawn(program: &Program) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Some((table, key)) = program
        .tables
        .iter()
        .find_map(|t| t.source_key.as_ref().map(|k| (t, k)))
    else {
        return out;
    };
    out.insert(
        if key.columns.len() == 1 {
            "single"
        } else {
            "composite"
        }
        .into(),
    );
    out.insert(
        if key.nullable.is_empty() {
            "not_null"
        } else {
            "nullable"
        }
        .into(),
    );
    out.insert(
        if key.primary_key {
            "primary_key"
        } else {
            "no_primary_key"
        }
        .into(),
    );
    for def in &program.defs {
        if def.source == table.name {
            out.insert(
                match def.key_space {
                    KeySpace::Aggregate { .. } => "aggregate",
                    _ => "one_to_one",
                }
                .into(),
            );
        }
        if let KeySpace::JoinOneToOne(join) = &def.key_space
            && join.joins.iter().any(|j| j.right == table.name)
        {
            out.insert("joined".into());
        }
    }
    let mut inserted: Vec<Key> = Vec::new();
    let mut current: HashMap<String, Key> = HashMap::new();
    for op in program.ops.iter().filter(|op| op.table() == table.name) {
        let writes: Vec<(Option<String>, Key)> = match op {
            Op::Insert { row, .. } => vec![(None, written_key(key, row).unwrap_or_default())],
            Op::BulkInsert { rows, .. } => rows
                .iter()
                .map(|row| (None, written_key(key, row).unwrap_or_default()))
                .collect(),
            Op::Update { pk, changes, .. } => written_key(key, changes)
                .map(|k| vec![(Some(pk.clone()), k)])
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        for (moved, k) in writes {
            for part in &k {
                out.insert(
                    format!("value_{:?}", KeyValueClass::of(part.as_deref())).to_lowercase(),
                );
            }
            if *op.expect() == OpOutcome::Fails {
                out.insert("collision".into());
            }
            let succeeded = *op.expect() == OpOutcome::Succeeds;
            match (moved, op) {
                (Some(pk), _) if succeeded => {
                    out.insert("move".into());
                    if k.contains(&None) {
                        out.insert("move_to_null".into());
                    }
                    if current.get(&pk).is_some_and(|old| old.contains(&None)) {
                        out.insert("move_from_null".into());
                    }
                    current.insert(pk, k);
                }
                (None, Op::Insert { row, .. }) if succeeded => {
                    current.insert(pk_of(row, &table.pk_col), k.clone());
                    inserted.push(k);
                }
                _ => inserted.push(k),
            }
        }
    }
    if inserted
        .iter()
        .enumerate()
        .any(|(i, a)| inserted[i + 1..].iter().any(|b| twins(a, b)))
    {
        out.insert("twins".into());
    }
    out
}

/// Whether keys `a` and `b` differ only in one part, `NULL` in one and `''`
/// in the other: the two keys #2126's encoding ran together.
pub fn twins(a: &Key, b: &Key) -> bool {
    let differing: Vec<usize> = (0..a.len()).filter(|&i| a[i] != b[i]).collect();
    matches!(differing.as_slice(), [i]
        if matches!((&a[*i], &b[*i]), (None, Some(e)) | (Some(e), None) if e.is_empty()))
}

#[cfg(feature = "proptest")]
pub(crate) mod strategy {
    use super::*;
    use crate::generate::ConcurrentCase;
    use proptest::prelude::*;

    /// How many keys a draw carries for inserts and for updates: the most
    /// rows and updates one table of [`super::super::trivial_program`]
    /// writes, so no key is reused in practice.
    const KEY_DRAWS: usize = super::super::MAX_SEED_ROWS + super::super::MAX_MUTATES;

    /// One key part: `NULL` (when `nullable`), or one of
    /// [`KEY_PART_VALUES`], `''` and `NULL` weighted up so the two often
    /// sit side by side.
    pub(crate) fn part(nullable: bool) -> BoxedStrategy<Option<String>> {
        let text = prop_oneof![
            3 => Just(String::new()),
            4 => proptest::sample::select(&KEY_PART_VALUES[1..]).prop_map(str::to_string),
        ];
        if nullable {
            prop_oneof![2 => Just(None), 5 => text.prop_map(Some)].boxed()
        } else {
            text.prop_map(Some).boxed()
        }
    }

    fn key(shape: &KeyShape) -> BoxedStrategy<Key> {
        shape
            .nullable
            .iter()
            .map(|n| part(*n))
            .collect::<Vec<_>>()
            .boxed()
    }

    /// One or two key columns, each nullable or not (a composite key draws
    /// a nullable part three times in four, and one in three draws both),
    /// with or without a primary key beside.
    fn key_shape(nullable_only: bool) -> impl Strategy<Value = KeyShape> {
        let single = if nullable_only {
            Just(vec![true]).boxed()
        } else {
            any::<bool>().prop_map(|n| vec![n]).boxed()
        };
        let composite = if nullable_only {
            prop_oneof![
                Just(vec![true, false]),
                Just(vec![false, true]),
                Just(vec![true, true]),
            ]
            .boxed()
        } else {
            prop_oneof![
                Just(vec![false, false]),
                Just(vec![true, false]),
                Just(vec![false, true]),
                Just(vec![true, true]),
            ]
            .boxed()
        };
        (prop_oneof![single, composite], any::<bool>()).prop_map(|(nullable, primary_key)| {
            KeyShape {
                nullable,
                primary_key,
            }
        })
    }

    /// A [`KeyDraw`]: a shape (with a nullable key column when
    /// `nullable_only`), a key per insert, and a key move on four updates in
    /// five.
    pub fn key_draw(nullable_only: bool) -> impl Strategy<Value = KeyDraw> {
        key_shape(nullable_only).prop_flat_map(|shape| {
            let inserts = prop::collection::vec(key(&shape), KEY_DRAWS);
            let moves = prop::collection::vec(prop::option::weighted(0.8, key(&shape)), KEY_DRAWS);
            (Just(shape), inserts, moves).prop_map(|(shape, inserts, moves)| KeyDraw {
                shape,
                inserts,
                moves,
            })
        })
    }

    /// A [`super::super::trivial_program`] whose first definition's source
    /// is read through a drawn `KEY (...)` ([`key_source`]).
    pub fn keyed_program(nullable_only: bool) -> impl Strategy<Value = Program> {
        (super::super::trivial_program(), key_draw(nullable_only))
            .prop_map(|(program, draw)| key_source(program, &draw))
    }

    /// What the convergence properties draw: a
    /// [`super::super::trivial_program`], or one time in four a
    /// [`keyed_program`].
    pub fn trivial_or_keyed_program() -> impl Strategy<Value = Program> {
        prop_oneof![
            3 => super::super::trivial_program().boxed(),
            1 => keyed_program(false).boxed(),
        ]
    }

    /// The concurrent tier's `source_key` case, its planted-bug gate for
    /// this dimension: a [`keyed_program`] with a nullable key column, in
    /// bursts of one to four ops over up to [`super::super::MAX_LANES`]
    /// lanes ([`super::super::concurrent_plan`]: the keyed table's writes
    /// keep to one lane, the other tables' race it), against an engine with
    /// [`super::super::HOT_WORKERS`] drain workers sealing every
    /// [`super::super::HOT_SEAL_INTERVAL_MS`].
    pub fn keyed_case() -> impl Strategy<Value = ConcurrentCase> {
        (
            keyed_program(true),
            1usize..=4,
            2usize..=super::super::MAX_LANES,
            super::super::HOT_WORKERS,
            super::super::HOT_SEAL_INTERVAL_MS,
        )
            .prop_map(|(program, burst_size, lanes, workers, seal_interval_ms)| {
                let plan = super::super::concurrent_plan(&program, burst_size, lanes);
                ConcurrentCase {
                    program,
                    burst_size,
                    plan,
                    workers,
                    seal_interval_ms,
                    build_chunk_rows: None,
                    reconcile_interval_ms: None,
                    housekeeping_interval_ms: None,
                }
            })
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{key_draw, keyed_case, keyed_program, trivial_or_keyed_program};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{DefShape, Mutate, TableSpec, build_program_multi_with_shapes};

    fn program(mutates: Vec<Mutate>, draw: &KeyDraw) -> Program {
        key_source(
            build_program_multi_with_shapes(
                &[TableSpec::numeric_only(
                    vec![(Some(1), None), (Some(2), None)],
                    mutates,
                )],
                &[(0, DefShape::OneToOne)],
            ),
            draw,
        )
    }

    fn k(v: Option<&str>) -> Key {
        vec![v.map(str::to_string)]
    }

    #[test]
    fn a_null_key_and_an_empty_key_are_two_rows_and_a_repeat_collides() {
        let draw = KeyDraw {
            shape: KeyShape {
                nullable: vec![true],
                primary_key: false,
            },
            inserts: vec![k(None), k(Some("")), k(None)],
            moves: vec![Some(k(Some(""))), Some(k(Some("\u{1}")))],
        };
        let update = |pk| Mutate::Update {
            pk,
            c1: None,
            c2: None,
        };
        let revive = |pk| Mutate::DuplicateInsert {
            pk,
            c1: None,
            c2: None,
        };
        let p = program(
            vec![
                // pk 2 is live: fails.
                revive(2),
                // pk 1 (NULL) moves onto '', pk 2's: fails.
                update(1),
                // pk 1 moves to chr(1).
                update(1),
                Mutate::Delete { pk: 2 },
                // pk 2 comes back on NULL, free since pk 1 moved.
                revive(2),
            ],
            &draw,
        );
        let expects: Vec<&OpOutcome> = p.ops.iter().map(Op::expect).collect();
        use OpOutcome::{Fails, Succeeds};
        assert_eq!(
            expects,
            [
                &Succeeds, &Succeeds, &Fails, &Fails, &Succeeds, &Succeeds, &Succeeds
            ]
        );
        assert_eq!(p.defs[0].source_key, Some(vec!["k0".to_string()]));
        let key = p.tables[0].source_key.as_ref().unwrap();
        assert_eq!(key.nullable, ["k0"]);
        let drawn = drawn(&p);
        for name in [
            "single",
            "nullable",
            "no_primary_key",
            "value_null",
            "value_empty",
            "value_chr1",
            "twins",
            "collision",
            "move",
            "move_from_null",
        ] {
            assert!(drawn.contains(name), "{name}: {drawn:?}");
        }
        assert!(!drawn.contains("move_to_null"), "{drawn:?}");
    }

    /// An update that also moves its row to another `pk_col` (the 1-1 JOIN
    /// tier's rekey) fails onto a live id, and otherwise moves the row and
    /// its key: the old id is gone, and the key still collides.
    #[test]
    fn a_rekey_onto_a_live_id_fails_and_onto_a_free_one_moves_the_row() {
        let draw = KeyDraw {
            shape: KeyShape {
                nullable: vec![true],
                primary_key: true,
            },
            inserts: vec![k(None), k(Some(""))],
            moves: vec![None],
        };
        let mut p = program(Vec::new(), &draw);
        let (table, pk_col) = (p.tables[0].name.clone(), p.tables[0].pk_col.clone());
        let pks: Vec<String> = p
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Insert { row, .. } => Some(pk_of(row, &pk_col)),
                _ => None,
            })
            .collect();
        let [a, b] = pks.as_slice() else {
            panic!("{pks:?}")
        };
        let seeded = p.ops.len();
        let update = |pk: &str, changes: Vec<(String, Option<String>)>| Op::Update {
            table: table.clone(),
            pk: pk.to_string(),
            changes,
            expect: OpOutcome::Succeeds,
        };
        let id = |to: &str| (pk_col.clone(), Some(to.to_string()));
        p.ops.extend([
            update(a, vec![id(b)]),
            update(a, vec![id("999")]),
            update(a, vec![("k0".to_string(), Some("x".to_string()))]),
            update("999", vec![("k0".to_string(), Some(String::new()))]),
            update(
                "999",
                vec![id("998"), ("k0".to_string(), Some("y".to_string()))],
            ),
        ]);
        resimulate(&mut p, &table);
        use OpOutcome::{AffectsNoRows, Fails, Succeeds};
        let expects: Vec<&OpOutcome> = p.ops[seeded..].iter().map(Op::expect).collect();
        assert_eq!(
            expects,
            [&Fails, &Succeeds, &AffectsNoRows, &Fails, &Succeeds]
        );
    }

    #[test]
    fn composite_twins_differ_in_one_part_only() {
        let s = |v: &str| Some(v.to_string());
        let draw = KeyDraw {
            shape: KeyShape {
                nullable: vec![true, true],
                primary_key: true,
            },
            inserts: vec![vec![None, s("x")], vec![s(""), s("y")]],
            moves: vec![Some(vec![s(""), None])],
        };
        let p = program(Vec::new(), &draw);
        assert!(!drawn(&p).contains("twins"));
        let draw = KeyDraw {
            inserts: vec![vec![None, s("x")], vec![s(""), s("x")]],
            ..draw
        };
        let p = program(
            vec![Mutate::Update {
                pk: 2,
                c1: None,
                c2: None,
            }],
            &draw,
        );
        let drawn = drawn(&p);
        assert!(drawn.contains("twins"), "{drawn:?}");
        assert!(drawn.contains("move_to_null"), "{drawn:?}");
        assert!(drawn.contains("composite") && drawn.contains("primary_key"));
    }
}
