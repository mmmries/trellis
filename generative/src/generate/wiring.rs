//! Application triggers and foreign keys on the sources (#2181, epic #2180
//! step 2c): the writes Postgres runs inside a drawn statement, which reach
//! capture as nested statements (`docs/staging-and-claiming/01-capture-by-triggers.md`,
//! "When capture re-reads").
//!
//! [`with_wiring`] draws, for some of a program's tables, a [`Wiring`]: a
//! [`Shadow`] kept by row or statement triggers, `BEFORE` or `AFTER`, in the
//! same table or another, linked by a foreign key that cascades or sets
//! `NULL`, and a [`Rewrite`] of the written row itself.
//!
//! # Seeds stay what they were
//!
//! Like [`super::txn_shapes`], the wiring is derived from the program's
//! digest ([`where_clause::digest`]), never drawn by proptest: every strategy
//! draws the program it drew before.
//!
//! # Every op's outcome holds, and lanes still own their keys
//!
//! A wiring writes only rows the lane whose statement fired it owns: the
//! written row itself, its shadow `k + SHADOW_OFFSET`, and the connection's
//! tally row `TALLY_BASE + pg_backend_pid()`. No drawn key reaches
//! [`SHADOW_OFFSET`] on a wired table, so no drawn op names any of them,
//! and a trigger never fails a statement or changes which drawn rows exist.
//! So the program's expected outcomes stand unchanged, the lanes stay keyed
//! by `(table, pk)`, and the oracle, which recomputes from the source, sees
//! every cascaded and triggered write.
//!
//! A table some op writes as a whole (`TRUNCATE`, a bulk insert, partition
//! maintenance) is never wired: a `TRUNCATE` of a referenced table fails, and
//! its row count would count the shadows.

use std::collections::{HashMap, HashSet};

use super::where_clause;
use crate::model::{
    ConcurrentPlan, FkAction, Op, PRIMARY_KEY_VALUE_TYPE, Program, Rewrite, SHADOW_OFFSET, Shadow,
    Table, Timing, Wiring,
};

/// How many eligible tables in 256 get a wiring.
pub const WIRE_SHARE: u64 = 128;

/// One step of SplitMix64.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Whether a wiring can fire on `table`: a plain table keyed by a `bigint`,
/// that no op writes as a whole, and whose every drawn key is below
/// [`SHADOW_OFFSET`].
fn wirable(table: &Table, program: &Program) -> bool {
    if table.partitioning.is_some()
        || table.column_type(&table.pk_col) != Some(PRIMARY_KEY_VALUE_TYPE)
    {
        return false;
    }
    program
        .ops
        .iter()
        .filter(|op| op.table() == table.name)
        .all(|op| match op.write() {
            Op::Insert { row, .. } => keys_fit(row, table),
            Op::Update { pk, changes, .. } => key_fits(Some(pk)) && keys_fit(changes, table),
            Op::Delete { pk, .. } => key_fits(Some(pk)),
            _ => false,
        })
}

fn keys_fit(columns: &[(String, Option<String>)], table: &Table) -> bool {
    columns
        .iter()
        .filter(|(c, _)| *c == table.pk_col)
        .all(|(_, v)| key_fits(v.as_ref()))
}

fn key_fits(value: Option<&String>) -> bool {
    value
        .and_then(|v| v.parse::<i64>().ok())
        .is_some_and(|k| k.unsigned_abs() < SHADOW_OFFSET.unsigned_abs())
}

/// Each column of `table` but the key, with the distinct values the program
/// writes to it, in order of first write.
fn drawn_values(program: &Program, table: &Table) -> Vec<(String, Vec<Option<String>>)> {
    let mut out: Vec<(String, Vec<Option<String>>)> = table
        .columns
        .iter()
        .filter(|c| c.name != table.pk_col)
        .map(|c| (c.name.clone(), Vec::new()))
        .collect();
    for op in program.ops.iter().filter(|op| op.table() == table.name) {
        let written = match op.write() {
            Op::Insert { row, .. } => row,
            Op::Update { changes, .. } => changes,
            _ => continue,
        };
        for (column, value) in written {
            if let Some((_, values)) = out.iter_mut().find(|(c, _)| c == column)
                && !values.contains(value)
            {
                values.push(value.clone());
            }
        }
    }
    out
}

/// Whether a column of `table` takes part in a `UNIQUE` constraint or its
/// source key's unique index: flipping it could make a write collide (#2251).
fn unique(table: &Table, column: &str) -> bool {
    table.unique_cols.iter().any(|c| c == column)
        || table.unique_sets.iter().flatten().any(|c| c == column)
        || table
            .source_key
            .as_ref()
            .is_some_and(|key| key.columns.iter().any(|c| c == column))
}

/// Whether a definition of `program` sums `column` of `table` (`SUM` or
/// `AVG`) in a type whose sum keeps the argument's range: `real`, `double
/// precision` or `interval`. The generators budget such a column's values
/// so no group's sum leaves that range ([`super::exact_sums`]); a flip or a
/// shadow writes one value into every row the trigger touches, past the
/// budget (#2263). Wiring stays off columns another generator constrains.
fn budgeted(program: &Program, table: &Table, column: &str) -> bool {
    use trellis::dev::defs::PgType;
    use trellis::dev::defs::ast::{Expr, ValueType};
    if !matches!(
        table.column_type(column),
        Some(ValueType::Float(_) | ValueType::Other(PgType::Interval))
    ) {
        return false;
    }
    let mut summed = false;
    for field in program.defs.iter().flat_map(|def| &def.fields) {
        field.expr.walk(&mut |e| {
            if let Expr::FunctionCall { name, args } = e
                && (name == "SUM" || name == "AVG")
            {
                for arg in args {
                    arg.walk(&mut |e| match e {
                        Expr::Column(c) | Expr::SourceColumn { column: c, .. } if c == column => {
                            summed = true;
                        }
                        _ => {}
                    });
                }
            }
        });
    }
    summed
}

/// Whether some definition of `program` names `column` of `table`.
fn read(program: &Program, table: &str, column: &str) -> bool {
    let quoted = format!("{column:?}");
    program
        .defs
        .iter()
        .any(|def| format!("{def:?}").contains(table) && format!("{def:?}").contains(&quoted))
}

/// A column and two values it flips between ([`Shadow::toggle`]).
type Flip = (String, Option<String>, Option<String>);

/// A column of `table` a [`Rewrite`] or a [`Shadow`] can flip, with two of
/// its drawn values: one no `UNIQUE` constraint or source key covers
/// ([`unique`]) and no other generator budgets ([`budgeted`]), which the
/// program writes at least two values to, preferring one a definition reads.
fn flip_column(program: &Program, table: &Table, pick: u64) -> Option<Flip> {
    let mut candidates: Vec<Flip> = drawn_values(program, table)
        .into_iter()
        .filter(|(c, values)| {
            values.len() >= 2 && !unique(table, c) && !budgeted(program, table, c)
        })
        .map(|(c, values)| (c, values[0].clone(), values[1].clone()))
        .collect();
    let read_ones: Vec<_> = candidates
        .iter()
        .filter(|(c, ..)| read(program, &table.name, c))
        .cloned()
        .collect();
    if !read_ones.is_empty() {
        candidates = read_ones;
    }
    if candidates.is_empty() {
        return None;
    }
    let index = (pick % candidates.len() as u64) as usize;
    Some(candidates.swap_remove(index))
}

/// Whether a 1-1 `JOIN` definition of `program` reads `table`. Its in-run
/// invariant reads `self_check`'s subtraction, which mistakes a pending
/// `AFTER` rewrite's nested old image for what was applied (#2209), so such
/// a table draws only the `BEFORE` rewrite until that is fixed.
fn read_by_one_to_one_join(program: &Program, table: &Table) -> bool {
    use trellis::dev::defs::ast::KeySpace;
    program.defs.iter().any(|def| match &def.key_space {
        KeySpace::JoinOneToOne(join) => {
            def.source == table.name || join.joins.iter().any(|j| j.right == table.name)
        }
        _ => false,
    })
}

/// `plan` with [`ConcurrentPlan::wiring`] drawn for `program`: each
/// [`wirable`] table, [`WIRE_SHARE`] in 256 of the time, gets a [`Shadow`]
/// in a wirable table with no `UNIQUE` constraint or source key not yet
/// shadowed (three
/// times in four) and a [`Rewrite`] (three times in four, and always when
/// it gets no shadow; `AFTER` three times in four, the one that nests a
/// statement), each where some column can flip.
pub fn with_wiring(mut plan: ConcurrentPlan, program: &Program) -> ConcurrentPlan {
    let digest = where_clause::digest(program) ^ 0x2181_0c0c;
    let wirable: Vec<&Table> = program
        .tables
        .iter()
        .filter(|t| wirable(t, program))
        .collect();
    let mut shadowed: HashSet<&str> = HashSet::new();
    let mut wiring = Vec::new();
    for (index, table) in program.tables.iter().enumerate() {
        if !wirable.iter().any(|t| t.name == table.name) {
            continue;
        }
        let d = mix(digest ^ mix(index as u64 + 1));
        if d % 256 >= WIRE_SHARE {
            continue;
        }
        let d = mix(d);
        let targets: Vec<(&Table, Flip)> = wirable
            .iter()
            .filter(|t| {
                t.unique_cols.is_empty()
                    && t.unique_sets.is_empty()
                    && t.source_key.is_none()
                    && !shadowed.contains(t.name.as_str())
            })
            .filter_map(|t| flip_column(program, t, d >> 40).map(|flip| (*t, flip)))
            .collect();
        let shadow = (!(d >> 8).is_multiple_of(4) && !targets.is_empty()).then(|| {
            let (target, toggle) = targets[((d >> 12) % targets.len() as u64) as usize].clone();
            let timing = if (d >> 17).is_multiple_of(2) {
                Timing::Before
            } else {
                Timing::After
            };
            let fk = match (timing, (d >> 19) % 3) {
                (Timing::Before, _) | (_, 0) => None,
                (_, 1) => Some(FkAction::Cascade),
                _ => Some(FkAction::SetNull),
            };
            // A budgeted column's shadows are `NULL`: every shadow carries
            // the template, so its first value would be summed once a row.
            let template = drawn_values(program, target)
                .into_iter()
                .map(|(c, values)| {
                    let value = values.into_iter().next().flatten();
                    let value = value.filter(|_| !budgeted(program, target, &c));
                    (c, value)
                })
                .collect();
            shadowed.insert(target.name.as_str());
            Shadow {
                table: target.name.clone(),
                statement: (d >> 16).is_multiple_of(2),
                timing,
                before_capture: (d >> 18).is_multiple_of(2),
                fk,
                template,
                toggle,
            }
        });
        let rewrite = (shadow.is_none() || !(d >> 10).is_multiple_of(4))
            .then(|| flip_column(program, table, d >> 48))
            .flatten()
            .map(|(column, a, b)| Rewrite {
                column,
                timing: if (d >> 22).is_multiple_of(4) || read_by_one_to_one_join(program, table) {
                    Timing::Before
                } else {
                    Timing::After
                },
                values: (a, b),
            });
        if shadow.is_some() || rewrite.is_some() {
            wiring.push(Wiring {
                table: table.name.clone(),
                shadow,
                rewrite,
            });
        }
    }
    plan.wiring = wiring;
    plan
}

/// The wiring shapes `plan` draws, by name, each paired with the key space
/// of every definition reading the table the wiring writes: the shadow's
/// table for a shadow, the wired table for a rewrite
/// ([`crate::run::Coverage::txn_shapes`]).
pub fn shapes(
    plan: &ConcurrentPlan,
    key_spaces: &HashMap<&str, Vec<&'static str>>,
) -> HashSet<(&'static str, &'static str)> {
    let mut out = HashSet::new();
    let mut add = |table: &str, shape: &'static str| {
        for space in key_spaces.get(table).into_iter().flatten() {
            out.insert((shape, *space));
        }
    };
    for wiring in &plan.wiring {
        if let Some(shadow) = &wiring.shadow {
            let mut names = vec![match (shadow.statement, shadow.timing) {
                (false, Timing::Before) => "shadow_row_before",
                (false, Timing::After) => "shadow_row_after",
                (true, Timing::Before) => "shadow_statement_before",
                (true, Timing::After) => "shadow_statement_after",
            }];
            names.push(if shadow.table == wiring.table {
                "shadow_same_table"
            } else {
                "shadow_other_table"
            });
            if shadow.statement {
                names.push(if shadow.before_capture {
                    "statement_trigger_before_capture"
                } else {
                    "statement_trigger_after_capture"
                });
            }
            match shadow.fk {
                Some(FkAction::Cascade) => names.push("fk_cascade"),
                Some(FkAction::SetNull) => names.push("fk_set_null"),
                None => {}
            }
            for name in names {
                add(&shadow.table, name);
            }
        }
        if let Some(rewrite) = &wiring.rewrite {
            add(
                &wiring.table,
                match rewrite.timing {
                    Timing::Before => "rewrite_before",
                    Timing::After => "rewrite_after",
                },
            );
        }
    }
    out
}

/// Every name [`shapes`] reports.
pub const SHAPES: &[&str] = &[
    "shadow_row_before",
    "shadow_row_after",
    "shadow_statement_before",
    "shadow_statement_after",
    "shadow_same_table",
    "shadow_other_table",
    "statement_trigger_before_capture",
    "statement_trigger_after_capture",
    "fk_cascade",
    "fk_set_null",
    "rewrite_before",
    "rewrite_after",
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Column, OpOutcome};
    use trellis::dev::defs::ast::ValueType;

    fn table(name: &str) -> Table {
        Table {
            name: name.to_string(),
            pk_col: "id".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    value_type: PRIMARY_KEY_VALUE_TYPE,
                },
                Column {
                    name: "c1".to_string(),
                    value_type: ValueType::Numeric,
                },
            ],
            unique_cols: Vec::new(),
            unique_sets: Vec::new(),
            partitioning: None,
            source_key: None,
        }
    }

    fn insert(table: &str, id: &str, c1: &str) -> Op {
        Op::Insert {
            table: table.to_string(),
            row: vec![
                ("id".to_string(), Some(id.to_string())),
                ("c1".to_string(), Some(c1.to_string())),
            ],
            expect: OpOutcome::Succeeds,
        }
    }

    fn program(ops: Vec<Op>) -> Program {
        Program {
            tables: vec![table("t0"), table("t1")],
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops,
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        }
    }

    #[test]
    fn a_table_with_a_key_past_the_shadows_or_a_whole_table_write_is_not_wired() {
        let ok = program(vec![insert("t0", "1", "1"), insert("t1", "2", "2")]);
        assert!(wirable(&ok.tables[0], &ok));
        let far = program(vec![insert("t0", &SHADOW_OFFSET.to_string(), "1")]);
        assert!(!wirable(&far.tables[0], &far));
        assert!(wirable(&far.tables[1], &far));
        let truncated = program(vec![
            insert("t0", "1", "1"),
            Op::Truncate {
                table: "t0".to_string(),
                expect: OpOutcome::Succeeds,
            },
        ]);
        assert!(!wirable(&truncated.tables[0], &truncated));
    }

    /// A keyed table's key columns never flip and it is never a shadow's
    /// table: either would write keys the program's expectations don't
    /// model, so a drawn write would collide on the key's unique index
    /// (#2251).
    #[test]
    fn a_keyed_tables_key_is_never_flipped_or_shadowed_into() {
        let keyed = |id: &str, c1: &str, k0: &str| {
            let Op::Insert {
                table,
                mut row,
                expect,
            } = insert("t0", id, c1)
            else {
                unreachable!()
            };
            row.push(("k0".to_string(), Some(k0.to_string())));
            Op::Insert { table, row, expect }
        };
        let mut drawn = (0, 0);
        for seed in 0..256 {
            let mut program = program(
                (0..6)
                    .flat_map(|i| {
                        [
                            keyed(&i.to_string(), &(seed + i).to_string(), &format!("k{i}")),
                            insert("t1", &i.to_string(), &(seed * 7 + i).to_string()),
                        ]
                    })
                    .collect(),
            );
            let t0 = &mut program.tables[0];
            t0.columns.push(Column {
                name: "k0".to_string(),
                value_type: ValueType::Text,
            });
            t0.source_key = Some(crate::model::SourceKey {
                columns: vec!["k0".to_string()],
                nullable: Vec::new(),
                primary_key: true,
            });
            let flip = flip_column(&program, &program.tables[0], seed);
            assert_eq!(flip.map(|(c, ..)| c).as_deref(), Some("c1"), "seed {seed}");
            for wiring in with_wiring(ConcurrentPlan::default(), &program).wiring {
                if let Some(shadow) = &wiring.shadow {
                    assert_eq!(shadow.table, "t1", "seed {seed}");
                    drawn.0 += 1;
                }
                if let Some(rewrite) = &wiring.rewrite {
                    assert_ne!(rewrite.column, "k0", "seed {seed}");
                    drawn.1 += 1;
                }
            }
        }
        assert!(drawn.0 > 0 && drawn.1 > 0, "{drawn:?}");
    }

    /// A column a definition sums in `double precision` (`fs`) is never
    /// flipped and its shadows are `NULL`, so wiring cannot write its
    /// extremes past the budget (#2263); one it only takes the `MIN` of
    /// (`f`) still flips.
    #[test]
    fn a_budgeted_column_is_never_flipped_or_copied_into_shadows() {
        let (mut rewrites, mut shadows, mut f_flips) = (0, 0, 0);
        for seed in 0..256 {
            let mut program = program(
                (0..6)
                    .flat_map(|i| {
                        let Op::Insert {
                            table,
                            mut row,
                            expect,
                        } = insert("t0", &i.to_string(), &(seed + i).to_string())
                        else {
                            unreachable!()
                        };
                        let big = Some(format!("-{}e300", seed + i));
                        row.push(("fs".to_string(), big.clone()));
                        row.push(("f".to_string(), big));
                        [Op::Insert { table, row, expect }]
                    })
                    .collect(),
            );
            for name in ["fs", "f"] {
                program.tables[0].columns.push(Column {
                    name: name.to_string(),
                    value_type: ValueType::Float(trellis::dev::defs::FloatWidth::Float8),
                });
            }
            program.defs.push(
                trellis::dev::defs::parse(
                    "TRANSFORM agg FROM t0 GROUP BY c1 \
                     SELECT c1 AS c1, SUM(fs) AS s, MIN(f) AS m",
                )
                .unwrap(),
            );
            let flip = flip_column(&program, &program.tables[0], seed);
            assert!(
                matches!(flip.map(|(c, ..)| c).as_deref(), Some("c1" | "f")),
                "seed {seed}"
            );
            for wiring in with_wiring(ConcurrentPlan::default(), &program).wiring {
                if let Some(rewrite) = &wiring.rewrite {
                    assert_ne!(rewrite.column, "fs", "seed {seed}");
                    f_flips += usize::from(rewrite.column == "f");
                    rewrites += 1;
                }
                if let Some(shadow) = &wiring.shadow {
                    assert_ne!(shadow.toggle.0, "fs", "seed {seed}");
                    let fs = shadow.template.iter().find(|(c, _)| c == "fs");
                    assert_eq!(fs, Some(&("fs".to_string(), None)), "seed {seed}");
                    let f = shadow.template.iter().find(|(c, _)| c == "f");
                    assert!(f.is_some_and(|(_, v)| v.is_some()), "seed {seed}");
                    shadows += 1;
                }
            }
        }
        assert!(rewrites > 0 && shadows > 0 && f_flips > 0);
    }

    /// The wiring is a function of the program alone, and each shadowed
    /// table has one shadow at most.
    #[test]
    fn the_wiring_is_derived_from_the_program() {
        let mut drawn = 0;
        for seed in 0..64 {
            let program = program(
                (0..6)
                    .map(|i| {
                        insert(
                            if i % 2 == 0 { "t0" } else { "t1" },
                            &i.to_string(),
                            &(seed * 7 + i).to_string(),
                        )
                    })
                    .collect(),
            );
            let a = with_wiring(ConcurrentPlan::default(), &program);
            let b = with_wiring(ConcurrentPlan::default(), &program);
            assert_eq!(a.wiring, b.wiring);
            let mut shadows: Vec<&str> = a
                .wiring
                .iter()
                .filter_map(|w| w.shadow.as_ref().map(|s| s.table.as_str()))
                .collect();
            let all = shadows.len();
            shadows.sort_unstable();
            shadows.dedup();
            assert_eq!(shadows.len(), all);
            drawn += a.wiring.len();
        }
        assert!(drawn > 0);
    }
}
