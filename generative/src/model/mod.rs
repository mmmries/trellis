//! Plain-data description of a generated program (design doc §1,
//! `docs/generative-test-suite.md`).
//!
//! A [`Program`] is exactly what the (future) shrinker minimizes and what
//! prints on a failing case: no engine internals beyond the AST types it
//! deliberately reuses ([`TransformDef`], [`ValueType`]) so a definition
//! this crate generates is byte-for-byte the same shape the parser produces
//! from concrete syntax.

use engine::defs::ast::{TransformDef, ValueType};

/// One column on a [`Table`].
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub value_type: ValueType,
}

/// A source table's shape. `pk_col` always names one of `columns` — see
/// [`Table::new`].
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub pk_col: String,
    pub columns: Vec<Column>,
}

impl Table {
    /// Builds a table from `pool`, giving it a primary-key column
    /// unconditionally — before any of `column_types` — so no future shrink
    /// step can strand a definition that references it (design doc §1).
    /// The PK column is always [`ValueType::Numeric`]: today's generator
    /// scope is 1-1/numeric-`+` definitions only, and a numeric PK is what
    /// every existing engine test builds against (see
    /// `engine/tests/apply.rs`'s `orders` table).
    pub fn new(pool: &mut NamePool, column_types: &[ValueType]) -> Table {
        let name = pool.next_table_name();
        let pk_col = pool.next_column_name();
        let mut columns = Vec::with_capacity(column_types.len() + 1);
        columns.push(Column {
            name: pk_col.clone(),
            value_type: ValueType::Numeric,
        });
        for value_type in column_types {
            columns.push(Column {
                name: pool.next_column_name(),
                value_type: *value_type,
            });
        }
        Table {
            name,
            pk_col,
            columns,
        }
    }
}

/// One source-table mutation. Values are the column's rendered *text* form
/// (`None` is SQL `NULL`), not a typed value: the backend seam applies each
/// op as raw source DML (design doc §1), where every bound parameter is
/// cast to its column's type in SQL text anyway, so there is no separate
/// typed representation to keep in sync here.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Insert {
        table: String,
        row: Vec<(String, Option<String>)>,
    },
    Update {
        table: String,
        pk: String,
        changes: Vec<(String, Option<String>)>,
    },
    Delete {
        table: String,
        pk: String,
    },
}

/// A generated program: a schema, the transform definitions over it, and a
/// sequence of source mutations (design doc §1).
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub tables: Vec<Table>,
    pub defs: Vec<TransformDef>,
    pub ops: Vec<Op>,
}

/// Small, fixed name pools (`t0`, `d0`, `c0`, ...) rather than random
/// identifiers — a shrunk counterexample you can read at a glance beats one
/// that is technically smaller (design doc §1). Each kind of name has its
/// own counter, so tables, definitions, and columns each start at 0
/// independently.
#[derive(Debug, Default, Clone)]
pub struct NamePool {
    tables: usize,
    defs: usize,
    columns: usize,
}

impl NamePool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_table_name(&mut self) -> String {
        let n = self.tables;
        self.tables += 1;
        format!("t{n}")
    }

    pub fn next_def_name(&mut self) -> String {
        let n = self.defs;
        self.defs += 1;
        format!("d{n}")
    }

    pub fn next_column_name(&mut self) -> String {
        let n = self.columns;
        self.columns += 1;
        format!("c{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_pools_are_stable_and_independent_across_construction() {
        let mut pool = NamePool::new();
        assert_eq!(pool.next_table_name(), "t0");
        assert_eq!(pool.next_table_name(), "t1");
        assert_eq!(pool.next_def_name(), "d0");
        assert_eq!(pool.next_column_name(), "c0");
        assert_eq!(pool.next_column_name(), "c1");
        assert_eq!(pool.next_def_name(), "d1");
        assert_eq!(pool.next_table_name(), "t2");
    }

    #[test]
    fn every_table_gets_its_pk_column_unconditionally() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[]);
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.columns[0].name, table.pk_col);
        assert_eq!(table.columns[0].value_type, ValueType::Numeric);
    }

    #[test]
    fn a_table_with_extra_columns_still_has_the_pk_first() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Text]);
        assert_eq!(table.pk_col, "c0");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c0", "c1", "c2"]
        );
        assert_eq!(table.columns[1].value_type, ValueType::Numeric);
        assert_eq!(table.columns[2].value_type, ValueType::Text);
    }

    #[test]
    fn a_program_round_trips_and_prints_legibly_via_debug() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[ValueType::Numeric]);
        let program = Program {
            tables: vec![table.clone()],
            defs: Vec::new(),
            ops: vec![Op::Insert {
                table: table.name.clone(),
                row: vec![
                    (table.pk_col.clone(), Some("1".to_string())),
                    ("c1".to_string(), Some("2".to_string())),
                ],
            }],
        };
        let printed = format!("{program:?}");
        assert!(printed.contains("Program"));
        assert!(printed.contains("t0"));
        assert!(printed.contains("Insert"));
        assert_eq!(program, program.clone());
    }
}
