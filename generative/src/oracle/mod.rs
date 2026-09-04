//! The independent recompute oracle (design doc §2): renders a
//! [`engine::defs::ast::TransformDef`]'s formulas back to a `SELECT` against
//! the source and treats Postgres itself as the correctness authority. Uses
//! only the shared components design doc §2 names — the parser AST, a small
//! AST→`SELECT` printer written here (sharing no code with the engine's
//! evaluator), and [`engine::defs::oracle::recompute`] (evaluation, a
//! *secondary* parity check, not the pipeline-driving maintenance code that
//! is [`crate::backend`]'s job alone).
//!
//! # The three-way comparison
//!
//! Per op we compare three renderings of the same logical target:
//!
//! - **persisted target** — what [`crate::backend::Backend::snapshot`] read
//!   back from the maintained target table;
//! - **evaluator** — [`engine::defs::oracle::recompute`], the engine's own
//!   evaluator run from scratch;
//! - **SQL oracle** — the definition rendered back to a `SELECT` and run by
//!   Postgres itself.
//!
//! The SQL oracle is the authority (ADR-0004: the grammar is an immutable
//! subset of Postgres, so a rendered `SELECT` can never be outgrown by the
//! language). Comparing the other two *against it* localizes a divergence:
//! `target ≠ SQL` is a pipeline / apply / fold / ordering bug;
//! `evaluator ≠ SQL` is an eval-layer drift bug — the direct test of the
//! "engine mirrors Postgres" claim.
//!
//! # DB access
//!
//! Both oracles take an [`engine::Pool`] — a connection handle — exactly as
//! [`engine::defs::oracle::recompute`] already does. The oracle *reads*; it
//! never imports `engine::client`/`engine::staging` or otherwise drives the
//! maintenance pipeline (that seam is [`crate::backend`]'s alone). A
//! pool connection has its `search_path` pinned, so an unqualified source
//! table name resolves the same way `recompute` relies on.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use engine::Pool;
use engine::defs::ast::{Expr, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::oracle::{OracleError, recompute};
use engine::numeric::Numeric;

use crate::model::Program;

/// One logical target's rows: `pk (rendered text) -> column -> value
/// (rendered text, `None` is SQL `NULL`)`. Matches the inner shape of
/// [`crate::backend::Snapshot`], and always excludes the primary-key column
/// itself (the pk is the row key, so carrying it as a column too would show
/// up as a spurious divergence against the evaluator recompute, which keys by
/// pk but never emits it as a field).
pub type Rows = BTreeMap<String, BTreeMap<String, Option<String>>>;

/// How two rendered-text cell values are judged equal (design doc §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    /// Byte-exact text equality — int, text, date, timestamp, uuid, boolean.
    Exact,
    /// Decimal by value, ignoring scale (`2.50 ≡ 2.5`), via
    /// [`engine::numeric::Numeric`] rather than hand-rolled parsing.
    DecimalByValue,
    /// Combined absolute+relative tolerance for floats. **Unreachable
    /// today**: [`ValueType`] has no float variant, so no column or
    /// derivation can ever produce one, so this is never constructed. Left
    /// as an explicit stub (rather than silently omitted) so the day the
    /// value language gains floats, the missing tolerance logic is a loud
    /// `unimplemented!`, not a wrong exact-equality comparison.
    #[allow(dead_code)]
    FloatTolerance,
}

impl Comparison {
    /// The comparison a column of `value_type` uses.
    fn for_type(value_type: ValueType) -> Comparison {
        match value_type {
            ValueType::Numeric => Comparison::DecimalByValue,
            ValueType::Text | ValueType::Boolean | ValueType::Uuid => Comparison::Exact,
        }
    }

    fn equal(self, expected: &Option<String>, got: &Option<String>) -> bool {
        match self {
            Comparison::Exact => expected == got,
            Comparison::DecimalByValue => match (expected, got) {
                (None, None) => true,
                (Some(a), Some(b)) => match (Numeric::parse(a), Numeric::parse(b)) {
                    (Ok(a), Ok(b)) => a.compare(&b) == Ordering::Equal,
                    // A value that doesn't parse as a decimal can't be
                    // compared by value; fall back to exact so a genuine
                    // difference still surfaces rather than being swallowed.
                    _ => a == b,
                },
                _ => false,
            },
            Comparison::FloatTolerance => {
                unimplemented!(
                    "float tolerance comparison: the value language has no float type yet \
                     (ValueType has no Float variant), so this is unreachable; implement a \
                     combined absolute+relative tolerance here when floats are added"
                )
            }
        }
    }
}

/// One differing cell, row, or column found while comparing a candidate
/// rendering against the SQL-oracle authority. `expected` is always the SQL
/// oracle's value; `got` is the candidate's (target or evaluator).
#[derive(Debug, Clone, PartialEq)]
pub enum Divergence {
    Cell {
        table: String,
        pk: String,
        column: String,
        expected: Option<String>,
        got: Option<String>,
    },
    /// A row the SQL oracle produced but the candidate is missing.
    MissingRow { table: String, pk: String },
    /// A row the candidate has but the SQL oracle did not produce.
    ExtraRow { table: String, pk: String },
    /// A column the SQL oracle produced but the candidate's row is missing.
    MissingColumn {
        table: String,
        pk: String,
        column: String,
    },
    /// A column the candidate's row has but the SQL oracle did not produce.
    ExtraColumn {
        table: String,
        pk: String,
        column: String,
    },
}

fn show(value: &Option<String>) -> String {
    match value {
        Some(text) => text.clone(),
        None => "NULL".to_string(),
    }
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Divergence::Cell {
                table,
                pk,
                column,
                expected,
                got,
            } => write!(
                f,
                "{table}[{pk}].{column}: expected={} got={}",
                show(expected),
                show(got)
            ),
            Divergence::MissingRow { table, pk } => {
                write!(
                    f,
                    "{table}[{pk}]: present in SQL oracle, absent in candidate"
                )
            }
            Divergence::ExtraRow { table, pk } => {
                write!(
                    f,
                    "{table}[{pk}]: present in candidate, absent in SQL oracle"
                )
            }
            Divergence::MissingColumn { table, pk, column } => write!(
                f,
                "{table}[{pk}].{column}: present in SQL oracle, absent in candidate"
            ),
            Divergence::ExtraColumn { table, pk, column } => write!(
                f,
                "{table}[{pk}].{column}: present in candidate, absent in SQL oracle"
            ),
        }
    }
}

/// The result of a three-way comparison for one definition's target.
///
/// Splits the findings by *which* candidate diverged from the SQL oracle, so
/// a caller learns which layer is wrong, not merely that something is
/// (design doc §2). Prints the whole [`Program`] on any divergence — a diff
/// without the program is unactionable (design doc §5).
#[derive(Debug, Clone)]
pub struct ThreeWayReport {
    /// `persisted target ≠ SQL oracle` — a pipeline/apply/fold/ordering bug.
    pub target_vs_sql: Vec<Divergence>,
    /// `evaluator ≠ SQL oracle` — an eval-layer drift bug (the ADR-0004
    /// "engine mirrors Postgres" claim, tested directly).
    pub evaluator_vs_sql: Vec<Divergence>,
    program: String,
}

impl ThreeWayReport {
    /// Whether any comparison diverged.
    pub fn diverged(&self) -> bool {
        !self.target_vs_sql.is_empty() || !self.evaluator_vs_sql.is_empty()
    }
}

impl fmt::Display for ThreeWayReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.diverged() {
            return write!(f, "no divergence");
        }
        if !self.target_vs_sql.is_empty() {
            writeln!(
                f,
                "target != SQL oracle (localizes a pipeline/apply/fold/ordering bug):"
            )?;
            for divergence in &self.target_vs_sql {
                writeln!(f, "  {divergence}")?;
            }
        }
        if !self.evaluator_vs_sql.is_empty() {
            writeln!(
                f,
                "evaluator != SQL oracle (localizes an eval-layer drift bug):"
            )?;
            for divergence in &self.evaluator_vs_sql {
                writeln!(f, "  {divergence}")?;
            }
        }
        write!(f, "program:\n{}", self.program)
    }
}

/// Quotes a Postgres identifier for safe interpolation into rendered SQL,
/// mirroring `engine::pool`'s own (crate-private) helper of the same name.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Renders a numeric calculated-field expression back to Postgres SQL text.
///
/// Deliberately independent of the engine's evaluator *and* of
/// `engine::defs::oracle::render_expr_sql` — the oracle's whole value is that
/// its `SELECT` shares no code with the thing it checks. Scoped to the
/// numeric-`+` slice (issue #5): anything beyond it panics naming the missing
/// work rather than guessing (design doc §2 "refuse to guess"), so the day
/// the generator widens, this fails loudly instead of emitting false
/// differentials.
fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => quote_ident(name),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::BinaryOp {
            op: Operator::Add,
            lhs,
            rhs,
        } => format!("({} + {})", render_expr(lhs), render_expr(rhs)),
        Expr::BinaryOp {
            op: Operator::GreaterThan,
            ..
        } => panic!(
            "oracle: `>` comparison rendering is out of scope for the numeric-+ slice \
             (issue #65 widens the grammar); extend `render_expr` when the generator emits it"
        ),
        Expr::StringLiteral(_) => panic!(
            "oracle: text-literal rendering is out of scope for the numeric-+ slice \
             (issue #63 widens the value model); extend `render_expr` when the generator emits it"
        ),
        Expr::FunctionCall { name, .. } => panic!(
            "oracle: function-call rendering ({name}) is out of scope for the numeric-+ slice \
             (issue #64 adds functions); extend `render_expr` when the generator emits it"
        ),
        Expr::RelationshipPath { rel, column } => panic!(
            "oracle: relationship-path rendering ('{rel}.{column}') is out of scope for the \
             numeric-+ slice (issue #25 is grammar + AST only; no generator support yet); \
             extend `render_expr` when the generator emits it"
        ),
    }
}

/// Renders a 1-1 `def` back to `SELECT <pk>, (<expr>) AS <field>, ... FROM
/// <source>`. Every projection is cast to `text` so the read-back matches the
/// rendered-text shape [`crate::backend::Snapshot`] uses.
///
/// # Panics
///
/// On any shape beyond the numeric-`+` 1-1 slice (issue #5), naming the
/// follow-up that widens it — an aggregate `GROUP BY` is issue #10; a
/// non-trivial predicate is a later slice.
fn render_select(def: &TransformDef, pk_column: &str) -> String {
    match &def.key_space {
        KeySpace::OneToOne => {}
        KeySpace::Aggregate { .. } => panic!(
            "oracle: aggregate GROUP BY rendering is the aggregate slice (issue #10), not the \
             numeric-+ 1-1 slice (issue #5); the oracle refuses to guess at an unmodeled shape"
        ),
    }
    match def.predicate {
        Predicate::True => {}
    }

    let mut select_list = vec![format!("{}::text", quote_ident(pk_column))];
    for field in &def.fields {
        select_list.push(format!(
            "({})::text as {}",
            render_expr(&field.expr),
            quote_ident(&field.name)
        ));
    }

    format!(
        "select {} from {}",
        select_list.join(", "),
        quote_ident(&def.source)
    )
}

/// Runs `def` rendered as a `SELECT` (see [`render_select`]) in `pool`'s
/// cluster and reads the result back into field-only [`Rows`] keyed by the
/// primary key. The authority side of the three-way comparison.
pub async fn sql_oracle(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
) -> Result<Rows, OracleError> {
    let sql = render_select(def, pk_column);
    let client = pool.get().await?;
    let db_rows = client.query(sql.as_str(), &[]).await?;

    let mut rows: Rows = BTreeMap::new();
    for db_row in db_rows {
        let pk: Option<String> = db_row.get(0);
        let pk = pk.expect("primary key column is never NULL");
        let mut by_column = BTreeMap::new();
        for (i, field) in def.fields.iter().enumerate() {
            by_column.insert(field.name.clone(), db_row.get::<_, Option<String>>(i + 1));
        }
        rows.insert(pk, by_column);
    }
    Ok(rows)
}

/// Recomputes `def`'s target with the engine's own evaluator
/// ([`engine::defs::oracle::recompute`]) and renders each [`Value`] to text,
/// into field-only [`Rows`] keyed by the primary key. The evaluator side of
/// the three-way comparison — a secondary parity check on the mirror-Postgres
/// claim, never the authority.
///
/// [`Value`]: engine::defs::eval::Value
pub async fn evaluator_oracle(
    pool: &Pool,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<Rows, OracleError> {
    let recomputed = recompute(pool, def, pk_column, source_columns).await?;
    let mut rows: Rows = BTreeMap::new();
    for (pk, fields) in recomputed {
        let by_column = fields
            .into_iter()
            .map(|(column, value)| (column, value.map(|v| v.to_string())))
            .collect();
        rows.insert(pk, by_column);
    }
    Ok(rows)
}

/// Strips the primary-key column from a target's rows as read back by
/// [`crate::backend::Backend::snapshot`], yielding the field-only [`Rows`] the
/// three-way comparison expects. The pk stays as each row's key.
pub fn target_fields(target: &Rows, pk_column: &str) -> Rows {
    target
        .iter()
        .map(|(pk, columns)| {
            let fields = columns
                .iter()
                .filter(|(name, _)| name.as_str() != pk_column)
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            (pk.clone(), fields)
        })
        .collect()
}

/// Diffs a `candidate` rendering against the SQL-oracle `authority`,
/// classifying each cell by the comparison its field's [`ValueType`] uses.
/// A column present on one side and absent on the other is a divergence, not
/// a silent skip (design doc §5).
fn diff(
    table: &str,
    authority: &Rows,
    candidate: &Rows,
    comparison_for: &dyn Fn(&str) -> Comparison,
) -> Vec<Divergence> {
    let mut divergences = Vec::new();

    let mut pks: BTreeMap<&String, ()> = BTreeMap::new();
    for pk in authority.keys().chain(candidate.keys()) {
        pks.insert(pk, ());
    }

    for pk in pks.keys() {
        match (authority.get(*pk), candidate.get(*pk)) {
            (Some(expected), Some(got)) => {
                let mut columns: BTreeMap<&String, ()> = BTreeMap::new();
                for column in expected.keys().chain(got.keys()) {
                    columns.insert(column, ());
                }
                for column in columns.keys() {
                    match (expected.get(*column), got.get(*column)) {
                        (Some(a), Some(b)) => {
                            if !comparison_for(column).equal(a, b) {
                                divergences.push(Divergence::Cell {
                                    table: table.to_string(),
                                    pk: (*pk).clone(),
                                    column: (*column).clone(),
                                    expected: a.clone(),
                                    got: b.clone(),
                                });
                            }
                        }
                        (Some(_), None) => divergences.push(Divergence::MissingColumn {
                            table: table.to_string(),
                            pk: (*pk).clone(),
                            column: (*column).clone(),
                        }),
                        (None, Some(_)) => divergences.push(Divergence::ExtraColumn {
                            table: table.to_string(),
                            pk: (*pk).clone(),
                            column: (*column).clone(),
                        }),
                        (None, None) => unreachable!("column came from the union of both sides"),
                    }
                }
            }
            (Some(_), None) => divergences.push(Divergence::MissingRow {
                table: table.to_string(),
                pk: (*pk).clone(),
            }),
            (None, Some(_)) => divergences.push(Divergence::ExtraRow {
                table: table.to_string(),
                pk: (*pk).clone(),
            }),
            (None, None) => unreachable!("pk came from the union of both sides"),
        }
    }

    divergences
}

/// Compares the three field-only renderings of `def`'s target against the SQL
/// oracle `sql`, producing a localized [`ThreeWayReport`].
///
/// Field types drive the per-cell comparison: every field the numeric-`+`
/// slice produces is [`ValueType::Numeric`] (so decimals compare by value),
/// but this resolves each field's type from `def` rather than assuming it, so
/// it stays correct as the grammar widens.
pub fn three_way(
    program: &Program,
    def: &TransformDef,
    target: &Rows,
    evaluator: &Rows,
    sql: &Rows,
) -> ThreeWayReport {
    let field_types: HashMap<&str, ValueType> = def
        .fields
        .iter()
        .map(|f| (f.name.as_str(), field_value_type(&f.expr)))
        .collect();
    let comparison_for = |column: &str| {
        field_types
            .get(column)
            .map(|vt| Comparison::for_type(*vt))
            .unwrap_or(Comparison::Exact)
    };

    ThreeWayReport {
        target_vs_sql: diff(&def.target, sql, target, &comparison_for),
        evaluator_vs_sql: diff(&def.target, sql, evaluator, &comparison_for),
        program: format!("{program:#?}"),
    }
}

/// The [`ValueType`] a numeric-`+`-slice field expression produces. Panics on
/// any shape [`render_expr`] also refuses, keeping the two in lockstep so the
/// oracle never infers a type for a shape it can't render.
fn field_value_type(expr: &Expr) -> ValueType {
    match expr {
        // A column reference in the numeric-+ slice is always Numeric — the
        // only column type a rendered field can reference here (the pk and
        // every addable column). When the grammar widens to Text/Boolean/Uuid
        // passthrough fields, this must consult the source schema.
        Expr::Column(_) | Expr::NumberLiteral(_) => ValueType::Numeric,
        Expr::BinaryOp {
            op: Operator::Add, ..
        } => ValueType::Numeric,
        _ => {
            // render_expr already panics on these with the follow-up issue
            // named; reaching here would mean the two drifted out of sync.
            render_expr(expr);
            unreachable!("render_expr panics on every shape field_value_type does not model")
        }
    }
}

/// Runs the full three-way comparison for a 1-1 `def`: reads the SQL oracle
/// and the evaluator recompute from `pool`, strips the pk column from the
/// caller-supplied persisted `target` rows (as read back by
/// [`crate::backend::Backend::snapshot`]), and diffs both against the SQL
/// oracle.
#[allow(clippy::too_many_arguments)]
pub async fn check(
    pool: &Pool,
    program: &Program,
    def: &TransformDef,
    pk_column: &str,
    source_columns: &HashMap<String, ValueType>,
    target: &Rows,
) -> Result<ThreeWayReport, OracleError> {
    let sql = sql_oracle(pool, def, pk_column).await?;
    let evaluator = evaluator_oracle(pool, def, pk_column, source_columns).await?;
    let target = target_fields(target, pk_column);
    Ok(three_way(program, def, &target, &evaluator, &sql))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A nested slice of literals is the most legible way to spell a fixture
    // inline; the complexity lint isn't worth a wrapper type for a test.
    #[allow(clippy::type_complexity)]
    fn rows(entries: &[(&str, &[(&str, Option<&str>)])]) -> Rows {
        entries
            .iter()
            .map(|(pk, cols)| {
                let by_column = cols
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.map(|v| v.to_string())))
                    .collect();
                (pk.to_string(), by_column)
            })
            .collect()
    }

    fn numeric_kind(_: &str) -> Comparison {
        Comparison::DecimalByValue
    }

    #[test]
    fn decimals_compare_equal_ignoring_scale() {
        assert!(Comparison::DecimalByValue.equal(&Some("2.50".into()), &Some("2.5".into())));
        assert!(Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.50".into())));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &Some("2.6".into())));
        assert!(Comparison::DecimalByValue.equal(&None, &None));
        assert!(!Comparison::DecimalByValue.equal(&Some("2.5".into()), &None));
    }

    #[test]
    fn exact_comparison_distinguishes_text_scale() {
        // The exact path must NOT treat "1.50" and "1.500" as equal — only
        // the decimal path does. This is why comparison is per-type.
        assert!(!Comparison::Exact.equal(&Some("1.50".into()), &Some("1.500".into())));
        assert!(Comparison::Exact.equal(&Some("hi".into()), &Some("hi".into())));
    }

    #[test]
    fn a_present_absent_cell_is_a_divergence_not_a_skip() {
        let authority = rows(&[("1", &[("total", Some("5"))])]);
        let candidate = rows(&[("1", &[])]);
        let divergences = diff("t", &authority, &candidate, &numeric_kind);
        assert_eq!(
            divergences,
            vec![Divergence::MissingColumn {
                table: "t".into(),
                pk: "1".into(),
                column: "total".into(),
            }]
        );
    }

    #[test]
    fn a_missing_row_is_a_divergence() {
        let authority = rows(&[("1", &[("total", Some("5"))])]);
        let candidate = rows(&[]);
        let divergences = diff("t", &authority, &candidate, &numeric_kind);
        assert_eq!(
            divergences,
            vec![Divergence::MissingRow {
                table: "t".into(),
                pk: "1".into(),
            }]
        );
    }

    #[test]
    fn a_decimal_cell_matches_across_scale_but_a_wrong_value_diverges() {
        let authority = rows(&[("1", &[("total", Some("2.50"))])]);
        let equal = rows(&[("1", &[("total", Some("2.5"))])]);
        assert!(diff("t", &authority, &equal, &numeric_kind).is_empty());

        let wrong = rows(&[("1", &[("total", Some("2.6"))])]);
        assert_eq!(
            diff("t", &authority, &wrong, &numeric_kind),
            vec![Divergence::Cell {
                table: "t".into(),
                pk: "1".into(),
                column: "total".into(),
                expected: Some("2.50".into()),
                got: Some("2.6".into()),
            }]
        );
    }

    #[test]
    fn report_display_includes_the_program_and_the_diverging_comparison() {
        let program = Program {
            tables: Vec::new(),
            defs: Vec::new(),
            ops: Vec::new(),
        };
        let report = ThreeWayReport {
            target_vs_sql: vec![Divergence::Cell {
                table: "d0".into(),
                pk: "1".into(),
                column: "total".into(),
                expected: Some("5".into()),
                got: Some("4".into()),
            }],
            evaluator_vs_sql: Vec::new(),
            program: format!("{program:#?}"),
        };
        let printed = report.to_string();
        assert!(printed.contains("target != SQL oracle"));
        assert!(!printed.contains("evaluator != SQL oracle"));
        assert!(printed.contains("d0[1].total: expected=5 got=4"));
        assert!(printed.contains("program:"));
        assert!(printed.contains("Program"));
    }

    #[test]
    fn render_select_renders_a_one_to_one_numeric_add() {
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::OneToOne,
            fields: vec![engine::defs::ast::FieldDef {
                name: "total".into(),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column("c1".into())),
                    rhs: Box::new(Expr::Column("c2".into())),
                },
            }],
            predicate: Predicate::True,
        };
        assert_eq!(
            render_select(&def, "c0"),
            "select \"c0\"::text, ((\"c1\" + \"c2\"))::text as \"total\" from \"t0\""
        );
    }

    #[test]
    #[should_panic(expected = "aggregate GROUP BY rendering is the aggregate slice (issue #10)")]
    fn render_select_panics_on_an_aggregate_key_space() {
        let def = TransformDef {
            target: "d0".into(),
            source: "t0".into(),
            key_space: KeySpace::Aggregate {
                group_by: vec!["c1".into()],
            },
            fields: Vec::new(),
            predicate: Predicate::True,
        };
        let _ = render_select(&def, "c0");
    }
}
