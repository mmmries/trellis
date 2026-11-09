//! The expression conformance properties: random typed expressions
//! ([`crate::expr_gen`]) checked against a real Postgres.
//!
//! After D2 Postgres computes every value, so the risk in an operator is in
//! what Rust still does: type inference, the parser's precedence and
//! associativity, rendering the tree back to SQL, and the refusals. Four
//! properties pin those, each against a throwaway table of random rows
//! (`NULL`, extremes, `NaN`, `±Infinity`, infinite dates):
//!
//! 1. **Type.** The column define creates for an admitted expression has the
//!    `format_type` of `CREATE TABLE AS SELECT <the user's text>`.
//! 2. **Value.** `SELECT <the engine's rendered SQL>` and `SELECT <the
//!    user's text>` agree on every row: equal text, or the same SQLSTATE
//!    class.
//! 3. **Round trip.** `parse(render(parse(text))) == parse(text)`, and
//!    parsing the text gives the tree it was written from.
//! 4. **Refusal.** A near miss is refused at define, with an error that
//!    names the construct.
//!
//! Define evaluates no constant (ADR-0022): an expression Postgres folds and
//! fails while planning (`2147483647 + 1`) defines, and its rows fail at run
//! time and are quarantined. The checker treats "admitted, and Postgres
//! raises class 22 on a where-false probe" as agreement, so no plan-time
//! constant failure is a missed refusal. It flags a refusal where Postgres
//! accepts (a false refusal), an admitted expression Postgres fails to plan
//! for another reason (a missed refusal: a type or resolution error define
//! should name), and either side of the literal-zero divisor rule: define
//! refuses a `/` or `%` whose divisor is written as a literal zero, and only
//! that.

use std::collections::HashMap;

use testkit::TestDatabase;
use tokio_postgres::{Client, NoTls};
use trellis::Config;
use trellis::dev::config::DEFAULT_SCHEMA;
use trellis::dev::defs::ast::{Expr, Operator, ValueType};
use trellis::dev::defs::oracle::render_expr_sql;
use trellis::dev::defs::{FloatWidth, IntWidth};
use trellis::dev::defs::{
    PgType, ValidationError, create_target_table, parse, render_definition_text,
    render_qualified_expr_sql, source_primary_key, validate,
};
use trellis::dev::pool::Pool;

use crate::expr_gen::{Column, NearMiss};

/// The throwaway table every expression is run against.
pub const TABLE: &str = "x";

struct Base {
    stem: &'static str,
    pg: &'static str,
    ty: ValueType,
    pool: &'static [&'static str],
}

fn bases() -> Vec<Base> {
    vec![
        Base {
            stem: "i2",
            pg: "smallint",
            ty: ValueType::Integer(IntWidth::Int2),
            pool: &["-32768", "32767", "0", "1", "-1", "100"],
        },
        Base {
            stem: "i4",
            pg: "integer",
            ty: ValueType::Integer(IntWidth::Int4),
            pool: &["-2147483648", "2147483647", "0", "1", "-1", "1000"],
        },
        Base {
            stem: "i8",
            pg: "bigint",
            ty: ValueType::Integer(IntWidth::Int8),
            pool: &[
                "-9223372036854775808",
                "9223372036854775807",
                "0",
                "1",
                "-1",
                "5000000000",
            ],
        },
        Base {
            stem: "n",
            pg: "numeric",
            ty: ValueType::Numeric,
            pool: &[
                "0",
                "1",
                "-1",
                "1.5",
                "0.000001",
                "12345678901234567890.123456789",
                "-99999999999999999999",
                "NaN",
                "100.00",
            ],
        },
        Base {
            stem: "f4",
            pg: "real",
            ty: ValueType::Float(FloatWidth::Float4),
            pool: &[
                "0",
                "-0",
                "1.5",
                "-2.5",
                "NaN",
                "Infinity",
                "-Infinity",
                "3.4028235e+38",
                "1e-45",
                "16777217",
            ],
        },
        Base {
            stem: "f8",
            pg: "double precision",
            ty: ValueType::Float(FloatWidth::Float8),
            pool: &[
                "0",
                "-0",
                "1.5",
                "NaN",
                "Infinity",
                "-Infinity",
                "1.7976931348623157e+308",
                "5e-324",
                "9007199254740993",
            ],
        },
        Base {
            stem: "t",
            pg: "text",
            ty: ValueType::Text,
            pool: &[
                "",
                "a",
                "abc",
                "aXbXc",
                "it's",
                "é",
                "日本語",
                "a b",
                "ab",
                "zzz",
                "  ",
            ],
        },
        Base {
            stem: "b",
            pg: "boolean",
            ty: ValueType::Boolean,
            pool: &["true", "false"],
        },
        Base {
            stem: "d",
            pg: "date",
            ty: ValueType::Other(PgType::Date),
            pool: &[
                "2024-02-29",
                "1970-01-01",
                "infinity",
                "-infinity",
                "4714-11-24 BC",
                "5874897-12-31",
            ],
        },
        Base {
            stem: "u",
            pg: "uuid",
            ty: ValueType::Uuid,
            pool: &[
                "00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12",
            ],
        },
        Base {
            stem: "tm",
            pg: "time",
            ty: ValueType::Other(PgType::Time),
            pool: &["00:00:00", "23:59:59.999999", "12:30:00", "24:00:00"],
        },
        Base {
            stem: "tt",
            pg: "timetz",
            ty: ValueType::Other(PgType::TimeTz),
            pool: &["00:00:00+00", "12:30:00-05", "12:30:00+05", "23:59:59.5+14"],
        },
        Base {
            stem: "tz",
            pg: "timestamptz",
            ty: ValueType::Other(PgType::TimestampTz),
            pool: &[
                "2024-02-29 12:00:00+00",
                "2024-02-29 07:00:00-05",
                "infinity",
                "-infinity",
                "4713-01-01 00:00:00+00 BC",
            ],
        },
        Base {
            stem: "ts",
            pg: "timestamp",
            ty: ValueType::Other(PgType::Timestamp),
            pool: &[
                "2024-02-29 12:00:00",
                "1970-01-01 00:00:00.5",
                "infinity",
                "-infinity",
                "4713-01-01 00:00:00 BC",
            ],
        },
    ]
}

/// The table's data columns (two per type, `<stem>a` and `<stem>b`) and the
/// type define sees for each.
pub fn columns() -> Vec<Column> {
    bases()
        .into_iter()
        .flat_map(|b| {
            ["a", "b"]
                .into_iter()
                .map(move |suffix| (format!("{}{suffix}", b.stem), b.ty))
        })
        .collect()
}

/// Which property a divergence broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Property {
    Type,
    Value,
    RoundTrip,
    Refusal,
    /// Define refused an expression Postgres accepts, by a refusal that is
    /// not a design refusal, or refused a zero divisor not written as a
    /// literal zero.
    FalseRefusal,
    /// Define admitted an expression Postgres fails to plan with an error
    /// other than a data exception (class 22), or one whose divisor is
    /// written as a literal zero.
    MissedRefusal,
}

/// A disagreement between the engine and Postgres.
#[derive(Debug, Clone)]
pub struct Divergence {
    pub property: Property,
    pub text: String,
    pub detail: String,
}

impl std::fmt::Display for Divergence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} divergence for `{}`: {}",
            self.property, self.text, self.detail
        )
    }
}

/// What a checked expression turned out to be, when nothing diverged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Agreement {
    /// Admitted; type and values agree.
    Admitted,
    /// Refused at define (the message), and Postgres raises on a
    /// `REGEXP_COUNT` pattern literal define asks it to judge.
    RefusedAndRaises(String),
    /// Refused at define (the message) for a divisor written as a literal
    /// zero. The rule reads only the text (ADR-0022 lets Trellis be
    /// stricter), so Postgres's verdict doesn't decide it: Postgres raises
    /// for every row with a dividend, and may drop the division unevaluated
    /// (a `CASE` branch no row takes, a strict operator beside `NULL`).
    RefusedByRule(String),
    /// Admitted, and Postgres raises a data exception (the SQLSTATE) while
    /// planning it: a constant it folds fails. Define evaluates no constant,
    /// so the rows fail at run time and are quarantined.
    FailsWhilePlanning(String),
}

/// SQLSTATE `invalid_regular_expression`.
const INVALID_REGEX: &str = "2201B";

/// The SQLSTATE of a failed query.
fn code_of(err: &tokio_postgres::Error) -> String {
    err.code()
        .map(|c| c.code().to_string())
        .unwrap_or_else(|| format!("no SQLSTATE: {err}"))
}

fn class_of(code: &str) -> &str {
    code.get(..2).unwrap_or(code)
}

/// The table, the connection and the engine pool the properties run on.
pub struct Fixture {
    client: Client,
    pool: Pool,
    source_columns: HashMap<String, ValueType>,
    next_target: std::cell::Cell<u32>,
}

impl Fixture {
    /// Creates the table in `db` and fills it with `rows` random rows (plus
    /// the boundary rows).
    pub async fn new(db: &TestDatabase, rows: usize) -> Fixture {
        let (client, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // The session settings the engine's own text parity relies on.
        client
            .batch_execute(&format!(
                "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; \
                 set extra_float_digits to 3"
            ))
            .await
            .expect("session settings");
        let pool =
            Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

        let bases = bases();
        let ddl: Vec<String> = bases
            .iter()
            .flat_map(|b| {
                ["a", "b"]
                    .into_iter()
                    .map(move |s| format!("{}{s} {}", b.stem, b.pg))
            })
            .collect();
        client
            .batch_execute(&format!(
                "create table public.{TABLE} (id integer primary key, {})",
                ddl.join(", ")
            ))
            .await
            .expect("create the table");

        // A deterministic generator, so a failure reproduces from the same
        // rows. Row `k` starts from pool entry `k` of every column (the
        // boundary rows come first), then goes random; one in six cells is
        // NULL and row 0 is all NULL.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let names: Vec<String> = columns().into_iter().map(|(n, _)| n).collect();
        let pools: Vec<&[&str]> = bases.iter().flat_map(|b| [b.pool, b.pool]).collect();
        for id in 0..rows {
            let cells: Vec<String> = pools
                .iter()
                .enumerate()
                .map(|(col, pool)| {
                    let roll = next();
                    if id == 0 || roll % 6 == 0 {
                        return "NULL".to_string();
                    }
                    let pick = if id < pool.len() + 1 && col % 2 == 0 {
                        id - 1
                    } else {
                        (next() as usize) % pool.len()
                    };
                    format!("'{}'", pool[pick % pool.len()].replace('\'', "''"))
                })
                .collect();
            client
                .batch_execute(&format!(
                    "insert into public.{TABLE} (id, {}) values ({id}, {})",
                    names.join(", "),
                    cells.join(", ")
                ))
                .await
                .expect("insert a row");
        }

        let mut source_columns: HashMap<String, ValueType> = columns().into_iter().collect();
        source_columns.insert("id".to_string(), ValueType::Integer(IntWidth::Int4));
        Fixture {
            client,
            pool,
            source_columns,
            next_target: std::cell::Cell::new(0),
        }
    }

    fn definition_text(&self, text: &str) -> String {
        let n = self.next_target.get();
        self.next_target.set(n + 1);
        format!("TRANSFORM tgt_{n} FROM {TABLE} SELECT {text} AS v")
    }

    /// `Some(SQLSTATE)` when `sql` fails.
    async fn raises(&self, sql: &str) -> Option<String> {
        self.client.query(sql, &[]).await.err().map(|e| code_of(&e))
    }

    /// The value of a one-row `select <expression>`, as text, or the
    /// SQLSTATE: what a pin compares a known answer with.
    pub async fn scalar(&self, expression: &str) -> Result<Option<String>, String> {
        match self
            .client
            .query_one(&format!("select ({expression})::text"), &[])
            .await
        {
            Ok(row) => Ok(row.get(0)),
            Err(e) => Err(code_of(&e)),
        }
    }

    /// The outcome of `create table (<word> integer)` with the word unquoted,
    /// which Postgres refuses for a word it reserves: the oracle
    /// behind the grammar's reserved-word refusal.
    pub async fn unquoted_column_name(&self, word: &str) -> Result<(), String> {
        self.client
            .batch_execute(&format!(
                "create temp table reserved_probe ({word} integer)"
            ))
            .await
            .map_err(|e| code_of(&e))?;
        self.client
            .batch_execute("drop table reserved_probe")
            .await
            .map_err(|e| code_of(&e))
    }

    /// The first column of every row, as text, or the SQLSTATE.
    async fn rows(&self, sql: &str) -> Result<Vec<Option<String>>, String> {
        match self.client.query(sql, &[]).await {
            Ok(rows) => Ok(rows.iter().map(|r| r.get::<_, Option<String>>(1)).collect()),
            Err(e) => Err(code_of(&e)),
        }
    }

    /// Checks one expression `text` the generator wrote as `intended`
    /// (when it knows the tree). Every property but Refusal applies.
    pub async fn check(
        &self,
        text: &str,
        intended: Option<&Expr>,
    ) -> Result<Agreement, Divergence> {
        let diverge = |property, detail: String| Divergence {
            property,
            text: text.to_string(),
            detail,
        };
        let def_text = self.definition_text(text);
        let def = match parse(&def_text) {
            Ok(def) => def,
            Err(e) => return Err(diverge(Property::FalseRefusal, format!("parse: {e}"))),
        };
        let expr = def.fields[0].expr.clone();

        // 3. Round trip.
        if let Some(intended) = intended
            && &expr != intended
        {
            return Err(diverge(
                Property::RoundTrip,
                format!("parsed to {expr:?}, written from {intended:?}"),
            ));
        }
        let rendered_def = render_definition_text(&def);
        match parse(&rendered_def) {
            Ok(again) if again == def => {}
            Ok(again) => {
                return Err(diverge(
                    Property::RoundTrip,
                    format!("`{rendered_def}` parsed to {again:?}, not {def:?}"),
                ));
            }
            Err(e) => {
                return Err(diverge(
                    Property::RoundTrip,
                    format!("`{rendered_def}` does not parse: {e}"),
                ));
            }
        }

        // Postgres plans the user's text against no rows: a constant it
        // folds into an error raises here.
        let probe = self
            .raises(&format!("select ({text}) from {TABLE} where false"))
            .await;

        match validate(&def, &self.source_columns) {
            Err(err @ ValidationError::ConstantDivisorZero { .. }) => {
                if !has_literal_zero_divisor(&expr) {
                    return Err(diverge(
                        Property::FalseRefusal,
                        format!("refused ({err}) but no divisor is written as a literal zero"),
                    ));
                }
                Ok(Agreement::RefusedByRule(err.to_string()))
            }
            Err(err) => Err(diverge(
                Property::FalseRefusal,
                format!("a well-typed expression was refused: {err}"),
            )),
            Ok(()) => {
                if has_literal_zero_divisor(&expr) {
                    return Err(diverge(
                        Property::MissedRefusal,
                        "admitted, but a divisor is written as a literal zero".to_string(),
                    ));
                }
                // Define asks Postgres to judge each literal pattern.
                if let Some(message) = self.rejected_pattern(&expr).await {
                    return Ok(Agreement::RefusedAndRaises(message));
                }
                match probe {
                    Some(code) if class_of(&code) == "22" => {
                        return Ok(Agreement::FailsWhilePlanning(code));
                    }
                    Some(code) => {
                        return Err(diverge(
                            Property::MissedRefusal,
                            format!("admitted, but Postgres raises {code} while planning it"),
                        ));
                    }
                    None => {}
                }
                self.check_type(text, &def, &diverge).await?;
                self.check_value(text, &expr, &diverge).await?;
                Ok(Agreement::Admitted)
            }
        }
    }

    /// 1. Type: the target column define creates has the type of `CREATE
    /// TABLE AS SELECT <text>`.
    async fn check_type(
        &self,
        text: &str,
        def: &trellis::dev::defs::ast::TransformDef,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<(), Divergence> {
        let source = format!("public.{}", def.source);
        let pk = source_primary_key(&self.pool, &source)
            .await
            .expect("the table's key");
        let target = &def.target;
        if let Err(e) = create_target_table(
            &self.pool,
            def,
            "public",
            &pk,
            &self.source_columns,
            &source,
        )
        .await
        {
            return Err(diverge(
                Property::Type,
                format!("define cannot create the target: {e}"),
            ));
        }
        let ctas = format!("ctas_{target}");
        let made = self
            .client
            .batch_execute(&format!(
                "create table public.{ctas} as select ({text}) as v from {TABLE} where false"
            ))
            .await;
        let type_of = |table: String| async move {
            self.client
                .query_one(
                    "select format_type(atttypid, atttypmod) from pg_attribute \
                     where attrelid = $1::text::regclass and attname = 'v'",
                    &[&format!("public.{table}")],
                )
                .await
                .map(|r| r.get::<_, String>(0))
        };
        let result = match made {
            Err(e) => Err(diverge(
                Property::MissedRefusal,
                format!(
                    "CREATE TABLE AS fails: {}",
                    e.as_db_error()
                        .map_or_else(|| e.to_string(), ToString::to_string)
                ),
            )),
            Ok(()) => {
                let engine = type_of(target.clone()).await.expect("target column type");
                let postgres = type_of(ctas.clone()).await.expect("ctas column type");
                if engine == postgres {
                    Ok(())
                } else {
                    Err(diverge(
                        Property::Type,
                        format!("define creates `{engine}`, Postgres types it `{postgres}`"),
                    ))
                }
            }
        };
        let _ = self
            .client
            .batch_execute(&format!(
                "drop table if exists public.{ctas}; drop table if exists public.\"{target}\""
            ))
            .await;
        result
    }

    /// 2. Value: the rendered SQL and the user's text agree on every row.
    async fn check_value(
        &self,
        text: &str,
        expr: &Expr,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<(), Divergence> {
        let user = self
            .rows(&format!(
                "select id, ({text})::text from {TABLE} order by id"
            ))
            .await;
        // The build's renderer, then the one a 1-1 apply and a `JOIN` write
        // embed (each column read through an alias): separate code, so each
        // is checked against the user's text.
        for (rendered_sql, from) in [
            (render_expr_sql(expr), TABLE.to_string()),
            (
                render_qualified_expr_sql(expr, "s"),
                format!("{TABLE} as s"),
            ),
        ] {
            let rendered = self
                .rows(&format!(
                    "select id, ({rendered_sql})::text from {from} order by id"
                ))
                .await;
            Self::compare_rows(&user, rendered, &rendered_sql, diverge)?;
        }
        // The two renderers type the expression alike (a value's text can
        // agree across types: `cast(x as bigint)` and `cast(x as numeric)`).
        // Preparing the statement types it without running it.
        let (build, qualified) = (render_expr_sql(expr), render_qualified_expr_sql(expr, "s"));
        if let Ok(statement) = self
            .client
            .prepare(&format!(
                "select ({build}), ({qualified}) from {TABLE} as s"
            ))
            .await
        {
            let columns = statement.columns();
            if columns[0].type_() != columns[1].type_() {
                return Err(diverge(
                    Property::Value,
                    format!(
                        "`{build}` is {} but `{qualified}` is {}",
                        columns[0].type_(),
                        columns[1].type_()
                    ),
                ));
            }
        }
        Ok(())
    }

    /// One renderer's rows against the user's.
    fn compare_rows(
        user: &Result<Vec<Option<String>>, String>,
        rendered: Result<Vec<Option<String>>, String>,
        rendered_sql: &str,
        diverge: &impl Fn(Property, String) -> Divergence,
    ) -> Result<(), Divergence> {
        match (user, rendered) {
            (Ok(u), Ok(r)) if *u == r => Ok(()),
            (Ok(u), Ok(r)) => {
                let at = u.iter().zip(&r).position(|(a, b)| a != b).unwrap_or(0);
                Err(diverge(
                    Property::Value,
                    format!(
                        "row {at}: Postgres gives {:?}, rendered `{rendered_sql}` gives {:?}",
                        u.get(at),
                        r.get(at)
                    ),
                ))
            }
            (Err(u), Err(r)) if class_of(u) == class_of(&r) => Ok(()),
            (u, r) => Err(diverge(
                Property::Value,
                format!(
                    "Postgres {}, rendered `{rendered_sql}` {}",
                    describe(u),
                    describe(&r)
                ),
            )),
        }
    }

    /// The refusal define gives `expr` when a `REGEXP_COUNT` pattern literal
    /// in it is one Postgres rejects (`2201B`): the probe define runs, here
    /// run on the harness's own connection.
    async fn rejected_pattern(&self, expr: &Expr) -> Option<String> {
        let mut stack = vec![expr];
        while let Some(e) = stack.pop() {
            match e {
                Expr::BinaryOp { lhs, rhs, .. } => {
                    stack.push(lhs);
                    stack.push(rhs);
                }
                Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => stack.push(operand),
                Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                    stack.extend(e.logic_children());
                }
                Expr::FunctionCall { name, args } => {
                    if name == "REGEXP_COUNT"
                        && let Some(Expr::StringLiteral(pattern)) = args.get(1)
                    {
                        let probe =
                            format!("select regexp_count('', '{}')", pattern.replace('\'', "''"));
                        if self.raises(&probe).await.as_deref() == Some(INVALID_REGEX) {
                            return Some(format!(
                                "regexp_count's pattern '{pattern}' is rejected by Postgres"
                            ));
                        }
                    }
                    stack.extend(args.iter());
                }
                _ => {}
            }
        }
        None
    }

    /// The Refusal property: `miss` is refused at define and the error
    /// names the construct.
    pub fn check_refusal(&self, miss: &NearMiss) -> Result<String, Divergence> {
        let diverge = |detail: String| Divergence {
            property: Property::Refusal,
            text: miss.text.clone(),
            detail: format!("[{}] {detail}", miss.kind),
        };
        let message = match parse(&self.definition_text(&miss.text)) {
            Err(e) => e.to_string(),
            Ok(def) => match validate(&def, &self.source_columns) {
                Err(e) => e.to_string(),
                Ok(()) => return Err(diverge("define accepted it".to_string())),
            },
        };
        let lower = message.to_lowercase();
        if miss.needles.iter().any(|n| lower.contains(n)) {
            Ok(message)
        } else {
            Err(diverge(format!(
                "the refusal does not name {:?}: {message}",
                miss.needles
            )))
        }
    }
}

fn describe(result: &Result<Vec<Option<String>>, String>) -> String {
    match result {
        Ok(rows) => format!("returns {} rows", rows.len()),
        Err(code) => format!("raises {code}"),
    }
}

/// Whether a `/` or `%` in `expr` has a divisor written as a literal zero
/// (`0`, `0.0`, `-0`, `REAL '0'`, `DOUBLE PRECISION '-0'`, under any `CAST`
/// or prefix `-`): the rule define's `ConstantDivisorZero` refusal states,
/// restated here so the harness checks define against it.
pub fn has_literal_zero_divisor(expr: &Expr) -> bool {
    fn literal_zero(divisor: &Expr) -> bool {
        match divisor {
            Expr::NumberLiteral(text) => {
                let digits = text.trim_start_matches('-');
                digits.chars().all(|c| c == '0' || c == '.') && digits.contains('0')
            }
            Expr::TypedLiteral {
                value_type: ValueType::Float(_),
                text,
            } => matches!(text.as_str(), "0" | "-0"),
            Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => literal_zero(operand),
            _ => false,
        }
    }
    match expr {
        Expr::BinaryOp { op, lhs, rhs } => {
            (matches!(op, Operator::Divide | Operator::Modulo) && literal_zero(rhs))
                || has_literal_zero_divisor(lhs)
                || has_literal_zero_divisor(rhs)
        }
        Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => {
            has_literal_zero_divisor(operand)
        }
        Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => expr
            .logic_children()
            .into_iter()
            .any(has_literal_zero_divisor),
        Expr::FunctionCall { args, .. } => args.iter().any(has_literal_zero_divisor),
        _ => false,
    }
}
