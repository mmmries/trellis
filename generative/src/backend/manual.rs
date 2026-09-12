//! The manual/single-worker backend (design doc §4 "Two runtimes, one
//! oracle"): harness-driven, one worker, lockstep apply -> quiesce ->
//! compare. Drives the real engine as far as its current 1-1/numeric-`+`
//! subset allows — a real [`engine::Client`] (one staging worker, one
//! application worker) against a real, already-migrated Postgres database,
//! reached only over raw source DML (never an application-level notify
//! API), matching the production ingestion path
//! (`docs/data-flow.md#ingestion-via-logical-replication`).

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use engine::config::DEFAULT_SCHEMA;
use engine::defs::ast::{Expr, KeySpace, Operator, Predicate, TransformDef, ValueType};
use engine::defs::{
    CatalogError, DdlError, install_definition, qualified_target_table, source_primary_key,
};
use engine::staging::{StagingError, await_converged, watermark_token};
use engine::{Client as EngineClient, ClientError, ClientOptions, Config, Pool};
use tokio_postgres::NoTls;

use super::Snapshot;
use crate::model::{Column, Op, Program, Table};

/// How long [`ManualBackend::quiesce`] waits for convergence before giving
/// up. Generous: this backend targets correctness, not latency, and a
/// genuinely stuck pipeline is exactly what should time out loudly rather
/// than hang the test suite forever.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// Failure modes across the manual backend's lifecycle. Composes the
/// engine's own error types via `From` rather than re-wrapping their
/// messages, matching `engine::ClientError`'s own convention.
#[derive(Debug)]
pub enum ManualBackendError {
    /// A definition's key-space isn't representable by this backend's
    /// renderer yet (see [`render_definition`]) — today, only
    /// [`KeySpace::OneToOne`].
    UnsupportedKeySpace,
    /// An op named a table [`ManualBackend::install`] was never given.
    UnknownTable {
        table: String,
    },
    /// [`ManualBackend::connect`] was given no explicitly-named connection
    /// target. Design doc §6: refuse to run against a database the run did
    /// not name, so this stays enforced if a "point at an existing cluster"
    /// mode is ever added.
    UnnamedTarget,
    Config(engine::Error),
    Client(ClientError),
    Catalog(CatalogError),
    Ddl(DdlError),
    Staging(StagingError),
    Db(tokio_postgres::Error),
}

impl From<engine::Error> for ManualBackendError {
    fn from(err: engine::Error) -> Self {
        ManualBackendError::Config(err)
    }
}

impl From<ClientError> for ManualBackendError {
    fn from(err: ClientError) -> Self {
        ManualBackendError::Client(err)
    }
}

impl From<CatalogError> for ManualBackendError {
    fn from(err: CatalogError) -> Self {
        ManualBackendError::Catalog(err)
    }
}

impl From<DdlError> for ManualBackendError {
    fn from(err: DdlError) -> Self {
        ManualBackendError::Ddl(err)
    }
}

impl From<StagingError> for ManualBackendError {
    fn from(err: StagingError) -> Self {
        ManualBackendError::Staging(err)
    }
}

impl From<tokio_postgres::Error> for ManualBackendError {
    fn from(err: tokio_postgres::Error) -> Self {
        ManualBackendError::Db(err)
    }
}

/// Quotes a Postgres identifier for safe interpolation into SQL text,
/// mirroring `engine::pool`'s own (crate-private) helper of the same name.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// The Postgres type name a [`ValueType`] casts to.
fn pg_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
    }
}

/// Renders `def` back to the concrete `TRANSFORM ... FROM ... SELECT ...`
/// syntax [`create_definition`] parses — the manual backend's only reason
/// to exist, since [`crate::model::Program`] stores the parsed AST
/// directly rather than source text. Only [`KeySpace::OneToOne`] is
/// supported (today's generator scope, design doc §1); anything else is a
/// generator bug this backend refuses to guess at.
fn render_definition(def: &TransformDef) -> Result<String, ManualBackendError> {
    if def.key_space != KeySpace::OneToOne {
        return Err(ManualBackendError::UnsupportedKeySpace);
    }
    let fields: Vec<String> = def
        .fields
        .iter()
        .map(|field| format!("{} AS {}", render_expr(&field.expr), field.name))
        .collect();
    debug_assert_eq!(def.predicate, Predicate::True);
    Ok(format!(
        "TRANSFORM {} FROM {} SELECT {}",
        def.target,
        def.source,
        fields.join(", ")
    ))
}

/// Renders a generator-built [`Expr`] back to the source text
/// [`super::install_definition`]/`engine::defs::parser::parse` re-parses.
///
/// A `BinaryOp`'s operands are *unconditionally* parenthesized (issue #67's
/// reviewer follow-up), not only when the operand is itself a lower-
/// precedence `BinaryOp`: with real operator precedence now in the parser
/// (`engine::defs::registry::OPERATORS`), a flat render like `a + b > c`
/// silently reconstructs a *different* tree than a nested one the generator
/// might build — e.g. `Add(a, GreaterThan(b, c))` would round-trip as
/// `a + b > c`, which `+`'s tighter binding re-parses as `Add(a,b) >
/// c` — the wrong tree. Always parenthesizing every operand (`(a) + (b > c)`)
/// is simpler than computing whether a given operand's own precedence
/// requires it, and correct regardless of what tree the generator composes,
/// so it's the shape this renderer commits to before the generator ever
/// nests `+` and `>` together (improvement-plan task B2). See
/// `tests::render_expr_parenthesizes_nested_binary_ops_so_they_round_trip`
/// for the regression pin, and `engine::defs::parser`'s grouping-paren
/// support (issue #67 follow-up) that makes the rendered text re-parseable
/// at all.
fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::NumberLiteral(text) => text.clone(),
        Expr::StringLiteral(text) => format!("'{}'", text.replace('\'', "''")),
        Expr::BinaryOp { op, lhs, rhs } => {
            format!(
                "({}) {} ({})",
                render_expr(lhs),
                render_operator(*op),
                render_expr(rhs)
            )
        }
        Expr::FunctionCall { name, args } => {
            let args: Vec<String> = args.iter().map(render_expr).collect();
            format!("{name}({})", args.join(", "))
        }
        Expr::RelationshipPath { .. } => {
            unreachable!(
                "the generator never constructs a RelationshipPath (issue #25 is grammar + AST only; no generator support yet)"
            )
        }
    }
}

fn render_operator(op: Operator) -> &'static str {
    match op {
        Operator::Add => "+",
        Operator::GreaterThan => ">",
    }
}

/// One row's placeholder assignment for an `INSERT`/`UPDATE` statement:
/// `column = $n::type` (or `column` for the column list), plus the bound
/// text value at that position.
struct Assignment {
    fragment: String,
    value: Option<String>,
}

/// The manual/single-worker backend. Owns a raw connection (DDL, DML,
/// watermark reads, snapshot reads) and, once [`ManualBackend::install`]
/// has run, a live [`EngineClient`] draining sealed batches into every
/// installed definition's target table.
pub struct ManualBackend {
    dsn: String,
    pool: Pool,
    raw: tokio_postgres::Client,
    engine_client: Option<EngineClient>,
    tables: HashMap<String, Table>,
    defs: Vec<TransformDef>,
}

impl ManualBackend {
    /// Connects to `dsn` — an already-migrated Trellis database (see
    /// `testkit::TestCluster::create_isolated_database`) — but installs
    /// nothing yet.
    ///
    /// `dsn` must be given explicitly by the caller (never inferred from an
    /// environment default): design doc §6 wants every run to refuse an
    /// unnamed target, moot today since `testkit` always hands one over
    /// explicitly, but enforced so it stays moot if an external-cluster mode
    /// is ever added. The resolved target is printed so a run's connection
    /// is never silently ambiguous.
    pub async fn connect(dsn: impl Into<String>) -> Result<Self, ManualBackendError> {
        let dsn = dsn.into();
        if dsn.trim().is_empty() {
            return Err(ManualBackendError::UnnamedTarget);
        }
        println!("generative: connecting ManualBackend to {dsn}");
        let config = Config::from_dsn(dsn.clone())?;
        let pool = Pool::new(&config)?;

        let (raw, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!("set search_path to {}, public", config.schema()))
            .await?;

        Ok(Self {
            dsn,
            pool,
            raw,
            engine_client: None,
            tables: HashMap::new(),
            defs: Vec::new(),
        })
    }

    async fn create_source_table(&self, table: &Table) -> Result<(), ManualBackendError> {
        let mut sql = format!("create table {} (", quote_ident(&table.name));
        for (i, column) in table.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(&column.name));
            sql.push(' ');
            sql.push_str(pg_type_name(column.value_type));
            if column.name == table.pk_col {
                sql.push_str(" primary key");
            }
        }
        sql.push(')');
        self.raw.batch_execute(&sql).await?;
        Ok(())
    }

    /// `source_columns` for `table`, as `engine::defs::install_definition`
    /// wants it.
    fn source_columns(table: &Table) -> HashMap<String, ValueType> {
        table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.value_type))
            .collect()
    }

    async fn install_definition(&mut self, def: &TransformDef) -> Result<(), ManualBackendError> {
        let source_table = self
            .tables
            .get(&def.source)
            .ok_or_else(|| ManualBackendError::UnknownTable {
                table: def.source.clone(),
            })?
            .clone();
        let source_columns = Self::source_columns(&source_table);

        let text = render_definition(def)?;

        // Issue #63 C1: `install_definition` is the same front door real
        // callers use — it creates the target table, then tries the fast,
        // set-based direct build first and falls back to the ring-based
        // `create_definition` only for a shape the direct build can't render
        // yet (`BackfillError::Unsupported`, folded into `CatalogError` —
        // see its doc comment). Routing the fuzz harness through it, instead
        // of hand-rolling the same create-table/backfill/persist sequence,
        // keeps this backend exercising the exact path production traffic
        // takes.
        install_definition(&self.pool, &text, &source_columns, "public").await?;
        Ok(())
    }

    fn column(&self, table: &str, column: &str) -> Option<&Column> {
        self.tables
            .get(table)
            .and_then(|t| t.columns.iter().find(|c| c.name == column))
    }

    fn assignment(
        &self,
        table: &str,
        column: &str,
        index: usize,
        value: &Option<String>,
    ) -> Assignment {
        let value_type = self
            .column(table, column)
            .map(|c| c.value_type)
            .unwrap_or(ValueType::Numeric);
        Assignment {
            fragment: format!(
                "{}=${}::text::{}",
                quote_ident(column),
                index,
                pg_type_name(value_type)
            ),
            value: value.clone(),
        }
    }
}

impl super::Backend for ManualBackend {
    type Error = ManualBackendError;

    async fn install(&mut self, program: &Program) -> Result<(), ManualBackendError> {
        for table in &program.tables {
            self.create_source_table(table).await?;
            self.tables.insert(table.name.clone(), table.clone());
        }
        for def in &program.defs {
            self.install_definition(def).await?;
            self.defs.push(def.clone());
        }

        let source_tables: Vec<String> = program
            .tables
            .iter()
            .map(|t| format!("{DEFAULT_SCHEMA}.{}", t.name))
            .collect();
        if !source_tables.is_empty() && self.engine_client.is_none() {
            let options = ClientOptions {
                staging_worker: true,
                application_threads: 1,
                source_tables,
                ..Default::default()
            };
            let client = EngineClient::start(self.dsn.clone(), options)?;
            self.engine_client = Some(client);
        }
        Ok(())
    }

    async fn apply(&mut self, op: &Op) -> Result<u64, ManualBackendError> {
        let affected = match op {
            Op::Insert { table, row, .. } => {
                let columns: Vec<&str> = row.iter().map(|(c, _)| c.as_str()).collect();
                let assignments: Vec<Assignment> = row
                    .iter()
                    .enumerate()
                    .map(|(i, (col, val))| Assignment {
                        fragment: format!(
                            "${}::text::{}",
                            i + 1,
                            pg_type_name(
                                self.column(table, col)
                                    .map(|c| c.value_type)
                                    .unwrap_or(ValueType::Numeric)
                            )
                        ),
                        value: val.clone(),
                    })
                    .collect();
                let column_list = columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let placeholders = assignments
                    .iter()
                    .map(|a| a.fragment.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "insert into {} ({column_list}) values ({placeholders})",
                    quote_ident(table)
                );
                let params: Vec<Option<String>> =
                    assignments.into_iter().map(|a| a.value).collect();
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                self.raw.execute(&sql, &params).await?
            }
            Op::Update {
                table, pk, changes, ..
            } => {
                let pk_col = self
                    .tables
                    .get(table)
                    .map(|t| t.pk_col.clone())
                    .ok_or_else(|| ManualBackendError::UnknownTable {
                        table: table.clone(),
                    })?;
                let mut assignments = Vec::with_capacity(changes.len());
                for (i, (col, val)) in changes.iter().enumerate() {
                    assignments.push(self.assignment(table, col, i + 1, val));
                }
                let pk_type = self
                    .column(table, &pk_col)
                    .map(|c| c.value_type)
                    .unwrap_or(ValueType::Numeric);
                let set_clause = assignments
                    .iter()
                    .map(|a| a.fragment.clone())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "update {} set {set_clause} where {}=${}::text::{}",
                    quote_ident(table),
                    quote_ident(&pk_col),
                    changes.len() + 1,
                    pg_type_name(pk_type),
                );
                let mut params: Vec<Option<String>> =
                    assignments.into_iter().map(|a| a.value).collect();
                params.push(Some(pk.clone()));
                let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
                    .iter()
                    .map(|v| v as &(dyn tokio_postgres::types::ToSql + Sync))
                    .collect();
                self.raw.execute(&sql, &params).await?
            }
            Op::Delete { table, pk, .. } => {
                let pk_col = self
                    .tables
                    .get(table)
                    .map(|t| t.pk_col.clone())
                    .ok_or_else(|| ManualBackendError::UnknownTable {
                        table: table.clone(),
                    })?;
                let pk_type = self
                    .column(table, &pk_col)
                    .map(|c| c.value_type)
                    .unwrap_or(ValueType::Numeric);
                let sql = format!(
                    "delete from {} where {}=$1::text::{}",
                    quote_ident(table),
                    quote_ident(&pk_col),
                    pg_type_name(pk_type),
                );
                self.raw.execute(&sql, &[pk]).await?
            }
        };
        Ok(affected)
    }

    async fn quiesce(&mut self) -> Result<(), ManualBackendError> {
        // Improvement-plan workstream C, task C1: opt-in per-call timing
        // around the watermark -> converge round trip, to test the
        // hypothesis (see `local_docs/generative-suite-improvement-plan.md`)
        // that the convergence property's wall-clock variance is caused by
        // the same ~10s seal age-gate stall suspected in
        // `local_docs/transit-comparison.md` §3.3. Silent and free unless
        // `GENERATIVE_QUIESCE_TIMING` is set: the env lookup happens once per
        // call so the default (unset) path pays exactly one `var_os` check
        // and no clock reads.
        let timing_enabled = std::env::var_os("GENERATIVE_QUIESCE_TIMING").is_some();
        let start = timing_enabled.then(std::time::Instant::now);

        let token = watermark_token(&self.raw).await?;
        let result = await_converged(&self.raw, token, QUIESCE_TIMEOUT).await;

        if let Some(start) = start {
            eprintln!("QUIESCE_TIMING {}", start.elapsed().as_millis());
        }
        result?;
        Ok(())
    }

    async fn snapshot(&mut self) -> Result<Snapshot, ManualBackendError> {
        let mut snapshot: Snapshot = BTreeMap::new();

        for table in self.tables.values() {
            let rows = read_table(
                &self.raw,
                &quote_ident(&table.name),
                &table.pk_col,
                &table.columns,
            )
            .await?;
            snapshot.insert(table.name.clone(), rows);
        }

        for def in &self.defs {
            let pk = source_primary_key(&self.pool, &def.source).await?;
            let target_columns: Vec<Column> = std::iter::once(Column {
                name: pk.name.clone(),
                value_type: ValueType::Numeric,
            })
            .chain(def.fields.iter().map(|f| Column {
                name: f.name.clone(),
                // The physical column type doesn't matter for a `::text`
                // read; only the name is used below.
                value_type: ValueType::Text,
            }))
            .collect();
            let qualified = qualified_target_table("public", def);
            let rows = read_table(&self.raw, &qualified, &pk.name, &target_columns).await?;
            snapshot.insert(def.target.clone(), rows);
        }

        Ok(snapshot)
    }
}

/// Reads `qualified_table` back as text, ordered by `pk_col`, into `pk ->
/// column -> value`.
async fn read_table(
    client: &tokio_postgres::Client,
    qualified_table: &str,
    pk_col: &str,
    columns: &[Column],
) -> Result<BTreeMap<String, BTreeMap<String, Option<String>>>, ManualBackendError> {
    let select_list = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(&c.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select {select_list} from {qualified_table} order by {}",
        quote_ident(pk_col)
    );
    let rows = client.query(&sql, &[]).await?;

    let mut result = BTreeMap::new();
    for row in rows {
        let mut by_column = BTreeMap::new();
        let pk_value: Option<String> = row.get(0);
        let pk_value = pk_value.expect("primary key column is never NULL");
        for (i, column) in columns.iter().enumerate() {
            by_column.insert(column.name.clone(), row.get::<_, Option<String>>(i));
        }
        result.insert(pk_value, by_column);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::defs::parse;

    /// The reviewer-flagged follow-up to issue #67 (real operator
    /// precedence): a nested, mixed-operator `Expr` — `Add(Column("a"),
    /// GreaterThan(Column("b"), Column("c")))`, i.e. the tree a source
    /// author would have to spell `a + (b > c)` to get — must round-trip
    /// through `render_expr` and back through the real parser to the exact
    /// same tree, not a reflowed one.
    ///
    /// Before this fix, `render_expr` rendered this tree flat as
    /// `a + b > c`, which the precedence-climbing parser (issue #67) then
    /// re-parses as `GreaterThan(Add(a, b), c)` — `+` binds tighter than
    /// `>`, so it silently reconstructs the *wrong* tree, one that even
    /// type-checks (`Numeric, Numeric -> Boolean`) even though it isn't what
    /// was rendered. Unconditional parenthesization
    /// (`render_expr`'s doc comment) fixes this by always rendering
    /// `(a) + (b > c)`, which only parses one way regardless of any
    /// operator's precedence.
    #[test]
    fn render_expr_parenthesizes_nested_binary_ops_so_they_round_trip() {
        let expr = Expr::BinaryOp {
            op: Operator::Add,
            lhs: Box::new(Expr::Column("a".to_string())),
            rhs: Box::new(Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column("b".to_string())),
                rhs: Box::new(Expr::Column("c".to_string())),
            }),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(
            def.fields[0].expr, expr,
            "round-tripping through render_expr -> parse must reproduce the exact original \
             tree; without unconditional parenthesization this would silently come back as \
             `GreaterThan(Add(a, b), c)` instead (`+` binds tighter than `>`, so a flat, \
             unparenthesized render loses the original grouping)"
        );
    }

    /// The mirror shape — `GreaterThan(Add(a, b), c)`, i.e. `(a + b) > c` —
    /// which happens to round-trip correctly even *without* parens (since
    /// `+`'s tighter precedence reconstructs the same grouping by accident).
    /// Pinned anyway so a future change to `render_expr` can't quietly regress
    /// this direction while only testing the other one.
    #[test]
    fn render_expr_round_trips_a_greater_than_wrapping_an_add() {
        let expr = Expr::BinaryOp {
            op: Operator::GreaterThan,
            lhs: Box::new(Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("a".to_string())),
                rhs: Box::new(Expr::Column("b".to_string())),
            }),
            rhs: Box::new(Expr::Column("c".to_string())),
        };

        let rendered = render_expr(&expr);
        let text = format!("TRANSFORM t FROM s SELECT {rendered} AS out");
        let def = parse(&text).unwrap_or_else(|e| {
            panic!("rendered expression {rendered:?} must re-parse cleanly: {e:?}")
        });

        assert_eq!(def.fields[0].expr, expr);
    }
}
