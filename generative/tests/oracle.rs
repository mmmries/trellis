//! Proves the Postgres-SQL oracle and its comparison with the persisted
//! target (`generative::oracle`): render a 1-1 numeric-`+` definition back
//! to a `SELECT`, run it against the same `testkit` cluster the program ran
//! in, and confirm a corrupted target is reported as `target ≠ SQL`. Comparator/report unit tests live
//! beside the code in `src/oracle/mod.rs`; these are the ones that need a real
//! database.

use std::collections::HashMap;

use generative::backend::{Backend, ManualBackend};
use generative::model::{NamePool, Op, OpOutcome, Program, Table};
use generative::oracle::{self, sql_oracle};
use testkit::TestCluster;
use trellis::Config;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};
use trellis::dev::defs::qualified_target_table;
use trellis::dev::pool::Pool;

/// The same 1-1 numeric-`+` program the backend seam test uses, so the oracle
/// is exercised against a shape the backend already proves it can maintain.
fn numeric_add_program() -> (Program, TransformDef, Table, String) {
    let mut pool = NamePool::new();
    let source = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Numeric]);
    let a = source.columns[1].name.clone();
    let b = source.columns[2].name.clone();
    let target_name = pool.next_table_name();

    let def = TransformDef {
        target: target_name.clone(),
        source: source.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(a.clone())),
                rhs: Box::new(Expr::Column(b.clone())),
            },
        }],
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };

    let program = Program {
        tables: vec![source.clone()],
        defs: vec![def.clone()],
        def_install_after_op: vec![0],
        ops: vec![
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("1".to_string())),
                    (a.clone(), Some("10.00".to_string())),
                    (b.clone(), Some("1.50".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
            Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some("2".to_string())),
                    (a.clone(), Some("20.00".to_string())),
                    (b.clone(), Some("2.00".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            },
        ],
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };

    (program, def, source.clone(), target_name)
}

fn source_columns(table: &Table) -> HashMap<String, ValueType> {
    table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.value_type))
        .collect()
}

async fn settle(backend: &mut ManualBackend, program: &Program) {
    backend.install(program).await.expect("install program");
    for op in &program.ops {
        backend.apply(op).await.expect("apply op");
    }
    backend.quiesce().await.expect("quiesce");
}

#[tokio::test]
async fn sql_oracle_matches_the_persisted_target_and_a_hand_computed_expected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();
    let columns = source_columns(&source);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;
    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot[&target_name].clone();

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // The SQL oracle equals a hand-computed expected: 10.00+1.50, 20.00+2.00.
    let sql = sql_oracle(&pool, &def, &pk_column)
        .await
        .expect("sql oracle");
    assert_eq!(sql["1"]["total"], Some("11.50".to_string()));
    assert_eq!(sql["2"]["total"], Some("22.00".to_string()));

    // And the full oracle check reports no divergence at all.
    let report = oracle::check(&pool, &program, &def, &pk_column, &columns, &target)
        .await
        .expect("oracle check");
    assert!(
        !report.diverged(),
        "healthy run must not diverge:\n{report}"
    );
}

#[tokio::test]
async fn an_injected_target_corruption_reads_as_target_not_sql() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (program, def, source, target_name) = numeric_add_program();
    let pk_column = source.pk_col.clone();
    let columns = source_columns(&source);

    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    settle(&mut backend, &program).await;

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");

    // Corrupt the persisted target directly — not via a source change, so the
    // maintenance pipeline won't correct it. Source is untouched, so the SQL
    // oracle still computes the right value.
    let qualified = qualified_target_table("public", &def);
    let client = pool.get().await.expect("pool connection");
    client
        .execute(
            &format!("update {qualified} set \"total\" = 999 where \"{pk_column}\" = 1"),
            &[],
        )
        .await
        .expect("corrupt target");

    let snapshot = backend.snapshot().await.expect("snapshot");
    let target = snapshot[&target_name].clone();

    let report = oracle::check(&pool, &program, &def, &pk_column, &columns, &target)
        .await
        .expect("oracle check");

    assert!(report.diverged(), "corruption must be caught");
    assert_eq!(
        report.target_vs_sql.len(),
        1,
        "corruption localizes to target != SQL: {report}"
    );
}

/// The exact-sum oracle (#1119) reads what Postgres's own `sum()`/`avg()`
/// read wherever those don't depend on scan order: a `double precision` or
/// `real` sum of two values (one correctly rounded addition), a single
/// value's `AVG`, and an `interval` group whose partial sums stay in range,
/// its `SUM` and `AVG` printed character for character as Postgres prints
/// them. Values are random bit patterns and fields, with the specials.
#[tokio::test]
async fn the_exact_sum_oracle_reads_postgres_where_postgres_is_order_free() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = db.connect_raw().await;
    raw.batch_execute(
        "create table public.es (id bigint primary key, g int, f float8, r real, iv interval); \
         create table public.es_one (id bigint primary key, g int, f float8)",
    )
    .await
    .expect("tables");
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let double = |next: &mut dyn FnMut() -> u64| -> String {
        match next() % 20 {
            0 => "NaN".to_string(),
            1 => "Infinity".to_string(),
            2 => "-Infinity".to_string(),
            3 => "-0".to_string(),
            // Subnormals.
            4 => format!("{:e}", f64::from_bits(next() % (1 << 52))),
            _ => {
                let v = f64::from_bits(next());
                if v.is_finite() {
                    format!("{v:e}")
                } else {
                    "1".to_string()
                }
            }
        }
    };
    let mut id = 0i64;
    for g in 0..300i32 {
        let fields = |next: &mut dyn FnMut() -> u64| {
            let signed = |v: u64, m: u64| (v % m) as i64 - (m / 2) as i64;
            format!(
                "{} mons {} days {} microseconds",
                signed(next(), 2_000_000),
                signed(next(), 20_000_000),
                signed(next(), 1 << 60)
            )
        };
        for _ in 0..2 {
            id += 1;
            let f = double(&mut next);
            // A `real` from a double's bits, in range.
            let r = match double(&mut next).parse::<f64>().map(|v| v as f32) {
                Ok(v) if v.is_finite() || v.is_nan() => format!("{v:e}"),
                _ => "Infinity".to_string(),
            };
            let iv = fields(&mut next);
            raw.execute(
                "insert into public.es values ($1, $2, $3::text::float8, \
                 ($4::text::float8)::real, $5::text::interval)",
                &[&id, &g, &f, &r, &iv],
            )
            .await
            .unwrap_or_else(|e| panic!("insert {f} {r} {iv}: {e}"));
        }
        let f = double(&mut next);
        raw.execute(
            "insert into public.es_one values ($1, $2, $3::text::float8)",
            &[&id, &g, &f],
        )
        .await
        .expect("insert one");
    }
    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let call = |name: &str, column: &str| FieldDef {
        name: format!("{}_{column}", name.to_lowercase()),
        expr: Expr::FunctionCall {
            name: name.to_string(),
            args: vec![Expr::Column(column.to_string())],
        },
    };
    let def = |source: &str, fields: Vec<FieldDef>| TransformDef {
        target: format!("{source}_t"),
        source: source.to_string(),
        key_space: KeySpace::Aggregate {
            group_by: vec!["g".to_string()],
        },
        fields: std::iter::once(FieldDef {
            name: "g".to_string(),
            expr: Expr::Column("g".to_string()),
        })
        .chain(fields)
        .collect(),
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
        source_key: None,
        explicit_source_schema: Some("public".to_string()),
        explicit_target_schema: None,
    };
    let floats_equal = |a: &str, b: &str| {
        let (a, b): (f64, f64) = (a.parse().expect("a float"), b.parse().expect("a float"));
        a == b || (a.is_nan() && b.is_nan())
    };
    let check = async |def: TransformDef, postgres: &str, floats: &[&str]| {
        let oracle = sql_oracle(&pool, &def, "id").await.expect("sql oracle");
        let rows = raw.query(postgres, &[]).await.expect("postgres");
        assert_eq!(oracle.len(), rows.len());
        for row in rows {
            let g: i32 = row.get(0);
            let ours = &oracle[&generative::model::group_key(&[Some(g.to_string())])];
            for (i, field) in def.fields.iter().skip(1).enumerate() {
                let theirs: Option<String> = row.get(i + 1);
                let mine = ours[&field.name].clone();
                let agree = match (&mine, &theirs) {
                    (Some(a), Some(b)) if floats.contains(&field.name.as_str()) => {
                        floats_equal(a, b)
                    }
                    _ => mine == theirs,
                };
                assert!(
                    agree,
                    "group {g}, {}: oracle {mine:?}, Postgres {theirs:?}",
                    field.name
                );
            }
        }
    };
    check(
        def(
            "es",
            vec![
                call("SUM", "f"),
                call("SUM", "r"),
                call("SUM", "iv"),
                call("AVG", "iv"),
            ],
        ),
        // Postgres's float `AVG` of two values divides its rounded sum, so
        // the float `AVG` is read over a single value below.
        "select g, sum(f)::text, sum(r)::text, sum(iv)::text, avg(iv)::text \
         from public.es group by g",
        &["sum_f", "sum_r"],
    )
    .await;
    check(
        def("es_one", vec![call("AVG", "f"), call("SUM", "f")]),
        "select g, avg(f)::text, sum(f)::text from public.es_one group by g",
        &["avg_f", "sum_f"],
    )
    .await;
}

/// The per-row oracle of a filtered plain 1-1 (#2043): it appends its key
/// condition to the `WHERE` rather than writing a second one, and a key whose
/// row fails the `WHERE` has no row rather than "vanishing".
#[tokio::test]
async fn the_per_row_oracle_reads_a_filtered_one_to_one() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;

    let (mut program, mut def, source, _) = numeric_add_program();
    let (a, b) = (
        source.columns[1].name.clone(),
        source.columns[2].name.clone(),
    );
    // `a / b` raises for `b = 0`, so the oracle falls back to reading row
    // by row; `a > 5` filters.
    def.fields[0].expr = Expr::BinaryOp {
        op: Operator::Divide,
        lhs: Box::new(Expr::Column(a.clone())),
        rhs: Box::new(Expr::Column(b.clone())),
    };
    def.predicate = Predicate::Filter(Expr::BinaryOp {
        op: Operator::GreaterThan,
        lhs: Box::new(Expr::Column(a.clone())),
        rhs: Box::new(Expr::NumberLiteral("5".to_string())),
    });
    program.defs = vec![def.clone()];
    program.ops.clear();
    let mut backend = ManualBackend::connect(db.dsn())
        .await
        .expect("connect manual backend");
    backend.install(&program).await.expect("install program");

    let raw = db.connect_raw().await;
    // 1 passes, 2 fails the WHERE (and would raise), 3 passes and raises.
    raw.batch_execute(&format!(
        "insert into {} ({}, {a}, {b}) values (1, 10, 2), (2, 1, 0), (3, 10, 0)",
        source.name, source.pk_col
    ))
    .await
    .expect("insert");

    let pool = Pool::new(&Config::from_dsn(db.dsn().to_string()).expect("config")).expect("pool");
    let (rows, raised) = oracle::sql_oracle_per_row(&pool, &def, &source.pk_col)
        .await
        .expect("per-row oracle");
    assert_eq!(rows.keys().collect::<Vec<_>>(), ["1"]);
    assert_eq!(raised.into_iter().collect::<Vec<_>>(), ["3".to_string()]);
}
