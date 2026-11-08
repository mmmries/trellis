//! Identifiers as Postgres reads them (#1508, ADR-0022): an unquoted name
//! folds to lower case in every name position, a quoted name `"…"` is exact
//! (`""` is a `"`) and never a keyword, a name over 63 bytes is refused, and
//! rendering quotes exactly the names that need it, so the rendered text
//! parses back to the same definition.

use super::ast::{
    AlterClause, DefinitionRef, Expr, FieldDef, JoinKind, JoinOn, JoinSpec, JoinedTable, KeySpace,
    Predicate, QualifiedColumn, Statement, TransformDef, TransformRef, render_definition_text,
};
use super::error::ParseError;
use super::parse;
use super::parser::parse_statement;

fn join_of(def: &TransformDef) -> &JoinSpec {
    match &def.key_space {
        KeySpace::Join(join) | KeySpace::JoinOneToOne(join) => join,
        other => panic!("expected a JOIN, got {other:?}"),
    }
}

fn column(table: &str, column: &str) -> QualifiedColumn {
    QualifiedColumn {
        schema: None,
        table: table.to_string(),
        column: column.to_string(),
    }
}

/// `TRANSFORM`, `FROM`, `JOIN`, their schemas, `KEY (…)`, aliases, `ON`,
/// `GROUP BY`, qualifiers in expressions and `AS` labels all fold.
#[test]
fn unquoted_names_fold_in_every_definition_position() {
    let def = parse(
        "TRANSFORM Rpt.Totals FROM Shop.Orders KEY (Id) AS O \
         JOIN Shop.Items KEY (ItemId) AS I ON O.Id = I.OrderId \
         GROUP BY O.Region SELECT SUM(I.Amount) AS Total, COUNT(*) AS N",
    )
    .unwrap();
    assert_eq!(def.target, "totals");
    assert_eq!(def.explicit_target_schema.as_deref(), Some("rpt"));
    assert_eq!(def.source, "orders");
    assert_eq!(def.explicit_source_schema.as_deref(), Some("shop"));
    assert_eq!(def.source_key, Some(vec!["id".to_string()]));
    let join = join_of(&def);
    assert_eq!(join.source_alias.as_deref(), Some("o"));
    assert_eq!(join.joins[0].right, "items");
    assert_eq!(join.joins[0].explicit_right_schema.as_deref(), Some("shop"));
    assert_eq!(join.joins[0].right_key, Some(vec!["itemid".to_string()]));
    assert_eq!(join.joins[0].right_alias.as_deref(), Some("i"));
    assert_eq!(
        join.joins[0].on,
        [JoinOn {
            lhs: column("orders", "id"),
            rhs: column("items", "orderid"),
        }]
    );
    assert_eq!(join.group_by, [column("orders", "region")]);
    let names: Vec<&str> = def.fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["total", "n"]);
    assert_eq!(
        def.fields[0].expr,
        Expr::FunctionCall {
            name: "SUM".to_string(),
            args: vec![Expr::SourceColumn {
                schema: None,
                table: "items".to_string(),
                column: "amount".to_string(),
            }],
        }
    );

    // A bare column, a field read by another field, a plain `GROUP BY` key
    // and a qualified one naming the `FROM` table.
    let def = parse("TRANSFORM T FROM S SELECT (A * B) AS Total, Total * 2 AS Dbl").unwrap();
    assert_eq!(
        def.fields[1].expr,
        Expr::BinaryOp {
            op: super::ast::Operator::Multiply,
            lhs: Box::new(Expr::Column("total".to_string())),
            rhs: Box::new(Expr::NumberLiteral("2".to_string())),
        }
    );
    let def = parse("TRANSFORM T FROM S GROUP BY OrderId, S.Region SELECT COUNT(*) AS N").unwrap();
    assert_eq!(
        def.key_space,
        KeySpace::Aggregate {
            group_by: vec!["orderid".to_string(), "region".to_string()],
        }
    );
    // A qualified column with its schema.
    let def = parse("TRANSFORM t FROM Shop.S SELECT Shop.S.Amt AS a").unwrap();
    assert_eq!(
        def.fields[0].expr,
        Expr::SourceColumn {
            schema: Some("shop".to_string()),
            table: "s".to_string(),
            column: "amt".to_string(),
        }
    );
}

/// `PAUSE`, `RESUME`, `DROP` and `ALTER TRANSFORM` address the folded name,
/// and an `ALTER` clause's field names fold.
#[test]
fn unquoted_names_fold_in_addressing_statements() {
    let reference = |target: &str, column: Option<&str>| TransformRef {
        target: target.to_string(),
        column: column.map(str::to_string),
    };
    assert_eq!(
        parse_statement("PAUSE TRANSFORM OrderTotals").unwrap(),
        Statement::Pause(reference("ordertotals", None))
    );
    assert_eq!(
        parse_statement("RESUME TRANSFORM OrderTotals.Total").unwrap(),
        Statement::Resume(reference("ordertotals", Some("total")))
    );
    assert_eq!(
        parse_statement("DROP TRANSFORM Rpt.OrderTotals").unwrap(),
        Statement::Drop(DefinitionRef::Transform("rpt.ordertotals".to_string()))
    );
    let Statement::AlterTransform(alter) =
        parse_statement("ALTER TRANSFORM OrderTotals ADD Amount AS Doubled, DROP Old").unwrap()
    else {
        panic!("an ALTER");
    };
    assert_eq!(alter.target, "ordertotals");
    assert_eq!(
        alter.clauses,
        [
            AlterClause::Add(FieldDef {
                name: "doubled".to_string(),
                expr: Expr::Column("amount".to_string()),
            }),
            AlterClause::Drop("old".to_string()),
        ]
    );
    // Quoted, the exact name.
    assert_eq!(
        parse_statement("DROP TRANSFORM \"OrderTotals\"").unwrap(),
        Statement::Drop(DefinitionRef::Transform("OrderTotals".to_string()))
    );
    assert_eq!(
        parse_statement("PAUSE TRANSFORM \"OrderTotals\".\"Total\"").unwrap(),
        Statement::Pause(reference("OrderTotals", Some("Total")))
    );
}

/// A quoted name is exact in every position, as in Postgres.
#[test]
fn quoted_names_are_exact() {
    let def = parse(
        "TRANSFORM \"Rpt\".\"Totals\" FROM \"Shop\".\"Orders\" KEY (\"Id\") AS \"O\" \
         JOIN items AS i ON \"O\".\"Id\" = i.order_id \
         GROUP BY \"O\".\"Region\" SELECT SUM(i.\"Amount\") AS \"Total\"",
    )
    .unwrap();
    assert_eq!(def.target, "Totals");
    assert_eq!(def.explicit_target_schema.as_deref(), Some("Rpt"));
    assert_eq!(def.source, "Orders");
    assert_eq!(def.explicit_source_schema.as_deref(), Some("Shop"));
    assert_eq!(def.source_key, Some(vec!["Id".to_string()]));
    let join = join_of(&def);
    assert_eq!(join.source_alias.as_deref(), Some("O"));
    assert_eq!(join.joins[0].on[0].lhs, column("Orders", "Id"));
    assert_eq!(join.group_by, [column("Orders", "Region")]);
    assert_eq!(def.fields[0].name, "Total");

    // The Prisma-style example the docs give.
    let def =
        parse("TRANSFORM org_users FROM \"User\" GROUP BY \"orgId\" SELECT COUNT(*) AS n").unwrap();
    assert_eq!(def.source, "User");
    assert_eq!(
        def.key_space,
        KeySpace::Aggregate {
            group_by: vec!["orgId".to_string()],
        }
    );
}

/// A quoted alias is exact: a qualifier in another case is another name.
#[test]
fn a_quoted_alias_matches_only_its_exact_spelling() {
    let def = parse("TRANSFORM t FROM a AS \"O\" JOIN b ON \"O\".k = b.k SELECT 1 AS one").unwrap();
    assert_eq!(join_of(&def).joins[0].on[0].lhs, column("a", "k"));
    // `o` is not `"O"`: left for the validator, as any unknown qualifier.
    let def = parse("TRANSFORM t FROM a AS \"O\" JOIN b ON o.k = b.k SELECT 1 AS one").unwrap();
    assert_eq!(join_of(&def).joins[0].on[0].lhs, column("o", "k"));
}

/// `""` inside a quoted name is one `"`.
#[test]
fn a_doubled_quote_is_a_quote() {
    let def = parse("TRANSFORM t FROM s SELECT \"a\"\"b\" AS \"\"\"\"").unwrap();
    assert_eq!(def.fields[0].expr, Expr::Column("a\"b".to_string()));
    assert_eq!(def.fields[0].name, "\"");
}

/// A quoted name is never a keyword, a literal, a reserved word or a
/// function: `"order"`, `"select"`, `"true"`, `"null"`, `"now"` and `"case"`
/// are columns, labels and tables.
#[test]
fn quoted_reserved_words_and_keywords_are_names() {
    for word in [
        "order", "select", "true", "null", "now", "case", "from", "User",
    ] {
        let quoted = format!("\"{word}\"");
        let def = parse(&format!(
            "TRANSFORM {quoted} FROM {quoted} SELECT {quoted} AS {quoted}, \
             {quoted}.{quoted} AS x"
        ))
        .unwrap_or_else(|e| panic!("{word}: {e}"));
        assert_eq!(def.target, word);
        assert_eq!(def.source, word);
        assert_eq!(def.fields[0].expr, Expr::Column(word.to_string()));
        assert_eq!(def.fields[0].name, word);
        assert_eq!(
            def.fields[1].expr,
            Expr::SourceColumn {
                schema: None,
                table: word.to_string(),
                column: word.to_string(),
            }
        );
        let def = parse(&format!(
            "TRANSFORM t FROM a AS {quoted} JOIN b ON {quoted}.k = b.k SELECT 1 AS one"
        ))
        .unwrap_or_else(|e| panic!("{word}: {e}"));
        assert_eq!(join_of(&def).source_alias.as_deref(), Some(word));
    }
    // `NOT` and `CASE` before a quoted name begin an operand.
    let def = parse("TRANSFORM t FROM s SELECT NOT \"x\" AS y").unwrap();
    assert_eq!(
        def.fields[0].expr,
        Expr::Not(Box::new(Expr::Column("x".to_string())))
    );
}

/// The quoted forms Postgres refuses, and the ones this grammar does.
#[test]
fn refused_quoted_forms() {
    let field = |expr: &str| parse(&format!("TRANSFORM t FROM s SELECT {expr} AS x")).unwrap_err();
    assert_eq!(field("\"\""), ParseError::ZeroLengthQuotedIdentifier);
    assert_eq!(field("U&\"d\\0061t\""), ParseError::UnicodeEscapeIdentifier);
    assert_eq!(field("u&\"x\""), ParseError::UnicodeEscapeIdentifier);
    assert_eq!(
        parse("TRANSFORM t FROM s SELECT \"abc AS x").unwrap_err(),
        ParseError::UnterminatedQuotedIdentifier
    );
    for name in ["a.b", "é"] {
        let err = field(&format!("\"{name}\""));
        assert!(
            matches!(&err, ParseError::UnsupportedQuotedIdentifier { name: n, .. } if n == name),
            "{err:?}"
        );
    }
    // `U` alone is a name, and `u &` an operator after it, as in Postgres.
    assert!(parse("TRANSFORM t FROM s SELECT U AS x").is_ok());
    // A quoted function or type name: Postgres looks it up exactly, so
    // `"int4"` is a type there but `"INT4"`, `"integer"` and `"bigint"` are
    // not, and this grammar spells both unquoted.
    for (expr, name) in [
        ("CAST(a AS \"int4\")", "int4"),
        ("CAST(a AS \"INT4\")", "INT4"),
        ("CAST(a AS \"integer\")", "integer"),
        ("CAST(NULL AS \"bigint\")", "bigint"),
        ("CAST('1' AS \"numeric\")", "numeric"),
        ("\"lower\"(t)", "lower"),
        ("\"SUM\"(a)", "SUM"),
        ("\"date\" '2024-01-31'", "date"),
    ] {
        let err = field(expr);
        assert!(
            matches!(&err, ParseError::UnsupportedQuotedIdentifier { name: n, .. } if n == name),
            "{expr}: {err:?}"
        );
        assert!(err.to_string().contains("written unquoted"), "{err}");
    }
}

/// An unquoted table name Postgres reserves is refused where a table is
/// named; the quoted spelling and a qualified one after `.` are names.
#[test]
fn reserved_table_names_are_refused_unquoted() {
    for text in [
        "TRANSFORM t FROM User SELECT id AS id",
        "TRANSFORM t FROM order SELECT id AS id",
        "TRANSFORM order FROM s SELECT id AS id",
        "TRANSFORM t FROM s JOIN order ON s.k = order.k SELECT s.id AS id",
        "TRANSFORM t FROM order.items SELECT id AS id",
    ] {
        let err = parse(text).unwrap_err();
        assert!(
            matches!(&err, ParseError::ReservedTableName { .. }),
            "{text}: {err:?}"
        );
        assert!(err.to_string().contains("quoted"), "{err}");
    }
    assert_eq!(
        parse("TRANSFORM t FROM User SELECT id AS id").unwrap_err(),
        ParseError::ReservedTableName {
            name: "user".to_string()
        }
    );
    for text in [
        "TRANSFORM t FROM \"User\" SELECT id AS id",
        "TRANSFORM t FROM public.order SELECT id AS id",
        "TRANSFORM t FROM s JOIN \"order\" ON s.k = \"order\".k SELECT s.id AS id",
    ] {
        assert!(parse(text).is_ok(), "{text}");
    }
}

/// An unquoted word Postgres reserves is refused where the statement
/// language takes a column (`GROUP BY`, `KEY (...)`, a `JOIN`'s `ON` and
/// `GROUP BY` heads), as Postgres refuses `GROUP BY order` and reads
/// `GROUP BY current_user` as a function; the quoted spelling is the name.
/// Grammar-1 text keeps reading it as the column it named.
#[test]
fn reserved_column_names_are_refused_unquoted_in_the_statement_language() {
    for (refused, quoted) in [
        (
            "TRANSFORM t FROM s GROUP BY order SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY \"order\" SELECT COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM s GROUP BY current_user SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s GROUP BY \"current_user\" SELECT COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM s GROUP BY left.g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM \"left\" GROUP BY \"left\".g SELECT COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM s KEY (id, select) SELECT a AS a",
            "TRANSFORM t FROM s KEY (id, \"select\") SELECT a AS a",
        ),
        (
            "TRANSFORM t FROM s JOIN \"order\" ON s.k = order.k GROUP BY s.g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM s JOIN \"order\" ON s.k = \"order\".k GROUP BY s.g \
             SELECT COUNT(*) AS n",
        ),
        (
            "TRANSFORM t FROM s JOIN b ON s.k = b.k GROUP BY table.g SELECT COUNT(*) AS n",
            "TRANSFORM t FROM \"table\" JOIN b ON \"table\".k = b.k GROUP BY \"table\".g \
             SELECT COUNT(*) AS n",
        ),
    ] {
        let err = parse(refused).unwrap_err();
        assert!(
            matches!(&err, ParseError::ReservedWord { .. }),
            "{refused}: {err:?}"
        );
        assert!(err.to_string().contains("quoted"), "{err}");
        if let Err(err) = parse(quoted) {
            panic!("{quoted}: {err}");
        }
    }
    // A label after a `.` is any word, as in Postgres.
    assert!(parse("TRANSFORM t FROM s GROUP BY s.order SELECT COUNT(*) AS n").is_ok());
    let stored = super::grammar::load_in(
        "TRANSFORM t FROM s GROUP BY order SELECT COUNT(*) AS n",
        1,
        1,
        2,
    )
    .unwrap();
    assert_eq!(
        stored.key_space,
        KeySpace::Aggregate {
            group_by: vec!["order".to_string()]
        }
    );
}

/// Every name position, with `{n}` where the name goes.
const NAME_POSITIONS: &[&str] = &[
    "TRANSFORM {n} FROM s SELECT a AS a",
    "TRANSFORM {n}.t FROM s SELECT a AS a",
    "TRANSFORM t FROM {n} SELECT a AS a",
    "TRANSFORM t FROM {n}.s SELECT a AS a",
    "TRANSFORM t FROM s KEY ({n}) SELECT a AS a",
    "TRANSFORM t FROM s GROUP BY {n} SELECT COUNT(*) AS n",
    "TRANSFORM t FROM s SELECT a AS {n}",
    "TRANSFORM t FROM s SELECT {n} AS a",
    "TRANSFORM t FROM s SELECT s.{n} AS a",
    "TRANSFORM t FROM s AS {n} JOIN b ON {n}.k = b.k GROUP BY b.g SELECT COUNT(*) AS n",
    "TRANSFORM t FROM s JOIN {n} ON s.k = b.k GROUP BY s.g SELECT COUNT(*) AS n",
    "TRANSFORM t FROM s JOIN b ON s.{n} = b.k GROUP BY s.g SELECT COUNT(*) AS n",
    "PAUSE TRANSFORM {n}",
    "RESUME TRANSFORM t.{n}",
    "DROP TRANSFORM {n}",
    "ALTER TRANSFORM {n} DROP a",
    "ALTER TRANSFORM t ADD a AS {n}",
];

/// A name over 63 bytes is refused in every position, after folding and
/// unescaping, naming the 63-byte name Postgres would truncate it to. One of
/// 63 bytes is a name.
#[test]
fn names_over_63_bytes_are_refused_in_every_position() {
    let long = "A".repeat(64);
    let at_limit = "a".repeat(63);
    // `""` unescapes to one byte, so this quoted name is 63 bytes.
    let escaped_at_limit = format!("\"{}\"\"\"", "b".repeat(62));
    let escaped_over = format!("\"{}\"\"\"", "b".repeat(63));
    for position in NAME_POSITIONS {
        let with = |name: &str| parse_statement(&position.replace("{n}", name));
        let err = with(&long).unwrap_err();
        assert_eq!(
            err,
            ParseError::NameTooLong {
                name: "a".repeat(64),
                truncated: at_limit.clone(),
            },
            "{position}"
        );
        assert!(err.to_string().contains("at most 63 bytes"), "{err}");
        assert!(
            matches!(with(&escaped_over), Err(ParseError::NameTooLong { .. })),
            "{position}"
        );
        for fits in [&at_limit, &escaped_at_limit] {
            if let Err(err) = with(fits) {
                panic!("{position} with {fits}: {err}");
            }
        }
    }
}

/// Names [`render_definition_text`] must spell so they read back: every word
/// Postgres reserves, words this grammar reads specially, mixed case,
/// quotes, spaces, digits first, other punctuation, and the longest name.
fn name_corpus() -> Vec<String> {
    let mut names: Vec<String> = super::registry::RESERVED_WORDS
        .iter()
        .map(|w| w.to_string())
        .collect();
    names.extend(
        super::registry::NON_IMMUTABLE_NAMES
            .iter()
            .map(|w| w.to_ascii_lowercase()),
    );
    names.extend(
        [
            "key",
            "without",
            "adopt",
            "existing",
            "source",
            "index",
            "date",
            "double",
            "precision",
            "interval",
            "timestamp",
            "cast",
            "extract",
            "count",
            "sum",
            "coalesce",
            "nullif",
            "is",
            "distinct",
            "outer",
            "inner",
            "u",
            "Total",
            "OrderId",
            "UPPER",
            "a b",
            "a\"b",
            "\"",
            "1abc",
            "_x",
            "x1",
            "a-b",
            "$x",
            "x$",
            "ab'c",
            " ",
        ]
        .map(str::to_string),
    );
    names.push("q".repeat(63));
    names.push("Q".repeat(63));
    names
}

/// One definition per shape, with `name` in each name position in turn and
/// fixed names elsewhere that no corpus name equals.
fn definitions_naming(name: &str) -> Vec<TransformDef> {
    let n = || name.to_string();
    let fixed = |s: &str| s.to_string();
    let one_to_one = |target: String,
                      target_schema: Option<String>,
                      source: String,
                      source_schema: Option<String>,
                      key: Option<Vec<String>>,
                      fields: Vec<FieldDef>| TransformDef {
        target,
        explicit_target_schema: target_schema,
        source,
        explicit_source_schema: source_schema,
        source_key: key,
        key_space: KeySpace::OneToOne,
        fields,
        predicate: Predicate::True,
        without_source_index: false,
        adopt_target: false,
    };
    let field = |name: String, expr: Expr| FieldDef { name, expr };
    let col = |c: String| Expr::Column(c);
    let plain_fields = || vec![field(fixed("zf"), col(fixed("zc")))];
    let mut defs = vec![
        one_to_one(n(), None, fixed("zs"), None, None, plain_fields()),
        one_to_one(
            fixed("zt"),
            Some(n()),
            fixed("zs"),
            None,
            None,
            plain_fields(),
        ),
        one_to_one(fixed("zt"), None, n(), None, None, plain_fields()),
        one_to_one(
            fixed("zt"),
            None,
            fixed("zs"),
            Some(n()),
            None,
            plain_fields(),
        ),
        one_to_one(
            fixed("zt"),
            None,
            fixed("zs"),
            None,
            Some(vec![n(), fixed("zk")]),
            plain_fields(),
        ),
        one_to_one(
            fixed("zt"),
            None,
            fixed("zs"),
            None,
            None,
            vec![
                field(n(), col(fixed("zc"))),
                field(fixed("zf"), col(n())),
                field(
                    fixed("zg"),
                    Expr::SourceColumn {
                        schema: Some(n()),
                        table: n(),
                        column: n(),
                    },
                ),
            ],
        ),
    ];
    let mut aggregate = one_to_one(fixed("zt"), None, fixed("zs"), None, None, plain_fields());
    aggregate.key_space = KeySpace::Aggregate {
        group_by: vec![n(), fixed("zk")],
    };
    aggregate.without_source_index = true;
    defs.push(aggregate);
    let join = |source: String,
                source_alias: Option<String>,
                right: String,
                right_alias: Option<String>,
                on_column: String| {
        let mut def = one_to_one(
            fixed("zt"),
            None,
            source.clone(),
            None,
            Some(vec![fixed("zk")]),
            vec![field(
                fixed("zf"),
                Expr::SourceColumn {
                    schema: None,
                    table: right.clone(),
                    column: on_column.clone(),
                },
            )],
        );
        def.key_space = KeySpace::Join(JoinSpec {
            source_alias,
            joins: vec![JoinedTable {
                kind: JoinKind::Left,
                right: right.clone(),
                explicit_right_schema: None,
                right_key: Some(vec![on_column.clone()]),
                right_alias,
                on: vec![JoinOn {
                    lhs: column(&source, &on_column),
                    rhs: column(&right, "zr"),
                }],
            }],
            group_by: vec![column(&source, &on_column)],
        });
        def
    };
    defs.push(join(n(), None, fixed("zb"), None, fixed("zc")));
    defs.push(join(fixed("za"), None, n(), None, fixed("zc")));
    defs.push(join(fixed("za"), Some(n()), fixed("zb"), None, fixed("zc")));
    defs.push(join(fixed("za"), None, fixed("zb"), Some(n()), fixed("zc")));
    defs.push(join(fixed("za"), Some(fixed("zx")), fixed("zb"), None, n()));
    defs
}

/// `parse(render(def)) == def` for every corpus name in every position.
#[test]
fn rendered_text_parses_back_to_the_same_definition() {
    for name in name_corpus() {
        for def in definitions_naming(&name) {
            let text = render_definition_text(&def);
            match parse(&text) {
                Ok(again) => assert_eq!(again, def, "{name:?}: {text}"),
                Err(err) => panic!("{name:?}: {text}: {err}"),
            }
        }
    }
}

/// A name that is `[a-z_][a-z0-9_]*` and not reserved renders bare; any
/// other is quoted, with `"` doubled.
#[test]
fn rendering_quotes_only_the_names_that_need_it() {
    let def = parse(
        "TRANSFORM \"OrderTotals\" FROM \"Orders\" GROUP BY \"orgId\", region \
         SELECT SUM(\"Amount\") AS \"Total\", SUM(\"order\") AS \"a\"\"b\", COUNT(*) AS n",
    )
    .unwrap();
    assert_eq!(
        render_definition_text(&def),
        "TRANSFORM \"OrderTotals\" FROM \"Orders\" GROUP BY \"orgId\", region \
         SELECT SUM(\"Amount\") AS \"Total\", SUM(\"order\") AS \"a\"\"b\", COUNT(*) AS n"
    );
}
