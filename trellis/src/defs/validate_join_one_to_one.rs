//! Define-time validation of a 1-1 `JOIN` definition (ADR-0021): the shared
//! `JOIN` rules (tables, `ON` terms and their key types), then the rules of a
//! `JOIN` that keeps one row per `FROM` row.
//!
//! This is the part that needs only the two tables' live columns. The
//! uniqueness of the joined table's `ON` columns and the refusals that read
//! the catalog (partitioned sides, a side that is another 1-1 target, a reader
//! of a 1-1 `JOIN` target) are the catalog's
//! ([`super::catalog`]'s `check_one_to_one_join`).

use std::collections::{HashMap, HashSet};

use super::ValueType;
use super::ast::{Expr, KeySpace, QualifiedColumn, TransformDef};
use super::join::Side;
use super::validate::{
    ColumnTypes, JoinColumnInfo, RESERVED_COLUMN_PREFIX, ValidationError, infer_expr_type_in,
    join_field_value_type, join_resolver, validate_join_tables_and_on,
};

/// Validates a [`KeySpace::JoinOneToOne`] definition against the live columns
/// of its tables (`root` is the `FROM` table's, `joined` each joined table's,
/// by position).
///
/// Admitted when the tables differ; every column is
/// qualified and exists; each `ON` term is an equality between one column of
/// each table, of one type and collation, on the join-key allowlist; no
/// column a hidden table copies is named like one of its ledger columns; and
/// each field is any expression over qualified columns of either table, with
/// no aggregate call, that types. A `WHERE` is [`check_predicate`]'s.
pub fn validate(
    def: &TransformDef,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
) -> Result<(), ValidationError> {
    let KeySpace::JoinOneToOne(join) = &def.key_space else {
        return Ok(());
    };
    let tables = join.joins.len() + 1;
    if join.joins.len() > super::join::MAX_POSITIONS {
        return Err(ValidationError::StarJoinTooManyTables { tables });
    }
    assert_eq!(
        joined.len(),
        join.joins.len(),
        "a 1-1 JOIN is validated against every joined table's columns"
    );
    validate_join_tables_and_on(def, join, root, joined)?;
    let resolve = join_resolver(def, join, root, joined);
    check_key_column_names(def, join)?;

    // The hidden tables copy these columns beside their ledger columns.
    for side in super::join::sides(join) {
        for column in super::join::side_columns(def, side) {
            if column.starts_with(RESERVED_COLUMN_PREFIX) {
                return Err(ValidationError::OneToOneJoinUnsupported {
                    what: format!("the column '{column}'"),
                    remedy: format!(
                        "a column starting with '{RESERVED_COLUMN_PREFIX}' would collide with \
                         the ledger columns Trellis keeps beside the columns it copies; read \
                         a column with another name"
                    ),
                });
            }
        }
    }

    let mut field_names = HashSet::with_capacity(def.fields.len());
    for field in &def.fields {
        if !field_names.insert(field.name.as_str()) {
            return Err(ValidationError::DuplicateFieldName {
                name: field.name.clone(),
            });
        }
        check_field_expr(&field.expr, &field.name, &resolve)?;
        // The field's type over both tables' columns, as define gives its
        // target column.
        join_field_value_type(def, field, root, joined)?;
    }
    if let Some(predicate) = def.predicate.filter() {
        check_predicate(def, predicate, root, joined, &resolve)?;
    }
    super::validate::check_join_collation_reads(def, join, root, joined)?;
    // `R`'s join-key index ends with the entry key, which a fan-out chunk
    // walks a key's entries by: one column past the `ON` columns.
    for table in &join.joins {
        super::validate::check_index_key_width(
            || {
                format!(
                    "each side of ON plus the row key of {}, which a 1-1 JOIN's hidden ledger \
                     indexes together,",
                    def.source
                )
            },
            table.on.len() + 1,
        )?;
    }
    // A ledger copies every column its side's fields read beside its own.
    for (table, columns) in super::join_one_to_one::ledger_widths(def) {
        super::validate::check_table_width(|| table, columns)?;
    }
    Ok(())
}

/// The checks of a 1-1 `JOIN`'s `WHERE`, as Postgres reads it after the
/// joins (ADR-0021): no aggregate call, every column qualified and resolved
/// through a table of the join, type `boolean` (a lone `NULL` is one by
/// position) and total ([`super::totality`]). Any such predicate is admitted,
/// over one table or several, the anti-join included.
fn check_predicate<'a>(
    def: &TransformDef,
    predicate: &Expr,
    root: &HashMap<String, JoinColumnInfo>,
    joined: &[&HashMap<String, JoinColumnInfo>],
    resolve: &impl Fn(&QualifiedColumn) -> Result<(Side, &'a JoinColumnInfo), ValidationError>,
) -> Result<(), ValidationError> {
    let mut aggregate = None;
    predicate.walk(&mut |e| {
        if let Expr::FunctionCall { name, .. } = e
            && aggregate.is_none()
            && super::registry::is_aggregate_name(name)
        {
            aggregate = Some(name.clone());
        }
    });
    if let Some(function) = aggregate {
        return Err(ValidationError::AggregateInPredicate { function });
    }
    check_field_expr(predicate, "WHERE", resolve)?;
    let columns = ColumnTypes::Join { def, root, joined };
    if !matches!(predicate, Expr::Null) {
        let found = infer_expr_type_in("WHERE", predicate, &columns)?;
        if found != ValueType::Boolean {
            return Err(ValidationError::PredicateNotBoolean { found });
        }
    }
    super::totality::check(predicate, &columns).map_err(|refused| {
        ValidationError::PredicateNotTotal {
            construct: refused.construct,
            reason: refused.reason,
        }
    })
}

/// `J` names its key column for each `FROM` column a position's `ON` reads
/// `p<position>_<column>`, cut at Postgres's 63 bytes
/// ([`super::join_one_to_one::j_column`]): two such columns of one position
/// whose names share their first 60 bytes would be one column of `J`.
fn check_key_column_names(
    def: &TransformDef,
    join: &super::ast::JoinSpec,
) -> Result<(), ValidationError> {
    for (position, table) in super::join::positions(join) {
        let mut seen: HashMap<String, String> = HashMap::new();
        for term in &table.on {
            let column = if term.lhs.table == def.source {
                &term.lhs.column
            } else {
                &term.rhs.column
            };
            let name = super::join_one_to_one::j_column(position, column);
            if let Some(earlier) = seen.insert(name, column.clone())
                && earlier != *column
            {
                return Err(ValidationError::OneToOneJoinUnsupported {
                    what: format!(
                        "the columns '{earlier}' and '{column}' of {} in the ON of '{}', whose \
                         names share their first {} bytes",
                        def.source,
                        table.right,
                        crate::defs::lexer::MAX_NAME_BYTES - position_prefix_len(position)
                    ),
                    remedy: "Trellis keeps a key column for each, named after that many bytes \
                             of its name; join on columns whose names differ sooner, or rename \
                             one"
                    .to_string(),
                });
            }
        }
    }
    Ok(())
}

/// The bytes `J`'s `p<position>_` prefix takes.
fn position_prefix_len(position: super::join::Position) -> usize {
    format!("p{}_", position.number()).len()
}

/// Walks one field's expression: every column is qualified and resolves, and
/// no call is an aggregate. Every [`Expr`] node is named, so a new one is a
/// compile error here until its rule is decided.
fn check_field_expr<'a>(
    expr: &Expr,
    field: &str,
    resolve: &impl Fn(&QualifiedColumn) -> Result<(Side, &'a JoinColumnInfo), ValidationError>,
) -> Result<(), ValidationError> {
    match expr {
        Expr::Column(column) => Err(ValidationError::JoinUnqualifiedColumn {
            field: field.to_string(),
            column: column.clone(),
        }),
        Expr::SourceColumn {
            schema,
            table,
            column,
        } => resolve(&QualifiedColumn {
            schema: schema.clone(),
            table: table.clone(),
            column: column.clone(),
        })
        .map(|_| ()),
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::BooleanLiteral(_)
        | Expr::Null
        | Expr::TypedLiteral { .. } => Ok(()),
        Expr::FunctionCall { name, .. } if super::registry::is_aggregate_name(name) => {
            Err(ValidationError::OneToOneJoinAggregate {
                field: field.to_string(),
                function: name.to_ascii_uppercase(),
            })
        }
        Expr::FunctionCall { .. }
        | Expr::BinaryOp { .. }
        | Expr::Negate(_)
        | Expr::Cast { .. }
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::Case { .. } => expr
            .children()
            .into_iter()
            .try_for_each(|child| check_field_expr(child, field, resolve)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;
    use crate::defs::validate::validate_join;
    use crate::defs::{PgType, ValueType};
    use crate::integer::IntWidth;

    fn info(pg_type: &str, value_type: ValueType) -> JoinColumnInfo {
        JoinColumnInfo {
            value_type,
            pg_type: pg_type.to_string(),
            type_oid: match pg_type {
                "integer" => 23,
                _ => 25,
            },
            typmod: -1,
            collation_oid: if pg_type == "text" { 100 } else { 0 },
            collation: None,
            collation_deterministic: true,
            is_enum: false,
            enum_labels: None,
            copy_type: pg_type.to_string(),
            copy_collation: None,
            trellis_target: false,
        }
    }

    fn sides() -> (
        HashMap<String, JoinColumnInfo>,
        HashMap<String, JoinColumnInfo>,
    ) {
        let int = || info("integer", ValueType::Integer(IntWidth::Int4));
        (
            HashMap::from([
                ("cust".to_string(), int()),
                ("total".to_string(), int()),
                ("note".to_string(), info("text", ValueType::Text)),
                ("__basis".to_string(), int()),
            ]),
            HashMap::from([
                ("id".to_string(), int()),
                ("name".to_string(), info("text", ValueType::Text)),
                (
                    "when".to_string(),
                    info("date", ValueType::Other(PgType::Date)),
                ),
            ]),
        )
    }

    fn check(select: &str) -> Result<(), ValidationError> {
        let def = parse(&format!(
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT {select}"
        ))
        .unwrap();
        let (from, joined) = sides();
        validate_join(&def, &from, &[&joined])
    }

    /// A 1-1 `JOIN`, a star included, is admitted with no setting: the
    /// development knobs that once gated them are gone, so
    /// `TRELLIS_DEV_ONE_TO_ONE_JOIN` or `TRELLIS_DEV_STAR_JOIN` set to any
    /// value, an "off" one included, changes nothing.
    #[test]
    fn a_one_to_one_join_is_admitted_with_no_knob() {
        assert_eq!(check("customers.name AS who"), Ok(()));
        let def = parse(
            "TRANSFORM v FROM orders INNER JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who, orders.total AS t",
        )
        .unwrap();
        let (from, joined) = sides();
        assert_eq!(validate_join(&def, &from, &[&joined]), Ok(()));
        let star = parse(
            "TRANSFORM v FROM orders JOIN customers ON orders.cust = customers.id \
             LEFT JOIN regions ON orders.cust = regions.id \
             SELECT customers.name AS who, regions.name AS region",
        )
        .unwrap();
        assert_eq!(validate_join(&star, &from, &[&joined, &joined]), Ok(()));
    }

    /// Runs [`a_one_to_one_join_is_admitted_with_no_knob`] in a child process
    /// whose environment sets the old knobs' variables to each value they read as
    /// off or on. The child sets it before anything runs, so nothing races
    /// `std::env::set_var` in this process.
    #[test]
    fn the_old_knob_variable_changes_nothing() {
        let exe = std::env::current_exe().expect("the test binary");
        for value in ["0", "off", "false", "1", "on"] {
            let out = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "defs::validate_join_one_to_one::tests::a_one_to_one_join_is_admitted_with_no_knob",
                    "--test-threads=1",
                ])
                .env("TRELLIS_DEV_ONE_TO_ONE_JOIN", value)
                .env("TRELLIS_DEV_STAR_JOIN", value)
                .output()
                .expect("run the child test");
            let stdout = String::from_utf8_lossy(&out.stdout);
            assert!(
                out.status.success() && stdout.contains("1 passed"),
                "TRELLIS_DEV_ONE_TO_ONE_JOIN and TRELLIS_DEV_STAR_JOIN={value}: {stdout}{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    #[test]
    fn a_column_named_by_two_on_terms_is_refused_on_either_side() {
        let (from, joined) = sides();
        let on = |on: &str| {
            let def = parse(&format!(
                "TRANSFORM v FROM orders LEFT JOIN customers ON {on} SELECT customers.name AS n"
            ))
            .unwrap();
            validate_join(&def, &from, &[&joined])
        };
        // The joined side's column repeated.
        assert_eq!(
            on("orders.cust = customers.id AND orders.total = customers.id"),
            Err(ValidationError::JoinOnRepeatedColumn {
                term: "orders.total = customers.id".to_string(),
                column: "customers.id".to_string(),
            })
        );
        // The FROM side's column repeated (the repeat is refused before the
        // second term's types are compared).
        assert_eq!(
            on("orders.cust = customers.id AND customers.name = orders.cust"),
            Err(ValidationError::JoinOnRepeatedColumn {
                term: "customers.name = orders.cust".to_string(),
                column: "orders.cust".to_string(),
            })
        );
        // A repeated pair keeps its own refusal.
        assert!(matches!(
            on("orders.cust = customers.id AND customers.id = orders.cust"),
            Err(ValidationError::JoinOnRepeatedPair { .. })
        ));
    }

    #[test]
    fn a_field_is_any_expression_over_qualified_columns_of_either_table() {
        for select in [
            "customers.name AS who, orders.note AS note",
            "orders.total + customers.id AS s",
            "CASE WHEN customers.id IS NULL THEN 0 ELSE orders.total END AS c",
            "COALESCE(customers.name, orders.note) AS pick",
            "NULLIF(orders.note, customers.name) AS n",
            "orders.total <> customers.id AS differs",
            "NOT (orders.total = 1) AS neg",
            "CAST(orders.total AS bigint) AS t",
        ] {
            assert_eq!(check(select), Ok(()), "{select}");
        }
    }

    #[test]
    fn aggregates_unqualified_and_unknown_columns_are_named_refusals() {
        // The deferred families parse, so the validator names them.
        for select in ["MAX(customers.id) AS m", "BOOL_AND(orders.total) AS b"] {
            assert!(
                matches!(
                    check(select),
                    Err(ValidationError::OneToOneJoinAggregate { .. })
                ),
                "{select}"
            );
        }
        assert!(matches!(
            check("total AS t"),
            Err(ValidationError::JoinUnqualifiedColumn { .. })
        ));
        assert!(matches!(
            check("other.total AS t"),
            Err(ValidationError::JoinUnknownTable { .. })
        ));
        assert!(matches!(
            check("customers.missing AS t"),
            Err(ValidationError::JoinUnresolvedColumn { .. })
        ));
        assert!(matches!(
            check("customers.name AS x, orders.note AS x"),
            Err(ValidationError::DuplicateFieldName { .. })
        ));
        // A column named like a ledger column would collide in `R`.
        assert!(matches!(
            check("orders.__basis AS b"),
            Err(ValidationError::OneToOneJoinUnsupported { .. })
        ));
        // A field that doesn't type is refused by the inference.
        assert!(check("orders.total + customers.name AS bad").is_err());
    }

    #[test]
    fn a_nondeterministic_collation_under_a_text_function_is_refused() {
        let def = parse(
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT STRPOS(customers.name, 'a') AS pos, customers.name AS plain",
        )
        .unwrap();
        let (from, mut joined) = sides();
        assert_eq!(validate_join(&def, &from, &[&joined]), Ok(()));
        joined.get_mut("name").unwrap().collation_deterministic = false;
        joined.get_mut("name").unwrap().collation = Some("ci".to_string());
        assert!(matches!(
            validate_join(&def, &from, &[&joined]),
            Err(ValidationError::NondeterministicCollationTextFunction(refused))
                if refused.field == "pos" && refused.table == "customers"
        ));
    }

    /// A star of `n` tables (`orders` and `c2`..`c<n>`), each joined on
    /// `orders.cust`, selecting a column of each.
    fn star(n: usize) -> Result<(), ValidationError> {
        star_text(&star_def(n), n)
    }

    fn star_def(n: usize) -> String {
        let joins: Vec<String> = (2..=n)
            .map(|i| format!("LEFT JOIN c{i} ON orders.cust = c{i}.id"))
            .collect();
        let fields: Vec<String> = (2..=n).map(|i| format!("c{i}.name AS n{i}")).collect();
        format!(
            "TRANSFORM v FROM orders {} SELECT {}",
            joins.join(" "),
            fields.join(", ")
        )
    }

    fn star_text(text: &str, n: usize) -> Result<(), ValidationError> {
        let def = parse(text).unwrap();
        let (from, joined) = sides();
        let joined: Vec<&HashMap<String, JoinColumnInfo>> = (2..=n).map(|_| &joined).collect();
        validate_join(&def, &from, &joined)
    }

    /// A star of 3 to 5 tables validates with nothing set, as the two-table
    /// JOIN does.
    #[test]
    fn a_star_validates_with_nothing_set() {
        for n in 2..=5 {
            assert_eq!(star(n), Ok(()), "{n} tables");
        }
    }

    /// U2 (#1464): define refuses 6 tables by name and accepts 5.
    #[test]
    fn u2_a_star_of_six_tables_is_refused_and_five_admitted() {
        assert_eq!(star(5), Ok(()));
        let refused = star(6);
        assert_eq!(
            refused,
            Err(ValidationError::StarJoinTooManyTables { tables: 6 })
        );
        let text = refused.unwrap_err().to_string();
        assert!(text.contains("reads 6 tables"), "{text}");
        assert!(text.contains("at most 5"), "{text}");
        assert_eq!(
            star(7),
            Err(ValidationError::StarJoinTooManyTables { tables: 7 })
        );
    }

    /// A snowflake (an `ON` reading another joined table, earlier or later)
    /// and an `ON` reading the root and two joined tables are refused by
    /// name, naming the other table.
    #[test]
    fn an_on_reading_another_joined_table_is_refused() {
        let refused = |on3: &str, other: &str| {
            let text = format!(
                "TRANSFORM v FROM orders JOIN c2 ON orders.cust = c2.id \
                 JOIN c3 ON {on3} SELECT c2.name AS a, c3.name AS b"
            );
            match star_text(&text, 3) {
                Err(ValidationError::JoinOnReadsAnotherJoinedTable {
                    table,
                    other: found,
                    ..
                }) => {
                    assert_eq!(table, "c3", "{on3}");
                    assert_eq!(found, other, "{on3}");
                }
                other => panic!("{on3}: {other:?}"),
            }
        };
        // A snowflake: c3 joins c2, not the FROM table.
        refused("c2.id = c3.id", "c2");
        // The root and two joined tables.
        refused("orders.cust = c3.id AND c2.id = c3.id", "c2");
        refused("orders.cust = c3.id AND orders.cust = c2.id", "c2");
        // The first position's ON reads a later table, which Postgres
        // refuses too.
        let text = "TRANSFORM v FROM orders JOIN c2 ON orders.cust = c3.id \
                    JOIN c3 ON orders.cust = c3.id SELECT c2.name AS a";
        assert!(matches!(
            star_text(text, 3),
            Err(ValidationError::JoinOnReadsAnotherJoinedTable { .. })
        ));
        let message = ValidationError::JoinOnReadsAnotherJoinedTable {
            table: "c3".to_string(),
            term: "c2.id = c3.id".to_string(),
            other: "c2".to_string(),
        }
        .to_string();
        assert!(message.contains("snowflake"), "{message}");
    }

    /// Two `ON` columns of the `FROM` table at one position whose names
    /// share their first 60 bytes would be one column of `J` once cut, at
    /// any position the two-table JOIN's included; they are refused, and a
    /// pair that differs within the first 60 bytes is admitted.
    #[test]
    fn on_columns_whose_key_column_names_collide_are_refused() {
        let long = |tail: &str| format!("{}{tail}", "k".repeat(60));
        let int = || info("integer", ValueType::Integer(IntWidth::Int4));
        let check = |x: &str, y: &str| {
            let (mut from, mut joined) = sides();
            from.insert(x.to_string(), int());
            from.insert(y.to_string(), int());
            joined.insert("other".to_string(), int());
            let def = parse(&format!(
                "TRANSFORM v FROM orders JOIN customers \
                 ON orders.{x} = customers.id AND orders.{y} = customers.other \
                 SELECT customers.name AS who"
            ))
            .unwrap();
            validate_join(&def, &from, &[&joined])
        };
        // 61 and 62 bytes, the first 60 shared: both cut to `p1_` + 60.
        let refused = check(&long("a"), &long("bb"));
        match &refused {
            Err(ValidationError::OneToOneJoinUnsupported { what, .. }) => {
                assert!(what.contains("first 60 bytes"), "{what}")
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(check(&long(""), &format!("{}j", "k".repeat(59))), Ok(()));
    }

    fn check_where(predicate: &str) -> Result<(), ValidationError> {
        check(&format!("customers.name AS who WHERE {predicate}"))
    }

    /// Any total boolean predicate is admitted (#1991 rule 18, ADR-0021):
    /// over one table, over both, the anti-join, and the degenerate ones.
    #[test]
    fn any_total_boolean_predicate_is_admitted() {
        for predicate in [
            "orders.total > 5",
            "customers.id IS NULL",
            "orders.total > customers.id",
            "customers.id IS NULL OR orders.total > 3",
            "customers.name = 'x'",
            "FALSE",
            "NULL",
            "(TRUE)",
        ] {
            assert_eq!(check_where(predicate), Ok(()), "{predicate}");
        }
        // A star's predicate reads any position.
        let star = parse(
            "TRANSFORM v FROM orders JOIN customers ON orders.cust = customers.id \
             LEFT JOIN regions ON orders.cust = regions.id \
             SELECT customers.name AS who \
             WHERE regions.id IS NULL OR customers.id > orders.total",
        )
        .unwrap();
        let (from, joined) = sides();
        assert_eq!(validate_join(&star, &from, &[&joined, &joined]), Ok(()));
    }

    /// The predicate's own checks, as the one-table `WHERE`'s: every column
    /// qualified and known, boolean, total. (The parser refuses an aggregate
    /// call outside an aggregate's fields first.)
    #[test]
    fn a_predicate_is_refused_by_its_own_checks() {
        assert!(matches!(
            check_where("total > 1"),
            Err(ValidationError::JoinUnqualifiedColumn { field, .. }) if field == "WHERE"
        ));
        assert!(check_where("orders.nope > 1").is_err());
        assert_eq!(
            check_where("orders.total + 1"),
            Err(ValidationError::PredicateNotBoolean {
                found: ValueType::Integer(IntWidth::Int4)
            })
        );
        let divided = check_where("orders.total * customers.id > 1");
        assert!(
            matches!(divided, Err(ValidationError::PredicateNotTotal { .. })),
            "{divided:?}"
        );
        // Two text columns of different tables may carry conflicting
        // collations (side condition C2): refused, as two of one table are.
        assert!(matches!(
            check_where("orders.note = customers.name"),
            Err(ValidationError::PredicateNotTotal { .. })
        ));
        // Two columns of one name in two tables are two columns.
        let def = parse(
            "TRANSFORM v FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who WHERE orders.name = customers.name",
        )
        .unwrap();
        let (mut from, joined) = sides();
        from.insert("name".to_string(), info("text", ValueType::Text));
        assert!(matches!(
            validate_join(&def, &from, &[&joined]),
            Err(ValidationError::PredicateNotTotal { .. })
        ));
    }

    /// A refusal over the predicate names it, not a field called `WHERE`.
    #[test]
    fn a_refusal_over_the_predicate_names_the_where() {
        let text = check_where("total > 1").unwrap_err().to_string();
        assert!(text.contains("the WHERE predicate"), "{text}");
        assert!(!text.contains("'WHERE'"), "{text}");
    }
}
