//! `J`'s layout and statements at more than one position (ADR-0021, #1765),
//! against a scratch database: the #1464 U1 pin, the null guard, the combined
//! insert, equal-by-value keys on one row, and each position's partial index.
//!
//! The definition is a star of three tables, built by hand, since define
//! admits only two: `a` joined to `b` on `(cust, tenant)` (integers) at
//! position 1 and to `c` on `region` (`numeric`) at position 2.

use std::collections::{BTreeSet, HashMap};

use tokio_postgres::types::ToSql;

use super::{JStatements, JoinedPosition, KeyText, PositionKeys, j_statements};
use crate::defs::ast::{KeySpace, TransformDef, ValueType};
use crate::defs::join::Position;
use crate::defs::join_one_to_one::{HiddenTable, hidden_tables_ddl, j_column};
use crate::defs::validate::JoinColumnInfo;
use crate::integer::IntWidth;
use crate::staging::one_to_one_ledger::LedgerRef;
use crate::staging::one_to_one_render::ImageColumn;

const J: &str = "\"public\".\"join1_7_j\"";

fn info(pg_type: &str, value_type: ValueType) -> JoinColumnInfo {
    JoinColumnInfo {
        value_type,
        pg_type: pg_type.to_string(),
        type_oid: 0,
        typmod: -1,
        collation_oid: 0,
        collation: None,
        collation_deterministic: true,
        is_enum: false,
        copy_type: pg_type.to_string(),
        copy_collation: None,
        trellis_target: false,
    }
}

/// `a ⋈ b ⋈ c`: position 1 on `(cust, tenant)`, position 2 on `region`.
fn star() -> TransformDef {
    let mut def = crate::defs::parse(
        "TRANSFORM v FROM a LEFT JOIN b ON a.cust = b.id AND a.tenant = b.tenant \
         SELECT b.name AS who",
    )
    .unwrap();
    let c = crate::defs::parse("TRANSFORM v FROM a LEFT JOIN c ON a.region = c.id SELECT c.n AS n")
        .unwrap();
    let KeySpace::JoinOneToOne(second) = c.key_space else {
        panic!("a 1-1 JOIN");
    };
    let KeySpace::JoinOneToOne(join) = &mut def.key_space else {
        panic!("a 1-1 JOIN");
    };
    join.joins.push(second.joins[0].clone());
    def
}

fn column(name: &str, type_sql: &str) -> ImageColumn {
    ImageColumn {
        name: name.to_string(),
        type_sql: type_sql.to_string(),
        collation: None,
    }
}

/// The star's two positions, as a page's shape holds them.
fn positions() -> [JoinedPosition; 2] {
    let at = |n: usize, key: Vec<ImageColumn>| {
        let position = Position::from_number(n).unwrap();
        JoinedPosition {
            position,
            table: format!("public.b{n}"),
            left: true,
            m: LedgerRef::hidden(
                "public",
                7,
                HiddenTable::JoinedEntries(position),
                "public.v",
            ),
            j_key: key.iter().map(|c| j_column(position, &c.name)).collect(),
            key,
            m_key: Vec::new(),
            m_columns: Vec::new(),
        }
    };
    [
        at(
            1,
            vec![column("cust", "integer"), column("tenant", "integer")],
        ),
        at(2, vec![column("region", "numeric")]),
    ]
}

fn keys(keys: &[&[&str]]) -> BTreeSet<KeyText> {
    keys.iter()
        .map(|key| key.iter().map(|k| k.to_string()).collect())
        .collect()
}

fn star_statements<'a>(
    positions: &'a [JoinedPosition; 2],
    first: &'a BTreeSet<KeyText>,
    second: &'a BTreeSet<KeyText>,
) -> JStatements<'a> {
    j_statements(
        J,
        &[
            PositionKeys {
                position: &positions[0],
                keys: first,
            },
            PositionKeys {
                position: &positions[1],
                keys: second,
            },
        ],
    )
    .expect("keys to lock")
}

/// #1464 U1: a page's `J` work is one insert and one lock, each over every
/// position's keys and sorted by `(position, key)`, so two pages that share
/// `J` rows at any positions queue on them in one order. Each position's
/// keys are cast to its group's types; the lock finds each position's rows by
/// `position = i` and its own columns. Plant `star_j_unsorted` drops the
/// sort.
#[test]
fn u1_the_j_statements_are_one_insert_and_one_lock_sorted_by_position_and_key() {
    let positions = positions();
    let (first, second) = (keys(&[&["2", "1"], &["1", "9"]]), keys(&[&["1.0"]]));
    let statements = star_statements(&positions, &first, &second);
    assert_eq!(
        statements.insert,
        "insert into \"public\".\"join1_7_j\" (position, \"p1_cust\", \"p1_tenant\", \
         \"p2_region\") select k.* from (\
         select 1::smallint, (u.c0)::integer, (u.c1)::integer, (null)::numeric \
         from unnest($1::text[], $2::text[]) as u(c0, c1) \
         union all \
         select 2::smallint, (null)::integer, (null)::integer, (u.c0)::numeric \
         from unnest($3::text[]) as u(c0)) k \
         order by 1, 2, 3, 4 on conflict do nothing"
    );
    assert_eq!(
        statements.lock,
        "select 1 from \"public\".\"join1_7_j\" j where j.ctid = any(array(\
         select g.ctid from \"public\".\"join1_7_j\" g where g.position = 1 \
         and (g.\"p1_cust\", g.\"p1_tenant\") in \
         (select (u.c0)::integer, (u.c1)::integer from unnest($1::text[], $2::text[]) as u(c0, c1)) \
         union all \
         select g.ctid from \"public\".\"join1_7_j\" g where g.position = 2 \
         and (g.\"p2_region\") in (select (u.c0)::numeric from unnest($3::text[]) as u(c0)))) \
         order by j.position, j.\"p1_cust\", j.\"p1_tenant\", j.\"p2_region\" for update of j"
    );
    // One text array per key column, position by position.
    assert_eq!(
        statements.params,
        [vec!["1", "2"], vec!["9", "1"], vec!["1.0"]]
    );
    // A position with no keys takes no part; none at all, no statement.
    let none = BTreeSet::new();
    let only_second = star_statements(&positions, &none, &second);
    assert!(!only_second.insert.contains("p1_cust"), "{only_second:?}");
    assert_eq!(only_second.params, [vec!["1.0"]]);
    assert!(j_statements(J, &[]).is_none());
}

/// A scratch database holding the star's hidden tables in `public`.
async fn database() -> (testkit::TestCluster, tokio_postgres::Client, String) {
    let cluster = testkit::TestCluster::start();
    let db = cluster.create_empty_database().await;
    let dsn = db.dsn().to_string();
    let client = connect(&dsn).await;
    let int = || info("integer", ValueType::Integer(IntWidth::Int4));
    let from = HashMap::from([
        ("cust".to_string(), int()),
        ("tenant".to_string(), int()),
        ("region".to_string(), info("numeric", ValueType::Numeric)),
    ]);
    let b = HashMap::from([
        ("id".to_string(), int()),
        ("tenant".to_string(), int()),
        ("name".to_string(), info("text", ValueType::Text)),
    ]);
    let c = HashMap::from([
        ("id".to_string(), info("numeric", ValueType::Numeric)),
        ("n".to_string(), int()),
    ]);
    client
        .batch_execute(&hidden_tables_ddl("public", 7, &star(), &from, &[&b, &c]))
        .await
        .expect("the hidden tables");
    (cluster, client, dsn)
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn params<'a>(statements: &'a JStatements<'_>) -> Vec<&'a (dyn ToSql + Sync)> {
    statements
        .params
        .iter()
        .map(|c| c as &(dyn ToSql + Sync))
        .collect()
}

async fn j_rows(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query(
            &format!(
                "select concat_ws(',', position, p1_cust, p1_tenant, p2_region) from {J} \
                 order by position, p1_cust, p1_tenant, p2_region"
            ),
            &[],
        )
        .await
        .expect("J")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// The star's hidden tables: an `M` per position (`_m`, `_m2`), `R` with
/// every position's key and an index per position, and `J`'s range check
/// and per-position null guard, which refuse a row of a position outside
/// the range, or with a `NULL` key column at its position (a row with a
/// `NULL` key would escape the partial unique index, so two writers could
/// insert, and lock, two rows for one key).
#[tokio::test]
async fn the_j_null_guard_and_range_check_refuse_a_row_no_key_owns() {
    let (_cluster, client, _) = database().await;
    for table in [
        "join1_7_r",
        "join1_7_m",
        "join1_7_m2",
        "join1_7_j",
        "join1_7_l",
    ] {
        assert!(
            client
                .query_one("select to_regclass($1) is not null", &[&table])
                .await
                .unwrap()
                .get::<_, bool>(0),
            "{table}"
        );
    }
    let indexes: Vec<String> = client
        .query(
            "select indexdef from pg_indexes where tablename = 'join1_7_r' order by indexname",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(
        indexes
            .iter()
            .any(|i| i.ends_with("(cust, tenant, __from_key)"))
            && indexes.iter().any(|i| i.ends_with("(region, __from_key)")),
        "{indexes:?}"
    );
    for (row, refused) in [
        ("(1, 1, 1, null)", false),
        ("(2, null, null, 1.5)", false),
        ("(1, 1, null, null)", true),
        ("(1, null, null, 1)", true),
        ("(2, null, null, null)", true),
        ("(3, null, null, 1)", true),
        ("(0, 1, 1, null)", true),
    ] {
        let result = client
            .execute(
                &format!("insert into {J} (position, p1_cust, p1_tenant, p2_region) values {row}"),
                &[],
            )
            .await;
        match result {
            Ok(_) => assert!(!refused, "{row} was admitted"),
            Err(e) => {
                assert!(refused, "{row}: {e}");
                assert_eq!(
                    e.code(),
                    Some(&tokio_postgres::error::SqlState::CHECK_VIOLATION),
                    "{row}: {e}"
                );
            }
        }
    }
}

/// #1765: one insert statement adds new keys at two positions, and is a
/// no-op for keys already present, at either position, including a key
/// equal by value to one present but different as text.
#[tokio::test]
async fn the_combined_insert_adds_keys_at_two_positions_and_skips_present_ones() {
    let (_cluster, client, _) = database().await;
    let positions = positions();
    let (first, second) = (keys(&[&["1", "1"], &["2", "2"]]), keys(&[&["1.0"]]));
    let statements = star_statements(&positions, &first, &second);
    let added = client
        .execute(&statements.insert, &params(&statements))
        .await
        .expect("insert");
    assert_eq!(added, 3);
    let (first, second) = (keys(&[&["1", "1"]]), keys(&[&["1.00"], &["3"]]));
    let again = super::j_statements(
        J,
        &[
            PositionKeys {
                position: &positions[0],
                keys: &first,
            },
            PositionKeys {
                position: &positions[1],
                keys: &second,
            },
        ],
    )
    .unwrap();
    let added = client
        .execute(&again.insert, &params(&again))
        .await
        .expect("insert again");
    assert_eq!(added, 1, "only region 3 is new");
    assert_eq!(j_rows(&client).await, ["1,1,1", "1,2,2", "2,1.0", "2,3"]);
    // The lock finds every row it names, at both positions, once.
    let locked = client
        .query(&again.lock, &params(&again))
        .await
        .expect("lock");
    assert_eq!(locked.len(), 3);
}

/// #1765's equal-by-value pin: `numeric` keys `1.0` and `1.00` at one
/// position are one `J` row, so a page holding `1.0` blocks a page locking
/// `1.00`, while a page on another key passes it.
#[tokio::test]
async fn numeric_keys_equal_by_value_at_one_position_serialize_on_one_j_row() {
    let (_cluster, mut client, dsn) = database().await;
    let positions = positions();
    let none = BTreeSet::new();
    let holds = keys(&[&["1.0"]]);
    let held = star_statements(&positions, &none, &holds);
    client
        .execute(&held.insert, &params(&held))
        .await
        .expect("insert 1.0");
    let txn = client.transaction().await.unwrap();
    txn.query(&held.lock, &params(&held))
        .await
        .expect("lock 1.0");

    let mut other = connect(&dsn).await;
    for (key, blocked) in [("1.00", true), ("2", false)] {
        let wanted = keys(&[&[key]]);
        let wants = star_statements(&positions, &none, &wanted);
        let theirs = other.transaction().await.unwrap();
        theirs
            .batch_execute("set local lock_timeout = '200ms'")
            .await
            .unwrap();
        theirs
            .execute(&wants.insert, &params(&wants))
            .await
            .expect("insert");
        let result = theirs.query(&wants.lock, &params(&wants)).await;
        match result {
            Ok(rows) => {
                assert!(!blocked, "{key} passed a page holding 1.0");
                assert_eq!(rows.len(), 1);
            }
            Err(e) => {
                assert!(blocked, "{key}: {e}");
                assert_eq!(
                    e.code(),
                    Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE),
                    "{key}: {e}"
                );
            }
        }
        theirs.rollback().await.unwrap();
    }
    txn.rollback().await.unwrap();
    assert_eq!(j_rows(&client).await, ["2,1.0"]);
}

/// #1765: the lock reaches each position's rows through that position's
/// partial unique index, not a scan of `J`.
#[tokio::test]
async fn the_lock_reads_each_position_through_its_partial_index() {
    let (_cluster, client, _) = database().await;
    client
        .batch_execute(&format!(
            "insert into {J} (position, p1_cust, p1_tenant) \
                 select 1, g, g from generate_series(1, 20000) g; \
             insert into {J} (position, p2_region) \
                 select 2, g from generate_series(1, 20000) g; \
             analyze {J}"
        ))
        .await
        .expect("fill J");
    let positions = positions();
    let (first, second) = (keys(&[&["5", "5"], &["7", "7"]]), keys(&[&["3"]]));
    let statements = star_statements(&positions, &first, &second);
    let plan: Vec<String> = client
        .query(
            &format!("explain {}", statements.lock),
            &params(&statements),
        )
        .await
        .expect("explain")
        .iter()
        .map(|row| row.get(0))
        .collect();
    let plan = plan.join("\n");
    for index in ["join1_7_j_p1_cust_p1_tenant_idx", "join1_7_j_p2_region_idx"] {
        assert!(plan.contains(&format!("using {index}")), "{plan}");
    }
    assert!(!plan.contains("Seq Scan"), "{plan}");
}
