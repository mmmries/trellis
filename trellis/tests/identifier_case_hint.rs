//! An unquoted name folds to lower case (#1508), so a name written
//! `OrderTotals` reads `ordertotals`. When a folded name finds nothing but
//! a name differing from it only in case exists, the refusal names that one
//! and its quoted spelling; an addressing statement acts only on the exact
//! name, never on a twin in another case.

#[path = "support/pipeline.rs"]
mod pipeline;

use pipeline::{connect, definer};
use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::{ErrorCode, TrellisError};

async fn status(raw: &Client, target: &str) -> Option<String> {
    raw.query_opt(
        "select status from transform_definitions where target_table = $1",
        &[&target],
    )
    .await
    .expect("read the status")
    .map(|row| row.get(0))
}

/// Asserts `err` is the case hint, naming `found`.
fn assert_case_hint(err: &TrellisError, found: &str) {
    let message = err.to_string();
    assert!(
        message.contains(&format!("did you mean {found}?"))
            && message.contains("folds to lower case"),
        "{message}"
    );
}

/// `DROP TRANSFORM OrderTotals` beside a `"OrderTotals"` target refuses,
/// naming the quoted spelling, and drops nothing; the quoted spelling
/// addresses it.
#[tokio::test]
async fn an_address_in_another_case_is_refused_with_the_quoted_spelling() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table orders (id bigint primary key, amt numeric)")
        .await
        .expect("seed the source");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM \"OrderTotals\" FROM orders SELECT amt AS amt")
        .await
        .expect("define");

    for statement in [
        "DROP TRANSFORM OrderTotals",
        "PAUSE TRANSFORM OrderTotals",
        "RESUME TRANSFORM OrderTotals.amt",
        "ALTER TRANSFORM OrderTotals ADD amt + 1 AS more",
    ] {
        let err = trellis.apply(statement).await.unwrap_err();
        assert_case_hint(&err, "\"OrderTotals\"");
        assert!(err.to_string().contains("'ordertotals'"), "{err}");
        assert_eq!(err.code(), ErrorCode::NotFound, "{statement}");
    }
    let err = trellis
        .apply("DROP TRANSFORM public.OrderTotals")
        .await
        .unwrap_err();
    assert_case_hint(&err, "public.\"OrderTotals\"");
    assert!(status(&raw, "public.OrderTotals").await.is_some());

    trellis
        .apply("PAUSE TRANSFORM \"OrderTotals\"")
        .await
        .expect("the quoted spelling addresses it");
    assert_eq!(
        status(&raw, "public.OrderTotals").await.as_deref(),
        Some("paused")
    );
    trellis
        .apply("DROP TRANSFORM \"OrderTotals\"")
        .await
        .expect("drop");
    assert_eq!(status(&raw, "public.OrderTotals").await, None);
    // Nothing differs only in case any more: a drop of nothing succeeds.
    trellis
        .apply("DROP TRANSFORM OrderTotals")
        .await
        .expect("a drop of nothing");
}

/// With a lower-case twin beside it, the unquoted address acts on the twin
/// alone: it is the exact folded name.
#[tokio::test]
async fn an_unquoted_address_acts_only_on_its_lower_case_twin() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table orders (id bigint primary key, amt numeric)")
        .await
        .expect("seed the source");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM \"OrderTotals\" FROM orders SELECT amt AS amt",
        "TRANSFORM OrderTotals FROM orders SELECT amt AS amt",
    ] {
        trellis.apply(text).await.expect("define");
    }
    assert!(status(&raw, "public.ordertotals").await.is_some());

    trellis
        .apply("PAUSE TRANSFORM OrderTotals")
        .await
        .expect("pause the twin");
    assert_eq!(
        status(&raw, "public.ordertotals").await.as_deref(),
        Some("paused")
    );
    assert_ne!(
        status(&raw, "public.OrderTotals").await.as_deref(),
        Some("paused")
    );
}

/// A table, schema or column a definition names in another case than the
/// catalog's is refused naming the quoted spelling.
#[tokio::test]
async fn a_define_naming_a_table_schema_or_column_in_another_case_gets_the_hint() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table \"Orders\" (id bigint primary key, \"Amount\" numeric(20, 0)); \
         create schema \"Shop\"; \
         create table \"Shop\".items (id bigint primary key, qty numeric)",
    )
    .await
    .expect("seed the sources");
    let trellis = definer(db.dsn()).await;

    for (text, found) in [
        (
            "TRANSFORM t FROM Orders SELECT id AS id",
            "\"Orders\"".to_string(),
        ),
        (
            "TRANSFORM t FROM Shop.items SELECT qty AS qty",
            "\"Shop\".items".to_string(),
        ),
        (
            "TRANSFORM t FROM \"Orders\" GROUP BY id SELECT SUM(Amount) AS total",
            "\"Amount\"".to_string(),
        ),
    ] {
        let err = trellis.apply(text).await.unwrap_err();
        assert_case_hint(&err, &found);
    }
    // A miss with no name in another case keeps its own error.
    let err = trellis
        .apply("TRANSFORM t FROM \"Orders\" SELECT nothing AS x")
        .await
        .unwrap_err();
    assert!(!err.to_string().contains("did you mean"), "{err}");

    trellis
        .apply("TRANSFORM t FROM \"Orders\" GROUP BY id SELECT SUM(\"Amount\") AS total")
        .await
        .expect("the quoted spelling reads the column");
}
