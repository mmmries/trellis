//! A definition whose expression Postgres fails while it plans the
//! statement (a constant out of range, a division by a zero that a call
//! computes) defines: define evaluates no constant (ADR-0022). Its rows fail
//! at run time with Postgres's own SQLSTATE and message, like any per-row
//! error, and a fuse stops it. This is the guarantee that stands in for a
//! define-time refusal: a loud stop, never a target that silently lacks
//! rows.
//!
//! One pin per shape, each ending where that shape's per-row errors end:
//!
//! - a 1-1 or `JOIN` built over a source with rows: the build quarantines
//!   the keys and the transform fuse quarantines the definition;
//! - a 1-1 over a source empty at define, on its first rows: the column fuse
//!   pauses the failing field, and the other fields go on applying;
//! - a `JOIN` over sources empty at define, on its first rows: the keys are
//!   quarantined and the transform fuse quarantines the definition;
//! - an aggregate, with rows or empty at define: its build plans the
//!   expression whatever the rows, fails whole, and pauses the definition
//!   with Postgres's error once its charges run out. It never goes `live`, so
//!   no live apply of it is reached.
//!
//! Every step is driven by hand (`drain_driver`); nothing waits for
//! convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::defs::{TransformStatus, ValueType};
use trellis::float::FloatWidth;
use trellis::integer::IntWidth;
use trellis::staging::build;
use trellis::staging::quarantine::DEFAULT_DEATH_THRESHOLD;
use trellis::{Config, Trellis, TrellisOptions};

const INT4: ValueType = ValueType::Integer(IntWidth::Int4);
const INT8: ValueType = ValueType::Integer(IntWidth::Int8);

/// `src` and `other` share their keys; `empty` never gets a row.
const DDL: &str = "create table public.src (id bigint primary key, g integer, v integer, r real); \
                   create table public.other (id bigint primary key, w integer); \
                   create table public.empty (id bigint primary key, g integer, v integer)";

const ROWS: i64 = 30;

const SEED: &str = "insert into public.src select i, i % 3, i, i from generate_series(1, 30) i; \
                    insert into public.other select i, i from generate_series(1, 30) i";

const CAPTURED: &[&str] = &["public.src", "public.other", "public.empty"];

/// Past `real`'s range: `COALESCE(r, <this>)` casts it to `real`.
const BEYOND_REAL: &str = "1000000000000000000000000000000000000000";

fn columns() -> Vec<(&'static str, ValueType)> {
    vec![
        ("id", INT8),
        ("g", INT4),
        ("v", INT4),
        ("r", ValueType::Float(FloatWidth::Float4)),
        ("w", INT4),
    ]
}

/// Each 1-1 form, the SQLSTATE Postgres raises for it while planning, and
/// a piece of its message.
fn one_to_one_forms() -> Vec<(String, &'static str, &'static str)> {
    vec![
        (
            "v + (2147483647 + 1)".to_string(),
            "22003",
            "integer out of range",
        ),
        (
            format!("COALESCE(r, {BEYOND_REAL})"),
            "22003",
            "is out of range for type real",
        ),
        // Overflow around a call over constants, which Postgres folds.
        (
            "2147483647 + CHAR_LENGTH('abc')".to_string(),
            "22003",
            "integer out of range",
        ),
        // A zero divisor a call computes: not a literal zero, so it defines.
        ("1.5 / CEIL(0.0)".to_string(), "22012", "division by zero"),
    ]
}

async fn facade(d: &Driver) -> Trellis {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a facade")
}

async fn status(trellis: &Trellis, target: &str) -> TransformStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is defined"))
        .status
}

/// `target`'s poisoned keys' SQLSTATEs, one per distinct value.
async fn poison_sqlstates(d: &Driver, target: &str) -> Vec<String> {
    d.rows(&format!(
        "select distinct coalesce(p.sqlstate, 'none') from poison p \
         join transform_definitions t on t.id = p.transform_id \
         where split_part(t.target_table, '.', 2) = '{target}'"
    ))
    .await
    .into_iter()
    .map(|row| row.trim_matches(['(', ')']).to_string())
    .collect()
}

/// Asserts that `target`'s quarantine sample is nonempty and that every
/// entry carries Postgres's message.
async fn assert_sample(trellis: &Trellis, target: &str, message: &str) {
    let sample = trellis
        .sample_quarantined(target, None, 100)
        .await
        .expect("sample the quarantine");
    assert!(!sample.is_empty(), "{target}: something is quarantined");
    for entry in &sample {
        assert!(
            entry.error_message.contains(message),
            "{target}: {:?} carries Postgres's message `{message}`",
            entry.error_message
        );
    }
}

/// Drains `batch` until it applies, at most `DEFAULT_DEATH_THRESHOLD + 1`
/// times: each failed drain charges its records once, and the one that
/// reaches a fuse applies the rest.
async fn drain_until_applied(d: &Driver, batch: i64) {
    for _ in 0..=DEFAULT_DEATH_THRESHOLD {
        if d.drain_frozen(batch, "w", &[])
            .await
            .finish_result()
            .await
            .is_ok()
        {
            return;
        }
    }
    panic!("batch {batch} never applied: a page retried for ever");
}

/// A 1-1 built over a source with rows: the build's chunks narrow each
/// failure to its key, the keys are quarantined with Postgres's SQLSTATE,
/// and the transform fuse quarantines the definition. The build finishes;
/// it doesn't retry for ever.
#[tokio::test]
async fn a_one_to_one_build_quarantines_a_constant_postgres_fails_while_planning() {
    let forms = one_to_one_forms();
    let definitions: Vec<String> = forms
        .iter()
        .enumerate()
        .map(|(i, (expr, _, _))| format!("TRANSFORM one{i} FROM public.src SELECT {expr} AS c"))
        .collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let d = Driver::start_unbuilt(
        &[],
        &format!("{DDL}; {SEED}"),
        &columns(),
        &definitions,
        CAPTURED,
    )
    .await;
    build::settle_builds_with(d.pool(), 5).await;
    let trellis = facade(&d).await;
    for (i, (expr, sqlstate, message)) in forms.iter().enumerate() {
        let target = format!("one{i}");
        assert_eq!(
            status(&trellis, &target).await,
            TransformStatus::Quarantined,
            "`{expr}`"
        );
        assert_eq!(poison_sqlstates(&d, &target).await, [*sqlstate], "`{expr}`");
        assert_sample(&trellis, &target, message).await;
        assert_eq!(
            d.rows(&format!("select count(*) from public.{target}"))
                .await,
            ["(0)"],
            "`{expr}`: no row was written"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

/// A 1-1 over a source empty at define goes `live` (its build runs no
/// statement), and its first rows fail in the batched apply. Each record is
/// isolated and charged to the failing field, the column fuse pauses that
/// field with Postgres's message, and the page applies without it.
#[tokio::test]
async fn a_one_to_one_empty_at_define_pauses_the_failing_field_on_its_first_rows() {
    let mut d = Driver::start(
        DDL,
        &columns(),
        &["TRANSFORM one FROM public.src SELECT v AS v, v + (2147483647 + 1) AS c"],
        CAPTURED,
    )
    .await;
    d.user().await.batch_execute(SEED).await.expect("seed");
    let batch = d.seal().await;
    drain_until_applied(&d, batch).await;
    let trellis = facade(&d).await;
    assert_eq!(status(&trellis, "one").await, TransformStatus::Live);
    assert_eq!(
        d.rows("select column_name, last_error from column_status where transform_table = 'one'")
            .await,
        ["(c,\"integer out of range\")"],
        "the column fuse paused the failing field"
    );
    assert_sample(&trellis, "one.c", "integer out of range").await;
    assert_eq!(
        d.rows("select count(*), count(c) from public.one where v = id")
            .await,
        [format!("({ROWS},0)")],
        "every row applies its other field, and none has the paused one"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` built over sources with rows quarantines the keys and the
/// definition, as a 1-1 does.
#[tokio::test]
async fn a_join_build_quarantines_a_constant_postgres_fails_while_planning() {
    let d = Driver::start_unbuilt(
        &[],
        &format!("{DDL}; {SEED}"),
        &columns(),
        &[
            "TRANSFORM j FROM public.src JOIN public.other ON other.id = src.id \
           SELECT src.v + (2147483647 + 1) AS c, other.w AS w",
        ],
        CAPTURED,
    )
    .await;
    build::settle_builds_with(d.pool(), 5).await;
    let trellis = facade(&d).await;
    assert_eq!(status(&trellis, "j").await, TransformStatus::Quarantined);
    assert_eq!(poison_sqlstates(&d, "j").await, ["22003"]);
    assert_sample(&trellis, "j", "integer out of range").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` over sources empty at define goes `live`, and its first rows
/// fail in the batched apply: their keys are quarantined with Postgres's
/// SQLSTATE and the transform fuse quarantines the definition.
#[tokio::test]
async fn a_join_empty_at_define_quarantines_on_its_first_rows() {
    let mut d = Driver::start(
        DDL,
        &columns(),
        &[
            "TRANSFORM j FROM public.src JOIN public.other ON other.id = src.id \
           SELECT src.v + (2147483647 + 1) AS c, other.w AS w",
        ],
        CAPTURED,
    )
    .await;
    d.user().await.batch_execute(SEED).await.expect("seed");
    let batch = d.seal().await;
    drain_until_applied(&d, batch).await;
    let trellis = facade(&d).await;
    assert_eq!(status(&trellis, "j").await, TransformStatus::Quarantined);
    assert_eq!(poison_sqlstates(&d, "j").await, ["22003"]);
    assert_sample(&trellis, "j", "integer out of range").await;
    assert_eq!(d.rows("select count(*) from public.j").await, ["(0)"]);
    trellis.shutdown().await.expect("shutdown");
}

/// An aggregate's build is one statement over the whole source (the group
/// basis's absolute write), and Postgres plans the expression in it whether
/// or not the source has rows: in an argument (computed per row) or around
/// the call (computed per group), over a single table or a `JOIN`. The
/// build fails whole, is charged and retried, and pauses the definition
/// with Postgres's message once its charges run out. It never goes `live`
/// with a partial or empty target.
#[tokio::test]
async fn an_aggregate_build_pauses_on_a_constant_postgres_fails_while_planning() {
    let definitions = [
        (
            "agg_arg",
            "TRANSFORM agg_arg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(v + (2147483647 + 1)) AS total",
        ),
        (
            "agg_outer",
            "TRANSFORM agg_outer FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(v) + (2147483647 + 1) AS total",
        ),
        (
            "agg_empty",
            "TRANSFORM agg_empty FROM public.empty GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(v) + (2147483647 + 1) AS total",
        ),
        (
            "agg_join",
            "TRANSFORM agg_join FROM public.src JOIN public.other ON other.id = src.id \
             GROUP BY src.g SELECT SUM(other.w + (2147483647 + 1)) AS total",
        ),
    ];
    let d = Driver::start_unbuilt(
        &[],
        &format!("{DDL}; {SEED}"),
        &columns(),
        &definitions.map(|(_, text)| text),
        CAPTURED,
    )
    .await;
    build::settle_builds_with(d.pool(), 5).await;
    let trellis = facade(&d).await;
    for (target, _) in definitions {
        let reported = trellis
            .status(target)
            .await
            .expect("status")
            .unwrap_or_else(|| panic!("{target} is defined"));
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported
            .backfill_failure
            .unwrap_or_else(|| panic!("{target}: the pause names the build's failure"));
        assert!(
            failure.last_error.contains("integer out of range"),
            "{target}: {}",
            failure.last_error
        );
        assert_eq!(
            d.rows(&format!("select count(*) from public.{target}"))
                .await,
            ["(0)"],
            "{target}: no group was written"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}
