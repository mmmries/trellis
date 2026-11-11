//! `Trellis::status` of a star 1-1 `JOIN` (#1775, #1460 sub-step 3.9):
//! `hidden_tables` lists `R`, each position's `M` and `J`, and the fan-out
//! warnings sample `R`'s index at each position and name, for each position
//! whose largest group passes the threshold, the position and that group's
//! join key, in position order. They are the only
//! runtime signal of a lookup table joined at any position (#1766's
//! Decision). The two-table `JOIN` is the one-position case.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`
//! and never polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use star::{Shape, Star};
use trellis::config::Config;
use trellis::{JoinStatus, Trellis, TrellisOptions};

async fn join_status(d: &Driver, star: Star) -> JoinStatus {
    Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis")
    .status(&star.name())
    .await
    .expect("status")
    .expect("the definition exists")
    .join
    .expect("a JOIN reports its join status")
}

/// The shapes these tests run: the two-table `JOIN`, both positions of
/// three tables, and the last of five.
fn shapes() -> Vec<Shape> {
    vec![
        Shape::TWO,
        Shape::new(2, 1),
        Shape::new(2, 2),
        Shape::new(4, 4),
    ]
}

/// Three `a` rows on key 10 at the position under test, one on key 20; at
/// every other position each `a` row has a key of its own.
const A_ROWS: &str = "(1, 10, 1, 'one'), (2, 10, 2, 'two'), (3, 10, 3, 'three'), \
                      (4, 20, 4, 'four')";
const B_ROWS: &str = "(1, 10, 100, 'ten'), (2, 20, 200, 'twenty')";

/// A group of three `a` rows on one key at any position warns past a
/// threshold of 2, naming that position and the key, and not at 3.
#[tokio::test]
async fn a_per_position_warning_names_its_position_and_key() {
    for shape in shapes() {
        let star = shape.stars()[0];
        let name = format!("{shape:?}");
        let d = star::start(&[star], A_ROWS, B_ROWS).await;
        d.set_fan_out_warning_rows(3);
        assert_eq!(warned(&d, star).await, [], "{name}: 3 is not past 3");
        d.set_fan_out_warning_rows(2);
        assert_eq!(
            warned(&d, star).await,
            [(shape.at, "10".to_string(), 3, 2)],
            "{name}: key 10 holds 3 rows, and no other position has a group"
        );
    }
}

/// Each warning's position, key, rows and threshold, in the order
/// `status()` lists them.
async fn warned(d: &Driver, star: Star) -> Vec<(usize, String, i64, i64)> {
    join_status(d, star)
        .await
        .fan_out_warnings
        .into_iter()
        .map(|w| (w.position, w.key, w.rows, w.threshold))
        .collect()
}

/// Two positions past the threshold each get a warning, in position order,
/// whichever holds the larger group: ADR-0021's warning is per position.
#[tokio::test]
async fn every_position_past_the_threshold_warns() {
    let shape = Shape::new(2, 2);
    let star = shape.stars()[0];
    let mut d = star::start(&[star], A_ROWS, B_ROWS).await;
    // Every `a` row onto `b1`'s key 1001: a group of 4 at position 1, past
    // position 2's group of 3 on key 10.
    star::step(&mut d, "update public.a set k1 = 1001").await;
    d.set_fan_out_warning_rows(2);
    assert_eq!(
        warned(&d, star).await,
        [(1, "1001".to_string(), 4, 2), (2, "10".to_string(), 3, 2)]
    );
    // Past 3, only position 1's group of 4 warns.
    d.set_fan_out_warning_rows(3);
    assert_eq!(warned(&d, star).await, [(1, "1001".to_string(), 4, 3)]);
}

/// `hidden_tables` lists `R`, then each position's `M` (`M`, `M2`, ...),
/// then `J`.
#[tokio::test]
async fn status_lists_every_positions_m() {
    for shape in shapes() {
        let star = shape.stars()[0];
        let d = star::start(&[star], A_ROWS, B_ROWS).await;
        let status = join_status(&d, star).await;
        let names: Vec<_> = status.hidden_tables.iter().map(|t| t.table).collect();
        let mut expected = vec!["R", "M"];
        expected.extend(["M2", "M3", "M4"].into_iter().take(shape.positions - 1));
        expected.push("J");
        assert_eq!(names, expected, "{shape:?}");
    }
}
