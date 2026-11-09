//! `Trellis::status` of a star 1-1 `JOIN` (#1775, #1460 sub-step 3.9):
//! `hidden_tables` lists `R`, each position's `M` and `J`, and the fan-out
//! warning samples `R`'s index at each position and names the position and
//! the join key of the largest group past the threshold. It is the only
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
use trellis::dev::defs::set_one_to_one_join_fan_out_warning_rows;
use trellis::{JoinStatus, Trellis, TrellisOptions};

/// Puts the process-wide warning threshold back when a test ends, even on
/// a panic.
struct Threshold;

impl Drop for Threshold {
    fn drop(&mut self) {
        set_one_to_one_join_fan_out_warning_rows(0);
    }
}

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
    let _serial = star::serial().await;
    let _threshold = Threshold;
    for shape in shapes() {
        let star = shape.stars()[0];
        let name = format!("{shape:?}");
        let d = star::start(&[star], A_ROWS, B_ROWS).await;
        set_one_to_one_join_fan_out_warning_rows(3);
        assert_eq!(
            join_status(&d, star).await.fan_out_warning,
            None,
            "{name}: 3 is not past 3"
        );
        set_one_to_one_join_fan_out_warning_rows(2);
        let warning = join_status(&d, star)
            .await
            .fan_out_warning
            .unwrap_or_else(|| panic!("{name}: key 10 holds 3 rows"));
        assert_eq!(
            (
                warning.position,
                warning.key.as_str(),
                warning.rows,
                warning.threshold
            ),
            (shape.at, "10", 3, 2),
            "{name}"
        );
    }
}

/// `hidden_tables` lists `R`, then each position's `M` (`M`, `M2`, ...),
/// then `J`.
#[tokio::test]
async fn status_lists_every_positions_m() {
    let _serial = star::serial().await;
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
