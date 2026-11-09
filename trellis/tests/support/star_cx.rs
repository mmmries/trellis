//! #1459's cross-position states at n = 3 for the star pins of #1774
//! (#1460 sub-step 3.8): the joined-table writers, and the races of two
//! pages at different positions on one root row.
//!
//! The star is #1464's fixture (`support/star.rs`, every table written by
//! the seed): `a(id, k1, k2, x, y)` joined to `b1` (position 1, the model's
//! `b`) and `b2` (position 2, the model's `e`). A model family's source
//! writes map `src_b(id, k, y)` to `b1(id, k, w, tag)` and `src_e(id, k, z)`
//! to `b2`.
//!
//! An asymmetric family runs in both orientations: as written, and with the
//! two positions swapped ([`swap`]), seed and writers alike.
//!
//! A binary using this module also declares `support/drain_driver.rs` as
//! `mod drain_driver` and `support/star.rs` as `mod star` at its root.

#![allow(dead_code)]

use crate::drain_driver::Driver;
use crate::star::{self, Shape, Star, Writer};
use trellis::staging::interleave::PausePoint;

/// The star of three tables.
pub const N3: Shape = Shape {
    positions: 2,
    at: 2,
};

/// #1459's base state at n = 3: `a` rows `(id, k1, k2, x, y)`, `b1` rows on
/// keys 1 to 3 and `b2` rows on keys 1 to 4.
pub const SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                        (1, 1, 1, 7, 'a1'), (2, 2, 2, 8, 'a2'), (3, 4, 4, 9, 'a3'), (4, 1, 5, 6, 'a4'); \
                        insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), (3, 3, 30, 'b3'); \
                        insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                        (3, 3, 300, 'e3'), (4, 4, 400, 'e4')";

/// #1459's crossing state (#1457 E2): `a` row 1 on `(1, 2)` and row 2 on
/// `(2, 1)`.
pub const CROSS_SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                              (1, 1, 2, 7, 'a1'), (2, 2, 1, 8, 'a2'); \
                              insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'); \
                              insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2')";

/// #1459's F11 state: [`SEED`] with `a` rows 7 on `(5, 1)` and 8 on
/// `(5, 3)`, and `b1` row 7 on key 71.
pub const LATE_SEED: &str = "insert into public.a (id, k1, k2, x, y) values \
                             (1, 1, 1, 7, 'a1'), (2, 2, 2, 8, 'a2'), (3, 4, 4, 9, 'a3'), (4, 1, 5, 6, 'a4'), \
                             (7, 5, 1, 1, 'a7'), (8, 5, 3, 2, 'a8'); \
                             insert into public.b1 values (1, 1, 10, 'b1'), (2, 2, 20, 'b2'), (3, 3, 30, 'b3'), \
                             (7, 71, 70, 'b7'); \
                             insert into public.b2 values (1, 1, 100, 'e1'), (2, 2, 200, 'e2'), \
                             (3, 3, 300, 'e3'), (4, 4, 400, 'e4')";

/// Turns the development knob on: every pin here defines a star.
pub fn knob() {
    trellis::dev::defs::set_star_join(true);
}

/// A `'static` copy of `s`, for a [`Writer`]: a test's few bodies.
pub fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// `sql` with positions 1 and 2 swapped: `public.b1` and `public.b2`, and
/// `a`'s columns `k1` and `k2`.
pub fn swap(sql: &str) -> String {
    sql.replace("public.b1", "\u{0}1")
        .replace("public.b2", "public.b1")
        .replace("\u{0}1", "public.b2")
        .replace("k1", "\u{0}2")
        .replace("k2", "k1")
        .replace("\u{0}2", "k2")
}

/// One orientation of a family: as written, or with positions 1 and 2
/// swapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Orientation {
    AsWritten,
    Swapped,
}

impl Orientation {
    /// Both orientations, for an asymmetric family.
    pub const BOTH: [Orientation; 2] = [Orientation::AsWritten, Orientation::Swapped];
    /// One, for a symmetric family.
    pub const ONE: [Orientation; 1] = [Orientation::AsWritten];

    pub fn sql(self, sql: &str) -> String {
        match self {
            Orientation::AsWritten => sql.to_string(),
            Orientation::Swapped => swap(sql),
        }
    }

    pub fn writer(self, writer: Writer) -> Writer {
        match writer {
            Writer::Change(sql) => Writer::Change(leak(self.sql(sql))),
            Writer::Rederive(table, keys) => Writer::Rederive(leak(self.sql(table)), keys),
            Writer::Both(table, keys, sql) => {
                Writer::Both(leak(self.sql(table)), keys, leak(self.sql(sql)))
            }
        }
    }
}

/// A two-writer family over `seed` in each of `orientations`, on every
/// star of [`N3`]: the uncaptured `pre` writes `(table, sql)` first, then
/// each writer in its own batch, either one's page frozen at its commit
/// while the other queues ([`race_at`]), then `after` in a batch of its own.
///
/// The ring holds two undrained batches at a time (`retire` needs a
/// segment's successor drained), so a change a family applies out of order
/// is written uncaptured in `pre` and repaired by a Re-derive in `after`.
pub async fn family(
    seed: &str,
    pre: &[(&str, &str)],
    first: Writer,
    second: Writer,
    after: Option<Writer>,
    orientations: &[Orientation],
) {
    for orientation in orientations {
        let (first, second) = (orientation.writer(first), orientation.writer(second));
        let after = after.map(|w| orientation.writer(w));
        for star in N3.stars() {
            for flip in [false, true] {
                let mut d = star::start_seeded(&[star], &orientation.sql(seed)).await;
                for (table, sql) in pre {
                    star::write_uncaptured(&d, &orientation.sql(table), &orientation.sql(sql))
                        .await;
                }
                let one = first.seal(&mut d).await;
                let two = second.seal(&mut d).await;
                let (x, y) = if flip { (two, one) } else { (one, two) };
                race_at(&mut d, star, x, y, PausePoint::BeforeCommit, false).await;
                if let Some(after) = after {
                    let batch = after.seal(&mut d).await;
                    d.drain(batch, "after").await;
                }
                star::assert_oracle(&mut d, &[star]).await;
            }
        }
    }
}

/// Drains `first` frozen at `point` (labelled by the target), then `second`,
/// which must queue on a lock the first holds there. Lets both finish and
/// asserts nothing deadlocked; the oracle is the caller's (`oracle` runs it
/// here).
pub async fn race_at(
    d: &mut Driver,
    star: Star,
    first: i64,
    second: i64,
    point: PausePoint,
    oracle: bool,
) {
    let target = star.target();
    let name = star.name();
    let mut held = d.drain_frozen(first, "first", &[(point, &target)]).await;
    let reached = held
        .reached_or_finished(point)
        .await
        .unwrap_or_else(|| panic!("{name}: the first page never reached {point:?}"));
    let queued = d.drain_frozen(second, "second", &[]).await;
    let blocked = star::queued_or_finished(d, reached.backend_pid, &queued).await;
    d.release(&mut held, point).await;
    let first_result = held.finish_result().await;
    let second_result = queued.finish_result().await;
    let deadlocks = d.deadlocks_logged();
    assert!(
        deadlocks.is_empty(),
        "{name}: frozen at {point:?}: {deadlocks:?}"
    );
    if let Err(err) = first_result {
        panic!("{name}: the first page failed: {err}");
    }
    if let Err(err) = second_result {
        panic!("{name}: the second page failed: {err}");
    }
    if oracle {
        d.settle().await;
        star::assert_star(d, star).await;
    }
    assert!(
        blocked,
        "{name}: the second page ran without queueing on the first's lock at {point:?}"
    );
}
