//! A star 1-1 `JOIN`'s `self_check` and hidden-state snapshot (#1778, #1460
//! sub-step 3.12): the audit compares the target against the full multi-join
//! (ADR-0021), one lateral per position, and walks `R`'s parts and then each
//! position's `M` parts (`m`, `mo`, `d` at position 1; `m2`, `mo2`, `d2` and
//! so on after it). The snapshot the generative suite's in-run invariant
//! reads holds every position's `M`, `J` rows and markers.
//!
//! - **Clean after mixed traffic**: root inserts, deletes and key moves at
//!   one and every position, `NULL` keys, joined inserts, deletes and key
//!   moves, at n = 3 and n = 4 under every kind tuple, walked at two page
//!   sizes.
//! - **Hand edits at position 2** (through the test-only path: no capture
//!   hears them): a target cell, `R`'s key column for position 2, an `M_2`
//!   value, a dropped `M_2` entry, an `M_2` entry with no row and a key with
//!   two live `M_2` entries are each reported, exactly, and nothing once
//!   undone.
//! - **O1** (#1464's metamorphic check): a star and the same star with its
//!   `JOIN` clauses permuted reach the same target under the same writes.
//! - **The snapshot**: a star's reads every position, its `J` rows filtered
//!   and fan-out markers filtered by position, and the two-table read
//!   refuses a star.

#[path = "support/drain_driver.rs"]
mod drain_driver;
#[path = "support/star.rs"]
mod star;

use drain_driver::Driver;
use star::{Kind, Shape, Star};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::dev::staging::one_to_one_join_snapshot::{hidden_snapshot, star_snapshot};
use trellis::staging::self_check::{Divergence, SelfCheckScope, compare_page};

/// Turns the development knob on: every pin here defines a star.
fn knob() {
    trellis::dev::defs::set_star_join(true);
}

/// The star of three tables, and of four.
const N3: Shape = Shape {
    positions: 2,
    at: 2,
};
const N4: Shape = Shape {
    positions: 3,
    at: 2,
};

/// The seed of `shape`: `a` rows 1 to 6 on key `id` at every position, and
/// at each position `p` a row of `b{p}` on keys 1 to 5 with `w = 100p + k`
/// and `tag = 'p{p}_{k}'`. So `a` row 6 is matched nowhere.
fn seed(shape: Shape) -> String {
    let keys: Vec<String> = shape.all().map(|p| format!("k{p}")).collect();
    let values: Vec<String> = shape.all().map(|_| "i".to_string()).collect();
    let mut seed = vec![format!(
        "insert into public.a (id, {keys}, x, y) \
         select i, {values}, 10 * i, 'a' || i from generate_series(1, 6) i",
        keys = keys.join(", "),
        values = values.join(", "),
    )];
    for p in shape.all() {
        seed.push(format!(
            "insert into public.b{p} select k, k, {p}00 + k, 'p{p}_' || k \
             from generate_series(1, 5) k"
        ));
    }
    seed.join("; ")
}

/// Every key column of `a` set to `value`, for an `update ... set`.
fn every_key(shape: Shape, value: &str) -> String {
    let set: Vec<String> = shape.all().map(|p| format!("k{p} = {value}")).collect();
    set.join(", ")
}

/// The mixed traffic: each entry one sealed and drained page.
fn traffic(shape: Shape) -> Vec<String> {
    let last = shape.positions;
    let keys: Vec<String> = shape.all().map(|p| format!("k{p}")).collect();
    let twos: Vec<&str> = shape.all().map(|_| "2").collect();
    vec![
        // A root insert on key 2 everywhere, and a joined change it reads.
        format!(
            "insert into public.a (id, {}, x, y) values (7, {}, 70, 'a7'); \
             update public.b2 set w = w + 1 where k = 2",
            keys.join(", "),
            twos.join(", ")
        ),
        // A root move at position 2 alone; a joined delete at position 1.
        "update public.a set k2 = 5 where id = 1; delete from public.b1 where k = 3".to_string(),
        // A `NULL` key at position 1; a joined insert matching row 6 at the
        // last position.
        format!(
            "update public.a set k1 = null where id = 4; \
             insert into public.b{last} values (9, 6, {last}06, 'p{last}_6')"
        ),
        // A joined key move at the last position; a root value change.
        format!(
            "update public.b{last} set k = 8 where k = 4; \
             update public.a set x = x + 1 where id in (2, 3)"
        ),
        // A root delete, and a root insert on key 3 everywhere.
        format!(
            "delete from public.a where id = 5; \
             insert into public.a (id, {}, x, y) values (8, {}, 80, 'a8')",
            keys.join(", "),
            shape.all().map(|_| "3").collect::<Vec<_>>().join(", ")
        ),
        // A root row moving every key, in the page of a joined change on its
        // new key at position 1.
        format!(
            "update public.a set {} where id = 2; \
             update public.b1 set tag = 'moved' where k = 4",
            every_key(shape, "4")
        ),
        // A joined key set to `NULL` at position 2, and a root `NULL` key at
        // the last position.
        format!(
            "update public.b2 set k = null where k = 1; \
             update public.a set k{last} = null where id = 3"
        ),
    ]
}

/// `star`'s definition id.
async fn id(d: &Driver, star: Star) -> i64 {
    star::definition_id(d, star).await
}

/// The divergence column of `column` in `star`'s hidden table `part` (`r`,
/// `m`, `m2`, ...).
async fn hidden_column(d: &Driver, star: Star, part: &str, column: &str) -> String {
    format!("join1_{}_{part}.{column}", id(d, star).await)
}

fn cell(key: &str, column: &str, persisted: Option<&str>, recomputed: Option<&str>) -> Divergence {
    Divergence::Cell {
        key: key.to_string(),
        column: column.to_string(),
        persisted: persisted.map(str::to_string),
        recomputed: recomputed.map(str::to_string),
    }
}

/// Asserts a walk of `star`'s `self_check` finds exactly `expected`, at a
/// page of 2 keys (so every part pages, and a cursor crosses into every
/// position's parts) and at 1000.
async fn assert_found(d: &Driver, star: Star, expected: Vec<Divergence>, when: &str) {
    let mut expected = expected;
    expected.sort_by_key(|d| format!("{d:?}"));
    for limit in [2, 1000] {
        let found = star::self_check(d, &star.target(), limit).await;
        assert_eq!(
            found,
            expected,
            "{} at {limit} keys a page, {when}",
            star.name()
        );
    }
}

/// Writes `plant`, asserts `star`'s walk finds `expected`, writes `undo` and
/// asserts it finds nothing.
async fn planted(d: &Driver, star: Star, plant: &str, undo: &str, expected: Vec<Divergence>) {
    d.ctl.batch_execute(plant).await.expect(plant);
    assert_found(d, star, expected, plant).await;
    d.ctl.batch_execute(undo).await.expect(undo);
    assert_found(d, star, Vec::new(), undo).await;
}

#[tokio::test]
async fn a_star_s_self_check_is_clean_after_mixed_traffic() {
    let _serial = star::serial().await;
    knob();
    for shape in [N3, N4] {
        let stars = shape.stars();
        let mut d = star::start_seeded(&stars, &seed(shape)).await;
        for star in &stars {
            assert_found(&d, *star, Vec::new(), "after the build").await;
        }
        for page in traffic(shape) {
            star::step(&mut d, &page).await;
        }
        // The oracle, `R` and every `M_p`, and the self_check at 1000.
        star::assert_oracle(&mut d, &stars).await;
        for star in &stars {
            assert_found(&d, *star, Vec::new(), "after the traffic").await;
        }
    }
}

#[tokio::test]
async fn hand_edits_at_position_2_are_each_reported_until_undone() {
    let _serial = star::serial().await;
    knob();
    let stars = N3.stars();
    let d = star::start_seeded(&stars, &seed(N3)).await;
    for star in stars {
        let t = star.target();
        let r = star::r_table(&d, star).await;
        let m2 = star::m_table(&d, star, 2).await;
        // A target cell read from position 2.
        planted(
            &d,
            star,
            &format!("update {t} set tag2 = 'zz' where id = 2"),
            &format!("update {t} set tag2 = 'p2_2' where id = 2"),
            vec![cell("2", "tag2", Some("zz"), Some("p2_2"))],
        )
        .await;
        // `R`'s key column for position 2.
        planted(
            &d,
            star,
            &format!("update {r} set k2 = 9 where __from_key = '2'"),
            &format!("update {r} set k2 = 2 where __from_key = '2'"),
            vec![cell(
                "2",
                &hidden_column(&d, star, "r", "k2").await,
                Some("9"),
                Some("2"),
            )],
        )
        .await;
        // An `M_2` value.
        planted(
            &d,
            star,
            &format!("update {m2} set w = 999 where __from_key = '3'"),
            &format!("update {m2} set w = 203 where __from_key = '3'"),
            vec![cell(
                "3",
                &hidden_column(&d, star, "m2", "w").await,
                Some("999"),
                Some("203"),
            )],
        )
        .await;
        // `b2` row 3 with no live `M_2` entry.
        planted(
            &d,
            star,
            &format!("update {m2} set __tombstone = true where __from_key = '3'"),
            &format!("update {m2} set __tombstone = false where __from_key = '3'"),
            vec![cell(
                "3",
                &hidden_column(&d, star, "m2", "__entry").await,
                None,
                Some("live"),
            )],
        )
        .await;
        // A live `M_2` entry with no `b2` row, on a key no row holds.
        let copy = |key: &str, k: &str| {
            format!(
                "insert into {m2} select (jsonb_populate_record(m, \
                     jsonb_build_object('__from_key', '{key}', 'k', {k}))).* \
                 from {m2} m where __from_key = '3'"
            )
        };
        planted(
            &d,
            star,
            &copy("9", "9"),
            &format!("delete from {m2} where __from_key = '9'"),
            vec![cell(
                "9",
                &hidden_column(&d, star, "m2", "__entry").await,
                Some("live"),
                None,
            )],
        )
        .await;
        // The same entry on key 3: two live `M_2` entries on one key.
        planted(
            &d,
            star,
            &copy("9", "3"),
            &format!("delete from {m2} where __from_key = '9'"),
            vec![
                cell(
                    "9",
                    &hidden_column(&d, star, "m2", "__entry").await,
                    Some("live"),
                    None,
                ),
                cell(
                    "3",
                    &hidden_column(&d, star, "m2", "live_entries").await,
                    Some("2"),
                    Some("1"),
                ),
            ],
        )
        .await;
    }
}

/// `star`'s definition with its `JOIN` clauses written in `order` (a
/// permutation of its positions), under the name `name`. The `SELECT` list
/// is the same as [`Star::definition`]'s, so the target is the same table.
fn permuted(star: Star, order: &[usize], name: &str) -> String {
    let joins: Vec<String> = order
        .iter()
        .map(|&p| {
            let kind = match star.kind(p) {
                Kind::Left => "LEFT JOIN",
                Kind::Inner => "INNER JOIN",
            };
            format!("{kind} public.b{p} ON a.k{p} = b{p}.k")
        })
        .collect();
    let total: String = star
        .shape
        .all()
        .map(|p| format!(" + COALESCE(b{p}.w, 0)"))
        .collect();
    let tags: String = star
        .shape
        .all()
        .map(|p| format!(", b{p}.tag AS tag{p}"))
        .collect();
    format!(
        "TRANSFORM {name} FROM public.a {} SELECT a.x{total} AS total{tags}, a.y AS y",
        joins.join(" ")
    )
}

/// Every permutation of `1..=n`, the identity first.
fn permutations(n: usize) -> Vec<Vec<usize>> {
    fn go(rest: Vec<usize>, prefix: Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if rest.is_empty() {
            out.push(prefix);
            return;
        }
        for (i, &p) in rest.iter().enumerate() {
            let mut rest = rest.clone();
            rest.remove(i);
            let mut prefix = prefix.clone();
            prefix.push(p);
            go(rest, prefix, out);
        }
    }
    let mut out = Vec::new();
    go((1..=n).collect(), Vec::new(), &mut out);
    out
}

/// O1 at `shape`, over `stars`: each star and every permutation of its
/// `JOIN` clauses defined side by side, the same traffic, then each
/// permutation's target equals the star's (`EXCEPT ALL` both ways), and
/// every one's `self_check` is clean.
async fn o1(shape: Shape, stars: &[Star]) {
    let orders = permutations(shape.positions);
    let mut definitions = Vec::new();
    let mut names = Vec::new();
    for star in stars {
        for (i, order) in orders.iter().enumerate() {
            let name = format!("{}_o{i}", star.name());
            definitions.push(permuted(*star, order, &name));
            names.push((*star, order.clone(), name));
        }
    }
    let ddl = format!("{}; {}", shape.ddl(), seed(shape));
    let columns = shape.columns();
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(c, t)| (c.as_str(), *t)).collect();
    let tables = shape.tables();
    let tables: Vec<&str> = tables.iter().map(String::as_str).collect();
    let definitions: Vec<&str> = definitions.iter().map(String::as_str).collect();
    let mut d = Driver::start(&ddl, &columns, &definitions, &tables).await;
    for page in traffic(shape) {
        star::step(&mut d, &page).await;
    }
    d.settle().await;
    let target_columns = {
        let tags: Vec<String> = shape.all().map(|p| format!("tag{p}")).collect();
        format!("id, total, {}, y", tags.join(", "))
    };
    for (star, order, name) in &names {
        let base = format!("select {target_columns} from public.{}_o0", star.name());
        let this = format!("select {target_columns} from public.{name}");
        for (left, right) in [(&this, &base), (&base, &this)] {
            let only = d.rows(&format!("({left}) except all ({right})")).await;
            assert!(
                only.is_empty(),
                "{name} (JOIN order {order:?}) and {}_o0 differ: {only:?} only in ({left})",
                star.name()
            );
        }
        let live = d
            .rows(&format!("({this}) except all ({})", star.expected()))
            .await;
        assert!(live.is_empty(), "{name}: {live:?} not in the live join");
        let target = format!("public.{name}");
        let found = star::self_check(&d, &target, 1000).await;
        assert!(found.is_empty(), "{name} self_check finds {found:?}");
        let diverging = trellis::staging::one_to_one_join_diverging_keys(d.pool(), &target)
            .await
            .expect("the oracle");
        assert!(diverging.is_empty(), "{name} diverges at {diverging:?}");
    }
}

#[tokio::test]
async fn o1_a_star_and_its_join_clauses_permuted_reach_the_same_target_at_n_3() {
    let _serial = star::serial().await;
    knob();
    o1(N3, &N3.stars()).await;
}

#[tokio::test]
async fn o1_a_star_and_its_join_clauses_permuted_reach_the_same_target_at_n_4() {
    let _serial = star::serial().await;
    knob();
    // Every order of the three positions, over two of the strength-2 cover's
    // kind tuples (an `INNER` at the first position, and in the middle).
    let stars: Vec<Star> = N4.stars().into_iter().skip(1).take(2).collect();
    o1(N4, &stars).await;
}

#[tokio::test]
async fn a_star_s_snapshot_reads_every_position() {
    let _serial = star::serial().await;
    knob();
    let star = N3.stars()[0];
    let d = star::start_seeded(&[star], &seed(N3)).await;
    let s = star_snapshot(d.pool(), &star.target())
        .await
        .expect("a snapshot")
        .expect("no lock is held, so the read isn't skipped");
    assert_eq!(s.positions.len(), 2);
    assert_eq!(s.fields, ["total", "tag1", "tag2", "y"]);
    // `R`'s entries carry a join key per position.
    let entry = s.from_entries.iter().find(|e| e.key == "2").expect("R 2");
    let some = |v: &str| vec![Some(v.to_string())];
    assert_eq!(entry.join_keys, [some("2"), some("2")]);
    for (p, view) in s.positions.iter().enumerate() {
        let p = p + 1;
        assert!(view.left, "position {p}");
        assert_eq!(view.joined_entries.len(), 5, "position {p}");
        assert_eq!(view.joined_applied.rows.len(), 5, "position {p}");
        // `J`'s rows at this position alone, through its own key columns:
        // never a `NULL` read from another position's.
        let mut keys: Vec<Option<String>> = view.keys.iter().flatten().cloned().collect();
        keys.sort();
        keys.dedup();
        assert!(keys.iter().all(Option::is_some), "position {p}: {keys:?}");
        for k in 1..=5 {
            assert!(
                keys.contains(&Some(k.to_string())),
                "position {p} has no J row for key {k}: {keys:?}"
            );
        }
        assert!(view.markers.is_empty(), "position {p}");
    }
    // Each live `R` entry read with its `M` entry at each position, giving
    // the target's row.
    let row = s.joined_rows.iter().find(|r| r.key == "3").expect("row 3");
    assert_eq!(row.joined, [Some("3".to_string()), Some("3".to_string())]);
    let target = s.rows.iter().find(|r| r.key == "3").expect("target 3");
    assert_eq!(row.values, target.values);
    let unmatched = s.joined_rows.iter().find(|r| r.key == "6").expect("row 6");
    assert_eq!(unmatched.joined, [None, None]);
    // The two-table read refuses a star rather than read position 1 alone.
    let refused = hidden_snapshot(d.pool(), &star.target()).await;
    assert!(refused.is_err(), "{refused:?}");
    // A `J` row dropped at position 2 is missing at position 2 alone.
    let id = id(&d, star).await;
    d.ctl
        .batch_execute(&format!(
            "delete from {DEFAULT_SCHEMA}.join1_{id}_j where position = 2 and p2_k2 = 4"
        ))
        .await
        .expect("drop a J row");
    let s = star_snapshot(d.pool(), &star.target())
        .await
        .expect("a snapshot")
        .expect("not skipped");
    let has = |p: usize, k: &str| {
        s.positions[p - 1]
            .keys
            .iter()
            .any(|key| key == &vec![Some(k.to_string())])
    };
    assert!(has(1, "4"), "position 1 keeps key 4");
    assert!(!has(2, "4"), "position 2 lost key 4");
    // A fan-out marker at position 2 is read at position 2 alone. Nothing
    // steps the driver after it, so no chunk takes it.
    d.ctl
        .batch_execute(&format!(
            "insert into {DEFAULT_SCHEMA}.join1_fan_out \
                 (transform_id, position, join_key, seg_seq) \
             values ({id}, 2, array['4'], 0)"
        ))
        .await
        .expect("plant a marker");
    let s = star_snapshot(d.pool(), &star.target())
        .await
        .expect("a snapshot")
        .expect("not skipped");
    assert!(s.positions[0].markers.is_empty(), "{:?}", s.positions[0]);
    let markers: Vec<&Vec<String>> = s.positions[1].markers.iter().map(|m| &m.join_key).collect();
    assert_eq!(markers, [&vec!["4".to_string()]]);
}

/// Every page of one comparison of `star`'s target, with no wait and no
/// re-check: the divergences, and whether a page couldn't vouch for every
/// key.
async fn compare_walk(d: &Driver, star: Star) -> (Vec<Divergence>, bool) {
    let mut after = None;
    let (mut found, mut all) = (Vec::new(), false);
    loop {
        let page = compare_page(d.pool(), &star.target(), SelfCheckScope { after, limit: 4 })
            .await
            .expect("a page of the comparison");
        found.extend(page.divergences);
        all |= page.uncertain_all;
        after = page.next_after;
        if after.is_none() {
            return (found, all);
        }
    }
}

/// A change the ring can't vouch for (an image-less `Recompute`) on the
/// table at any position touches every `a` row on its key, so the target's
/// pages can't vouch for any key: as a two-table `JOIN`'s `b` ring does, at
/// every position of the star.
#[tokio::test]
async fn an_uncertain_key_at_any_position_leaves_every_key_uncertain() {
    let _serial = star::serial().await;
    knob();
    let star = N4.stars()[0];
    for p in N4.all() {
        let d = star::start_seeded(&[star], &seed(N4)).await;
        let (found, all) = compare_walk(&d, star).await;
        assert_eq!(
            (found, all),
            (Vec::new(), false),
            "position {p}, nothing pending"
        );
        d.stage_recomputes(&N4.b_at(p), &["3"]).await;
        let (found, all) = compare_walk(&d, star).await;
        assert_eq!(found, Vec::new(), "position {p}");
        assert!(
            all,
            "position {p}: an image-less change at b{p} leaves every key uncertain"
        );
    }
}
