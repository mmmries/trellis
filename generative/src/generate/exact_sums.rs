//! `SUM`/`AVG` over `real`, `double precision` and `interval` (#1119), as
//! an arm of the plain `MIN`/`MAX` tier ([`super::min_max`]) and of the
//! `JOIN` tier ([`super::join`]). Each arm is drawn after everything its
//! tier drew before, so a case without it is the case it always was.
//!
//! The values ([`double_value`], [`real_value`], [`interval_value`]) take the
//! shapes where an order-dependent sum and the exact one differ: large
//! magnitudes that cancel (`1e20`, `1`, `-1e20`), inexact decimals (`0.1`),
//! subnormals, `-0`, `NaN` and the infinities, and `interval`s with mixed
//! signs, equal values in different fields (`1 mon`, `30 days`), and fields
//! near their type's limits (Postgres 17's infinities on a server that has
//! them, [`interval_infinities`]).
//!
//! No group's sum may leave its type's range at any moment: the engine
//! would hold the group's key in quarantine, which stalls every convergence
//! wait. Under concurrent drains a group may hold any subset of the values
//! ever written, so the plain arm keeps a budget of the magnitudes it has
//! written, per sign and field ([`Budget`]), and draws a large value only
//! while the budget covers it. The `JOIN` arm's values are bounded instead
//! so that every product of the tier's row counts stays in range.

use std::sync::OnceLock;

use trellis::dev::defs::ast::{Expr, FieldDef, TransformDef, ValueType};
use trellis::dev::defs::{FloatWidth, PgType};

use crate::model::{Column, Op, Program};

/// Whether the Postgres the tests run against (the `postgres` on `PATH`,
/// as the testkit starts it) has infinite `interval`s: 17 and later.
pub fn interval_infinities() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        let out = std::process::Command::new("postgres")
            .arg("--version")
            .output()
            .ok();
        let text = out.map(|o| String::from_utf8_lossy(&o.stdout).into_owned());
        let major = text
            .as_deref()
            .and_then(|t| t.split_whitespace().nth(2))
            .and_then(|v| v.split('.').next())
            .and_then(|m| m.parse::<u32>().ok());
        major.is_some_and(|m| m >= 17)
    })
}

/// A summed argument's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SumType {
    Real,
    Double,
    Interval,
}

impl SumType {
    pub const ALL: [SumType; 3] = [SumType::Real, SumType::Double, SumType::Interval];

    pub fn value_type(self) -> ValueType {
        match self {
            SumType::Real => ValueType::Float(FloatWidth::Float4),
            SumType::Double => ValueType::Float(FloatWidth::Float8),
            SumType::Interval => ValueType::Other(PgType::Interval),
        }
    }

    /// The plain arm's column of this type.
    pub fn column(self) -> &'static str {
        match self {
            SumType::Real => "rs",
            SumType::Double => "fs",
            SumType::Interval => "ivs",
        }
    }
}

/// splitmix64, so a seed is the arm's whole stream of values.
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn signed(&mut self, magnitude: i64) -> i64 {
        self.below(2 * magnitude as u64 + 1) as i64 - magnitude
    }

    fn sign(&mut self) -> f64 {
        if self.next() & 1 == 0 { 1.0 } else { -1.0 }
    }
}

/// How much magnitude each sign of each field may still take: the values
/// written so far, any subset of which a group may hold, sum to at most
/// the type's limit.
#[derive(Debug, Clone)]
pub(crate) struct Budget {
    double: [f64; 2],
    real: [f64; 2],
    /// Months, days, microseconds; positive then negative.
    interval: [[i128; 2]; 3],
    /// Which infinite `interval` is drawn: `infinity` and `-infinity` in one
    /// group raise.
    pub(crate) negative_infinity: bool,
}

impl Default for Budget {
    fn default() -> Self {
        // Below each limit, so no sum reaches it: a `double precision` or
        // `real` sum that rounds to its maximum is fine, and one that is an
        // infinity's fields reads as the infinity on Postgres 17.
        Budget {
            double: [1.7e308; 2],
            real: [3.3e38; 2],
            interval: [
                [i128::from(i32::MAX) - 1; 2],
                [i128::from(i32::MAX) - 1; 2],
                [i128::from(i64::MAX) - 1; 2],
            ],
            negative_infinity: false,
        }
    }
}

impl Budget {
    /// Takes `v` from the budget if it covers it.
    fn take_float(slot: &mut [f64; 2], v: f64) -> bool {
        if !v.is_finite() {
            return true;
        }
        let side = usize::from(v < 0.0);
        if v.abs() <= slot[side] {
            slot[side] -= v.abs();
            true
        } else {
            false
        }
    }

    fn take_interval(&mut self, fields: [i128; 3]) -> bool {
        let fits = fields
            .iter()
            .zip(&self.interval)
            .all(|(v, slot)| v.abs() <= slot[usize::from(*v < 0)]);
        if fits {
            for (v, slot) in fields.iter().zip(&mut self.interval) {
                slot[usize::from(*v < 0)] -= v.abs();
            }
        }
        fits
    }
}

/// A float spelled as Postgres reads it.
fn float_text(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string()
    } else {
        format!("{v:e}")
    }
}

/// A `real` spelled as Postgres reads it.
fn real_text(v: f32) -> String {
    if v.is_finite() {
        format!("{v:e}")
    } else {
        float_text(f64::from(v))
    }
}

/// A `double precision` value (per mille): `NULL` 100; small halves, which
/// cancel exactly, 300; inexact decimals 200; magnitudes that cancel around
/// a small one (`1e20`, `1e16`, `2^53`, `1 + ε`) 120; subnormals and the
/// smallest normals 80; random bit patterns of binades near 1, 100; of any
/// binade up to `1e300`, 15; near the maximum while the budget covers them,
/// 10; both zeros 60; `NaN` and the infinities 5 each. The large and special
/// values are rare, so most groups' sums aren't one of them.
pub(crate) fn double_value(rng: &mut Rng, budget: &mut Budget) -> Option<String> {
    let v = match rng.below(1000) {
        0..=99 => return None,
        100..=399 => rng.signed(40) as f64 / 2.0,
        400..=599 => rng.sign() * [0.1, 0.2, 0.3, 1.0 / 3.0, 2.0 / 3.0][rng.below(5) as usize],
        600..=719 => {
            rng.sign()
                * [1e20, 1e16, 9_007_199_254_740_992.0, 1.0 + f64::EPSILON][rng.below(4) as usize]
        }
        720..=799 => {
            rng.sign()
                * match rng.below(4) {
                    0 => f64::from_bits(1 + rng.below(16)),
                    1 => f64::from_bits(rng.below(1 << 52)),
                    2 => f64::MIN_POSITIVE,
                    _ => f64::from_bits(f64::MIN_POSITIVE.to_bits() - 1),
                }
        }
        800..=899 => {
            // A random mantissa in the binades from 2^-8 to 2^8.
            let exponent = 1023 - 8 + rng.below(17);
            rng.sign() * f64::from_bits((exponent << 52) | rng.below(1 << 52))
        }
        900..=914 => loop {
            let v = f64::from_bits(rng.next());
            if v.is_finite() && v.abs() <= 1e300 {
                break v;
            }
        },
        915..=924 => {
            let v = rng.sign() * f64::MAX / (2.0 + rng.below(4) as f64);
            if !Budget::take_float(&mut budget.double, v) {
                return Some("1".to_string());
            }
            v
        }
        925..=954 => 0.0,
        955..=984 => -0.0,
        985..=989 => f64::NAN,
        990..=994 => f64::INFINITY,
        _ => f64::NEG_INFINITY,
    };
    Some(float_text(v))
}

/// A `real` value, drawn as [`double_value`] draws but in `real`'s range:
/// its subnormals and its maximum.
pub(crate) fn real_value(rng: &mut Rng, budget: &mut Budget) -> Option<String> {
    let v: f32 = match rng.below(1000) {
        0..=99 => return None,
        100..=399 => rng.signed(40) as f32 / 2.0,
        400..=599 => {
            (rng.sign() as f32) * [0.1f32, 0.2, 0.3, 1.0 / 3.0, 2.0 / 3.0][rng.below(5) as usize]
        }
        600..=719 => {
            (rng.sign() as f32)
                * [1e20f32, 1e8, 16_777_216.0, 1.0 + f32::EPSILON][rng.below(4) as usize]
        }
        720..=799 => {
            (rng.sign() as f32)
                * match rng.below(4) {
                    0 => f32::from_bits(1 + rng.below(16) as u32),
                    1 => f32::from_bits(rng.below(1 << 23) as u32),
                    2 => f32::MIN_POSITIVE,
                    _ => f32::from_bits(f32::MIN_POSITIVE.to_bits() - 1),
                }
        }
        800..=899 => {
            let exponent = (127 - 8 + rng.below(17)) as u32;
            (rng.sign() as f32) * f32::from_bits((exponent << 23) | rng.below(1 << 23) as u32)
        }
        900..=914 => loop {
            let v = f32::from_bits(rng.next() as u32);
            if v.is_finite() && v.abs() <= 1e30 {
                break v;
            }
        },
        915..=924 => {
            let v = (rng.sign() as f32) * f32::MAX / (2.0 + rng.below(4) as f32);
            if !Budget::take_float(&mut budget.real, f64::from(v)) {
                return Some("1".to_string());
            }
            v
        }
        925..=954 => 0.0,
        955..=984 => -0.0,
        985..=989 => f32::NAN,
        990..=994 => f32::INFINITY,
        _ => f32::NEG_INFINITY,
    };
    Some(real_text(v))
}

/// An `interval` of `months`, `days` and `micros`, spelled in those units
/// (Postgres keeps each field as given).
fn interval_text(months: i128, days: i128, micros: i128) -> String {
    format!("{months} mons {days} days {micros} microseconds")
}

/// An `interval` value (per mille): `NULL` 100; small fields of mixed signs
/// 450; equal values in different fields (`1 mon`, `30 days`) 150;
/// microseconds past `2^53`, where `AVG`'s division in `double precision`
/// rounds, 120; a field near its limit while the budget covers it, 60; zero
/// 110; on Postgres 17+, an infinity of the budget's one sign, 10.
pub(crate) fn interval_value(rng: &mut Rng, budget: &mut Budget) -> Option<String> {
    let fields: [i128; 3] = match rng.below(1000) {
        0..=99 => return None,
        100..=549 => [
            i128::from(rng.signed(30)),
            i128::from(rng.signed(60)),
            i128::from(rng.signed(172_800_000_000)),
        ],
        550..=699 => match rng.below(6) {
            0 => [1, 0, 0],
            1 => [0, 30, 0],
            2 => [0, 1, 0],
            3 => [0, 0, 86_400_000_000],
            4 => [1, -30, 0],
            _ => [0, -1, 86_400_000_000],
        },
        700..=819 => [
            0,
            0,
            i128::from(rng.sign() as i64) * ((1 << 53) + i128::from(rng.signed(1 << 40))),
        ],
        820..=879 => match rng.below(3) {
            0 => [i128::from(rng.signed(i64::from(i32::MAX) / 3)), 0, 0],
            1 => [0, i128::from(rng.signed(i64::from(i32::MAX) / 3)), 0],
            _ => [0, 0, i128::from(rng.signed(i64::MAX / 3))],
        },
        880..=989 => [0, 0, 0],
        // One sign of infinity per budget: both in one group raise.
        _ if interval_infinities() => {
            return Some(
                if budget.negative_infinity {
                    "-infinity"
                } else {
                    "infinity"
                }
                .to_string(),
            );
        }
        _ => [0, 1, 0],
    };
    if !budget.take_interval(fields) {
        return Some("00:00:00".to_string());
    }
    Some(interval_text(fields[0], fields[1], fields[2]))
}

/// A value of `sum_type`.
pub(crate) fn value(sum_type: SumType, rng: &mut Rng, budget: &mut Budget) -> Option<String> {
    match sum_type {
        SumType::Real => real_value(rng, budget),
        SumType::Double => double_value(rng, budget),
        SumType::Interval => interval_value(rng, budget),
    }
}

/// `SUM` or `AVG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SumFn {
    Sum,
    Avg,
}

impl SumFn {
    fn name(self) -> &'static str {
        match self {
            SumFn::Sum => "SUM",
            SumFn::Avg => "AVG",
        }
    }
}

/// The plain arm's draw: each definition's (by index) exact-sum fields, and
/// the seed of its values.
#[derive(Debug, Clone, PartialEq)]
pub struct PlainSumsDraw {
    pub seed: u64,
    pub fields: Vec<Vec<(SumFn, SumType)>>,
}

/// The plain arm of a case whose op stream is seeded `seed`, for half the
/// seeds: one to three fields per definition.
pub fn plain_sums_of(seed: u64) -> Option<PlainSumsDraw> {
    let mut rng = Rng(seed ^ 0x1119_1119_1119_1119);
    if rng.below(2) == 0 {
        return None;
    }
    let fields = (0..3)
        .map(|_| {
            (0..=rng.below(3))
                .map(|_| {
                    let func = if rng.below(2) == 0 {
                        SumFn::Sum
                    } else {
                        SumFn::Avg
                    };
                    (func, SumType::ALL[rng.below(3) as usize])
                })
                .collect()
        })
        .collect();
    Some(PlainSumsDraw {
        seed: rng.next(),
        fields,
    })
}

/// The `JOIN` arm for a case whose first op drew `pick`, for three cases in
/// ten: the arguments' type, and whether the special values are written
/// (one of those cases in three: in a `JOIN`'s large groups they would
/// otherwise be the value of most groups).
pub fn join_sums_of(pick: usize) -> Option<(SumType, bool)> {
    let mut rng = Rng(pick as u64 ^ 0x1119_1119_1119_1119);
    let sum_type = match rng.below(10) {
        0 => SumType::Real,
        1 => SumType::Double,
        2 => SumType::Interval,
        _ => return None,
    };
    Some((sum_type, rng.below(3) == 0))
}

/// `program` (one table, `table`) with the plain arm: the table gets a
/// column of each [`SumType`] ([`SumType::column`]), definition `i` the
/// fields `draw.fields[i]` (named `sum_fs`, `avg_ivs`, …), every insert a
/// value of each, and one update in three a new value for one or two of
/// them.
pub fn with_plain_sums(mut program: Program, table: &str, draw: &PlainSumsDraw) -> Program {
    let mut rng = Rng(draw.seed);
    let mut budget = Budget {
        negative_infinity: rng.below(2) == 0,
        ..Budget::default()
    };
    for t in program.tables.iter_mut().filter(|t| t.name == table) {
        t.columns.extend(SumType::ALL.iter().map(|s| Column {
            name: s.column().to_string(),
            value_type: s.value_type(),
        }));
    }
    for (def, fields) in program.defs.iter_mut().zip(&draw.fields) {
        add_fields(def, fields);
    }
    for op in &mut program.ops {
        match op {
            Op::Insert { table: t, row, .. } if t == table => {
                for s in SumType::ALL {
                    row.push((s.column().to_string(), value(s, &mut rng, &mut budget)));
                }
            }
            Op::Update {
                table: t, changes, ..
            } if t == table && rng.below(3) == 0 => {
                for _ in 0..=rng.below(2) {
                    let s = SumType::ALL[rng.below(3) as usize];
                    if !changes.iter().any(|(c, _)| c == s.column()) {
                        changes.push((s.column().to_string(), value(s, &mut rng, &mut budget)));
                    }
                }
            }
            _ => {}
        }
    }
    program
}

/// Adds `fields` to `def`, once each.
fn add_fields(def: &mut TransformDef, fields: &[(SumFn, SumType)]) {
    for &(func, sum_type) in fields {
        let name = format!("{}_{}", func.name().to_lowercase(), sum_type.column());
        if def.fields.iter().any(|f| f.name == name) {
            continue;
        }
        def.fields.push(FieldDef {
            name,
            expr: Expr::FunctionCall {
                name: func.name().to_string(),
                args: vec![Expr::Column(sum_type.column().to_string())],
            },
        });
    }
}

/// The `JOIN` arm's value of draw `v` (the tier's `-20..60`) for an
/// argument of `sum_type`: a fixed table, a quarter of it the shapes the
/// module doc names, the rest small; the first three entries are the
/// special values only with `specials`. Every magnitude times the tier's
/// largest product of row counts (`450 × 450`) stays in range.
pub fn join_value(sum_type: SumType, v: i8, specials: bool) -> String {
    let i = i64::from(v) + 20;
    let i = if !specials && i < 3 { 40 + i } else { i };
    let k = i - 40;
    match sum_type {
        SumType::Double => float_text(match i {
            0 => f64::NAN,
            1 => f64::INFINITY,
            2 => f64::NEG_INFINITY,
            3 => -0.0,
            4 => f64::from_bits(1),
            5 => -f64::from_bits(f64::MIN_POSITIVE.to_bits() - 1),
            6 => f64::MIN_POSITIVE,
            7 => 1e20,
            8 => -1e20,
            9 => 1e300,
            10 => -1e300,
            11 => 0.1,
            12 => 0.2,
            13 => -0.3,
            14 => 1.0 / 3.0,
            15 => 1e16,
            16 => 1.0 + f64::EPSILON,
            17 => 9_007_199_254_740_992.0,
            18 => -1e-300,
            19 => 3.0 * f64::from_bits(1),
            _ => k as f64 / 4.0,
        }),
        SumType::Real => real_text(match i {
            0 => f32::NAN,
            1 => f32::INFINITY,
            2 => f32::NEG_INFINITY,
            3 => -0.0,
            4 => f32::from_bits(1),
            5 => -f32::from_bits(f32::MIN_POSITIVE.to_bits() - 1),
            6 => f32::MIN_POSITIVE,
            7 => 1e20,
            8 => -1e20,
            9 => 1e30,
            10 => -1e30,
            11 => 0.1,
            12 => 0.2,
            13 => -0.3,
            14 => 1.0 / 3.0,
            15 => 16_777_216.0,
            16 => 1.0 + f32::EPSILON,
            17 => 1e8,
            18 => -1e-30,
            19 => 3.0 * f32::from_bits(1),
            _ => k as f32 / 4.0,
        }),
        SumType::Interval => match i {
            // `infinity` alone: with `-infinity` a group would raise.
            0 if interval_infinities() => "infinity".to_string(),
            0 | 2 => interval_text(1, 0, 0),
            1 | 3 => interval_text(0, 30, 0),
            4 => interval_text(-1, 0, 0),
            5 => interval_text(0, 0, 86_400_000_000),
            6 => interval_text(0, 1, 0),
            7 => interval_text(0, 0, -1),
            8 => interval_text(1, -1, 0),
            9 => interval_text(0, -1, 86_399_999_999),
            10 => interval_text(10_000, 0, 0),
            11 => interval_text(0, -10_000, 0),
            12 => interval_text(0, 0, 40_000_000_000_000),
            13 => interval_text(0, 0, -39_999_999_999_999),
            14 => interval_text(2, -3, 14_706_789_000),
            15 => interval_text(-12, 2, 0),
            16 => interval_text(0, 3, -43_200_000_000),
            17 => interval_text(7, 0, 1),
            18 => interval_text(0, 0, 0),
            19 => interval_text(-1, 31, -1),
            _ => match k.rem_euclid(3) {
                0 => interval_text(0, i128::from(k), 0),
                1 => interval_text(0, 0, i128::from(k) * 1_800_000_000),
                _ => interval_text(i128::from(k), i128::from(k), 0),
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// No subset of the plain arm's values leaves its type's range: the
    /// values' magnitudes, per sign and field, sum below the limits, for
    /// many seeds and far more values than a case writes.
    #[test]
    fn no_subset_of_the_plain_values_overflows() {
        for seed in 0..50 {
            let mut rng = Rng(seed);
            let mut budget = Budget::default();
            let (mut double, mut real) = ([0f64; 2], [0f64; 2]);
            let mut interval = [[0i128; 2]; 3];
            let mut specials = 0;
            for _ in 0..3_000 {
                if let Some(v) = double_value(&mut rng, &mut budget) {
                    let v: f64 = v.parse().expect("a double");
                    if v.is_finite() {
                        double[usize::from(v < 0.0)] += v.abs();
                    } else {
                        specials += 1;
                    }
                }
                if let Some(v) = real_value(&mut rng, &mut budget) {
                    let v: f32 = v.parse().expect("a real");
                    if v.is_finite() {
                        real[usize::from(v < 0.0)] += f64::from(v.abs());
                    }
                }
                if let Some(v) = interval_value(&mut rng, &mut budget)
                    && let Some(rest) = v.strip_suffix(" microseconds")
                {
                    let parts: Vec<i128> = rest.split(' ').filter_map(|p| p.parse().ok()).collect();
                    for (field, p) in parts.iter().enumerate() {
                        interval[field][usize::from(*p < 0)] += p.abs();
                    }
                }
            }
            assert!(specials > 0);
            assert!(double.iter().all(|s| *s < f64::MAX), "{double:?}");
            assert!(real.iter().all(|s| *s < f64::from(f32::MAX)), "{real:?}");
            for (field, limit) in interval.iter().zip([
                i128::from(i32::MAX),
                i128::from(i32::MAX),
                i128::from(i64::MAX),
            ]) {
                assert!(field.iter().all(|s| *s < limit), "{interval:?}");
            }
        }
    }

    /// Every `JOIN` value, times the tier's largest product of row counts,
    /// stays in its type's range.
    #[test]
    fn join_values_stay_in_range_over_every_product() {
        const PRODUCT: f64 = 450.0 * 450.0;
        for v in -20i8..60 {
            let d: f64 = join_value(SumType::Double, v, true).parse().unwrap();
            assert!(!d.is_finite() || (d.abs() * PRODUCT).is_finite());
            let r: f32 = join_value(SumType::Real, v, true).parse().unwrap();
            assert!(!r.is_finite() || f64::from(r.abs()) * PRODUCT < f64::from(f32::MAX));
            let text = join_value(SumType::Interval, v, true);
            if let Some(rest) = text.strip_suffix(" microseconds") {
                let parts: Vec<f64> = rest.split(' ').filter_map(|p| p.parse().ok()).collect();
                assert!(parts[0].abs() * PRODUCT < f64::from(i32::MAX));
                assert!(parts[1].abs() * PRODUCT < f64::from(i32::MAX));
                assert!(parts[2].abs() * PRODUCT < 9.2e18);
            }
        }
    }
}
