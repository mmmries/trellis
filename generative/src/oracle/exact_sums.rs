//! The oracle's own `SUM`/`AVG` over `real`, `double precision` and
//! `interval` (#1119): the exact sum, computed here in Rust from the group's
//! values as Postgres prints them, not Postgres's order-dependent `sum()`.
//!
//! - **Float.** Every finite value is an integer multiple of `2^-1074`, so
//!   the group's sum is one exact integer in that unit ([`Big`]). The `SUM`
//!   rounds it once (ties to even) to the result type: `real` for a `real`
//!   argument, else `double precision`. The `AVG` rounds the exact quotient
//!   by the count once to `double precision`; a quotient of at most
//!   `2^-1075` reads `0`. `NaN` and the infinities are counted: `NaN` while
//!   a `NaN` or both infinities are counted, else the infinity. A finite sum
//!   that rounds past the type's range raises, as Postgres's cast does.
//! - **`interval`.** The fields (months, days, microseconds) are summed
//!   exactly. The `SUM` raises when a field's sum is out of its range, or
//!   on Postgres 17 and later when the fields are an infinity's. The `AVG`
//!   divides the exact sums as Postgres's `interval_div` does, step by step
//!   in `double precision`. `infinity` and `-infinity` (Postgres 17+) are
//!   counted; both at once raise.
//!
//! A raise is the cell [`raises`] returns: the engine holds such a group's
//! key in quarantine instead of writing a value, which no target text
//! equals.

use std::cmp::Ordering;

/// What an exact-sum field aggregates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Real,
    Double,
    Interval,
}

/// `SUM` or `AVG`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Func {
    Sum,
    Avg,
}

/// The cell of a group whose evaluation raises `message`.
pub(super) fn raises(message: &str) -> String {
    format!("ERROR: {message}")
}

/// The field's value for a group whose non-`NULL` argument values print as
/// `values`, as text; `None` (SQL `NULL`) for no values. `pg17` says the
/// server has infinite intervals.
///
/// # Panics
///
/// On a value that doesn't read as `kind`: Postgres printed it.
pub(super) fn evaluate(kind: Kind, func: Func, values: &[&str], pg17: bool) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    Some(match kind {
        Kind::Real | Kind::Double => {
            let parsed: Vec<f64> = values
                .iter()
                .map(|v| {
                    // A `real` reads as one, then widens exactly.
                    let parsed = match kind {
                        Kind::Real => v.parse::<f32>().map(f64::from),
                        _ => v.parse::<f64>(),
                    };
                    parsed.unwrap_or_else(|_| panic!("oracle: {v:?} is not a float"))
                })
                .collect();
            match float_result(&parsed, kind, func) {
                // A `real` `SUM` prints as one: it was rounded to one.
                Ok(value) => float_text(value, kind == Kind::Real && func == Func::Sum),
                Err(message) => raises(message),
            }
        }
        Kind::Interval => {
            let parsed: Vec<Interval> = values
                .iter()
                .map(|v| {
                    Interval::parse(v).unwrap_or_else(|| panic!("oracle: {v:?} is not an interval"))
                })
                .collect();
            match interval_result(&parsed, func, pg17) {
                Ok(value) => value.render(),
                Err(message) => raises(message),
            }
        }
    })
}

/// A float as text that parses back to it: Postgres's spellings of the
/// special values, Rust's shortest round trip otherwise (of a `real` with
/// `real`).
fn float_text(value: f64, real: bool) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else if real {
        format!("{:e}", value as f32)
    } else {
        format!("{value:e}")
    }
}

/// A float field's value: `kind`'s values are `real`s widened (exactly) or
/// `double precision`s.
fn float_result(values: &[f64], kind: Kind, func: Func) -> Result<f64, &'static str> {
    let (mut nan, mut up, mut down) = (false, false, false);
    let (mut positive, mut negative) = (Big::default(), Big::default());
    for &v in values {
        if v.is_nan() {
            nan = true;
        } else if v == f64::INFINITY {
            up = true;
        } else if v == f64::NEG_INFINITY {
            down = true;
        } else {
            let bits = v.to_bits();
            let exponent = (bits >> 52) & 0x7ff;
            let fraction = bits & ((1 << 52) - 1);
            // A normal value is `(2^52 + fraction) × 2^(exponent - 1075)`, a
            // subnormal `fraction × 2^-1074`: in units of `2^-1074`, the
            // mantissa shifted left `max(exponent, 1) - 1` bits.
            let mantissa = if exponent == 0 {
                fraction
            } else {
                fraction | (1 << 52)
            };
            let shift = exponent.max(1) as u32 - 1;
            if v.is_sign_negative() {
                negative.add_shifted(mantissa, shift);
            } else {
                positive.add_shifted(mantissa, shift);
            }
        }
    }
    if nan || (up && down) {
        return Ok(f64::NAN);
    }
    if up {
        return Ok(f64::INFINITY);
    }
    if down {
        return Ok(f64::NEG_INFINITY);
    }
    let (negative_sum, magnitude) = match positive.cmp(&negative) {
        Ordering::Less => (true, negative.sub(&positive)),
        _ => (false, positive.sub(&negative)),
    };
    let count = values.len() as u64;
    let rounded = match func {
        // `real`: 24 bits, and no unit below `2^-149` (`2^925` of ours).
        Func::Sum if kind == Kind::Real => {
            let r = round(&magnitude, 1, 24, 925);
            if r > f64::from(f32::MAX) {
                return Err("value out of range: overflow");
            }
            r
        }
        Func::Sum => round(&magnitude, 1, 53, 0),
        Func::Avg => {
            // The exact quotient is at most `2^-1075` (half a unit of ours)
            // when twice the magnitude is at most the count.
            let mut twice = magnitude.clone();
            twice.add_big(&magnitude);
            if twice.cmp(&Big::from(count)) != Ordering::Greater {
                return Ok(0.0);
            }
            round(&magnitude, count, 53, 0)
        }
    };
    if rounded.is_infinite() {
        return Err("value out of range: overflow");
    }
    Ok(if negative_sum { -rounded } else { rounded })
}

/// `magnitude / count` (in units of `2^-1074`) rounded, ties to even, to a
/// `precision`-bit mantissa whose unit is at least `2^min_unit` of ours;
/// `Infinity` past `double precision`'s range.
fn round(magnitude: &Big, count: u64, precision: u32, min_unit: u32) -> f64 {
    let (quotient, remainder) = magnitude.div_rem(count);
    let length = quotient.bit_len();
    let unit = length.saturating_sub(precision).max(min_unit);
    let mut mantissa = quotient.bits(unit, precision + 1);
    if unit == 0 {
        // The quotient is whole in our unit already: the remainder alone
        // rounds it.
        match (u128::from(remainder) * 2).cmp(&u128::from(count)) {
            Ordering::Greater => mantissa += 1,
            Ordering::Equal if mantissa & 1 == 1 => mantissa += 1,
            _ => {}
        }
    } else {
        // What is dropped against half a unit: the bit below the unit, then
        // whatever lies under it (a nonzero remainder counts too).
        let half = quotient.bit(unit - 1);
        let tail = quotient.any_below(unit - 1) || remainder != 0;
        if half && (tail || mantissa & 1 == 1) {
            mantissa += 1;
        }
    }
    compose(mantissa, unit as i32 - 1074)
}

/// `mantissa × 2^exponent`, exactly (`Infinity` when out of range).
fn compose(mantissa: u64, exponent: i32) -> f64 {
    let mut value = mantissa as f64;
    let mut exponent = exponent;
    // Scale in steps that stay exact: powers of two within the normal range.
    while exponent > 0 {
        let step = exponent.min(1000);
        value *= f64::from_bits(((1023 + step) as u64) << 52);
        exponent -= step;
    }
    while exponent < 0 {
        // Down to a normal result, then one final step that may make it
        // subnormal: `mantissa` has at most 54 bits, so that step loses
        // nothing a correctly rounded mantissa holds.
        let step = (-exponent).min(1000);
        value *= f64::from_bits(((1023 - step) as u64) << 52);
        exponent += step;
    }
    value
}

/// An unsigned integer of any size, little-endian 64-bit limbs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Big(Vec<u64>);

impl From<u64> for Big {
    fn from(value: u64) -> Self {
        Big(vec![value])
    }
}

impl Big {
    fn trim(&mut self) {
        while self.0.last() == Some(&0) {
            self.0.pop();
        }
    }

    /// `self += value << shift`.
    fn add_shifted(&mut self, value: u64, shift: u32) {
        let wide = u128::from(value) << (shift % 64);
        let limb = (shift / 64) as usize;
        self.add_at(limb, wide as u64);
        self.add_at(limb + 1, (wide >> 64) as u64);
        self.trim();
    }

    /// `self += value << (64 × limb)`.
    fn add_at(&mut self, mut limb: usize, value: u64) {
        let mut carry = u128::from(value);
        while carry != 0 {
            if self.0.len() <= limb {
                self.0.resize(limb + 1, 0);
            }
            let sum = u128::from(self.0[limb]) + carry;
            self.0[limb] = sum as u64;
            carry = sum >> 64;
            limb += 1;
        }
    }

    fn add_big(&mut self, other: &Big) {
        for (i, &limb) in other.0.iter().enumerate() {
            self.add_shifted(limb, (i * 64) as u32);
        }
    }

    /// `self - other`, `self >= other`.
    fn sub(&self, other: &Big) -> Big {
        let mut out = self.0.clone();
        let mut borrow = 0i128;
        for (i, limb) in out.iter_mut().enumerate() {
            let rhs = i128::from(other.0.get(i).copied().unwrap_or(0));
            let mut diff = i128::from(*limb) - rhs - borrow;
            borrow = 0;
            if diff < 0 {
                diff += 1 << 64;
                borrow = 1;
            }
            *limb = diff as u64;
        }
        assert_eq!(borrow, 0, "subtracting a larger integer");
        let mut out = Big(out);
        out.trim();
        out
    }

    fn cmp(&self, other: &Big) -> Ordering {
        let (a, b) = (self.0.len(), other.0.len());
        if a != b {
            return a.cmp(&b);
        }
        for i in (0..a).rev() {
            match self.0[i].cmp(&other.0[i]) {
                Ordering::Equal => continue,
                o => return o,
            }
        }
        Ordering::Equal
    }

    fn div_rem(&self, divisor: u64) -> (Big, u64) {
        let mut out = vec![0u64; self.0.len()];
        let mut remainder = 0u128;
        for i in (0..self.0.len()).rev() {
            let current = (remainder << 64) | u128::from(self.0[i]);
            out[i] = (current / u128::from(divisor)) as u64;
            remainder = current % u128::from(divisor);
        }
        let mut out = Big(out);
        out.trim();
        (out, remainder as u64)
    }

    fn bit_len(&self) -> u32 {
        match self.0.last() {
            None => 0,
            Some(&top) => (self.0.len() as u32 - 1) * 64 + (64 - top.leading_zeros()),
        }
    }

    fn bit(&self, at: u32) -> bool {
        self.0
            .get((at / 64) as usize)
            .is_some_and(|limb| limb >> (at % 64) & 1 == 1)
    }

    /// The `count` bits (at most 64) from bit `from` up.
    fn bits(&self, from: u32, count: u32) -> u64 {
        (0..count)
            .filter(|i| self.bit(from + i))
            .fold(0u64, |acc, i| acc | (1 << i))
    }

    /// Whether any bit below `at` is set.
    fn any_below(&self, at: u32) -> bool {
        let whole = (at / 64) as usize;
        self.0.iter().take(whole).any(|&l| l != 0)
            || (!at.is_multiple_of(64)
                && self
                    .0
                    .get(whole)
                    .is_some_and(|l| l & ((1u64 << (at % 64)) - 1) != 0))
    }
}

/// An `interval`: its fields, or an infinity (Postgres 17+).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Interval {
    Finite {
        months: i64,
        days: i64,
        micros: i128,
    },
    Infinity,
    NegativeInfinity,
}

impl Interval {
    /// The value of `text` as `IntervalStyle = postgres` prints it (or
    /// Postgres reads it: `<number> <unit>` pairs and one
    /// `[+-]HH:MM:SS[.f]`, each signed on its own), field by field: a year
    /// is twelve months, a week seven days, the clock units microseconds.
    /// `None` for text it doesn't read.
    pub(super) fn parse(text: &str) -> Option<Interval> {
        match text.trim() {
            "infinity" => return Some(Interval::Infinity),
            "-infinity" => return Some(Interval::NegativeInfinity),
            _ => {}
        }
        let (mut months, mut days, mut micros) = (0i64, 0i64, 0i128);
        let mut tokens = text.split_whitespace();
        while let Some(token) = tokens.next() {
            if token.contains(':') {
                let (negative, clock) = match token.as_bytes()[0] {
                    b'-' => (true, &token[1..]),
                    b'+' => (false, &token[1..]),
                    _ => (false, token),
                };
                let mut parts = clock.split(':');
                let hours: i128 = parts.next()?.parse().ok()?;
                let minutes: i128 = parts.next()?.parse().ok()?;
                let seconds = super::micros_of(parts.next().unwrap_or("0"))?;
                if parts.next().is_some() {
                    return None;
                }
                let span = (hours * 60 + minutes) * 60_000_000 + seconds;
                micros += if negative { -span } else { span };
                continue;
            }
            let unit = tokens.next()?;
            let unit = unit.strip_suffix('s').unwrap_or(unit);
            let value: i64 = token.parse().ok()?;
            match unit {
                "year" => months += value * 12,
                "mon" | "month" => months += value,
                "week" => days += value * 7,
                "day" => days += value,
                "hour" => micros += i128::from(value) * 3_600_000_000,
                "min" | "minute" => micros += i128::from(value) * 60_000_000,
                "sec" | "second" => micros += i128::from(value) * 1_000_000,
                "microsecond" => micros += i128::from(value),
                _ => return None,
            }
        }
        Some(Interval::Finite {
            months,
            days,
            micros,
        })
    }

    /// The text `IntervalStyle = postgres` prints for it (`interval_out`).
    pub(super) fn render(self) -> String {
        let (months, days, micros) = match self {
            Interval::Infinity => return "infinity".to_string(),
            Interval::NegativeInfinity => return "-infinity".to_string(),
            Interval::Finite {
                months,
                days,
                micros,
            } => (months, days, micros),
        };
        let mut parts: Vec<String> = Vec::new();
        let mut before = false;
        for (value, unit) in [(months / 12, "year"), (months % 12, "mon"), (days, "day")] {
            if value != 0 {
                parts.push(format!(
                    "{}{value} {unit}{}",
                    if before && value > 0 { "+" } else { "" },
                    if value != 1 { "s" } else { "" }
                ));
                before = value < 0;
            }
        }
        if micros != 0 || parts.is_empty() {
            let sign = if micros < 0 {
                "-"
            } else if before {
                "+"
            } else {
                ""
            };
            let m = micros.unsigned_abs();
            let (seconds, fraction) = (m / 1_000_000, m % 1_000_000);
            let mut clock = format!(
                "{sign}{:02}:{:02}:{:02}",
                seconds / 3600,
                seconds / 60 % 60,
                seconds % 60
            );
            if fraction != 0 {
                clock.push_str(format!(".{fraction:06}").trim_end_matches('0'));
            }
            parts.push(clock);
        }
        parts.join(" ")
    }
}

/// Whether `months`, `days` and `micros` are an `interval`'s fields: each in
/// its type's range, and on Postgres 17+ not an infinity's.
fn in_range(months: i128, days: i128, micros: i128, pg17: bool) -> bool {
    let int = i128::from(i32::MIN)..=i128::from(i32::MAX);
    let big = i128::from(i64::MIN)..=i128::from(i64::MAX);
    let sentinel = |m: i32, d: i32, u: i64| {
        months == i128::from(m) && days == i128::from(d) && micros == i128::from(u)
    };
    int.contains(&months)
        && int.contains(&days)
        && big.contains(&micros)
        && !(pg17
            && (sentinel(i32::MAX, i32::MAX, i64::MAX) || sentinel(i32::MIN, i32::MIN, i64::MIN)))
}

const OUT_OF_RANGE: &str = "interval out of range";

/// An `interval` field's value.
fn interval_result(values: &[Interval], func: Func, pg17: bool) -> Result<Interval, &'static str> {
    let (mut up, mut down) = (false, false);
    let (mut months, mut days, mut micros) = (0i128, 0i128, 0i128);
    for value in values {
        match *value {
            Interval::Infinity => up = true,
            Interval::NegativeInfinity => down = true,
            Interval::Finite {
                months: m,
                days: d,
                micros: u,
            } => {
                months += i128::from(m);
                days += i128::from(d);
                micros += u;
            }
        }
    }
    match (up, down) {
        (true, true) => return Err(OUT_OF_RANGE),
        (true, false) => return Ok(Interval::Infinity),
        (false, true) => return Ok(Interval::NegativeInfinity),
        (false, false) => {}
    }
    let finite = |m: i128, d: i128, u: i128| Interval::Finite {
        months: m as i64,
        days: d as i64,
        micros: u,
    };
    match func {
        Func::Sum if in_range(months, days, micros, pg17) => Ok(finite(months, days, micros)),
        Func::Sum => Err(OUT_OF_RANGE),
        Func::Avg => {
            let (m, d, u) = interval_div(months, days, micros, values.len() as f64)?;
            if in_range(m, d, u, pg17) {
                Ok(finite(m, d, u))
            } else {
                Err(OUT_OF_RANGE)
            }
        }
    }
}

/// Postgres's `interval_div(span, factor)` (`timestamp.c`) over the exact
/// fields of `span`, in `double precision` step by step as it computes: the
/// months' fraction carries into days, the days' into seconds (each rounded
/// to microseconds), whole days of the seconds back into the days.
fn interval_div(
    months: i128,
    days: i128,
    micros: i128,
    factor: f64,
) -> Result<(i128, i128, i128), &'static str> {
    let tsround = |x: f64| (x * 1e6).round_ties_even() / 1e6;
    let (m, d, u) = (months as f64, days as f64, micros as f64);
    let result_months = (m / factor).trunc();
    let result_days = (d / factor).trunc();
    let month_remainder_days = tsround((m / factor - result_months) * 30.0);
    let mut sec_remainder = tsround(
        (d / factor - result_days + month_remainder_days - month_remainder_days.trunc()) * 86400.0,
    );
    let mut days_out = result_days;
    if sec_remainder.abs() >= 86400.0 {
        let carry = (sec_remainder / 86400.0).trunc();
        days_out += carry;
        sec_remainder -= carry * 86400.0;
    }
    days_out += month_remainder_days.trunc();
    let time = (u / factor + sec_remainder * 1e6).round_ties_even();
    // `(int64) rint(...)` past `bigint`'s range raises.
    let limit = 2f64.powi(63);
    if !(-limit..limit).contains(&time) {
        return Err(OUT_OF_RANGE);
    }
    Ok((result_months as i128, days_out as i128, time as i128))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum(values: &[f64]) -> f64 {
        float_result(values, Kind::Double, Func::Sum).unwrap()
    }

    fn avg(values: &[f64]) -> f64 {
        float_result(values, Kind::Double, Func::Avg).unwrap()
    }

    /// The order-dependence shapes read the exact sum: `{1e20, 1, -1e20}`
    /// is 1 in every order, ten `0.1`s widened from `real` sum to `real`'s
    /// nearest `1`, `0.1 + 0.2` is the double nearest the exact sum.
    #[test]
    fn floats_sum_exactly_and_round_once() {
        assert_eq!(sum(&[1e20, 1.0, -1e20]), 1.0);
        assert_eq!(sum(&[1e20, -1e20, 1.0]), 1.0);
        let tenth = f64::from(0.1f32);
        assert_eq!(
            float_result(&[tenth; 10], Kind::Real, Func::Sum).unwrap(),
            1.0
        );
        // 0.1 + 0.2 is exactly 0.3000000000000000166533453693773481063544750213623046875,
        // whose nearest double is 0.30000000000000004 (IEEE addition agrees here).
        assert_eq!(sum(&[0.1, 0.2]), 0.1 + 0.2);
        assert_eq!(sum(&[]), 0.0);
        assert_eq!(sum(&[5e-324, 5e-324]), 1e-323);
        assert_eq!(sum(&[f64::MAX, -f64::MAX, 1.0]), 1.0);
        assert_eq!(
            float_result(&[f64::MAX, f64::MAX], Kind::Double, Func::Sum),
            Err("value out of range: overflow")
        );
        assert_eq!(
            float_result(
                &[f64::from(f32::MAX), f64::from(f32::MAX)],
                Kind::Real,
                Func::Sum
            ),
            Err("value out of range: overflow")
        );
        // The specials resolve as Postgres resolves them.
        assert!(sum(&[1.0, f64::NAN]).is_nan());
        assert!(sum(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
        assert_eq!(sum(&[f64::INFINITY, 1.0]), f64::INFINITY);
        assert_eq!(sum(&[f64::NEG_INFINITY, -0.0]), f64::NEG_INFINITY);
    }

    /// Every sum of two random doubles rounds as IEEE addition (itself
    /// correctly rounded) does, and every sum of a small group of exactly
    /// representable values equals its plain sum.
    #[test]
    fn two_value_sums_agree_with_ieee_addition() {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..20_000 {
            let (a, b) = (f64::from_bits(next()), f64::from_bits(next()));
            if !a.is_finite() || !b.is_finite() || (a + b).is_infinite() {
                continue;
            }
            let got = sum(&[a, b]);
            assert!(
                got == a + b,
                "{a:e} + {b:e}: got {got:e}, IEEE says {:e}",
                a + b
            );
            // A real pair: their exact sum rounded to `real`.
            let (x, y) = (a as f32, b as f32);
            if x.is_finite() && y.is_finite() && (x + y).is_finite() {
                let got = float_result(&[f64::from(x), f64::from(y)], Kind::Real, Func::Sum);
                assert_eq!(got, Ok(f64::from(x + y)), "{x:e} + {y:e} as real");
            }
            // A quotient by 2 and by 1 is exact.
            assert_eq!(avg(&[a]), if a.abs() <= 5e-324 / 2.0 { 0.0 } else { a });
        }
    }

    /// `AVG` rounds the exact quotient once: `{1, 2}` over 2, a third, and a
    /// quotient of at most half the smallest subnormal reads `0`.
    #[test]
    fn float_averages_round_the_exact_quotient() {
        assert_eq!(avg(&[1.0, 2.0]), 1.5);
        assert_eq!(avg(&[1.0, 0.0, 0.0]), 1.0 / 3.0);
        assert_eq!(avg(&[1e20, 1.0, -1e20]), 1.0 / 3.0);
        assert_eq!(avg(&[5e-324, 0.0]), 0.0);
        assert_eq!(avg(&[5e-324, 5e-324, 5e-324, 0.0]), 5e-324);
        assert_eq!(avg(&[-5e-324, -5e-324, -5e-324, 0.0]), -5e-324);
        assert_eq!(avg(&[f64::MAX, f64::MAX]), f64::MAX);
    }

    fn iv(months: i64, days: i64, micros: i128) -> Interval {
        Interval::Finite {
            months,
            days,
            micros,
        }
    }

    /// `interval_out`'s spellings read back to their fields, and the
    /// renderer prints Postgres's.
    #[test]
    fn intervals_read_and_print_field_by_field() {
        for (text, value) in [
            ("00:00:00", iv(0, 0, 0)),
            ("1 day", iv(0, 1, 0)),
            ("-1 days +23:00:00", iv(0, -1, 82_800_000_000)),
            (
                "1 year 2 mons 3 days 04:05:06.789",
                iv(14, 3, 14_706_789_000),
            ),
            (
                "-1 years -2 mons +3 days -04:05:06",
                iv(-14, 3, -14_706_000_000),
            ),
            ("1 mon -1 days", iv(1, -1, 0)),
            ("-00:00:00.000001", iv(0, 0, -1)),
            (
                "178956970 years 7 mons 2147483647 days 2562047788:00:54.775807",
                iv(
                    i64::from(i32::MAX),
                    i64::from(i32::MAX),
                    i128::from(i64::MAX),
                ),
            ),
            (
                "-178956970 years -8 mons -2147483648 days -2562047788:00:54.775808",
                iv(
                    i64::from(i32::MIN),
                    i64::from(i32::MIN),
                    i128::from(i64::MIN),
                ),
            ),
            ("infinity", Interval::Infinity),
            ("-infinity", Interval::NegativeInfinity),
        ] {
            assert_eq!(Interval::parse(text), Some(value), "{text}");
            assert_eq!(value.render(), text);
        }
    }

    fn isum(values: &[Interval], pg17: bool) -> Result<Interval, &'static str> {
        interval_result(values, Func::Sum, pg17)
    }

    /// The `SUM` is exact whatever the order and raises only on its final
    /// value; both infinities raise.
    #[test]
    fn interval_sums_raise_only_on_the_final_value() {
        let max = iv(0, i64::from(i32::MAX), 0);
        assert_eq!(
            isum(&[max, iv(0, 1, 0), iv(0, -1, 0)], false),
            Ok(max),
            "an order whose partial sum overflows"
        );
        assert_eq!(isum(&[max, iv(0, 1, 0)], false), Err(OUT_OF_RANGE));
        assert_eq!(
            isum(&[iv(0, 0, i128::from(i64::MIN)), iv(0, 0, -1)], false),
            Err(OUT_OF_RANGE)
        );
        let sentinel = iv(
            i64::from(i32::MAX),
            i64::from(i32::MAX),
            i128::from(i64::MAX),
        );
        let parts = [
            iv(
                i64::from(i32::MAX),
                i64::from(i32::MAX),
                i128::from(i64::MAX) - 1,
            ),
            iv(0, 0, 1),
        ];
        assert_eq!(isum(&parts, false), Ok(sentinel));
        assert_eq!(isum(&parts, true), Err(OUT_OF_RANGE));
        assert_eq!(
            isum(&[Interval::Infinity, iv(1, 0, 0)], true),
            Ok(Interval::Infinity)
        );
        assert_eq!(
            isum(&[Interval::Infinity, Interval::NegativeInfinity], true),
            Err(OUT_OF_RANGE)
        );
    }

    /// The `AVG` cascades fractions as `interval_div` does, including over a
    /// sum out of range.
    #[test]
    fn interval_averages_cascade_like_interval_div() {
        let avg = |values: &[Interval]| interval_result(values, Func::Avg, true);
        // One month over two is 15 days; a day over two 12 hours.
        assert_eq!(avg(&[iv(1, 0, 0), iv(0, 0, 0)]), Ok(iv(0, 15, 0)));
        assert_eq!(
            avg(&[iv(0, 1, 0), iv(0, 0, 0)]),
            Ok(iv(0, 0, 43_200_000_000))
        );
        // A month over three: 10 days.
        assert_eq!(
            avg(&[iv(1, 0, 0), iv(0, 0, 0), iv(0, 0, 0)]),
            Ok(iv(0, 10, 0))
        );
        // Two days of the maximum average to it.
        let max = iv(0, i64::from(i32::MAX), 0);
        assert_eq!(avg(&[max, max]), Ok(max));
    }
}
