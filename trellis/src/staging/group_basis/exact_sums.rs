//! Float and `interval` sums ("Float and interval sums" in the module doc of
//! `super`): how a `SUM` or `AVG` over `real`, `double precision` or
//! `interval` enters the `numeric` partials and is read back out.
//!
//! Each value is converted exactly to one `numeric` ([`SumKind::summand`]),
//! so its partials are a `numeric` argument's ([`super::NUMERIC_PARTS`]),
//! kept by the same statements: the finite values' sum and a map of
//! exceptions under a reference scale no finite summand leaves
//! ([`SumKind::reference_scale`]), so the map counts only the special values.
//!
//! - **Float.** A finite value is `m × 2^k` (decoded from `float8send`; a
//!   `real` widens to `double precision` exactly first), written as
//!   `m × 5^-k × 10^k` for `k < 0`, at scale 1074, the scale of the smallest
//!   subnormal. `NaN` and the infinities are the `numeric`
//!   special values. The `SUM` is the exact sum rounded once to the result
//!   type ([`SumKind::sum`]); the `AVG` is the exact sum over the count,
//!   truncated 1100 digits after the point, which no rounding boundary of a
//!   `double precision` lies between, then rounded once (to 0 at most half
//!   the smallest subnormal, which the cast would refuse as an underflow).
//! - **`interval`.** A finite value's fields (months, days, microseconds)
//!   are packed into one integer, `months × 10^120 + days × 10^60 +
//!   microseconds`. A sum of packed values is the packing of the fieldwise
//!   sums while each field's sum is below `5 × 10^59` in magnitude, which a
//!   `JOIN` cell's sum of products of counts under `2^63` stays: so read
//!   back, the fields are exact ([`decoded`]). An infinite value (Postgres
//!   17+) is the `numeric` infinity of its sign. The `SUM` raises only when
//!   its final value is out of range; the `AVG` divides the exact sums by
//!   Postgres's own `interval_div` cascade, in `double precision` as it does.

use crate::defs::ast::ValueType;
use crate::defs::pg_type::PgType;
use crate::float::FloatWidth;
use crate::integer::IntWidth;

/// The scale every finite float summand has: that of `2^-1074`, the
/// smallest subnormal `double precision`, so its exact decimal value has at
/// most 1074 digits after the point.
pub(crate) const FLOAT_SCALE: i32 = 1074;

/// Half the smallest subnormal `double precision`, `2^-1075`: a quotient no
/// larger rounds to 0 (ties to even), which a cast from `numeric` refuses
/// as an underflow instead.
pub(crate) const HALF_SUBNORMAL: &str = "(power(5::numeric, 1075) * 1e-1075)";

/// The packing base of an `interval` summand's fields (see the module doc).
const BASE: &str = "1e60";

/// How a summed argument's values enter its partials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SumKind {
    /// An integer or `numeric`, summed as itself.
    #[default]
    Exact,
    /// `real` or `double precision`, summed exactly.
    Float(FloatWidth),
    /// `interval`, summed exactly fieldwise.
    Interval,
}

impl SumKind {
    /// The kind of a `SUM` or `AVG` argument of type `value_type`, and the
    /// type its `SUM` casts to: `None` for one no sum admits.
    pub(crate) fn of(value_type: ValueType) -> Option<(SumKind, &'static str)> {
        Some(match value_type {
            ValueType::Integer(IntWidth::Int2 | IntWidth::Int4) => (SumKind::Exact, "bigint"),
            ValueType::Integer(IntWidth::Int8) | ValueType::Numeric => (SumKind::Exact, "numeric"),
            ValueType::Float(FloatWidth::Float4) => (SumKind::Float(FloatWidth::Float4), "real"),
            ValueType::Float(FloatWidth::Float8) => {
                (SumKind::Float(FloatWidth::Float8), "double precision")
            }
            ValueType::Other(PgType::Interval) => (SumKind::Interval, "interval"),
            _ => return None,
        })
    }

    /// Whether its partials are a `numeric` argument's
    /// ([`super::NUMERIC_PARTS`]) when `value_type` is `numeric`.
    pub(crate) fn keeps_map(self, value_type: ValueType) -> bool {
        self != SumKind::Exact || value_type == ValueType::Numeric
    }

    /// The reference scale of its partials whatever its column declares:
    /// every finite summand's scale. `None` for an exact argument, whose
    /// column decides it.
    pub(crate) fn reference_scale(self) -> Option<i32> {
        match self {
            SumKind::Exact => None,
            SumKind::Float(_) => Some(FLOAT_SCALE),
            SumKind::Interval => Some(0),
        }
    }

    /// The exact `numeric` its partials sum for the argument value `x`
    /// (`NULL` for `NULL`): `x` itself for an exact argument.
    pub(crate) fn summand(self, x: &str) -> String {
        match self {
            SumKind::Exact => x.to_string(),
            SumKind::Float(_) => float_summand(x),
            SumKind::Interval => interval_summand(x),
        }
    }

    /// Its `SUM` from `value`, the `numeric` value of its partials
    /// (`super::sum_value`), cast to `cast`.
    pub(crate) fn sum(self, value: &str, cast: &str) -> String {
        match self {
            SumKind::Exact | SumKind::Float(_) => format!("({value})::{cast}"),
            SumKind::Interval => interval_value(value, &interval_sum),
        }
    }

    /// Its `AVG` from `value` (as [`Self::sum`]) and its non-null count
    /// `count`, non-zero.
    pub(crate) fn avg(self, value: &str, count: &str) -> String {
        match self {
            SumKind::Exact => format!("({value}) / ({count})::numeric"),
            SumKind::Float(_) => {
                let quotient = format!("(div(({value}) * 1e1100, ({count})) * 1e-1100)");
                format!(
                    "(case when scale(({value})) is null then ({value})::double precision \
                           when abs({quotient}) <= {HALF_SUBNORMAL} then 0::double precision \
                           else {quotient}::double precision end)"
                )
            }
            SumKind::Interval => interval_value(value, &|m, d, u| interval_avg(m, d, u, count)),
        }
    }
}

/// The exact `numeric` value of the float `x` (see the module doc), at scale
/// [`FLOAT_SCALE`] when finite.
fn float_summand(x: &str) -> String {
    let f = format!("(({x})::double precision)");
    // The bits of `|x|`: an 11-bit biased exponent `e` above a 52-bit
    // fraction. A normal value is `(2^52 + fraction) × 2^(e - 1075)`, a
    // subnormal (`e` 0) `fraction × 2^-1074`.
    let bits = format!("(('x' || encode(float8send(abs{f}), 'hex'))::bit(64)::bigint)");
    let mantissa = format!(
        "(({bits} & 4503599627370495) + \
          (case when {bits} >= 4503599627370496 then 4503599627370496 else 0 end))::numeric"
    );
    let k = format!("(greatest({bits} >> 52, 1) - 1075)");
    format!(
        "(case when {f} = 'NaN'::double precision then 'NaN'::numeric \
               when {f} = 'Infinity'::double precision then 'Infinity'::numeric \
               when {f} = '-Infinity'::double precision then '-Infinity'::numeric \
               else round(sign{f}::numeric * {mantissa} * \
                   (case when {k} >= 0 then power(2::numeric, {k}) \
                         else power(5::numeric, -{k}) * ('1e' || {k})::numeric end), \
                   {FLOAT_SCALE}) end)"
    )
}

/// The packed `numeric` value of the `interval` `x` (see the module doc).
fn interval_summand(x: &str) -> String {
    let x = format!("(({x})::interval)");
    format!(
        "(case when not isfinite{x} \
                    then (case when {x} > interval '0' then 'Infinity' else '-Infinity' end)::numeric \
               else round((extract(year from {x}) * 12 + extract(month from {x})) * 1e120 \
                          + extract(day from {x}) * {BASE} \
                          + extract(hour from {x}) * 3600000000 \
                          + extract(minute from {x}) * 60000000 \
                          + extract(microseconds from {x}), 0) end)"
    )
}

/// The packed fields' balanced remainder by [`BASE`]: the field below them.
fn low_field(packed: &str) -> String {
    format!("(mod(mod({packed}, {BASE}) + 1.5e60, {BASE}) - 5e59)")
}

/// `build(months, days, microseconds)` over the fields of the packed sum
/// `value` ([`decoded`]), for an `interval` `SUM` or `AVG` whose value is
/// finite and counts no infinity; an infinity while it counts one, and
/// Postgres's `interval out of range` while it counts both (`value` is
/// `NaN`). A literal infinity is cast from a `CASE` that reads `value`, so
/// the planner never folds it, which on Postgres 15 and 16 would fail.
fn interval_value(value: &str, build: &dyn Fn(&str, &str, &str) -> String) -> String {
    let infinite =
        |text: &str| format!("(case when ({value}) = 'NaN' then '{text}' end)::interval");
    format!(
        "(case when ({value}) = 'NaN'::numeric then {} + {} \
               when scale(({value})) is null \
                    then (case when ({value}) > 0 then 'infinity' else '-infinity' end)::interval \
               else {} end)",
        infinite("infinity"),
        infinite("-infinity"),
        decoded(value, build)
    )
}

/// `build(months, days, microseconds)` over the fields of the packed sum
/// `value`, each a `numeric` integer, in a scalar subquery.
fn decoded(value: &str, build: &dyn Fn(&str, &str, &str) -> String) -> String {
    format!(
        "(select {} from ( \
             select __q, __u, {} as __d from ( \
                 select div(__p - __u, {BASE}) as __q, __u from ( \
                     select __p, {} as __u from (select ({value})::numeric as __p) as __i0 \
                 ) as __i1) as __i2) as __i3 \
           cross join lateral (select div(__q - __d, {BASE}) as __m) as __i4)",
        build("__m", "__d", "__u"),
        low_field("__q"),
        low_field("__p"),
    )
}

/// `x` clamped to `integer` (`bits` 32) or `bigint` (64).
fn clamped(x: &str, bits: u32) -> String {
    let (min, max) = if bits == 32 {
        (i64::from(i32::MIN), i64::from(i32::MAX))
    } else {
        (i64::MIN, i64::MAX)
    };
    format!("least(greatest({x}, {min}), {max})")
}

/// The `interval` of `micros` microseconds, a `bigint`-range `numeric`
/// integer, exactly: as two products of `1 microsecond` by a
/// `double precision` that holds its factor exactly (`interval * float8`
/// is how Postgres scales one).
fn microseconds(micros: &str) -> String {
    format!(
        "interval '1 microsecond' * (div({micros}, 4503599627370496) * 4503599627370496)::double precision \
         + interval '1 microsecond' * mod({micros}, 4503599627370496)::double precision"
    )
}

/// The `interval` of the exact fields `m`, `d` and `u`, raising Postgres's
/// `interval out of range` when a field is out of its range (or, on
/// Postgres 17+, the fields are an infinity's): each field clamped to its
/// range, then one unit more in the direction it was clamped, which
/// overflows only a clamped field. An in-range field is exact.
fn interval_sum(m: &str, d: &str, u: &str) -> String {
    let (cm, cd, cu) = (clamped(m, 32), clamped(d, 32), clamped(u, 64));
    format!(
        "(make_interval(months => {cm}::int, days => {cd}::int) + {micros} \
          + interval '1 month' * sign({m} - {cm})::double precision \
          + interval '1 day' * sign({d} - {cd})::double precision \
          + interval '1 microsecond' * sign({u} - {cu})::double precision)",
        micros = microseconds(&cu),
    )
}

/// The `AVG` of the exact fields `m`, `d` and `u` over `count` values:
/// Postgres's `interval_div(sum, count)` (`timestamp.c`), its steps in
/// `double precision` as it takes them, so a group whose sum is in range
/// has Postgres's own `avg()`. `TSROUND` is a round to microseconds.
pub(crate) fn interval_avg(m: &str, d: &str, u: &str, count: &str) -> String {
    let tsround =
        |x: &str| format!("(round(({x}) * 1000000::double precision) / 1000000::double precision)");
    let mrd = tsround(&format!("({m}::double precision / __f - __mo) * 30"));
    let sr = tsround(&format!(
        "({d}::double precision / __f - __dd + __mrd - trunc(__mrd)) * 86400"
    ));
    // `sec_remainder`'s whole days, carried into the days when at least one.
    let carry = "(trunc(__sr / 86400) * (abs(__sr) >= 86400)::int)";
    format!(
        "(select make_interval(months => __mo::int, days => (__dd + {carry} + trunc(__mrd))::int) \
                 + interval '1 microsecond' \
                   * round({u}::double precision / __f + (__sr - {carry} * 86400) * 1000000::double precision) \
          from (select *, {mrd} as __mrd from ( \
                    select __f, trunc({m}::double precision / __f) as __mo, \
                           trunc({d}::double precision / __f) as __dd \
                    from (select ({count})::double precision as __f) as __a0) as __a1) as __a2 \
          cross join lateral (select {sr} as __sr) as __a3)"
    )
}

#[cfg(test)]
mod tests {
    //! Against Postgres: the summands are exact and agree with the audit's
    //! own rendering (`super::super::super::self_check_groups`), and the
    //! sums and averages read back as decided (#1119): Postgres's own
    //! `sum()`/`avg()` where those don't depend on their scan order, the
    //! exact result where they do. Run against every Postgres the testkit
    //! is pointed at (`ASDF_POSTGRES_VERSION`).

    use super::*;
    use crate::staging::self_check_groups as audit;

    async fn client() -> (
        testkit::TestCluster,
        testkit::TestDatabase,
        tokio_postgres::Client,
    ) {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = db.connect_raw().await;
        (cluster, db, client)
    }

    /// A deterministic stream of 64-bit values (xorshift64*).
    struct Bits(u64);

    impl Bits {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn signed(&mut self, magnitude: u64) -> i64 {
            let v = self.below(magnitude) as i64;
            if self.next() & 1 == 0 { v } else { -v }
        }
    }

    /// Every bit pattern of a `double precision` (normal, subnormal, both
    /// zeros, the extremes and the specials) converts to a `numeric` that
    /// casts back to it, at scale 1074 when finite, and the audit's own
    /// rendering of the conversion gives the same `numeric`; so does every
    /// `real`, widened.
    #[tokio::test]
    async fn every_float_converts_exactly_and_the_audit_agrees() {
        let (_cluster, _db, client) = client().await;
        let mut bits = Bits(0x9e37_79b9_7f4a_7c15);
        let mut doubles: Vec<f64> = (0..20_000)
            .map(|_| f64::from_bits(bits.next()))
            .filter(|f| !f.is_nan())
            .collect();
        doubles.extend([
            0.0,
            -0.0,
            0.1,
            f64::MIN_POSITIVE,
            f64::from_bits(1),
            f64::from_bits(0x000f_ffff_ffff_ffff),
            f64::MAX,
            f64::MIN,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]);
        let reals: Vec<f32> = (0..20_000)
            .map(|_| f32::from_bits(bits.next() as u32))
            .filter(|f| !f.is_nan())
            .chain([f32::from_bits(1), f32::MAX, f32::NAN, f32::INFINITY])
            .collect();
        for (kind, ty, array) in [
            (SumKind::Float(FloatWidth::Float8), "double precision", "$1"),
            (SumKind::Float(FloatWidth::Float4), "real", "$1"),
        ] {
            let x = "__t.x";
            let sql = format!(
                "select count(*), \
                        count(*) filter (where ({s})::{ty} is distinct from {x}), \
                        count(*) filter (where ({s}) is distinct from ({a})), \
                        count(*) filter (where scale(({s})) is distinct from {FLOAT_SCALE} \
                                           and {x} not in ('NaN', 'Infinity', '-Infinity')) \
                 from unnest({array}::{ty}[]) as __t(x)",
                s = kind.summand(x),
                a = audit::summand(kind, x),
            );
            let row = client
                .query_one(&sql, &[if ty == "real" { &reals } else { &doubles }])
                .await
                .expect("convert");
            let counts: (i64, i64, i64, i64) = (row.get(0), row.get(1), row.get(2), row.get(3));
            assert!(counts.0 > 10_000, "{ty}: {counts:?}");
            assert_eq!(
                (counts.1, counts.2, counts.3),
                (0, 0, 0),
                "{ty}: (total, not round-tripping, audit disagreeing, off scale)"
            );
        }
        let exact: String = client
            .query_one(
                &format!(
                    "select trim_scale({})::text",
                    SumKind::Float(FloatWidth::Float8).summand("0.1::double precision")
                ),
                &[],
            )
            .await
            .expect("0.1")
            .get(0);
        assert_eq!(
            exact,
            "0.1000000000000000055511151231257827021181583404541015625"
        );
    }

    /// The `SUM` and `AVG` of a float kind over `values` (SQL literals), as
    /// the apply reads them and as the audit does, each as text: the exact
    /// sum of the summands, specials summed as `numeric` sums them.
    async fn float_reads(
        client: &tokio_postgres::Client,
        width: FloatWidth,
        values: &[&str],
    ) -> [String; 4] {
        let kind = SumKind::Float(width);
        let ty = width.pg_name();
        let rows: Vec<String> = values.iter().map(|v| format!("('{v}'::{ty})")).collect();
        let sql = format!(
            "select ({sum})::text, ({avg})::text, ({asum})::text, ({aavg})::text \
             from (select sum({s}) as v, count(x) as n \
                   from (values {rows}) as __t(x)) as __g",
            s = kind.summand("x"),
            sum = kind.sum("v", ty),
            avg = kind.avg("v", "n"),
            asum = audit::sum_of(kind, "v", ty),
            aavg = audit::avg_of(kind, "v", "n"),
            rows = rows.join(", "),
        );
        let row = client.query_one(&sql, &[]).await.expect(&sql);
        [row.get(0), row.get(1), row.get(2), row.get(3)]
    }

    /// The read-time rounding: once, to nearest with ties to even, from the
    /// exact sum and the exact quotient, whatever order Postgres's own
    /// `sum()` would have added in.
    #[tokio::test]
    async fn float_sums_and_averages_round_once() {
        let (_cluster, _db, client) = client().await;
        use FloatWidth::{Float4, Float8};
        for (width, values, sum, avg) in [
            // Postgres's `sum()` is 0 or 1 by its scan order.
            (
                Float8,
                &["1e20", "1", "-1e20"][..],
                "1",
                "0.3333333333333333",
            ),
            // The exact sum is a tie between two doubles: the even one.
            (
                Float8,
                &["0.1", "0.2"],
                "0.30000000000000004",
                "0.15000000000000002",
            ),
            // Half the smallest subnormal is a tie with 0: 0.
            (Float8, &["5e-324", "0"], "5e-324", "0"),
            (Float8, &["5e-324", "5e-324", "0"], "1e-323", "5e-324"),
            (
                Float8,
                &["1.7976931348623157e308", "-1.7976931348623157e308", "1"],
                "1",
                "0.3333333333333333",
            ),
            (Float8, &["NaN", "1"], "NaN", "NaN"),
            (Float8, &["Infinity", "-Infinity"], "NaN", "NaN"),
            (Float8, &["-Infinity", "1e308"], "-Infinity", "-Infinity"),
            // Ten `0.1::real` sum to 1.0000000149…, which is `1` as a
            // `real`; Postgres's `sum(real)` adds in `real`, to 1.0000001.
            (
                Float4,
                &[
                    "0.1", "0.1", "0.1", "0.1", "0.1", "0.1", "0.1", "0.1", "0.1", "0.1",
                ],
                "1",
                "0.10000000149011612",
            ),
        ] {
            let read = float_reads(&client, width, values).await;
            assert_eq!(
                read,
                [sum, avg, sum, avg].map(str::to_string),
                "{values:?} (apply sum, apply avg, audit sum, audit avg)"
            );
        }
        // A sum past the type's range raises, as an evaluation error of
        // its group.
        let err = client
            .query_one(
                &format!(
                    "select ({})::text from (select sum({}) as v from (values \
                         (1.7976931348623157e308::double precision), \
                         (1.7976931348623157e308)) as __t(x)) as __g",
                    SumKind::Float(Float8).sum("v", "double precision"),
                    SumKind::Float(Float8).summand("x")
                ),
                &[],
            )
            .await
            .expect_err("out of range");
        let message = err.as_db_error().map(|e| e.message().to_string());
        assert!(
            message.as_ref().is_some_and(|m| m.contains("out of range")),
            "{err:?}"
        );
    }

    /// The `SUM` and `AVG` of `interval`s (SQL literals), as the apply and
    /// the audit read them, each as text, or the error.
    async fn interval_reads(
        client: &tokio_postgres::Client,
        values: &[&str],
    ) -> Result<[String; 4], String> {
        let kind = SumKind::Interval;
        let rows: Vec<String> = values
            .iter()
            .map(|v| format!("('{v}'::interval)"))
            .collect();
        let sql = format!(
            "select ({sum})::text, ({avg})::text, ({asum})::text, ({aavg})::text \
             from (select sum({s}) as v, count(x) as n \
                   from (values {rows}) as __t(x)) as __g",
            s = kind.summand("x"),
            sum = kind.sum("v", "interval"),
            avg = kind.avg("v", "n"),
            asum = audit::sum_of(kind, "v", "interval"),
            aavg = audit::avg_of(kind, "v", "n"),
            rows = rows.join(", "),
        );
        client
            .query_one(&sql, &[])
            .await
            .map(|row| [row.get(0), row.get(1), row.get(2), row.get(3)])
            .map_err(|e| {
                e.as_db_error()
                    .map_or(e.to_string(), |e| e.message().to_string())
            })
    }

    /// An `interval` sum raises only when its final value is out of range,
    /// whichever order its values came in; its average divides the exact
    /// sums even then.
    #[tokio::test]
    async fn interval_sums_raise_only_out_of_range() {
        let (_cluster, _db, client) = client().await;
        let max_days = "2147483647 days";
        for order in [
            [max_days, "1 day", "-1 day"],
            [max_days, "-1 day", "1 day"],
            ["1 day", max_days, "-1 day"],
        ] {
            assert_eq!(
                interval_reads(&client, &order).await,
                Ok([
                    "2147483647 days",
                    "715827882 days 08:00:00.003433",
                    "2147483647 days",
                    "715827882 days 08:00:00.003433",
                ]
                .map(str::to_string)),
                "{order:?}"
            );
        }
        for out in [
            &[max_days, "1 day"][..],
            &["-178956970 years -8 mons", "-1 mon"],
            &["2562047788:00:54.775807", "00:00:00.000001"],
            &["-9223372036854775808 microseconds", "-00:00:00.000001"],
        ] {
            let read = interval_reads(&client, out).await;
            assert!(
                read.as_ref()
                    .is_err_and(|e| e.contains("interval out of range")),
                "{out:?}: {read:?}"
            );
        }
        // Only the average of a sum out of range is in range: it is the
        // fields' quotient (Postgres's own `avg()` raises).
        let avg: String = client
            .query_one(
                &format!(
                    "select ({})::text from (select sum({}) as v, count(x) as n from (values \
                         ('2147483647 days'::interval), ('2147483647 days')) as __t(x)) as __g",
                    SumKind::Interval.avg("v", "n"),
                    SumKind::Interval.summand("x"),
                ),
                &[],
            )
            .await
            .expect("an average in range")
            .get(0);
        assert_eq!(avg, "2147483647 days");
        // A finite sum whose fields are an infinity's (in range on 15 and
        // 16, `interval out of range` on 17+): its average divides the
        // fields, in the apply and the audit alike, where Postgres 17 reads
        // the fields' text as the infinity.
        for (values, avg) in [
            (
                [
                    "178956970 years 7 mons 2147483647 days",
                    "2562047788:00:54.775807",
                ],
                "89478485 years 3 mons 1073741838 days 1281023906:00:27.387904",
            ),
            (
                [
                    "-178956970 years -8 mons -2147483648 days",
                    "-9223372036854775808 microseconds",
                ],
                "-89478485 years -4 mons -1073741824 days -1281023894:00:27.387904",
            ),
        ] {
            let sql = format!(
                "select ({})::text, ({})::text from (select sum({}) as v, count(x) as n \
                 from (values ('{}'::interval), ('{}'::interval)) as __t(x)) as __g",
                SumKind::Interval.avg("v", "n"),
                audit::avg_of(SumKind::Interval, "v", "n"),
                SumKind::Interval.summand("x"),
                values[0],
                values[1],
            );
            let row = client.query_one(&sql, &[]).await.expect(&sql);
            let read: (String, String) = (row.get(0), row.get(1));
            assert_eq!(read, (avg.to_string(), avg.to_string()), "{values:?}");
        }
        // Infinite intervals exist from Postgres 17.
        let version: i32 = client
            .query_one("select current_setting('server_version_num')::int", &[])
            .await
            .expect("version")
            .get(0);
        if version >= 170_000 {
            assert_eq!(
                interval_reads(&client, &["infinity", "1 day"]).await,
                Ok(["infinity"; 4].map(str::to_string))
            );
            assert_eq!(
                interval_reads(&client, &["-infinity", "-infinity"]).await,
                Ok(["-infinity"; 4].map(str::to_string))
            );
            let both = interval_reads(&client, &["infinity", "-infinity", "1 day"]).await;
            assert!(
                both.as_ref()
                    .is_err_and(|e| e.contains("interval out of range")),
                "{both:?}"
            );
        }
    }

    /// Random groups of `interval`s, no sum out of range: the apply's and
    /// the audit's `SUM` and `AVG` are Postgres's `sum()` and `avg()`, field
    /// for field (`interval_send`).
    #[tokio::test]
    async fn interval_sums_and_averages_are_postgres_own() {
        let (_cluster, _db, client) = client().await;
        let mut bits = Bits(0x0123_4567_89ab_cdef);
        let (mut groups, mut months, mut days, mut micros) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for g in 0..400i32 {
            let size = 1 + bits.below(12);
            // Magnitudes from small to near the range over a group.
            let scale = 10u64.pow(bits.below(8) as u32);
            for _ in 0..size {
                groups.push(g);
                months.push(bits.signed(1 + 1_000 * scale / 1_000) as i32);
                days.push(bits.signed(1 + 100 * scale) as i32 % 100_000_000);
                micros.push(bits.signed(1 + 86_400_000_000 * scale.min(10_000_000)));
            }
        }
        let kind = SumKind::Interval;
        let sql = format!(
            "with __t as ( \
                 select g, format('%s mons %s days %s microseconds', m, d, u)::interval as x \
                 from unnest($1::int[], $2::int[], $3::int[], $4::bigint[]) as __u(g, m, d, u)), \
             __g as ( \
                 select g, sum(x) as pg_sum, avg(x) as pg_avg, sum({s}) as v, count(x) as n \
                 from __t group by g) \
             select count(*), \
                    count(*) filter (where interval_send(pg_sum) is distinct from interval_send({sum})), \
                    count(*) filter (where interval_send(pg_avg) is distinct from interval_send({avg})), \
                    count(*) filter (where interval_send(pg_sum) is distinct from interval_send({asum})), \
                    count(*) filter (where interval_send(pg_avg) is distinct from interval_send({aavg})) \
             from __g",
            s = kind.summand("x"),
            sum = kind.sum("v", "interval"),
            avg = kind.avg("v", "n"),
            asum = audit::sum_of(kind, "v", "interval"),
            aavg = audit::avg_of(kind, "v", "n"),
        );
        let row = client
            .query_one(&sql, &[&groups, &months, &days, &micros])
            .await
            .expect("sums");
        let counts: (i64, i64, i64, i64, i64) =
            (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        assert_eq!(
            counts,
            (400, 0, 0, 0, 0),
            "(groups, differing sum, avg, audit sum, avg)"
        );
        // And the summand is the audit's.
        let differing: i64 = client
            .query_one(
                &format!(
                    "select count(*) filter (where ({}) is distinct from ({})) \
                     from (select format('%s mons %s days %s microseconds', m, d, u)::interval as x \
                           from unnest($1::int[], $2::int[], $3::bigint[]) as __u(m, d, u)) as __t",
                    kind.summand("x"),
                    audit::summand(kind, "x"),
                ),
                &[&months, &days, &micros],
            )
            .await
            .expect("summands")
            .get(0);
        assert_eq!(differing, 0);
    }
}
