//! The join-key type allowlist: which Postgres types the engine can match
//! as keys by their `::text` rendering. A type is admitted by its identity,
//! its OID (ADR-0020), never by the text that names it: a domain or other
//! user type named like a listed type (`create domain "integer" as numeric`)
//! is another type. A `JOIN`'s `ON` columns
//! ([`super::validate::validate_join`]), a 1-1 source's primary key
//! ([`super::ddl::source_primary_key`]) and the copied key types
//! ([`super::key_types`]) all gate on it.

use super::floor::{BASELINE, Release};

/// One type [`TEXT_STABLE_JOIN_KEY_TYPES`] admits as a join key, and the
/// release it arrived in. The release is a required part of every entry, so a
/// type can't join the list without declaring it (ADR-0024, [`super::floor`]).
#[derive(Debug, Clone, Copy)]
struct KeyTypeSpec {
    /// The type's name as `format_type` renders it, modifier stripped: the
    /// name a definition's floor records ([`JoinKey::Listed`]) and messages
    /// show.
    name: &'static str,
    /// The `pg_catalog` type's OID, by which a column's type is admitted.
    oid: u32,
    /// The release this type arrived in as a join key, which raises the floor
    /// of a definition keyed on it.
    since: Release,
}

const fn key_type(name: &'static str, oid: u32, since: Release) -> KeyTypeSpec {
    KeyTypeSpec { name, oid, since }
}

/// A way a definition's `JOIN ... ON` key is admitted, by the entry that
/// declares it. Recorded for the definition's floor where the validator
/// admits the key ([`admit_join_key`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKey {
    /// A type on the allowlist, by its name.
    Listed(&'static str),
    /// An enum type, which no fixed list can name: the catalog says a type
    /// is one (`pg_type.typtype = 'e'`).
    ///
    /// Admitting it is safe on the same "text-stability is a property of the
    /// rendering" grounds every listed type is admitted under: `enumout`
    /// (what `<col>::text` calls — verified live against `pg_cast`,
    /// `trellis/tests/defs_enum.rs`) renders each value as its own label text
    /// verbatim, with no separator, no escaping, and no GUC dependence. An
    /// enum value's identity *is* its label text (a `pg_enum` row keyed by
    /// OID, `enumlabel` the payload), so there is no deeper representation
    /// for two distinct values to collide under: a bijection by
    /// construction, not merely by the absence of a discovered
    /// counterexample.
    Enum,
}

impl JoinKey {
    /// The release this key type arrived in as a join key: its allowlist
    /// entry's. `None` for a name no entry declares.
    pub fn since(&self) -> Option<Release> {
        match *self {
            JoinKey::Listed(name) => TEXT_STABLE_JOIN_KEY_TYPES
                .iter()
                .find(|spec| spec.name == name)
                .map(|spec| spec.since),
            JoinKey::Enum => Some(BASELINE),
        }
    }
}

/// Postgres types whose equality is *text-stable* — `a::text = b::text`
/// agrees with the type's native typed `=` for every value. This is a
/// positive allowlist, and a per-type fact: `character`'s native `=` is
/// blank-padding-insensitive while its `::text` rendering is blank-padded, so
/// it is off the list even though `character varying` and `text` are on it.
///
/// Only the join key's *own* type matters for this list, not what it's
/// compared against, so allowed/rejected status is a per-type fact. A type
/// modifier never changes it, and isn't part of a type's identity: a
/// precision rounds on *input* (`'12:00:00.5678'::time(2)` stores
/// `12:00:00.57`), and what is stored still renders canonically.
/// Anything not named here — `numeric` (`1.0::text` != `1.00::text` though
/// numerically equal), `real`/`double precision` (`-0` and `0`
/// are `=` in Postgres but render as `'-0'` and `'0'`, so text matching
/// splits one value across two keys; verified on a live server, and the
/// unlock would be a typed key index comparing decoded values, not a new
/// encoding), `character`/`citext`
/// (blank-padding or
/// case-insensitivity native to the type but not its `::text` form),
/// `interval` (`timestamp` is admitted alongside `date`/`time`/
/// `timetz` because every renderer agrees on it, and
/// `timestamptz` because `TimeZone` is pinned on every
/// renderer; see the temporal block below for why `interval` alone stays
/// off), `boolean`, `json`/`jsonb`, or any unknown type — is rejected as a
/// join key.
///
/// `bytea` joins the list below on the same "text-stability is
/// a property of the rendering" reasoning `oid` and four of the six temporal
/// families were admitted under — `docs/type-support.md` had it marked
/// `🎯 typed index`, and that turned out to be exactly the wrong prediction
/// again. `byteaout` under the already-pinned `bytea_output = 'hex'`
/// (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection: every
/// distinct byte string has exactly one canonical `\x`-prefixed, lowercase,
/// even-length hex spelling, and every such spelling names exactly one byte
/// string — there is no `bytea` analogue of float's `-0`/`0` or interval's
/// `'1 day'`/`'24 hours'`, because a fixed-width positional encoding cannot
/// produce two spellings of the same value. Verified on a live server: a
/// grid spanning the empty value, embedded `NUL` bytes, and every byte value
/// from `0x00` to `0xff` produces exactly as many distinct `::text` groups as
/// distinct values (`defs_bytea.rs`'s
/// `bytea_text_rendering_is_a_bijection_under_hex_output`). The type
/// matrix's scope note ("key role needs decoded comparison") assumed a typed
/// key index was the prerequisite; asked of a live server, it is
/// not — pinning `bytea_output` alone already makes raw `::text` matching
/// agree with `bytea`'s native `=` for every value.
const TEXT_STABLE_JOIN_KEY_TYPES: &[KeyTypeSpec] = &[
    key_type("smallint", 21, BASELINE),
    key_type("integer", 23, BASELINE),
    key_type("bigint", 20, BASELINE),
    // `oid` is an *unsigned* 32-bit integer whose `oid_out`
    // rendering is canonical decimal — no sign, no leading zeros, no
    // padding — so `a::text = b::text` agrees with `oid`'s native `=` for
    // every value, exactly as it does for the three signed widths above.
    // It is not a `ValueType::Integer` (Postgres gives it no arithmetic at
    // all; see `pg_type::PgType::Oid`), but text-stability is a property of
    // the *rendering*, not of the operator set, so it belongs here.
    key_type("oid", 26, BASELINE),
    key_type("uuid", 2950, BASELINE),
    key_type("text", 25, BASELINE),
    key_type("character varying", 1043, BASELINE),
    // Four of the six temporal families, on the same
    // "text-stability is a property of the rendering" reasoning `oid` was
    // admitted under. `docs/type-support.md` had all six marked `🎯 typed
    // index`, assuming a typed key index was the prerequisite; for
    // these four it is not, and the evidence is per-family rather than
    // per-block — see `crate::temporal`'s module doc for the live queries.
    //
    // `date_out` under the already-pinned `DateStyle = 'ISO, YMD'`
    // (`crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS`) is a bijection on its
    // values: the year field widens (`5874897-12-31`), the era is an
    // explicit ` BC` suffix, and `infinity`/`-infinity` have their own
    // spellings. `time_out` and `timetz_out` need no GUC at all — verified
    // identical under `ISO`, `SQL`, `Postgres` and `German` `DateStyle`s
    // and under three session `TimeZone`s.
    //
    // `timetz` is the one to double-take on, and it is safe for a reason
    // opposite to the obvious one: its `=` is *narrower* than "same instant
    // of day", not wider. `select '12:00:00+00'::timetz =
    // '17:30:00+05:30'::timetz` is **false** — `timetz_cmp_internal` sorts
    // by GMT-equivalent time and then by zone, so equality is identity on
    // the stored `(time, zone)` pair, which is exactly what `timetz_out`
    // prints.
    //
    // `timestamp without time zone` is a bijection under `::text` too, and
    // it would be refused if `::text` were not the engine's only renderer.
    // A bare `to_jsonb(t.*)` row body renders a `timestamp` through
    // `jsonb`'s ISO-8601 writer: `2024-06-15T12:34:56`, not
    // `2024-06-15 12:34:56`, and a key seeded once each way would become two
    // target rows for one Postgres group. The live-row reads therefore build
    // an explicit per-column `jsonb_build_object` rendered the same way
    // `::text` is (`staging::apply::row_as_text_jsonb_sql`), which is why
    // `timestamp` joins `date`/`time`/`timetz` here — see
    // `crate::temporal::is_render_consistent`.
    //
    // One remaining deliberate absence:
    //
    // * `interval` — `'24 hours'` and `'1 day'` are one value with two
    //   renderings. The float `-0`/`0` defect; no GUC and no renderer
    //   reconciliation fixes it, though a typed key index would.
    key_type("date", 1082, BASELINE),
    key_type("time without time zone", 1083, BASELINE),
    key_type("time with time zone", 1266, BASELINE),
    key_type("timestamp without time zone", 1114, BASELINE),
    // `timestamp with time zone` joins the four above.
    // `timestamptz_out` renders the stored instant in the session's `TimeZone`,
    // so it needs two independent properties to be text-stable — the
    // `to_jsonb`/`::text` reconciliation (which it shares with
    // `timestamp`) and, separately, `TimeZone` itself being pinned identically
    // everywhere Trellis renders a value. The pool pins
    // `crate::pool::DETERMINISTIC_TEXT_OUTPUT_GUCS` (which includes `TimeZone =
    // 'UTC'`) on every connection, and each capture function pins the same
    // settings as `SET` clauses (`crate::capture::sql`), so `timestamptz_out`
    // is a bijection on the instant in both. See
    // `crate::temporal::is_bijective_under_text`/ `is_render_consistent` for
    // the per-family verdict this list mirrors.
    key_type("timestamp with time zone", 1184, BASELINE),
    // `byteaout` under the pinned `bytea_output = 'hex'` is a
    // bijection on its values — see the doc comment above for the live
    // evidence. `bytea` has no length/precision modifier, so
    // there is nothing for one to change.
    key_type("bytea", 17, BASELINE),
    // `cidr`/`macaddr`/`macaddr8` join the list; `inet` — despite
    // sharing `cidr`'s `pg_cast` row — deliberately does not. Full account in
    // `crate::netaddr`'s module doc, condensed here:
    //
    // `macaddr`/`macaddr8` have no `pg_cast` row for `text` at all (checked
    // the same way `boolean`'s was checked), so their `::text` *is*
    // `macaddr_out`/`macaddr8_out`, and both are bijections — every accepted
    // input spelling (colon/hyphen/dot-grouped/bare hex) normalizes on
    // output to one canonical lowercase colon-separated form.
    //
    // `inet` and `cidr` share one `pg_cast` row (`pg_catalog.text(inet)`,
    // `prosrc = network_show`, reused for `cidr` because its on-disk
    // representation *is* an `inet` with host bits forced to zero) — but
    // whether that second renderer actually diverges from the type's own
    // output function is a per-type fact, not a per-row one. `cidr_out`
    // never omits the netmask (a `cidr` value's whole point is that the
    // network prefix matters), so `cidr_out(v)::text = v::text` holds for
    // every value — verified live across a v4/v6 grid including the
    // host-bits-zero-only values `cidr_in` accepts. `inet_out` — what a
    // capture trigger images, and what it builds a staged `key` from —
    // *does* diverge from `network_show`: it omits the `/prefixlen` suffix
    // exactly when the stored netmask covers the whole address
    // (`'192.168.1.5'::inet::text` via `inet_out` is `192.168.1.5`; via the
    // cast, `192.168.1.5/32`), the identical "one value, two renderers, no
    // arbiter" shape `boolean` has above — so a raw `{col}::text = $1`
    // match would silently mismatch `inet` exactly the way `boolean`
    // would. `inet` is refused here for that reason;
    // its `GROUP BY` key role is still admitted, the same split `boolean`
    // got — see `validate::reject_unsupported_group_by_key_type`'s `Inet`
    // arm.
    key_type("cidr", 650, BASELINE),
    key_type("macaddr", 829, BASELINE),
    key_type("macaddr8", 774, BASELINE),
    // `boolean` is deliberately *not* here, and the reason is
    // new — every other admission/refusal on this list turns on whether
    // `<type>_out` (the type's own output function, what capture images and
    // what `<col>::text` normally reduces to) is a bijection.
    // `boolout` *is* one (`'t'`/`'f'`, nothing else). The problem is that
    // `<col>::text` does not call `boolout` at all: `select castfunc::regproc
    // from pg_cast where castsource = 'boolean'::regtype and casttarget =
    // 'text'::regtype` names `pg_catalog.text(boolean)`, a *second*,
    // dedicated cast function Postgres ships only for `boolean`, which
    // renders the SQL-standard `'true'`/`'false'` instead of `boolout`'s
    // `'t'`/`'f'`. Checked against every other type on this list
    // (`smallint`/`integer`/`bigint`/`oid`/`uuid`/`text`/`character
    // varying`/the four temporal families/`bytea`) and none has a
    // `pg_cast` row for `text` at all — their `::text` *is* their output
    // function, which is exactly why raw-text matching has been sound for
    // all of them. `boolean` is the only type on the block with a second,
    // independent renderer, and it is silent: nothing here fails to type
    // or parse, two spellings of one value just stop comparing equal.
    //
    // That is live-load-bearing, not theoretical: a capture trigger images a
    // column with `format('%s', col)`, which calls `boolout`, and builds a
    // primary key's staged `key` text from that — `'t'`/`'f'`. A *bulk* key
    // lookup that casts the *bound parameter* to the column's native type
    // (`col = any($1::text[]::boolean[])`) calls `boolin` — an
    // input function far more permissive than `boolout` is strict, so it
    // parses `'t'`/`'f'` and `'true'`/`'false'` alike back to the same
    // native value and the divergence never surfaces there. But a
    // *scalar* single-key lookup in the older, unindexed
    // `{key}::text = $1` form would compare a live column's
    // `pg_catalog.text(boolean)` rendering (`'true'`/`'false'`) against a
    // CDC-derived `key` built from `boolout` (`'t'`/`'f'`) — and those two
    // strings are simply not equal. Admitting `boolean` here today would
    // silently mismatch exactly the shape `timestamp` would have under
    // `to_jsonb` (one value, two renderers, no arbiter), just from a
    // different renderer pair, since `boolean` is the only type with this
    // second cast.
    //
    // The `GROUP BY` key role does *not* have this problem at the final SQL
    // layer — see `validate::reject_unsupported_group_by_key_type`'s own
    // `Boolean` arm — because the group basis (`staging::group_basis`) parses
    // a group key's text into its native type, never a bare `col::text` comparison,
    // so it never touches `pg_catalog.text (boolean)` at all. Extending the SQL-layer
    // native-array-cast pattern to the remaining scalar lookup sites (or
    // a typed key index, which would subsume it) is what would let
    // `boolean` join this list; until then it stays off. See
    // `trellis/tests/defs_boolean.rs` for the live evidence and
    // `docs/type-support.md`'s "Boolean semantics" section.
    //
    // `bit` and `bit varying` join this list on the *original*
    // "text-stability is a property of the rendering" reasoning `oid`/
    // `bytea`/four of the six temporal families were admitted under —
    // checked against `pg_cast`, not assumed, as every family on this list
    // must be. `select
    // castfunc::regproc from pg_cast where castsource in
    // ('bit'::regtype, 'varbit'::regtype) and casttarget = 'text'::regtype`
    // returns **no rows at all** on a live Postgres 17: unlike `boolean`,
    // neither bit-string type has a second, dedicated `::text` cast — their
    // `::text` *is* `bit_out`/`varbit_out`, exactly like every other type on
    // this list except `boolean`. `bit_out`/`varbit_out` are `IMMUTABLE`,
    // read no GUC, and render a bit string as its literal `'0'`/`'1'`
    // characters with no separator, no padding beyond the value's own stored
    // bits, and no second spelling of any value — a fixed-alphabet,
    // fixed-width-per-symbol encoding has no `bytea`-hex-style ambiguity to
    // begin with, so it needs no positional-encoding argument the way
    // `bytea`'s bijection proof did either.
    //
    // Both spellings are admitted *here* — this role never asks a bare
    // `ValueType` to declare a brand-new column, it only ever compares an
    // already-existing one (`{col} = any($1::text[]::{ty}[])`, via
    // `staging::apply`'s `key_column_pg_type`, which introspects the
    // column's own concrete `format_type` — `bit(5)`, not bare `bit` — so a
    // fixed-length key's own width is never lost) or, for a 1-1 primary key,
    // copies the source's concrete introspected type verbatim
    // (`ddl::source_primary_key`). Fixed-length `bit` is *not* admitted as a
    // `GROUP BY` key for a completely different, DDL-only reason — see
    // `validate::reject_unsupported_group_by_key_type`'s `VarBit` arm.
    key_type("bit", 1560, BASELINE),
    key_type("bit varying", 1562, BASELINE),
    // `jsonb` is deliberately **absent**, and — unlike
    // `boolean`/`inet` — not because of a second, disagreeing `::text`
    // renderer: `select castfunc from pg_cast where castsource =
    // 'jsonb'::regtype and casttarget = 'text'::regtype` returns no rows,
    // so `::text` is `jsonb_out` directly, and `jsonb_out` genuinely
    // canonicalizes object key order (`{"b":1,"a":2}` and `{"a":2,"b":1}`
    // both render `{"a": 2, "b": 1}`). The hazard is one level deeper: an
    // embedded JSON *number* preserves its literal input scale the way
    // `numeric` itself does, so `'{"a":1}'::jsonb` and
    // `'{"a":1.0}'::jsonb` are `=` (verified live) but render as
    // `{"a": 1}` and `{"a": 1.0}` — one value, two renderings, the same
    // equivalence-class shape `real`/`double precision`'s `-0`/`0` and
    // `interval`'s `'1 day'`/`'24 hours'` have. See `crate::jsonb`'s
    // module doc for the full live evidence and `trellis/tests/
    // defs_jsonb.rs`. The unlock is a typed key index, exactly as it
    // is for `numeric`/`real`/`double precision`/`timestamptz` above —
    // `jsonb` needs no jsonb-specific mechanism, just the same one every
    // other numeric-bearing family here is waiting on.
];

/// [`TEXT_STABLE_JOIN_KEY_TYPES`] rendered for a user-facing error message,
/// so the "supported join key types are ..." list in
/// [`super::validate::ValidationError::JoinUnsupportedKeyType`]'s `Display`
/// cannot drift out of step with the allowlist it describes.
///
/// A hand-maintained second copy of the list would drift whenever a family
/// is admitted.
pub(crate) fn supported_join_key_types() -> String {
    let names: Vec<&str> = TEXT_STABLE_JOIN_KEY_TYPES
        .iter()
        .map(|spec| spec.name)
        .collect();
    names.join(", ")
}

/// Every entry of [`TEXT_STABLE_JOIN_KEY_TYPES`], as the key it admits: what
/// the feature-floor manifest (`super::floor_manifest`) lists.
#[cfg(test)]
pub(super) fn listed_join_keys() -> Vec<JoinKey> {
    TEXT_STABLE_JOIN_KEY_TYPES
        .iter()
        .map(|spec| JoinKey::Listed(spec.name))
        .collect()
}

/// Whether the type whose OID is `type_oid` is in
/// [`TEXT_STABLE_JOIN_KEY_TYPES`]: [`key_entry`]'s rule for a type that isn't
/// an enum. A pure question: it records nothing for a floor.
pub(crate) fn is_text_stable_join_key_type(type_oid: u32) -> bool {
    listed_join_key(type_oid).is_some()
}

/// The allowlist entry for the type whose OID is `type_oid`, as the key it
/// admits.
fn listed_join_key(type_oid: u32) -> Option<JoinKey> {
    TEXT_STABLE_JOIN_KEY_TYPES
        .iter()
        .find(|spec| spec.oid == type_oid)
        .map(|spec| JoinKey::Listed(spec.name))
}

/// The entry that admits a key of the type whose OID is `type_oid`
/// (`is_enum`: the catalog says it is an enum, [`JoinKey::Enum`]), or `None`
/// when the type is refused: the one key-type rule for a `JOIN ... ON` key
/// ([`admit_join_key`]) and a source's row-identity key
/// ([`super::ddl::admit_key_types`]).
pub(crate) fn key_entry(type_oid: u32, is_enum: bool) -> Option<JoinKey> {
    if is_enum {
        // An enum is never a `pg_catalog` type, whatever it is named.
        return Some(JoinKey::Enum);
    }
    listed_join_key(type_oid)
}

/// Whether a `JOIN ... ON` key of the type whose OID is `type_oid`
/// (`is_enum`: an enum type) is admitted, recording the entry that admits it
/// for the floor of the definition being validated on this thread, if any
/// ([`super::floor::record`]). The validator's only way to admit a join key.
pub(crate) fn admit_join_key(type_oid: u32, is_enum: bool) -> bool {
    let Some(key) = key_entry(type_oid, is_enum) else {
        return false;
    };
    super::floor::record(super::floor::Feature::JoinKey(key));
    true
}

/// An OID no `pg_catalog` type has: the first a user type can get
/// (`FirstNormalObjectId`).
#[cfg(test)]
pub(crate) const USER_TYPE_OID: u32 = 16_384;

/// The `pg_catalog` type `format_type` renders as `rendering`, modifier
/// included; `None` for text it renders no such type as (an internal name
/// like `int4` or `timestamptz`, an extension's type, an enum), which a key
/// probe tries as a user type ([`USER_TYPE_OID`]). Tests only: the gates
/// themselves never read a type from its text.
#[cfg(test)]
pub(crate) fn rendered_type(rendering: &str) -> Option<tokio_postgres::types::Type> {
    use tokio_postgres::types::Type;
    Some(match rendering {
        "smallint" => Type::INT2,
        "integer" => Type::INT4,
        "bigint" => Type::INT8,
        "integer[]" => Type::INT4_ARRAY,
        "oid" => Type::OID,
        "uuid" => Type::UUID,
        "text" => Type::TEXT,
        "text[]" => Type::TEXT_ARRAY,
        "character varying" | "character varying(20)" | "character varying(255)" => Type::VARCHAR,
        "character" | "character(3)" => Type::BPCHAR,
        "\"char\"" => Type::CHAR,
        "name" => Type::NAME,
        "numeric" | "numeric(10,2)" => Type::NUMERIC,
        "real" => Type::FLOAT4,
        "double precision" => Type::FLOAT8,
        "boolean" => Type::BOOL,
        "date" => Type::DATE,
        "time without time zone" | "time(3) without time zone" => Type::TIME,
        "time with time zone" => Type::TIMETZ,
        "timestamp without time zone" | "timestamp(3) without time zone" => Type::TIMESTAMP,
        "timestamp with time zone" | "timestamp(3) with time zone" => Type::TIMESTAMPTZ,
        "interval" => Type::INTERVAL,
        "bytea" => Type::BYTEA,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        "inet" => Type::INET,
        "cidr" => Type::CIDR,
        "macaddr" => Type::MACADDR,
        "macaddr8" => Type::MACADDR8,
        "bit" | "bit(8)" | "\"bit\"" => Type::BIT,
        "bit varying" | "bit varying(8)" => Type::VARBIT,
        "money" => Type::MONEY,
        "xml" => Type::XML,
        "tsvector" => Type::TS_VECTOR,
        "tsquery" => Type::TSQUERY,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use tokio_postgres::types::Type;

    use super::*;
    use crate::defs::floor::{Feature, collect};

    /// Each entry's OID is the `pg_catalog` type its name renders.
    #[test]
    fn each_entry_is_the_type_its_name_renders() {
        let expected = [
            ("smallint", Type::INT2),
            ("integer", Type::INT4),
            ("bigint", Type::INT8),
            ("oid", Type::OID),
            ("uuid", Type::UUID),
            ("text", Type::TEXT),
            ("character varying", Type::VARCHAR),
            ("date", Type::DATE),
            ("time without time zone", Type::TIME),
            ("time with time zone", Type::TIMETZ),
            ("timestamp without time zone", Type::TIMESTAMP),
            ("timestamp with time zone", Type::TIMESTAMPTZ),
            ("bytea", Type::BYTEA),
            ("cidr", Type::CIDR),
            ("macaddr", Type::MACADDR),
            ("macaddr8", Type::MACADDR8),
            ("bit", Type::BIT),
            ("bit varying", Type::VARBIT),
        ];
        let listed: Vec<(&str, u32)> = TEXT_STABLE_JOIN_KEY_TYPES
            .iter()
            .map(|spec| (spec.name, spec.oid))
            .collect();
        let expected: Vec<(&str, u32)> = expected.iter().map(|(n, t)| (*n, t.oid())).collect();
        assert_eq!(listed, expected);
    }

    /// The validator's way to admit a join key records the entry that admits
    /// it, for every type on the allowlist and for an enum, and records
    /// nothing for a refusal. The allowlist itself is private to this module,
    /// so nothing else can admit a key type without its release.
    #[test]
    fn a_join_key_is_admitted_only_through_an_entry_with_a_release() {
        for spec in TEXT_STABLE_JOIN_KEY_TYPES {
            let (admitted, recorded) = collect(|| admit_join_key(spec.oid, false));
            assert!(admitted, "{}", spec.name);
            assert_eq!(recorded, [Feature::JoinKey(JoinKey::Listed(spec.name))]);
            assert_eq!(recorded[0].since(), Some(spec.since));
        }
        let (admitted, recorded) = collect(|| admit_join_key(USER_TYPE_OID, true));
        assert!(admitted);
        assert_eq!(recorded, [Feature::JoinKey(JoinKey::Enum)]);
        assert!(recorded[0].since().is_some());
        for refused in [
            Type::NUMERIC,
            Type::FLOAT4,
            Type::BOOL,
            Type::INET,
            Type::JSONB,
            Type::INTERVAL,
            Type::BPCHAR,
            Type::INT4_ARRAY,
        ] {
            let (admitted, recorded) = collect(|| admit_join_key(refused.oid(), false));
            assert!(!admitted && recorded.is_empty(), "{refused}");
        }
        // A user type is never a listed one, whatever it is named: a domain
        // (`create domain "integer" as numeric`) has an OID of its own.
        let (admitted, recorded) = collect(|| admit_join_key(USER_TYPE_OID, false));
        assert!(!admitted && recorded.is_empty());
    }

    /// The pure question and the recording admission agree on every type.
    #[test]
    fn the_pure_question_agrees_with_the_admission() {
        for ty in [Type::INT4, Type::VARCHAR, Type::NUMERIC, Type::INET] {
            assert_eq!(
                is_text_stable_join_key_type(ty.oid()),
                collect(|| admit_join_key(ty.oid(), false)).0,
                "{ty}"
            );
        }
    }
}
