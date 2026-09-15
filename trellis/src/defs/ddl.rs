//! Target-table DDL for a 1-1 [`TransformDef`] (issue #25).
//!
//! **Neighbor-table convention**: the target table is created under the
//! literal name `def.target`, in a separately-configurable *target schema*
//! (`Config::target_schema`, issue #15) — deliberately decoupled from the
//! Trellis-managed catalog schema (`Config::schema`) everything else
//! `crate::pool` points `search_path` at, and defaulting to `public` (the
//! same default any other bare `CREATE TABLE` would land in) rather than
//! the Trellis instance schema: a POC that defaulted target tables into the
//! Trellis schema found a high chance of future name conflicts as Trellis
//! grows its own catalog/state tables there. The target table never lands
//! back onto the source table either way (`docs/data-flow.md`'s
//! "calculated columns live on a neighbor table" rule: writing them onto the
//! replicated source row would feed our own WAL into ingestion). No extra
//! prefix/suffix is added to `def.target` because `transform_definitions.target_table`
//! is already `unique` (see `V2__transform_catalog.sql`), so collisions
//! across definitions are already ruled out at the catalog layer; deriving
//! a *different* name from it would just be a second name to keep in sync
//! for no benefit. That catalog constraint is global across every target
//! schema today (not scoped per-schema) — more conservative than strictly
//! necessary now that target tables can live in different schemas, but
//! left as-is pending a decision on whether per-schema uniqueness is worth
//! the extra catalog complexity.
//!
//! The target's primary key is inherited from the source table's own primary
//! key (name and type, introspected live from `pg_catalog` — the one piece
//! of schema introspection this issue needs, distinct from the general
//! "introspect the whole source schema" question the catalog module (#23)
//! left to intake). Only a single-column primary key is supported, matching
//! the 1-1 grammar's single-source-row assumption; every calculated field is
//! typed per its inferred [`super::ast::ValueType`] (issue #63 widened this
//! from a blanket `numeric` to `numeric`/`text`/`boolean`, reusing
//! [`super::validate::infer_field_types`] rather than a second type-inference
//! implementation).

use std::collections::HashMap;
use std::fmt;

use crate::error_code::{self, ErrorCode};
use crate::pool::{Pool, quote_ident};

use super::ast::{Expr, FieldDef, KeySpace, TransformDef, ValueType};
use super::validate::ValidationError;

/// The Postgres column type for a calculated field or grouping column of a
/// given [`ValueType`] — shared by [`create_target_table`] and
/// [`create_aggregate_target_table`] rather than duplicated. `pub(crate)`
/// so `staging::apply_aggregate` (issue #11) can render the same casts for
/// its own group-key/probe SQL without a second type-name table.
pub(crate) fn pg_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
    }
}

/// The source column a bare passthrough/rename field reads, if `field`'s
/// expression is exactly a reference to one source column (`SELECT author AS
/// author` or `SELECT author AS foo`) — the only shape whose target column
/// type can be narrowed to the source column's *concrete* Postgres type
/// (issue #45). Returns `None` for any other expression (arithmetic,
/// aggregates, function calls, literals), whose result genuinely can't be
/// narrower than its inferred [`ValueType`], so those keep collapsing through
/// [`pg_type_name`] exactly as before.
///
/// Mirrors [`super::validate`]/[`super::eval`]'s resolution order: a reference
/// to a *different* calculated field that happens to share a source column's
/// name resolves to that field, not the source column, so it isn't treated as
/// a source-column passthrough here.
fn passthrough_source_column<'a>(
    field: &'a FieldDef,
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<&'a str> {
    let Expr::Column(name) = &field.expr else {
        return None;
    };
    if !source_columns.contains_key(name) {
        return None;
    }
    let resolves_to_other_calc_field =
        name != &field.name && def.fields.iter().any(|f| &f.name == name);
    if resolves_to_other_calc_field {
        return None;
    }
    Some(name.as_str())
}

/// The source table's primary key, as introspected from `pg_catalog`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryKeyColumn {
    pub name: String,
    /// The column's Postgres type, rendered by `format_type` (e.g.
    /// `integer`, `text`, `bigint`) — safe to interpolate directly into DDL
    /// text since it comes from the catalog, not user input.
    pub data_type: String,
}

/// Why target-table DDL could not be generated or executed.
#[derive(Debug)]
pub enum DdlError {
    /// The source table has no primary key at all.
    NoPrimaryKey { source_table: String },
    /// The source table's primary key spans more than one column; only a
    /// single-column primary key is supported by this 1-1 slice.
    CompositePrimaryKeyUnsupported { source_table: String },
    /// `def`'s calculated fields failed type inference — meaning `def`
    /// reached DDL generation without having passed [`super::validate::validate`]
    /// against this same `source_columns`, since a validated definition's
    /// fields always type-check.
    InvalidDefinition(ValidationError),
    /// A persisted relationship's stored `definition_text` failed to re-parse
    /// while resolving relationships referenced by `def` (issue #40). Only
    /// arises on stored-data corruption or cross-version parser drift, but the
    /// catalog layer surfaces it rather than panicking.
    RelationshipReparse(super::error::ParseError),
    /// Substituting an [`super::ast::KeySpace::Aggregate`] definition's
    /// cross-field-alias references (see
    /// [`super::backfill::substituted_field_exprs`]) failed — a cyclic alias
    /// chain or a pathologically large expansion. A real cycle is already
    /// rejected by [`super::validate::validate`] before DDL generation runs,
    /// so this should not be reachable for a definition that reaches this
    /// point; kept as a typed error rather than a panic, matching this
    /// module's treatment of every other "should not happen" case above.
    /// Boxed because [`super::backfill::BackfillError`] itself has a
    /// [`super::backfill::BackfillError::Ddl`] variant holding a [`DdlError`]
    /// — an unboxed cycle here would make both types infinite-sized.
    AliasSubstitution(Box<super::backfill::BackfillError>),
    /// A direct Postgres protocol/query error.
    Db(tokio_postgres::Error),
    /// Acquiring a connection from the pool failed.
    Pool(crate::error::Error),
}

impl DdlError {
    /// This error's stable, coarse [`ErrorCode`] category (`docs/decisions/0008-public-api-design.md`,
    /// decision 3). Delegates to the wrapped error's own `code()` where one
    /// nests here ([`DdlError::InvalidDefinition`], [`DdlError::AliasSubstitution`],
    /// [`DdlError::Pool`]) or to [`error_code::classify_pg_error`] for a raw
    /// Postgres error, so the mapping composes rather than re-deriving a
    /// category for an error type that already has one.
    pub fn code(&self) -> ErrorCode {
        match self {
            // The source table's shape doesn't support the 1-1 DDL slice —
            // a rejected definition, same category as any other validation
            // failure.
            DdlError::NoPrimaryKey { .. } | DdlError::CompositePrimaryKeyUnsupported { .. } => {
                ErrorCode::Validation
            }
            DdlError::InvalidDefinition(err) => err.code(),
            // Stored-data corruption or cross-version parser drift, not a
            // rejection of the current call's input.
            DdlError::RelationshipReparse(_) => ErrorCode::Internal,
            DdlError::AliasSubstitution(err) => err.code(),
            DdlError::Db(err) => error_code::classify_pg_error(err),
            DdlError::Pool(err) => err.code(),
        }
    }
}

impl fmt::Display for DdlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DdlError::NoPrimaryKey { source_table } => {
                write!(f, "source table '{source_table}' has no primary key")
            }
            DdlError::CompositePrimaryKeyUnsupported { source_table } => write!(
                f,
                "source table '{source_table}' has a composite primary key, which the 1-1 \
                 target-DDL slice does not support"
            ),
            DdlError::InvalidDefinition(err) => {
                write!(f, "cannot generate target-table DDL: {err}")
            }
            DdlError::RelationshipReparse(err) => write!(
                f,
                "cannot generate target-table DDL: a referenced relationship's stored \
                 definition failed to re-parse: {err}"
            ),
            DdlError::AliasSubstitution(err) => write!(
                f,
                "cannot generate target-table DDL: calculated-field alias substitution \
                 error: {err}"
            ),
            DdlError::Db(err) => {
                write!(f, "target-table DDL database error: ")?;
                crate::error::write_pg_error(f, err)
            }
            DdlError::Pool(err) => write!(f, "failed to acquire a connection: {err}"),
        }
    }
}

impl std::error::Error for DdlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DdlError::NoPrimaryKey { .. } | DdlError::CompositePrimaryKeyUnsupported { .. } => None,
            DdlError::InvalidDefinition(err) => Some(err),
            DdlError::RelationshipReparse(err) => Some(err),
            DdlError::AliasSubstitution(err) => Some(err),
            DdlError::Db(err) => Some(err),
            DdlError::Pool(err) => Some(err),
        }
    }
}

impl From<tokio_postgres::Error> for DdlError {
    fn from(err: tokio_postgres::Error) -> Self {
        DdlError::Db(err)
    }
}

impl From<crate::error::Error> for DdlError {
    fn from(err: crate::error::Error) -> Self {
        DdlError::Pool(err)
    }
}

impl From<ValidationError> for DdlError {
    fn from(err: ValidationError) -> Self {
        DdlError::InvalidDefinition(err)
    }
}

impl From<super::backfill::BackfillError> for DdlError {
    fn from(err: super::backfill::BackfillError) -> Self {
        DdlError::AliasSubstitution(Box::new(err))
    }
}

/// Collapses a [`super::catalog::CatalogError`] from relationship resolution
/// (issue #40) into a [`DdlError`]. Relationship resolution does catalog reads
/// (DB/pool errors), a `pg_catalog` column-type lookup that raises
/// `Validate(UnknownRelationshipColumn)` for a missing to-side column, and —
/// via `relationship_by_name` — a re-parse of each referenced relationship's
/// stored `definition_text`, which can raise `Parse` on stored-data
/// corruption or parser drift. The backfill/unknown-value-type variants come
/// from paths `resolve_relationships` never exercises, so those remain
/// unreachable here.
fn map_resolve_error(err: super::catalog::CatalogError) -> DdlError {
    use super::catalog::CatalogError;
    match err {
        CatalogError::Db(e) => DdlError::Db(e),
        CatalogError::Pool(e) => DdlError::Pool(e),
        CatalogError::Validate(e) => DdlError::InvalidDefinition(e),
        CatalogError::Parse(e) => DdlError::RelationshipReparse(e),
        other => unreachable!("resolve_relationships cannot produce {other:?}"),
    }
}

/// Introspects `source_table`'s primary key from `pg_catalog`, via a bound
/// `::regclass` cast (`to_regclass($1)`) rather than string-interpolating the
/// table name into the query. `to_regclass` resolves a schema-qualified
/// `"schema.table"` string exactly (issue #76, ADR-0007) — every real caller
/// now passes one (`Definition::source_table`, or
/// `catalog::resolve_source_for_install`'s equivalent at definition-acceptance
/// time), so this no longer depends on the connection's `search_path`
/// (`crate::pool`'s session bootstrap) the way it did before issue #72/#76. A
/// bare table name still resolves via `search_path` exactly as before, for
/// any caller that genuinely has nothing more specific.
pub async fn source_primary_key(
    pool: &Pool,
    source_table: &str,
) -> Result<PrimaryKeyColumn, DdlError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_index i
             join pg_attribute a
               on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
             where i.indrelid = pg_catalog.to_regclass($1) and i.indisprimary",
            &[&source_table],
        )
        .await?;

    match rows.len() {
        0 => Err(DdlError::NoPrimaryKey {
            source_table: source_table.to_string(),
        }),
        1 => Ok(PrimaryKeyColumn {
            name: rows[0].get(0),
            data_type: rows[0].get(1),
        }),
        _ => Err(DdlError::CompositePrimaryKeyUnsupported {
            source_table: source_table.to_string(),
        }),
    }
}

/// Every column of `source_table`, mapped to its *concrete* Postgres type as
/// rendered by `format_type` (e.g. `integer`, `bigint`, `character
/// varying(255)`) — the same `pg_catalog` introspection [`source_primary_key`]
/// does for the primary key, widened to every column. Used by
/// [`create_target_table`] to give a bare source-column passthrough field its
/// source column's exact type rather than collapsing it through [`ValueType`]
/// (issue #45): a passthrough of an `integer` FK must stay `integer` on the
/// target so it remains eligible as a relationship join key, instead of
/// widening to `numeric` (which the join-key allowlist excludes). The rendered
/// type is safe to interpolate into DDL for the same reason
/// [`PrimaryKeyColumn::data_type`] is — it comes from the catalog, not user
/// input.
async fn source_column_pg_types(
    pool: &Pool,
    source_table: &str,
) -> Result<HashMap<String, String>, DdlError> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "select a.attname::text, pg_catalog.format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             where a.attrelid = pg_catalog.to_regclass($1)
               and a.attnum > 0
               and not a.attisdropped",
            &[&source_table],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The neighbor target table's name for `def` — see module docs for why this
/// is simply `def.target` unchanged. Bare — `def.target` is always the
/// unqualified table name, even for a definition whose `TRANSFORM` clause
/// explicitly wrote a `schema.table` spelling (issue #76; see
/// [`super::ast::TransformDef`]'s own doc comment for why that dotted
/// spelling never lands in this field), independent of which schema
/// [`qualified_target_table`] actually creates it under. Note this is *not*
/// the same string `transform_definitions.target_table` persists as of issue
/// #73: the catalog's own identity column holds the fully-qualified
/// `schema.table` form (built via `intake::publication::qualify`, at
/// definition-acceptance time — see `catalog::create_definition_inner`), not
/// this bare name. Callers that need a live connection (whose `search_path`
/// already resolves this bare name to the right physical table —
/// `pool::session_bootstrap` pins `target_schema` onto it) can keep using
/// this unqualified; callers that need the persisted identity string must
/// read `transform_definitions.target_table` instead, not reconstruct it
/// from this function.
pub fn neighbor_table_name(def: &TransformDef) -> &str {
    &def.target
}

/// The fully schema-qualified name of `def`'s neighbor target table under
/// `target_schema` (see the module doc comment) — `"{target_schema}"."{def.target}"`,
/// each component quoted independently via [`quote_ident`]. Built for direct
/// interpolation into DDL text, not as a persisted-identity string: this
/// quotes each component separately, whereas `transform_definitions.target_table`
/// (issue #73) is the plain, unquoted `"schema.table"` form
/// `intake::publication::qualify` builds — the two are never byte-for-byte
/// equal, so don't compare or persist this function's output as if it were
/// that identity.
pub fn qualified_target_table(target_schema: &str, def: &TransformDef) -> String {
    format!(
        "{}.{}",
        quote_ident(target_schema),
        quote_ident(neighbor_table_name(def))
    )
}

/// The read-side counterpart to [`qualified_target_table`] (issue #76,
/// ADR-0007): quotes an already-qualified `"schema.table"` name — as read
/// back from [`super::model::Definition::source_table`]
/// (`transform_definitions.source_table`), or freshly resolved by
/// `catalog::resolve_source_for_install`/`create_definition_inner`'s own
/// `qualified_source` at definition-acceptance time — for direct
/// interpolation into DDL/DML text, each component quoted independently via
/// [`quote_ident`]. This is what every physical SQL-builder that reads a
/// definition's live source table (backfill, CDC apply, quarantine
/// recompute) must use in place of a bare `quote_ident(&def.source)`/
/// `quote_ident(source_key)`, which would otherwise leave the schema to
/// resolve against whatever `search_path` the executing session happens to
/// carry (`pool::session_bootstrap`'s pinned `Config::schema`/
/// `Config::target_schema`/`"public"`) — exactly the bug class ADR-0007
/// exists to close.
///
/// Splits on the first `.`, matching `intake::publication::qualify`'s sole
/// construction site for this shape (which rejects a `.` inside either
/// component, so the first `.` here is always the real separator). Falls
/// back to quoting `qualified` whole when it carries no `.` at all — not a
/// shape any production caller produces (every real source is qualified by
/// the time it reaches here), but keeps this usable by tests/oracles that
/// hand-build a plan against a bare table name in the connection's own
/// default schema.
pub(crate) fn qualified_source_table(qualified: &str) -> String {
    quote_qualified_ident(qualified)
}

/// The target-side counterpart to [`qualified_source_table`] — same
/// component-independent quoting of an already-qualified `"schema.table"`
/// string, for a definition's *target* identity
/// ([`super::model::Definition::target_table`]) rather than its source.
///
/// Broader sweep, reviewer follow-up to issue #74 (epic #78's own
/// whole-branch review): the live CDC-apply write path (`staging::apply`'s
/// `apply_target`/truncate-clears loop, `staging::apply_aggregate`'s
/// target-write sites, `staging::quarantine`'s `recompute_column`) never got
/// this fix on the target side, even though issue #76 already let a
/// `TRANSFORM` clause spell an explicit non-default target schema — every
/// one of those sites was still binding `def.def.target` (bare) straight
/// into `quote_ident`, which silently mis-resolved (or simply couldn't find)
/// a target explicitly qualified outside the connection's pinned
/// `search_path`. Every such site now reads
/// [`super::model::Definition::target_table`] (or a plan field carrying it
/// forward, mirroring how `source`/`qualified_source` already got threaded
/// through in #76) through this function instead.
pub(crate) fn qualified_target_table_ident(qualified: &str) -> String {
    quote_qualified_ident(qualified)
}

/// Shared quoting logic for [`qualified_source_table`]/
/// [`qualified_target_table_ident`]: splits an already-qualified
/// `"schema.table"` string on its first `.` and quotes each component
/// independently, for direct interpolation into DDL/DML text. See
/// [`qualified_source_table`]'s own doc comment for the fallback/splitting
/// rationale — identical for both callers, since neither cares whether the
/// qualified string came from a source or target identity.
fn quote_qualified_ident(qualified: &str) -> String {
    match qualified.split_once('.') {
        Some((schema, table)) => format!("{}.{}", quote_ident(schema), quote_ident(table)),
        None => quote_ident(qualified),
    }
}

/// Creates `def`'s neighbor target table (idempotent: `create table if not
/// exists`) with `pk` as its primary key and one column per calculated
/// field, typed per that field's inferred [`ValueType`] (`numeric`, `text`,
/// or `boolean`). `source_columns` is the same source-column-to-[`ValueType`]
/// map `def` was validated against — needed here to re-derive each field's
/// type, since the grammar has no separate "declare a target column's type"
/// syntax (a field's inferred type *is* its target column's type).
///
/// `target_schema` is the schema the table is created under (see
/// [`qualified_target_table`]) — distinct from the connection's own
/// Trellis-managed schema, so this is always schema-qualified explicitly
/// rather than relying on `search_path`.
///
/// `source_table` is `def.source`'s fully-qualified `"schema.table"` identity
/// (issue #76, ADR-0007) — the caller's own already-resolved
/// `catalog::resolve_source_for_install` result — used below (via
/// [`qualified_source_table`]) to introspect a passthrough field's concrete
/// source column type, rather than the bare `def.source` left to
/// `search_path`.
pub async fn create_target_table(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    pk: &PrimaryKeyColumn,
    source_columns: &HashMap<String, ValueType>,
    source_table: &str,
) -> Result<(), DdlError> {
    // Issue #40: a relationship-enriched field's type is the referenced
    // to-side column's type, which `infer_field_types` reads from resolved
    // relationship metadata. Resolve it the same way `create_definition` does
    // (catalog + `pg_catalog` lookups); a relationship-free definition
    // resolves to an empty map and behaves exactly as before.
    let relationships = super::catalog::resolve_relationships(pool, def)
        .await
        .map_err(map_resolve_error)?;
    let field_types = super::validate::infer_field_types(def, source_columns, &relationships)?;

    // Issue #45: a bare source-column passthrough keeps the source column's
    // *concrete* Postgres type instead of collapsing through `ValueType` (so a
    // passthrough of an `integer` FK stays `integer`, not `numeric`, and can
    // still serve as a relationship join key). Only introspect the source
    // table's column types when at least one such field exists — a definition
    // with none behaves exactly as before, no extra query.
    //
    // Staging (`staging::apply`) still casts every value through its
    // `ValueType`-based cast (e.g. `::text::numeric`) before the INSERT,
    // regardless of the narrower concrete column type declared here — it
    // relies on Postgres's implicit assignment cast (`numeric` -> `integer`,
    // `text` -> `varchar(n)`, ...) to land the value. That's only safe because
    // a bare passthrough's value provably originates from this same,
    // identically-typed source column, so it always satisfies the narrower
    // column's constraints. If a passthrough field's value could ever diverge
    // from its source column's type/width, this coupling would need
    // revisiting (staging would need to cast to the concrete type too).
    let passthroughs: HashMap<&str, &str> = def
        .fields
        .iter()
        .filter_map(|f| {
            passthrough_source_column(f, def, source_columns).map(|col| (f.name.as_str(), col))
        })
        .collect();
    let source_pg_types = if passthroughs.is_empty() {
        HashMap::new()
    } else {
        source_column_pg_types(pool, source_table).await?
    };

    let mut sql = format!(
        "create table if not exists {} ({} {} primary key",
        qualified_target_table(target_schema, def),
        quote_ident(&pk.name),
        pk.data_type,
    );
    for field in &def.fields {
        let pg_type = match passthroughs
            .get(field.name.as_str())
            .and_then(|col| source_pg_types.get(*col))
        {
            Some(concrete) => concrete.clone(),
            None => pg_type_name(
                field_types
                    .get(&field.name)
                    .copied()
                    .unwrap_or(ValueType::Numeric),
            )
            .to_string(),
        };
        sql.push_str(&format!(", {} {}", quote_ident(&field.name), pg_type));
    }
    sql.push(')');

    let client = pool.get().await?;
    client.batch_execute(&sql).await?;
    Ok(())
}

/// Whether `field` is a direct `AVG(...)` call — the shape whose delta
/// model (issue #11) needs hidden running-sum/running-count partials
/// alongside its visible column, since `avg = sum / count` and neither half
/// alone is invertible (see `defs::invertibility`'s doc comment on
/// [`super::invertibility::PartialField`]). Checked structurally rather than
/// via `invertibility::classify` here: the grammar only ever parses an `AVG`
/// call over a `Numeric` argument (`registry::AGGREGATE_FUNCTION_SPECS`
/// restricts every aggregate's argument type to `Numeric`), so there is no
/// `Text`/`Boolean`-argument `AVG` this gate would need to route to the
/// recompute path instead — the name alone determines the answer for every
/// definition this grammar can actually produce.
///
/// Takes the field's expression directly (rather than a [`FieldDef`]) so a
/// caller can classify a *substituted* expression — see
/// [`create_aggregate_target_table`]'s doc comment on why a bare
/// cross-field-alias reference (`total2 = total` where `total = SUM(amount)`)
/// must be classified against its self-contained, alias-resolved form rather
/// than the raw `Expr::Column("total")` a naive per-field read would see.
fn is_avg_field(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. } if name == "AVG")
}

/// Whether `field` is a direct `SUM(...)` call — see [`is_avg_field`]'s doc
/// comment for why this is checked structurally rather than via
/// `invertibility::classify`. `SUM` needs its own hidden running-count
/// partial (see [`count_column_names`]) for the same reason `AVG` needs
/// one: Postgres's `sum()` is `NULL`, not `0`, over zero non-null values, and
/// without a count the delta model can't distinguish "no contributions left"
/// from "contributions that net to zero" once a group's row count is no
/// longer directly observable from the running sum alone.
fn is_sum_field(expr: &Expr) -> bool {
    matches!(expr, Expr::FunctionCall { name, .. } if name == "SUM")
}

/// The hidden running-sum partial column name an `AVG` field maintains
/// (issue #11's delta model): the numerator `AVG`'s visible column is derived
/// from (`avg = sum / count`). Always one per `AVG` field — unlike the
/// running-count partial (see [`count_column_names`]'s doc comment on issue
/// #48), two `AVG` fields' running sums can never be shared: even when two
/// fields aggregate the exact same argument, their *sums* are only equal by
/// construction for `SUM`-vs-`AVG` pairs that also share a count (an `AVG`
/// field always needs its own sum regardless, since `SUM`'s visible column
/// already *is* that shared sum for the `SUM` half — there is no second
/// consumer to fold onto). `pub(crate)` so `staging::apply_aggregate` binds
/// against the exact same name this module creates, rather than re-deriving
/// it.
pub(crate) fn avg_sum_column(field_name: &str) -> String {
    format!("__{field_name}_sum")
}

/// The `Expr` a `SUM`/`AVG` field aggregates over — `None` for any other
/// field shape. [`count_column_names`] uses this to decide which fields'
/// hidden count partials can share a column; `staging::apply_aggregate`'s
/// `AggregateTargetPlan` runs the equivalent lookup against its own
/// already-classified fields, over the same [`Expr`] equality, to derive
/// matching names without a second implementation of this rule.
fn count_needing_arg(expr: &Expr) -> Option<&Expr> {
    if !is_sum_field(expr) && !is_avg_field(expr) {
        return None;
    }
    match expr {
        Expr::FunctionCall { args, .. } => args.first(),
        _ => None,
    }
}

/// Assigns every `SUM`/`AVG` field in `fields` (in declaration order) the
/// name of the hidden running-count partial column it maintains (issue #11's
/// delta model: the count of non-null argument values contributing to the
/// group, matching Postgres's own `count(<same argument>)` "skip NULLs"
/// semantics) — `pub(crate)` so `staging::apply_aggregate`'s
/// `AggregateTargetPlan` derives the exact same names this module's DDL
/// creates, rather than re-deriving them independently (a divergence there
/// would mean the apply path writes to a column the DDL never created, or
/// vice versa).
///
/// **Issue #48**: naively, every count-needing field got its own
/// `__{field}_count` column, even when two fields aggregate the exact
/// identical argument expression (e.g. `SUM(amount) AS total, AVG(amount) AS
/// average` both aggregating the same `amount` column) and are therefore
/// provably counting the exact same set of non-null-contributing rows. This
/// function detects that case — via [`Expr`]'s derived structural
/// `PartialEq` on [`count_needing_arg`]'s result — and has the later field's
/// entry point at the earlier field's column name instead of minting a
/// second, redundant one.
///
/// It deliberately does **not** merge count columns across fields whose
/// arguments differ (e.g. the issue's own motivating example, `SUM(word_count)
/// AS total_words, SUM(byte_size) AS total_bytes`) into one target-wide
/// `__group_count`, even though the issue asked for exactly that: two
/// different source columns can have different `NULL`s on the very same row,
/// so their "count of non-null contributing rows" can genuinely diverge (a
/// row with a `NULL` `word_count` but a real `byte_size` contributes to one
/// count and not the other). Forcing them onto one shared column would
/// silently corrupt whichever field's count that column doesn't actually
/// track the moment their arguments' `NULL` patterns diverge — the delta
/// model would compute the wrong "does this group still have any non-null
/// contributor" answer for one of the two fields, producing a stale `0`/wrong
/// number where Postgres would show `NULL`, or vice versa (see
/// `staging::apply_aggregate`'s `sum_goes_null_not_zero_when_a_groups_remaining_rows_are_all_null`
/// test for the exact failure shape this would reintroduce). Only fields
/// that provably always agree — same argument expression — are ever merged.
/// `substituted` maps each field name to its cross-field-alias-resolved
/// expression (see [`super::backfill::substituted_field_exprs`]) — classifying
/// off the substituted view rather than each field's raw, possibly-aliasing
/// `Expr` is what lets a field that only resolves to a bare `SUM`/`AVG` call
/// *after* substitution (`total2 = total` where `total = SUM(amount)`) still
/// get a hidden count-column name here, consistent with
/// [`create_aggregate_target_table`]'s own (also substituted) classification
/// of the very same field.
pub(crate) fn count_column_names(
    fields: &[FieldDef],
    substituted: &HashMap<String, Expr>,
) -> HashMap<String, String> {
    count_column_names_from(
        fields.iter().filter_map(|f| {
            count_needing_arg(&substituted[&f.name]).map(|arg| (f.name.as_str(), arg))
        }),
    )
}

/// The core of [`count_column_names`], generalized over any source of
/// `(field_name, aggregated_arg)` pairs rather than a literal `&[FieldDef]` —
/// `staging::apply_aggregate`'s `AggregateTargetPlan` has already classified
/// its fields into [`super::ast::FieldDef`]-free `AggFieldPlan`s by the time
/// it needs these names, so it builds its own `(name, arg)` pairs from that
/// classification plus its `field_exprs` map and calls straight into this,
/// rather than re-implementing the merge rule (see [`count_column_names`]'s
/// doc comment on why that rule's correctness matters) a second time.
pub(crate) fn count_column_names_from<'a>(
    entries: impl Iterator<Item = (&'a str, &'a Expr)>,
) -> HashMap<String, String> {
    let mut by_arg: Vec<(&Expr, String)> = Vec::new();
    let mut names = HashMap::new();
    for (field_name, arg) in entries {
        let name = match by_arg.iter().find(|(seen, _)| **seen == *arg) {
            Some((_, existing)) => existing.clone(),
            None => {
                let fresh = format!("__{field_name}_count");
                by_arg.push((arg, fresh.clone()));
                fresh
            }
        };
        names.insert(field_name.to_string(), name);
    }
    names
}

/// Creates an [`super::ast::KeySpace::Aggregate`] definition's neighbor
/// target table (idempotent, same convention as [`create_target_table`]),
/// whose primary key is the composite tuple of grouping columns rather than
/// a single column inherited from the source — a `GROUP BY` target has no
/// single source row to inherit a key from; the group itself is the key.
///
/// Each grouping column's type comes from `source_columns` (the same
/// [`ValueType`]-only map every other column type in this grammar is
/// derived from — there's no separate exact-Postgres-type introspection for
/// grouping columns, unlike the 1-1 primary key's [`source_primary_key`]).
///
/// A calculated field whose name matches a grouping column (the
/// `SELECT order_id AS order_id, SUM(amount) AS total` passthrough idiom)
/// contributes no separate column — it's assumed to be that same grouping
/// value passed through, already covered by the primary key column above.
///
/// `target_schema` is the schema the table is created under — see
/// [`create_target_table`]'s doc comment on why this is always
/// schema-qualified explicitly rather than relying on `search_path`.
///
/// # Panics
///
/// If `def.key_space` is not [`KeySpace::Aggregate`].
pub async fn create_aggregate_target_table(
    pool: &Pool,
    def: &TransformDef,
    target_schema: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), DdlError> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        panic!("create_aggregate_target_table called on a non-aggregate definition");
    };

    // Substitute cross-field-alias references (e.g. `total2 = total` where
    // `total = SUM(amount)`) before classifying any field as `SUM`/`AVG`
    // below — `is_avg_field`/`is_sum_field`/`count_column_names` must agree
    // with `backfill_aggregate`'s and `apply_aggregate::classify_fields`'s own
    // (already substituted) classification of the same field, or the columns
    // this DDL creates diverge from the columns those paths later write to.
    let substituted = super::backfill::substituted_field_exprs(def)?;

    // Issue #94: a GROUP BY aggregate field may aggregate a *to-one*
    // relationship path (`SUM(post.word_count)`), whose type is the to-side
    // column's, not any column of `source_columns`. Resolve the relationship
    // metadata the same way [`create_target_table`] does so type inference can
    // reach it; a relationship-free aggregate resolves to an empty map and
    // behaves exactly as before.
    let relationships = super::catalog::resolve_relationships(pool, def)
        .await
        .map_err(map_resolve_error)?;
    let field_types = super::validate::infer_field_types(def, source_columns, &relationships)?;

    let mut sql = format!(
        "create table if not exists {} (",
        qualified_target_table(target_schema, def)
    );
    for (i, column) in group_by.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let pg_type = pg_type_name(
            source_columns
                .get(column)
                .copied()
                .unwrap_or(ValueType::Numeric),
        );
        sql.push_str(&format!("{} {}", quote_ident(column), pg_type));
    }
    // Issue #48: fields aggregating the exact same argument (e.g. `SUM(amount)
    // AS total, AVG(amount) AS average`) share one hidden running-count
    // partial column rather than each minting its own — see
    // `count_column_names`'s doc comment for why this dedup is scoped to
    // "same argument expression" rather than "any count-needing field on this
    // target", which would silently corrupt the delta model once two fields'
    // arguments have different `NULL` patterns.
    let count_cols = count_column_names(&def.fields, &substituted);
    let mut emitted_count_cols: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for field in &def.fields {
        if group_by.contains(&field.name) {
            continue;
        }
        let pg_type = pg_type_name(
            field_types
                .get(&field.name)
                .copied()
                .unwrap_or(ValueType::Numeric),
        );
        sql.push_str(&format!(", {} {}", quote_ident(&field.name), pg_type));

        let expr = &substituted[&field.name];
        // AVG's hidden partials (see `is_avg_field`'s doc comment): the
        // visible column above holds the derived `sum / count`. The running
        // sum is always this field's own; the running count may already have
        // been declared by an earlier field sharing this exact argument.
        if is_avg_field(expr) {
            let sum_col = avg_sum_column(&field.name);
            sql.push_str(&format!(", {} numeric", quote_ident(&sum_col)));
            let count_col = &count_cols[&field.name];
            if emitted_count_cols.insert(count_col.as_str()) {
                sql.push_str(&format!(", {} bigint", quote_ident(count_col)));
            }
        } else if is_sum_field(expr) {
            // SUM's hidden count partial (see `is_sum_field`'s doc comment):
            // the visible column above holds the running sum directly, but
            // this is needed to tell "sum of nothing" (NULL) from "sum that
            // happens to net to zero" (0).
            let count_col = &count_cols[&field.name];
            if emitted_count_cols.insert(count_col.as_str()) {
                sql.push_str(&format!(", {} bigint", quote_ident(count_col)));
            }
        }
    }
    let pk_columns: Vec<String> = group_by.iter().map(|c| quote_ident(c)).collect();
    sql.push_str(&format!(", primary key ({})", pk_columns.join(", ")));
    sql.push(')');

    let client = pool.get().await?;
    client.batch_execute(&sql).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::{FieldDef, KeySpace, Operator, Predicate};

    fn def() -> TransformDef {
        TransformDef {
            target: "order_totals".to_string(),
            source: "orders".to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![FieldDef {
                name: "total".to_string(),
                expr: crate::defs::ast::Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(crate::defs::ast::Expr::Column("price".to_string())),
                    rhs: Box::new(crate::defs::ast::Expr::Column("tax".to_string())),
                },
            }],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn neighbor_table_name_is_the_definitions_target() {
        assert_eq!(neighbor_table_name(&def()), "order_totals");
    }

    #[test]
    fn qualified_target_table_combines_target_schema_and_name() {
        assert_eq!(
            qualified_target_table("public", &def()),
            "\"public\".\"order_totals\""
        );
        assert_eq!(
            qualified_target_table("analytics", &def()),
            "\"analytics\".\"order_totals\""
        );
    }
}
