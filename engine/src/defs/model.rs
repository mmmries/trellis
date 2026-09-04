//! The definition model (issue #23): a validated [`TransformDef`] (issue
//! #22's AST) plus the identity the catalog assigns once it's persisted.

use std::collections::HashMap;

use super::ast::{RelationshipDef, TransformDef, ValueType};

/// A transform definition as stored in the catalog: the parsed AST — source
/// table, target table name, and calculated fields are all on
/// [`TransformDef`] already — plus the catalog-assigned id, the source
/// table's version at the moment this definition was created (the value
/// stage 05's version fence will read), and the source-column type map `def`
/// was validated against (issue #63's write-path gap: persisted so the
/// physical apply path — [`crate::staging::apply::compute`] — can evaluate
/// non-Numeric fields the same way [`super::catalog::create_definition`]
/// validated them, rather than re-deriving or defaulting to Numeric).
#[derive(Debug, Clone, PartialEq)]
pub struct Definition {
    pub id: i64,
    pub source_version: i64,
    pub def: TransformDef,
    pub source_columns: HashMap<String, ValueType>,
}

/// A relationship declaration as stored in the catalog (issue #26): the
/// parsed [`RelationshipDef`] plus the catalog-assigned id. Unlike
/// [`Definition`], there's no `source_version`/`source_columns` to carry —
/// a relationship's endpoints aren't validated against a live column-type
/// map at creation time (no source-schema DDL, ADR-0005; cardinality/FK
/// validation is later issue scope), so nothing here depends on the
/// from-side table's version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDefinition {
    pub id: i64,
    pub def: RelationshipDef,
}

/// Which role [`super::catalog::resolve_node`] is being asked to establish
/// for a table: a **source** table is one Trellis reads over logical
/// replication and never issues DDL against (ADR-0005); a **target** table
/// is one a transform declares and creates. These aren't mutually
/// exclusive on a table over its lifetime — a transform's target is a
/// completely ordinary table a *later* transform can subscribe to as its
/// source (chained/multi-hop transforms, exercised by
/// `engine/tests/apply.rs`'s two-hop propagation tests), so the same
/// physical table ends up resolved under both roles. [`SchemaNode`] tracks
/// that as two independent flags rather than one exclusive kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Source,
    Target,
}

/// A first-class identity for a table Trellis knows about — as a source, a
/// target, or (via chained transforms) both — that transforms (and, later,
/// relationships) resolve their endpoints against instead of a bare
/// table-name string. One row per physical table: `is_source`/`is_target`
/// each start `false` and are only ever set to `true` by
/// [`super::catalog::resolve_node`], never back to `false`. Only identity is
/// persisted ([`super::catalog`]'s `schema_nodes` table); a node's columns
/// and types are introspected live from `pg_catalog`/`information_schema`
/// rather than cached here, per ADR-0005.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaNode {
    pub id: i64,
    pub table_name: String,
    pub is_source: bool,
    pub is_target: bool,
}

/// Which kind of dependency a [`super::catalog`] edge represents (issue
/// #21). [`EdgeKind::Relationship`] is persisted by `create_relationship`
/// (issue #26); [`EdgeKind::Source`] remains the only kind a transform
/// itself persists — a transform's `FROM` is its only join input the AST
/// can produce (see `engine/src/defs/ast.rs`'s `TransformDef::source`, a
/// single `String`, no multi-source join yet). `Join` exists so the column
/// this enum backs doesn't need a migration when join-edge persistence
/// lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Source,
    Join,
    Relationship,
}

impl EdgeKind {
    /// The text this variant is persisted/queried as in `schema_edges.kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            EdgeKind::Source => "source",
            EdgeKind::Join => "join",
            EdgeKind::Relationship => "relationship",
        }
    }
}

/// A directed dependency edge between two [`SchemaNode`]s: `to_node`
/// depends on `from_node` via `kind` (e.g. a `Source` edge from `orders` to
/// `order_totals` means `order_totals` is a transform target reading from
/// `orders`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaEdge {
    pub id: i64,
    pub from_node_id: i64,
    pub to_node_id: i64,
    pub kind: EdgeKind,
}
