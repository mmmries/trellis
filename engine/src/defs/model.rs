//! The definition model (issue #23): a validated [`TransformDef`] (issue
//! #22's AST) plus the identity the catalog assigns once it's persisted.

use std::collections::HashMap;

use super::ast::{TransformDef, ValueType};

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
