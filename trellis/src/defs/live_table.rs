//! A table's live columns, as Postgres reports them now: the one
//! `pg_attribute` read every check of a definition's tables builds on.
//!
//! Define, a resume's re-validation, `ALTER TRANSFORM`, the definition floor
//! and the capture pass's key-type check all read a table's columns through
//! [`LiveTable::read`], and each takes the view it needs from the result: the
//! validator's typed columns ([`LiveTable::typed`],
//! [`LiveTable::value_types`]), a key column's collation, a column's copy
//! type, or its recorded key type ([`LiveColumn::ty`]).

use std::collections::HashMap;

use tokio_postgres::GenericClient;
use tokio_postgres::types::Type;

use super::ast::ValueType;
use super::catalog::{VIRTUAL_EXPRESSION_SQL, VirtualColumns};
use super::pg_type::PgType;
use super::validate::JoinColumnInfo;

/// One live column of a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveColumn {
    /// The column as the validator reads it. Its `value_type` is
    /// [`PgType::Unrecognized`] for a type the registry can't place, which
    /// [`LiveTable::typed`] leaves out.
    pub info: JoinColumnInfo,
    /// The type's `nspname.typname`, unquoted: the name a recorded key type
    /// carries ([`super::key_types::ColumnType`]).
    pub type_name: String,
}

impl LiveColumn {
    /// The column's type as a key's recorded type is compared with it.
    pub(crate) fn ty(&self) -> super::key_types::ColumnType {
        super::key_types::ColumnType {
            type_oid: self.info.type_oid,
            type_name: self.type_name.clone(),
            typmod: self.info.typmod,
        }
    }

    /// `format_type`'s rendering of the column's type, modifier included,
    /// which the key allowlist is written in and messages name.
    pub(crate) fn display(&self) -> &str {
        &self.info.pg_type
    }

    /// [`LiveColumn::display`] for a `pg_catalog` type, and the type's
    /// qualified name otherwise, so a message can't name a user type as the
    /// built-in one it is named like (`public."integer"`, not `"integer"`).
    pub(crate) fn qualified_display(&self) -> &str {
        if self.type_name.starts_with("pg_catalog.") {
            self.display()
        } else {
            &self.info.copy_type
        }
    }

    /// The collation's name, when the column has one that is
    /// nondeterministic.
    pub(crate) fn nondeterministic_collation(&self) -> Option<&str> {
        if self.info.collation_deterministic {
            return None;
        }
        self.info.collation.as_deref()
    }
}

/// Every live column of a table, and its virtual generated columns.
#[derive(Debug, Clone, Default)]
pub(crate) struct LiveTable {
    /// Every column, whatever its type.
    pub columns: HashMap<String, LiveColumn>,
    /// The table's virtual generated columns with their expressions, for
    /// [`super::catalog::reject_virtual_generated_reads`].
    pub virtual_columns: VirtualColumns,
}

impl LiveTable {
    /// Reads `qualified` (an unquoted `schema.table`) in one query. Empty for
    /// a table that doesn't exist.
    ///
    /// Each column's [`ValueType`] is classified by its `atttypid` as
    /// [`super::pg_type::value_type_for_oid`] does, which also places a
    /// user-defined enum.
    pub(crate) async fn read(
        client: &impl GenericClient,
        qualified: &str,
    ) -> Result<LiveTable, tokio_postgres::Error> {
        let rows = client
            .query_typed(
                &format!(
                    "select a.attname::text,
                        pg_catalog.format_type(a.atttypid, a.atttypmod),
                        a.atttypid,
                        a.atttypmod,
                        a.attcollation,
                        co.collname::text,
                        coalesce(co.collisdeterministic, true),
                        t.typtype = 'e',
                        {},
                        {},
                        {VIRTUAL_EXPRESSION_SQL},
                        n.nspname::text,
                        t.typname::text,
                        exists (select 1 from transform_definitions where target_table = $2)
                 from pg_catalog.pg_attribute a
                 join pg_catalog.pg_type t on t.oid = a.atttypid
                 join pg_catalog.pg_namespace n on n.oid = t.typnamespace
                 left join pg_catalog.pg_collation co on co.oid = a.attcollation
                 where a.attrelid = pg_catalog.to_regclass($1)
                   and a.attnum > 0
                   and not a.attisdropped",
                    super::copy_type::type_sql("a.atttypid"),
                    super::copy_type::collation_sql("a.attcollation")
                ),
                &[
                    (&super::ddl::regclass_arg(qualified), Type::TEXT),
                    (&qualified, Type::TEXT),
                ],
            )
            .await?;
        let mut table = LiveTable::default();
        for row in rows {
            let name: String = row.get(0);
            if let Some(expression) = row.get::<_, Option<String>>(10) {
                table.virtual_columns.insert(name.clone(), expression);
            }
            let type_oid: u32 = row.get(2);
            let is_enum: bool = row.get(7);
            let schema: String = row.get(11);
            let typname: String = row.get(12);
            let value_type = super::pg_type::value_type_for_read_type(
                type_oid,
                is_enum.then_some((schema.as_str(), typname.as_str())),
            );
            table.columns.insert(
                name,
                LiveColumn {
                    info: JoinColumnInfo {
                        value_type,
                        pg_type: row.get(1),
                        type_oid,
                        typmod: row.get(3),
                        collation_oid: row.get(4),
                        collation: row.get(5),
                        collation_deterministic: row.get(6),
                        is_enum,
                        copy_type: row.get(8),
                        copy_collation: row.get(9),
                        // A column of a table Trellis creates has no type
                        // modifier (ADR-0020).
                        trellis_target: row.get(13),
                    },
                    type_name: format!("{schema}.{typname}"),
                },
            );
        }
        Ok(table)
    }

    /// The column named `name`, of any type.
    pub(crate) fn column(&self, name: &str) -> Option<&LiveColumn> {
        self.columns.get(name)
    }

    /// The columns the validator can type, as
    /// [`super::validate::validate_join`] reads them. A column whose type the
    /// registry can't place at all (an array, a range, a composite, a
    /// domain, `citext`) is left out, so the validator reports a reference to
    /// it as an unknown column: everything downstream needs its real Postgres
    /// type, and `Unrecognized` isn't one. Promoting those families is
    /// `docs/type-support.md`'s deferred work (#122).
    pub(crate) fn typed(&self) -> HashMap<String, JoinColumnInfo> {
        self.columns
            .iter()
            .filter(|(_, column)| column.info.value_type != ValueType::Other(PgType::Unrecognized))
            .map(|(name, column)| (name.clone(), column.info.clone()))
            .collect()
    }

    /// [`LiveTable::typed`]'s columns with their [`ValueType`]s, as
    /// [`super::validate::validate`] reads them.
    pub(crate) fn value_types(&self) -> HashMap<String, ValueType> {
        self.columns
            .iter()
            .filter(|(_, column)| column.info.value_type != ValueType::Other(PgType::Unrecognized))
            .map(|(name, column)| (name.clone(), column.info.value_type))
            .collect()
    }
}
