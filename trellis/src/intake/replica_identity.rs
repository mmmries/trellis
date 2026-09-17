//! The checked REPLICA IDENTITY FULL requirement (issue #7): a definition
//! whose derivation needs a changed row's *old* image (a child deleted from
//! a one-to-many aggregate, or a re-parented row) must be rejected rather
//! than silently fed a missing image, and the rejection must name the exact
//! DDL an operator needs to run — Trellis does not issue DDL against tables
//! it doesn't own. See docs/staging-and-claiming/01-intake-and-lsn-confirmation.md.
//!
//! The two functions are split so the rejection can be tested independently:
//! [`needs_old_image`] is the predicate over a [`TransformDef`] —
//! `false` for [`KeySpace::OneToOne`] (a pure function of the *current* row)
//! and `true` for [`KeySpace::Aggregate`] (issue #47: a row leaving or
//! re-entering a group needs the row's old image to know which group it's
//! leaving) — while [`require_replica_identity_full`] takes a plain `bool`,
//! so its own test can exercise the `true` side directly rather than having
//! to construct a `KeySpace::Aggregate` definition just to reach it. The
//! chained call site `require_replica_identity_full(src, needs_old_image(def))`
//! is how the two compose; `defs::catalog::assert_replica_identity_supports_aggregate`
//! is the one that actually wires it in for `KeySpace::Aggregate` (it
//! doesn't call [`needs_old_image`] itself, since it needs to distinguish
//! "the source is already `REPLICA IDENTITY FULL`" from "it needs to be" —
//! see that function's own doc comment). `defs::catalog::assert_replica_identity_supports_projection`
//! (issue #129, epic #127; extended to the from-side by issue #158) is the
//! analogous gate for a to-one relationship's to-side *and* from-side
//! tables — not `needs_old_image`-driven either, and not part of
//! this predicate, since a relationship's own [`crate::defs::ast::RelationshipDef`]
//! isn't a [`TransformDef`] at all.
use super::error::IntakeError;
use crate::defs::ast::{KeySpace, TransformDef};

/// Whether `def`'s derivation needs the old image of a row — `false` for
/// every shape the current grammar can express (see the module doc).
pub fn needs_old_image(def: &TransformDef) -> bool {
    match def.key_space {
        KeySpace::OneToOne => false,
        // A row leaving or re-entering a group (delete, or an update that
        // changes a grouping-key value) changes that group's aggregate, and
        // recomputing it needs the row's old image to know which group it's
        // leaving.
        KeySpace::Aggregate { .. } => true,
    }
}

/// Rejects `src_table` if `needs_old_image` is `true`, with the exact
/// `ALTER TABLE ... REPLICA IDENTITY FULL;` text the error must carry
/// (issue #7's requirement) so an operator can copy it verbatim.
pub fn require_replica_identity_full(
    src_table: &str,
    needs_old_image: bool,
) -> Result<(), IntakeError> {
    if needs_old_image {
        return Err(IntakeError::ReplicaIdentityRequired {
            table: src_table.to_string(),
            statement: format!("ALTER TABLE {src_table} REPLICA IDENTITY FULL;"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::ast::Predicate;

    fn one_to_one_def(source: &str) -> TransformDef {
        TransformDef {
            target: "derived".to_string(),
            source: source.to_string(),
            key_space: KeySpace::OneToOne,
            fields: vec![],
            predicate: Predicate::True,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }

    #[test]
    fn one_to_one_transforms_never_need_the_old_image() {
        assert!(!needs_old_image(&one_to_one_def("orders")));
    }

    #[test]
    fn a_definition_that_does_need_it_is_rejected_with_the_exact_ddl() {
        // No `KeySpace` variant needs the old image yet, so construct the
        // `true` side directly to exercise the rejection.
        let err = require_replica_identity_full("line_items", true)
            .expect_err("a definition needing the old image must be rejected");
        assert_eq!(
            err.to_string(),
            "table line_items needs its old row image for a derivation that requires it; run \
             this against the source database first: ALTER TABLE line_items REPLICA IDENTITY \
             FULL;"
        );
    }

    #[test]
    fn a_definition_that_does_not_need_it_is_accepted() {
        require_replica_identity_full("orders", false).expect("should not be rejected");
    }
}
