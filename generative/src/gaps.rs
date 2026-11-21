//! The register of known correctness gaps
//! (`docs/known-correctness-gaps.md`), as data a drawn shape can name.
//!
//! A run that judges "never silently wrong" ([`crate::run::judge`]) lets a
//! definition end silently wrong only when the shape it drew is tagged with
//! the register entry it reproduces. Each [`Gap`] is one entry, named by the
//! entry's stable anchor (`<a id="…">` above its heading). The anchors never
//! change once used, and a closed entry's anchor leaves with it, so the
//! register is a checked allowlist: the unit test below fails when an entry
//! and this list disagree, and CI's link check (`check-doc-links.py`)
//! resolves every `docs/known-correctness-gaps.md#…` this file names.

/// One entry of `docs/known-correctness-gaps.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Gap {
    /// `docs/known-correctness-gaps.md#alter-type-rewrite`
    AlterTypeRewrite,
    /// `docs/known-correctness-gaps.md#enum-label-readded`
    EnumLabelReadded,
    /// `docs/known-correctness-gaps.md#column-name-swap`
    ColumnNameSwap,
    /// `docs/known-correctness-gaps.md#nondeterministic-collation`
    NondeterministicCollation,
    /// `docs/known-correctness-gaps.md#key-not-null-flip`
    KeyNotNullFlip,
    /// `docs/known-correctness-gaps.md#deferrable-key-replica-duplicate`
    DeferrableKeyReplicaDuplicate,
    /// `docs/known-correctness-gaps.md#capture-disrupted-between-passes`
    CaptureDisruptedBetweenPasses,
    /// `docs/known-correctness-gaps.md#unlogged-source-reset`
    UnloggedSourceReset,
    /// `docs/known-correctness-gaps.md#merge-moves-partition`
    MergeMovesPartition,
    /// `docs/known-correctness-gaps.md#source-rename-roundtrip`
    SourceRenameRoundtrip,
    /// `docs/known-correctness-gaps.md#default-partition-short-leaf`
    DefaultPartitionShortLeaf,
    /// `docs/known-correctness-gaps.md#leaf-reattach-around-upgrade`
    LeafReattachAroundUpgrade,
    /// `docs/known-correctness-gaps.md#owned-tables-written`
    OwnedTablesWritten,
    /// `docs/known-correctness-gaps.md#partial-restore`
    PartialRestore,
    /// `docs/known-correctness-gaps.md#repeatable-read-old-key`
    RepeatableReadOldKey,
    /// `docs/known-correctness-gaps.md#transaction-pooler`
    TransactionPooler,
}

/// The repair an entry documents, as the harness runs it: through library
/// calls (`Trellis::request_backfill`, `PAUSE`/`RESUME TRANSFORM`), never
/// the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repair {
    /// `request_backfill` of the definition's source table.
    RequestBackfill,
    /// `PAUSE` then `RESUME` of the definition.
    PauseResume,
    /// The entry documents no repair the harness can run (a configuration
    /// or a restore); a shape tagged with it can't be judged by the
    /// register.
    None,
}

impl Gap {
    /// Every entry, in the register's order.
    pub const ALL: [Gap; 16] = [
        Gap::AlterTypeRewrite,
        Gap::EnumLabelReadded,
        Gap::ColumnNameSwap,
        Gap::NondeterministicCollation,
        Gap::KeyNotNullFlip,
        Gap::DeferrableKeyReplicaDuplicate,
        Gap::CaptureDisruptedBetweenPasses,
        Gap::UnloggedSourceReset,
        Gap::MergeMovesPartition,
        Gap::SourceRenameRoundtrip,
        Gap::DefaultPartitionShortLeaf,
        Gap::LeafReattachAroundUpgrade,
        Gap::OwnedTablesWritten,
        Gap::PartialRestore,
        Gap::RepeatableReadOldKey,
        Gap::TransactionPooler,
    ];

    /// The entry's stable anchor in `docs/known-correctness-gaps.md`.
    pub fn slug(self) -> &'static str {
        match self {
            Gap::AlterTypeRewrite => "alter-type-rewrite",
            Gap::EnumLabelReadded => "enum-label-readded",
            Gap::ColumnNameSwap => "column-name-swap",
            Gap::NondeterministicCollation => "nondeterministic-collation",
            Gap::KeyNotNullFlip => "key-not-null-flip",
            Gap::DeferrableKeyReplicaDuplicate => "deferrable-key-replica-duplicate",
            Gap::CaptureDisruptedBetweenPasses => "capture-disrupted-between-passes",
            Gap::UnloggedSourceReset => "unlogged-source-reset",
            Gap::MergeMovesPartition => "merge-moves-partition",
            Gap::SourceRenameRoundtrip => "source-rename-roundtrip",
            Gap::DefaultPartitionShortLeaf => "default-partition-short-leaf",
            Gap::LeafReattachAroundUpgrade => "leaf-reattach-around-upgrade",
            Gap::OwnedTablesWritten => "owned-tables-written",
            Gap::PartialRestore => "partial-restore",
            Gap::RepeatableReadOldKey => "repeatable-read-old-key",
            Gap::TransactionPooler => "transaction-pooler",
        }
    }

    /// The first repair the entry's **Repair** names that the harness can
    /// run, or [`Repair::None`] when it names only a configuration change,
    /// a restore, or a redefinition, or needs a change to the source first
    /// (a duplicate removed, a collation changed back).
    pub fn repair(self) -> Repair {
        match self {
            Gap::MergeMovesPartition
            | Gap::SourceRenameRoundtrip
            | Gap::DefaultPartitionShortLeaf
            | Gap::LeafReattachAroundUpgrade => Repair::PauseResume,
            Gap::NondeterministicCollation
            | Gap::DeferrableKeyReplicaDuplicate
            | Gap::OwnedTablesWritten
            | Gap::PartialRestore
            | Gap::TransactionPooler => Repair::None,
            Gap::AlterTypeRewrite
            | Gap::EnumLabelReadded
            | Gap::ColumnNameSwap
            | Gap::KeyNotNullFlip
            | Gap::CaptureDisruptedBetweenPasses
            | Gap::UnloggedSourceReset
            | Gap::RepeatableReadOldKey => Repair::RequestBackfill,
        }
    }
}

impl std::fmt::Display for Gap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "known-correctness-gaps.md#{}", self.slug())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REGISTER: &str = include_str!("../../docs/known-correctness-gaps.md");

    /// The anchors the register declares, in order.
    fn anchors() -> Vec<&'static str> {
        REGISTER
            .lines()
            .filter_map(|line| line.strip_prefix("<a id=\""))
            .filter_map(|rest| rest.split_once('"').map(|(slug, _)| slug))
            .collect()
    }

    /// Every entry has an anchor on the line right before its heading
    /// (two lines up, past the blank line), and `Gap::ALL` lists exactly
    /// those anchors in the register's order. A new entry, a closed one, or
    /// a renamed anchor fails here until this list follows.
    #[test]
    fn the_list_is_the_register() {
        let lines: Vec<&str> = REGISTER.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with("### ") {
                assert!(
                    i >= 2 && lines[i - 2].starts_with("<a id=\""),
                    "entry {line:?} has no stable anchor above it"
                );
            }
        }
        let listed: Vec<&str> = Gap::ALL.iter().map(|g| g.slug()).collect();
        assert_eq!(listed, anchors());
    }
}
