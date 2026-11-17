//! The record of every [`ErrorId`](super::ErrorId) assigned so far, checked
//! against the `error_id()` maps in the error enums.

/// Every code assigned, in order, with the variant that reports it. A new
/// error variant takes the next number and is appended here. An entry is
/// never removed or renumbered: a code is public API. Renaming a variant
/// renames its entry and keeps the code. A code no variant reports any more
/// stays as `Tnnnn retired: <why>`, and is never reported again.
const ASSIGNED: &[&str] = &[
    "T0001 ParseError::UnexpectedChar",
    "T0002 ParseError::UnterminatedString",
    "T0003 ParseError::UnexpectedToken",
    "T0004 ParseError::UnexpectedEof",
    "T0005 ParseError::UnsupportedKeySpace",
    "T0006 ParseError::UnsupportedOperator",
    "T0007 ParseError::UnsupportedFunction",
    "T0008 ParseError::FunctionArityMismatch",
    "T0009 ParseError::NonImmutableConstruct",
    "T0010 ParseError::UnsupportedPredicate",
    "T0011 ParseError::AtLeastOneArgumentRequired",
    "T0012 ParseError::TooManyQualifiedNameParts",
    "T0013 ParseError::TooManyColumnNameParts",
    "T0014 ParseError::GroupByKeyNotFromSource",
    "T0015 ParseError::TableAlias",
    "T0016 ParseError::AliasedTableName",
    "T0017 ParseError::UnsupportedCastTarget",
    "T0018 ParseError::PrefixMinusChainTooLong",
    "T0019 ParseError::ReservedWord",
    "T0020 ParseError::SplitOperator",
    "T0021 ParseError::DoubleMinus",
    "T0022 ParseError::NumericTrailingJunk",
    "T0023 ParseError::UnsupportedNumericForm",
    "T0024 ParseError::NonAssociativeComparison",
    "T0025 ParseError::UnsupportedIsForm",
    "T0026 ParseError::NotChainTooLong",
    "T0027 ParseError::SimpleCaseUnsupported",
    "T0028 SharedCondition::UnsupportedLiteralType",
    "T0029 ParseError::UnsupportedCastOperator",
    "T0030 ParseError::ExpressionTooDeep",
    "T0031 ParseError::MalformedDefinitionAddress",
    "T0032 ParseError::UnterminatedQuotedIdentifier",
    "T0033 ParseError::ZeroLengthQuotedIdentifier",
    "T0034 ParseError::UnicodeEscapeIdentifier",
    "T0035 ParseError::UnsupportedQuotedIdentifier",
    "T0036 ParseError::NameTooLong",
    "T0037 ParseError::ReservedTableName",
    "T0038 ParseError::GrammarVersionTooOld",
    "T0039 ParseError::GrammarVersionTooNew",
    "T0040 ValidationError::DuplicateFieldName",
    "T0041 ValidationError::ReservedFieldName",
    "T0042 ValidationError::ReservedGroupByColumnName",
    "T0043 ValidationError::FieldNamedAfterPrimaryKey",
    "T0044 ValidationError::UnresolvedColumn",
    "T0045 ValidationError::Cycle",
    "T0046 ValidationError::FieldExpansionTooLarge",
    "T0047 ValidationError::TableCycle",
    "T0048 ValidationError::TargetEqualsSource",
    "T0049 ValidationError::JoinNeedsBothTables",
    "T0050 ValidationError::JoinSelf",
    "T0051 ValidationError::JoinTooManyTables",
    "T0052 ValidationError::JoinUnknownTable",
    "T0053 ValidationError::JoinUnqualifiedColumn",
    "T0054 ValidationError::JoinUnresolvedColumn",
    "T0055 ValidationError::JoinOnSameSide",
    "T0056 ValidationError::JoinMixedSideArgument",
    "T0057 ValidationError::JoinOnRepeatedPair",
    "T0058 ValidationError::JoinOnRepeatedColumn",
    "T0059 ValidationError::IndexKeyTooWide",
    "T0060 ValidationError::TableTooWide",
    "T0061 ValidationError::JoinKeyTypeMismatch",
    "T0062 ValidationError::JoinUnsupportedKeyType",
    "T0063 ValidationError::JoinNondeterministicCollation",
    "T0064 ValidationError::JoinFieldShape",
    "T0065 ValidationError::OneToOneJoinAggregate",
    "T0066 ValidationError::OneToOneJoinKeyNotUnique",
    "T0067 ValidationError::OneToOneJoinUnsupported",
    "T0068 ValidationError::StarJoinTooManyTables",
    "T0069 ValidationError::JoinOnReadsAnotherJoinedTable",
    "T0070 ValidationError::DeferredAggregate",
    "T0071 ValidationError::UnsupportedAggregateArgument",
    "T0072 ValidationError::UnsupportedExtremeArgument",
    "T0073 ValidationError::ExtremeOfExpression",
    "T0074 ValidationError::ChainedExtreme",
    "T0075 ValidationError::ExtremesWithoutSourceIndex",
    "T0076 ValidationError::ReservedTargetSuffix",
    "T0077 ValidationError::TargetNameTooLong",
    "T0078 ValidationError::TypeMismatch",
    "T0079 ValidationError::MixedFloatExactComparison",
    "T0080 ValidationError::MixedTemporalComparison",
    "T0081 ValidationError::TextOrderingComparison",
    "T0082 ValidationError::ComparisonTypeMismatch",
    "T0083 ValidationError::NullIfFirstArgumentCast",
    "T0084 ValidationError::CaseConditionNotBoolean",
    "T0085 ValidationError::CaseResultTypeMismatch",
    "T0086 ValidationError::UntypedNull",
    "T0087 ValidationError::ConstantDivisorZero",
    "T0088 ValidationError::IntegerDivision",
    "T0089 ValidationError::FloatModulo",
    "T0090 ValidationError::TemporalOperand",
    "T0091 ValidationError::TemporalLiteral",
    "T0092 ValidationError::UnsupportedCast",
    "T0093 ValidationError::NumericFunctionArgument",
    "T0094 ValidationError::ScaleNotIntegerLiteral",
    "T0095 ValidationError::FunctionArgTypeMismatch",
    "T0096 ValidationError::NonLiteralRegexPattern",
    "T0097 ValidationError::InvalidTypedLiteral",
    "T0098 retired: ValidationError::UnsupportedLiteralType reports T0028",
    "T0099 ValidationError::UnresolvedGroupByColumn",
    "T0100 ValidationError::UnsupportedGroupByKeyType",
    "T0101 ValidationError::GroupByKeysShareTargetColumn",
    "T0102 ValidationError::UngroupedColumnReference",
    "T0103 ValidationError::AggregateOfAggregateField",
    "T0104 ValidationError::AmbiguousColumnReference",
    "T0105 ValidationError::QualifiedColumnNotFromSource",
    "T0106 ValidationError::GroupingColumnFieldMustBePassthrough",
    "T0107 ValidationError::CalculatedFieldShadowsSourceColumn",
    "T0108 ValidationError::NondeterministicKeyCollation",
    "T0109 ValidationError::NondeterministicCollationTextFunction",
    "T0110 ValidationError::QualifiedSourceTableNotFound",
    "T0111 ValidationError::QualifiedTargetTableNotFound",
    "T0112 ValidationError::StringAggSeparatorNotLiteral",
    "T0113 DdlError::NoPrimaryKey",
    "T0114 DdlError::DeferrablePrimaryKey",
    "T0115 DdlError::KeyOnTrellisTarget",
    "T0116 DdlError::KeyNotBacked",
    "T0117 DdlError::KeyDiffersFromReaders",
    "T0118 DdlError::RecordedKeyMissing",
    "T0119 DdlError::NoPrimaryKeyToRecord",
    "T0120 DdlError::MalformedCompositeKey",
    "T0121 DdlError::UnsupportedPrimaryKeyType",
    "T0122 DdlError::UnroutedAggregate",
    "T0123 DdlError::Db",
    "T0124 CatalogError::Db",
    "T0125 CatalogError::UnknownValueType",
    "T0126 SharedCondition::SourceTableNotFound",
    "T0127 CatalogError::InvalidRegexPattern",
    "T0128 CatalogError::TableNotAccessible",
    "T0129 CatalogError::KeyRepairBlocked",
    "T0130 CatalogError::KeyConstraintBlocked",
    "T0131 CatalogError::KeyColumnTaken",
    "T0132 CatalogError::SourceKeyChanged",
    "T0133 CatalogError::VirtualGeneratedColumn",
    "T0134 CatalogError::SourceIndexMissing",
    "T0135 CatalogError::ExtremeIndexMissing",
    "T0136 CatalogError::ExtremeSourceNotReadable",
    "T0137 CatalogError::LedgerTableMissing",
    "T0138 CatalogError::TargetTableMissing",
    "T0139 CatalogError::TargetInCatalogSchema",
    "T0140 CatalogError::TargetTableSuffixCollision",
    "T0141 CatalogError::TargetTableExists",
    "T0142 CatalogError::TargetNameHasReaders",
    "T0143 CatalogError::TargetNotAdoptable",
    "T0144 CatalogError::SourceNotChangeKeyed",
    "T0145 CatalogError::PartitionedSource",
    "T0146 CatalogError::RowSecurityApplies",
    "T0147 CatalogError::Subscribed",
    "T0148 SharedCondition::TransformNotFound",
    "T0149 CatalogError::NameCaseMismatch",
    "T0150 CatalogError::TransformNotPaused",
    "T0151 CatalogError::AboveRunningRelease",
    "T0152 CatalogError::CannotRun",
    "T0153 CatalogError::DependentsBlockDrop",
    "T0154 CatalogError::TransformNotLive",
    "T0155 CatalogError::AlterFieldNotFound",
    "T0156 CatalogError::AlterFieldAlreadyExists",
    "T0157 CatalogError::UnsupportedAlter",
    "T0158 ColumnPauseLockTimeout",
    "T0159 BackfillError::Db",
    "T0160 BackfillError::Unsupported",
    "T0161 BackfillError::Superseded",
    "T0162 ApplyError::Db",
    "T0163 ApplyError::ClaimLost",
    "T0164 ApplyError::PageAlreadyCommitted",
    "T0165 ApplyError::VersionFenceMiss",
    "T0166 ApplyError::LedgerEntryCollected",
    "T0167 ApplyError::JoinKeyMoved",
    "T0168 ApplyError::JoinOneToOneHalt",
    "T0169 ApplyError::HopBoundExceeded",
    "T0170 ApplyError::GroupBasisUnsupported",
    "T0171 ApplyError::GroupLockGap",
    "T0172 ApplyError::SourceTableDropped",
    "T0173 ApplyError::ColumnNotPaused",
    "T0174 ApplyError::DefinitionNotLive",
    "T0175 ApplyError::ColumnAwaitingCapture",
    "T0176 retired: ApplyError::TransformNotFound reports T0148",
    "T0177 ApplyError::TransformNotPaused",
    "T0178 ApplyError::KeyNotHeld",
    "T0179 ApplyError::ReleaseLockTimeout",
    "T0180 retired: ApplyError::ColumnPauseLockTimeout reports T0158",
    "T0181 retired: ApplyError::ResumeRefused reports its reason's code",
    "T0182 ApplyError::TruncateWithoutLsn",
    "T0183 StagingError::InvalidRingSlot",
    "T0184 StagingError::ProducerAlreadyRunning",
    "T0185 StagingError::RingFull",
    "T0186 StagingError::SealGateBlocked",
    "T0187 StagingError::Raced",
    "T0188 StagingError::UnfencedSealedSegment",
    "T0189 StagingError::ConvergenceTimeout",
    "T0190 StagingError::Db",
    "T0191 retired: SelfCheckError::TargetNotFound reports T0148",
    "T0192 SelfCheckError::UnsupportedKeySpace",
    "T0193 SelfCheckError::InvalidScope",
    "T0194 SelfCheckError::UnsupportedExpr",
    "T0195 SelfCheckError::PageTimedOut",
    "T0196 SelfCheckError::UnreadableJob",
    "T0197 SelfCheckError::Db",
    "T0198 IntakeError::Db",
    "T0199 IntakeError::InvalidTableName",
    "T0200 IntakeError::DottedIdentifierComponent",
    "T0201 IntakeError::NoIdentityKey",
    "T0202 ClientError::HeartbeatNotUnderReclaimTtl",
    "T0203 ClientError::Spawn",
    "T0204 ClientError::ThreadExitedBeforeReady",
    "T0205 ClientError::ThreadPanicked",
    "T0206 ClientError::Db",
    "T0207 TrellisError::MigrationsPending",
    "T0208 TrellisError::Db",
    "T0209 retired: TrellisError::SourceTableNotFound reports T0126",
    "T0210 TrellisError::TableNotCaptured",
    "T0211 TrellisError::AmbiguousSourceTable",
    "T0212 TrellisError::BlockingSpawn",
    "T0213 TrellisError::BlockingThreadExitedBeforeReady",
    "T0214 TrellisError::BlockingThreadGone",
    "T0215 TrellisError::CancelledByShutdown",
    "T0216 TrellisError::CalledFromAsyncContext",
    "T0217 retired: TrellisError::TransformNotFound reports T0148",
    "T0218 TrellisError::ColumnNotFound",
    "T0219 TrellisError::CallTimeout",
    "T0220 OtherVersionLive",
    "T0221 TrellisError::TargetSelect",
    "T0222 Error::Config",
    "T0223 Error::BuildPool",
    "T0224 Error::Pool",
    "T0225 Error::Connect",
    "T0226 Error::Migrate",
    "T0227 Error::IncompatibleInstance",
    "T0228 retired: Error::OtherVersionLive reports T0220",
    "T0229 Error::BelowCatalogFloor",
    "T0230 Error::MigrationHistory",
    "T0231 Error::RestoreDefinition",
    "T0232 Error::SchemaDump",
    "T0233 ValidationError::ConflictingCollations",
    "T0234 ValidationError::PredicateNotBoolean",
    "T0235 ValidationError::AggregateInPredicate",
    "T0236 ValidationError::PredicateColumnNotFound",
    "T0237 ValidationError::PredicateNotTotal",
    "T0238 ValidationError::FilterNotYetSupported",
    "T0239 ValidationError::FilteredExtreme",
];

/// Every `.rs` file under `src/` but this one, as (path, text): read at test
/// time, so an error enum in a new file is checked without being listed.
fn sources() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).expect("read src") {
            let path = entry.expect("a src entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs")
                && !path.ends_with("error_code/assigned.rs")
            {
                let text = std::fs::read_to_string(&path).expect("read a source file");
                out.push((path.display().to_string(), text));
            }
        }
    }
    let mut out = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut out,
    );
    out
}

/// Each `Enum::Variant ... => ErrorId(n),` arm in `source`, as `Tnnnn
/// Enum::Variant`. Every `ErrorId(<number>)` in `source` must be such an
/// arm, naming one variant, so no code is assigned where this can't see it.
fn arms(path: &str, source: &str) -> Vec<(u16, String)> {
    let mut found = Vec::new();
    for line in source.lines() {
        let line = line.trim();
        let literals = line
            .match_indices("ErrorId(")
            .filter(|(at, _)| {
                line[at + "ErrorId(".len()..].starts_with(|c: char| c.is_ascii_digit())
            })
            .count();
        if literals == 0 {
            continue;
        }
        let arm = line.split_once(" => ErrorId(").and_then(|(pattern, rest)| {
            let (number, _) = rest.split_once(')')?;
            Some((pattern, number.parse::<u16>().ok()?))
        });
        let Some((pattern, number)) = arm.filter(|(pattern, _)| {
            literals == 1 && !pattern.contains('|') && !pattern.starts_with('_')
        }) else {
            panic!(
                "{path}: `{line}` assigns a code outside a one-variant `Variant => ErrorId(n),` \
                 arm, which this test can't check"
            );
        };
        let variant: String = pattern
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
            .collect();
        found.push((number, format!("T{number:04} {variant}")));
    }
    found
}

/// Codes are unique across the enums, every recorded code is still reported
/// by the variant recorded for it, unless retired, and every code reported is
/// recorded (so a retired code is never reported again).
#[test]
fn every_assigned_code_is_reported_once_by_its_variant() {
    let mut found: Vec<(u16, String)> = sources()
        .iter()
        .flat_map(|(path, source)| arms(path, source))
        .collect();
    found.sort();
    for pair in found.windows(2) {
        assert_ne!(
            pair[0].0, pair[1].0,
            "{} and {} share a code",
            pair[0].1, pair[1].1
        );
    }
    for (index, entry) in ASSIGNED.iter().enumerate() {
        assert!(
            entry.starts_with(&format!("T{:04} ", index + 1)),
            "ASSIGNED is out of order at {entry}: codes are sequential"
        );
        if entry.contains(" retired: ") {
            continue;
        }
        assert!(
            found.iter().any(|(_, arm)| arm == entry),
            "{entry} is no longer reported: a code is never removed or moved to another \
             variant (a renamed variant renames its entry)"
        );
    }
    for (_, arm) in &found {
        assert!(
            ASSIGNED.contains(&arm.as_str()),
            "{arm} is not in ASSIGNED: give a new variant the next code and record it there"
        );
    }
}
