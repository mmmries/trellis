//! Golden SQL of every statement generator of [`GroupTargetPlan`], for a
//! matrix of group shapes (#1870): a refactor of how the statements are
//! assembled must leave their text unchanged, byte for byte, since the
//! statements are plan-sensitive. See [`crate::staging::golden_sql`].

use super::*;
use crate::staging::golden_sql::Golden;

fn columns() -> HashMap<String, ValueType> {
    [
        ("g", ValueType::Integer(IntWidth::Int4)),
        ("h", ValueType::Text),
        ("a", ValueType::Integer(IntWidth::Int4)),
        ("b", ValueType::Integer(IntWidth::Int8)),
        ("m", ValueType::Numeric),
        ("f", ValueType::Float(crate::float::FloatWidth::Float8)),
    ]
    .into_iter()
    .map(|(name, ty)| (name.to_string(), ty))
    .collect()
}

/// The matrix: a name and a definition's text over [`columns`].
const SHAPES: &[(&str, &str)] = &[
    (
        "count_star",
        "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
    ),
    (
        "count_column",
        "TRANSFORM t FROM s GROUP BY g SELECT COUNT(a) AS n",
    ),
    (
        "sum_integer",
        "TRANSFORM t FROM s GROUP BY g SELECT SUM(a) AS s",
    ),
    (
        "sum_numeric",
        "TRANSFORM t FROM s GROUP BY g SELECT SUM(m) AS s",
    ),
    (
        "min_max",
        "TRANSFORM t FROM s GROUP BY g, b SELECT MIN(a) AS lo, MAX(a) AS hi",
    ),
    (
        "avg",
        "TRANSFORM t FROM s GROUP BY g SELECT AVG(m) AS v, AVG(a) AS w",
    ),
    (
        "mixed",
        "TRANSFORM t FROM s GROUP BY g, h SELECT g AS g, h AS h, COUNT(*) AS n, \
         SUM(a + 1) AS s, AVG(m) AS v, COUNT(b) AS c, MAX(f) AS hi, SUM(m) AS sm, \
         MIN(b) AS lo",
    ),
    (
        "text_key",
        "TRANSFORM t FROM s GROUP BY h SELECT COUNT(*) AS n, SUM(b) AS s",
    ),
    // Past `MAX_NULL_PATTERN_COLUMNS` group columns, a lookup's `NULL`
    // patterns give way to one `is not distinct from` branch.
    (
        "five_keys",
        "TRANSFORM t FROM s GROUP BY g, h, a, b, f SELECT COUNT(*) AS n, SUM(m) AS s",
    ),
];

/// `text`'s shape, each `numeric` sum's reference scale set to `scale`.
fn shape_of(text: &str, scale: Option<i32>) -> GroupShape {
    let def = crate::defs::parse(text).expect("parses");
    let mut shape = route(&def, &columns()).expect("admitted");
    let scales: Vec<Option<i32>> = shape
        .args
        .iter()
        .map(|arg| if arg.numeric { scale } else { None })
        .collect();
    shape.set_scales(&scales);
    shape
}

/// The group identity of a target over `shape`: its `GROUP BY` columns,
/// nullable, as an aggregate target's are.
fn identity(shape: &GroupShape) -> Vec<PrimaryKeyColumn> {
    shape
        .group_cols
        .iter()
        .zip(&shape.group_types)
        .map(|(name, data_type)| PrimaryKeyColumn {
            name: name.clone(),
            data_type: data_type.clone(),
            nullable: true,
            collation: None,
        })
        .collect()
}

/// The source's row identity: a key column that can't be `NULL` and one
/// that can (another aggregate's group column, when the source is its
/// target), so a key lookup renders both of its match forms.
fn source_key() -> Vec<PrimaryKeyColumn> {
    [("id", "integer", false), ("k", "text", true)]
        .into_iter()
        .map(|(name, data_type, nullable)| PrimaryKeyColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            collation: None,
        })
        .collect()
}

/// Every numeric argument's map is free ([`StagedParts::free`]).
fn all_free(shape: &GroupShape) -> StagedParts {
    StagedParts {
        free: (1..=shape.args.len())
            .filter(|i| shape.args[i - 1].numeric)
            .collect(),
    }
}

fn image_columns() -> Vec<String> {
    vec!["g".to_string(), "n".to_string()]
}

/// The statements of a plain aggregate's plan.
fn aggregate(golden: &mut Golden, plan: &GroupTargetPlan) {
    let images = image_columns();
    golden.add(
        "hidden_table_ddl",
        &hidden_table_ddl("trellis", 7, &plan.shape),
    );
    golden.add(
        "target_table_ddl",
        &target_table_ddl("public.t", &plan.shape),
    );
    golden.add(
        "scale_comments_sql",
        &scale_comments_sql("trellis.agg_7", &plan.shape.numeric_scales()),
    );
    golden.add("staged_ddl", &plan.staged_ddl());
    golden.add("working_ddl", &plan.working_ddl());
    golden.add("name_groups_sql", &plan.name_groups_sql());
    golden.add("stage_sql", &plan.stage_sql());
    golden.add("lock_sql", &plan.lock_sql());
    golden.add("apply_sql", &plan.apply_sql(None));
    golden.add("apply_sql images", &plan.apply_sql(Some(&images)));
    golden.add("probe_rows_sql", &plan.probe_rows_sql());
    if !plan.shape.extremes().is_empty() {
        for x in plan.shape.extremes() {
            golden.add(
                &format!("extreme_probe_sql {}", x.named("")),
                &plan.extreme_probe_sql(&x),
            );
        }
        golden.add("probe_sql", &plan.probe_sql(None));
        golden.add("probe_sql images", &plan.probe_sql(Some(&images)));
    }
    golden.add("recompute_sql", &plan.recompute_sql(None, None));
    golden.add(
        "recompute_sql images node",
        &plan.recompute_sql(Some(&images), Some("\"public\".\"s_1\"")),
    );
    golden.add("rebuild_groups_sql", &plan.rebuild_groups_sql(None));
    for read_node in [false, true] {
        let scope = RebuildScope {
            hidden: "h.\"g\" > 0".to_string(),
            target: "t.\"g\" > 0".to_string(),
            images: "s.\"g\" > 0".to_string(),
            node: Some("\"public\".\"s_1\"".to_string()),
            read_node,
        };
        golden.add(
            &format!("rebuild_groups_sql scope read_node={read_node}"),
            &plan.rebuild_groups_sql(Some(&scope)),
        );
    }
    golden.add(
        "rebuild_sql source",
        &plan.rebuild_sql(None, &RebuildRead::Source),
    );
    golden.add(
        "rebuild_sql node images",
        &plan.rebuild_sql(
            Some(&images),
            &RebuildRead::Node("\"public\".\"s_1\"".to_string()),
        ),
    );
    golden.add(
        "rebuild_sql probe",
        &plan.rebuild_sql(None, &RebuildRead::Probe),
    );
}

#[test]
fn an_aggregate_plan_emits_its_golden_sql() {
    for (name, text) in SHAPES {
        for scale in [None, Some(2)] {
            let shape = shape_of(text, scale);
            if scale.is_some() && !shape.args.iter().any(|a| a.numeric) {
                continue;
            }
            let plan = GroupTargetPlan::new(
                "trellis",
                7,
                "public.t",
                "public.s",
                source_key(),
                identity(&shape),
                shape.clone(),
            );
            let mut golden = Golden::default();
            aggregate(&mut golden, &plan);
            if shape.args.iter().any(|a| a.numeric) {
                // A page whose every staged map is `NULL`.
                plan.set_staged_parts(Some(all_free(&shape)));
                golden.add("free apply_sql", &plan.apply_sql(None));
            }
            let suffix = scale.map_or(String::new(), |s| format!("_scale{s}"));
            golden.check(&format!("aggregate_{name}{suffix}"));
        }
    }
}

/// A `JOIN` structure's plan over `shape`, its group columns under
/// `collations` (source, hidden, target) where given.
fn join_plan(
    shape: &GroupShape,
    writer: Writer,
    collations: Option<[Vec<Option<String>>; 3]>,
) -> GroupTargetPlan {
    let source = match writer {
        Writer::JoinPartials { .. } => Some(("public.s", "trellis_join_rp_7", source_key())),
        _ => None,
    };
    let identity = match writer {
        Writer::JoinTarget => identity(shape),
        _ => Vec::new(),
    };
    let floor_key = match writer {
        Writer::JoinPartials { .. } => Some("\"trellis\".\"join_7_p\"".to_string()),
        _ => None,
    };
    let plan = GroupTargetPlan::for_join(
        "public.t",
        "\"trellis\".\"join_7_p\"".to_string(),
        "trellis_join_p_7",
        source,
        identity,
        shape.clone(),
        writer,
        floor_key,
        String::new(),
    )
    .with_ring("trellis");
    match collations {
        Some([source, hidden, target]) => plan.with_collations(GroupCollations {
            source,
            hidden,
            target,
        }),
        None => plan,
    }
}

/// The statements of one `JOIN` side's partials (`P` or `M`).
fn join_partials(golden: &mut Golden, plan: &GroupTargetPlan) {
    let images = image_columns();
    golden.add("staged_ddl", &plan.staged_ddl());
    golden.add("working_ddl", &plan.working_ddl());
    golden.add("deltas_ddl", &plan.deltas_ddl().expect("a side has deltas"));
    golden.add("name_groups_sql", &plan.name_groups_sql());
    golden.add("stage_sql", &plan.stage_sql());
    golden.add("lock_sql", &plan.lock_sql());
    golden.add("apply_sql", &plan.apply_sql(None));
    golden.add("probe_rows_sql", &plan.probe_rows_sql());
    golden.add("recompute_sql", &plan.recompute_sql(None, None));
    golden.add(
        "recompute_sql images node",
        &plan.recompute_sql(Some(&images), Some("\"public\".\"s_1\"")),
    );
    for strict in [false, true] {
        golden.add(
            &format!("join_group_sql strict={strict}"),
            &plan.join_group_sql("\"public\".\"s\"", "pg_temp.\"g\"", strict),
        );
    }
    let [fill, write] = plan.join_build_sql("pg_temp.\"g\"", "pg_temp.\"w\"", "pg_temp.\"d\"");
    golden.add("join_build_sql fill", &fill);
    golden.add("join_build_sql write", &write);
}

/// The statements of a `JOIN`'s target groups (`T`).
fn join_target(golden: &mut Golden, plan: &GroupTargetPlan) {
    let images = image_columns();
    golden.add("staged_ddl", &plan.staged_ddl());
    golden.add("working_ddl", &plan.working_ddl());
    golden.add("lock_sql", &plan.lock_sql());
    golden.add("apply_sql", &plan.apply_sql(None));
    golden.add("apply_sql images", &plan.apply_sql(Some(&images)));
    golden.add("apply_hidden_sql", &plan.apply_hidden_sql());
    golden.add("match_hidden_build_sql", &plan.match_hidden_build_sql());
    golden.add("update_hidden_build_sql", &plan.update_hidden_build_sql());
    golden.add("target_groups_sql", &plan.target_groups_sql());
    golden.add("compare_held_sql", &plan.compare_held_sql(None));
    golden.add(
        "compare_held_sql images",
        &plan.compare_held_sql(Some(&images)),
    );
}

/// A `JOIN`'s structures over the count, sum and numeric-sum shapes: a
/// structure's shape has no fields or extremes, its arguments only.
const JOIN_SHAPES: &[(&str, &str)] = &[
    (
        "count_star",
        "TRANSFORM t FROM s GROUP BY g SELECT COUNT(*) AS n",
    ),
    (
        "sums",
        "TRANSFORM t FROM s GROUP BY g, h SELECT COUNT(a) AS c, SUM(b) AS s, \
         SUM(m) AS sm, AVG(m) AS v",
    ),
    (
        "five_keys",
        "TRANSFORM t FROM s GROUP BY g, h, a, b, f SELECT SUM(m) AS s",
    ),
];

#[test]
fn a_join_structure_plan_emits_its_golden_sql() {
    let icu = Some("\"pg_catalog\".\"und-x-icu\"".to_string());
    let posix = Some("\"pg_catalog\".\"POSIX\"".to_string());
    for (name, text) in JOIN_SHAPES {
        // `T` computes the fields; a side's partials hold its arguments
        // only.
        let full = shape_of(text, Some(2));
        let mut shape = full.clone();
        shape.aggs.clear();
        shape.fields.clear();
        // Every text column re-collated between the populations.
        let moved = |c: &Option<String>| -> Vec<Option<String>> {
            shape
                .group_cols
                .iter()
                .map(|col| if col == "h" { c.clone() } else { None })
                .collect()
        };
        for collated in [false, true] {
            let collations = collated.then(|| [moved(&posix), moved(&icu), moved(&posix)]);
            let label = if collated { "_collated" } else { "" };
            let deltas = Writer::JoinPartials {
                deltas: "pg_temp.\"trellis_join_dp_7\"".to_string(),
            };
            let side = join_plan(&shape, deltas, collations.clone());
            let mut golden = Golden::default();
            join_partials(&mut golden, &side);
            side.set_staged_parts(Some(all_free(&shape)));
            golden.add("free apply_sql", &side.apply_sql(None));
            golden.check(&format!("join_partials_{name}{label}"));

            let t = join_plan(&full, Writer::JoinTarget, collations);
            let mut golden = Golden::default();
            join_target(&mut golden, &t);
            t.set_staged_parts(Some(all_free(&shape)));
            golden.add("free apply_sql", &t.apply_sql(None));
            golden.add("free match_hidden_build_sql", &t.match_hidden_build_sql());
            golden.add("free update_hidden_build_sql", &t.update_hidden_build_sql());
            golden.check(&format!("join_target_{name}{label}"));
        }
    }
}

/// A few statements of a `JOIN` structure's plan, labelled `label`, for
/// `join::golden`'s plans built from a definition.
pub(in crate::staging) fn structure(golden: &mut Golden, label: &str, plan: &GroupTargetPlan) {
    if let Writer::JoinPartials { .. } = plan.writer {
        golden.add(&format!("{label} stage_sql"), &plan.stage_sql());
        golden.add(
            &format!("{label} recompute_sql"),
            &plan.recompute_sql(None, None),
        );
    } else {
        golden.add(
            &format!("{label} compare_held_sql"),
            &plan.compare_held_sql(None),
        );
    }
    golden.add(&format!("{label} apply_sql"), &plan.apply_sql(None));
}
