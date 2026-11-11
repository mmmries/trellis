//! Golden SQL of a `JOIN`'s own statements ([`JoinTargetPlan`]), for a
//! matrix of `JOIN` shapes (#1870); see [`crate::staging::golden_sql`]. The
//! statements of its structures' [`GroupTargetPlan`]s are pinned by
//! `group_basis::golden`; this pins the plan [`JoinTargetPlan::new`] builds
//! from a definition, which those structures' statements are rendered from
//! too.

use super::*;
use crate::defs::model::StoredStatus;
use crate::integer::IntWidth;
use crate::staging::golden_sql::Golden;
use crate::staging::group_basis::StagedParts;

fn types(columns: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    columns
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

/// The matrix: a name, a definition's text over `orders` and `items`, and
/// the visible target's column types.
/// A visible target's column types: each name and type.
type FieldTypes = &'static [(&'static str, &'static str)];

const SHAPES: &[(&str, &str, FieldTypes)] = &[
    (
        "count_star",
        "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
         GROUP BY orders.region WITHOUT SOURCE INDEX SELECT orders.region AS region, \
         COUNT(*) AS n",
        &[("n", "bigint")],
    ),
    (
        "left_sums",
        "TRANSFORM t FROM orders LEFT JOIN items ON orders.id = items.order_id \
         GROUP BY orders.region, items.sku WITHOUT SOURCE INDEX \
         SELECT orders.region AS region, items.sku AS sku, COUNT(*) AS n, \
         SUM(items.qty) AS qty, AVG(orders.total) AS avg_total, COUNT(items.qty) AS c",
        &[
            ("n", "bigint"),
            ("qty", "bigint"),
            ("avg_total", "numeric"),
            ("c", "bigint"),
        ],
    ),
    (
        "two_key_join",
        "TRANSFORM t FROM orders JOIN items ON orders.id = items.order_id \
         AND orders.region = items.sku GROUP BY items.sku WITHOUT SOURCE INDEX \
         SELECT items.sku AS sku, SUM(orders.total) AS total",
        &[("total", "numeric")],
    ),
];

fn plan_of(text: &str, field_types: &[(&str, &str)], collated: bool) -> JoinTargetPlan {
    let orders = types(&[
        ("id", ValueType::Integer(IntWidth::Int8)),
        ("region", ValueType::Text),
        ("total", ValueType::Numeric),
    ]);
    let items = types(&[
        ("order_id", ValueType::Integer(IntWidth::Int8)),
        ("sku", ValueType::Text),
        ("qty", ValueType::Integer(IntWidth::Int4)),
    ]);
    let definition = Definition {
        id: 7,
        source_version: 1,
        def: crate::defs::parse(text).expect("parses"),
        source_columns: orders,
        status: StoredStatus::Live,
        source_table: "public.orders".to_string(),
        target_table: "public.t".to_string(),
    };
    let field_types = field_types
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.to_string()))
        .collect();
    let posix = "\"pg_catalog\".\"POSIX\"".to_string();
    let icu = "\"pg_catalog\".\"und-x-icu\"".to_string();
    let text_columns = |collation: &str| -> ColumnCollations {
        if collated {
            ["region", "sku"]
                .map(|c| (c.to_string(), collation.to_string()))
                .into_iter()
                .collect()
        } else {
            ColumnCollations::new()
        }
    };
    let collations = JoinCollations {
        from: text_columns(&posix),
        joined: text_columns(&posix),
        target: text_columns(&posix),
        hidden: [
            HiddenTable::FromPartials,
            HiddenTable::JoinedPartials,
            HiddenTable::TargetPartials,
        ]
        .map(|table| (table, text_columns(&icu)))
        .into_iter()
        .collect(),
    };
    let key = |name: &str| PrimaryKeyColumn {
        name: name.to_string(),
        data_type: "bigint".to_string(),
        nullable: false,
        collation: None,
    };
    JoinTargetPlan::new(
        "trellis",
        &definition,
        &("public.items".to_string(), items),
        &field_types,
        &HashMap::new(),
        &collations,
        Vec::new(),
        [vec![key("id")], vec![key("order_id")]],
    )
    .expect("a JOIN plan")
}

fn statements(golden: &mut Golden, plan: &JoinTargetPlan) {
    golden.add("staged_keys_sql", &plan.staged_keys_sql());
    golden.add("refill_sql", &plan.refill_sql());
    golden.add("j_insert_sql", &plan.j_insert_sql());
    golden.add("j_lock_sql", &plan.j_lock_sql());
    golden.add("product_sql", &plan.product_sql());
    for (side, label) in [(&plan.from, "from"), (&plan.joined, "joined")] {
        group_basis::golden::structure(golden, label, &side.plan);
        // A build's lock of the cells that take no `J` row.
        golden.add(
            &format!("{label} lock_sql_where null_key_filter"),
            &side.plan.lock_sql_where(&side.null_key_filter()),
        );
    }
    group_basis::golden::structure(golden, "t", &plan.t);
}

#[test]
fn a_join_plan_emits_its_golden_sql() {
    for (name, text, field_types) in SHAPES {
        for collated in [false, true] {
            let mut plan = plan_of(text, field_types, collated);
            let mut golden = Golden::default();
            statements(&mut golden, &plan);
            // A page that staged sides on both, every map `NULL`
            // (`SidePlan::delta_map_free`), and a release recomputing.
            for side in [&mut plan.from, &mut plan.joined] {
                side.plan.sides.push("(1, 1)".to_string());
                let free = (1..=side.plan.shape.args.len()).collect::<Vec<_>>();
                let flags = vec![true; free.len()];
                side.plan
                    .set_staged_parts(Some(StagedParts::from_flags(free, flags)));
            }
            golden.add("free product_sql", &plan.product_sql());
            plan.from.recompute = true;
            golden.add("recompute product_sql", &plan.product_sql());
            let label = if collated { "_collated" } else { "" };
            golden.check(&format!("join_{name}{label}"));
        }
    }
}
