//! Golden SQL of the statements that match a batch of keys through the key's
//! index, one arm per `NULL` pattern ([`super::keyset`]), over a
//! single-column, a two-column and a three-column nullable key.

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::staging::golden_sql::Golden;

fn key(columns: &[(&str, &str)]) -> Vec<PrimaryKeyColumn> {
    columns
        .iter()
        .map(|(name, data_type)| PrimaryKeyColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
            collation: None,
        })
        .collect()
}

#[test]
fn keyset_statements_are_unchanged() {
    let shapes = [
        (
            key(&[("id", "integer")]),
            vec![vec![Some("1")], vec![Some("2")], vec![None]],
        ),
        (
            key(&[("warehouse", "integer"), ("sku", "text")]),
            vec![
                vec![Some("1"), Some("a")],
                vec![None, Some("b")],
                vec![Some("2"), None],
                vec![None, None],
            ],
        ),
        (
            key(&[("a", "integer"), ("b", "text"), ("c", "bigint")]),
            vec![
                vec![Some("1"), Some("x"), Some("1")],
                vec![None, Some("y"), Some("2")],
                vec![Some("3"), None, None],
            ],
        ),
    ];
    let mut golden = Golden::default();
    let columns = ["id".to_string(), "total".to_string()];
    for (pk, parts) in &shapes {
        let n = pk.len();
        let keys: Vec<String> = parts.iter().map(|p| ddl::encode_pk_key(pk, p)).collect();
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        let live = super::apply::live_rows_query("public.src", pk, &columns, &keys).expect("keys");
        golden.add(&format!("{n}: live rows"), &live.docs);
        golden.add(
            &format!("{n}: new images"),
            &super::target_mutations::new_images_query("public.tgt", &columns, pk, keys.clone())
                .expect("keys")
                .sql,
        );
        let mut patterns: Vec<Vec<bool>> = parts
            .iter()
            .map(|p| p.iter().map(Option::is_none).collect())
            .collect();
        patterns.dedup();
        for pattern in &patterns {
            let p: String = pattern.iter().map(|&n| if n { 'N' } else { 'v' }).collect();
            let pick = super::build::one_to_one::sweep_pick(pk, pattern);
            golden.add(&format!("{n} {p}: sweep source"), &pick.src_from);
            golden.add(&format!("{n} {p}: sweep target"), &pick.target_where);
            let lock = super::apply::lock_statement("tgt", pk, pattern, "cols");
            golden.add(&format!("{n} {p}: lock"), &lock);
            let delete = super::apply::delete_statement("tgt", pk, pattern, "ret");
            golden.add(&format!("{n} {p}: delete"), &delete);
        }
    }
    golden.check("keyset");
}
