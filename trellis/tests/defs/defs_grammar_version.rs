//! ADR-0018: every stored definition carries the grammar version
//! its text was written under, and every read of stored text checks it.

#[path = "support/pipeline.rs"]
mod pipeline;

use pipeline::{connect, numeric_columns};

use testkit::TestCluster;
use trellis::config::Config;
use trellis::defs::grammar::CURRENT_GRAMMAR_VERSION;
use trellis::defs::{CatalogError, ParseError, install_definition};

/// Define stamps the oldest grammar version that reads the text as the
/// current one does (ADR-0018): grammar 1 for text with no quoted name and
/// no upper-case letter in a name, the current one otherwise.
#[tokio::test]
async fn definitions_are_stamped_with_the_oldest_grammar_that_reads_them_the_same() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric, \"B\" numeric)")
        .await
        .unwrap();
    for (text, target, stamped) in [
        ("TRANSFORM t FROM s SELECT a + a AS x", "public.t", 1),
        (
            "TRANSFORM \"T2\" FROM s SELECT \"B\" AS x",
            "public.T2",
            CURRENT_GRAMMAR_VERSION,
        ),
        ("TRANSFORM T3 FROM S SELECT A AS X", "public.t3", 2),
    ] {
        install_definition(&db.pool, text, &numeric_columns(&["a", "B"]), "public")
            .await
            .unwrap_or_else(|e| panic!("define {text}: {e}"));
        let v: i32 = raw
            .query_one(
                "select grammar_version from transform_definitions where target_table = $1",
                &[&target],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(v, stamped, "{text}");
    }
}

/// A definition written under a newer grammar is one this binary can't run
/// (ADR-0024 rule 3): a lookup skips it without reading its text, and leaves
/// it as it was.
#[tokio::test]
async fn a_definition_with_a_newer_grammar_is_skipped_unread_and_left_untouched() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &numeric_columns(&["a"]),
        "public",
    )
    .await
    .expect("define");
    let newer = CURRENT_GRAMMAR_VERSION + 1;
    raw.execute(
        "update transform_definitions set grammar_version = $1",
        &[&newer],
    )
    .await
    .unwrap();
    let before = raw
        .query_one(
            "select definition_text, status, definition_version, grammar_version \
             from transform_definitions",
            &[],
        )
        .await
        .unwrap();

    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Ok(None) => {}
        other => panic!("expected the definition skipped, got {other:?}"),
    }

    let after = raw
        .query_one(
            "select definition_text, status, definition_version, grammar_version \
             from transform_definitions",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.get::<_, String>(0), after.get::<_, String>(0));
    assert_eq!(before.get::<_, String>(1), after.get::<_, String>(1));
    assert_eq!(before.get::<_, i64>(2), after.get::<_, i64>(2));
    assert_eq!(after.get::<_, i32>(3), newer);
}

/// Attaching over a definition written under a newer grammar pauses that
/// one definition, naming its grammar version and the remedy, instead of
/// refusing the attach (ADR-0018, ADR-0024 rule 3).
#[tokio::test]
async fn attaching_pauses_a_definition_with_a_newer_grammar_naming_the_remedy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &numeric_columns(&["a"]),
        "public",
    )
    .await
    .expect("define");
    raw.execute(
        "update transform_definitions set grammar_version = $1",
        &[&(CURRENT_GRAMMAR_VERSION + 1)],
    )
    .await
    .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("a newer definition pauses; it doesn't refuse the attach");
    let row = raw
        .query_one(
            "select d.status, f.error, f.kind from transform_definitions d \
             join capture_failures f on f.transform_id = d.id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "paused");
    let error: String = row.get(1);
    assert!(
        error.contains("public.t")
            && error.contains(&format!("grammar version {}", CURRENT_GRAMMAR_VERSION + 1))
            && error.contains("roll forward")
            && error.contains("then RESUME it or DROP it there"),
        "{error}"
    );
    assert_eq!(row.get::<_, String>(2), "capture");
    // A second attach finds it paused and changes nothing.
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("re-attach");
    let again: String = raw
        .query_one("select error from capture_failures", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(again, error);
}

#[tokio::test]
async fn attaching_pauses_a_definition_older_than_the_window_and_names_the_fix() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &numeric_columns(&["a"]),
        "public",
    )
    .await
    .expect("define");
    raw.execute("update transform_definitions set grammar_version = 0", &[])
        .await
        .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("an older definition pauses; it doesn't refuse the attach");

    let status: String = raw
        .query_one("select status from transform_definitions", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(status, "paused");
    let error: String = raw
        .query_one("select error from capture_failures", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        error.contains("grammar version 0")
            && error.contains("1 and later")
            && error.contains("drop the transform and define it again"),
        "{error}"
    );
    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Err(CatalogError::Parse(ParseError::GrammarVersionTooOld {
            stored: 0,
            oldest: 1,
        })) => {}
        other => panic!("expected GrammarVersionTooOld, got {other:?}"),
    }
    trellis::dev::capture::load_catalog(&raw)
        .await
        .expect("capture keeps working beside the paused definition");
}

/// Stored text inside the window that this release refuses (written by an
/// earlier release that accepted it, here a bare reserved word, ADR-0022)
/// pauses that one transform at attach (`migrate`) with a `capture_failure` naming the
/// refusal and the fix. The other transforms keep running, `RESUME` refuses
/// with the same refusal, and `DROP TRANSFORM` clears it.
#[tokio::test]
async fn stored_text_this_release_refuses_pauses_that_transform_naming_the_fix() {
    use trellis::{TransformStatus, Trellis, TrellisOptions};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.rw_src (id integer primary key, val integer, \"order\" integer); \
         insert into public.rw_src values (1, 5, 10)",
    )
    .await
    .unwrap();
    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect");
    definer
        .apply("TRANSFORM healthy FROM rw_src SELECT val AS v")
        .await
        .expect("define healthy");
    definer
        .apply("TRANSFORM legacy FROM rw_src SELECT rw_src.order + 1 AS o")
        .await
        .expect("define legacy");
    definer.shutdown().await.expect("shutdown definer");
    // The text an earlier release accepted and stored.
    raw.execute(
        "update transform_definitions \
         set definition_text = 'TRANSFORM legacy FROM rw_src SELECT order + 1 AS o' \
         where target_table = 'public.legacy'",
        &[],
    )
    .await
    .unwrap();
    // The upgraded binary attaches before it runs.
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("an unreadable definition pauses; it doesn't refuse the attach");

    let running = Trellis::connect(
        config.clone(),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("one unreadable definition doesn't stop the instance");
    let legacy = running
        .status("legacy")
        .await
        .expect("status")
        .expect("legacy is registered");
    assert_eq!(legacy.status, TransformStatus::Paused);
    let failure = legacy.capture_failure.expect("a capture failure says why");
    assert!(
        failure.error.contains("public.legacy")
            && failure.error.contains("'order' is a reserved word")
            && failure
                .error
                .contains("drop the transform and define it again"),
        "{}",
        failure.error
    );

    raw.execute("insert into public.rw_src values (2, 7, 11)", &[])
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let status = running.status("healthy").await.unwrap().unwrap().status;
        if status == TransformStatus::Live {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "healthy never went live: {status:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let token = running.watermark_token().await.expect("token");
    running
        .await_converged(token, std::time::Duration::from_secs(30))
        .await
        .expect("the other transform keeps converging");
    let values: Vec<i32> = raw
        .query("select v from public.healthy order by id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(values, [5, 7]);

    let err = running
        .apply("RESUME TRANSFORM legacy")
        .await
        .expect_err("RESUME can't read the stored text");
    assert!(err.to_string().contains("reserved word"), "{err}");
    running
        .apply("DROP TRANSFORM legacy")
        .await
        .expect("a paused, unreadable transform drops");
    assert!(running.status("legacy").await.unwrap().is_none());
    running.shutdown().await.expect("shutdown");
}

/// A grammar change that refuses text Postgres also refuses needs no bump
/// (ADR-0018): a stored definition whose text the current grammar refuses
/// pauses at attach, naming the refusal, and capture keeps working for the
/// others instead of failing on it.
#[tokio::test]
async fn attaching_pauses_a_definition_whose_text_the_grammar_now_refuses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .unwrap();
    for text in [
        "TRANSFORM t FROM s SELECT a + 1 AS x",
        "TRANSFORM healthy FROM s SELECT a AS a",
    ] {
        install_definition(&db.pool, text, &numeric_columns(&["a"]), "public")
            .await
            .expect("define");
    }
    // Text an earlier release accepted: a number directly followed by a word.
    raw.execute(
        "update transform_definitions \
         set definition_text = 'TRANSFORM t FROM s SELECT a+1AS x' \
         where target_table = 'public.t'",
        &[],
    )
    .await
    .unwrap();

    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("refused text pauses; it doesn't refuse the attach");

    let statuses: Vec<(String, String)> = raw
        .query(
            "select target_table, status from transform_definitions order by id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(statuses[0], ("public.t".to_string(), "paused".to_string()));
    assert_ne!(statuses[1].1, "paused", "{statuses:?}");
    let error: String = raw
        .query_one("select error from capture_failures", &[])
        .await
        .unwrap()
        .get(0);
    assert!(
        error.contains("trailing junk after numeric literal")
            && error.contains("'AS' directly follows the number '1'")
            && error.contains("drop the transform and define it again"),
        "{error}"
    );
    match trellis::defs::catalog::definition_by_target(&db.pool, "t").await {
        Err(CatalogError::Parse(ParseError::NumericTrailingJunk { number, junk })) => {
            assert_eq!((number.as_str(), junk.as_str()), ("1", "AS"));
        }
        other => panic!("expected NumericTrailingJunk, got {other:?}"),
    }
    let catalog = trellis::dev::capture::load_catalog(&raw)
        .await
        .expect("capture keeps working beside the paused definition");
    let readers: Vec<&str> = catalog
        .definitions
        .iter()
        .map(|d| d.target.as_str())
        .collect();
    assert_eq!(readers, ["public.healthy"]);
    let trellis = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
        .await
        .expect("a second attach over the paused definition succeeds");
    let refused = trellis
        .apply("RESUME TRANSFORM t")
        .await
        .expect_err("RESUME re-reads the refused text");
    assert!(
        refused
            .to_string()
            .contains("trailing junk after numeric literal"),
        "{refused}"
    );
    trellis
        .apply("DROP TRANSFORM t")
        .await
        .expect("DROP doesn't need the text to parse");
}

/// A stored grammar-1 definition naming a column or field `null`, `false` or
/// `true` bare reads that name, as v0.1.0 did, not the literal grammar 2
/// reads (ADR-0018): its target equals Postgres's reading of the column
/// after changes and after `ALTER TRANSFORM` rewrites it. The same text
/// defined now reads the literal and is stamped grammar 2.
#[tokio::test]
async fn stored_grammar_1_text_reads_a_bare_null_false_or_true_as_a_name() {
    use trellis::{TransformStatus, Trellis, TrellisOptions};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer, \"null\" integer, \
         \"false\" boolean, \"TRUE\" integer); \
         insert into public.s values (1, 1, 10, true, 100)",
    )
    .await
    .unwrap();
    let config = Config::from_dsn(db.dsn().to_string()).unwrap();
    // (target, the text v0.1.0 stored, the same reading spelled in grammar 2,
    // Postgres's reading of each target column)
    let stored = [
        (
            "tn",
            "TRANSFORM tn FROM s SELECT null AS x",
            "TRANSFORM tn FROM s SELECT \"null\" AS x",
            "select id, \"null\"::text from public.s order by id",
            "select id, x::text from public.tn order by id",
        ),
        (
            "tc",
            "TRANSFORM tc FROM s SELECT COALESCE(null, a) AS x",
            "TRANSFORM tc FROM s SELECT COALESCE(\"null\", a) AS x",
            "select id, coalesce(\"null\", a)::text from public.s order by id",
            "select id, x::text from public.tc order by id",
        ),
        (
            "tf",
            "TRANSFORM tf FROM s SELECT false AS x",
            "TRANSFORM tf FROM s SELECT \"false\" AS x",
            "select id, \"false\"::text from public.s order by id",
            "select id, x::text from public.tf order by id",
        ),
        (
            "tu",
            "TRANSFORM tu FROM s SELECT TRUE + 1 AS x",
            "TRANSFORM tu FROM s SELECT \"TRUE\" + 1 AS x",
            "select id, (\"TRUE\" + 1)::text from public.s order by id",
            "select id, x::text from public.tu order by id",
        ),
        (
            "tl",
            "TRANSFORM tl FROM s SELECT a AS true, true + 1 AS y",
            "TRANSFORM tl FROM s SELECT a AS \"true\", \"true\" + 1 AS y",
            "select id, (a + 1)::text from public.s order by id",
            "select id, y::text from public.tl order by id",
        ),
    ];
    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect");
    for (_, _, quoted, _, _) in &stored {
        definer
            .apply(quoted)
            .await
            .unwrap_or_else(|e| panic!("{quoted}: {e}"));
    }
    definer.shutdown().await.expect("shutdown definer");
    // The text and version v0.1.0 stored.
    for (target, text, _, _, _) in &stored {
        raw.execute(
            "update transform_definitions set definition_text = $1, grammar_version = 1 \
             where target_table = $2",
            &[text, &format!("public.{target}")],
        )
        .await
        .unwrap();
    }
    trellis::migrate::migrate_with_pool(&db.pool, &config)
        .await
        .expect("attach");
    let running = Trellis::connect(
        config.clone(),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("connect");
    raw.batch_execute(
        "update public.s set \"null\" = 20, a = 2, \"false\" = true, \"TRUE\" = 200 where id = 1; \
         insert into public.s values (2, 3, 30, true, 300), (3, 4, null, false, 400)",
    )
    .await
    .unwrap();

    let rows = |q: &'static str| {
        let raw = &raw;
        async move {
            raw.query(q, &[])
                .await
                .unwrap()
                .iter()
                .map(|r| (r.get::<_, i32>(0), r.get::<_, Option<String>>(1)))
                .collect::<Vec<_>>()
        }
    };
    let live = async |running: &Trellis, name: &str| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while running.status(name).await.unwrap().unwrap().status != TransformStatus::Live {
            assert!(
                std::time::Instant::now() < deadline,
                "{name} never went live"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    };
    let converge = async |running: &Trellis| {
        let token = running.watermark_token().await.expect("token");
        running
            .await_converged(token, std::time::Duration::from_secs(60))
            .await
            .expect("converged");
    };
    for (target, _, _, _, _) in &stored {
        live(&running, target).await;
    }
    converge(&running).await;
    for (target, _, _, postgres, read) in &stored {
        let expected = rows(postgres).await;
        assert!(expected.iter().any(|(_, v)| v.is_some()), "{target}");
        assert_eq!(rows(read).await, expected, "{target}");
    }

    // `ALTER TRANSFORM` rewrites the stored text in grammar 2, quoting the
    // name, and the target keeps reading the column.
    running
        .apply("ALTER TRANSFORM tn ADD a AS b")
        .await
        .expect("alter");
    let (text, version): (String, i32) = {
        let row = raw
            .query_one(
                "select definition_text, grammar_version from transform_definitions \
                 where target_table = 'public.tn'",
                &[],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert!(text.contains("\"null\" AS x"), "{text}");
    assert_eq!(version, 2, "{text}");
    raw.execute("update public.s set \"null\" = 21 where id = 1", &[])
        .await
        .unwrap();
    converge(&running).await;
    assert_eq!(
        rows("select id, x::text from public.tn order by id").await,
        rows("select id, \"null\"::text from public.s order by id").await
    );

    // The same text defined now reads the literal, stamped grammar 2.
    for (text, target, read, expected) in [
        (
            "TRANSFORM fc FROM s SELECT COALESCE(null, a) AS x",
            "public.fc",
            "select id, x::text from public.fc order by id",
            "select id, a::text from public.s order by id",
        ),
        (
            "TRANSFORM ff FROM s SELECT false AS x",
            "public.ff",
            "select id, x::text from public.ff order by id",
            "select id, false::text from public.s order by id",
        ),
    ] {
        running
            .apply(text)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
        let v: i32 = raw
            .query_one(
                "select grammar_version from transform_definitions where target_table = $1",
                &[&target],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(v, 2, "{text}");
        live(&running, target.trim_start_matches("public.")).await;
        converge(&running).await;
        assert_eq!(rows(read).await, rows(expected).await, "{text}");
    }
    running.shutdown().await.expect("shutdown");
}
