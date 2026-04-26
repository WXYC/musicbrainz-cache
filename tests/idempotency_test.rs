//! Per-step idempotency test for the MusicBrainz cache pipeline.
//!
//! Each pipeline step (Schema, Import, Filter, Indexes, Analyze) must be
//! safe to run twice in a row without changing observable database state
//! (table row counts). This is the safety net for `--resume`: if a run
//! crashes between PG-commit and state-save, the next `--resume` will
//! re-execute the step, and that re-execution must not duplicate data
//! or fail.
//!
//! See CLAUDE.md "Resume safety" for the discipline this test enforces.
//!
//! Gated on `TEST_DATABASE_URL`: returns early when unset.

use std::collections::BTreeMap;
use std::path::PathBuf;

use musicbrainz_cache::{filter, import, schema};

fn test_db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbdump")
}

fn library_db_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/library.db")
}

/// Take a snapshot of every `mb_*` table's row count.
///
/// Returned as a `BTreeMap` for deterministic ordering in assertion diffs.
fn take_snapshot(client: &mut postgres::Client) -> BTreeMap<String, i64> {
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' \
             AND tablename LIKE 'mb_%' ORDER BY tablename",
            &[],
        )
        .unwrap();
    let mut snapshot = BTreeMap::new();
    for row in rows {
        let table: String = row.get(0);
        let count: i64 = client
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .unwrap()
            .get(0);
        snapshot.insert(table, count);
    }
    snapshot
}

/// Take a snapshot of every `idx_mb_*` index's existence.
fn index_snapshot(client: &mut postgres::Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' \
             AND indexname LIKE 'idx_mb_%' ORDER BY indexname",
            &[],
        )
        .unwrap();
    rows.iter().map(|r| r.get::<_, String>(0)).collect()
}

/// Run a single pipeline step. Mirrors the closures inside `main.rs::run_step`
/// but invokes them directly so the test can re-execute one step at a time.
fn run_step(
    step_name: &str,
    client: &mut postgres::Client,
    fixtures: &std::path::Path,
    library_db: &std::path::Path,
) -> anyhow::Result<()> {
    match step_name {
        "schema" => schema::apply_schema(client),
        "import" => import::import_all(client, fixtures).map(|_| ()),
        "filter" => {
            let library_artists = filter::load_library_artists(library_db)?;
            let matching = filter::find_matching_artist_ids(client, &library_artists)?;
            filter::prune_to_matching(client, &matching)?;
            Ok(())
        }
        "indexes" => schema::create_indexes(client),
        "analyze" => schema::analyze_tables(client),
        other => panic!("unknown step: {other}"),
    }
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test idempotency_test -- --ignored
fn test_each_step_is_idempotent() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();

    // Start every run from a clean schema.
    schema::drop_all_tables(&mut client).unwrap();

    let fixtures = fixtures_dir();
    let library_db = library_db_path();

    // Steps must run in order so each has its prerequisites satisfied
    // (e.g. Filter needs imported data; Indexes needs filtered data).
    for step in ["schema", "import", "filter", "indexes", "analyze"] {
        // First execution: builds whatever this step builds on top of the
        // prior step's output.
        run_step(step, &mut client, &fixtures, &library_db)
            .unwrap_or_else(|e| panic!("first run of step {step} failed: {e:#}"));
        let snapshot1 = take_snapshot(&mut client);
        let indexes1 = index_snapshot(&mut client);

        // Second execution: same step against the same DB. Must succeed and
        // must not change the row counts of any table -- this is the
        // idempotency contract resume relies on.
        run_step(step, &mut client, &fixtures, &library_db)
            .unwrap_or_else(|e| panic!("second run of step {step} failed (not idempotent): {e:#}"));
        let snapshot2 = take_snapshot(&mut client);
        let indexes2 = index_snapshot(&mut client);

        assert_eq!(
            snapshot1, snapshot2,
            "row counts changed when re-running step {step:?} -- step is not idempotent",
        );
        assert_eq!(
            indexes1, indexes2,
            "index set changed when re-running step {step:?} -- step is not idempotent",
        );
    }
}
