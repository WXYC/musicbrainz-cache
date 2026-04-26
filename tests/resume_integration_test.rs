//! Orchestrator-level resume integration test.
//!
//! Drives the pipeline functions in the same order `main.rs` does (schema ->
//! import -> filter -> indexes -> analyze) while persisting `PipelineState`
//! between phases. Verifies that after a partial run the saved state file
//! reflects only the completed steps, and that a subsequent resume completes
//! the remaining steps without re-running the earlier ones.
//!
//! Gated on TEST_DATABASE_URL: returns early when unset (matches the pattern
//! used in `parity_test.rs`).

use musicbrainz_cache::state::{PipelineState, Step};
use std::path::PathBuf;

fn test_db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbdump")
}

fn library_db_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/library.db")
}

/// Run a single pipeline step, marking it complete in state and persisting on success.
/// Mirrors the orchestration loop a state-aware `main.rs` would use.
fn run_step<F>(
    state: &mut PipelineState,
    state_path: &std::path::Path,
    step: Step,
    f: F,
) -> anyhow::Result<bool>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    if state.is_complete(step) {
        return Ok(false);
    }
    f()?;
    state.mark_complete(step);
    state.save(state_path)?;
    Ok(true)
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test resume_integration_test -- --ignored
fn test_pipeline_resume_skips_completed_steps() {
    let Some(db_url) = test_db_url() else { return };

    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("pipeline.state");

    // ----- First run: complete only schema + import, then "interrupt". -----
    {
        let mut state = PipelineState::load(&state_path).unwrap();
        let mut client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();

        let schema_ran = run_step(&mut state, &state_path, Step::Schema, || {
            musicbrainz_cache::schema::apply_schema(&mut client)
        })
        .unwrap();
        assert!(schema_ran, "Schema should run on first invocation");

        let import_ran = run_step(&mut state, &state_path, Step::Import, || {
            musicbrainz_cache::import::import_all(&mut client, &fixtures_dir()).map(|_| ())
        })
        .unwrap();
        assert!(import_ran, "Import should run on first invocation");

        // Simulated interruption before filter/indexes/analyze.
    }

    // The state file persists exactly the two completed steps.
    {
        let state = PipelineState::load(&state_path).unwrap();
        assert!(state.is_complete(Step::Schema));
        assert!(state.is_complete(Step::Import));
        assert!(!state.is_complete(Step::Filter));
        assert!(!state.is_complete(Step::Indexes));
        assert!(!state.is_complete(Step::Analyze));
    }

    // ----- Second run (resume): only the remaining steps execute. -----
    let (schema_ran_again, import_ran_again, filter_ran, indexes_ran, analyze_ran);
    {
        let mut state = PipelineState::load(&state_path).unwrap();
        let mut client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();

        schema_ran_again = run_step(&mut state, &state_path, Step::Schema, || {
            musicbrainz_cache::schema::apply_schema(&mut client)
        })
        .unwrap();

        import_ran_again = run_step(&mut state, &state_path, Step::Import, || {
            musicbrainz_cache::import::import_all(&mut client, &fixtures_dir()).map(|_| ())
        })
        .unwrap();

        filter_ran = run_step(&mut state, &state_path, Step::Filter, || {
            let library_artists =
                musicbrainz_cache::filter::load_library_artists(&library_db_path())?;
            let matching =
                musicbrainz_cache::filter::find_matching_artist_ids(&mut client, &library_artists)?;
            musicbrainz_cache::filter::prune_to_matching(&mut client, &matching)
        })
        .unwrap();

        indexes_ran = run_step(&mut state, &state_path, Step::Indexes, || {
            musicbrainz_cache::schema::create_indexes(&mut client)
        })
        .unwrap();

        analyze_ran = run_step(&mut state, &state_path, Step::Analyze, || {
            musicbrainz_cache::schema::analyze_tables(&mut client)
        })
        .unwrap();
    }

    assert!(
        !schema_ran_again,
        "Schema must be skipped on resume (was already complete)"
    );
    assert!(
        !import_ran_again,
        "Import must be skipped on resume (was already complete)"
    );
    assert!(filter_ran, "Filter should run on resume");
    assert!(indexes_ran, "Indexes should run on resume");
    assert!(analyze_ran, "Analyze should run on resume");

    // Final state has every step recorded as complete.
    let final_state = PipelineState::load(&state_path).unwrap();
    for step in [
        Step::Schema,
        Step::Import,
        Step::Filter,
        Step::Indexes,
        Step::Analyze,
    ] {
        assert!(
            final_state.is_complete(step),
            "Step {:?} should be complete after resume",
            step
        );
    }

    // Sanity-check that the filter step actually ran against real data:
    // only the WXYC library artists (Autechre, Stereolab, Jessica Pratt)
    // should remain after filtering.
    let mut client = postgres::Client::connect(&db_url, postgres::NoTls).unwrap();
    let rows = client
        .query("SELECT name FROM mb_artist ORDER BY name", &[])
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        names,
        vec![
            "Autechre".to_string(),
            "Jessica Pratt".to_string(),
            "Stereolab".to_string(),
        ],
        "Filter step should have pruned to library artists"
    );
}
