//! End-to-end resume integration test for the `musicbrainz-cache` binary.
//!
//! Subprocesses the binary with `--resume` and a pre-populated state file,
//! verifies that completed steps are skipped (per stderr logs) and that the
//! remaining steps run and update the state file.
//!
//! Gated on `TEST_DATABASE_URL`: returns early when unset (matches the pattern
//! used in `parity_test.rs`).

use musicbrainz_cache::state::{PipelineState, Step};
use std::path::PathBuf;
use std::process::Command;

fn test_db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn library_db_path() -> PathBuf {
    fixtures_root().join("library.db")
}

/// Path to the compiled binary for this crate. Cargo provides this env var
/// to integration tests automatically.
fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_musicbrainz-cache")
}

/// Drop every `mb_*` table so each invocation starts from a known state.
fn reset_database(db_url: &str) {
    let mut client =
        postgres::Client::connect(db_url, postgres::NoTls).expect("connect to test database");
    let rows = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'mb_%'",
            &[],
        )
        .unwrap();
    for row in rows {
        let table: String = row.get(0);
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {} CASCADE", table))
            .unwrap();
    }
}

/// Drop every `idx_mb_*` index. Used to simulate a crash that occurred
/// before the Indexes step ran.
fn drop_mb_indexes(db_url: &str) {
    let mut client =
        postgres::Client::connect(db_url, postgres::NoTls).expect("connect to test database");
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname LIKE 'idx_mb_%'",
            &[],
        )
        .unwrap();
    for row in rows {
        let idx: String = row.get(0);
        client
            .batch_execute(&format!("DROP INDEX IF EXISTS {}", idx))
            .unwrap();
    }
}

/// Run the binary with the supplied args plus the standard fixture flags.
fn run_binary(args: &[&str], db_url: &str) -> std::process::Output {
    Command::new(binary_path())
        .arg("--data-dir")
        .arg(fixtures_root())
        .arg("--library-db")
        .arg(library_db_path())
        .arg("--database-url")
        .arg(db_url)
        .arg("--skip-download")
        .args(args)
        .env("RUST_LOG", "info")
        .output()
        .expect("spawn binary")
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test resume_integration_test -- --ignored
fn test_full_resume_skips_every_step() {
    let Some(db_url) = test_db_url() else { return };

    reset_database(&db_url);

    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("pipeline.state");

    // First run: full pipeline against an empty DB.
    let out = run_binary(&["--state-file", state_path.to_str().unwrap()], &db_url);
    assert!(
        out.status.success(),
        "fresh run failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let state = PipelineState::load(&state_path).unwrap();
    for step in [
        Step::Schema,
        Step::Import,
        Step::Filter,
        Step::Indexes,
        Step::Analyze,
    ] {
        assert!(
            state.is_complete(step),
            "fresh run should mark {:?} complete",
            step
        );
    }

    // Second run with --resume against the fully-complete state: every step
    // should log a skip message.
    let out = run_binary(
        &["--resume", "--state-file", state_path.to_str().unwrap()],
        &db_url,
    );
    assert!(
        out.status.success(),
        "resume run failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    for label in [
        "Skipping Apply schema",
        "Skipping Import TSV files",
        "Skipping Filter to WXYC library artists",
        "Skipping Create indexes",
        "Skipping Analyze",
    ] {
        assert!(
            stderr.contains(label),
            "expected log {:?} on full-resume run.\nstderr was:\n{}",
            label,
            stderr,
        );
    }
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test resume_integration_test -- --ignored
fn test_partial_state_resume_runs_remaining_steps() {
    let Some(db_url) = test_db_url() else { return };

    reset_database(&db_url);

    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("pipeline.state");

    // Seed the database via a normal pipeline run, then rewrite the state
    // file to mark only Schema + Import complete -- as if the run had
    // crashed between Import and Filter.
    let fresh = run_binary(&["--state-file", state_path.to_str().unwrap()], &db_url);
    assert!(
        fresh.status.success(),
        "fresh seed run failed.\nstderr: {}",
        String::from_utf8_lossy(&fresh.stderr),
    );

    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.mark_complete(Step::Import);
    state.save(&state_path).unwrap();

    // create_indexes is idempotent (CREATE INDEX IF NOT EXISTS), so we
    // could leave the indexes in place. We drop them here only to verify
    // the resume's Indexes step actually re-creates them, not because it
    // would fail otherwise.
    drop_mb_indexes(&db_url);

    // Resume: Schema + Import skipped, Filter/Indexes/Analyze run.
    let out = run_binary(
        &["--resume", "--state-file", state_path.to_str().unwrap()],
        &db_url,
    );
    assert!(
        out.status.success(),
        "resume run failed.\nstderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);

    assert!(
        stderr.contains("Skipping Apply schema"),
        "expected Schema to be skipped.\nstderr:\n{}",
        stderr,
    );
    assert!(
        stderr.contains("Skipping Import TSV files"),
        "expected Import to be skipped.\nstderr:\n{}",
        stderr,
    );
    assert!(
        stderr.contains("=== Filter to WXYC library artists ==="),
        "expected Filter to run.\nstderr:\n{}",
        stderr,
    );
    assert!(
        stderr.contains("=== Create indexes ==="),
        "expected Indexes to run.\nstderr:\n{}",
        stderr,
    );
    assert!(
        stderr.contains("=== Analyze ==="),
        "expected Analyze to run.\nstderr:\n{}",
        stderr,
    );

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

    // Filter actually ran against real data: only the WXYC library artists
    // (Autechre, Stereolab, Jessica Pratt) should remain.
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
        "Filter step should have pruned to library artists",
    );
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test resume_integration_test -- --ignored
fn test_existing_state_without_resume_is_refused() {
    let Some(db_url) = test_db_url() else { return };

    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("pipeline.state");

    // Leftover state from a "previous" run.
    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.save(&state_path).unwrap();

    let out = run_binary(&["--state-file", state_path.to_str().unwrap()], &db_url);
    assert!(
        !out.status.success(),
        "binary should bail when state file exists without --resume.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already exists") && stderr.contains("--resume"),
        "expected refusal message mentioning --resume.\nstderr:\n{}",
        stderr,
    );
}

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test --test resume_integration_test -- --ignored
fn test_resume_with_missing_state_file_starts_fresh() {
    let Some(db_url) = test_db_url() else { return };

    reset_database(&db_url);

    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("pipeline.state");
    assert!(!state_path.exists());

    let out = run_binary(
        &["--resume", "--state-file", state_path.to_str().unwrap()],
        &db_url,
    );
    assert!(
        out.status.success(),
        "resume-with-missing-state run failed.\nstderr: {}",
        String::from_utf8_lossy(&out.stderr),
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not exist") && stderr.contains("starting fresh"),
        "expected warning about missing state file.\nstderr:\n{}",
        stderr,
    );

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
            "Step {:?} should be complete after fresh resume",
            step
        );
    }
}
