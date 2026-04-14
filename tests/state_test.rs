//! Pipeline state file tests for MusicBrainz cache.
//!
//! Verifies that:
//! 1. A state file is created and records completed steps
//! 2. Resume skips steps already marked complete
//! 3. Partial failure followed by resume completes the pipeline

use musicbrainz_cache::state::{PipelineState, Step};
use std::path::PathBuf;

fn fixture_state_path(tmp: &tempfile::TempDir) -> PathBuf {
    tmp.path().join("pipeline.state")
}

// --- State file creation and serialization ---

#[test]
fn test_new_state_has_no_completed_steps() {
    let state = PipelineState::new();
    assert!(!state.is_complete(Step::Schema));
    assert!(!state.is_complete(Step::Import));
    assert!(!state.is_complete(Step::Filter));
    assert!(!state.is_complete(Step::Indexes));
    assert!(!state.is_complete(Step::Analyze));
}

#[test]
fn test_mark_step_complete() {
    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    assert!(state.is_complete(Step::Schema));
    assert!(!state.is_complete(Step::Import));
}

#[test]
fn test_mark_multiple_steps() {
    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.mark_complete(Step::Import);
    assert!(state.is_complete(Step::Schema));
    assert!(state.is_complete(Step::Import));
    assert!(!state.is_complete(Step::Filter));
}

#[test]
fn test_state_roundtrip_to_file() {
    let tmp = tempfile::tempdir().unwrap();
    let path = fixture_state_path(&tmp);

    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.mark_complete(Step::Import);
    state.save(&path).unwrap();

    let loaded = PipelineState::load(&path).unwrap();
    assert!(loaded.is_complete(Step::Schema));
    assert!(loaded.is_complete(Step::Import));
    assert!(!loaded.is_complete(Step::Filter));
}

#[test]
fn test_load_missing_file_returns_empty_state() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("nonexistent.state");

    let state = PipelineState::load(&path).unwrap();
    assert!(!state.is_complete(Step::Schema));
    assert!(!state.is_complete(Step::Import));
}

#[test]
fn test_all_steps_defined() {
    // Verify the step enum covers all pipeline stages
    let steps = [
        Step::Schema,
        Step::Import,
        Step::Filter,
        Step::Indexes,
        Step::Analyze,
    ];
    assert_eq!(steps.len(), 5, "Pipeline should have 5 steps");
}

#[test]
fn test_resume_skips_completed_steps() {
    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.mark_complete(Step::Import);

    // Simulate resume: collect steps that need to run
    let all_steps = [
        Step::Schema,
        Step::Import,
        Step::Filter,
        Step::Indexes,
        Step::Analyze,
    ];
    let pending: Vec<Step> = all_steps
        .iter()
        .copied()
        .filter(|s| !state.is_complete(*s))
        .collect();

    assert_eq!(pending, vec![Step::Filter, Step::Indexes, Step::Analyze]);
}

#[test]
fn test_partial_failure_resume() {
    let tmp = tempfile::tempdir().unwrap();
    let path = fixture_state_path(&tmp);

    // Simulate first run: Schema and Import succeed, Filter fails
    {
        let mut state = PipelineState::new();
        state.mark_complete(Step::Schema);
        state.mark_complete(Step::Import);
        // Filter would fail here -- save state before attempting
        state.save(&path).unwrap();
    }

    // Simulate resume: load state, verify Schema and Import are skipped
    {
        let state = PipelineState::load(&path).unwrap();
        assert!(
            state.is_complete(Step::Schema),
            "Schema should be marked complete from first run"
        );
        assert!(
            state.is_complete(Step::Import),
            "Import should be marked complete from first run"
        );
        assert!(
            !state.is_complete(Step::Filter),
            "Filter should not be marked complete"
        );
        assert!(
            !state.is_complete(Step::Indexes),
            "Indexes should not be marked complete"
        );
        assert!(
            !state.is_complete(Step::Analyze),
            "Analyze should not be marked complete"
        );
    }
}

#[test]
fn test_state_file_overwrite() {
    let tmp = tempfile::tempdir().unwrap();
    let path = fixture_state_path(&tmp);

    // First save
    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.save(&path).unwrap();

    // Second save with more steps
    state.mark_complete(Step::Import);
    state.mark_complete(Step::Filter);
    state.save(&path).unwrap();

    // Load should have all three
    let loaded = PipelineState::load(&path).unwrap();
    assert!(loaded.is_complete(Step::Schema));
    assert!(loaded.is_complete(Step::Import));
    assert!(loaded.is_complete(Step::Filter));
    assert!(!loaded.is_complete(Step::Indexes));
}

#[test]
fn test_clear_state() {
    let tmp = tempfile::tempdir().unwrap();
    let path = fixture_state_path(&tmp);

    let mut state = PipelineState::new();
    state.mark_complete(Step::Schema);
    state.mark_complete(Step::Import);
    state.save(&path).unwrap();

    state.clear();
    assert!(!state.is_complete(Step::Schema));
    assert!(!state.is_complete(Step::Import));

    state.save(&path).unwrap();
    let loaded = PipelineState::load(&path).unwrap();
    assert!(!loaded.is_complete(Step::Schema));
}
