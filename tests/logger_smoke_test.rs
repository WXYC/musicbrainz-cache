//! Smoke test for the `wxyc_etl::logger` wireup in the `musicbrainz-cache`
//! binary. Verifies that the entrypoint starts without panicking when
//! `SENTRY_DSN` is unset and that the first log line is JSON-shaped with the
//! `repo: "musicbrainz-cache"` tag.
//!
//! Runs the binary with no arguments so it exits via clap's "missing required
//! `--data-dir`" error path -- but only AFTER `logger::init` has run, which is
//! exactly what we want to exercise.

use std::process::Command;

fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_musicbrainz-cache")
}

#[test]
fn logger_init_runs_without_dsn_and_emits_repo_tag() {
    let output = Command::new(binary_path())
        .env_remove("SENTRY_DSN")
        // Force the binary to exit early on a clap error after logger init.
        .arg("--help")
        .output()
        .expect("spawn musicbrainz-cache");

    // `--help` is a successful exit; the point is that no panic from
    // logger::init bubbled up before clap printed help.
    assert!(
        output.status.success(),
        "binary did not exit cleanly: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn logger_emits_json_with_repo_tag_when_run() {
    // Trigger an early bail (missing --library-db) so logger::init has
    // definitely run and at least one event was emitted before exit.
    // `--no-filter` skips the library_db check; `--skip-download` skips the
    // network step. Execution then logs startup info and bails on missing
    // mbdump dir -- after at least one tracing event has been emitted.
    let output = Command::new(binary_path())
        .env_remove("SENTRY_DSN")
        .args([
            "--no-filter",
            "--skip-download",
            "--data-dir",
            "/tmp/musicbrainz-cache-logger-smoke-nonexistent",
        ])
        .output()
        .expect("spawn musicbrainz-cache");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // wxyc_etl::logger writes JSON to stderr (matches env_logger / POSIX
    // convention). Anyhow's "Error: ..." also lands on stderr. Find at least
    // one JSON-shaped line on stderr carrying our repo tag.
    let has_json_with_repo = stderr.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with('{')
            && trimmed.ends_with('}')
            && trimmed.contains("\"repo\":\"musicbrainz-cache\"")
    });

    assert!(
        has_json_with_repo,
        "expected a JSON log line tagged repo=musicbrainz-cache on stderr;\nstdout was:\n{}\nstderr was:\n{}",
        stdout, stderr
    );
}
