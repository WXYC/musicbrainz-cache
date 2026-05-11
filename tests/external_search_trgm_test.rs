//! Integration test for the LML external-search trigram indexes
//! (`migrations/0004_mb_alias_release_recording_trgm.sql`, mirrored in
//! `schema/create_indexes.sql`).
//!
//! Asserts that the three GIN trigram indexes on
//! `mb_artist_alias`/`mb_release`/`mb_recording`'s `lower(name)` columns
//! exist after `apply_schema()` runs and are pg_trgm-backed.
//!
//! Requires a PostgreSQL instance on port 5434 (`docker compose up -d`).
//! Mirrors the convention in `tests/wxyc_library_v2_test.rs` — no
//! `#[ignore]`, since this repo's CI provisions the service container.

use musicbrainz_cache::schema;
use postgres::{Client, NoTls};
use std::sync::{Mutex, MutexGuard};

/// Default URL matches `wxyc_library_v2_test.rs` — the same docker compose
/// service container backs both test binaries. Override via `DATABASE_URL`.
const DEFAULT_DB_URL: &str = "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz";

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

/// Serialize the two tests in this binary so the second test's
/// `drop_all_tables` doesn't race with the first test's `apply_schema`.
/// Mirrors `tests/import_test.rs::DB_LOCK`.
static DB_LOCK: Mutex<()> = Mutex::new(());

fn lock_db() -> MutexGuard<'static, ()> {
    DB_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn fresh_client() -> Client {
    let mut client = Client::connect(&db_url(), NoTls)
        .expect("Failed to connect to test DB; is `docker compose up -d` running?");
    schema::drop_all_tables(&mut client).expect("drop_all_tables");
    schema::apply_schema(&mut client).expect("apply_schema");
    // Indexes ship in `create_indexes.sql`, applied separately from the
    // table DDL. `apply_schema` runs both, but call `create_indexes`
    // explicitly so the test is robust if that ordering ever changes.
    schema::create_indexes(&mut client).expect("create_indexes");
    client
}

const EXPECTED_INDEXES: &[&str] = &[
    "idx_mb_artist_alias_name_lower_trgm",
    "idx_mb_release_name_lower_trgm",
    "idx_mb_recording_name_lower_trgm",
];

#[test]
fn test_mb_external_search_trgm_indexes_exist() {
    let _lock = lock_db();
    let mut client = fresh_client();

    for idx in EXPECTED_INDEXES {
        let row = client
            .query_one(
                "SELECT indexdef FROM pg_indexes \
                 WHERE schemaname = 'public' AND indexname = $1",
                &[idx],
            )
            .unwrap_or_else(|_| panic!("index '{idx}' missing after apply_schema()"));
        let indexdef: String = row.get(0);
        // Indexdef should contain both `gin` and `gin_trgm_ops` — that's
        // what makes the `%` similarity operator hit the index instead of
        // falling back to a seq-scan.
        assert!(
            indexdef.to_lowercase().contains("using gin"),
            "index '{idx}' should be a GIN index, got: {indexdef}"
        );
        assert!(
            indexdef.contains("gin_trgm_ops"),
            "index '{idx}' should use gin_trgm_ops, got: {indexdef}"
        );
        assert!(
            indexdef.to_lowercase().contains("lower(name)"),
            "index '{idx}' should be on lower(name), got: {indexdef}"
        );
    }
}

#[test]
fn test_mb_external_search_trgm_indexes_idempotent_on_reapply() {
    let _lock = lock_db();
    let mut client = fresh_client();

    // Re-applying the schema must be a no-op for these indexes
    // (CREATE INDEX IF NOT EXISTS) and the second apply must not error.
    schema::apply_schema(&mut client).expect("second apply_schema");
    schema::apply_schema(&mut client).expect("third apply_schema");

    // The three indexes still exist after the repeated applies.
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = 'public' \
             AND indexname IN ($1, $2, $3) \
             ORDER BY indexname",
            &[
                &EXPECTED_INDEXES[0],
                &EXPECTED_INDEXES[1],
                &EXPECTED_INDEXES[2],
            ],
        )
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let mut expected: Vec<&str> = EXPECTED_INDEXES.to_vec();
    expected.sort();
    assert_eq!(
        names, expected,
        "expected all three trigram indexes to survive repeated apply_schema()"
    );
}
