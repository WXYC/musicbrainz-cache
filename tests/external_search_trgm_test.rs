//! Integration test for the LML external-search trigram indexes
//! (`migrations/0004_mb_alias_release_recording_trgm_indexes.sql`,
//! mirrored in `schema/create_indexes.sql`).
//!
//! Asserts that the three GIN trigram indexes on
//! `mb_artist_alias`/`mb_release`/`mb_recording`'s `lower(name)` columns
//! exist after `apply_schema()` + `create_indexes()` run and are
//! pg_trgm-backed.
//!
//! Test gating mirrors `tests/wxyc_library_v2_test.rs`: each `#[test]`
//! carries `#[ignore]` so the no-PG `cargo test` job in CI doesn't try to
//! run them. The `test-postgres` CI job provisions PostgreSQL on port 5434
//! and runs the suite via `cargo test -- --ignored --test-threads=1`.
//!
//! `--test-threads=1` is what coordinates the cross-binary DB writes
//! (this binary's `drop_all_tables` and `wxyc_library_v2_test.rs`'s
//! `DROP TABLE wxyc_library` operate on the same DB). The in-binary
//! `DB_LOCK` below serializes the two tests in THIS binary; CI's
//! `--test-threads=1` is what handles the cross-binary case.

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
    // `drop_all_tables` only drops `mb_*` tables; leftover `wxyc_library`
    // from a sibling test binary doesn't interfere with the assertions
    // below (the only thing we look at is the three `idx_mb_*_name_lower_trgm`
    // indexes, and CREATE INDEX IF NOT EXISTS is idempotent).
    schema::drop_all_tables(&mut client).expect("drop_all_tables");
    // `apply_schema` only loads `create_database.sql`; the indexes live in
    // `create_indexes.sql` and must be applied via `create_indexes` —
    // these are two separate functions in `src/schema.rs`.
    schema::apply_schema(&mut client).expect("apply_schema");
    schema::create_indexes(&mut client).expect("create_indexes");
    client
}

const EXPECTED_INDEXES: &[&str] = &[
    "idx_mb_artist_alias_name_lower_trgm",
    "idx_mb_release_name_lower_trgm",
    "idx_mb_recording_name_lower_trgm",
];

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
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
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn test_mb_external_search_trgm_indexes_idempotent_on_reapply() {
    let _lock = lock_db();
    let mut client = fresh_client();

    // Re-applying the schema must be a no-op for these indexes
    // (CREATE INDEX IF NOT EXISTS) and the second apply must not error.
    schema::apply_schema(&mut client).expect("second apply_schema");
    schema::apply_schema(&mut client).expect("third apply_schema");
    schema::create_indexes(&mut client).expect("second create_indexes");

    // Bind via `ANY($1::text[])` so the query scales with the size of
    // `EXPECTED_INDEXES` — positional `$1, $2, $3` wouldn't pick up new
    // entries if the list grew.
    let expected_vec: Vec<&str> = EXPECTED_INDEXES.to_vec();
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = 'public' \
             AND indexname = ANY($1::text[]) \
             ORDER BY indexname",
            &[&expected_vec],
        )
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let mut expected_sorted: Vec<&str> = EXPECTED_INDEXES.to_vec();
    expected_sorted.sort();
    assert_eq!(
        names, expected_sorted,
        "expected all trigram indexes in EXPECTED_INDEXES to survive repeated apply_schema()"
    );
}
