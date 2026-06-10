//! Regression test for WXYC/musicbrainz-cache#50.
//!
//! The previous `create_indexes` implementation parsed
//! `schema/create_indexes.sql` with `sql.split(';')` and skipped any chunk
//! whose trimmed form started with `--`. That naive parser silently dropped
//! every CREATE INDEX statement preceded by a leading `--` comment block,
//! because the comment block lives in the same chunk as the next statement
//! (the chunk boundary is the previous statement's `;`).
//!
//! Three indexes were latently dropped on fresh-build / DR rebuild paths:
//! - `idx_mb_recording_gid` — preceded by the file header comment.
//! - `idx_mb_artist_name_lower_trgm` — preceded by the 3-line "Trigram GIN
//!   index supports the `%` similarity operator" comment block.
//! - `idx_mb_artist_tag_artist` — preceded by the 6-line "The three
//!   idx_mb_*_name_lower_trgm indexes immediately above mirror..." block.
//!
//! This test asserts all three exist on `pg_indexes` after a fresh
//! `drop_all_tables` → `apply_schema` → `create_indexes` cycle. Under the
//! old code path the test FAILS for all three.
//!
//! Mirrors the gating and fresh-client setup of
//! `tests/external_search_trgm_test.rs` (same `#[ignore]` + `DB_LOCK`
//! pattern; `--test-threads=1` coordinates the cross-binary case).

use musicbrainz_cache::schema;
use postgres::{Client, NoTls};
use std::sync::{Mutex, MutexGuard};

const DEFAULT_DB_URL: &str = "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz";

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

static DB_LOCK: Mutex<()> = Mutex::new(());

fn lock_db() -> MutexGuard<'static, ()> {
    DB_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn fresh_client() -> Client {
    let mut client = Client::connect(&db_url(), NoTls)
        .expect("Failed to connect to test DB; is `docker compose up -d` running?");
    schema::drop_all_tables(&mut client).expect("drop_all_tables");
    schema::apply_schema(&mut client).expect("apply_schema");
    schema::create_indexes(&mut client).expect("create_indexes");
    client
}

/// Indexes the OLD `split(';')` parser silently dropped because the file
/// places `--` comment blocks immediately before each statement.
const PREVIOUSLY_DROPPED_INDEXES: &[&str] = &[
    "idx_mb_recording_gid",
    "idx_mb_artist_name_lower_trgm",
    "idx_mb_artist_tag_artist",
];

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn test_create_indexes_no_longer_drops_comment_preceded_indexes() {
    let _lock = lock_db();
    let mut client = fresh_client();

    let expected_vec: Vec<&str> = PREVIOUSLY_DROPPED_INDEXES.to_vec();
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = 'public' \
             AND indexname = ANY($1::text[]) \
             ORDER BY indexname",
            &[&expected_vec],
        )
        .expect("pg_indexes query");
    let found: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let mut expected_sorted: Vec<&str> = PREVIOUSLY_DROPPED_INDEXES.to_vec();
    expected_sorted.sort();
    assert_eq!(
        found, expected_sorted,
        "create_indexes must apply every CREATE INDEX in schema/create_indexes.sql, \
         including statements preceded by `--` comment blocks. \
         Expected {expected_sorted:?}, found {found:?}. \
         If indexes are missing, src/schema.rs::create_indexes is likely back to \
         naively split(';')-parsing the file — use client.batch_execute instead \
         (see WXYC/musicbrainz-cache#50)."
    );
}

/// Specifically pin `idx_mb_artist_name_lower_trgm` — the example called
/// out in the issue's acceptance criteria. Adds a property check (must be a
/// GIN trgm index on `lower(name)`) on top of the existence check above, so
/// if someone "fixes" the parser by emitting a no-op CREATE INDEX with the
/// right name but wrong shape, this still catches it.
#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn test_idx_mb_artist_name_lower_trgm_is_gin_trgm_on_lower_name() {
    let _lock = lock_db();
    let mut client = fresh_client();

    let row = client
        .query_one(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = 'public' AND indexname = $1",
            &[&"idx_mb_artist_name_lower_trgm"],
        )
        .expect(
            "idx_mb_artist_name_lower_trgm missing after create_indexes — \
             the naive split(';') parser regression has returned",
        );
    let indexdef: String = row.get(0);
    let lower = indexdef.to_lowercase();
    assert!(
        lower.contains("using gin"),
        "idx_mb_artist_name_lower_trgm should be a GIN index, got: {indexdef}"
    );
    assert!(
        indexdef.contains("gin_trgm_ops"),
        "idx_mb_artist_name_lower_trgm should use gin_trgm_ops, got: {indexdef}"
    );
    assert!(
        lower.contains("lower(name)"),
        "idx_mb_artist_name_lower_trgm should be on lower(name), got: {indexdef}"
    );
}
