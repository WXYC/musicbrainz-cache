//! Integration tests for the v2 `wxyc_library` cross-cache identity hook
//! (E1 §4.1.2 of plans/library-hook-canonicalization.md).
//!
//! Validates the schema mirror in `schema/create_database.sql` +
//! `schema/create_indexes.sql` (applied via `apply_schema()` +
//! `create_indexes()`) plus the `populate_wxyc_library_v2()` loader from
//! `src/wxyc_loader.rs`. Per the wiki §4.1.2, this cache has legacy
//! production data; the dual-write window is implicit (the legacy hook
//! data already exists out-of-band and our writes target the new
//! `wxyc_library` table independently).
//!
//! Gated on `--ignored` per repo convention (`cargo test -- --ignored
//! --test-threads=1`); requires PostgreSQL on port 5434 (`docker compose
//! up -d`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use musicbrainz_cache::wxyc_loader::{populate_wxyc_library_v2, NORMALIZER_NAME};
use wxyc_etl::text::{to_identity_match_form, to_identity_match_form_title};

const DEFAULT_DB_URL: &str = "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz";

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string())
}

/// Connect to PG and apply the cache schema (creates `wxyc_library` via the
/// dual-source mirror in `schema/create_database.sql`). Drops any prior
/// `wxyc_library` table first so each test starts clean.
fn fresh_client() -> postgres::Client {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).expect("connect");
    // Drop just our table — leaves the rest of the cache schema alone so
    // multiple tests can share a DB if run with --test-threads=1.
    client
        .batch_execute("DROP TABLE IF EXISTS wxyc_library CASCADE")
        .expect("drop wxyc_library");
    musicbrainz_cache::schema::apply_schema(&mut client).expect("apply_schema");
    musicbrainz_cache::schema::create_indexes(&mut client).expect("create_indexes");
    client
}

/// Fixture rows mirror discogs-etl's #178 `_FIXTURE_ROWS` so the two repos'
/// integration tests stay in lockstep. Includes "Nilüfer Yanya" to exercise
/// the diacritic-fold path through the storage layer end-to-end.
const FIXTURE_ROWS: &[(i32, &str, &str, &str, &str, &str)] = &[
    (1, "Juana Molina", "DOGA", "LP", "Sonamos", "Rock"),
    (
        2,
        "Jessica Pratt",
        "On Your Own Love Again",
        "LP",
        "Drag City",
        "Rock",
    ),
    (
        3,
        "Chuquimamani-Condori",
        "Edits",
        "CD",
        "self-released",
        "Electronic",
    ),
    (
        4,
        "Duke Ellington & John Coltrane",
        "Duke Ellington & John Coltrane",
        "LP",
        "Impulse Records",
        "Jazz",
    ),
    (5, "Stereolab", "Aluminum Tunes", "CD", "Duophonic", "Rock"),
    // Diacritic-bearing canonical name from wxyc-shared's
    // wxycCanonicalArtistNames. Exercises the diacritic-fold path
    // (ü -> u) end-to-end through the loader's normalization.
    (6, "Nilüfer Yanya", "Painless", "LP", "ATO Records", "Rock"),
];

static FIXTURE_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Build a per-test SQLite library.db in a unique temp path. Each test gets
/// its own file so concurrent runs (or sequential runs that leak temp files)
/// don't collide.
fn build_library_db() -> PathBuf {
    let dir = tempfile::tempdir().expect("tempdir").keep();
    // Use both an atomic counter and the wall clock so paths are unique
    // across processes and across re-runs.
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = FIXTURE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!("library-{now_nanos}-{n}.db"));
    let conn = rusqlite::Connection::open(&path).expect("open sqlite");
    conn.execute_batch(
        "CREATE TABLE library (
            id INTEGER PRIMARY KEY,
            artist TEXT NOT NULL,
            title TEXT NOT NULL,
            format TEXT,
            label TEXT,
            genre TEXT
        )",
    )
    .expect("create library");
    {
        let mut stmt = conn
            .prepare(
                "INSERT INTO library (id, artist, title, format, label, genre) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .expect("prepare insert");
        for (id, artist, title, format, label, genre) in FIXTURE_ROWS {
            stmt.execute(rusqlite::params![id, artist, title, format, label, genre])
                .expect("insert fixture row");
        }
    }
    path
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn schema_lands_all_expected_indexes() {
    let mut client = fresh_client();
    let expected_indexes: &[&str] = &[
        "wxyc_library_pkey",
        "wxyc_library_norm_artist_idx",
        "wxyc_library_norm_title_idx",
        "wxyc_library_artist_id_idx",
        "wxyc_library_format_id_idx",
        "wxyc_library_release_year_idx",
        "wxyc_library_norm_artist_trgm_idx",
        "wxyc_library_norm_title_trgm_idx",
    ];
    let rows = client
        .query(
            "SELECT indexname FROM pg_indexes \
             WHERE schemaname = 'public' AND tablename = 'wxyc_library'",
            &[],
        )
        .expect("select indexes");
    let present: std::collections::HashSet<String> =
        rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for &idx in expected_indexes {
        assert!(
            present.contains(idx),
            "missing index {idx}; present: {present:?}"
        );
    }
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn loader_writes_every_fixture_row() {
    let mut client = fresh_client();
    let library_db = build_library_db();

    let attempted = populate_wxyc_library_v2(&mut client, &library_db, "backend")
        .expect("populate_wxyc_library_v2");
    assert_eq!(attempted, FIXTURE_ROWS.len() as u64);

    let rows = client
        .query(
            "SELECT library_id, artist_name, album_title, norm_artist, norm_title, snapshot_source \
             FROM wxyc_library ORDER BY library_id",
            &[],
        )
        .expect("select wxyc_library");

    let expected_ids: std::collections::HashSet<i32> = FIXTURE_ROWS.iter().map(|r| r.0).collect();
    let present_ids: std::collections::HashSet<i32> =
        rows.iter().map(|r| r.get::<_, i32>(0)).collect();
    assert_eq!(present_ids, expected_ids);

    for row in &rows {
        let library_id: i32 = row.get(0);
        let artist_name: String = row.get(1);
        let album_title: String = row.get(2);
        let norm_artist: String = row.get(3);
        let norm_title: String = row.get(4);
        let snapshot_source: String = row.get(5);
        assert!(
            !artist_name.is_empty(),
            "artist_name empty for {library_id}"
        );
        assert!(
            !album_title.is_empty(),
            "album_title empty for {library_id}"
        );
        assert!(
            !norm_artist.is_empty(),
            "norm_artist empty for {library_id}"
        );
        assert!(!norm_title.is_empty(), "norm_title empty for {library_id}");
        assert_eq!(
            snapshot_source, "backend",
            "snapshot_source for {library_id} was {snapshot_source:?}"
        );
    }
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn loader_is_idempotent() {
    let mut client = fresh_client();
    let library_db = build_library_db();

    let first = populate_wxyc_library_v2(&mut client, &library_db, "backend").expect("first run");
    let second = populate_wxyc_library_v2(&mut client, &library_db, "backend").expect("second run");
    // Both report rows-attempted (pre-conflict).
    assert_eq!(first, second);
    assert_eq!(first, FIXTURE_ROWS.len() as u64);

    // Idempotency is observable in COUNT(*), not the return value.
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM wxyc_library", &[])
        .expect("count")
        .get(0);
    assert_eq!(count, FIXTURE_ROWS.len() as i64);
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn loader_rejects_invalid_snapshot_source() {
    let mut client = fresh_client();
    let library_db = build_library_db();
    // The loader's argument check fires before any write, mirroring the
    // §3.1 CHECK constraint vocabulary. We don't get as far as the DB.
    let err = populate_wxyc_library_v2(&mut client, &library_db, "bogus")
        .expect_err("bogus snapshot_source must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("snapshot_source"),
        "expected snapshot_source error, got: {msg}"
    );
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn loader_rejects_empty_artist_or_title() {
    // Postgres `NOT NULL` rejects SQL NULL but NOT empty strings; without
    // the explicit guard in the COPY loop, a library.db row with an empty
    // artist would silently land with an empty norm_artist and defeat
    // downstream NULL-aware joins. Pin the loud-failure behavior.
    use rusqlite::Connection as SqliteConnection;
    use tempfile::TempDir;

    let mut client = fresh_client();
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("library.db");
    let conn = SqliteConnection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE library (\
            id INTEGER PRIMARY KEY, \
            artist TEXT NOT NULL, \
            title TEXT NOT NULL\
        );",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO library (id, artist, title) VALUES (?, ?, ?)",
        rusqlite::params![1_i64, "", "Some Title"],
    )
    .unwrap();
    drop(conn);

    let err = populate_wxyc_library_v2(&mut client, &db_path, "backend")
        .expect_err("empty artist must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("artist_name or album_title is empty"),
        "expected empty-input error, got: {msg}"
    );
    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM wxyc_library", &[])
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "loader must not write any rows when bailing");
}

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored --test-threads=1
fn normalizer_pin_includes_diacritic_fold() {
    let mut client = fresh_client();
    let library_db = build_library_db();
    populate_wxyc_library_v2(&mut client, &library_db, "backend").expect("populate");

    // Audit string mirrors the discogs-etl loader's NORMALIZER_NAME so the
    // cross-cache audit trail stays consistent.
    assert_eq!(NORMALIZER_NAME, "wxyc_etl::text::to_identity_match_form");

    // Library row 1: "Juana Molina" / "DOGA" / "Sonamos" — no diacritics,
    // just lowercase. Hard-coded value pin catches drift in wxyc-etl's
    // identity normalizer (the property assertion below would pass even if
    // both sides changed in lockstep).
    let row = client
        .query_one(
            "SELECT artist_name, album_title, label_name, norm_artist, norm_title, norm_label \
             FROM wxyc_library WHERE library_id = 1",
            &[],
        )
        .expect("row 1");
    let artist: String = row.get(0);
    let title: String = row.get(1);
    let label: String = row.get(2);
    let norm_artist: String = row.get(3);
    let norm_title: String = row.get(4);
    let norm_label: String = row.get(5);
    assert_eq!(norm_artist, to_identity_match_form(&artist));
    assert_eq!(norm_title, to_identity_match_form_title(&title));
    assert_eq!(norm_label, to_identity_match_form(&label));
    assert_eq!(norm_artist, "juana molina");
    assert_eq!(norm_title, "doga");
    assert_eq!(norm_label, "sonamos");

    // Library row 6: "Nilüfer Yanya" — exercises the ü -> u diacritic-fold
    // path through storage. Property assertion (no hard-coded value)
    // because the rest of the title-side algorithm could legitimately
    // evolve without breaking the diacritic-fold contract.
    let row6: String = client
        .query_one(
            "SELECT norm_artist FROM wxyc_library WHERE library_id = 6",
            &[],
        )
        .expect("row 6")
        .get(0);
    assert!(
        !row6.contains('ü'),
        "diacritic survived normalization: {row6:?}"
    );
    assert_eq!(
        row6.to_lowercase(),
        row6,
        "normalization should lowercase: {row6:?}"
    );
}
