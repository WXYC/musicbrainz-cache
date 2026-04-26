//! Parity tests for the MusicBrainz cache Rust import pipeline.
//!
//! Verifies that the Rust implementation produces identical row counts and
//! sample data to expected baselines derived from the Python implementation's
//! output on the same fixture TSVs.
//!
//! Gated on TEST_DATABASE_URL. Skips when the env var is unset.

use std::collections::HashMap;
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

/// Expected row counts after importing all fixture TSVs (before filtering).
/// These baselines are derived from the Python import_tsv.py output on the
/// same fixture data.
const EXPECTED_IMPORT_COUNTS: &[(&str, i64)] = &[
    ("mb_area_type", 3),
    ("mb_gender", 3),
    ("mb_tag", 5),
    ("mb_area", 5),
    ("mb_country_area", 2),
    ("mb_artist", 10),
    ("mb_artist_alias", 5),
    ("mb_artist_tag", 8),
    ("mb_artist_credit", 5),
    ("mb_artist_credit_name", 5),
    ("mb_release_group", 5),
    ("mb_recording", 5),
    ("mb_medium", 3),
    ("mb_track", 5),
];

/// Expected row counts after filtering to library artists (Autechre, Stereolab,
/// Jessica Pratt). These baselines are derived from the Python filter_artists.py
/// output on the same fixture data.
const EXPECTED_FILTERED_COUNTS: &[(&str, i64)] = &[
    ("mb_artist", 3),
    ("mb_artist_alias", 3),
    ("mb_artist_tag", 5),
    ("mb_tag", 3),
    ("mb_area", 5),
    ("mb_country_area", 2),
    ("mb_area_type", 3),
    ("mb_gender", 3),
    ("mb_artist_credit", 3),
    ("mb_artist_credit_name", 3),
    ("mb_release_group", 3),
    ("mb_recording", 3),
    ("mb_medium", 3),
    ("mb_track", 3),
];

/// Sample artist rows expected after import (id, name, sort_name).
const EXPECTED_ARTISTS: &[(i32, &str, &str)] = &[
    (100, "Autechre", "Autechre"),
    (200, "Stereolab", "Stereolab"),
    (300, "Jessica Pratt", "Pratt, Jessica"),
    (400, "Fake Artist One", "Fake Artist One"),
    (600, "Björk", "Bjork"),
    (700, "Sigur Rós", "Sigur Ros"),
    (800, "Cécile McLorin Salvant", "Salvant, Cécile McLorin"),
];

/// Artists expected to survive filtering.
const EXPECTED_FILTERED_ARTISTS: &[&str] = &["Autechre", "Jessica Pratt", "Stereolab"];

fn set_up_imported_db(db_url: &str) -> postgres::Client {
    let mut client = postgres::Client::connect(db_url, postgres::NoTls).unwrap();
    // apply_schema is idempotent (CREATE TABLE IF NOT EXISTS). Drop first
    // to give each test a clean slate; otherwise rows from a prior test would
    // make the (now-idempotent) Import step skip every table.
    musicbrainz_cache::schema::drop_all_tables(&mut client).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();
    musicbrainz_cache::import::import_all(&mut client, &fixtures_dir()).unwrap();
    client
}

fn set_up_filtered_db(db_url: &str) -> postgres::Client {
    let mut client = set_up_imported_db(db_url);
    let library_artists =
        musicbrainz_cache::filter::load_library_artists(&library_db_path()).unwrap();
    let matching =
        musicbrainz_cache::filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();
    musicbrainz_cache::filter::prune_to_matching(&mut client, &matching).unwrap();
    client
}

fn get_row_counts(client: &mut postgres::Client) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    let tables = [
        "mb_area_type",
        "mb_gender",
        "mb_tag",
        "mb_area",
        "mb_country_area",
        "mb_artist",
        "mb_artist_alias",
        "mb_artist_tag",
        "mb_artist_credit",
        "mb_artist_credit_name",
        "mb_release_group",
        "mb_recording",
        "mb_medium",
        "mb_track",
    ];
    for table in tables {
        let row = client
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .unwrap();
        counts.insert(table.to_string(), row.get(0));
    }
    counts
}

// --- Import parity tests ---

#[test]
#[ignore] // Requires PostgreSQL: TEST_DATABASE_URL=... cargo test parity -- --ignored
fn test_parity_import_row_counts() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);
    let counts = get_row_counts(&mut client);

    for &(table, expected) in EXPECTED_IMPORT_COUNTS {
        let actual = counts[table];
        assert_eq!(
            actual, expected,
            "Import parity: {table} expected {expected} rows, got {actual}"
        );
    }
}

#[test]
#[ignore]
fn test_parity_import_artist_data() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);

    for &(id, name, sort_name) in EXPECTED_ARTISTS {
        let row = client
            .query_one(
                "SELECT name, sort_name FROM mb_artist WHERE id = $1",
                &[&id],
            )
            .unwrap();
        let actual_name: &str = row.get(0);
        let actual_sort: &str = row.get(1);
        assert_eq!(
            actual_name, name,
            "Artist {id}: name expected '{name}', got '{actual_name}'"
        );
        assert_eq!(
            actual_sort, sort_name,
            "Artist {id}: sort_name expected '{sort_name}', got '{actual_sort}'"
        );
    }
}

#[test]
#[ignore]
fn test_parity_import_null_handling() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);

    // Autechre (100): group type (2), UK area (221), NULL gender, begin_area=1000
    let row = client
        .query_one(
            "SELECT type, area, gender, begin_area FROM mb_artist WHERE id = 100",
            &[],
        )
        .unwrap();
    assert_eq!(
        row.get::<_, Option<i32>>(0),
        Some(2),
        "Autechre type should be 2 (group)"
    );
    assert_eq!(
        row.get::<_, Option<i32>>(1),
        Some(221),
        "Autechre area should be UK"
    );
    assert!(
        row.get::<_, Option<i32>>(2).is_none(),
        "Autechre gender should be NULL"
    );
    assert_eq!(
        row.get::<_, Option<i32>>(3),
        Some(1000),
        "Autechre begin_area should be Manchester"
    );

    // Björk (600): person type (1), NULL area, female gender (2)
    let row = client
        .query_one(
            "SELECT type, area, gender FROM mb_artist WHERE id = 600",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), Some(1));
    assert!(
        row.get::<_, Option<i32>>(1).is_none(),
        "Björk area should be NULL"
    );
    assert_eq!(
        row.get::<_, Option<i32>>(2),
        Some(2),
        "Björk gender should be Female"
    );
}

#[test]
#[ignore]
fn test_parity_import_alias_data() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);

    // Autechre alias: "ae"
    let row = client
        .query_one(
            "SELECT name FROM mb_artist_alias WHERE artist = 100 AND id = 1",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "ae");

    // Total alias count should match fixture
    let row = client
        .query_one("SELECT COUNT(*) FROM mb_artist_alias", &[])
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 5, "Expected 5 aliases total");

    // Stereolab alias name: "Stéréolab"
    let row = client
        .query_one("SELECT name FROM mb_artist_alias WHERE artist = 200", &[])
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "Stéréolab");

    // Jessica Pratt alias: "J. Pratt"
    let row = client
        .query_one("SELECT name FROM mb_artist_alias WHERE artist = 300", &[])
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "J. Pratt");
}

#[test]
#[ignore]
fn test_parity_import_tag_associations() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);

    // Autechre: electronic (1), experimental (3)
    let rows = client
        .query(
            "SELECT t.name FROM mb_artist_tag at JOIN mb_tag t ON t.id = at.tag \
             WHERE at.artist = 100 ORDER BY t.name",
            &[],
        )
        .unwrap();
    let tags: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(tags, vec!["electronic", "experimental"]);

    // Stereolab: experimental (3), rock (2)
    let rows = client
        .query(
            "SELECT t.name FROM mb_artist_tag at JOIN mb_tag t ON t.id = at.tag \
             WHERE at.artist = 200 ORDER BY t.name",
            &[],
        )
        .unwrap();
    let tags: Vec<&str> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(tags, vec!["experimental", "rock"]);
}

#[test]
#[ignore]
fn test_parity_import_recording_data() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_imported_db(&db_url);

    // Recording 1: "VI Scose Poise" by Autechre (credit 100)
    let row = client
        .query_one(
            "SELECT name, artist_credit, gid::text FROM mb_recording WHERE id = 1",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "VI Scose Poise");
    assert_eq!(row.get::<_, Option<i32>>(1), Some(100));
    assert_eq!(
        row.get::<_, &str>(2),
        "bbbb1111-1111-1111-1111-111111111111"
    );
}

// --- Filter parity tests ---

#[test]
#[ignore]
fn test_parity_filtered_row_counts() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);
    let counts = get_row_counts(&mut client);

    for &(table, expected) in EXPECTED_FILTERED_COUNTS {
        let actual = counts[table];
        assert_eq!(
            actual, expected,
            "Filter parity: {table} expected {expected} rows, got {actual}"
        );
    }
}

#[test]
#[ignore]
fn test_parity_filtered_artist_set() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);

    let rows = client
        .query("SELECT name FROM mb_artist ORDER BY name", &[])
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let expected: Vec<String> = EXPECTED_FILTERED_ARTISTS
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        names, expected,
        "Filtered artist set does not match expected"
    );
}

#[test]
#[ignore]
fn test_parity_filtered_no_orphan_credits() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);

    // Every artist_credit_name should reference a kept artist
    let orphans = client
        .query(
            "SELECT acn.artist FROM mb_artist_credit_name acn \
             LEFT JOIN mb_artist a ON a.id = acn.artist \
             WHERE a.id IS NULL",
            &[],
        )
        .unwrap();
    assert!(
        orphans.is_empty(),
        "Orphan artist references in artist_credit_name after filtering"
    );
}

#[test]
#[ignore]
fn test_parity_filtered_no_orphan_tags() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);

    // Every tag in mb_tag should be referenced by at least one artist_tag
    let orphans = client
        .query(
            "SELECT id FROM mb_tag WHERE id NOT IN (SELECT DISTINCT tag FROM mb_artist_tag)",
            &[],
        )
        .unwrap();
    assert!(
        orphans.is_empty(),
        "Orphan tags remain after filtering: {:?}",
        orphans
            .iter()
            .map(|r| r.get::<_, i32>(0))
            .collect::<Vec<_>>()
    );
}

#[test]
#[ignore]
fn test_parity_filtered_release_groups() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);

    let rows = client
        .query("SELECT name FROM mb_release_group ORDER BY name", &[])
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        names,
        vec!["Confield", "Dots and Loops", "On Your Own Love Again"],
        "Only release groups for matching artists should survive"
    );
}

#[test]
#[ignore]
fn test_parity_filtered_recordings() {
    let Some(db_url) = test_db_url() else { return };
    let mut client = set_up_filtered_db(&db_url);

    let rows = client
        .query("SELECT name FROM mb_recording ORDER BY name", &[])
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(
        names,
        vec!["Back, Baby", "Brakhage", "VI Scose Poise"],
        "Only recordings for matching artist credits should survive"
    );
}
