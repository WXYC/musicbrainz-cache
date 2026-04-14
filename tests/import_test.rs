use musicbrainz_cache::import::{self, TableSpec, DERIVED_TABLES, TABLES};
use std::collections::HashSet;
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbdump")
}

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz".into()
    })
}

// --- Unit tests (no database required) ---

#[test]
fn test_table_spec_column_counts_match() {
    for spec in TABLES {
        assert_eq!(
            spec.source_indices.len(),
            spec.db_columns.len(),
            "Column count mismatch for table {}: {} source indices vs {} db columns",
            spec.table,
            spec.source_indices.len(),
            spec.db_columns.len(),
        );
    }
}

#[test]
fn test_table_names_are_unique() {
    let mut table_names = HashSet::new();
    for spec in TABLES {
        assert!(
            table_names.insert(spec.table),
            "Duplicate table name: {}",
            spec.table,
        );
    }
}

#[test]
fn test_dump_file_names_are_unique() {
    let mut dump_files = HashSet::new();
    for spec in TABLES {
        assert!(
            dump_files.insert(spec.dump_file),
            "Duplicate dump file: {}",
            spec.dump_file,
        );
    }
}

#[test]
fn test_all_14_tables_defined() {
    assert_eq!(TABLES.len(), 14, "Expected 14 table specs");
}

#[test]
fn test_artist_table_indices() {
    let artist = TABLES.iter().find(|s| s.table == "mb_artist").unwrap();
    assert_eq!(artist.source_indices, &[0, 2, 3, 10, 11, 12, 17, 13]);
    assert_eq!(
        artist.db_columns,
        &[
            "id",
            "name",
            "sort_name",
            "type",
            "area",
            "gender",
            "begin_area",
            "comment"
        ],
    );
}

#[test]
fn test_artist_alias_table_indices() {
    let alias = TABLES
        .iter()
        .find(|s| s.table == "mb_artist_alias")
        .unwrap();
    assert_eq!(alias.source_indices, &[0, 1, 2, 7, 3, 6, 14]);
    assert_eq!(
        alias.db_columns,
        &[
            "id",
            "artist",
            "name",
            "sort_name",
            "locale",
            "type",
            "primary_for_locale"
        ],
    );
}

#[test]
fn test_track_table_indices() {
    let track = TABLES.iter().find(|s| s.table == "mb_track").unwrap();
    assert_eq!(track.source_indices, &[0, 2, 3, 4, 6, 7, 8]);
    assert_eq!(
        track.db_columns,
        &[
            "id",
            "recording",
            "medium",
            "position",
            "name",
            "artist_credit",
            "length"
        ],
    );
}

#[test]
fn test_derived_tables() {
    assert!(DERIVED_TABLES.contains(&"artist_tag"));
    assert!(DERIVED_TABLES.contains(&"tag"));
    assert_eq!(DERIVED_TABLES.len(), 2);
}

#[test]
fn test_dependency_order() {
    // Reference tables (area_type, gender, tag) must come before tables that FK them.
    let table_positions: std::collections::HashMap<&str, usize> = TABLES
        .iter()
        .enumerate()
        .map(|(i, s)| (s.table, i))
        .collect();

    // area_type before area
    assert!(table_positions["mb_area_type"] < table_positions["mb_area"]);
    // gender before artist
    assert!(table_positions["mb_gender"] < table_positions["mb_artist"]);
    // area before artist (area FK)
    assert!(table_positions["mb_area"] < table_positions["mb_artist"]);
    // artist before artist_alias
    assert!(table_positions["mb_artist"] < table_positions["mb_artist_alias"]);
    // artist before artist_tag
    assert!(table_positions["mb_artist"] < table_positions["mb_artist_tag"]);
    // artist_credit before artist_credit_name
    assert!(table_positions["mb_artist_credit"] < table_positions["mb_artist_credit_name"]);
    // artist_credit before release_group
    assert!(table_positions["mb_artist_credit"] < table_positions["mb_release_group"]);
    // artist_credit before recording
    assert!(table_positions["mb_artist_credit"] < table_positions["mb_recording"]);
    // recording before track
    assert!(table_positions["mb_recording"] < table_positions["mb_track"]);
    // medium before track
    assert!(table_positions["mb_medium"] < table_positions["mb_track"]);
}

// --- Integration tests (require PostgreSQL on port 5434) ---

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored
fn test_import_artist_table() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    // Import reference tables first (area_type, gender, area) for FK satisfaction
    for spec in TABLES.iter().filter(|s| {
        s.table == "mb_area_type"
            || s.table == "mb_gender"
            || s.table == "mb_area"
            || s.table == "mb_country_area"
    }) {
        import::import_table(&mut client, spec, &fixtures_dir()).unwrap();
    }

    let artist_spec = TABLES.iter().find(|s| s.table == "mb_artist").unwrap();
    let count = import::import_table(&mut client, artist_spec, &fixtures_dir()).unwrap();
    assert_eq!(count, 10, "Expected 10 artist rows");

    // Verify specific values
    let row = client
        .query_one(
            "SELECT name, sort_name, comment FROM mb_artist WHERE id = 100",
            &[],
        )
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "Autechre");
    assert_eq!(row.get::<_, &str>(1), "Autechre");
    assert_eq!(row.get::<_, &str>(2), "");

    // Verify NULL handling
    let row = client
        .query_one(
            "SELECT gender, begin_area FROM mb_artist WHERE id = 100",
            &[],
        )
        .unwrap();
    assert!(
        row.get::<_, Option<i32>>(0).is_none(),
        "Autechre gender should be NULL"
    );
}

#[test]
#[ignore]
fn test_import_null_handling() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    // Import area_type, gender, area
    for spec in TABLES.iter().filter(|s| {
        s.table == "mb_area_type"
            || s.table == "mb_gender"
            || s.table == "mb_area"
            || s.table == "mb_country_area"
    }) {
        import::import_table(&mut client, spec, &fixtures_dir()).unwrap();
    }

    let artist_spec = TABLES.iter().find(|s| s.table == "mb_artist").unwrap();
    import::import_table(&mut client, artist_spec, &fixtures_dir()).unwrap();

    // Artist 600 (Björk) has no area (NULL) and gender=2 (Female)
    let row = client
        .query_one("SELECT area, gender FROM mb_artist WHERE id = 600", &[])
        .unwrap();
    assert!(
        row.get::<_, Option<i32>>(0).is_none(),
        "Björk area should be NULL"
    );
    assert_eq!(row.get::<_, Option<i32>>(1), Some(2));
}

#[test]
#[ignore]
fn test_import_column_extraction() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    for spec in TABLES.iter().filter(|s| {
        s.table == "mb_area_type"
            || s.table == "mb_gender"
            || s.table == "mb_area"
            || s.table == "mb_country_area"
    }) {
        import::import_table(&mut client, spec, &fixtures_dir()).unwrap();
    }

    let artist_spec = TABLES.iter().find(|s| s.table == "mb_artist").unwrap();
    import::import_table(&mut client, artist_spec, &fixtures_dir()).unwrap();

    // The artist TSV has 19 columns but we only import 8
    let cols: Vec<String> = client
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'mb_artist' ORDER BY ordinal_position",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(cols.len(), 8, "mb_artist should have exactly 8 columns");
}

#[test]
#[ignore]
fn test_import_all_tables() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    let total = import::import_all(&mut client, &fixtures_dir()).unwrap();
    assert!(total > 0, "Expected rows imported");

    // Verify row counts for each table
    let expected: &[(&str, i64)] = &[
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
    for &(table, expected_count) in expected {
        let row = client
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .unwrap();
        let count: i64 = row.get(0);
        assert_eq!(count, expected_count, "Row count mismatch for {table}");
    }
}

#[test]
#[ignore]
fn test_import_skips_missing_file() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    let missing_spec = TableSpec {
        dump_file: "nonexistent_file",
        table: "mb_area_type",
        source_indices: &[0, 1],
        db_columns: &["id", "name"],
    };
    let count = import::import_table(&mut client, &missing_spec, &fixtures_dir()).unwrap();
    assert_eq!(count, 0, "Missing file should return 0 rows");
}

#[test]
#[ignore]
fn test_import_recording_tables() {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();

    // Import all prerequisites
    import::import_all(&mut client, &fixtures_dir()).unwrap();

    let recording_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_recording", &[])
        .unwrap()
        .get(0);
    assert_eq!(recording_count, 5);

    let medium_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_medium", &[])
        .unwrap()
        .get(0);
    assert_eq!(medium_count, 3);

    let track_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_track", &[])
        .unwrap()
        .get(0);
    assert_eq!(track_count, 5);

    // Verify recording GID is stored correctly
    let row = client
        .query_one("SELECT gid::text FROM mb_recording WHERE id = 1", &[])
        .unwrap();
    assert_eq!(
        row.get::<_, &str>(0),
        "bbbb1111-1111-1111-1111-111111111111"
    );
}
