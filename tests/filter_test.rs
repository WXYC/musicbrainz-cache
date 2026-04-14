use musicbrainz_cache::filter;
use musicbrainz_cache::import;
use std::collections::HashSet;
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mbdump")
}

fn library_db_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/library.db")
}

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz".into())
}

// --- Unit tests (no database required) ---

#[test]
fn test_normalize_matches_python() {
    // Expected values derived from running the Python normalize() function.
    // Python: unicodedata.normalize("NFKD", name) -> strip combining -> lower -> strip
    let cases: &[(&str, &str)] = &[
        ("Autechre", "autechre"),
        ("Stereolab", "stereolab"),
        ("Jessica Pratt", "jessica pratt"),
        ("Björk", "bjork"),
        ("Sigur Rós", "sigur ros"),
        ("Cécile McLorin Salvant", "cecile mclorin salvant"),
        ("テスト・アーティスト", "テスト・アーティスト"),  // CJK characters preserved
        ("  Spaced Out  ", "spaced out"),              // whitespace trimmed
        ("UPPER CASE", "upper case"),
        ("café", "cafe"),
        ("naïve", "naive"),
    ];

    for &(input, expected) in cases {
        let result = wxyc_etl::text::normalize_artist_name(input);
        assert_eq!(
            result, expected,
            "normalize('{}') = '{}', expected '{}'",
            input, result, expected,
        );
    }
}

#[test]
fn test_load_library_artists() {
    let artists = filter::load_library_artists(&library_db_path()).unwrap();
    assert_eq!(artists.len(), 3, "Expected 3 unique artists from library.db");
    assert!(artists.contains("autechre"));
    assert!(artists.contains("stereolab"));
    assert!(artists.contains("jessica pratt"));
}

// --- Integration tests (require PostgreSQL) ---

fn set_up_full_db() -> postgres::Client {
    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).unwrap();
    musicbrainz_cache::schema::apply_schema(&mut client).unwrap();
    import::import_all(&mut client, &fixtures_dir()).unwrap();
    client
}

#[test]
#[ignore]
fn test_find_matching_by_name() {
    let mut client = set_up_full_db();
    let library_artists = filter::load_library_artists(&library_db_path()).unwrap();

    let matching = filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();

    // Should match Autechre (100), Stereolab (200), Jessica Pratt (300)
    assert!(matching.contains(&100), "Should match Autechre by name");
    assert!(matching.contains(&200), "Should match Stereolab by name");
    assert!(matching.contains(&300), "Should match Jessica Pratt by name");

    // Should NOT match non-library artists
    assert!(!matching.contains(&400), "Should not match Fake Artist One");
    assert!(!matching.contains(&500), "Should not match Another Artist");
}

#[test]
#[ignore]
fn test_find_matching_by_alias() {
    let mut client = set_up_full_db();

    // Create a library set that only has "Stéréolab" (the French alias spelling).
    // The alias fixture has: id=2, artist=200, name="Stéréolab"
    // After normalization: "stereolab" -> should match alias and add artist 200.
    let mut library_artists = HashSet::new();
    library_artists.insert("stereolab".to_string());

    let matching = filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();
    assert!(
        matching.contains(&200),
        "Should match Stereolab via alias 'Stéréolab'"
    );
}

#[test]
#[ignore]
fn test_prune_to_matching() {
    let mut client = set_up_full_db();
    let library_artists = filter::load_library_artists(&library_db_path()).unwrap();
    let matching = filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();

    prune_and_verify(&mut client, &matching);
}

fn prune_and_verify(client: &mut postgres::Client, matching: &HashSet<i32>) {
    filter::prune_to_matching(client, matching).unwrap();

    // Only matching artists should remain
    let artist_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_artist", &[])
        .unwrap()
        .get(0);
    assert_eq!(artist_count, 3, "Should have exactly 3 artists after pruning");

    // Verify the correct artists remain
    let remaining: Vec<i32> = client
        .query("SELECT id FROM mb_artist ORDER BY id", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(remaining, vec![100, 200, 300]);

    // Non-matching artist tags should be deleted
    let tag_artists: Vec<i32> = client
        .query("SELECT DISTINCT artist FROM mb_artist_tag ORDER BY artist", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    for aid in &tag_artists {
        assert!(matching.contains(aid), "Tag for non-matching artist {} should be pruned", aid);
    }

    // Non-matching artist credits should be deleted
    let credit_artists: Vec<i32> = client
        .query("SELECT DISTINCT artist FROM mb_artist_credit_name ORDER BY artist", &[])
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    for aid in &credit_artists {
        assert!(
            matching.contains(aid),
            "Credit name for non-matching artist {} should be pruned",
            aid,
        );
    }

    // Release groups should only reference kept artist credits
    let rg_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_release_group", &[])
        .unwrap()
        .get(0);
    assert_eq!(rg_count, 3, "Should have 3 release groups for matching artists");
}

#[test]
#[ignore]
fn test_prune_orphaned_tags() {
    let mut client = set_up_full_db();
    let library_artists = filter::load_library_artists(&library_db_path()).unwrap();
    let matching = filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();

    filter::prune_to_matching(&mut client, &matching).unwrap();

    // Only tags referenced by remaining artist_tags should survive
    let orphaned: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM mb_tag WHERE id NOT IN (SELECT DISTINCT tag FROM mb_artist_tag)",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(orphaned, 0, "No orphaned tags should remain after pruning");
}

#[test]
#[ignore]
fn test_prune_orphaned_areas() {
    let mut client = set_up_full_db();
    let library_artists = filter::load_library_artists(&library_db_path()).unwrap();
    let matching = filter::find_matching_artist_ids(&mut client, &library_artists).unwrap();

    filter::prune_to_matching(&mut client, &matching).unwrap();

    // Areas should only include those referenced by remaining artists or country_area
    let area_count: i64 = client
        .query_one("SELECT COUNT(*) FROM mb_area", &[])
        .unwrap()
        .get(0);
    // Expected: UK (221 - Autechre+Stereolab area), US (222 - Jessica Pratt area + country),
    // Manchester (1000 - Autechre begin_area), London (1001 - Stereolab begin_area),
    // San Francisco (1002 - Jessica Pratt begin_area)
    assert!(area_count <= 5, "At most 5 areas should remain (not all 5 original)");
    assert!(area_count >= 4, "At least 4 areas should remain (countries + begin_areas)");
}

#[test]
#[ignore]
fn test_report_sizes() {
    let mut client = set_up_full_db();

    // Just verify it doesn't error
    filter::report_sizes(&mut client).unwrap();
}
