use musicbrainz_cache::download::{ARCHIVES, CORE_FILES, DERIVED_FILES};
use std::collections::HashSet;

#[test]
fn test_core_files_count() {
    // 9 original + 3 recording tables = 12
    assert_eq!(CORE_FILES.len(), 12);
}

#[test]
fn test_derived_files_count() {
    assert_eq!(DERIVED_FILES.len(), 2);
}

#[test]
fn test_no_overlap_between_core_and_derived() {
    let core: HashSet<&str> = CORE_FILES.iter().copied().collect();
    let derived: HashSet<&str> = DERIVED_FILES.iter().copied().collect();
    let overlap: Vec<&&str> = core.intersection(&derived).collect();
    assert!(
        overlap.is_empty(),
        "Overlap between core and derived: {:?}",
        overlap
    );
}

#[test]
fn test_archives_cover_all_files() {
    let mut all_files: HashSet<&str> = HashSet::new();
    for &(_, files) in ARCHIVES {
        for &f in files {
            all_files.insert(f);
        }
    }
    // All import table dump_files should be covered
    for spec in musicbrainz_cache::import::TABLES {
        assert!(
            all_files.contains(spec.dump_file),
            "Table {} (dump_file='{}') not covered by any archive",
            spec.table,
            spec.dump_file,
        );
    }
}

#[test]
fn test_recording_tables_in_core() {
    assert!(CORE_FILES.contains(&"recording"));
    assert!(CORE_FILES.contains(&"medium"));
    assert!(CORE_FILES.contains(&"track"));
}
