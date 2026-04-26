use musicbrainz_cache::download::{self, ARCHIVES, CORE_FILES, DERIVED_FILES};
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

// --- tar.bz2 extraction tests ---

/// Create a tiny tar.bz2 archive containing the specified files under a prefix directory.
fn create_test_archive(archive_path: &std::path::Path, prefix: &str, files: &[(&str, &[u8])]) {
    let bz2_file = std::fs::File::create(archive_path).unwrap();
    let encoder = bzip2::write::BzEncoder::new(bz2_file, bzip2::Compression::fast());
    let mut builder = tar::Builder::new(encoder);

    for (name, data) in files {
        let full_path = format!("{prefix}/{name}");
        let mut header = tar::Header::new_gnu();
        header.set_path(&full_path).unwrap();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, *data).unwrap();
    }

    builder.into_inner().unwrap().finish().unwrap();
}

#[test]
fn test_extract_tables_from_archive() {
    let tmp = tempfile::tempdir().unwrap();
    let archive_path = tmp.path().join("mbdump.tar.bz2");
    let output_dir = tmp.path().join("output");

    // Create archive with 3 files: artist, tag, extra_table
    create_test_archive(
        &archive_path,
        "mbdump",
        &[
            ("artist", b"100\tAutechre\n200\tStereolab\n"),
            ("tag", b"1\telectronic\n2\trock\n"),
            ("extra_table", b"should not be extracted\n"),
        ],
    );

    // Extract only artist and tag
    download::extract_tables(&archive_path, &["artist", "tag"], &output_dir).unwrap();

    // Verify correct files extracted
    assert!(
        output_dir.join("artist").exists(),
        "artist should be extracted"
    );
    assert!(output_dir.join("tag").exists(), "tag should be extracted");
    assert!(
        !output_dir.join("extra_table").exists(),
        "extra_table should NOT be extracted"
    );

    // Verify content is correct
    let artist_content = std::fs::read_to_string(output_dir.join("artist")).unwrap();
    assert!(artist_content.contains("Autechre"));
    assert!(artist_content.contains("Stereolab"));

    let tag_content = std::fs::read_to_string(output_dir.join("tag")).unwrap();
    assert!(tag_content.contains("electronic"));
    assert!(tag_content.contains("rock"));
}

#[test]
fn test_extract_handles_missing_files_gracefully() {
    let tmp = tempfile::tempdir().unwrap();
    let archive_path = tmp.path().join("mbdump.tar.bz2");
    let output_dir = tmp.path().join("output");

    // Archive only has "artist"
    create_test_archive(&archive_path, "mbdump", &[("artist", b"100\tAutechre\n")]);

    // Request both "artist" and "nonexistent"
    download::extract_tables(&archive_path, &["artist", "nonexistent"], &output_dir).unwrap();

    // "artist" should be extracted, "nonexistent" just logged as missing
    assert!(output_dir.join("artist").exists());
    assert!(!output_dir.join("nonexistent").exists());
}

#[test]
fn test_extract_creates_output_directory() {
    let tmp = tempfile::tempdir().unwrap();
    let archive_path = tmp.path().join("mbdump.tar.bz2");
    let output_dir = tmp.path().join("nested").join("output");

    create_test_archive(&archive_path, "mbdump", &[("artist", b"100\tAutechre\n")]);

    // output_dir doesn't exist yet
    assert!(!output_dir.exists());

    download::extract_tables(&archive_path, &["artist"], &output_dir).unwrap();

    assert!(output_dir.exists());
    assert!(output_dir.join("artist").exists());
}

#[test]
fn test_extract_empty_needed_set() {
    let tmp = tempfile::tempdir().unwrap();
    let archive_path = tmp.path().join("mbdump.tar.bz2");
    let output_dir = tmp.path().join("output");

    create_test_archive(&archive_path, "mbdump", &[("artist", b"100\tAutechre\n")]);

    // Request no files -- should succeed without extracting anything
    download::extract_tables(&archive_path, &[], &output_dir).unwrap();

    // Output dir created but no files extracted
    assert!(output_dir.exists());
    assert!(!output_dir.join("artist").exists());
}

// --- HTTP download tests ---
//
// Exercises the reqwest-backed `download_file` against a local tiny_http
// server. Gated behind TEST_DOWNLOAD=1 to avoid network/server flakiness
// in default `cargo test` runs.

#[test]
fn test_download_and_extract_archive_from_local_server() {
    if std::env::var("TEST_DOWNLOAD").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let archive_path = tmp.path().join("source.tar.bz2");

    // Build a real tar.bz2 once and reuse the existing helper.
    create_test_archive(
        &archive_path,
        "mbdump",
        &[
            ("artist", b"100\tAutechre\n200\tStereolab\n"),
            ("tag", b"1\telectronic\n"),
        ],
    );
    let archive_bytes = std::fs::read(&archive_path).unwrap();

    // Bind tiny_http on an ephemeral port so parallel test runs don't collide.
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/dump.tar.bz2");

    // Serve a single request, then return.
    let archive_bytes_for_server = archive_bytes.clone();
    let server_thread = std::thread::spawn(move || {
        if let Ok(request) = server.recv() {
            assert_eq!(request.url(), "/dump.tar.bz2");
            let response = tiny_http::Response::from_data(archive_bytes_for_server);
            request.respond(response).unwrap();
        }
        // Server drops here, releasing the port.
    });

    // Exercise the real download path.
    let dest_path = tmp.path().join("downloaded.tar.bz2");
    download::download_file(&url, &dest_path).expect("download_file should succeed");

    server_thread.join().unwrap();

    // Downloaded bytes must match the served bytes exactly.
    let downloaded_bytes = std::fs::read(&dest_path).unwrap();
    assert_eq!(
        downloaded_bytes.len(),
        archive_bytes.len(),
        "Downloaded size mismatch"
    );
    assert_eq!(
        downloaded_bytes, archive_bytes,
        "Downloaded bytes differ from served bytes"
    );

    // And the downloaded archive should extract through the real extract path.
    let output_dir = tmp.path().join("extracted");
    download::extract_tables(&dest_path, &["artist", "tag"], &output_dir).unwrap();
    assert!(output_dir.join("artist").exists());
    assert!(output_dir.join("tag").exists());

    let artist_content = std::fs::read_to_string(output_dir.join("artist")).unwrap();
    assert!(artist_content.contains("Autechre"));
    assert!(artist_content.contains("Stereolab"));
}

#[test]
fn test_download_skips_when_destination_exists() {
    if std::env::var("TEST_DOWNLOAD").is_err() {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let dest_path = tmp.path().join("already_here.bin");
    std::fs::write(&dest_path, b"pre-existing content").unwrap();

    // Bind a server but expect it to receive zero requests.
    let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
    let port = server.server_addr().to_ip().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/never-fetched");

    // Quietly drop the server after a short window. If `download_file`
    // incorrectly issued a request, recv would observe it; we don't call
    // recv because we expect no traffic.
    drop(server);

    download::download_file(&url, &dest_path).unwrap();

    // Content must be untouched (no overwrite of existing file).
    let content = std::fs::read(&dest_path).unwrap();
    assert_eq!(content, b"pre-existing content");
}
