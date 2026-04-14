use std::path::Path;

/// Find the URL of the latest MusicBrainz full export.
pub fn find_latest_dump_url() -> anyhow::Result<String> {
    todo!()
}

/// Download a file with progress logging.
pub fn download_file(_url: &str, _dest: &Path) -> anyhow::Result<()> {
    todo!()
}

/// Extract only the needed table files from a tar.bz2 archive.
pub fn extract_tables(
    _archive_path: &Path,
    _needed_files: &[&str],
    _output_dir: &Path,
) -> anyhow::Result<()> {
    todo!()
}
