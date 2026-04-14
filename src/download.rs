use anyhow::{bail, Context};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

const BASE_URL: &str = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport";

/// Files we need from mbdump.tar.bz2.
pub const CORE_FILES: &[&str] = &[
    "artist",
    "artist_alias",
    "area",
    "area_type",
    "country_area",
    "gender",
    "artist_credit",
    "artist_credit_name",
    "release_group",
    "recording",
    "medium",
    "track",
];

/// Files we need from mbdump-derived.tar.bz2.
pub const DERIVED_FILES: &[&str] = &["artist_tag", "tag"];

/// Archives to download and their needed files.
pub const ARCHIVES: &[(&str, &[&str])] = &[
    ("mbdump.tar.bz2", CORE_FILES),
    ("mbdump-derived.tar.bz2", DERIVED_FILES),
];

/// Find the URL of the latest MusicBrainz full export.
pub fn find_latest_dump_url() -> anyhow::Result<String> {
    log::info!("Finding latest dump at {}", BASE_URL);

    let resp = reqwest::blocking::get(format!("{BASE_URL}/"))
        .context("Failed to fetch dump directory listing")?
        .text()?;

    // Parse directory listing for date-stamped directories (YYYYMMDD-HHMMSS)
    let re = regex_lite::Regex::new(r#"href="(\d{8}-\d{6})/""#).unwrap();
    let mut dates: Vec<&str> = re.captures_iter(&resp).map(|c| c.get(1).unwrap().as_str()).collect();

    if dates.is_empty() {
        bail!("No dump directories found at {BASE_URL}");
    }

    dates.sort();
    let latest = dates.last().unwrap();
    let url = format!("{BASE_URL}/{latest}");
    log::info!("Latest dump: {}", url);
    Ok(url)
}

/// Download a file with progress logging.
pub fn download_file(url: &str, dest: &Path) -> anyhow::Result<()> {
    if dest.exists() {
        log::info!("Already downloaded: {}", dest.display());
        return Ok(());
    }

    log::info!("Downloading {} ...", url);
    let start = std::time::Instant::now();

    let resp = reqwest::blocking::get(url)
        .with_context(|| format!("Failed to download {url}"))?
        .error_for_status()?;

    let total = resp.content_length().unwrap_or(0);
    let mut downloaded: u64 = 0;
    let mut reader = resp;
    let mut file = std::fs::File::create(dest)
        .with_context(|| format!("Failed to create {}", dest.display()))?;

    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        std::io::Write::write_all(&mut file, &buf[..n])?;
        downloaded += n as u64;
        if total > 0 && downloaded % (100 * 1024 * 1024) < n as u64 {
            let pct = 100.0 * downloaded as f64 / total as f64;
            log::info!(
                "  {:.0}% ({} MB / {} MB)",
                pct,
                downloaded / (1024 * 1024),
                total / (1024 * 1024),
            );
        }
    }

    let elapsed = start.elapsed();
    let size_mb = dest.metadata()?.len() as f64 / (1024.0 * 1024.0);
    log::info!(
        "Downloaded {}: {:.0} MB in {:.0}s",
        dest.file_name().unwrap_or_default().to_string_lossy(),
        size_mb,
        elapsed.as_secs_f64(),
    );
    Ok(())
}

/// Find a parallel bzip2 decompressor (lbzip2 or pbzip2).
fn find_decompressor() -> Option<PathBuf> {
    for tool in &["lbzip2", "pbzip2"] {
        if let Ok(output) = Command::new("which").arg(tool).output() {
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !path.is_empty() {
                    return Some(PathBuf::from(path));
                }
            }
        }
    }
    None
}

/// Extract tables using a parallel decompressor piped to tar.
fn extract_tables_subprocess(
    archive_path: &Path,
    needed_files: &[&str],
    output_dir: &Path,
    decompressor: &Path,
    archive_prefix: &str,
) -> bool {
    let patterns: Vec<String> = needed_files
        .iter()
        .map(|f| format!("{archive_prefix}/{f}"))
        .collect();

    let decomp = match Command::new(decompressor)
        .args(["-dc", &archive_path.to_string_lossy()])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(p) => p,
        Err(e) => {
            log::warn!("Failed to spawn decompressor: {}", e);
            return false;
        }
    };

    let mut tar_args = vec![
        "xf".to_string(),
        "-".to_string(),
        "--strip-components=1".to_string(),
        "-C".to_string(),
        output_dir.to_string_lossy().to_string(),
    ];
    tar_args.extend(patterns);

    let tar_result = Command::new("tar")
        .args(&tar_args)
        .stdin(decomp.stdout.unwrap())
        .stderr(std::process::Stdio::piped())
        .output();

    match tar_result {
        Ok(output) if output.status.success() => true,
        Ok(output) => {
            log::warn!(
                "tar failed (exit {}): {}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stderr).trim(),
            );
            false
        }
        Err(e) => {
            log::warn!("Failed to run tar: {}", e);
            false
        }
    }
}

/// Extract tables using Rust's bzip2 + tar crates (fallback).
fn extract_tables_streaming(
    archive_path: &Path,
    needed_files: &[&str],
    output_dir: &Path,
) -> anyhow::Result<()> {
    let file = std::fs::File::open(archive_path)
        .with_context(|| format!("Failed to open {}", archive_path.display()))?;
    let decompressor = bzip2::read::BzDecoder::new(file);
    let mut archive = tar::Archive::new(decompressor);

    let needed: HashSet<&str> = needed_files.iter().copied().collect();
    let mut remaining: HashSet<&str> = needed.clone();

    for entry in archive.entries()? {
        if remaining.is_empty() {
            break;
        }
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();

        if remaining.contains(name) {
            let dest = output_dir.join(name);
            entry.unpack(&dest)?;
            let size_mb = entry.size() as f64 / (1024.0 * 1024.0);
            log::info!("  Extracted {} ({:.1} MB)", name, size_mb);
            remaining.remove(name);
        }
    }

    Ok(())
}

/// Extract only the needed table files from a tar.bz2 archive.
///
/// Attempts parallel decompression via lbzip2/pbzip2 first, falls back
/// to Rust's bzip2 + tar crates.
pub fn extract_tables(
    archive_path: &Path,
    needed_files: &[&str],
    output_dir: &Path,
) -> anyhow::Result<()> {
    log::info!("Extracting from {} ...", archive_path.display());
    std::fs::create_dir_all(output_dir)?;

    let mut extracted_via_subprocess = false;

    if let Some(decompressor) = find_decompressor() {
        log::info!("Using parallel decompressor: {}", decompressor.display());
        extracted_via_subprocess = extract_tables_subprocess(
            archive_path,
            needed_files,
            output_dir,
            &decompressor,
            "mbdump",
        );
    }

    if !extracted_via_subprocess {
        if find_decompressor().is_some() {
            log::info!("Subprocess extraction failed, falling back to Rust bzip2+tar");
        }
        extract_tables_streaming(archive_path, needed_files, output_dir)?;
    }

    let extracted_count = needed_files
        .iter()
        .filter(|f| output_dir.join(f).exists())
        .count();
    let missing: Vec<&&str> = needed_files
        .iter()
        .filter(|f| !output_dir.join(f).exists())
        .collect();
    if !missing.is_empty() {
        log::warn!("Missing files: {:?}", missing);
    }
    log::info!(
        "Extracted {}/{} files",
        extracted_count,
        needed_files.len()
    );
    Ok(())
}
