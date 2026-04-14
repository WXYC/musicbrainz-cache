#!/usr/bin/env python3
"""Download and extract MusicBrainz data dumps.

Fetches the latest mbdump.tar.bz2 and mbdump-derived.tar.bz2 from
data.metabrainz.org, then extracts only the table files we need.

Usage:
    python scripts/download_dump.py --output-dir data/
    python scripts/download_dump.py --output-dir data/ --skip-download  # extract only
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport"

# Files we need from each archive
CORE_FILES = {
    "artist",
    "artist_alias",
    "area",
    "area_type",
    "country_area",
    "gender",
    "artist_credit",
    "artist_credit_name",
    "release_group",
    "url",
    "link",
    "link_type",
    "release",
    "l_release_group_url",
    "l_release_url",
}

DERIVED_FILES = {
    "artist_tag",
    "tag",
}

ARCHIVES = [
    ("mbdump.tar.bz2", CORE_FILES),
    ("mbdump-derived.tar.bz2", DERIVED_FILES),
]


def _find_decompressor() -> str | None:
    """Return the path to lbzip2 or pbzip2, or None if neither is installed."""
    for tool in ("lbzip2", "pbzip2"):
        path = shutil.which(tool)
        if path is not None:
            return path
    return None


def _extract_tables_subprocess(
    archive_path: Path,
    needed_files: set[str],
    output_dir: Path,
    decompressor: str,
    archive_prefix: str = "mbdump",
) -> bool:
    """Extract tables using a parallel decompressor piped to tar.

    Returns True on success, False on failure (caller should fall back).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    patterns = [f"{archive_prefix}/{name}" for name in sorted(needed_files)]

    try:
        decomp_proc = subprocess.Popen(
            [decompressor, "-dc", str(archive_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        tar_proc = subprocess.Popen(
            ["tar", "xf", "-", "--strip-components=1", "-C", str(output_dir)] + patterns,
            stdin=decomp_proc.stdout,
            stderr=subprocess.PIPE,
        )
        # Allow decomp_proc to receive SIGPIPE if tar exits early
        decomp_proc.stdout.close()

        _, tar_stderr = tar_proc.communicate()
        decomp_proc.wait()

        if tar_proc.returncode != 0:
            logger.warning(
                "tar failed (exit %d): %s",
                tar_proc.returncode,
                tar_stderr.decode(errors="replace").strip(),
            )
            return False

        return True

    except OSError as exc:
        logger.warning("Subprocess extraction failed: %s", exc)
        return False


def _extract_tables_streaming(
    archive_path: Path,
    needed_files: set[str],
    output_dir: Path,
) -> None:
    """Extract tables using Python's tarfile in streaming mode with early exit.

    Uses 'r|bz2' (streaming mode) instead of 'r:bz2' (seekable mode)
    to avoid EOFError on truncated archive tails.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    remaining = set(needed_files)

    with tarfile.open(archive_path, "r|bz2") as tar:
        for member in tar:
            if not remaining:
                break
            name = member.name.split("/")[-1] if "/" in member.name else member.name
            if name in remaining:
                member.name = name
                tar.extract(member, output_dir)
                size_mb = member.size / (1024 * 1024)
                logger.info("  Extracted %s (%.1f MB)", name, size_mb)
                remaining.discard(name)


def find_latest_dump_url() -> str:
    """Find the URL of the latest full export directory."""
    import httpx

    logger.info("Finding latest dump at %s", BASE_URL)
    resp = httpx.get(f"{BASE_URL}/", follow_redirects=True, timeout=30)
    resp.raise_for_status()

    # Parse directory listing for date-stamped directories (YYYYMMDD-HHMMSS)
    import re

    dates = re.findall(r'href="(\d{8}-\d{6})/"', resp.text)
    if not dates:
        raise RuntimeError(f"No dump directories found at {BASE_URL}")

    latest = sorted(dates)[-1]
    url = f"{BASE_URL}/{latest}"
    logger.info("Latest dump: %s", url)
    return url


def download_file(url: str, dest: Path) -> None:
    """Download a file with progress logging."""
    import httpx

    if dest.exists():
        logger.info("Already downloaded: %s", dest.name)
        return

    logger.info("Downloading %s ...", url)
    start = time.time()

    with httpx.stream("GET", url, follow_redirects=True, timeout=None) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0

        with open(dest, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total and downloaded % (100 * 1024 * 1024) < len(chunk):
                    pct = 100 * downloaded / total
                    logger.info(
                        "  %.0f%% (%d MB / %d MB)",
                        pct,
                        downloaded // (1024 * 1024),
                        total // (1024 * 1024),
                    )

    elapsed = time.time() - start
    size_mb = dest.stat().st_size / (1024 * 1024)
    logger.info("Downloaded %s: %.0f MB in %.0fs", dest.name, size_mb, elapsed)


def extract_tables(archive_path: Path, needed_files: set[str], output_dir: Path) -> None:
    """Extract only the needed table files from a tar.bz2 archive.

    Attempts parallel decompression via lbzip2/pbzip2 first, falls back
    to Python's tarfile in streaming mode.
    """
    logger.info("Extracting from %s ...", archive_path.name)
    output_dir.mkdir(parents=True, exist_ok=True)

    decompressor = _find_decompressor()
    extracted_via_subprocess = False

    if decompressor:
        logger.info("Using parallel decompressor: %s", decompressor)
        extracted_via_subprocess = _extract_tables_subprocess(
            archive_path, needed_files, output_dir, decompressor
        )

    if not extracted_via_subprocess:
        if decompressor:
            logger.info("Subprocess extraction failed, falling back to Python tarfile")
        _extract_tables_streaming(archive_path, needed_files, output_dir)

    extracted = [f for f in needed_files if (output_dir / f).exists()]
    missing = needed_files - set(extracted)
    if missing:
        logger.warning("Missing files: %s", ", ".join(sorted(missing)))
    logger.info("Extracted %d/%d files", len(extracted), len(needed_files))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download and extract MusicBrainz data dumps.")
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="Directory for downloads and extracted files"
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip download, extract existing archives"
    )
    parser.add_argument("--dump-url", help="Override dump URL (default: auto-detect latest)")
    args = parser.parse_args(argv)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    dump_url = None
    if not args.skip_download:
        dump_url = args.dump_url or find_latest_dump_url()

    # Download phase (sequential -- network bound)
    for archive_name, _needed_files in ARCHIVES:
        archive_path = args.output_dir / archive_name
        if not args.skip_download:
            download_file(f"{dump_url}/{archive_name}", archive_path)

    # Extraction phase (parallel -- CPU bound)
    extraction_tasks = []
    for archive_name, needed_files in ARCHIVES:
        archive_path = args.output_dir / archive_name
        if not archive_path.exists():
            logger.error("Archive not found: %s", archive_path)
            continue
        extraction_tasks.append((archive_path, needed_files))

    mbdump_dir = args.output_dir / "mbdump"
    if len(extraction_tasks) > 1:
        with ThreadPoolExecutor(max_workers=len(extraction_tasks)) as executor:
            futures = {
                executor.submit(extract_tables, archive_path, needed_files, mbdump_dir): (
                    archive_path.name
                )
                for archive_path, needed_files in extraction_tasks
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception("Extraction failed for %s", name)
                    raise
    elif extraction_tasks:
        archive_path, needed_files = extraction_tasks[0]
        extract_tables(archive_path, needed_files, mbdump_dir)

    logger.info("Done. Extracted files in %s", mbdump_dir)


if __name__ == "__main__":
    main()
