#!/usr/bin/env python3
"""MusicBrainz cache pipeline orchestrator.

Downloads MusicBrainz data dumps, imports into PostgreSQL, filters to WXYC
library artists, and builds indexes for querying.

Usage:
    # Full pipeline (download + import + filter)
    python scripts/run_pipeline.py --data-dir data/ --library-db data/library.db

    # Import only (skip download, use existing extracted files)
    python scripts/run_pipeline.py --data-dir data/ --library-db data/library.db --skip-download

    # Import without filtering (keep all artists)
    python scripts/run_pipeline.py --data-dir data/ --skip-download --no-filter
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent.parent / "schema"


def wait_for_postgres(db_url: str, timeout: int = 30) -> None:
    """Wait for PostgreSQL to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            conn = psycopg.connect(db_url)
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"PostgreSQL not available after {timeout}s")


def run_step(description: str, cmd: list[str]) -> None:
    """Run a subprocess step with logging."""
    logger.info("=== %s ===", description)
    start = time.time()
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logger.error("Step failed: %s (exit code %d)", description, result.returncode)
        sys.exit(result.returncode)
    elapsed = time.time() - start
    logger.info("=== %s complete (%.1fs) ===\n", description, elapsed)


def apply_schema(db_url: str) -> None:
    """Apply the database schema."""
    logger.info("Applying schema...")
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
    conn.close()
    logger.info("Schema applied.")


def create_indexes(db_url: str) -> None:
    """Create secondary indexes after bulk import."""
    logger.info("Creating indexes...")
    start = time.time()
    conn = psycopg.connect(db_url, autocommit=True)
    sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
    with conn.cursor() as cur:
        for statement in sql.split(";"):
            statement = statement.strip()
            if not statement or statement.startswith("--"):
                continue
            # Extract index name for logging
            parts = statement.split()
            idx_name = parts[2] if len(parts) > 2 else "unknown"
            idx_start = time.time()
            cur.execute(statement)
            logger.info("  %s (%.1fs)", idx_name, time.time() - idx_start)
    conn.close()
    logger.info("Indexes created in %.1fs", time.time() - start)


def run_analyze(db_url: str) -> None:
    """ANALYZE all tables to update planner statistics.

    After copy-and-swap filtering, tables have no dead tuples, so VACUUM FULL
    is unnecessary. ANALYZE updates statistics for the query planner.
    """
    tables = [
        "mb_artist",
        "mb_artist_alias",
        "mb_artist_tag",
        "mb_tag",
        "mb_area",
        "mb_area_type",
        "mb_country_area",
        "mb_gender",
        "mb_artist_credit",
        "mb_artist_credit_name",
        "mb_release_group",
        "mb_recording",
        "mb_medium",
        "mb_track",
    ]
    logger.info("Running ANALYZE...")
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in tables:
            logger.info("  ANALYZE %s...", table)
            start = time.time()
            cur.execute(f"ANALYZE {table}")
            logger.info("  ANALYZE %s done (%.1fs)", table, time.time() - start)
    conn.close()
    logger.info("ANALYZE complete.")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MusicBrainz cache pipeline.")
    parser.add_argument(
        "--data-dir", type=Path, required=True, help="Directory for downloads and extracted files"
    )
    parser.add_argument(
        "--library-db", type=Path, help="Path to library.db (required unless --no-filter)"
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL", "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz"
        ),
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip download, use existing files"
    )
    parser.add_argument(
        "--no-filter", action="store_true", help="Import all artists without filtering"
    )
    parser.add_argument("--dump-url", help="Override dump URL (default: auto-detect latest)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    pipeline_start = time.time()

    if not args.no_filter and not args.library_db:
        print("Error: --library-db required unless --no-filter is set", file=sys.stderr)
        sys.exit(1)

    logger.info("MusicBrainz cache pipeline starting")
    logger.info("  Data dir: %s", args.data_dir)
    logger.info(
        "  Database: %s",
        args.database_url.split("@")[-1] if "@" in args.database_url else args.database_url,
    )
    logger.info("  Filter: %s", "disabled" if args.no_filter else args.library_db)

    # Step 1: Download and extract
    if not args.skip_download:
        download_cmd = [
            sys.executable,
            "scripts/download_dump.py",
            "--output-dir",
            str(args.data_dir),
        ]
        if args.dump_url:
            download_cmd.extend(["--dump-url", args.dump_url])
        run_step("Download and extract MusicBrainz dumps", download_cmd)

    mbdump_dir = args.data_dir / "mbdump"
    if not mbdump_dir.exists():
        logger.error("mbdump directory not found: %s", mbdump_dir)
        sys.exit(1)

    # Step 2: Wait for PostgreSQL
    wait_for_postgres(args.database_url)

    # Step 3: Apply schema
    apply_schema(args.database_url)

    # Step 4: Import TSV files
    run_step(
        "Import TSV files",
        [
            sys.executable,
            "scripts/import_tsv.py",
            "--data-dir",
            str(mbdump_dir),
            "--database-url",
            args.database_url,
        ],
    )

    # Step 5: Filter to WXYC artists
    if not args.no_filter:
        run_step(
            "Filter to WXYC library artists",
            [
                sys.executable,
                "scripts/filter_artists.py",
                "--library-db",
                str(args.library_db),
                "--database-url",
                args.database_url,
            ],
        )

    # Step 6: Create indexes
    create_indexes(args.database_url)

    # Step 7: Analyze
    run_analyze(args.database_url)

    elapsed = time.time() - pipeline_start
    logger.info("Pipeline complete in %.1fs", elapsed)


if __name__ == "__main__":
    main()
