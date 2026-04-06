#!/usr/bin/env python3
"""Filter MusicBrainz data to only WXYC library artists.

Matches MB artists against WXYC library.db by normalized name,
including artist aliases. Prunes all non-matching artists and their
dependent data (tags, credits, release groups).

Usage:
    python scripts/filter_artists.py \
        --library-db data/library.db \
        --database-url postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import time
import unicodedata
from pathlib import Path

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def normalize(name: str) -> str:
    """Normalize an artist name for matching (same as discogs-cache)."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def load_library_artists(library_db: Path) -> set[str]:
    """Load normalized WXYC artist names from library.db."""
    conn = sqlite3.connect(str(library_db))
    rows = conn.execute("SELECT DISTINCT artist FROM library").fetchall()
    conn.close()

    artists = set()
    for (name,) in rows:
        if name:
            artists.add(normalize(name))

    logger.info("Loaded %d unique WXYC artist names from library.db", len(artists))
    return artists


def find_matching_artist_ids(conn: psycopg.Connection, library_artists: set[str]) -> set[int]:
    """Find MB artist IDs matching WXYC library artists by name or alias."""
    matching_ids: set[int] = set()

    # Match by primary artist name
    logger.info("Matching by artist name...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM mb_artist")
        checked = 0
        for row in cur:
            if normalize(row[1]) in library_artists:
                matching_ids.add(row[0])
            checked += 1
            if checked % 500_000 == 0:
                logger.info("  Checked %d artists, %d matches so far", checked, len(matching_ids))

    name_matches = len(matching_ids)
    logger.info("Found %d matches by artist name", name_matches)

    # Match by artist alias
    logger.info("Matching by artist alias...")
    with conn.cursor() as cur:
        cur.execute("SELECT artist, name FROM mb_artist_alias")
        for row in cur:
            if normalize(row[1]) in library_artists:
                matching_ids.add(row[0])

    alias_matches = len(matching_ids) - name_matches
    logger.info("Found %d additional matches by alias (%d total)", alias_matches, len(matching_ids))

    return matching_ids


def prune_to_matching(conn: psycopg.Connection, matching_ids: set[int]) -> None:
    """Delete all non-matching artists and cascade to dependent tables."""
    logger.info("Pruning to %d matching artists...", len(matching_ids))
    start = time.time()

    with conn.cursor() as cur:
        # Load matching IDs into a temp table for efficient joins
        cur.execute("CREATE TEMP TABLE _keep_ids (id integer PRIMARY KEY)")
        with cur.copy("COPY _keep_ids (id) FROM STDIN") as copy:
            for aid in matching_ids:
                copy.write_row((aid,))
        conn.commit()

        # Count before
        cur.execute("SELECT COUNT(*) FROM mb_artist")
        before = cur.fetchone()[0]

        # Delete non-matching artists (FK CASCADE handles dependent tables)
        cur.execute("DELETE FROM mb_artist WHERE id NOT IN (SELECT id FROM _keep_ids)")
        deleted = cur.rowcount
        conn.commit()

        # Also prune artist_credits that no longer have any linked artist
        cur.execute("""
            DELETE FROM mb_artist_credit
            WHERE id NOT IN (SELECT DISTINCT artist_credit FROM mb_artist_credit_name)
        """)
        credit_deleted = cur.rowcount
        conn.commit()

        # Prune release_groups with no valid artist_credit
        cur.execute("""
            DELETE FROM mb_release_group
            WHERE artist_credit NOT IN (SELECT id FROM mb_artist_credit)
        """)
        rg_deleted = cur.rowcount
        conn.commit()

        # Prune orphaned tags
        cur.execute("""
            DELETE FROM mb_tag
            WHERE id NOT IN (SELECT DISTINCT tag FROM mb_artist_tag)
        """)
        tag_deleted = cur.rowcount
        conn.commit()

        # Prune orphaned areas
        cur.execute("""
            DELETE FROM mb_area
            WHERE id NOT IN (
                SELECT area FROM mb_artist WHERE area IS NOT NULL
                UNION SELECT begin_area FROM mb_artist WHERE begin_area IS NOT NULL
            )
            AND id NOT IN (SELECT area FROM mb_country_area)
        """)
        area_deleted = cur.rowcount
        conn.commit()

        cur.execute("DROP TABLE _keep_ids")
        conn.commit()

    elapsed = time.time() - start
    logger.info(
        "Pruned in %.1fs: %d/%d artists deleted, %d credits, %d release_groups, %d tags, %d areas",
        elapsed,
        deleted,
        before,
        credit_deleted,
        rg_deleted,
        tag_deleted,
        area_deleted,
    )


def report_sizes(conn: psycopg.Connection) -> None:
    """Report row counts for all tables."""
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
    ]
    logger.info("Table sizes after filtering:")
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            logger.info("  %-30s %10d rows", table, count)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Filter MusicBrainz data to WXYC library artists.")
    parser.add_argument("--library-db", type=Path, required=True, help="Path to library.db")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL", "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz"
        ),
    )
    args = parser.parse_args(argv)

    library_artists = load_library_artists(args.library_db)

    conn = psycopg.connect(args.database_url, autocommit=True)
    matching_ids = find_matching_artist_ids(conn, library_artists)
    prune_to_matching(conn, matching_ids)
    report_sizes(conn)
    conn.close()


if __name__ == "__main__":
    main()
