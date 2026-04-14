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


def _save_kept(
    cur: psycopg.Cursor, table: str, where_clause: str
) -> tuple[str, str, int]:
    """Copy kept rows into a temp table. Returns (table, temp_name, row_count)."""
    temp = f"_kept_{table}"
    start = time.time()
    cur.execute(f"CREATE TEMP TABLE {temp} AS SELECT * FROM {table} WHERE {where_clause}")
    kept = cur.rowcount
    logger.info("  %s: keeping %d rows (%.1fs)", table, kept, time.time() - start)
    return table, temp, kept


def prune_to_matching(conn: psycopg.Connection, matching_ids: set[int]) -> None:
    """Prune to matching artists using copy-and-swap.

    Instead of deleting millions of non-matching rows (slow, generates dead tuples),
    copies the kept rows into temp tables, truncates the originals, and re-inserts.
    """
    logger.info("Pruning to %d matching artists...", len(matching_ids))
    start = time.time()

    with conn.cursor() as cur:
        # Load matching IDs into a temp table
        cur.execute("CREATE TEMP TABLE _keep_ids (id integer PRIMARY KEY)")
        with cur.copy("COPY _keep_ids (id) FROM STDIN") as copy:
            for aid in matching_ids:
                copy.write_row((aid,))
        conn.commit()

        # Disable FK triggers for the truncate/re-insert
        cur.execute("SET session_replication_role = 'replica'")

        # Phase 1: Save kept rows to temp tables.
        # Order matters: later queries reference earlier temp tables.
        logger.info("Phase 1: selecting kept rows...")
        swaps = []
        swaps.append(_save_kept(cur, "mb_artist", "id IN (SELECT id FROM _keep_ids)"))
        swaps.append(_save_kept(cur, "mb_artist_alias", "artist IN (SELECT id FROM _keep_ids)"))
        swaps.append(_save_kept(cur, "mb_artist_tag", "artist IN (SELECT id FROM _keep_ids)"))
        swaps.append(
            _save_kept(cur, "mb_artist_credit_name", "artist IN (SELECT id FROM _keep_ids)")
        )
        swaps.append(
            _save_kept(
                cur,
                "mb_artist_credit",
                "id IN (SELECT DISTINCT artist_credit FROM _kept_mb_artist_credit_name)",
            )
        )
        swaps.append(
            _save_kept(
                cur,
                "mb_release_group",
                "artist_credit IN (SELECT id FROM _kept_mb_artist_credit)",
            )
        )
        swaps.append(
            _save_kept(
                cur,
                "mb_recording",
                "artist_credit IN (SELECT id FROM _kept_mb_artist_credit)",
            )
        )
        swaps.append(
            _save_kept(cur, "mb_track", "recording IN (SELECT id FROM _kept_mb_recording)")
        )
        swaps.append(
            _save_kept(cur, "mb_medium", "id IN (SELECT DISTINCT medium FROM _kept_mb_track)")
        )
        swaps.append(
            _save_kept(cur, "mb_tag", "id IN (SELECT DISTINCT tag FROM _kept_mb_artist_tag)")
        )
        swaps.append(
            _save_kept(
                cur,
                "mb_area",
                """id IN (
                    SELECT area FROM _kept_mb_artist WHERE area IS NOT NULL
                    UNION SELECT begin_area FROM _kept_mb_artist WHERE begin_area IS NOT NULL
                ) OR id IN (SELECT area FROM mb_country_area)""",
            )
        )

        # Small reference tables: not filtered, but must be included in TRUNCATE
        # because they're referenced by FK constraints from tables we are truncating.
        ref_tables = ["mb_country_area", "mb_area_type", "mb_gender"]
        for rt in ref_tables:
            swaps.append(_save_kept(cur, rt, "TRUE"))

        # Phase 2: Truncate all tables in one statement.
        # Listing all tables together satisfies FK constraints without CASCADE.
        logger.info("Phase 2: truncating tables...")
        all_tables = ", ".join(table for table, _, _ in swaps)
        cur.execute(f"TRUNCATE {all_tables}")
        conn.commit()

        # Phase 3: Re-insert kept rows.
        logger.info("Phase 3: re-inserting kept rows...")
        for table, temp, _ in swaps:
            t = time.time()
            cur.execute(f"INSERT INTO {table} SELECT * FROM {temp}")
            logger.info("  %s: inserted (%.1fs)", table, time.time() - t)
            cur.execute(f"DROP TABLE {temp}")
        conn.commit()

        # Clean up
        cur.execute("DROP TABLE _keep_ids")
        cur.execute("SET session_replication_role = 'DEFAULT'")
        conn.commit()

    elapsed = time.time() - start
    logger.info("Pruning complete in %.1fs", elapsed)


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
        "mb_recording",
        "mb_medium",
        "mb_track",
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
