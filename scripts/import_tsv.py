#!/usr/bin/env python3
"""Import MusicBrainz data dump TSV files into PostgreSQL.

MusicBrainz dumps use PostgreSQL COPY TEXT format: tab-separated, \\N for NULL,
no headers. Each file contains ALL columns from the source table. We extract
only the columns we need using positional indexing.

Usage:
    python scripts/import_tsv.py --data-dir data/mbdump --database-url postgresql://...
"""

from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class TableSpec:
    """Mapping from a MusicBrainz dump file to our schema."""

    dump_file: str  # filename inside mbdump/ (e.g., "artist")
    table: str  # target table name (e.g., "mb_artist")
    source_indices: list[int]  # column positions to extract from the TSV (0-based)
    db_columns: list[str]  # corresponding column names in our schema
    null_transform: dict[int, str] | None = None  # source_idx -> default value for NULL


# MusicBrainz source column orders (from CreateTables.sql):
#
# artist: id(0), gid(1), name(2), sort_name(3), begin_date_year(4), begin_date_month(5),
#         begin_date_day(6), end_date_year(7), end_date_month(8), end_date_day(9),
#         type(10), area(11), gender(12), comment(13), edits_pending(14),
#         last_updated(15), ended(16), begin_area(17), end_area(18)
#
# artist_alias: id(0), artist(1), name(2), locale(3), edits_pending(4),
#               last_updated(5), type(6), sort_name(7), begin_date_year(8)...(13),
#               primary_for_locale(14), ended(15)
#
# area: id(0), gid(1), name(2), type(3), edits_pending(4), last_updated(5),
#       begin_date_year(6)...(11), ended(12), comment(13)
#
# area_type: id(0), name(1), parent(2), child_order(3), description(4), gid(5)
#
# country_area: area(0)
#
# gender: id(0), name(1), parent(2), child_order(3), description(4), gid(5)
#
# tag: id(0), name(1), ref_count(2)
#
# artist_tag: artist(0), tag(1), count(2), last_updated(3)
#
# artist_credit: id(0), name(1), artist_count(2), ref_count(3), created(4),
#                edits_pending(5), gid(6)
#
# artist_credit_name: artist_credit(0), position(1), artist(2), name(3), join_phrase(4)
#
# release_group: id(0), gid(1), name(2), artist_credit(3), type(4), comment(5),
#                edits_pending(6), last_updated(7)

TABLES: list[TableSpec] = [
    TableSpec("area_type", "mb_area_type", [0, 1], ["id", "name"]),
    TableSpec("gender", "mb_gender", [0, 1], ["id", "name"]),
    TableSpec("tag", "mb_tag", [0, 1], ["id", "name"]),
    TableSpec("area", "mb_area", [0, 2, 3], ["id", "name", "type"]),
    TableSpec("country_area", "mb_country_area", [0], ["area"]),
    TableSpec(
        "artist",
        "mb_artist",
        [0, 2, 3, 10, 11, 12, 17, 13],
        ["id", "name", "sort_name", "type", "area", "gender", "begin_area", "comment"],
    ),
    TableSpec(
        "artist_alias",
        "mb_artist_alias",
        [0, 1, 2, 7, 3, 6, 14],
        ["id", "artist", "name", "sort_name", "locale", "type", "primary_for_locale"],
    ),
    TableSpec("artist_tag", "mb_artist_tag", [0, 1, 2], ["artist", "tag", "count"]),
    TableSpec(
        "artist_credit",
        "mb_artist_credit",
        [0, 1, 2],
        ["id", "name", "artist_count"],
    ),
    TableSpec(
        "artist_credit_name",
        "mb_artist_credit_name",
        [0, 1, 2, 3, 4],
        ["artist_credit", "position", "artist", "name", "join_phrase"],
    ),
    TableSpec(
        "release_group",
        "mb_release_group",
        [0, 2, 3, 4],
        ["id", "name", "artist_credit", "type"],
    ),
]

# Tables that come from mbdump-derived.tar.bz2 instead of mbdump.tar.bz2
DERIVED_TABLES = {"artist_tag"}


def import_table(conn: psycopg.Connection, spec: TableSpec, data_dir: Path) -> int:
    """Import a single table from its TSV dump file.

    Reads the full-width TSV, extracts only the columns we need,
    and streams them to PostgreSQL via COPY.
    """
    tsv_path = data_dir / spec.dump_file
    if not tsv_path.exists():
        logger.warning("File not found, skipping: %s", tsv_path)
        return 0

    start = time.time()
    row_count = 0
    buf = io.BytesIO()

    with open(tsv_path, encoding="utf-8") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            try:
                extracted = [parts[i] for i in spec.source_indices]
            except IndexError:
                continue

            # Apply null transforms
            if spec.null_transform:
                for idx, default in spec.null_transform.items():
                    pos = spec.source_indices.index(idx)
                    if extracted[pos] == "\\N":
                        extracted[pos] = default

            buf.write(("\t".join(extracted) + "\n").encode("utf-8"))
            row_count += 1

            if row_count % 500_000 == 0:
                logger.info("  %s: %d rows read...", spec.dump_file, row_count)

    buf.seek(0)
    columns = ", ".join(spec.db_columns)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {spec.table} ({columns}) FROM STDIN WITH (FORMAT text)") as copy:
            while chunk := buf.read(8 * 1024 * 1024):
                copy.write(chunk)
    conn.commit()

    elapsed = time.time() - start
    logger.info("  %s -> %s: %d rows in %.1fs", spec.dump_file, spec.table, row_count, elapsed)
    return row_count


def import_all(
    conn: psycopg.Connection, data_dir: Path, tables: list[TableSpec] | None = None
) -> None:
    """Import all tables in dependency order."""
    specs = tables or TABLES
    total_start = time.time()
    total_rows = 0

    for spec in specs:
        rows = import_table(conn, spec, data_dir)
        total_rows += rows

    elapsed = time.time() - total_start
    logger.info("Import complete: %d total rows in %.1fs", total_rows, elapsed)


def main(argv: list[str] | None = None) -> None:
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Import MusicBrainz TSV dumps into PostgreSQL.")
    parser.add_argument(
        "--data-dir", type=Path, required=True, help="Directory containing extracted mbdump files"
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL", "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz"
        ),
        help="PostgreSQL URL",
    )
    args = parser.parse_args(argv)

    conn = psycopg.connect(args.database_url)
    import_all(conn, args.data_dir)
    conn.close()


if __name__ == "__main__":
    main()
