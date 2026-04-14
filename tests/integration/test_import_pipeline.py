"""PostgreSQL integration tests for the MusicBrainz cache import pipeline.

Tests the full import flow: schema creation -> TSV import -> artist filtering.
Uses small fixture TSV data with WXYC canonical artists.

Requires PostgreSQL (gated by DATABASE_URL_TEST).
"""

from __future__ import annotations

import psycopg
import pytest

from .conftest import FIXTURES_DIR, SCHEMA_DIR, WXYC_ARTISTS, filter_artists, import_tsv, run_pipeline

pytestmark = pytest.mark.postgres

# Tables expected after schema creation
EXPECTED_TABLES = {
    "mb_area_type",
    "mb_gender",
    "mb_tag",
    "mb_area",
    "mb_country_area",
    "mb_artist",
    "mb_artist_alias",
    "mb_artist_tag",
    "mb_artist_credit",
    "mb_artist_credit_name",
    "mb_release_group",
    "mb_recording",
    "mb_medium",
    "mb_track",
    "mb_url",
    "mb_link_type",
    "mb_link",
    "mb_release",
    "mb_l_release_group_url",
    "mb_l_release_url",
}

# Expected row counts after importing fixture TSV data
EXPECTED_COUNTS = {
    "mb_area_type": 3,
    "mb_gender": 3,
    "mb_tag": 8,
    "mb_area": 7,
    "mb_country_area": 2,
    "mb_artist": 9,
    "mb_artist_alias": 5,
    "mb_artist_tag": 6,
    "mb_artist_credit": 6,
    "mb_artist_credit_name": 7,
    "mb_release_group": 6,
    "mb_recording": 4,
    "mb_medium": 2,
    "mb_track": 3,
    "mb_url": 2,
    "mb_link_type": 1,
    "mb_link": 2,
    "mb_release": 2,
    "mb_l_release_group_url": 1,
    "mb_l_release_url": 1,
}


def _apply_schema(conn: psycopg.Connection) -> None:
    """Apply the database schema."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())


def _import_fixtures(conn: psycopg.Connection) -> None:
    """Import all fixture TSV files."""
    import_tsv.import_all(conn, FIXTURES_DIR)


class TestSchemaCreation:
    """Verify schema DDL creates all expected tables."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)
        self.__class__._conn = conn
        yield
        conn.close()

    def test_all_tables_exist(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name LIKE 'mb_%'"
            )
            tables = {row[0] for row in cur.fetchall()}
        assert tables == EXPECTED_TABLES

    def test_foreign_key_constraints_exist(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.table_constraints "
                "WHERE constraint_type = 'FOREIGN KEY' AND table_schema = 'public'"
            )
            fk_count = cur.fetchone()[0]
        assert fk_count > 0, "Schema should have foreign key constraints"

    def test_primary_keys_exist(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.table_constraints "
                "WHERE constraint_type = 'PRIMARY KEY' AND table_schema = 'public' "
                "AND table_name LIKE 'mb_%'"
            )
            pk_tables = {row[0] for row in cur.fetchall()}
        # Most tables should have a primary key (artist_credit_name is composite)
        assert "mb_artist" in pk_tables
        assert "mb_area" in pk_tables
        assert "mb_tag" in pk_tables


class TestTsvImport:
    """Import fixture TSV data and verify row counts and data integrity."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)
        conn_for_import = psycopg.connect(db_url)
        _import_fixtures(conn_for_import)
        conn_for_import.close()
        self.__class__._conn = conn
        yield
        conn.close()

    @pytest.mark.parametrize("table,expected_count", list(EXPECTED_COUNTS.items()))
    def test_row_counts(self, table: str, expected_count: int) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            actual = cur.fetchone()[0]
        assert actual == expected_count, f"{table}: expected {expected_count}, got {actual}"

    def test_autechre_artist_data(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT id, name, sort_name FROM mb_artist WHERE id = 1000")
            row = cur.fetchone()
        assert row is not None
        assert row[1] == "Autechre"
        assert row[2] == "Autechre"

    def test_cat_power_has_alias(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT name FROM mb_artist_alias WHERE artist = 1002 AND name = 'Chan Marshall'"
            )
            row = cur.fetchone()
        assert row is not None

    def test_duke_ellington_credit_has_two_names(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM mb_artist_credit_name WHERE artist_credit = 5003"
            )
            count = cur.fetchone()[0]
        assert count == 2, "Duke Ellington & John Coltrane credit should have 2 names"

    def test_autechre_tags(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT t.name FROM mb_artist_tag at "
                "JOIN mb_tag t ON t.id = at.tag "
                "WHERE at.artist = 1000 ORDER BY t.name"
            )
            tags = [row[0] for row in cur.fetchall()]
        assert "electronic" in tags
        assert "idm" in tags

    def test_recording_has_gid(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT gid, name FROM mb_recording WHERE id = 7000")
            row = cur.fetchone()
        assert row is not None
        assert row[1] == "VI Scose Poise"

    def test_url_link_relationships(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT u.url FROM mb_l_release_group_url lrgu "
                "JOIN mb_url u ON u.id = lrgu.url "
                "WHERE lrgu.release_group = 6000"
            )
            row = cur.fetchone()
        assert row is not None
        assert "spotify" in row[0]

    def test_foreign_key_integrity(self) -> None:
        """Verify FK relationships hold: every artist_credit_name references a valid artist."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT acn.artist FROM mb_artist_credit_name acn "
                "LEFT JOIN mb_artist a ON a.id = acn.artist "
                "WHERE a.id IS NULL"
            )
            orphans = cur.fetchall()
        assert len(orphans) == 0, f"Orphan artist references in artist_credit_name: {orphans}"


class TestArtistFiltering:
    """Import data, filter to WXYC artists, verify only matching artists remain."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url, library_db):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)

        # Import with a separate connection (import_table commits per table)
        import_conn = psycopg.connect(db_url)
        _import_fixtures(import_conn)
        import_conn.close()

        # Load library artists and filter
        library_artists = filter_artists.load_library_artists(library_db)
        filter_conn = psycopg.connect(db_url, autocommit=True)
        matching_ids = filter_artists.find_matching_artist_ids(filter_conn, library_artists)
        filter_artists.prune_to_matching(filter_conn, matching_ids)
        filter_conn.close()

        self.__class__._conn = conn
        yield
        conn.close()

    def test_non_library_artist_removed(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_artist WHERE id = 9999")
            count = cur.fetchone()[0]
        assert count == 0, "Non-library artist (id=9999) should be pruned"

    def test_wxyc_artists_remain(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT name FROM mb_artist ORDER BY name")
            names = [row[0] for row in cur.fetchall()]
        for artist in WXYC_ARTISTS:
            assert artist in names, f"WXYC artist '{artist}' should remain after filtering"

    def test_artist_count_reduced(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_artist")
            count = cur.fetchone()[0]
        # 8 WXYC artists should remain (9 total minus 1 non-library)
        assert count == 8

    def test_non_library_credits_pruned(self) -> None:
        """Artist credit 5005 (Non Library Artist) should be removed."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_artist_credit WHERE id = 5005")
            count = cur.fetchone()[0]
        assert count == 0

    def test_matching_release_groups_kept(self) -> None:
        """Release groups tied to matching artists should survive filtering."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT name FROM mb_release_group ORDER BY name")
            names = [row[0] for row in cur.fetchall()]
        assert "Confield" in names
        assert "Moon Pix" in names
        assert "Non Library Album" not in names

    def test_tags_pruned_to_matching(self) -> None:
        """Only tags referenced by matching artists should remain."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT t.name FROM mb_tag t "
                "JOIN mb_artist_tag at ON t.id = at.tag "
                "ORDER BY t.name"
            )
            tags = [row[0] for row in cur.fetchall()]
        assert "electronic" in tags
        assert "jazz" in tags

    def test_url_relationships_survive(self) -> None:
        """URL links for Autechre's release group should survive filtering."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_l_release_group_url")
            count = cur.fetchone()[0]
        assert count >= 1


class TestIndexCreation:
    """Verify secondary indexes can be created after import."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)

        import_conn = psycopg.connect(db_url)
        _import_fixtures(import_conn)
        import_conn.close()

        run_pipeline.create_indexes(db_url)

        self.__class__._conn = conn
        yield
        conn.close()

    def test_indexes_created(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND indexname LIKE 'idx_mb_%'"
            )
            indexes = {row[0] for row in cur.fetchall()}
        assert "idx_mb_artist_name_lower" in indexes
        assert "idx_mb_recording_gid" in indexes
        assert "idx_mb_artist_alias_name_lower" in indexes
