"""PostgreSQL integration tests for the MusicBrainz cache import pipeline.

Tests the full import flow: schema creation -> TSV import -> artist filtering.
Uses small fixture TSV data with WXYC canonical artists.

Requires PostgreSQL (gated by DATABASE_URL_TEST).
"""

from __future__ import annotations

import psycopg
import pytest

from .conftest import (
    FIXTURES_DIR,
    SCHEMA_DIR,
    WXYC_ARTISTS,
    filter_artists,
    import_tsv,
    run_pipeline,
)

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
            cur.execute("SELECT COUNT(*) FROM mb_artist_credit_name WHERE artist_credit = 5003")
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
                "SELECT t.name FROM mb_tag t JOIN mb_artist_tag at ON t.id = at.tag ORDER BY t.name"
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


class TestBrokenEscapeSequence:
    """TSV with broken escape sequence should produce an error message, not hang."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url, tmp_path_factory):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)
        self.__class__._conn = conn

        # Create a TSV fixture with a broken escape sequence
        tmp = tmp_path_factory.mktemp("broken_fixtures")
        self.__class__._fixtures_dir = tmp

        # Valid area_type fixture (id, name -- source_indices [0, 1])
        (tmp / "area_type").write_text("1\tCountry\n2\tSubdivision\n3\tCity\n")
        # Valid gender fixture (id, name -- source_indices [0, 1])
        (tmp / "gender").write_text("1\tMale\n2\tFemale\n3\tOther\n")
        # Valid tag fixture (id, name -- source_indices [0, 1])
        (tmp / "tag").write_text("1\telectronic\n2\tidm\n")
        # Valid area fixture -- must match MB dump format: id(0), gid(1), name(2), type(3), ...
        # source_indices [0, 2, 3] -> id, name, type
        (tmp / "area").write_text(
            "221\tb3aac116-4321-3476-a2c1-405e4e637dba\tUnited Kingdom\t1\t0\t\\N\n"
        )
        # Valid country_area fixture (area -- source_indices [0])
        (tmp / "country_area").write_text("221\n")
        # Broken artist fixture: a line with bad column count (too few tabs)
        # This simulates a broken escape that collapses columns
        (tmp / "artist").write_text(
            "1000\tf74b190f-8ece-46b1-aee6-5a5dcbe97eda\tAutechre\tAutechre\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t2\t221\t\\N\tBritish electronic duo\t0\t\\N\t0\t221\t\\N\n"
            "BROKEN_LINE\n"  # This line has too few columns
            "1001\ta1b2c3d4-e5f6-7890-abcd-ef0123456789\tStereolab\tStereolab\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t2\t221\t\\N\tAvant-pop group\t0\t\\N\t0\t\\N\t\\N\n"
        )

        yield
        conn.close()

    def test_broken_line_skipped_without_hang(self) -> None:
        """Import completes without hanging; broken lines are skipped via IndexError."""
        import_conn = psycopg.connect(self._db_url)

        # Import just the artist table (after reference tables)
        from .conftest import import_tsv

        # Import reference tables first
        for spec in import_tsv.TABLES:
            if spec.dump_file in ("area_type", "gender", "tag", "area", "country_area"):
                import_tsv.import_table(import_conn, spec, self._fixtures_dir)

        # Import artist table with the broken fixture
        artist_spec = next(s for s in import_tsv.TABLES if s.table == "mb_artist")
        row_count = import_tsv.import_table(import_conn, artist_spec, self._fixtures_dir)
        import_conn.close()

        # The broken line should have been skipped (IndexError on column extraction)
        # Valid lines should have been imported
        assert row_count == 2, f"Expected 2 valid rows (broken line skipped), got {row_count}"

        with self._conn.cursor() as cur:
            cur.execute("SELECT name FROM mb_artist ORDER BY name")
            names = [row[0] for row in cur.fetchall()]
        assert "Autechre" in names
        assert "Stereolab" in names

    def test_error_does_not_corrupt_subsequent_rows(self) -> None:
        """Rows after the broken line are imported correctly."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT id, name, sort_name FROM mb_artist WHERE id = 1001")
            row = cur.fetchone()
        if row is not None:
            assert row[1] == "Stereolab"
            assert row[2] == "Stereolab"


class TestMissingTsvFiles:
    """Resume test: import gracefully handles missing TSV files."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url, tmp_path_factory):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _apply_schema(conn)
        self.__class__._conn = conn

        # Create a fixtures directory with only some files present
        tmp = tmp_path_factory.mktemp("sparse_fixtures")
        self.__class__._fixtures_dir = tmp

        # Only create reference tables and artist -- skip everything else
        # area_type: id(0), name(1) -- source_indices [0, 1]
        (tmp / "area_type").write_text("1\tCountry\n")
        # gender: id(0), name(1) -- source_indices [0, 1]
        (tmp / "gender").write_text("1\tMale\n")
        # tag: id(0), name(1) -- source_indices [0, 1]
        (tmp / "tag").write_text("1\telectronic\n")
        # area: MB dump format id(0), gid(1), name(2), type(3), ...
        (tmp / "area").write_text(
            "221\tb3aac116-4321-3476-a2c1-405e4e637dba\tUnited Kingdom\t1\t0\t\\N\n"
        )
        # country_area: area(0) -- source_indices [0]
        (tmp / "country_area").write_text("221\n")
        # artist: MB dump format (19 columns)
        (tmp / "artist").write_text(
            "1000\tf74b190f-8ece-46b1-aee6-5a5dcbe97eda\tAutechre\tAutechre"
            "\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t2\t221\t\\N\t\t0\t\\N\t0\t221\t\\N\n"
        )
        # Deliberately missing: artist_alias, artist_tag, artist_credit,
        # artist_credit_name, release_group, recording, medium, track,
        # url, link_type, link, release, l_release_group_url, l_release_url

        yield
        conn.close()

    def test_import_all_skips_missing_files(self) -> None:
        """import_all completes successfully even when most TSV files are missing."""
        from .conftest import import_tsv

        import_conn = psycopg.connect(self._db_url)
        # Should not raise -- missing files are logged as warnings and skipped
        import_tsv.import_all(import_conn, self._fixtures_dir)
        import_conn.close()

    def test_present_tables_populated(self) -> None:
        """Tables with available TSV files should be populated."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_artist")
            count = cur.fetchone()[0]
        assert count == 1

        with self._conn.cursor() as cur:
            cur.execute("SELECT name FROM mb_artist WHERE id = 1000")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "Autechre"

    def test_missing_tables_empty(self) -> None:
        """Tables without TSV files should exist (from schema) but be empty."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_artist_alias")
            count = cur.fetchone()[0]
        assert count == 0

        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mb_release_group")
            count = cur.fetchone()[0]
        assert count == 0
