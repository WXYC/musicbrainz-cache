"""Unit tests for import_tsv.py."""

import importlib.util
import sys
from pathlib import Path

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_tsv.py"
_spec = importlib.util.spec_from_file_location("import_tsv", _SCRIPT_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["import_tsv"] = _mod
_spec.loader.exec_module(_mod)

TABLES = _mod.TABLES
TableSpec = _mod.TableSpec
DERIVED_TABLES = _mod.DERIVED_TABLES


class TestTableSpecs:
    """Validate TableSpec definitions."""

    def test_all_specs_have_matching_column_counts(self) -> None:
        """source_indices and db_columns must have the same length."""
        for spec in TABLES:
            assert len(spec.source_indices) == len(spec.db_columns), (
                f"{spec.table}: source_indices ({len(spec.source_indices)}) "
                f"!= db_columns ({len(spec.db_columns)})"
            )

    def test_all_specs_have_unique_table_names(self) -> None:
        names = [spec.table for spec in TABLES]
        assert len(names) == len(set(names))

    def test_all_specs_have_unique_dump_files(self) -> None:
        files = [spec.dump_file for spec in TABLES]
        assert len(files) == len(set(files))

    def test_source_indices_are_non_negative(self) -> None:
        for spec in TABLES:
            for idx in spec.source_indices:
                assert idx >= 0, f"{spec.table}: negative index {idx}"

    def test_reference_tables_come_first(self) -> None:
        """Reference tables (area_type, gender, tag) must be imported before dependent tables."""
        names = [spec.table for spec in TABLES]
        assert names.index("mb_area_type") < names.index("mb_area")
        assert names.index("mb_gender") < names.index("mb_artist")
        assert names.index("mb_tag") < names.index("mb_artist_tag")
        assert names.index("mb_area") < names.index("mb_artist")
        assert names.index("mb_artist") < names.index("mb_artist_alias")
        assert names.index("mb_artist") < names.index("mb_artist_tag")
        assert names.index("mb_artist_credit") < names.index("mb_artist_credit_name")
        assert names.index("mb_artist_credit") < names.index("mb_release_group")

    def test_expected_tables_present(self) -> None:
        names = {spec.table for spec in TABLES}
        expected = {
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
        }
        assert names == expected

    def test_derived_tables_subset(self) -> None:
        dump_files = {spec.dump_file for spec in TABLES}
        assert DERIVED_TABLES.issubset(dump_files)


class TestTableSpecArtist:
    """Verify the artist table spec extracts the right columns."""

    def test_artist_columns(self) -> None:
        spec = next(s for s in TABLES if s.table == "mb_artist")
        assert spec.db_columns == [
            "id",
            "name",
            "sort_name",
            "type",
            "area",
            "gender",
            "begin_area",
            "comment",
        ]

    def test_artist_source_indices(self) -> None:
        """Artist source indices match the MusicBrainz CREATE TABLE column order."""
        spec = next(s for s in TABLES if s.table == "mb_artist")
        # id=0, name=2, sort_name=3, type=10, area=11, gender=12, begin_area=17, comment=13
        assert spec.source_indices == [0, 2, 3, 10, 11, 12, 17, 13]
