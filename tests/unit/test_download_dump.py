"""Unit tests for download_dump.py."""

import importlib.util
import io
import os
import sys
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "download_dump.py"
_spec = importlib.util.spec_from_file_location("download_dump", _SCRIPT_PATH)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["download_dump"] = _mod
_spec.loader.exec_module(_mod)

CORE_FILES = _mod.CORE_FILES
DERIVED_FILES = _mod.DERIVED_FILES
ARCHIVES = _mod.ARCHIVES
extract_tables = _mod.extract_tables


def _get(name: str):
    """Get a module attribute, raising a clear error if it doesn't exist yet."""
    return getattr(_mod, name)


def _create_test_archive(path: Path, prefix: str, file_names: list[str]) -> None:
    """Create a small tar.bz2 archive with dummy files for testing."""
    with tarfile.open(path, "w:bz2") as tar:
        for name in file_names:
            data = f"data for {name}\n".encode()
            info = tarfile.TarInfo(name=f"{prefix}/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


class TestConstants:
    """Validate archive and file set definitions."""

    def test_core_files_count(self) -> None:
        assert len(CORE_FILES) == 9

    def test_derived_files_count(self) -> None:
        assert len(DERIVED_FILES) == 2

    def test_archives_reference_correct_sets(self) -> None:
        names_to_sets = dict(ARCHIVES)
        assert names_to_sets["mbdump.tar.bz2"] is CORE_FILES
        assert names_to_sets["mbdump-derived.tar.bz2"] is DERIVED_FILES

    def test_no_overlap_between_core_and_derived(self) -> None:
        assert CORE_FILES.isdisjoint(DERIVED_FILES)


class TestFindDecompressor:
    """Test parallel decompressor detection."""

    def test_returns_none_when_no_tools_installed(self) -> None:
        with patch("download_dump.shutil.which", return_value=None):
            assert _get("_find_decompressor")() is None

    def test_prefers_lbzip2(self) -> None:
        def fake_which(tool):
            return f"/usr/bin/{tool}" if tool in ("lbzip2", "pbzip2") else None

        with patch("download_dump.shutil.which", side_effect=fake_which):
            assert _get("_find_decompressor")() == "/usr/bin/lbzip2"

    def test_falls_back_to_pbzip2(self) -> None:
        def fake_which(tool):
            return "/usr/bin/pbzip2" if tool == "pbzip2" else None

        with patch("download_dump.shutil.which", side_effect=fake_which):
            assert _get("_find_decompressor")() == "/usr/bin/pbzip2"


class TestExtractTablesStreaming:
    """Test the Python streaming fallback extractor."""

    def test_extracts_needed_files(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist", "artist_alias", "extra_table"])
        _get("_extract_tables_streaming")(archive, {"artist", "artist_alias"}, output)

        assert (output / "artist").exists()
        assert (output / "artist_alias").exists()
        assert not (output / "extra_table").exists()

    def test_extracts_correct_content(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])
        _get("_extract_tables_streaming")(archive, {"artist"}, output)

        assert (output / "artist").read_text() == "data for artist\n"

    def test_strips_prefix(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])
        _get("_extract_tables_streaming")(archive, {"artist"}, output)

        assert (output / "artist").exists()
        assert not (output / "mbdump" / "artist").exists()

    def test_early_exit_skips_remaining_entries(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        files = ["artist"] + [f"extra_{i}" for i in range(50)]
        _create_test_archive(archive, "mbdump", files)

        _get("_extract_tables_streaming")(archive, {"artist"}, output)

        assert (output / "artist").exists()
        assert not any((output / f"extra_{i}").exists() for i in range(50))

    def test_handles_empty_needed_set(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])
        _get("_extract_tables_streaming")(archive, set(), output)

        assert not (output / "artist").exists()

    def test_creates_output_dir(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "nested" / "output"
        _create_test_archive(archive, "mbdump", ["artist"])
        _get("_extract_tables_streaming")(archive, {"artist"}, output)

        assert output.is_dir()
        assert (output / "artist").exists()


class TestExtractTablesSubprocess:
    """Test the subprocess-based extractor."""

    def test_returns_false_when_decompressor_not_found(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])

        result = _get("_extract_tables_subprocess")(archive, {"artist"}, output, "/nonexistent/lbzip2")
        assert result is False

    @pytest.mark.skipif(
        not any(
            os.path.isfile(os.path.join(d, tool))
            for d in os.environ.get("PATH", "").split(":")
            for tool in ("lbzip2", "pbzip2")
        ),
        reason="No parallel bzip2 tool installed",
    )
    def test_extracts_files_when_tool_available(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist", "tag", "extra"])

        decompressor = _get("_find_decompressor")()
        result = _get("_extract_tables_subprocess")(archive, {"artist", "tag"}, output, decompressor)

        assert result is True
        assert (output / "artist").exists()
        assert (output / "tag").exists()
        assert not (output / "extra").exists()


class TestExtractTablesOrchestrator:
    """Test the public extract_tables function end-to-end."""

    def test_falls_back_to_streaming_when_no_decompressor(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist", "tag", "extra"])

        with patch("download_dump._find_decompressor", return_value=None):
            extract_tables(archive, {"artist", "tag"}, output)

        assert (output / "artist").exists()
        assert (output / "tag").exists()
        assert not (output / "extra").exists()

    def test_falls_back_on_subprocess_failure(self, tmp_path: Path) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])

        with (
            patch("download_dump._find_decompressor", return_value="/usr/bin/lbzip2"),
            patch("download_dump._extract_tables_subprocess", return_value=False),
        ):
            extract_tables(archive, {"artist"}, output)

        assert (output / "artist").exists()

    def test_logs_missing_files(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        archive = tmp_path / "test.tar.bz2"
        output = tmp_path / "output"
        _create_test_archive(archive, "mbdump", ["artist"])

        with patch("download_dump._find_decompressor", return_value=None):
            extract_tables(archive, {"artist", "nonexistent"}, output)

        assert "Missing files" in caplog.text
        assert "nonexistent" in caplog.text
