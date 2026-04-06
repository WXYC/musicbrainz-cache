"""Unit tests for filter_artists.py."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "filter_artists.py"
_spec = importlib.util.spec_from_file_location("filter_artists", _SCRIPT_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

normalize = _mod.normalize


class TestNormalize:
    """Artist name normalization for matching."""

    def test_lowercase(self) -> None:
        assert normalize("Autechre") == "autechre"

    def test_strip_diacritics(self) -> None:
        assert normalize("Björk") == "bjork"

    def test_strip_whitespace(self) -> None:
        assert normalize("  Cat Power  ") == "cat power"

    def test_nfkd_decomposition(self) -> None:
        assert normalize("Café Tacvba") == "cafe tacvba"

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Duke Ellington", "duke ellington"),
            ("Chuquimamani-Condori", "chuquimamani-condori"),
            ("Prince Jammy", "prince jammy"),
            ("大貫妙子", "大貫妙子"),  # CJK characters pass through
        ],
    )
    def test_various_artists(self, raw: str, expected: str) -> None:
        assert normalize(raw) == expected
