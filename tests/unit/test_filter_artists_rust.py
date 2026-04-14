"""Tests for Rust-accelerated artist filtering in filter_artists.py.

Verifies parity between the Python normalize()+set-lookup path and the
Rust batch_normalize() path, plus alias handling and performance.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "filter_artists.py"
_spec = importlib.util.spec_from_file_location("filter_artists", _SCRIPT_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

normalize = _mod.normalize

try:
    from wxyc_etl.text import batch_normalize, normalize_artist_name

    _HAS_WXYC_ETL = True
except ImportError:
    _HAS_WXYC_ETL = False

pytestmark = pytest.mark.skipif(not _HAS_WXYC_ETL, reason="wxyc-etl not installed")

# Canonical WXYC example artists (pre-normalized, matching CLAUDE.md example data)
LIBRARY_ARTISTS = {
    "autechre",
    "stereolab",
    "cat power",
    "jessica pratt",
    "chuquimamani-condori",
    "duke ellington",
    "juana molina",
    "father john misty",
    "prince jammy",
    "sessa",
    "anne gillis",
    "rafael toral",
    "buck meek",
    "nourished by time",
    "large professor",
    "rochelle jordan",
}


class TestNormalizeParity:
    """Rust normalize_artist_name must produce byte-identical output to Python normalize."""

    @pytest.mark.parametrize(
        "name",
        [
            # Basic cases
            "Autechre",
            "STEREOLAB",
            "Cat Power",
            # Diacritics / NFKD
            "Björk",
            "Café Tacvba",
            "Sigur Rós",
            "Ólafur Arnalds",
            "José González",
            # Combining characters
            "Bjo\u0308rk",  # 'o' + combining diaeresis
            "Rene\u0301e",  # 'e' + combining acute
            # CJK (should pass through unchanged)
            "大貫妙子",
            "坂本龍一",
            # Whitespace
            "  Cat Power  ",
            "   ",
            "",
            # Compatibility characters (NFKD decomposes these)
            "\ufb01nger",  # fi ligature + "nger"
            "na\u00efve",  # i with diaeresis
        ],
    )
    def test_normalize_parity(self, name: str) -> None:
        py_result = normalize(name)
        rs_result = normalize_artist_name(name)
        assert py_result == rs_result, (
            f"Parity failure for {name!r}: Python={py_result!r}, Rust={rs_result!r}"
        )

    def test_batch_normalize_matches_single(self) -> None:
        """batch_normalize() must produce same results as individual normalize_artist_name() calls."""
        names = ["Autechre", "Björk", "  Cat Power  ", "大貫妙子", ""]
        batch_results = batch_normalize(names)
        single_results = [normalize_artist_name(n) for n in names]
        assert batch_results == single_results
