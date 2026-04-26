"""Tests for Rust-accelerated artist filtering in filter_artists.py.

Verifies parity between the Python normalize()+set-lookup path and the
Rust batch_normalize() path, plus alias handling and performance.
"""

import importlib.util
import random
import string
import time
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

# A 16-name subset of the canonical WXYC artist pool (`wxycCanonicalArtistNames`
# in @wxyc/shared, also documented in the org-level CLAUDE.md). Names are
# pre-normalized to match the form `normalize_artist_name` produces.
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
            # Diacritics / NFKD (drawn from canonical WXYC artists with non-ASCII names)
            "Nilüfer Yanya",  # ü
            "Hermanos Gutiérrez",  # é
            "Csillagrablók",  # ó
            "Sonido Dueñez",  # ñ — combining-tilde decomposition (canonical via @wxyc/shared#81)
            "Aşıq Altay",  # multi-diacritic Turkish (ş + ı; canonical via @wxyc/shared#81)
            "GIDEÖN",  # capital Ö mid-word (canonical via @wxyc/shared#84)
            # Combining characters (NFD form of canonical names)
            "Nilu\u0308fer Yanya",  # 'u' + combining diaeresis (NFD of Nilüfer Yanya)
            "Hermanos Gutie\u0301rrez",  # 'e' + combining acute (NFD of Gutiérrez)
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
        names = ["Autechre", "Nilüfer Yanya", "  Cat Power  ", "大貫妙子", ""]
        batch_results = batch_normalize(names)
        single_results = [normalize_artist_name(n) for n in names]
        assert batch_results == single_results


# Simulated DB rows: (id, name). All artist names are drawn from the canonical
# WXYC pool; rows 4/5/6/8/15/17 are NOT in the LIBRARY_ARTISTS test subset
# above, so they should fail to match (verifying the filter is set-bounded).
MB_ARTIST_ROWS = [
    (1, "Autechre"),
    (2, "STEREOLAB"),
    (3, "Cat Power"),
    (4, "Yo La Tengo"),  # canonical, not in test library subset
    (5, "Animal Collective"),  # canonical, not in test library subset
    (6, "Tinariwen"),  # canonical, not in test library subset
    (7, "Jessica Pratt"),
    (8, "Nilüfer Yanya"),  # diacritics — canonical, not in test library subset
    (9, "Chuquimamani-Condori"),
    (10, "Duke Ellington"),
    (11, "Juana Molina"),
    (12, "Father John Misty"),
    (13, "  Cat Power  "),  # whitespace variant
    (14, ""),  # empty string
    (15, "Csillagrablók"),  # diacritics — canonical, not in test library subset
    (16, "Prince Jammy"),
    (17, "Nilu\u0308fer Yanya"),  # combining diaeresis form — normalizes to "nilufer yanya"
]

# Simulated alias rows: (artist_id, alias_name)
MB_ALIAS_ROWS = [
    (100, "Ae"),  # alias for Autechre — not in library set
    (101, "Cat Power"),  # alias matches
    (102, "The Stereolab Group"),  # doesn't match
    (103, "Buck Meek"),  # matches
    (104, "Large Professor"),  # matches
    (105, "Nourished By Time"),  # matches (case difference)
]


def _python_filter(rows: list[tuple[int, str]], library_set: set[str]) -> set[int]:
    """Python fallback: per-row normalize + set lookup."""
    return {row_id for row_id, name in rows if normalize(name) in library_set}


def _rust_filter(rows: list[tuple[int, str]], library_set: set[str]) -> set[int]:
    """Rust path: batch_normalize + set lookup."""
    if not rows:
        return set()
    ids, names = zip(*rows)
    normalized = batch_normalize(list(names))
    return {ids[i] for i, norm in enumerate(normalized) if norm in library_set}


class TestBatchFilterParity:
    """Rust batch path must produce identical match results to Python per-row path."""

    def test_artist_name_matching(self) -> None:
        py_matches = _python_filter(MB_ARTIST_ROWS, LIBRARY_ARTISTS)
        rs_matches = _rust_filter(MB_ARTIST_ROWS, LIBRARY_ARTISTS)
        assert py_matches == rs_matches

    def test_expected_matches(self) -> None:
        """Verify which specific artists should match."""
        matches = _rust_filter(MB_ARTIST_ROWS, LIBRARY_ARTISTS)
        # Should match: Autechre(1), STEREOLAB(2), Cat Power(3), Jessica Pratt(7),
        # Chuquimamani-Condori(9), Duke Ellington(10), Juana Molina(11),
        # Father John Misty(12), whitespace Cat Power(13), Prince Jammy(16)
        assert matches == {1, 2, 3, 7, 9, 10, 11, 12, 13, 16}

    def test_non_matches(self) -> None:
        """Verify canonical artists outside this test's library subset don't match."""
        matches = _rust_filter(MB_ARTIST_ROWS, LIBRARY_ARTISTS)
        # Yo La Tengo(4), Animal Collective(5), Tinariwen(6), Nilüfer Yanya(8),
        # empty(14), Csillagrablók(15), Nilüfer Yanya NFD(17) should NOT match
        for non_match_id in [4, 5, 6, 8, 14, 15, 17]:
            assert non_match_id not in matches

    def test_empty_input(self) -> None:
        assert _rust_filter([], LIBRARY_ARTISTS) == set()

    def test_empty_library(self) -> None:
        assert _rust_filter(MB_ARTIST_ROWS, set()) == set()


class TestBatchFilterAliasHandling:
    """Alias matching must work identically through the Rust batch path."""

    def test_alias_parity(self) -> None:
        py_matches = _python_filter(MB_ALIAS_ROWS, LIBRARY_ARTISTS)
        rs_matches = _rust_filter(MB_ALIAS_ROWS, LIBRARY_ARTISTS)
        assert py_matches == rs_matches

    def test_expected_alias_matches(self) -> None:
        matches = _rust_filter(MB_ALIAS_ROWS, LIBRARY_ARTISTS)
        # Cat Power(101), Buck Meek(103), Large Professor(104),
        # Nourished By Time(105) should match
        assert matches == {101, 103, 104, 105}

    def test_combined_artist_and_alias_matching(self) -> None:
        """Simulate the full find_matching_artist_ids flow."""
        # First pass: match by artist name
        matching_ids = _rust_filter(MB_ARTIST_ROWS, LIBRARY_ARTISTS)
        # Second pass: match by alias — adds new artist IDs
        alias_matches = _rust_filter(MB_ALIAS_ROWS, LIBRARY_ARTISTS)
        matching_ids |= alias_matches
        # Should contain both direct and alias matches
        assert 1 in matching_ids  # Autechre (direct)
        assert 103 in matching_ids  # Buck Meek (alias)


def _random_artist_name(rng: random.Random) -> str:
    """Generate a random artist-like name with occasional diacritics."""
    length = rng.randint(3, 25)
    chars = []
    for _ in range(length):
        if rng.random() < 0.05:
            chars.append(rng.choice("àáâãäåèéêëìíîïòóôõöùúûüñç"))
        elif rng.random() < 0.15:
            chars.append(" ")
        else:
            chars.append(rng.choice(string.ascii_letters))
    return "".join(chars)


@pytest.mark.slow
class TestBatchFilterPerformance:
    """Benchmark: Rust batch_normalize must be significantly faster than Python per-row."""

    def test_batch_filter_performance(self) -> None:
        rng = random.Random(42)
        n_names = 100_000
        n_library = 6_500

        # Generate synthetic data
        all_names = [_random_artist_name(rng) for _ in range(n_names)]
        library_set = {normalize(n) for n in all_names[:n_library]}

        # Time Python path
        py_start = time.perf_counter()
        py_results = [normalize(name) in library_set for name in all_names]
        py_elapsed = time.perf_counter() - py_start

        # Time Rust path
        rs_start = time.perf_counter()
        normalized = batch_normalize(all_names)
        rs_results = [norm in library_set for norm in normalized]
        rs_elapsed = time.perf_counter() - rs_start

        # Verify identical results
        assert py_results == rs_results

        speedup = py_elapsed / rs_elapsed
        print(f"\nPython: {py_elapsed:.3f}s, Rust: {rs_elapsed:.3f}s, speedup: {speedup:.1f}x")
        # batch_normalize offloads normalization to Rust but set membership is
        # still in Python, so ~1.5-2x at 100K. Full batch_filter_artists (when
        # exposed via PyO3) would yield much higher speedups by keeping
        # everything in Rust.
        assert speedup >= 1.3, (
            f"Expected at least 1.3x speedup, got {speedup:.1f}x "
            f"(Python={py_elapsed:.3f}s, Rust={rs_elapsed:.3f}s)"
        )
