"""Integration-specific fixtures for musicbrainz-cache."""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
from pathlib import Path

import pytest

# Import scripts as modules
_SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"

_import_spec = importlib.util.spec_from_file_location("import_tsv", _SCRIPTS_DIR / "import_tsv.py")
_import_mod = importlib.util.module_from_spec(_import_spec)
sys.modules["import_tsv"] = _import_mod
_import_spec.loader.exec_module(_import_mod)

_filter_spec = importlib.util.spec_from_file_location(
    "filter_artists", _SCRIPTS_DIR / "filter_artists.py"
)
_filter_mod = importlib.util.module_from_spec(_filter_spec)
sys.modules["filter_artists"] = _filter_mod
_filter_spec.loader.exec_module(_filter_mod)

_pipeline_spec = importlib.util.spec_from_file_location(
    "run_pipeline", _SCRIPTS_DIR / "run_pipeline.py"
)
_pipeline_mod = importlib.util.module_from_spec(_pipeline_spec)
sys.modules["run_pipeline"] = _pipeline_mod
_pipeline_spec.loader.exec_module(_pipeline_mod)

import_tsv = _import_mod
filter_artists = _filter_mod
run_pipeline = _pipeline_mod

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

# WXYC canonical artists present in the fixture TSV data
WXYC_ARTISTS = [
    "Autechre",
    "Stereolab",
    "Cat Power",
    "Jessica Pratt",
    "Duke Ellington",
    "Juana Molina",
    "Prince Jammy",
    "Sessa",
]


@pytest.fixture(scope="module")
def library_db(tmp_path_factory):
    """Create a temporary SQLite library.db with WXYC artists."""
    tmp = tmp_path_factory.mktemp("library")
    db_path = tmp / "library.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE library (id INTEGER PRIMARY KEY, artist TEXT, album TEXT, title TEXT)"
    )
    for i, artist in enumerate(WXYC_ARTISTS):
        conn.execute(
            "INSERT INTO library (id, artist, album, title) VALUES (?, ?, ?, ?)",
            (i + 1, artist, f"Album by {artist}", f"Track by {artist}"),
        )
    conn.commit()
    conn.close()
    return db_path
