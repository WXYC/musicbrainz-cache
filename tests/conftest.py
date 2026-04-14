"""Shared test fixtures for musicbrainz-cache."""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest
from psycopg import sql

ADMIN_URL = os.environ.get(
    "DATABASE_URL_TEST", "postgresql://musicbrainz:musicbrainz@localhost:5434/postgres"
)


def _postgres_available() -> bool:
    """Check if PostgreSQL is reachable."""
    try:
        conn = psycopg.connect(ADMIN_URL, autocommit=True)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="module")
def db_url():
    """Create a temporary database for the test module, drop it on teardown."""
    if not _postgres_available():
        pytest.skip("PostgreSQL not available (set DATABASE_URL_TEST)")

    db_name = f"mb_test_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    yield test_url

    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


@pytest.fixture(scope="module")
def db_conn(db_url):
    """Provide a connection to the temporary database."""
    conn = psycopg.connect(db_url, autocommit=True)
    yield conn
    conn.close()
