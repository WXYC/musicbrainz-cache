-- Consolidated cross-cache identity hook table (E1 §4.1.2 of the
-- cross-cache-identity rollout).
--
-- Spec: https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md
-- §3.1 (full schema) + §4.1.2 (this cache: Homebrew musicbrainz, port 5432).
--
-- One row per WXYC library release. ``library_id`` mirrors Backend's
-- ``wxyc_schema.library.id``. The normalized columns (``norm_artist``,
-- ``norm_title``, ``norm_label``) are populated by
-- ``wxyc_etl::text::to_identity_match_form{,_title}`` (the locked-on
-- baseline; not the WX-2 comparison form ``to_match_form``). All
-- consumers (LML, semantic-index, audits) join on these normalized
-- columns, so the algorithm must stay pinned across caches.
--
-- ``artist_id`` / ``label_id`` / ``format_id`` / ``release_year`` are
-- nullable per §3.1: per-cache loaders populate what their source
-- exposes, and library.db (the SQLite catalog export this cache reads)
-- does not carry Backend's integer IDs.
--
-- CONCURRENTLY deviation (read this carefully): wiki §4.1.2 calls for
-- "all indexes built CONCURRENTLY since the cache is large and
-- active". The literal directive cannot be honored in a single
-- sqlx-cli migration: sqlx-postgres sends every migration's SQL as a
-- single PG simple-Query message, and PG treats multi-statement simple
-- queries as one implicit transaction. ``CREATE INDEX CONCURRENTLY``
-- refuses to run inside any transaction block, implicit or explicit.
-- The discogs-etl analog (0003_wxyc_library_v2.py) sidesteps this by
-- using psycopg autocommit with one ``cur.execute()`` per CONCURRENTLY
-- statement. The Rust/sqlx equivalent would be splitting into N
-- one-statement no-transaction migrations -- a heavyweight wrapping
-- for a brand-new empty table that has no readers and no rows on
-- first apply (the only state where CONCURRENTLY is meaningful). On
-- re-apply, ``IF NOT EXISTS`` short-circuits before any locking, so
-- there's nothing for CONCURRENTLY to protect against. Plain
-- ``CREATE INDEX IF NOT EXISTS`` is therefore the appropriate Rust/sqlx
-- realisation of the §4.1.2 intent: idempotent, lock-light on re-run,
-- and behaviorally equivalent to the discogs-etl path on first run
-- (both build into an empty table). If/when this migration ever needs
-- to land against a populated wxyc_library, split into per-index
-- no-transaction migrations at that time.
--
-- Idempotency: ``CREATE TABLE IF NOT EXISTS`` + ``CREATE INDEX IF NOT
-- EXISTS`` make every statement re-runnable, as required by the
-- dual-source mirror (schema/create_database.sql +
-- schema/create_indexes.sql) the runtime ``apply_schema()`` path uses.

CREATE TABLE IF NOT EXISTS wxyc_library (
    library_id      INTEGER PRIMARY KEY,
    artist_id       INTEGER,
    artist_name     TEXT NOT NULL,
    album_title     TEXT NOT NULL,
    label_id        INTEGER,
    label_name      TEXT,
    format_id       INTEGER,
    format_name     TEXT,
    wxyc_genre      TEXT,
    call_letters    TEXT,
    call_numbers    INTEGER,
    release_year    SMALLINT,
    norm_artist     TEXT NOT NULL,
    norm_title      TEXT NOT NULL,
    norm_label      TEXT,
    snapshot_at     TIMESTAMPTZ NOT NULL,
    snapshot_source TEXT NOT NULL
        CHECK (snapshot_source IN ('backend', 'tubafrenzy', 'llm'))
);

CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_idx
    ON wxyc_library (norm_artist);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_idx
    ON wxyc_library (norm_title);
CREATE INDEX IF NOT EXISTS wxyc_library_artist_id_idx
    ON wxyc_library (artist_id);
CREATE INDEX IF NOT EXISTS wxyc_library_format_id_idx
    ON wxyc_library (format_id);
CREATE INDEX IF NOT EXISTS wxyc_library_release_year_idx
    ON wxyc_library (release_year);

-- GIN trigram indexes for fuzzy lookup on the normalized columns.
-- ``gin_trgm_ops`` requires the pg_trgm extension; the baseline
-- (0001_initial.sql) already creates it.
CREATE INDEX IF NOT EXISTS wxyc_library_norm_artist_trgm_idx
    ON wxyc_library USING GIN (norm_artist gin_trgm_ops);
CREATE INDEX IF NOT EXISTS wxyc_library_norm_title_trgm_idx
    ON wxyc_library USING GIN (norm_title gin_trgm_ops);
