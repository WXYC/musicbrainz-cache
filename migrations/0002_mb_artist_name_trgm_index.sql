-- Add a pg_trgm GIN index on lower(mb_artist.name) for fuzzy artist search.
--
-- LML's external-cache fallback (lookup/external_search.py::_MB_ARTIST_FUZZY_SQL)
-- runs `WHERE lower(name) % lower($1)` against mb_artist. Without a trigram
-- index, `%` falls back to a sequential scan of the filtered mb_artist table
-- on every call. The 2026-04-27 lossy-mojibake matcher run (815 calls) took
-- ~25 minutes; with this index it drops to a few minutes.
--
-- pg_trgm is enabled by 0001_initial.sql (CREATE EXTENSION IF NOT EXISTS pg_trgm).
--
-- Idempotency: CREATE INDEX IF NOT EXISTS, so re-applying is a no-op.

CREATE INDEX IF NOT EXISTS idx_mb_artist_name_lower_trgm
    ON mb_artist USING GIN (lower(name) gin_trgm_ops);
