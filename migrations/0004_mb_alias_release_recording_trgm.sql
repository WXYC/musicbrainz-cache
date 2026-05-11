-- Add pg_trgm GIN indexes on the three remaining `name` columns LML's
-- external-cache fallback queries via `%` (similarity operator).
--
-- Context: 0002_mb_artist_name_trgm_index covered the artist side. LML
-- subsequently extended its mojibake-recovery / external-cache fallback to
-- query two more tables, neither of which had a trigram index — every `%`
-- match falls back to a seq-scan over the filtered (but still multi-million-row)
-- tables.
--
-- The three indexes are mechanically identical to 0002 in shape:
--   1. `mb_artist_alias` — LML's lookup/external_search.py UNIONs the alias
--      table with mb_artist to catch ASCII transliterations and alternate
--      spellings. Tracked in WXYC/musicbrainz-cache#33.
--   2. `mb_release` — Phase 1.7 added an `album` skeleton path that fuzzy-
--      matches release names. Tracked in WXYC/musicbrainz-cache#34.
--   3. `mb_recording` — Phase 1.7 added a `song` skeleton path that fuzzy-
--      matches recording names. Tracked alongside #34.
--
-- These three are decoupled from the new wxyc_library hook (E1 §4.1.2,
-- shipped in 0003_wxyc_library_v2.sql); the hook indexes serve cross-cache
-- identity composition, while these serve LML's pre-cutover external-cache
-- fallback path. They were originally framed as "absorbed by #47" in that
-- ticket's body, but the §4.1.2 migration only touched wxyc_library; this
-- file delivers what #33/#34 actually called for.
--
-- pg_trgm is enabled by 0001_initial.sql.
--
-- Idempotency: CREATE INDEX IF NOT EXISTS on every statement; re-applying
-- against a populated cache is a no-op.

CREATE INDEX IF NOT EXISTS idx_mb_artist_alias_name_lower_trgm
    ON mb_artist_alias USING GIN (lower(name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_mb_release_name_lower_trgm
    ON mb_release USING GIN (lower(name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_mb_recording_name_lower_trgm
    ON mb_recording USING GIN (lower(name) gin_trgm_ops);
