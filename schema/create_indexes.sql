-- Secondary indexes for MusicBrainz cache tables.
-- Applied after bulk import and filtering for faster pipeline throughput.
--
-- Idempotency: every CREATE INDEX uses IF NOT EXISTS so the Indexes step
-- can be re-run safely under `--resume` (see CLAUDE.md "Resume safety").

CREATE INDEX IF NOT EXISTS idx_mb_recording_gid ON mb_recording(gid);
CREATE INDEX IF NOT EXISTS idx_mb_recording_credit ON mb_recording(artist_credit);
CREATE INDEX IF NOT EXISTS idx_mb_track_recording ON mb_track(recording);
CREATE INDEX IF NOT EXISTS idx_mb_track_medium ON mb_track(medium);
CREATE INDEX IF NOT EXISTS idx_mb_artist_name_lower ON mb_artist (lower(name));
-- Trigram GIN index supports the `lower(name) % lower($1)` fuzzy search used by
-- LML's external-cache fallback (lookup/external_search.py::_MB_ARTIST_FUZZY_SQL).
-- Without it, `%` falls back to a seq-scan on the full filtered mb_artist table.
CREATE INDEX IF NOT EXISTS idx_mb_artist_name_lower_trgm
    ON mb_artist USING GIN (lower(name) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_mb_artist_area ON mb_artist (area);
CREATE INDEX IF NOT EXISTS idx_mb_artist_alias_artist ON mb_artist_alias (artist);
CREATE INDEX IF NOT EXISTS idx_mb_artist_alias_name_lower ON mb_artist_alias (lower(name));
CREATE INDEX IF NOT EXISTS idx_mb_artist_tag_artist ON mb_artist_tag (artist);
CREATE INDEX IF NOT EXISTS idx_mb_artist_tag_tag ON mb_artist_tag (tag);
CREATE INDEX IF NOT EXISTS idx_mb_artist_credit_name_artist ON mb_artist_credit_name (artist);
CREATE INDEX IF NOT EXISTS idx_mb_artist_credit_name_credit ON mb_artist_credit_name (artist_credit);
CREATE INDEX IF NOT EXISTS idx_mb_release_group_credit ON mb_release_group (artist_credit);
CREATE INDEX IF NOT EXISTS idx_mb_area_type ON mb_area (type);
CREATE INDEX IF NOT EXISTS idx_mb_url_gid ON mb_url(gid);
CREATE INDEX IF NOT EXISTS idx_mb_release_gid ON mb_release(gid);
CREATE INDEX IF NOT EXISTS idx_mb_release_release_group ON mb_release(release_group);
CREATE INDEX IF NOT EXISTS idx_mb_release_credit ON mb_release(artist_credit);
CREATE INDEX IF NOT EXISTS idx_mb_l_release_group_url_rg ON mb_l_release_group_url(release_group);
CREATE INDEX IF NOT EXISTS idx_mb_l_release_group_url_url ON mb_l_release_group_url(url);
CREATE INDEX IF NOT EXISTS idx_mb_l_release_url_release ON mb_l_release_url(release);
CREATE INDEX IF NOT EXISTS idx_mb_l_release_url_url ON mb_l_release_url(url);
CREATE INDEX IF NOT EXISTS idx_mb_link_type ON mb_link(link_type);
