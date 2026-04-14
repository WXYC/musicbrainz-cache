-- Secondary indexes for MusicBrainz cache tables.
-- Applied after bulk import and filtering for faster pipeline throughput.

CREATE INDEX idx_mb_recording_gid ON mb_recording(gid);
CREATE INDEX idx_mb_recording_credit ON mb_recording(artist_credit);
CREATE INDEX idx_mb_track_recording ON mb_track(recording);
CREATE INDEX idx_mb_track_medium ON mb_track(medium);
CREATE INDEX idx_mb_artist_name_lower ON mb_artist (lower(name));
CREATE INDEX idx_mb_artist_area ON mb_artist (area);
CREATE INDEX idx_mb_artist_alias_artist ON mb_artist_alias (artist);
CREATE INDEX idx_mb_artist_alias_name_lower ON mb_artist_alias (lower(name));
CREATE INDEX idx_mb_artist_tag_artist ON mb_artist_tag (artist);
CREATE INDEX idx_mb_artist_tag_tag ON mb_artist_tag (tag);
CREATE INDEX idx_mb_artist_credit_name_artist ON mb_artist_credit_name (artist);
CREATE INDEX idx_mb_artist_credit_name_credit ON mb_artist_credit_name (artist_credit);
CREATE INDEX idx_mb_release_group_credit ON mb_release_group (artist_credit);
CREATE INDEX idx_mb_area_type ON mb_area (type);
CREATE INDEX idx_mb_url_gid ON mb_url(gid);
CREATE INDEX idx_mb_release_gid ON mb_release(gid);
CREATE INDEX idx_mb_release_release_group ON mb_release(release_group);
CREATE INDEX idx_mb_release_credit ON mb_release(artist_credit);
CREATE INDEX idx_mb_l_release_group_url_rg ON mb_l_release_group_url(release_group);
CREATE INDEX idx_mb_l_release_group_url_url ON mb_l_release_group_url(url);
CREATE INDEX idx_mb_l_release_url_release ON mb_l_release_url(release);
CREATE INDEX idx_mb_l_release_url_url ON mb_l_release_url(url);
CREATE INDEX idx_mb_link_type ON mb_link(link_type);
