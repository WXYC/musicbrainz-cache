-- Baseline migration for the WXYC musicbrainz-cache schema.
--
-- Snapshot of the schema applied today via src/schema.rs::apply_schema()
-- (concatenation of schema/create_database.sql followed by
-- schema/create_indexes.sql). The runtime path still uses apply_schema();
-- this baseline exists so that future schema changes can ship as
-- numbered sqlx migrations (0002_*, 0003_*, ...). Switching the runtime
-- path to `sqlx migrate run` lands separately in WXYC/wxyc-etl#56.
--
-- Idempotency: every statement is re-runnable so existing prod databases
-- can be stamped at 0001_initial without re-applying. See CLAUDE.md
-- "Migrations" for the stamp procedure.

-- =============================================================================
-- create_database.sql
-- =============================================================================

-- MusicBrainz cache schema for WXYC genre/area analysis.
-- Imports a subset of MusicBrainz tables relevant to artist classification.
-- Table prefix mb_ to avoid conflicts when sharing a PostgreSQL instance.
--
-- Idempotency: every statement is re-runnable. Re-applying the schema against
-- a populated database is a no-op and does NOT drop existing data. This is a
-- requirement of the `--resume` flow (see CLAUDE.md "Resume safety"). Tests:
-- tests/idempotency_test.rs.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Reference tables

CREATE TABLE IF NOT EXISTS mb_area_type (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_gender (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_tag (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_area (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    type        integer REFERENCES mb_area_type(id)
);

CREATE TABLE IF NOT EXISTS mb_country_area (
    area        integer PRIMARY KEY REFERENCES mb_area(id)
);

-- Core artist tables

CREATE TABLE IF NOT EXISTS mb_artist (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    sort_name   text NOT NULL,
    type        integer,
    area        integer REFERENCES mb_area(id),
    gender      integer REFERENCES mb_gender(id),
    begin_area  integer REFERENCES mb_area(id),
    comment     text NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS mb_artist_alias (
    id          integer PRIMARY KEY,
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    name        text NOT NULL,
    sort_name   text NOT NULL,
    locale      text,
    type        integer,
    primary_for_locale boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS mb_artist_tag (
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    tag         integer NOT NULL REFERENCES mb_tag(id),
    count       integer NOT NULL,
    PRIMARY KEY (artist, tag)
);

-- Release matching tables

CREATE TABLE IF NOT EXISTS mb_artist_credit (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    artist_count smallint NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_artist_credit_name (
    artist_credit integer NOT NULL REFERENCES mb_artist_credit(id) ON DELETE CASCADE,
    position    smallint NOT NULL,
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    name        text NOT NULL,
    join_phrase text NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS mb_release_group (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    artist_credit integer NOT NULL REFERENCES mb_artist_credit(id),
    type        integer
);

-- Recording tables (for AcousticBrainz feature lookup)

CREATE TABLE IF NOT EXISTS mb_recording (
    id          integer PRIMARY KEY,
    gid         uuid NOT NULL,          -- MusicBrainz recording MBID
    name        text NOT NULL,
    artist_credit integer REFERENCES mb_artist_credit(id),
    length      integer                  -- milliseconds
);

CREATE TABLE IF NOT EXISTS mb_medium (
    id          integer PRIMARY KEY,
    release     integer,
    position    integer,
    format      integer
);

CREATE TABLE IF NOT EXISTS mb_track (
    id          integer PRIMARY KEY,
    recording   integer NOT NULL REFERENCES mb_recording(id),
    medium      integer NOT NULL REFERENCES mb_medium(id),
    position    integer NOT NULL,
    name        text NOT NULL,
    artist_credit integer REFERENCES mb_artist_credit(id),
    length      integer
);

-- URL/streaming tables

CREATE TABLE IF NOT EXISTS mb_url (
    id          integer PRIMARY KEY,
    gid         uuid NOT NULL,
    url         text NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_link_type (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    entity_type0 text NOT NULL,
    entity_type1 text NOT NULL
);

CREATE TABLE IF NOT EXISTS mb_link (
    id          integer PRIMARY KEY,
    link_type   integer NOT NULL REFERENCES mb_link_type(id)
);

CREATE TABLE IF NOT EXISTS mb_release (
    id              integer PRIMARY KEY,
    gid             uuid NOT NULL,
    name            text NOT NULL,
    artist_credit   integer NOT NULL REFERENCES mb_artist_credit(id),
    release_group   integer NOT NULL REFERENCES mb_release_group(id)
);

CREATE TABLE IF NOT EXISTS mb_l_release_group_url (
    id          integer PRIMARY KEY,
    link        integer NOT NULL REFERENCES mb_link(id),
    release_group integer NOT NULL REFERENCES mb_release_group(id),
    url         integer NOT NULL REFERENCES mb_url(id)
);

CREATE TABLE IF NOT EXISTS mb_l_release_url (
    id          integer PRIMARY KEY,
    link        integer NOT NULL REFERENCES mb_link(id),
    release     integer NOT NULL REFERENCES mb_release(id),
    url         integer NOT NULL REFERENCES mb_url(id)
);

-- Indexes are created separately after bulk import (see create_indexes.sql).

-- =============================================================================
-- create_indexes.sql
-- =============================================================================

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
