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
