-- MusicBrainz cache schema for WXYC genre/area analysis.
-- Imports a subset of MusicBrainz tables relevant to artist classification.
-- Table prefix mb_ to avoid conflicts when sharing a PostgreSQL instance.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Drop in FK-safe order (children first)
DROP TABLE IF EXISTS mb_track CASCADE;
DROP TABLE IF EXISTS mb_medium CASCADE;
DROP TABLE IF EXISTS mb_recording CASCADE;
DROP TABLE IF EXISTS mb_artist_tag CASCADE;
DROP TABLE IF EXISTS mb_artist_credit_name CASCADE;
DROP TABLE IF EXISTS mb_release_group CASCADE;
DROP TABLE IF EXISTS mb_artist_credit CASCADE;
DROP TABLE IF EXISTS mb_artist_alias CASCADE;
DROP TABLE IF EXISTS mb_artist CASCADE;
DROP TABLE IF EXISTS mb_country_area CASCADE;
DROP TABLE IF EXISTS mb_area CASCADE;
DROP TABLE IF EXISTS mb_area_type CASCADE;
DROP TABLE IF EXISTS mb_gender CASCADE;
DROP TABLE IF EXISTS mb_tag CASCADE;

-- Reference tables

CREATE TABLE mb_area_type (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE mb_gender (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE mb_tag (
    id          integer PRIMARY KEY,
    name        text NOT NULL
);

CREATE TABLE mb_area (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    type        integer REFERENCES mb_area_type(id)
);

CREATE TABLE mb_country_area (
    area        integer PRIMARY KEY REFERENCES mb_area(id)
);

-- Core artist tables

CREATE TABLE mb_artist (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    sort_name   text NOT NULL,
    type        integer,
    area        integer REFERENCES mb_area(id),
    gender      integer REFERENCES mb_gender(id),
    begin_area  integer REFERENCES mb_area(id),
    comment     text NOT NULL DEFAULT ''
);

CREATE TABLE mb_artist_alias (
    id          integer PRIMARY KEY,
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    name        text NOT NULL,
    sort_name   text NOT NULL,
    locale      text,
    type        integer,
    primary_for_locale boolean NOT NULL DEFAULT false
);

CREATE TABLE mb_artist_tag (
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    tag         integer NOT NULL REFERENCES mb_tag(id),
    count       integer NOT NULL,
    PRIMARY KEY (artist, tag)
);

-- Release matching tables

CREATE TABLE mb_artist_credit (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    artist_count smallint NOT NULL
);

CREATE TABLE mb_artist_credit_name (
    artist_credit integer NOT NULL REFERENCES mb_artist_credit(id) ON DELETE CASCADE,
    position    smallint NOT NULL,
    artist      integer NOT NULL REFERENCES mb_artist(id) ON DELETE CASCADE,
    name        text NOT NULL,
    join_phrase text NOT NULL DEFAULT ''
);

CREATE TABLE mb_release_group (
    id          integer PRIMARY KEY,
    name        text NOT NULL,
    artist_credit integer NOT NULL REFERENCES mb_artist_credit(id),
    type        integer
);

-- Recording tables (for AcousticBrainz feature lookup)

CREATE TABLE mb_recording (
    id          integer PRIMARY KEY,
    gid         uuid NOT NULL,          -- MusicBrainz recording MBID
    name        text NOT NULL,
    artist_credit integer REFERENCES mb_artist_credit(id),
    length      integer                  -- milliseconds
);

CREATE TABLE mb_medium (
    id          integer PRIMARY KEY,
    release     integer,
    position    integer,
    format      integer
);

CREATE TABLE mb_track (
    id          integer PRIMARY KEY,
    recording   integer NOT NULL REFERENCES mb_recording(id),
    medium      integer NOT NULL REFERENCES mb_medium(id),
    position    integer NOT NULL,
    name        text NOT NULL,
    artist_credit integer REFERENCES mb_artist_credit(id),
    length      integer
);

-- Indexes are created separately after bulk import (see create_indexes.sql).
