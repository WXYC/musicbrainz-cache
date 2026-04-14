# musicbrainz-cache

Rust binary that builds a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports 14 table types into PostgreSQL, filters to WXYC library artists, and builds indexes.

## Architecture

- `src/main.rs` -- CLI orchestrator (clap). Coordinates the pipeline: download -> schema -> import -> filter -> indexes -> analyze.
- `src/download.rs` -- HTTP download (`reqwest`) and tar.bz2 extraction (parallel `lbzip2`/`pbzip2` with Rust `bzip2`+`tar` fallback).
- `src/import.rs` -- TSV import. Reads headerless MusicBrainz dump files, extracts columns by positional index, streams to PostgreSQL via COPY.
- `src/filter.rs` -- Artist filtering. Loads WXYC library.db (SQLite), matches by normalized name + aliases, prunes via copy-and-swap.
- `src/schema.rs` -- DDL application (create_database.sql, create_indexes.sql) and ANALYZE.
- `schema/` -- PostgreSQL DDL (14 tables) and secondary indexes (14 indexes).

## Dependencies

- **wxyc-etl** (path dependency) -- `text::normalize_artist_name` for name normalization, `schema::musicbrainz` for table constants.
- **postgres** -- Synchronous PostgreSQL client (matches wxyc-etl).
- **rusqlite** -- SQLite for reading library.db.
- **reqwest** (blocking) -- HTTP client for MusicBrainz dump downloads.
- **bzip2** + **tar** -- Fallback decompression when lbzip2/pbzip2 aren't available.

## Development

```bash
# Start test database
docker compose up -d

# Run unit tests
cargo test

# Run integration tests (requires PostgreSQL on port 5434)
cargo test -- --ignored --test-threads=1

# Run the pipeline with fixture data
cargo run -- --data-dir tests/fixtures --library-db tests/fixtures/library.db --skip-download

# Lint
cargo clippy -- -D warnings -A clippy::manual_is_multiple_of
cargo fmt --check
```

## Table Mapping

14 tables are imported from MusicBrainz dumps. Each `TableSpec` in `src/import.rs` maps a dump filename to a target table using positional column indices. The dependency order ensures foreign key constraints are satisfied during import.

Reference tables (area_type, gender, tag) are imported first, then core tables (area, country_area, artist), then dependent tables (aliases, tags, credits, release groups, recordings, tracks).

## Filtering Strategy

Uses copy-and-swap instead of DELETE to avoid dead tuples. Steps:
1. Load matching artist IDs into a temp table
2. Save kept rows for each table into temp tables (cascading from artists -> credits -> recordings -> tracks)
3. TRUNCATE all tables together (satisfies FK constraints)
4. Re-insert kept rows from temp tables

## Testing

- **Unit tests** (16): TableSpec validation, column mapping, dependency ordering, normalization parity, library loading, download constants.
- **Integration tests** (12): Full import, NULL handling, column extraction, artist matching, pruning, orphan cleanup. Require PostgreSQL on port 5434.
