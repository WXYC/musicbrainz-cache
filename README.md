# musicbrainz-cache

ETL pipeline for building a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports into PostgreSQL, filters to WXYC library artists, and builds indexes for querying.

## Quick Start

```bash
# Start PostgreSQL
docker compose up -d

# Build
cargo build --release

# Run the full pipeline
./target/release/musicbrainz-cache \
    --data-dir data/ \
    --library-db data/library.db
```

## Usage

```
musicbrainz-cache [OPTIONS] --data-dir <DATA_DIR>

Options:
    --data-dir <DATA_DIR>          Directory for downloads and extracted files
    --library-db <LIBRARY_DB>      Path to library.db (required unless --no-filter)
    --database-url <DATABASE_URL>  PostgreSQL URL [default: postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz]
    --skip-download                Skip download, use existing files
    --no-filter                    Import all artists without filtering
    --dump-url <DUMP_URL>          Override dump URL (default: auto-detect latest)
```

## Pipeline Steps

1. **Download** -- Fetches `mbdump.tar.bz2` and `mbdump-derived.tar.bz2` from data.metabrainz.org. Uses parallel decompression via lbzip2/pbzip2 when available.
2. **Schema** -- Applies `schema/create_database.sql` (DROP + CREATE for 14 tables).
3. **Import** -- Reads headerless TSV files, extracts needed columns by positional index, streams to PostgreSQL via COPY. 14 tables imported in FK dependency order.
4. **Filter** -- Matches MusicBrainz artists against WXYC library.db by normalized name and aliases. Prunes non-matching data using copy-and-swap for efficiency.
5. **Index** -- Creates 14 secondary indexes from `schema/create_indexes.sql`.
6. **Analyze** -- Runs ANALYZE on all tables for query planner statistics.

## Tables

| Table | Source | Columns |
|-------|--------|---------|
| mb_area_type | area_type | id, name |
| mb_gender | gender | id, name |
| mb_tag | tag (derived) | id, name |
| mb_area | area | id, name, type |
| mb_country_area | country_area | area |
| mb_artist | artist | id, name, sort_name, type, area, gender, begin_area, comment |
| mb_artist_alias | artist_alias | id, artist, name, sort_name, locale, type, primary_for_locale |
| mb_artist_tag | artist_tag (derived) | artist, tag, count |
| mb_artist_credit | artist_credit | id, name, artist_count |
| mb_artist_credit_name | artist_credit_name | artist_credit, position, artist, name, join_phrase |
| mb_release_group | release_group | id, name, artist_credit, type |
| mb_recording | recording | id, gid, name, artist_credit, length |
| mb_medium | medium | id, release, position, format |
| mb_track | track | id, recording, medium, position, name, artist_credit, length |

## Development

```bash
# Start test database
docker compose up -d

# Run unit tests
cargo test

# Run integration tests (requires PostgreSQL on port 5434)
cargo test -- --ignored --test-threads=1

# Lint
cargo clippy -- -D warnings
cargo fmt --check
```

## Requirements

- Rust 1.75+
- PostgreSQL 16 (via Docker Compose on port 5434)
- [wxyc-etl](https://github.com/WXYC/wxyc-etl) crate (path dependency at `../../../wxyc-etl/wxyc-etl`)
- Optional: lbzip2 or pbzip2 for faster archive extraction
