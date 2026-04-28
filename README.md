# musicbrainz-cache

ETL pipeline for building a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports into PostgreSQL, filters to WXYC library artists, and builds indexes for querying.

## Quick Start

```bash
# Start PostgreSQL
docker compose up -d

# Build
cargo build --release

# Run the full pipeline
export DATABASE_URL_MUSICBRAINZ=postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz
./target/release/musicbrainz-cache build \
    --data-dir data/ \
    --library-db data/library.db
```

## Usage

`musicbrainz-cache` exposes two subcommands matching the standard WXYC cache-builder shape:

```
musicbrainz-cache build [OPTIONS]    # full pipeline (resumable)
musicbrainz-cache import [OPTIONS]   # download + schema + TSV import only
```

### `build` — full pipeline

Download the dump, apply the schema, import TSVs, filter to the WXYC library, build indexes, and ANALYZE. Each database-mutating step is idempotent and resumable via `--resume`.

```
musicbrainz-cache build [OPTIONS]

Options:
    --database-url <URL>           PostgreSQL URL (falls back to DATABASE_URL_MUSICBRAINZ)
    --data-dir <PATH>              Working data directory [default: ./data]
    --state-file <PATH>            Path to the pipeline state file [default: ./state.json]
    --resume                       Resume from the existing state file
    --library-db <PATH>            Path to library.db (required unless --no-filter)
    --skip-download                Skip download, use existing files in --data-dir
    --no-filter                    Import all artists without filtering to the WXYC library
    --dump-url <URL>               Override dump URL (default: auto-detect latest)
```

### `import` — fresh dump load

Download (unless `--skip-download`), apply the schema, and import the dump TSVs. Use `--fresh` to drop the `mb_*` tables before importing.

```
musicbrainz-cache import [OPTIONS]

Options:
    --database-url <URL>           PostgreSQL URL (falls back to DATABASE_URL_MUSICBRAINZ)
    --data-dir <PATH>              Working data directory [default: ./data]
    --fresh                        Drop existing mb_* tables before importing
    --skip-download                Skip download, use existing files in --data-dir
    --dump-url <URL>               Override dump URL (default: auto-detect latest)
```

### Database URL

Every cache builder follows the same `--database-url` convention: the flag wins, otherwise the tool's environment variable (`DATABASE_URL_MUSICBRAINZ` for this binary) is used. If neither is set, the binary errors out.

```bash
# Either of these works:
export DATABASE_URL_MUSICBRAINZ=postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz
musicbrainz-cache build --data-dir data/ --library-db data/library.db

musicbrainz-cache build \
    --database-url postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz \
    --data-dir data/ \
    --library-db data/library.db
```

### Legacy invocation (deprecated)

Pre-#24 invocations without a subcommand (`musicbrainz-cache --data-dir ... --library-db ...`) still work but log a deprecation warning to stderr; they are rewritten internally to `musicbrainz-cache build ...`. New scripts should always use the explicit `build` or `import` subcommand.

## Resume

The `build` pipeline persists per-step completion to a state file (`--state-file`, default `./state.json`) after each successful step. To recover from a crashed or interrupted run, re-invoke with `--resume`: previously-completed steps log a "Skipping ..." message and are not re-executed; the run picks up at the first incomplete step and continues to completion.

State semantics:

| `--resume` | state file exists | Behavior |
|---|---|---|
| no  | no  | Fresh run; state file is created during the run. |
| no  | yes | Refused with an error. Pass `--resume` to continue, or remove the file to start fresh. This avoids accidentally clobbering a prior run's progress. |
| yes | no  | Logs a warning and starts fresh; state file is created during the run. |
| yes | yes | Loads completed steps and resumes; remaining steps execute and update the file. |

Only the database-mutating steps (Schema, Import, Filter, Indexes, Analyze) participate in resume. The Download step is governed separately by `--skip-download` and is naturally idempotent (existing archives are reused). With `--no-filter`, the Filter step is recorded as complete without running so subsequent steps can advance during a later resume.

## Pipeline Steps

1. **Download** -- Fetches `mbdump.tar.bz2` and `mbdump-derived.tar.bz2` from data.metabrainz.org. Uses parallel decompression via lbzip2/pbzip2 when available.
2. **Schema** -- Applies `schema/create_database.sql` (CREATE TABLE IF NOT EXISTS for 14 tables).
3. **Import** -- Reads headerless TSV files, extracts needed columns by positional index, streams to PostgreSQL via COPY. 14 tables imported in FK dependency order.
4. **Filter** -- Matches MusicBrainz artists against WXYC library.db by normalized name and aliases. Prunes non-matching data using copy-and-swap for efficiency.
5. **Index** -- Creates 15 secondary indexes from `schema/create_indexes.sql` (includes a `pg_trgm` GIN index on `lower(mb_artist.name)` for LML's fuzzy-search fallback).
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
- [wxyc-etl](https://crates.io/crates/wxyc-etl) crate (resolved from crates.io; pinned to `0.1.0` in `Cargo.toml`)
- Optional: lbzip2 or pbzip2 for faster archive extraction
