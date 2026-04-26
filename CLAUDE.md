# musicbrainz-cache

Rust binary that builds a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports 14 table types into PostgreSQL, filters to WXYC library artists, and builds indexes.

This repo is **Rust-only**. The pipeline previously lived in `scripts/*.py` (filter_artists, import_tsv, run_pipeline, download_dump) but was ported to `src/*.rs` in `cdfd453` ("Remove Python code"). Do not reintroduce Python scripts or Python tests under this repo unless the architecture is being deliberately reversed -- the Rust binary is the supported entry point and the Rust test suite (`tests/*.rs`) covers normalization parity (`filter_test`), import row counts vs Python baselines (`parity_test`), filter behavior (`filter_test`), end-to-end import (`import_test`), and idempotency (`idempotency_test`).

## Architecture

- `src/main.rs` -- CLI orchestrator (clap). Coordinates the pipeline: download -> schema -> import -> filter -> indexes -> analyze. Consumes `PipelineState` (see Resume) so `--resume` skips already-completed steps.
- `src/download.rs` -- HTTP download (`reqwest`) and tar.bz2 extraction (parallel `lbzip2`/`pbzip2` with Rust `bzip2`+`tar` fallback).
- `src/import.rs` -- TSV import. Reads headerless MusicBrainz dump files, extracts columns by positional index, streams to PostgreSQL via COPY.
- `src/filter.rs` -- Artist filtering. Loads WXYC library.db (SQLite), matches by normalized name + aliases, prunes via copy-and-swap.
- `src/schema.rs` -- DDL application (create_database.sql, create_indexes.sql) and ANALYZE.
- `src/state.rs` -- Pipeline state persistence for resume support. Records completed steps so interrupted runs can resume.
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

## Resume

`main.rs` consumes `PipelineState` (`src/state.rs`). Each database-mutating step (Schema, Import, Filter, Indexes, Analyze) is wrapped by a `run_step` helper that checks `state.is_complete(...)` before running and persists the state file (default `./state`, override with `--state-file`) immediately on success. The Download step is not part of the state machine -- it has its own `--skip-download` flag and is naturally idempotent.

CLI contract:

- `--resume` + state file present: load and skip completed steps.
- `--resume` + no state file: warn and start fresh.
- no `--resume` + state file present: refuse with an error (avoids clobbering prior progress).
- no `--resume` + no state file: fresh run; state file created during execution.

With `--no-filter`, the Filter step is recorded as complete without running so a subsequent `--resume` can advance past it.

## Resume safety

`--resume` is only safe when two invariants hold:

1. **commit-before-save**: `state.save()` MUST run AFTER the step's PG work has committed. The `run_step` helper in `main.rs` enforces this -- it calls `f()` (which uses `postgres::Client` autocommit, so each `batch_execute`/`copy_in` commits before returning) and only then calls `state.mark_complete(...)` followed by `state.save(...)`. If the order were inverted, a crash mid-commit could leave the state file ahead of the database, causing the step to be skipped on resume despite incomplete data.
2. **idempotent steps**: every step's SQL must be safe to run twice in a row without changing observable state. A crash between PG commit and `state.save()` will cause that step to re-execute on the next `--resume`; if the step is not idempotent, that re-execution would either fail or duplicate data.

How each step satisfies idempotency:

- **Schema** (`schema/create_database.sql`): every statement uses `CREATE EXTENSION IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`. Re-applying against a populated database is a no-op and does NOT drop existing data. Tests that need a clean slate must call `schema::drop_all_tables` first.
- **Import** (`src/import.rs`): `import_table` checks `SELECT COUNT(*)` on the destination table and skips the COPY when rows are already present. This avoids the PRIMARY-KEY UniqueViolation that re-COPYing would trip and prevents duplicates on tables without a PK.
- **Filter** (`src/filter.rs`): copy-and-swap is naturally idempotent. On re-run the matching artist set is identical (same library.db, same artist names), the same rows are saved to temp tables, the originals are TRUNCATE'd, and the same rows are re-inserted. Net change: zero rows.
- **Indexes** (`schema/create_indexes.sql`): every `CREATE INDEX` uses `IF NOT EXISTS`, so re-running on an already-indexed database is a no-op.
- **Analyze** (`src/schema.rs::analyze_tables`): `ANALYZE` is inherently idempotent.

The `tests/idempotency_test.rs` integration test exercises every step twice in a row against a fixture database and asserts that row counts and the index set are unchanged on the second invocation. It is the safety net that catches regressions in any of the rules above.

## Testing

```bash
# Unit tests (no database required)
cargo test

# Integration tests (requires PostgreSQL on port 5434)
cargo test -- --ignored --test-threads=1

# Parity tests (requires TEST_DATABASE_URL)
TEST_DATABASE_URL=postgresql://musicbrainz:musicbrainz@localhost:5434/postgres \
  cargo test parity -- --ignored --test-threads=1
```

- **Unit tests** (22): TableSpec validation, column mapping, dependency ordering, normalization parity, library loading, download constants, tar.bz2 extraction, pipeline state persistence.
- **Parity tests** (12): Import row counts vs baselines, sample data verification, NULL handling, alias/tag/recording data, filtered row counts, filtered artist sets, orphan detection. Gated on `TEST_DATABASE_URL`.
- **State tests** (10): State file creation, step tracking, roundtrip serialization, resume skip logic, partial failure + resume, state clear.
- **Resume integration tests** (4): End-to-end subprocess of the binary with `--resume`. Cover full-state skip, partial-state resume (skip Schema+Import, run Filter+Indexes+Analyze), refusal when state exists without `--resume`, and warn-and-start-fresh when `--resume` is passed with no state file. Gated on `TEST_DATABASE_URL`.
- **Idempotency test** (1): Runs each pipeline step twice in a row and asserts row counts and the index set are unchanged on the second invocation. Enforces the "Resume safety" invariants. Gated on `TEST_DATABASE_URL`.
- **Integration tests** (12): Full import, NULL handling, column extraction, artist matching, pruning, orphan cleanup. Require PostgreSQL on port 5434.
