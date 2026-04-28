# musicbrainz-cache

Rust binary that builds a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports 14 table types into PostgreSQL, filters to WXYC library artists, and builds indexes.

This repo is **Rust-only**. The pipeline previously lived in `scripts/*.py` (filter_artists, import_tsv, run_pipeline, download_dump) but was ported to `src/*.rs` in `cdfd453` ("Remove Python code"). Do not reintroduce Python scripts or Python tests under this repo unless the architecture is being deliberately reversed -- the Rust binary is the supported entry point and the Rust test suite (`tests/*.rs`) covers normalization parity (`filter_test`), import row counts vs Python baselines (`parity_test`), filter behavior (`filter_test`), end-to-end import (`import_test`), and idempotency (`idempotency_test`).

## Architecture

- `src/main.rs` -- CLI orchestrator (clap). Exposes the standard WXYC cache-builder subcommands: `build` (full pipeline, resumable via `--resume`) and `import` (download + schema + TSV load, with `--fresh` to drop tables first). Shared `--database-url` / `--data-dir` / `--state-file` / `--resume` / `--fresh` come from `wxyc_etl::cli` (`DatabaseArgs`, `ResumableBuildArgs`, `ImportArgs`); the database URL falls back to `DATABASE_URL_MUSICBRAINZ` via `wxyc_etl::cli::resolve_database_url`. Legacy invocations without a subcommand are rewritten to `build` with a stderr deprecation warning. `build` consumes `PipelineState` so `--resume` skips already-completed steps.
- `src/download.rs` -- HTTP download (`reqwest`) and tar.bz2 extraction (parallel `lbzip2`/`pbzip2` with Rust `bzip2`+`tar` fallback).
- `src/import.rs` -- TSV import. Reads headerless MusicBrainz dump files, extracts columns by positional index, streams to PostgreSQL via COPY.
- `src/filter.rs` -- Artist filtering. Loads WXYC library.db (SQLite), matches by normalized name + aliases, prunes via copy-and-swap.
- `src/schema.rs` -- DDL application (create_database.sql, create_indexes.sql) and ANALYZE.
- `src/state.rs` -- Pipeline state persistence for resume support. Records completed steps so interrupted runs can resume.
- `schema/` -- PostgreSQL DDL (14 tables) and secondary indexes (15 indexes). Applied at runtime by `apply_schema()`. Mirrored as the baseline `migrations/0001_initial.sql` for sqlx-cli (see "Migrations"); subsequent index changes ship as numbered migrations (`0002_*.sql` ...).
- `migrations/` -- sqlx-cli migration files. `0001_initial.sql` is a snapshot of `schema/*.sql`; future schema changes ship as `0002_*`, `0003_*`, etc. Not yet wired into the deploy path (see "Migrations").

## Observability

`src/main.rs` calls `wxyc_etl::logger::init` at the top of `main` and holds the returned guard for the lifetime of the process. Every log line is emitted as a single JSON object with the four cross-pipeline tags: `repo = "musicbrainz-cache"`, `tool = "musicbrainz-cache build"`, `step` (per-event), and `run_id` (UUIDv4 generated at startup). `repo` and `tool` are attached via a root span entered for the lifetime of `main`; `log::*` events are bridged into that span by `tracing_log::LogTracer`.

When `SENTRY_DSN` is set in the environment, panics and `tracing::error!` events forward to Sentry tagged with the same fields. With no DSN, Sentry stays inactive but JSON logging still initializes — provisioning the DSN in Railway / GitHub Actions / EC2 is tracked separately (see the `TODO(sentry-dsn)` comment in `main.rs`).

## Scheduling

The full rebuild runs on GitHub Actions via `.github/workflows/rebuild-cache.yml`:

- **Cron**: `0 6 5 * *` — 06:00 UTC on the 5th of each month. Offset from `discogs-etl`'s monthly rebuild so two large jobs don't co-run.
- **Manual**: `workflow_dispatch` exposes two inputs: `dump_url` (override the auto-detected MusicBrainz dump URL) and `skip_download` (reuse `mbdump` already on the runner — only useful when chaining a re-run after `dump_url` was wrong).
- **Library DB**: fetched from the `streaming-data-v1` release on `WXYC/library-metadata-lookup` (where `discogs-etl`'s daily Sync Library job publishes it).
- **Required secrets** (operator-provisioned, not created here): `DATABASE_URL_MUSICBRAINZ`, optional `SENTRY_DSN`.
- **Runner capacity**: the job uses `ubuntu-latest` with a 350-minute timeout. The dump is ~6 GB compressed / ~30 GB extracted, which fits the GitHub-hosted runner's disk budget but not by a wide margin. If runs start failing on disk or the 6h job limit, move to a self-hosted runner (TODO comment in the workflow).

## Dependencies

- **wxyc-etl** (`= "0.1.0"`, crates.io) -- `text::normalize_artist_name` for name normalization, `schema::musicbrainz` for table constants, `logger::init` for Sentry + structured JSON logs.
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
DATABASE_URL_MUSICBRAINZ=postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz \
    cargo run -- build --data-dir tests/fixtures --library-db tests/fixtures/library.db --skip-download

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

## Migrations

Schema evolution uses [`sqlx-cli`](https://crates.io/crates/sqlx-cli). Migration files live in `migrations/` at the repo root and are applied in lex order (`0001_initial.sql`, `0002_*.sql`, ...).

**Status**: `sqlx migrate run` is wired into the monthly rebuild workflow (`.github/workflows/rebuild-cache.yml`) and runs before the rebuild itself. Every migration is idempotent (`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`), so applying against a populated prod DB is a no-op other than populating `_sqlx_migrations`. Incremental schema changes added between rebuilds are picked up on the next monthly run; the runtime `src/schema.rs::apply_schema()` path is still the source of truth for fresh-rebuild DDL and stays in sync because every new migration is also written into `schema/create_database.sql` / `schema/create_indexes.sql`.

**Install the CLI** (not a Cargo dep -- runtime uses the `postgres` crate):

```bash
cargo install sqlx-cli --no-default-features --features postgres
```

**Add a new migration**:

```bash
# Generates migrations/<timestamp>_<name>.sql (or 0002_<name>.sql with --sequential)
sqlx migrate add --source migrations <name>
```

**Run migrations against an empty Postgres** (smoke test):

```bash
docker compose up -d
createdb -h localhost -p 5434 -U musicbrainz musicbrainz_migrations_test
sqlx migrate run \
    --database-url postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz_migrations_test \
    --source migrations
```

**Idempotency is mandatory**: because the rebuild workflow re-applies every migration on every run, every statement must be re-runnable (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). This is enforced informally by code review; non-idempotent migrations would error on the second monthly run.

**Adding a schema change**: write a new `migrations/000N_*.sql` AND update the corresponding `schema/*.sql` file so fresh rebuilds (`apply_schema()`) produce the same end-state as the migration sequence. Re-applying both paths against the same DB must be a no-op.

## Resume

`main.rs` consumes `PipelineState` (`src/state.rs`) inside the `build` subcommand. Each database-mutating step (Schema, Import, Filter, Indexes, Analyze) is wrapped by a `run_step` helper that checks `state.is_complete(...)` before running and persists the state file (default `./state.json`, override with `--state-file`) immediately on success. The Download step is not part of the state machine -- it has its own `--skip-download` flag and is naturally idempotent. The `import` subcommand runs a one-shot download + schema + TSV load and does not use a state file.

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
