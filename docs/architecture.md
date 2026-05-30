# Architecture

## Modules

- `src/main.rs` -- CLI orchestrator (clap). Exposes the standard WXYC cache-builder subcommands: `build` (full pipeline, resumable via `--resume`) and `import` (download + schema + TSV load, with `--fresh` to drop tables first). Shared `--database-url` / `--data-dir` / `--state-file` / `--resume` / `--fresh` come from `wxyc_etl::cli` (`DatabaseArgs`, `ResumableBuildArgs`, `ImportArgs`); the database URL falls back to `DATABASE_URL_MUSICBRAINZ` via `wxyc_etl::cli::resolve_database_url`. Legacy invocations without a subcommand are rewritten to `build` with a stderr deprecation warning. `build` consumes `PipelineState` so `--resume` skips already-completed steps.
- `src/download.rs` -- HTTP download (`reqwest`) and tar.bz2 extraction (parallel `lbzip2`/`pbzip2` with Rust `bzip2`+`tar` fallback).
- `src/import.rs` -- TSV import. Reads headerless MusicBrainz dump files, extracts columns by positional index, streams to PostgreSQL via COPY.
- `src/filter.rs` -- Artist filtering. Loads WXYC library.db (SQLite), matches by normalized name + aliases, prunes via copy-and-swap.
- `src/schema.rs` -- DDL application (create_database.sql, create_indexes.sql) and ANALYZE.
- `src/state.rs` -- Pipeline state persistence for resume support. Records completed steps so interrupted runs can resume.
- `schema/` -- PostgreSQL DDL (14 tables) and secondary indexes (15 indexes). Applied at runtime by `apply_schema()`. Mirrored as the baseline `migrations/0001_initial.sql` for sqlx-cli (see `docs/migrations.md`); subsequent index changes ship as numbered migrations (`0002_*.sql` ...).
- `migrations/` -- sqlx-cli migration files. `0001_initial.sql` is a snapshot of `schema/*.sql`; future schema changes ship as `0002_*`, `0003_*`, etc. Not yet wired into the deploy path (see `docs/migrations.md`).

## Table Mapping

14 tables are imported from MusicBrainz dumps. Each `TableSpec` in `src/import.rs` maps a dump filename to a target table using positional column indices. The dependency order ensures foreign key constraints are satisfied during import.

Reference tables (area_type, gender, tag) are imported first, then core tables (area, country_area, artist), then dependent tables (aliases, tags, credits, release groups, recordings, tracks).

## Filtering Strategy

Uses copy-and-swap instead of DELETE to avoid dead tuples. Steps:
1. Load matching artist IDs into a temp table
2. Save kept rows for each table into temp tables (cascading from artists -> credits -> recordings -> tracks)
3. TRUNCATE all tables together (satisfies FK constraints)
4. Re-insert kept rows from temp tables
