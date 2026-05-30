# musicbrainz-cache

Rust binary that builds a WXYC-filtered MusicBrainz cache database. Downloads MusicBrainz data dumps, imports 14 table types into PostgreSQL, filters to WXYC library artists, and builds indexes.

This repo is **Rust-only**. The pipeline previously lived in `scripts/*.py` (filter_artists, import_tsv, run_pipeline, download_dump) but was ported to `src/*.rs` in `cdfd453` ("Remove Python code"). Do not reintroduce Python scripts or Python tests under this repo unless the architecture is being deliberately reversed -- the Rust binary is the supported entry point and the Rust test suite (`tests/*.rs`) covers normalization parity (`filter_test`), import row counts vs Python baselines (`parity_test`), filter behavior (`filter_test`), end-to-end import (`import_test`), and idempotency (`idempotency_test`).

## Topic guides

CLAUDE.md is a router for the always-loaded reference card. Topic depth lives in `docs/`:

- **[`docs/architecture.md`](docs/architecture.md)** — Module layout (`src/main.rs`, `download`, `import`, `filter`, `schema`, `state`), the 14 imported table mapping, copy-and-swap filtering strategy
- **[`docs/observability.md`](docs/observability.md)** — `wxyc_etl::logger::init` setup, the four required tags (`repo`, `tool`, `step`, `run_id`), Sentry forwarding via `SENTRY_DSN`
- **[`docs/scheduling.md`](docs/scheduling.md)** — Monthly GH Actions cron (`0 6 5 * *`), `workflow_dispatch` inputs, library.db source, required secrets, runner capacity notes
- **[`docs/migrations.md`](docs/migrations.md)** — sqlx-cli migration files, monthly rebuild wiring, idempotency-mandatory rule, dual-update rule for `schema/*.sql` + `migrations/000N_*.sql`
- **[`docs/resume.md`](docs/resume.md)** — `--resume` CLI contract, per-step idempotency analysis, the commit-before-save invariant, `idempotency_test` safety net
- **[`docs/testing.md`](docs/testing.md)** — Test commands, suite breakdown (unit, parity, state, resume, idempotency, integration), `TEST_DATABASE_URL` gating

Read the relevant topic doc before doing work in that area.

## Dependencies

- **wxyc-etl** (`"0.4.0"`, crates.io) -- `text::to_match_form` (WX-2 Normalizer Charter, comparison form) for artist-name matching, `schema::musicbrainz` for table constants, `logger::init` for Sentry + structured JSON logs. v0.4.0 also ships the canonical `wxyc_identity_match_*` plpgsql sources under `data/`; this repo vendors `wxyc_unaccent.rules` + `wxyc_identity_match_functions.sql` under `vendor/wxyc-etl/` (top-level `data/` is .gitignored for MB dumps) plus the parity fixture under `tests/fixtures/identity_normalization_cases.csv` (all SHA-pinned in `wxyc-etl-pin.txt`) and deploys the function family via `migrations/0005_wxyc_identity_match_functions.sql`.
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
