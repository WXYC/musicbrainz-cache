# Testing

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
- **Idempotency test** (1): Runs each pipeline step twice in a row and asserts row counts and the index set are unchanged on the second invocation. Enforces the "Resume safety" invariants (see `docs/resume.md`). Gated on `TEST_DATABASE_URL`.
- **Integration tests** (12): Full import, NULL handling, column extraction, artist matching, pruning, orphan cleanup. Require PostgreSQL on port 5434.
