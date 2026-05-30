# Resume

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
