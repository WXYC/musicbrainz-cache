# Migrations

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
