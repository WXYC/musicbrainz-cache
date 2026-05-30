# Scheduling

The full rebuild runs on GitHub Actions via `.github/workflows/rebuild-cache.yml`:

- **Cron**: `0 6 5 * *` — 06:00 UTC on the 5th of each month. Offset from `discogs-etl`'s monthly rebuild so two large jobs don't co-run.
- **Manual**: `workflow_dispatch` exposes two inputs: `dump_url` (override the auto-detected MusicBrainz dump URL) and `skip_download` (reuse `mbdump` already on the runner — only useful when chaining a re-run after `dump_url` was wrong).
- **Library DB**: fetched from the `streaming-data-v1` release on `WXYC/library-metadata-lookup` (where `discogs-etl`'s daily Sync Library job publishes it).
- **Required secrets** (operator-provisioned, not created here): `DATABASE_URL_MUSICBRAINZ`, optional `SENTRY_DSN`.
- **Runner capacity**: the job uses `ubuntu-latest` with a 350-minute timeout. The dump is ~6 GB compressed / ~30 GB extracted, which fits the GitHub-hosted runner's disk budget but not by a wide margin. If runs start failing on disk or the 6h job limit, move to a self-hosted runner (TODO comment in the workflow).
