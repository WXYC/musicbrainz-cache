# Observability

`src/main.rs` calls `wxyc_etl::logger::init` at the top of `main` and holds the returned guard for the lifetime of the process. Every log line is emitted as a single JSON object with the four cross-pipeline tags: `repo = "musicbrainz-cache"`, `tool = "musicbrainz-cache build"`, `step` (per-event), and `run_id` (UUIDv4 generated at startup). `repo` and `tool` are attached via a root span entered for the lifetime of `main`; `log::*` events are bridged into that span by `tracing_log::LogTracer`.

When `SENTRY_DSN` is set in the environment, panics and `tracing::error!` events forward to Sentry tagged with the same fields. With no DSN, Sentry stays inactive but JSON logging still initializes — provisioning the DSN in Railway / GitHub Actions / EC2 is tracked separately (see the `TODO(sentry-dsn)` comment in `main.rs`).
