use anyhow::Context;
use std::path::Path;

const SCHEMA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/schema");

/// Apply the database schema from create_database.sql.
///
/// Idempotent: every statement uses `CREATE TABLE IF NOT EXISTS` /
/// `CREATE EXTENSION IF NOT EXISTS`. Re-applying against a populated
/// database is a no-op and does NOT drop existing data. This is required
/// so that `--resume` can safely re-run the Schema step without erasing
/// the work of the Import or Filter steps. To reset the database for
/// tests or a clean rebuild, call [`drop_all_tables`] first.
pub fn apply_schema(client: &mut postgres::Client) -> anyhow::Result<()> {
    let sql_path = Path::new(SCHEMA_DIR).join("create_database.sql");
    let sql = std::fs::read_to_string(&sql_path)
        .with_context(|| format!("Failed to read {}", sql_path.display()))?;
    client.batch_execute(&sql)?;
    log::info!("Schema applied.");
    Ok(())
}

/// Drop every `mb_*` table in the public schema (CASCADE).
///
/// This is the destructive reset that used to live inside `apply_schema`.
/// It is intentionally a separate function so production runs (especially
/// `--resume`) never accidentally drop a populated database. Tests and
/// "clean rebuild" workflows that want a blank slate should call this
/// before [`apply_schema`].
pub fn drop_all_tables(client: &mut postgres::Client) -> anyhow::Result<()> {
    let rows = client.query(
        "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'mb_%'",
        &[],
    )?;
    for row in rows {
        let table: String = row.get(0);
        client.batch_execute(&format!("DROP TABLE IF EXISTS {table} CASCADE"))?;
    }
    Ok(())
}

/// Create secondary indexes from create_indexes.sql.
pub fn create_indexes(client: &mut postgres::Client) -> anyhow::Result<()> {
    let sql_path = Path::new(SCHEMA_DIR).join("create_indexes.sql");
    let sql = std::fs::read_to_string(&sql_path)
        .with_context(|| format!("Failed to read {}", sql_path.display()))?;

    for statement in sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() || statement.starts_with("--") {
            continue;
        }
        client.batch_execute(statement)?;
    }
    log::info!("Indexes created.");
    Ok(())
}

/// Run ANALYZE on all tables to update planner statistics.
pub fn analyze_tables(client: &mut postgres::Client) -> anyhow::Result<()> {
    use wxyc_etl::schema::musicbrainz::ALL_TABLES;

    for table in ALL_TABLES {
        client.batch_execute(&format!("ANALYZE {table}"))?;
    }
    log::info!("ANALYZE complete.");
    Ok(())
}
