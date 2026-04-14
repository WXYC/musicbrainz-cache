use anyhow::Context;
use std::path::Path;

const SCHEMA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/schema");

/// Apply the database schema from create_database.sql.
pub fn apply_schema(client: &mut postgres::Client) -> anyhow::Result<()> {
    let sql_path = Path::new(SCHEMA_DIR).join("create_database.sql");
    let sql = std::fs::read_to_string(&sql_path)
        .with_context(|| format!("Failed to read {}", sql_path.display()))?;
    client.batch_execute(&sql)?;
    log::info!("Schema applied.");
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
