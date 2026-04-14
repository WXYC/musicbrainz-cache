/// Apply the database schema from create_database.sql.
pub fn apply_schema(_client: &mut postgres::Client) -> anyhow::Result<()> {
    todo!()
}

/// Create secondary indexes from create_indexes.sql.
pub fn create_indexes(_client: &mut postgres::Client) -> anyhow::Result<()> {
    todo!()
}

/// Run ANALYZE on all tables.
pub fn analyze_tables(_client: &mut postgres::Client) -> anyhow::Result<()> {
    todo!()
}
