use std::collections::HashSet;
use std::path::Path;

/// Load normalized WXYC artist names from library.db.
pub fn load_library_artists(_library_db: &Path) -> anyhow::Result<HashSet<String>> {
    todo!()
}

/// Find MB artist IDs matching WXYC library artists by name or alias.
pub fn find_matching_artist_ids(
    _client: &mut postgres::Client,
    _library_artists: &HashSet<String>,
) -> anyhow::Result<HashSet<i32>> {
    todo!()
}

/// Prune to matching artists using copy-and-swap.
pub fn prune_to_matching(
    _client: &mut postgres::Client,
    _matching_ids: &HashSet<i32>,
) -> anyhow::Result<()> {
    todo!()
}

/// Report row counts for all tables.
pub fn report_sizes(_client: &mut postgres::Client) -> anyhow::Result<()> {
    todo!()
}
