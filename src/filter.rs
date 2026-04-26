use anyhow::Context;
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;
use wxyc_etl::text::normalize_artist_name;

/// Load normalized WXYC artist names from library.db.
pub fn load_library_artists(library_db: &Path) -> anyhow::Result<HashSet<String>> {
    let conn = rusqlite::Connection::open(library_db)
        .with_context(|| format!("Failed to open {}", library_db.display()))?;

    let mut stmt = conn.prepare("SELECT DISTINCT artist FROM library")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

    let mut artists = HashSet::new();
    for name in rows {
        let name = name?;
        if !name.is_empty() {
            artists.insert(normalize_artist_name(&name));
        }
    }

    log::info!(
        "Loaded {} unique WXYC artist names from library.db",
        artists.len()
    );
    Ok(artists)
}

/// Find MB artist IDs matching WXYC library artists by name or alias.
pub fn find_matching_artist_ids(
    client: &mut postgres::Client,
    library_artists: &HashSet<String>,
) -> anyhow::Result<HashSet<i32>> {
    let mut matching_ids: HashSet<i32> = HashSet::new();

    // Match by primary artist name
    log::info!("Matching by artist name...");
    let mut checked = 0u64;
    for row in client.query("SELECT id, name FROM mb_artist", &[])? {
        let id: i32 = row.get(0);
        let name: &str = row.get(1);
        if library_artists.contains(&normalize_artist_name(name)) {
            matching_ids.insert(id);
        }
        checked += 1;
        if checked % 500_000 == 0 {
            log::info!(
                "  Checked {} artists, {} matches so far",
                checked,
                matching_ids.len()
            );
        }
    }
    let name_matches = matching_ids.len();
    log::info!("Found {} matches by artist name", name_matches);

    // Match by artist alias
    log::info!("Matching by artist alias...");
    for row in client.query("SELECT artist, name FROM mb_artist_alias", &[])? {
        let artist_id: i32 = row.get(0);
        let name: &str = row.get(1);
        if library_artists.contains(&normalize_artist_name(name)) {
            matching_ids.insert(artist_id);
        }
    }
    let alias_matches = matching_ids.len() - name_matches;
    log::info!(
        "Found {} additional matches by alias ({} total)",
        alias_matches,
        matching_ids.len()
    );

    Ok(matching_ids)
}

/// Copy kept rows into a temp table. Returns (table_name, temp_name, row_count).
fn save_kept(
    client: &mut postgres::Client,
    table: &str,
    where_clause: &str,
) -> anyhow::Result<(String, String, u64)> {
    let temp = format!("_kept_{table}");
    let start = std::time::Instant::now();
    let rows = client.execute(
        &format!("CREATE TEMP TABLE {temp} AS SELECT * FROM {table} WHERE {where_clause}"),
        &[],
    )?;
    log::info!(
        "  {}: keeping {} rows ({:.1}s)",
        table,
        rows,
        start.elapsed().as_secs_f64()
    );
    Ok((table.to_string(), temp, rows))
}

/// Prune to matching artists using copy-and-swap.
///
/// Instead of deleting millions of non-matching rows (slow, generates dead tuples),
/// copies the kept rows into temp tables, truncates the originals, and re-inserts.
pub fn prune_to_matching(
    client: &mut postgres::Client,
    matching_ids: &HashSet<i32>,
) -> anyhow::Result<()> {
    log::info!("Pruning to {} matching artists...", matching_ids.len());
    let start = std::time::Instant::now();

    // Load matching IDs into a temp table
    client.batch_execute("CREATE TEMP TABLE _keep_ids (id integer PRIMARY KEY)")?;
    {
        let mut writer = client.copy_in("COPY _keep_ids (id) FROM STDIN")?;
        for &id in matching_ids {
            writeln!(writer, "{}", id)?;
        }
        writer.finish()?;
    }

    // Disable FK triggers for the truncate/re-insert
    client.batch_execute("SET session_replication_role = 'replica'")?;

    // Phase 1: Save kept rows to temp tables.
    // Order matters: later queries reference earlier temp tables.
    log::info!("Phase 1: selecting kept rows...");
    // Built sequentially: later queries reference earlier temp tables.
    let mut swaps = vec![save_kept(
        client,
        "mb_artist",
        "id IN (SELECT id FROM _keep_ids)",
    )?];
    swaps.push(save_kept(
        client,
        "mb_artist_alias",
        "artist IN (SELECT id FROM _keep_ids)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_artist_tag",
        "artist IN (SELECT id FROM _keep_ids)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_artist_credit_name",
        "artist IN (SELECT id FROM _keep_ids)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_artist_credit",
        "id IN (SELECT DISTINCT artist_credit FROM _kept_mb_artist_credit_name)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_release_group",
        "artist_credit IN (SELECT id FROM _kept_mb_artist_credit)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_recording",
        "artist_credit IN (SELECT id FROM _kept_mb_artist_credit)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_track",
        "recording IN (SELECT id FROM _kept_mb_recording)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_medium",
        "id IN (SELECT DISTINCT medium FROM _kept_mb_track)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_tag",
        "id IN (SELECT DISTINCT tag FROM _kept_mb_artist_tag)",
    )?);
    swaps.push(save_kept(
        client,
        "mb_area",
        "id IN (\
            SELECT area FROM _kept_mb_artist WHERE area IS NOT NULL \
            UNION SELECT begin_area FROM _kept_mb_artist WHERE begin_area IS NOT NULL\
        ) OR id IN (SELECT area FROM mb_country_area)",
    )?);
    // Include reference tables in the truncate to satisfy FK constraints.
    // country_area: keep rows where area is in the kept area set.
    // area_type and gender: keep all rows (tiny reference tables).
    swaps.push(save_kept(
        client,
        "mb_country_area",
        "area IN (SELECT id FROM _kept_mb_area)",
    )?);
    swaps.push(save_kept(client, "mb_area_type", "TRUE")?);
    swaps.push(save_kept(client, "mb_gender", "TRUE")?);

    // Phase 2: Truncate all tables in one statement.
    //
    // `mb_release` is included even though we don't import or filter it: the schema
    // declares an FK from `mb_release.artist_credit` to `mb_artist_credit(id)`, and
    // PostgreSQL refuses to TRUNCATE a referenced table unless every referencing
    // table is in the same TRUNCATE statement. Listing it here keeps the table
    // empty (its production state) and satisfies PG's atomic FK check.
    log::info!("Phase 2: truncating tables...");
    let mut all_tables: Vec<&str> = swaps.iter().map(|(t, _, _)| t.as_str()).collect();
    all_tables.push("mb_release");
    client.batch_execute(&format!("TRUNCATE {}", all_tables.join(", ")))?;

    // Phase 3: Re-insert kept rows.
    log::info!("Phase 3: re-inserting kept rows...");
    for (table, temp, _) in &swaps {
        let t = std::time::Instant::now();
        client.execute(&format!("INSERT INTO {table} SELECT * FROM {temp}"), &[])?;
        log::info!("  {}: inserted ({:.1}s)", table, t.elapsed().as_secs_f64());
        client.execute(&format!("DROP TABLE {temp}"), &[])?;
    }

    // Clean up
    client.batch_execute("DROP TABLE _keep_ids")?;
    client.batch_execute("SET session_replication_role = 'origin'")?;

    log::info!("Pruning complete in {:.1}s", start.elapsed().as_secs_f64());
    Ok(())
}

/// Report row counts for all tables.
pub fn report_sizes(client: &mut postgres::Client) -> anyhow::Result<()> {
    use wxyc_etl::schema::musicbrainz::ALL_TABLES;

    log::info!("Table sizes after filtering:");
    for table in ALL_TABLES {
        let row = client.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])?;
        let count: i64 = row.get(0);
        log::info!("  {:30} {:>10} rows", table, count);
    }
    Ok(())
}
