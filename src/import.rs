use anyhow::Context;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

/// Mapping from a MusicBrainz dump file to our schema.
///
/// MusicBrainz TSV files are headerless, so we use positional indices
/// to extract the columns we need.
pub struct TableSpec {
    /// Filename inside mbdump/ (e.g., "artist")
    pub dump_file: &'static str,
    /// Target table name (e.g., "mb_artist")
    pub table: &'static str,
    /// Column positions to extract from the TSV (0-based)
    pub source_indices: &'static [usize],
    /// Corresponding column names in our schema
    pub db_columns: &'static [&'static str],
}

/// All table specs in dependency order (reference tables first).
pub static TABLES: &[TableSpec] = &[
    TableSpec {
        dump_file: "area_type",
        table: "mb_area_type",
        source_indices: &[0, 1],
        db_columns: &["id", "name"],
    },
    TableSpec {
        dump_file: "gender",
        table: "mb_gender",
        source_indices: &[0, 1],
        db_columns: &["id", "name"],
    },
    TableSpec {
        dump_file: "tag",
        table: "mb_tag",
        source_indices: &[0, 1],
        db_columns: &["id", "name"],
    },
    TableSpec {
        dump_file: "area",
        table: "mb_area",
        source_indices: &[0, 2, 3],
        db_columns: &["id", "name", "type"],
    },
    TableSpec {
        dump_file: "country_area",
        table: "mb_country_area",
        source_indices: &[0],
        db_columns: &["area"],
    },
    TableSpec {
        dump_file: "artist",
        table: "mb_artist",
        source_indices: &[0, 2, 3, 10, 11, 12, 17, 13],
        db_columns: &[
            "id",
            "name",
            "sort_name",
            "type",
            "area",
            "gender",
            "begin_area",
            "comment",
        ],
    },
    TableSpec {
        dump_file: "artist_alias",
        table: "mb_artist_alias",
        source_indices: &[0, 1, 2, 7, 3, 6, 14],
        db_columns: &[
            "id",
            "artist",
            "name",
            "sort_name",
            "locale",
            "type",
            "primary_for_locale",
        ],
    },
    TableSpec {
        dump_file: "artist_tag",
        table: "mb_artist_tag",
        source_indices: &[0, 1, 2],
        db_columns: &["artist", "tag", "count"],
    },
    TableSpec {
        dump_file: "artist_credit",
        table: "mb_artist_credit",
        source_indices: &[0, 1, 2],
        db_columns: &["id", "name", "artist_count"],
    },
    TableSpec {
        dump_file: "artist_credit_name",
        table: "mb_artist_credit_name",
        source_indices: &[0, 1, 2, 3, 4],
        db_columns: &["artist_credit", "position", "artist", "name", "join_phrase"],
    },
    TableSpec {
        dump_file: "release_group",
        table: "mb_release_group",
        source_indices: &[0, 2, 3, 4],
        db_columns: &["id", "name", "artist_credit", "type"],
    },
    TableSpec {
        dump_file: "recording",
        table: "mb_recording",
        source_indices: &[0, 1, 2, 3, 4],
        db_columns: &["id", "gid", "name", "artist_credit", "length"],
    },
    TableSpec {
        dump_file: "medium",
        table: "mb_medium",
        source_indices: &[0, 1, 2, 3],
        db_columns: &["id", "release", "position", "format"],
    },
    TableSpec {
        dump_file: "track",
        table: "mb_track",
        source_indices: &[0, 2, 3, 4, 6, 7, 8],
        db_columns: &[
            "id",
            "recording",
            "medium",
            "position",
            "name",
            "artist_credit",
            "length",
        ],
    },
];

/// Tables that come from mbdump-derived.tar.bz2 instead of mbdump.tar.bz2.
pub static DERIVED_TABLES: &[&str] = &["artist_tag", "tag"];

/// Import a single table from its TSV dump file.
///
/// Reads the full-width TSV, extracts only the columns we need,
/// and streams them to PostgreSQL via COPY.
pub fn import_table(
    client: &mut postgres::Client,
    spec: &TableSpec,
    data_dir: &Path,
) -> anyhow::Result<u64> {
    let tsv_path = data_dir.join(spec.dump_file);
    if !tsv_path.exists() {
        log::warn!("File not found, skipping: {}", tsv_path.display());
        return Ok(0);
    }

    let start = std::time::Instant::now();
    let file = std::fs::File::open(&tsv_path)
        .with_context(|| format!("Failed to open {}", tsv_path.display()))?;
    let reader = BufReader::new(file);

    // Read and extract columns into a buffer
    let mut buf = Vec::new();
    let mut row_count: u64 = 0;

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split('\t').collect();

        // Extract columns by index; skip rows with too few columns
        let max_idx = spec.source_indices.iter().copied().max().unwrap_or(0);
        if parts.len() <= max_idx {
            continue;
        }

        let extracted: Vec<&str> = spec.source_indices.iter().map(|&i| parts[i]).collect();
        for (j, val) in extracted.iter().enumerate() {
            if j > 0 {
                buf.push(b'\t');
            }
            buf.extend_from_slice(val.as_bytes());
        }
        buf.push(b'\n');
        row_count += 1;

        if row_count % 500_000 == 0 {
            log::info!("  {}: {} rows read...", spec.dump_file, row_count);
        }
    }

    let buf_size = buf.len();
    log::info!(
        "  {}: read complete ({} rows, {} MB), copying to {}...",
        spec.dump_file,
        row_count,
        buf_size / (1024 * 1024),
        spec.table,
    );

    // Stream to PostgreSQL via COPY
    let columns = spec.db_columns.join(", ");
    let copy_stmt = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text)",
        spec.table, columns
    );
    let mut writer = client.copy_in(&copy_stmt)?;
    writer.write_all(&buf)?;
    writer.finish()?;

    let elapsed = start.elapsed();
    log::info!(
        "  {} -> {}: {} rows in {:.1}s",
        spec.dump_file,
        spec.table,
        row_count,
        elapsed.as_secs_f64(),
    );
    Ok(row_count)
}

/// Import all tables in dependency order.
pub fn import_all(client: &mut postgres::Client, data_dir: &Path) -> anyhow::Result<u64> {
    let start = std::time::Instant::now();
    let mut total = 0;
    for spec in TABLES {
        total += import_table(client, spec, data_dir)?;
    }
    let elapsed = start.elapsed();
    log::info!(
        "Import complete: {} total rows in {:.1}s",
        total,
        elapsed.as_secs_f64()
    );
    Ok(total)
}
