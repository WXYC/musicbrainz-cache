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
        db_columns: &["id", "name", "sort_name", "type", "area", "gender", "begin_area", "comment"],
    },
    TableSpec {
        dump_file: "artist_alias",
        table: "mb_artist_alias",
        source_indices: &[0, 1, 2, 7, 3, 6, 14],
        db_columns: &["id", "artist", "name", "sort_name", "locale", "type", "primary_for_locale"],
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
        db_columns: &["id", "recording", "medium", "position", "name", "artist_credit", "length"],
    },
];

/// Tables that come from mbdump-derived.tar.bz2 instead of mbdump.tar.bz2.
pub static DERIVED_TABLES: &[&str] = &["artist_tag", "tag"];

/// Import a single table from its TSV dump file.
///
/// Reads the full-width TSV, extracts only the columns we need,
/// and streams them to PostgreSQL via COPY.
pub fn import_table(
    _client: &mut postgres::Client,
    _spec: &TableSpec,
    _data_dir: &Path,
) -> anyhow::Result<u64> {
    todo!()
}

/// Import all tables in dependency order.
pub fn import_all(client: &mut postgres::Client, data_dir: &Path) -> anyhow::Result<u64> {
    let mut total = 0;
    for spec in TABLES {
        total += import_table(client, spec, data_dir)?;
    }
    Ok(total)
}
