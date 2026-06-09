//! WXYC library hook loader for the Homebrew musicbrainz-cache.
//!
//! Implements E1 §4.1.2 of the cross-cache-identity rollout:
//! <https://github.com/WXYC/wiki/blob/main/plans/library-hook-canonicalization.md#412-homebrew-musicbrainz-port-5432>
//!
//! Reads a SQLite `library.db` (the same file [`crate::filter`] uses to
//! determine which MusicBrainz artists to keep) and writes one row per
//! library release into the consolidated `wxyc_library` table created by
//! migration `0003_wxyc_library_v2.sql`.
//!
//! # Normalization
//!
//! The new schema's `norm_artist` / `norm_title` / `norm_label` columns are
//! populated by [`wxyc_etl::text::to_identity_match_form`] (the locked-on
//! baseline; same function used for `norm_label` since labels share the
//! artist-side pipeline) and [`wxyc_etl::text::to_identity_match_form_title`]
//! for titles. **This is intentionally NOT [`wxyc_etl::text::to_match_form`]**
//! — the WX-2 comparison form lives in `filter.rs` for artist matching, but
//! the cross-cache-identity hook stays on the locked-on identity baseline so
//! every consumer cache normalizes identically.
//!
//! # Idempotency
//!
//! `INSERT ... ON CONFLICT (library_id) DO NOTHING` makes the loader safe to
//! re-run. Re-running against the same library.db is a no-op modulo the
//! `snapshot_at` of the rows that were already present (which is preserved by
//! `DO NOTHING`).
//!
//! # Backend ID columns
//!
//! `artist_id` / `label_id` / `format_id` / `release_year` are stamped NULL
//! today: library.db (the SQLite catalog export this cache reads) does not
//! carry Backend's integer IDs. They exist in the schema (per wiki §3.1) for
//! forward compatibility with a future Backend-direct loader.

use anyhow::Context;
use std::io::Write;
use std::path::Path;
use wxyc_etl::pg::to_pg_text_form;
use wxyc_etl::text::{to_identity_match_form, to_identity_match_form_title};

/// Snapshot-source vocabulary mirroring the §3.1 CHECK constraint.
///
/// Loaders pass one of these to indicate which CatalogSource produced the
/// row. The cache-side CHECK rejects anything else.
const VALID_SNAPSHOT_SOURCES: &[&str] = &["backend", "tubafrenzy", "llm"];

/// Audit string identifying the locked-on baseline normalizer.
///
/// Mirrored in the discogs-etl loader (`loaders/wxyc.py::NORMALIZER_NAME`).
/// Tests and integration audits assert this constant verbatim so a future
/// API rename in `wxyc_etl::text` is caught immediately.
pub const NORMALIZER_NAME: &str = "wxyc_etl::text::to_identity_match_form";

/// One row to be written into `wxyc_library`.
///
/// Mirrors §3.1's column list. `artist_id` / `label_id` / `format_id` /
/// `release_year` are nullable: per-cache loaders populate what their
/// source exposes, and library.db does not carry Backend's integer IDs.
#[derive(Debug, Clone)]
struct LibraryRow {
    library_id: i32,
    artist_name: String,
    album_title: String,
    label_name: Option<String>,
    format_name: Option<String>,
    wxyc_genre: Option<String>,
    call_letters: Option<String>,
    call_numbers: Option<i32>,
}

/// Adapt to whatever optional columns are present in the library.db.
///
/// The minimal-fixture schema is `(artist)` (see tests/fixtures/library.db).
/// The prod schema additionally carries title, label, format, etc. We always
/// require `id`/`artist`/`title`; the rest are optional.
fn existing_columns(conn: &rusqlite::Connection, table: &str) -> anyhow::Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut cols = Vec::new();
    for r in rows {
        cols.push(r?);
    }
    Ok(cols)
}

const OPTIONAL_COLUMNS: &[&str] = &[
    "label",
    "genre",
    "call_letters",
    "release_call_number",
    "format",
];

fn read_library_db(library_db: &Path) -> anyhow::Result<Vec<LibraryRow>> {
    let conn = rusqlite::Connection::open(library_db)
        .with_context(|| format!("Failed to open {}", library_db.display()))?;

    let cols = existing_columns(&conn, "library")?;
    let has = |c: &str| cols.iter().any(|x| x == c);
    if !has("id") || !has("artist") || !has("title") {
        anyhow::bail!(
            "library.db at {} is missing required columns id/artist/title (have: {:?})",
            library_db.display(),
            cols
        );
    }

    // SELECT a fixed prefix plus whatever optional columns exist. We map the
    // optional positions back to fields below by name lookup.
    let mut select_parts: Vec<&str> = vec!["id", "artist", "title"];
    for &c in OPTIONAL_COLUMNS {
        if has(c) {
            select_parts.push(c);
        }
    }
    let query = format!("SELECT {} FROM library", select_parts.join(", "));

    let mut stmt = conn.prepare(&query)?;
    let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
    let rows_iter = stmt.query_map([], |row| {
        let mut id_opt: Option<i64> = None;
        let mut artist: Option<String> = None;
        let mut title: Option<String> = None;
        let mut label: Option<String> = None;
        let mut format_name: Option<String> = None;
        let mut genre: Option<String> = None;
        let mut call_letters: Option<String> = None;
        let mut release_call_number: Option<i64> = None;

        for (i, name) in column_names.iter().enumerate() {
            match name.as_str() {
                "id" => id_opt = row.get(i)?,
                "artist" => artist = row.get(i)?,
                "title" => title = row.get(i)?,
                "label" => label = row.get(i)?,
                "format" => format_name = row.get(i)?,
                "genre" => genre = row.get(i)?,
                "call_letters" => call_letters = row.get(i)?,
                "release_call_number" => release_call_number = row.get(i)?,
                _ => {}
            }
        }
        Ok(LibraryRow {
            library_id: id_opt
                .ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        "id is NULL".into(),
                    )
                })?
                .try_into()
                .map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        "library.id does not fit in i32".into(),
                    )
                })?,
            artist_name: artist.unwrap_or_default(),
            album_title: title.unwrap_or_default(),
            label_name: label,
            format_name,
            wxyc_genre: genre,
            call_letters,
            // Mirror the `id` overflow handling above: i32 is what the PG
            // schema expects; a SQLite value that doesn't fit must surface
            // as a hard error, not silently become NULL. Catalog
            // call_numbers are small in practice, but a malformed source
            // row should fail loud.
            call_numbers: release_call_number
                .map(|n| {
                    i32::try_from(n).map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Integer,
                            format!("library.release_call_number {n} does not fit in i32").into(),
                        )
                    })
                })
                .transpose()?,
        })
    })?;

    let mut out = Vec::new();
    for r in rows_iter {
        out.push(r?);
    }
    Ok(out)
}

/// Identity-tier normalization for the optional `label_name` column.
///
/// We want NULL to flow through to PostgreSQL for the nullable `norm_label`
/// column so downstream NULL-aware joins behave correctly. The `.filter`
/// collapses both the `None` input case AND a `Some("")` post-normalization
/// case (e.g. a `Some("   ")` whitespace-only label, or a Some("") empty
/// label that schema drift or a bulk-import artifact could produce) to a
/// single `None`.
fn norm_label(label: Option<&str>) -> Option<String> {
    label.map(to_identity_match_form).filter(|s| !s.is_empty())
}

/// Populate the consolidated `wxyc_library` hook from a SQLite library.db.
///
/// Per E1 §4.1.2 + §3.1: every library row is written. Idempotent on
/// `library_id` via `ON CONFLICT DO NOTHING`. `snapshot_at` is stamped
/// server-side via `now()` so all rows in a single load share a timestamp
/// without requiring a chrono dep on the Rust side.
///
/// Returns the number of rows attempted (pre-conflict). Idempotency is
/// observable via `SELECT COUNT(*) FROM wxyc_library` rather than the return
/// value.
///
/// # Errors
///
/// - Returns an error if `snapshot_source` is not one of `backend`,
///   `tubafrenzy`, `llm`. The cache-side CHECK constraint enforces the same
///   vocabulary; this client-side check fails fast before opening a write.
/// - Returns an error if `library_db` cannot be opened or its `library`
///   table is missing `id`/`artist`/`title`.
/// - Propagates PostgreSQL errors from the COPY-style write path.
pub fn populate_wxyc_library_v2(
    client: &mut postgres::Client,
    library_db: &Path,
    snapshot_source: &str,
) -> anyhow::Result<u64> {
    if !VALID_SNAPSHOT_SOURCES.contains(&snapshot_source) {
        anyhow::bail!(
            "snapshot_source must be one of {:?}, got {:?}",
            VALID_SNAPSHOT_SOURCES,
            snapshot_source
        );
    }

    let rows = read_library_db(library_db)?;
    if rows.is_empty() {
        log::warn!(
            "populate_wxyc_library_v2: no rows from {}",
            library_db.display()
        );
        return Ok(0);
    }

    // Validate every row BEFORE opening the COPY stream. Bailing
    // mid-stream would leave `writer.finish()` uncalled and the
    // connection's COPY state implicit-dropped — the outer transaction
    // still rolls back cleanly (TEMP table + ON COMMIT DROP), but the
    // pre-pass is cheap and keeps the COPY loop a tight write path.
    // Postgres `NOT NULL` on the staging table rejects SQL NULL but NOT
    // empty strings, so this is the only place an empty artist/title
    // gets caught before it lands with an empty norm_* column and
    // defeats downstream NULL-aware joins.
    for r in &rows {
        if r.artist_name.is_empty() || r.album_title.is_empty() {
            anyhow::bail!(
                "library_id {}: artist_name or album_title is empty (artist={:?}, title={:?}). \
                 library.db rows must have non-empty artist/title; fix the source row \
                 before re-running the loader.",
                r.library_id,
                r.artist_name,
                r.album_title,
            );
        }
    }
    let attempted = rows.len() as u64;

    // Stage rows into a TEMP table via COPY, then INSERT ... ON CONFLICT
    // from the temp into wxyc_library. This pattern lets us:
    //   1. Use the fast COPY path for the bulk write.
    //   2. Stamp `snapshot_at = now()` server-side (no chrono dep).
    //   3. Apply ON CONFLICT (library_id) DO NOTHING on the merge so the
    //      load is idempotent without per-row roundtrips.
    let mut tx = client.transaction().context("begin tx")?;
    tx.batch_execute(
        "CREATE TEMP TABLE _wxyc_library_stage (
            library_id   integer PRIMARY KEY,
            artist_id    integer,
            artist_name  text NOT NULL,
            album_title  text NOT NULL,
            label_id     integer,
            label_name   text,
            format_id    integer,
            format_name  text,
            wxyc_genre   text,
            call_letters text,
            call_numbers integer,
            release_year smallint,
            norm_artist  text NOT NULL,
            norm_title   text NOT NULL,
            norm_label   text
        ) ON COMMIT DROP",
    )?;

    {
        let mut writer = tx.copy_in(
            "COPY _wxyc_library_stage (\
                library_id, artist_id, artist_name, album_title, \
                label_id, label_name, format_id, format_name, \
                wxyc_genre, call_letters, call_numbers, release_year, \
                norm_artist, norm_title, norm_label\
             ) FROM STDIN WITH (FORMAT text, NULL '\\N')",
        )?;
        for r in &rows {
            // Empty-string validation runs in the pre-pass above; reaching
            // this loop means every row has non-empty artist/title and is
            // safe to write. Keep this loop a tight COPY-write path with
            // no early bails — finishing the writer cleanly is cheaper
            // than relying on transaction rollback.
            let norm_artist = to_identity_match_form(&r.artist_name);
            let norm_title = to_identity_match_form_title(&r.album_title);
            let norm_label_v = norm_label(r.label_name.as_deref());
            writeln!(
                writer,
                "{lib_id}\t\\N\t{artist}\t{title}\t\\N\t{label}\t\\N\t{fmt}\t{genre}\t{call_letters}\t{call_numbers}\t\\N\t{n_artist}\t{n_title}\t{n_label}",
                lib_id = r.library_id,
                artist = tsv_escape(&to_pg_text_form(&r.artist_name)),
                title = tsv_escape(&to_pg_text_form(&r.album_title)),
                label = tsv_escape_opt(
                    r.label_name.as_deref().map(to_pg_text_form).as_deref(),
                ),
                fmt = tsv_escape_opt(
                    r.format_name.as_deref().map(to_pg_text_form).as_deref(),
                ),
                genre = tsv_escape_opt(
                    r.wxyc_genre.as_deref().map(to_pg_text_form).as_deref(),
                ),
                call_letters = tsv_escape_opt(
                    r.call_letters.as_deref().map(to_pg_text_form).as_deref(),
                ),
                call_numbers = r
                    .call_numbers
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "\\N".to_string()),
                n_artist = tsv_escape(&to_pg_text_form(&norm_artist)),
                n_title = tsv_escape(&to_pg_text_form(&norm_title)),
                n_label = tsv_escape_opt(norm_label_v.as_deref()),
            )?;
        }
        writer.finish()?;
    }

    let merge_sql = "INSERT INTO wxyc_library (\
            library_id, artist_id, artist_name, album_title, \
            label_id, label_name, format_id, format_name, \
            wxyc_genre, call_letters, call_numbers, release_year, \
            norm_artist, norm_title, norm_label, \
            snapshot_at, snapshot_source\
         ) \
         SELECT library_id, artist_id, artist_name, album_title, \
                label_id, label_name, format_id, format_name, \
                wxyc_genre, call_letters, call_numbers, release_year, \
                norm_artist, norm_title, norm_label, \
                now(), $1 \
         FROM _wxyc_library_stage \
         ON CONFLICT (library_id) DO NOTHING";
    tx.execute(merge_sql, &[&snapshot_source])?;
    tx.commit()?;

    log::info!(
        "populate_wxyc_library_v2: wrote {attempted} rows (snapshot_source={snapshot_source}, normalizer={NORMALIZER_NAME})"
    );
    Ok(attempted)
}

/// Escape a string for PG COPY FORMAT text.
///
/// PG COPY's text format reserves backslash, tab, newline, and carriage
/// return. We escape those; everything else passes through as-is. NUL has
/// already been stripped at the [`to_pg_text_form`] boundary above.
fn tsv_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}

fn tsv_escape_opt(s: Option<&str>) -> String {
    match s {
        None => "\\N".to_string(),
        Some(v) => tsv_escape(v),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizer_name_audit_constant_is_pinned() {
        // This pin protects the cross-cache audit trail: discogs-etl's
        // loaders/wxyc.py emits the same string in INFO logs. A rename in
        // wxyc_etl::text would otherwise drift silently.
        assert_eq!(NORMALIZER_NAME, "wxyc_etl::text::to_identity_match_form");
    }

    #[test]
    fn snapshot_source_vocabulary_matches_check_constraint() {
        // Mirrors §3.1's CHECK (snapshot_source IN ('backend','tubafrenzy','llm')).
        // If the vocabulary ever expands, both this constant AND the schema
        // CHECK must change in lock-step.
        assert_eq!(VALID_SNAPSHOT_SOURCES, &["backend", "tubafrenzy", "llm"]);
    }

    #[test]
    fn tsv_escape_handles_reserved_characters() {
        assert_eq!(tsv_escape("plain"), "plain");
        assert_eq!(tsv_escape("with\ttab"), "with\\ttab");
        assert_eq!(tsv_escape("with\nlf"), "with\\nlf");
        assert_eq!(tsv_escape("back\\slash"), "back\\\\slash");
    }

    #[test]
    fn norm_label_returns_none_for_none() {
        assert!(norm_label(None).is_none());
    }

    #[test]
    fn norm_label_normalizes_some() {
        // "Sonamos" should fold to lowercase via the locked-on baseline.
        assert_eq!(norm_label(Some("Sonamos")).as_deref(), Some("sonamos"));
    }

    #[test]
    fn norm_label_drops_empty_string() {
        // `Some("")` and `Some("   ")` (whitespace that the normalizer
        // collapses to "") must come back as `None` so downstream
        // NULL-aware lookups on norm_label behave correctly.
        assert_eq!(norm_label(Some("")), None);
        assert_eq!(norm_label(Some("   ")), None);
    }
}
