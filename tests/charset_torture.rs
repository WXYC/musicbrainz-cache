//! WX-1.2.9 detector for the TSV → PG COPY byte path.
//!
//! Drives the cross-repo `@wxyc/shared` charset-torture corpus through
//! `import::import_table` against a temp `charset_probe` table, then
//! SELECTs back and asserts byte equality. Uses the wxyc-etl#79
//! EXPECTED_FAILURES + unexpected-pass detector pattern.
//!
//! See WXYC/docs#15 for the WX-1 plan.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use musicbrainz_cache::import::{import_table, TableSpec};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct CorpusEntry {
    input: String,
    notes: String,
}

#[derive(Deserialize)]
struct Corpus {
    categories: HashMap<String, Vec<CorpusEntry>>,
}

fn load_corpus() -> Corpus {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/charset-torture.json");
    let bytes = std::fs::read(&path).expect("vendored corpus exists");
    serde_json::from_slice(&bytes).expect("corpus is valid JSON")
}

/// Inputs whose TSV→PG round-trip cannot succeed today.
fn expected_failures() -> HashMap<&'static str, &'static str> {
    let mut m = HashMap::new();
    // Literal tab byte conflicts with TSV's field separator. Not a
    // musicbrainz-cache bug — MusicBrainz's own dumps cannot carry literal
    // tabs in TEXT columns either.
    m.insert(
        "tab\there",
        "[mbc:tsv-tab-byte] literal tab is the TSV field separator",
    );
    // PG COPY ... WITH (FORMAT text) treats backslash as an escape character.
    // The MusicBrainz dumps escape backslashes upstream; if a future MB schema
    // ships unescaped TSV, this test will start failing as an unexpected pass.
    m.insert(
        "back\\slash",
        "[mbc:pg-copy-backslash] PG COPY FORMAT text interprets backslash as escape",
    );
    m
}

fn db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz".into()
    })
}

const TSV_FILENAME: &str = "charset_torture";
const PROBE_TABLE: &str = "charset_probe";

#[test]
#[ignore] // Requires PostgreSQL: cargo test -- --ignored
fn corpus_tsv_pg_roundtrip() {
    let corpus = load_corpus();
    let known_failures = expected_failures();

    // Flatten and assign sequential ids so we can SELECT them back deterministically.
    let entries: Vec<(usize, &str, &str, &str)> = corpus
        .categories
        .iter()
        .flat_map(|(category, entries)| {
            entries
                .iter()
                .map(move |e| (category.as_str(), e.input.as_str(), e.notes.as_str()))
        })
        .enumerate()
        .map(|(i, (cat, input, notes))| (i + 1, cat, input, notes))
        .collect();

    let tmp = tempfile::tempdir().expect("tempdir");
    let tsv_path = tmp.path().join(TSV_FILENAME);

    // Build a TSV. Skip entries that would break the TSV format itself
    // (literal tab, newline) so the file is well-formed; those entries are
    // checked against EXPECTED_FAILURES separately. NUL bytes are kept in the
    // TSV — `import_table` strips U+0000 at the PG TEXT write boundary per
    // WXYC/docs#18, so the round-trip read returns the stripped form.
    let mut tsv = String::new();
    let mut written: Vec<(usize, &str)> = Vec::new();
    for (id, _, input, _) in &entries {
        if input.contains('\t') || input.contains('\n') {
            continue;
        }
        tsv.push_str(&id.to_string());
        tsv.push('\t');
        tsv.push_str(input);
        tsv.push('\n');
        written.push((*id, *input));
    }
    fs::write(&tsv_path, tsv).expect("write TSV");

    let mut client = postgres::Client::connect(&db_url(), postgres::NoTls).expect("connect");
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table}; CREATE TEMP TABLE {table} (id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
            table = PROBE_TABLE
        ))
        .expect("create probe table");

    let spec = TableSpec {
        dump_file: TSV_FILENAME,
        table: PROBE_TABLE,
        source_indices: &[0, 1],
        db_columns: &["id", "name"],
    };
    let imported = import_table(&mut client, &spec, tmp.path()).expect("import_table");
    assert_eq!(
        imported as usize,
        written.len(),
        "import row count mismatch"
    );

    let mut unexpected_failures: Vec<String> = Vec::new();
    let mut unexpected_passes: Vec<String> = Vec::new();

    for (id, category, input, notes) in &entries {
        let known = known_failures.get(input).copied();
        let select_result = client.query_opt(
            &format!("SELECT name FROM {} WHERE id = $1", PROBE_TABLE),
            &[&(*id as i32)],
        );

        let actual: Option<String> = match select_result {
            Ok(Some(row)) => Some(row.get(0)),
            Ok(None) => None,
            Err(e) => {
                if known.is_none() {
                    unexpected_failures.push(format!(
                        "{category}: {input:?} -> SELECT error: {e}\n    notes: {notes}"
                    ));
                }
                continue;
            }
        };

        // Strip-at-boundary policy (WXYC/docs#18): U+0000 is stripped at the
        // PG TEXT write boundary in import_table, so the round-trip read
        // comes back as the NUL-stripped form.
        let expected: String = input.replace('\0', "");
        let passed = actual.as_deref() == Some(expected.as_str());
        match (passed, known) {
            (true, None) => {}
            (true, Some(_tag)) => {
                unexpected_passes.push(format!(
                    "{category}: {input:?} now round-trips; remove from EXPECTED_FAILURES"
                ));
            }
            (false, Some(_tag)) => {}
            (false, None) => {
                unexpected_failures.push(format!(
                    "{category}: {input:?} -> {actual:?}\n    notes: {notes}"
                ));
            }
        }
    }

    let mut report = String::new();
    if !unexpected_failures.is_empty() {
        report.push_str(&format!(
            "\nUnexpected failures ({}):\n  {}\n",
            unexpected_failures.len(),
            unexpected_failures.join("\n  ")
        ));
    }
    if !unexpected_passes.is_empty() {
        report.push_str(&format!(
            "\nUnexpected passes ({}):\n  {}\n",
            unexpected_passes.len(),
            unexpected_passes.join("\n  ")
        ));
    }
    assert!(report.is_empty(), "{report}");
}
