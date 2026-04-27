use anyhow::{bail, Context};
use clap::{Args, Parser, Subcommand};
use musicbrainz_cache::state::{PipelineState, Step};
use musicbrainz_cache::{download, filter, import, schema};
use std::path::{Path, PathBuf};
use wxyc_etl::cli::{resolve_database_url, DatabaseArgs, ImportArgs, ResumableBuildArgs};

const DATABASE_ENV_NAME: &str = "DATABASE_URL_MUSICBRAINZ";

/// MusicBrainz cache pipeline for WXYC.
///
/// Downloads MusicBrainz data dumps, imports into PostgreSQL, filters to WXYC
/// library artists, and builds indexes for querying.
#[derive(Parser)]
#[command(name = "musicbrainz-cache")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build the WXYC-filtered cache (download → schema → import → filter → indexes → analyze).
    Build(BuildCmd),
    /// Load a fresh MusicBrainz dump into PostgreSQL (download + schema + import).
    Import(ImportCmd),
}

#[derive(Args)]
struct BuildCmd {
    #[command(flatten)]
    db: DatabaseArgs,

    #[command(flatten)]
    build: ResumableBuildArgs,

    /// Path to library.db (required unless --no-filter is set).
    #[arg(long)]
    library_db: Option<PathBuf>,

    /// Skip download, use existing files in --data-dir.
    #[arg(long)]
    skip_download: bool,

    /// Import all artists without filtering to the WXYC library.
    #[arg(long)]
    no_filter: bool,

    /// Override dump URL (default: auto-detect latest).
    #[arg(long)]
    dump_url: Option<String>,
}

#[derive(Args)]
struct ImportCmd {
    #[command(flatten)]
    db: DatabaseArgs,

    #[command(flatten)]
    import: ImportArgs,

    /// Skip download, use existing files in --data-dir.
    #[arg(long)]
    skip_download: bool,

    /// Override dump URL (default: auto-detect latest).
    #[arg(long)]
    dump_url: Option<String>,
}

fn wait_for_postgres(db_url: &str, timeout_secs: u64) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        match postgres::Client::connect(db_url, postgres::NoTls) {
            Ok(_) => return Ok(()),
            Err(_) if start.elapsed().as_secs() < timeout_secs => {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            Err(e) => bail!("PostgreSQL not available after {}s: {}", timeout_secs, e),
        }
    }
}

/// Initialize pipeline state from CLI flags.
///
/// - `--resume` with an existing state file: load it.
/// - `--resume` with no state file: warn and start fresh.
/// - No `--resume` with an existing state file: bail (refuse to clobber a
///   prior run's state without an explicit opt-in).
/// - No `--resume` with no state file: start fresh.
fn init_state(state_path: &Path, resume: bool) -> anyhow::Result<PipelineState> {
    let exists = state_path.exists();
    match (resume, exists) {
        (true, true) => {
            let state = PipelineState::load(state_path)
                .with_context(|| format!("Failed to load state file: {}", state_path.display()))?;
            log::info!("Resuming from state file: {}", state_path.display());
            Ok(state)
        }
        (true, false) => {
            log::warn!(
                "--resume passed but state file {} does not exist; starting fresh",
                state_path.display()
            );
            Ok(PipelineState::new())
        }
        (false, true) => {
            bail!(
                "State file {} already exists from a prior run. Pass --resume to continue, \
                 or remove the file to start fresh.",
                state_path.display()
            );
        }
        (false, false) => Ok(PipelineState::new()),
    }
}

/// Run a step if it isn't already marked complete; persist state on success.
fn run_step<F>(
    state: &mut PipelineState,
    state_path: &Path,
    step: Step,
    label: &str,
    f: F,
) -> anyhow::Result<()>
where
    F: FnOnce() -> anyhow::Result<()>,
{
    if state.is_complete(step) {
        log::info!("Skipping {} (already complete)", label);
        return Ok(());
    }
    log::info!("=== {} ===", label);
    f()?;
    state.mark_complete(step);
    state
        .save(state_path)
        .with_context(|| format!("Failed to persist state file: {}", state_path.display()))?;
    Ok(())
}

fn db_display(db_url: &str) -> &str {
    if let Some(idx) = db_url.find('@') {
        &db_url[idx + 1..]
    } else {
        db_url
    }
}

fn download_dumps(data_dir: &Path, dump_url: Option<&str>) -> anyhow::Result<()> {
    let dump_url = match dump_url {
        Some(url) => url.to_string(),
        None => download::find_latest_dump_url()?,
    };

    std::fs::create_dir_all(data_dir)?;

    for &(archive_name, _) in download::ARCHIVES {
        let archive_path = data_dir.join(archive_name);
        download::download_file(&format!("{dump_url}/{archive_name}"), &archive_path)?;
    }

    let mbdump_dir = data_dir.join("mbdump");
    for &(archive_name, needed_files) in download::ARCHIVES {
        let archive_path = data_dir.join(archive_name);
        if archive_path.exists() {
            download::extract_tables(&archive_path, needed_files, &mbdump_dir)?;
        } else {
            log::error!("Archive not found: {}", archive_path.display());
        }
    }
    Ok(())
}

fn run_build(cmd: BuildCmd) -> anyhow::Result<()> {
    let pipeline_start = std::time::Instant::now();

    if !cmd.no_filter && cmd.library_db.is_none() {
        bail!("--library-db required unless --no-filter is set");
    }

    let database_url = resolve_database_url(&cmd.db, DATABASE_ENV_NAME)?;
    let state_path = cmd.build.state_file.clone();
    let data_dir = cmd.build.data_dir.clone();

    let mut state = init_state(&state_path, cmd.build.resume)?;

    log::info!("MusicBrainz cache pipeline starting");
    log::info!("  Data dir: {}", data_dir.display());
    log::info!("  Database: {}", db_display(&database_url));
    log::info!("  State file: {}", state_path.display());
    log::info!(
        "  Filter: {}",
        if cmd.no_filter {
            "disabled".to_string()
        } else {
            cmd.library_db
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        }
    );

    // Download is intentionally not part of `PipelineState` -- it has its own
    // `--skip-download` flag and is naturally idempotent (existing archives
    // are reused). The resumable steps are the database-mutating ones below.
    if !cmd.skip_download {
        download_dumps(&data_dir, cmd.dump_url.as_deref())?;
    }

    let mbdump_dir = data_dir.join("mbdump");
    if !mbdump_dir.exists() {
        bail!("mbdump directory not found: {}", mbdump_dir.display());
    }

    wait_for_postgres(&database_url, 30)?;
    let mut client = postgres::Client::connect(&database_url, postgres::NoTls)
        .context("Failed to connect to PostgreSQL")?;

    run_step(
        &mut state,
        &state_path,
        Step::Schema,
        "Apply schema",
        || schema::apply_schema(&mut client),
    )?;

    run_step(
        &mut state,
        &state_path,
        Step::Import,
        "Import TSV files",
        || import::import_all(&mut client, &mbdump_dir).map(|_| ()),
    )?;

    if !cmd.no_filter {
        let library_db = cmd.library_db.as_ref().unwrap().clone();
        run_step(
            &mut state,
            &state_path,
            Step::Filter,
            "Filter to WXYC library artists",
            || {
                let library_artists = filter::load_library_artists(&library_db)?;
                let matching = filter::find_matching_artist_ids(&mut client, &library_artists)?;
                filter::prune_to_matching(&mut client, &matching)?;
                filter::report_sizes(&mut client)?;
                Ok(())
            },
        )?;
    } else if !state.is_complete(Step::Filter) {
        // With --no-filter we never run the Filter step, but for resume
        // accounting we still mark it complete so subsequent steps advance.
        state.mark_complete(Step::Filter);
        state
            .save(&state_path)
            .with_context(|| format!("Failed to persist state file: {}", state_path.display()))?;
        log::info!("Skipping Filter (--no-filter set)");
    }

    run_step(
        &mut state,
        &state_path,
        Step::Indexes,
        "Create indexes",
        || schema::create_indexes(&mut client),
    )?;

    run_step(&mut state, &state_path, Step::Analyze, "Analyze", || {
        schema::analyze_tables(&mut client)
    })?;

    let elapsed = pipeline_start.elapsed();
    log::info!("Pipeline complete in {:.1}s", elapsed.as_secs_f64());
    Ok(())
}

fn run_import(cmd: ImportCmd) -> anyhow::Result<()> {
    let database_url = resolve_database_url(&cmd.db, DATABASE_ENV_NAME)?;
    let data_dir = cmd.import.data_dir.clone();

    log::info!("MusicBrainz import starting");
    log::info!("  Data dir: {}", data_dir.display());
    log::info!("  Database: {}", db_display(&database_url));
    log::info!("  Fresh: {}", cmd.import.fresh);

    if !cmd.skip_download {
        download_dumps(&data_dir, cmd.dump_url.as_deref())?;
    }

    let mbdump_dir = data_dir.join("mbdump");
    if !mbdump_dir.exists() {
        bail!("mbdump directory not found: {}", mbdump_dir.display());
    }

    wait_for_postgres(&database_url, 30)?;
    let mut client = postgres::Client::connect(&database_url, postgres::NoTls)
        .context("Failed to connect to PostgreSQL")?;

    if cmd.import.fresh {
        log::info!("=== Drop existing tables (--fresh) ===");
        schema::drop_all_tables(&mut client)?;
    }

    log::info!("=== Apply schema ===");
    schema::apply_schema(&mut client)?;

    log::info!("=== Import TSV files ===");
    import::import_all(&mut client, &mbdump_dir)?;

    log::info!("Import complete.");
    Ok(())
}

/// Pre-process argv to support the legacy invocation shape (no subcommand).
///
/// Before issue #24 the binary accepted top-level flags like
/// `musicbrainz-cache --data-dir X --library-db Y`. The current convention
/// requires a `build` or `import` subcommand. To avoid breaking existing
/// scripts and CI, we detect the legacy form (first arg is a flag) and
/// rewrite it to `musicbrainz-cache build <flags>...` with a stderr
/// deprecation warning.
fn apply_legacy_shim(mut args: Vec<String>) -> Vec<String> {
    if args.len() < 2 {
        return args;
    }
    let first = &args[1];
    // Pass clap's own --help/--version through unchanged.
    if matches!(first.as_str(), "--help" | "-h" | "--version" | "-V") {
        return args;
    }
    if first.starts_with('-') {
        eprintln!(
            "warning: invoking `musicbrainz-cache` without a subcommand is deprecated; \
             use `musicbrainz-cache build [...]` instead."
        );
        args.insert(1, "build".to_string());
    }
    args
}

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = apply_legacy_shim(std::env::args().collect());
    let cli = Cli::parse_from(args);

    match cli.command {
        Command::Build(cmd) => run_build(cmd),
        Command::Import(cmd) => run_import(cmd),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_shim_inserts_build_when_first_arg_is_flag() {
        let args = vec![
            "musicbrainz-cache".into(),
            "--data-dir".into(),
            "data/".into(),
        ];
        let shimmed = apply_legacy_shim(args);
        assert_eq!(
            shimmed,
            vec![
                "musicbrainz-cache".to_string(),
                "build".into(),
                "--data-dir".into(),
                "data/".into()
            ]
        );
    }

    #[test]
    fn legacy_shim_passes_through_subcommand() {
        let args = vec![
            "musicbrainz-cache".into(),
            "build".into(),
            "--resume".into(),
        ];
        let shimmed = apply_legacy_shim(args.clone());
        assert_eq!(shimmed, args);
    }

    #[test]
    fn legacy_shim_passes_through_import_subcommand() {
        let args = vec![
            "musicbrainz-cache".into(),
            "import".into(),
            "--fresh".into(),
        ];
        let shimmed = apply_legacy_shim(args.clone());
        assert_eq!(shimmed, args);
    }

    #[test]
    fn legacy_shim_preserves_help_and_version() {
        for flag in ["--help", "-h", "--version", "-V"] {
            let args = vec!["musicbrainz-cache".into(), flag.into()];
            let shimmed = apply_legacy_shim(args.clone());
            assert_eq!(shimmed, args);
        }
    }

    #[test]
    fn legacy_shim_no_args() {
        let args = vec!["musicbrainz-cache".into()];
        let shimmed = apply_legacy_shim(args.clone());
        assert_eq!(shimmed, args);
    }

    #[test]
    fn build_subcommand_parses_shared_flags() {
        let cli = Cli::try_parse_from([
            "musicbrainz-cache",
            "build",
            "--database-url",
            "postgresql://example/db",
            "--data-dir",
            "/tmp/data",
            "--state-file",
            "/tmp/state.json",
            "--library-db",
            "/tmp/library.db",
            "--resume",
        ])
        .unwrap();
        let Command::Build(b) = cli.command else {
            panic!("expected Build subcommand");
        };
        assert_eq!(
            b.db.database_url.as_deref(),
            Some("postgresql://example/db")
        );
        assert_eq!(b.build.data_dir, PathBuf::from("/tmp/data"));
        assert_eq!(b.build.state_file, PathBuf::from("/tmp/state.json"));
        assert_eq!(b.library_db, Some(PathBuf::from("/tmp/library.db")));
        assert!(b.build.resume);
    }

    #[test]
    fn import_subcommand_parses_fresh_flag() {
        let cli = Cli::try_parse_from([
            "musicbrainz-cache",
            "import",
            "--fresh",
            "--data-dir",
            "/tmp/data",
        ])
        .unwrap();
        let Command::Import(i) = cli.command else {
            panic!("expected Import subcommand");
        };
        assert!(i.import.fresh);
        assert_eq!(i.import.data_dir, PathBuf::from("/tmp/data"));
    }

    #[test]
    fn database_url_falls_back_to_env_var() {
        // Smoke test that the binary's env name is wired up correctly.
        let args = DatabaseArgs { database_url: None };
        std::env::set_var(DATABASE_ENV_NAME, "postgresql://envhost/db");
        let url = resolve_database_url(&args, DATABASE_ENV_NAME).unwrap();
        std::env::remove_var(DATABASE_ENV_NAME);
        assert_eq!(url, "postgresql://envhost/db");
    }
}
