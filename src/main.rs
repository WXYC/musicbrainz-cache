use anyhow::{bail, Context};
use clap::Parser;
use musicbrainz_cache::state::{PipelineState, Step};
use musicbrainz_cache::{download, filter, import, schema};
use std::path::{Path, PathBuf};

/// MusicBrainz cache pipeline for WXYC.
///
/// Downloads MusicBrainz data dumps, imports into PostgreSQL, filters to WXYC
/// library artists, and builds indexes for querying.
#[derive(Parser)]
#[command(name = "musicbrainz-cache")]
struct Cli {
    /// Directory for downloads and extracted files.
    #[arg(long)]
    data_dir: PathBuf,

    /// Path to library.db (required unless --no-filter is set).
    #[arg(long)]
    library_db: Option<PathBuf>,

    /// PostgreSQL connection URL.
    #[arg(
        long,
        default_value_t = default_database_url(),
    )]
    database_url: String,

    /// Skip download, use existing files.
    #[arg(long)]
    skip_download: bool,

    /// Import all artists without filtering.
    #[arg(long)]
    no_filter: bool,

    /// Override dump URL (default: auto-detect latest).
    #[arg(long)]
    dump_url: Option<String>,

    /// Resume an interrupted run by reading the state file and skipping
    /// already-completed steps. If the state file does not exist, a fresh
    /// run is performed (with a warning).
    #[arg(long)]
    resume: bool,

    /// Path to the pipeline state file. Created during the run; consulted
    /// on subsequent invocations when --resume is passed.
    #[arg(long, default_value = "./state")]
    state_file: PathBuf,
}

fn default_database_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz".into()
    })
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

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let cli = Cli::parse();
    let pipeline_start = std::time::Instant::now();

    if !cli.no_filter && cli.library_db.is_none() {
        bail!("--library-db required unless --no-filter is set");
    }

    let mut state = init_state(&cli.state_file, cli.resume)?;

    let db_display = if let Some(idx) = cli.database_url.find('@') {
        &cli.database_url[idx + 1..]
    } else {
        &cli.database_url
    };
    log::info!("MusicBrainz cache pipeline starting");
    log::info!("  Data dir: {}", cli.data_dir.display());
    log::info!("  Database: {}", db_display);
    log::info!("  State file: {}", cli.state_file.display());
    log::info!(
        "  Filter: {}",
        if cli.no_filter {
            "disabled".to_string()
        } else {
            cli.library_db
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        }
    );

    // Download is intentionally not part of `PipelineState` -- it has its own
    // `--skip-download` flag and is naturally idempotent (existing archives
    // are reused). The resumable steps are the database-mutating ones below.
    if !cli.skip_download {
        let dump_url = match &cli.dump_url {
            Some(url) => url.clone(),
            None => download::find_latest_dump_url()?,
        };

        std::fs::create_dir_all(&cli.data_dir)?;

        // Download archives sequentially (network-bound)
        for &(archive_name, _) in download::ARCHIVES {
            let archive_path = cli.data_dir.join(archive_name);
            download::download_file(&format!("{dump_url}/{archive_name}"), &archive_path)?;
        }

        // Extract archives (could be parallelized, but kept simple)
        let mbdump_dir = cli.data_dir.join("mbdump");
        for &(archive_name, needed_files) in download::ARCHIVES {
            let archive_path = cli.data_dir.join(archive_name);
            if archive_path.exists() {
                download::extract_tables(&archive_path, needed_files, &mbdump_dir)?;
            } else {
                log::error!("Archive not found: {}", archive_path.display());
            }
        }
    }

    let mbdump_dir = cli.data_dir.join("mbdump");
    if !mbdump_dir.exists() {
        bail!("mbdump directory not found: {}", mbdump_dir.display());
    }

    // Wait for PostgreSQL and establish a single client used by every
    // resumable step.
    wait_for_postgres(&cli.database_url, 30)?;
    let mut client = postgres::Client::connect(&cli.database_url, postgres::NoTls)
        .context("Failed to connect to PostgreSQL")?;

    run_step(
        &mut state,
        &cli.state_file,
        Step::Schema,
        "Apply schema",
        || schema::apply_schema(&mut client),
    )?;

    run_step(
        &mut state,
        &cli.state_file,
        Step::Import,
        "Import TSV files",
        || import::import_all(&mut client, &mbdump_dir).map(|_| ()),
    )?;

    if !cli.no_filter {
        let library_db = cli.library_db.as_ref().unwrap().clone();
        run_step(
            &mut state,
            &cli.state_file,
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
        state.save(&cli.state_file).with_context(|| {
            format!("Failed to persist state file: {}", cli.state_file.display())
        })?;
        log::info!("Skipping Filter (--no-filter set)");
    }

    run_step(
        &mut state,
        &cli.state_file,
        Step::Indexes,
        "Create indexes",
        || schema::create_indexes(&mut client),
    )?;

    run_step(
        &mut state,
        &cli.state_file,
        Step::Analyze,
        "Analyze",
        || schema::analyze_tables(&mut client),
    )?;

    let elapsed = pipeline_start.elapsed();
    log::info!("Pipeline complete in {:.1}s", elapsed.as_secs_f64());
    Ok(())
}
