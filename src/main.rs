use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use reviewloop::config::Config;
use reviewloop::db::Db;
use reviewloop::model::{JobStatus, NewJob};
use reviewloop::util::{compute_next_poll_at, sha256_file};
use std::{
    fs,
    io::IsTerminal,
    path::{Path, PathBuf},
};
use tracing::warn;

#[derive(Debug, Parser)]
#[command(name = "reviewloop")]
#[command(about = "Automate paperreview.ai submission and retrieval workflows")]
struct Cli {
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        force: bool,
    },
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    Submit {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        force: bool,
    },
    Approve {
        #[arg(long)]
        job_id: String,
    },
    ImportToken {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        token: String,
        #[arg(long, default_value = "manual")]
        source: String,
    },
    Status {
        #[arg(long)]
        paper_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Retry {
        #[arg(long)]
        job_id: String,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Run {
        #[arg(long, default_value_t = true)]
        panel: bool,
    },
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let Cli { config, command } = Cli::parse();
    Config::ensure_global_config_dir()?;
    Config::ensure_global_data_dir()?;

    match command {
        Command::Init { force } => {
            let init_path = config
                .clone()
                .unwrap_or_else(|| PathBuf::from("reviewloop.toml"));
            cmd_init(&init_path, force)
        }
        Command::Daemon {
            command: DaemonCommand::Run { panel },
        } => {
            let panel_enabled = panel && std::io::stdout().is_terminal();
            if panel && !panel_enabled {
                eprintln!("note: panel requested but stdout is not a TTY; running without panel.");
            }
            let (config, db) = load_runtime(config.as_deref(), panel_enabled)?;
            reviewloop::worker::run_daemon(&config, &db, panel_enabled).await
        }
        Command::Submit { paper_id, force } => {
            let (config, db) = load_runtime(config.as_deref(), false)?;
            cmd_submit(&config, &db, &paper_id, force).await
        }
        Command::Approve { job_id } => {
            let (_config, db) = load_runtime(config.as_deref(), false)?;
            cmd_approve(&db, &job_id)
        }
        Command::ImportToken {
            paper_id,
            token,
            source,
        } => {
            let (config, db) = load_runtime(config.as_deref(), false)?;
            cmd_import_token(&config, &db, &paper_id, &token, &source)
        }
        Command::Status { paper_id, json } => {
            let (_config, db) = load_runtime(config.as_deref(), false)?;
            cmd_status(&db, paper_id.as_deref(), json)
        }
        Command::Retry { job_id } => {
            let (config, db) = load_runtime(config.as_deref(), false)?;
            cmd_retry(&config, &db, &job_id)
        }
    }
}

fn cmd_init(config_path: &Path, force: bool) -> Result<()> {
    Config::ensure_global_config_dir()?;
    Config::ensure_global_data_dir()?;

    if let Some(parent) = config_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create config parent directory: {}",
                parent.display()
            )
        })?;
    }

    if config_path.exists() && !force {
        anyhow::bail!(
            "config file already exists: {} (use --force to overwrite)",
            config_path.display()
        );
    }

    Config::save_template(config_path)?;

    let cfg = Config::default();
    ensure_runtime_dirs(&cfg)?;
    let db = Db::from_config(&cfg)?;
    db.init_schema()?;

    let db_label = cfg
        .db_path()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| ":memory:".to_string());
    println!(
        "Initialized ReviewLoop.\n- config: {}\n- state dir: {}\n- db: {}",
        config_path.display(),
        cfg.state_dir().display(),
        db_label
    );
    println!("\n{}", render_guardrail_notice(&cfg));
    Ok(())
}

fn load_runtime(config_override: Option<&Path>, force_stderr_logs: bool) -> Result<(Config, Db)> {
    let loaded = Config::load_layered_with_metadata(config_override)?;
    let layer_chain = loaded
        .layers
        .iter()
        .map(|p| p.display().to_string())
        .collect::<Vec<_>>()
        .join(" -> ");
    let config = loaded.config;

    reviewloop::logging::init_logging(&config, force_stderr_logs)?;
    tracing::info!(layers = %layer_chain, "loaded configuration layers");
    print_guardrail_warnings(&config);

    ensure_runtime_dirs(&config)?;
    let db = Db::from_config(&config)?;
    db.init_schema()?;

    Ok((config, db))
}

fn ensure_runtime_dirs(config: &Config) -> Result<()> {
    fs::create_dir_all(config.state_dir()).with_context(|| {
        format!(
            "failed to create state dir: {}",
            config.state_dir().display()
        )
    })?;
    fs::create_dir_all(config.state_dir().join("artifacts"))?;

    if let Some(db_path) = config.db_path()
        && let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create db parent directory: {}", parent.display())
        })?;
    }

    Ok(())
}

fn render_guardrail_notice(config: &Config) -> String {
    format!(
        "Site Load Guardrails (defaults):\n\
         - core.max_submissions_per_tick = {} (recommended: 1)\n\
         - core.max_concurrency = {} (recommended: <=2)\n\
         - trigger.pdf.max_scan_papers = {} (recommended: <=10)\n\
         - polling.schedule_minutes = {:?} (starts at 10m)\n\
         These limits help avoid overloading review providers.",
        config.core.max_submissions_per_tick,
        config.core.max_concurrency,
        config.trigger.pdf.max_scan_papers,
        config.polling.schedule_minutes
    )
}

fn print_guardrail_warnings(config: &Config) {
    if config.core.max_submissions_per_tick > 2 {
        warn!(
            "warning: core.max_submissions_per_tick={} is high; consider <=2 to reduce site load.",
            config.core.max_submissions_per_tick
        );
    }
    if config.core.max_concurrency > 3 {
        warn!(
            "warning: core.max_concurrency={} is high; consider <=3 unless provider confirms higher limits.",
            config.core.max_concurrency
        );
    }
    if config.trigger.pdf.max_scan_papers > 50 {
        warn!(
            "warning: trigger.pdf.max_scan_papers={} is high; large scans may generate excessive submit candidates.",
            config.trigger.pdf.max_scan_papers
        );
    }
}

async fn cmd_submit(config: &Config, db: &Db, paper_id: &str, force: bool) -> Result<()> {
    let paper = config
        .find_paper(paper_id)
        .with_context(|| format!("paper_id not found in config: {paper_id}"))?;

    let pdf_path = Path::new(&paper.pdf_path);
    if !pdf_path.exists() {
        anyhow::bail!("pdf file not found: {}", pdf_path.display());
    }

    let pdf_hash = sha256_file(pdf_path)?;
    if !force && db.has_duplicate_guard(&paper.backend, &pdf_hash)? {
        println!(
            "Skipped submit: existing active/completed job already covers backend={} hash={}",
            paper.backend, pdf_hash
        );
        return Ok(());
    }

    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            config.providers.stanford.email.clone(),
            config.providers.stanford.venue.clone(),
        ),
        _ => (String::new(), None),
    };

    let job = db.create_job(&NewJob {
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash,
        status: JobStatus::Queued,
        email,
        venue,
        git_tag: None,
        git_commit: None,
        next_poll_at: None,
    })?;

    db.add_event(
        Some(&job.id),
        "manual_submit_requested",
        serde_json::json!({ "paper_id": paper_id, "force": force }),
    )?;

    reviewloop::worker::submit_job(config, db, &job.id).await?;
    println!("Submitted job {} for paper_id={paper_id}", job.id);
    Ok(())
}

fn cmd_approve(db: &Db, job_id: &str) -> Result<()> {
    let Some(job) = db.get_job(job_id)? else {
        anyhow::bail!("job not found: {job_id}");
    };

    if job.status != JobStatus::PendingApproval {
        anyhow::bail!(
            "job {} is in status {}, only PENDING_APPROVAL can be approved",
            job_id,
            job.status.as_str()
        );
    }

    db.update_job_state(job_id, JobStatus::Queued, None, Some(None), Some(None))?;
    db.add_event(Some(job_id), "approved", serde_json::json!({}))?;

    println!("Approved job {job_id}, now QUEUED");
    Ok(())
}

fn cmd_import_token(
    config: &Config,
    db: &Db,
    paper_id: &str,
    token: &str,
    source: &str,
) -> Result<()> {
    db.record_email_token(token, source, None)?;

    let next_poll = compute_next_poll_at(
        Utc::now(),
        &config.polling.schedule_minutes,
        0,
        config.polling.jitter_percent,
    );

    if let Some(job) = db.find_latest_open_job_for_paper(paper_id)? {
        db.attach_token_to_job(&job.id, token, next_poll)?;
        db.add_event(
            Some(&job.id),
            "token_imported",
            serde_json::json!({ "source": source, "token": token }),
        )?;
        println!("Attached token to existing job {}", job.id);
        return Ok(());
    }

    let paper = config
        .find_paper(paper_id)
        .with_context(|| format!("paper_id not found in config: {paper_id}"))?;

    let pdf_hash = if Path::new(&paper.pdf_path).exists() {
        sha256_file(Path::new(&paper.pdf_path))?
    } else {
        "unknown".to_string()
    };

    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            config.providers.stanford.email.clone(),
            config.providers.stanford.venue.clone(),
        ),
        _ => (String::new(), None),
    };

    let job = db.create_job(&NewJob {
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash,
        status: JobStatus::Processing,
        email,
        venue,
        git_tag: None,
        git_commit: None,
        next_poll_at: Some(next_poll),
    })?;
    db.attach_token_to_job(&job.id, token, next_poll)?;

    db.add_event(
        Some(&job.id),
        "token_imported",
        serde_json::json!({ "source": source, "token": token }),
    )?;

    println!("Created job {} and attached imported token", job.id);
    Ok(())
}

fn cmd_status(db: &Db, paper_id: Option<&str>, as_json: bool) -> Result<()> {
    let rows = db.list_status_views(paper_id)?;

    if as_json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }

    if rows.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    println!(
        "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<20}  {:<20}  {:<8}",
        "JOB ID", "PAPER", "BACKEND", "STATUS", "ATTEMPT", "NEXT POLL", "STARTED", "ELAPSED"
    );
    println!("{}", "-".repeat(160));
    let now = Utc::now();
    for row in rows {
        let started = row.started_at.unwrap_or(row.created_at);
        let elapsed = format_elapsed(started, now);
        println!(
            "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<20}  {:<20}  {:<8}",
            row.id,
            row.paper_id,
            row.backend,
            row.status,
            row.attempt,
            row.next_poll_at
                .map(|v| v.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            started.to_rfc3339(),
            elapsed
        );
        if let Some(err) = row.last_error {
            println!("  error: {err}");
        }
    }

    Ok(())
}

fn format_elapsed(started: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> String {
    let secs = (now - started).num_seconds().max(0);
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        return format!("{}m", secs / 60);
    }
    if secs < 86_400 {
        return format!("{}h{}m", secs / 3600, (secs % 3600) / 60);
    }
    format!("{}d{}h", secs / 86_400, (secs % 86_400) / 3600)
}

fn cmd_retry(config: &Config, db: &Db, job_id: &str) -> Result<()> {
    let Some(job) = db.get_job(job_id)? else {
        anyhow::bail!("job not found: {job_id}");
    };

    if let Some(_token) = &job.token {
        let next = compute_next_poll_at(
            Utc::now(),
            &config.polling.schedule_minutes,
            0,
            config.polling.jitter_percent,
        );
        db.update_job_state(
            &job.id,
            JobStatus::Processing,
            Some(0),
            Some(Some(next)),
            Some(None),
        )?;
    } else {
        db.update_job_state(&job.id, JobStatus::Queued, Some(0), Some(None), Some(None))?;
    }

    db.add_event(Some(&job.id), "retried", serde_json::json!({}))?;
    println!("Retry scheduled for job {job_id}");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::render_guardrail_notice;
    use reviewloop::config::Config;

    #[test]
    fn guardrail_notice_mentions_core_limits() {
        let cfg = Config::default();
        let notice = render_guardrail_notice(&cfg);
        assert!(notice.contains("core.max_submissions_per_tick"));
        assert!(notice.contains("trigger.pdf.max_scan_papers"));
        assert!(notice.contains("starts at 10m"));
    }
}
