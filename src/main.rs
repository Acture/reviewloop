mod artifact;
mod backend;
mod config;
mod db;
mod email;
mod fallback;
mod model;
mod token;
mod trigger;
mod util;
mod worker;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use config::Config;
use db::Db;
use model::{JobStatus, NewJob};
use std::{
    fs,
    path::{Path, PathBuf},
};
use util::{compute_next_poll_at, sha256_file};

#[derive(Debug, Parser)]
#[command(name = "reviewloop")]
#[command(about = "Automate paperreview.ai submission and retrieval workflows")]
struct Cli {
    #[arg(long, default_value = "reviewloop.toml")]
    config: PathBuf,

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
    Run,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Init { force } => cmd_init(&cli.config, force),
        Command::Daemon {
            command: DaemonCommand::Run,
        } => {
            let (config, db) = load_runtime(&cli.config)?;
            worker::run_daemon(&config, &db).await
        }
        Command::Submit { paper_id, force } => {
            let (config, db) = load_runtime(&cli.config)?;
            cmd_submit(&config, &db, &paper_id, force).await
        }
        Command::Approve { job_id } => {
            let (_config, db) = load_runtime(&cli.config)?;
            cmd_approve(&db, &job_id)
        }
        Command::ImportToken {
            paper_id,
            token,
            source,
        } => {
            let (config, db) = load_runtime(&cli.config)?;
            cmd_import_token(&config, &db, &paper_id, &token, &source)
        }
        Command::Status { paper_id, json } => {
            let (_config, db) = load_runtime(&cli.config)?;
            cmd_status(&db, paper_id.as_deref(), json)
        }
        Command::Retry { job_id } => {
            let (config, db) = load_runtime(&cli.config)?;
            cmd_retry(&config, &db, &job_id)
        }
    }
}

fn cmd_init(config_path: &Path, force: bool) -> Result<()> {
    if config_path.exists() && !force {
        anyhow::bail!(
            "config file already exists: {} (use --force to overwrite)",
            config_path.display()
        );
    }

    Config::save_template(config_path)?;

    let cfg = Config::default();
    fs::create_dir_all(cfg.state_dir())
        .with_context(|| format!("failed to create state dir: {}", cfg.state_dir().display()))?;
    fs::create_dir_all(cfg.state_dir().join("artifacts"))?;

    let db = Db::new(&cfg.state_dir());
    db.init_schema()?;

    println!(
        "Initialized ReviewLoop.\n- config: {}\n- state dir: {}",
        config_path.display(),
        cfg.state_dir().display()
    );
    Ok(())
}

fn load_runtime(config_path: &Path) -> Result<(Config, Db)> {
    let config = Config::load(config_path)?;

    fs::create_dir_all(config.state_dir()).with_context(|| {
        format!(
            "failed to create state dir: {}",
            config.state_dir().display()
        )
    })?;
    fs::create_dir_all(config.state_dir().join("artifacts"))?;

    let db = Db::new(&config.state_dir());
    db.init_schema()?;

    Ok((config, db))
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

    worker::submit_job(config, db, &job.id).await?;
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
        "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<20}",
        "JOB ID", "PAPER", "BACKEND", "STATUS", "ATTEMPT", "NEXT POLL"
    );
    println!("{}", "-".repeat(120));
    for row in rows {
        println!(
            "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<20}",
            row.id,
            row.paper_id,
            row.backend,
            row.status,
            row.attempt,
            row.next_poll_at
                .map(|v| v.to_rfc3339())
                .unwrap_or_else(|| "-".to_string())
        );
        if let Some(err) = row.last_error {
            println!("  error: {err}");
        }
    }

    Ok(())
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
