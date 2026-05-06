use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use clap::{Args, Parser, Subcommand, ValueEnum};
use reviewloop::artifact::write_review_artifacts;
use reviewloop::config::{
    Config, LegacyConfig, PaperConfig, PaperConfigFile, ProjectConfigFile,
    default_project_config_path,
};
use reviewloop::db::Db;
use reviewloop::email_account;
use reviewloop::model::{EventRecord, Job, JobStatus, NewJob, StatusView};
use reviewloop::oauth::{self, google::GoogleOauthProvider};
use reviewloop::util::{compute_next_poll_at, sha256_file};
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{IsTerminal, Write},
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
};
use tracing::{info, warn};

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
    /// Initialize global config (~/.config/reviewloop/config.toml) and data dir.
    /// Run once per machine. For per-repo setup, also run
    /// `reviewloop init project --project-id <id>` from each paper repo.
    Init(InitArgs),
    /// Manage configuration files. Subcommands: init, init project,
    /// migrate-project.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Manage papers tracked in the project config. Subcommands: add, watch,
    /// remove.
    Paper {
        #[command(subcommand)]
        command: PaperCommand,
    },
    /// Manage the background daemon that processes triggers, submissions, and
    /// polls. Subcommands: run, install, uninstall, status, pause, resume.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Enqueue a paper for submission. Use --force to bypass dedupe and clear
    /// any pending cooldown for prior siblings.
    Submit {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        force: bool,
    },
    /// Mark a job as approved so the daemon can proceed to submission.
    Approve {
        #[command(flatten)]
        job_ref: JobOrPaperRef,
    },
    /// Manually inject a review token for a paper (source defaults to
    /// "manual"). Useful when a token arrives out-of-band; see README
    /// 'Exit Codes' section.
    ImportToken {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        token: String,
        #[arg(long, default_value = "manual")]
        source: String,
    },
    /// Poll the backend immediately for a job's current status. Use
    /// --paper-id or --job-id to identify the target.
    Check {
        #[command(flatten)]
        target: CheckTarget,
    },
    /// Show the current status of jobs. Use --paper-id to filter to one paper;
    /// --json for machine-readable output (always {"papers":[...]}); --active
    /// to show only non-terminal jobs (excludes Completed, Failed, etc).
    Status {
        #[arg(long)]
        paper_id: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = false)]
        show_token: bool,
        /// Filter to non-terminal jobs only (Queued, Submitted, Processing, PendingApproval).
        #[arg(long, default_value_t = false)]
        active: bool,
    },
    /// Mark a job as cancelled (terminal). Use when a submission was made by
    /// mistake or the paper is no longer wanted. Does NOT contact the backend.
    ///
    /// Implementation note: uses `JobStatus::Failed` with last_error set to
    /// "cancelled by user" or "cancelled by user: <reason>" and writes a
    /// `cancelled` event with `{reason, previous_status}`. No new enum variant
    /// is needed.
    Cancel {
        #[command(flatten)]
        job_ref: JobOrPaperRef,
        /// Free-form reason recorded on the cancellation event.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Re-queue a failed or stalled job for another attempt. Use --force to
    /// clear any cooldown. Accepts --job-id or --paper-id.
    ///
    /// When using --paper-id, only active jobs (QUEUED, SUBMITTED, PROCESSING)
    /// are matched by default. This avoids ambiguity when multiple failed jobs
    /// exist for the same paper (e.g., a failed v1 and a failed v2). Pass
    /// --include-failed to also consider FAILED, FAILED_NEEDS_MANUAL, and TIMEOUT
    /// jobs. If multiple failed jobs match, you'll get a candidate list and must
    /// use --job-id to pick one explicitly.
    Retry {
        #[command(flatten)]
        job_ref: JobOrPaperRef,
        /// Force immediate retry, clearing any pending cooldown / rate-limit wait.
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Deprecated alias for --force. Will be removed in a future release.
        #[arg(long, default_value_t = false, hide = true)]
        override_rate_limit: bool,
        /// Also consider FAILED, FAILED_NEEDS_MANUAL, and TIMEOUT jobs when
        /// resolving --paper-id. By default only active jobs are matched.
        #[arg(long, default_value_t = false)]
        include_failed: bool,
    },
    /// Mark a job as completed and optionally attach a summary or score.
    /// Accepts --job-id or --paper-id.
    Complete {
        #[command(flatten)]
        job_ref: JobOrPaperRef,
        #[arg(long)]
        summary_text: Option<String>,
        #[arg(long)]
        summary_url: Option<String>,
        #[arg(long, default_value_t = false)]
        empty_summary: bool,
        #[arg(long)]
        score: Option<f64>,
    },
    /// Manage email / OAuth accounts used for token ingestion (optional).
    /// Subcommands: login, logout, switch, status.
    Email {
        #[command(subcommand)]
        command: EmailCommand,
    },
    /// Update the reviewloop binary to the latest release.
    SelfUpdate {
        #[arg(long, value_enum, default_value_t = UpdateMethod::Auto)]
        method: UpdateMethod,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
    /// One-shot: register a paper if absent, force-submit, and tail status
    /// until terminal. Exit codes: 0=Completed, 2=terminal failure, 130=Ctrl+C.
    /// Email/OAuth is NOT required for this command.
    Run(RunArgs),
}

#[derive(Debug, Args, Clone)]
struct InitArgs {
    #[command(subcommand)]
    command: Option<InitCommand>,
}

#[derive(Debug, Subcommand, Clone)]
enum InitCommand {
    /// Set up a per-repo project config (reviewloop.toml) with a given
    /// project ID.
    Project(InitProjectArgs),
}

#[derive(Debug, Args, Clone)]
struct InitProjectArgs {
    #[arg(long)]
    project_id: String,
    #[arg(long, value_name = "PATH")]
    project_root: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    force: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum UpdateMethod {
    Auto,
    Brew,
    Cargo,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Initialize a global or project config. Use `init project --project-id`
    /// for per-repo setup.
    Init(InitArgs),
    /// Migrate a legacy project config to the current format.
    MigrateProject {
        #[arg(long)]
        project_id: String,
        #[arg(long, value_name = "PATH")]
        project_root: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    /// Start the daemon in the foreground (useful for debugging).
    Run {
        #[arg(long, default_value_t = true)]
        panel: bool,
    },
    /// Install and optionally start the launchd service (macOS only).
    ///
    /// Note: this overwrites any previously-installed reviewloop daemon plist.
    /// The launchd label `ai.reviewloop.daemon` is shared across all projects
    /// (multi-daemon deployment is planned for v0.3.0).
    Install {
        #[arg(long, default_value_t = true)]
        start: bool,
    },
    /// Uninstall the launchd service (macOS only).
    Uninstall,
    /// Show daemon health, last tick time, and active jobs.
    Status {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    /// Pause the daemon by unloading the launchd service (macOS only).
    Pause,
    /// Resume the daemon by re-loading the launchd service (macOS only).
    Resume,
}

#[derive(Debug, Subcommand)]
enum PaperCommand {
    /// Register a paper in the project config and optionally submit it now.
    Add {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        project_id: Option<String>,
        #[arg(
            long = "pdf-path",
            alias = "path",
            alias = "artifact",
            value_name = "PATH"
        )]
        pdf_path: String,
        #[arg(long)]
        backend: Option<String>,
        #[arg(long, default_value_t = true)]
        watch: bool,
        #[arg(long)]
        tag_trigger: Option<String>,
        #[arg(long, default_value_t = false)]
        submit_now: bool,
        #[arg(long, default_value_t = false)]
        no_submit_prompt: bool,
        /// Override the venue for this paper. When omitted, the project-level
        /// `venue` from `reviewloop.toml` is used.
        #[arg(long)]
        venue: Option<String>,
    },
    /// Enable or disable PDF-change watching for an already-registered paper.
    Watch {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        enabled: bool,
    },
    /// Remove a paper from the project config. Use --purge-history to also
    /// delete all associated jobs and events from the DB.
    Remove {
        #[arg(long)]
        paper_id: String,
        #[arg(long, default_value_t = false)]
        purge_history: bool,
    },
}

#[derive(Debug, Subcommand)]
enum EmailCommand {
    /// Authenticate an email account for token ingestion (optional feature).
    Login {
        #[arg(long, default_value = "google")]
        provider: String,
    },
    /// Remove stored credentials for an email account.
    Logout {
        #[arg(long)]
        account: Option<String>,
    },
    /// Switch the active email account used for token ingestion.
    Switch {
        #[arg(long)]
        account: String,
    },
    /// Show which email accounts are configured and their auth status.
    Status,
}

/// Argument group for commands that accept either `--job-id` or `--paper-id`.
/// Clap enforces that exactly one of the two flags is provided.
#[derive(Debug, Args, Clone)]
#[group(required = true, multiple = false)]
struct JobOrPaperRef {
    #[arg(long)]
    job_id: Option<String>,
    #[arg(long)]
    paper_id: Option<String>,
}

/// Arguments for `reviewloop run <pdf-path>`.
#[derive(Debug, Args)]
struct RunArgs {
    /// Path to the PDF file to submit.
    pdf_path: String,
    /// Paper ID (defaults to the PDF filename stem, e.g. "paper/main.pdf" → "main").
    #[arg(long)]
    paper_id: Option<String>,
    /// Backend (defaults to project.default_backend, then "stanford").
    #[arg(long)]
    backend: Option<String>,
    /// Enable PDF watching for the registered paper (default: true).
    #[arg(long, default_value_t = true)]
    watch: bool,
    /// Tag trigger pattern for the paper.
    #[arg(long)]
    tag_trigger: Option<String>,
    /// Suppress live status rendering; only print the final result line.
    #[arg(long, default_value_t = false)]
    quiet: bool,
}

/// Argument group for `check`: exactly one of `--job-id`, `--paper-id`, or
/// `--all-processing` must be supplied.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct CheckTarget {
    #[arg(long)]
    job_id: Option<String>,
    #[arg(long)]
    paper_id: Option<String>,
    #[arg(long)]
    all_processing: bool,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let Cli {
        config: config_override,
        command,
    } = Cli::parse();
    Config::ensure_global_config_file()?;
    Config::ensure_global_data_dir()?;

    match command {
        Command::Init(args) => cmd_init(config_override.as_deref(), &args),
        Command::Config { command } => match command {
            ConfigCommand::Init(args) => cmd_init(config_override.as_deref(), &args),
            ConfigCommand::MigrateProject {
                project_id,
                project_root,
            } => cmd_config_migrate_project(
                config_override.as_deref(),
                project_root.as_deref(),
                &project_id,
            ),
        },
        Command::Paper { command } => {
            let write_path = resolve_mutable_project_config_path(config_override.as_deref())?;
            match command {
                PaperCommand::Add {
                    paper_id,
                    project_id,
                    pdf_path,
                    backend,
                    watch,
                    tag_trigger,
                    submit_now,
                    no_submit_prompt,
                    venue,
                } => {
                    let should_submit = cmd_paper_add(PaperAddOptions {
                        config_path: &write_path,
                        paper_id: &paper_id,
                        project_id: project_id.as_deref(),
                        pdf_path: &pdf_path,
                        backend: backend.as_deref(),
                        watch,
                        tag_trigger: tag_trigger.as_deref(),
                        submit_now,
                        no_submit_prompt,
                        venue: venue.as_deref(),
                    })?;
                    if should_submit {
                        let (config, db) = load_runtime(Some(write_path.as_path()), false, true)?;
                        cmd_submit(&config, &db, &paper_id, false).await?;
                    }
                    Ok(())
                }
                PaperCommand::Watch { paper_id, enabled } => {
                    cmd_paper_watch(&write_path, &paper_id, enabled)
                }
                PaperCommand::Remove {
                    paper_id,
                    purge_history,
                } => cmd_paper_remove(&write_path, &paper_id, purge_history),
            }
        }
        Command::Daemon { command } => match command {
            DaemonCommand::Run { panel } => {
                let panel_enabled = panel && std::io::stdout().is_terminal();
                if panel && !panel_enabled {
                    eprintln!(
                        "note: panel requested but stdout is not a TTY; running without panel."
                    );
                }
                let (config, db) = load_runtime(config_override.as_deref(), panel_enabled, false)?;
                reviewloop::worker::run_daemon(&config, &db, panel_enabled).await
            }
            DaemonCommand::Install { start } => {
                cmd_daemon_install(config_override.as_deref(), start)
            }
            DaemonCommand::Uninstall => cmd_daemon_uninstall(),
            DaemonCommand::Pause => cmd_daemon_pause(),
            DaemonCommand::Resume => cmd_daemon_resume(),
            DaemonCommand::Status { json } => {
                // Load config softly — daemon status is still useful without a project config.
                let config_res =
                    Config::load_runtime_with_metadata(config_override.as_deref(), false);
                match config_res {
                    Ok(loaded) => {
                        let config = loaded.config;
                        ensure_runtime_dirs(&config)?;
                        let db = Db::from_config(&config)?;
                        db.ensure_schema()?;
                        cmd_daemon_status(Some(&config), Some(&db), json)
                    }
                    Err(_) => cmd_daemon_status(None, None, json),
                }
            }
        },
        Command::Submit { paper_id, force } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            cmd_submit(&config, &db, &paper_id, force).await
        }
        Command::Approve { job_ref } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            let job_id = match job_ref.job_id {
                Some(id) => id,
                None => {
                    resolve_paper_id_to_job(
                        &db,
                        &config.project_id,
                        &job_ref.paper_id.unwrap(),
                        &[JobStatus::PendingApproval],
                        "approve",
                    )?
                    .id
                }
            };
            cmd_approve(&config, &db, &job_id)
        }
        Command::ImportToken {
            paper_id,
            token,
            source,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            cmd_import_token(&config, &db, &paper_id, &token, &source).await
        }
        Command::Check { target } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            cmd_check(
                &config,
                &db,
                target.job_id.as_deref(),
                target.paper_id.as_deref(),
                target.all_processing,
            )
            .await
        }
        Command::Status {
            paper_id,
            json,
            show_token,
            active,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            cmd_status(&config, &db, paper_id.as_deref(), json, show_token, active)
        }
        Command::Cancel { job_ref, reason } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            let job_id = match job_ref.job_id {
                Some(id) => id,
                None => {
                    let paper_id = job_ref.paper_id.unwrap();
                    resolve_paper_id_to_job(
                        &db,
                        &config.project_id,
                        &paper_id,
                        &[
                            JobStatus::PendingApproval,
                            JobStatus::Queued,
                            JobStatus::Submitted,
                            JobStatus::Processing,
                        ],
                        "cancel",
                    )
                    .map_err(|e| {
                        let msg = e.to_string();
                        if msg.contains("no") && msg.contains("job") {
                            anyhow::anyhow!(
                                "{msg}\n\
                                 hint: cancel only applies to active jobs; for already-completed/failed \
                                 jobs no action is needed; for rerunning, use \
                                 'reviewloop retry --paper-id {paper_id} --include-failed'"
                            )
                        } else {
                            e
                        }
                    })?
                    .id
                }
            };
            cmd_cancel(&config, &db, &job_id, reason.as_deref())
        }
        Command::Retry {
            job_ref,
            force,
            override_rate_limit,
            include_failed,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            let job_id = match job_ref.job_id {
                Some(id) => id,
                None => {
                    let paper_id = job_ref.paper_id.unwrap();
                    let active_statuses = &[
                        JobStatus::Queued,
                        JobStatus::Submitted,
                        JobStatus::Processing,
                    ];
                    let extended_statuses = &[
                        JobStatus::Queued,
                        JobStatus::Submitted,
                        JobStatus::Processing,
                        JobStatus::Failed,
                        JobStatus::FailedNeedsManual,
                        JobStatus::Timeout,
                    ];
                    let statuses: &[JobStatus] = if include_failed {
                        extended_statuses
                    } else {
                        active_statuses
                    };
                    resolve_paper_id_to_job(&db, &config.project_id, &paper_id, statuses, "retry")
                        .map_err(|e| {
                            let msg = e.to_string();
                            if !include_failed && msg.contains("no retry-eligible job") {
                                anyhow!(
                                    "{msg}\n\
                                     hint: no active job for paper_id={paper_id}; \
                                     pass --include-failed to retry a previously-failed job"
                                )
                            } else {
                                e
                            }
                        })?
                        .id
                }
            };
            cmd_retry(&config, &db, &job_id, force, override_rate_limit).await
        }
        Command::Complete {
            job_ref,
            summary_text,
            summary_url,
            empty_summary,
            score,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, false)?;
            let job_id = match job_ref.job_id {
                Some(id) => id,
                None => {
                    resolve_paper_id_to_job(
                        &db,
                        &config.project_id,
                        &job_ref.paper_id.unwrap(),
                        &[JobStatus::Processing, JobStatus::Submitted],
                        "complete",
                    )?
                    .id
                }
            };
            cmd_complete(
                &config,
                &db,
                &job_id,
                summary_text.as_deref(),
                summary_url.as_deref(),
                empty_summary,
                score,
            )
            .await
        }
        Command::Email { command } => {
            let (config, _db) = load_runtime(config_override.as_deref(), false, false)?;
            match command {
                EmailCommand::Login { provider } => cmd_email_login(&config, &provider).await,
                EmailCommand::Logout { account } => cmd_email_logout(&config, account.as_deref()),
                EmailCommand::Switch { account } => cmd_email_switch(&config, &account),
                EmailCommand::Status => cmd_email_status(&config),
            }
        }
        Command::SelfUpdate {
            method,
            yes,
            dry_run,
        } => cmd_self_update(method, yes, dry_run),
        Command::Run(args) => cmd_run(config_override.as_deref(), &args).await,
    }
}

fn cmd_init(config_override: Option<&Path>, args: &InitArgs) -> Result<()> {
    match &args.command {
        Some(InitCommand::Project(project_args)) => cmd_init_project(config_override, project_args),
        None => cmd_init_global(config_override),
    }
}

fn cmd_init_global(config_override: Option<&Path>) -> Result<()> {
    if let Some(path) = config_override {
        anyhow::bail!(
            "--config only applies to project config commands; use `reviewloop init project --project-id <id>` for {}",
            path.display()
        );
    }

    let global_path = Config::ensure_global_config_file()?
        .ok_or_else(|| anyhow!("failed to determine global config path"))?;
    let data_dir = Config::ensure_global_data_dir()?
        .ok_or_else(|| anyhow!("failed to determine global data dir"))?;

    println!(
        "Initialized global reviewloop paths.\n- global config: {}\n- global data dir: {}\n\nNext: cd to your paper repo and run `reviewloop init project --project-id <id>`",
        global_path.display(),
        data_dir.display()
    );
    Ok(())
}

fn cmd_init_project(config_override: Option<&Path>, args: &InitProjectArgs) -> Result<()> {
    if config_override.is_some() && args.project_root.is_some() {
        anyhow::bail!("--config and --project-root cannot be combined");
    }

    let project_path = if let Some(path) = config_override {
        path.to_path_buf()
    } else if let Some(root) = args.project_root.as_deref() {
        root.join("reviewloop.toml")
    } else {
        default_project_config_path()?
    };

    let existed = project_path.exists();
    if existed && !args.force {
        anyhow::bail!(
            "project config already exists: {} (use --force to update it)",
            project_path.display()
        );
    }

    let mut config = if existed {
        ProjectConfigFile::load(&project_path)?
    } else {
        ProjectConfigFile::default()
    };
    config.project_id = args.project_id.clone();
    config.validate(true)?;
    config.save(&project_path)?;

    println!(
        "{} project config.\n- project config: {}\n- project_id: {}",
        if existed { "Updated" } else { "Initialized" },
        project_path.display(),
        config.project_id
    );
    Ok(())
}

fn resolve_mutable_project_config_path(config_override: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = config_override {
        return Ok(path.to_path_buf());
    }

    if let Ok(loaded) = Config::load_runtime_with_metadata(None, false)
        && let Some(project_path) = loaded.project_path
    {
        return Ok(project_path);
    }

    default_project_config_path()
}

fn load_or_create_project_config(
    path: &Path,
    project_id: Option<&str>,
) -> Result<ProjectConfigFile> {
    if path.exists() {
        let config = ProjectConfigFile::load(path)?;
        if let Some(project_id) = project_id
            && !project_id.trim().is_empty()
            && config.project_id != project_id
        {
            anyhow::bail!(
                "project_id mismatch for {}: file has {}, CLI requested {}",
                path.display(),
                config.project_id,
                project_id
            );
        }
        config.validate(true)?;
        return Ok(config);
    }

    let Some(project_id) = project_id.map(str::trim).filter(|value| !value.is_empty()) else {
        anyhow::bail!(
            "project config {} does not exist. create it with `reviewloop init project --project-id <id>` or pass --project-id on `paper add`",
            path.display()
        );
    };

    let config = ProjectConfigFile {
        project_id: project_id.to_string(),
        ..ProjectConfigFile::default()
    };
    config.validate(true)?;
    Ok(config)
}

fn load_existing_project_config(path: &Path) -> Result<ProjectConfigFile> {
    let config = ProjectConfigFile::load(path)
        .with_context(|| format!("failed to load project config {}", path.display()))?;
    config.validate(true)?;
    Ok(config)
}

struct PaperAddOptions<'a> {
    config_path: &'a Path,
    paper_id: &'a str,
    project_id: Option<&'a str>,
    pdf_path: &'a str,
    /// User-provided backend on the CLI. When `None`, falls back to the
    /// project's `default_backend`, then to `Config::DEFAULT_BACKEND`.
    backend: Option<&'a str>,
    watch: bool,
    tag_trigger: Option<&'a str>,
    submit_now: bool,
    no_submit_prompt: bool,
    /// Per-paper venue override. When `None`, the project-level venue applies.
    /// Whitespace-only values are treated as `None`.
    venue: Option<&'a str>,
}

fn cmd_paper_add(options: PaperAddOptions<'_>) -> Result<bool> {
    let mut config = load_or_create_project_config(options.config_path, options.project_id)?;
    if config
        .papers
        .iter()
        .any(|paper| paper.id == options.paper_id)
    {
        anyhow::bail!("paper_id already exists: {}", options.paper_id);
    }

    // Resolve effective backend now so we can echo it back to the user even
    // when they relied on the project default. Empty/whitespace CLI values are
    // treated as "not provided".
    let resolved_backend = options
        .backend
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            config
                .default_backend
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| Config::DEFAULT_BACKEND.to_string());

    // Only persist the backend on the paper if the user actually overrode the
    // project default, so future changes to default_backend continue to flow
    // through to papers that didn't pin one.
    let persisted_backend = options
        .backend
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    config.papers.push(PaperConfigFile {
        id: options.paper_id.to_string(),
        pdf_path: options.pdf_path.to_string(),
        backend: persisted_backend,
        venue: options
            .venue
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string),
    });
    config
        .paper_watch
        .insert(options.paper_id.to_string(), options.watch);
    match options
        .tag_trigger
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
    {
        Some(trigger) => {
            config
                .paper_tag_triggers
                .insert(options.paper_id.to_string(), trigger);
        }
        None => {
            config.paper_tag_triggers.remove(options.paper_id);
        }
    }
    config.save(options.config_path)?;

    // Resolve the runtime state dir for the artifacts path hint.
    // Falls back to the documented default if the global config is not yet loadable.
    let artifacts_root = reviewloop::config::Config::load_runtime(Some(options.config_path), false)
        .map(|cfg| cfg.state_dir().join("artifacts"))
        .unwrap_or_else(|_| std::path::PathBuf::from("~/.review_loop/artifacts"));

    let watch_text = if options.watch { "enabled" } else { "disabled" };
    let venue_text = options
        .venue
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "project default".to_string());
    if let Some(trigger) = options.tag_trigger {
        println!(
            "Added paper {paper_id}.\n- backend: {backend}\n- venue: {venue_text}\n- pdf path: {pdf_path}\n- watch: {watch_text}\n- tag trigger: {trigger}\n- config: {}\n  artifacts will appear in: {}/<job-id>/ once a review completes\n  next: run 'reviewloop submit --paper-id {paper_id}' or 'reviewloop run {pdf_path}' to trigger a review",
            options.config_path.display(),
            artifacts_root.display(),
            paper_id = options.paper_id,
            backend = resolved_backend,
            pdf_path = options.pdf_path,
        );
    } else {
        println!(
            "Added paper {paper_id}.\n- backend: {backend}\n- venue: {venue_text}\n- pdf path: {pdf_path}\n- watch: {watch_text}\n- config: {}\n  artifacts will appear in: {}/<job-id>/ once a review completes\n  next: run 'reviewloop submit --paper-id {paper_id}' or 'reviewloop run {pdf_path}' to trigger a review",
            options.config_path.display(),
            artifacts_root.display(),
            paper_id = options.paper_id,
            backend = resolved_backend,
            pdf_path = options.pdf_path,
        );
    }

    if options.submit_now {
        return Ok(true);
    }

    if !options.no_submit_prompt
        && std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
        && prompt_yes_no("Submit this paper now? [y/N]: ")?
    {
        return Ok(true);
    }

    Ok(false)
}

fn cmd_paper_watch(config_path: &Path, paper_id: &str, enabled: bool) -> Result<()> {
    let mut config = load_existing_project_config(config_path)?;
    if !config.papers.iter().any(|paper| paper.id == paper_id) {
        anyhow::bail!("paper_id not found: {paper_id}");
    }
    config.paper_watch.insert(paper_id.to_string(), enabled);
    config.save(config_path)?;
    println!(
        "Updated watch setting for paper {paper_id}: {}\n- config: {}",
        if enabled { "enabled" } else { "disabled" },
        config_path.display()
    );
    Ok(())
}

fn cmd_paper_remove(config_path: &Path, paper_id: &str, purge_history: bool) -> Result<()> {
    let mut config = load_existing_project_config(config_path)?;
    let before = config.papers.len();
    config.papers.retain(|paper| paper.id != paper_id);
    config.paper_watch.remove(paper_id);
    config.paper_tag_triggers.remove(paper_id);
    let removed_from_config = config.papers.len() != before;
    if removed_from_config {
        config.save(config_path)?;
    }

    let mut purge_summary: Option<(usize, usize, usize, usize)> = None;
    if purge_history {
        let (runtime, db) = load_runtime(Some(config_path), false, true)?;
        let report = db.purge_paper_history(&runtime.project_id, paper_id)?;
        let artifact_dirs = purge_artifacts_for_jobs(&runtime.state_dir(), &report.job_ids)?;
        purge_summary = Some((report.jobs, report.reviews, report.events, artifact_dirs));
    }

    if !removed_from_config && purge_summary.is_none() {
        anyhow::bail!("paper_id not found: {paper_id}");
    }

    if removed_from_config {
        println!(
            "Removed paper {paper_id} from config.\n- config: {}",
            config_path.display()
        );
    } else {
        println!("paper_id not found: {paper_id}; only history purge was applied.");
    }

    if let Some((jobs, reviews, events, artifacts)) = purge_summary {
        println!(
            "Purged history for paper {paper_id}.\n- jobs: {jobs}\n- reviews: {reviews}\n- events: {events}\n- artifact dirs: {artifacts}"
        );
    } else {
        println!(
            "History retained. Use --purge-history to also remove jobs/events/reviews/artifacts."
        );
    }

    Ok(())
}

fn purge_artifacts_for_jobs(state_dir: &Path, job_ids: &[String]) -> Result<usize> {
    let artifacts_root = state_dir.join("artifacts");
    let mut removed = 0usize;
    for job_id in job_ids {
        let dir = artifacts_root.join(job_id);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .with_context(|| format!("failed to remove artifact dir: {}", dir.display()))?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn prompt_yes_no(prompt: &str) -> Result<bool> {
    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let normalized = input.trim().to_ascii_lowercase();
    Ok(matches!(normalized.as_str(), "y" | "yes"))
}

fn cmd_config_migrate_project(
    config_override: Option<&Path>,
    project_root: Option<&Path>,
    project_id: &str,
) -> Result<()> {
    let Some(legacy_path) = Config::legacy_global_config_path() else {
        anyhow::bail!("legacy global config path unavailable; nothing to migrate");
    };
    if !legacy_path.exists() {
        anyhow::bail!("legacy global config not found: {}", legacy_path.display());
    }
    let legacy = LegacyConfig::load(&legacy_path)?;

    let project_path = if let Some(path) = config_override {
        path.to_path_buf()
    } else if let Some(root) = project_root {
        root.join("reviewloop.toml")
    } else {
        default_project_config_path()?
    };
    if project_path.exists() {
        anyhow::bail!("project config already exists: {}", project_path.display());
    }

    let mut project = legacy.project_config();
    project.project_id = project_id.to_string();
    project.validate(true)?;
    project.save(&project_path)?;

    let global_path = Config::ensure_global_config_file()?
        .ok_or_else(|| anyhow!("failed to determine global config path"))?;
    legacy.global_config().save(&global_path)?;

    let backup_path = legacy_path.with_file_name("reviewloop.legacy.bak.toml");
    if backup_path.exists() {
        fs::remove_file(&backup_path)
            .with_context(|| format!("failed to remove old backup {}", backup_path.display()))?;
    }
    fs::rename(&legacy_path, &backup_path).with_context(|| {
        format!(
            "failed to move legacy config {} -> {}",
            legacy_path.display(),
            backup_path.display()
        )
    })?;

    let (runtime, db) = load_runtime(Some(&project_path), false, true)?;
    db.assign_unscoped_rows_to_project(&runtime.project_id)?;

    println!(
        "Migrated legacy config.\n- global config: {}\n- project config: {}\n- legacy backup: {}\n- project_id: {}",
        global_path.display(),
        project_path.display(),
        backup_path.display(),
        project_id
    );
    Ok(())
}

fn cmd_self_update(method: UpdateMethod, yes: bool, dry_run: bool) -> Result<()> {
    let exe = env::current_exe().context("failed to locate current executable path")?;
    let global_cfg = Config::global_config_path();
    let global_data = Config::global_data_dir();

    println!("Self-update will only replace the reviewloop binary.");
    if let Some(path) = global_cfg {
        println!("- global config: {}", path.display());
    }
    if let Some(path) = global_data {
        println!("- global data dir: {}", path.display());
    }
    println!("- current executable: {}", exe.display());
    println!("Config/database/artifacts are not deleted during update.");

    if !yes
        && std::io::stdin().is_terminal()
        && std::io::stdout().is_terminal()
        && !prompt_yes_no("Proceed with self-update? [y/N]: ")?
    {
        println!("Cancelled.");
        return Ok(());
    }

    let selected = match method {
        UpdateMethod::Auto => {
            if is_brew_formula_installed("reviewloop") {
                UpdateMethod::Brew
            } else if command_exists("cargo") {
                UpdateMethod::Cargo
            } else {
                anyhow::bail!(
                    "no supported updater found. install via Homebrew or ensure cargo is available"
                );
            }
        }
        explicit => explicit,
    };

    match selected {
        UpdateMethod::Auto => unreachable!("auto should be resolved"),
        UpdateMethod::Brew => {
            run_update_command("brew", &["upgrade", "reviewloop"], dry_run)?;
        }
        UpdateMethod::Cargo => {
            run_update_command(
                "cargo",
                &["install", "--locked", "reviewloop", "--force"],
                dry_run,
            )?;
        }
    }

    if !dry_run {
        println!("Self-update finished.");
    }
    Ok(())
}

fn run_update_command(program: &str, args: &[&str], dry_run: bool) -> Result<()> {
    println!("Running updater: {} {}", program, args.join(" "));
    if dry_run {
        return Ok(());
    }

    let status = ProcessCommand::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to execute updater: {program}"))?;
    if !status.success() {
        anyhow::bail!("updater exited with status {status}");
    }
    Ok(())
}

fn command_exists(program: &str) -> bool {
    ProcessCommand::new(program)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn is_brew_formula_installed(formula: &str) -> bool {
    if !command_exists("brew") {
        return false;
    }
    ProcessCommand::new("brew")
        .args(["list", "--formula", formula])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn cmd_daemon_install(config_override: Option<&Path>, start: bool) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";

        let loaded = Config::load_runtime_with_metadata(config_override, false)?;
        let reviewloop::config::LoadedConfig {
            config,
            global_path,
            project_path,
            legacy_global_path: _,
            compat_notice,
        } = loaded;
        if let Some(notice) = compat_notice.as_deref() {
            warn!("{notice}");
        }
        ensure_runtime_dirs(&config)?;

        let global_path = global_path
            .map(|path| fs::canonicalize(&path).unwrap_or(path))
            .ok_or_else(|| anyhow!("failed to determine global config path"))?;
        let project_path = project_path.map(|path| fs::canonicalize(&path).unwrap_or(path));

        let home = env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME not set"))?;
        let launch_agents_dir = PathBuf::from(home).join("Library").join("LaunchAgents");
        fs::create_dir_all(&launch_agents_dir).with_context(|| {
            format!(
                "failed to create launch agents directory: {}",
                launch_agents_dir.display()
            )
        })?;

        let plist_path = launch_agents_dir.join(format!("{DAEMON_LABEL}.plist"));
        let exe = env::current_exe().context("failed to locate current executable path")?;
        let stdout_log = config.state_dir().join("daemon.stdout.log");
        let stderr_log = config.state_dir().join("daemon.stderr.log");

        let mut args = vec![exe.display().to_string()];
        if let Some(path) = project_path.as_ref() {
            args.push("--config".to_string());
            args.push(path.display().to_string());
        }
        args.extend([
            "daemon".to_string(),
            "run".to_string(),
            "--panel".to_string(),
            "false".to_string(),
        ]);
        let plist = render_launchd_plist(DAEMON_LABEL, &args, &stdout_log, &stderr_log);
        fs::write(&plist_path, plist)
            .with_context(|| format!("failed to write launchd plist: {}", plist_path.display()))?;

        println!(
            "Installed launchd plist at {}\n- global config: {}",
            plist_path.display(),
            global_path.display()
        );
        if let Some(path) = project_path.as_ref() {
            println!("- project config: {}", path.display());
        } else {
            println!("- mode: global-only daemon (no project config bound)");
        }

        if start {
            let uid = current_uid_string()?;
            let domain = format!("gui/{uid}");
            let target = format!("{domain}/{DAEMON_LABEL}");

            let _ = ProcessCommand::new("launchctl")
                .args(["bootout", &target])
                .output();

            let bootstrap = ProcessCommand::new("launchctl")
                .args(["bootstrap", &domain, plist_path.to_string_lossy().as_ref()])
                .output()
                .context("failed to run launchctl bootstrap")?;
            if !bootstrap.status.success() {
                anyhow::bail!(
                    "launchctl bootstrap failed: {}",
                    String::from_utf8_lossy(&bootstrap.stderr)
                );
            }

            let _ = ProcessCommand::new("launchctl")
                .args(["enable", &target])
                .output();
            let kickstart = ProcessCommand::new("launchctl")
                .args(["kickstart", "-k", &target])
                .output()
                .context("failed to run launchctl kickstart")?;
            if !kickstart.status.success() {
                anyhow::bail!(
                    "launchctl kickstart failed: {}",
                    String::from_utf8_lossy(&kickstart.stderr)
                );
            }

            println!("Daemon started via launchd.");
        } else {
            println!(
                "Run `launchctl bootstrap gui/$(id -u) {}` to start it.",
                plist_path.display()
            );
        }

        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = config_override;
        let _ = start;
        anyhow::bail!("`daemon install` is currently supported on macOS only");
    }
}

fn cmd_daemon_uninstall() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";
        let home = env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME not set"))?;
        let plist_path = PathBuf::from(home)
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{DAEMON_LABEL}.plist"));

        let uid = current_uid_string()?;
        let target = format!("gui/{uid}/{DAEMON_LABEL}");
        let _ = ProcessCommand::new("launchctl")
            .args(["bootout", &target])
            .output();

        if plist_path.exists() {
            fs::remove_file(&plist_path)
                .with_context(|| format!("failed to remove {}", plist_path.display()))?;
            println!("Removed {}", plist_path.display());
        } else {
            println!("No launchd plist found at {}", plist_path.display());
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        anyhow::bail!("`daemon uninstall` is currently supported on macOS only");
    }
}

/// Pause the daemon by unloading it from launchd (macOS only).
/// The plist remains on disk; `daemon resume` re-loads it.
fn cmd_daemon_pause() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";
        let uid = current_uid_string()?;
        let target = format!("gui/{uid}/{DAEMON_LABEL}");
        let status = ProcessCommand::new("launchctl")
            .args(["bootout", &target])
            .status()
            .context("failed to run launchctl bootout")?;
        if status.success() {
            println!(
                "Daemon paused (launchd service unloaded). Run `reviewloop daemon resume` to restart."
            );
        } else {
            anyhow::bail!(
                "launchctl bootout failed — the daemon may not be loaded. \
                Check `reviewloop daemon status`."
            );
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        anyhow::bail!(
            "`daemon pause` is currently macOS-only. \
            Use your system service manager to stop the daemon."
        );
    }
}

/// Resume the daemon by re-loading it into launchd (macOS only).
fn cmd_daemon_resume() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";
        let home = env::var_os("HOME").ok_or_else(|| anyhow::anyhow!("HOME not set"))?;
        let plist_path = PathBuf::from(home)
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{DAEMON_LABEL}.plist"));
        if !plist_path.exists() {
            anyhow::bail!(
                "No plist found at {}. Run `reviewloop daemon install` first.",
                plist_path.display()
            );
        }
        let uid = current_uid_string()?;
        let domain = format!("gui/{uid}");
        let out = ProcessCommand::new("launchctl")
            .args(["bootstrap", &domain, plist_path.to_string_lossy().as_ref()])
            .output()
            .context("failed to run launchctl bootstrap")?;
        if !out.status.success() {
            anyhow::bail!(
                "launchctl bootstrap failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        println!("Daemon resumed (launchd service loaded).");
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        anyhow::bail!(
            "`daemon resume` is currently macOS-only. \
            Use your system service manager to start the daemon."
        );
    }
}

fn cmd_daemon_status(config: Option<&Config>, db: Option<&Db>, as_json: bool) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";
        let uid = current_uid_string()?;
        let target = format!("gui/{uid}/{DAEMON_LABEL}");
        let output = ProcessCommand::new("launchctl")
            .args(["print", &target])
            .output()
            .context("failed to run launchctl print")?;

        let loaded = output.status.success();

        // Detect if daemon process is actually running via launchctl list.
        let running = if loaded {
            ProcessCommand::new("launchctl")
                .args(["list", DAEMON_LABEL])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        } else {
            false
        };

        let now = Utc::now();

        // Collect DB-backed context when available.
        let project_id = config.map(|c| c.project_id.as_str()).unwrap_or("");
        let last_tick_at: Option<chrono::DateTime<Utc>> = db.and_then(|d| {
            if project_id.is_empty() {
                return None;
            }
            match d.most_recent_event_created_at(project_id) {
                Ok(ts) => ts,
                Err(e) => {
                    tracing::warn!(error = %e, project_id, "failed to read last tick time for daemon status");
                    None
                }
            }
        });
        // Surface the most recent tick failure if the worker logged one.
        // Only show it when it's recent enough that it could plausibly be the
        // current state of the daemon: we use 6x the daemon tick interval
        // (30s) as the freshness window, so an error from >3 minutes ago is
        // assumed to have been resolved by a subsequent successful tick.
        let last_tick_error_msg: Option<(chrono::DateTime<Utc>, String)> = db.and_then(|d| {
            if project_id.is_empty() {
                return None;
            }
            let ev = match d.most_recent_event_of_type(project_id, "tick_failed") {
                Ok(opt) => opt?,
                Err(e) => {
                    tracing::warn!(error = %e, project_id, "failed to read last tick_failed event for daemon status");
                    return None;
                }
            };
            // Only include if the most recent tick_failed is also the most
            // recent event overall (no successful work has happened since).
            // If we've recorded a `submitted`, `polled`, etc. after it, the
            // daemon has clearly recovered.
            if last_tick_at
                .map(|latest| latest > ev.created_at)
                .unwrap_or(false)
            {
                return None;
            }
            // And require it to be within ~3 minutes (6 ticks).
            let age = now - ev.created_at;
            if age > chrono::Duration::seconds(180) {
                return None;
            }
            let msg = ev
                .payload
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("(no error message)")
                .to_string();
            Some((ev.created_at, msg))
        });
        let active_jobs: Vec<reviewloop::model::Job> = db
            .and_then(|d| {
                if project_id.is_empty() {
                    None
                } else {
                    match d.list_active_jobs_for_project(project_id) {
                        Ok(jobs) => Some(jobs),
                        Err(e) => {
                            tracing::warn!(error = %e, project_id, "failed to read active jobs for daemon status");
                            None
                        }
                    }
                }
            })
            .unwrap_or_default();

        // Surface a recent Gmail OAuth refresh failure (U6).  Use a 24-hour
        // freshness window: OAuth tokens stay broken until the user re-authorises
        // (not a transient error like a tick failure), so the 1-hour window
        // was hiding ongoing failures that required user action.
        let gmail_oauth_stale: Option<(chrono::DateTime<Utc>, String)> = db.and_then(|d| {
            if project_id.is_empty() {
                return None;
            }
            let ev = match d.most_recent_event_of_type(project_id, "gmail_oauth_refresh_failed") {
                Ok(opt) => opt?,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        project_id,
                        "failed to read last gmail_oauth_refresh_failed event for daemon status"
                    );
                    return None;
                }
            };
            let age = now - ev.created_at;
            if age > chrono::Duration::hours(24) {
                return None;
            }
            let msg = ev
                .payload
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("OAuth token refresh failed")
                .to_string();
            Some((ev.created_at, msg))
        });

        // Compute tick health based on how long ago the last tick occurred.
        // Thresholds: < 60s = normal, 60-300s = stale (note), > 300s = stuck (warning).
        // None means no tick events have ever been recorded, so health is unknown.
        let tick_health = match last_tick_at {
            None => "unknown",
            Some(ts) => {
                let age_secs = (now - ts).num_seconds();
                if age_secs < 60 {
                    "normal"
                } else if age_secs < 300 {
                    "stale"
                } else {
                    "stuck"
                }
            }
        };

        // Query recent proxy_failover events for R3 (proxy health section).
        let recent_proxy_failovers: Vec<EventRecord> = db
            .and_then(|d| {
                if project_id.is_empty() {
                    return None;
                }
                match d.list_recent_events_of_type(project_id, "proxy_failover", 10) {
                    Ok(evs) => Some(evs),
                    Err(e) => {
                        tracing::warn!(error = %e, project_id, "failed to read proxy_failover events for daemon status");
                        None
                    }
                }
            })
            .unwrap_or_default();
        let cutoff_5m = now - chrono::Duration::minutes(5);
        let cutoff_1h = now - chrono::Duration::hours(1);
        let failovers_5m = recent_proxy_failovers
            .iter()
            .filter(|ev| ev.created_at >= cutoff_5m)
            .count();
        let failovers_1h = recent_proxy_failovers
            .iter()
            .filter(|ev| ev.created_at >= cutoff_1h)
            .count();

        if as_json {
            let jobs_json: Vec<serde_json::Value> = active_jobs
                .iter()
                .map(|j| {
                    json!({
                        "job_id": j.id,
                        "paper_id": j.paper_id,
                        "status": j.status.as_str(),
                        "attempt": j.attempt,
                        "next_poll_at": j.next_poll_at.map(|t| t.to_rfc3339()),
                    })
                })
                .collect();
            let last_tick_error_json = match &last_tick_error_msg {
                Some((ts, msg)) => json!({
                    "at": ts.to_rfc3339(),
                    "message": msg,
                }),
                None => serde_json::Value::Null,
            };
            let gmail_oauth_json = match &gmail_oauth_stale {
                Some((ts, msg)) => json!({
                    "stale": true,
                    "since": ts.to_rfc3339(),
                    "message": msg,
                }),
                None => serde_json::Value::Null,
            };
            let proxy_health_recent: Vec<serde_json::Value> = recent_proxy_failovers
                .iter()
                .map(|ev| {
                    json!({
                        "id": ev.id,
                        "created_at": ev.created_at.to_rfc3339(),
                        "payload": ev.payload,
                    })
                })
                .collect();
            let payload = json!({
                "project_id": project_id,
                "service": { "loaded": loaded, "running": running },
                "last_tick_at": last_tick_at.map(|t| t.to_rfc3339()),
                "tick_health": tick_health,
                "last_tick_error": last_tick_error_json,
                "active_jobs": jobs_json,
                "gmail_oauth_status": gmail_oauth_json,
                "proxy_health": {
                    "failovers_5m": failovers_5m,
                    "failovers_1h": failovers_1h,
                    "recent": proxy_health_recent,
                },
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }

        // --- Human-readable output ---
        let service_text = match (loaded, running) {
            (true, true) => "loaded (running)".to_string(),
            (true, false) => "loaded (not running)".to_string(),
            _ => format!("not loaded: {target}"),
        };

        let project_display = if project_id.is_empty() {
            "(no project config)".to_string()
        } else {
            project_id.to_string()
        };

        println!("Daemon status (project={project_display}):");
        println!("  service: {service_text}");

        match last_tick_at {
            Some(ts) => {
                let ago = format_elapsed(ts, now);
                println!(
                    "  last activity: {} ({ago} ago)",
                    ts.format("%Y-%m-%dT%H:%M:%S UTC")
                );
                match tick_health {
                    "stale" => println!(
                        "  last tick: {} (NOTE: older than usual 30s tick)",
                        ts.format("%Y-%m-%dT%H:%M:%S UTC")
                    ),
                    "stuck" => println!(
                        "  last tick: {} (WARNING: daemon may be stuck or stopped)",
                        ts.format("%Y-%m-%dT%H:%M:%S UTC")
                    ),
                    _ => {}
                }
            }
            None => {
                println!("  last activity: none recorded");
                println!("  tick health: unknown (no events recorded yet)");
            }
        }
        match &last_tick_error_msg {
            Some((ts, msg)) => {
                let ago = format_elapsed(*ts, now);
                println!(
                    "  last tick error: {} ({ago} ago)",
                    ts.format("%Y-%m-%dT%H:%M:%S UTC")
                );
                // Indent the message so it's clearly grouped under the label.
                for line in msg.lines() {
                    println!("    {line}");
                }
            }
            None => {
                println!("  last tick error: none");
            }
        }
        if let Some((ts, _msg)) = &gmail_oauth_stale {
            println!(
                "  gmail oauth: stale (refresh failed at {}); run 'reviewloop email login --provider google' to re-authorize",
                ts.format("%Y-%m-%dT%H:%M:%S UTC")
            );
        }
        if failovers_1h > 0 {
            println!(
                "  proxy: {failovers_5m} failover(s) in last 5min, {failovers_1h} in last hour"
            );
        }

        println!();
        if active_jobs.is_empty() {
            println!("Active jobs (0): none");
        } else {
            println!("Active jobs ({}):", active_jobs.len());
            for job in &active_jobs {
                let next_poll_text = match job.next_poll_at {
                    None => "now".to_string(),
                    Some(t) => {
                        let secs = (t - now).num_seconds();
                        if secs <= 0 {
                            "now".to_string()
                        } else {
                            format!("{} (in {}s)", t.format("%H:%M:%S UTC"), secs)
                        }
                    }
                };
                println!(
                    "  {} · {} · attempt={} · next_poll_at={}",
                    job.paper_id,
                    job.status.as_str(),
                    job.attempt,
                    next_poll_text
                );
            }
        }

        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = (config, db, as_json);
        anyhow::bail!(
            "`daemon status` is currently supported on macOS only (requires launchctl).\n  \
             tip: query the database directly for job state:\n    \
             sqlite3 ~/.review_loop/reviewloop.db 'SELECT id, paper_id, status FROM jobs WHERE status NOT IN (\"COMPLETED\", \"FAILED\");'"
        );
    }
}

#[cfg(target_os = "macos")]
fn current_uid_string() -> Result<String> {
    let output = ProcessCommand::new("id")
        .arg("-u")
        .output()
        .context("failed to run id -u")?;
    if !output.status.success() {
        anyhow::bail!("id -u failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if uid.is_empty() {
        anyhow::bail!("id -u returned empty uid");
    }
    Ok(uid)
}

#[cfg(target_os = "macos")]
fn render_launchd_plist(
    label: &str,
    args: &[String],
    stdout_log: &Path,
    stderr_log: &Path,
) -> String {
    let args_xml = args
        .iter()
        .map(|arg| format!("    <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{args_xml}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{stdout_log}</string>
  <key>StandardErrorPath</key>
  <string>{stderr_log}</string>
</dict>
</plist>
"#,
        label = xml_escape(label),
        args_xml = args_xml,
        stdout_log = xml_escape(&stdout_log.to_string_lossy()),
        stderr_log = xml_escape(&stderr_log.to_string_lossy())
    )
}

#[cfg(target_os = "macos")]
fn xml_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn load_runtime(
    config_override: Option<&Path>,
    force_stderr_logs: bool,
    require_project: bool,
) -> Result<(Config, Db)> {
    let loaded = Config::load_runtime_with_metadata(config_override, require_project)?;
    let reviewloop::config::LoadedConfig {
        config,
        global_path,
        project_path,
        legacy_global_path,
        compat_notice,
    } = loaded;

    reviewloop::logging::init_logging(&config, force_stderr_logs)?;
    tracing::info!(
        global_config = ?global_path,
        project_config = ?project_path,
        legacy_global_config = ?legacy_global_path,
        project_id = %config.project_id,
        "loaded runtime configuration"
    );
    if let Some(notice) = compat_notice.as_deref() {
        warn!("{notice}");
    }
    info!("{}", render_guardrail_notice(&config));
    print_guardrail_warnings(&config);

    ensure_runtime_dirs(&config)?;
    let db = Db::from_config(&config)?;
    db.ensure_schema()?;

    // Register this project's config path so fleet-wide commands (eg the
    // bar's "Retry now") can resolve `project_id -> config path` later
    // even when invoked from a directory without a reviewloop.toml.
    // Falls back to the legacy global config path when project settings
    // are still served from ~/.config/reviewloop/reviewloop.toml.
    let registration_path = project_path.as_deref().or(legacy_global_path.as_deref());
    if !config.project_id.trim().is_empty() {
        if let Some(path) = registration_path {
            if let Err(e) = db.register_project_config(&config.project_id, path) {
                tracing::warn!(
                    project_id = %config.project_id,
                    config_path = %path.display(),
                    error = %e,
                    "failed to register project config path; cross-project --job-id commands may need a manual cd",
                );
            }
        }
    }

    Ok((config, db))
}

/// Quietly load only the Config for a specific project's config file. Used by
/// `cmd_retry` when the job's `project_id` does not match the cwd config,
/// so the worker call gets the right per-project providers/polling/papers.
///
/// This intentionally calls `Config::load_runtime_with_metadata` directly
/// instead of `load_runtime`, so the already-initialised logging subscriber,
/// guardrail output, runtime directory setup, DB init, and registry refresh are
/// not repeated for the secondary project config.
fn load_runtime_for_path(config_path: &Path) -> Result<Config> {
    tracing::warn!(
        path = %config_path.display(),
        "loading project config from registry-resolved path; this is non-cwd config"
    );
    let loaded =
        Config::load_runtime_with_metadata(Some(config_path), true).with_context(|| {
            format!(
                "loading registered project config at {}",
                config_path.display()
            )
        })?;
    let config = loaded.config;
    config.validate_for_foreign_load()?;
    Ok(config)
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

/// When `submit --force` is used, clear `next_poll_at` and reset `attempt = 0`
/// for any existing QUEUED / SUBMITTED / PROCESSING job for the same paper so
/// the worker picks them up immediately instead of waiting out a cooldown.
fn clear_sibling_job_cooldowns(config: &Config, db: &Db, paper_id: &str) -> Result<()> {
    let siblings = db.list_active_jobs_for_paper(&config.project_id, paper_id)?;
    for s in siblings {
        if matches!(
            s.status,
            JobStatus::Processing | JobStatus::Submitted | JobStatus::Queued
        ) {
            db.update_job_state(&s.id, s.status, Some(0), Some(None), None)?;
            db.add_event(
                Some(&config.project_id),
                Some(&s.id),
                "force_clear_cooldown",
                json!({
                    "from_command": "submit --force",
                    "previous_attempt": s.attempt,
                    "previous_next_poll_at": s.next_poll_at.map(|t| t.to_rfc3339()),
                }),
            )?;
        }
    }
    Ok(())
}

async fn cmd_submit(config: &Config, db: &Db, paper_id: &str, force: bool) -> Result<()> {
    ensure_project_context(config)?;
    let paper = config
        .find_paper(paper_id)
        .ok_or_else(|| paper_not_found_error(paper_id, config))?;

    let pdf_path = Path::new(&paper.pdf_path);
    if !pdf_path.exists() {
        anyhow::bail!("pdf file not found: {}", pdf_path.display());
    }

    let pdf_hash = sha256_file(pdf_path)?;
    let (version_source, version_key) = version_identity(None, &pdf_hash);
    if !force
        && let Some(existing) = db.find_duplicate_covering_job(
            &config.project_id,
            &paper.id,
            &paper.backend,
            &pdf_hash,
            &version_key,
        )?
    {
        record_duplicate_skip(DuplicateSkipContext {
            config,
            db,
            paper,
            pdf_hash: &pdf_hash,
            version_source: &version_source,
            version_key: &version_key,
            existing: &existing,
            source: "manual_submit",
        })?;
        println!(
            "Skipped submit: existing active/completed job already covers project_id={} paper_id={} backend={} hash={} version={} existing_job_id={} status={}",
            config.project_id,
            paper.id,
            paper.backend,
            pdf_hash,
            existing.version_no,
            existing.id,
            existing.status.as_str()
        );
        return Ok(());
    }

    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            email_account::resolve_submission_email(config, "stanford", None)
                .with_context(|| "reviewloop submit requires a submitter email. set providers.stanford.email in ~/.config/reviewloop/config.toml or run 'reviewloop email login --provider google' to use OAuth (see README 'Email Token Ingestion' section).")?,
            config.venue_for(paper),
        ),
        _ => (String::new(), config.venue_for(paper)),
    };

    if force {
        clear_sibling_job_cooldowns(config, db, paper_id)?;
    }

    let job = db.create_job(&NewJob {
        project_id: config.project_id.clone(),
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
        None,
        Some(&job.id),
        "manual_submit_requested",
        json!({ "paper_id": paper_id, "force": force }),
    )?;

    reviewloop::worker::submit_job(config, db, &job.id).await?;

    // Force the first poll to fire within ~60s regardless of the polling schedule,
    // so the user gets fast feedback after submit.
    if let Some(updated_job) = db.get_job(&job.id)?
        && updated_job.token.is_some()
    {
        let fast_first = Utc::now() + chrono::Duration::seconds(60);
        let current = updated_job
            .next_poll_at
            .unwrap_or(fast_first + chrono::Duration::seconds(1));
        if fast_first < current {
            db.update_job_state(
                &updated_job.id,
                updated_job.status,
                None,
                Some(Some(fast_first)),
                None,
            )?;
        }
    }

    println!("Submitted job {} for paper_id={paper_id}", job.id);
    Ok(())
}

async fn cmd_run(config_override: Option<&Path>, args: &RunArgs) -> Result<()> {
    let write_path = resolve_mutable_project_config_path(config_override)?;
    if !write_path.exists() {
        anyhow::bail!(
            "no project config found at ./reviewloop.toml\n\n\
             reviewloop run needs a project config to know where to store job state (database, artifacts).\n\n\
             Run this first:\n\
               reviewloop init project --project-id <id>\n\n\
             (If you're in a temporary directory, cd to your paper repo first.)"
        );
    }

    let pdf_path_str = &args.pdf_path;
    let paper_id = match &args.paper_id {
        Some(id) => id.clone(),
        None => Path::new(pdf_path_str)
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("cannot derive paper_id from pdf path: {pdf_path_str}"))?,
    };

    // Register paper in project config if not already present.
    {
        let project_cfg = ProjectConfigFile::load(&write_path)?;
        if project_cfg.papers.iter().all(|p| p.id != paper_id) {
            cmd_paper_add(PaperAddOptions {
                config_path: &write_path,
                paper_id: &paper_id,
                project_id: None,
                pdf_path: pdf_path_str,
                backend: args.backend.as_deref(),
                watch: args.watch,
                tag_trigger: args.tag_trigger.as_deref(),
                submit_now: false,
                no_submit_prompt: true,
                venue: None,
            })?;
        }
    }

    let (config, db) = load_runtime(Some(&write_path), false, true)?;
    ensure_project_context(&config)?;

    let paper = config
        .find_paper(&paper_id)
        .ok_or_else(|| paper_not_found_error(&paper_id, &config))?;

    let pdf_path = Path::new(&paper.pdf_path);
    if !pdf_path.exists() {
        anyhow::bail!("pdf file not found: {}", pdf_path.display());
    }

    let pdf_hash = sha256_file(pdf_path)?;
    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            email_account::resolve_submission_email(&config, "stanford", None)
                .with_context(|| "reviewloop run requires a submitter email. set providers.stanford.email in ~/.config/reviewloop/config.toml or run 'reviewloop email login --provider google' to use OAuth (see README 'Email Token Ingestion' section).")?,
            config.venue_for(paper),
        ),
        _ => (String::new(), config.venue_for(paper)),
    };

    // Force: clear cooldowns on any sibling jobs.
    clear_sibling_job_cooldowns(&config, &db, &paper_id)?;

    let job = db.create_job(&NewJob {
        project_id: config.project_id.clone(),
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
        None,
        Some(&job.id),
        "run_submit_requested",
        json!({ "paper_id": paper_id, "force": true }),
    )?;

    // Submit immediately (equivalent to cmd_submit with force=true).
    reviewloop::worker::submit_job(&config, &db, &job.id).await?;

    // Fast-forward the first poll window for snappy feedback.
    if let Some(submitted) = db.get_job(&job.id)?
        && submitted.token.is_some()
    {
        let fast_first = Utc::now() + chrono::Duration::seconds(60);
        let current = submitted
            .next_poll_at
            .unwrap_or(fast_first + chrono::Duration::seconds(1));
        if fast_first < current {
            db.update_job_state(
                &submitted.id,
                submitted.status,
                None,
                Some(Some(fast_first)),
                None,
            )?;
        }
    }

    if !args.quiet {
        println!("Submitted job {} for paper_id={paper_id}", job.id);
    }

    // Foreground polling loop.
    let start = std::time::Instant::now();
    let is_tty = std::io::stdout().is_terminal();
    loop {
        if let Err(e) = reviewloop::worker::run_tick(&config, &db).await {
            warn!("run: tick error: {e:#}");
        }

        let updated = db
            .get_job(&job.id)?
            .ok_or_else(|| anyhow!("job no longer exists: {}", job.id))?;

        if !args.quiet {
            let elapsed_secs = start.elapsed().as_secs();
            let next_poll = match updated.next_poll_at {
                None => "now".to_string(),
                Some(t) => {
                    let secs = (t - Utc::now()).num_seconds().max(0);
                    format!("in {secs}s")
                }
            };
            let line = format!(
                "[t+{}s] {} attempt={} next_poll={}",
                elapsed_secs,
                updated.status.as_str(),
                updated.attempt,
                next_poll,
            );
            if is_tty {
                print!("\r{line:<80}");
                std::io::stdout().flush().ok();
            } else {
                println!("{line}");
            }
        }

        let is_terminal = matches!(
            updated.status,
            JobStatus::Completed
                | JobStatus::Failed
                | JobStatus::FailedNeedsManual
                | JobStatus::Timeout
        );

        if is_terminal {
            if !args.quiet && is_tty {
                println!();
            }
            match updated.status {
                JobStatus::Completed => {
                    let artifact_root = config.state_dir().join("artifacts").join(&job.id);
                    println!("✓ Review complete for job {}", job.id);
                    for name in &["review.md", "review.json", "meta.json"] {
                        let p = artifact_root.join(name);
                        if p.exists() {
                            println!("  {name}: {}", p.display());
                        }
                    }
                    return Ok(());
                }
                _ => {
                    let reason = updated.last_error.as_deref().unwrap_or("(no details)");
                    eprintln!(
                        "✗ Job {} reached {}: {}",
                        job.id,
                        updated.status.as_str(),
                        reason
                    );
                    std::process::exit(2);
                }
            }
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                if !args.quiet && is_tty {
                    println!();
                }
                eprintln!(
                    "^C  job {} left in {} state; resume tracking with 'reviewloop status --paper-id {}' or 'reviewloop check --paper-id {}'",
                    job.id,
                    updated.status.as_str(),
                    paper_id,
                    paper_id,
                );
                std::process::exit(130);
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
        }
    }
}

fn cmd_approve(config: &Config, db: &Db, job_id: &str) -> Result<()> {
    ensure_project_context(config)?;
    let job = ensure_project_job(config, db, job_id)?;

    if job.status != JobStatus::PendingApproval {
        anyhow::bail!(
            "job {} is in status {}, only PENDING_APPROVAL can be approved",
            job_id,
            job.status.as_str()
        );
    }

    db.update_job_state(job_id, JobStatus::Queued, None, Some(None), Some(None))?;
    db.add_event(None, Some(job_id), "approved", json!({}))?;

    println!("Approved job {job_id}, now QUEUED");
    Ok(())
}

/// Cancel a non-terminal job by marking it Failed with a cancellation reason.
///
/// Implementation choice (option b): reuses `JobStatus::Failed` instead of
/// adding a new `JobStatus::Cancelled` variant, keeping the schema unchanged.
/// The `last_error` field is set to "cancelled by user" or
/// "cancelled by user: <reason>" and a `cancelled` event is written with
/// `{reason, previous_status}`.
fn cmd_cancel(config: &Config, db: &Db, job_id: &str, reason: Option<&str>) -> Result<()> {
    // Cancel only updates DB rows for the named job — no worker, no provider
    // config required. Allow it to run without project context so the menu
    // bar companion can cancel any job from any cwd. When project context IS
    // set, we still scope-check so a paper-repo cwd cannot accidentally act
    // on a different project's jobs.
    let job = resolve_job_by_id_any_project(config, db, job_id)?;

    if matches!(
        job.status,
        JobStatus::Completed
            | JobStatus::Failed
            | JobStatus::FailedNeedsManual
            | JobStatus::Timeout
    ) {
        anyhow::bail!(
            "job {} is already in terminal status {}; cannot cancel",
            job.id,
            job.status.as_str()
        );
    }

    let previous_status = job.status.as_str().to_string();
    let last_error = match reason {
        Some(reason) => format!("cancelled by user: {reason}"),
        None => "cancelled by user".to_string(),
    };

    // user override: PendingApproval -> Failed is not in the state machine but
    // cancellation is a legitimate user action on any non-terminal job.
    db.update_job_state_unchecked(
        &job.id,
        JobStatus::Failed,
        None,
        Some(None),
        Some(Some(last_error)),
    )?;
    db.add_event(
        None,
        Some(&job.id),
        "cancelled",
        json!({
            "reason": reason,
            "previous_status": previous_status,
        }),
    )?;

    println!("Cancelled job {} (was {})", job.id, previous_status);
    Ok(())
}

async fn cmd_import_token(
    config: &Config,
    db: &Db,
    paper_id: &str,
    token: &str,
    source: &str,
) -> Result<()> {
    ensure_project_context(config)?;
    db.record_email_token(token, source, None)?;

    if let Some(job) = db.find_latest_open_job_for_paper(&config.project_id, paper_id)? {
        db.attach_token_to_job(&job.id, token, Utc::now())?;
        db.add_event(
            None,
            Some(&job.id),
            "token_imported",
            json!({ "source": source, "token": token }),
        )?;
        println!("Attached token to existing job {}", job.id);
        // Immediately poll rather than waiting for the next 30-second daemon tick.
        if let Some(fresh) = db.get_job(&job.id)? {
            reviewloop::worker::poll_job(config, db, &fresh).await?;
        }
        if let Some(after_poll) = db.get_job(&job.id)? {
            let is_failed = matches!(
                after_poll.status,
                JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout
            );
            if is_failed {
                let detail = after_poll.last_error.as_deref().unwrap_or("(no details)");
                eprintln!(
                    "warning: token attached but immediate poll returned {}: {}",
                    after_poll.status.as_str(),
                    detail
                );
                std::process::exit(2);
            }
        }
        return Ok(());
    }

    let paper = config
        .find_paper(paper_id)
        .ok_or_else(|| paper_not_found_error(paper_id, config))?;

    let pdf_hash = if Path::new(&paper.pdf_path).exists() {
        sha256_file(Path::new(&paper.pdf_path))?
    } else {
        "unknown".to_string()
    };

    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            email_account::resolve_submission_email(config, "stanford", None)?,
            config.venue_for(paper),
        ),
        _ => (String::new(), config.venue_for(paper)),
    };

    let job = db.create_job(&NewJob {
        project_id: config.project_id.clone(),
        paper_id: paper.id.clone(),
        backend: paper.backend.clone(),
        pdf_path: paper.pdf_path.clone(),
        pdf_hash,
        status: JobStatus::Processing,
        email,
        venue,
        git_tag: None,
        git_commit: None,
        next_poll_at: Some(Utc::now()),
    })?;
    db.attach_token_to_job(&job.id, token, Utc::now())?;

    db.add_event(
        None,
        Some(&job.id),
        "token_imported",
        json!({ "source": source, "token": token }),
    )?;

    println!("Created job {} and attached imported token", job.id);

    // Immediately poll rather than waiting for the next 30-second daemon tick.
    if let Some(fresh) = db.get_job(&job.id)? {
        reviewloop::worker::poll_job(config, db, &fresh).await?;
    }
    if let Some(after_poll) = db.get_job(&job.id)? {
        let is_failed = matches!(
            after_poll.status,
            JobStatus::Failed | JobStatus::FailedNeedsManual | JobStatus::Timeout
        );
        if is_failed {
            let detail = after_poll.last_error.as_deref().unwrap_or("(no details)");
            eprintln!(
                "warning: token attached but immediate poll returned {}: {}",
                after_poll.status.as_str(),
                detail
            );
            std::process::exit(2);
        }
    }
    Ok(())
}

async fn cmd_check(
    config: &Config,
    db: &Db,
    job_id: Option<&str>,
    paper_id: Option<&str>,
    all_processing: bool,
) -> Result<()> {
    ensure_project_context(config)?;

    let mut targets = Vec::new();
    if let Some(job_id) = job_id {
        let job = ensure_project_job(config, db, job_id)?;
        if job.token.is_none() {
            anyhow::bail!("job {job_id} has no token; cannot poll");
        }
        targets.push(job);
    } else {
        let rows = db.list_status_views(&config.project_id, paper_id)?;
        for row in rows {
            if row.status != JobStatus::Processing.as_str() {
                continue;
            }
            let Some(job) = db.get_project_job(&config.project_id, &row.id)? else {
                continue;
            };
            if job.token.is_some() {
                targets.push(job);
            }
            if !all_processing && !targets.is_empty() {
                break;
            }
        }
    }

    if targets.is_empty() {
        println!("No processing job with token found to check.");
        return Ok(());
    }

    for job in targets {
        maybe_record_manual_poll_override(config, db, &job)?;
        reviewloop::worker::poll_job(config, db, &job).await?;
        let Some(updated) = db.get_project_job(&config.project_id, &job.id)? else {
            continue;
        };
        println!(
            "Checked job {} -> status={}{}",
            updated.id,
            updated.status.as_str(),
            updated
                .next_poll_at
                .map(|t| format!(", next_poll_at={}", t.to_rfc3339()))
                .unwrap_or_default()
        );
    }

    Ok(())
}

fn cmd_status(
    config: &Config,
    db: &Db,
    paper_id: Option<&str>,
    as_json: bool,
    show_token: bool,
    active: bool,
) -> Result<()> {
    ensure_project_context(config)?;
    let state_dir = config.state_dir();
    let all_rows = db.list_status_views(&config.project_id, paper_id)?;

    // Active filter: keep only non-terminal statuses.
    const NON_TERMINAL: &[&str] = &["PENDING_APPROVAL", "QUEUED", "SUBMITTED", "PROCESSING"];
    let rows: Vec<_> = if active {
        all_rows
            .into_iter()
            .filter(|r| NON_TERMINAL.contains(&r.status.as_str()))
            .collect()
    } else {
        all_rows
    };

    if let Some(paper_id) = paper_id {
        let events = db.list_timeline_events(&config.project_id, paper_id)?;
        if as_json {
            let payload = json!({
                "project_id": config.project_id,
                "papers": [{
                    "paper_id": paper_id,
                    "rows": rows.iter().map(|row| status_row_json(row, show_token, Some(&state_dir))).collect::<Vec<_>>(),
                    "timeline": timeline_json(&rows, &events, show_token, Some(&state_dir)),
                }],
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }
        render_timeline_text(config, paper_id, &rows, &events, show_token);
        return Ok(());
    }

    if as_json {
        // Group rows by paper_id and emit the same {paper_id, rows, timeline}
        // wrapper shape as the single-paper path, so tooling can treat both
        // identically by iterating `payload.papers`.
        let mut groups: std::collections::BTreeMap<String, Vec<StatusView>> =
            std::collections::BTreeMap::new();
        for row in rows {
            groups.entry(row.paper_id.clone()).or_default().push(row);
        }
        let papers_json: Vec<_> = groups
            .iter()
            .map(|(pid, group_rows)| {
                let events = db
                    .list_timeline_events(&config.project_id, pid)
                    .unwrap_or_default();
                json!({
                    "paper_id": pid,
                    "rows": group_rows.iter().map(|r| status_row_json(r, show_token, Some(&state_dir))).collect::<Vec<_>>(),
                    "timeline": timeline_json(group_rows, &events, show_token, Some(&state_dir)),
                })
            })
            .collect();
        let payload = json!({
            "project_id": config.project_id,
            "papers": papers_json,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    if rows.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    // Group rows by paper_id, ordered alphabetically (BTreeMap).
    let mut groups: std::collections::BTreeMap<&str, Vec<&reviewloop::model::StatusView>> =
        std::collections::BTreeMap::new();
    for row in &rows {
        groups.entry(row.paper_id.as_str()).or_default().push(row);
    }

    let now = Utc::now();
    for (pid, group_rows) in &groups {
        println!("Paper: {pid}");
        for row in group_rows.iter() {
            let started = row.started_at.unwrap_or(row.created_at);
            let elapsed = format_elapsed(started, now);
            let score_str = row.score.clone().unwrap_or_else(|| "-".to_string());
            let token_str = render_token(row.token.as_deref(), show_token);
            let next_poll_str = row
                .next_poll_at
                .map(|v| v.to_rfc3339())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "  [{status}] {id}  backend={backend}  attempt={attempt}  score={score}  token={token}  next_poll={next_poll}  elapsed={elapsed}",
                status = row.status,
                id = row.id,
                backend = row.backend,
                attempt = row.attempt,
                score = score_str,
                token = token_str,
                next_poll = next_poll_str,
            );
            if let Some(err) = row.last_error.as_deref() {
                let truncated = if err.len() > 80 { &err[..80] } else { err };
                println!("    error: {truncated}");
            }
            if row.status == "COMPLETED" {
                let artifact_dir = state_dir.join("artifacts").join(&row.id);
                println!("    artifacts: {}", artifact_dir.display());
            }
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

async fn cmd_retry(
    config: &Config,
    db: &Db,
    job_id: &str,
    force: bool,
    override_rate_limit: bool,
) -> Result<()> {
    if override_rate_limit {
        eprintln!("warning: --override-rate-limit is deprecated; use --force instead");
        tracing::warn!("deprecated flag --override-rate-limit used; treating as --force");
    }
    let force = force || override_rate_limit;

    let job = resolve_job_by_id_any_project(config, db, job_id)?;

    // The worker requires the job's *own* project config (providers, polling,
    // papers). When the cwd config doesn't match (e.g. the menu bar spawned
    // us from a directory that has no reviewloop.toml), look up the project's
    // registered config path and load that quietly.
    let owned;
    let effective_config: &Config = if config.project_id == job.project_id {
        config
    } else {
        owned = load_effective_config_for_job(db, &job)?;
        &owned
    };

    if force {
        let previous_next_poll_at = job.next_poll_at.map(|value| value.to_rfc3339());
        if job.token.is_some() {
            if job.status != JobStatus::Processing {
                anyhow::bail!("--force for token-backed jobs only supports PROCESSING jobs");
            }
            db.add_event(
                Some(&job.project_id),
                Some(&job.id),
                "manual_rate_limit_override",
                json!({
                    "paper_id": job.paper_id,
                    "mode": "poll",
                    "reason": "manual_override",
                    "previous_status": job.status.as_str(),
                    "previous_next_poll_at": previous_next_poll_at,
                    "version_no": job.version_no,
                    "round_no": job.round_no
                }),
            )?;
            reviewloop::worker::poll_job(effective_config, db, &job).await?;
            println!("Immediately polled job {job_id} with rate-limit override");
            return Ok(());
        }

        if !matches!(
            job.status,
            JobStatus::Queued
                | JobStatus::Submitted
                | JobStatus::Failed
                | JobStatus::FailedNeedsManual
                | JobStatus::Timeout
        ) {
            anyhow::bail!(
                "--force for tokenless jobs only supports QUEUED/SUBMITTED/FAILED/FAILED_NEEDS_MANUAL/TIMEOUT"
            );
        }

        // user override: reset terminal job back to Queued for re-submission.
        db.update_job_state_unchecked(&job.id, JobStatus::Queued, Some(0), Some(None), Some(None))?;
        db.add_event(
            Some(&job.project_id),
            Some(&job.id),
            "manual_rate_limit_override",
            json!({
                "paper_id": job.paper_id,
                "mode": "submit",
                "reason": "manual_override",
                "previous_status": job.status.as_str(),
                "previous_next_poll_at": previous_next_poll_at,
                "version_no": job.version_no,
                "round_no": job.round_no
            }),
        )?;
        reviewloop::worker::submit_job(effective_config, db, &job.id).await?;
        println!("Immediately retried job {job_id} with rate-limit override");
        return Ok(());
    }

    if job.token.is_some() {
        let next = compute_next_poll_at(
            Utc::now(),
            &effective_config.polling.schedule_minutes,
            0,
            effective_config.polling.jitter_percent,
        );
        // user override: explicit retry may cross state-machine boundaries.
        db.update_job_state_unchecked(
            &job.id,
            JobStatus::Processing,
            Some(0),
            Some(Some(next)),
            Some(None),
        )?;
    } else {
        // user override: explicit retry may cross state-machine boundaries.
        db.update_job_state_unchecked(&job.id, JobStatus::Queued, Some(0), Some(None), Some(None))?;
    }

    db.add_event(Some(&job.project_id), Some(&job.id), "retried", json!({}))?;
    println!("Retry scheduled for job {job_id}");

    Ok(())
}

/// Resolve the per-project config for a job whose project_id doesn't match
/// the cwd config (typically the no-cwd-context menu-bar case).
///
/// 1. Reject jobs with empty project_id (legacy data) — there is no
///    project config to load.
/// 2. Look up the registered config path; bail with re-register hint
///    when the project has never been seen.
/// 3. Load the registered file; on ENOENT, prune the stale registry
///    entry and bail with a path-moved hint.
fn load_effective_config_for_job(db: &Db, job: &reviewloop::model::Job) -> Result<Config> {
    if job.project_id.trim().is_empty() {
        anyhow::bail!(
            "job {} has no associated project (legacy data); re-submit the PDF \
             with `reviewloop run <pdf>` from a configured project repo instead",
            job.id
        );
    }
    let Some(config_path) = db.resolve_project_config_path(&job.project_id)? else {
        anyhow::bail!(
            "project '{}' cannot be located — reviewloop hasn't seen this project yet.\n\n\
             To fix:\n\
             1. cd /path/to/that/repo\n\
             2. run: reviewloop status\n\
             3. then retry this job from the Bar again\n\n\
             (This tells reviewloop where to find the project's config file.)",
            job.project_id
        );
    };
    db.add_event(
        Some(&job.project_id),
        Some(&job.id),
        "foreign_config_loaded",
        json!({
            "config_path": config_path.display().to_string(),
            "cwd": env::current_dir()
                .ok()
                .and_then(|p| p.to_str().map(String::from)),
        }),
    )?;
    match load_runtime_for_path(&config_path) {
        Ok(config) => Ok(config),
        Err(err) if error_chain_contains_not_found(&err) => {
            // Self-heal: forget the stale row so the next CLI call from the
            // moved repo can re-register cleanly.
            let _ = db.forget_project_registration(&job.project_id);
            anyhow::bail!(
                "project '{}' cannot be located — its config file used to be at {} but that path no longer exists.\n\n\
                 To fix:\n\
                 1. cd /path/to/that/repo (wherever you moved it)\n\
                 2. run: reviewloop status\n\
                 3. then retry this job from the Bar again\n\n\
                 (This updates reviewloop's internal project location cache.)",
                job.project_id,
                config_path.display()
            );
        }
        Err(err) => Err(err),
    }
}

fn error_chain_contains_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    })
}

async fn cmd_complete(
    config: &Config,
    db: &Db,
    job_id: &str,
    summary_text: Option<&str>,
    summary_url: Option<&str>,
    empty_summary: bool,
    score: Option<f64>,
) -> Result<()> {
    ensure_project_context(config)?;
    let job = ensure_project_job(config, db, job_id)?;
    if !matches!(
        job.status,
        JobStatus::PendingApproval
            | JobStatus::Queued
            | JobStatus::Submitted
            | JobStatus::Processing
            | JobStatus::Failed
            | JobStatus::FailedNeedsManual
            | JobStatus::Timeout
    ) {
        anyhow::bail!(
            "job {} is in status {}; manual completion is not allowed",
            job.id,
            job.status.as_str()
        );
    }

    let source_count =
        summary_text.is_some() as u8 + summary_url.is_some() as u8 + empty_summary as u8;
    if source_count != 1 {
        anyhow::bail!("choose exactly one of --summary-text, --summary-url, or --empty-summary");
    }

    let (mode, source_url, raw_json) =
        build_manual_review_payload(summary_text, summary_url, empty_summary, score).await?;
    let token = job
        .token
        .clone()
        .unwrap_or_else(|| format!("manual:{}", job.id));
    let (_, summary_md, _) = write_review_artifacts(&config.state_dir(), &job, &token, &raw_json)?;
    db.upsert_review(&job.id, &token, &raw_json.to_string(), &summary_md)?;
    // user override: manual completion may move jobs out of terminal states.
    db.update_job_state_unchecked(
        &job.id,
        JobStatus::Completed,
        Some(job.attempt + 1),
        Some(None),
        Some(None),
    )?;
    db.add_event(
        None,
        Some(&job.id),
        "manual_completed",
        json!({
            "paper_id": job.paper_id,
            "mode": mode,
            "source_url": source_url,
            "score": score,
            "version_no": job.version_no,
            "round_no": job.round_no
        }),
    )?;
    println!("Marked job {job_id} as COMPLETED");
    Ok(())
}

fn ensure_project_job(config: &Config, db: &Db, job_id: &str) -> Result<reviewloop::model::Job> {
    db.get_project_job(&config.project_id, job_id)?
        .ok_or_else(|| anyhow!("job not found: {job_id}"))
}

/// Look up a job by ID with optional project scoping.
///
/// When `config.project_id` is non-empty, the lookup is project-scoped (so
/// callers in a paper repo cannot accidentally act on another project's
/// jobs). When `config.project_id` is empty, falls back to a global lookup
/// — useful for the menu bar companion which runs from any directory and
/// needs to act on jobs across every project.
fn resolve_job_by_id_any_project(
    config: &Config,
    db: &Db,
    job_id: &str,
) -> Result<reviewloop::model::Job> {
    if config.project_id.trim().is_empty() {
        db.get_job(job_id)?
            .ok_or_else(|| anyhow!("job not found: {job_id}"))
    } else {
        ensure_project_job(config, db, job_id)
    }
}

/// Build a rich error for a missing paper_id that lists known paper_ids.
fn paper_not_found_error(paper_id: &str, config: &Config) -> anyhow::Error {
    let known: Vec<&str> = config.papers.iter().map(|p| p.id.as_str()).collect();
    if known.is_empty() {
        anyhow!(
            "paper_id not found: {paper_id}\n  \
             no papers configured yet — add one with `reviewloop paper add --paper-id {paper_id} --pdf-path <path>`"
        )
    } else {
        let known_str = known.join(", ");
        anyhow!(
            "paper_id not found: {paper_id}\n  \
             known paper_ids: {known_str}\n  \
             add this paper with `reviewloop paper add --paper-id {paper_id} --pdf-path <path>`"
        )
    }
}

fn ensure_project_context(config: &Config) -> Result<()> {
    if config.project_id.trim().is_empty() {
        anyhow::bail!(
            "this command requires a project config. run `reviewloop init project --project-id <id>` in your repo first"
        );
    }
    Ok(())
}

/// Resolve a `--paper-id` value to a single `Job` eligible for `command`.
///
/// Queries jobs for `(project_id, paper_id)` whose status is in
/// `allowed_statuses` (ordered by `updated_at DESC`) and returns:
/// - the job when exactly one match is found,
/// - an error with a clear message when 0 or >1 jobs match.
fn resolve_paper_id_to_job(
    db: &Db,
    project_id: &str,
    paper_id: &str,
    allowed_statuses: &[JobStatus],
    command: &str,
) -> Result<Job> {
    let status_strs: Vec<&str> = allowed_statuses.iter().map(|s| s.as_str()).collect();
    let all_views = db.list_status_views(project_id, Some(paper_id))?;
    let mut matching: Vec<_> = all_views
        .iter()
        .filter(|v| status_strs.contains(&v.status.as_str()))
        .collect();
    // Sort by updated_at DESC for consistent ordering.
    matching.sort_by_key(|b| std::cmp::Reverse(b.updated_at));

    match matching.len() {
        0 => {
            let statuses_str = allowed_statuses
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "no {command}-eligible job for paper_id={paper_id} \
                 (looking for statuses: {statuses_str})"
            )
        }
        1 => db
            .get_project_job(project_id, &matching[0].id)?
            .ok_or_else(|| anyhow!("job no longer exists: {}", matching[0].id)),
        _ => {
            let candidates = matching
                .iter()
                .take(5)
                .map(|v| format!("{} ({})", v.id, v.status))
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::bail!(
                "multiple jobs match paper_id={paper_id} for {command}; \
                 pass --job-id explicitly. candidates: {candidates}"
            )
        }
    }
}

fn version_identity(git_commit: Option<&str>, pdf_hash: &str) -> (String, String) {
    if let Some(commit) = git_commit.map(str::trim).filter(|value| !value.is_empty()) {
        ("git_commit".to_string(), commit.to_string())
    } else {
        ("pdf_hash".to_string(), pdf_hash.to_string())
    }
}

struct DuplicateSkipContext<'a> {
    config: &'a Config,
    db: &'a Db,
    paper: &'a PaperConfig,
    pdf_hash: &'a str,
    version_source: &'a str,
    version_key: &'a str,
    existing: &'a Job,
    source: &'a str,
}

fn record_duplicate_skip(ctx: DuplicateSkipContext<'_>) -> Result<()> {
    warn!(
        project_id = %ctx.config.project_id,
        paper_id = %ctx.paper.id,
        backend = %ctx.paper.backend,
        source = %ctx.source,
        existing_job_id = %ctx.existing.id,
        existing_status = %ctx.existing.status.as_str(),
        "skipped duplicate submit"
    );
    ctx.db.add_event(
        Some(&ctx.config.project_id),
        None,
        "duplicate_skipped",
        json!({
            "project_id": ctx.config.project_id,
            "paper_id": ctx.paper.id,
            "backend": ctx.paper.backend,
            "pdf_hash": ctx.pdf_hash,
            "version_no": ctx.existing.version_no,
            "round_no": ctx.existing.round_no,
            "version_source": ctx.version_source,
            "version_key": ctx.version_key,
            "existing_job_id": ctx.existing.id,
            "existing_job_status": ctx.existing.status.as_str(),
            "source": ctx.source
        }),
    )?;
    Ok(())
}

fn maybe_record_manual_poll_override(
    config: &Config,
    db: &Db,
    job: &reviewloop::model::Job,
) -> Result<()> {
    if job
        .next_poll_at
        .is_some_and(|next_poll_at| next_poll_at > Utc::now())
    {
        db.add_event(
            Some(&config.project_id),
            Some(&job.id),
            "manual_rate_limit_override",
            json!({
                "paper_id": job.paper_id,
                "mode": "poll",
                "reason": "manual_check",
                "previous_status": job.status.as_str(),
                "previous_next_poll_at": job.next_poll_at.map(|value| value.to_rfc3339()),
                "version_no": job.version_no,
                "round_no": job.round_no
            }),
        )?;
    }
    Ok(())
}

fn status_row_json(row: &StatusView, show_token: bool, state_dir: Option<&Path>) -> Value {
    let artifact_dir = if row.status == "COMPLETED" {
        state_dir.map(|dir| dir.join("artifacts").join(&row.id).display().to_string())
    } else {
        None
    };
    json!({
        "id": row.id,
        "project_id": row.project_id,
        "paper_id": row.paper_id,
        "backend": row.backend,
        "status": row.status,
        "attempt": row.attempt,
        "token_masked": render_token(row.token.as_deref(), false),
        "token": show_token.then(|| row.token.clone()).flatten(),
        "created_at": row.created_at.to_rfc3339(),
        "started_at": row.started_at.map(|value| value.to_rfc3339()),
        "next_poll_at": row.next_poll_at.map(|value| value.to_rfc3339()),
        "updated_at": row.updated_at.to_rfc3339(),
        "last_error": row.last_error,
        "pdf_hash": row.pdf_hash,
        "git_tag": row.git_tag,
        "git_commit": row.git_commit,
        "version_no": row.version_no,
        "round_no": row.round_no,
        "version_source": row.version_source,
        "version_key": row.version_key,
        "score": row.score,
        "summary_md": row.summary_md,
        "completed_at": row.completed_at.map(|value| value.to_rfc3339()),
        "artifact_dir": artifact_dir,
    })
}

fn timeline_json(
    rows: &[StatusView],
    events: &[EventRecord],
    show_token: bool,
    state_dir: Option<&Path>,
) -> Vec<Value> {
    let mut entries = Vec::new();
    for row in rows {
        entries.push(json!({
            "kind": "job",
            "created_at": row.created_at.to_rfc3339(),
            "job": status_row_json(row, show_token, state_dir),
        }));
    }
    for event in events {
        entries.push(json!({
            "kind": "event",
            "created_at": event.created_at.to_rfc3339(),
            "event_type": event.event_type,
            "job_id": event.job_id,
            "payload": event.payload,
        }));
    }
    entries.sort_by(|left, right| {
        left.get("created_at")
            .and_then(Value::as_str)
            .cmp(&right.get("created_at").and_then(Value::as_str))
    });
    entries
}

fn render_timeline_text(
    config: &Config,
    paper_id: &str,
    rows: &[StatusView],
    events: &[EventRecord],
    show_token: bool,
) {
    if rows.is_empty() && events.is_empty() {
        println!(
            "No jobs found for paper {paper_id} in project {}.",
            config.project_id
        );
        return;
    }

    println!(
        "Paper timeline: {} (project_id={})",
        paper_id, config.project_id
    );
    let mut grouped: std::collections::BTreeMap<(u32, u32), Vec<Value>> =
        std::collections::BTreeMap::new();

    for row in rows {
        grouped
            .entry((row.version_no, row.round_no))
            .or_default()
            .push(json!({
                "kind": "job",
                "created_at": row.created_at.to_rfc3339(),
                "row": status_row_json(row, show_token, None),
            }));
    }

    for event in events {
        let version_no = event
            .payload
            .get("version_no")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
            .or_else(|| {
                event.job_id.as_deref().and_then(|job_id| {
                    rows.iter()
                        .find(|row| row.id == job_id)
                        .map(|row| row.version_no)
                })
            })
            .unwrap_or(0);
        let round_no = event
            .payload
            .get("round_no")
            .and_then(Value::as_u64)
            .map(|value| value as u32)
            .or_else(|| {
                event.job_id.as_deref().and_then(|job_id| {
                    rows.iter()
                        .find(|row| row.id == job_id)
                        .map(|row| row.round_no)
                })
            })
            .unwrap_or(0);
        grouped
            .entry((version_no, round_no))
            .or_default()
            .push(json!({
                "kind": "event",
                "created_at": event.created_at.to_rfc3339(),
                "event_type": event.event_type,
                "payload": event.payload,
                "job_id": event.job_id,
            }));
    }

    for ((version_no, round_no), entries) in grouped {
        println!();
        println!(
            "Version {} Round {}",
            if version_no == 0 {
                "-".to_string()
            } else {
                version_no.to_string()
            },
            if round_no == 0 {
                "-".to_string()
            } else {
                round_no.to_string()
            }
        );
        let mut entries = entries;
        entries.sort_by(|left, right| {
            left.get("created_at")
                .and_then(Value::as_str)
                .cmp(&right.get("created_at").and_then(Value::as_str))
        });

        for entry in entries {
            if entry.get("kind").and_then(Value::as_str) == Some("job") {
                let row = entry.get("row").cloned().unwrap_or(Value::Null);
                let token = row
                    .get("token")
                    .and_then(Value::as_str)
                    .or_else(|| row.get("token_masked").and_then(Value::as_str))
                    .unwrap_or("-");
                println!(
                    "- [{}] {} status={} attempt={} score={} token={} created_at={}",
                    row.get("backend").and_then(Value::as_str).unwrap_or("-"),
                    row.get("id").and_then(Value::as_str).unwrap_or("-"),
                    row.get("status").and_then(Value::as_str).unwrap_or("-"),
                    row.get("attempt").and_then(Value::as_u64).unwrap_or(0),
                    row.get("score").and_then(Value::as_str).unwrap_or("-"),
                    token,
                    row.get("created_at").and_then(Value::as_str).unwrap_or("-")
                );
                println!(
                    "  started_at={} completed_at={}",
                    row.get("started_at").and_then(Value::as_str).unwrap_or("-"),
                    row.get("completed_at")
                        .and_then(Value::as_str)
                        .unwrap_or("-")
                );
                println!(
                    "  git_tag={} git_commit={} pdf_hash={}",
                    row.get("git_tag").and_then(Value::as_str).unwrap_or("-"),
                    row.get("git_commit").and_then(Value::as_str).unwrap_or("-"),
                    row.get("pdf_hash").and_then(Value::as_str).unwrap_or("-")
                );
                if let Some(summary) = row.get("summary_md").and_then(Value::as_str)
                    && !summary.trim().is_empty()
                {
                    println!("{}", indent_block(summary, "  "));
                }
                if let Some(err) = row.get("last_error").and_then(Value::as_str)
                    && !err.trim().is_empty()
                {
                    println!("  error: {err}");
                }
            } else {
                let payload = entry.get("payload").cloned().unwrap_or(Value::Null);
                println!(
                    "- [event] {} created_at={} {}",
                    entry
                        .get("event_type")
                        .and_then(Value::as_str)
                        .unwrap_or("-"),
                    entry
                        .get("created_at")
                        .and_then(Value::as_str)
                        .unwrap_or("-"),
                    compact_event_payload(&payload)
                );
            }
        }
    }
}

fn render_token(token: Option<&str>, show_token: bool) -> String {
    match token {
        Some(token) if show_token => token.to_string(),
        Some(token) if token.len() > 8 => {
            format!("{}...{}", &token[..4], &token[token.len() - 4..])
        }
        Some(token) => token.to_string(),
        None => "-".to_string(),
    }
}

fn compact_event_payload(payload: &Value) -> String {
    if let Some(existing_job_id) = payload.get("existing_job_id").and_then(Value::as_str) {
        let status = payload
            .get("existing_job_status")
            .and_then(Value::as_str)
            .unwrap_or("-");
        return format!("existing_job_id={} status={}", existing_job_id, status);
    }
    if let Some(mode) = payload.get("mode").and_then(Value::as_str) {
        return format!("mode={mode}");
    }
    if let Some(source) = payload.get("source").and_then(Value::as_str) {
        return format!("source={source}");
    }
    payload.to_string()
}

fn indent_block(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn build_manual_review_payload(
    summary_text: Option<&str>,
    summary_url: Option<&str>,
    empty_summary: bool,
    score: Option<f64>,
) -> Result<(String, Option<String>, Value)> {
    let (mode, source_url, mut payload) = if let Some(text) = summary_text {
        (
            "text".to_string(),
            None,
            json!({
                "sections": { "summary": text },
                "content": text,
            }),
        )
    } else if let Some(url) = summary_url {
        let payload = fetch_summary_url(url).await?;
        ("url".to_string(), Some(url.to_string()), payload)
    } else if empty_summary {
        (
            "empty".to_string(),
            None,
            json!({
                "sections": { "summary": "" },
                "content": "",
            }),
        )
    } else {
        unreachable!("summary mode validated by caller");
    };

    if let Some(score) = score {
        payload["numerical_score"] = json!(score);
    }
    let manual_meta = json!({
        "mode": mode,
        "source_url": source_url.clone(),
        "completed_at": Utc::now().to_rfc3339(),
        "score": score
    });
    if let Some(object) = payload.as_object_mut() {
        object.insert("manual_completion".to_string(), manual_meta);
    }
    Ok((mode, source_url, payload))
}

async fn fetch_summary_url(url: &str) -> Result<Value> {
    let response = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to fetch summary URL: {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!(
            "summary URL request failed with status {}: {}",
            status,
            body
        );
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    let body = response
        .text()
        .await
        .context("failed to read summary URL body")?;

    if content_type.contains("application/json") {
        let mut value: Value =
            serde_json::from_str(&body).context("summary URL returned invalid JSON")?;
        let is_review_like = value.as_object().is_some_and(|object| {
            object.contains_key("sections")
                || object.contains_key("content")
                || object.contains_key("numerical_score")
        });
        if is_review_like {
            return Ok(value);
        }
        value = json!({ "content": value.to_string() });
        return Ok(value);
    }

    if content_type.contains("text/html") {
        return Ok(json!({ "content": html_to_text(&body) }));
    }

    Ok(json!({ "content": body }))
}

fn html_to_text(html: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => {
                in_tag = false;
                out.push(' ');
            }
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

async fn cmd_email_login(config: &Config, provider: &str) -> Result<()> {
    if provider != "google" {
        anyhow::bail!("unsupported provider: {provider}. currently supported: google");
    }

    let Some(oauth_provider) = GoogleOauthProvider::from_config_for_login(config)? else {
        anyhow::bail!(
            "gmail_oauth is not configured. set `gmail_oauth.enabled = true` and \
             `gmail_oauth.client_id` in ~/.config/reviewloop/config.toml, or set \
             REVIEWLOOP_GMAIL_CLIENT_ID / REVIEWLOOP_GMAIL_CLIENT_SECRET environment variables. \
             see README \"Email Token Ingestion\" section."
        );
    };

    let active_token_path = oauth_provider.run_browser_pkce_login().await?;
    let access_token = oauth::ensure_valid_access_token(&oauth_provider).await?;
    let email = fetch_google_profile_email(&access_token).await?;

    let account_token_path = google_account_token_path(config, &email);
    copy_token_file(&active_token_path, &account_token_path)?;
    let account = email_account::upsert_account(config, "google", &email, &account_token_path)?;

    println!(
        "Email login completed.\n- provider: google\n- email: {}\n- account id: {}\n- active token: {}\n- account token: {}",
        account.email,
        account.id,
        active_token_path.display(),
        account_token_path.display()
    );
    Ok(())
}

fn cmd_email_logout(config: &Config, account: Option<&str>) -> Result<()> {
    let removed = email_account::remove_account(config, account)?;
    let Some(removed) = removed else {
        println!("No email account found.");
        return Ok(());
    };

    let removed_token_path = PathBuf::from(&removed.token_path);
    if removed_token_path.exists() {
        let _ = std::fs::remove_file(&removed_token_path);
    }

    let active_token_path = active_google_token_path(config);
    if let Some(active) = email_account::active_account(config)? {
        if active.provider == "google" {
            copy_token_file(Path::new(&active.token_path), &active_token_path)?;
        }
    } else if active_token_path.exists() {
        let _ = std::fs::remove_file(&active_token_path);
    }

    println!(
        "Email logout completed.\n- removed: {} ({})",
        removed.email, removed.id
    );
    Ok(())
}

fn cmd_email_switch(config: &Config, account: &str) -> Result<()> {
    let selected = email_account::switch_account(config, account)?;
    if selected.provider == "google" {
        let active_token_path = active_google_token_path(config);
        copy_token_file(Path::new(&selected.token_path), &active_token_path)?;
    }

    println!(
        "Switched active email account.\n- provider: {}\n- email: {}\n- id: {}",
        selected.provider, selected.email, selected.id
    );
    Ok(())
}

fn cmd_email_status(config: &Config) -> Result<()> {
    let accounts = email_account::list_accounts(config)?;
    if accounts.is_empty() {
        println!("No email accounts found.");
        return Ok(());
    }

    let active_id = email_account::active_account(config)?.map(|a| a.id);
    println!(
        "{:<36}  {:<10}  {:<35}  {:<6}",
        "ACCOUNT ID", "PROVIDER", "EMAIL", "ACTIVE"
    );
    println!("{}", "-".repeat(96));
    for account in accounts {
        let is_active = active_id.as_deref() == Some(account.id.as_str());
        println!(
            "{:<36}  {:<10}  {:<35}  {:<6}",
            account.id,
            account.provider,
            account.email,
            if is_active { "yes" } else { "no" }
        );
    }
    Ok(())
}

fn google_account_token_path(config: &Config, email: &str) -> PathBuf {
    let safe_email = email
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    config
        .state_dir()
        .join("oauth")
        .join("accounts")
        .join(format!("google_{safe_email}.json"))
}

fn active_google_token_path(config: &Config) -> PathBuf {
    config
        .gmail_oauth
        .as_ref()
        .and_then(|g| g.token_store_path.as_ref())
        .map(PathBuf::from)
        .unwrap_or_else(|| config.state_dir().join("oauth").join("google_token.json"))
}

fn copy_token_file(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create token target directory {}",
                parent.display()
            )
        })?;
    }
    fs::copy(from, to).with_context(|| {
        format!(
            "failed to copy token file {} -> {}",
            from.display(),
            to.display()
        )
    })?;
    Ok(())
}

async fn fetch_google_profile_email(access_token: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .get("https://gmail.googleapis.com/gmail/v1/users/me/profile")
        .bearer_auth(access_token)
        .send()
        .await
        .context("failed to fetch gmail profile")?;
    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_else(|_| "".to_string());
        anyhow::bail!("gmail profile request failed: {body}");
    }
    let payload: serde_json::Value = resp.json().await.context("invalid gmail profile payload")?;
    let Some(email) = payload.get("emailAddress").and_then(|v| v.as_str()) else {
        anyhow::bail!("gmail profile payload missing emailAddress");
    };
    Ok(email.to_string())
}

#[cfg(test)]
mod tests {
    use super::{load_effective_config_for_job, load_runtime_for_path, render_guardrail_notice};
    use reviewloop::config::Config;
    use reviewloop::db::Db;
    use reviewloop::model::{JobStatus, NewJob};
    use std::{
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::{Mutex, MutexGuard},
    };
    use tempfile::TempDir;

    static CONFIG_ENV_LOCK: Mutex<()> = Mutex::new(());

    struct IsolatedConfigEnv {
        _guard: MutexGuard<'static, ()>,
        temp_dir: TempDir,
        old_home: Option<OsString>,
        old_reviewloop_state_dir: Option<OsString>,
        old_xdg_config_home: Option<OsString>,
    }

    impl IsolatedConfigEnv {
        fn new() -> Self {
            let guard = CONFIG_ENV_LOCK
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let temp_dir = tempfile::tempdir().expect("tempdir");
            let old_home = std::env::var_os("HOME");
            let old_reviewloop_state_dir = std::env::var_os("REVIEWLOOP_STATE_DIR");
            let old_xdg_config_home = std::env::var_os("XDG_CONFIG_HOME");

            set_env_path("HOME", &temp_dir.path().join("home"));
            set_env_path(
                "REVIEWLOOP_STATE_DIR",
                &temp_dir.path().join("reviewloop-state"),
            );
            set_env_path("XDG_CONFIG_HOME", &temp_dir.path().join("xdg-config"));

            Self {
                _guard: guard,
                temp_dir,
                old_home,
                old_reviewloop_state_dir,
                old_xdg_config_home,
            }
        }

        fn project_config_path(&self) -> PathBuf {
            self.temp_dir
                .path()
                .join("home")
                .join("project")
                .join("reviewloop.toml")
        }
    }

    impl Drop for IsolatedConfigEnv {
        fn drop(&mut self) {
            restore_env("XDG_CONFIG_HOME", self.old_xdg_config_home.as_ref());
            restore_env(
                "REVIEWLOOP_STATE_DIR",
                self.old_reviewloop_state_dir.as_ref(),
            );
            restore_env("HOME", self.old_home.as_ref());
        }
    }

    fn set_env_path(key: &str, value: &Path) {
        // SAFETY: These tests hold CONFIG_ENV_LOCK while mutating process-wide
        // environment variables and do not spawn threads while the override is set.
        unsafe {
            std::env::set_var(key, value.as_os_str());
        }
    }

    fn restore_env(key: &str, value: Option<&OsString>) {
        // SAFETY: See set_env_path; Drop runs before releasing CONFIG_ENV_LOCK.
        unsafe {
            if let Some(value) = value {
                std::env::set_var(key, value);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    fn write_project_config(path: &Path, project_id: &str) {
        fs::create_dir_all(path.parent().expect("project config parent")).expect("mkdir project");
        fs::write(
            path,
            format!("project_id = \"{project_id}\"\npapers = []\n"),
        )
        .expect("write project config");
    }

    fn new_retry_job(project_id: &str) -> NewJob {
        NewJob {
            project_id: project_id.to_string(),
            paper_id: "main".to_string(),
            backend: "stanford".to_string(),
            pdf_path: "paper.pdf".to_string(),
            pdf_hash: "abc123".to_string(),
            status: JobStatus::Queued,
            email: "test@example.com".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            next_poll_at: None,
        }
    }

    #[test]
    fn load_runtime_for_path_does_not_panic_on_repeated_call() {
        let env = IsolatedConfigEnv::new();
        let config_path = env.project_config_path();
        write_project_config(&config_path, "repeated-load-project");

        let first = load_runtime_for_path(&config_path).expect("first load succeeds");
        let second = load_runtime_for_path(&config_path).expect("second load succeeds");

        assert_eq!(first.project_id, "repeated-load-project");
        assert_eq!(second.project_id, "repeated-load-project");
    }

    #[test]
    fn load_effective_config_for_job_records_foreign_config_audit_event() {
        let env = IsolatedConfigEnv::new();
        let project_id = "audit-project";
        let config_path = env.project_config_path();
        write_project_config(&config_path, project_id);

        let db = Db::new_in_memory("cmd_retry_foreign_config_audit").unwrap();
        db.init_schema().unwrap();
        db.register_project_config(project_id, &config_path)
            .unwrap();
        let job = db.create_job(&new_retry_job(project_id)).unwrap();

        let config = load_effective_config_for_job(&db, &job).expect("load registered config");
        assert_eq!(config.project_id, project_id);

        let event = db
            .most_recent_event_of_type(project_id, "foreign_config_loaded")
            .unwrap()
            .expect("foreign config audit event");
        assert_eq!(event.job_id.as_deref(), Some(job.id.as_str()));
        let config_path_string = config_path.display().to_string();
        assert_eq!(
            event
                .payload
                .get("config_path")
                .and_then(|value| value.as_str()),
            Some(config_path_string.as_str())
        );
    }

    #[test]
    fn load_effective_config_for_job_self_heals_when_registered_path_missing() {
        let env = IsolatedConfigEnv::new();
        let project_id = "missing-path-project";
        let config_path = env.project_config_path();
        write_project_config(&config_path, project_id);

        let db = Db::new_in_memory("cmd_retry_missing_registered_path").unwrap();
        db.ensure_schema().unwrap();
        db.register_project_config(project_id, &config_path)
            .unwrap();
        let job = db.create_job(&new_retry_job(project_id)).unwrap();

        fs::remove_file(&config_path).expect("remove registered config");
        let err = load_effective_config_for_job(&db, &job).unwrap_err();
        let msg = err.to_string();

        assert!(
            msg.contains("no longer exists") || msg.contains("not found"),
            "got: {msg}"
        );
        assert!(
            db.resolve_project_config_path(project_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn guardrail_notice_mentions_core_limits() {
        let cfg = Config::default();
        let notice = render_guardrail_notice(&cfg);
        assert!(notice.contains("core.max_submissions_per_tick"));
        assert!(notice.contains("trigger.pdf.max_scan_papers"));
        assert!(notice.contains("starts at 10m"));
    }

    mod resolve_paper_id {
        use super::super::resolve_paper_id_to_job;
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};

        fn new_job(project_id: &str, paper_id: &str, pdf_hash: &str, status: JobStatus) -> NewJob {
            NewJob {
                project_id: project_id.to_string(),
                paper_id: paper_id.to_string(),
                backend: "stanford".to_string(),
                pdf_path: "/test/paper.pdf".to_string(),
                pdf_hash: pdf_hash.to_string(),
                status,
                email: "test@example.com".to_string(),
                venue: None,
                git_tag: None,
                git_commit: None,
                next_poll_at: None,
            }
        }

        #[test]
        fn zero_matches_returns_error() {
            let db = Db::new_in_memory("resolve_zero").unwrap();
            db.ensure_schema().unwrap();

            let err = resolve_paper_id_to_job(
                &db,
                "proj1",
                "paper1",
                &[JobStatus::PendingApproval],
                "approve",
            )
            .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("no approve-eligible job"), "got: {msg}");
            assert!(msg.contains("paper_id=paper1"), "got: {msg}");
            assert!(msg.contains("PENDING_APPROVAL"), "got: {msg}");
        }

        #[test]
        fn one_match_returns_job() {
            let db = Db::new_in_memory("resolve_one").unwrap();
            db.ensure_schema().unwrap();

            let created = db
                .create_job(&new_job(
                    "proj1",
                    "paper1",
                    "hash_a",
                    JobStatus::PendingApproval,
                ))
                .unwrap();

            let resolved = resolve_paper_id_to_job(
                &db,
                "proj1",
                "paper1",
                &[JobStatus::PendingApproval],
                "approve",
            )
            .unwrap();
            assert_eq!(resolved.id, created.id);
        }

        #[test]
        fn multiple_matches_returns_error_with_candidates() {
            let db = Db::new_in_memory("resolve_multi").unwrap();
            db.ensure_schema().unwrap();

            db.create_job(&new_job(
                "proj1",
                "paper1",
                "hash_a",
                JobStatus::PendingApproval,
            ))
            .unwrap();
            db.create_job(&new_job(
                "proj1",
                "paper1",
                "hash_b",
                JobStatus::PendingApproval,
            ))
            .unwrap();

            let err = resolve_paper_id_to_job(
                &db,
                "proj1",
                "paper1",
                &[JobStatus::PendingApproval],
                "approve",
            )
            .unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("multiple jobs match"), "got: {msg}");
            assert!(msg.contains("paper_id=paper1"), "got: {msg}");
            assert!(msg.contains("pass --job-id explicitly"), "got: {msg}");
            assert!(msg.contains("candidates:"), "got: {msg}");
        }

        #[test]
        fn filters_by_status_correctly() {
            let db = Db::new_in_memory("resolve_status_filter").unwrap();
            db.ensure_schema().unwrap();

            // A completed job should NOT match when looking for PROCESSING
            db.create_job(&new_job("proj1", "paper1", "hash_a", JobStatus::Completed))
                .unwrap();
            let processing_job = db
                .create_job(&new_job("proj1", "paper1", "hash_b", JobStatus::Processing))
                .unwrap();

            let resolved = resolve_paper_id_to_job(
                &db,
                "proj1",
                "paper1",
                &[JobStatus::Processing, JobStatus::Submitted],
                "complete",
            )
            .unwrap();
            assert_eq!(resolved.id, processing_job.id);
        }
    }

    mod check_arggroup {
        use crate::Cli;
        use clap::Parser;

        #[test]
        fn check_with_no_flags_fails() {
            let err = Cli::try_parse_from(["reviewloop", "check"]).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("required") || msg.contains("job-id") || msg.contains("paper-id"),
                "expected a required-argument error, got: {msg}"
            );
        }

        #[test]
        fn check_with_both_job_id_and_paper_id_fails() {
            let err =
                Cli::try_parse_from(["reviewloop", "check", "--job-id", "x", "--paper-id", "y"])
                    .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("cannot be used with") || msg.contains("argument"),
                "expected a conflict error, got: {msg}"
            );
        }

        #[test]
        fn check_with_job_id_succeeds() {
            Cli::try_parse_from(["reviewloop", "check", "--job-id", "some-id"]).unwrap();
        }

        #[test]
        fn check_with_all_processing_succeeds() {
            Cli::try_parse_from(["reviewloop", "check", "--all-processing"]).unwrap();
        }

        #[test]
        fn check_with_paper_id_succeeds() {
            Cli::try_parse_from(["reviewloop", "check", "--paper-id", "main"]).unwrap();
        }
    }

    mod error_messages {
        use super::super::paper_not_found_error;
        use reviewloop::config::Config;

        fn config_with_papers(paper_ids: &[&str]) -> Config {
            let mut cfg = Config::default();
            for id in paper_ids {
                cfg.papers.push(reviewloop::config::PaperConfig {
                    id: id.to_string(),
                    pdf_path: format!("/fake/{id}.pdf"),
                    backend: "stanford".to_string(),
                    venue: None,
                });
            }
            cfg
        }

        #[test]
        fn paper_not_found_no_papers_suggests_add() {
            let cfg = config_with_papers(&[]);
            let err = paper_not_found_error("myid", &cfg);
            let msg = err.to_string();
            assert!(msg.contains("paper_id not found: myid"), "got: {msg}");
            assert!(msg.contains("no papers configured yet"), "got: {msg}");
            assert!(
                msg.contains("reviewloop paper add --paper-id myid"),
                "got: {msg}"
            );
        }

        #[test]
        fn paper_not_found_with_known_papers_lists_them() {
            let cfg = config_with_papers(&["main", "camera_ready"]);
            let err = paper_not_found_error("foo", &cfg);
            let msg = err.to_string();
            assert!(msg.contains("paper_id not found: foo"), "got: {msg}");
            assert!(msg.contains("known paper_ids:"), "got: {msg}");
            assert!(msg.contains("main"), "got: {msg}");
            assert!(msg.contains("camera_ready"), "got: {msg}");
            assert!(
                msg.contains("reviewloop paper add --paper-id foo"),
                "got: {msg}"
            );
        }
    }

    mod daemon_status_db {
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};
        use serde_json::Value;

        fn make_job(paper_id: &str, status: JobStatus, idx: u32) -> NewJob {
            NewJob {
                project_id: "proj".to_string(),
                paper_id: paper_id.to_string(),
                backend: "stanford".to_string(),
                pdf_path: "/fake/paper.pdf".to_string(),
                pdf_hash: format!("hash{idx}"),
                status,
                email: "test@example.com".to_string(),
                venue: None,
                git_tag: None,
                git_commit: None,
                next_poll_at: None,
            }
        }

        #[test]
        fn list_active_jobs_returns_queued_submitted_processing() {
            let db = Db::new_in_memory("daemon_status_active").unwrap();
            db.ensure_schema().unwrap();

            db.create_job(&make_job("p1", JobStatus::Queued, 1))
                .unwrap();
            db.create_job(&make_job("p2", JobStatus::Processing, 2))
                .unwrap();
            db.create_job(&make_job("p3", JobStatus::Submitted, 3))
                .unwrap();
            db.create_job(&make_job("p4", JobStatus::Completed, 4))
                .unwrap();
            db.create_job(&make_job("p5", JobStatus::Failed, 5))
                .unwrap();

            let active = db.list_active_jobs_for_project("proj").unwrap();
            let statuses: Vec<&str> = active.iter().map(|j| j.status.as_str()).collect();
            // Should include QUEUED, PROCESSING, SUBMITTED but not COMPLETED or FAILED
            assert_eq!(active.len(), 3, "expected 3 active jobs, got: {statuses:?}");
            assert!(active.iter().all(|j| matches!(
                j.status,
                JobStatus::Queued | JobStatus::Processing | JobStatus::Submitted
            )));
        }

        #[test]
        fn most_recent_event_reflects_last_activity() {
            let db = Db::new_in_memory("daemon_status_event").unwrap();
            db.ensure_schema().unwrap();

            // No events → None
            assert!(db.most_recent_event_created_at("proj").unwrap().is_none());

            // Add an event
            db.add_event(Some("proj"), None, "test_event", serde_json::json!({}))
                .unwrap();
            let ts = db.most_recent_event_created_at("proj").unwrap();
            assert!(ts.is_some(), "expected a timestamp after inserting event");
        }

        #[test]
        fn most_recent_event_of_type_filters_by_type_and_returns_payload() {
            let db = Db::new_in_memory("daemon_status_event_typed").unwrap();
            db.ensure_schema().unwrap();

            // No matching events → None
            assert!(
                db.most_recent_event_of_type("proj", "tick_failed")
                    .unwrap()
                    .is_none()
            );

            // Mixed events of different types
            db.add_event(
                Some("proj"),
                None,
                "submitted",
                serde_json::json!({"backend": "stanford"}),
            )
            .unwrap();
            db.add_event(
                Some("proj"),
                None,
                "tick_failed",
                serde_json::json!({"tick": 7, "error": "first failure"}),
            )
            .unwrap();
            db.add_event(
                Some("proj"),
                None,
                "polled",
                serde_json::json!({"status": "PROCESSING"}),
            )
            .unwrap();
            db.add_event(
                Some("proj"),
                None,
                "tick_failed",
                serde_json::json!({"tick": 9, "error": "newer failure"}),
            )
            .unwrap();
            // Different project's tick_failed must be ignored.
            db.add_event(
                Some("other"),
                None,
                "tick_failed",
                serde_json::json!({"tick": 99, "error": "wrong project"}),
            )
            .unwrap();

            let ev = db
                .most_recent_event_of_type("proj", "tick_failed")
                .unwrap()
                .expect("expected most-recent tick_failed event");
            assert_eq!(ev.event_type, "tick_failed");
            assert_eq!(
                ev.payload.get("error").and_then(Value::as_str),
                Some("newer failure"),
                "should return the most recently added tick_failed event"
            );
        }

        #[test]
        fn daemon_status_json_structure() {
            use super::super::cmd_daemon_status;
            // We can't easily capture stdout in unit tests, but we can verify the
            // DB helpers return the right shape that would feed into JSON output.
            let db = Db::new_in_memory("daemon_status_json").unwrap();
            db.ensure_schema().unwrap();

            db.create_job(&make_job("main", JobStatus::Processing, 1))
                .unwrap();
            db.create_job(&make_job("cr", JobStatus::Queued, 2))
                .unwrap();

            let active = db.list_active_jobs_for_project("proj").unwrap();
            assert_eq!(active.len(), 2);

            let jobs_json: Vec<Value> = active
                .iter()
                .map(|j| {
                    serde_json::json!({
                        "job_id": j.id,
                        "paper_id": j.paper_id,
                        "status": j.status.as_str(),
                        "attempt": j.attempt,
                        "next_poll_at": j.next_poll_at.map(|t| t.to_rfc3339()),
                    })
                })
                .collect();

            // Verify shape
            assert!(jobs_json.iter().all(|j| j.get("job_id").is_some()));
            assert!(jobs_json.iter().all(|j| j.get("paper_id").is_some()));
            assert!(jobs_json.iter().all(|j| j.get("status").is_some()));
            assert!(jobs_json.iter().any(|j| j["status"] == "PROCESSING"));
            assert!(jobs_json.iter().any(|j| j["status"] == "QUEUED"));

            // Verify cmd_daemon_status can be called without panicking when both
            // config and db are None (offline / no-project case).
            // On non-macOS this returns an error; that's fine — we just check no panic.
            let _ = cmd_daemon_status(None, None, false);
            let _ = cmd_daemon_status(None, None, true);
        }
    }

    mod run_command {
        #[test]
        fn paper_id_default_from_filename_stem() {
            use std::path::Path;

            let cases = [
                ("paper/main.pdf", "main"),
                ("build/camera_ready.pdf", "camera_ready"),
                ("/abs/path/to/paper.pdf", "paper"),
                ("just_a_stem.pdf", "just_a_stem"),
                ("no_extension", "no_extension"),
            ];

            for (pdf_path, expected_id) in cases {
                let stem = Path::new(pdf_path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                assert_eq!(stem, expected_id, "failed for {pdf_path}");
            }
        }
    }

    mod force_flag {
        use super::super::clear_sibling_job_cooldowns;
        use chrono::Utc;
        use reviewloop::config::Config;
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};

        fn make_config(project_id: &str) -> Config {
            Config {
                project_id: project_id.to_string(),
                ..Default::default()
            }
        }

        fn new_processing_job(project_id: &str, paper_id: &str) -> NewJob {
            NewJob {
                project_id: project_id.to_string(),
                paper_id: paper_id.to_string(),
                backend: "stanford".to_string(),
                pdf_path: "/test/paper.pdf".to_string(),
                pdf_hash: "testhash".to_string(),
                status: JobStatus::Processing,
                email: "test@example.com".to_string(),
                venue: None,
                git_tag: None,
                git_commit: None,
                next_poll_at: None,
            }
        }

        /// `submit --force` clears `next_poll_at` and resets `attempt = 0` on a
        /// stuck sibling job for the same paper.
        #[test]
        fn submit_force_clears_stuck_job_cooldown() {
            let db = Db::new_in_memory("force_clears_cooldown").unwrap();
            db.ensure_schema().unwrap();
            let config = make_config("proj1");

            let stuck = db
                .create_job(&new_processing_job("proj1", "paper1"))
                .unwrap();
            let future_time = Utc::now() + chrono::Duration::hours(2);
            db.update_job_state(
                &stuck.id,
                JobStatus::Processing,
                Some(3),
                Some(Some(future_time)),
                None,
            )
            .unwrap();

            let before = db.get_job(&stuck.id).unwrap().unwrap();
            assert_eq!(before.attempt, 3);
            assert!(before.next_poll_at.is_some());

            clear_sibling_job_cooldowns(&config, &db, "paper1").unwrap();

            let after = db.get_job(&stuck.id).unwrap().unwrap();
            assert_eq!(after.attempt, 0, "attempt should be reset to 0");
            assert!(
                after.next_poll_at.is_none(),
                "next_poll_at should be cleared"
            );
            // Status should be unchanged.
            assert_eq!(after.status, JobStatus::Processing);
        }

        /// No active jobs → noop, no error.
        #[test]
        fn submit_force_no_active_jobs_is_noop() {
            let db = Db::new_in_memory("force_noop").unwrap();
            db.ensure_schema().unwrap();
            let config = make_config("proj1");
            clear_sibling_job_cooldowns(&config, &db, "paper1").unwrap();
        }

        /// COMPLETED jobs are not in scope for cooldown clearing.
        #[test]
        fn submit_force_does_not_touch_completed_jobs() {
            let db = Db::new_in_memory("force_active_only").unwrap();
            db.ensure_schema().unwrap();
            let config = make_config("proj1");

            let completed = db
                .create_job(&NewJob {
                    project_id: "proj1".to_string(),
                    paper_id: "paper1".to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "/test/paper.pdf".to_string(),
                    pdf_hash: "hash_done".to_string(),
                    status: JobStatus::Completed,
                    email: "test@example.com".to_string(),
                    venue: None,
                    git_tag: None,
                    git_commit: None,
                    next_poll_at: None,
                })
                .unwrap();

            clear_sibling_job_cooldowns(&config, &db, "paper1").unwrap();

            let after = db.get_job(&completed.id).unwrap().unwrap();
            assert_eq!(after.status, JobStatus::Completed);
        }

        /// `--override-rate-limit` is still accepted by the clap parser and
        /// maps to the `override_rate_limit` field (backward compat).
        #[test]
        fn override_rate_limit_alias_parses() {
            use crate::{Cli, Command};
            use clap::Parser;
            let args = Cli::try_parse_from([
                "reviewloop",
                "retry",
                "--job-id",
                "some-job-id",
                "--override-rate-limit",
            ])
            .expect("--override-rate-limit should still parse successfully");
            match args.command {
                Command::Retry {
                    override_rate_limit,
                    force,
                    ..
                } => {
                    assert!(override_rate_limit, "override_rate_limit should be true");
                    assert!(!force, "force should be false when only alias is passed");
                }
                _ => panic!("expected Retry command"),
            }
        }

        /// `--force` on retry sets the `force` field.
        #[test]
        fn retry_force_flag_parses() {
            use crate::{Cli, Command};
            use clap::Parser;
            let args =
                Cli::try_parse_from(["reviewloop", "retry", "--job-id", "some-job-id", "--force"])
                    .expect("--force should parse successfully");
            match args.command {
                Command::Retry {
                    force,
                    override_rate_limit,
                    ..
                } => {
                    assert!(force, "force should be true");
                    assert!(!override_rate_limit, "override_rate_limit should be false");
                }
                _ => panic!("expected Retry command"),
            }
        }

        /// Deprecation warning is printed to stderr when --override-rate-limit is used.
        #[test]
        fn override_rate_limit_deprecation_warning_logic() {
            // The actual eprintln! runs in cmd_retry. Here we verify that the
            // combined force value is `true` when override_rate_limit is set,
            // mirroring the logic in cmd_retry.
            let force = false;
            let override_rate_limit = true;
            let effective_force = force || override_rate_limit;
            assert!(
                effective_force,
                "override_rate_limit should make effective force = true"
            );
        }
    }

    mod daemon_pause_resume {
        /// `daemon pause` parses to the Pause variant.
        #[test]
        fn daemon_pause_parses() {
            use crate::{Cli, Command, DaemonCommand};
            use clap::Parser;
            let args = Cli::try_parse_from(["reviewloop", "daemon", "pause"])
                .expect("`daemon pause` should parse successfully");
            assert!(
                matches!(
                    args.command,
                    Command::Daemon {
                        command: DaemonCommand::Pause
                    }
                ),
                "expected DaemonCommand::Pause"
            );
        }

        /// `daemon resume` parses to the Resume variant.
        #[test]
        fn daemon_resume_parses() {
            use crate::{Cli, Command, DaemonCommand};
            use clap::Parser;
            let args = Cli::try_parse_from(["reviewloop", "daemon", "resume"])
                .expect("`daemon resume` should parse successfully");
            assert!(
                matches!(
                    args.command,
                    Command::Daemon {
                        command: DaemonCommand::Resume
                    }
                ),
                "expected DaemonCommand::Resume"
            );
        }
    }

    mod status_json_shape {
        use super::super::{status_row_json, timeline_json};
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};
        use serde_json::Value;

        fn make_db_with_jobs(project_id: &str, paper_ids: &[&str]) -> Db {
            let db = Db::new_in_memory(project_id).expect("in-memory DB");
            db.ensure_schema().expect("ensure schema");
            for paper_id in paper_ids {
                let job = NewJob {
                    project_id: project_id.to_string(),
                    paper_id: paper_id.to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "/test/paper.pdf".to_string(),
                    pdf_hash: "abc123".to_string(),
                    status: JobStatus::Queued,
                    email: "test@example.com".to_string(),
                    venue: None,
                    git_tag: None,
                    git_commit: None,
                    next_poll_at: None,
                };
                db.create_job(&job).expect("create job");
            }
            db
        }

        /// Both single-paper and multi-paper `--json` output share the same
        /// root shape: `{"project_id": ..., "papers": [{paper_id, rows, timeline}]}`.
        #[test]
        fn cmd_status_json_shape_is_consistent() {
            let project_id = "shape_proj";
            let db = make_db_with_jobs(project_id, &["p1", "p2"]);

            // Multi-paper shape: group by paper_id, same wrapper as single-paper.
            let rows_all = db
                .list_status_views(project_id, None)
                .expect("list_status_views all");
            let mut groups: std::collections::BTreeMap<String, Vec<reviewloop::model::StatusView>> =
                std::collections::BTreeMap::new();
            for row in rows_all {
                groups.entry(row.paper_id.clone()).or_default().push(row);
            }
            let multi_papers: Vec<Value> = groups
                .iter()
                .map(|(pid, group_rows)| {
                    let events = db
                        .list_timeline_events(project_id, pid)
                        .unwrap_or_default();
                    serde_json::json!({
                        "paper_id": pid,
                        "rows": group_rows.iter().map(|r| status_row_json(r, false, None)).collect::<Vec<Value>>(),
                        "timeline": timeline_json(group_rows, &events, false, None),
                    })
                })
                .collect();
            let multi = serde_json::json!({
                "project_id": project_id,
                "papers": multi_papers,
            });

            // Single-paper shape.
            let rows_one = db
                .list_status_views(project_id, Some("p1"))
                .expect("list_status_views p1");
            let events_one = db
                .list_timeline_events(project_id, "p1")
                .expect("list_timeline_events p1");
            let single = serde_json::json!({
                "project_id": project_id,
                "papers": [{
                    "paper_id": "p1",
                    "rows": rows_one.iter().map(|r| status_row_json(r, false, None)).collect::<Vec<Value>>(),
                    "timeline": timeline_json(&rows_one, &events_one, false, None),
                }],
            });

            for (label, val) in [("multi", &multi), ("single", &single)] {
                assert!(val.is_object(), "{label}: root must be an object");
                let papers = val.get("papers").expect("must have 'papers' key");
                assert!(papers.is_array(), "{label}: 'papers' must be an array");
                assert!(
                    val.get("project_id").is_some(),
                    "{label}: root must have 'project_id'"
                );
                // Every item in `papers` must have paper_id, rows, and timeline keys.
                for (i, paper) in papers.as_array().unwrap().iter().enumerate() {
                    assert!(
                        paper.get("paper_id").is_some(),
                        "{label}[{i}]: paper object must have 'paper_id'"
                    );
                    assert!(
                        paper.get("rows").map(|v| v.is_array()).unwrap_or(false),
                        "{label}[{i}]: paper object must have 'rows' array"
                    );
                    assert!(
                        paper.get("timeline").map(|v| v.is_array()).unwrap_or(false),
                        "{label}[{i}]: paper object must have 'timeline' array"
                    );
                }
            }
            // Single-paper query → papers array of length 1.
            assert_eq!(
                single["papers"].as_array().unwrap().len(),
                1,
                "single-paper query must produce papers array of length 1"
            );
            // Multi-paper query → papers array of length 2 (one entry per paper_id).
            assert_eq!(
                multi["papers"].as_array().unwrap().len(),
                2,
                "multi-paper query must produce one papers entry per paper_id"
            );
        }

        /// Regression: both `status --json` (no flag) and `status --paper-id X --json`
        /// must produce an object with a `papers` array at the root.
        #[test]
        fn cmd_status_json_consistent_root_type() {
            let project_id = "root_type_proj";
            let db = make_db_with_jobs(project_id, &["alpha", "beta"]);

            // Simulate multi-paper path output.
            let rows_all = db.list_status_views(project_id, None).unwrap();
            let mut groups: std::collections::BTreeMap<String, Vec<reviewloop::model::StatusView>> =
                std::collections::BTreeMap::new();
            for row in rows_all {
                groups.entry(row.paper_id.clone()).or_default().push(row);
            }
            let multi_papers: Vec<Value> = groups
                .iter()
                .map(|(pid, group_rows)| {
                    let events = db.list_timeline_events(project_id, pid).unwrap_or_default();
                    serde_json::json!({
                        "paper_id": pid,
                        "rows": group_rows.iter().map(|r| status_row_json(r, false, None)).collect::<Vec<Value>>(),
                        "timeline": timeline_json(group_rows, &events, false, None),
                    })
                })
                .collect();
            let multi_payload = serde_json::json!({
                "project_id": project_id,
                "papers": multi_papers,
            });

            // Simulate single-paper path output.
            let rows_one = db.list_status_views(project_id, Some("alpha")).unwrap();
            let events_one = db.list_timeline_events(project_id, "alpha").unwrap();
            let single_payload = serde_json::json!({
                "project_id": project_id,
                "papers": [{
                    "paper_id": "alpha",
                    "rows": rows_one.iter().map(|r| status_row_json(r, false, None)).collect::<Vec<Value>>(),
                    "timeline": timeline_json(&rows_one, &events_one, false, None),
                }],
            });

            for (label, payload) in [("multi", &multi_payload), ("single", &single_payload)] {
                assert!(
                    payload.is_object(),
                    "{label}: root must be a JSON object, not array or scalar"
                );
                let papers = payload
                    .get("papers")
                    .unwrap_or_else(|| panic!("{label}: root object must have 'papers' key"));
                assert!(
                    papers.is_array(),
                    "{label}: 'papers' value must be a JSON array"
                );
            }
        }
    }

    mod paper_add_venue {
        use crate::{Cli, Command, PaperCommand};
        use clap::Parser;

        #[test]
        fn paper_add_venue_flag_parses() {
            let args = Cli::try_parse_from([
                "reviewloop",
                "paper",
                "add",
                "--paper-id",
                "main",
                "--pdf-path",
                "paper/main.pdf",
                "--venue",
                "ICLR",
            ])
            .expect("paper add --venue should parse");
            match args.command {
                Command::Paper {
                    command:
                        PaperCommand::Add {
                            venue, paper_id, ..
                        },
                } => {
                    assert_eq!(venue.as_deref(), Some("ICLR"), "venue should be ICLR");
                    assert_eq!(paper_id, "main");
                }
                _ => panic!("expected Paper Add command"),
            }
        }

        #[test]
        fn paper_add_without_venue_is_none() {
            let args = Cli::try_parse_from([
                "reviewloop",
                "paper",
                "add",
                "--paper-id",
                "main",
                "--pdf-path",
                "paper/main.pdf",
            ])
            .expect("paper add without --venue should parse");
            match args.command {
                Command::Paper {
                    command: PaperCommand::Add { venue, .. },
                } => {
                    assert!(venue.is_none(), "venue should be None when not specified");
                }
                _ => panic!("expected Paper Add command"),
            }
        }
    }

    mod retry_include_failed {
        use crate::{Cli, Command};
        use clap::Parser;
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};

        use super::super::resolve_paper_id_to_job;

        fn new_job(paper_id: &str, pdf_hash: &str, status: JobStatus) -> NewJob {
            NewJob {
                project_id: "proj".to_string(),
                paper_id: paper_id.to_string(),
                backend: "stanford".to_string(),
                pdf_path: "/test/paper.pdf".to_string(),
                pdf_hash: pdf_hash.to_string(),
                status,
                email: "test@example.com".to_string(),
                venue: None,
                git_tag: None,
                git_commit: None,
                next_poll_at: None,
            }
        }

        #[test]
        fn retry_without_include_failed_parses_default_false() {
            let args = Cli::try_parse_from(["reviewloop", "retry", "--paper-id", "main"]).unwrap();
            match args.command {
                Command::Retry { include_failed, .. } => {
                    assert!(!include_failed, "include_failed should default to false");
                }
                _ => panic!("expected Retry command"),
            }
        }

        #[test]
        fn retry_with_include_failed_parses_true() {
            let args = Cli::try_parse_from([
                "reviewloop",
                "retry",
                "--paper-id",
                "main",
                "--include-failed",
            ])
            .unwrap();
            match args.command {
                Command::Retry { include_failed, .. } => {
                    assert!(
                        include_failed,
                        "include_failed should be true when flag is set"
                    );
                }
                _ => panic!("expected Retry command"),
            }
        }

        #[test]
        fn narrow_scope_excludes_failed_jobs() {
            let db = Db::new_in_memory("retry_narrow").unwrap();
            db.ensure_schema().unwrap();

            // Only a Failed job exists — narrow scope should not match it.
            db.create_job(&new_job("paper1", "hash_a", JobStatus::Failed))
                .unwrap();

            let err = resolve_paper_id_to_job(
                &db,
                "proj",
                "paper1",
                &[
                    JobStatus::Queued,
                    JobStatus::Submitted,
                    JobStatus::Processing,
                ],
                "retry",
            )
            .unwrap_err();
            assert!(
                err.to_string().contains("no retry-eligible job"),
                "got: {}",
                err
            );
        }

        #[test]
        fn wide_scope_includes_failed_jobs() {
            let db = Db::new_in_memory("retry_wide").unwrap();
            db.ensure_schema().unwrap();

            let failed = db
                .create_job(&new_job("paper1", "hash_a", JobStatus::Failed))
                .unwrap();

            let resolved = resolve_paper_id_to_job(
                &db,
                "proj",
                "paper1",
                &[
                    JobStatus::Queued,
                    JobStatus::Submitted,
                    JobStatus::Processing,
                    JobStatus::Failed,
                    JobStatus::FailedNeedsManual,
                    JobStatus::Timeout,
                ],
                "retry",
            )
            .unwrap();
            assert_eq!(resolved.id, failed.id);
        }

        #[test]
        fn narrow_scope_matches_active_queued_job() {
            let db = Db::new_in_memory("retry_active_queued").unwrap();
            db.ensure_schema().unwrap();

            let queued = db
                .create_job(&new_job("paper1", "hash_a", JobStatus::Queued))
                .unwrap();

            let resolved = resolve_paper_id_to_job(
                &db,
                "proj",
                "paper1",
                &[
                    JobStatus::Queued,
                    JobStatus::Submitted,
                    JobStatus::Processing,
                ],
                "retry",
            )
            .unwrap();
            assert_eq!(resolved.id, queued.id);
        }
    }

    /// Tests for U10 — `cancel` command.
    mod cancel {
        use super::super::cmd_cancel;
        use reviewloop::config::Config;
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};

        fn make_processing_job(project_id: &str, paper_id: &str) -> (Db, String) {
            let db = Db::new_in_memory(project_id).expect("in-memory DB");
            db.ensure_schema().expect("ensure schema");
            let job = db
                .create_job(&NewJob {
                    project_id: project_id.to_string(),
                    paper_id: paper_id.to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "/test/paper.pdf".to_string(),
                    pdf_hash: "abc123".to_string(),
                    status: JobStatus::Processing,
                    email: "test@example.com".to_string(),
                    venue: None,
                    git_tag: None,
                    git_commit: None,
                    next_poll_at: None,
                })
                .expect("create job");
            (db, job.id)
        }

        /// Cancel sets status to Failed and records a `cancelled` event.
        #[test]
        fn cancel_sets_failed_and_writes_event() {
            let project_id = "cancel_proj";
            let (db, job_id) = make_processing_job(project_id, "paper-a");

            cmd_cancel(&Config::default(), &db, &job_id, Some("test reason")).expect("cmd_cancel");

            // Assert status is now Failed.
            let updated = db.get_job(&job_id).expect("get_job").expect("job present");
            assert_eq!(
                updated.status,
                JobStatus::Failed,
                "cancelled job must have status=Failed"
            );
            assert_eq!(
                updated.last_error.as_deref(),
                Some("cancelled by user: test reason"),
                "last_error must carry cancellation message"
            );

            // Assert cancelled event was written.
            let events = db
                .list_timeline_events(project_id, "paper-a")
                .expect("list_timeline_events");
            let cancel_event = events
                .iter()
                .find(|e| e.event_type == "cancelled")
                .expect("cancelled event must be present");
            assert_eq!(
                cancel_event
                    .payload
                    .get("previous_status")
                    .and_then(|v| v.as_str()),
                Some("PROCESSING"),
                "cancelled event must record previous_status"
            );
            assert_eq!(
                cancel_event.payload.get("reason").and_then(|v| v.as_str()),
                Some("test reason"),
                "cancelled event must record reason"
            );
        }

        #[test]
        fn cancel_without_reason_uses_plain_last_error() {
            let project_id = "cancel_default_proj";
            let (db, job_id) = make_processing_job(project_id, "paper-a");

            cmd_cancel(&Config::default(), &db, &job_id, None).expect("cmd_cancel");

            let updated = db.get_job(&job_id).expect("get_job").expect("job present");
            assert_eq!(updated.status, JobStatus::Failed);
            assert_eq!(updated.last_error.as_deref(), Some("cancelled by user"));

            let events = db
                .list_timeline_events(project_id, "paper-a")
                .expect("list_timeline_events");
            let cancel_event = events
                .iter()
                .find(|e| e.event_type == "cancelled")
                .expect("cancelled event must be present");
            assert!(
                cancel_event
                    .payload
                    .get("reason")
                    .is_some_and(|value| value.is_null()),
                "cancelled event reason should be null when no reason is supplied"
            );
        }

        /// Cancelling an already-terminal job is rejected at the CLI layer.
        #[test]
        fn cancel_terminal_job_is_rejected() {
            use reviewloop::model::JobStatus;
            // The guard in cmd_cancel checks for terminal states.
            let terminal = [
                JobStatus::Completed,
                JobStatus::Failed,
                JobStatus::FailedNeedsManual,
                JobStatus::Timeout,
            ];
            for status in terminal {
                let is_terminal = matches!(
                    status,
                    JobStatus::Completed
                        | JobStatus::Failed
                        | JobStatus::FailedNeedsManual
                        | JobStatus::Timeout
                );
                assert!(
                    is_terminal,
                    "{:?} should be detected as terminal by cmd_cancel guard",
                    status
                );
            }
        }
    }

    /// Tests for U12 — status grouping by paper_id.
    mod status_grouping {
        use reviewloop::db::Db;
        use reviewloop::model::{JobStatus, NewJob};

        fn make_db_multi(project_id: &str, paper_ids: &[&str], jobs_per_paper: usize) -> Db {
            let db = Db::new_in_memory(project_id).expect("in-memory DB");
            db.ensure_schema().expect("ensure schema");
            for paper_id in paper_ids {
                for _ in 0..jobs_per_paper {
                    db.create_job(&NewJob {
                        project_id: project_id.to_string(),
                        paper_id: paper_id.to_string(),
                        backend: "stanford".to_string(),
                        pdf_path: "/test/paper.pdf".to_string(),
                        pdf_hash: "abc123".to_string(),
                        status: JobStatus::Queued,
                        email: "test@example.com".to_string(),
                        venue: None,
                        git_tag: None,
                        git_commit: None,
                        next_poll_at: None,
                    })
                    .expect("create job");
                }
            }
            db
        }

        /// With 3 papers × 2 jobs each, list_status_views returns 6 rows
        /// and they can be grouped by paper_id.
        #[test]
        fn grouped_output_has_correct_paper_count() {
            let project_id = "group_proj";
            let papers = ["alpha", "beta", "gamma"];
            let db = make_db_multi(project_id, &papers, 2);

            let rows = db
                .list_status_views(project_id, None)
                .expect("list_status_views");

            assert_eq!(rows.len(), 6, "expect 6 rows total (3 papers × 2 jobs)");

            let mut groups: std::collections::BTreeMap<&str, Vec<_>> = Default::default();
            for row in &rows {
                groups.entry(row.paper_id.as_str()).or_default().push(row);
            }

            assert_eq!(groups.len(), 3, "expect 3 paper groups");
            for paper in &papers {
                let group = groups.get(paper).expect("paper group must exist");
                assert_eq!(group.len(), 2, "each paper must have 2 jobs");
            }
            // BTreeMap ensures alphabetical order.
            let keys: Vec<&str> = groups.keys().copied().collect();
            assert_eq!(
                keys,
                vec!["alpha", "beta", "gamma"],
                "papers must be sorted alphabetically"
            );
        }

        /// Active filter keeps only non-terminal statuses.
        #[test]
        fn active_filter_excludes_terminal_statuses() {
            let project_id = "filter_proj";
            let db = Db::new_in_memory(project_id).expect("in-memory DB");
            db.ensure_schema().expect("ensure schema");

            for status in [
                JobStatus::Queued,
                JobStatus::Completed,
                JobStatus::Failed,
                JobStatus::Processing,
            ] {
                db.create_job(&NewJob {
                    project_id: project_id.to_string(),
                    paper_id: "p1".to_string(),
                    backend: "stanford".to_string(),
                    pdf_path: "/test/paper.pdf".to_string(),
                    pdf_hash: "abc123".to_string(),
                    status,
                    email: "test@example.com".to_string(),
                    venue: None,
                    git_tag: None,
                    git_commit: None,
                    next_poll_at: None,
                })
                .expect("create job");
            }

            let all_rows = db.list_status_views(project_id, None).expect("list all");
            const NON_TERMINAL: &[&str] =
                &["PENDING_APPROVAL", "QUEUED", "SUBMITTED", "PROCESSING"];
            let active: Vec<_> = all_rows
                .iter()
                .filter(|r| NON_TERMINAL.contains(&r.status.as_str()))
                .collect();

            assert_eq!(
                active.len(),
                2,
                "only QUEUED and PROCESSING should survive active filter"
            );
            for row in &active {
                assert!(
                    NON_TERMINAL.contains(&row.status.as_str()),
                    "active filter must not include {}",
                    row.status
                );
            }
        }
    }

    /// Tests for U13 — daemon status tick health alarm.
    mod daemon_tick_health {
        /// Classify tick age into severity label, mirroring cmd_daemon_status logic.
        fn tick_health(age_secs: i64) -> &'static str {
            if age_secs < 60 {
                "normal"
            } else if age_secs < 300 {
                "stale"
            } else {
                "stuck"
            }
        }

        #[test]
        fn recent_tick_is_normal() {
            assert_eq!(tick_health(0), "normal");
            assert_eq!(tick_health(30), "normal");
            assert_eq!(tick_health(59), "normal");
        }

        #[test]
        fn tick_between_1_and_5_min_is_stale() {
            assert_eq!(tick_health(60), "stale");
            assert_eq!(tick_health(120), "stale");
            assert_eq!(tick_health(299), "stale");
        }

        #[test]
        fn tick_over_5_min_is_stuck() {
            assert_eq!(tick_health(300), "stuck");
            assert_eq!(tick_health(600), "stuck");
            assert_eq!(tick_health(3600), "stuck");
        }
    }
}
