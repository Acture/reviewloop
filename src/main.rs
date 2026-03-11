use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use reviewloop::artifact::write_review_artifacts;
use reviewloop::config::{
    Config, LegacyConfig, PaperConfig, ProjectConfigFile, default_project_config_path,
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
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    Paper {
        #[command(subcommand)]
        command: PaperCommand,
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
    Check {
        #[arg(long)]
        job_id: Option<String>,
        #[arg(long)]
        paper_id: Option<String>,
        #[arg(long, default_value_t = false)]
        all_processing: bool,
    },
    Status {
        #[arg(long)]
        paper_id: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = false)]
        show_token: bool,
    },
    Retry {
        #[arg(long)]
        job_id: String,
        #[arg(long, default_value_t = false)]
        override_rate_limit: bool,
    },
    Complete {
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        summary_text: Option<String>,
        #[arg(long)]
        summary_url: Option<String>,
        #[arg(long, default_value_t = false)]
        empty_summary: bool,
        #[arg(long)]
        score: Option<f64>,
    },
    Email {
        #[command(subcommand)]
        command: EmailCommand,
    },
    SelfUpdate {
        #[arg(long, value_enum, default_value_t = UpdateMethod::Auto)]
        method: UpdateMethod,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum UpdateMethod {
    Auto,
    Brew,
    Cargo,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    MigrateProject {
        #[arg(long)]
        project_id: String,
        #[arg(long, value_name = "PATH")]
        project_root: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Run {
        #[arg(long, default_value_t = true)]
        panel: bool,
    },
    Install {
        #[arg(long, default_value_t = true)]
        start: bool,
    },
    Uninstall,
    Status,
}

#[derive(Debug, Subcommand)]
enum PaperCommand {
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
        backend: String,
        #[arg(long, default_value_t = true)]
        watch: bool,
        #[arg(long)]
        tag_trigger: Option<String>,
        #[arg(long, default_value_t = false)]
        submit_now: bool,
        #[arg(long, default_value_t = false)]
        no_submit_prompt: bool,
    },
    Watch {
        #[arg(long)]
        paper_id: String,
        #[arg(long)]
        enabled: bool,
    },
    Remove {
        #[arg(long)]
        paper_id: String,
        #[arg(long, default_value_t = false)]
        purge_history: bool,
    },
}

#[derive(Debug, Subcommand)]
enum EmailCommand {
    Login {
        #[arg(long, default_value = "google")]
        provider: String,
    },
    Logout {
        #[arg(long)]
        account: Option<String>,
    },
    Switch {
        #[arg(long)]
        account: String,
    },
    Status,
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
        Command::Config { command } => match command {
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
                } => {
                    let should_submit = cmd_paper_add(PaperAddOptions {
                        config_path: &write_path,
                        paper_id: &paper_id,
                        project_id: project_id.as_deref(),
                        pdf_path: &pdf_path,
                        backend: &backend,
                        watch,
                        tag_trigger: tag_trigger.as_deref(),
                        submit_now,
                        no_submit_prompt,
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
                let (config, db) = load_runtime(config_override.as_deref(), panel_enabled, true)?;
                reviewloop::worker::run_daemon(&config, &db, panel_enabled).await
            }
            DaemonCommand::Install { start } => {
                cmd_daemon_install(config_override.as_deref(), start)
            }
            DaemonCommand::Uninstall => cmd_daemon_uninstall(),
            DaemonCommand::Status => cmd_daemon_status(),
        },
        Command::Submit { paper_id, force } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_submit(&config, &db, &paper_id, force).await
        }
        Command::Approve { job_id } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_approve(&config, &db, &job_id)
        }
        Command::ImportToken {
            paper_id,
            token,
            source,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_import_token(&config, &db, &paper_id, &token, &source)
        }
        Command::Check {
            job_id,
            paper_id,
            all_processing,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_check(
                &config,
                &db,
                job_id.as_deref(),
                paper_id.as_deref(),
                all_processing,
            )
            .await
        }
        Command::Status {
            paper_id,
            json,
            show_token,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_status(&config, &db, paper_id.as_deref(), json, show_token)
        }
        Command::Retry {
            job_id,
            override_rate_limit,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
            cmd_retry(&config, &db, &job_id, override_rate_limit).await
        }
        Command::Complete {
            job_id,
            summary_text,
            summary_url,
            empty_summary,
            score,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false, true)?;
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
    }
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
            "project config {} does not exist. create it with `reviewloop config migrate-project --project-id <id>` or pass --project-id on `paper add`",
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
    backend: &'a str,
    watch: bool,
    tag_trigger: Option<&'a str>,
    submit_now: bool,
    no_submit_prompt: bool,
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

    config.papers.push(PaperConfig {
        id: options.paper_id.to_string(),
        pdf_path: options.pdf_path.to_string(),
        backend: options.backend.to_string(),
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

    let watch_text = if options.watch { "enabled" } else { "disabled" };
    if let Some(trigger) = options.tag_trigger {
        println!(
            "Added paper {paper_id}.\n- backend: {backend}\n- pdf path: {pdf_path}\n- watch: {watch_text}\n- tag trigger: {trigger}\n- config: {}",
            options.config_path.display(),
            paper_id = options.paper_id,
            backend = options.backend,
            pdf_path = options.pdf_path,
        );
    } else {
        println!(
            "Added paper {paper_id}.\n- backend: {backend}\n- pdf path: {pdf_path}\n- watch: {watch_text}\n- config: {}",
            options.config_path.display(),
            paper_id = options.paper_id,
            backend = options.backend,
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
        println!("paper_id {paper_id} not found in config; only history purge was applied.");
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
    let Some(legacy_path) = Config::legacy_global_config_path().filter(|path| path.exists()) else {
        anyhow::bail!("legacy global config not found; nothing to migrate");
    };
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

        let cfg_path = resolve_mutable_project_config_path(config_override)?;
        let cfg_path = fs::canonicalize(&cfg_path).unwrap_or(cfg_path);
        let (config, _db) = load_runtime(Some(&cfg_path), false, true)?;
        ensure_runtime_dirs(&config)?;

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

        let args = vec![
            exe.display().to_string(),
            "--config".to_string(),
            cfg_path.display().to_string(),
            "daemon".to_string(),
            "run".to_string(),
            "--panel".to_string(),
            "false".to_string(),
        ];
        let plist = render_launchd_plist(DAEMON_LABEL, &args, &stdout_log, &stderr_log);
        fs::write(&plist_path, plist)
            .with_context(|| format!("failed to write launchd plist: {}", plist_path.display()))?;

        println!("Installed launchd plist at {}", plist_path.display());

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

fn cmd_daemon_status() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        const DAEMON_LABEL: &str = "ai.reviewloop.daemon";
        let uid = current_uid_string()?;
        let target = format!("gui/{uid}/{DAEMON_LABEL}");
        let output = ProcessCommand::new("launchctl")
            .args(["print", &target])
            .output()
            .context("failed to run launchctl print")?;

        if output.status.success() {
            println!("launchd job is loaded: {target}");
        } else {
            println!("launchd job is not loaded: {target}");
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    {
        anyhow::bail!("`daemon status` is currently supported on macOS only");
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
        .with_context(|| format!("paper_id not found in project config: {paper_id}"))?;

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
            email_account::resolve_submission_email(config, "stanford", None)?,
            Some(config.effective_stanford_venue()),
        ),
        _ => (String::new(), None),
    };

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
    println!("Submitted job {} for paper_id={paper_id}", job.id);
    Ok(())
}

fn cmd_approve(config: &Config, db: &Db, job_id: &str) -> Result<()> {
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

    if let Some(job) = db.find_latest_open_job_for_paper(&config.project_id, paper_id)? {
        db.attach_token_to_job(&job.id, token, next_poll)?;
        db.add_event(
            None,
            Some(&job.id),
            "token_imported",
            json!({ "source": source, "token": token }),
        )?;
        println!("Attached token to existing job {}", job.id);
        return Ok(());
    }

    let paper = config
        .find_paper(paper_id)
        .with_context(|| format!("paper_id not found in project config: {paper_id}"))?;

    let pdf_hash = if Path::new(&paper.pdf_path).exists() {
        sha256_file(Path::new(&paper.pdf_path))?
    } else {
        "unknown".to_string()
    };

    let (email, venue) = match paper.backend.as_str() {
        "stanford" => (
            email_account::resolve_submission_email(config, "stanford", None)?,
            Some(config.effective_stanford_venue()),
        ),
        _ => (String::new(), None),
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
        next_poll_at: Some(next_poll),
    })?;
    db.attach_token_to_job(&job.id, token, next_poll)?;

    db.add_event(
        None,
        Some(&job.id),
        "token_imported",
        json!({ "source": source, "token": token }),
    )?;

    println!("Created job {} and attached imported token", job.id);
    Ok(())
}

async fn cmd_check(
    config: &Config,
    db: &Db,
    job_id: Option<&str>,
    paper_id: Option<&str>,
    all_processing: bool,
) -> Result<()> {
    if job_id.is_some() && (paper_id.is_some() || all_processing) {
        anyhow::bail!("--job-id cannot be combined with --paper-id or --all-processing");
    }

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
) -> Result<()> {
    let rows = db.list_status_views(&config.project_id, paper_id)?;

    if let Some(paper_id) = paper_id {
        let events = db.list_timeline_events(&config.project_id, paper_id)?;
        if as_json {
            let payload = json!({
                "rows": rows.iter().map(|row| status_row_json(row, show_token)).collect::<Vec<_>>(),
                "timeline": timeline_json(&rows, &events, show_token),
            });
            println!("{}", serde_json::to_string_pretty(&payload)?);
            return Ok(());
        }
        render_timeline_text(config, paper_id, &rows, &events, show_token);
        return Ok(());
    }

    if as_json {
        let payload = rows
            .iter()
            .map(|row| status_row_json(row, show_token))
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    if rows.is_empty() {
        println!("No jobs found.");
        return Ok(());
    }

    println!(
        "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<8}  {:<14}  {:<20}  {:<20}  {:<8}",
        "JOB ID",
        "PAPER",
        "BACKEND",
        "STATUS",
        "ATTEMPT",
        "SCORE",
        "TOKEN",
        "NEXT POLL",
        "STARTED",
        "ELAPSED"
    );
    println!("{}", "-".repeat(200));
    let now = Utc::now();
    for row in rows {
        let started = row.started_at.unwrap_or(row.created_at);
        let elapsed = format_elapsed(started, now);
        println!(
            "{:<36}  {:<16}  {:<10}  {:<18}  {:<7}  {:<8}  {:<14}  {:<20}  {:<20}  {:<8}",
            row.id,
            row.paper_id,
            row.backend,
            row.status,
            row.attempt,
            row.score.clone().unwrap_or_else(|| "-".to_string()),
            render_token(row.token.as_deref(), show_token),
            row.next_poll_at
                .map(|v| v.to_rfc3339())
                .unwrap_or_else(|| "-".to_string()),
            started.to_rfc3339(),
            elapsed
        );
        if let Some(err) = row.last_error.clone() {
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

async fn cmd_retry(
    config: &Config,
    db: &Db,
    job_id: &str,
    override_rate_limit: bool,
) -> Result<()> {
    let job = ensure_project_job(config, db, job_id)?;

    if override_rate_limit {
        let previous_next_poll_at = job.next_poll_at.map(|value| value.to_rfc3339());
        if job.token.is_some() {
            if job.status != JobStatus::Processing {
                anyhow::bail!(
                    "--override-rate-limit for token-backed jobs only supports PROCESSING jobs"
                );
            }
            db.add_event(
                Some(&config.project_id),
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
            reviewloop::worker::poll_job(config, db, &job).await?;
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
                "--override-rate-limit for tokenless jobs only supports QUEUED/SUBMITTED/FAILED/FAILED_NEEDS_MANUAL/TIMEOUT"
            );
        }

        db.update_job_state(&job.id, JobStatus::Queued, Some(0), Some(None), Some(None))?;
        db.add_event(
            Some(&config.project_id),
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
        reviewloop::worker::submit_job(config, db, &job.id).await?;
        println!("Immediately retried job {job_id} with rate-limit override");
        return Ok(());
    }

    if job.token.is_some() {
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

    db.add_event(None, Some(&job.id), "retried", json!({}))?;
    println!("Retry scheduled for job {job_id}");

    Ok(())
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
    db.update_job_state(
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
        .ok_or_else(|| anyhow!("job not found in project {}: {}", config.project_id, job_id))
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

fn status_row_json(row: &StatusView, show_token: bool) -> Value {
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
    })
}

fn timeline_json(rows: &[StatusView], events: &[EventRecord], show_token: bool) -> Vec<Value> {
    let mut entries = Vec::new();
    for row in rows {
        entries.push(json!({
            "kind": "job",
            "created_at": row.created_at.to_rfc3339(),
            "job": status_row_json(row, show_token),
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
                "row": status_row_json(row, show_token),
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
            "gmail oauth credentials missing. configure gmail_oauth.client_id/client_secret \
or set REVIEWLOOP_GMAIL_CLIENT_ID and REVIEWLOOP_GMAIL_CLIENT_SECRET \
or build with these values injected at compile time"
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
