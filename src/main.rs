use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};
use reviewloop::config::{Config, PaperConfig};
use reviewloop::db::Db;
use reviewloop::email_account;
use reviewloop::model::{JobStatus, NewJob};
use reviewloop::oauth::{self, google::GoogleOauthProvider};
use reviewloop::util::{compute_next_poll_at, sha256_file};
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
    },
    Retry {
        #[arg(long)]
        job_id: String,
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
        Command::Paper { command } => {
            let write_path = resolve_mutable_config_path(config_override.as_deref())?;
            match command {
                PaperCommand::Add {
                    paper_id,
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
                        pdf_path: &pdf_path,
                        backend: &backend,
                        watch,
                        tag_trigger: tag_trigger.as_deref(),
                        submit_now,
                        no_submit_prompt,
                    })?;
                    if should_submit {
                        let (config, db) = load_runtime(Some(write_path.as_path()), false)?;
                        cmd_submit(&config, &db, &paper_id, false).await?;
                    }
                    Ok(())
                }
                PaperCommand::Watch { paper_id, enabled } => {
                    cmd_paper_watch(&write_path, &paper_id, enabled)
                }
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
                let (config, db) = load_runtime(config_override.as_deref(), panel_enabled)?;
                reviewloop::worker::run_daemon(&config, &db, panel_enabled).await
            }
            DaemonCommand::Install { start } => {
                cmd_daemon_install(config_override.as_deref(), start)
            }
            DaemonCommand::Uninstall => cmd_daemon_uninstall(),
            DaemonCommand::Status => cmd_daemon_status(),
        },
        Command::Submit { paper_id, force } => {
            let (config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_submit(&config, &db, &paper_id, force).await
        }
        Command::Approve { job_id } => {
            let (_config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_approve(&db, &job_id)
        }
        Command::ImportToken {
            paper_id,
            token,
            source,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_import_token(&config, &db, &paper_id, &token, &source)
        }
        Command::Check {
            job_id,
            paper_id,
            all_processing,
        } => {
            let (config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_check(
                &config,
                &db,
                job_id.as_deref(),
                paper_id.as_deref(),
                all_processing,
            )
            .await
        }
        Command::Status { paper_id, json } => {
            let (_config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_status(&db, paper_id.as_deref(), json)
        }
        Command::Retry { job_id } => {
            let (config, db) = load_runtime(config_override.as_deref(), false)?;
            cmd_retry(&config, &db, &job_id)
        }
        Command::Email { command } => {
            let (config, _db) = load_runtime(config_override.as_deref(), false)?;
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

fn resolve_mutable_config_path(config_override: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = config_override {
        return Ok(path.to_path_buf());
    }

    let local = PathBuf::from("reviewloop.toml");
    if local.exists() {
        return Ok(local);
    }

    if let Some(global) = Config::ensure_global_config_file()? {
        return Ok(global);
    }

    anyhow::bail!("unable to resolve writable config path")
}

fn load_or_create_config(path: &Path) -> Result<Config> {
    if path.exists() {
        return Config::load(path);
    }
    let cfg = Config::default();
    cfg.save(path)?;
    Ok(cfg)
}

struct PaperAddOptions<'a> {
    config_path: &'a Path,
    paper_id: &'a str,
    pdf_path: &'a str,
    backend: &'a str,
    watch: bool,
    tag_trigger: Option<&'a str>,
    submit_now: bool,
    no_submit_prompt: bool,
}

fn cmd_paper_add(options: PaperAddOptions<'_>) -> Result<bool> {
    let mut config = load_or_create_config(options.config_path)?;
    if config.find_paper(options.paper_id).is_some() {
        anyhow::bail!("paper_id already exists: {}", options.paper_id);
    }

    config.papers.push(PaperConfig {
        id: options.paper_id.to_string(),
        pdf_path: options.pdf_path.to_string(),
        backend: options.backend.to_string(),
    });
    config.set_paper_watch(options.paper_id, options.watch);
    config.set_paper_tag_trigger(
        options.paper_id,
        options
            .tag_trigger
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_string),
    );
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
    let mut config = load_or_create_config(config_path)?;
    if config.find_paper(paper_id).is_none() {
        anyhow::bail!("paper_id not found: {paper_id}");
    }
    config.set_paper_watch(paper_id, enabled);
    config.save(config_path)?;
    println!(
        "Updated watch setting for paper {paper_id}: {}\n- config: {}",
        if enabled { "enabled" } else { "disabled" },
        config_path.display()
    );
    Ok(())
}

fn prompt_yes_no(prompt: &str) -> Result<bool> {
    print!("{prompt}");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let normalized = input.trim().to_ascii_lowercase();
    Ok(matches!(normalized.as_str(), "y" | "yes"))
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

        let cfg_path = resolve_mutable_config_path(config_override)?;
        let cfg_path = fs::canonicalize(&cfg_path).unwrap_or(cfg_path);
        let config = Config::load(&cfg_path)?;
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
            email_account::resolve_submission_email(config, "stanford", None)?,
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
            email_account::resolve_submission_email(config, "stanford", None)?,
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
        let Some(job) = db.get_job(job_id)? else {
            anyhow::bail!("job not found: {job_id}");
        };
        if job.token.is_none() {
            anyhow::bail!("job {job_id} has no token; cannot poll");
        }
        targets.push(job);
    } else {
        let rows = db.list_status_views(paper_id)?;
        for row in rows {
            if row.status != JobStatus::Processing.as_str() {
                continue;
            }
            let Some(job) = db.get_job(&row.id)? else {
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
        reviewloop::worker::poll_job(config, db, &job).await?;
        let Some(updated) = db.get_job(&job.id)? else {
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
