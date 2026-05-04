//! reviewloop-bar — menu bar companion for the reviewloop daemon.
//!
//! Read-only against the same SQLite database the daemon writes to.
//! Triggers actions by spawning `reviewloop` subprocesses.
//!
//! Build:   cargo build   --bin reviewloop-bar --features bar
//! Install: cargo install --path . --bin reviewloop-bar --features bar
//!
//! ## v2 feature summary
//!
//! Implemented:
//! - Per-active-job submenus (Retry now / Open artifacts / Open log)
//! - "Submit new…" — native file picker via `rfd`, spawns `reviewloop run <pdf>`
//! - "Pause daemon" / "Resume daemon" — shells out to `reviewloop daemon pause/resume`
//! - Cross-platform `open_path` (macOS: `open`, Linux: `xdg-open`, Windows: `explorer`)
//! - Menu rebuilt on every 5 s refresh tick so job list stays current
//!
//! Deferred (noted here for future phases):
//! - **Multi-project switching**: requires `Db::list_known_project_ids()` (future db.rs
//!   addition). For now the bar is single-project, resolved from env
//!   `REVIEWLOOP_PROJECT_ID` or `config.project_id`.
//! - **Retry Failed enumeration**: requires `Db::list_failed_jobs_for_project()`.
//!   A `reviewloop retry --paper-id <id>` is offered per active job instead.
//! - **bar/state.toml persistence**: deferred until multi-project switching lands.

use anyhow::{Context as _, Result};
use chrono::Utc;
use muda::{Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use reviewloop::config::Config;
use reviewloop::db::Db;
use reviewloop::model::Job;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::time::{Duration, Instant};
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder};
use tray_icon::TrayIconBuilder;

fn main() {
    tracing_subscriber::fmt::init();
    if let Err(e) = try_main() {
        eprintln!("reviewloop-bar: {e:#}");
        std::process::exit(1);
    }
}

fn try_main() -> Result<()> {
    let config = Config::load_runtime(None, false).context("loading config")?;
    let db = Db::from_config(&config).context("opening database")?;
    db.init_schema().context("initialising schema")?;

    // Project ID: env var first, then config, else None (shows hint in menu).
    let project_id: Option<String> = std::env::var("REVIEWLOOP_PROJECT_ID").ok().or_else(|| {
        if config.project_id.trim().is_empty() {
            None
        } else {
            Some(config.project_id.clone())
        }
    });

    let artifacts_dir = config.state_dir().join("artifacts");
    let log_path: PathBuf = config
        .logging
        .file_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| config.state_dir().join("reviewloop.log"));

    run_tray(db, project_id, artifacts_dir, log_path)
}

// ── Icon helpers ─────────────────────────────────────────────────────────────

fn make_icon(r: u8, g: u8, b: u8) -> tray_icon::Icon {
    // 16×16 solid-colour RGBA square — good enough for v2.
    let pixels: Vec<u8> = (0..16u32 * 16).flat_map(|_| [r, g, b, 255u8]).collect();
    tray_icon::Icon::from_rgba(pixels, 16, 16).expect("icon dimensions are valid")
}

// ── Platform helpers ─────────────────────────────────────────────────────────

fn open_path(path: &Path) {
    #[cfg(target_os = "macos")]
    {
        let _ = Command::new("open").arg(path).spawn();
    }
    #[cfg(target_os = "linux")]
    {
        let _ = Command::new("xdg-open").arg(path).spawn();
    }
    #[cfg(target_os = "windows")]
    {
        let _ = Command::new("explorer").arg(path).spawn();
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        tracing::warn!("open_path: unsupported platform for {}", path.display());
    }
}

fn open_log(path: &Path) {
    #[cfg(target_os = "macos")]
    {
        // `-t` opens in the default text editor.
        let _ = Command::new("open").arg("-t").arg(path).spawn();
    }
    #[cfg(not(target_os = "macos"))]
    open_path(path);
}

// ── DB helpers ────────────────────────────────────────────────────────────────

/// Returns `(active_job_count, has_failed_jobs)` for the given project.
fn query_status(db: &Db, project_id: &str) -> Result<(usize, bool)> {
    let active = db.list_active_jobs_for_project(project_id)?.len();
    let counts = db.status_counts(project_id)?;
    let failed = counts.get("failed").copied().unwrap_or(0);
    Ok((active, failed > 0))
}

// ── Job label formatting ──────────────────────────────────────────────────────

fn format_job_label(job: &Job) -> String {
    let poll_info = match job.next_poll_at {
        Some(next) => {
            let secs = next.signed_duration_since(Utc::now()).num_seconds();
            if secs > 0 {
                format!(" · in {secs}s")
            } else {
                " · polling…".to_string()
            }
        }
        None => String::new(),
    };
    format!(
        "{} · {} · attempt={}{}",
        job.paper_id,
        job.status.as_str(),
        job.attempt,
        poll_info
    )
}

// ── Click action dispatch ─────────────────────────────────────────────────────

#[derive(Clone)]
enum ClickAction {
    OpenArtifacts(PathBuf),
    OpenLog(PathBuf),
    /// Open the per-job artifact directory.
    OpenJobArtifacts(PathBuf),
    /// Open the shared log for a job (same file, different entry point).
    OpenJobLog(PathBuf),
    /// Retry a specific job.
    RetryJob(String),
    /// Open a native file picker and submit the chosen PDF.
    SubmitNew,
    PauseDaemon,
    ResumeDaemon,
    Quit,
}

// ── Menu builder ─────────────────────────────────────────────────────────────

/// Rebuild the tray menu from scratch and update the click map.
///
/// Called on first tick and every 5 s thereafter.
fn rebuild_menu(
    tray: &tray_icon::TrayIcon,
    db: &Db,
    project_id: Option<&str>,
    artifacts_dir: &Path,
    log_path: &Path,
    click_map: &mut HashMap<muda::MenuId, ClickAction>,
) {
    click_map.clear();

    let menu = Menu::new();

    // ── Project header ────────────────────────────────────────────────────────
    // Multi-project switching is deferred (needs Db::list_known_project_ids).
    let project_label = match project_id {
        Some(p) => format!("Project: {p}"),
        None => "Project: not set — REVIEWLOOP_PROJECT_ID".to_string(),
    };
    let project_item = MenuItem::new(&project_label, false, None);
    let _ = menu.append(&project_item);
    let _ = menu.append(&PredefinedMenuItem::separator());

    // ── Status summary ────────────────────────────────────────────────────────
    let (status_label, icon_r, icon_g, icon_b) = match project_id {
        None => (
            "No project — set REVIEWLOOP_PROJECT_ID".to_string(),
            128u8,
            128u8,
            128u8,
        ),
        Some(pid) => match query_status(db, pid) {
            Err(e) => {
                tracing::warn!("bar: db query error: {e}");
                (format!("DB error: {e}"), 200u8, 100u8, 30u8)
            }
            Ok((active, has_errors)) => {
                if has_errors {
                    (
                        format!("{active} active · recent error(s)"),
                        200u8,
                        30u8,
                        30u8,
                    )
                } else if active > 0 {
                    (format!("{active} active job(s)"), 30u8, 100u8, 200u8)
                } else {
                    ("No active jobs".to_string(), 128u8, 128u8, 128u8)
                }
            }
        },
    };
    let _ = tray.set_icon(Some(make_icon(icon_r, icon_g, icon_b)));
    let status_item = MenuItem::new(format!("Status: {status_label}"), false, None);
    let _ = menu.append(&status_item);

    // ── Per-active-job submenus ───────────────────────────────────────────────
    if let Some(pid) = project_id {
        if let Ok(jobs) = db.list_active_jobs_for_project(pid) {
            if !jobs.is_empty() {
                let _ = menu.append(&PredefinedMenuItem::separator());
                for job in &jobs {
                    let label = format_job_label(job);
                    let job_sub = Submenu::new(&label, true);

                    let retry_item = MenuItem::new("Retry now", true, None);
                    let open_art_item = MenuItem::new("Open artifacts", true, None);
                    let open_log_item = MenuItem::new("Open log", true, None);

                    let _ = job_sub.append(&retry_item);
                    let _ = job_sub.append(&open_art_item);
                    let _ = job_sub.append(&open_log_item);
                    let _ = menu.append(&job_sub);

                    let job_artifacts = artifacts_dir.join(&job.id);
                    click_map.insert(
                        retry_item.id().clone(),
                        ClickAction::RetryJob(job.id.clone()),
                    );
                    click_map.insert(
                        open_art_item.id().clone(),
                        ClickAction::OpenJobArtifacts(job_artifacts),
                    );
                    click_map.insert(
                        open_log_item.id().clone(),
                        ClickAction::OpenJobLog(log_path.to_path_buf()),
                    );
                }
            }
        }
    }

    // ── Top-level actions ─────────────────────────────────────────────────────
    let _ = menu.append(&PredefinedMenuItem::separator());

    let open_artifacts_item = MenuItem::new("Open Artifacts Folder", true, None);
    let open_log_global_item = MenuItem::new("Open Daemon Log", true, None);
    click_map.insert(
        open_artifacts_item.id().clone(),
        ClickAction::OpenArtifacts(artifacts_dir.to_path_buf()),
    );
    click_map.insert(
        open_log_global_item.id().clone(),
        ClickAction::OpenLog(log_path.to_path_buf()),
    );
    let _ = menu.append(&open_artifacts_item);
    let _ = menu.append(&open_log_global_item);

    let _ = menu.append(&PredefinedMenuItem::separator());

    // "Submit new…" — opens native file picker; deferred on unsupported rfd envs.
    let submit_item = MenuItem::new("Submit new\u{2026}", true, None);
    click_map.insert(submit_item.id().clone(), ClickAction::SubmitNew);
    let _ = menu.append(&submit_item);

    // Pause / Resume daemon (macOS-only; shown disabled on other platforms).
    #[cfg(target_os = "macos")]
    {
        let pause_item = MenuItem::new("Pause daemon", true, None);
        let resume_item = MenuItem::new("Resume daemon", true, None);
        click_map.insert(pause_item.id().clone(), ClickAction::PauseDaemon);
        click_map.insert(resume_item.id().clone(), ClickAction::ResumeDaemon);
        let _ = menu.append(&pause_item);
        let _ = menu.append(&resume_item);
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = menu.append(&MenuItem::new(
            "Pause/Resume daemon (macOS only)",
            false,
            None,
        ));
    }

    let _ = menu.append(&PredefinedMenuItem::separator());

    let quit_item = MenuItem::new("Quit reviewloop-bar", true, None);
    click_map.insert(quit_item.id().clone(), ClickAction::Quit);
    let _ = menu.append(&quit_item);

    tray.set_menu(Some(Box::new(menu)));
}

// ── Action executor ───────────────────────────────────────────────────────────

fn execute_action(action: &ClickAction) -> bool {
    match action {
        ClickAction::OpenArtifacts(path) | ClickAction::OpenJobArtifacts(path) => {
            open_path(path);
        }
        ClickAction::OpenLog(path) | ClickAction::OpenJobLog(path) => {
            open_log(path);
        }
        ClickAction::RetryJob(job_id) => {
            tracing::info!("bar: retry job {job_id}");
            let _ = Command::new("reviewloop")
                .args(["retry", "--job-id", job_id, "--force"])
                .spawn();
        }
        ClickAction::SubmitNew => {
            // rfd::FileDialog::pick_file() is synchronous and must run on the
            // main thread (NSOpenPanel on macOS). Since tao's event loop
            // callback runs on the main thread, this is safe.
            let file = rfd::FileDialog::new()
                .add_filter("PDF", &["pdf"])
                .pick_file();
            if let Some(path) = file {
                let path_str = path.to_string_lossy().into_owned();
                tracing::info!("bar: submitting {path_str}");
                let _ = Command::new("reviewloop").args(["run", &path_str]).spawn();
            }
        }
        ClickAction::PauseDaemon => {
            tracing::info!("bar: pausing daemon");
            let _ = Command::new("reviewloop").args(["daemon", "pause"]).spawn();
        }
        ClickAction::ResumeDaemon => {
            tracing::info!("bar: resuming daemon");
            let _ = Command::new("reviewloop")
                .args(["daemon", "resume"])
                .spawn();
        }
        ClickAction::Quit => return true,
    }
    false
}

// ── Event loop ────────────────────────────────────────────────────────────────

fn run_tray(
    db: Db,
    project_id: Option<String>,
    artifacts_dir: PathBuf,
    log_path: PathBuf,
) -> Result<()> {
    // On macOS, EventLoop must be created first — it initialises NSApplication.
    let event_loop = EventLoopBuilder::<()>::new().build();

    // Initial tray icon (grey = loading).
    let tray = TrayIconBuilder::new()
        .with_icon(make_icon(128, 128, 128))
        .with_tooltip("reviewloop")
        .build()
        .context("creating tray icon")?;

    // Shared click-action map; rebuilt on each refresh tick.
    // Rc<RefCell<...>> is fine since tao runs everything on the main thread.
    let click_map: Rc<RefCell<HashMap<muda::MenuId, ClickAction>>> =
        Rc::new(RefCell::new(HashMap::new()));

    // Force an immediate refresh on the first timer tick.
    let mut last_refresh = Instant::now()
        .checked_sub(Duration::from_secs(10))
        .unwrap_or_else(Instant::now);

    // ── Run ────────────────────────────────────────────────────────────────────
    event_loop.run(move |event, _, control_flow| {
        // Wake up at most every 5 seconds to refresh status.
        *control_flow = ControlFlow::WaitUntil(Instant::now() + Duration::from_secs(5));

        // Drain all pending menu-click events.
        while let Ok(ev) = MenuEvent::receiver().try_recv() {
            let action = click_map.borrow().get(&ev.id).cloned();
            if let Some(action) = action {
                if execute_action(&action) {
                    *control_flow = ControlFlow::Exit;
                    return;
                }
            }
        }

        // Refresh on the very first tick and every ~5 s thereafter.
        let is_tick = matches!(
            event,
            Event::NewEvents(StartCause::Init)
                | Event::NewEvents(StartCause::ResumeTimeReached { .. })
        );
        if is_tick && last_refresh.elapsed() >= Duration::from_secs(4) {
            last_refresh = Instant::now();
            rebuild_menu(
                &tray,
                &db,
                project_id.as_deref(),
                &artifacts_dir,
                &log_path,
                &mut click_map.borrow_mut(),
            );
        }
    });

    // `event_loop.run` diverges on macOS (returns `!`). On other platforms it
    // may return; either way this line is never reached in practice.
    #[allow(unreachable_code)]
    Ok(())
}
