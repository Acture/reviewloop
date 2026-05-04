//! reviewloop-bar — menu bar companion for the reviewloop daemon.
//!
//! Read-only against the same SQLite database the daemon writes to.
//! Triggers actions by spawning `reviewloop` subprocesses.
//!
//! Build:   cargo build   --bin reviewloop-bar --features bar
//! Install: cargo install --path . --bin reviewloop-bar --features bar

use anyhow::{Context as _, Result};
use muda::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use reviewloop::config::Config;
use reviewloop::db::Db;
use std::path::{Path, PathBuf};
use std::process::Command;
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
    // 16×16 solid-colour RGBA square — good enough for v1.
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

// ── Tray refresh ─────────────────────────────────────────────────────────────

fn do_refresh(
    tray: &tray_icon::TrayIcon,
    status_item: &MenuItem,
    db: &Db,
    project_id: Option<&str>,
) {
    let Some(pid) = project_id else {
        status_item.set_text("No project — set REVIEWLOOP_PROJECT_ID");
        let _ = tray.set_icon(Some(make_icon(128, 128, 128)));
        return;
    };

    let (label, r, g, b) = match query_status(db, pid) {
        Err(e) => {
            tracing::warn!("bar: db query error: {e}");
            (format!("DB error: {e}"), 200u8, 100u8, 30u8)
        }
        Ok((active, has_errors)) => {
            if has_errors {
                (
                    format!("{active} active job(s) — recent error(s)"),
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
    };

    status_item.set_text(format!("Status: {label}"));
    let _ = tray.set_icon(Some(make_icon(r, g, b)));
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

    // ── Menu ─────────────────────────────────────────────────────────────────
    let status_item = MenuItem::new("Status: loading\u{2026}", false, None);
    let open_artifacts = MenuItem::new("Open Artifacts Folder", true, None);
    let open_log_item = MenuItem::new("Open Daemon Log", true, None);
    let quit_item = MenuItem::new("Quit reviewloop-bar", true, None);

    let menu = Menu::new();
    menu.append(&status_item).context("appending status item")?;
    menu.append(&PredefinedMenuItem::separator())
        .context("appending sep")?;
    menu.append(&open_artifacts)
        .context("appending artifacts item")?;
    menu.append(&open_log_item).context("appending log item")?;
    menu.append(&PredefinedMenuItem::separator())
        .context("appending sep")?;
    menu.append(&quit_item).context("appending quit item")?;

    // ── Tray icon ─────────────────────────────────────────────────────────────
    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_icon(make_icon(128, 128, 128))
        .with_tooltip("reviewloop")
        .build()
        .context("creating tray icon")?;

    // Store IDs before items are moved into the closure.
    let artifacts_id = open_artifacts.id().clone();
    let log_id = open_log_item.id().clone();
    let quit_id = quit_item.id().clone();

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
            if ev.id == artifacts_id {
                open_path(&artifacts_dir);
            } else if ev.id == log_id {
                open_log(&log_path);
            } else if ev.id == quit_id {
                *control_flow = ControlFlow::Exit;
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
            do_refresh(&tray, &status_item, &db, project_id.as_deref());
        }
    });

    // `event_loop.run` diverges on macOS (returns `!`).  On other platforms it
    // may return; either way this line is never reached in practice.
    #[allow(unreachable_code)]
    Ok(())
}
