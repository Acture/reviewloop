//! reviewloop-bar — menu bar companion for the reviewloop daemon.
//!
//! Read-only against the same SQLite database the daemon writes to.
//! Triggers actions by spawning `reviewloop` subprocesses.
//!
//! Build:   cargo build   --bin reviewloop-bar --features bar
//! Install: cargo install --path . --bin reviewloop-bar --features bar
//!
//! ## Design — fleet view (v3)
//!
//! The bar is a **fleet dashboard**, not a per-project tool. It opens the
//! shared SQLite database and shows status across every project that has
//! activity, grouped by `project_id`. There is no `REVIEWLOOP_PROJECT_ID`
//! requirement; running the bar from any directory works.
//!
//! - Top-level: aggregate counts (active / recent failures / project count)
//! - Per project: a submenu with that project's active jobs and recent
//!   failures, each with retry / open-artifacts / open-log actions
//! - Legacy bucket: jobs with empty `project_id` (pre-Phase-0 data) are
//!   surfaced under a synthetic "(legacy)" group so they remain visible
//!   without contaminating real project counts in the menu header
//! - Submit new… spawns `reviewloop run <pdf>` with cwd set to the PDF's
//!   parent directory so the CLI's own config discovery picks the project
//! - Pause/Resume daemon — state-aware via launchctl
//!
//! ## Daemon scoping (v0.2.0)
//!
//! The bar shows fleet-wide job data from all projects registered in the
//! shared SQLite database. However, the daemon is single-project-bound:
//! the launchd label `ai.reviewloop.daemon` is shared across all projects,
//! and only one daemon can be installed at a time. The bar's "Pause /
//! Resume daemon" buttons control that single daemon. Multi-daemon support
//! is planned for v0.3.0.

use anyhow::{Context as _, Result};
use chrono::Utc;
use muda::{Menu, MenuEvent, MenuItem, PredefinedMenuItem, Submenu};
use reviewloop::config::Config;
use reviewloop::db::Db;
use reviewloop::model::{Job, JobStatus};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tao::event::{Event, StartCause};
use tao::event_loop::{ControlFlow, EventLoopBuilder};
use tray_icon::TrayIconBuilder;

/// Synthetic project_id label used when `Job::project_id` is empty/whitespace.
const LEGACY_PROJECT_LABEL: &str = "(legacy)";

/// Cap on how many recent failures we surface per project, both at the
/// SQL window-function layer and the menu layer.
const FAILURES_PER_PROJECT_LIMIT: usize = 5;

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

    let artifacts_dir = config.state_dir().join("artifacts");
    let log_path: PathBuf = config
        .logging
        .file_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| config.state_dir().join("reviewloop.log"));

    run_tray(db, artifacts_dir, log_path)
}

// ── Icon helpers ─────────────────────────────────────────────────────────────

fn make_icon(r: u8, g: u8, b: u8) -> tray_icon::Icon {
    // 18×18 RGBA: a coloured disc that fills most of the canvas with a thin
    // darker rim for definition on light menu bars. Outside the disc is fully
    // transparent so the icon sits cleanly next to other tray items.
    const SIZE: i32 = 18;
    const CENTER: f32 = 8.5;
    const OUTER: f32 = 8.5;
    const RIM_WIDTH: f32 = 1.2;
    let dim = |c: u8| -> u8 { ((c as u16) * 60 / 100) as u8 };
    let rim = (dim(r), dim(g), dim(b));

    let mut pixels = Vec::with_capacity((SIZE * SIZE * 4) as usize);
    for y in 0..SIZE {
        for x in 0..SIZE {
            let dx = x as f32 - CENTER;
            let dy = y as f32 - CENTER;
            let dist = (dx * dx + dy * dy).sqrt();
            let (pr, pg, pb, pa) = if dist <= OUTER - RIM_WIDTH {
                (r, g, b, 255u8)
            } else if dist <= OUTER {
                let alpha = ((OUTER - dist).clamp(0.0, 1.0) * 255.0) as u8;
                (rim.0, rim.1, rim.2, alpha.max(180))
            } else {
                (0, 0, 0, 0)
            };
            pixels.extend_from_slice(&[pr, pg, pb, pa]);
        }
    }
    tray_icon::Icon::from_rgba(pixels, SIZE as u32, SIZE as u32).expect("icon dimensions are valid")
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
}

fn open_log(path: &Path) {
    open_path(path);
}

#[cfg(target_os = "macos")]
const DAEMON_LABEL: &str = "ai.reviewloop.daemon";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DaemonState {
    loaded: bool,
    running: bool,
}

#[cfg(target_os = "macos")]
fn poll_daemon_state() -> Option<DaemonState> {
    let uid = Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())?;

    let target = format!("gui/{uid}/{DAEMON_LABEL}");
    let loaded = Command::new("launchctl")
        .args(["print", &target])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    let running = if loaded {
        Command::new("launchctl")
            .args(["list", DAEMON_LABEL])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    } else {
        false
    };

    Some(DaemonState { loaded, running })
}

#[cfg(not(target_os = "macos"))]
fn poll_daemon_state() -> Option<DaemonState> {
    None
}

// ── Background state snapshot ────────────────────────────────────────────────

/// State polled by the background thread, read cheaply by the main event loop.
#[derive(Default, Clone)]
struct BarSnapshot {
    all_active: Vec<Job>,
    all_failed: Vec<Job>,
    daemon_state: Option<DaemonState>,
    db_error: Option<String>,
}

/// Run `poll_daemon_state()` in a child thread with a 3-second watchdog.
fn poll_daemon_state_with_timeout() -> Option<DaemonState> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(poll_daemon_state());
    });
    rx.recv_timeout(Duration::from_secs(3)).unwrap_or(None)
}

/// Spawn a background thread that refreshes `snapshot` every `interval`.
fn start_background_poller(
    db_path: PathBuf,
    snapshot: Arc<Mutex<BarSnapshot>>,
    interval: Duration,
) {
    std::thread::spawn(move || {
        loop {
            let db = Db::new_file(db_path.clone());
            let mut new_snap = BarSnapshot::default();
            new_snap.daemon_state = poll_daemon_state_with_timeout();

            if let Err(e) = db.init_schema() {
                new_snap.db_error = Some(format!("init: {e}"));
            } else {
                match db.list_active_jobs_all() {
                    Ok(jobs) => new_snap.all_active = jobs,
                    Err(e) => new_snap.db_error = Some(format!("active: {e}")),
                }
                if new_snap.db_error.is_none() {
                    match db.list_failed_jobs_all_per_project(FAILURES_PER_PROJECT_LIMIT) {
                        Ok(jobs) => new_snap.all_failed = jobs,
                        Err(e) => new_snap.db_error = Some(format!("failed: {e}")),
                    }
                }
            }

            if let Ok(mut snap) = snapshot.lock() {
                *snap = new_snap;
            }

            std::thread::sleep(interval);
        }
    });
}

// ── String helpers ────────────────────────────────────────────────────────────

/// Truncate `s` to at most `n` Unicode scalar values, appending `…` if cut.
fn truncate_chars(s: &str, n: usize) -> String {
    let truncated: String = s.chars().take(n).collect();
    if truncated.chars().count() < s.chars().count() {
        format!("{truncated}…")
    } else {
        truncated
    }
}

/// Returns the project key used for grouping. Empty/whitespace project_ids
/// (pre-Phase-0 legacy data) collapse to a single synthetic bucket.
fn project_key(job: &Job) -> &str {
    if job.project_id.trim().is_empty() {
        LEGACY_PROJECT_LABEL
    } else {
        job.project_id.as_str()
    }
}

// ── Aggregation helpers ──────────────────────────────────────────────────────

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Aggregate {
    active_count: usize,
    failed_count: usize,
    project_count: usize,
}

fn aggregate(snap: &BarSnapshot) -> Aggregate {
    let mut projects: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for j in snap.all_active.iter().chain(snap.all_failed.iter()) {
        projects.insert(project_key(j));
    }
    Aggregate {
        active_count: snap.all_active.len(),
        failed_count: snap.all_failed.len(),
        project_count: projects.len(),
    }
}

/// Group active + failed jobs by project, preserving stable (alphabetical) order.
/// The legacy bucket is appended last so real projects sort first.
fn group_by_project(snap: &BarSnapshot) -> Vec<ProjectGroup> {
    let mut map: BTreeMap<String, ProjectGroup> = BTreeMap::new();
    for j in &snap.all_active {
        let k = project_key(j).to_string();
        map.entry(k.clone())
            .or_insert_with(|| ProjectGroup::new(k))
            .active
            .push(j.clone());
    }
    for j in &snap.all_failed {
        let k = project_key(j).to_string();
        map.entry(k.clone())
            .or_insert_with(|| ProjectGroup::new(k))
            .failed
            .push(j.clone());
    }
    let mut groups: Vec<ProjectGroup> = map.into_values().collect();
    // Move legacy bucket (if present) to the end.
    groups.sort_by_key(|g| (g.id == LEGACY_PROJECT_LABEL, g.id.clone()));
    groups
}

#[derive(Debug, Clone)]
struct ProjectGroup {
    id: String,
    active: Vec<Job>,
    failed: Vec<Job>,
}

impl ProjectGroup {
    fn new(id: String) -> Self {
        Self {
            id,
            active: Vec::new(),
            failed: Vec::new(),
        }
    }
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

fn format_failed_job_label(job: &Job) -> String {
    let err_snippet = job
        .last_error
        .as_deref()
        .map(|e| format!(" · {}", truncate_chars(e, 60)))
        .unwrap_or_default();
    format!(
        "{} · {} · attempt={}{}",
        job.paper_id,
        job.status.as_str(),
        job.attempt,
        err_snippet
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
    /// Retry a specific job (active or failed).
    RetryJob(String),
    /// Cancel an active job (QUEUED / SUBMITTED / PROCESSING).
    CancelJob(String),
    /// Open a native file picker and submit the chosen PDF.
    SubmitNew,
    PauseDaemon,
    ResumeDaemon,
    Quit,
}

// ── Menu signature (rebuild throttling) ──────────────────────────────────────

/// Compact representation of the visible menu state. The main loop only
/// rebuilds the menu when this changes — `tray.set_menu` while a menu is open
/// closes it on macOS, so a 5s blanket rebuild would dismiss menus the user is
/// reading. The icon, by contrast, is updated every tick (cheap and silent).
#[derive(Debug, Clone, PartialEq, Eq)]
struct MenuSignature {
    aggregate: Aggregate,
    daemon_state: Option<DaemonState>,
    db_error: Option<String>,
    last_action: Option<String>,
    /// (project_id, active_job_id, status, attempt, has_next_poll)
    active_jobs: Vec<(String, String, JobStatus, u32, bool)>,
    /// (project_id, failed_job_id, status, attempt, error_snippet)
    failed_jobs: Vec<(String, String, JobStatus, u32, String)>,
}

fn compute_signature(snap: &BarSnapshot, last_action: Option<&str>) -> MenuSignature {
    MenuSignature {
        aggregate: aggregate(snap),
        daemon_state: snap.daemon_state,
        db_error: snap.db_error.clone(),
        last_action: last_action.map(|s| s.to_string()),
        active_jobs: snap
            .all_active
            .iter()
            .map(|j| {
                (
                    project_key(j).to_string(),
                    j.id.clone(),
                    j.status,
                    j.attempt,
                    j.next_poll_at.is_some(),
                )
            })
            .collect(),
        failed_jobs: snap
            .all_failed
            .iter()
            .map(|j| {
                (
                    project_key(j).to_string(),
                    j.id.clone(),
                    j.status,
                    j.attempt,
                    j.last_error
                        .as_deref()
                        .map(|e| truncate_chars(e, 60))
                        .unwrap_or_default(),
                )
            })
            .collect(),
    }
}

// ── Icon colour rules ────────────────────────────────────────────────────────

fn icon_color(snap: &BarSnapshot) -> (u8, u8, u8) {
    if snap.db_error.is_some() {
        (200, 100, 30) // orange — DB error
    } else if !snap.all_failed.is_empty() {
        (200, 30, 30) // red — recent failures
    } else if !snap.all_active.is_empty() {
        (30, 100, 200) // blue — active jobs
    } else {
        (140, 140, 140) // grey — idle
    }
}

// ── Menu builder ─────────────────────────────────────────────────────────────

/// Rebuild the tray menu from scratch and update the click map.
fn rebuild_menu(
    tray: &tray_icon::TrayIcon,
    artifacts_dir: &Path,
    log_path: &Path,
    snapshot: &BarSnapshot,
    last_action: &Arc<Mutex<Option<(String, Instant)>>>,
    click_map: &mut HashMap<muda::MenuId, ClickAction>,
) {
    click_map.clear();

    let menu = Menu::new();

    // ── Last-action summary (TTL 5min) ───────────────────────────────────────
    if let Ok(guard) = last_action.lock() {
        if let Some((summary, ts)) = guard.as_ref() {
            let elapsed = ts.elapsed();
            if elapsed < Duration::from_secs(300) {
                let display = if elapsed >= Duration::from_secs(60) {
                    let mins = elapsed.as_secs() / 60;
                    format!("{summary} ({mins}m ago)")
                } else {
                    summary.clone()
                };
                let item = MenuItem::new(format!("↳ {display}"), false, None);
                let _ = menu.append(&item);
                let _ = menu.append(&PredefinedMenuItem::separator());
            }
        }
    }

    // ── Status header ────────────────────────────────────────────────────────
    let agg = aggregate(snapshot);
    let status_label = if let Some(ref e) = snapshot.db_error {
        format!("DB error: {}", truncate_chars(e, 80))
    } else if agg.active_count == 0 && agg.failed_count == 0 {
        "No active jobs".to_string()
    } else if agg.failed_count > 0 && agg.active_count > 0 {
        format!(
            "{} active · {} recent failure(s) · {} project(s)",
            agg.active_count, agg.failed_count, agg.project_count
        )
    } else if agg.failed_count > 0 {
        format!(
            "{} recent failure(s) · {} project(s)",
            agg.failed_count, agg.project_count
        )
    } else {
        format!(
            "{} active · {} project(s)",
            agg.active_count, agg.project_count
        )
    };
    let status_item = MenuItem::new(format!("Status: {status_label}"), false, None);
    let _ = menu.append(&status_item);

    // ── Per-project submenus ─────────────────────────────────────────────────
    let groups = group_by_project(snapshot);
    if !groups.is_empty() {
        let _ = menu.append(&PredefinedMenuItem::separator());
        for group in &groups {
            let header = format!(
                "{} ({}A · {}F)",
                group.id,
                group.active.len(),
                group.failed.len()
            );
            let project_sub = Submenu::new(&header, true);

            for job in &group.active {
                let label = format_job_label(job);
                let job_sub = Submenu::new(&label, true);

                let retry_item = MenuItem::new("Retry now", true, None);
                let cancel_item = MenuItem::new("Cancel job", true, None);
                let open_art_item = MenuItem::new("Open artifacts", true, None);
                let open_log_item = MenuItem::new("Open log", true, None);

                let _ = job_sub.append(&retry_item);
                let _ = job_sub.append(&cancel_item);
                let _ = job_sub.append(&PredefinedMenuItem::separator());
                let _ = job_sub.append(&open_art_item);
                let _ = job_sub.append(&open_log_item);
                let _ = project_sub.append(&job_sub);

                let job_artifacts = artifacts_dir.join(&job.id);
                click_map.insert(
                    retry_item.id().clone(),
                    ClickAction::RetryJob(job.id.clone()),
                );
                click_map.insert(
                    cancel_item.id().clone(),
                    ClickAction::CancelJob(job.id.clone()),
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

            if !group.failed.is_empty() {
                if !group.active.is_empty() {
                    let _ = project_sub.append(&PredefinedMenuItem::separator());
                }
                let failures_header = MenuItem::new(
                    format!("Recent failures ({})", group.failed.len()),
                    false,
                    None,
                );
                let _ = project_sub.append(&failures_header);
                for job in &group.failed {
                    let label = format_failed_job_label(job);
                    let item = MenuItem::new(&label, true, None);
                    click_map.insert(item.id().clone(), ClickAction::RetryJob(job.id.clone()));
                    let _ = project_sub.append(&item);
                }
            }

            let _ = menu.append(&project_sub);
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

    // "Submit new…" — always enabled. The spawned `reviewloop run` discovers
    // its project from the PDF's parent directory at exec time.
    let submit_item = MenuItem::new("Submit new\u{2026}", true, None);
    click_map.insert(submit_item.id().clone(), ClickAction::SubmitNew);
    let _ = menu.append(&submit_item);

    // Pause / Resume daemon — state-aware.
    #[cfg(target_os = "macos")]
    {
        let _ = menu.append(&PredefinedMenuItem::separator());
        match snapshot.daemon_state {
            None
            | Some(DaemonState {
                loaded: false,
                running: _,
            }) => {
                let _ = menu.append(&MenuItem::new(
                    "Pause/Resume daemon (service not installed)",
                    false,
                    None,
                ));
            }
            Some(DaemonState {
                loaded: true,
                running,
            }) => {
                if running {
                    let pause_item = MenuItem::new("Pause daemon", true, None);
                    let resume_item =
                        MenuItem::new("Resume daemon (currently running)", false, None);
                    click_map.insert(pause_item.id().clone(), ClickAction::PauseDaemon);
                    let _ = menu.append(&pause_item);
                    let _ = menu.append(&resume_item);
                } else {
                    let pause_item = MenuItem::new("Pause daemon (currently stopped)", false, None);
                    let resume_item = MenuItem::new("Resume daemon", true, None);
                    click_map.insert(resume_item.id().clone(), ClickAction::ResumeDaemon);
                    let _ = menu.append(&pause_item);
                    let _ = menu.append(&resume_item);
                }
            }
        }
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

// ── Action executor ──────────────────────────────────────────────────────────

/// Run a short-lived reviewloop subcommand, capture its output, and update
/// the last-action summary.
fn run_action_cmd(
    args: &[&str],
    cwd: Option<&Path>,
    action_name: &str,
    last_action: &Arc<Mutex<Option<(String, Instant)>>>,
) {
    let mut cmd = Command::new("reviewloop");
    cmd.args(args);
    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }
    match cmd.output() {
        Ok(out) if out.status.success() => {
            set_last_action(last_action, format!("{action_name}: OK"));
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let first_line = stderr.lines().next().unwrap_or("unknown error");
            set_last_action(last_action, format!("{action_name} failed: {first_line}"));
        }
        Err(e) => {
            set_last_action(last_action, format!("{action_name} error: {e}"));
        }
    }
}

fn set_last_action(last_action: &Arc<Mutex<Option<(String, Instant)>>>, summary: String) {
    tracing::info!("bar: {summary}");
    if let Ok(mut g) = last_action.lock() {
        *g = Some((summary, Instant::now()));
    }
}

fn execute_action(
    action: &ClickAction,
    last_action: &Arc<Mutex<Option<(String, Instant)>>>,
) -> bool {
    match action {
        ClickAction::OpenArtifacts(path) | ClickAction::OpenJobArtifacts(path) => {
            open_path(path);
        }
        ClickAction::OpenLog(path) | ClickAction::OpenJobLog(path) => {
            open_log(path);
        }
        ClickAction::RetryJob(job_id) => {
            tracing::info!("bar: retry job {job_id}");
            let job_id = job_id.clone();
            let la = Arc::clone(last_action);
            std::thread::spawn(move || {
                run_action_cmd(
                    &["retry", "--job-id", &job_id, "--force"],
                    None,
                    "Retry",
                    &la,
                );
            });
        }
        ClickAction::CancelJob(job_id) => {
            tracing::info!("bar: cancel job {job_id}");
            let job_id = job_id.clone();
            let la = Arc::clone(last_action);
            std::thread::spawn(move || {
                run_action_cmd(
                    &[
                        "cancel",
                        "--job-id",
                        &job_id,
                        "--reason",
                        "cancelled from menu bar",
                    ],
                    None,
                    "Cancel",
                    &la,
                );
            });
        }
        ClickAction::SubmitNew => {
            // rfd::FileDialog::pick_file() is synchronous and must run on the
            // main thread (NSOpenPanel on macOS).
            let file = rfd::FileDialog::new()
                .add_filter("PDF", &["pdf"])
                .pick_file();
            if let Some(path) = file {
                let path_str = path.to_string_lossy().into_owned();
                let parent_dir = path.parent().map(Path::to_path_buf);
                let filename = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| path_str.clone());
                let has_project_config = parent_dir
                    .as_ref()
                    .map(|parent| parent.join("reviewloop.toml").exists())
                    .unwrap_or(false);
                if !has_project_config {
                    tracing::info!(
                        file = %path_str,
                        "bar: submit skipped because selected PDF is not in a directory with reviewloop.toml"
                    );
                    rfd::MessageDialog::new()
                        .set_title("PDF must be inside a project repo")
                        .set_description(format!(
                            "The selected PDF must be in a directory with a `reviewloop.toml` \
                             (created via `reviewloop init project`). Picked file: `{filename}`."
                        ))
                        .set_buttons(rfd::MessageButtons::Ok)
                        .show();
                } else {
                    tracing::info!("bar: submitting {path_str}");
                    let la = Arc::clone(last_action);
                    set_last_action(&la, format!("Submit started: {filename}"));
                    std::thread::spawn(move || {
                        run_action_cmd(&["run", &path_str], parent_dir.as_deref(), "Submit", &la);
                    });
                }
            }
        }
        ClickAction::PauseDaemon => {
            tracing::info!("bar: pausing daemon");
            let la = Arc::clone(last_action);
            std::thread::spawn(move || {
                run_action_cmd(&["daemon", "pause"], None, "Pause", &la);
            });
        }
        ClickAction::ResumeDaemon => {
            tracing::info!("bar: resuming daemon");
            let la = Arc::clone(last_action);
            std::thread::spawn(move || {
                run_action_cmd(&["daemon", "resume"], None, "Resume", &la);
            });
        }
        ClickAction::Quit => return true,
    }
    false
}

// ── Event loop ────────────────────────────────────────────────────────────────

fn run_tray(db: Db, artifacts_dir: PathBuf, log_path: PathBuf) -> Result<()> {
    // On macOS, EventLoop must be created first — it initialises NSApplication.
    let event_loop = EventLoopBuilder::<()>::new().build();

    // Initial tray icon (grey = loading).
    let tray = TrayIconBuilder::new()
        .with_icon(make_icon(140, 140, 140))
        .with_tooltip("reviewloop")
        .build()
        .context("creating tray icon")?;

    let click_map: Rc<RefCell<HashMap<muda::MenuId, ClickAction>>> =
        Rc::new(RefCell::new(HashMap::new()));

    let last_action: Arc<Mutex<Option<(String, Instant)>>> = Arc::new(Mutex::new(None));

    let snapshot: Arc<Mutex<BarSnapshot>> = Arc::new(Mutex::new(BarSnapshot::default()));

    start_background_poller(
        db.path.clone(),
        Arc::clone(&snapshot),
        Duration::from_secs(5),
    );

    // Force an immediate refresh on the first timer tick.
    let mut last_refresh = Instant::now()
        .checked_sub(Duration::from_secs(10))
        .unwrap_or_else(Instant::now);
    let mut last_signature: Option<MenuSignature> = None;

    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::WaitUntil(Instant::now() + Duration::from_secs(5));

        while let Ok(ev) = MenuEvent::receiver().try_recv() {
            let action = click_map.borrow().get(&ev.id).cloned();
            if let Some(action) = action {
                if execute_action(&action, &last_action) {
                    *control_flow = ControlFlow::Exit;
                    return;
                }
            }
        }

        let is_tick = matches!(
            event,
            Event::NewEvents(StartCause::Init)
                | Event::NewEvents(StartCause::ResumeTimeReached { .. })
        );
        if is_tick && last_refresh.elapsed() >= Duration::from_secs(4) {
            last_refresh = Instant::now();
            let snap = snapshot.lock().map(|g| g.clone()).unwrap_or_default();

            // Always refresh the icon — cheap, silent, no menu disruption.
            let (r, g, b) = icon_color(&snap);
            let _ = tray.set_icon(Some(make_icon(r, g, b)));

            // Only rebuild the menu when the visible state has actually
            // changed. tray.set_menu while a menu is open closes it on
            // macOS, so we avoid rebuilding on identical ticks.
            let last_summary = last_action
                .lock()
                .ok()
                .and_then(|g| g.as_ref().map(|(s, _)| s.clone()));
            let sig = compute_signature(&snap, last_summary.as_deref());
            let changed = last_signature.as_ref() != Some(&sig);
            if changed {
                last_signature = Some(sig);
                rebuild_menu(
                    &tray,
                    &artifacts_dir,
                    &log_path,
                    &snap,
                    &last_action,
                    &mut click_map.borrow_mut(),
                );
            }
        }
    });

    #[allow(unreachable_code)]
    Ok(())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use reviewloop::model::JobStatus;

    fn job(project_id: &str, id: &str, status: JobStatus, attempt: u32) -> Job {
        Job {
            id: id.to_string(),
            project_id: project_id.to_string(),
            paper_id: "paper".to_string(),
            backend: "stanford".to_string(),
            pdf_path: "/tmp/x.pdf".to_string(),
            pdf_hash: format!("hash-{id}"),
            status,
            token: None,
            email: "a@b.c".to_string(),
            venue: None,
            git_tag: None,
            git_commit: None,
            attempt,
            started_at: None,
            next_poll_at: None,
            last_error: None,
            fallback_used: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            version_no: 1,
            round_no: 1,
            version_source: "pdf_hash".to_string(),
            version_key: String::new(),
        }
    }

    #[test]
    fn truncate_chars_ascii() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        assert_eq!(truncate_chars("hello world", 5), "hello…");
    }

    #[test]
    fn truncate_chars_non_ascii_no_panic() {
        let chinese = "服务器错误：连接超时，请稍后重试并检查网络设置";
        let result = truncate_chars(chinese, 10);
        assert!(result.chars().count() <= 11);
        assert!(std::str::from_utf8(result.as_bytes()).is_ok());
    }

    #[test]
    fn truncate_chars_emoji_no_panic() {
        let emojis = "🔥💥🚀⭐🎉🌟💫✨🎊🎁extra";
        let result = truncate_chars(emojis, 10);
        assert_eq!(result, "🔥💥🚀⭐🎉🌟💫✨🎊🎁…");
    }

    #[test]
    fn truncate_chars_exact_boundary() {
        let s = "abcde";
        assert_eq!(truncate_chars(s, 5), "abcde");
        assert_eq!(truncate_chars(s, 4), "abcd…");
    }

    #[test]
    fn project_key_collapses_blank_ids_to_legacy() {
        let blank = job("", "j1", JobStatus::Queued, 0);
        let ws = job("   ", "j2", JobStatus::Queued, 0);
        let real = job("proj-a", "j3", JobStatus::Queued, 0);
        assert_eq!(project_key(&blank), LEGACY_PROJECT_LABEL);
        assert_eq!(project_key(&ws), LEGACY_PROJECT_LABEL);
        assert_eq!(project_key(&real), "proj-a");
    }

    #[test]
    fn aggregate_counts_distinct_projects_across_active_and_failed() {
        let snap = BarSnapshot {
            all_active: vec![
                job("a", "1", JobStatus::Queued, 0),
                job("b", "2", JobStatus::Processing, 0),
            ],
            all_failed: vec![
                job("b", "3", JobStatus::Failed, 1),
                job("c", "4", JobStatus::Failed, 1),
            ],
            ..Default::default()
        };
        let agg = aggregate(&snap);
        assert_eq!(agg.active_count, 2);
        assert_eq!(agg.failed_count, 2);
        assert_eq!(agg.project_count, 3); // a, b, c
    }

    #[test]
    fn aggregate_legacy_jobs_count_as_one_project() {
        let snap = BarSnapshot {
            all_active: vec![
                job("", "1", JobStatus::Queued, 0),
                job("   ", "2", JobStatus::Queued, 0),
            ],
            ..Default::default()
        };
        assert_eq!(aggregate(&snap).project_count, 1);
    }

    #[test]
    fn group_by_project_buckets_jobs_and_pushes_legacy_to_end() {
        let snap = BarSnapshot {
            all_active: vec![
                job("zeta", "z1", JobStatus::Queued, 0),
                job("alpha", "a1", JobStatus::Queued, 0),
                job("", "leg1", JobStatus::Queued, 0),
            ],
            all_failed: vec![job("alpha", "a2", JobStatus::Failed, 2)],
            ..Default::default()
        };
        let groups = group_by_project(&snap);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].id, "alpha");
        assert_eq!(groups[0].active.len(), 1);
        assert_eq!(groups[0].failed.len(), 1);
        assert_eq!(groups[1].id, "zeta");
        assert_eq!(groups[2].id, LEGACY_PROJECT_LABEL);
    }

    #[test]
    fn icon_color_priority_db_error_over_failures() {
        let mut snap = BarSnapshot::default();
        snap.db_error = Some("disk full".to_string());
        snap.all_failed.push(job("p", "1", JobStatus::Failed, 1));
        assert_eq!(icon_color(&snap), (200, 100, 30));
    }

    #[test]
    fn icon_color_priority_failures_over_active() {
        let snap = BarSnapshot {
            all_active: vec![job("p", "1", JobStatus::Queued, 0)],
            all_failed: vec![job("p", "2", JobStatus::Failed, 1)],
            ..Default::default()
        };
        assert_eq!(icon_color(&snap), (200, 30, 30));
    }

    #[test]
    fn icon_color_active_only_is_blue() {
        let snap = BarSnapshot {
            all_active: vec![job("p", "1", JobStatus::Queued, 0)],
            ..Default::default()
        };
        assert_eq!(icon_color(&snap), (30, 100, 200));
    }

    #[test]
    fn icon_color_idle_is_grey() {
        assert_eq!(icon_color(&BarSnapshot::default()), (140, 140, 140));
    }

    #[test]
    fn signature_unchanged_for_identical_snapshots() {
        let snap = BarSnapshot {
            all_active: vec![job("p", "1", JobStatus::Queued, 0)],
            ..Default::default()
        };
        let s1 = compute_signature(&snap, None);
        let s2 = compute_signature(&snap, None);
        assert_eq!(s1, s2);
    }

    #[test]
    fn signature_changes_when_active_count_changes() {
        let s1 = compute_signature(&BarSnapshot::default(), None);
        let snap = BarSnapshot {
            all_active: vec![job("p", "1", JobStatus::Queued, 0)],
            ..Default::default()
        };
        let s2 = compute_signature(&snap, None);
        assert_ne!(s1, s2);
    }

    #[test]
    fn signature_changes_when_last_action_changes() {
        let snap = BarSnapshot::default();
        let s1 = compute_signature(&snap, None);
        let s2 = compute_signature(&snap, Some("did a thing"));
        assert_ne!(s1, s2);
    }
}
